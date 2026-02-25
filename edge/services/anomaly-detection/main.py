#!/usr/bin/env python3
"""
Anomaly Detector Service (Edge ML)
Consumes transformed sensor data from Redpanda, runs ONNX autoencoder,
adds anomaly_score + is_anomaly, publishes to scored-sensor-data.
"""

import json
import logging
import os
import signal
import sys
from collections import defaultdict, deque
from time import time
from typing import Dict, Any, Deque, Tuple

import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import onnxruntime as ort

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("anomaly-detector")


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

class AnomalyDetector:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

        # --- load model bundle ---
        self._load_bundle(config["artifact_dir"])

        # --- kafka ---
        self.consumer = KafkaConsumer(
            config["input_topic"],
            bootstrap_servers=config["redpanda_brokers"],
            group_id=config["consumer_group"],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=200,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=config["redpanda_brokers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            compression_type="lz4",
        )

        # buffers per (device_id, sensor_type)
        self.buffers: Dict[Tuple[str, str], Deque[float]] = defaultdict(
            lambda: deque(maxlen=self.window_size)
        )

        self.running = False
        self.count_in = 0
        self.count_out = 0

        self.lat_sum_ms = 0.0
        self.lat_n = 0

        logger.info(
            "Loaded model bundle: window={self.window_size}, thr={self.threshold:.6f}, version={self.model_version}"
        )

    def _load_bundle(self, artifact_dir: str):
        preprocess = load_json(os.path.join(artifact_dir, "preprocess.json"))
        thresholds = load_json(os.path.join(artifact_dir, "thresholds.json"))

        self.window_size = int(preprocess["window_size"])
        self.mean = float(preprocess["mean"])
        self.std = float(preprocess["std"])
        self.threshold = float(thresholds["threshold"])

        self.model_version = "unknown"
        meta_path = os.path.join(artifact_dir, "metadata.json")
        if os.path.exists(meta_path):
            self.model_version = load_json(meta_path).get("version", "unknown")

        model_path = os.path.join(artifact_dir, "model.onnx")
        self.sess = ort.InferenceSession(model_path, providers=["CPUExecutionProvider"])
        self.input_name = self.sess.get_inputs()[0].name
        self.output_name = self.sess.get_outputs()[0].name

    def _send_dlq(self, original: Dict[str, Any], error: str):
        dlq_topic = self.config.get("dlq_topic")
        if not dlq_topic:
            return
        msg = {
            "error": error,
            "model_version": self.model_version,
            "original": original,
            "ts": int(time.time()),
        }
        try:
            self.producer.send(dlq_topic, key=str(original.get("device_id", "")), value=msg).get(timeout=10)
            self.count_dlq += 1
        except Exception as e:
            logger.warning(f"DLQ publish failed: {e}")

    def score_window(self, window_raw: np.ndarray) -> float:
        # z-score normalization (same as training/export)
        x = (window_raw - self.mean) / (self.std + 1e-12)
        x = x.astype(np.float32).reshape(1, self.window_size)  # (1, window)
        recon = self.sess.run([self.output_name], {self.input_name: x})[0]
        mse = float(np.mean((recon - x) ** 2))
        return mse

    def process_record(self, data: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        Expects at least:
          device_id, sensor_type, value, timestamp
        """
        # optional filter: only run ML for one sensor type
        only_type = self.config.get("only_sensor_type", "").strip().lower()
        sensor_type = str(data.get("sensor_type", "")).strip().lower()
        if only_type and sensor_type != only_type:
            return None

        device_id = str(data.get("device_id", "")).strip()
        if not device_id or "value" not in data:
            return None

        key = (device_id, sensor_type)
        self.buffers[key].append(float(data["value"]))

        # not enough points yet
        if len(self.buffers[key]) < self.window_size:
            return None

        window = np.array(self.buffers[key], dtype=np.float32)
        score = self.score_window(window)
        is_anom = score > self.threshold

        out = dict(data)
        out["anomaly_score"] = score
        out["is_anomaly"] = bool(is_anom)
        out["model_version"] = self.model_version
        out["window_size"] = self.window_size
        return out

    def start(self):
        logger.info("=" * 50)
        logger.info("Anomaly Detector Service (ONNX)")
        logger.info("=" * 50)
        logger.info("Consuming: {self.config['input_topic']}")
        logger.info("Publishing: {self.config['output_topic']}")
        if self.config.get("dlq_topic"):
            logger.info("DLQ: {self.config['dlq_topic']}")
        if self.config.get("only_sensor_type"):
            logger.info("Filter: only sensor_type={self.config['only_sensor_type']}")

        self.running = True

        try:
            for msg in self.consumer:
                if not self.running:
                    break

                self.count_in += 1
                data = msg.value

                t0 = time.perf_counter()
                try:
                    out = self.process_record(data)
                except Exception as e:
                    self._send_dlq(data, f"process_record failed: {e}")
                    continue

                if out is None:
                    continue

                device_id = str(out.get("device_id", ""))

                try:
                    self.producer.send(
                        self.config["output_topic"],
                        key=device_id,
                        value=out
                    ).get(timeout=10)

                    # commit only after successful publish
                    self.consumer.commit()

                    self.count_out += 1
                except Exception as e:
                    self._send_dlq(out, f"publish/commit failed: {e}")
                    # do NOT commit -> will retry message later
                    continue
                finally:
                    dt_ms = (time.perf_counter() - t0) * 1000.0
                    self.lat_sum_ms += dt_ms
                    self.lat_n += 1

                if self.count_in % 200 == 0:
                    avg = (self.lat_sum_ms / self.lat_n) if self.lat_n else 0.0
                    logger.info(
                        "[INFO] in={self.count_in} out={self.count_out} dlq={self.count_dlq} "
                        "avg_latency_ms={avg:.2f} model={self.model_version}"
                    )

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt")
        finally:
            self.stop()

    def stop(self):
        logger.info("Stopping anomaly-detector...")
        self.running = False
        try:
            if self.producer:
                self.producer.flush()
                self.producer.close()
        except Exception:
            pass
        try:
            if self.consumer:
                self.consumer.close()
        except Exception:
            pass
        logger.info("Totals: in={self.count_in}, out={self.count_out}, dlq={self.count_dlq}")


def signal_handler(signum, frame):
    logger.info("Received signal {signum}")
    sys.exit(0)


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    config = {
        "redpanda_brokers": os.getenv("REDPANDA_BROKERS", "redpanda:9092").split(","),
        "input_topic": os.getenv("INPUT_TOPIC", "transformed-sensor-data"),
        "output_topic": os.getenv("OUTPUT_TOPIC", "scored-sensor-data"),
        "dlq_topic": os.getenv("DLQ_TOPIC", "ml-inference-dlq"),
        "consumer_group": os.getenv("CONSUMER_GROUP", "anomaly-detector"),
        "artifact_dir": os.getenv("ARTIFACT_DIR", "/app/artifacts/anomaly-detector/v1"),
        "only_sensor_type": os.getenv("ONLY_SENSOR_TYPE", "").strip().lower(),
    }

    detector = AnomalyDetector(config)
    detector.start()


if __name__ == "__main__":
    main()
