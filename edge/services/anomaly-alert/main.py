#!/usr/bin/env python3
"""
Anomaly Alert Service
Consumes transformed sensor data, aggregates by device+time window,
runs ML inference on complete feature vectors, and logs anomalies.

- Downloads model from MinIO on startup
- Polls for model updates every 60 seconds
- Buffers sensor readings to match training format (pivoted features)
- Independent consumer group for non-blocking operation
- Graceful restart on model updates
"""

import json
import logging
import os
import sys
import time
import io
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from collections import defaultdict

import onnxruntime as ort
import numpy as np
import boto3
from botocore.client import Config as BotoConfig
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SensorBuffer:
    """Buffers sensor readings by device and time window for aggregation"""
    
    def __init__(self, window_seconds: int = 10, max_buffer_size: int = 10000):
        self.window_seconds = window_seconds
        self.max_buffer_size = max_buffer_size
        self.buffer = defaultdict(dict)  # {device_id: {sensor_type: (value, timestamp)}}
        self.last_cleanup = time.time()
        self.cleanup_interval = 30  # seconds
    
    def add_reading(self, device_id: str, sensor_type: str, value: float, timestamp: str):
        """Add a sensor reading to the buffer"""
        self.buffer[device_id][sensor_type] = (value, timestamp)
        
        # Periodic cleanup to prevent memory growth
        if time.time() - self.last_cleanup > self.cleanup_interval:
            self._cleanup_old_entries()
    
    def _cleanup_old_entries(self):
        """Remove stale device entries"""
        current_time = time.time()
        devices_to_remove = []
        
        for device_id, sensors in self.buffer.items():
            if not sensors:
                devices_to_remove.append(device_id)
                continue
            
            # Check if all sensor readings are old
            all_old = True
            for sensor_type, (value, timestamp) in list(sensors.items()):
                try:
                    ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    age = (datetime.now(ts.tzinfo) - ts).total_seconds()
                    if age < 60:  # Keep readings less than 1 minute old
                        all_old = False
                    else:
                        del sensors[sensor_type]
                except:
                    pass
            
            if all_old or not sensors:
                devices_to_remove.append(device_id)
        
        for device_id in devices_to_remove:
            del self.buffer[device_id]
        
        if devices_to_remove:
            logger.debug(f"Cleaned up {len(devices_to_remove)} stale device buffers")
        
        self.last_cleanup = current_time
        
        # Hard limit on buffer size
        if len(self.buffer) > self.max_buffer_size:
            # Keep only the most recently updated devices
            sorted_devices = sorted(
                self.buffer.items(),
                key=lambda x: max((t for _, t in x[1].values()), default=''),
                reverse=True
            )
            self.buffer = dict(sorted_devices[:self.max_buffer_size])
            logger.warning(f"Buffer size exceeded, trimmed to {self.max_buffer_size} devices")
    
    def get_complete_reading(self, device_id: str, required_features: list) -> Optional[Dict[str, float]]:
        """Get complete feature vector if all sensors are present"""
        if device_id not in self.buffer:
            return None
        
        device_sensors = self.buffer[device_id]
        
        # Check if we have all required features
        if not all(feature in device_sensors for feature in required_features):
            return None
        
        # Build feature vector
        features = {}
        for feature in required_features:
            value, timestamp = device_sensors[feature]
            features[feature] = value
        
        return features
    
    def remove_device(self, device_id: str):
        """Remove device from buffer after inference"""
        if device_id in self.buffer:
            del self.buffer[device_id]
    
    def get_stats(self):
        """Get buffer statistics"""
        return {
            'total_devices': len(self.buffer),
            'total_readings': sum(len(sensors) for sensors in self.buffer.values())
        }


class AnomalyAlertService:
    """Real-time anomaly detection on edge data streams"""

    def __init__(self):
        self.config = {
            'kafka_brokers': os.getenv('REDPANDA_BROKERS', 'redpanda:9092'),
            'kafka_topic': os.getenv('REDPANDA_TOPIC', 'transformed-sensor-data'),
            'consumer_group': os.getenv('CONSUMER_GROUP', 'anomaly-alert-group'),
            'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
            'minio_access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            'minio_secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            'model_bucket': os.getenv('MODEL_BUCKET', 'iot-models'),
            'update_interval': int(os.getenv('MODEL_UPDATE_INTERVAL', '60')),
            'buffer_window': int(os.getenv('BUFFER_WINDOW_SECONDS', '10')),
            'max_buffer_size': int(os.getenv('MAX_BUFFER_SIZE', '10000')),
        }
        
        self.session = None
        self.feature_columns = []
        self.preprocess = {}
        self.thresholds = {}       
        self.model_metadata = {}
        self.current_model_version = None
        self.s3_client = None
        self.consumer = None
        self.model_lock = threading.Lock()  # For atomic model updates
        self.sensor_buffer = SensorBuffer(
            window_seconds=self.config['buffer_window'],
            max_buffer_size=self.config['max_buffer_size']
        )

    def setup_minio(self):
        """Connect to MinIO"""
        self.s3_client = boto3.client(
            's3',
            endpoint_url=self.config['minio_endpoint'],
            aws_access_key_id=self.config['minio_access_key'],
            aws_secret_access_key=self.config['minio_secret_key'],
            config=BotoConfig(signature_version='s3v4', connect_timeout=5, read_timeout=10),
            region_name='us-east-1'
        )
        logger.info(f"✓ Connected to MinIO: {self.config['minio_endpoint']}")

    def download_model(self) -> bool:
        try:
            bucket = self.config['model_bucket']
            model_name = os.getenv("MODEL_NAME", "anomaly-detector-autoencoder")
            base = f"models/{model_name}/latest"

            # metadata.json (version)
            md = io.BytesIO()
            self.s3_client.download_fileobj(bucket, f"{base}/metadata.json", md)
            md.seek(0)
            metadata = json.loads(md.read().decode("utf-8"))
            model_version = metadata.get("version") or metadata.get("trained_at") or "unknown"

            if model_version == self.current_model_version and self.session is not None:
                return False

            logger.info(f"Downloading new AE bundle version: {model_version}")

            # preprocess.json
            pp = io.BytesIO()
            self.s3_client.download_fileobj(bucket, f"{base}/preprocess.json", pp)
            pp.seek(0)
            preprocess = json.loads(pp.read().decode("utf-8"))

            # thresholds.json
            th = io.BytesIO()
            self.s3_client.download_fileobj(bucket, f"{base}/thresholds.json", th)
            th.seek(0)
            thresholds = json.loads(th.read().decode("utf-8"))

            # model.onnx
            onnx_buf = io.BytesIO()
            self.s3_client.download_fileobj(bucket, f"{base}/model.onnx", onnx_buf)
            onnx_bytes = onnx_buf.getvalue()

            new_sess = ort.InferenceSession(onnx_bytes, providers=["CPUExecutionProvider"])

            with self.model_lock:
                old = self.current_model_version
                self.session = new_sess
                # feature order tulee preprocess.json:stä (tai metadata.json:stä)
                self.feature_columns = preprocess.get("feature_columns", preprocess.get("feature_order", []))
                self.preprocess = preprocess
                self.thresholds = thresholds
                self.model_metadata = metadata
                self.current_model_version = model_version

            if old is None:
                logger.info(f"✓ Loaded initial AE model: {model_version}")
            else:
                logger.warning(f"🔄 AE model updated: {old} → {model_version}")

            logger.info(f"  Features: {self.feature_columns}")
            logger.info(f"  mse_threshold={thresholds.get('mse_threshold')}")
            return True

        except Exception as e:
            logger.error(f"Failed to download/swap AE bundle: {e}")
            if self.session is None:
                logger.warning("⚠️  No AE model available yet - waiting...")
                return False
            logger.warning("Keeping previous model version")
            return False

    def model_update_loop(self):
        """Background thread to check for model updates and hot-swap"""
        logger.info(f"Model update checker started (interval: {self.config['update_interval']}s)")
        
        # Add initial random delay to stagger checks across replicas (0-30s)
        # Reduces MinIO load spikes when multiple pods check simultaneously
        import random
        initial_delay = random.uniform(0, 30)
        logger.info(f"Initial delay: {initial_delay:.1f}s (staggers checks across replicas)")
        time.sleep(initial_delay)
        
        while True:  # Run forever, no restart needed
            time.sleep(self.config['update_interval'])
            try:
                updated = self.download_model()
                if updated:
                    logger.info("Model update complete - continuing inference with new model")
            except Exception as e:
                logger.error(f"Model update check failed: {e}")

    def setup_kafka(self):
        """Setup Kafka consumer"""
        self.consumer = KafkaConsumer(
            self.config['kafka_topic'],
            bootstrap_servers=self.config['kafka_brokers'],
            group_id=self.config['consumer_group'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )
        logger.info(f"✓ Connected to Kafka: {self.config['kafka_brokers']}")
        logger.info(f"  Topic: {self.config['kafka_topic']}")
        logger.info(f"  Consumer group: {self.config['consumer_group']}")

    def prepare_features(self, feature_dict: Dict[str, float]) -> Optional[np.ndarray]:
        """Convert feature dictionary to numpy array in correct order (thread-safe)"""
        try:
            # Get feature columns under lock
            with self.model_lock:
                feature_columns = self.feature_columns.copy()
            
            # Build feature vector in the same order as training
            features = []
            for feature_name in feature_columns:
                value = feature_dict.get(feature_name)
                if value is None:
                    logger.warning(f"Missing feature '{feature_name}' in aggregated data")
                    return None
                features.append(float(value))
            
            return np.array([features])
        
        except Exception as e:
            logger.error(f"Feature extraction failed: {e}")
            return None

    def run_inference(self, device_id: str, feature_dict: Dict[str, float], timestamp: str) -> Optional[Dict[str, Any]]:
        with self.model_lock:
            if self.session is None:
                return None
            sess = self.session
            feature_columns = self.feature_columns.copy()
            preprocess = dict(self.preprocess)
            thresholds = dict(self.thresholds)

        vec = []
        for name in feature_columns:
            v = feature_dict.get(name)
            if v is None:
                return None
            vec.append(float(v))

        x = np.array(vec, dtype=np.float32)

        mean = preprocess.get("mean", 0.0)
        std = preprocess.get("std", 1.0)

        if isinstance(mean, list):
            mean = np.asarray(mean, dtype=np.float32)
            std = np.asarray(std, dtype=np.float32)
            std = np.where(std < 1e-12, 1.0, std)
            xn = (x - mean) / std
        else:
            m = float(mean)
            s = float(std) if float(std) > 1e-12 else 1.0
            xn = (x - m) / s

        xb = xn.reshape(1, -1)

        try:
            recon = sess.run(["output"], {"input": xb})[0]
            mse = float(np.mean((recon - xb) ** 2))
            thr = float(thresholds.get("mse_threshold", thresholds.get("threshold", 0.0)))

            return {
                "is_anomaly": mse > thr,
                "anomaly_score": mse,
                "threshold": thr,
                "device_id": device_id,
                "features": feature_dict,
                "timestamp": timestamp,
            }
        except Exception as e:
            logger.error(f"Inference failed: {e}")
            return None

    def process_message(self, message: Dict[str, Any]):
        """Process a single sensor reading"""
        device_id = message.get('device_id')
        sensor_type = message.get('sensor_type')
        value = message.get('value')
        timestamp = message.get('timestamp')
        
        if not all([device_id, sensor_type, value is not None, timestamp]):
            logger.warning("Incomplete message, skipping")
            return
        
        # Add to buffer
        self.sensor_buffer.add_reading(device_id, sensor_type, value, timestamp)
        
        # Try to get complete reading
        complete_features = self.sensor_buffer.get_complete_reading(device_id, self.feature_columns)
        
        if complete_features:
            # Run inference on complete feature vector
            result = self.run_inference(device_id, complete_features, timestamp)
            
            if result and result['is_anomaly']:
                # Log anomaly with details
                features_str = ', '.join([f"{k}={v:.2f}" for k, v in complete_features.items()])
                logger.warning(
                    f"🚨 ANOMALY DETECTED | "
                    f"device={device_id} | "
                    f"features=[{features_str}] | "
                    f"score={result['anomaly_score']:.4f} | "
                    f"threshold={result['threshold']:.4f} | "
                    f"timestamp={timestamp}"
                )
            
            # Clear buffer for this device after inference
            self.sensor_buffer.remove_device(device_id)

    def run(self):
        """Main processing loop"""
        logger.info("=" * 60)
        logger.info("Anomaly Alert Service Starting")
        logger.info("=" * 60)
        
        # Setup connections
        self.setup_minio()
        
        # Try to download model (don't crash if not available)
        has_model = self.download_model()
        if not has_model:
            logger.warning("⚠️  Starting without ML model - will retry every 60s")
        
        self.setup_kafka()
        
        # Start model update checker in background
        update_thread = threading.Thread(target=self.model_update_loop, daemon=True)
        update_thread.start()
        
        logger.info("=" * 60)
        if has_model:
            logger.info("🚀 Ready to process messages with ML inference")
        else:
            logger.info("🚀 Ready to process messages (pass-through mode, waiting for model)")
        logger.info(f"   Buffer window: {self.config['buffer_window']}s")
        logger.info(f"   Max buffer size: {self.config['max_buffer_size']} devices")
        logger.info("=" * 60)
        
        processed = 0
        anomalies = 0
        last_stats_time = time.time()
        
        try:
            for message in self.consumer:
                data = message.value
                
                self.process_message(data)
                processed += 1
                
                # Periodic stats
                if time.time() - last_stats_time > 60:
                    buffer_stats = self.sensor_buffer.get_stats()
                    logger.info(
                        f"📊 Processed: {processed} messages | "
                        f"Buffered devices: {buffer_stats['total_devices']} | "
                        f"Buffered readings: {buffer_stats['total_readings']}"
                    )
                    last_stats_time = time.time()
        
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
            sys.exit(1)
        finally:
            if self.consumer:
                self.consumer.close()
            logger.info("Service stopped")


def main():
    service = AnomalyAlertService()
    service.run()


if __name__ == '__main__':
    main()
