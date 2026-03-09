#!/usr/bin/env python3
"""
Anomaly Alert Service
Consumes transformed sensor data, aggregates by device+time window,
runs ML inference on complete feature vectors, and logs anomalies.

- Downloads existing model from MinIO on startup (from 'latest' path)
- Polls MinIO for model updates at regular interval
- Buffers sensor readings to match training format (pivoted features)
- Independent consumer group for non-blocking operation
- Atomic model hot-swapping without downtime
"""

import json
import logging
import os
import sys
import time
import io
import threading
from datetime import datetime
from typing import Optional, Dict, Any
from collections import defaultdict

import onnxruntime as ort
import numpy as np
import boto3
from botocore.client import Config as BotoConfig
from kafka import KafkaConsumer

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
    """
    Real-time anomaly detection on edge data streams.
    
    - Downloads model from MinIO on startup (from 'latest' path)
    - Polls MinIO for model updates at regular interval
    - Buffers sensor readings to aggregate complete feature vectors
    - Performs ONNX inference for anomaly detection
    - Atomically hot-swaps models without downtime
    """

    def __init__(self):
        self.config = {
            'kafka_brokers': os.getenv('REDPANDA_BROKERS', 'redpanda:9092'),
            'kafka_topic': os.getenv('REDPANDA_TOPIC', 'transformed-sensor-data'),
            'consumer_group': os.getenv('CONSUMER_GROUP', 'anomaly-alert-group'),
            'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
            'minio_access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            'minio_secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            'model_bucket': os.getenv('MODEL_BUCKET', 'iot-models'),
            'model_name': os.getenv('MODEL_NAME', 'anomaly-detector-autoencoder'),
            'update_interval': int(os.getenv('MODEL_UPDATE_INTERVAL', '30')),
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
        try:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=self.config['minio_endpoint'],
                aws_access_key_id=self.config['minio_access_key'],
                aws_secret_access_key=self.config['minio_secret_key'],
                config=BotoConfig(signature_version='s3v4', connect_timeout=5, read_timeout=10),
                region_name='us-east-1'
            )
            logger.info(f"✓ Connected to MinIO: {self.config['minio_endpoint']}")
        except Exception as e:
            logger.error(f"✗ Failed to connect to MinIO: {e}")
            raise

    def download_model(self, model_name: str = None, prefix: str = None) -> bool:
        """
        Download model from MinIO and perform atomic hot-swap.
        
        If prefix is provided, downloads from that specific version path.
        Otherwise, downloads from 'latest' path (used for polling).
        
        Returns True if model was updated, False if same version or error.
        """
        if self.s3_client is None:
            logger.error("✗ MinIO client not available - cannot download model")
            raise RuntimeError("MinIO client not initialized - service should not have started")
        
        try:
            bucket = self.config['model_bucket']
            if model_name is None:
                model_name = self.config['model_name']
            if prefix is None:
                base = f"models/{model_name}/latest"
            else:
                base = prefix

            # metadata.json (version)
            md = io.BytesIO()
            self.s3_client.download_fileobj(bucket, f"{base}/metadata.json", md)
            md.seek(0)
            metadata = json.loads(md.read().decode("utf-8"))
            model_version = metadata.get("version") or metadata.get("trained_at") or "unknown"

            if model_version == self.current_model_version and self.session is not None:
                logger.debug(f"Model version {model_version} already loaded, skipping download")
                return False

            logger.info(f"Downloading model bundle: version={model_version}, path={base}")

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

            # Load new model FIRST (atomic swap)
            new_sess = ort.InferenceSession(onnx_bytes, providers=["CPUExecutionProvider"])
            new_feature_columns = preprocess.get("feature_columns", preprocess.get("feature_order", []))

            # Atomic swap: update all model state at once
            with self.model_lock:
                old = self.current_model_version
                old_session = self.session
                self.session = new_sess
                self.feature_columns = new_feature_columns
                self.preprocess = preprocess
                self.thresholds = thresholds
                self.model_metadata = metadata
                self.current_model_version = model_version

            # Old session will be garbage collected after lock is released
            if old_session is not None:
                del old_session

            if old is None:
                logger.info(f"✓ Successfully loaded initial model: version={model_version}, features={len(new_feature_columns)}, threshold={thresholds.get('mse_threshold', 'N/A')}")
            else:
                logger.info(f"✓ Successfully swapped model atomically: {old} → {model_version} (features={len(new_feature_columns)})")
            return True

        except Exception as e:
            if self.session is None:
                logger.warning(f"⚠️  No model available - download failed: {e}")
            else:
                logger.warning(f"⚠️  Model download failed, keeping previous version {self.current_model_version}: {e}")
            return False

    def model_update_loop(self):
        """Background thread to poll MinIO for model updates"""
        interval = self.config['update_interval']
        logger.info(f"Model update checker started - polling MinIO every {interval} seconds")
        
        # Add initial random delay to stagger checks across replicas (0-30s)
        import random
        initial_delay = random.uniform(0, 30)
        time.sleep(initial_delay)
        
        last_poll_time = time.time()
        
        while True:
            current_time = time.time()
            if current_time - last_poll_time >= self.config['update_interval']:
                try:
                    updated = self.download_model()  # Polls 'latest' path
                    last_poll_time = current_time
                except Exception as e:
                    logger.error(f"✗ Model update check (polling) failed: {e}")
                    last_poll_time = current_time  # Still update time to avoid tight error loop
            
            time.sleep(1)  # Small sleep to prevent busy waiting

    def setup_kafka(self):
        """Setup Kafka consumer for sensor data (not for model updates)"""
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
        logger.info(f"  Topic: {self.config['kafka_topic']} (sensor data)")
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
        # Check if model is available (thread-safe)
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

    def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single sensor reading. Returns inference result if available, None otherwise."""
        device_id = message.get('device_id')
        sensor_type = message.get('sensor_type')
        value = message.get('value')
        timestamp = message.get('timestamp')
        
        if not all([device_id, sensor_type, value is not None, timestamp]):
            logger.debug("Incomplete message, skipping")
            return None
        
        # Check if model is available before processing
        with self.model_lock:
            has_model = self.session is not None
            if has_model:
                feature_columns = self.feature_columns.copy()
        
        if not has_model:
            logger.debug(f"No model available - discarding message for device={device_id}, sensor={sensor_type}")
            return None
        
        # Add to buffer
        self.sensor_buffer.add_reading(device_id, sensor_type, value, timestamp)
        
        # Try to get complete reading
        complete_features = self.sensor_buffer.get_complete_reading(device_id, feature_columns)
        
        if complete_features:
            # Run inference on complete feature vector
            result = self.run_inference(device_id, complete_features, timestamp)
            
            if result and result['is_anomaly']:
                logger.warning(
                    f"🚨 ANOMALY | device={device_id} | score={result['anomaly_score']:.4f} | threshold={result['threshold']:.4f}"
                )
            
            # Clear buffer for this device after inference
            self.sensor_buffer.remove_device(device_id)
            
            return result  # Return result for tracking

    def run(self):
        """Main processing loop"""
        logger.info("=" * 60)
        logger.info("Anomaly Alert Service Starting")
        logger.info("=" * 60)
        
        # Setup connections
        self.setup_minio()
        
        # Try to download existing model at startup (from 'latest' path)
        has_model = self.download_model()
        if not has_model:
            logger.warning("⚠️  Starting without ML model - will poll MinIO for updates")
        
        self.setup_kafka()
        
        # Start model update checker in background (polls MinIO periodically)
        update_thread = threading.Thread(target=self.model_update_loop, daemon=True)
        update_thread.start()
        
        logger.info("=" * 60)
        if has_model:
            logger.info(f"🚀 Ready to process messages with ML inference (model version: {self.current_model_version})")
        else:
            logger.info("🚀 Ready to process messages (waiting for model)")
        logger.info("=" * 60)
        
        processed = 0
        anomalies = 0
        last_stats_time = time.time()
        
        try:
            for message in self.consumer:
                data = message.value
                
                result = self.process_message(data)
                processed += 1
                if result and result.get('is_anomaly'):
                    anomalies += 1
                
                # Periodic stats
                if time.time() - last_stats_time > 10:
                    buffer_stats = self.sensor_buffer.get_stats()
                    logger.info(f"Stats: processed={processed}, buffered_devices={buffer_stats['total_devices']}, anomalies={anomalies}")
                    last_stats_time = time.time()
                    anomalies = 0  # Reset counter
        
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
