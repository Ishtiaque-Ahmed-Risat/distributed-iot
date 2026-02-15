#!/usr/bin/env python3
"""
Batch Analytics & ML Training Job
Reads historical sensor data from Cassandra, computes aggregations,
and trains/updates an anomaly detection model.

This is designed to run as a periodic job (e.g., hourly or daily).
Currently uses plain Python with pandas/numpy for processing.
For production scale, replace with PySpark (same logic, distributed).

Model artifacts are saved to MinIO (S3-compatible storage).
"""

import json
import logging
import os
import sys
import io
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any

import numpy as np
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from sklearn.ensemble import IsolationForest
import joblib
import boto3
from botocore.client import Config as BotoConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchAnalyticsJob:
    """Reads from Cassandra, computes analytics, trains ML model"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.session = None
        self.s3_client = None

    def setup_cassandra(self):
        """Connect to Cassandra"""
        hosts = self.config['cassandra_hosts']
        cluster = Cluster(
            hosts,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='DC1'),
            protocol_version=4
        )
        self.session = cluster.connect('iot_data')
        logger.info(f"✓ Connected to Cassandra: {hosts}")

    def setup_minio(self):
        """Connect to MinIO (S3-compatible)"""
        self.s3_client = boto3.client(
            's3',
            endpoint_url=self.config['minio_endpoint'],
            aws_access_key_id=self.config['minio_access_key'],
            aws_secret_access_key=self.config['minio_secret_key'],
            config=BotoConfig(signature_version='s3v4'),
            region_name='us-east-1'
        )

        # Ensure bucket exists
        bucket = self.config['model_bucket']
        try:
            self.s3_client.head_bucket(Bucket=bucket)
        except Exception:
            self.s3_client.create_bucket(Bucket=bucket)
            logger.info(f"✓ Created MinIO bucket: {bucket}")

        logger.info(f"✓ Connected to MinIO: {self.config['minio_endpoint']}")

    def fetch_data(self, hours_back: int = 24) -> pd.DataFrame:
        """
        Fetch recent sensor data from Cassandra.
        Reads from time-bucketed partitions for efficiency.
        """
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=hours_back)

        # Get date buckets to query
        dates = []
        d = start_time.date()
        while d <= now.date():
            dates.append(d.strftime('%Y-%m-%d'))
            d += timedelta(days=1)

        # Get list of devices
        device_rows = self.session.execute(
            "SELECT DISTINCT device_id FROM device_latest"
        )
        devices = [r.device_id for r in device_rows]

        if not devices:
            logger.warning("No devices found in Cassandra")
            return pd.DataFrame()

        logger.info(f"Fetching data for {len(devices)} devices, {len(dates)} date buckets")

        all_rows = []
        for device_id in devices:
            for dt in dates:
                rows = self.session.execute(
                    "SELECT device_id, timestamp, sensor_type, value, unit, is_anomaly "
                    "FROM sensor_readings "
                    "WHERE device_id = %s AND date = %s "
                    "AND timestamp >= %s AND timestamp <= %s",
                    (device_id, dt, start_time, now)
                )
                for row in rows:
                    all_rows.append({
                        'device_id': row.device_id,
                        'timestamp': row.timestamp,
                        'sensor_type': row.sensor_type,
                        'value': row.value,
                        'unit': row.unit,
                        'is_anomaly': row.is_anomaly
                    })

        df = pd.DataFrame(all_rows)
        logger.info(f"Fetched {len(df)} records from Cassandra")
        return df

    def compute_aggregations(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Compute per-device, per-sensor aggregations"""
        if df.empty:
            return {}

        stats = {}
        for (device_id, sensor_type), group in df.groupby(['device_id', 'sensor_type']):
            key = f"{device_id}/{sensor_type}"
            values = group['value']
            stats[key] = {
                'device_id': device_id,
                'sensor_type': sensor_type,
                'count': len(values),
                'mean': float(values.mean()),
                'std': float(values.std()) if len(values) > 1 else 0.0,
                'min': float(values.min()),
                'max': float(values.max()),
                'median': float(values.median()),
                'p95': float(values.quantile(0.95)),
                'p05': float(values.quantile(0.05)),
            }

        logger.info(f"Computed aggregations for {len(stats)} device/sensor combinations")
        return stats

    def train_anomaly_model(self, df: pd.DataFrame) -> Any:
        """
        Train an Isolation Forest anomaly detection model.
        Each device+sensor gets features: value, rolling mean, rolling std.

        This is the ML abstraction point — replace with any model:
        - LSTM autoencoder for temporal anomalies
        - Prophet for trend anomalies
        - Custom deep learning model
        """
        if df.empty or len(df) < 10:
            logger.warning("Not enough data to train model")
            return None

        logger.info("Training anomaly detection model (IsolationForest)...")

        # Pivot: one row per timestamp per device, features = sensor values
        pivot = df.pivot_table(
            index=['device_id', 'timestamp'],
            columns='sensor_type',
            values='value',
            aggfunc='first'
        ).reset_index()

        # Fill NaN with column mean
        feature_cols = [c for c in pivot.columns if c not in ['device_id', 'timestamp']]
        pivot[feature_cols] = pivot[feature_cols].fillna(pivot[feature_cols].mean())

        if not feature_cols:
            logger.warning("No feature columns found")
            return None

        X = pivot[feature_cols].values

        # Train Isolation Forest
        model = IsolationForest(
            n_estimators=100,
            contamination=0.05,  # Expect ~5% anomalies
            random_state=42,
            n_jobs=-1
        )
        model.fit(X)

        # Score the training data
        scores = model.decision_function(X)
        predictions = model.predict(X)
        n_anomalies = (predictions == -1).sum()

        logger.info(
            f"Model trained on {len(X)} samples, {len(feature_cols)} features. "
            f"Detected {n_anomalies} anomalies ({100*n_anomalies/len(X):.1f}%)"
        )

        # Package model with metadata
        model_package = {
            'model': model,
            'feature_columns': feature_cols,
            'training_samples': len(X),
            'anomaly_ratio': float(n_anomalies / len(X)),
            'trained_at': datetime.now(timezone.utc).isoformat(),
            'score_stats': {
                'mean': float(scores.mean()),
                'std': float(scores.std()),
                'threshold': float(np.percentile(scores, 5))
            }
        }

        return model_package

    def save_model_to_minio(self, model_package: Dict):
        """Save trained model to MinIO"""
        if model_package is None:
            return

        bucket = self.config['model_bucket']
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

        # Save model artifact
        model_buffer = io.BytesIO()
        joblib.dump(model_package['model'], model_buffer)
        model_buffer.seek(0)

        model_key = f"models/anomaly_detection/model_{timestamp}.joblib"
        self.s3_client.upload_fileobj(model_buffer, bucket, model_key)
        logger.info(f"✓ Saved model to MinIO: {model_key}")

        # Also save as 'latest' for edge to pull
        model_buffer.seek(0)
        self.s3_client.upload_fileobj(model_buffer, bucket, "models/anomaly_detection/latest.joblib")
        logger.info("✓ Updated 'latest' model pointer")

        # Save metadata
        metadata = {
            'feature_columns': model_package['feature_columns'],
            'training_samples': model_package['training_samples'],
            'anomaly_ratio': model_package['anomaly_ratio'],
            'trained_at': model_package['trained_at'],
            'score_stats': model_package['score_stats'],
            'model_path': model_key
        }
        metadata_buffer = io.BytesIO(json.dumps(metadata, indent=2).encode())
        self.s3_client.upload_fileobj(metadata_buffer, bucket, "models/anomaly_detection/latest_metadata.json")
        logger.info("✓ Saved model metadata")

    def save_stats_to_minio(self, stats: Dict):
        """Save aggregation stats to MinIO"""
        if not stats:
            return

        bucket = self.config['model_bucket']
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

        stats_buffer = io.BytesIO(json.dumps(stats, indent=2).encode())
        stats_key = f"analytics/aggregations/stats_{timestamp}.json"
        self.s3_client.upload_fileobj(stats_buffer, bucket, stats_key)
        logger.info(f"✓ Saved aggregation stats to MinIO: {stats_key}")

    def run(self):
        """Execute the batch job"""
        logger.info("=" * 50)
        logger.info("Batch Analytics & ML Training Job")
        logger.info("=" * 50)
        logger.info(f"[CONFIG] Cassandra: {self.config['cassandra_hosts']}")
        logger.info(f"[CONFIG] MinIO: {self.config['minio_endpoint']}")
        logger.info(f"[CONFIG] Hours back: {self.config['hours_back']}")

        start_time = datetime.now(timezone.utc)

        # Setup connections
        self.setup_cassandra()
        self.setup_minio()

        # Step 1: Fetch data
        logger.info("\n--- Step 1: Fetching data from Cassandra ---")
        df = self.fetch_data(hours_back=self.config['hours_back'])

        if df.empty:
            logger.info("No data found. Exiting.")
            return

        # Step 2: Compute aggregations
        logger.info("\n--- Step 2: Computing aggregations ---")
        stats = self.compute_aggregations(df)
        self.save_stats_to_minio(stats)

        # Step 3: Train anomaly detection model
        logger.info("\n--- Step 3: Training anomaly detection model ---")
        model_package = self.train_anomaly_model(df)
        self.save_model_to_minio(model_package)

        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(f"\n{'=' * 50}")
        logger.info(f"Job completed in {elapsed:.1f}s")
        logger.info(f"Records processed: {len(df)}")
        logger.info(f"{'=' * 50}")


def main():
    config = {
        'cassandra_hosts': os.getenv('CASSANDRA_HOSTS', 'cassandra').split(','),
        'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
        'minio_access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        'minio_secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        'model_bucket': os.getenv('MODEL_BUCKET', 'iot-models'),
        'hours_back': int(os.getenv('HOURS_BACK', '24')),
    }

    job = BatchAnalyticsJob(config)

    try:
        job.run()
    except Exception as e:
        logger.error(f"Job failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
