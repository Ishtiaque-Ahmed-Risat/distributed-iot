#!/usr/bin/env python3
"""
Batch Analytics & ML Training Job with PySpark
Reads historical sensor data from Cassandra using Spark,
computes aggregations, and trains an anomaly detection model.

This is designed to run as a periodic job (e.g., hourly or daily).
Uses PySpark for distributed processing of large datasets.

Model artifacts are saved to MinIO (S3-compatible storage).
"""

import json
import logging
import os
import sys
import io
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType
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


class SparkBatchAnalyticsJob:
    """Reads from Cassandra using Spark, computes analytics, trains ML model"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.spark = None
        self.s3_client = None

    def setup_spark(self):
        """Initialize Spark session with Cassandra connector"""
        logger.info("Initializing Spark session...")
        
        # Download Cassandra connector if needed (handled by Spark packages)
        self.spark = SparkSession.builder \
            .appName("IoT-ML-Training-Job") \
            .config("spark.master", "local[*]") \
            .config("spark.cassandra.connection.host", self.config['cassandra_hosts'][0]) \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        
        # Reduce verbose logging
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info(f"✓ Spark session initialized (version {self.spark.version})")

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

    def fetch_data(self, hours_back: int = 24):
        """
        Fetch recent sensor data from Cassandra using Spark.
        Returns a Spark DataFrame.
        """
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=hours_back)

        logger.info(f"Fetching data from Cassandra (last {hours_back} hours)...")

        # Read from Cassandra using Spark
        df = self.spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="sensor_readings", keyspace="iot_data") \
            .load()

        # Filter by timestamp
        df = df.filter(
            (F.col("timestamp") >= start_time) & 
            (F.col("timestamp") <= now)
        )

        count = df.count()
        logger.info(f"Fetched {count} records from Cassandra")
        
        if count == 0:
            logger.warning("No data found in the specified time range")
        
        return df

    def compute_aggregations(self, df):
        """
        Compute per-device, per-sensor aggregations using Spark.
        Returns a Python dict with aggregation results.
        """
        if df.rdd.isEmpty():
            return {}

        logger.info("Computing aggregations with Spark...")

        # Group by device_id and sensor_type, compute stats
        agg_df = df.groupBy("device_id", "sensor_type").agg(
            F.count("value").alias("count"),
            F.mean("value").alias("mean"),
            F.stddev("value").alias("std"),
            F.min("value").alias("min"),
            F.max("value").alias("max"),
            F.expr("percentile_approx(value, 0.5)").alias("median"),
            F.expr("percentile_approx(value, 0.95)").alias("p95"),
            F.expr("percentile_approx(value, 0.05)").alias("p05")
        )

        # Collect results (this is small - one row per device/sensor combo)
        results = agg_df.collect()
        
        stats = {}
        for row in results:
            key = f"{row.device_id}/{row.sensor_type}"
            stats[key] = {
                'device_id': row.device_id,
                'sensor_type': row.sensor_type,
                'count': int(row['count']),
                'mean': float(row['mean']) if row['mean'] is not None else 0.0,
                'std': float(row['std']) if row['std'] is not None else 0.0,
                'min': float(row['min']) if row['min'] is not None else 0.0,
                'max': float(row['max']) if row['max'] is not None else 0.0,
                'median': float(row['median']) if row['median'] is not None else 0.0,
                'p95': float(row['p95']) if row['p95'] is not None else 0.0,
                'p05': float(row['p05']) if row['p05'] is not None else 0.0,
            }

        logger.info(f"Computed aggregations for {len(stats)} device/sensor combinations")
        return stats

    def train_anomaly_model(self, df):
        """
        Train an Isolation Forest anomaly detection model.
        Uses Spark for data preparation, sklearn for model training.
        """
        if df.rdd.isEmpty():
            logger.warning("No data available for training")
            return None

        count = df.count()
        if count < 10:
            logger.warning(f"Not enough data to train model (only {count} records)")
            return None

        logger.info("Training anomaly detection model (IsolationForest)...")

        # Pivot data: each row is (device_id, timestamp) with sensor values as columns
        pivot_df = df.groupBy("device_id", "timestamp").pivot("sensor_type").agg(
            F.first("value")
        )

        # Get feature columns (sensor types)
        feature_cols = [col for col in pivot_df.columns if col not in ['device_id', 'timestamp']]
        
        if not feature_cols:
            logger.warning("No feature columns found after pivot")
            return None

        logger.info(f"Training on {count} samples with {len(feature_cols)} features: {feature_cols}")

        # Fill nulls with 0 (or could use mean)
        for col in feature_cols:
            pivot_df = pivot_df.fillna({col: 0.0})

        # Collect feature data to numpy array for sklearn
        # For huge datasets, you'd use Spark ML instead, but sklearn is simpler
        feature_data = pivot_df.select(feature_cols).toPandas().values

        # Train Isolation Forest
        model = IsolationForest(
            n_estimators=100,
            contamination=0.05,  # Expect ~5% anomalies
            random_state=42,
            n_jobs=-1
        )
        model.fit(feature_data)

        # Score the training data
        scores = model.decision_function(feature_data)
        predictions = model.predict(feature_data)
        n_anomalies = (predictions == -1).sum()

        logger.info(
            f"Model trained on {len(feature_data)} samples, {len(feature_cols)} features. "
            f"Detected {n_anomalies} anomalies ({100*n_anomalies/len(feature_data):.1f}%)"
        )

        # Package model with metadata
        model_package = {
            'model': model,
            'feature_columns': feature_cols,
            'training_samples': len(feature_data),
            'anomaly_ratio': float(n_anomalies / len(feature_data)),
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

        # Save model artifact (serialize once, use twice)
        model_buffer = io.BytesIO()
        joblib.dump(model_package['model'], model_buffer)
        model_data = model_buffer.getvalue()

        # Upload versioned model
        model_key = f"models/anomaly_detection/model_{timestamp}.joblib"
        self.s3_client.upload_fileobj(io.BytesIO(model_data), bucket, model_key)
        logger.info(f"✓ Saved model to MinIO: {model_key}")

        # Also save as 'latest' for edge to pull
        self.s3_client.upload_fileobj(io.BytesIO(model_data), bucket, "models/anomaly_detection/latest.joblib")
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
        logger.info("Batch Analytics & ML Training Job (PySpark)")
        logger.info("=" * 50)
        logger.info(f"[CONFIG] Cassandra: {self.config['cassandra_hosts']}")
        logger.info(f"[CONFIG] MinIO: {self.config['minio_endpoint']}")
        logger.info(f"[CONFIG] Hours back: {self.config['hours_back']}")

        start_time = datetime.now(timezone.utc)

        # Setup connections
        self.setup_spark()
        self.setup_minio()

        try:
            # Step 1: Fetch data
            logger.info("\n--- Step 1: Fetching data from Cassandra ---")
            df = self.fetch_data(hours_back=self.config['hours_back'])

            if df.rdd.isEmpty():
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
            logger.info(f"Records processed: {df.count()}")
            logger.info(f"{'=' * 50}")

        finally:
            # Always stop Spark session
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")


def main():
    config = {
        'cassandra_hosts': os.getenv('CASSANDRA_HOSTS', 'cassandra').split(','),
        'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
        'minio_access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        'minio_secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        'model_bucket': os.getenv('MODEL_BUCKET', 'iot-models'),
        'hours_back': int(os.getenv('HOURS_BACK', '24')),
    }

    job = SparkBatchAnalyticsJob(config)

    try:
        job.run()
    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
