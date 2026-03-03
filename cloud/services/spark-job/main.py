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
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Tuple
import torch
import torch.nn as nn
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import boto3
from botocore.client import Config as BotoConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ==================== ONNX Export Functions ====================

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)


def export_bundle(model, window_size, mean, std, threshold, out_dir, feature_columns=None, model_name=None, version=None):
    ensure_dir(out_dir)

    if model_name is None:
        model_name = os.getenv("AE_MODEL_NAME", "anomaly-detector-autoencoder")
    if version is None:
        version = os.path.basename(out_dir)

    # 1) Export ONNX
    model.eval()
    dummy = torch.zeros(1, window_size, dtype=torch.float32)
    onnx_path = os.path.join(out_dir, "model.onnx")

    # IMPORTANT: use input/output names that match edge code: input/output
    torch.onnx.export(
        model,
        dummy,
        onnx_path,
        input_names=["input"],
        output_names=["output"],
        dynamic_axes={"input": {0: "batch"}, "output": {0: "batch"}},
        opset_version=17
    )

    # 2) preprocess.json
    # mean/std can be scalar (windowed) OR list (pivoted)
    preprocess = {
        "window_size": int(window_size),
        "normalization": "zscore",
        "mean": mean,
        "std": std,
    }
    if feature_columns is not None:
        preprocess["feature_columns"] = feature_columns

    with open(os.path.join(out_dir, "preprocess.json"), "w") as f:
        json.dump(preprocess, f, indent=2)

    # 3) thresholds.json
    thresholds = {
        "type": "mse_reconstruction",
        "mse_threshold": float(threshold),
        # keep backward-compatible key too
        "threshold": float(threshold),
    }
    with open(os.path.join(out_dir, "thresholds.json"), "w") as f:
        json.dump(thresholds, f, indent=2)

    # 4) metadata.json
    metadata = {
        "model_name": model_name,
        "version": version,
        "exported_at": int(time.time()),
        "format": "onnx",
        "opset": 17,
    }
    if feature_columns is not None:
        metadata["n_features"] = len(feature_columns)

    with open(os.path.join(out_dir, "metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"✓ Exported ONNX bundle: {out_dir}")


# ==================== MinIO Upload Functions ====================

def _get_s3_client():
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(s3, bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)


def upload_file(s3, bucket: str, local_path: str, remote_key: str):
    s3.upload_file(local_path, bucket, remote_key)


def upload_bundle_to_minio(
    local_dir: str,
    model_name: str,
    version: str,
    bucket: str = None,
) -> Tuple[str, str]:
    """
    Upload ONNX bundle directory to MinIO.
    
    Edge services poll MinIO periodically to check for model updates.

    Expects these files in local_dir:
      - model.onnx
      - preprocess.json
      - thresholds.json
      - metadata.json

    Uploads to:
      models/<model_name>/<version>/...  (versioned path)
      models/<model_name>/latest/...     (latest path for polling)

    Returns (bucket, version_prefix).
    """
    if bucket is None:
        bucket = os.getenv("MODEL_BUCKET", "iot-models")

    required = ["model.onnx", "preprocess.json", "thresholds.json", "metadata.json"]
    missing = [f for f in required if not os.path.exists(os.path.join(local_dir, f))]
    if missing:
        raise FileNotFoundError(f"Bundle missing files: {missing} in {local_dir}")

    s3 = _get_s3_client()
    ensure_bucket(s3, bucket)

    version_prefix = f"models/{model_name}/{version}"
    latest_prefix = f"models/{model_name}/latest"

    uploads: List[Tuple[str, str]] = []
    for f in required:
        lp = os.path.join(local_dir, f)
        uploads.append((lp, f"{version_prefix}/{f}"))
        uploads.append((lp, f"{latest_prefix}/{f}"))

    logger.info(f"Uploading model bundle to MinIO: bucket={bucket}, version={version_prefix}, latest={latest_prefix}")
    try:
        for local_path, remote_key in uploads:
            upload_file(s3, bucket, local_path, remote_key)
        logger.info(f"✓ Successfully uploaded model bundle to MinIO: {bucket}/{version_prefix}")
        return bucket, version_prefix
    except Exception as e:
        logger.error(f"✗ Failed to upload model bundle to MinIO: {e}", exc_info=True)
        raise




# ==================== Model Classes ====================

class Autoencoder(nn.Module):
    def __init__(self, input_size):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_size, 5),
            nn.ReLU(),
            nn.Linear(5, 2),
            nn.ReLU(),
        )
        self.decoder = nn.Sequential(
            nn.Linear(2, 5),
            nn.ReLU(),
            nn.Linear(5, input_size),
        )
    def forward(self, x):
        return self.decoder(self.encoder(x))


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
        logger.info(f"Time range: {start_time} to {now}")

        # Read from Cassandra using Spark
        # Spark Cassandra connector will scan all partitions
        df = self.spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="sensor_readings", keyspace="iot_data") \
            .load()

        # Debug: check total rows before filtering
        total_count = df.count()
        logger.info(f"Total rows in sensor_readings table (before filter): {total_count}")

        # Show sample data structure
        if total_count > 0:
            logger.info("Sample data structure:")
            df.select("device_id", "date", "timestamp", "sensor_type", "value").show(5, truncate=False)
            logger.info(f"Unique devices: {df.select('device_id').distinct().count()}")
            logger.info(f"Date range in data: {df.agg(F.min('timestamp').alias('min_ts'), F.max('timestamp').alias('max_ts')).collect()}")

        # Filter by timestamp
        df = df.filter(
            (F.col("timestamp") >= start_time) & 
            (F.col("timestamp") <= now)
        )

        count = df.count()
        logger.info(f"Fetched {count} records from Cassandra (after timestamp filter)")
        
        if count == 0:
            logger.warning("No data found in the specified time range")
            logger.warning(f"Consider checking if data timestamps are in UTC and match the filter range")
        
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

    def train_autoencoder_bundle_and_publish(self, df):
        """
        Train Autoencoder on pivoted feature vectors,
        export ONNX bundle, upload to MinIO, publish model-update.
        This matches edge buffering that waits for complete feature vectors.
        """
        if df.rdd.isEmpty():
            logger.warning("No data available for autoencoder training")
            return None

        # 1) Pivot to (device_id, timestamp) rows with sensor_type columns
        logger.info(f"Input DataFrame for AE: {df.count()} rows")
        logger.info(f"Unique (device_id, timestamp) combinations: {df.select('device_id', 'timestamp').distinct().count()}")
        
        pivot_df = df.groupBy("device_id", "timestamp").pivot("sensor_type").agg(F.first("value"))

        pivot_count = pivot_df.count()
        logger.info(f"After pivot: {pivot_count} rows")

        feature_cols = [c for c in pivot_df.columns if c not in ("device_id", "timestamp")]
        if not feature_cols:
            logger.warning("No feature columns found after pivot for AE training")
            logger.warning(f"Pivot columns: {pivot_df.columns}")
            return None

        # Optional: limit amount of data collected to driver
        limit_rows = int(self.config.get("ae_train_limit_rows", 50000))
        pivot_df = pivot_df.orderBy(F.col("timestamp").desc()).limit(limit_rows)

        # Fill nulls with per-column mean (better than 0.0)
        means_row = pivot_df.agg(*[F.avg(F.col(c)).alias(c) for c in feature_cols]).collect()[0].asDict()
        for c in feature_cols:
            pivot_df = pivot_df.fillna({c: float(means_row.get(c) or 0.0)})

        # Collect to driver for PyTorch training
        pdf = pivot_df.select(feature_cols).toPandas()
        X = pdf.to_numpy(dtype=np.float32)

        if X.shape[0] < 1:
            logger.warning(f"Not enough samples for AE training (n={X.shape[0]})")
            return None

        # 2) Per-feature standardization (mean/std vectors)
        mu = X.mean(axis=0).astype(np.float32)
        sigma = X.std(axis=0).astype(np.float32)
        sigma[sigma < 1e-6] = 1.0
        Xn = (X - mu) / sigma

        # 3) Train AE (input_size = number of features)
        input_size = Xn.shape[1]
        epochs = int(self.config.get("ae_epochs", 5))
        lr = float(self.config.get("ae_lr", 1e-3))

        torch.manual_seed(0)
        model = Autoencoder(input_size=input_size)
        opt = torch.optim.Adam(model.parameters(), lr=lr)
        loss_fn = nn.MSELoss()

        Xt = torch.tensor(Xn, dtype=torch.float32)

        model.train()
        for _ in range(epochs):
            opt.zero_grad()
            recon = model(Xt)
            loss = loss_fn(recon, Xt)
            loss.backward()
            opt.step()

        # 4) Threshold
        model.eval()
        with torch.no_grad():
            recon = model(Xt)
            losses = torch.mean((recon - Xt) ** 2, dim=1).cpu().numpy()

        threshold = float(losses.mean() + 2.0 * losses.std())

        # 5) Export bundle
        model_name = self.config.get("ae_model_name", "anomaly-detector-autoencoder")
        version = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_dir = f"/tmp/{model_name}/{version}"

        logger.info(f"Exporting ONNX bundle: model={model_name}, version={version}, features={len(feature_cols)}")
        export_bundle(
            model=model,
            window_size=input_size,
            mean=mu.tolist(),
            std=sigma.tolist(),
            threshold=threshold,
            out_dir=out_dir,
            feature_columns=feature_cols, 
        )

        bucket = self.config["model_bucket"]
        bucket, prefix = upload_bundle_to_minio(
            local_dir=out_dir,
            model_name=model_name,
            version=version,
            bucket=bucket,
        )

        logger.info(f"✓ Model training and upload complete: {model_name} v{version} (threshold={threshold:.6f})")
        logger.info(f"  Model available at: {bucket}/{prefix}")

        return {
            "model_name": model_name,
            "version": version,
            "feature_columns": feature_cols,
            "threshold": threshold,
            "bucket": bucket,
            "prefix": prefix,
        }

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

            # Step 3: Train autoencoder + publish ONNX bundle for edge
            logger.info("\n--- Step 3: Training autoencoder model (ONNX bundle) ---")
            self.train_autoencoder_bundle_and_publish(df)

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
        'ae_epochs': int(os.getenv('AE_EPOCHS', '50')),
        'ae_lr': float(os.getenv('AE_LR', '0.001')),
        'ae_model_name': os.getenv('AE_MODEL_NAME', 'anomaly-detector-autoencoder'),
        'ae_train_limit_rows': int(os.getenv('AE_TRAIN_LIMIT_ROWS', '50000')),
    }

    job = SparkBatchAnalyticsJob(config)

    try:
        job.run()
    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
