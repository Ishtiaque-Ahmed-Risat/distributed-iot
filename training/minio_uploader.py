import os
from typing import List, Tuple

import boto3
from botocore.client import Config as BotoConfig


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

    Expects these files in local_dir:
      - model.onnx
      - preprocess.json
      - thresholds.json
      - metadata.json

    Uploads to:
      models/<model_name>/<version>/...
      models/<model_name>/latest/...

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

    for local_path, remote_key in uploads:
        upload_file(s3, bucket, local_path, remote_key)

    return bucket, version_prefix
