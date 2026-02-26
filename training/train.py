import os

import numpy as np
import torch
from torch import nn
from datetime import datetime, timedelta, timezone
from cassandra.cluster import Cluster
from minio_uploader import upload_bundle_to_minio

from training.model import Autoencoder
from training.export_onnx import export_bundle
from model_update_publisher import publish_model_update

# ---------------- CONFIG ----------------

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = "iot_data"
TABLE = "sensor_readings"

DEVICE_ID = os.getenv("TRAIN_DEVICE_ID", "device_0000")
SENSOR_TYPE = os.getenv("TRAIN_SENSOR_TYPE", "temperature")

WINDOW_SIZE = 20
EPOCHS = 30
HOURS_BACK = 24

# ---------------- DATA LOADING ----------------

def fetch_data():
    print("Connecting to Cassandra...")
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)

    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=HOURS_BACK)

    date_str = start_time.strftime("%Y-%m-%d")

    query = f"""
        SELECT timestamp, value
        FROM {TABLE}
        WHERE device_id = %s AND date = %s
    """

    rows = session.execute(query, (DEVICE_ID, date_str))

    values = []
    for row in rows:
        values.append(float(row.value))

    cluster.shutdown()

    print(f"Fetched {len(values)} rows from Cassandra")
    return np.array(values)


def create_sequences(x, window_size):
    return np.array([x[i:i+window_size] for i in range(len(x) - window_size)])

# ---------------- TRAINING ----------------

def train():
    data = fetch_data()

    if len(data) < WINDOW_SIZE + 10:
        raise RuntimeError("Not enough data to train model")

    sequences = create_sequences(data, WINDOW_SIZE)

    # Normalization
    mean = sequences.mean()
    std = sequences.std() + 1e-8
    sequences = (sequences - mean) / std

    sequences = torch.tensor(sequences, dtype=torch.float32)

    model = Autoencoder(input_size=WINDOW_SIZE)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)

    model.train()

    print("Training autoencoder...")
    for epoch in range(EPOCHS):
        optimizer.zero_grad()
        recon = model(sequences)
        loss = criterion(recon, sequences)
        loss.backward()
        optimizer.step()

        if (epoch + 1) % 5 == 0:
            print(f"Epoch {epoch+1}, Loss: {loss.item():.6f}")

    model.eval()
    with torch.no_grad():
        recon = model(sequences)
        losses = torch.mean((recon - sequences) ** 2, dim=1)

    threshold = losses.mean() + 2 * losses.std()

    version = datetime.now().strftime("v%Y%m%d_%H%M%S")
    out_dir = f"artifacts/anomaly-detector/{version}"

    export_bundle(
        model=model,
        window_size=WINDOW_SIZE,
        mean=float(mean),
        std=float(std),
        threshold=float(threshold.item()),
        out_dir=out_dir
    )

    # ---- MinIO upload (version + latest) ----
    model_name = "anomaly-detector"
    bucket, prefix = upload_bundle_to_minio(
        local_dir=out_dir,
        model_name=model_name,
        version=version,
    )

    print(f"Uploaded model bundle to MinIO bucket={bucket}, prefix={prefix}")

    publish_model_update(
        model_name=model_name,
        version=version,
        bucket=bucket,
        prefix=prefix,
    )

    print("Training complete.")
    print(f"Threshold: {threshold.item():.6f}")
