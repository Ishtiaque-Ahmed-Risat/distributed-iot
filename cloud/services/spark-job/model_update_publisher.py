import os
import json
from kafka import KafkaProducer


def publish_model_update(
    model_name: str,
    version: str,
    bucket: str,
    prefix: str,
):
    brokers = os.getenv("REDPANDA_BROKERS", "redpanda:9092").split(",")
    topic = os.getenv("MODEL_UPDATE_TOPIC", "model-updates")

    producer = KafkaProducer(
        bootstrap_servers=brokers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    message = {
        "model_name": model_name,
        "version": version,
        "bucket": bucket,
        "prefix": prefix,
    }

    producer.send(topic, value=message).get(timeout=10)
    producer.flush()
    producer.close()

    print(f"Published model update: {message}")