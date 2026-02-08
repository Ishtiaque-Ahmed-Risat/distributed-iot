# Data Ingestion Service

Subscribes to MQTT broker and forwards sensor data to Redpanda message queue.

**Language**: Python  
**Framework**: paho-mqtt, kafka-python

## Functionality

- Subscribes to MQTT topics: `sensors/+/data`
- Validates incoming sensor data
- Publishes to Redpanda topic: `raw-sensor-data`
- Automatic reconnection on failure
- Message counting and logging

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MQTT_BROKER` | `tcp://emqx:1883` | MQTT broker URL |
| `MQTT_TOPIC` | `sensors/+/data` | MQTT topic pattern |
| `REDPANDA_BROKERS` | `redpanda:9092` | Redpanda broker addresses (comma-separated) |
| `REDPANDA_TOPIC` | `raw-sensor-data` | Redpanda topic name |

## Building

```bash
# Build Docker image
docker build -t iot-ingestion:latest .
```

## Running

```bash
# Run locally
pip install -r requirements.txt
python main.py

# Run with Docker
docker run --rm \
  -e MQTT_BROKER=tcp://localhost:1883 \
  -e REDPANDA_BROKERS=localhost:9092 \
  iot-ingestion:latest
```

## Scaling

In production, run multiple replicas (set in docker-compose.yml or K8s):
```yaml
# Uncomment in docker-compose.yml to enable
# deploy:
#   replicas: 3
```
