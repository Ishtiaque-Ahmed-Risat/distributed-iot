# Anomaly Alert Service

Real-time ML inference on edge data streams.

## Features
- Downloads ML model from MinIO on startup
- Polls for model updates every 60 seconds
- Runs inference on transformed sensor data
- Logs anomalies with details
- Independent consumer group (non-blocking)
- Graceful restart on model updates

## Configuration
Environment variables:
- `REDPANDA_BROKERS`: Kafka brokers (default: redpanda:9092)
- `REDPANDA_TOPIC`: Topic to consume (default: transformed-sensor-data)
- `CONSUMER_GROUP`: Consumer group ID (default: anomaly-alert-group)
- `MINIO_ENDPOINT`: MinIO endpoint (default: http://minio:9000)
- `MODEL_BUCKET`: S3 bucket name (default: iot-models)
- `MODEL_UPDATE_INTERVAL`: Check interval in seconds (default: 60)

## Deployment
```bash
# Build image
docker build -t anomaly-alert:latest .

# Deploy to K3s
kubectl apply -f ../../k3s/anomaly-alert/
```

## Architecture
- **Replicas**: 2 (with HPA: 2-10 based on CPU/memory)
- **PDB**: minAvailable=1 for rolling updates
- **Scaling**: Independent from other services
- **Updates**: Automatic restart when model changes
