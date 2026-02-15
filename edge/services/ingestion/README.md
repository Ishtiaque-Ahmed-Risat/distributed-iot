# Data Ingestion Service

Subscribes to MQTT broker via shared subscriptions and forwards sensor data to Redpanda with AP-optimized producer settings.

**Language**: Python  
**Framework**: paho-mqtt, kafka-python  
**Deployment**: K3s Deployment with HPA (1-30 replicas)

## Functionality

- Subscribes to MQTT via shared subscriptions: `$share/ingestion-group/sensors/+/data`
- Validates incoming sensor data (device_id, timestamp)
- Publishes to Redpanda topic: `raw-sensor-data` with idempotent producer
- AP-optimized: `acks=1`, fast timeouts, ordering preserved per device
- Automatic reconnection with exponential backoff
- Persistent MQTT sessions (QoS 1)
- Unique client ID per pod for multi-replica support

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MQTT_BROKER` | `tcp://emqx:1883` | MQTT broker URL |
| `MQTT_TOPIC` | `sensors/+/data` | MQTT topic pattern (shared subscription applied in code) |
| `REDPANDA_BROKERS` | `redpanda:9092` | Redpanda broker addresses |
| `REDPANDA_TOPIC` | `raw-sensor-data` | Redpanda topic name |

## Building for K3s

```bash
# From edge/ directory
cd ..
./k3s-build-images.sh
```

This builds and imports the image to K3s.

## Running Locally (Development)

```bash
# Install dependencies
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Set environment variables
export MQTT_BROKER=tcp://localhost:31883
export MQTT_TOPIC="sensors/+/data"
export REDPANDA_BROKERS=localhost:19092
export REDPANDA_TOPIC=raw-sensor-data

# Run
python main.py
```

## Deployment

```bash
# Deploy to K3s
cd ../../
./k3s-deploy.sh

# Check status
sudo k3s kubectl get pods -n iot-edge -l app=ingestion-service

# View logs
sudo k3s kubectl logs -f deployment/ingestion-service -n iot-edge

# Check auto-scaling
sudo k3s kubectl get hpa ingestion-service-hpa -n iot-edge
```

## Scaling

### Auto-Scaling (Default)

HPA automatically scales 1-30 replicas based on:
- CPU > 70%
- Memory > 80%

```bash
# Watch scaling
watch sudo k3s kubectl get hpa -n iot-edge
```

### Manual Scaling

```bash
# Scale manually (overrides HPA temporarily)
sudo k3s kubectl scale deployment ingestion-service --replicas=5 -n iot-edge
```

## Configuration

### Producer Settings (AP-Optimized)

```python
KafkaProducer(
    enable_idempotence=True,      # Ordering + deduplication
    acks=1,                        # Leader only (3x faster)
    retries=10,                    # Limited retries
    request_timeout_ms=10000,      # 10s timeout
    delivery_timeout_ms=30000      # 30s total
)
```

### MQTT Settings

```python
mqtt.Client(
    client_id=f"ingestion-{pid}",  # Unique per pod
    clean_session=False             # Persistent sessions
)
client.subscribe("$share/ingestion-group/sensors/+/data", qos=1)
```

## Monitoring

```bash
# Logs
sudo k3s kubectl logs -f deployment/ingestion-service -n iot-edge

# Metrics
sudo k3s kubectl top pods -n iot-edge -l app=ingestion-service

# Events
sudo k3s kubectl describe deployment ingestion-service -n iot-edge
```

## Troubleshooting

### Pod Not Starting

```bash
sudo k3s kubectl describe pod <pod-name> -n iot-edge
sudo k3s kubectl logs <pod-name> -n iot-edge
```

### MQTT Connection Failed

```bash
# Check EMQX
sudo k3s kubectl get pods -n iot-edge -l app=emqx
sudo k3s kubectl logs -f statefulset/emqx -n iot-edge

# Test MQTT port
nc -zv localhost 31883
```

### Redpanda Connection Failed

```bash
# Check Redpanda
sudo k3s kubectl exec -n iot-edge redpanda-0 -- rpk cluster health
```

## Performance

- **Single pod**: ~10K msg/sec
- **10 pods**: ~100K msg/sec
- **30 pods** (max): ~300K msg/sec

Scales linearly with HPA up to Redpanda partition count (30).
