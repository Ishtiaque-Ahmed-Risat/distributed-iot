# Edge Gateway - K3s Deployment

Edge gateway services for IoT data collection, processing, and storage on Kubernetes (K3s).

## Architecture

```
IoT Devices (MQTT, QoS 1)
      ↓
   EMQX Broker (StatefulSet, Clustered)
      ↓ (Shared Subscription)
Data Ingestion Service (Deployment, HPA 1-30)
      ↓
  Redpanda (StatefulSet, 30 partitions)
      ↓  raw-sensor-data
Transformation Service (Deployment, HPA 1-30)
      ↓  transformed-sensor-data
InfluxDB Writer Service (Deployment, HPA 1-10)
      ↓
  InfluxDB (DaemonSet, node-local, 7-day buffer)
```

## Components

### Infrastructure

- **EMQX** (StatefulSet): MQTT broker with clustering, session replication, AP-optimized
- **Redpanda** (StatefulSet): Message queue with 30 partitions, clustering-ready
- **InfluxDB** (DaemonSet): Time-series database, one per node, zero network hops

### Application Services (Python)

- **Device Registry** (FastAPI): REST API for device metadata (Port 31080)
- **Data Ingestion**: MQTT→Redpanda with shared subscriptions, AP producer
- **Data Transformation**: Unit conversion & semantic normalization
- **InfluxDB Writer**: Batched writes to node-local InfluxDB
- **Anomaly Alert**: Real-time ML inference, polls model from MinIO every 60s

## Quick Start

```bash
# Build Docker images for K3s
./k3s-build-images.sh

# Deploy to K3s
./k3s-deploy.sh

# Check status
./k3s-status.sh

# View pods
sudo k3s kubectl get pods -n iot-edge

# Scale for demo (3-node cluster)
./demo-mode.sh

# Scale back to local mode
./local-mode.sh
```

## Accessing Services

All services exposed via NodePort:

| Service | Internal URL | External URL | Credentials |
|---------|-------------|--------------|-------------|
| **EMQX Dashboard** | http://emqx:18083 | http://localhost:31803 | admin / public |
| **InfluxDB UI** | http://influxdb:8086 | http://localhost:31086 | admin / adminpassword |
| **Device Registry** | http://device-registry:8080 | http://localhost:31080/docs | - |
| **Redpanda Admin** | http://redpanda:9644 | http://localhost:31964 | - |

Replace `localhost` with K3s node IP for remote access.

## Data Flow

1. **Devices register**: POST to Device Registry API
2. **IoT devices publish**: MQTT topic `sensors/{device_id}/data` (QoS 1)
3. **EMQX**: Receives with persistent sessions, distributes via shared subscriptions
4. **Ingestion**: Forwards to Redpanda `raw-sensor-data` (idempotent, acks=1)
5. **Transformation**: Unit conversion, semantic normalization → `transformed-sensor-data`
6. **InfluxDB Writer**: Batched writes to node-local InfluxDB (DaemonSet)
7. **InfluxDB**: 7-day retention, ready for edge ML inference

**Key Features:**
- ✅ Ordering preserved per device (partition key + idempotence)
- ✅ Auto-scaling (1-30 replicas based on load)
- ✅ Fault tolerance (pod, node, network failures)
- ✅ High availability (demo mode: 3-node cluster)
- ✅ AP-optimized (3-4x faster, occasional loss OK)

## Monitoring

### Quick Status

```bash
./k3s-status.sh
```

### Pod Status

```bash
sudo k3s kubectl get pods -n iot-edge --watch
sudo k3s kubectl describe pod <pod-name> -n iot-edge
sudo k3s kubectl top pods -n iot-edge
```

### Service Logs

```bash
# Follow logs
sudo k3s kubectl logs -f deployment/ingestion-service -n iot-edge
sudo k3s kubectl logs -f deployment/transformation-service -n iot-edge
sudo k3s kubectl logs -f deployment/influxdb-writer -n iot-edge

# All data pipeline logs
sudo k3s kubectl logs -f -l component=data-pipeline -n iot-edge

# EMQX cluster logs
sudo k3s kubectl logs -f statefulset/emqx -n iot-edge

# Redpanda logs
sudo k3s kubectl logs -f statefulset/redpanda -n iot-edge
```

### Auto-Scaling Status

```bash
sudo k3s kubectl get hpa -n iot-edge
sudo k3s kubectl describe hpa ingestion-service-hpa -n iot-edge
```

### EMQX Metrics

```bash
sudo k3s kubectl port-forward -n iot-edge svc/emqx 18083:18083
curl http://localhost:18083/api/v5/metrics
```

### Redpanda Topics

```bash
# List topics
sudo k3s kubectl exec -n iot-edge redpanda-0 -- rpk topic list

# Consume messages
sudo k3s kubectl exec -n iot-edge redpanda-0 -- \
  rpk topic consume raw-sensor-data --num 10

# Topic details
sudo k3s kubectl exec -n iot-edge redpanda-0 -- \
  rpk topic describe raw-sensor-data
```

### InfluxDB Query

```bash
sudo k3s kubectl port-forward -n iot-edge svc/influxdb 8086:8086 &

curl -X POST "http://localhost:8086/api/v2/query?org=iot-org" \
  -H "Authorization: Token my-super-secret-token" \
  -H "Content-Type: application/json" \
  -d '{"query": "from(bucket:\"sensor-data\") |> range(start:-1h) |> limit(n:100)"}'
```

## Scaling

### Auto-Scaling (HPA)

Python services automatically scale:

- **Ingestion**: 1-30 replicas (CPU > 70% or Memory > 80%)
- **Transformation**: 1-30 replicas (CPU > 70% or Memory > 80%)
- **Writer**: 1-10 replicas (CPU > 70% or Memory > 80%)

```bash
# Watch auto-scaling in action
watch sudo k3s kubectl get hpa -n iot-edge
```

### Manual Scaling

```bash
# Scale infrastructure for demo (3-node cluster)
./demo-mode.sh
# Result: EMQX 3 replicas, Redpanda 3 replicas (RF=3)

# Scale back to local mode
./local-mode.sh
# Result: EMQX 1 replica, Redpanda 1 replica (RF=1)
```

## Configuration

### ConfigMap

Central configuration in `k3s/01-configmap.yaml`:

```yaml
MQTT_BROKER: "tcp://emqx:1883"
MQTT_TOPIC: "sensors/+/data"
REDPANDA_BROKERS: "redpanda:9092"
REDPANDA_TOPIC_RAW: "raw-sensor-data"
REDPANDA_TOPIC_TRANSFORMED: "transformed-sensor-data"
INFLUX_URL: "http://influxdb:8086"
```

Update:
```bash
sudo k3s kubectl edit configmap edge-config -n iot-edge
sudo k3s kubectl rollout restart deployment/ingestion-service -n iot-edge
```

### Secrets

Sensitive data in `k3s/02-secrets.yaml`:

```bash
sudo k3s kubectl edit secret edge-secrets -n iot-edge
```

## Troubleshooting

### Pods Not Starting

```bash
sudo k3s kubectl get pods -n iot-edge
sudo k3s kubectl describe pod <pod-name> -n iot-edge
sudo k3s kubectl logs <pod-name> -n iot-edge
```

**Common fixes:**
- Image pull error → `./k3s-build-images.sh`
- Resource limits → Check `sudo k3s kubectl top nodes`

### No Data Flow

```bash
# 1. Check ingestion logs
sudo k3s kubectl logs -f deployment/ingestion-service -n iot-edge

# 2. Check Redpanda has messages
sudo k3s kubectl exec -n iot-edge redpanda-0 -- \
  rpk topic consume raw-sensor-data --num 1

# 3. Check writer logs
sudo k3s kubectl logs -f deployment/influxdb-writer -n iot-edge

# 4. Verify data in InfluxDB (after port-forward)
sudo k3s kubectl port-forward -n iot-edge svc/influxdb 8086:8086
# Then query via curl (see above)
```

### Reset Everything

```bash
./k3s-undeploy.sh
sudo k3s kubectl delete namespace iot-edge
./k3s-deploy.sh
```

## Service Development

### Build and Update

```bash
# Make code changes in services/
cd services/ingestion
# Edit main.py

# Rebuild and update
cd ../../
./k3s-build-images.sh

# Rolling update (zero downtime)
sudo k3s kubectl rollout restart deployment/ingestion-service -n iot-edge

# Check rollout status
sudo k3s kubectl rollout status deployment/ingestion-service -n iot-edge
```

### Local Testing

```bash
cd services/ingestion

# Create venv
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export MQTT_BROKER=tcp://localhost:31883
export REDPANDA_BROKERS=localhost:19092
export REDPANDA_TOPIC=raw-sensor-data

# Run locally
python main.py
```

## Performance Tuning

### Increase Redpanda Partitions

```bash
sudo k3s kubectl exec -n iot-edge redpanda-0 -- \
  rpk topic alter raw-sensor-data --partitions 50
```

### Adjust HPA Thresholds

```bash
sudo k3s kubectl edit hpa ingestion-service-hpa -n iot-edge

# Change CPU target from 70% to 60%
```

### Increase Resource Limits

```bash
sudo k3s kubectl edit deployment ingestion-service -n iot-edge

# Update resources.limits
```

## Directory Structure

```
edge/
├── k3s/                        # K3s manifests
│   ├── 00-namespace.yaml
│   ├── 01-configmap.yaml
│   ├── 02-secrets.yaml
│   ├── emqx/                   # EMQX StatefulSet
│   ├── redpanda/               # Redpanda StatefulSet
│   ├── influxdb/               # InfluxDB DaemonSet
│   ├── ingestion-service/      # Ingestion Deployment + HPA
│   ├── transformation-service/ # Transformation Deployment + HPA
│   ├── influxdb-writer/        # Writer Deployment + HPA
│   └── device-registry/        # Registry Deployment
├── services/                   # Service source code
│   ├── ingestion/
│   ├── transformation/
│   ├── influxdb-writer/
│   └── device-registry/
├── k3s-build-images.sh         # Build and import images
├── k3s-deploy.sh               # Deploy all services
├── k3s-undeploy.sh             # Remove all services
├── k3s-status.sh               # Check deployment status
├── demo-mode.sh                # Scale to 3-node cluster
└── local-mode.sh               # Scale to single node
```

## See Also

- [QUICKSTART.md](../QUICKSTART.md) - Quick start guide
- [SETUP.md](../SETUP.md) - Detailed setup and troubleshooting
- [OPTIMIZATION_SUMMARY.md](../OPTIMIZATION_SUMMARY.md) - Architecture decisions
- [docs/DATA_TRANSFORMATION.md](../docs/DATA_TRANSFORMATION.md) - Transformation layer
- [docs/LLM_INTEGRATION.md](../docs/LLM_INTEGRATION.md) - LLM extensibility

---

**Production-ready K3s deployment for edge IoT gateways!** 🚀
