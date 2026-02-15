# Quick Start Guide

Get the distributed IoT gateway running on K3s in **10 minutes**!

---

## Prerequisites

```bash
# Check if K3s is installed
sudo k3s kubectl version

# Check requirements
python3 --version  # Python 3.11+
docker --version   # Docker 20.10+
```

**If K3s is not installed**, see [Step 1](#step-1-install-k3s) below.

---

## Scripts Overview

| Script | Purpose | When to Use |
|--------|---------|-------------|
| **`k3s-build-images.sh`** | Build Docker images | First time, or after code changes |
| **`k3s-deploy.sh`** | Full deploy (infra + services) | Quick one-shot deployment |
| **`k3s-deploy-infra.sh`** | Deploy infra (EMQX, Redpanda, InfluxDB, topics) | Manual control: run first |
| **`k3s-deploy-services.sh`** | Deploy services (after infra is up) | Manual control: run second |
| **`demo-mode.sh`** | Scale to 3 replicas (HA) | Before presentations/demos |
| **`local-mode.sh`** | Scale back to default (2 replicas) | After demos (save resources) |
| **`k3s-status.sh`** | Check system status | Anytime |
| **`k3s-undeploy.sh`** | Remove all resources | Clean up |

**Typical workflow:**
```bash
./k3s-build-images.sh       # Build once
./k3s-deploy.sh             # Full deploy (infra + services)

# OR for manual control:
./k3s-deploy-infra.sh       # Deploy infra, wait for pods
./k3s-deploy-services.sh    # Deploy services after infra is up

# Scaling:
./demo-mode.sh              # Scale up for demo (optional)
./local-mode.sh             # Scale down after demo (optional)
```

---

## Step 1: Install K3s (2 minutes)

```bash
# Install K3s
curl -sfL https://get.k3s.io | sh -

# Verify
sudo k3s kubectl get nodes
```

**Expected Output:**
```
NAME       STATUS   ROLES                  AGE   VERSION
my-node    Ready    control-plane,master   30s   v1.28.5+k3s1
```

---

## Step 2: Deploy Edge Gateway (3 minutes)

```bash
cd edge

# Build images
./k3s-build-images.sh

# Deploy to K3s (starts with 2 replicas - default HA)
./k3s-deploy.sh
```

**Verify:**
```bash
sudo k3s kubectl get pods -n iot-edge
# Wait until all pods show STATUS = Running
```

---

## Step 3: Start Device Simulator (30 seconds)

```bash
cd ../simulator
./run-simulator.sh
```

**Expected Output:**
```
Starting IoT device simulator...
✓ Connected to MQTT broker at localhost:31883
Published 30 readings in 0.05s (Total: 30)
```

---

## Step 4: Verify Data Flow

```bash
# Check logs
sudo k3s kubectl logs -f deployment/ingestion-service -n iot-edge
# You should see: "✓ Subscribed to shared topic: $share/ingestion-group/sensors/+/data"

# Query InfluxDB (wait 30 seconds for data)
curl -X POST "http://localhost:31086/api/v2/query?org=iot-org" \
  -H "Authorization: Token my-super-secret-token" \
  -H "Content-Type: application/json" \
  -d '{"query": "from(bucket:\"sensor-data\") |> range(start:-5m) |> limit(n:5)"}'
```

If you see data, **congratulations!** 🎉

---

## Accessing Services

All services exposed via NodePort:

| Service | URL | Credentials |
|---------|-----|-------------|
| **EMQX Dashboard** | http://localhost:31803 | admin / public |
| **InfluxDB UI** | http://localhost:31086 | admin / adminpassword |
| **Device Registry API** | http://localhost:31080/docs | - |
| **Redpanda Admin** | http://localhost:31964 | - |

---

## Scaling

### Demo Mode (High Availability)

Scale up for presentations:

```bash
cd edge
./demo-mode.sh
```

**What it does:**
- EMQX: 2 → 3 replicas (clustered, fault-tolerant)
- Redpanda: 1 → 3 replicas (RF=3, distributed)
- **Capacity:** 100K+ devices, 1M+ msg/sec

### Local Mode

Scale back down:

```bash
cd edge
./local-mode.sh
```

**What it does:**
- EMQX: 3 → 2 replicas
- Redpanda: 3 → 1 replica
- **Capacity:** 10K+ devices, 100K+ msg/sec

**Note:** Python services auto-scale (2-30 replicas) based on CPU load via HPA.

---

## Monitoring

### Quick Status

```bash
cd edge
./k3s-status.sh
```

### Watch Pods

```bash
sudo k3s kubectl get pods -n iot-edge --watch
```

### View Logs

```bash
# Ingestion service
sudo k3s kubectl logs -f deployment/ingestion-service -n iot-edge

# Transformation service
sudo k3s kubectl logs -f deployment/transformation-service -n iot-edge

# InfluxDB writer
sudo k3s kubectl logs -f deployment/influxdb-writer -n iot-edge
```

### Check Auto-Scaling

```bash
sudo k3s kubectl get hpa -n iot-edge
```

### EMQX Dashboard

- URL: http://localhost:31803
- Login: `admin` / `public`
- View: Connected devices, message rates, topics

### InfluxDB UI

- URL: http://localhost:31086
- Login: `admin` / `adminpassword`
- Query sensor data with Flux

---

## Stopping the System

### Stop Simulator
Press `Ctrl+C` in the simulator terminal

### Undeploy Edge Gateway
```bash
cd edge
./k3s-undeploy.sh
```

### Stop K3s Completely
```bash
sudo systemctl stop k3s
```

### Restart K3s
```bash
sudo systemctl start k3s
cd edge
./k3s-deploy.sh
```

---

## Troubleshooting

### Pods Not Starting

```bash
# Check status
sudo k3s kubectl get pods -n iot-edge

# Describe pod
sudo k3s kubectl describe pod <pod-name> -n iot-edge

# Check events
sudo k3s kubectl get events -n iot-edge --sort-by='.lastTimestamp'
```

**Common fixes:**
- Image pull errors → Run `./k3s-build-images.sh` again
- Resource constraints → Check `sudo k3s kubectl top nodes`

### Simulator Can't Connect

```bash
# Check EMQX status
sudo k3s kubectl get pods -n iot-edge -l app=emqx

# Check EMQX logs
sudo k3s kubectl logs -f statefulset/emqx -n iot-edge

# Test MQTT port
nc -zv localhost 31883
```

### No Data in InfluxDB

```bash
# Check ingestion logs (should show MQTT connection)
sudo k3s kubectl logs -f deployment/ingestion-service -n iot-edge

# Check transformation logs
sudo k3s kubectl logs -f deployment/transformation-service -n iot-edge

# Check writer logs (should show InfluxDB writes)
sudo k3s kubectl logs -f deployment/influxdb-writer -n iot-edge
```

**Wait 30-60 seconds** for data to flow through the pipeline.

### EMQX/Redpanda Not Clustering (Demo Mode)

```bash
# Check EMQX cluster status
sudo k3s kubectl exec -n iot-edge emqx-0 -- emqx ctl cluster status

# Check Redpanda cluster health
sudo k3s kubectl exec -n iot-edge redpanda-0 -- rpk cluster health
```

### General Debug

```bash
# Get all resource status
cd edge
./k3s-status.sh

# Check all logs
sudo k3s kubectl logs -n iot-edge --all-containers=true --tail=50

# Restart a specific service
sudo k3s kubectl rollout restart deployment/ingestion-service -n iot-edge
```

---

## System Architecture

### Data Flow

```
IoT Devices (MQTT)
    ↓
EMQX Broker (port 31883)
    ↓
Ingestion Service (shared subscription)
    ↓
Redpanda (Kafka)
    ↓
Transformation Service (filters, aggregates)
    ↓
Redpanda (transformed topic)
    ↓
InfluxDB Writer → InfluxDB (node-local persistence)
```

### Components

| Component | Type | Replicas | Purpose |
|-----------|------|----------|---------|
| **EMQX** | StatefulSet | 2 (3 in demo) | MQTT broker |
| **Redpanda** | StatefulSet | 1 (3 in demo) | Message queue |
| **InfluxDB** | DaemonSet | 1 per node | Time-series DB (7-day retention) |
| **Ingestion** | Deployment + HPA | 2-30 | MQTT → Redpanda |
| **Transformation** | Deployment + HPA | 2-30 | Data processing |
| **InfluxDB Writer** | Deployment + HPA | 2-10 | Redpanda → InfluxDB |
| **Device Registry** | Deployment | 2 | Device management API |

### Fault Tolerance & Load Sharing

- **MQTT**: Auto-reconnect, persistent sessions, QoS 1
- **Redpanda**: 10 partitions per topic, device_id key → ordering per device
- **InfluxDB**: Node-local, accepts 1-min data loss on node failure
- **Python Services**: Auto-scaling, stateless, PodDisruptionBudgets
- **Startup**: Init containers ensure services wait for dependencies (EMQX, Redpanda topics, InfluxDB)
- **Topics**: Auto-creation disabled; init job creates topics with 10 partitions before services start

---

## Configuration

### Increase Device Count

Edit `simulator/config.yaml`:
```yaml
simulation:
  num_devices: 100  # Change from 30
  publish_interval: 10
```

### Adjust Auto-Scaling

Edit `edge/k3s/ingestion-service/hpa.yaml`:
```yaml
spec:
  minReplicas: 2    # Start with 2 replicas
  maxReplicas: 50   # Scale up to 50
  targetCPUUtilizationPercentage: 60  # Scale at 60% CPU
```

Apply changes:
```bash
sudo k3s kubectl apply -f edge/k3s/ingestion-service/hpa.yaml
```

---

## Performance Tuning

### Local Mode (default replicas)
- **Devices:** 10,000+
- **Messages/sec:** 100,000+
- **Resources:** 4 CPU, 8GB RAM

### Demo Mode (3 replicas)
- **Devices:** 100,000+
- **Messages/sec:** 1,000,000+
- **Resources:** 8 CPU, 16GB RAM

### Production Recommendations
- Multi-node K3s cluster (3+ nodes)
- Dedicated nodes for InfluxDB (DaemonSet distributes automatically)
- Increase HPA `maxReplicas` based on load
- Monitor with Prometheus + Grafana

---

## Next Steps

1. **Add more devices**: Edit `simulator/config.yaml`
2. **Scale for demo**: Run `./demo-mode.sh`
3. **Explore dashboards**: EMQX (31803), InfluxDB (31086)
4. **Check auto-scaling**: `sudo k3s kubectl get hpa -n iot-edge --watch`
5. **Test fault tolerance**: Kill a pod, watch it recover
6. **Review architecture**: See `PROJECT_STRUCTURE.md`
7. **Deep dive**: See `SETUP.md` for detailed configuration

---

## Advanced Usage

### Manual Scaling

```bash
# Scale ingestion service
sudo k3s kubectl scale deployment ingestion-service --replicas=5 -n iot-edge

# Scale EMQX (manual only)
sudo k3s kubectl scale sts emqx --replicas=3 -n iot-edge

# Scale Redpanda (manual only)
sudo k3s kubectl scale sts redpanda --replicas=3 -n iot-edge
```

### Port Forwarding (Alternative Access)

```bash
# EMQX Dashboard
sudo k3s kubectl port-forward -n iot-edge svc/emqx 18083:18083

# InfluxDB
sudo k3s kubectl port-forward -n iot-edge svc/influxdb 8086:8086

# Device Registry
sudo k3s kubectl port-forward -n iot-edge svc/device-registry 8000:8000
```

### Export Metrics

```bash
# CPU/Memory usage
sudo k3s kubectl top pods -n iot-edge

# Export to file
sudo k3s kubectl get pods -n iot-edge -o wide > pods-status.txt
```

---

## Clean Up

### Remove Everything

```bash
cd edge
./k3s-undeploy.sh

# Uninstall K3s completely
/usr/local/bin/k3s-uninstall.sh
```

### Keep K3s, Remove Edge Gateway

```bash
cd edge
./k3s-undeploy.sh
```

---

## Getting Help

- **Documentation**: See `SETUP.md` for detailed guide
- **Architecture**: See `PROJECT_STRUCTURE.md`
- **Issues**: Check logs with `./k3s-status.sh`
- **Optimization**: See `OPTIMIZATION_SUMMARY.md`

---

**Happy IoT Gateway Building!** 🚀
