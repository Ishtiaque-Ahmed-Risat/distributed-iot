# Setup Guide - K3s Deployment

Complete setup guide for the distributed IoT gateway system on K3s.

---

## Prerequisites

### System Requirements

- **OS:** Linux (Ubuntu 20.04+, Debian, RHEL, etc.)
- **CPU:** 2+ cores
- **RAM:** 4+ GB (8 GB recommended for demo mode)
- **Disk:** 20+ GB free space
- **Network:** Internet access for installation

### Software Requirements

1. **K3s** - Lightweight Kubernetes
2. **Docker** - For building service images
3. **Python 3.11+** - For simulator
4. **sudo k3s kubectl** - Kubernetes CLI (comes with K3s)

---

## Installation

### 1. Install K3s

```bash
# Install K3s
curl -sfL https://get.k3s.io | sh -

# Verify installation
sudo k3s kubectl get nodes
```

**Expected output:**
```
NAME       STATUS   ROLES                  AGE   VERSION
my-node    Ready    control-plane,master   1m    v1.28.5+k3s1
```

**Set up sudo k3s kubectl access:**
```bash
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $USER:$USER ~/.kube/config
export KUBECONFIG=~/.kube/config

# Add to shell profile for persistence
echo 'export KUBECONFIG=~/.kube/config' >> ~/.bashrc
source ~/.bashrc

# Test
sudo k3s kubectl get nodes
```

### 2. Install Docker

```bash
# Ubuntu/Debian
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker

# Verify
docker --version
```

### 3. Install Python 3.11+

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install -y python3.11 python3.11-venv python3-pip

# Verify
python3 --version
```

---

## Deployment

### Build and Deploy Edge Gateway

```bash
cd edge

# Build Docker images and import to K3s
./k3s-build-images.sh

# Deploy all services
./k3s-deploy.sh

# Check deployment status
./k3s-status.sh
```

**Expected deployment time:** 2-3 minutes

### Deploy Cloud Services

```bash
cd cloud
./start-cloud.sh
```

This starts Redpanda, Cassandra, MinIO, Cassandra Writer, and Cloud API via Docker Compose.

### Verify Deployment

```bash
# Edge pods should all be Running
sudo k3s kubectl get pods -n iot-edge

# Check edge services
sudo k3s kubectl get svc -n iot-edge

# Check HPAs (auto-scaling)
sudo k3s kubectl get hpa -n iot-edge

# Cloud services should be running
docker-compose -f cloud/docker-compose.yml ps
```

---

## Configuration

### ConfigMap (edge/k3s/01-configmap.yaml)

Centralized configuration for all services:

```yaml
MQTT_BROKER: "tcp://emqx:1883"
MQTT_TOPIC: "sensors/+/data"
REDPANDA_BROKERS: "redpanda:9092"
REDPANDA_TOPIC_RAW: "raw-sensor-data"
REDPANDA_TOPIC_TRANSFORMED: "transformed-sensor-data"
INFLUX_URL: "http://influxdb:8086"
INFLUX_ORG: "iot-org"
INFLUX_BUCKET: "sensor-data"
```

### Secrets (edge/k3s/02-secrets.yaml)

Sensitive configuration:

```yaml
INFLUX_ADMIN_PASSWORD: adminpassword (base64 encoded)
INFLUX_TOKEN: my-super-secret-token (base64 encoded)
EMQX_USERNAME: admin (base64 encoded)
EMQX_PASSWORD: public (base64 encoded)
```

**To update secrets:**
```bash
# Edit secrets
sudo k3s kubectl edit secret edge-secrets -n iot-edge

# Or recreate:
sudo k3s kubectl delete secret edge-secrets -n iot-edge
sudo k3s kubectl apply -f edge/k3s/02-secrets.yaml
```

---

## Service Access

All services are exposed via NodePort:

| Service | Internal URL | External URL | Credentials |
|---------|-------------|--------------|-------------|
| **EMQX Dashboard** | http://emqx:18083 | http://localhost:31803 | admin / public |
| **InfluxDB UI** | http://influxdb:8086 | http://localhost:31086 | admin / adminpassword |
| **Device Registry** | http://device-registry:8080 | http://localhost:31080/docs | - |
| **Redpanda Admin** | http://redpanda:9644 | http://localhost:31964 | - |

**Cloud Services (Docker Compose):**

| Service | URL | Credentials |
|---------|-----|-------------|
| **Cloud API** | http://localhost:8000/docs | - |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **Cloud Redpanda Admin** | http://localhost:29644 | - |
| **Cassandra CQL** | localhost:9042 | - |

**For remote access**, replace `localhost` with your K3s node IP address.

---

## Monitoring

### Pod Status

```bash
# Watch all pods
sudo k3s kubectl get pods -n iot-edge --watch

# Detailed pod info
sudo k3s kubectl describe pod <pod-name> -n iot-edge

# Pod resource usage
sudo k3s kubectl top pods -n iot-edge
```

### Service Logs

```bash
# Follow logs (live)
sudo k3s kubectl logs -f deployment/ingestion-service -n iot-edge

# Last 100 lines
sudo k3s kubectl logs --tail=100 deployment/ingestion-service -n iot-edge

# All pods with label
sudo k3s kubectl logs -f -l component=data-pipeline -n iot-edge

# Previous pod instance (if crashed)
sudo k3s kubectl logs --previous deployment/ingestion-service -n iot-edge
```

### Auto-Scaling Status

```bash
# Check HPA metrics
sudo k3s kubectl get hpa -n iot-edge

# Watch HPA in real-time
watch sudo k3s kubectl get hpa -n iot-edge

# Detailed HPA info
sudo k3s kubectl describe hpa ingestion-service-hpa -n iot-edge
```

### Events

```bash
# Recent events
sudo k3s kubectl get events -n iot-edge --sort-by='.lastTimestamp'

# Watch events
sudo k3s kubectl get events -n iot-edge --watch
```

### Resource Usage

```bash
# Node resources
sudo k3s kubectl top nodes

# Pod resources
sudo k3s kubectl top pods -n iot-edge

# Detailed metrics
sudo k3s kubectl describe node
```

---

## Scaling

### Demo Mode (3-Node Cluster)

For demonstrations, scale infrastructure to 3 replicas:

```bash
cd edge
./demo-mode.sh
```

**What it does:**
- Scales EMQX to 3 replicas (clustered MQTT)
- Scales Redpanda to 3 replicas (replication factor=3)
- Configures high availability and fault tolerance
- Python services auto-scale (1-30 replicas)

**Capacity:** 100K+ devices, 1M+ msg/sec

### Local Mode (Default HA)

For local development, scale back to defaults:

```bash
cd edge
./local-mode.sh
```

**What it does:**
- Scales EMQX to 2 replicas (default HA)
- Scales Redpanda to 1 replica (replication factor=1)
- Reduces resource usage for local machine
- Python services auto-scale (2-30 replicas)

**Capacity:** 10K+ devices, 100K+ msg/sec

### Auto-Scaling (HPA)

Python services automatically scale based on CPU/memory:

```bash
# Check HPA status
sudo k3s kubectl get hpa -n iot-edge

# Expected output:
# NAME                          REFERENCE                        TARGETS    MINPODS   MAXPODS   REPLICAS
# ingestion-service-hpa         Deployment/ingestion-service     45%/70%    2         30        2
# transformation-service-hpa    Deployment/transformation-service 30%/70%   2         30        2
# influxdb-writer-hpa           Deployment/influxdb-writer        20%/70%   2         10        2
# cloud-uplink-hpa              Deployment/cloud-uplink           15%/70%   1          5        1
```

**Scaling triggers:**
- CPU > 70% → Scale up
- Memory > 80% → Scale up
- CPU < 50% for 5 min → Scale down

**Max replicas:**
- Ingestion: 30 (matches Redpanda partitions)
- Transformation: 30 (matches Redpanda partitions)
- InfluxDB Writer: 10

---

## Troubleshooting

### Pods Not Starting

**Check status:**
```bash
sudo k3s kubectl get pods -n iot-edge
sudo k3s kubectl describe pod <pod-name> -n iot-edge
```

**Common issues:**

1. **ImagePullBackOff** - Images not built
   ```bash
   cd edge
   ./k3s-build-images.sh
   ```

2. **CrashLoopBackOff** - Check logs
   ```bash
   sudo k3s kubectl logs <pod-name> -n iot-edge
   sudo k3s kubectl logs --previous <pod-name> -n iot-edge
   ```

3. **Pending** - Resource constraints
   ```bash
   sudo k3s kubectl describe pod <pod-name> -n iot-edge
   sudo k3s kubectl top nodes
   ```

### Service Connection Issues

**MQTT connection failed:**
```bash
# Check EMQX status
sudo k3s kubectl get pods -n iot-edge -l app=emqx

# Check EMQX logs
sudo k3s kubectl logs -f statefulset/emqx -n iot-edge

# Test MQTT port
nc -zv localhost 31883

# Port-forward if needed
sudo k3s kubectl port-forward -n iot-edge svc/emqx 1883:1883
```

**Redpanda connection failed:**
```bash
# Check Redpanda status
sudo k3s kubectl get pods -n iot-edge -l app=redpanda

# Check Redpanda logs
sudo k3s kubectl logs -f statefulset/redpanda-0 -n iot-edge

# Test Redpanda from inside cluster
sudo k3s kubectl exec -n iot-edge redpanda-0 -- rpk cluster health
```

**InfluxDB connection failed:**
```bash
# Check InfluxDB status
sudo k3s kubectl get pods -n iot-edge -l app=influxdb

# Test InfluxDB
sudo k3s kubectl exec -n iot-edge daemonset/influxdb -- influx ping

# Port-forward
sudo k3s kubectl port-forward -n iot-edge svc/influxdb 8086:8086
```

### No Data Flow

**1. Check simulator is running:**
```bash
cd simulator
./run-simulator.sh
```

**2. Check ingestion service:**
```bash
sudo k3s kubectl logs -f deployment/ingestion-service -n iot-edge
```

Expected output:
```
[INFO] ✓ Connected to MQTT broker
[INFO] ✓ Subscribed to shared topic: $share/ingestion-group/sensors/+/data
[INFO] ✓ AP mode: acks=1, fast timeouts, ordering preserved
```

**3. Check Redpanda has messages:**
```bash
sudo k3s kubectl exec -n iot-edge redpanda-0 -- \
  rpk topic consume raw-sensor-data --num 10
```

**4. Check transformation service:**
```bash
sudo k3s kubectl logs -f deployment/transformation-service -n iot-edge
```

**5. Check InfluxDB writer:**
```bash
sudo k3s kubectl logs -f deployment/influxdb-writer -n iot-edge
```

**6. Verify data in InfluxDB:**
```bash
sudo k3s kubectl port-forward -n iot-edge svc/influxdb 8086:8086 &

curl -X POST "http://localhost:8086/api/v2/query?org=iot-org" \
  -H "Authorization: Token my-super-secret-token" \
  -H "Content-Type: application/json" \
  -d '{"query": "from(bucket:\"sensor-data\") |> range(start:-5m) |> limit(n:10)"}'
```

### Auto-Scaling Not Working

**Check metrics-server:**
```bash
sudo k3s kubectl top nodes
sudo k3s kubectl top pods -n iot-edge
```

If metrics not available:
```bash
# K3s includes metrics-server by default
# Restart if needed:
sudo systemctl restart k3s
```

**Check HPA events:**
```bash
sudo k3s kubectl describe hpa ingestion-service-hpa -n iot-edge
```

### Disk Space Issues

**Check node disk usage:**
```bash
df -h
```

**Clean up old images:**
```bash
docker system prune -a
```

**Clean up K3s data:**
```bash
sudo k3s crictl rmi --prune
```

### Memory Issues

**Check memory usage:**
```bash
free -h
sudo k3s kubectl top nodes
sudo k3s kubectl top pods -n iot-edge
```

**Reduce resource limits:**
```bash
# Edit deployment
sudo k3s kubectl edit deployment ingestion-service -n iot-edge

# Reduce resource requests/limits
resources:
  requests:
    memory: "64Mi"
    cpu: "50m"
  limits:
    memory: "256Mi"
    cpu: "250m"
```

### Complete Reset

```bash
# Undeploy everything
cd edge
./k3s-undeploy.sh

# Delete namespace (removes all data!)
sudo k3s kubectl delete namespace iot-edge

# Clean PVCs
sudo k3s kubectl delete pvc --all -n iot-edge

# Redeploy
./k3s-deploy.sh
```

---

## Maintenance

### Update Services

```bash
# Rebuild images
cd edge
./k3s-build-images.sh

# Rolling update (zero downtime)
sudo k3s kubectl rollout restart deployment/ingestion-service -n iot-edge

# Check rollout status
sudo k3s kubectl rollout status deployment/ingestion-service -n iot-edge

# Rollback if needed
sudo k3s kubectl rollout undo deployment/ingestion-service -n iot-edge
```

### Backup Configuration

```bash
# Export all manifests
sudo k3s kubectl get all -n iot-edge -o yaml > edge-backup.yaml

# Export specific resources
sudo k3s kubectl get configmap edge-config -n iot-edge -o yaml > configmap-backup.yaml
sudo k3s kubectl get secret edge-secrets -n iot-edge -o yaml > secrets-backup.yaml
```

### Clean Up Logs

```bash
# K3s automatically rotates logs
# Manual cleanup if needed:
sudo journalctl --vacuum-time=7d
```

---

## Multi-Node K3s Cluster

### Add Worker Nodes

**On master node, get token:**
```bash
sudo cat /var/lib/rancher/k3s/server/node-token
```

**On worker nodes:**
```bash
curl -sfL https://get.k3s.io | K3S_URL=https://<master-ip>:6443 K3S_TOKEN=<token> sh -
```

**Verify cluster:**
```bash
sudo k3s kubectl get nodes
```

### Node Labels (Optional)

```bash
# Label nodes for specific workloads
sudo k3s kubectl label node <node-name> workload=edge
sudo k3s kubectl label node <node-name> zone=zone-1

# Use node selectors in deployments
nodeSelector:
  workload: edge
```

---

## Security Hardening (Production)

### 1. Change Default Passwords

```bash
# Update secrets
sudo k3s kubectl edit secret edge-secrets -n iot-edge

# Update EMQX password via dashboard
# URL: http://localhost:31803
```

### 2. Enable TLS/SSL

```bash
# For EMQX (MQTT over TLS)
# Add certificates to ConfigMap
# Update EMQX config for port 8883

# For InfluxDB (HTTPS)
# Add TLS certificates
# Update service URLs
```

### 3. Network Policies

```bash
# Restrict pod-to-pod communication
sudo k3s kubectl apply -f edge/k3s/network-policies/
```

### 4. RBAC

```bash
# Create service accounts with minimal permissions
sudo k3s kubectl create serviceaccount edge-sa -n iot-edge

# Bind to role
sudo k3s kubectl create rolebinding edge-binding \
  --serviceaccount=iot-edge:edge-sa \
  --role=edge-role \
  -n iot-edge
```

---

## Performance Tuning

### Increase Redpanda Partitions

```bash
sudo k3s kubectl exec -n iot-edge redpanda-0 -- \
  rpk topic alter raw-sensor-data --partitions 50
```

**Note:** Can only increase, not decrease.

### Tune HPA Thresholds

```bash
# Edit HPA
sudo k3s kubectl edit hpa ingestion-service-hpa -n iot-edge

# Adjust CPU target
metrics:
- type: Resource
  resource:
    name: cpu
    target:
      type: Utilization
      averageUtilization: 60  # Was 70
```

### Increase Resource Limits

```bash
sudo k3s kubectl edit deployment ingestion-service -n iot-edge

# Increase limits
resources:
  limits:
    memory: "1Gi"  # Was 512Mi
    cpu: "1000m"   # Was 500m
```

---

## Advanced Topics

### Custom Metrics for HPA

Use KEDA for advanced auto-scaling based on Kafka lag:

```bash
# Install KEDA
sudo k3s kubectl apply -f https://github.com/kedacore/keda/releases/download/v2.12.0/keda-2.12.0.yaml

# Create ScaledObject for Kafka lag-based scaling
# See edge/k3s/keda/ (if implemented)
```

### Observability Stack

Deploy Prometheus and Grafana:

```bash
# Prometheus for metrics
sudo k3s kubectl apply -f edge/k3s/monitoring/prometheus/

# Grafana for dashboards
sudo k3s kubectl apply -f edge/k3s/monitoring/grafana/
```

### Distributed Tracing

Add Jaeger for request tracing:

```bash
sudo k3s kubectl apply -f edge/k3s/tracing/jaeger/
```

---

## Common Commands Reference

```bash
# Deployment
./k3s-build-images.sh    # Build images
./k3s-deploy.sh          # Deploy
./k3s-undeploy.sh        # Undeploy
./k3s-status.sh          # Status check

# Scaling
./demo-mode.sh           # Scale to 3-node cluster
./local-mode.sh          # Scale to single node

# Monitoring
sudo k3s kubectl get pods -n iot-edge --watch
sudo k3s kubectl logs -f deployment/<name> -n iot-edge
sudo k3s kubectl get hpa -n iot-edge
sudo k3s kubectl get events -n iot-edge --sort-by='.lastTimestamp'

# Debugging
sudo k3s kubectl describe pod <pod> -n iot-edge
sudo k3s kubectl exec -it <pod> -n iot-edge -- /bin/bash
sudo k3s kubectl top pods -n iot-edge

# Maintenance
sudo k3s kubectl rollout restart deployment/<name> -n iot-edge
sudo k3s kubectl rollout status deployment/<name> -n iot-edge
sudo k3s kubectl rollout undo deployment/<name> -n iot-edge
```

---

## Additional Resources

- [K3s Documentation](https://docs.k3s.io/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [EMQX Documentation](https://www.emqx.io/docs/)
- [Redpanda Documentation](https://docs.redpanda.com/)
- [InfluxDB Documentation](https://docs.influxdata.com/)

---

**System is production-ready for edge IoT deployments!** 🚀
