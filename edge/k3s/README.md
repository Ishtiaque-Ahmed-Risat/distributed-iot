# K3s Deployment for IoT Edge Gateway

This directory contains Kubernetes manifests for deploying the IoT Edge Gateway system to K3s.

## Architecture

```
┌─────────────────────────────────────────────────┐
│              K3s Cluster (Edge)                 │
│                                                 │
│  ┌──────────────────────────────────────────┐  │
│  │  Namespace: iot-edge                     │  │
│  │                                          │  │
│  │  Infrastructure:                         │  │
│  │    - EMQX (MQTT Broker)                  │  │
│  │    - Redpanda (Message Queue)            │  │
│  │    - InfluxDB (Time-Series DB)           │  │
│  │                                          │  │
│  │  Application Services:                   │  │
│  │    - Device Registry (REST API)          │  │
│  │    - Ingestion Service                   │  │
│  │    - Transformation Service              │  │
│  │    - InfluxDB Writer                     │  │
│  │                                          │  │
│  │  Features:                               │  │
│  │    - Auto-scaling (HPA)                  │  │
│  │    - Health checks                       │  │
│  │    - Resource limits                     │  │
│  │    - Persistent volumes                  │  │
│  └──────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
```

## Directory Structure

```
k3s/
├── 00-namespace.yaml              # Namespace definition
├── 01-configmap.yaml              # Configuration data
├── 02-secrets.yaml                # Sensitive data
├── emqx/                          # EMQX MQTT Broker
│   ├── deployment.yaml
│   ├── service.yaml
│   └── pvc.yaml
├── redpanda/                      # Redpanda Message Queue
│   ├── statefulset.yaml
│   └── service.yaml
├── influxdb/                      # InfluxDB Time-Series DB
│   ├── deployment.yaml
│   ├── service.yaml
│   └── pvc.yaml
├── device-registry/               # Device Registry API
│   ├── deployment.yaml
│   └── service.yaml
├── ingestion-service/             # Data Ingestion
│   ├── deployment.yaml
│   └── hpa.yaml
├── transformation-service/        # Data Transformation
│   ├── deployment.yaml
│   └── hpa.yaml
└── influxdb-writer/               # InfluxDB Writer
    ├── deployment.yaml
    └── hpa.yaml
```

## Prerequisites

### 1. Install K3s

```bash
# Install K3s (lightweight Kubernetes)
curl -sfL https://get.k3s.io | sh -

# Check installation
sudo k3s sudo k3s kubectl get nodes

# Set up sudo k3s kubectl for current user (optional)
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $USER:$USER ~/.kube/config
export KUBECONFIG=~/.kube/config

# Verify
sudo k3s kubectl get nodes
```

### 2. Build Docker Images

```bash
cd edge
./k3s-build-images.sh
```

This script:
- Builds all custom service images (ingestion, device-registry, transformation, influxdb-writer)
- Imports images into K3s

## Deployment

### Quick Deploy

```bash
cd edge
./k3s-deploy.sh
```

This script deploys all services in the correct order and waits for them to be ready.

### Manual Deployment

```bash
# Deploy in order
sudo k3s kubectl apply -f k3s/00-namespace.yaml
sudo k3s kubectl apply -f k3s/01-configmap.yaml
sudo k3s kubectl apply -f k3s/02-secrets.yaml

# Infrastructure
sudo k3s kubectl apply -f k3s/emqx/
sudo k3s kubectl apply -f k3s/redpanda/
sudo k3s kubectl apply -f k3s/influxdb/

# Wait for infrastructure to be ready
sudo k3s kubectl wait --for=condition=ready pod -l app=emqx -n iot-edge --timeout=120s
sudo k3s kubectl wait --for=condition=ready pod -l app=redpanda -n iot-edge --timeout=120s
sudo k3s kubectl wait --for=condition=ready pod -l app=influxdb -n iot-edge --timeout=120s

# Application services
sudo k3s kubectl apply -f k3s/device-registry/
sudo k3s kubectl apply -f k3s/ingestion-service/
sudo k3s kubectl apply -f k3s/transformation-service/
sudo k3s kubectl apply -f k3s/influxdb-writer/
```

## Accessing Services

All services are exposed via NodePort:

| Service | Internal | External (NodePort) | Credentials |
|---------|----------|---------------------|-------------|
| **EMQX Dashboard** | emqx:18083 | localhost:31803 | admin / public |
| **EMQX MQTT** | emqx:1883 | localhost:31883 | - |
| **InfluxDB UI** | influxdb:8086 | localhost:31086 | admin / adminpassword |
| **Device Registry** | device-registry:8080 | localhost:31080 | - |
| **Redpanda Kafka** | redpanda:9092 | localhost:31092 | - |
| **Redpanda Admin** | redpanda:9644 | localhost:31964 | - |

### Access from IoT Simulator

Update simulator configuration to use NodePort:

```yaml
# simulator/config.yaml
mqtt:
  broker: "<K3s-Node-IP>"
  port: 31883  # NodePort for EMQX

device_registry:
  enabled: true
  url: "http://<K3s-Node-IP>:31080"
```

## Management

### Check Status

```bash
# Quick status
./k3s-status.sh

# Or manually
sudo k3s kubectl get all -n iot-edge
sudo k3s kubectl get pods -n iot-edge -o wide
sudo k3s kubectl get svc -n iot-edge
sudo k3s kubectl get pvc -n iot-edge
sudo k3s kubectl get hpa -n iot-edge
```

### View Logs

```bash
# Specific service
sudo k3s kubectl logs -f deployment/ingestion-service -n iot-edge
sudo k3s kubectl logs -f deployment/device-registry -n iot-edge

# All pods with label
sudo k3s kubectl logs -l component=data-pipeline -n iot-edge --tail=100

# Redpanda (StatefulSet)
sudo k3s kubectl logs -f statefulset/redpanda -n iot-edge
```

### Scale Services

```bash
# Manual scaling
sudo k3s kubectl scale deployment ingestion-service --replicas=3 -n iot-edge
sudo k3s kubectl scale deployment transformation-service --replicas=2 -n iot-edge

# Check HPA (Horizontal Pod Autoscaler)
sudo k3s kubectl get hpa -n iot-edge
sudo k3s kubectl describe hpa ingestion-service-hpa -n iot-edge
```

### Update Service

```bash
# Edit deployment
sudo k3s kubectl edit deployment ingestion-service -n iot-edge

# Or apply changes
sudo k3s kubectl apply -f k3s/ingestion-service/deployment.yaml

# Check rollout status
sudo k3s kubectl rollout status deployment/ingestion-service -n iot-edge

# Rollback if needed
sudo k3s kubectl rollout undo deployment/ingestion-service -n iot-edge
```

### Restart Service

```bash
# Rolling restart (zero downtime)
sudo k3s kubectl rollout restart deployment/ingestion-service -n iot-edge

# Or delete pod (will be recreated)
sudo k3s kubectl delete pod <pod-name> -n iot-edge
```

## Auto-Scaling

Services are configured with Horizontal Pod Autoscalers (HPA):

```yaml
# Example HPA configuration
minReplicas: 1
maxReplicas: 10
metrics:
  - CPU: 70% threshold
  - Memory: 80% threshold
```

**Configured Services:**
- `ingestion-service`: 1-10 replicas
- `transformation-service`: 1-5 replicas
- `influxdb-writer`: 1-3 replicas

### Test Auto-Scaling

```bash
# Generate load on ingestion service
# (Run from simulator)
cd simulator
# Edit config.yaml to increase num_devices and decrease interval
./run-simulator.sh

# Watch scaling in another terminal
sudo k3s kubectl get hpa -n iot-edge --watch
```

## Monitoring

### Built-in Monitoring

```bash
# Resource usage
sudo k3s kubectl top pods -n iot-edge
sudo k3s kubectl top nodes

# Pod events
sudo k3s kubectl get events -n iot-edge --sort-by='.lastTimestamp'

# Describe for detailed info
sudo k3s kubectl describe pod <pod-name> -n iot-edge
```

### Install Metrics Server (if not present)

```bash
sudo k3s kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

### Prometheus + Grafana (Optional)

```bash
# Install Prometheus operator
sudo k3s kubectl apply -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/bundle.yaml

# Add ServiceMonitors for your services
```

## Troubleshooting

### Pods Not Starting

```bash
# Check pod status
sudo k3s kubectl get pods -n iot-edge

# Describe pod
sudo k3s kubectl describe pod <pod-name> -n iot-edge

# Check logs
sudo k3s kubectl logs <pod-name> -n iot-edge

# Common issues:
# - Image not found: Run ./k3s-build-images.sh
# - PVC pending: Check storage class
# - CrashLoopBackOff: Check logs for application errors
```

### Images Not Found

```bash
# Rebuild and import images
cd edge
./k3s-build-images.sh

# Verify images in K3s
sudo k3s crictl images | grep -E "(ingestion|device-registry|transformation|influxdb-writer)"
```

### Service Not Accessible

```bash
# Check service
sudo k3s kubectl get svc -n iot-edge

# Check endpoints
sudo k3s kubectl get endpoints -n iot-edge

# Port forward for testing
sudo k3s kubectl port-forward -n iot-edge svc/device-registry 8080:8080
```

### Storage Issues

```bash
# Check PVCs
sudo k3s kubectl get pvc -n iot-edge

# Describe PVC
sudo k3s kubectl describe pvc <pvc-name> -n iot-edge

# Check available storage
sudo k3s kubectl get storageclass
```

### Reset Everything

```bash
# Delete all resources
./k3s-undeploy.sh

# Clean up volumes (if needed)
sudo rm -rf /var/lib/rancher/k3s/storage/*

# Redeploy
./k3s-deploy.sh
```

## Uninstall

### Remove Deployment

```bash
cd edge
./k3s-undeploy.sh
```

### Uninstall K3s

```bash
# Stop K3s
sudo systemctl stop k3s

# Uninstall
sudo /usr/local/bin/k3s-uninstall.sh
```

## Configuration

### Environment Variables

Edit `k3s/01-configmap.yaml` to change:
- MQTT broker settings
- Redpanda topic names
- InfluxDB configuration
- Batch sizes and intervals
- LLM settings

### Secrets

Edit `k3s/02-secrets.yaml` for:
- InfluxDB admin password
- InfluxDB tokens
- Other sensitive data

**Note:** For production, use proper secret management (sealed-secrets, external-secrets, Vault).

### Resource Limits

Adjust in each deployment.yaml:

```yaml
resources:
  requests:
    memory: "128Mi"
    cpu: "100m"
  limits:
    memory: "512Mi"
    cpu: "500m"
```

## Production Considerations

### High Availability

1. **Multi-node K3s cluster:**
   ```bash
   # On master
   curl -sfL https://get.k3s.io | sh -
   
   # Get token
   sudo cat /var/lib/rancher/k3s/server/node-token
   
   # On workers
   curl -sfL https://get.k3s.io | K3S_URL=https://master:6443 K3S_TOKEN=<token> sh -
   ```

2. **Replicate StatefulSets:**
   ```yaml
   # redpanda/statefulset.yaml
   replicas: 3  # Change from 1
   ```

3. **Add pod anti-affinity** to spread pods across nodes

### Security

1. **Network Policies:**
   ```yaml
   apiVersion: networking.k8s.io/v1
   kind: NetworkPolicy
   metadata:
     name: deny-all
     namespace: iot-edge
   spec:
     podSelector: {}
     policyTypes:
     - Ingress
     - Egress
   ```

2. **RBAC:** Create ServiceAccounts with minimal permissions

3. **Pod Security Standards:** Enforce restricted policies

### Persistence

1. **Backup PVCs regularly**
2. **Use external storage** (NFS, Longhorn, etc.)
3. **Configure retention policies**

### Monitoring

1. **Install Prometheus + Grafana**
2. **Add ServiceMonitors**
3. **Set up alerts** (PagerDuty, Slack)
4. **Log aggregation** (Loki, Elasticsearch)

## Next Steps

1. **Deploy multi-gateway cluster** for geo-distribution
2. **Add etcd** for coordination and leader election
3. **Implement cloud uplink service** for edge-to-cloud replication
4. **Add ML inference service** with GPUs
5. **Implement GitOps** with ArgoCD or Flux

## References

- [K3s Documentation](https://docs.k3s.io/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [EMQX on Kubernetes](https://www.emqx.io/docs/en/v5.0/deploy/install-k8s.html)
- [Redpanda on Kubernetes](https://docs.redpanda.com/docs/deploy/deployment-option/self-hosted/kubernetes/)
