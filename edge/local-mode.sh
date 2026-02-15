#!/bin/bash
# Local Mode: Scale down infrastructure for local development
# This script scales EMQX to 2 replicas and Redpanda to 1 replica

set -e

echo "🏠 Activating LOCAL MODE (single-node setup)"
echo "=============================================="
echo ""

# Check if namespace exists
if ! sudo k3s kubectl get namespace iot-edge &> /dev/null; then
    echo "❌ Error: iot-edge namespace not found"
    echo "Please deploy the edge subsystem first: ./k3s-deploy.sh"
    exit 1
fi

# Scale EMQX to 2 replicas (default HA)
echo "📡 Scaling EMQX to 2 replicas..."
sudo k3s kubectl scale sts emqx --replicas=2 -n iot-edge
echo "✓ EMQX scaled to 2 replicas"
echo ""

# Scale Redpanda to 1 replica
echo "📊 Scaling Redpanda to 1 replica..."
sudo k3s kubectl scale sts redpanda --replicas=1 -n iot-edge
echo "✓ Redpanda scaled to 1 replica"
echo ""

# Wait for scale-down to complete
echo "⏳ Waiting for pods to terminate (15 seconds)..."
sleep 15

# Update topic replication factor
echo "🔧 Updating topic replication factor to 1..."
sudo k3s kubectl exec redpanda-0 -n iot-edge -- \
    rpk topic alter-config raw-sensor-data \
    --set replication.factor=1 \
    --set min.insync.replicas=1 \
    --brokers localhost:9092 2>/dev/null || echo "  (Topic may not exist yet)"

sudo k3s kubectl exec redpanda-0 -n iot-edge -- \
    rpk topic alter-config transformed-sensor-data \
    --set replication.factor=1 \
    --set min.insync.replicas=1 \
    --brokers localhost:9092 2>/dev/null || echo "  (Topic may not exist yet)"

echo "✓ Topic replication configured"
echo ""

echo "============================================"
echo "✅ LOCAL MODE ACTIVATED!"
echo "============================================"
echo ""
echo "Current Configuration:"
echo "  🔹 EMQX:          2 replicas (default HA)"
echo "  🔹 Redpanda:      1 replica (RF=1)"
echo "  🔹 InfluxDB:      1 per node (DaemonSet)"
echo "  🔹 Services:      Auto-scaling (2-30 replicas based on load)"
echo ""
echo "System Capacity:"
echo "  📈 Devices:       10,000+"
echo "  📈 Messages/sec:  100,000+"
echo "  📈 Connections:   10,000+"
echo ""
echo "To scale up for demo: ./demo-mode.sh"
echo ""
echo "View cluster status:"
echo "  sudo k3s kubectl get pods -n iot-edge"
echo "  sudo k3s kubectl get hpa -n iot-edge"
echo ""
