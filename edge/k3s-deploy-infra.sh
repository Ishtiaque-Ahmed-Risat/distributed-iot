#!/bin/bash
#
# Deploy infrastructure: Namespace, configs, EMQX, Redpanda, InfluxDB, and topics
# Run this FIRST, then run k3s-deploy-services.sh after all pods are Running.
#

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "============================================"
echo "Deploying Infrastructure"
echo "============================================"
echo ""

# Check K3s
if ! sudo k3s kubectl get nodes &> /dev/null; then
    echo -e "${RED}✗ Cannot connect to K3s cluster${NC}"
    exit 1
fi
echo -e "${GREEN}✓ K3s cluster is running${NC}"
echo ""

# Namespace + config
echo -e "${YELLOW}[1/5] Namespace and configurations...${NC}"
sudo k3s kubectl apply -f k3s/00-namespace.yaml
sudo k3s kubectl apply -f k3s/01-configmap.yaml
sudo k3s kubectl apply -f k3s/02-secrets.yaml
echo -e "${GREEN}✓ Done${NC}"
echo ""

# EMQX
echo -e "${YELLOW}[2/5] EMQX (MQTT Broker)...${NC}"
sudo k3s kubectl apply -f k3s/emqx/
echo -e "${GREEN}✓ EMQX deployed${NC}"
echo ""

# Redpanda (StatefulSet + services, NOT topic-init-job yet)
echo -e "${YELLOW}[3/5] Redpanda (Message Queue)...${NC}"
sudo k3s kubectl apply -f k3s/redpanda/service.yaml
sudo k3s kubectl apply -f k3s/redpanda/service-headless.yaml
sudo k3s kubectl apply -f k3s/redpanda/pdb.yaml
sudo k3s kubectl apply -f k3s/redpanda/statefulset.yaml
echo -e "${GREEN}✓ Redpanda deployed${NC}"
echo ""

# InfluxDB
echo -e "${YELLOW}[4/5] InfluxDB (Time-Series DB)...${NC}"
sudo k3s kubectl apply -f k3s/influxdb/
echo -e "${GREEN}✓ InfluxDB deployed${NC}"
echo ""

# Wait for infra pods
echo -e "${YELLOW}Waiting for infrastructure pods...${NC}"
sudo k3s kubectl wait --for=condition=ready pod -l app=emqx -n iot-edge --timeout=180s 2>/dev/null || echo -e "${YELLOW}⚠ EMQX not ready yet${NC}"
sudo k3s kubectl wait --for=condition=ready pod -l app=redpanda -n iot-edge --timeout=180s 2>/dev/null || echo -e "${YELLOW}⚠ Redpanda not ready yet${NC}"
sudo k3s kubectl wait --for=condition=ready pod -l app=influxdb -n iot-edge --timeout=120s 2>/dev/null || echo -e "${YELLOW}⚠ InfluxDB not ready yet${NC}"
echo -e "${GREEN}✓ Infrastructure pods ready${NC}"
echo ""

# Topic initialization
echo -e "${YELLOW}[5/5] Creating Redpanda topics (10 partitions each)...${NC}"
sudo k3s kubectl delete job redpanda-topic-init -n iot-edge 2>/dev/null || true
sleep 2
sudo k3s kubectl apply -f k3s/redpanda/topic-init-job.yaml
echo "  Waiting for topic init job to complete..."
if sudo k3s kubectl wait --for=condition=complete job/redpanda-topic-init -n iot-edge --timeout=300s 2>/dev/null; then
    echo -e "${GREEN}✓ Topics created${NC}"
else
    echo -e "${RED}⚠ Topic init did not complete in time. Check logs:${NC}"
    echo "  sudo k3s kubectl logs job/redpanda-topic-init -n iot-edge"
fi

echo ""
echo "============================================"
echo -e "${GREEN}Infrastructure Ready!${NC}"
echo "============================================"
echo ""
sudo k3s kubectl get pods -n iot-edge
echo ""
echo -e "Next: ${YELLOW}./k3s-deploy-services.sh${NC}"
echo ""