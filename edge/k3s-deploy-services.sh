#!/bin/bash
#
# Deploy application services: Device Registry, Ingestion, Transformation, InfluxDB Writer, Cloud Uplink
# Run this AFTER k3s-deploy-infra.sh and all infrastructure pods are Running.
#

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "============================================"
echo "Deploying Application Services"
echo "============================================"
echo ""

# Verify infrastructure is running
echo -e "${YELLOW}Checking infrastructure...${NC}"
MISSING=false

for app in emqx redpanda influxdb; do
    COUNT=$(sudo k3s kubectl get pods -n iot-edge -l app=$app --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
    if [ "$COUNT" -eq 0 ]; then
        echo -e "  ${RED}✗ $app is not running${NC}"
        MISSING=true
    else
        echo -e "  ${GREEN}✓ $app is running ($COUNT pod(s))${NC}"
    fi
done

if [ "$MISSING" = true ]; then
    echo ""
    echo -e "${RED}Infrastructure is not fully ready. Run k3s-deploy-infra.sh first.${NC}"
    exit 1
fi
echo ""

# Deploy services
echo -e "${YELLOW}[1/6] Device Registry...${NC}"
sudo k3s kubectl apply -f k3s/device-registry/
echo -e "${GREEN}✓ Deployed${NC}"

echo -e "${YELLOW}[2/6] Ingestion Service...${NC}"
sudo k3s kubectl apply -f k3s/ingestion-service/
echo -e "${GREEN}✓ Deployed${NC}"

echo -e "${YELLOW}[3/6] Transformation Service...${NC}"
sudo k3s kubectl apply -f k3s/transformation-service/
echo -e "${GREEN}✓ Deployed${NC}"

echo -e "${YELLOW}[4/6] InfluxDB Writer...${NC}"
sudo k3s kubectl apply -f k3s/influxdb-writer/
echo -e "${GREEN}✓ Deployed${NC}"

echo -e "${YELLOW}[5/6] Cloud Uplink...${NC}"
sudo k3s kubectl apply -f k3s/cloud-uplink/
echo -e "${GREEN}✓ Deployed${NC}"

echo -e "${YELLOW}[6/6] Anomaly Alert...${NC}"
sudo k3s kubectl apply -f k3s/anomaly-alert/
echo -e "${GREEN}✓ Deployed${NC}"

echo ""
echo -e "${YELLOW}Waiting for services to be ready...${NC}"
sleep 10
echo ""

echo "============================================"
echo -e "${GREEN}All Services Deployed!${NC}"
echo "============================================"
echo ""
sudo k3s kubectl get pods -n iot-edge
echo ""
echo "Service Access:"
echo "  EMQX Dashboard:  http://localhost:31803  (admin/public)"
echo "  InfluxDB UI:     http://localhost:31086  (admin/adminpassword)"
echo "  Device Registry: http://localhost:31080/docs"
echo "  Redpanda Admin:  http://localhost:31964"
echo "  Cloud API:       http://localhost:8000/docs (when cloud is running)"
echo ""
echo "HPA Status:"
sudo k3s kubectl get hpa -n iot-edge 2>/dev/null || echo "  (no HPA found)"
echo ""