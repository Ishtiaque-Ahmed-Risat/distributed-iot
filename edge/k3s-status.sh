#!/bin/bash
#
# Check status of IoT Edge Gateway on K3s
#

set -e

echo "============================================"
echo "IoT Edge Gateway Status"
echo "============================================"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}Pods:${NC}"
sudo k3s kubectl get pods -n iot-edge -o wide

echo ""
echo -e "${YELLOW}Services:${NC}"
sudo k3s kubectl get svc -n iot-edge

echo ""
echo -e "${YELLOW}PersistentVolumeClaims:${NC}"
sudo k3s kubectl get pvc -n iot-edge

echo ""
echo -e "${YELLOW}HorizontalPodAutoscalers:${NC}"
sudo k3s kubectl get hpa -n iot-edge

echo ""
echo -e "${YELLOW}Resource Usage:${NC}"
sudo k3s kubectl top pods -n iot-edge 2>/dev/null || echo "Metrics not available (install metrics-server)"

echo ""
