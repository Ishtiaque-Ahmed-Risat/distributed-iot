#!/bin/bash
#
# Undeploy IoT Edge Gateway from K3s
# This script removes all deployed resources from K3s
#

set -e

echo "============================================"
echo "Undeploying IoT Edge Gateway from K3s"
echo "============================================"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Confirmation
read -p "This will delete all resources in the iot-edge namespace. Continue? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 0
fi

echo ""
echo -e "${YELLOW}Deleting namespace and all resources...${NC}"

# Delete entire namespace (cascades to all resources)
sudo k3s kubectl delete namespace iot-edge --timeout=120s

echo ""
echo -e "${GREEN}✓ All resources removed${NC}"
echo ""
echo "To redeploy:"
echo "  1. Build images:     ./k3s-build-images.sh"
echo "  2. Deploy infra:    ./k3s-deploy-infra.sh"
echo "  3. Deploy services: ./k3s-deploy-services.sh"
echo ""