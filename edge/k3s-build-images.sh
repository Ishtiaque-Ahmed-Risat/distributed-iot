#!/bin/bash
#
# Build Docker images for K3s deployment
# This script builds all custom service images and imports them into K3s
#

set -e

echo "============================================"
echo "Building Docker Images for K3s"
echo "============================================"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Build ingestion service
echo -e "\n${YELLOW}[1/4] Building ingestion-service...${NC}"
cd services/ingestion
docker build -t ingestion-service:latest .
cd ../..
echo -e "${GREEN}✓ ingestion-service built${NC}"

# Build device-registry
echo -e "\n${YELLOW}[2/4] Building device-registry...${NC}"
cd services/device-registry
docker build -t device-registry:latest .
cd ../..
echo -e "${GREEN}✓ device-registry built${NC}"

# Build transformation-service
echo -e "\n${YELLOW}[3/4] Building transformation-service...${NC}"
cd services/transformation
docker build -t transformation-service:latest .
cd ../..
echo -e "${GREEN}✓ transformation-service built${NC}"

# Build influxdb-writer
echo -e "\n${YELLOW}[4/4] Building influxdb-writer...${NC}"
cd services/influxdb-writer
docker build -t influxdb-writer:latest .
cd ../..
echo -e "${GREEN}✓ influxdb-writer built${NC}"

# Import images to K3s (if K3s is running)
if command -v k3s &> /dev/null; then
    echo -e "\n${YELLOW}Importing images to K3s...${NC}"
    
    docker save ingestion-service:latest | sudo k3s ctr images import -
    docker save device-registry:latest | sudo k3s ctr images import -
    docker save transformation-service:latest | sudo k3s ctr images import -
    docker save influxdb-writer:latest | sudo k3s ctr images import -
    
    echo -e "${GREEN}✓ Images imported to K3s${NC}"
else
    echo -e "${YELLOW}⚠ K3s not found. Skipping image import.${NC}"
    echo "Images built locally. Deploy K3s first, then run this script again."
fi

echo ""
echo "============================================"
echo -e "${GREEN}All images built successfully!${NC}"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Install K3s: curl -sfL https://get.k3s.io | sh -"
echo "  2. Deploy: ./k3s-deploy.sh"
echo ""
