#!/bin/bash
# Cloud Services Startup Script

set -e

echo "========================================"
echo "  IoT Cloud Services - Startup Script"
echo "========================================"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "Error: docker-compose is not installed"
    exit 1
fi

# Navigate to cloud directory
cd "$(dirname "$0")"

echo "Starting cloud services..."
echo ""
echo "Services starting:"
echo "  - Redpanda (Message Bus)    : http://localhost:29644"
echo "  - Cassandra (Database)      : localhost:9042"
echo "  - MinIO (Data Lake)         : http://localhost:9001 (minioadmin/minioadmin)"
echo ""

docker-compose up -d

echo ""
echo "Waiting for services to be healthy..."
sleep 15

# Check service health
echo ""
echo "Service Status:"
docker-compose ps

echo ""
echo "========================================"
echo "Cloud Services Started Successfully!"
echo "========================================"
echo ""
echo "Useful Commands:"
echo "  - View logs:     docker-compose logs -f"
echo "  - Stop services: docker-compose down"
echo ""
echo "MinIO Console:   http://localhost:9001"
echo "  Username: minioadmin"
echo "  Password: minioadmin"
echo ""
echo "Note: Additional services (API Gateway, ML Training, Spark)"
echo "      will be added in Phase 3."
echo ""
