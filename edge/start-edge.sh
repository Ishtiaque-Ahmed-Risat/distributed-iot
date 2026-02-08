#!/bin/bash
# Edge Gateway Startup Script

set -e

echo "========================================"
echo "  IoT Edge Gateway - Startup Script"
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

# Navigate to edge directory
cd "$(dirname "$0")"

echo "Building services..."
docker-compose build

echo ""
echo "Starting edge gateway services..."
echo ""
echo "Services starting:"
echo "  - EMQX (MQTT Broker)      : http://localhost:18083 (admin/public)"
echo "  - Redpanda (Message Queue): http://localhost:9644"
echo "  - InfluxDB (Time-Series)  : http://localhost:8086 (admin/adminpassword)"
echo "  - Data Ingestion Service  : (background)"
echo "  - InfluxDB Writer Service : (background)"
echo ""

docker-compose up -d

echo ""
echo "Waiting for services to be healthy..."
sleep 10

# Check service health
echo ""
echo "Service Status:"
docker-compose ps

echo ""
echo "========================================"
echo "Edge Gateway Started Successfully!"
echo "========================================"
echo ""
echo "Useful Commands:"
echo "  - View logs:     docker-compose logs -f"
echo "  - Stop services: docker-compose down"
echo "  - Restart:       docker-compose restart"
echo ""
echo "EMQX Dashboard:  http://localhost:18083"
echo "  Username: admin"
echo "  Password: public"
echo ""
echo "InfluxDB UI:     http://localhost:8086"
echo "  Username: admin"
echo "  Password: adminpassword"
echo ""
echo "Next Steps:"
echo "  1. Go to ../simulator/ and run: ./run-simulator.sh"
echo "  2. Monitor logs: docker-compose logs -f ingestion-service"
echo ""
