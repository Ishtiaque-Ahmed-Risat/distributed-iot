#!/bin/bash
# Cloud Services Startup Script

set -e

echo "========================================"
echo "  IoT Cloud Services - Startup"
echo "========================================"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

cd "$(dirname "$0")"

# Auto-detect host IP for Redpanda external advertise address
# This allows K3s pods on the same machine to reach cloud Redpanda
export HOST_IP=${HOST_IP:-$(hostname -I | awk '{print $1}')}
echo "Host IP: $HOST_IP"
echo "(Cloud Redpanda will advertise external address as $HOST_IP:29092)"
echo ""

echo "Starting cloud infrastructure and services..."
echo ""
echo "Infrastructure:"
echo "  - Redpanda (Message Bus)      : localhost:29092 (Kafka), localhost:29644 (Admin)"
echo "  - Cassandra (Time-series DB)  : localhost:9042"
echo "  - MinIO (Data Lake)           : http://localhost:9001 (minioadmin/minioadmin)"
echo ""
echo "Services:"
echo "  - Cassandra Writer            : consumes from Redpanda -> writes to Cassandra"
echo "  - Cloud API                   : http://localhost:8000/docs"
echo ""

docker-compose up -d --build

echo ""
echo "Waiting for init jobs and services to stabilize..."
sleep 10

echo ""
echo "Service Status:"
docker-compose ps

echo ""
echo "========================================"
echo "Cloud Services Started!"
echo "========================================"
echo ""
echo "Access Points:"
echo "  Cloud API (Swagger UI)  : http://localhost:8000/docs"
echo "  MinIO Console           : http://localhost:9001 (minioadmin/minioadmin)"
echo "  Redpanda Admin          : http://localhost:29644"
echo ""
echo "Run ML Training Job:"
echo "  docker-compose -f docker-compose.yml -f docker-compose.job.yml run spark-job"
echo ""
echo "View logs:"
echo "  docker-compose logs -f cassandra-writer"
echo "  docker-compose logs -f cloud-api"
echo ""
