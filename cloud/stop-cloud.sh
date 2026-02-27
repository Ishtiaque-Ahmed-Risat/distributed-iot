#!/bin/bash
# Cloud Services Stop Script

set -e

echo "========================================"
echo "  Stopping Cloud Services"
echo "========================================"

cd "$(dirname "$0")"

docker-compose down

echo ""
echo "Cloud services stopped."
echo ""
echo "To also remove data volumes:"
echo "  docker compose down -v"
echo ""
