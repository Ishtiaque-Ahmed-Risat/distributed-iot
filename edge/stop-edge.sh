#!/bin/bash
# Edge Gateway Stop Script

set -e

echo "========================================"
echo "  Stopping Edge Gateway"
echo "========================================"

cd "$(dirname "$0")"

docker-compose down

echo ""
echo "Edge Gateway stopped."
echo ""
echo "To remove volumes (data will be lost):"
echo "  docker-compose down -v"
echo ""
