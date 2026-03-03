#!/bin/bash
# Run Spark Job for ML Training
# Builds and runs the Spark job container to train anomaly detection models

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "Spark Job - ML Training"
echo "=========================================="
echo ""

# Build the Spark job image
echo "Building Spark job image..."
docker-compose -f docker-compose.yml -f docker-compose.job.yml build spark-job

if [ $? -ne 0 ]; then
    echo "✗ Build failed"
    exit 1
fi

echo ""
echo "✓ Build successful"
echo ""

# Run the Spark job
echo "Running Spark job..."
docker-compose -f docker-compose.yml -f docker-compose.job.yml run --rm spark-job

if [ $? -ne 0 ]; then
    echo "✗ Spark job failed"
    exit 1
fi

echo ""
echo "✓ Spark job completed successfully"
