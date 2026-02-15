#!/bin/bash
#
# Full deployment: infrastructure + services
# For manual control, run k3s-deploy-infra.sh and k3s-deploy-services.sh separately.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================"
echo "Full Deployment: Infrastructure + Services"
echo "============================================"
echo ""

# Phase 1: Infrastructure
"$SCRIPT_DIR/k3s-deploy-infra.sh"

echo ""
echo "============================================"
echo "Infrastructure is up. Deploying services..."
echo "============================================"
echo ""

# Phase 2: Services
"$SCRIPT_DIR/k3s-deploy-services.sh"
