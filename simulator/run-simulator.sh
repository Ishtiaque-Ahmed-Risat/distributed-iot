#!/bin/bash
# IoT Device Simulator Startup Script

set -e

echo "================================"
echo "IoT Device Simulator"
echo "================================"

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# Run simulator
echo ""
echo "Starting IoT device simulator..."
echo "Press Ctrl+C to stop"
echo ""

python3 device-simulator.py