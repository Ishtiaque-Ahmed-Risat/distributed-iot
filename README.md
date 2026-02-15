# Distributed 5G IoT Gateway System

A distributed IoT system with edge processing, real-time ML inference, and cloud analytics.

## Architecture
![System Architecture](docs/architecture.png)

```
IoT Devices → Device Registry (descriptions) 
           → EMQX → Ingestion → Redpanda(raw)
           → Transformation Service (unit conversion, semantic normalization)
           → Redpanda(transformed) → InfluxDB
           → Cloud (Cassandra, MinIO, Spark)
```

**Key Feature**: Data Transformation Layer with LLM extensibility for semantic understanding.

## Project Structure

```
iot_system/
├── edge/               # Edge gateway services and infrastructure
├── cloud/              # Cloud services and infrastructure
├── simulator/          # IoT device simulator
├── shared/             # Shared libraries and configurations
└── docs/               # Documentation
```

## Quick Start

See [QUICKSTART.md](QUICKSTART.md) for detailed instructions.

### 1. Install K3s
```bash
curl -sfL https://get.k3s.io | sh -
```

### 2. Deploy Edge Gateway
```bash
cd edge
./k3s-build-images.sh
./k3s-deploy.sh
```

### 3. Start Simulator
```bash
cd simulator
./run-simulator.sh
```

## Tech Stack

**Edge:**
- EMQX (MQTT Broker)
- Device Registry (Python FastAPI) - REST API for device descriptions
- Data Ingestion Service (Python)
- **Data Transformation Service (Python)** - Unit conversion & semantic normalization
- Redpanda (Message Queue - raw & transformed topics)
- InfluxDB (Time-series Buffer - 7 days)
- InfluxDB Writer Service (Python)
- ML Inference Service (Python) - Phase 2
- K3s (Orchestration)

**Cloud:**
- Redpanda (Message Bus)
- Cassandra (Time-series DB)
- MinIO (Data Lake)
- Spark (Batch Analytics)
- Go (API Gateway, Services)
- Python (ML Training)
- Kubernetes (Orchestration)

## Development Phases

- **Phase 1** (Weeks 1-2): Basic edge gateway with MQTT and storage
- **Phase 2** (Weeks 3-4): Edge processing with ML inference
- **Phase 3** (Weeks 5-6): Cloud integration and analytics

## Requirements

- **K3s** (lightweight Kubernetes for edge)
- **Docker** (for building images)
- **Python 3.11+** (for services and simulator)
- **Kubernetes** (optional, for cloud deployment)

## Documentation

See `docs/` for detailed architecture and API documentation.
