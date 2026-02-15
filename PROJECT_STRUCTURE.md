# Project Structure

```
distributed-iot/
├── README.md                      # Main project documentation
├── QUICKSTART.md                  # Quick start guide (K3s)
├── SETUP.md                       # Complete setup and troubleshooting
├── OPTIMIZATION_SUMMARY.md        # Architecture decisions and optimizations
├── PROJECT_STRUCTURE.md           # This file
├── .gitignore                     # Git ignore rules
│
├── simulator/                     # IoT Device Simulator
│   ├── device-simulator.py        # Main simulator (Python)
│   ├── config.yaml                # Simulator configuration (K3s ports)
│   ├── requirements.txt           # Python dependencies
│   ├── run-simulator.sh           # Startup script
│   └── README.md                  # Simulator documentation
│
├── edge/                          # Edge Gateway (K3s)
│   ├── k3s/                       # K3s manifests
│   │   ├── 00-namespace.yaml     # iot-edge namespace
│   │   ├── 01-configmap.yaml     # Centralized configuration
│   │   ├── 02-secrets.yaml       # Sensitive data
│   │   ├── emqx/                 # EMQX MQTT Broker
│   │   │   ├── statefulset.yaml  # StatefulSet (clustering-ready)
│   │   │   ├── service.yaml      # NodePort service
│   │   │   ├── service-headless.yaml  # Headless for DNS discovery
│   │   │   └── pdb.yaml          # Pod disruption budget
│   │   ├── redpanda/             # Redpanda Message Queue
│   │   │   ├── statefulset.yaml  # StatefulSet (clustering-ready)
│   │   │   ├── service.yaml      # NodePort service
│   │   │   ├── service-headless.yaml  # Headless for DNS discovery
│   │   │   ├── topic-init-job.yaml  # Topic creation with AP config
│   │   │   └── pdb.yaml          # Pod disruption budget
│   │   ├── influxdb/             # InfluxDB Time-Series Database
│   │   │   ├── daemonset.yaml    # DaemonSet (one per node)
│   │   │   └── service.yaml      # Service with node-local routing
│   │   ├── device-registry/      # Device Registry API
│   │   │   ├── deployment.yaml   # Deployment
│   │   │   └── service.yaml      # NodePort service
│   │   ├── ingestion-service/    # Data Ingestion
│   │   │   ├── deployment.yaml   # Deployment
│   │   │   ├── hpa.yaml          # HPA (1-30 replicas)
│   │   │   └── pdb.yaml          # Pod disruption budget
│   │   ├── transformation-service/  # Data Transformation
│   │   │   ├── deployment.yaml   # Deployment
│   │   │   ├── hpa.yaml          # HPA (1-30 replicas)
│   │   │   └── pdb.yaml          # Pod disruption budget
│   │   ├── influxdb-writer/      # InfluxDB Writer
│   │   │   ├── deployment.yaml   # Deployment
│   │   │   ├── hpa.yaml          # HPA (1-10 replicas)
│   │   │   └── pdb.yaml          # Pod disruption budget
│   │   └── cloud-uplink/         # Edge → Cloud Bridge
│   │       ├── deployment.yaml   # Deployment
│   │       ├── hpa.yaml          # HPA (1-5 replicas)
│   │       └── pdb.yaml          # Pod disruption budget
│   │
│   ├── services/                  # Service source code
│   │   ├── ingestion/            # Data Ingestion Service (Python)
│   │   │   ├── main.py           # MQTT → Redpanda (shared subscriptions)
│   │   │   ├── requirements.txt  # Dependencies
│   │   │   ├── Dockerfile        # Container image
│   │   │   └── README.md         # Documentation
│   │   ├── transformation/       # Transformation Service (Python)
│   │   │   ├── main.py           # Unit conversion, normalization
│   │   │   ├── requirements.txt
│   │   │   ├── Dockerfile
│   │   │   └── README.md
│   │   ├── influxdb-writer/      # InfluxDB Writer (Python)
│   │   │   ├── main.py           # Redpanda → InfluxDB (batched)
│   │   │   ├── requirements.txt
│   │   │   ├── Dockerfile
│   │   │   └── README.md
│   │   ├── device-registry/      # Device Registry (Python/FastAPI)
│   │   │   ├── main.py           # REST API for device metadata
│   │   │   ├── requirements.txt
│   │   │   ├── Dockerfile
│   │   │   ├── README.md
│   │   │   ├── QUICKSTART.md
│   │   │   └── test_api.py
│   │   └── cloud-uplink/         # Edge → Cloud Bridge (Python)
│   │       ├── main.py           # Edge Redpanda → Cloud Redpanda
│   │       ├── requirements.txt
│   │       └── Dockerfile
│   │
│   ├── k3s-build-images.sh        # Build and import images to K3s
│   ├── k3s-deploy.sh              # Deploy all services
│   ├── k3s-undeploy.sh            # Remove all services
│   ├── k3s-status.sh              # Check deployment status
│   ├── demo-mode.sh               # Scale to 3-node cluster
│   ├── local-mode.sh              # Scale to single node
│   └── README.md                  # Edge documentation
│
├── cloud/                         # Cloud Services (Docker Compose)
│   ├── docker-compose.yml         # Infrastructure + services
│   ├── docker-compose.job.yml     # ML training job (on-demand)
│   ├── start-cloud.sh             # Start cloud services
│   ├── stop-cloud.sh              # Stop cloud services
│   ├── init/                      # Init scripts and schemas
│   │   └── cassandra-schema.cql   # Cassandra table definitions
│   ├── services/                  # Cloud service source code
│   │   ├── cassandra-writer/      # Redpanda → Cassandra writer
│   │   │   ├── main.py
│   │   │   ├── Dockerfile
│   │   │   └── requirements.txt
│   │   ├── cloud-api/             # REST API (FastAPI)
│   │   │   ├── main.py
│   │   │   ├── Dockerfile
│   │   │   └── requirements.txt
│   │   └── spark-job/             # Batch analytics + ML training
│   │       ├── main.py
│   │       ├── Dockerfile
│   │       └── requirements.txt
│   └── README.md                  # Cloud documentation
│
└── docs/                          # Documentation
    └── architecture.png           # System architecture diagram
```

## Component Overview

### Simulator
- **Purpose**: Simulate thousands of IoT devices
- **Language**: Python
- **Protocol**: MQTT (QoS 1) to K3s NodePort 31883
- **Features**: Fault-tolerant reconnection, persistent sessions
- **Configurable**: Device count, sampling rate, sensor types

### Edge Gateway (K3s)

#### Infrastructure

**EMQX** (StatefulSet)
- MQTT broker with clustering support
- AP-optimized: local session locking, 30min expiry
- Shared subscriptions for load balancing
- Ports: MQTT 31883, Dashboard 31803
- Scaling: Manual (1 or 3 replicas)

**Redpanda** (StatefulSet)
- Message queue with 30 partitions
- Clustering-ready with DNS discovery
- Topics: `raw-sensor-data`, `transformed-sensor-data`
- Retention: 1 hour (AP-optimized)
- Ports: Kafka 9092, Admin 31964
- Scaling: Manual (1 or 3 replicas)

**InfluxDB** (DaemonSet)
- Time-series database, one per node
- Node-local storage with hostPath
- 7-day retention for edge ML inference
- Port: 31086
- Scaling: Automatic (follows node count)

#### Application Services (Python)

**Device Registry** (FastAPI)
- REST API for device metadata
- Port: 31080
- No auto-scaling (lightweight)

**Data Ingestion** (Python)
- MQTT shared subscriptions: `$share/ingestion-group/sensors/+/data`
- AP producer: `acks=1`, fast timeouts
- Idempotent for ordering preservation
- Auto-scales: 1-30 replicas (HPA)

**Data Transformation** (Python)
- Unit conversion and semantic normalization
- LLM-extensible for future enhancements
- Consumer group-based load balancing
- Auto-scales: 1-30 replicas (HPA)

**InfluxDB Writer** (Python)
- Batched writes (1000 points or 10s)
- Writes to node-local InfluxDB (zero network hops)
- Consumer group-based load balancing
- Auto-scales: 1-10 replicas (HPA)

**Cloud Uplink** (Python)
- Bridges edge Redpanda → cloud Redpanda
- Preserves per-device ordering via partition key
- Manual commit after successful cloud write
- Auto-scales: 1-5 replicas (HPA)

### Cloud (Docker Compose)

**Cloud Redpanda**
- Receives uplinked data from edge
- Topic: `edge-sensor-data` (10 partitions)
- 24-hour retention

**Cassandra**
- Time-bucketed schema: `PRIMARY KEY ((device_id, date), timestamp DESC)`
- 90-day TTL with TimeWindowCompactionStrategy
- Fast per-device range queries for ML training

**Cassandra Writer** (Python)
- Consumes from cloud Redpanda → writes to Cassandra
- Unlogged batches grouped by partition key for efficiency
- Also maintains `device_latest` table

**Cloud API** (FastAPI)
- REST endpoints for historical queries
- Swagger UI at `/docs`
- Device readings, latest values, stats, export

**Spark Job** (Python, on-demand)
- Batch analytics + ML model training (IsolationForest)
- Reads from Cassandra, saves models to MinIO
- Run with: `docker-compose -f docker-compose.yml -f docker-compose.job.yml run spark-job`

**MinIO** (S3-compatible)
- Stores trained ML model artifacts
- Used by Spark job for model persistence

## Data Flow

```
EDGE (K3s)                                   CLOUD (Docker Compose)

IoT Devices (Simulator)
    ↓ MQTT (QoS 1, port 31883)
EMQX Broker (StatefulSet)
    ↓ Shared Subscription
Ingestion Service (HPA 2-30)
    ↓ key=device_id
Edge Redpanda: raw-sensor-data (10 partitions)
    ↓ Consumer Group
Transformation Service (HPA 2-30)
    ↓ Unit conversion, normalization
Edge Redpanda: transformed-sensor-data
    ├──→ InfluxDB Writer (HPA 2-10)       Cloud Uplink (HPA 1-5)
    │       ↓ Batched writes                    ↓ key=device_id preserved
    │    InfluxDB (7-day local)            Cloud Redpanda: edge-sensor-data
    │                                           ↓
    │                                      Cassandra Writer → Cassandra (90-day)
    │                                           ↑
    │                                      Cloud API (FastAPI)
    │                                      Spark Job → MinIO (models)
```

## Scaling Model

### Local Mode (Default)
- **EMQX**: 1 replica
- **Redpanda**: 1 replica (RF=1)
- **InfluxDB**: 1 per node (DaemonSet)
- **Services**: Auto-scale 1-30 based on load
- **Capacity**: 10K devices, 100K msg/sec

### Demo Mode
- **EMQX**: 3 replicas (clustered)
- **Redpanda**: 3 replicas (RF=3)
- **InfluxDB**: 1 per node (DaemonSet)
- **Services**: Auto-scale 1-30 based on load
- **Capacity**: 100K+ devices, 1M+ msg/sec

## Key Features

- ✅ **Auto-scaling**: Python services scale 1-30 replicas (HPA)
- ✅ **Fault tolerance**: Survives pod, node, network failures
- ✅ **High availability**: Demo mode provides 3-node cluster
- ✅ **Ordering preserved**: Per-device ordering via partition key + idempotence
- ✅ **AP-optimized**: 3-4x faster, occasional loss acceptable
- ✅ **Node-local storage**: Zero network hops for InfluxDB writes
- ✅ **Easy scaling**: `demo-mode.sh` / `local-mode.sh` scripts

## Development Phases

- **Phase 1** (Complete): K3s edge gateway with auto-scaling ✅
- **Phase 2** (Complete): Cloud integration (Redpanda, Cassandra, API, Spark) ✅
- **Phase 3** (Planned): Edge ML inference for real-time anomaly detection

## Quick Commands

```bash
# Start cloud
cd cloud && ./start-cloud.sh

# Deploy edge
cd edge && ./k3s-build-images.sh && ./k3s-deploy.sh

# Run simulator
cd simulator && ./run-simulator.sh

# Monitor edge
sudo k3s kubectl get pods -n iot-edge --watch
sudo k3s kubectl get hpa -n iot-edge

# Monitor cloud
docker-compose -f cloud/docker-compose.yml logs -f cassandra-writer

# Query cloud API
curl http://localhost:8000/api/v1/devices

# Run ML training
cd cloud && docker-compose -f docker-compose.yml -f docker-compose.job.yml run spark-job

# Scale to demo mode
cd edge && ./demo-mode.sh

# Undeploy
cd edge && ./k3s-undeploy.sh
cd cloud && ./stop-cloud.sh
```

## Documentation

- **QUICKSTART.md**: Quick start guide
- **SETUP.md**: Detailed setup, configuration, troubleshooting
- **OPTIMIZATION_SUMMARY.md**: Architecture decisions, trade-offs
- **edge/README.md**: Edge gateway details
- **docs/DATA_TRANSFORMATION.md**: Transformation layer
- **docs/LLM_INTEGRATION.md**: LLM extensibility

---

**Production-ready K3s deployment for distributed IoT gateways!** 🚀
