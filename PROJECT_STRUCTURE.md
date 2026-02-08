# Project Structure

```
iot_system/
├── README.md                    # Main project documentation
├── SETUP.md                     # Complete setup guide
├── PROJECT_STRUCTURE.md         # This file
├── test-system.sh              # Integration test script
├── .gitignore                  # Git ignore rules
│
├── simulator/                   # IoT Device Simulator
│   ├── device-simulator.py     # Main simulator (Python)
│   ├── config.yaml             # Simulator configuration
│   ├── requirements.txt        # Python dependencies
│   ├── run-simulator.sh        # Startup script
│   └── README.md               # Simulator documentation
│
├── edge/                        # Edge Gateway Services
│   ├── docker-compose.yml      # Edge infrastructure & services
│   ├── start-edge.sh           # Single script to start edge
│   ├── stop-edge.sh            # Single script to stop edge
│   ├── README.md               # Edge documentation
│   │
│   └── services/               # Edge microservices
│       ├── ingestion/          # Data Ingestion Service (Go)
│       │   ├── main.go         # MQTT → Redpanda
│       │   ├── go.mod
│       │   ├── go.sum
│       │   ├── Dockerfile
│       │   └── README.md
│       │
│       └── influxdb-writer/    # InfluxDB Writer Service (Go)
│           ├── main.go         # Redpanda → InfluxDB
│           ├── go.mod
│           ├── go.sum
│           ├── Dockerfile
│           └── README.md
│
└── cloud/                       # Cloud Services
    ├── docker-compose.yml      # Cloud infrastructure
    ├── start-cloud.sh          # Single script to start cloud
    ├── stop-cloud.sh           # Single script to stop cloud
    ├── README.md               # Cloud documentation
    │
    └── services/               # Cloud microservices (Phase 2-3)
        ├── api-gateway/        # (To be added)
        ├── data-writer/        # (To be added)
        ├── ml-training/        # (To be added)
        └── device-mgmt/        # (To be added)
```

## Component Overview

### Simulator
- **Purpose**: Simulate thousands of IoT devices
- **Language**: Python
- **Outputs**: MQTT messages to edge gateway
- **Configurable**: Number of devices, sampling rate, sensor types

### Edge Gateway

#### Infrastructure (Docker Compose)
- **EMQX**: MQTT broker (port 1883, dashboard 18083)
- **Redpanda**: Message queue (port 19092)
- **InfluxDB**: Time-series database (port 8086)

#### Services
1. **Data Ingestion Service** (Go)
   - Subscribes to MQTT topics
   - Validates sensor data
   - Publishes to Redpanda
   - Replica: 1 (configurable to 3+)

2. **InfluxDB Writer Service** (Go)
   - Consumes from Redpanda
   - Batch writes to InfluxDB
   - 7-day retention policy
   - Replica: 1 (configurable to 2+)

### Cloud (Phase 1 - Basic Infrastructure)

#### Infrastructure (Docker Compose)
- **Redpanda**: Central message bus (port 29092)
- **Cassandra**: Time-series database (port 9042)
- **MinIO**: S3-compatible data lake (port 9000/9001)

#### Services (Phase 2-3)
- API Gateway (Go) - To be added
- Data Writer (Go) - To be added
- ML Training (Python) - To be added
- Spark Analytics - To be added

## Data Flow (Phase 1)

```
IoT Devices
    ↓ MQTT (QoS 1)
EMQX Broker
    ↓ Subscribe
Data Ingestion Service (Go)
    ↓ Produce
Redpanda (Message Queue)
    ↓ Consume
InfluxDB Writer Service (Go)
    ↓ Write
InfluxDB (7-day buffer)
```

## Configuration Files

| File | Purpose |
|------|---------|
| `simulator/config.yaml` | Device simulator settings |
| `edge/docker-compose.yml` | Edge services configuration |
| `cloud/docker-compose.yml` | Cloud services configuration |
| `edge/services/*/Dockerfile` | Service container definitions |

## Scripts

| Script | Purpose |
|--------|---------|
| `simulator/run-simulator.sh` | Start IoT device simulator |
| `edge/start-edge.sh` | Start edge gateway (one command) |
| `edge/stop-edge.sh` | Stop edge gateway |
| `cloud/start-cloud.sh` | Start cloud services (one command) |
| `cloud/stop-cloud.sh` | Stop cloud services |
| `test-system.sh` | Run integration tests |

## Volumes (Persistent Data)

### Edge
- `emqx-data`: MQTT broker session data
- `redpanda-data`: Message queue persistence
- `influxdb-data`: Time-series data (7 days)

### Cloud
- `cloud-redpanda-data`: Cloud message queue
- `cassandra-data`: Cloud database
- `minio-data`: Object storage / data lake

## Ports

### Edge Gateway
- `1883`: MQTT (devices connect here)
- `18083`: EMQX Dashboard
- `19092`: Redpanda Kafka API
- `8086`: InfluxDB API & UI
- `9644`: Redpanda Admin API

### Cloud Services
- `29092`: Cloud Redpanda Kafka API
- `9042`: Cassandra CQL
- `9000`: MinIO S3 API
- `9001`: MinIO Console

Note: Cloud ports offset by +10000 to avoid conflicts during dev.

## Development Workflow

1. **Start Edge**
   ```bash
   cd edge && ./start-edge.sh
   ```

2. **Start Simulator**
   ```bash
   cd simulator && ./run-simulator.sh
   ```

3. **Monitor**
   ```bash
   cd edge && docker-compose logs -f
   ```

4. **Test**
   ```bash
   ./test-system.sh
   ```

5. **Start Cloud** (optional)
   ```bash
   cd cloud && ./start-cloud.sh
   ```

## Scaling Configuration

All services have commented scaling options in `docker-compose.yml`:

```yaml
# Uncomment to enable:
# deploy:
#   replicas: 3
#   resources:
#     limits:
#       cpus: '1'
#       memory: 1G
```

## Next Phases

### Phase 2 (Weeks 3-4)
- Add Edge Aggregation Service (Go)
- Add ML Inference Service (Python)
- Add Cloud Uplink Service (Go)
- K3s deployment manifests

### Phase 3 (Weeks 5-6)
- Add Cloud Data Writer (Go)
- Add API Gateway (Go)
- Add ML Training Service (Python)
- Add Spark cluster
- Full edge-cloud integration

## Design Principles

✅ **Modular**: Each service is independent  
✅ **Containerized**: Everything runs in Docker  
✅ **Minimal**: Only essential components  
✅ **Scalable**: Replica settings ready for production  
✅ **Observable**: Logs, metrics, dashboards  
✅ **Fault Tolerant**: Message queues, retries, persistence  

## Technology Stack

| Layer | Technology | Language | Purpose |
|-------|-----------|----------|---------|
| Simulator | Python | Python | IoT device simulation |
| MQTT Broker | EMQX | Erlang | Device connectivity |
| Message Queue | Redpanda | C++ | Fault-tolerant messaging |
| Time-Series DB | InfluxDB | Go | Edge data buffering |
| Data Ingestion | Custom | Go | High-throughput processing |
| InfluxDB Writer | Custom | Go | Batch database writes |
| Cloud DB | Cassandra | Java | Scalable time-series storage |
| Data Lake | MinIO | Go | S3-compatible object storage |
| Orchestration | K3s/K8s | Go | Container orchestration |
