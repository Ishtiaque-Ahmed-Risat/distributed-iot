# Cloud Infrastructure

Cloud services for long-term storage, batch analytics, ML training, and REST API.  
Deployed via **Docker Compose** on the cloud/datacenter side.

## Architecture

```
Edge (K3s)                              Cloud (Docker Compose)
─────────────                           ──────────────────────
Edge Redpanda                           Cloud Redpanda
  └─ Cloud Uplink ──────────────────→     └─ Cassandra Writer ──→ Cassandra
                                                                    ↑
                                          Cloud API (FastAPI) ──────┘
                                          Spark Job (batch) ────────┘
                                          MinIO (model store)
```

## Quick Start

```bash
# 1. Start cloud services
cd cloud
./start-cloud.sh

# 2. Build & deploy edge (including cloud-uplink)
cd ../edge
./k3s-build-images.sh
./k3s-deploy.sh

# 3. Run the simulator
cd ../simulator
./run-simulator.sh

# 4. Verify data flow
curl http://localhost:8000/api/v1/devices           # List devices
curl http://localhost:8000/api/v1/devices/device_0000/latest  # Latest readings

# 5. Run ML training job (on demand)
cd ../cloud
docker-compose -f docker-compose.yml -f docker-compose.job.yml run spark-job
```

## Components

| Service | Port | Description |
|---------|------|-------------|
| **Redpanda** | `29092` (Kafka), `29644` (Admin) | Message bus receiving edge data |
| **Cassandra** | `9042` | Long-term time-series storage (90-day retention) |
| **MinIO** | `9000` (API), `9001` (Console) | S3-compatible data lake for ML models |
| **Cassandra Writer** | — | Consumes `edge-sensor-data` → writes to Cassandra |
| **Cloud API** | `8000` | REST API for querying historical data ([Swagger UI](http://localhost:8000/docs)) |
| **Spark Job** | — | On-demand batch analytics + ML model training |

## Cassandra Schema

Time-bucketed design for fast per-device queries:

```
sensor_readings:  PRIMARY KEY ((device_id, date), timestamp DESC)
device_latest:    PRIMARY KEY (device_id, sensor_type)
device_stats:     PRIMARY KEY ((device_id, date), sensor_type, hour)
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/api/v1/devices` | List all device IDs |
| GET | `/api/v1/devices/{id}/readings` | Historical readings (filterable) |
| GET | `/api/v1/devices/{id}/latest` | Latest reading per sensor |
| GET | `/api/v1/devices/{id}/stats` | Aggregated statistics |
| GET | `/api/v1/export/{id}?date=YYYY-MM-DD` | Export full day of data |

## Data Ordering Guarantee

Per-device ordering is preserved end-to-end:
1. Edge Redpanda: `key=device_id` → same partition per device
2. Cloud Uplink: same key preserved in cloud Redpanda
3. Cloud Redpanda: `key=device_id` → same partition
4. Cassandra Writer: single consumer per partition → sequential writes
5. Cassandra: `CLUSTERING ORDER BY (timestamp DESC)` → sorted on read

## Useful Commands

```bash
# View logs
docker-compose logs -f cassandra-writer
docker-compose logs -f cloud-api

# Check Cassandra data
docker exec -it cloud-cassandra cqlsh -e "SELECT * FROM iot_data.device_latest LIMIT 10;"

# Check Redpanda topics
docker exec -it cloud-redpanda rpk topic list

# Stop cloud
./stop-cloud.sh

# Stop + remove all data
docker-compose down -v
```
