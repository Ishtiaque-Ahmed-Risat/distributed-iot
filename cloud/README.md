# Cloud Services

Cloud infrastructure for IoT data storage, analytics, and ML training.

## Current Phase (Phase 1)

Basic cloud infrastructure:
- **Redpanda**: Central message bus
- **Cassandra**: Time-series database
- **MinIO**: S3-compatible data lake

## Future Phases

Phase 2-3 will add:
- API Gateway service (Go)
- Data Writer service (Go)
- Device Management service (Go)
- ML Training service (Python)
- Spark cluster for batch analytics

## Quick Start

```bash
# Start cloud services
./start-cloud.sh

# Stop services
./stop-cloud.sh
```

## Accessing Services

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Redpanda Admin | http://localhost:29644 | - |
| Cassandra CQL | localhost:9042 | - |

## Service Ports

- **Redpanda**: 29092 (Kafka), 28082 (HTTP), 28081 (Schema Registry)
- **Cassandra**: 9042 (CQL), 7199 (JMX)
- **MinIO**: 9000 (API), 9001 (Console)

Note: Cloud ports are offset (+10000) from edge ports to avoid conflicts during development.

## Data Flow (Planned)

```
Edge Gateway → Cloud Redpanda → Data Writer → Cassandra + MinIO
                                            → Spark (batch analytics)
                                            → ML Training
```

## Development

Services are containerized separately and will be added incrementally:
- `services/api-gateway/` (Phase 2)
- `services/data-writer/` (Phase 2)
- `services/ml-training/` (Phase 3)
- `services/spark/` (Phase 3)
