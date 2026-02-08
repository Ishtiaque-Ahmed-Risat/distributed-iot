# Edge Gateway

Edge gateway services for IoT data collection, buffering, and processing.

## Architecture

```
IoT Devices (MQTT)
      ↓
   EMQX Broker
      ↓
Data Ingestion Service (Python)
      ↓
  Redpanda (Message Queue)
      ↓  raw-sensor-data
Transformation Service (Python)
      ↓  transformed-sensor-data
InfluxDB Writer Service (Python)
      ↓
  InfluxDB (7-day buffer)
```

## Services

### Infrastructure
- **EMQX**: MQTT broker for device connectivity
- **Redpanda**: Message queue for fault-tolerant data flow
- **InfluxDB**: Time-series database for local buffering (7-day retention)

### Applications (Python)
- **Device Registry** (FastAPI): REST API for device registration with descriptions (Port 8080)
- **Data Ingestion**: Subscribes to MQTT, publishes to Redpanda
- **Data Transformation**: Unit conversion & semantic normalization (LLM-ready)
- **InfluxDB Writer**: Consumes from Redpanda, writes to InfluxDB

## Quick Start

```bash
# Start all edge services
./start-edge.sh

# View logs
docker-compose logs -f

# Stop services
./stop-edge.sh
```

## Accessing Services

| Service | URL | Credentials |
|---------|-----|-------------|
| EMQX Dashboard | http://localhost:18083 | admin / public |
| InfluxDB UI | http://localhost:8086 | admin / adminpassword |
| Redpanda Admin | http://localhost:9644 | - |

## Service Configuration

Edit `docker-compose.yml` to:
- Scale services (uncomment `deploy.replicas`)
- Adjust resource limits
- Modify retention policies
- Change ports

## Data Flow

1. **Devices register**: POST `/api/v1/devices` with descriptions
2. **IoT devices publish** to: `sensors/{device_id}/data`
3. **EMQX** receives and persists messages (QoS 1)
4. **Ingestion service** forwards to Redpanda topic: `raw-sensor-data`
5. **Transformation service** converts units & normalizes types → `transformed-sensor-data`
6. **InfluxDB writer** consumes and stores in InfluxDB
7. Data retained for 7 days in InfluxDB

**Key Feature**: Data Transformation with LLM extensibility! See `docs/LLM_INTEGRATION.md`

## Monitoring

```bash
# Service logs
docker-compose logs -f [service-name]

# EMQX metrics
curl http://localhost:18083/api/v5/metrics

# Redpanda topics
docker exec edge-redpanda rpk topic list

# InfluxDB query
docker exec edge-influxdb influx query 'from(bucket:"sensor-data") |> range(start:-1h)'
```

## Scaling

To run multiple replicas (HA mode):

1. Uncomment `deploy.replicas` in `docker-compose.yml`
2. Use `docker-compose up -d --scale ingestion-service=3`

Note: For production, deploy to K3s using manifests in `deployments/`.

## Troubleshooting

**Service not starting:**
```bash
docker-compose logs [service-name]
```

**Reset everything:**
```bash
docker-compose down -v
./start-edge.sh
```

**Check Redpanda topics:**
```bash
docker exec edge-redpanda rpk topic list
docker exec edge-redpanda rpk topic consume raw-sensor-data --num 10
```
