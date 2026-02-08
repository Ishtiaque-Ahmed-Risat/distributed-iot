# Setup Guide

Complete setup guide for the Distributed IoT Gateway System.

## Prerequisites

### Required Software

1. **Docker** (20.10+)
   ```bash
   docker --version
   ```

2. **Docker Compose** (2.0+)
   ```bash
   docker-compose --version
   ```

3. **Python** (3.11+)
   ```bash
   python3 --version
   ```

4. **Go** (1.22+) - for building services
   ```bash
   go version
   ```

### System Requirements

**For Development:**
- CPU: 4+ cores
- RAM: 8 GB minimum, 16 GB recommended
- Storage: 20 GB free space
- OS: Linux, macOS, or Windows with WSL2

**For Production:**
- Edge: 4-8 cores, 8-16 GB RAM per gateway
- Cloud: Scalable based on load

## Quick Start (Development)

### Step 1: Start Edge Gateway

```bash
cd edge
./start-edge.sh
```

This starts:
- EMQX (MQTT Broker) - Port 1883, Dashboard: 18083
- Redpanda (Message Queue) - Port 19092
- InfluxDB (Time-Series DB) - Port 8086
- Data Ingestion Service
- InfluxDB Writer Service

Wait for all services to be healthy (~30 seconds).

### Step 2: Start Device Simulator

```bash
cd simulator
./run-simulator.sh
```

This simulates 10 IoT devices publishing sensor data every 10 seconds.

### Step 3: Verify Data Flow

**Check EMQX Dashboard:**
- URL: http://localhost:18083
- Login: admin / public
- Go to "Monitoring" → "Topics" to see `sensors/device_XXXX/data`

**Check InfluxDB:**
```bash
# List buckets
docker exec edge-influxdb influx bucket list

# Query data
docker exec edge-influxdb influx query 'from(bucket:"sensor-data") |> range(start:-5m) |> limit(n:10)'
```

**Check Redpanda:**
```bash
# List topics
docker exec edge-redpanda rpk topic list

# Consume messages
docker exec edge-redpanda rpk topic consume raw-sensor-data --num 10
```

**View Service Logs:**
```bash
cd edge
docker-compose logs -f ingestion-service
docker-compose logs -f influxdb-writer
```

### Step 4: Start Cloud Services (Optional for Phase 1)

```bash
cd cloud
./start-cloud.sh
```

This starts:
- Redpanda (Cloud Message Bus) - Port 29092
- Cassandra (Cloud Database) - Port 9042
- MinIO (Data Lake) - Port 9000/9001

## Configuration

### Simulator Configuration

Edit `simulator/config.yaml`:
```yaml
simulation:
  num_devices: 100        # Scale up to 1000+
  interval_seconds: 5     # Faster sampling
```

### Edge Services

Edit `edge/docker-compose.yml`:
- Scale replicas (uncomment `deploy.replicas`)
- Adjust resource limits
- Modify retention policies

### Environment Variables

Services use environment variables (see docker-compose.yml):
- `MQTT_BROKER`: MQTT broker address
- `REDPANDA_BROKERS`: Redpanda broker list
- `INFLUX_URL`: InfluxDB URL
- etc.

## Monitoring

### EMQX Dashboard
- URL: http://localhost:18083
- Monitor connected clients, topics, message rates

### InfluxDB UI
- URL: http://localhost:8086
- Username: admin / adminpassword
- Query and visualize sensor data

### Redpanda Admin API
```bash
# Cluster info
curl http://localhost:9644/v1/cluster/health

# Topics
docker exec edge-redpanda rpk topic list
```

### Service Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f ingestion-service
```

## Troubleshooting

### Services not starting

```bash
# Check Docker
docker info

# Check logs
cd edge
docker-compose logs

# Restart
docker-compose restart
```

### Port conflicts

If ports are already in use:
1. Stop conflicting services
2. Or edit ports in `docker-compose.yml`

### Simulator can't connect

1. Ensure edge gateway is running: `cd edge && docker-compose ps`
2. Check EMQX is healthy: `docker exec edge-emqx emqx ping`
3. Check MQTT port: `nc -zv localhost 1883`

### No data in InfluxDB

1. Check if simulator is running and sending data
2. Check ingestion service logs: `docker-compose logs ingestion-service`
3. Check InfluxDB writer logs: `docker-compose logs influxdb-writer`
4. Verify Redpanda has messages: `docker exec edge-redpanda rpk topic consume raw-sensor-data --num 1`

### Reset everything

```bash
# Stop and remove all containers and volumes
cd edge
docker-compose down -v

cd ../cloud
docker-compose down -v

# Restart
cd ../edge
./start-edge.sh
```

## Development

### Building Services Locally

```bash
# Build Go services
cd edge/services/ingestion
go build

cd ../influxdb-writer
go build
```

### Running Services Locally (without Docker)

```bash
# Set environment variables
export MQTT_BROKER=tcp://localhost:1883
export REDPANDA_BROKERS=localhost:19092
export INFLUX_URL=http://localhost:8086
export INFLUX_TOKEN=my-super-secret-token

# Run service
./ingestion-service
```

### Adding New Services

1. Create service directory: `edge/services/my-service/`
2. Add Dockerfile
3. Add to `docker-compose.yml`
4. Build and run

## Scaling

### Edge Gateway Cluster

To run multiple gateway nodes (K3s):
1. See `edge/deployments/` for Kubernetes manifests
2. Deploy with: `kubectl apply -f edge/deployments/`

### Increase Device Count

Edit `simulator/config.yaml`:
```yaml
simulation:
  num_devices: 1000  # More devices
  interval_seconds: 10
```

### Service Replicas

Edit `docker-compose.yml`:
```yaml
ingestion-service:
  deploy:
    replicas: 3  # Run 3 instances
```

Or use CLI:
```bash
docker-compose up -d --scale ingestion-service=3
```

## Next Steps

After Phase 1 is working:
- **Phase 2**: Add edge ML inference, aggregation services
- **Phase 3**: Add cloud integration, Spark analytics, ML training

See project README for full roadmap.

## Support

- Documentation: `docs/`
- Issues: Check logs and troubleshooting section
- Architecture: See main `README.md`
