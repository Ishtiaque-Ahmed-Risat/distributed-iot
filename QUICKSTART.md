1# Quick Start Guide

Get the IoT system running in 3 steps!

## Prerequisites Check

```bash
# Check Docker
docker --version
# Expected: Docker version 20.10+

# Check Docker Compose
docker-compose --version
# Expected: Docker Compose version 2.0+

# Check Python
python3 --version
# Expected: Python 3.11+
```

If any are missing, see [SETUP.md](SETUP.md) for installation instructions.

---

## Step 1: Start Edge Gateway (30 seconds)

```bash
cd edge
./start-edge.sh
```

**Expected Output:**
```
Edge Gateway Started Successfully!

EMQX Dashboard:  http://localhost:18083
  Username: admin
  Password: public

InfluxDB UI:     http://localhost:8086
  Username: admin
  Password: adminpassword
```

**Verify:**
- Open http://localhost:18083 (EMQX Dashboard)
- Login with `admin` / `public`
- You should see the dashboard

---

## Step 2: Start Device Simulator (30 seconds)

Open a **new terminal** window:

```bash
cd simulator
./run-simulator.sh
```

**Expected Output:**
```
Starting IoT device simulator...
Created 30 sensors across 10 devices
Published 30 readings in 0.05s (Total: 30)
Published 30 readings in 0.04s (Total: 60)
...
```

The simulator will keep running and sending data every 10 seconds.

**Verify:**
- In EMQX Dashboard (http://localhost:18083)
- Go to **Monitoring** → **Topics**
- You should see topics like `sensors/device_0001/data` with message counts

---

## Step 3: Verify Data Flow (2 minutes)

Open **another terminal** window:

```bash
# Check if data is in InfluxDB
docker exec edge-influxdb influx query \
  'from(bucket:"sensor-data") |> range(start:-5m) |> limit(n:10)'
```

**Expected Output:**
```
Result: _result
Table: keys: [_start, _stop, _field, _measurement, device_id, sensor_type, unit]
_time                      _value
-----                      ------
2024-02-07T10:15:00Z       22.5
2024-02-07T10:15:10Z       23.1
...
```

If you see data, **congratulations!** Your IoT system is working! 🎉

---

## Monitoring the System

### View Service Logs

```bash
# All services
cd edge
docker-compose logs -f

# Specific service
docker-compose logs -f ingestion-service
docker-compose logs -f influxdb-writer
```

### EMQX Dashboard
- URL: http://localhost:18083
- Login: `admin` / `public`
- Monitor:
  - Connected devices
  - Message rates
  - Topics and subscriptions

### InfluxDB UI
- URL: http://localhost:8086
- Login: `admin` / `adminpassword`
- Query sensor data:
  ```flux
  from(bucket: "sensor-data")
    |> range(start: -1h)
    |> filter(fn: (r) => r._measurement == "sensor_data")
  ```

### Redpanda Topics

```bash
# List topics
docker exec edge-redpanda rpk topic list

# Consume messages (see live data)
docker exec edge-redpanda rpk topic consume raw-sensor-data --num 10
```

---

## Stopping the System

### Stop Simulator
In the simulator terminal, press `Ctrl+C`

### Stop Edge Gateway
```bash
cd edge
./stop-edge.sh
```

---

## Troubleshooting

### Simulator can't connect to MQTT

**Check if edge is running:**
```bash
cd edge
docker-compose ps
```

All services should show "Up" status.

**Restart edge:**
```bash
cd edge
docker-compose restart
```

### No data in InfluxDB

**Check ingestion service:**
```bash
cd edge
docker-compose logs ingestion-service
```

You should see:
```
[INFO] Connected to MQTT broker
[INFO] Subscribed to topic: sensors/+/data
[INFO] Processed 100 messages
```

**Check InfluxDB writer:**
```bash
docker-compose logs influxdb-writer
```

You should see:
```
[INFO] Wrote 1000 points to InfluxDB
```

### Port already in use

If you see "port already allocated":

1. Find what's using the port:
   ```bash
   sudo lsof -i :1883  # MQTT port
   sudo lsof -i :8086  # InfluxDB port
   ```

2. Stop that service or change ports in `docker-compose.yml`

---

## Next Steps

### Scale Up Devices

Edit `simulator/config.yaml`:
```yaml
simulation:
  num_devices: 100  # More devices
  interval_seconds: 5  # Faster sampling
```

Restart simulator.

### Scale Up Services

Edit `edge/docker-compose.yml`:
```yaml
ingestion-service:
  # Uncomment these lines:
  deploy:
    replicas: 3
```

Restart edge:
```bash
cd edge
docker-compose up -d --scale ingestion-service=3
```

### Start Cloud Services

```bash
cd cloud
./start-cloud.sh
```

Access MinIO: http://localhost:9001 (`minioadmin` / `minioadmin`)

### Run Integration Tests

```bash
./test-system.sh
```

### Add More Features

See [README.md](README.md) for Phase 2 and Phase 3 features:
- Edge ML inference
- Edge aggregation
- Cloud synchronization
- Spark analytics
- ML training pipeline

---

## Common Commands Cheat Sheet

```bash
# Start everything
cd edge && ./start-edge.sh
cd simulator && ./run-simulator.sh

# Check status
cd edge && docker-compose ps

# View logs
cd edge && docker-compose logs -f [service-name]

# Restart a service
cd edge && docker-compose restart [service-name]

# Stop everything
cd edge && ./stop-edge.sh

# Clean everything (removes data!)
cd edge && docker-compose down -v

# Test system
./test-system.sh
```

---

## Getting Help

1. Check logs: `docker-compose logs [service]`
2. See [SETUP.md](SETUP.md) for detailed troubleshooting
3. Check service health: `docker-compose ps`
4. Verify connectivity: `nc -zv localhost 1883` (MQTT)

---

## Architecture Diagram

```
┌─────────────────────────────────────────────┐
│         IoT Devices (Simulator)             │
│  device_0001, device_0002, ... device_0010  │
└──────────────────┬──────────────────────────┘
                   │ MQTT (QoS 1)
                   ▼
┌─────────────────────────────────────────────┐
│              EMQX Broker                    │
│         (Port 1883, Dashboard 18083)        │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│        Data Ingestion Service (Go)          │
│           MQTT → Redpanda                   │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│         Redpanda (Message Queue)            │
│      Topic: raw-sensor-data                 │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│       InfluxDB Writer Service (Go)          │
│          Redpanda → InfluxDB                │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│     InfluxDB (Time-Series Database)         │
│       7-day retention, Port 8086            │
└─────────────────────────────────────────────┘
```

---

**You're all set!** 🚀

Your distributed IoT gateway is now collecting and processing sensor data from simulated devices.
