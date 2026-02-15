# InfluxDB Writer Service

Consumes transformed sensor data from Redpanda and writes to node-local InfluxDB with batching.

**Language**: Python  
**Framework**: kafka-python, influxdb-client  
**Deployment**: K3s Deployment with HPA (1-10 replicas)

## Functionality

- Consumes from: `transformed-sensor-data`
- Batched writes to InfluxDB (1000 points or 10s interval)
- Writes to node-local InfluxDB (zero network hops via DaemonSet)
- Consumer group: `influxdb-writer` (automatic load balancing)
- Manual commit after successful write

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDPANDA_BROKERS` | `redpanda:9092` | Redpanda broker addresses |
| `REDPANDA_TOPIC` | `transformed-sensor-data` | Input topic |
| `CONSUMER_GROUP` | `influxdb-writer` | Consumer group ID |
| `INFLUX_URL` | `http://influxdb:8086` | InfluxDB URL (routes to node-local instance) |
| `INFLUX_TOKEN` | - | InfluxDB auth token |
| `INFLUX_ORG` | `iot-org` | InfluxDB organization |
| `INFLUX_BUCKET` | `sensor-data` | InfluxDB bucket |
| `BATCH_SIZE` | `1000` | Write batch size |
| `FLUSH_INTERVAL` | `10` | Flush interval (seconds) |

## Building for K3s

```bash
cd ..
./k3s-build-images.sh
```

## Running Locally

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

export REDPANDA_BROKERS=localhost:19092
export REDPANDA_TOPIC=transformed-sensor-data
export INFLUX_URL=http://localhost:8086
export INFLUX_TOKEN=my-super-secret-token
export INFLUX_ORG=iot-org
export INFLUX_BUCKET=sensor-data

python main.py
```

## Deployment

```bash
cd ../../
./k3s-deploy.sh

sudo k3s kubectl get pods -n iot-edge -l app=influxdb-writer
sudo k3s kubectl logs -f deployment/influxdb-writer -n iot-edge
sudo k3s kubectl get hpa influxdb-writer-hpa -n iot-edge
```

## Scaling

Auto-scales 1-10 replicas based on CPU/Memory.
Higher replica count = higher write throughput.

```bash
watch sudo k3s kubectl get hpa -n iot-edge
```

## Performance

- **Single pod**: ~10K writes/sec
- **10 pods**: ~100K writes/sec

InfluxDB handles concurrent writers well.
