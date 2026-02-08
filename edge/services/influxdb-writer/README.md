# InfluxDB Writer Service

Consumes transformed sensor data from Redpanda and writes to InfluxDB.

**Language**: Python  
**Framework**: kafka-python, influxdb-client

## Functionality

- Consumes from Redpanda topic: `transformed-sensor-data`
- Writes to InfluxDB time-series database
- Batch writing for performance
- Stores both transformed and original values
- Automatic flush on interval or batch size

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDPANDA_BROKERS` | `redpanda:9092` | Redpanda broker addresses |
| `REDPANDA_TOPIC` | `transformed-sensor-data` | Input topic name |
| `CONSUMER_GROUP` | `influxdb-writer` | Kafka consumer group |
| `INFLUX_URL` | `http://influxdb:8086` | InfluxDB server URL |
| `INFLUX_TOKEN` | `my-super-secret-token` | InfluxDB auth token |
| `INFLUX_ORG` | `iot-org` | InfluxDB organization |
| `INFLUX_BUCKET` | `sensor-data` | InfluxDB bucket name |
| `BATCH_SIZE` | `1000` | Points per batch |
| `FLUSH_INTERVAL` | `10` | Flush interval (seconds) |

## Data Schema

The service writes data with the following structure:

**Measurement**: `sensor_data`

**Tags** (indexed):
- `device_id`: Device identifier
- `sensor_type`: Standardized sensor type
- `unit`: Standardized unit
- `original_unit`: Original unit before conversion
- `transformed_by`: Transformation method (rule/llm)
- `transformation_id`: Conversion rule applied

**Fields** (not indexed):
- `value`: Transformed value
- `original_value`: Original value before conversion
- `is_anomaly`: Boolean anomaly flag

**Timestamp**: From sensor reading

## Example Query

Query InfluxDB for sensor data:

```flux
from(bucket: "sensor-data")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_data")
  |> filter(fn: (r) => r.device_id == "sensor_001")
  |> filter(fn: (r) => r._field == "value")
```

Compare original vs transformed values:

```flux
from(bucket: "sensor-data")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "sensor_data")
  |> filter(fn: (r) => r.transformation_id =~ /.*->.*/)
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> map(fn: (r) => ({ r with diff: r.value - r.original_value }))
```

Find anomalies:

```flux
from(bucket: "sensor-data")
  |> range(start: -1d)
  |> filter(fn: (r) => r._measurement == "sensor_data")
  |> filter(fn: (r) => r._field == "is_anomaly")
  |> filter(fn: (r) => r._value == true)
```

## Building

```bash
# Build Docker image
docker build -t iot-influxdb-writer:latest .
```

## Running

```bash
# Run locally
pip install -r requirements.txt
python main.py

# Run with Docker
docker run --rm \
  -e REDPANDA_BROKERS=localhost:9092 \
  -e INFLUX_URL=http://localhost:8086 \
  -e INFLUX_TOKEN=your-token \
  iot-influxdb-writer:latest
```

## Performance Tuning

### Increase Batch Size
```bash
docker run -e BATCH_SIZE=5000 iot-influxdb-writer:latest
```

### Adjust Flush Interval
```bash
docker run -e FLUSH_INTERVAL=30 iot-influxdb-writer:latest
```

## Scaling

Run multiple replicas with different consumer groups:

```yaml
# In docker-compose.yml
deploy:
  replicas: 2
```

Each replica will consume from different partitions for parallel processing.

## Monitoring

Check service logs for write statistics:

```bash
docker logs -f edge-influxdb-writer
```

Output includes:
```
[INFO] Wrote 1000 points to InfluxDB (Total: 5000)
```

## Data Retention

The InfluxDB bucket is configured with 7-day retention by default. Adjust in docker-compose.yml:

```yaml
environment:
  - DOCKER_INFLUXDB_INIT_RETENTION=30d  # 30 days
```
