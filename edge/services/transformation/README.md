# Data Transformation Service

Transforms raw sensor data with unit conversion and semantic normalization. LLM-extensible for future semantic understanding.

**Language**: Python  
**Framework**: kafka-python
**Deployment**: K3s Deployment with HPA (1-30 replicas)

## Functionality

- Consumes from Redpanda topic: `raw-sensor-data`
- Unit conversion (e.g., Fahrenheit → Celsius)
- Semantic type normalization
- Publishes to: `transformed-sensor-data`
- Consumer group: `transformation-service` (automatic load balancing)
- Manual commit for reliability

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDPANDA_BROKERS` | `redpanda:9092` | Redpanda broker addresses |
| `INPUT_TOPIC` | `raw-sensor-data` | Input topic |
| `OUTPUT_TOPIC` | `transformed-sensor-data` | Output topic |
| `CONSUMER_GROUP` | `transformation-service` | Consumer group ID |
| `ENABLE_LLM` | `false` | Enable LLM-based transformation |
| `LLM_SERVICE_URL` | `http://llm-service:8080` | LLM service endpoint |

## Building for K3s

```bash
# From edge/ directory
cd ..
./k3s-build-images.sh
```

## Running Locally

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

export REDPANDA_BROKERS=localhost:19092
export INPUT_TOPIC=raw-sensor-data
export OUTPUT_TOPIC=transformed-sensor-data
export CONSUMER_GROUP=transformation-service

python main.py
```

## Deployment

```bash
cd ../../
./k3s-deploy.sh

sudo k3s kubectl get pods -n iot-edge -l app=transformation-service
sudo k3s kubectl logs -f deployment/transformation-service -n iot-edge
sudo k3s kubectl get hpa transformation-service-hpa -n iot-edge
```

## Scaling

Auto-scales 1-30 replicas based on CPU/Memory.
Each replica processes a subset of Redpanda partitions.

```bash
watch sudo k3s kubectl get hpa -n iot-edge
```

## See Also

- [DATA_TRANSFORMATION.md](../../../docs/DATA_TRANSFORMATION.md) - Transformation details
- [LLM_INTEGRATION.md](../../../docs/LLM_INTEGRATION.md) - LLM extensibility
