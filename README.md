# Distributed 5G IoT Gateway System

## Architecture

![System Architecture](docs/architecture.png)


## Prerequisites

- **K3s** (lightweight Kubernetes for edge)
- **Docker** and **docker-compose**
- **Python 3.11+**

## Scripts Overview

**Edge scripts** (in `edge/`):
- `k3s-build-images.sh` - Build Docker images
- `k3s-deploy-infra.sh` - Deploy infrastructure (EMQX, Redpanda, InfluxDB)
- `k3s-deploy-services.sh` - Deploy application services
- `k3s-undeploy.sh` - Remove all resources

**Cloud scripts** (in `cloud/`):
- `start-cloud.sh` - Start cloud services
- `stop-cloud.sh` - Stop cloud services

## Install K3s

```bash
curl -sfL https://get.k3s.io | sh -
sudo k3s kubectl get nodes
```

## Deploy Edge

```bash
cd edge
./k3s-build-images.sh
./k3s-deploy-infra.sh
./k3s-deploy-services.sh
```

Verify: `sudo k3s kubectl get pods -n iot-edge`

## Deploy Cloud

```bash
cd cloud
./start-cloud.sh
```

Verify: Open http://localhost:8000/docs

## Start Simulator

Starts publishing data from simulated IoT devices, which will propagate through the edge and cloud systems.

```bash
cd simulator
./run-simulator.sh
```

## Verify Data Flow

**Query Cassandra:**
```bash
docker exec -it cloud-cassandra cqlsh -e "SELECT * FROM iot_data.device_latest LIMIT 10;"
```

## Run Spark Job

Train an anomaly detection model on historical data:

```bash
cd cloud
docker-compose -f docker-compose.yml -f docker-compose.job.yml run --rm spark-job
```

This analyzes the last 24 hours of data, trains an Isolation Forest model, and saves it to MinIO.

## Accessing Services

### Edge Services

| Service | URL | Credentials |
|---------|-----|-------------|
| EMQX Dashboard | http://localhost:31803 | admin / public |
| InfluxDB UI | http://localhost:31086 | admin / adminpassword |
| Redpanda Admin | http://localhost:31964 | — |

### Cloud Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Cloud API (Swagger) | http://localhost:8000/docs | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Cloud Redpanda Admin | http://localhost:29644 | — |

## Stopping the System

**Stop Simulator:**
Press `Ctrl+C` in the simulator terminal

**Stop Edge:**
```bash
cd edge
./k3s-undeploy.sh
```

**Stop Cloud:**
```bash
cd cloud
./stop-cloud.sh
```
