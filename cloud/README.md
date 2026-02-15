# Cloud Infrastructure

Cloud services for batch analytics, ML training, and long-term storage. **Note**: Cloud infrastructure is not yet implemented for K3s deployment.

## Planned Architecture

```
Edge Gateway (K3s)
      ↓ (Cloud Uplink Service - TODO)
Cloud Redpanda (Message Bus)
      ↓
Cassandra (Long-term Time-series Storage)
MinIO (Data Lake for raw data)
Spark (Batch Analytics)
ML Training Pipeline (Cloud-based model updates)
```

## Components (Planned)

### Infrastructure
- **Redpanda**: Central message bus for all edge gateways
- **Cassandra**: Distributed time-series database
- **MinIO**: S3-compatible data lake for raw sensor data
- **Spark**: Batch processing for historical analytics

### Services
- **Cloud Uplink**: Periodic sync from edge InfluxDB to cloud (every 1 minute)
- **API Gateway**: REST API for querying historical data
- **ML Training**: Train models on aggregated data, push to edge
- **Analytics Dashboard**: Grafana-based visualization

## Current Status

**Phase 1 (Completed)**: Edge gateway with K3s deployment ✅  
**Phase 2 (TODO)**: Cloud infrastructure deployment  
**Phase 3 (TODO)**: Edge-cloud integration and ML pipeline

## Future Implementation

Cloud services will be deployed on:
- **Kubernetes** (for production cloud deployment)
- **K3s** (for local testing and development)

Deployment manifests will be created in `cloud/k8s/` directory.

## See Also

- [Edge Gateway](../edge/README.md) - Currently implemented
- [Project README](../README.md) - Overall project structure
- [OPTIMIZATION_SUMMARY.md](../OPTIMIZATION_SUMMARY.md) - Edge architecture decisions
