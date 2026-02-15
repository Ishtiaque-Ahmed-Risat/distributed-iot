# Device Registry - Quick Start (K3s)

## 🚀 5-Minute Guide

### Deploy with Edge System

```bash
cd edge
./k3s-build-images.sh
./k3s-deploy.sh
```

### Access Interactive Docs

```
http://localhost:31080/docs
```

Click **"Try it out"** on any endpoint and test it live!

**For remote access**: Replace `localhost` with K3s node IP.

---

## 📝 Common Tasks

### 1. Register a Device

**Interactive (Easiest)**:
1. Go to http://localhost:31080/docs
2. Click `POST /api/v1/devices`
3. Click "Try it out"
4. Fill the form
5. Click "Execute"

**Command Line**:
```bash
curl -X POST http://localhost:31080/api/v1/devices \
  -H "Content-Type: application/json" \
  -d '{
    "device_id": "sensor_001",
    "device_type": "temperature_sensor",
    "description": "Office temperature monitor",
    "location": "Office Room 101",
    "sensors": [
      {
        "sensor_type": "temperature",
        "unit": "celsius",
        "min_value": 15,
        "max_value": 30
      }
    ]
  }'
```

**Python**:
```python
import requests

requests.post("http://localhost:31080/api/v1/devices", json={
    "device_id": "sensor_001",
    "device_type": "temperature_sensor",
    "description": "Office temperature monitor",
    "location": "Office Room 101",
    "sensors": [{
        "sensor_type": "temperature",
        "unit": "celsius",
        "min_value": 15,
        "max_value": 30
    }]
})
```

### 2. List All Devices

```bash
curl http://localhost:31080/api/v1/devices | jq

# With filters
curl "http://localhost:31080/api/v1/devices?device_type=temperature_sensor" | jq
```

### 3. Get Device Details

```bash
curl http://localhost:31080/api/v1/devices/sensor_001 | jq
```

### 4. Update Device

```bash
curl -X PUT http://localhost:31080/api/v1/devices/sensor_001 \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Updated description",
    "location": "New location"
  }'
```

### 5. Delete Device

```bash
curl -X DELETE http://localhost:31080/api/v1/devices/sensor_001
```

---

## 🔍 Monitoring

### Check Service Status

```bash
sudo k3s kubectl get pods -n iot-edge -l app=device-registry
sudo k3s kubectl logs -f deployment/device-registry -n iot-edge
```

### Test API

```bash
# Health check
curl http://localhost:31080/health

# API info
curl http://localhost:31080/
```

---

## 🐛 Troubleshooting

### Port Not Accessible

```bash
# Check service
sudo k3s kubectl get svc device-registry -n iot-edge

# Port forward if needed
sudo k3s kubectl port-forward -n iot-edge svc/device-registry 8080:8080

# Then access on port 8080
curl http://localhost:8080/health
```

### Pod Not Running

```bash
sudo k3s kubectl describe pod -n iot-edge -l app=device-registry
sudo k3s kubectl logs -n iot-edge -l app=device-registry
```

---

## 📖 Full API Documentation

See [README.md](README.md) for complete API reference and architecture details.

Interactive docs always available at: **http://localhost:31080/docs**
