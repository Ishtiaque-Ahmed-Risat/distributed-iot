# Device Registry - Quick Start

## 🚀 5-Minute Guide

### Start Service

```bash
# Option 1: With entire edge system
cd edge && ./start-edge.sh

# Option 2: Standalone
cd edge/services/device-registry
python main.py
```

### Access Interactive Docs

```
http://localhost:8080/docs
```

Click **"Try it out"** on any endpoint and test it live!

---

## 📝 Common Tasks

### 1. Register a Device

**Interactive (Easiest)**:
1. Go to http://localhost:8080/docs
2. Click `POST /api/v1/devices`
3. Click "Try it out"
4. Fill the form
5. Click "Execute"

**Command Line**:
```bash
curl -X POST http://localhost:8080/api/v1/devices \
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

requests.post("http://localhost:8080/api/v1/devices", json={
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
curl http://localhost:8080/api/v1/devices | jq

# With filters
curl "http://localhost:8080/api/v1/devices?device_type=temperature_sensor" | jq
curl "http://localhost:8080/api/v1/devices?status=active" | jq
```

### 3. Get Device Info

```bash
curl http://localhost:8080/api/v1/devices/sensor_001 | jq
```

### 4. Update Last Seen (Heartbeat)

```bash
curl -X POST http://localhost:8080/api/v1/devices/sensor_001/heartbeat
```

### 5. Get Device Sensors

```bash
curl http://localhost:8080/api/v1/devices/sensor_001/sensors | jq
```

### 6. Health Check

```bash
curl http://localhost:8080/health | jq
```

---

## 🧪 Test the Service

```bash
cd edge/services/device-registry
python test_api.py
```

**Expected output**: ✅ ALL TESTS PASSED!

---

## 🐛 Troubleshooting

### Service won't start

```bash
# Check if port 8080 is in use
lsof -i :8080

# Use different port
PORT=8081 python main.py
```

### Dependencies missing

```bash
pip install -r requirements.txt
```

### Docker build fails

```bash
cd edge/services/device-registry
docker build -t device-registry:test .
docker run -p 8080:8080 device-registry:test
```

### Test API connection

```bash
curl http://localhost:8080/health

# Expected response:
# {"status":"healthy","devices":0}
```

---

## 📚 Documentation

- **Swagger UI**: http://localhost:8080/docs (interactive!)
- **ReDoc**: http://localhost:8080/redoc (clean docs)
- **Full README**: `README.md`
- **Conversion Guide**: `/DEVICE_REGISTRY_CONVERSION.md`

---

## 🎓 API Examples

### Complex Registration

```json
{
  "device_id": "freezer_monitor_001",
  "device_type": "industrial_freezer",
  "description": "Critical cold chain monitoring for pharmaceutical storage (-80°C to -60°C). Alerts if temperature exceeds -65°C for more than 5 minutes.",
  "location": "Warehouse B, Cold Storage Room 3, Unit A5",
  "metadata": {
    "building": "Warehouse B",
    "floor": "Basement",
    "room": "Cold Storage 3",
    "unit": "A5",
    "critical": "true",
    "alert_email": "ops@example.com",
    "manufacturer": "FreezerTech Inc",
    "model": "ULT-8000",
    "serial": "FT2024-12345",
    "install_date": "2024-01-15",
    "service_interval_days": "90"
  },
  "sensors": [
    {
      "sensor_type": "temperature",
      "unit": "celsius",
      "min_value": -85,
      "max_value": -55,
      "description": "Primary temperature probe (internal chamber)"
    },
    {
      "sensor_type": "temperature",
      "unit": "celsius",
      "min_value": -85,
      "max_value": -55,
      "description": "Backup temperature probe (redundancy)"
    },
    {
      "sensor_type": "door_status",
      "unit": "boolean",
      "description": "Door open/close sensor (security)"
    }
  ]
}
```

### Multi-Sensor Device

```json
{
  "device_id": "env_station_roof",
  "device_type": "weather_station",
  "description": "Rooftop environmental monitoring station",
  "location": "Building A, Rooftop, North Corner",
  "sensors": [
    {"sensor_type": "temperature", "unit": "celsius", "min_value": -20, "max_value": 50},
    {"sensor_type": "humidity", "unit": "percent", "min_value": 0, "max_value": 100},
    {"sensor_type": "pressure", "unit": "hPa", "min_value": 950, "max_value": 1050},
    {"sensor_type": "wind_speed", "unit": "km/h", "min_value": 0, "max_value": 200},
    {"sensor_type": "rainfall", "unit": "mm", "min_value": 0, "max_value": 500}
  ]
}
```

---

## ⚡ Pro Tips

1. **Use Swagger UI for development** - fastest way to test
2. **Add detailed descriptions** - helps LLM integration later
3. **Use metadata liberally** - store anything useful
4. **Set realistic min/max values** - helps anomaly detection
5. **Keep device_id consistent** - use same format everywhere

---

## 🔗 Integration with Other Services

### Simulator Auto-Registration

Devices automatically register when simulator starts:

```yaml
# simulator/config.yaml
device_registry:
  enabled: true
  url: "http://localhost:8080"
```

### Transformation Service Queries

Transformation service queries device context:

```go
// In transformation service
deviceInfo := queryDeviceRegistry(deviceID)
// Uses description for context-aware conversion
```

---

## 📊 Monitor Service

### Watch Logs

```bash
# Docker
docker logs -f edge-device-registry

# Local
tail -f device_registry.log
```

### Check Health

```bash
watch -n 5 'curl -s http://localhost:8080/health | jq'
```

### Count Devices

```bash
curl -s http://localhost:8080/api/v1/devices | jq '.count'
```

---

## 🎯 Next Steps

1. **Start the service** ✅
2. **Visit http://localhost:8080/docs** ✅
3. **Register a test device** ✅
4. **Run the simulator** ✅
5. **Watch devices appear** ✅

**You're ready to go!** 🚀
