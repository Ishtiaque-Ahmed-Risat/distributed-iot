# IoT Device Simulator

Simulates multiple IoT devices sending sensor data via MQTT.

## Features

- Simulates multiple devices with various sensor types (temperature, humidity, pressure)
- Configurable number of devices and sampling rate
- Automatic anomaly injection for testing ML models
- MQTT QoS 1 for reliable delivery

## Configuration

Edit `config.yaml` to adjust:
- MQTT broker address
- Number of devices to simulate
- Sampling interval
- Sensor parameters and anomaly rates

## Usage

```bash
# Run with script (recommended)
./run-simulator.sh

# Or run directly
python3 device-simulator.py
```

## Scaling

To simulate more devices, edit `config.yaml`:
```yaml
simulation:
  num_devices: 100  # Scale to 1000+ for load testing
  interval_seconds: 10
```

## MQTT Topics

Data is published to: `sensors/{device_id}/data`

Example: `sensors/device_0001/data`

## Message Format

```json
{
  "device_id": "device_0001",
  "sensor_type": "temperature",
  "value": 22.5,
  "unit": "celsius",
  "timestamp": 1707321600,
  "is_anomaly": false
}
```
