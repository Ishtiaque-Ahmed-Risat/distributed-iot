# Data Transformation Service

Unit conversion and semantic type standardization for sensor data.

**Language**: Python  
**Framework**: kafka-python

## Functionality

- **Unit Conversion**: Automatically converts units to standard formats
  - Temperature: Fahrenheit/Kelvin → Celsius
  - Pressure: PSI/Bar → hPa
  - Distance: Feet/Miles → Meters/Kilometers
  - Speed: MPH → KM/H
  
- **Semantic Standardization**: Maps sensor type variations to standard names
  - `temp`, `ambient_temp` → `temperature`
  - `rh`, `relative_humidity` → `humidity`
  - `atm_pressure` → `pressure`

- **LLM-Ready**: Hooks for future LLM-based semantic transformation

## Data Flow

```
Raw Sensor Data (Redpanda) → Transformation → Transformed Data (Redpanda)
```

Input Topic: `raw-sensor-data`  
Output Topic: `transformed-sensor-data`

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDPANDA_BROKERS` | `redpanda:9092` | Redpanda broker addresses |
| `INPUT_TOPIC` | `raw-sensor-data` | Input topic name |
| `OUTPUT_TOPIC` | `transformed-sensor-data` | Output topic name |
| `CONSUMER_GROUP` | `transformation-service` | Kafka consumer group |
| `ENABLE_LLM` | `false` | Enable LLM transformation |
| `LLM_SERVICE_URL` | `http://llm-service:8080` | LLM service endpoint |

## Conversion Rules

The service includes built-in conversion rules that are automatically applied based on unit and sensor type:

```python
# Temperature
fahrenheit → celsius: (F - 32) × 5/9
kelvin → celsius: K - 273.15

# Pressure  
psi → hpa: PSI × 68.9476
bar → hpa: Bar × 1000

# Distance
feet → meters: Feet × 0.3048
miles → kilometers: Miles × 1.60934

# Speed
mph → kmh: MPH × 1.60934
```

## Example Transformation

**Input**:
```json
{
  "device_id": "sensor_001",
  "sensor_type": "temp",
  "value": 72.5,
  "unit": "fahrenheit",
  "timestamp": 1709876543
}
```

**Output**:
```json
{
  "device_id": "sensor_001",
  "sensor_type": "temperature",
  "value": 22.5,
  "unit": "celsius",
  "original_value": 72.5,
  "original_unit": "fahrenheit",
  "timestamp": 1709876543,
  "transformed_by": "rule",
  "transformation_id": "fahrenheit->celsius"
}
```

## Building

```bash
# Build Docker image
docker build -t iot-transformation:latest .
```

## Running

```bash
# Run locally
pip install -r requirements.txt
python main.py

# Run with Docker
docker run --rm \
  -e REDPANDA_BROKERS=localhost:9092 \
  iot-transformation:latest
```

## Adding New Conversion Rules

Edit `main.py` and add to the `RuleEngine` class:

```python
def _register_conversions(self):
    # Add your conversion
    self.unit_conversions['inches->cm'] = {
        'from_unit': 'inches',
        'to_unit': 'cm',
        'convert': lambda inches: inches * 2.54,
        'description': 'Inches to Centimeters'
    }
```

## Future: LLM Integration

Set `ENABLE_LLM=true` to enable LLM-based transformations:
- Context-aware unit inference
- Semantic understanding of device descriptions
- Intelligent value validation
- Metadata enrichment

See commented code in `main.py` for implementation details.

## Scaling

```yaml
# Uncomment in docker-compose.yml
# deploy:
#   replicas: 2
```
