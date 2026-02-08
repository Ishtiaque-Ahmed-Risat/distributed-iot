# LLM Integration Architecture

## Overview

The system is designed with **extensibility for LLM integration** while maintaining a practical rule-based fallback. This document explains how to add LLM capabilities for semantic data transformation.

## Architecture

### Current State (Rule-Based)

```
Device Registration → Device Registry (metadata + descriptions)
                            ↓
IoT Devices → EMQX → Ingestion → Redpanda(raw)
                                      ↓
                           Transformation Service
                                (Rule Engine)
                                      ↓
                                Redpanda(transformed)
                                      ↓
                                  InfluxDB
```

### Future State (LLM-Enhanced)

```
Device Registration → Device Registry (metadata + descriptions)
           ↓                  ↓
    LLM Analysis      Context Provider
    (semantics)              ↓
           ↓          Transformation Service
           └──────────→  ├─ Rule Engine (fallback)
                         └─ LLM Engine (enhancement)
                                ↓
                         Redpanda(transformed)
```

## Use Cases for LLM Integration

### 1. Device Onboarding

**Problem**: Devices provide free-text descriptions that need semantic understanding.

**Example**:
```json
{
  "device_id": "sensor_xyz",
  "description": "Temperature probe in industrial freezer (-40°C to 0°C)"
}
```

**LLM Task**:
- Extract: sensor type = temperature
- Extract: environment = industrial, freezer
- Extract: expected range = -40 to 0°C
- Infer: Critical monitoring (freezer failure detection)
- Suggest: Pair with door sensor, power monitor

### 2. Unit Conversion & Validation

**Problem**: Ambiguous units or missing unit information.

**Example**:
```json
{
  "device_id": "sensor_abc",
  "sensor_type": "pressure",
  "value": 14.7,
  "unit": ""
}
```

**LLM Task**:
- Analyze device description: "Barometric pressure sensor for weather station"
- Infer unit: PSI (standard atmospheric = 14.7 PSI)
- Convert to standard: 1013.25 hPa
- Validate: Value reasonable for weather monitoring

### 3. Semantic Type Mapping

**Problem**: Various naming conventions for sensor types.

**Example**:
```json
{
  "sensor_type": "ambient_env_temp",
  "description": "Room air conditioning monitor"
}
```

**LLM Task**:
- Normalize type: "ambient_env_temp" → "temperature"
- Add semantic tags: ["indoor", "hvac", "comfort"]
- Suggest related sensors: humidity, co2, occupancy

### 4. Anomaly Explanation

**Problem**: Understanding why an anomaly occurred.

**Example**:
```json
{
  "device_id": "fridge_01",
  "description": "Refrigerator temperature sensor",
  "value": 25,
  "unit": "celsius",
  "expected_range": [2, 8]
}
```

**LLM Analysis**:
```
Anomaly detected: 25°C (expected 2-8°C)
Possible causes:
1. Door left open
2. Compressor failure
3. Power outage recovery
4. Incorrect sensor placement
Recommended actions:
- Check door status sensor
- Verify power log
- Alert maintenance if persists >30min
```

### 5. Multi-Sensor Correlation

**Problem**: Understanding relationships between sensors.

**Device Description**:
```
"HVAC system with temperature, humidity, and CO2 sensors in conference room"
```

**LLM Insights**:
- High CO2 + Normal temp → Room occupied, increase ventilation
- High temp + Low humidity → AC working but dry, add humidifier
- Pattern: Temp spikes before CO2 → Body heat from occupancy

## LLM Service Interface

### API Design

```go
type LLMService interface {
    // Analyze device description during registration
    AnalyzeDevice(ctx context.Context, device Device) (*DeviceAnalysis, error)
    
    // Transform sensor data with semantic understanding
    TransformData(ctx context.Context, data SensorData, context DeviceContext) (*SemanticTransform, error)
    
    // Explain anomaly with context
    ExplainAnomaly(ctx context.Context, anomaly AnomalyEvent, history []SensorData) (*AnomalyExplanation, error)
    
    // Suggest optimal sensor configuration
    SuggestConfiguration(ctx context.Context, device Device, environment Environment) (*ConfigSuggestion, error)
}
```

### Request/Response Examples

#### Device Analysis

**Request**:
```json
{
  "device_id": "sensor_123",
  "description": "Solar panel temperature monitor for efficiency tracking",
  "location": "Rooftop, South-facing",
  "existing_sensors": ["temperature"]
}
```

**LLM Response**:
```json
{
  "analysis": {
    "primary_function": "efficiency_monitoring",
    "environment": {
      "type": "outdoor",
      "exposure": "direct_sunlight",
      "orientation": "south"
    },
    "sensor_validation": {
      "temperature": {
        "expected_range": [-20, 85],
        "unit": "celsius",
        "critical": true,
        "optimal_range": [15, 45]
      }
    },
    "suggestions": [
      {
        "sensor": "light_intensity",
        "reason": "Correlate temperature with solar irradiance for efficiency calculation"
      },
      {
        "sensor": "humidity",
        "reason": "Detect condensation that affects panel efficiency"
      }
    ],
    "alerts": [
      {
        "condition": "temperature > 75°C",
        "severity": "warning",
        "reason": "Reduced efficiency, potential damage above 85°C"
      }
    ]
  }
}
```

#### Data Transformation

**Request**:
```json
{
  "sensor_data": {
    "device_id": "sensor_123",
    "sensor_type": "temp",
    "value": 158,
    "unit": "",
    "timestamp": 1707321600
  },
  "device_context": {
    "description": "Solar panel temperature monitor",
    "location": "Rooftop",
    "environment": "outdoor"
  }
}
```

**LLM Response**:
```json
{
  "transformed_data": {
    "sensor_type": "temperature",
    "value": 70,
    "unit": "celsius",
    "confidence": 0.95,
    "reasoning": "158°F = 70°C, inferred Fahrenheit from typical solar panel operating temperature range"
  },
  "validation": {
    "status": "valid",
    "message": "Temperature within expected range for solar panels under load"
  },
  "insights": [
    "Panel operating at high efficiency temperature",
    "Consider checking cooling if sustained above 75°C"
  ]
}
```

## Implementation Steps

### Phase 1: Foundation (Current)

✅ **Completed**:
- Device Registry with description fields
- Transformation Service with rule engine
- Extension points in code (commented LLM hooks)
- Data flow supports both rule-based and LLM paths

### Phase 2: LLM Service Deployment

**Option A: Cloud LLM (OpenAI, Anthropic)**

```yaml
# edge/docker-compose.yml
services:
  transformation-service:
    environment:
      - ENABLE_LLM=true
      - LLM_SERVICE_URL=https://api.openai.com/v1
      - LLM_API_KEY=${OPENAI_API_KEY}
      - LLM_MODEL=gpt-4
```

**Option B: Self-Hosted LLM (Ollama)**

```yaml
# edge/docker-compose.yml
services:
  ollama:
    image: ollama/ollama:latest
    ports:
      - "11434:11434"
    volumes:
      - ollama-models:/root/.ollama
    # GPU support (if available)
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
  
  transformation-service:
    environment:
      - ENABLE_LLM=true
      - LLM_SERVICE_URL=http://ollama:11434
      - LLM_MODEL=llama3
```

**Option C: Edge-Optimized LLM (ONNX Runtime)**

```yaml
services:
  llm-service:
    image: iot-llm-service:latest
    build: ./services/llm-service
    environment:
      - MODEL_PATH=/models/distilbert-base-uncased.onnx
    volumes:
      - ./models:/models
```

### Phase 3: Implement LLM Client

Uncomment and implement in `transformation/main.go`:

```go
type LLMClient struct {
    baseURL string
    apiKey  string
    model   string
    httpClient *http.Client
}

func NewLLMClient(config LLMConfig) *LLMClient {
    return &LLMClient{
        baseURL: config.URL,
        apiKey:  config.APIKey,
        model:   config.Model,
        httpClient: &http.Client{Timeout: 30 * time.Second},
    }
}

func (llm *LLMClient) TransformData(ctx context.Context, data SensorData, deviceDesc string) (*LLMTransformResult, error) {
    prompt := fmt.Sprintf(`
Analyze this sensor data and provide transformation guidance:

Device Description: %s
Sensor Type: %s
Value: %.2f
Unit: %s

Tasks:
1. Validate or correct sensor type
2. Infer unit if missing or ambiguous
3. Convert to standard SI unit
4. Validate value is reasonable for this sensor type
5. Provide confidence score (0-1)

Respond in JSON format:
{
  "sensor_type": "standardized_type",
  "value": converted_value,
  "unit": "standard_unit",
  "confidence": 0.95,
  "reasoning": "explanation",
  "warnings": ["any concerns"]
}
`, deviceDesc, data.SensorType, data.Value, data.Unit)

    // Call LLM API
    response, err := llm.callAPI(ctx, prompt)
    if err != nil {
        return nil, err
    }
    
    // Parse and return
    var result LLMTransformResult
    if err := json.Unmarshal(response, &result); err != nil {
        return nil, err
    }
    
    return &result, nil
}

func (s *TransformationService) transformWithLLM(data SensorData) (TransformedData, error) {
    // Get device context from registry
    device, err := s.registryClient.GetDevice(data.DeviceID)
    if err != nil {
        // Fallback to rule-based
        return s.transformWithRules(data), nil
    }
    
    // Call LLM
    llmResult, err := s.llmClient.TransformData(context.Background(), data, device.Description)
    if err != nil {
        log.Printf("[WARN] LLM failed, falling back to rules: %v", err)
        return s.transformWithRules(data), nil
    }
    
    // Use LLM result if confidence is high
    if llmResult.Confidence > 0.8 {
        return TransformedData{
            DeviceID:      data.DeviceID,
            SensorType:    llmResult.SensorType,
            Value:         llmResult.Value,
            Unit:          llmResult.Unit,
            OriginalValue: data.Value,
            OriginalUnit:  data.Unit,
            TransformedBy: "llm",
            TransformationID: llmResult.SessionID,
            Metadata: map[string]interface{}{
                "llm_confidence": llmResult.Confidence,
                "llm_reasoning":  llmResult.Reasoning,
                "llm_warnings":   llmResult.Warnings,
            },
        }, nil
    }
    
    // Low confidence, use rules
    return s.transformWithRules(data), nil
}
```

### Phase 4: Hybrid Strategy

Implement intelligent routing:

```go
func (s *TransformationService) transformData(data SensorData) TransformedData {
    // Strategy 1: Try rules first (fast)
    ruleResult := s.ruleEngine.Transform(data)
    
    // If rule found and confident, use it
    if ruleResult.Matched && ruleResult.Confidence == 1.0 {
        return ruleResult.Data
    }
    
    // Strategy 2: Use LLM for ambiguous cases
    if s.config.EnableLLMTransform {
        llmResult, err := s.transformWithLLM(data)
        if err == nil && llmResult.Confidence > 0.8 {
            // Cache LLM result as new rule
            s.ruleEngine.LearnFromLLM(data, llmResult)
            return llmResult
        }
    }
    
    // Strategy 3: Fallback to rule result (even if uncertain)
    return ruleResult.Data
}
```

## Prompting Strategies

### Device Onboarding Prompt

```
You are an IoT device analyst. Analyze this device registration:

Device ID: {device_id}
Type: {device_type}
Description: {description}
Location: {location}

Tasks:
1. Validate sensor types are correctly named
2. Determine expected value ranges based on description
3. Identify environment (indoor/outdoor/industrial/etc.)
4. Suggest complementary sensors that would add value
5. Identify potential failure modes to monitor

Provide structured JSON response with analysis and recommendations.
```

### Data Transformation Prompt

```
You are a sensor data expert. Transform this sensor reading:

Context:
- Device: {device_description}
- Location: {location}
- Sensor: {sensor_type}
- Value: {value} {unit}

Tasks:
1. Confirm or correct sensor type (if ambiguous)
2. Validate unit (infer if missing)
3. Convert to SI standard unit
4. Check if value is reasonable for this context
5. Confidence score (0-1)

JSON format: {"sensor_type": "...", "value": X, "unit": "...", "confidence": 0.95}
```

### Anomaly Explanation Prompt

```
Explain this sensor anomaly:

Device: {description}
Expected: {expected_range}
Actual: {actual_value}
History: [last 10 readings]

Provide:
1. Likely root causes (ranked by probability)
2. Recommended immediate actions
3. Related sensors to check
4. Whether this requires urgent attention

Format as actionable JSON.
```

## Performance Considerations

### Latency Management

| Strategy | Latency | Use Case |
|----------|---------|----------|
| **Rule-based** | <1ms | Standard conversions |
| **LLM (cached)** | <5ms | Previously seen patterns |
| **LLM (API call)** | 100-500ms | New/ambiguous data |
| **LLM (local)** | 50-200ms | Self-hosted Ollama |

### Caching Strategy

```go
type LLMCache struct {
    cache map[string]LLMResult
    ttl   time.Duration
}

func (c *LLMCache) Get(key string) (*LLMResult, bool) {
    // Check cache before calling LLM
}

func (c *LLMCache) Set(key string, result LLMResult) {
    // Cache result for similar future queries
}
```

### Cost Optimization

**For Cloud LLM (OpenAI)**:
- Cache aggressively: Similar devices get cached results
- Batch non-urgent queries: Process in bulk during off-peak
- Use cheaper models for simple tasks: gpt-3.5 for basic validation
- Reserve gpt-4 for complex analysis

**For Self-Hosted LLM**:
- Resource allocation: 4-8 GB RAM, 1 GPU optional
- Model selection: Llama2-7B for edge, Llama2-70B for cloud
- Quantization: Use 4-bit quantized models for edge deployment

## Testing LLM Integration

### Unit Tests

```go
func TestLLMTransformation(t *testing.T) {
    // Mock LLM client
    mockLLM := &MockLLMClient{
        responses: map[string]LLMResult{
            "temp,72,fahrenheit": {
                SensorType: "temperature",
                Value:      22.22,
                Unit:       "celsius",
                Confidence: 0.95,
            },
        },
    }
    
    service := NewTransformationService(config)
    service.llmClient = mockLLM
    
    data := SensorData{
        SensorType: "temp",
        Value:      72,
        Unit:       "fahrenheit",
    }
    
    result := service.transformData(data)
    
    assert.Equal(t, "temperature", result.SensorType)
    assert.InDelta(t, 22.22, result.Value, 0.01)
    assert.Equal(t, "celsius", result.Unit)
}
```

### Integration Tests

```bash
# Test with real LLM
curl -X POST http://localhost:8080/transform \
  -H "Content-Type: application/json" \
  -d '{
    "device_id": "test_sensor",
    "sensor_type": "ambient_temp",
    "value": 68,
    "unit": "F",
    "description": "Living room temperature sensor"
  }'
```

## Security & Privacy

### Data Privacy

- **No PII in prompts**: Sanitize device descriptions
- **Local processing option**: Self-host LLM for sensitive data
- **Audit logging**: Track all LLM transformations

### API Security

```go
// Secure LLM API calls
func (llm *LLMClient) callAPI(ctx context.Context, prompt string) ([]byte, error) {
    req, _ := http.NewRequestWithContext(ctx, "POST", llm.baseURL, body)
    req.Header.Set("Authorization", "Bearer " + llm.apiKey)
    req.Header.Set("Content-Type", "application/json")
    
    // Add timeout and retry logic
    // ...
}
```

## Monitoring LLM Performance

### Metrics to Track

```
- llm_requests_total: Total LLM API calls
- llm_request_duration_seconds: Latency histogram
- llm_confidence_score: Distribution of confidence scores
- llm_fallback_total: Times rule-based fallback was used
- llm_cache_hit_rate: Cache effectiveness
- llm_cost_usd: Running cost (for cloud LLM)
```

### Alerts

```yaml
- name: LLMHighLatency
  condition: llm_request_duration_seconds > 1s
  action: Switch to rule-based for performance

- name: LLMLowConfidence
  condition: llm_confidence_score < 0.7 for 10m
  action: Review prompts, retrain if needed

- name: LLMHighCost
  condition: llm_cost_usd > threshold
  action: Increase caching, use cheaper model
```

## Conclusion

The system is **architected for LLM integration** with:
- ✅ Extension points in code
- ✅ Device registry for context
- ✅ Hybrid rule/LLM strategy
- ✅ Fallback mechanisms
- ✅ Performance optimizations
- ✅ Cost controls

**When ready**, simply:
1. Deploy LLM service (cloud or self-hosted)
2. Set `ENABLE_LLM=true`
3. Uncomment LLM code in transformation service
4. Enjoy semantic intelligence! 🧠
