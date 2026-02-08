#!/usr/bin/env python3
"""
Data Transformation Service
Unit conversion and semantic standardization
Consumes from raw-sensor-data, transforms, publishes to transformed-sensor-data
"""

import json
import logging
import os
import signal
import sys
from typing import Dict, Any, Callable, Optional
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RuleEngine:
    """Handles rule-based transformations"""
    
    def __init__(self):
        self.unit_conversions: Dict[str, Dict[str, Any]] = {}
        self.type_mapping: Dict[str, str] = {}
        self._register_conversions()
        self._register_type_mapping()
    
    def _register_conversions(self):
        """Register standard unit conversion rules"""
        
        # Temperature conversions
        self.unit_conversions['fahrenheit->celsius'] = {
            'from_unit': 'fahrenheit',
            'to_unit': 'celsius',
            'convert': lambda f: (f - 32) * 5 / 9,
            'description': 'Fahrenheit to Celsius'
        }
        
        self.unit_conversions['kelvin->celsius'] = {
            'from_unit': 'kelvin',
            'to_unit': 'celsius',
            'convert': lambda k: k - 273.15,
            'description': 'Kelvin to Celsius'
        }
        
        # Pressure conversions
        self.unit_conversions['psi->hpa'] = {
            'from_unit': 'psi',
            'to_unit': 'hpa',
            'convert': lambda psi: psi * 68.9476,
            'description': 'PSI to hPa'
        }
        
        self.unit_conversions['bar->hpa'] = {
            'from_unit': 'bar',
            'to_unit': 'hpa',
            'convert': lambda bar: bar * 1000,
            'description': 'Bar to hPa'
        }
        
        # Distance conversions
        self.unit_conversions['feet->meters'] = {
            'from_unit': 'feet',
            'to_unit': 'meters',
            'convert': lambda ft: ft * 0.3048,
            'description': 'Feet to Meters'
        }
        
        self.unit_conversions['miles->kilometers'] = {
            'from_unit': 'miles',
            'to_unit': 'kilometers',
            'convert': lambda mi: mi * 1.60934,
            'description': 'Miles to Kilometers'
        }
        
        # Speed conversions
        self.unit_conversions['mph->kmh'] = {
            'from_unit': 'mph',
            'to_unit': 'kmh',
            'convert': lambda mph: mph * 1.60934,
            'description': 'MPH to KM/H'
        }
    
    def _register_type_mapping(self):
        """Register semantic type standardization"""
        # Temperature variations
        self.type_mapping['temp'] = 'temperature'
        self.type_mapping['temperature'] = 'temperature'
        self.type_mapping['ambient_temp'] = 'temperature'
        
        # Humidity variations
        self.type_mapping['humidity'] = 'humidity'
        self.type_mapping['relative_humidity'] = 'humidity'
        self.type_mapping['rh'] = 'humidity'
        
        # Pressure variations
        self.type_mapping['pressure'] = 'pressure'
        self.type_mapping['atmospheric_pressure'] = 'pressure'
        self.type_mapping['atm_pressure'] = 'pressure'
        
        # Motion variations
        self.type_mapping['motion'] = 'motion'
        self.type_mapping['movement'] = 'motion'
        self.type_mapping['pir'] = 'motion'
    
    def standardize_type(self, sensor_type: str) -> str:
        """Standardize sensor type name"""
        normalized = sensor_type.lower().strip()
        return self.type_mapping.get(normalized, normalized)
    
    def get_standard_unit(self, sensor_type: str, current_unit: str) -> str:
        """Get standard unit for sensor type"""
        standards = {
            'temperature': 'celsius',
            'humidity': 'percent',
            'pressure': 'hpa',
            'distance': 'meters',
            'speed': 'kmh'
        }
        return standards.get(sensor_type, current_unit)
    
    def convert_value(self, value: float, from_unit: str, to_unit: str) -> tuple[float, bool]:
        """
        Convert value from one unit to another
        Returns: (converted_value, was_converted)
        """
        from_unit = from_unit.lower().strip()
        to_unit = to_unit.lower().strip()
        
        # No conversion needed
        if from_unit == to_unit:
            return value, True
        
        # Look up conversion rule
        key = f"{from_unit}->{to_unit}"
        if key in self.unit_conversions:
            conversion = self.unit_conversions[key]
            converted_value = conversion['convert'](value)
            return converted_value, True
        
        # No conversion found
        return value, False


class TransformationService:
    """Handles data transformation pipeline"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.consumer = None
        self.producer = None
        self.rule_engine = RuleEngine()
        self.transform_count = 0
        self.running = False
    
    def setup_kafka(self):
        """Initialize Kafka consumer and producer"""
        try:
            # Create consumer
            self.consumer = KafkaConsumer(
                self.config['input_topic'],
                bootstrap_servers=self.config['redpanda_brokers'],
                group_id=self.config['consumer_group'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                max_poll_records=100
            )
            
            # Create producer
            self.producer = KafkaProducer(
                bootstrap_servers=self.config['redpanda_brokers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                compression_type='lz4'
            )
            
            logger.info(f"Connected to Redpanda: {self.config['redpanda_brokers']}")
            logger.info(f"Consuming from: {self.config['input_topic']}")
            logger.info(f"Publishing to: {self.config['output_topic']}")
            
        except Exception as e:
            logger.error(f"Failed to setup Kafka: {e}")
            raise
    
    def transform_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform sensor data"""
        # Standardize sensor type
        standard_type = self.rule_engine.standardize_type(data['sensor_type'])
        
        # Get standard unit for this type
        standard_unit = self.rule_engine.get_standard_unit(standard_type, data['unit'])
        
        # Convert value if needed
        converted_value, was_converted = self.rule_engine.convert_value(
            data['value'],
            data['unit'],
            standard_unit
        )
        
        # Create transformed data
        transformed = {
            'device_id': data['device_id'],
            'sensor_type': standard_type,  # Standardized type
            'value': converted_value,      # Converted value
            'unit': standard_unit,         # Standardized unit
            'original_value': data['value'],
            'original_unit': data['unit'],
            'timestamp': data['timestamp'],
            'is_anomaly': data.get('is_anomaly', False),
            'description': data.get('description', ''),
            'metadata': data.get('metadata', {}),
            'transformed_by': 'rule',
            'transformation_id': ''
        }
        
        if was_converted and data['unit'] != standard_unit:
            transformed['transformation_id'] = f"{data['unit']}->{standard_unit}"
            logger.info(
                f"[TRANSFORM] {data['device_id']}: "
                f"{data['value']:.2f} {data['unit']} → "
                f"{converted_value:.2f} {standard_unit}"
            )
        
        # Future: LLM transformation hook
        if self.config['enable_llm']:
            logger.debug("[LLM_HOOK] LLM transformation enabled but not implemented")
            # transformed = self._llm_transform(transformed, data.get('description', ''))
        
        return transformed
    
    def process_message(self, message):
        """Process a single message"""
        try:
            data = message.value
            
            # Transform the data
            transformed = self.transform_data(data)
            
            # Publish to output topic
            future = self.producer.send(
                self.config['output_topic'],
                key=data['device_id'],
                value=transformed
            )
            
            # Wait for send to complete
            future.get(timeout=10)
            
            self.transform_count += 1
            if self.transform_count % 100 == 0:
                logger.info(f"[INFO] Transformed {self.transform_count} records")
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def start(self):
        """Start the transformation service"""
        logger.info("=" * 50)
        logger.info("Data Transformation Service")
        logger.info("=" * 50)
        logger.info("[INFO] Rule-based transformations enabled")
        
        if self.config['enable_llm']:
            logger.info(f"[INFO] LLM transformations enabled (endpoint: {self.config['llm_url']})")
        else:
            logger.info("[INFO] LLM transformations disabled (ready for future integration)")
        
        # Setup Kafka
        self.setup_kafka()
        
        self.running = True
        logger.info("Data Transformation Service started successfully")
        
        # Main processing loop
        try:
            for message in self.consumer:
                if not self.running:
                    break
                self.process_message(message)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the transformation service"""
        logger.info("Shutting down Data Transformation Service...")
        self.running = False
        
        # Close Kafka connections
        if self.producer:
            self.producer.flush()
            self.producer.close()
        
        if self.consumer:
            self.consumer.close()
        
        logger.info(f"Total records transformed: {self.transform_count}")
        logger.info("Service stopped")


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}")
    sys.exit(0)


def main():
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Load configuration from environment
    config = {
        'redpanda_brokers': os.getenv('REDPANDA_BROKERS', 'redpanda:9092').split(','),
        'input_topic': os.getenv('INPUT_TOPIC', 'raw-sensor-data'),
        'output_topic': os.getenv('OUTPUT_TOPIC', 'transformed-sensor-data'),
        'consumer_group': os.getenv('CONSUMER_GROUP', 'transformation-service'),
        'enable_llm': os.getenv('ENABLE_LLM', 'false').lower() == 'true',
        'llm_url': os.getenv('LLM_SERVICE_URL', 'http://llm-service:8080')
    }
    
    logger.info(f"[CONFIG] Input Topic: {config['input_topic']}")
    logger.info(f"[CONFIG] Output Topic: {config['output_topic']}")
    logger.info(f"[CONFIG] LLM Enabled: {config['enable_llm']}")
    
    # Create and start service
    service = TransformationService(config)
    
    try:
        service.start()
    except Exception as e:
        logger.error(f"Service failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()


# ============================================
# Future: LLM Integration Hook
# ============================================
# 
# def _llm_transform(self, data: Dict[str, Any], description: str) -> Dict[str, Any]:
#     """
#     Use LLM to:
#     1. Understand device description semantically
#     2. Infer correct sensor type if ambiguous
#     3. Apply context-aware unit conversions
#     4. Enrich metadata with semantic understanding
#     """
#     import requests
#     
#     prompt = f"""
#     Device Description: {description}
#     Sensor Type: {data['sensor_type']}
#     Current Value: {data['value']} {data['unit']}
#     
#     Tasks:
#     1. Confirm or correct sensor type based on description
#     2. Suggest appropriate standard unit
#     3. Validate value range for this sensor type
#     4. Provide semantic tags
#     """
#     
#     # Call LLM service
#     response = requests.post(
#         f"{self.config['llm_url']}/analyze",
#         json={'prompt': prompt}
#     )
#     
#     llm_result = response.json()
#     
#     # Apply LLM suggestions
#     data['transformed_by'] = 'llm'
#     data['transformation_id'] = llm_result.get('session_id', '')
#     data['metadata']['llm_suggestions'] = llm_result.get('suggestions', {})
#     
#     return data
