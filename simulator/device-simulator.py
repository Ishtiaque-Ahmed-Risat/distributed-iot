#!/usr/bin/env python3
"""
IoT Device Simulator
Simulates multiple IoT sensors sending data via MQTT
"""

import json
import time
import random
import logging
from datetime import datetime
from typing import Dict, List
import paho.mqtt.client as mqtt
import yaml
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SensorSimulator:
    """Simulates a single IoT sensor"""
    
    def __init__(self, device_id: str, sensor_type: str, config: Dict):
        self.device_id = device_id
        self.sensor_type = sensor_type
        self.config = config
        self.value = config['initial_value']
        
    def generate_reading(self) -> Dict:
        """Generate sensor reading with optional anomaly"""
        # Normal variation
        variation = random.uniform(
            self.config['variation_min'],
            self.config['variation_max']
        )
        self.value += variation
        
        # Keep within bounds
        self.value = max(self.config['min_value'], 
                        min(self.config['max_value'], self.value))
        
        # Introduce anomaly occasionally
        is_anomaly = random.random() < self.config.get('anomaly_rate', 0.01)
        if is_anomaly:
            self.value *= random.uniform(1.5, 3.0)
            logger.warning(f"Anomaly generated for {self.device_id}: {self.value:.2f}")
        
        return {
            'device_id': self.device_id,
            'sensor_type': self.sensor_type,
            'value': round(self.value, 2),
            'unit': self.config['unit'],
            'timestamp': int(datetime.utcnow().timestamp()),
            'is_anomaly': is_anomaly
        }


class IoTDeviceSimulator:
    """Main simulator managing multiple devices"""
    
    def __init__(self, config_file: str = 'config.yaml'):
        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.mqtt_client = None
        self.sensors: List[SensorSimulator] = []
        self.running = False
        
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Connected to MQTT broker at {self.config['mqtt']['broker']}")
        else:
            logger.error(f"Failed to connect, return code {rc}")
    
    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            logger.warning("Unexpected disconnect from MQTT broker")
    
    def on_publish(self, client, userdata, mid):
        logger.debug(f"Message {mid} published")
    
    def setup_mqtt(self):
        """Initialize MQTT client"""
        self.mqtt_client = mqtt.Client(
            client_id=f"simulator_{int(time.time())}"
        )
        
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.mqtt_client.on_publish = self.on_publish
        
        # Connect to broker
        broker = self.config['mqtt']['broker']
        port = self.config['mqtt']['port']
        
        logger.info(f"Connecting to MQTT broker {broker}:{port}...")
        self.mqtt_client.connect(broker, port, keepalive=60)
        self.mqtt_client.loop_start()
    
    def register_devices_with_registry(self):
        """Register devices with device-registry API (optional)"""
        if not self.config.get('device_registry', {}).get('enabled', False):
            logger.info("Device registration disabled in config")
            return
        
        registry_url = self.config['device_registry'].get('url', 'http://localhost:8080')
        num_devices = self.config['simulation']['num_devices']
        
        logger.info(f"Registering {num_devices} devices with registry at {registry_url}")
        
        for i in range(num_devices):
            device_id = f"device_{i:04d}"
            
            # Prepare registration payload
            registration = {
                "device_id": device_id,
                "device_type": "simulated_sensor",
                "description": f"Simulated IoT device {device_id} for testing and demo",
                "location": f"Simulation Zone {i % 10}",
                "metadata": {
                    "simulator": "true",
                    "zone": str(i % 10)
                },
                "sensors": [
                    {
                        "sensor_type": sensor_type,
                        "unit": sensor_config['unit'],
                        "min_value": sensor_config['min_value'],
                        "max_value": sensor_config['max_value'],
                        "description": f"{sensor_type.capitalize()} sensor"
                    }
                    for sensor_type, sensor_config in self.config['sensor_types'].items()
                ]
            }
            
            try:
                response = requests.post(
                    f"{registry_url}/api/v1/devices",
                    json=registration,
                    timeout=5
                )
                if response.status_code in [200, 201]:
                    logger.info(f"✓ Registered {device_id}")
                else:
                    logger.warning(f"Failed to register {device_id}: {response.status_code}")
            except Exception as e:
                logger.warning(f"Could not register {device_id}: {e}")
        
        logger.info("Device registration complete")
    
    def create_devices(self):
        """Create simulated devices based on config"""
        num_devices = self.config['simulation']['num_devices']
        
        for i in range(num_devices):
            device_id = f"device_{i:04d}"
            
            # Create sensors for each device
            for sensor_type, sensor_config in self.config['sensor_types'].items():
                sensor = SensorSimulator(device_id, sensor_type, sensor_config)
                self.sensors.append(sensor)
        
        logger.info(f"Created {len(self.sensors)} sensors across {num_devices} devices")
    
    def publish_reading(self, sensor: SensorSimulator):
        """Publish sensor reading to MQTT"""
        reading = sensor.generate_reading()
        
        topic = self.config['mqtt']['topic_template'].format(
            device_id=reading['device_id']
        )
        
        payload = json.dumps(reading)
        
        result = self.mqtt_client.publish(
            topic,
            payload,
            qos=self.config['mqtt']['qos']
        )
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            logger.debug(f"Published to {topic}: {reading['value']}")
        else:
            logger.error(f"Failed to publish to {topic}")
    
    def run(self):
        """Main simulation loop"""
        self.setup_mqtt()
        self.create_devices()
        
        # Register devices with registry (if enabled)
        self.register_devices_with_registry()
        
        interval = self.config['simulation']['interval_seconds']
        logger.info(f"Starting simulation with {interval}s interval")
        
        self.running = True
        message_count = 0
        
        try:
            while self.running:
                start_time = time.time()
                
                # Publish readings from all sensors
                for sensor in self.sensors:
                    self.publish_reading(sensor)
                    message_count += 1
                
                elapsed = time.time() - start_time
                logger.info(
                    f"Published {len(self.sensors)} readings in {elapsed:.2f}s "
                    f"(Total: {message_count})"
                )
                
                # Sleep until next interval
                sleep_time = max(0, interval - elapsed)
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Simulation stopped by user")
        finally:
            self.stop()
    
    def stop(self):
        """Stop simulation and cleanup"""
        self.running = False
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        logger.info("Simulator stopped")


def main():
    logger.info("IoT Device Simulator starting...")
    simulator = IoTDeviceSimulator('config.yaml')
    simulator.run()


if __name__ == '__main__':
    main()
