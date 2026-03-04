#!/usr/bin/env python3
"""
IoT Device Simulator
Simulates multiple IoT devices, each with its own MQTT client and thread.
Each device operates independently — just like real IoT hardware.
"""

import json
import time
import random
import logging
import threading
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

# Global message counter (no lock)
global_message_count = 0


class SensorSimulator:
    """Simulates a single sensor on a device"""

    def __init__(self, device_id: str, sensor_type: str, config: Dict):
        self.device_id = device_id
        self.sensor_type = sensor_type
        self.config = config
        self.value = config['initial_value']

    def generate_reading(self) -> Dict:
        """Generate sensor reading with optional anomaly"""
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
            logger.warning(f"Anomaly generated for {self.device_id}/{self.sensor_type}: {self.value:.2f}")

        return {
            'device_id': self.device_id,
            'sensor_type': self.sensor_type,
            'value': round(self.value, 2),
            'unit': self.config['unit'],
            'timestamp': datetime.utcnow().timestamp(),
            'is_anomaly': is_anomaly
        }


class DeviceClient:
    """
    Represents a single IoT device with its own MQTT client.
    Each device runs in its own thread, connecting and publishing independently.
    """

    def __init__(self, device_id: str, mqtt_config: Dict, sensor_type: str, sensor_config: Dict, interval: float):
        self.device_id = device_id
        self.mqtt_config = mqtt_config
        self.interval = interval
        self.sensor: SensorSimulator = None
        self.mqtt_client: mqtt.Client = None
        self.running = False
        self.thread: threading.Thread = None
        self.message_count = 0
        self.connected = False

        # Create one sensor for this device
        self.sensor = SensorSimulator(device_id, sensor_type, sensor_config)

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            logger.info(f"[{self.device_id}] ✓ Connected to MQTT broker")
        else:
            self.connected = False
            logger.error(f"[{self.device_id}] Connection failed (code {rc})")

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        if rc != 0:
            logger.warning(f"[{self.device_id}] Unexpected disconnect (code {rc}), reconnecting...")
        else:
            logger.info(f"[{self.device_id}] Disconnected cleanly")

    def _setup_mqtt(self):
        """Create and configure this device's own MQTT client"""
        self.mqtt_client = mqtt.Client(
            client_id=self.device_id,
            clean_session=False
        )
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_disconnect = self._on_disconnect
        self.mqtt_client.reconnect_delay_set(min_delay=1, max_delay=120)

        broker = self.mqtt_config['broker']
        port = self.mqtt_config['port']

        try:
            self.mqtt_client.connect(broker, port, keepalive=60)
            self.mqtt_client.loop_start()
        except Exception as e:
            logger.error(f"[{self.device_id}] Initial connection failed: {e}, retrying in background...")
            self.mqtt_client.loop_start()

    def _publish_loop(self):
        """Main loop: publish sensor reading at configured interval"""
        global global_message_count
        # Stagger startup to avoid thundering herd
        jitter = random.uniform(0, self.interval)
        time.sleep(jitter)

        while self.running:
            start = time.time()

            reading = self.sensor.generate_reading()
            topic = self.mqtt_config['topic_template'].format(device_id=self.device_id)
            payload = json.dumps(reading)

            try:
                result = self.mqtt_client.publish(
                    topic, payload, qos=self.mqtt_config.get('qos', 1)
                )
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    self.message_count += 1
                    global_message_count += 1
                elif result.rc == mqtt.MQTT_ERR_NO_CONN:
                    logger.warning(f"[{self.device_id}] Not connected — message queued")
                    self.message_count += 1  # QoS 1 queues it
                    global_message_count += 1
                else:
                    logger.error(f"[{self.device_id}] Publish failed (rc={result.rc})")
            except Exception as e:
                logger.error(f"[{self.device_id}] Publish error: {e}")

            elapsed = time.time() - start
            sleep_time = max(0, self.interval - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    def start(self):
        """Start this device's MQTT client and publishing thread"""
        self._setup_mqtt()
        self.running = True
        self.thread = threading.Thread(target=self._publish_loop, daemon=True, name=self.device_id)
        self.thread.start()

    def stop(self):
        """Stop this device"""
        self.running = False
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()


class IoTDeviceSimulator:
    """Manages multiple independent device clients"""

    def __init__(self, config_file: str = 'config.yaml'):
        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)

        self.devices: List[DeviceClient] = []

    def register_devices_with_registry(self, device_sensor_map: Dict[str, str]):
        """Register devices with device-registry API (optional)"""
        if not self.config.get('device_registry', {}).get('enabled', False):
            logger.info("Device registration disabled in config")
            return

        registry_url = self.config['device_registry'].get('url', 'http://localhost:8080')
        num_devices = self.config['simulation']['num_devices']
        sensor_configs = self.config['sensor_types']

        logger.info(f"Registering {num_devices} devices with registry at {registry_url}")

        for i in range(num_devices):
            device_id = f"device_{i:04d}"
            sensor_type = device_sensor_map[device_id]
            sensor_config = sensor_configs[sensor_type]
            
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
                ]
            }

            try:
                response = requests.post(
                    f"{registry_url}/api/v1/devices",
                    json=registration,
                    timeout=5
                )
                if response.status_code in [200, 201]:
                    logger.info(f"✓ Registered {device_id} with {sensor_type} sensor")
                else:
                    logger.warning(f"Failed to register {device_id}: {response.status_code}")
            except Exception as e:
                logger.warning(f"Could not register {device_id}: {e}")

        logger.info("Device registration complete")

    def create_devices(self):
        """Create one DeviceClient per device, each with its own MQTT client and one randomly selected sensor"""
        num_devices = self.config['simulation']['num_devices']
        interval = self.config['simulation']['interval_seconds']
        sensor_configs = self.config['sensor_types']
        sensor_types = list(sensor_configs.keys())
        
        # Store device-to-sensor mapping for registration
        self.device_sensor_map = {}

        for i in range(num_devices):
            device_id = f"device_{i:04d}"
            # Randomly select one sensor type for this device
            selected_sensor_type = random.choice(sensor_types)
            selected_sensor_config = sensor_configs[selected_sensor_type]
            self.device_sensor_map[device_id] = selected_sensor_type
            
            device = DeviceClient(
                device_id=device_id,
                mqtt_config=self.config['mqtt'],
                sensor_type=selected_sensor_type,
                sensor_config=selected_sensor_config,
                interval=interval
            )
            self.devices.append(device)

        logger.info(f"Created {num_devices} devices (each with one randomly selected sensor), each with its own MQTT client")

    def run(self):
        """Start all device clients and monitor"""
        self.create_devices()
        self.register_devices_with_registry(self.device_sensor_map)

        interval = self.config['simulation']['interval_seconds']
        logger.info(f"Starting {len(self.devices)} independent device clients (interval: {interval}s)")

        # Get message limit from config, default to infinity if null
        max_messages = self.config['simulation'].get('max_messages')
        if max_messages is None:
            max_messages = float('inf')
        logger.info(f"Message limit: {max_messages if max_messages != float('inf') else 'unlimited'}")

        # Start all devices
        for device in self.devices:
            device.start()

        logger.info(f"All {len(self.devices)} devices started")

        # Monitor loop
        global global_message_count
        try:
            while True:
                time.sleep(0.1)
                total = sum(d.message_count for d in self.devices)
                connected = sum(1 for d in self.devices if d.connected)
                logger.info(
                    f"[Monitor] Devices: {connected}/{len(self.devices)} connected | "
                    f"Total messages: {total}"
                )
                if global_message_count >= max_messages:
                    logger.info(f"Global message count reached {global_message_count}, stopping simulator")
                    break
        except KeyboardInterrupt:
            logger.info("Simulation stopped by user")
        finally:
            self.stop()

    def stop(self):
        """Stop all device clients"""
        logger.info("Stopping all devices...")
        for device in self.devices:
            device.stop()
        logger.info("All devices stopped")
        logger.info(f"Global message count: {global_message_count}")


def main():
    logger.info("IoT Device Simulator starting...")
    simulator = IoTDeviceSimulator('config.yaml')
    simulator.run()


if __name__ == '__main__':
    main()
