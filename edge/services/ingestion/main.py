#!/usr/bin/env python3
"""
Data Ingestion Service
MQTT → Redpanda data pipeline
Subscribes to MQTT sensor topics and publishes to Redpanda
"""

import json
import logging
import os
import signal
import sys
from typing import Dict, Any
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IngestionService:
    """Handles MQTT to Redpanda data ingestion"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.mqtt_client = None
        self.kafka_producer = None
        self.message_count = 0
        self.running = False
        
    def setup_kafka(self):
        """Initialize Kafka/Redpanda producer"""
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.config['redpanda_brokers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas
                retries=3,
                max_in_flight_requests_per_connection=5,
                compression_type='lz4'
            )
            logger.info(f"Connected to Redpanda: {self.config['redpanda_brokers']}")
        except Exception as e:
            logger.error(f"Failed to connect to Redpanda: {e}")
            raise
    
    def on_connect(self, client, userdata, flags, rc):
        """MQTT connection callback"""
        if rc == 0:
            logger.info(f"Connected to MQTT broker: {self.config['mqtt_broker']}")
            
            # Subscribe to sensor topics
            client.subscribe(self.config['mqtt_topic'], qos=1)
            logger.info(f"Subscribed to topic: {self.config['mqtt_topic']}")
        else:
            logger.error(f"Failed to connect to MQTT broker, return code {rc}")
    
    def on_disconnect(self, client, userdata, rc):
        """MQTT disconnection callback"""
        if rc != 0:
            logger.warning(f"Unexpected disconnect from MQTT broker (rc={rc})")
    
    def on_message(self, client, userdata, msg):
        """MQTT message callback"""
        try:
            # Parse sensor data
            payload = msg.payload.decode('utf-8')
            data = json.loads(payload)
            
            # Validate basic fields
            if not data.get('device_id') or not data.get('timestamp'):
                logger.warning("Invalid message: missing device_id or timestamp")
                return
            
            device_id = data['device_id']
            
            # Publish to Redpanda
            future = self.kafka_producer.send(
                self.config['redpanda_topic'],
                key=device_id,
                value=data
            )
            
            # Add callback for success/failure
            future.add_callback(self.on_kafka_success)
            future.add_errback(self.on_kafka_error)
            
            self.message_count += 1
            if self.message_count % 100 == 0:
                logger.info(f"[INFO] Processed {self.message_count} messages")
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def on_kafka_success(self, metadata):
        """Kafka produce success callback"""
        logger.debug(f"Message sent to partition {metadata.partition} at offset {metadata.offset}")
    
    def on_kafka_error(self, exc):
        """Kafka produce error callback"""
        logger.error(f"Failed to send message to Redpanda: {exc}")
    
    def setup_mqtt(self):
        """Initialize MQTT client"""
        self.mqtt_client = mqtt.Client(client_id="ingestion-service")
        
        # Set callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.mqtt_client.on_message = self.on_message
        
        # Configure connection
        self.mqtt_client.reconnect_delay_set(min_delay=1, max_delay=120)
        
        # Connect to broker
        broker_host, broker_port = self.parse_mqtt_broker(self.config['mqtt_broker'])
        
        try:
            self.mqtt_client.connect(broker_host, broker_port, keepalive=60)
            logger.info(f"Connecting to MQTT broker {broker_host}:{broker_port}...")
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            raise
    
    def parse_mqtt_broker(self, broker_url: str):
        """Parse MQTT broker URL"""
        # Remove protocol prefix if present
        if '://' in broker_url:
            broker_url = broker_url.split('://')[1]
        
        # Split host:port
        if ':' in broker_url:
            host, port = broker_url.rsplit(':', 1)
            return host, int(port)
        else:
            return broker_url, 1883
    
    def start(self):
        """Start the ingestion service"""
        logger.info("=" * 50)
        logger.info("Data Ingestion Service")
        logger.info("=" * 50)
        
        # Setup Kafka producer
        self.setup_kafka()
        
        # Setup MQTT client
        self.setup_mqtt()
        
        # Start MQTT loop
        self.mqtt_client.loop_start()
        
        self.running = True
        logger.info("Data Ingestion Service started successfully")
        
        # Keep running until signal
        try:
            while self.running:
                signal.pause()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
            self.stop()
    
    def stop(self):
        """Stop the ingestion service"""
        logger.info("Shutting down Data Ingestion Service...")
        self.running = False
        
        # Disconnect MQTT
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        
        # Flush and close Kafka producer
        if self.kafka_producer:
            self.kafka_producer.flush()
            self.kafka_producer.close()
        
        logger.info(f"Total messages processed: {self.message_count}")
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
        'mqtt_broker': os.getenv('MQTT_BROKER', 'tcp://emqx:1883'),
        'mqtt_topic': os.getenv('MQTT_TOPIC', 'sensors/+/data'),
        'redpanda_brokers': os.getenv('REDPANDA_BROKERS', 'redpanda:9092').split(','),
        'redpanda_topic': os.getenv('REDPANDA_TOPIC', 'raw-sensor-data')
    }
    
    logger.info(f"[CONFIG] MQTT Broker: {config['mqtt_broker']}")
    logger.info(f"[CONFIG] MQTT Topic: {config['mqtt_topic']}")
    logger.info(f"[CONFIG] Redpanda Brokers: {config['redpanda_brokers']}")
    logger.info(f"[CONFIG] Redpanda Topic: {config['redpanda_topic']}")
    
    # Create and start service
    service = IngestionService(config)
    
    try:
        service.start()
    except Exception as e:
        logger.error(f"Service failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
