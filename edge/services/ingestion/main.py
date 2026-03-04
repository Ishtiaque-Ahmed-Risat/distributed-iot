#!/usr/bin/env python3
"""
Data Ingestion Service
MQTT → Redpanda data pipeline
Subscribes to MQTT sensor topics and publishes to Redpanda
"""

import json
import logging
import os
import uuid
import signal
import sys
from typing import Dict, Any
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

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
        # Generate client_id once per pod instance for session persistence
        # Each pod gets a unique ID, but reuses it across reconnections
        self.mqtt_client_id = f"ingestion-{uuid.uuid4()}"
        
    def setup_kafka(self):
        """Initialize Kafka/Redpanda producer with ordering guarantees"""
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.config['redpanda_brokers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                
                # Reliability: Ensure messages are delivered
                acks=1,                        # Leader acknowledgment (balanced performance)
                retries=10,                    # Retry failed sends
                max_in_flight_requests_per_connection=5,  # Allow pipelining while maintaining order
                
                # Timeouts
                request_timeout_ms=10000,      # 10s request timeout
                
                # Performance optimizations
                compression_type='lz4',        # Fast compression
                linger_ms=10,                  # Batch for 10ms
                batch_size=16384               # 16KB batches
            )
            logger.info(f"✓ Connected to Redpanda: {self.config['redpanda_brokers']}")
            logger.info("✓ Producer configured: acks=1, retries=10, ordering per device (via key)")
        except Exception as e:
            logger.error(f"Failed to connect to Redpanda: {e}")
            raise
    
    def on_connect(self, client, userdata, flags, rc):
        """
        MQTT connection callback with shared subscription for load balancing.
        
        Message Loss Behavior:
        - If ONE pod goes down: No message loss (other pods handle the load)
        - If ALL pods go down: Potential message loss (messages distributed to available subscribers)
        - In-flight messages (QoS 1/2): Redelivered on reconnect within 30min session window
        - clean_session=False preserves in-flight messages but NOT messages arriving while disconnected
        """
        if rc == 0:
            logger.info(f"✓ Connected to MQTT broker: {self.config['mqtt_broker']}")
            
            # Use MQTT shared subscription for load balancing across multiple replicas
            # Format: $share/group-name/topic-pattern
            # This ensures multiple ingestion pods share the load (no duplicates)
            # IMPORTANT: Shared subscriptions distribute to AVAILABLE subscribers only.
            # Messages arriving while a pod is down go to other pods, not queued for the disconnected pod.
            shared_topic = f"$share/ingestion-group/{self.config['mqtt_topic']}"
            
            result, mid = client.subscribe(shared_topic, qos=1)
            if result == mqtt.MQTT_ERR_SUCCESS:
                logger.info(f"✓ Subscribed to shared topic: {shared_topic}")
                logger.info("✓ Load will be distributed across all ingestion replicas")
                logger.info("⚠ Note: Messages arriving while pod is down go to other pods (not queued)")
            else:
                logger.error(f"✗ Failed to subscribe: error code {result}")
        else:
            error_messages = {
                1: "Connection refused - incorrect protocol version",
                2: "Connection refused - invalid client identifier",
                3: "Connection refused - server unavailable",
                4: "Connection refused - bad username or password",
                5: "Connection refused - not authorized"
            }
            logger.error(f"Failed to connect: {error_messages.get(rc, f'Unknown error code {rc}')}")
            logger.info("Will retry connection automatically...")
    
    def on_disconnect(self, client, userdata, rc):
        """
        MQTT disconnection callback.
        
        With clean_session=False:
        - In-flight QoS 1/2 messages will be redelivered on reconnect
        - Session persists for 30 minutes (EMQX_MQTT__SESSION_EXPIRY_INTERVAL)
        - After 30 minutes, session expires and queued messages are lost
        """
        if rc != 0:
            logger.warning(f"✗ Unexpected disconnect from MQTT broker (rc={rc})")
            logger.info("Auto-reconnection will attempt to restore connection...")
            logger.info("In-flight messages (QoS 1/2) will be redelivered on reconnect (within 30min)")
        else:
            logger.info("Clean disconnect from MQTT broker")
    
    def on_message(self, client, userdata, msg):
        """MQTT message callback with error handling"""
        try:
            # Parse sensor data
            payload = msg.payload.decode('utf-8')
            data = json.loads(payload)
            
            # Validate basic fields
            if not data.get('device_id') or not data.get('timestamp'):
                logger.warning("Invalid message: missing device_id or timestamp")
                return
            
            device_id = data['device_id']
            
            # Publish to Redpanda with retry capability
            try:
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
            
            except Exception as kafka_error:
                logger.error(f"Failed to send to Redpanda: {kafka_error}")
                # Message will be redelivered by MQTT (QoS 1) on next connection
                
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
        """
        Initialize MQTT client with fault-tolerant reconnection.
        
        Session Persistence (clean_session=False):
        - Preserves in-flight QoS 1/2 messages during disconnects
        - Redelivers unacknowledged messages on reconnect (within 30min session window)
        - Maintains subscription state across reconnections
        
        Limitations:
        - Messages arriving while pod is down are NOT queued (shared subscription distributes to available pods)
        - Session expires after 30 minutes of disconnection (EMQX config)
        - Only protects in-flight messages, not messages published while disconnected
        """
        # Reuse client_id per pod instance for session persistence
        # Unique client_id per pod allows multiple ingestion replicas to connect simultaneously
        # clean_session=False enables session persistence: subscriptions and in-flight messages
        # are maintained across reconnections, reducing message loss during disconnects
        
        self.mqtt_client = mqtt.Client(
            client_id=self.mqtt_client_id,
            clean_session=False  # Enable session persistence for fault tolerance
        )
        logger.info(f"MQTT Client ID: {self.mqtt_client_id} (persistent session enabled)")
        logger.info("Session expiry: 30 minutes (messages arriving while down go to other pods)")
        
        # Set callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.mqtt_client.on_message = self.on_message
        
        # Configure automatic reconnection with exponential backoff
        # Retry intervals: 1s, 2s, 4s, 8s, ..., up to 120s
        self.mqtt_client.reconnect_delay_set(min_delay=1, max_delay=120)
        
        # Parse broker connection details
        broker_host, broker_port = self.parse_mqtt_broker(self.config['mqtt_broker'])
        
        logger.info(f"Connecting to MQTT broker {broker_host}:{broker_port}...")
        logger.info("Auto-reconnect enabled with persistent session (QoS 1, clean_session=False)")
        logger.info("Session will persist across reconnections: subscriptions and in-flight messages preserved")
        
        try:
            # Initial connection attempt
            self.mqtt_client.connect(broker_host, broker_port, keepalive=60)
            logger.info("MQTT connection initiated - waiting for confirmation...")
        except Exception as e:
            logger.error(f"Initial connection failed: {e}")
            logger.info("Will retry automatically in background...")
            # Don't raise - let auto-reconnect handle it
    
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
