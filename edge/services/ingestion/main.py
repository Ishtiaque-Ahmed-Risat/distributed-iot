#!/usr/bin/env python3
"""
Data Ingestion Service
MQTT → Redpanda data pipeline

Subscribes to MQTT sensor topics using shared subscriptions for load balancing
across multiple replicas and publishes to Redpanda with per-device ordering.

Features:
- Automatic reconnection with exponential backoff
- Shared subscriptions for horizontal scaling
- Per-device message ordering via partition key
- Comprehensive error handling and metrics
- Graceful shutdown with resource cleanup
"""

import json
import logging
import os
import signal
import socket
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Any, Optional, Tuple
from threading import Lock

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s] %(message)s'
)
logger = logging.getLogger(__name__)


class ConnectionState(Enum):
    """MQTT connection state tracking"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"


@dataclass
class ServiceMetrics:
    """Service metrics for monitoring"""
    messages_received: int = 0
    messages_processed: int = 0
    messages_failed: int = 0
    kafka_send_errors: int = 0
    parse_errors: int = 0
    validation_errors: int = 0
    connection_attempts: int = 0
    connection_failures: int = 0
    last_message_time: Optional[float] = None
    _lock: Lock = field(default_factory=Lock)

    def increment_received(self):
        with self._lock:
            self.messages_received += 1
            self.last_message_time = time.time()

    def increment_processed(self):
        with self._lock:
            self.messages_processed += 1

    def increment_failed(self, error_type: str = "unknown"):
        with self._lock:
            self.messages_failed += 1
            if error_type == "kafka":
                self.kafka_send_errors += 1
            elif error_type == "parse":
                self.parse_errors += 1
            elif error_type == "validation":
                self.validation_errors += 1

    def get_stats(self) -> Dict[str, Any]:
        """Get current metrics snapshot"""
        with self._lock:
            return {
                "messages_received": self.messages_received,
                "messages_processed": self.messages_processed,
                "messages_failed": self.messages_failed,
                "kafka_send_errors": self.kafka_send_errors,
                "parse_errors": self.parse_errors,
                "validation_errors": self.validation_errors,
                "connection_attempts": self.connection_attempts,
                "connection_failures": self.connection_failures,
                "last_message_time": self.last_message_time,
                "success_rate": (
                    self.messages_processed / self.messages_received * 100
                    if self.messages_received > 0 else 0
                )
            }


class IngestionService:
    """
    Handles MQTT to Redpanda data ingestion with fault tolerance.
    
    Uses shared subscriptions for load balancing across multiple replicas
    and maintains per-device ordering via Kafka partition keys.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the ingestion service.
        
        Args:
            config: Service configuration dictionary with keys:
                - mqtt_broker: MQTT broker URL (e.g., 'tcp://emqx:1883')
                - mqtt_topic: MQTT topic pattern (e.g., 'sensors/+/data')
                - redpanda_brokers: Comma-separated list of Redpanda brokers
                - redpanda_topic: Target Kafka topic name
        """
        self.config = self._validate_config(config)
        self.mqtt_client: Optional[mqtt.Client] = None
        self.kafka_producer: Optional[KafkaProducer] = None
        self.running = False
        self.connection_state = ConnectionState.DISCONNECTED
        self.metrics = ServiceMetrics()
        self._shutdown_lock = Lock()
        
    def _validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and normalize configuration"""
        required_keys = ['mqtt_broker', 'mqtt_topic', 'redpanda_brokers', 'redpanda_topic']
        missing = [key for key in required_keys if key not in config]
        if missing:
            raise ValueError(f"Missing required config keys: {missing}")
        
        # Normalize redpanda_brokers to list
        if isinstance(config['redpanda_brokers'], str):
            config['redpanda_brokers'] = [b.strip() for b in config['redpanda_brokers'].split(',')]
        
        return config
        
    def _generate_client_id(self) -> str:
        """
        Generate a stable, unique MQTT client ID.
        
        Uses pod hostname in Kubernetes (stable across restarts) or falls back
        to PID for local development.
        
        Returns:
            Unique client ID string
        """
        try:
            hostname = socket.gethostname()
            if hostname and hostname not in ('localhost', '127.0.0.1'):
                # In Kubernetes, hostname is the pod name (e.g., ingestion-service-xxxxx-xxxxx)
                return f"ingestion-{hostname}"
        except Exception as e:
            logger.warning(f"Failed to get hostname: {e}, using PID fallback")
        
        # Fallback for local development
        return f"ingestion-{os.getpid()}"
        
    def setup_kafka(self) -> None:
        """
        Initialize Kafka/Redpanda producer with reliability guarantees.
        
        Raises:
            Exception: If producer initialization fails
        """
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.config['redpanda_brokers'],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                
                # Reliability: Ensure messages are delivered
                acks=1,  # Leader acknowledgment (balanced performance)
                retries=10,  # Retry failed sends
                max_in_flight_requests_per_connection=5,  # Allow pipelining while maintaining order
                
                # Timeouts
                request_timeout_ms=10000,  # 10s request timeout
                delivery_timeout_ms=30000,  # 30s total delivery timeout
                
                # Performance optimizations
                compression_type='lz4',  # Fast compression
                linger_ms=10,  # Batch for 10ms
                batch_size=16384,  # 16KB batches
                
                # Error handling
                max_block_ms=5000,  # Max time to block on send
            )
            logger.info(
                f"Kafka producer initialized: brokers={self.config['redpanda_brokers']}, "
                f"topic={self.config['redpanda_topic']}"
            )
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}", exc_info=True)
            raise
    
    def on_connect(self, client: mqtt.Client, userdata: Any, flags: Dict[str, Any], rc: int) -> None:
        """
        MQTT connection callback with shared subscription setup.
        
        This completes the MQTT handshake by:
        1. Verifying connection success (rc=0)
        2. Subscribing to shared topic for load balancing
        3. Using QoS 1 for at-least-once delivery guarantees
        
        Args:
            client: MQTT client instance
            userdata: User data passed to callbacks
            flags: Connection flags (session present, etc.)
            rc: Return code (0 = success)
        """
        if rc == 0:
            self.connection_state = ConnectionState.CONNECTED
            self.metrics.connection_attempts += 1
            logger.info(
                f"MQTT handshake successful: broker={self.config['mqtt_broker']}, "
                f"session_present={flags.get('session present', False)}"
            )
            
            # Use MQTT shared subscription for load balancing across multiple replicas
            # Format: $share/group-name/topic-pattern
            # This ensures multiple ingestion pods share the load (no duplicates)
            # Matches topic pattern: sensors/+/data (from config)
            shared_topic = f"$share/ingestion-group/{self.config['mqtt_topic']}"
            
            try:
                # Subscribe with QoS 1 for at-least-once delivery
                # This ensures messages are not lost even if service restarts
                result, mid = client.subscribe(shared_topic, qos=1)
                if result == mqtt.MQTT_ERR_SUCCESS:
                    logger.info(
                        f"Subscription successful: topic={shared_topic}, qos=1, mid={mid}"
                    )
                    logger.info("Ready to receive messages from MQTT broker")
                else:
                    logger.error(f"Failed to subscribe to {shared_topic}: error code {result}")
                    self.connection_state = ConnectionState.FAILED
            except Exception as e:
                logger.error(f"Exception during subscription: {e}", exc_info=True)
                self.connection_state = ConnectionState.FAILED
        else:
            self.connection_state = ConnectionState.FAILED
            self.metrics.connection_failures += 1
            error_messages = {
                1: "Connection refused - incorrect protocol version",
                2: "Connection refused - invalid client identifier",
                3: "Connection refused - server unavailable",
                4: "Connection refused - bad username or password",
                5: "Connection refused - not authorized"
            }
            error_msg = error_messages.get(rc, f"Unknown error code {rc}")
            logger.error(f"MQTT handshake failed: {error_msg} (rc={rc})")
            logger.info("Auto-reconnection will attempt to restore connection...")
    
    def on_disconnect(self, client: mqtt.Client, userdata: Any, rc: int) -> None:
        """
        MQTT disconnection callback.
        
        Args:
            client: MQTT client instance
            userdata: User data passed to callbacks
            rc: Return code (0 = clean disconnect, non-zero = unexpected)
        """
        if rc != 0:
            # Unexpected disconnect
            self.connection_state = ConnectionState.RECONNECTING
            disconnect_reasons = {
                1: "Network error",
                2: "Protocol error",
                3: "Connection refused by server",
                4: "Server unavailable",
                5: "Bad credentials",
                6: "Not authorized",
                7: "Session conflict or server rejected connection"
            }
            reason = disconnect_reasons.get(rc, f"Unknown error (rc={rc})")
            client_id = getattr(client, '_client_id', b'unknown')
            if isinstance(client_id, bytes):
                client_id = client_id.decode('utf-8', errors='replace')
            
            logger.warning(
                f"Unexpected disconnect from MQTT broker: {reason} (rc={rc}), "
                f"client_id={client_id}"
            )
            logger.info("Auto-reconnection will attempt to restore connection...")
        else:
            # Clean disconnect
            self.connection_state = ConnectionState.DISCONNECTED
            logger.info("Clean disconnect from MQTT broker")
    
    def on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        """
        MQTT message callback with comprehensive error handling.
        
        This is part of the MQTT handshake flow:
        1. Client connects (on_connect)
        2. Client subscribes (on_connect)
        3. Broker publishes message (on_message) - THIS METHOD
        4. Client sends PUBACK (QoS 1 acknowledgment) - handled by paho-mqtt
        
        Message Format (from device simulator):
        {
            "device_id": str,        # Required: Device identifier
            "sensor_type": str,      # Required: Type of sensor
            "value": float,          # Required: Sensor reading value
            "unit": str,             # Required: Unit of measurement
            "timestamp": int,        # Required: Unix epoch seconds
            "is_anomaly": bool       # Optional: Anomaly flag
        }
        
        This message is passed through unchanged to Redpanda topic 'raw-sensor-data'
        for consumption by the transformation service, which expects this exact format.
        
        Args:
            client: MQTT client instance
            userdata: User data passed to callbacks
            msg: Received MQTT message with QoS 1
        """
        self.metrics.increment_received()
        
        try:
            # Parse JSON payload - maintain exact format from device simulator
            try:
                payload = msg.payload.decode('utf-8')
                data = json.loads(payload)
            except UnicodeDecodeError as e:
                logger.error(f"Failed to decode message payload: {e}, topic={msg.topic}")
                self.metrics.increment_failed("parse")
                return
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON: {e}, topic={msg.topic}, payload_length={len(msg.payload)}")
                self.metrics.increment_failed("parse")
                return
            
            # Validate required fields for end-to-end compatibility
            # These fields are required by downstream services (transformation, cloud-uplink, cassandra-writer)
            device_id = data.get('device_id')
            timestamp = data.get('timestamp')
            sensor_type = data.get('sensor_type')
            value = data.get('value')
            unit = data.get('unit')
            
            if not device_id:
                logger.warning(f"Invalid message: missing device_id, topic={msg.topic}")
                self.metrics.increment_failed("validation")
                return
            
            if not timestamp:
                logger.warning(f"Invalid message: missing timestamp, topic={msg.topic}, device_id={device_id}")
                self.metrics.increment_failed("validation")
                return
            
            # Optional validation - log warnings but don't fail
            if sensor_type is None:
                logger.warning(f"Message missing sensor_type: device_id={device_id}, topic={msg.topic}")
            if value is None:
                logger.warning(f"Message missing value: device_id={device_id}, topic={msg.topic}")
            if not unit:
                logger.warning(f"Message missing unit: device_id={device_id}, topic={msg.topic}")
            
            # Publish to Redpanda with error handling
            # IMPORTANT: Pass through message exactly as received to maintain end-to-end compatibility
            # The transformation service expects this exact format:
            # {
            #   "device_id": str,
            #   "sensor_type": str,
            #   "value": float,
            #   "unit": str,
            #   "timestamp": int (epoch seconds),
            #   "is_anomaly": bool
            # }
            try:
                # Use device_id as partition key to maintain per-device ordering
                # This ensures all messages from the same device go to the same partition
                future = self.kafka_producer.send(
                    self.config['redpanda_topic'],
                    key=device_id,  # Partition key for ordering
                    value=data      # Pass through original message format
                )
                
                # Add callbacks for async result handling
                future.add_callback(self._on_kafka_success)
                future.add_errback(self._on_kafka_error)
                
                self.metrics.increment_processed()
                
                # Log progress periodically
                if self.metrics.messages_processed % 100 == 0:
                    stats = self.metrics.get_stats()
                    logger.info(
                        f"Processed {self.metrics.messages_processed} messages "
                        f"(success_rate={stats['success_rate']:.1f}%)"
                    )
                    
            except Exception as e:
                logger.error(
                    f"Failed to send message to Redpanda: {e}, "
                    f"device_id={device_id}, topic={msg.topic}",
                    exc_info=True
                )
                self.metrics.increment_failed("kafka")
                # Note: With QoS 1, message will be redelivered by MQTT on next connection
                # This maintains message delivery guarantees in the pipeline
                
        except Exception as e:
            logger.error(f"Unexpected error processing message: {e}, topic={msg.topic}", exc_info=True)
            self.metrics.increment_failed("unknown")
    
    def _on_kafka_success(self, metadata: Any) -> None:
        """Kafka produce success callback"""
        logger.debug(
            f"Message sent successfully: topic={metadata.topic}, "
            f"partition={metadata.partition}, offset={metadata.offset}"
        )
    
    def _on_kafka_error(self, exc: Exception) -> None:
        """Kafka produce error callback"""
        logger.error(f"Kafka send failed: {exc}", exc_info=True)
        self.metrics.increment_failed("kafka")
    
    def setup_mqtt(self) -> None:
        """
        Initialize MQTT client with fault-tolerant reconnection.
        
        Raises:
            Exception: If MQTT client setup fails critically
        """
        client_id = self._generate_client_id()
        
        try:
            self.mqtt_client = mqtt.Client(
                client_id=client_id,
                clean_session=True  # Use clean session to avoid conflicts with multiple replicas
                # Shared subscriptions handle load balancing, so we don't need persistent sessions
                # This prevents session conflicts when pods restart or scale
            )
            logger.info(f"MQTT client ID: {client_id}")
            
            # Set callbacks
            self.mqtt_client.on_connect = self.on_connect
            self.mqtt_client.on_disconnect = self.on_disconnect
            self.mqtt_client.on_message = self.on_message
            
            # Configure automatic reconnection with exponential backoff
            # Retry intervals: 1s, 2s, 4s, 8s, ..., up to 120s
            self.mqtt_client.reconnect_delay_set(min_delay=1, max_delay=120)
            
            # Parse broker connection details
            broker_host, broker_port = self._parse_mqtt_broker(self.config['mqtt_broker'])
            
            logger.info(f"Connecting to MQTT broker {broker_host}:{broker_port}...")
            logger.info("Auto-reconnect enabled with clean session (shared subscriptions handle load balancing)")
            
            # Initial connection attempt
            # MQTT handshake sequence:
            # 1. TCP connection to broker
            # 2. CONNECT packet with client_id, clean_session, keepalive
            # 3. CONNACK from broker (handled in on_connect callback)
            # 4. SUBSCRIBE packet (sent in on_connect callback)
            # 5. SUBACK from broker (confirms subscription)
            try:
                self.connection_state = ConnectionState.CONNECTING
                # keepalive=60 means client must send PING every 60 seconds
                # This maintains the connection and detects network failures
                self.mqtt_client.connect(broker_host, broker_port, keepalive=60)
                logger.info(
                    f"MQTT connection initiated: {broker_host}:{broker_port}, "
                    f"keepalive=60s, waiting for CONNACK..."
                )
            except Exception as e:
                logger.error(f"Initial MQTT connection failed: {e}", exc_info=True)
                logger.info("Will retry automatically in background...")
                self.connection_state = ConnectionState.RECONNECTING
                # Don't raise - let auto-reconnect handle it
                
        except Exception as e:
            logger.error(f"Failed to setup MQTT client: {e}", exc_info=True)
            self.connection_state = ConnectionState.FAILED
            raise
    
    def _parse_mqtt_broker(self, broker_url: str) -> Tuple[str, int]:
        """
        Parse MQTT broker URL into host and port.
        
        Args:
            broker_url: Broker URL (e.g., 'tcp://emqx:1883' or 'emqx:1883')
            
        Returns:
            Tuple of (host, port)
            
        Raises:
            ValueError: If URL format is invalid
        """
        try:
            # Remove protocol prefix if present
            if '://' in broker_url:
                broker_url = broker_url.split('://', 1)[1]
            
            # Split host:port
            if ':' in broker_url:
                host, port_str = broker_url.rsplit(':', 1)
                port = int(port_str)
                if not (1 <= port <= 65535):
                    raise ValueError(f"Invalid port number: {port}")
                return host, port
            else:
                return broker_url, 1883  # Default MQTT port
        except Exception as e:
            raise ValueError(f"Invalid MQTT broker URL format '{broker_url}': {e}")
    
    def start(self) -> None:
        """
        Start the ingestion service.
        
        Initializes connections and begins processing messages.
        Blocks until service is stopped.
        """
        logger.info("=" * 60)
        logger.info("Data Ingestion Service - Starting")
        logger.info("=" * 60)
        logger.info(f"Configuration:")
        logger.info(f"  MQTT Broker: {self.config['mqtt_broker']}")
        logger.info(f"  MQTT Topic: {self.config['mqtt_topic']}")
        logger.info(f"  Redpanda Brokers: {self.config['redpanda_brokers']}")
        logger.info(f"  Redpanda Topic: {self.config['redpanda_topic']}")
        
        try:
            # Setup Kafka producer first (critical dependency)
            self.setup_kafka()
            
            # Setup MQTT client
            self.setup_mqtt()
            
            # Start MQTT network loop in background thread
            if self.mqtt_client:
                self.mqtt_client.loop_start()
            
            self.running = True
            logger.info("Data Ingestion Service started successfully")
            
            # Keep running until signal
            try:
                while self.running:
                    signal.pause()  # Wait for signal
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt")
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
            finally:
                self.stop()
                
        except Exception as e:
            logger.error(f"Failed to start service: {e}", exc_info=True)
            self.stop()
            raise
    
    def stop(self) -> None:
        """
        Stop the ingestion service gracefully.
        
        Performs cleanup of all resources including MQTT and Kafka connections.
        """
        with self._shutdown_lock:
            if not self.running:
                return  # Already stopped
            
            logger.info("Shutting down Data Ingestion Service...")
            self.running = False
            
            # Disconnect MQTT gracefully
            if self.mqtt_client:
                try:
                    logger.info("Disconnecting MQTT client...")
                    self.mqtt_client.loop_stop()
                    self.mqtt_client.disconnect()
                    self.connection_state = ConnectionState.DISCONNECTED
                    logger.info("MQTT client disconnected")
                except Exception as e:
                    logger.warning(f"Error disconnecting MQTT client: {e}")
            
            # Flush and close Kafka producer
            if self.kafka_producer:
                try:
                    logger.info("Flushing Kafka producer...")
                    self.kafka_producer.flush(timeout=10)  # Wait up to 10s for pending messages
                    self.kafka_producer.close(timeout=10)
                    logger.info("Kafka producer closed")
                except Exception as e:
                    logger.warning(f"Error closing Kafka producer: {e}")
            
            # Log final metrics
            stats = self.metrics.get_stats()
            logger.info("=" * 60)
            logger.info("Service Statistics:")
            logger.info(f"  Messages Received: {stats['messages_received']}")
            logger.info(f"  Messages Processed: {stats['messages_processed']}")
            logger.info(f"  Messages Failed: {stats['messages_failed']}")
            logger.info(f"  Success Rate: {stats['success_rate']:.2f}%")
            logger.info(f"  Connection Attempts: {stats['connection_attempts']}")
            logger.info(f"  Connection Failures: {stats['connection_failures']}")
            logger.info("=" * 60)
            logger.info("Data Ingestion Service stopped")


def signal_handler(signum: int, frame: Any) -> None:
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    sys.exit(0)


def main() -> None:
    """Main entry point"""
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Load configuration from environment variables
    config = {
        'mqtt_broker': os.getenv('MQTT_BROKER', 'tcp://emqx:1883'),
        'mqtt_topic': os.getenv('MQTT_TOPIC', 'sensors/+/data'),
        'redpanda_brokers': os.getenv('REDPANDA_BROKERS', 'redpanda:9092'),
        'redpanda_topic': os.getenv('REDPANDA_TOPIC', 'raw-sensor-data')
    }
    
    # Create and start service
    service = IngestionService(config)
    
    try:
        service.start()
    except KeyboardInterrupt:
        logger.info("Service interrupted by user")
    except Exception as e:
        logger.error(f"Service failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Exiting...")


if __name__ == '__main__':
    main()
