#!/usr/bin/env python3
"""
Cloud Uplink Service
Edge Redpanda → Cloud Redpanda data bridge
Consumes transformed sensor data from the edge and produces to cloud Redpanda,
preserving per-device ordering via partition key.
"""

import json
import logging
import os
import signal
import sys
import time
from typing import Dict, Any, List
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CloudUplinkService:
    """Bridges edge Redpanda to cloud Redpanda with ordering guarantees"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.consumer = None
        self.producer = None
        self.uplink_count = 0
        self.running = False
        self.last_stats_time = time.time()

    def setup_edge_consumer(self):
        """Initialize consumer for edge Redpanda"""
        try:
            self.consumer = KafkaConsumer(
                self.config['edge_topic'],
                bootstrap_servers=self.config['edge_brokers'],
                group_id=self.config['consumer_group'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='earliest',
                enable_auto_commit=False,  # Manual commit after cloud produce
                max_poll_records=self.config['batch_size']
            )
            logger.info(f"✓ Connected to edge Redpanda: {self.config['edge_brokers']}")
            logger.info(f"  Consuming from: {self.config['edge_topic']}")
        except Exception as e:
            logger.error(f"Failed to connect to edge Redpanda: {e}")
            raise

    def setup_cloud_producer(self):
        """Initialize producer for cloud Redpanda"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config['cloud_brokers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',           # Ensure cloud write is durable
                retries=10,
                max_in_flight_requests_per_connection=5,
                request_timeout_ms=15000,
                compression_type='lz4',
                linger_ms=50,         # Batch for 50ms for throughput
                batch_size=32768      # 32KB batches
            )
            logger.info(f"✓ Connected to cloud Redpanda: {self.config['cloud_brokers']}")
            logger.info(f"  Producing to: {self.config['cloud_topic']}")
        except Exception as e:
            logger.error(f"Failed to connect to cloud Redpanda: {e}")
            raise

    def uplink_batch(self, messages):
        """Send a batch of messages to cloud Redpanda, preserving device key"""
        futures = []
        for msg in messages:
            device_id = msg.key  # Already deserialized
            data = msg.value

            future = self.producer.send(
                self.config['cloud_topic'],
                key=device_id,    # Same key → same partition → ordering preserved
                value=data
            )
            futures.append(future)

        # Wait for all sends to complete
        errors = 0
        for future in futures:
            try:
                future.get(timeout=15)
            except KafkaError as e:
                logger.error(f"Failed to send message to cloud: {e}")
                errors += 1

        if errors > 0:
            logger.warning(f"Batch had {errors}/{len(futures)} errors")
            return False

        return True

    def start(self):
        """Start the uplink service"""
        logger.info("=" * 50)
        logger.info("Cloud Uplink Service")
        logger.info("=" * 50)
        logger.info(f"[CONFIG] Edge Brokers: {self.config['edge_brokers']}")
        logger.info(f"[CONFIG] Edge Topic: {self.config['edge_topic']}")
        logger.info(f"[CONFIG] Cloud Brokers: {self.config['cloud_brokers']}")
        logger.info(f"[CONFIG] Cloud Topic: {self.config['cloud_topic']}")
        logger.info(f"[CONFIG] Batch Size: {self.config['batch_size']}")

        # Setup connections
        self.setup_edge_consumer()
        self.setup_cloud_producer()

        self.running = True
        logger.info("Cloud Uplink Service started successfully")

        # Main processing loop
        try:
            while self.running:
                # Poll edge Redpanda for messages
                records = self.consumer.poll(
                    timeout_ms=self.config['poll_interval_ms'],
                    max_records=self.config['batch_size']
                )

                if not records:
                    continue

                # Flatten all messages from all partitions
                messages = []
                for tp, msgs in records.items():
                    messages.extend(msgs)

                if not messages:
                    continue

                # Send batch to cloud
                success = self.uplink_batch(messages)

                if success:
                    # Commit offsets only after successful cloud write
                    self.consumer.commit()
                    self.uplink_count += len(messages)

                    # Log stats periodically
                    now = time.time()
                    if now - self.last_stats_time >= 30:
                        logger.info(
                            f"[STATS] Uplinked {self.uplink_count} messages total "
                            f"(batch: {len(messages)})"
                        )
                        self.last_stats_time = now
                else:
                    logger.warning("Batch failed — will retry on next poll (no offset commit)")

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            self.stop()

    def stop(self):
        """Stop the uplink service"""
        logger.info("Shutting down Cloud Uplink Service...")
        self.running = False

        if self.producer:
            self.producer.flush()
            self.producer.close()

        if self.consumer:
            self.consumer.close()

        logger.info(f"Total messages uplinked: {self.uplink_count}")
        logger.info("Service stopped")


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}")
    sys.exit(0)


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    config = {
        'edge_brokers': os.getenv('EDGE_REDPANDA_BROKERS', 'redpanda:9092').split(','),
        'edge_topic': os.getenv('EDGE_REDPANDA_TOPIC', 'transformed-sensor-data'),
        'cloud_brokers': os.getenv('CLOUD_REDPANDA_BROKERS', 'localhost:29092').split(','),
        'cloud_topic': os.getenv('CLOUD_REDPANDA_TOPIC', 'edge-sensor-data'),
        'consumer_group': os.getenv('CONSUMER_GROUP', 'cloud-uplink'),
        'batch_size': int(os.getenv('UPLINK_BATCH_SIZE', '500')),
        'poll_interval_ms': int(os.getenv('POLL_INTERVAL_MS', '1000')),
    }

    service = CloudUplinkService(config)

    try:
        service.start()
    except Exception as e:
        logger.error(f"Service failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
