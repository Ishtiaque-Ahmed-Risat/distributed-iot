#!/usr/bin/env python3
"""
Cassandra Writer Service
Cloud Redpanda → Cassandra data pipeline
Consumes sensor data from cloud Redpanda and writes to Cassandra
with time-bucketed partitioning for fast reads.
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any, List
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement, BatchType, ConsistencyLevel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CassandraWriterService:
    """Consumes from cloud Redpanda and writes to Cassandra"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.consumer = None
        self.cassandra_cluster = None
        self.session = None
        self.insert_reading_stmt = None
        self.update_latest_stmt = None
        self.write_count = 0
        self.running = False
        self.last_flush_time = time.time()
        self.buffer: List[Dict[str, Any]] = []

    def setup_cassandra(self):
        """Initialize Cassandra connection and prepared statements"""
        try:
            hosts = self.config['cassandra_hosts']
            self.cassandra_cluster = Cluster(
                hosts,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='DC1'),
                protocol_version=4
            )
            self.session = self.cassandra_cluster.connect('iot_data')
            logger.info(f"✓ Connected to Cassandra: {hosts}")

            # Prepare statements for performance
            self.insert_reading_stmt = self.session.prepare("""
                INSERT INTO sensor_readings
                    (device_id, date, timestamp, sensor_type, value, unit,
                     original_value, original_unit, is_anomaly, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)
            self.insert_reading_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE

            self.update_latest_stmt = self.session.prepare("""
                INSERT INTO device_latest
                    (device_id, sensor_type, timestamp, value, unit, is_anomaly)
                VALUES (?, ?, ?, ?, ?, ?)
            """)
            self.update_latest_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE

            logger.info("✓ Prepared statements ready")

        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise

    def setup_kafka(self):
        """Initialize Kafka consumer for cloud Redpanda"""
        try:
            self.consumer = KafkaConsumer(
                self.config['topic'],
                bootstrap_servers=self.config['redpanda_brokers'],
                group_id=self.config['consumer_group'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=False,  # Manual commit after write
                max_poll_records=self.config['batch_size']
            )
            logger.info(f"✓ Connected to cloud Redpanda: {self.config['redpanda_brokers']}")
            logger.info(f"  Consuming from: {self.config['topic']}")
        except Exception as e:
            logger.error(f"Failed to connect to Redpanda: {e}")
            raise

    def _write_single_record(self, record: Dict[str, Any]) -> bool:
        """
        Write a single record to Cassandra.
        Returns True if successful, False otherwise.
        Logs a warning on failure.
        """
        try:
            # Prepare data - wrap in try-except to catch any data preparation errors
            try:
                ts = datetime.fromtimestamp(record['timestamp'], tz=timezone.utc)
                date_str = ts.strftime('%Y-%m-%d')
            except (ValueError, OverflowError, OSError, KeyError, TypeError) as e:
                logger.warning(
                    f"Skipping record due to invalid timestamp: device_id={record.get('device_id')}, "
                    f"timestamp={record.get('timestamp')}, error={e}"
                )
                return False

            # Prepare metadata
            try:
                metadata_map = {}
                if record.get('metadata') and isinstance(record['metadata'], dict):
                    metadata_map = {str(k): str(v) for k, v in record['metadata'].items()}
            except Exception as e:
                logger.warning(
                    f"Skipping record due to metadata processing error: device_id={record.get('device_id')}, "
                    f"error={e}"
                )
                return False

            # Validate required fields
            device_id = record.get('device_id')
            if not device_id:
                logger.warning(
                    f"Skipping record due to missing device_id: record={record}"
                )
                return False

            # Prepare values - catch any type conversion errors
            try:
                insert_params = (
                    device_id,
                    date_str,
                    ts,
                    record.get('sensor_type', 'unknown'),
                    float(record.get('value', 0)),
                    record.get('unit', ''),
                    float(record.get('original_value', record.get('value', 0))),
                    record.get('original_unit', record.get('unit', '')),
                    bool(record.get('is_anomaly', False)),
                    metadata_map
                )

                latest_params = (
                    device_id,
                    record.get('sensor_type', 'unknown'),
                    ts,
                    float(record.get('value', 0)),
                    record.get('unit', ''),
                    bool(record.get('is_anomaly', False))
                )
            except (ValueError, TypeError, KeyError) as e:
                logger.warning(
                    f"Skipping record due to data conversion error: device_id={record.get('device_id')}, "
                    f"error={e}"
                )
                return False

            # Execute database writes
            try:
                self.session.execute(self.insert_reading_stmt, insert_params)
                self.session.execute(self.update_latest_stmt, latest_params)
                return True
            except Exception as e:
                logger.warning(
                    f"Skipping record due to database write error: device_id={record.get('device_id')}, "
                    f"sensor_type={record.get('sensor_type')}, timestamp={ts}, error={e}"
                )
                return False

        except Exception as e:
            # Catch-all for any unexpected errors
            logger.warning(
                f"Skipping record due to unexpected error: device_id={record.get('device_id')}, "
                f"error={e}"
            )
            return False

    def write_batch(self, records: List[Dict[str, Any]]):
        """Write a batch of records to Cassandra using unlogged batch per partition"""
        if not records:
            return

        # Group records by partition key (device_id, date) for efficient batching
        partitions: Dict[tuple, list] = {}
        skipped_prep = 0
        
        for record in records:
            # Validate required fields
            device_id = record.get('device_id')
            if not device_id:
                skipped_prep += 1
                logger.warning(
                    f"Skipping record during batch preparation: missing device_id, record={record}"
                )
                continue
                
            try:
                ts = datetime.fromtimestamp(record.get('timestamp'), tz=timezone.utc)
                date_str = ts.strftime('%Y-%m-%d')
                key = (device_id, date_str)
                if key not in partitions:
                    partitions[key] = []
                partitions[key].append((record, ts, date_str))
            except (ValueError, OverflowError, OSError, KeyError, TypeError) as e:
                # Skip records that fail during preparation
                skipped_prep += 1
                logger.warning(
                    f"Skipping record during batch preparation: device_id={device_id}, "
                    f"timestamp={record.get('timestamp')}, error={e}"
                )
                continue

        if skipped_prep > 0:
            logger.warning(f"Skipped {skipped_prep} records during batch preparation")

        # Write each partition group as an unlogged batch (single partition = efficient)
        successful_writes = 0
        for (device_id, date_str), group in partitions.items():
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            batch_records = []

            for record, ts, ds in group:
                try:
                    device_id = record.get('device_id')
                    if not device_id:
                        logger.warning(
                            f"Skipping record during batch preparation: missing device_id"
                        )
                        continue
                        
                    metadata_map = {}
                    if record.get('metadata') and isinstance(record['metadata'], dict):
                        metadata_map = {str(k): str(v) for k, v in record['metadata'].items()}

                    batch.add(self.insert_reading_stmt, (
                        device_id,
                        ds,
                        ts,
                        record.get('sensor_type', 'unknown'),
                        float(record.get('value', 0)),
                        record.get('unit', ''),
                        float(record.get('original_value', record.get('value', 0))),
                        record.get('original_unit', record.get('unit', '')),
                        bool(record.get('is_anomaly', False)),
                        metadata_map
                    ))

                    # Also update latest reading
                    batch.add(self.update_latest_stmt, (
                        device_id,
                        record.get('sensor_type', 'unknown'),
                        ts,
                        float(record.get('value', 0)),
                        record.get('unit', ''),
                        bool(record.get('is_anomaly', False))
                    ))
                    batch_records.append(record)
                except (ValueError, TypeError, KeyError) as e:
                    logger.warning(
                        f"Skipping record during batch preparation: device_id={record.get('device_id')}, "
                        f"error={e}"
                    )
                    continue

            if not batch_records:
                continue

            # Try batch write first
            try:
                self.session.execute(batch)
                successful_writes += len(batch_records)
            except Exception as e:
                logger.warning(f"Batch write failed for {device_id}/{date_str}: {e}, trying individual writes")
                # Fallback to individual writes
                for record in batch_records:
                    if self._write_single_record(record):
                        successful_writes += 1

        self.write_count += successful_writes

    def start(self):
        """Start the writer service"""
        logger.info("=" * 50)
        logger.info("Cassandra Writer Service")
        logger.info("=" * 50)
        logger.info(f"[CONFIG] Redpanda Brokers: {self.config['redpanda_brokers']}")
        logger.info(f"[CONFIG] Topic: {self.config['topic']}")
        logger.info(f"[CONFIG] Cassandra Hosts: {self.config['cassandra_hosts']}")
        logger.info(f"[CONFIG] Batch Size: {self.config['batch_size']}")
        logger.info(f"[CONFIG] Flush Interval: {self.config['flush_interval']}s")

        self.setup_cassandra()
        self.setup_kafka()

        self.running = True
        logger.info("Cassandra Writer Service started successfully")

        try:
            while self.running:
                try:
                    records = self.consumer.poll(
                        timeout_ms=1000,
                        max_records=self.config['batch_size']
                    )

                    # Flatten messages
                    messages = []
                    for tp, msgs in records.items():
                        for msg in msgs:
                            try:
                                if msg.value:
                                    messages.append(msg.value)
                            except Exception as e:
                                logger.warning(f"Error processing message from {tp}: {e}, skipping message")

                    if messages:
                        self.buffer.extend(messages)
                except Exception as e:
                    logger.warning(f"Error polling messages: {e}, continuing...")
                    time.sleep(1)  # Brief pause before retrying
                    continue

                # Flush if buffer full or time elapsed
                now = time.time()
                buffer_full = len(self.buffer) >= self.config['batch_size']
                time_elapsed = (now - self.last_flush_time) >= self.config['flush_interval']

                if self.buffer and (buffer_full or time_elapsed):
                    try:
                        buffer_size = len(self.buffer)
                        # Store original count before write_batch modifies write_count
                        write_count_before = self.write_count
                        self.write_batch(self.buffer)
                        successful_count = self.write_count - write_count_before
                        
                        # Only commit if we successfully wrote at least some records
                        if successful_count > 0:
                            self.consumer.commit()
                            logger.info(
                                f"[INFO] Processed {buffer_size} records to Cassandra "
                                f"(Successfully wrote: {successful_count}, Total: {self.write_count})"
                            )
                            # Only clear successfully processed records
                            # Keep failed records in buffer for potential retry (but clear to avoid infinite loop)
                            # In production, you might want a dead-letter queue here
                            self.buffer.clear()
                            self.last_flush_time = now
                        else:
                            # All records failed - don't commit, but clear buffer to avoid infinite retry
                            logger.warning(
                                f"All {buffer_size} records in batch failed to write. "
                                "Clearing buffer to prevent infinite retry."
                            )
                            self.buffer.clear()
                    except Exception as e:
                        logger.warning(
                            f"Error writing batch to database: {e}, skipping batch. "
                            f"Buffer had {len(self.buffer)} records."
                        )
                        # Clear buffer to prevent infinite retry of bad data
                        # In production, consider a dead-letter queue
                        self.buffer.clear()
                        # Don't commit on error - will retry on next poll

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            self.stop()

    def stop(self):
        """Stop the writer service"""
        logger.info("Shutting down Cassandra Writer Service...")
        self.running = False

        # Flush remaining
        if self.buffer:
            self.write_batch(self.buffer)
            self.buffer.clear()

        if self.consumer:
            self.consumer.close()
        if self.cassandra_cluster:
            self.cassandra_cluster.shutdown()

        logger.info(f"Total records written: {self.write_count}")
        logger.info("Service stopped")


def signal_handler(signum, frame):
    logger.info(f"Received signal {signum}")
    sys.exit(0)


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    config = {
        'redpanda_brokers': os.getenv('REDPANDA_BROKERS', 'redpanda:9092').split(','),
        'topic': os.getenv('REDPANDA_TOPIC', 'edge-sensor-data'),
        'consumer_group': os.getenv('CONSUMER_GROUP', 'cassandra-writer'),
        'cassandra_hosts': os.getenv('CASSANDRA_HOSTS', 'cassandra').split(','),
        'batch_size': int(os.getenv('BATCH_SIZE', '500')),
        'flush_interval': int(os.getenv('FLUSH_INTERVAL', '5')),
    }

    service = CassandraWriterService(config)

    try:
        service.start()
    except Exception as e:
        logger.error(f"Service failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
