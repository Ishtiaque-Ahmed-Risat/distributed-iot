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
        self.insert_reading_v1_stmt = None
        self.insert_reading_v3_stmt = None
        self.update_latest_stmt = None
        self.write_count = 0
        self.running = False
        self.last_flush_time = time.time()
        self.buffer: List[Dict[str, Any]] = []

        # Write 1 row to sensor_readings for every N rows written to sensor_readings_v3
        self.sensor_readings_sample_every = int(self.config.get('sensor_readings_sample_every', 100))
        self._sensor_readings_seen = 0

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
            # sensor_readings (legacy table) - sampled writes
            self.insert_reading_v1_stmt = self.session.prepare("""
                INSERT INTO sensor_readings
                    (device_id, date, timestamp, sensor_type, value, unit,
                     original_value, original_unit, is_anomaly, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)
            self.insert_reading_v1_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE

            # sensor_readings_v3 (main table) - full writes
            self.insert_reading_v3_stmt = self.session.prepare("""
                INSERT INTO sensor_readings_v3
                    (device_id, date, timestamp, event_id, sensor_type, value, unit,
                    original_value, original_unit, is_anomaly, metadata)
                VALUES (?, ?, ?, now(), ?, ?, ?, ?, ?, ?, ?)
            """)
            self.insert_reading_v3_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE

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
                # Always write full-fidelity record to v3
                self.session.execute(self.insert_reading_v3_stmt, insert_params)

                # Write a sampled copy to legacy table (v1)
                self._sensor_readings_seen += 1
                if self.sensor_readings_sample_every > 0 and (
                    self._sensor_readings_seen % self.sensor_readings_sample_every == 0
                ):
                    self.session.execute(self.insert_reading_v1_stmt, insert_params)

                # Also update latest reading
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
        """
        Write a batch of records to Cassandra.

        Behavior:
        - Always write all records to sensor_readings_v3 (batched by partition (device_id, date)).
        - Update device_latest per record (individual writes to avoid huge/cross-partition batches).
        - Write 1 sampled row to sensor_readings (legacy) for every N successful v3 writes.
            Sampling is applied ONLY after a successful v3 write so it cannot be "lost" on batch failure.
        """
        if not records:
            return

        # Group by v3 partition key: (device_id, date) where date is Cassandra 'date' (python datetime.date)
        partitions: Dict[tuple, list] = {}
        skipped_prep = 0

        for record in records:
            device_id = record.get("device_id")
            if not device_id:
                skipped_prep += 1
                logger.warning(f"Skipping record: missing device_id, record={record}")
                continue

            try:
                ts = datetime.fromtimestamp(record.get("timestamp"), tz=timezone.utc)
                date_val = ts.date()                  # for sensor_readings_v3 (date type)
                date_str = ts.strftime("%Y-%m-%d")    # for sensor_readings (text type)
            except Exception as e:
                skipped_prep += 1
                logger.warning(
                    f"Skipping record: bad timestamp device_id={device_id}, ts={record.get('timestamp')}, err={e}"
                )
                continue

            key = (device_id, date_val)
            partitions.setdefault(key, []).append((record, ts, date_val, date_str))

        if skipped_prep:
            logger.warning(f"Skipped {skipped_prep} records during batch preparation")

        successful_writes = 0

        # Helper: build params safely
        def _build_metadata_map(r: Dict[str, Any]) -> Dict[str, str]:
            md = r.get("metadata")
            if md and isinstance(md, dict):
                return {str(k): str(v) for k, v in md.items()}
            return {}

        for (device_id, date_val), group in partitions.items():
            # Batch ONLY v3 inserts (single partition) -> efficient and avoids "batch too large" from mixing tables
            batch_v3 = BatchStatement(batch_type=BatchType.UNLOGGED)
            prepared = []  # keep per-record prepared info so we can update latest + sampling after success

            for record, ts, dv, ds in group:
                try:
                    metadata_map = _build_metadata_map(record)

                    v3_params = (
                        record["device_id"],
                        dv,                 # date (Cassandra date)
                        ts,                 # timestamp
                        record.get("sensor_type", "unknown"),
                        float(record.get("value", 0)),
                        record.get("unit", ""),
                        float(record.get("original_value", record.get("value", 0))),
                        record.get("original_unit", record.get("unit", "")),
                        bool(record.get("is_anomaly", False)),
                        metadata_map
                    )

                    # legacy table uses date as TEXT (schema shows date text)
                    v1_params = (
                        record["device_id"],
                        ds,                 # date (TEXT)
                        ts,
                        record.get("sensor_type", "unknown"),
                        float(record.get("value", 0)),
                        record.get("unit", ""),
                        float(record.get("original_value", record.get("value", 0))),
                        record.get("original_unit", record.get("unit", "")),
                        bool(record.get("is_anomaly", False)),
                        metadata_map
                    )

                    latest_params = (
                        record["device_id"],
                        record.get("sensor_type", "unknown"),
                        ts,
                        float(record.get("value", 0)),
                        record.get("unit", ""),
                        bool(record.get("is_anomaly", False)),
                    )

                    batch_v3.add(self.insert_reading_v3_stmt, v3_params)
                    prepared.append((v1_params, latest_params))

                except Exception as e:
                    logger.warning(
                        f"Skipping record during batch build: device_id={record.get('device_id')} err={e}"
                    )
                    continue

            if not prepared:
                continue

            # Try batch v3 write
            try:
                self.session.execute(batch_v3)

                # After successful v3 batch: update latest + sampling
                for v1_params, latest_params in prepared:
                    # latest update (separate to avoid cross-partition batches)
                    try:
                        self.session.execute(self.update_latest_stmt, latest_params)
                    except Exception as e:
                        logger.warning(f"device_latest update failed: device_id={latest_params[0]} err={e}")

                    # Count successful v3 write
                    successful_writes += 1
                    self._sensor_readings_seen += 1

                    # Sample to legacy table AFTER success, so it cannot be lost on batch failure
                    if self.sensor_readings_sample_every > 0 and (
                        self._sensor_readings_seen % self.sensor_readings_sample_every == 0
                    ):
                        try:
                            self.session.execute(self.insert_reading_v1_stmt, v1_params)
                            logger.info(
                                f"[SAMPLED] wrote 1 row to sensor_readings at seen={self._sensor_readings_seen}"
                            )
                        except Exception as e:
                            logger.warning(f"[SAMPLED-FAIL] sensor_readings insert failed: err={e}")

            except Exception as e:
                # Batch failed (e.g. "Batch too large") -> do per-record v3 writes
                logger.warning(
                    f"Batch v3 write failed for device_id={device_id}, date={date_val}: {e}. Falling back to individual writes."
                )

                # Individual writes: v3 -> latest -> sampling
                for (record, ts, dv, ds), (v1_params, latest_params) in zip(group, prepared):
                    try:
                        metadata_map = _build_metadata_map(record)
                        v3_params = (
                            record["device_id"], dv, ts,
                            record.get("sensor_type", "unknown"),
                            float(record.get("value", 0)),
                            record.get("unit", ""),
                            float(record.get("original_value", record.get("value", 0))),
                            record.get("original_unit", record.get("unit", "")),
                            bool(record.get("is_anomaly", False)),
                            metadata_map
                        )

                        self.session.execute(self.insert_reading_v3_stmt, v3_params)

                        # latest
                        try:
                            self.session.execute(self.update_latest_stmt, latest_params)
                        except Exception as le:
                            logger.warning(f"device_latest update failed: device_id={latest_params[0]} err={le}")

                        successful_writes += 1
                        self._sensor_readings_seen += 1

                        if self.sensor_readings_sample_every > 0 and (
                            self._sensor_readings_seen % self.sensor_readings_sample_every == 0
                        ):
                            try:
                                self.session.execute(self.insert_reading_v1_stmt, v1_params)
                                logger.info(
                                    f"[SAMPLED] wrote 1 row to sensor_readings at seen={self._sensor_readings_seen}"
                                )
                            except Exception as se:
                                logger.warning(f"[SAMPLED-FAIL] sensor_readings insert failed: err={se}")

                    except Exception as ie:
                        logger.warning(
                            f"Individual v3 write failed: device_id={record.get('device_id')} err={ie}"
                        )
                        continue

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
                            self.buffer.clear()
                            self.last_flush_time = now
                        else:
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
        # Write 1 row into sensor_readings for every N rows written into sensor_readings_v3
        # Set to 0 to disable legacy writes
        'sensor_readings_sample_every': int(os.getenv('SENSOR_READINGS_SAMPLE_EVERY', '1000')),
    }

    service = CassandraWriterService(config)

    try:
        service.start()
    except Exception as e:
        logger.error(f"Service failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
    