#!/usr/bin/env python3
"""
InfluxDB Writer Service
Redpanda → InfluxDB data pipeline
Consumes transformed sensor data and writes to InfluxDB
"""

import json
import logging
import os
import signal
import sys
from typing import Dict, Any, List
from datetime import datetime
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class InfluxWriterService:
    """Handles Redpanda to InfluxDB writes"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.consumer = None
        self.influx_client = None
        self.write_api = None
        self.point_buffer: List[Point] = []
        self.write_count = 0
        self.running = False
        self.last_flush_time = time.time()
    
    def setup_influxdb(self):
        """Initialize InfluxDB client"""
        try:
            self.influx_client = InfluxDBClient(
                url=self.config['influx_url'],
                token=self.config['influx_token'],
                org=self.config['influx_org']
            )
            
            # Test connection
            health = self.influx_client.health()
            logger.info(f"[INFO] InfluxDB health: {health.status}")
            
            # Create write API (with batching)
            self.write_api = self.influx_client.write_api(
                write_options=ASYNCHRONOUS
            )
            
            logger.info(f"Connected to InfluxDB: {self.config['influx_url']}")
            logger.info(f"Writing to bucket: {self.config['influx_bucket']}")
            
        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB: {e}")
            raise
    
    def setup_kafka(self):
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                self.config['redpanda_topic'],
                bootstrap_servers=self.config['redpanda_brokers'],
                group_id=self.config['consumer_group'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                max_poll_records=100
            )
            
            logger.info(f"Connected to Redpanda: {self.config['redpanda_brokers']}")
            logger.info(f"Consuming from: {self.config['redpanda_topic']}")
            
        except Exception as e:
            logger.error(f"Failed to setup Kafka: {e}")
            raise
    
    def create_point(self, data: Dict[str, Any]) -> Point:
        """Create InfluxDB point from sensor data"""
        # Create point with measurement name
        point = Point("sensor_data")
        
        # Add tags (indexed)
        point.tag("device_id", data['device_id'])
        point.tag("sensor_type", data['sensor_type'])
        point.tag("unit", data['unit'])
        point.tag("original_unit", data['original_unit'])
        point.tag("transformed_by", data['transformed_by'])
        
        if data.get('transformation_id'):
            point.tag("transformation_id", data['transformation_id'])
        
        # Add fields (not indexed)
        point.field("value", float(data['value']))
        point.field("original_value", float(data['original_value']))
        point.field("is_anomaly", bool(data.get('is_anomaly', False)))
        
        # Set timestamp
        timestamp = datetime.fromtimestamp(data['timestamp'])
        point.time(timestamp, WritePrecision.S)
        
        return point
    
    def process_message(self, message):
        """Process a single message"""
        try:
            data = message.value
            
            # Create InfluxDB point
            point = self.create_point(data)
            
            # Add to buffer
            self.point_buffer.append(point)
            
            # Flush if buffer is full or interval passed
            current_time = time.time()
            batch_full = len(self.point_buffer) >= self.config['batch_size']
            time_elapsed = (current_time - self.last_flush_time) >= self.config['flush_interval']
            
            if batch_full or time_elapsed:
                self.flush()
                self.last_flush_time = current_time
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def flush(self):
        """Flush buffered points to InfluxDB"""
        if not self.point_buffer:
            return
        
        try:
            # Write all points
            self.write_api.write(
                bucket=self.config['influx_bucket'],
                org=self.config['influx_org'],
                record=self.point_buffer
            )
            
            # Force flush
            self.write_api.flush()
            
            self.write_count += len(self.point_buffer)
            logger.info(f"[INFO] Wrote {len(self.point_buffer)} points to InfluxDB (Total: {self.write_count})")
            
            # Clear buffer
            self.point_buffer.clear()
            
        except Exception as e:
            logger.error(f"Error writing to InfluxDB: {e}")
    
    def start(self):
        """Start the writer service"""
        logger.info("=" * 50)
        logger.info("InfluxDB Writer Service")
        logger.info("=" * 50)
        
        # Setup connections
        self.setup_influxdb()
        self.setup_kafka()
        
        self.running = True
        logger.info("InfluxDB Writer Service started successfully")
        logger.info(f"Batch size: {self.config['batch_size']}")
        logger.info(f"Flush interval: {self.config['flush_interval']}s")
        
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
        """Stop the writer service"""
        logger.info("Shutting down InfluxDB Writer Service...")
        self.running = False
        
        # Flush remaining points
        self.flush()
        
        # Close connections
        if self.write_api:
            self.write_api.close()
        
        if self.influx_client:
            self.influx_client.close()
        
        if self.consumer:
            self.consumer.close()
        
        logger.info(f"Total points written: {self.write_count}")
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
        'redpanda_topic': os.getenv('REDPANDA_TOPIC', 'transformed-sensor-data'),
        'consumer_group': os.getenv('CONSUMER_GROUP', 'influxdb-writer'),
        'influx_url': os.getenv('INFLUX_URL', 'http://influxdb:8086'),
        'influx_token': os.getenv('INFLUX_TOKEN', 'my-super-secret-token'),
        'influx_org': os.getenv('INFLUX_ORG', 'iot-org'),
        'influx_bucket': os.getenv('INFLUX_BUCKET', 'sensor-data'),
        'batch_size': int(os.getenv('BATCH_SIZE', '1000')),
        'flush_interval': int(os.getenv('FLUSH_INTERVAL', '10'))
    }
    
    logger.info(f"[CONFIG] Redpanda Brokers: {config['redpanda_brokers']}")
    logger.info(f"[CONFIG] Redpanda Topic: {config['redpanda_topic']}")
    logger.info(f"[CONFIG] InfluxDB URL: {config['influx_url']}")
    logger.info(f"[CONFIG] InfluxDB Bucket: {config['influx_bucket']}")
    logger.info(f"[CONFIG] Batch Size: {config['batch_size']}")
    logger.info(f"[CONFIG] Flush Interval: {config['flush_interval']}s")
    
    # Create and start service
    service = InfluxWriterService(config)
    
    try:
        service.start()
    except Exception as e:
        logger.error(f"Service failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
