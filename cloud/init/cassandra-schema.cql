-- ============================================
-- IoT Cloud - Cassandra Schema
-- ============================================
-- Time-bucketed design for fast time-range queries
-- Partition key: (device_id, date) keeps partitions bounded
-- Clustering key: timestamp DESC for most-recent-first reads

CREATE KEYSPACE IF NOT EXISTS iot_data
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND durable_writes = true;

-- Primary sensor data table (time-bucketed)
CREATE TABLE IF NOT EXISTS iot_data.sensor_readings (
    device_id    text,
    date         text,              -- YYYY-MM-DD bucket (bounds partition size)
    timestamp    timestamp,         -- actual reading time (UTC)
    sensor_type  text,
    value        double,
    unit         text,
    original_value double,
    original_unit  text,
    is_anomaly   boolean,
    metadata     map<text, text>,
    PRIMARY KEY ((device_id, date), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': '1',
    'compaction_window_unit': 'DAYS'
  }
  AND default_time_to_live = 7776000   -- 90 days auto-expiry
  AND gc_grace_seconds = 864000;       -- 10 days

-- Aggregated stats table (for fast ML reads)
CREATE TABLE IF NOT EXISTS iot_data.device_stats (
    device_id    text,
    date         text,
    sensor_type  text,
    hour         int,               -- 0-23
    count        counter,
    PRIMARY KEY ((device_id, date), sensor_type, hour)
) WITH CLUSTERING ORDER BY (sensor_type ASC, hour ASC);

-- Latest reading per device (for dashboards)
CREATE TABLE IF NOT EXISTS iot_data.device_latest (
    device_id    text,
    sensor_type  text,
    timestamp    timestamp,
    value        double,
    unit         text,
    is_anomaly   boolean,
    PRIMARY KEY (device_id, sensor_type)
);
