#!/usr/bin/env python3
"""
Cloud API Service
REST API for querying historical sensor data from Cassandra.
Provides endpoints for dashboards, ML jobs, and external consumers.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import uvicorn
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================
# Models
# ============================================

class SensorReading(BaseModel):
    device_id: str
    timestamp: str
    sensor_type: str
    value: float
    unit: str
    original_value: float
    original_unit: str
    is_anomaly: bool


class DeviceLatest(BaseModel):
    device_id: str
    sensor_type: str
    timestamp: str
    value: float
    unit: str
    is_anomaly: bool


class DeviceStats(BaseModel):
    device_id: str
    date: str
    sensor_type: str
    readings_count: int
    min_value: float
    max_value: float
    avg_value: float


class HealthResponse(BaseModel):
    status: str
    cassandra: str
    timestamp: str

# ============================================
# App
# ============================================

app = FastAPI(
    title="IoT Cloud API",
    description="REST API for querying historical IoT sensor data from Cassandra",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Cassandra connection (module-level, shared across requests)
cassandra_cluster = None
cassandra_session = None


def get_session():
    """Get or create Cassandra session"""
    global cassandra_cluster, cassandra_session
    if cassandra_session is None or cassandra_session.is_shutdown:
        hosts = os.getenv('CASSANDRA_HOSTS', 'cassandra').split(',')
        cassandra_cluster = Cluster(
            hosts,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='DC1'),
            protocol_version=4
        )
        cassandra_session = cassandra_cluster.connect('iot_data')
        logger.info(f"Connected to Cassandra: {hosts}")
    return cassandra_session


@app.on_event("startup")
async def startup():
    """Initialize Cassandra on startup"""
    try:
        get_session()
        logger.info("Cloud API Service started")
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra on startup: {e}")


@app.on_event("shutdown")
async def shutdown():
    """Clean up Cassandra connection"""
    global cassandra_cluster
    if cassandra_cluster:
        cassandra_cluster.shutdown()


# ============================================
# Endpoints
# ============================================

@app.get("/health", response_model=HealthResponse)
def health_check():
    """Health check endpoint"""
    try:
        session = get_session()
        session.execute("SELECT now() FROM system.local")
        return HealthResponse(
            status="healthy",
            cassandra="connected",
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Unhealthy: {e}")


@app.get("/api/v1/devices/{device_id}/readings", response_model=List[SensorReading])
def get_device_readings(
    device_id: str,
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format (default: today)"),
    sensor_type: Optional[str] = Query(None, description="Filter by sensor type"),
    hours: int = Query(24, description="Number of hours to look back", ge=1, le=168),
    limit: int = Query(1000, description="Max records to return", ge=1, le=10000)
):
    """
    Get sensor readings for a device.
    Uses time-bucketed partitions for efficient reads.
    """
    session = get_session()

    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=hours)

    # Determine which date buckets to query
    if date:
        dates = [date]
    else:
        dates = []
        d = start_time.date()
        while d <= now.date():
            dates.append(d.strftime('%Y-%m-%d'))
            d += timedelta(days=1)

    readings = []
    for dt in dates:
        if sensor_type:
            query = """
                SELECT device_id, timestamp, sensor_type, value, unit,
                       original_value, original_unit, is_anomaly
                FROM sensor_readings
                WHERE device_id = %s AND date = %s
                  AND timestamp >= %s AND timestamp <= %s
                LIMIT %s
            """
            rows = session.execute(query, (device_id, dt, start_time, now, limit))
        else:
            query = """
                SELECT device_id, timestamp, sensor_type, value, unit,
                       original_value, original_unit, is_anomaly
                FROM sensor_readings
                WHERE device_id = %s AND date = %s
                  AND timestamp >= %s AND timestamp <= %s
                LIMIT %s
            """
            rows = session.execute(query, (device_id, dt, start_time, now, limit))

        for row in rows:
            readings.append(SensorReading(
                device_id=row.device_id,
                timestamp=row.timestamp.isoformat(),
                sensor_type=row.sensor_type,
                value=row.value,
                unit=row.unit,
                original_value=row.original_value,
                original_unit=row.original_unit,
                is_anomaly=row.is_anomaly
            ))

    # Sort by timestamp descending (most recent first)
    readings.sort(key=lambda r: r.timestamp, reverse=True)
    return readings[:limit]


@app.get("/api/v1/devices/{device_id}/latest", response_model=List[DeviceLatest])
def get_device_latest(device_id: str):
    """Get the latest reading for each sensor type of a device"""
    session = get_session()

    rows = session.execute(
        "SELECT device_id, sensor_type, timestamp, value, unit, is_anomaly "
        "FROM device_latest WHERE device_id = %s",
        (device_id,)
    )

    return [
        DeviceLatest(
            device_id=row.device_id,
            sensor_type=row.sensor_type,
            timestamp=row.timestamp.isoformat(),
            value=row.value,
            unit=row.unit,
            is_anomaly=row.is_anomaly
        )
        for row in rows
    ]


@app.get("/api/v1/devices/{device_id}/stats", response_model=List[DeviceStats])
def get_device_stats(
    device_id: str,
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD (default: today)"),
    hours: int = Query(24, description="Hours to aggregate", ge=1, le=168)
):
    """
    Get aggregated stats for a device.
    Reads from sensor_readings and computes min/max/avg.
    """
    session = get_session()

    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=hours)

    if date:
        dates = [date]
    else:
        dates = []
        d = start_time.date()
        while d <= now.date():
            dates.append(d.strftime('%Y-%m-%d'))
            d += timedelta(days=1)

    # Aggregate per sensor_type
    stats_map = {}
    for dt in dates:
        rows = session.execute(
            "SELECT sensor_type, value FROM sensor_readings "
            "WHERE device_id = %s AND date = %s AND timestamp >= %s AND timestamp <= %s",
            (device_id, dt, start_time, now)
        )
        for row in rows:
            key = (dt, row.sensor_type)
            if key not in stats_map:
                stats_map[key] = {'values': [], 'date': dt, 'sensor_type': row.sensor_type}
            stats_map[key]['values'].append(row.value)

    results = []
    for key, data in stats_map.items():
        values = data['values']
        results.append(DeviceStats(
            device_id=device_id,
            date=data['date'],
            sensor_type=data['sensor_type'],
            readings_count=len(values),
            min_value=min(values),
            max_value=max(values),
            avg_value=sum(values) / len(values)
        ))

    return results


@app.get("/api/v1/devices", response_model=List[str])
def list_devices():
    """List all known device IDs from the latest readings table"""
    session = get_session()
    rows = session.execute("SELECT DISTINCT device_id FROM device_latest")
    return [row.device_id for row in rows]


@app.get("/api/v1/export/{device_id}")
def export_device_data(
    device_id: str,
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
):
    """
    Export all readings for a device on a given date.
    Used by Spark jobs for batch processing.
    Returns raw JSON array for streaming consumption.
    """
    session = get_session()

    rows = session.execute(
        "SELECT device_id, timestamp, sensor_type, value, unit, "
        "original_value, original_unit, is_anomaly "
        "FROM sensor_readings WHERE device_id = %s AND date = %s",
        (device_id, date)
    )

    data = []
    for row in rows:
        data.append({
            'device_id': row.device_id,
            'timestamp': row.timestamp.isoformat(),
            'sensor_type': row.sensor_type,
            'value': row.value,
            'unit': row.unit,
            'original_value': row.original_value,
            'original_unit': row.original_unit,
            'is_anomaly': row.is_anomaly
        })

    return {"device_id": device_id, "date": date, "count": len(data), "readings": data}


if __name__ == '__main__':
    port = int(os.getenv('API_PORT', '8000'))
    uvicorn.run(app, host="0.0.0.0", port=port)
