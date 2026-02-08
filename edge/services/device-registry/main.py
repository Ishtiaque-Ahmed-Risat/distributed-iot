#!/usr/bin/env python3
"""
Device Registry Service - FastAPI
REST API for registering IoT devices with descriptions and sensor specifications
"""

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict
from datetime import datetime
import uvicorn
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# FastAPI app with automatic documentation
app = FastAPI(
    title="IoT Device Registry",
    description="Device registration and metadata management for IoT gateway",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# ============================================
# Pydantic Models (automatic validation)
# ============================================

class SensorSpec(BaseModel):
    """Sensor specification"""
    sensor_type: str = Field(..., description="Type of sensor (e.g., temperature, humidity)")
    unit: str = Field(..., description="Unit of measurement (e.g., celsius, percent)")
    min_value: Optional[float] = Field(None, description="Minimum expected value")
    max_value: Optional[float] = Field(None, description="Maximum expected value")
    description: Optional[str] = Field(None, description="Sensor description")
    
    class Config:
        schema_extra = {
            "example": {
                "sensor_type": "temperature",
                "unit": "celsius",
                "min_value": -40,
                "max_value": 85,
                "description": "Ambient temperature sensor"
            }
        }


class DeviceRegistration(BaseModel):
    """Device registration request"""
    device_id: str = Field(..., description="Unique device identifier")
    device_type: str = Field(..., description="Type of device (e.g., sensor, actuator)")
    description: str = Field(..., description="Human-readable device description")
    location: Optional[str] = Field(None, description="Physical location of device")
    metadata: Optional[Dict[str, str]] = Field(default_factory=dict, description="Additional metadata")
    sensors: List[SensorSpec] = Field(..., description="List of sensors on this device")
    
    @validator('device_id')
    def validate_device_id(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('device_id cannot be empty')
        return v.strip()
    
    @validator('sensors')
    def validate_sensors(cls, v):
        if not v or len(v) == 0:
            raise ValueError('At least one sensor must be specified')
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "device_id": "device_0001",
                "device_type": "environmental_sensor",
                "description": "Temperature and humidity sensor in server room",
                "location": "Server Room A, Rack 5",
                "metadata": {
                    "floor": "2",
                    "building": "Main Office"
                },
                "sensors": [
                    {
                        "sensor_type": "temperature",
                        "unit": "celsius",
                        "min_value": -10,
                        "max_value": 50,
                        "description": "Ambient air temperature"
                    }
                ]
            }
        }


class Device(BaseModel):
    """Complete device information"""
    device_id: str
    device_type: str
    description: str
    location: Optional[str] = None
    metadata: Dict[str, str] = Field(default_factory=dict)
    sensors: List[SensorSpec]
    registered_at: datetime
    last_seen: datetime
    status: str = "active"
    
    class Config:
        schema_extra = {
            "example": {
                "device_id": "device_0001",
                "device_type": "environmental_sensor",
                "description": "Temperature sensor in server room",
                "location": "Server Room A",
                "metadata": {"floor": "2"},
                "sensors": [{"sensor_type": "temperature", "unit": "celsius"}],
                "registered_at": "2024-02-07T10:00:00Z",
                "last_seen": "2024-02-07T10:15:00Z",
                "status": "active"
            }
        }


class DeviceList(BaseModel):
    """List of devices response"""
    devices: List[Device]
    count: int


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    devices: int


class HeartbeatResponse(BaseModel):
    """Heartbeat response"""
    status: str
    message: Optional[str] = None


# ============================================
# In-Memory Storage (use database in production)
# ============================================

devices: Dict[str, Device] = {}


# ============================================
# API Endpoints
# ============================================

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """
    Health check endpoint
    Returns service status and device count
    """
    return HealthResponse(
        status="healthy",
        devices=len(devices)
    )


@app.post(
    "/api/v1/devices",
    response_model=Device,
    status_code=status.HTTP_201_CREATED,
    tags=["Devices"],
    summary="Register a new device",
    response_description="Device registered successfully"
)
async def register_device(registration: DeviceRegistration):
    """
    Register a new IoT device with description and sensor specifications.
    
    This endpoint is used during device onboarding to store:
    - Device metadata and description (for LLM context)
    - Sensor specifications (types, units, expected ranges)
    - Location and additional metadata
    
    **Note**: Device descriptions are used by the transformation service
    for context-aware data processing and future LLM integration.
    """
    # Check if device already exists
    if registration.device_id in devices:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Device {registration.device_id} already exists"
        )
    
    # Create device record
    device = Device(
        device_id=registration.device_id,
        device_type=registration.device_type,
        description=registration.description,
        location=registration.location,
        metadata=registration.metadata,
        sensors=registration.sensors,
        registered_at=datetime.utcnow(),
        last_seen=datetime.utcnow(),
        status="active"
    )
    
    devices[device.device_id] = device
    
    logger.info(f"[REGISTER] Device: {device.device_id} ({device.device_type}) - {device.description}")
    
    return device


@app.get(
    "/api/v1/devices",
    response_model=DeviceList,
    tags=["Devices"],
    summary="List all devices"
)
async def list_devices(
    status_filter: Optional[str] = None,
    device_type: Optional[str] = None
):
    """
    List all registered devices.
    
    Optional filters:
    - status: Filter by device status (active, inactive, error)
    - device_type: Filter by device type
    """
    device_list = list(devices.values())
    
    # Apply filters
    if status_filter:
        device_list = [d for d in device_list if d.status == status_filter]
    
    if device_type:
        device_list = [d for d in device_list if d.device_type == device_type]
    
    return DeviceList(
        devices=device_list,
        count=len(device_list)
    )


@app.get(
    "/api/v1/devices/{device_id}",
    response_model=Device,
    tags=["Devices"],
    summary="Get device by ID"
)
async def get_device(device_id: str):
    """
    Get detailed information about a specific device.
    
    This endpoint is used by the transformation service to retrieve
    device context (description, sensor specs) for intelligent processing.
    """
    if device_id not in devices:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device {device_id} not found"
        )
    
    return devices[device_id]


@app.put(
    "/api/v1/devices/{device_id}",
    response_model=Device,
    tags=["Devices"],
    summary="Update device information"
)
async def update_device(device_id: str, registration: DeviceRegistration):
    """
    Update device information.
    
    Note: This updates all fields. For partial updates, use PATCH.
    """
    if device_id not in devices:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device {device_id} not found"
        )
    
    # Preserve registration time
    existing = devices[device_id]
    
    updated_device = Device(
        device_id=registration.device_id,
        device_type=registration.device_type,
        description=registration.description,
        location=registration.location,
        metadata=registration.metadata,
        sensors=registration.sensors,
        registered_at=existing.registered_at,  # Preserve
        last_seen=datetime.utcnow(),
        status=existing.status
    )
    
    devices[device_id] = updated_device
    
    logger.info(f"[UPDATE] Device: {device_id}")
    
    return updated_device


@app.delete(
    "/api/v1/devices/{device_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["Devices"],
    summary="Delete a device"
)
async def delete_device(device_id: str):
    """
    Delete a device from the registry.
    
    This removes all device metadata. Use with caution.
    """
    if device_id not in devices:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device {device_id} not found"
        )
    
    del devices[device_id]
    
    logger.info(f"[DELETE] Device: {device_id}")
    
    return None


@app.post(
    "/api/v1/devices/{device_id}/heartbeat",
    response_model=HeartbeatResponse,
    tags=["Devices"],
    summary="Update device last seen"
)
async def device_heartbeat(device_id: str):
    """
    Update device last_seen timestamp.
    
    Used by devices to indicate they are still active.
    Can be called periodically to track device connectivity.
    """
    if device_id not in devices:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device {device_id} not found"
        )
    
    devices[device_id].last_seen = datetime.utcnow()
    
    return HeartbeatResponse(
        status="updated",
        message=f"Heartbeat recorded for {device_id}"
    )


@app.get(
    "/api/v1/devices/{device_id}/sensors",
    response_model=List[SensorSpec],
    tags=["Devices"],
    summary="Get device sensors"
)
async def get_device_sensors(device_id: str):
    """
    Get list of sensors for a specific device.
    
    Used by transformation service to understand sensor specifications.
    """
    if device_id not in devices:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device {device_id} not found"
        )
    
    return devices[device_id].sensors


# ============================================
# Startup/Shutdown Events
# ============================================

@app.on_event("startup")
async def startup_event():
    """Log startup"""
    logger.info("=" * 50)
    logger.info("Device Registry Service (FastAPI)")
    logger.info("=" * 50)
    logger.info(f"API Documentation: http://localhost:{os.getenv('PORT', '8080')}/docs")
    logger.info(f"ReDoc: http://localhost:{os.getenv('PORT', '8080')}/redoc")
    logger.info("=" * 50)


@app.on_event("shutdown")
async def shutdown_event():
    """Log shutdown"""
    logger.info(f"Shutting down. Total devices registered: {len(devices)}")


# ============================================
# Main Entry Point
# ============================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
        access_log=True
    )
