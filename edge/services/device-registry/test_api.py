#!/usr/bin/env python3
"""
Test script for Device Registry API
Can be run with: python test_api.py
"""

import requests
import json
from datetime import datetime

BASE_URL = "http://localhost:31080"  # K3s NodePort

def print_response(title, response):
    """Pretty print API response"""
    print(f"\n{'='*60}")
    print(f"{title}")
    print(f"{'='*60}")
    print(f"Status: {response.status_code}")
    try:
        print(json.dumps(response.json(), indent=2))
    except:
        print(response.text)

def test_device_registry():
    """Test all Device Registry endpoints"""
    
    print("\n🧪 Testing Device Registry API")
    print("="*60)
    
    # Test 1: Health Check
    print("\n1️⃣  Testing Health Check...")
    response = requests.get(f"{BASE_URL}/health")
    print_response("GET /health", response)
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
    print("✅ Health check passed")
    
    # Test 2: Register Device
    print("\n2️⃣  Testing Device Registration...")
    device_data = {
        "device_id": "test_device_001",
        "device_type": "environmental_sensor",
        "description": "Test temperature and humidity sensor in lab",
        "location": "Lab Room 101",
        "metadata": {
            "building": "Science Building",
            "floor": "1"
        },
        "sensors": [
            {
                "sensor_type": "temperature",
                "unit": "celsius",
                "min_value": -10,
                "max_value": 50,
                "description": "Ambient temperature"
            },
            {
                "sensor_type": "humidity",
                "unit": "percent",
                "min_value": 0,
                "max_value": 100,
                "description": "Relative humidity"
            }
        ]
    }
    
    response = requests.post(
        f"{BASE_URL}/api/v1/devices",
        json=device_data
    )
    print_response("POST /api/v1/devices", response)
    assert response.status_code == 201
    assert response.json()["device_id"] == "test_device_001"
    print("✅ Device registration passed")
    
    # Test 3: Get Device
    print("\n3️⃣  Testing Get Device...")
    response = requests.get(f"{BASE_URL}/api/v1/devices/test_device_001")
    print_response("GET /api/v1/devices/test_device_001", response)
    assert response.status_code == 200
    assert response.json()["device_id"] == "test_device_001"
    print("✅ Get device passed")
    
    # Test 4: List Devices
    print("\n4️⃣  Testing List Devices...")
    response = requests.get(f"{BASE_URL}/api/v1/devices")
    print_response("GET /api/v1/devices", response)
    assert response.status_code == 200
    assert response.json()["count"] >= 1
    print("✅ List devices passed")
    
    # Test 5: Get Device Sensors
    print("\n5️⃣  Testing Get Device Sensors...")
    response = requests.get(f"{BASE_URL}/api/v1/devices/test_device_001/sensors")
    print_response("GET /api/v1/devices/test_device_001/sensors", response)
    assert response.status_code == 200
    assert len(response.json()) == 2
    print("✅ Get sensors passed")
    
    # Test 6: Heartbeat
    print("\n6️⃣  Testing Heartbeat...")
    response = requests.post(f"{BASE_URL}/api/v1/devices/test_device_001/heartbeat")
    print_response("POST /api/v1/devices/test_device_001/heartbeat", response)
    assert response.status_code == 200
    assert response.json()["status"] == "updated"
    print("✅ Heartbeat passed")
    
    # Test 7: Register Another Device
    print("\n7️⃣  Testing Register Another Device...")
    device_data2 = {
        "device_id": "test_device_002",
        "device_type": "motion_sensor",
        "description": "PIR motion sensor for security",
        "location": "Entrance Hall",
        "sensors": [
            {
                "sensor_type": "motion",
                "unit": "boolean",
                "description": "Motion detection"
            }
        ]
    }
    response = requests.post(f"{BASE_URL}/api/v1/devices", json=device_data2)
    assert response.status_code == 201
    print("✅ Second device registered")
    
    # Test 8: List with Filter
    print("\n8️⃣  Testing List with Filter...")
    response = requests.get(f"{BASE_URL}/api/v1/devices?device_type=environmental_sensor")
    print_response("GET /api/v1/devices?device_type=environmental_sensor", response)
    assert response.status_code == 200
    assert response.json()["count"] == 1
    print("✅ Filtered list passed")
    
    # Test 9: Duplicate Registration (Should Fail)
    print("\n9️⃣  Testing Duplicate Registration (Expected Failure)...")
    response = requests.post(f"{BASE_URL}/api/v1/devices", json=device_data)
    print_response("POST /api/v1/devices (duplicate)", response)
    assert response.status_code == 409  # Conflict
    print("✅ Duplicate detection passed")
    
    # Test 10: Invalid Data (Should Fail)
    print("\n🔟 Testing Invalid Data (Expected Failure)...")
    invalid_data = {
        "device_id": "",  # Empty ID
        "device_type": "test",
        "description": "Invalid",
        "sensors": []  # Empty sensors
    }
    response = requests.post(f"{BASE_URL}/api/v1/devices", json=invalid_data)
    print_response("POST /api/v1/devices (invalid)", response)
    assert response.status_code == 422  # Validation error
    print("✅ Validation passed")
    
    # Test 11: Get Non-Existent Device (Should Fail)
    print("\n1️⃣1️⃣  Testing Get Non-Existent Device (Expected Failure)...")
    response = requests.get(f"{BASE_URL}/api/v1/devices/nonexistent")
    print_response("GET /api/v1/devices/nonexistent", response)
    assert response.status_code == 404
    print("✅ 404 handling passed")
    
    # Test 12: Delete Device
    print("\n1️⃣2️⃣  Testing Delete Device...")
    response = requests.delete(f"{BASE_URL}/api/v1/devices/test_device_001")
    print(f"\nStatus: {response.status_code}")
    assert response.status_code == 204
    print("✅ Delete passed")
    
    # Verify deletion
    response = requests.get(f"{BASE_URL}/api/v1/devices/test_device_001")
    assert response.status_code == 404
    print("✅ Deletion verified")
    
    print("\n" + "="*60)
    print("🎉 ALL TESTS PASSED!")
    print("="*60)
    print("\n📚 API Documentation available at:")
    print(f"   Swagger UI: {BASE_URL}/docs")
    print(f"   ReDoc:      {BASE_URL}/redoc")

if __name__ == "__main__":
    try:
        test_device_registry()
    except requests.exceptions.ConnectionError:
        print("\n❌ Error: Could not connect to Device Registry")
        print("Make sure the service is running:")
        print("  cd edge && ./k3s-deploy.sh")
        print("  OR")
        print("  cd edge/services/device-registry && python main.py")
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
