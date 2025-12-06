from pydantic import BaseModel, field_validator # <--- Changed import
from typing import Optional
from datetime import datetime
from typing import Dict, Any
import math

class TelemetryRow(BaseModel):
    ranger_id: str
    activity_type: str
    acc_x: float = 0.0
    acc_y: float = 0.0
    acc_z: float = 0.0
    gyro_x: float = 0.0
    gyro_y: float = 0.0
    gyro_z: float = 0.0
    
    # 1. Reject Unrealistic Values (Updated for V2)
    @field_validator('acc_x', 'acc_y', 'acc_z', 'gyro_x', 'gyro_y', 'gyro_z')
    @classmethod
    def validate_sensor_range(cls, v: float) -> float:
        if math.isnan(v):
            return 0.0

        if v > 1000 or v < -1000:
            raise ValueError(f"Sensor value {v} is out of safe range (-1000 to 1000)")
        return v
    
    # 2. Clean up Text (Updated for V2)
    @field_validator('activity_type')
    @classmethod
    def normalize_activity(cls, v: str) -> str:
        return v.upper().strip()
    

class TelemetryResponse(BaseModel):
    id: int
    ranger_id: str
    activity_type: str
    timestamp: datetime
    sensor_data: Dict[str, Any]

    class Config:
        from_attributes = True  # Allows reading from SQLModel