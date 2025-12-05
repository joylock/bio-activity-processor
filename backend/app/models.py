from typing import Optional, Dict
from sqlmodel import SQLModel, Field
from sqlalchemy import Column, JSON
from datetime import datetime

class TelemetryLog(SQLModel, table = True):
    __tablename = "telemetry_logs"
    id : Optional[int] = Field(default = None, primary_key = True)

    timestamp: datetime = Field(default_factory=datetime.utcnow)
    ranger_id: str = Field(index=True)
    activity_type: str

    sensor_data: Dict = Field(default={}, sa_column=Column(JSON))

    status : str = Field(default = "processed")