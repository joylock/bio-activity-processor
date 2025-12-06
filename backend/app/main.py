from fastapi import FastAPI, Depends, UploadFile, File
from sqlmodel import Session, select
from typing import List, Optional  # <--- REQUIRED for List[schemas.TelemetryResponse]
from datetime import datetime
from . import database, models, schemas # <--- Make sure 'schemas' is here!
import pandas as pd
import io
from pydantic import ValidationError

app = FastAPI(title="Bio-Activity-Processor")

@app.on_event("startup")
def on_startup():
    database.create_db_and_tables()

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...), session: Session = Depends(database.get_session)):
    content = await file.read()
    df = pd.read_csv(io.BytesIO(content))

    results = []
    errors = []

    missing_rows = df[df['ranger_id'].isna() | df['activity_type'].isna()]
    for index, row in missing_rows.iterrows():
        errors.append({
            "row_index": int(index) + 2,
            "error": "Missing ranger_id or activity_type",
        })

    df.dropna(subset=['ranger_id', 'activity_type'], inplace=True)

    df.fillna(0.0, inplace=True)

    for index, row in df.iterrows():
        try:
            # --- THE NEW VALIDATION STEP ---
            # Convert Pandas row to dict and validate it
            row_data = row.to_dict()
            valid_row = schemas.TelemetryRow(**row_data) 
            
            # If we get here, data is CLEAN. 
            # Notice we use valid_row.activity_type (which is now lowercase "running")
            
            sensor_data = {
                "acc_x": valid_row.acc_x,
                "acc_y": valid_row.acc_y,
                "acc_z": valid_row.acc_z,
                "gyro_x": valid_row.gyro_x,
                "gyro_y": valid_row.gyro_y,
                "gyro_z": valid_row.gyro_z
            }

            telemetry_log = models.TelemetryLog(
                ranger_id=valid_row.ranger_id,
                activity_type=valid_row.activity_type, 
                sensor_data=sensor_data
            )

            session.add(telemetry_log)
            results.append(telemetry_log)

        except ValidationError as e:
            # Catch the error and record it
            errors.append({"row_index": index + 2, "error": str(e)})

    session.commit()

    return {
        "processed_count": len(results),
        "rejected_count": len(errors),
        "errors": errors  # This will show you exactly why GreenRanger failed
    }

@app.get("/telemetry", response_model=List[schemas.TelemetryResponse])
def get_telemetry(
    session: Session = Depends(database.get_session),
    ranger_id: Optional[str] = None,
    activity_type: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
):
    """
    Search and Filter Telemetry Logs.
    - Filter by Ranger ID
    - Filter by Activity (e.g., 'running')
    - Time Travel: Get data between start_time and end_time
    """
    query = select(models.TelemetryLog)

    # 1. Apply Filters dynamically
    if ranger_id:
        query = query.where(models.TelemetryLog.ranger_id == ranger_id)
    
    if activity_type:
        # Normalize input to match stored lowercase data
        query = query.where(models.TelemetryLog.activity_type == activity_type.lower().strip())
        
    # 2. Time-Range Analytics logic
    if start_time:
        query = query.where(models.TelemetryLog.timestamp >= start_time)
    if end_time:
        query = query.where(models.TelemetryLog.timestamp <= end_time)

    # Execute query
    results = session.exec(query).all()
    
    if not results:
        return []
        
    return results


@app.get("/stats/{ranger_id}")
def get_ranger_stats(
    ranger_id: str, 
    session: Session = Depends(database.get_session)
):
    """
    Analytics Engine: Calculates Avg/Max/Min for sensor data.
    """
    # 1. Fetch all logs for this Ranger
    logs = session.exec(select(models.TelemetryLog).where(models.TelemetryLog.ranger_id == ranger_id)).all()

    if not logs:
        return {"error": "Ranger not found"}

    # 2. Extract specific metric (e.g., acc_x) to analyze
    # Since data is in JSON, we extract it in Python (Simple & Safe for Hackathon)
    acc_x_values = [log.sensor_data.get("acc_x", 0.0) for log in logs]
    
    # 3. Perform Math
    total_logs = len(acc_x_values)
    avg_acc_x = sum(acc_x_values) / total_logs if total_logs > 0 else 0
    max_acc_x = max(acc_x_values) if total_logs > 0 else 0
    min_acc_x = min(acc_x_values) if total_logs > 0 else 0

    return {
        "ranger_id": ranger_id,
        "total_missions": total_logs,
        "analytics": {
            "avg_acc_x": round(avg_acc_x, 4),
            "max_acc_x": max_acc_x,
            "min_acc_x": min_acc_x,
            "status": "CRITICAL" if max_acc_x > 900 else "NOMINAL" # Bonus: Anomaly Flag
        }
    }