from fastapi import FastAPI, Depends, UploadFile, File
from sqlmodel import Session
from .import database, models
import pandas as pd
import io

app = FastAPI(title = "Bio-Activity-Processor")

@app.on_event("startup")
def on_startup():
    database.create_db_and_tables()

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...), session : Session = Depends(database.get_session)):
    content = await file.read()

    df = pd.read_csv(io.BytesIO(content))

    results = []

    for index,row in df.iterrows():
        
        sensor_data = {
            "acc_x": row.get("acc_x", 0.0),
            "acc_y": row.get("acc_y", 0.0),
            "acc_z": row.get("acc_z", 0.0),
            "gyro_x": row.get("gyro_x", 0.0),
            "gyro_y": row.get("gyro_y", 0.0),
            "gyro_z": row.get("gyro_z", 0.0),
            "grav_x": row.get("grav_x", 0.0),
            "grav_y": row.get("grav_y", 0.0),
            "grav_z": row.get("grav_z", 0.0)
        }

        telemetry_log = models.TelemetryLog(
            ranger_id = str(row["ranger_id"]),
            activity_type = str(row["activity_type"]),
            sensor_data = sensor_data
        )

        session.add(telemetry_log)
        results.append(telemetry_log)
    
    session.commit()

    return {"message" : f"Successfully processed {len(results)} rows"}

