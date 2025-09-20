from fastapi import APIRouter, UploadFile, File, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from backend.database import engine
import pandas as pd
import io

router = APIRouter(prefix="/upload", tags=["upload"])

DB_SCHEMA = "air_quality_demo_data"
TABLE = f"{DB_SCHEMA}.air_quality_raw"

@router.post("/air_quality")
def upload_air_quality_csv(
    file: UploadFile = File(...),
    on_conflict: str = Query("ignore", pattern="^(ignore|fail)$")
):
    try:
        raw = file.file.read()
        df = pd.read_csv(io.BytesIO(raw))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot read CSV: {e}")

    required = ["Date Local", "Parameter Name", "Arithmetic Mean", "State Name"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required columns: {missing}")

    df = df.copy()
    df["Date Local"] = pd.to_datetime(df["Date Local"]).dt.date
    df["Arithmetic Mean"] = pd.to_numeric(df["Arithmetic Mean"], errors="coerce")
    df = df.dropna(subset=["Date Local","Arithmetic Mean"])

    cols = ["Date Local","Parameter Name","Arithmetic Mean","Local Site Name","State Name","County Name","City Name","CBSA Name"]
    insert_sql = f"""
        INSERT INTO {TABLE} ("Date Local","Parameter Name","Arithmetic Mean",
            "Local Site Name","State Name","County Name","City Name","CBSA Name")
        VALUES (:Date_Local,:Parameter_Name,:Arithmetic_Mean,:Local_Site_Name,
                :State_Name,:County_Name,:City_Name,:CBSA_Name)
    """

    inserted = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            try:
                conn.execute(text(insert_sql), {
                    "Date_Local": row.get("Date Local"),
                    "Parameter_Name": row.get("Parameter Name"),
                    "Arithmetic_Mean": row.get("Arithmetic Mean"),
                    "Local_Site_Name": row.get("Local Site Name"),
                    "State_Name": row.get("State Name"),
                    "County_Name": row.get("County Name"),
                    "City_Name": row.get("City Name"),
                    "CBSA_Name": row.get("CBSA Name"),
                })
                inserted += 1
            except SQLAlchemyError:
                pass

    return {"rows_inserted": inserted}
