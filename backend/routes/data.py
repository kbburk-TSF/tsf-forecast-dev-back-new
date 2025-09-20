from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import text
from backend.database import engine

router = APIRouter(prefix="/data", tags=["data"])

DB_SCHEMA = "air_quality_demo_data"
TABLE = f"{DB_SCHEMA}.air_quality_raw"

def _safe_query(sql: str, params: dict):
    try:
        with engine.begin() as conn:
            res = conn.execute(text(sql), params).mappings().all()
            return [dict(r) for r in res]
    except Exception as e:
        raise HTTPException(status_code=500, detail={"sql": sql, "params": params, "error": str(e)})

@router.get("/air_quality/last")
def last_rows(limit: int = Query(50, ge=1, le=500)):
    sql = f"""
    SELECT "Date Local", "Parameter Name", "Arithmetic Mean",
           "Local Site Name", "State Name", "County Name",
           "City Name", "CBSA Name"
    FROM {TABLE}
    ORDER BY "Date Local" DESC
    LIMIT :limit
    """
    rows = _safe_query(sql, {"limit": limit})
    return {"rows": rows}

@router.get("/air_quality/last_date")
def last_date(state: str, parameter: str):
    sql = f"""
    SELECT MAX("Date Local") AS max_date
    FROM {TABLE}
    WHERE "State Name" = :state AND "Parameter Name" = :parameter
    """
    try:
        with engine.begin() as conn:
            row = conn.execute(text(sql), {"state": state, "parameter": parameter}).first()
            max_date = row[0] if row else None
    except Exception as e:
        raise HTTPException(status_code=500, detail={"sql": sql, "state": state, "parameter": parameter, "error": str(e)})
    return {"state": state, "parameter": parameter, "last_date": max_date}
