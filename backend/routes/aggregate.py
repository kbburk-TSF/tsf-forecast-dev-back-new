from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text
from backend.database import engine
import pandas as pd

router = APIRouter(prefix="/aggregate", tags=["aggregate"])

DB_SCHEMA = "air_quality_demo_data"
TABLE = f"{DB_SCHEMA}.air_quality_raw"

@router.get("/state_daily")
def state_daily(state: str, parameter: str, agg: str = Query("mean", pattern="^(mean|sum)$")):
    sql = f"""
    SELECT "Date Local"::date AS date, "Arithmetic Mean" AS value
    FROM {TABLE}
    WHERE "State Name" = :state AND "Parameter Name" = :parameter
    ORDER BY "Date Local"
    """
    with engine.begin() as conn:
        rows = conn.execute(text(sql), {"state": state, "parameter": parameter}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail="No data for given filters")

    df = pd.DataFrame(rows)
    out = df.groupby("date", as_index=False)["value"].mean() if agg == "mean" else df.groupby("date", as_index=False)["value"].sum()
    out["value"] = out["value"].astype(float)
    return {"state": state, "parameter": parameter, "agg": agg, "series": out.to_dict(orient="records")}
