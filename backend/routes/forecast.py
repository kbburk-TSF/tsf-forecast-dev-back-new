from fastapi import APIRouter, HTTPException, Query, Response
from sqlalchemy import text
from backend.database import engine
from backend.utils_forecast import forecast_seasonal_naive_dow, forecast_ewma
import pandas as pd
import io
from datetime import date

router = APIRouter(prefix="/forecast", tags=["forecast"])

DB_SCHEMA = "air_quality_demo_data"
TABLE = f"{DB_SCHEMA}.air_quality_raw"

def _history_df(state: str, parameter: str, agg: str):
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
    return df.groupby("date", as_index=False)["value"].mean() if agg == "mean" else df.groupby("date", as_index=False)["value"].sum()

@router.get("/state_daily")
def forecast_state_daily(
    state: str,
    parameter: str,
    h: int = Query(30, ge=1, le=365),
    agg: str = Query("mean", pattern="^(mean|sum)$"),
    method: str = Query("seasonal_naive_dow", pattern="^(seasonal_naive_dow|ewma)$")
):
    hist = _history_df(state, parameter, agg)
    fc = (
        forecast_seasonal_naive_dow(hist, h=h, lookback_weeks=8)
        if method == "seasonal_naive_dow" else
        forecast_ewma(hist, h=h, span=14)
    )
    return {
        "state": state,
        "parameter": parameter,
        "agg": agg,
        "method": method,
        "history": hist.assign(date=hist["date"].astype(str)).to_dict(orient="records"),
        "forecast": fc.assign(date=fc["date"].astype(str)).to_dict(orient="records"),
    }

@router.get("/export/state_daily")
def export_state_daily_csv(
    state: str,
    parameter: str,
    h: int = Query(30, ge=1, le=365),
    agg: str = Query("mean", pattern="^(mean|sum)$"),
    method: str = Query("seasonal_naive_dow", pattern="^(seasonal_naive_dow|ewma)$")
):
    hist = _history_df(state, parameter, agg).rename(columns={"date": "DATE", "value": "VALUE"})
    fc = (
        forecast_seasonal_naive_dow(hist.rename(columns={"DATE": "date", "VALUE": "value"}), h=h, lookback_weeks=8)
        if method == "seasonal_naive_dow" else
        forecast_ewma(hist.rename(columns={"DATE": "date", "VALUE": "value"}), h=h, span=14)
    )
    fc = fc.rename(columns={"date": "DATE", "value": "FORECAST_VALUE"})

    # Merge on DATE to align history & forecast; future dates will have VALUE empty.
    out = pd.merge(hist, fc, on="DATE", how="outer").sort_values("DATE")
    out = out[["DATE", "VALUE", "FORECAST_VALUE"]]
    out["DATE"] = out["DATE"].astype(str)

    buf = io.StringIO()
    out.to_csv(buf, index=False)

    # Filename: tsf-air_quality_demo_data-<state>-<parameter>-<agg>-<method>-<YYYYMMDD>.csv
    today = date.today().strftime("%Y%m%d")
    fname = f"tsf-air_quality_demo_data-{state}-{parameter}-{agg}-{method}-{today}.csv"
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'}
    )
