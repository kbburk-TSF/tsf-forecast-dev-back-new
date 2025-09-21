
from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from backend.database import engine as app_engine
import pandas as pd
import os
from datetime import datetime
from uuid import uuid4

router = APIRouter(prefix="/forms", tags=["forms"])
templates = Jinja2Templates(directory="backend/templates")

DB_TABLE = "air_quality_raw"  # relies on search_path

def _set_search_path(conn):
    conn.execute(text("SET search_path TO air_quality_demo_data, public"))

def _list_params():
    sql = f"""
        SELECT DISTINCT "Parameter Name" AS param
        FROM {DB_TABLE}
        WHERE "Parameter Name" IS NOT NULL
        ORDER BY "Parameter Name"
    """
    with app_engine.begin() as conn:
        _set_search_path(conn)
        rows = conn.execute(text(sql)).mappings().all()
    return [r["param"] for r in rows]

def _list_states():
    sql = f"""
        SELECT DISTINCT "State Name" AS state
        FROM {DB_TABLE}
        WHERE "State Name" IS NOT NULL
        ORDER BY "State Name"
    """
    with app_engine.begin() as conn:
        _set_search_path(conn)
        rows = conn.execute(text(sql)).mappings().all()
    return [r["state"] for r in rows]

def _daily_mean_df(parameter: str, state: str) -> pd.DataFrame:
    sql = f"""
        SELECT DATE("Date Local") AS date, AVG("Arithmetic Mean") AS value
        FROM {DB_TABLE}
        WHERE "Parameter Name" = :parameter AND "State Name" = :state
        GROUP BY DATE("Date Local")
        ORDER BY DATE("Date Local")
    """
    with app_engine.begin() as conn:
        _set_search_path(conn)
        rows = conn.execute(text(sql), {"parameter": parameter, "state": state}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail="No rows found for that Parameter/State.")
    df = pd.DataFrame(rows).rename(columns={"date":"date","value":"value"})
    return df

def _safe_name(x: str) -> str:
    return "".join(c for c in x if c.isalnum() or c in ("-","_"," ")).strip().replace(" ","_")

@router.get("/classical", response_class=HTMLResponse)
def classical_form(request: Request):
    params = _list_params()
    states = _list_states()
    return templates.TemplateResponse("forms/classical.html", {"request": request, "params": params, "states": states})

@router.post("/classical/run")
def classical_run(parameter: str = Form(...), state: str = Form(...)):
    # 1) Build aggregated series
    df = _daily_mean_df(parameter, state)[["date","value"]]

    # 2) Identify
    forecast_id = str(uuid4())
    safe_param = _safe_name(parameter)
    safe_state = _safe_name(state)
    forecast_name = f"{safe_param}_{safe_state}"  # no date, no extension

    # 3) Assemble final CSV: forecast_id, forecast_name, date, value
    final_df = df.copy()
    final_df.insert(0, "forecast_name", forecast_name)
    final_df.insert(0, "forecast_id", forecast_id)

    # 4) Save and return
    jobs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "_jobs"))
    os.makedirs(jobs_dir, exist_ok=True)
    out_path = os.path.abspath(os.path.join(jobs_dir, f"{forecast_name}.csv"))
    final_df.to_csv(out_path, index=False)
    return FileResponse(out_path, media_type="text/csv", filename=os.path.basename(out_path))
