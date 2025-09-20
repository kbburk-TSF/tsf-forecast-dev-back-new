
from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from backend.database import engine
import pandas as pd
import os
from datetime import datetime

router = APIRouter(prefix="/forms", tags=["forms"])
templates = Jinja2Templates(directory="backend/templates")

DB_TABLE = "air_quality_demo_data.air_quality_raw"

def _list_params():
    sql = f"""
        SELECT DISTINCT "Parameter Name" AS param
        FROM {table}
        WHERE "Parameter Name" IS NOT NULL
        ORDER BY "Parameter Name"
    """
    with engine.begin() as conn:
        return [r["param"] for r in conn.execute(text(sql.format(table=DB_TABLE))).mappings().all()]

def _list_states():
    sql = f"""
        SELECT DISTINCT "State Name" AS state
        FROM {table}
        WHERE "State Name" IS NOT NULL
        ORDER BY "State Name"
    """
    with engine.begin() as conn:
        return [r["state"] for r in conn.execute(text(sql.format(table=DB_TABLE))).mappings().all()]

def _daily_mean(parameter: str, state: str) -> pd.DataFrame:
    sql = f"""
        SELECT DATE("Date Local") AS date, AVG("Arithmetic Mean") AS value
        FROM {table}
        WHERE "Parameter Name" = :parameter AND "State Name" = :state
        GROUP BY DATE("Date Local")
        ORDER BY DATE("Date Local")
    """
    with engine.begin() as conn:
        rows = conn.execute(text(sql.format(table=DB_TABLE)), {"parameter": parameter, "state": state}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail="No rows for that Parameter/State.")
    df = pd.DataFrame(rows)
    df.rename(columns={"date":"DATE","value":"VALUE"}, inplace=True)
    return df

@router.get("/raw", response_class=HTMLResponse)
def raw_form(request: Request):
    params = _list_params()
    states = _list_states()
    return templates.TemplateResponse("forms/raw.html", {"request": request, "params": params, "states": states})

@router.post("/raw/export")
def raw_export(parameter: str = Form(...), state: str = Form(...)):
    # Build aggregated series
    df = _daily_mean(parameter, state)

    # Filename: {Parameter}_{State}_{yyyymmdd}_raw.csv
    today = datetime.utcnow().strftime("%Y%m%d")
    safe_param = "".join(c for c in parameter if c.isalnum() or c in ("-","_")).strip().replace(" ", "_")
    safe_state = "".join(c for c in state if c.isalnum() or c in ("-","_")).strip().replace(" ", "_")
    fname = f"{safe_param}_{safe_state}_{today}_raw.csv"

    # Save under backend/_jobs (ensure dir)
    out_dir = os.path.join(os.path.dirname(__file__), "..", "_jobs")
    os.makedirs(out_dir, exist_ok=True)
    fpath = os.path.abspath(os.path.join(out_dir, fname))
    df.to_csv(fpath, index=False)

    # Optionally trigger classical via HTTP (best-effort, non-blocking)
    try:
        import threading, requests
        start_url = os.getenv("CLASSICAL_START_URL")  # e.g., http://localhost:8000/classical/start
        if start_url:
            payload = {
                "parameter": parameter,
                "state": state,
                "raw_csv": fpath
            }
            def _fire():
                try:
                    requests.post(start_url, json=payload, timeout=3)
                except Exception:
                    pass
            threading.Thread(target=_fire, daemon=True).start()
    except Exception:
        pass

    return FileResponse(fpath, media_type="text/csv", filename=os.path.basename(fpath))
