
from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text, create_engine
from backend.database import engine as app_engine
import pandas as pd
import os, time, io, requests
from datetime import datetime

router = APIRouter(prefix="/forms", tags=["forms"])
templates = Jinja2Templates(directory="backend/templates")

DB_TABLE = "air_quality_raw"  # unqualified; relies on search_path

ENGINE_DATABASE_URL = os.getenv("ENGINE_DATABASE_URL", "").strip()
ENGINE_STAGING_TABLE = os.getenv("ENGINE_STAGING_TABLE", "engine.staging_historical")
engine2 = create_engine(ENGINE_DATABASE_URL) if ENGINE_DATABASE_URL else None

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
    df = pd.DataFrame(rows).rename(columns={"date":"DATE","value":"VALUE"})
    return df

def _safe_name(x: str) -> str:
    return "".join(c for c in x if c.isalnum() or c in ("-","_"," ")).strip().replace(" ","_")

def _write_csv(df: pd.DataFrame, folder: str, filename: str) -> str:
    os.makedirs(folder, exist_ok=True)
    fpath = os.path.abspath(os.path.join(folder, filename))
    df.to_csv(fpath, index=False)
    return fpath

def _get_base_url(request: Request) -> str:
    base = os.getenv("BACKEND_BASE_URL", "").strip().rstrip("/")
    if base:
        return base
    return str(request.base_url).rstrip("/")

def _call_classical_start(base_url: str, parameter: str, state: str) -> str:
    url = f"{base_url}/classical/start"
    payload = {
        "target_value": parameter,
        "state_name": state,
        "agg": "mean",
        "ftype": "F",
    }
    r = requests.post(url, json=payload, timeout=15)
    r.raise_for_status()
    data = r.json()
    job_id = data.get("job_id")
    if not job_id:
        raise HTTPException(status_code=500, detail="No job_id returned from classical/start")
    return job_id

def _wait_for_job(base_url: str, job_id: str, timeout_sec: int = 240) -> dict:
    url = f"{base_url}/classical/status"
    import time as _t
    t0 = _t.time()
    while _t.time() - t0 < timeout_sec:
        rr = requests.get(url, params={"job_id": job_id}, timeout=10)
        rr.raise_for_status()
        st = rr.json()
        if st.get("state") == "ready":
            return st
        if st.get("state") in ("error", "failed"):
            raise HTTPException(status_code=500, detail=st.get("message", "classical job failed"))
        _t.sleep(1.0)
    raise HTTPException(status_code=504, detail="Timeout waiting for classical job")

def _download_job_csv(base_url: str, job_id: str) -> pd.DataFrame:
    url = f"{base_url}/classical/download"
    rr = requests.get(url, params={"job_id": job_id}, timeout=30)
    rr.raise_for_status()
    import io as _io
    return pd.read_csv(_io.StringIO(rr.text))

def _insert_to_staging(df: pd.DataFrame, parameter: str, state: str, forecast_id: str):
    if not engine2:
        return
    tmp = df.copy()
    if "DATE" in tmp.columns and "VALUE" in tmp.columns:
        tmp["parameter_name"] = parameter
        tmp["state_name"] = state
        tmp["forecast_id"] = forecast_id
        ins_sql = f"""
            INSERT INTO {ENGINE_STAGING_TABLE} (date, value, parameter_name, state_name, forecast_id)
            VALUES (:date, :value, :parameter_name, :state_name, :forecast_id)
        """
        try:
            recs = tmp[["DATE","VALUE","parameter_name","state_name","forecast_id"]].to_dict(orient="records")
            with engine2.begin() as conn:
                for r in recs:
                    conn.execute(text(ins_sql), {
                        "date": r["DATE"],
                        "value": r["VALUE"],
                        "parameter_name": r["parameter_name"],
                        "state_name": r["state_name"],
                        "forecast_id": r["forecast_id"],
                    })
        except Exception:
            pass

@router.get("/classical", response_class=HTMLResponse)
def classical_form(request: Request):
    params = _list_params()
    states = _list_states()
    return templates.TemplateResponse("forms/classical.html", {"request": request, "params": params, "states": states})

@router.post("/classical/run")
def classical_run(request: Request, parameter: str = Form(...), state: str = Form(...)):
    # 1) RAW aggregation (saved only for record-keeping)
    df_raw = _daily_mean_df(parameter, state)
    today = datetime.utcnow().strftime("%Y%m%d")
    safe_param = _safe_name(parameter)
    safe_state = _safe_name(state)
    jobs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "_jobs"))
    raw_name = f"{safe_param}_{safe_state}_{today}_raw.csv"
    _write_csv(df_raw, jobs_dir, raw_name)

    # 2) Run classical, wait, download FINAL df
    base = _get_base_url(request)
    job_id = _call_classical_start(base, parameter, state)
    _ = _wait_for_job(base, job_id, timeout_sec=240)
    df_final = _download_job_csv(base, job_id)

    # 3) Compute final filename and inject forecast_name as 2nd column
    final_name = f"{safe_param}_{safe_state}_{today}.csv"
    # Insert at position 1 (second column), repeating filename for all rows
    df_final.insert(1, "forecast_name", final_name)

    # 4) Add job id (forecast_id) as first column (if not already present)
    if "forecast_id" not in df_final.columns or df_final.columns.get_loc("forecast_id") != 0:
        df_final.insert(0, "forecast_id", job_id)

    # 5) Save final CSV and best-effort insert to staging
    final_path = _write_csv(df_final, jobs_dir, final_name)
    _insert_to_staging(df_final, parameter, state, job_id)

    # 6) Return final file
    return FileResponse(final_path, media_type="text/csv", filename=final_name)
