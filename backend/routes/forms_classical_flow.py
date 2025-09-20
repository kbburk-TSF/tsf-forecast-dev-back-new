
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

DB_TABLE = "air_quality_raw"

ENGINE_DATABASE_URL = os.getenv("ENGINE_DATABASE_URL", "").strip()
ENGINE_STAGING_TABLE = os.getenv("ENGINE_STAGING_TABLE", "engine.staging_historical")
engine2 = create_engine(ENGINE_DATABASE_URL) if ENGINE_DATABASE_URL else None

def _list_params():
    sql = f"""
        SELECT DISTINCT "Parameter Name" AS param
        FROM {DB_TABLE}
        WHERE "Parameter Name" IS NOT NULL
        ORDER BY "Parameter Name"
    """
    with app_engine.begin() as conn:
        conn.execute(text(\'SET search_path TO air_quality_demo_data, public\'))
        return [r["param"] for r in conn.execute(text(sql)).mappings().all()]

def _list_states():
    sql = f"""
        SELECT DISTINCT "State Name" AS state
        FROM {DB_TABLE}
        WHERE "State Name" IS NOT NULL
        ORDER BY "State Name"
    """
    with app_engine.begin() as conn:
        conn.execute(text(\'SET search_path TO air_quality_demo_data, public\'))
        return [r["state"] for r in conn.execute(text(sql)).mappings().all()]

def _daily_mean_df(parameter: str, state: str) -> pd.DataFrame:
    sql = f"""
        SELECT DATE("Date Local") AS date, AVG("Arithmetic Mean") AS value
        FROM {DB_TABLE}
        WHERE "Parameter Name" = :parameter AND "State Name" = :state
        GROUP BY DATE("Date Local")
        ORDER BY DATE("Date Local")
    """
    with app_engine.begin() as conn:
        conn.execute(text(\'SET search_path TO air_quality_demo_data, public\'))
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

def _call_classical_start(parameter: str, state: str) -> str:
    url = os.getenv("CLASSICAL_START_URL", "/classical/start")
    if url.startswith("/"):
        url = os.getenv("BACKEND_BASE_URL", "").rstrip("/") + url
    payload = {
        "target_value": parameter,
        "state_name": state,
        "agg": "mean",
        "ftype": "F",
    }
    r = requests.post(url, json=payload, timeout=10)
    r.raise_for_status()
    data = r.json()
    job_id = data.get("job_id")
    if not job_id:
        raise HTTPException(status_code=500, detail="No job_id returned from classical/start")
    return job_id

def _wait_for_job(job_id: str, timeout_sec: int = 180) -> dict:
    status_url = os.getenv("CLASSICAL_STATUS_URL", "/classical/status")
    if status_url.startswith("/"):
        status_url = os.getenv("BACKEND_BASE_URL", "").rstrip("/") + status_url
    t0 = time.time()
    while time.time() - t0 < timeout_sec:
        rr = requests.get(status_url, params={"job_id": job_id}, timeout=10)
        rr.raise_for_status()
        st = rr.json()
        if st.get("state") == "ready":
            return st
        if st.get("state") in ("error","failed"):
            raise HTTPException(status_code=500, detail=st.get("message","classical job failed"))
        time.sleep(1.0)
    raise HTTPException(status_code=504, detail="Timeout waiting for classical job")

def _download_job_csv(job_id: str) -> pd.DataFrame:
    dl_url = os.getenv("CLASSICAL_DOWNLOAD_URL", "/classical/download")
    if dl_url.startswith("/"):
        dl_url = os.getenv("BACKEND_BASE_URL", "").rstrip("/") + dl_url
    rr = requests.get(dl_url, params={"job_id": job_id}, timeout=30)
    rr.raise_for_status()
    df = pd.read_csv(io.StringIO(rr.text))
    return df

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
def classical_run(parameter: str = Form(...), state: str = Form(...)):
    df_raw = _daily_mean_df(parameter, state)
    today = datetime.utcnow().strftime("%Y%m%d")
    safe_param = _safe_name(parameter)
    safe_state = _safe_name(state)
    raw_name = f"{safe_param}_{safe_state}_{today}_raw.csv"
    jobs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "_jobs"))
    raw_path = _write_csv(df_raw, jobs_dir, raw_name)

    job_id = _call_classical_start(parameter, state)
    _ = _wait_for_job(job_id, timeout_sec=180)
    df_final = _download_job_csv(job_id)
    df_final.insert(0, "forecast_id", job_id)

    final_name = f"{safe_param}_{safe_state}_{today}.csv"
    final_path = _write_csv(df_final, jobs_dir, final_name)

    _insert_to_staging(df_final, parameter, state, job_id)

    return FileResponse(final_path, media_type="text/csv", filename=final_name)
