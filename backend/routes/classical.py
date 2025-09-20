
import os
# --- Channel binding & DSN sanitization (Neon-safe) ---
# Some environments inject a trailing newline into values; strip them.
for k in ["DATABASE_URL", "NEON_DATABASE_URL", "PGCHANNELBINDING"]:
    if k in os.environ and isinstance(os.environ[k], str):
        os.environ[k] = os.environ[k].strip()

# If PGCHANNELBINDING is unset, default to 'disable' for broad compatibility.
os.environ["PGCHANNELBINDING"] = os.environ.get("PGCHANNELBINDING", "disable").strip()

import json
import threading
from typing import Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from psycopg2.extras import RealDictCursor
import psycopg2
import pandas as pd
import numpy as np
from pathlib import Path

router = APIRouter(prefix="/classical", tags=["classical"])

# Table to query for history — default matches the rest of the app
TSF_TABLE = os.getenv("TSF_TABLE", "air_quality_raw").strip()

JOBS_DIR = Path(__file__).resolve().parent.parent / "_jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)

def _get_conn():
    # Prefer DATABASE_URL; fall back to NEON_* pieces if present
    dsn = os.getenv("DATABASE_URL", "").strip()
    if not dsn:
        host = os.getenv("NEON_HOST", "").strip()
        db = os.getenv("NEON_DB", "").strip()
        user = os.getenv("NEON_USER", "").strip()
        pwd = os.getenv("NEON_PASSWORD", "").strip()
        port = os.getenv("NEON_PORT", "5432").strip()
        if not (host and db and user and pwd):
            raise RuntimeError("No DATABASE_URL and incomplete NEON_* env vars")
        dsn = f"postgresql://{user}:{pwd}@{host}:{port}/{db}?sslmode=require"
    # Ensure sslmode=require present
    if "sslmode=" not in dsn:
        sep = "&" if "?" in dsn else "?"
        dsn = f"{dsn}{sep}sslmode=require"
    # psycopg2 will pick up PGCHANNELBINDING from env (sanitized above)
    conn = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
    # Set search_path so unqualified table resolves to your canonical schema first
    with conn.cursor() as cur:
        cur.execute("SET search_path TO air_quality_demo_data, public")
    return conn

def _load_series_from_neon(parameter: str, state: Optional[str]) -> pd.DataFrame:
    q = f"""
        SELECT
            DATE("Date Local") AS date,
            AVG("Arithmetic Mean") AS value
        FROM {TSF_TABLE}
        WHERE "Parameter Name" = %s
          AND (%s IS NULL OR "State Name" = %s)
        GROUP BY DATE("Date Local")
        ORDER BY DATE("Date Local")
    """
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(q, (parameter, state, state))
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="No rows for that selection")
    df = pd.DataFrame(rows)
    df.rename(columns={"date": "DATE", "value": "VALUE"}, inplace=True)
    return df

# ---- minimal SES/HW/ARIMA placeholder wiring (unchanged core assumed) ----
# Assume you already have your _gen_all etc. defined in your working copy.
# Below are thin wrappers: start/status/download with job file IO compatibility.

def _job_file(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.json"

def _csv_file(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.csv"

@router.post("/start")
def start(
    target_value: str = Query(...),
    state_name: Optional[str] = Query(None),
    agg: str = Query("mean"),
    ftype: str = Query("F"),
):
    # Create job stub
    job_id = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    jf = _job_file(job_id)
    jf.write_text(json.dumps({"job_id": job_id, "state": "queued"}))
    # Spawn background worker
    def _run():
        try:
            hist = _load_series_from_neon(target_value, state_name)
            # In your real copy, call your _gen_all(...) to compute final csv
            # For this drop-in, we just echo history as VALUE and add trivial cols
            out = hist.copy()
            out.to_csv(_csv_file(job_id), index=False)
            jf.write_text(json.dumps({"job_id": job_id, "state": "ready"}))
        except Exception as e:
            jf.write_text(json.dumps({"job_id": job_id, "state": "error", "message": str(e)}))
    threading.Thread(target=_run, daemon=True).start()
    return {"job_id": job_id, "state": "queued"}

@router.get("/status")
def status(job_id: str = Query(...)):
    jf = _job_file(job_id)
    if not jf.exists():
        raise HTTPException(status_code=404, detail="job not found")
    return json.loads(jf.read_text())

@router.get("/download")
def download(job_id: str = Query(...)):
    cf = _csv_file(job_id)
    if not cf.exists():
        raise HTTPException(status_code=404, detail="file not ready")
    return FileResponse(cf, media_type="text/csv", filename=cf.name)
