
import os
# --- Sanitize env for Neon/libpq ---
for k in ["DATABASE_URL", "NEON_DATABASE_URL", "PGCHANNELBINDING"]:
    if k in os.environ and isinstance(os.environ[k], str):
        os.environ[k] = os.environ[k].strip()
os.environ["PGCHANNELBINDING"] = os.environ.get("PGCHANNELBINDING", "disable").strip()

import json
import threading
from typing import Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException
from fastapi.params import Body
from fastapi.responses import FileResponse
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
import psycopg2
import pandas as pd
from pathlib import Path

router = APIRouter(prefix="/classical", tags=["classical"])

TSF_TABLE = os.getenv("TSF_TABLE", "air_quality_raw").strip()
JOBS_DIR = Path(__file__).resolve().parent.parent / "_jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)

def _get_conn():
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
    if "sslmode=" not in dsn:
        sep = "&" if "?" in dsn else "?"
        dsn = f"{dsn}{sep}sslmode=require"
    conn = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
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

class StartRequest(BaseModel):
    target_value: str
    state_name: Optional[str] = None
    agg: str = "mean"
    ftype: str = "F"

def _job_file(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.json"

def _csv_file(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.csv"

@router.post("/start")
def start(req: StartRequest = Body(...)):
    job_id = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    jf = _job_file(job_id)
    jf.write_text(json.dumps({"job_id": job_id, "state": "queued"}))
    def _run():
        try:
            hist = _load_series_from_neon(req.target_value, req.state_name)
            out = hist.copy()  # placeholder; your real pipeline would write combined forecast CSV
            out.to_csv(_csv_file(job_id), index=False)
            jf.write_text(json.dumps({"job_id": job_id, "state": "ready"}))
        except Exception as e:
            jf.write_text(json.dumps({"job_id": job_id, "state": "error", "message": str(e)}))
    threading.Thread(target=_run, daemon=True).start()
    return {"job_id": job_id, "state": "queued"}

@router.get("/status")
def status(job_id: str):
    jf = _job_file(job_id)
    if not jf.exists():
        raise HTTPException(status_code=404, detail="job not found")
    return json.loads(jf.read_text())

@router.get("/download")
def download(job_id: str):
    cf = _csv_file(job_id)
    if not cf.exists():
        raise HTTPException(status_code=404, detail="file not ready")
    return FileResponse(cf, media_type="text/csv", filename=cf.name)
