
import os
# --- Sanitize env for Neon/libpq ---
for k in ["DATABASE_URL", "NEON_DATABASE_URL", "PGCHANNELBINDING"]:
    if k in os.environ and isinstance(os.environ[k], str):
        os.environ[k] = os.environ[k].strip()
os.environ["PGCHANNELBINDING"] = os.environ.get("PGCHANNELBINDING", "disable").strip()

import json
import threading
from typing import Optional, Tuple, List
from datetime import datetime
from fastapi import APIRouter, HTTPException
from fastapi.params import Body
from fastapi.responses import FileResponse
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
import psycopg2
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import text

# Statsmodels
from statsmodels.tsa.holtwinters import SimpleExpSmoothing, ExponentialSmoothing
from statsmodels.tsa.arima.model import ARIMA

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

def _load_daily_series(parameter: str, state: Optional[str]) -> pd.DataFrame:
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
    df["DATE"] = pd.to_datetime(df["DATE"])
    df = df.sort_values("DATE").reset_index(drop=True)
    return df

def _resample(df: pd.DataFrame, freq: str) -> pd.Series:
    # Mean aggregate to period frequency; set index to period start
    s = df.set_index("DATE")["VALUE"].resample(freq).mean().dropna()
    return s

def _fit_ses(s: pd.Series) -> pd.Series:
    try:
        model = SimpleExpSmoothing(s, initialization_method="estimated").fit(optimized=True)
        return model.fittedvalues
    except Exception:
        # Fallback to simple rolling mean
        return s.rolling(3, min_periods=1).mean()

def _fit_hwes(s: pd.Series) -> pd.Series:
    try:
        model = ExponentialSmoothing(s, trend="add", seasonal=None, initialization_method="estimated").fit(optimized=True)
        return model.fittedvalues
    except Exception:
        return s.rolling(3, min_periods=1).mean()

def _fit_arima_best(s: pd.Series) -> pd.Series:
    # Try a tiny grid of ARIMA orders and pick the lowest AIC
    candidates: List[Tuple[int,int,int]] = [(1,0,0),(0,1,1),(1,1,0)]
    best = None
    best_aic = np.inf
    for order in candidates:
        try:
            res = ARIMA(s, order=order).fit()
            if res.aic < best_aic:
                best_aic = res.aic
                best = res
        except Exception:
            continue
    if best is None:
        return s.rolling(3, min_periods=1).mean()
    # use in-sample predictions aligned to index
    pred = best.fittedvalues
    # Ensure same index as s
    pred = pred.reindex(s.index, method="nearest")
    return pred

def _expand_to_daily(period_series: pd.Series) -> pd.Series:
    # period_series has period starts as index; expand to daily by forward-fill within each period
    daily = period_series.copy()
    daily.index = pd.to_datetime(daily.index)
    # Build daily index from min to max
    idx = pd.date_range(start=daily.index.min(), end=daily.index.max() + pd.tseries.frequencies.to_offset("MS") - pd.Timedelta(days=1), freq="D")
    # Reindex at period starts, then forward-fill
    expanded = daily.reindex(idx, method=None)
    expanded = expanded.ffill()
    # trim to original overall daily range of the data
    return expanded

def _gen_all(df_daily: pd.DataFrame) -> pd.DataFrame:
    # Monthly
    s_m = _resample(df_daily, "MS")
    ses_m = _fit_ses(s_m)
    hw_m  = _fit_hwes(s_m)
    ar_m  = _fit_arima_best(s_m)

    # Quarterly
    s_q = _resample(df_daily, "QS")
    ses_q = _fit_ses(s_q)
    hw_q  = _fit_hwes(s_q)
    ar_q  = _fit_arima_best(s_q)

    # Expand to daily coverage
    ses_m_d = _expand_to_daily(ses_m)
    hw_m_d  = _expand_to_daily(hw_m)
    ar_m_d  = _expand_to_daily(ar_m)

    ses_q_d = _expand_to_daily(ses_q)
    hw_q_d  = _expand_to_daily(hw_q)
    ar_q_d  = _expand_to_daily(ar_q)

    # Assemble final DataFrame
    out = df_daily.set_index("DATE")[["VALUE"]].copy()
    out["SES-M"]   = ses_m_d.reindex(out.index, method="ffill")
    out["SES-Q"]   = ses_q_d.reindex(out.index, method="ffill")
    out["HWES-M"]  = hw_m_d.reindex(out.index, method="ffill")
    out["HWES-Q"]  = hw_q_d.reindex(out.index, method="ffill")
    out["ARIMA-M"] = ar_m_d.reindex(out.index, method="ffill")
    out["ARIMA-Q"] = ar_q_d.reindex(out.index, method="ffill")
    out = out.reset_index()
    return out

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
            hist = _load_daily_series(req.target_value, req.state_name)
            final = _gen_all(hist)
            final.to_csv(_csv_file(job_id), index=False)
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
