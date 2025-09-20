
import os
# --- Ops hardening: sanitize env/newlines and channel binding for Neon ---
for _k in ["DATABASE_URL", "NEON_DATABASE_URL", "PGCHANNELBINDING"]:
    if _k in os.environ and isinstance(os.environ[_k], str):
        os.environ[_k] = os.environ[_k].strip()
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
import numpy as np
from pathlib import Path

# Models
from statsmodels.tsa.holtwinters import ExponentialSmoothing, Holt
import pmdarima as pm  # auto.arima

router = APIRouter(prefix="/classical", tags=["classical"])

# Use unqualified table name so search_path resolves schema; allow override via TSF_TABLE
DEFAULT_TABLE = os.getenv("TSF_TABLE", "air_quality_raw")

JOBS_DIR = Path(__file__).resolve().parent.parent / "_jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)

# -------- progress utilities --------
def _job_file(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.json"

def _csv_file(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.csv"

def _status_write(job_id: str, state: str, message: str = None, progress: int = None):
    payload = {"job_id": job_id, "state": state}
    if message is not None:
        payload["message"] = message
    if progress is not None:
        payload["progress"] = int(progress)
    _job_file(job_id).write_text(json.dumps(payload))

# ------------------------------------

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
    # Ensure search_path so unqualified DEFAULT_TABLE resolves to your canonical schema
    try:
        with conn.cursor() as cur:
            cur.execute("SET search_path TO air_quality_demo_data, public")
    except Exception:
        pass
    return conn

def _load_daily(parameter: str, state: Optional[str], county: Optional[str], city: Optional[str], cbsa: Optional[str]) -> pd.DataFrame:
    where = ['"Parameter Name" = %s']
    params = [parameter]
    if state:  where.append('"State Name" = %s');  params.append(state)
    if county: where.append('"County Name" = %s'); params.append(county)
    if city:   where.append('"City Name" = %s');   params.append(city)
    if cbsa:   where.append('"CBSA Name" = %s');   params.append(cbsa)
    q = f"""
        SELECT DATE("Date Local") AS date, AVG("Arithmetic Mean") AS value
        FROM {DEFAULT_TABLE}
        WHERE {' AND '.join(where)}
        GROUP BY DATE("Date Local")
        ORDER BY DATE("Date Local")
    """
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="No rows for that selection")
    df = pd.DataFrame(rows).rename(columns={"date":"DATE","value":"VALUE"})
    df["DATE"] = pd.to_datetime(df["DATE"])
    df = df.sort_values("DATE").reset_index(drop=True)
    return df

def _ensure_positive(y: pd.Series) -> bool:
    return (y > 0).all()

def _forecast_daily_path(y_train: pd.Series, horizon_dates: pd.DatetimeIndex, model: str) -> pd.Series:
    steps = len(horizon_dates)
    if steps <= 0:
        return pd.Series(index=horizon_dates, dtype=float)

    if model == "SES":
        # multiplicative trend requires positives; fallback to additive if not
        if _ensure_positive(y_train):
            try:
                fit = ExponentialSmoothing(y_train, trend='mul', seasonal=None, initialization_method="estimated").fit(optimized=True)
                fc = fit.forecast(steps)
            except Exception:
                fit = ExponentialSmoothing(y_train, trend='add', seasonal=None, initialization_method="estimated").fit(optimized=True)
                fc = fit.forecast(steps)
        else:
            fit = ExponentialSmoothing(y_train, trend='add', seasonal=None, initialization_method="estimated").fit(optimized=True)
            fc = fit.forecast(steps)

    elif model == "HOLT":
        try:
            fit = Holt(y_train, exponential=False, damped_trend=True, initialization_method="estimated").fit(optimized=True)
            fc = fit.forecast(steps)
        except Exception:
            last = y_train.ewm(alpha=0.3, adjust=False).mean().iloc[-1]
            fc = pd.Series([last]*steps, index=horizon_dates)

    elif model == "ARIMA":
        try:
            arma = pm.auto_arima(y_train, seasonal=False, stepwise=True, suppress_warnings=True, error_action="ignore")
            fc_vals = arma.predict(steps)
            fc = pd.Series(fc_vals, index=horizon_dates)
        except Exception:
            last = y_train.ewm(alpha=0.3, adjust=False).mean().iloc[-1]
            fc = pd.Series([last]*steps, index=horizon_dates)
    else:
        raise ValueError("Unknown model")

    if not isinstance(fc, pd.Series):
        fc = pd.Series(fc, index=horizon_dates)
    else:
        fc.index = horizon_dates
    return fc

def _build_final(daily: pd.DataFrame, progress_fn):
    idx_daily = daily["DATE"]
    y = daily.set_index("DATE")["VALUE"].asfreq("D").interpolate(limit_direction="both")

    # determine total steps for progress: (n_months-1)*3 + (n_quarters-1)*3
    m_starts = y.resample("MS").mean().index
    q_starts = y.resample("QS").mean().index
    total_steps = max(0, (len(m_starts)-1)*3) + max(0, (len(q_starts)-1)*3)
    done = 0

    def step(model_label, period_label):
        nonlocal done
        done += 1
        # Scale progress from 10%..90% across steps
        pct = 10 + int(80 * (done / max(1, total_steps)))
        progress_fn(f"{model_label}", pct, message=period_label)

    # MONTHLY roll-forward
    progress_fn("monthly", 20, message="initializing")
    ses_m = pd.Series(index=pd.DatetimeIndex([]), dtype=float)
    holt_m = pd.Series(index=pd.DatetimeIndex([]), dtype=float)
    arima_m = pd.Series(index=pd.DatetimeIndex([]), dtype=float)
    for i in range(1, len(m_starts)):
        start = m_starts[i]
        end = start + pd.offsets.MonthEnd(0)
        horizon = pd.date_range(start=start, end=end, freq="D")
        train_end = start - pd.Timedelta(days=1)
        y_train = y.loc[:train_end].dropna()
        if y_train.empty:
            continue
        # SES
        ses_fc = _forecast_daily_path(y_train, horizon, "SES")
        ses_m = pd.concat([ses_m, ses_fc])
        step("monthly: SES", f"{start.date()} ({i}/{len(m_starts)-1})")
        # HOLT
        holt_fc = _forecast_daily_path(y_train, horizon, "HOLT")
        holt_m = pd.concat([holt_m, holt_fc])
        step("monthly: HOLT", f"{start.date()} ({i}/{len(m_starts)-1})")
        # ARIMA
        arima_fc = _forecast_daily_path(y_train, horizon, "ARIMA")
        arima_m = pd.concat([arima_m, arima_fc])
        step("monthly: ARIMA", f"{start.date()} ({i}/{len(m_starts)-1})")

    # QUARTERLY roll-forward
    progress_fn("quarterly", 60, message="initializing")
    ses_q = pd.Series(index=pd.DatetimeIndex([]), dtype=float)
    holt_q = pd.Series(index=pd.DatetimeIndex([]), dtype=float)
    arima_q = pd.Series(index=pd.DatetimeIndex([]), dtype=float)
    for i in range(1, len(q_starts)):
        start = q_starts[i]
        end = start + pd.offsets.QuarterEnd(startingMonth=12)
        horizon = pd.date_range(start=start, end=end, freq="D")
        train_end = start - pd.Timedelta(days=1)
        y_train = y.loc[:train_end].dropna()
        if y_train.empty:
            continue
        # SES
        ses_fc = _forecast_daily_path(y_train, horizon, "SES")
        ses_q = pd.concat([ses_q, ses_fc])
        step("quarterly: SES", f"{start.date()} ({i}/{len(q_starts)-1})")
        # HOLT
        holt_fc = _forecast_daily_path(y_train, horizon, "HOLT")
        holt_q = pd.concat([holt_q, holt_fc])
        step("quarterly: HOLT", f"{start.date()} ({i}/{len(q_starts)-1})")
        # ARIMA
        arima_fc = _forecast_daily_path(y_train, horizon, "ARIMA")
        arima_q = pd.concat([arima_q, arima_fc])
        step("quarterly: ARIMA", f"{start.date()} ({i}/{len(q_starts)-1})")

    # Final window: from first historical day to last covered forecast day
    last_day = pd.Timestamp(max([
        (s.index.max() if len(s.index) else idx_daily.max()) for s in [ses_m, holt_m, arima_m, ses_q, holt_q, arima_q]
    ]))
    all_days = pd.date_range(start=idx_daily.min(), end=last_day, freq="D")

    out = pd.DataFrame(index=all_days)
    out["VALUE"]   = y.reindex(all_days)
    out["SES-M"]   = ses_m.reindex(all_days)
    out["HWES-M"]  = holt_m.reindex(all_days)
    out["ARIMA-M"] = arima_m.reindex(all_days)
    out["SES-Q"]   = ses_q.reindex(all_days)
    out["HWES-Q"]  = holt_q.reindex(all_days)
    out["ARIMA-Q"] = arima_q.reindex(all_days)
    out = out.reset_index().rename(columns={"index":"DATE"})
    return out

class StartRequest(BaseModel):
    target_value: str
    state_name: Optional[str] = None
    county_name: Optional[str] = None
    city_name: Optional[str] = None
    cbsa_name: Optional[str] = None
    agg: str = "mean"
    ftype: str = "F"

@router.post("/start")
def start(req: StartRequest = Body(...)):
    job_id = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    _status_write(job_id, "queued", progress=5)

    def _run():
        try:
            _status_write(job_id, "loading-data", progress=10)
            daily = _load_daily(req.target_value, req.state_name, req.county_name, req.city_name, req.cbsa_name)

            def _cb(state, prog, message=None):
                _status_write(job_id, state, message=message, progress=prog)

            final = _build_final(daily, _cb)
            _status_write(job_id, "finalizing", progress=95)
            final.to_csv(_csv_file(job_id), index=False)
            _status_write(job_id, "ready", progress=100)
        except Exception as e:
            _status_write(job_id, "error", message=str(e))

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
