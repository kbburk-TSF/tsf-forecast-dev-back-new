
import os
# --- Ops hardening: sanitize env/newlines and channel binding for Neon ---
for _k in ["DATABASE_URL", "NEON_DATABASE_URL", "PGCHANNELBINDING"]:
    if _k in os.environ and isinstance(os.environ[_k], str):
        os.environ[_k] = os.environ[_k].strip()
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

# Models
from statsmodels.tsa.holtwinters import ExponentialSmoothing, Holt
import pmdarima as pm  # auto.arima

router = APIRouter(prefix="/classical", tags=["classical"])

# Use unqualified table name so search_path resolves schema; allow override via TSF_TABLE
DEFAULT_TABLE = os.getenv("TSF_TABLE", "air_quality_raw")

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

def _resample_period_means(df_daily: pd.DataFrame, freq: str) -> pd.Series:
    return df_daily.set_index("DATE")["VALUE"].resample(freq).mean().dropna()

def _rolling_forecast_monthly(y_m: pd.Series) -> pd.Series:
    # Walk-forward: for i=1..n-1, fit on 0..i-1, predict month i; no beyond-history month.
    idx = y_m.index
    n = len(y_m)
    f_ses = pd.Series(index=idx, dtype=float)
    f_holt = pd.Series(index=idx, dtype=float)
    f_arima = pd.Series(index=idx, dtype=float)
    for i in range(1, n):
        y_train = y_m.iloc[:i]
        # SES = ETS ZZZ with multiplicative trend -> ExponentialSmoothing with trend='mul'
        try:
            ses_model = ExponentialSmoothing(y_train, trend='mul', seasonal=None, initialization_method="estimated")
            ses_fit = ses_model.fit(optimized=True)
            f_ses.iloc[i] = float(ses_fit.forecast(1)[0])
        except Exception:
            f_ses.iloc[i] = y_train.ewm(alpha=0.3, adjust=False).mean().iloc[-1]
        # HOLT = two-parameter, additive, damped
        try:
            holt_model = Holt(y_train, exponential=False, damped_trend=True, initialization_method="estimated")
            holt_fit = holt_model.fit(optimized=True)
            f_holt.iloc[i] = float(holt_fit.forecast(1)[0])
        except Exception:
            f_holt.iloc[i] = y_train.ewm(alpha=0.3, adjust=False).mean().iloc[-1]
        # ARIMA = auto.arima
        try:
            arima_model = pm.auto_arima(y_train, seasonal=False, stepwise=True, suppress_warnings=True, error_action="ignore")
            f_arima.iloc[i] = float(arima_model.predict(1)[0])
        except Exception:
            f_arima.iloc[i] = y_train.ewm(alpha=0.3, adjust=False).mean().iloc[-1]
    # Expand to daily spans for each predicted month (fill across that month’s calendar days)
    out_idx = pd.date_range(start=y_m.index.min(), end=y_m.index.max() + pd.offsets.MonthEnd(0), freq="D")
    ses_d = pd.Series(index=out_idx, dtype=float)
    holt_d = pd.Series(index=out_idx, dtype=float)
    arima_d = pd.Series(index=out_idx, dtype=float)
    for i in range(1, n):
        start = idx[i]
        end = (start + pd.offsets.MonthEnd(0))
        span = pd.date_range(start=start, end=end, freq="D")
        ses_d.loc[span] = f_ses.iloc[i]
        holt_d.loc[span] = f_holt.iloc[i]
        arima_d.loc[span] = f_arima.iloc[i]
    return ses_d, holt_d, arima_d

def _rolling_forecast_quarterly(y_q: pd.Series) -> pd.Series:
    idx = y_q.index
    n = len(y_q)
    f_ses = pd.Series(index=idx, dtype=float)
    f_holt = pd.Series(index=idx, dtype=float)
    f_arima = pd.Series(index=idx, dtype=float)
    for i in range(1, n):
        y_train = y_q.iloc[:i]
        try:
            ses_model = ExponentialSmoothing(y_train, trend='mul', seasonal=None, initialization_method="estimated")
            ses_fit = ses_model.fit(optimized=True)
            f_ses.iloc[i] = float(ses_fit.forecast(1)[0])
        except Exception:
            f_ses.iloc[i] = y_train.ewm(alpha=0.3, adjust=False).mean().iloc[-1]
        try:
            holt_model = Holt(y_train, exponential=False, damped_trend=True, initialization_method="estimated")
            holt_fit = holt_model.fit(optimized=True)
            f_holt.iloc[i] = float(holt_fit.forecast(1)[0])
        except Exception:
            f_holt.iloc[i] = y_train.ewm(alpha=0.3, adjust=False).mean().iloc[-1]
        try:
            arima_model = pm.auto_arima(y_train, seasonal=False, stepwise=True, suppress_warnings=True, error_action="ignore")
            f_arima.iloc[i] = float(arima_model.predict(1)[0])
        except Exception:
            f_arima.iloc[i] = y_train.ewm(alpha=0.3, adjust=False).mean().iloc[-1]
    # Expand to daily spans for each predicted quarter (fill across that quarter’s calendar days)
    out_idx = pd.date_range(start=y_q.index.min(), end=y_q.index.max() + pd.offsets.QuarterEnd(startingMonth=12), freq="D")
    ses_d = pd.Series(index=out_idx, dtype=float)
    holt_d = pd.Series(index=out_idx, dtype=float)
    arima_d = pd.Series(index=out_idx, dtype=float)
    for i in range(1, n):
        start = idx[i]
        end = (start + pd.offsets.QuarterEnd(startingMonth=12))
        span = pd.date_range(start=start, end=end, freq="D")
        ses_d.loc[span] = f_ses.iloc[i]
        holt_d.loc[span] = f_holt.iloc[i]
        arima_d.loc[span] = f_arima.iloc[i]
    return ses_d, holt_d, arima_d

def _build_final(df_daily: pd.DataFrame) -> pd.DataFrame:
    # Resample to period means
    y_m = _resample_period_means(df_daily, "MS")   # months start
    y_q = _resample_period_means(df_daily, "QS")   # quarters start

    # Rolling forecasts per spec (start at 2nd period, span full month/quarter, no beyond-history)
    ses_m_d, holt_m_d, arima_m_d = _rolling_forecast_monthly(y_m)
    ses_q_d, holt_q_d, arima_q_d = _rolling_forecast_quarterly(y_q)

    # Final daily index: from first history day to last day covered by any fill
    last_day = max(
        x.index[~x.isna()].max() for x in [ses_m_d, holt_m_d, arima_m_d, ses_q_d, holt_q_d, arima_q_d]
        if (~x.isna()).any()
    )
    daily_idx = pd.date_range(start=df_daily["DATE"].min(), end=last_day, freq="D")

    out = pd.DataFrame(index=daily_idx)
    out["VALUE"]   = df_daily.set_index("DATE")["VALUE"].reindex(daily_idx)
    out["SES-M"]   = ses_m_d.reindex(daily_idx)
    out["HWES-M"]  = holt_m_d.reindex(daily_idx)
    out["ARIMA-M"] = arima_m_d.reindex(daily_idx)
    out["SES-Q"]   = ses_q_d.reindex(daily_idx)
    out["HWES-Q"]  = holt_q_d.reindex(daily_idx)
    out["ARIMA-Q"] = arima_q_d.reindex(daily_idx)
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
            daily = _load_daily(req.target_value, req.state_name, req.county_name, req.city_name, req.cbsa_name)
            final = _build_final(daily)
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
