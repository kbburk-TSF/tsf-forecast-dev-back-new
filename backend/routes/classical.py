
import os

# -------- Runtime knobs (fast & stable on small servers) --------
# Pin thread counts (helps on small Render instances)
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")

# Sanitize envs commonly afflicted by newlines; Neon-safe channel binding
for _k in ["DATABASE_URL", "NEON_DATABASE_URL", "PGCHANNELBINDING"]:
    if _k in os.environ and isinstance(os.environ[_k], str):
        os.environ[_k] = os.environ[_k].strip()
os.environ["PGCHANNELBINDING"] = os.environ.get("PGCHANNELBINDING", "disable").strip()
# ---------------------------------------------------------------

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

# Fast local job storage in /tmp (NOT the repo dir)
JOBS_DIR = Path(os.getenv("TSF_JOBS_DIR", "/tmp/tsf_jobs"))
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

# ---------- model helpers (stability + speed) ----------
def _ensure_positive(y: pd.Series) -> bool:
    return (y > 0).all()

def _zscale(series: pd.Series):
    m = float(series.mean())
    s = float(series.std(ddof=0))
    if not np.isfinite(s) or s == 0.0:
        s = 1.0
    return (series - m) / s, m, s

def _inv_zscale(fc: pd.Series, m: float, s: float):
    return fc * s + m

def _detect_fast_seasonality(y: pd.Series) -> Optional[int]:
    """Heuristic: pick among common daily periods that show the strongest autocorr: 7, 30, 365.
       Return m or None if no clear signal (keeps runtime small vs exhaustive search)."""
    candidates = [7, 30, 365]
    best_m, best_r = None, 0.0
    y0 = y.dropna()
    if len(y0) < 30:
        return None
    mu = y0.mean()
    if not np.isfinite(mu):
        return None
    yv = y0 - mu
    for m in candidates:
        if len(yv) <= m + 2:
            continue
        r = float(yv.autocorr(lag=m) or 0.0)
        if abs(r) > abs(best_r) and abs(r) >= 0.2:
            best_r, best_m = r, m
    return best_m

def _forecast_daily_path(y_train: pd.Series, horizon_dates: pd.DatetimeIndex, model: str) -> pd.Series:
    steps = len(horizon_dates)
    if steps <= 0:
        return pd.Series(index=horizon_dates, dtype=float)

    # Guard: constant/near-constant -> flat forecast at last value
    if y_train.nunique(dropna=True) <= 1:
        const_val = float(y_train.dropna().iloc[-1])
        return pd.Series([const_val] * steps, index=horizon_dates, dtype=float)

    # z-score for numeric stability; invert after forecasting
    yz, m, s = _zscale(y_train)

    if model == "SES":
        # multiplicative trend requires positives; try Box-Cox when safe
        if _ensure_positive(y_train):
            try:
                fit = ExponentialSmoothing(
                    yz,
                    trend="mul",
                    seasonal=None,
                    initialization_method="estimated",
                    use_boxcox=True,
                    remove_bias=True,
                ).fit(optimized=True)
                fc = fit.forecast(steps)
            except Exception:
                fit = ExponentialSmoothing(
                    yz, trend="add", seasonal=None, initialization_method="estimated"
                ).fit(optimized=True)
                fc = fit.forecast(steps)
        else:
            fit = ExponentialSmoothing(
                yz, trend="add", seasonal=None, initialization_method="estimated"
            ).fit(optimized=True)
            fc = fit.forecast(steps)

    elif model == "HOLT":
        try:
            fit = Holt(
                yz, exponential=False, damped_trend=True, initialization_method="estimated"
            ).fit(optimized=True)
            fc = fit.forecast(steps)
        except Exception:
            last = yz.ewm(alpha=0.3, adjust=False).mean().iloc[-1]
            fc = pd.Series([last] * steps, index=horizon_dates, dtype=float)

    elif model == "ARIMA":
        try:
            m_seas = _detect_fast_seasonality(y_train)  # None, 7, 30, or 365
            seasonal = bool(m_seas)
            arma = pm.auto_arima(
                yz,
                seasonal=seasonal,
                m=(m_seas or 1),
                stepwise=True,
                suppress_warnings=True,
                error_action="ignore",
                information_criterion="aicc",
                max_p=2, max_q=2, max_d=1,
                max_P=1, max_Q=1, max_D=1,
                max_order=5,
                n_jobs=1,
            )
            fc_vals = arma.predict(steps)
            fc = pd.Series(fc_vals, index=horizon_dates, dtype=float)
        except Exception:
            last = yz.ewm(alpha=0.3, adjust=False).mean().iloc[-1]
            fc = pd.Series([last] * steps, index=horizon_dates, dtype=float)
    else:
        raise ValueError("Unknown model")

    if not isinstance(fc, pd.Series):
        fc = pd.Series(fc, index=horizon_dates, dtype=float)
    else:
        fc.index = horizon_dates

    # invert scaling
    out = _inv_zscale(fc, m, s)

    # clip extreme outliers to a sane envelope around history (prevents blowups)
    q1, q3 = float(y_train.quantile(0.25)), float(y_train.quantile(0.75))
    iqr = max(1e-9, q3 - q1)
    lo = q1 - 10.0 * iqr
    hi = q3 + 10.0 * iqr
    return out.clip(lo, hi)

# ---------- build final (daily-path, granular progress) ----------
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
        ses_m = pd.concat([ses_m, _forecast_daily_path(y_train, horizon, "SES")])
        step("monthly: SES", f"{start.date()} ({i}/{len(m_starts)-1})")
        holt_m = pd.concat([holt_m, _forecast_daily_path(y_train, horizon, "HOLT")])
        step("monthly: HOLT", f"{start.date()} ({i}/{len(m_starts)-1})")
        arima_m = pd.concat([arima_m, _forecast_daily_path(y_train, horizon, "ARIMA")])
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
        ses_q = pd.concat([ses_q, _forecast_daily_path(y_train, horizon, "SES")])
        step("quarterly: SES", f"{start.date()} ({i}/{len(q_starts)-1})")
        holt_q = pd.concat([holt_q, _forecast_daily_path(y_train, horizon, "HOLT")])
        step("quarterly: HOLT", f"{start.date()} ({i}/{len(q_starts)-1})")
        arima_q = pd.concat([arima_q, _forecast_daily_path(y_train, horizon, "ARIMA")])
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

# ---------- API ----------
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
