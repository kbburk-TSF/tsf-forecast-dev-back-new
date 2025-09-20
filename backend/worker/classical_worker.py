
import os, json
from typing import Optional
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor

from statsmodels.tsa.holtwinters import ExponentialSmoothing, Holt
import pmdarima as pm  # auto.arima

# Threads: avoid oversubscription on small boxes
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
for _k in ["DATABASE_URL", "NEON_DATABASE_URL", "PGCHANNELBINDING"]:
    if _k in os.environ and isinstance(os.environ[_k], str):
        os.environ[_k] = os.environ[_k].strip()
os.environ["PGCHANNELBINDING"] = os.environ.get("PGCHANNELBINDING", "disable").strip()

DEFAULT_TABLE = os.getenv("TSF_TABLE", "air_quality_raw")

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
    q = f'''
        SELECT DATE("Date Local") AS date, AVG("Arithmetic Mean") AS value
        FROM {DEFAULT_TABLE}
        WHERE {' AND '.join(where)}
        GROUP BY DATE("Date Local")
        ORDER BY DATE("Date Local")
    '''
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    if not rows:
        raise RuntimeError("No data for selection")
    df = pd.DataFrame(rows).rename(columns={"date":"DATE","value":"VALUE"})
    df["DATE"] = pd.to_datetime(df["DATE"])
    df = df.sort_values("DATE").reset_index(drop=True)
    return df

# ---- stability helpers ----
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
    cands = [7, 30, 365]
    best_m, best_r = None, 0.0
    y0 = y.dropna()
    if len(y0) < 30:
        return None
    mu = y0.mean()
    if not np.isfinite(mu):
        return None
    yv = y0 - mu
    for m in cands:
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
    if y_train.nunique(dropna=True) <= 1:
        const_val = float(y_train.dropna().iloc[-1])
        return pd.Series([const_val]*steps, index=horizon_dates, dtype=float)
    yz, m, s = _zscale(y_train)

    if model == "SES":
        if _ensure_positive(y_train):
            try:
                fit = ExponentialSmoothing(yz, trend='mul', seasonal=None, initialization_method="estimated", use_boxcox=True, remove_bias=True).fit(optimized=True)
                fc = fit.forecast(steps)
            except Exception:
                fit = ExponentialSmoothing(yz, trend='add', seasonal=None, initialization_method="estimated").fit(optimized=True)
                fc = fit.forecast(steps)
        else:
            fit = ExponentialSmoothing(yz, trend='add', seasonal=None, initialization_method="estimated").fit(optimized=True)
            fc = fit.forecast(steps)

    elif model == "HOLT":
        try:
            fit = Holt(yz, exponential=False, damped_trend=True, initialization_method="estimated").fit(optimized=True)
            fc = fit.forecast(steps)
        except Exception:
            last = yz.ewm(alpha=0.3, adjust=False).mean().iloc[-1]
            fc = pd.Series([last]*steps, index=horizon_dates, dtype=float)

    elif model == "ARIMA":
        try:
            m_seas = _detect_fast_seasonality(y_train)
            seasonal = bool(m_seas)
            arma = pm.auto_arima(
                yz, seasonal=seasonal, m=(m_seas or 1),
                stepwise=True, suppress_warnings=True, error_action="ignore",
                information_criterion="aicc",
                max_p=2, max_q=2, max_d=1, max_P=1, max_Q=1, max_D=1, max_order=5,
                n_jobs=1,
            )
            fc_vals = arma.predict(steps)
            fc = pd.Series(fc_vals, index=horizon_dates, dtype=float)
        except Exception:
            last = yz.ewm(alpha=0.3, adjust=False).mean().iloc[-1]
            fc = pd.Series([last]*steps, index=horizon_dates, dtype=float)
    else:
        raise ValueError("Unknown model")

    if not isinstance(fc, pd.Series):
        fc = pd.Series(fc, index=horizon_dates, dtype=float)
    else:
        fc.index = horizon_dates

    out = _inv_zscale(fc, m, s)
    q1, q3 = float(y_train.quantile(0.25)), float(y_train.quantile(0.75))
    iqr = max(1e-9, q3 - q1)
    lo = q1 - 10.0 * iqr
    hi = q3 + 10.0 * iqr
    return out.clip(lo, hi)

def _build_final(daily: pd.DataFrame, tick):
    idx_daily = daily["DATE"]
    y = daily.set_index("DATE")["VALUE"].asfreq("D").interpolate(limit_direction="both")
    m_starts = y.resample("MS").mean().index
    q_starts = y.resample("QS").mean().index
    total_steps = max(0, (len(m_starts)-1)*3) + max(0, (len(q_starts)-1)*3)
    done = 0
    def step(model_label, period_label):
        nonlocal done
        done += 1
        pct = 10 + int(80 * (done / max(1, total_steps)))
        tick(model_label, pct, period_label)

    # monthly
    tick("monthly", 20, "initializing")
    ses_m = pd.Series(index=pd.DatetimeIndex([]), dtype=float)
    holt_m = pd.Series(index=pd.DatetimeIndex([]), dtype=float)
    arima_m = pd.Series(index=pd.DatetimeIndex([]), dtype=float)
    for i in range(1, len(m_starts)):
        start = m_starts[i]
        end = start + pd.offsets.MonthEnd(0)
        horizon = pd.date_range(start=start, end=end, freq="D")
        train_end = start - pd.Timedelta(days=1)
        y_train = y.loc[:train_end].dropna()
        if y_train.empty: continue
        ses_m = pd.concat([ses_m, _forecast_daily_path(y_train, horizon, "SES")]);  step("monthly: SES", f"{start.date()} ({i}/{len(m_starts)-1})")
        holt_m = pd.concat([holt_m, _forecast_daily_path(y_train, horizon, "HOLT")]); step("monthly: HOLT", f"{start.date()} ({i}/{len(m_starts)-1})")
        arima_m = pd.concat([arima_m, _forecast_daily_path(y_train, horizon, "ARIMA")]); step("monthly: ARIMA", f"{start.date()} ({i}/{len(m_starts)-1})")
    # quarterly
    tick("quarterly", 60, "initializing")
    ses_q = pd.Series(index=pd.DatetimeIndex([]), dtype=float)
    holt_q = pd.Series(index=pd.DatetimeIndex([]), dtype=float)
    arima_q = pd.Series(index=pd.DatetimeIndex([]), dtype=float)
    for i in range(1, len(q_starts)):
        start = q_starts[i]
        end = start + pd.offsets.QuarterEnd(startingMonth=12)
        horizon = pd.date_range(start=start, end=end, freq="D")
        train_end = start - pd.Timedelta(days=1)
        y_train = y.loc[:train_end].dropna()
        if y_train.empty: continue
        ses_q = pd.concat([ses_q, _forecast_daily_path(y_train, horizon, "SES")]);   step("quarterly: SES", f"{start.date()} ({i}/{len(q_starts)-1})")
        holt_q = pd.concat([holt_q, _forecast_daily_path(y_train, horizon, "HOLT")]); step("quarterly: HOLT", f"{start.date()} ({i}/{len(q_starts)-1})")
        arima_q = pd.concat([arima_q, _forecast_daily_path(y_train, horizon, "ARIMA")]); step("quarterly: ARIMA", f"{start.date()} ({i}/{len(q_starts)-1})")

    last_day = pd.Timestamp(max([(s.index.max() if len(s.index) else idx_daily.max()) for s in [ses_m, holt_m, arima_m, ses_q, holt_q, arima_q]]))
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

def run_job(job_id: str, target_value: str, state_name: Optional[str], county_name: Optional[str], city_name: Optional[str], cbsa_name: Optional[str], agg: str, ftype: str, jobs_dir: str):
    from rq import get_current_job
    job = get_current_job()
    def tick(state, progress, message):
        job.meta["progress"] = int(progress)
        job.meta["message"] = f"{state} — {message}"
        job.save_meta()

    job.meta["progress"] = 5; job.meta["message"] = "queued"; job.save_meta()

    df_daily = _load_daily(target_value, state_name, county_name, city_name, cbsa_name)
    job.meta["progress"] = 15; job.meta["message"] = "loading-data"; job.save_meta()

    final = _build_final(df_daily, tick)
    job.meta["progress"] = 95; job.meta["message"] = "finalizing"; job.save_meta()

    out_dir = Path(jobs_dir); out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{job_id}.csv"
    final.to_csv(out_path, index=False)

    job.meta["progress"] = 100; job.meta["message"] = "ready"; job.save_meta()
    return {"csv": str(out_path)}
