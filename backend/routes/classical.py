from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse
from typing import Optional, Tuple
import os, uuid, threading, time, json
import pandas as pd
import numpy as np

# DB
import psycopg2
from psycopg2.extras import RealDictCursor

import os
# --- Neon/libpq env fixes (do not touch forecast logic) ---
for _k in ["DATABASE_URL", "NEON_DATABASE_URL", "PGCHANNELBINDING"]:
    if _k in os.environ and isinstance(os.environ[_k], str):
        os.environ[_k] = os.environ[_k].strip()
os.environ["PGCHANNELBINDING"] = os.environ.get("PGCHANNELBINDING", "disable").strip()
router = APIRouter()

# ---------- Paths ----------
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_BASE_DIR, ".."))          # backend/
_DATA_DIR = os.path.join(_ROOT, "data")
_OUTPUT_DIR = os.path.join(_DATA_DIR, "output")
os.makedirs(_OUTPUT_DIR, exist_ok=True)

# ---------- Durable job store ----------
JOBS_DIR = os.path.join(_OUTPUT_DIR, "_jobs")
os.makedirs(JOBS_DIR, exist_ok=True)
HEARTBEAT_SECS = 120  # mark paused after this; client can POST /resume

def _job_path(job_id: str) -> str: return os.path.join(JOBS_DIR, f"{job_id}.json")
def _job_write(job_id: str, **data):
    data = dict(data); data["updated_at"] = time.time()
    with open(_job_path(job_id), "w", encoding="utf-8") as f: json.dump(data, f)
def _job_read(job_id: str) -> Optional[dict]:
    p = _job_path(job_id)
    if not os.path.exists(p): return None
    with open(p, "r", encoding="utf-8") as f: return json.load(f)
def _pulse(job_id: str, **fields):
    job = _job_read(job_id) or {}; job.update(fields); _job_write(job_id, **job)
def _is_stale(job: dict) -> bool:
    return job.get("state") == "running" and (time.time() - float(job.get("updated_at",0))) > HEARTBEAT_SECS

# ---------- Helpers ----------
def _clean(x: Optional[str]) -> Optional[str]:
    if x is None: return None
    s = str(x).strip()
    return s if s else None

def _compose_instance_name(
    target_value: str,
    state: Optional[str] = "",
    county_name: Optional[str] = "",
    city_name: Optional[str] = "",
    cbsa_name: Optional[str] = "",
    ftype: str = "F"
) -> str:
    """Builds a safe filename from the input parameters."""
    parts = []
    for p in [target_value, state, county_name, city_name, cbsa_name, ftype or "F"]:
        if p:
            safe = str(p).strip().replace("/", "-").replace("\\", "-").replace(" ", "_")
            parts.append(safe)
    return "_".join(parts)

def _monthly_quarterly(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    m = df.set_index("DATE")["VALUE"].resample("MS").mean()
    q = df.set_index("DATE")["VALUE"].resample("QS").mean()
    return m, q

# ---------- DB Helpers ----------
def _get_conn():
    import re
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        # Normalize SQLAlchemy-style schemes to plain Postgres for psycopg2
        dsn = re.sub(r"^(postgresql?|postgres)\+psycopg2?://", r"\1://", dsn, flags=re.I)
        if "sslmode=" not in dsn:
            dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
        __conn = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
        with __conn.cursor() as __cur:
            __cur.execute("SET search_path TO air_quality_demo_data, public")
        return __conn
    host = os.getenv("NEON_HOST")
    db   = os.getenv("NEON_DB")
    user = os.getenv("NEON_USER")
    pwd  = os.getenv("NEON_PASSWORD")
    port = os.getenv("NEON_PORT", "5432")
    if not all([host, db, user, pwd]):
        raise HTTPException(status_code=500, detail="Neon connection env vars not set (DATABASE_URL or NEON_*).")
    __conn = psycopg2.connect(
        host=host, dbname=db, user=user, password=pwd, port=port,
        cursor_factory=RealDictCursor, sslmode="require"
    )
        with __conn.cursor() as __cur:
            __cur.execute("SET search_path TO air_quality_demo_data, public")
        return __conn

DEFAULT_TABLE = os.getenv("TSF_TABLE", 'demo_air_quality.air_quality_raw')

# ---------- Load series from Neon ----------
def _load_series_from_neon(
    db: str,
    target_value: str,
    state: Optional[str],
    county_name: Optional[str],
    city_name: Optional[str],
    cbsa_name: Optional[str],
    agg: str,
    ftype: str,
) -> pd.DataFrame:
    table = DEFAULT_TABLE
    q = f"""
        SELECT
            DATE("Date Local") AS date,
            AVG("Arithmetic Mean") AS value
        FROM {table}
        WHERE "Parameter Name" = %s
          AND (%s IS NULL OR "State Name"  = %s)
          AND (%s IS NULL OR "County Name" = %s)
          AND (%s IS NULL OR "City Name"   = %s)
          AND (%s IS NULL OR "CBSA Name"   = %s)
        GROUP BY DATE("Date Local")
        ORDER BY DATE("Date Local")
    """
    params = [
        _clean(target_value),
        _clean(state), _clean(state),
        _clean(county_name), _clean(county_name),
        _clean(city_name), _clean(city_name),
        _clean(cbsa_name), _clean(cbsa_name),
    ]
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(q, params)
            rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Neon query failed: {str(e)}")
    if not rows:
        raise HTTPException(status_code=404, detail="No matching rows in Neon for the given filters.")
    df = pd.DataFrame(rows)
    df.rename(columns={"date":"DATE", "value":"VALUE"}, inplace=True)
    df["DATE"] = pd.to_datetime(df["DATE"])
    df = df.sort_values("DATE")
    return df

# ---------- Forecast models ----------
def _walk_forward_ses(job_id: str, label: str, series: pd.Series, base_percent: int, span_percent: int) -> pd.Series:
    from statsmodels.tsa.holtwinters import SimpleExpSmoothing
    preds, idx = {}, series.index
    n = max(len(series) - 1, 1)
    for i in range(1, len(series)):
        hist = series.iloc[:i]
        try:
            fit = SimpleExpSmoothing(hist, initialization_method="estimated").fit(optimized=True)
            pred = float(fit.forecast(1)[0])
        except Exception:
            span = max(2, min(12, max(2, len(hist)//2)))
            pred = float(hist.ewm(span=span, adjust=False).mean().iloc[-1])
        preds[idx[i]] = pred
        pct = base_percent + int((i / n) * span_percent)
        _pulse(job_id, state="running", message=f"{label} {i}/{n}", percent=pct, done=0, total=1)
    if len(series) >= 1:
        hist = series
        try:
            fit = SimpleExpSmoothing(hist, initialization_method="estimated").fit(optimized=True)
            pred = float(fit.forecast(1)[0])
        except Exception:
            span = max(2, min(12, max(2, len(hist)//2)))
            pred = float(hist.ewm(span=span, adjust=False).mean().iloc[-1])
        last_ts = idx[-1]
        next_ts = (last_ts + (pd.offsets.MonthBegin(1) if idx.freqstr == "MS" else pd.offsets.QuarterBegin(1))).normalize()
        preds[next_ts] = pred
    return pd.Series(preds, name="ses").sort_index()

def _walk_forward_hwes(job_id: str, label: str, series: pd.Series, base_percent: int, span_percent: int) -> pd.Series:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    preds, idx = {}, series.index
    n = max(len(series) - 1, 1)
    for i in range(1, len(series)):
        hist = series.iloc[:i]
        try:
            fit = ExponentialSmoothing(hist, trend="add", seasonal=None, initialization_method="estimated").fit(optimized=True)
            pred = float(fit.forecast(1)[0])
        except Exception:
            pred = float(hist.ewm(span=max(2, min(12, max(2, len(hist)//2))), adjust=False).mean().iloc[-1])
        preds[idx[i]] = pred
        pct = base_percent + int((i / n) * span_percent)
        _pulse(job_id, state="running", message=f"{label} {i}/{n}", percent=pct, done=0, total=1)
    if len(series) >= 1:
        hist = series
        try:
            fit = ExponentialSmoothing(hist, trend="add", seasonal=None, initialization_method="estimated").fit(optimized=True)
            pred = float(fit.forecast(1)[0])
        except Exception:
            pred = float(hist.ewm(span=max(2, min(12, max(2, len(hist)//2))), adjust=False).mean().iloc[-1])
        last_ts = idx[-1]
        next_ts = (last_ts + (pd.offsets.MonthBegin(1) if idx.freqstr == "MS" else pd.offsets.QuarterBegin(1))).normalize()
        preds[next_ts] = pred
    return pd.Series(preds, name="hwes").sort_index()

def _walk_forward_arima(job_id: str, label: str, series: pd.Series, base_percent: int, span_percent: int) -> pd.Series:
    from statsmodels.tsa.arima.model import ARIMA
    preds, idx = {}, series.index
    n = max(len(series) - 1, 1)

    def _fit_forecast(hist: pd.Series) -> float:
        try:
            for order in [(1,1,0), (1,0,0)]:
                try:
                    fit = ARIMA(hist, order=order, enforce_stationarity=False, enforce_invertibility=False)\
                          .fit(method_kwargs={"warn_convergence": False})
                    return float(fit.forecast(1)[0])
                except Exception:
                    continue
        except Exception:
            pass
        span = max(2, min(12, max(2, len(hist)//2)))
        return float(hist.ewm(span=span, adjust=False).mean().iloc[-1])

    for i in range(1, len(series)):
        hist = series.iloc[:i]
        pred = _fit_forecast(hist)
        preds[idx[i]] = pred
        pct = base_percent + int((i / n) * span_percent)
        _pulse(job_id, state="running", message=f"{label} {i}/{n}", percent=pct, done=0, total=1)

    if len(series) >= 1:
        hist = series
        pred = _fit_forecast(hist)
        last_ts = idx[-1]
        next_ts = (last_ts + (pd.offsets.MonthBegin(1) if idx.freqstr == "MS" else pd.offsets.QuarterBegin(1))).normalize()
        preds[next_ts] = pred

    return pd.Series(preds, name="arima").sort_index()

# ---------- Build output ----------
def _gen_all(job_id: str, df: pd.DataFrame) -> pd.DataFrame:
    _pulse(job_id, state="running", message="Resampling to monthly/quarterly…", percent=12, done=0, total=1)
    m, q = _monthly_quarterly(df)

    # Progress allocation across six tracks
    ses_m   = _walk_forward_ses(  job_id, "SES-M",   m, base_percent=12, span_percent=14)  # 12→26
    ses_q   = _walk_forward_ses(  job_id, "SES-Q",   q, base_percent=26, span_percent=14)  # 26→40
    hwes_m  = _walk_forward_hwes( job_id, "HWES-M",  m, base_percent=40, span_percent=14)  # 40→54
    hwes_q  = _walk_forward_hwes( job_id, "HWES-Q",  q, base_percent=54, span_percent=14)  # 54→68
    arima_m = _walk_forward_arima(job_id, "ARIMA-M", m, base_percent=68, span_percent=14)  # 68→82
    arima_q = _walk_forward_arima(job_id, "ARIMA-Q", q, base_percent=82, span_percent=15)  # 82→97

    hist_start = pd.to_datetime(df["DATE"].min()).normalize()
    hist_end   = pd.to_datetime(df["DATE"].max()).normalize()
    out_end_cap = (hist_end + pd.offsets.QuarterEnd(1)).normalize()  # upper bound; we trim later

    def fill_days(series_pred: pd.Series, freq: str, span_end: pd.Timestamp) -> pd.Series:
        idx_all = pd.date_range(hist_start, span_end, freq="D")
        out = pd.Series(index=idx_all, dtype="float64")
        for ts, val in series_pred.items():
            end = (ts + (pd.offsets.MonthEnd(0) if freq == "MS" else pd.offsets.QuarterEnd(0))).normalize()
            if end > span_end: end = span_end
            rng = pd.date_range(ts, end, freq="D")
            out.loc[rng] = float(val)
        return out

    m_fill_ses   = fill_days(ses_m,   "MS", out_end_cap)
    q_fill_ses   = fill_days(ses_q,   "QS", out_end_cap)
    m_fill_hwes  = fill_days(hwes_m,  "MS", out_end_cap)
    q_fill_hwes  = fill_days(hwes_q,  "QS", out_end_cap)
    m_fill_arima = fill_days(arima_m, "MS", out_end_cap)
    q_fill_arima = fill_days(arima_q, "QS", out_end_cap)

    last_dates = [s.last_valid_index() for s in [m_fill_ses, q_fill_ses, m_fill_hwes, q_fill_hwes, m_fill_arima, q_fill_arima] if s is not None]
    coverage_end = max([d for d in last_dates if d is not None], default=hist_end)

    all_dates = pd.date_range(hist_start, coverage_end, freq="D")
    out = pd.DataFrame({"DATE": all_dates})

    # VALUE within history; NaN beyond hist_end
    value_map = df.set_index("DATE")["VALUE"]
    out["VALUE"] = value_map.reindex(all_dates).values

    out["SES-M"]   = m_fill_ses.reindex(all_dates).values
    out["SES-Q"]   = q_fill_ses.reindex(all_dates).values
    out["HWES-M"]  = m_fill_hwes.reindex(all_dates).values
    out["HWES-Q"]  = q_fill_hwes.reindex(all_dates).values
    out["ARIMA-M"] = m_fill_arima.reindex(all_dates).values
    out["ARIMA-Q"] = q_fill_arima.reindex(all_dates).values

    _pulse(job_id, state="running", message="Composing output CSV…", percent=98, done=0, total=1)
    return out

# ---------- Runner ----------
def _spawn_runner(job_id: str, params: dict):
    def _runner():
        try:
            _pulse(job_id, state="running", message="Loading data (Neon)…", percent=8, done=0, total=1)
            df = _load_series_from_neon(
                params["db"], params["target_value"],
                params.get("state"), params.get("county_name"),
                params.get("city_name"), params.get("cbsa_name"),
                params.get("agg","mean"), params.get("ftype","F")
            )
            out = _gen_all(job_id, df)
            inst_name = _compose_instance_name(
                params["target_value"], params.get("state"), params.get("county_name"),
                params.get("city_name"), params.get("cbsa_name"), params.get("ftype","F")
            )
            out_path = os.path.join(_OUTPUT_DIR, f"{inst_name}.csv")
            out.to_csv(out_path, index=False)
            _pulse(job_id, state="ready", message="Completed", percent=100, done=1, total=1, output_file=out_path)
        except Exception as e:
            _pulse(job_id, state="error", message=str(e), percent=0, done=0, total=1)
    threading.Thread(target=_runner, daemon=True).start()

# ---------- API endpoints ----------
@router.get("/classical/probe")
def classical_probe(
    db: str,
    target_value: str,
    state: Optional[str] = Query(default=""),
    state_name: Optional[str] = Query(default=""),
    county_name: Optional[str] = Query(default=""),
    city_name: Optional[str] = Query(default=""),
    cbsa_name: Optional[str] = Query(default=""),
    agg: str = "mean",
    ftype: str = "F",
):
    _state = (state or state_name or "").strip()
    df = _load_series_from_neon(db, target_value, _state, county_name, city_name, cbsa_name, agg, ftype)
    return {
        "rows": int(len(df)),
        "start_date": df["DATE"].min().strftime("%Y-%m-%d"),
        "end_date": df["DATE"].max().strftime("%Y-%m-%d"),
        "source": "neon",
        "table": DEFAULT_TABLE,
    }

@router.post("/classical/start")
async def classical_start(
    request: Request,
    db: Optional[str] = None,
    target_value: Optional[str] = None,
    state: Optional[str] = None,
    state_name: Optional[str] = None,
    county_name: Optional[str] = None,
    city_name: Optional[str] = None,
    cbsa_name: Optional[str] = None,
    agg: Optional[str] = "mean",
    ftype: Optional[str] = "F",
):
    body = {}
    try:
        if request.headers.get("content-type","").lower().startswith("application/json"):
            body = await request.json()
    except Exception:
        body = {}

    def pick(key: str, *aliases, default: Optional[str] = ""):
        for k in (key, *aliases):
            if k in body and body[k] is not None and str(body[k]).strip() != "":
                return str(body[k]).strip()
        return (locals().get(key) or default)

    _db     = pick("db", default="demo")              if db is None           else db
    _target = pick("target_value")                    if target_value is None else target_value
    _state  = (pick("state") or pick("state_name"))   if (state is None and state_name is None) else (state or state_name or "")
    _county = pick("county_name")                     if county_name is None  else county_name
    _city   = pick("city_name")                       if city_name is None    else city_name
    _cbsa   = pick("cbsa_name")                       if cbsa_name is None    else cbsa_name
    _agg    = pick("agg", default="mean")             if agg is None          else agg
    _ftype  = pick("ftype", default="F")              if ftype is None        else ftype

    if not _target: raise HTTPException(status_code=422, detail="target_value is required")

    job_id = str(uuid.uuid4())
    _job_write(job_id, state="queued", message="Queued", percent=0, done=0, total=1,
               params={"db":_db,"target_value":_target,"state":_state,"county_name":_county,"city_name":_city,
                       "cbsa_name":_cbsa,"agg":_agg,"ftype":_ftype})
    _spawn_runner(job_id, _job_read(job_id)["params"])
    return {"job_id": job_id}

@router.get("/classical/status")
def classical_status(job_id: str = Query(...)):
    job = _job_read(job_id)
    if not job:
        return {"state":"missing","message":"Job not found","done":0,"total":1,"percent":0}
    if _is_stale(job):
        _pulse(job_id, state="paused", message="Worker paused (stale heartbeat)", percent=job.get("percent",0), done=0, total=1)
        job = _job_read(job_id)
    return {"job_id": job_id, **job}

@router.post("/classical/resume")
def classical_resume(job_id: str = Query(...)):
    job = _job_read(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("state") == "ready":
        return {"job_id": job_id, **job}
    _pulse(job_id, state="queued", message="Resuming…", percent=job.get("percent",0), done=0, total=1)
    params = (job.get("params") or {})
    threading.Thread(target=lambda: _spawn_runner(job_id, params), daemon=True).start()
    return {"job_id": job_id, **_job_read(job_id)}

@router.get("/classical/download")
def classical_download(job_id: str = Query(...)):
    job = _job_read(job_id)
    if not job or job.get("state") != "ready":
        raise HTTPException(status_code=409, detail="Job not ready")
    path = job.get("output_file")
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Output not found")
    return FileResponse(path, filename=os.path.basename(path), media_type="text/csv")
