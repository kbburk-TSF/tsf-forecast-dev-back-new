
# backend/routes/views.py
# Replacement routes exposing arima_m, hwes_m, ses_m to the frontend.
# FastAPI + psycopg (requires DATABASE_URL env).

from typing import Optional, List, Dict
from fastapi import APIRouter, HTTPException, Query as FQuery
from fastapi.responses import HTMLResponse, StreamingResponse
import os, datetime as dt
import psycopg
from psycopg.rows import dict_row

router = APIRouter(prefix="/views", tags=["views"])

DSN = os.environ.get("DATABASE_URL") or os.environ.get("PG_DSN") or os.environ.get("POSTGRES_URL")
if not DSN:
    # fallback to discrete envs
    host = os.environ.get("PGHOST","localhost")
    user = os.environ.get("PGUSER","postgres")
    pwd  = os.environ.get("PGPASSWORD","")
    db   = os.environ.get("PGDATABASE","postgres")
    port = os.environ.get("PGPORT","5432")
    DSN = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"

def _connect():
    return psycopg.connect(DSN, autocommit=True, row_factory=dict_row)

BASE_VIEW = "engine.tsf_vw_full"

# Columns now include explicit model series
COLS = (
    "date, value, arima_m, hwes_m, ses_m, "
    "model_name, fv_l, fv, fv_u, fv_mean_mape, fv_interval_odds, fv_interval_sig, "
    "fv_variance, fv_variance_mean, fv_mean_mape_c, low, high"
)

def _date(s: Optional[str]) -> Optional[dt.date]:
    if not s: return None
    return dt.date.fromisoformat(s)

@router.get("/ids")
def list_ids(
    scope: Optional[str] = None,
    model: Optional[str] = None,
    series: Optional[str] = None,
) -> List[Dict]:
    sql = f"select distinct forecast_name as id, forecast_name as name from {BASE_VIEW}"
    where = []
    args: List = []
    if scope and scope != "global":
        where.append("scope = %s"); args.append(scope)
    if model:
        where.append("model_name = %s"); args.append(model)
    if series:
        where.append("upper(series) = upper(%s)"); args.append(series)
    if where:
        sql += " where " + " and ".join(where)
    sql += " order by 1"
    with _connect() as cx, cx.cursor() as cur:
        cur.execute(sql, args)
        return list(cur.fetchall())

@router.get("/query")
def query_view(
    scope: Optional[str] = None,
    model: Optional[str] = None,
    series: Optional[str] = None,
    forecast_id: Optional[str] = FQuery(None, alias="forecast_id"),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    page_size: int = 2000,
):
    df = _date(date_from)
    dt_to = _date(date_to)
    where = []
    args: List = []
    if scope and scope != "global":
        where.append("scope = %s"); args.append(scope)
    if model:
        where.append("model_name = %s"); args.append(model)
    if series:
        where.append("upper(series) = upper(%s)"); args.append(series)
    if forecast_id:
        where.append("forecast_name = %s"); args.append(forecast_id)
    if df:
        where.append("date >= %s"); args.append(df)
    if dt_to:
        where.append("date <= %s"); args.append(dt_to)

    sql = f"select {COLS} from {BASE_VIEW}"
    if where:
        sql += " where " + " and ".join(where)
    sql += " order by date asc"
    offset = max(0, (page-1) * page_size)
    sql += f" limit {int(page_size)} offset {int(offset)}"

    with _connect() as cx, cx.cursor() as cur:
        cur.execute(sql, args)
        rows = list(cur.fetchall())
        return {"rows": rows, "page": page, "page_size": page_size}

@router.get("/export")
def export_csv(
    scope: Optional[str] = None,
    model: Optional[str] = None,
    series: Optional[str] = None,
    forecast_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    cols = ["date","value","arima_m","hwes_m","ses_m","model_name","fv_l","fv","fv_u",
            "fv_mean_mape","fv_interval_odds","fv_interval_sig",
            "fv_variance","fv_variance_mean","fv_mean_mape_c","low","high"]

    where = []
    args: List = []
    if scope and scope != "global":
        where.append("scope = %s"); args.append(scope)
    if model:
        where.append("model_name = %s"); args.append(model)
    if series:
        where.append("upper(series) = upper(%s)"); args.append(series)
    if forecast_id:
        where.append("forecast_name = %s"); args.append(forecast_id)
    if date_from:
        where.append("date >= %s"); args.append(_date(date_from))
    if date_to:
        where.append("date <= %s"); args.append(_date(date_to))

    sql = f"select {', '.join(cols)} from {BASE_VIEW}"
    if where:
        sql += " where " + " and ".join(where)
    sql += " order by date asc"

    def row_iter():
        yield (",".join(cols) + "\n").encode("utf-8")
        with _connect() as cx, cx.cursor() as cur:
            cur.execute(sql, args)
            for r in cur:
                out = []
                for c in cols:
                    v = r[c]
                    if v is None:
                        out.append("")
                    else:
                        s = str(v)
                        if any(ch in s for ch in [',','\n','"']):
                            s = '"' + s.replace('"','""') + '"'
                        out.append(s)
                yield (",".join(out) + "\n").encode("utf-8")

    bits = [scope or "view"]
    if model: bits.append(model)
    if series: bits.append(series.upper())
    if forecast_id: bits.append(str(forecast_id))
    fname = "tsf_export_" + "_".join(bits) + ".csv"
    return StreamingResponse(row_iter(), media_type="text/csv",
                             headers={"Content-Disposition": f"attachment; filename={fname}"})
