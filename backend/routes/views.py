
# backend/routes/views.py
# Read-only Views API for the front-end "Views" tab.
# IMPORTANT: Uses ONLY the TSF_ENGINE_APP environment variable for DB access.
# Does NOT touch or import your global database engine.

from typing import Optional, Dict, List
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import os

router = APIRouter(prefix="/views", tags=["views"])

def _build_engine_from_env() -> Engine:
    url = os.getenv("TSF_ENGINE_APP")
    if not url:
        raise RuntimeError("TSF_ENGINE_APP not set")
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if url.startswith("postgresql://") and "+psycopg" not in url and "+psycopg2" not in url:
        try:
            import psycopg  # noqa: F401
            url = url.replace("postgresql://", "postgresql+psycopg://", 1)
        except Exception:
            url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return create_engine(url, pool_pre_ping=True)

_engine: Engine = None

def _conn():
    global _engine
    if _engine is None:
        _engine = _build_engine_from_env()
    return _engine.begin()

def _discover_views() -> List[Dict[str,str]]:
    sql = """
    SELECT schemaname, viewname
    FROM pg_catalog.pg_views
    WHERE schemaname='engine'
      AND (viewname = 'tsf_vw_daily_best' OR viewname LIKE '%_vw_daily_best')
    """
    with _conn() as conn:
        rows = conn.execute(text(sql)).mappings().all()
    return [dict(r) for r in rows]

def _exists(views, name: str) -> bool:
    return any(v["schemaname"] == "engine" and v["viewname"] == name for v in views)

def _extract_models(views) -> List[str]:
    models = set()
    for v in views:
        name = v["viewname"]
        if name == "tsf_vw_daily_best":
            continue
        if name.endswith("_vw_daily_best"):
            base = name[: -len("_vw_daily_best")]
            for sfx in ("_instance_forecast_s", "_instance_forecast_ms", "_instance_forecast_msq", "_instance_forecast_msqm"):
                if base.endswith(sfx):
                    base = base[: -len(sfx)]
                    break
            models.add(base)
    return sorted(models)

def _resolve_view(scope: str, model: Optional[str], series: Optional[str], views) -> str:
    s = (scope or "").lower()
    if s == "global":
        if not _exists(views, "tsf_vw_daily_best"):
            raise HTTPException(404, "Global view not found")
        return "engine.tsf_vw_daily_best"
    if s == "per_model":
        if not model:
            raise HTTPException(400, "Model required for per_model")
        name = f"{model}_vw_daily_best"
        if not _exists(views, name):
            raise HTTPException(404, f"Per-model view not found: {name}")
        return f"engine.{name}"
    if s == "per_table":
        if not (model and series):
            raise HTTPException(400, "Model and series required for per_table")
        ser = series.lower()
        if ser not in ("s","ms","sq","sqm"):
            raise HTTPException(400, "Series must be S/MS/SQ/SQM")
        name = f"{model}_instance_forecast_{ser}_vw_daily_best"
        if not _exists(views, name):
            raise HTTPException(404, f"Per-table view not found: {name}")
        return f"engine.{name}"
    raise HTTPException(400, "Invalid scope")

@router.get("/meta")
def meta():
    views = _discover_views()
    models = _extract_models(views)
    most_recent = {}

    def fetch_recent(vname: str):
        sql = f"SELECT forecast_id FROM {vname} ORDER BY created_at DESC NULLS LAST LIMIT 1"
        with _conn() as conn:
            row = conn.execute(text(sql)).mappings().first()
            return str(row["forecast_id"]) if row and row.get("forecast_id") else None

    if _exists(views, "tsf_vw_daily_best"):
        rid = fetch_recent("engine.tsf_vw_daily_best")
        if rid:
            most_recent["global||"] = rid

    for m in models:
        v = f"engine.{m}_vw_daily_best"
        if _exists(views, f"{m}_vw_daily_best"):
            rid = fetch_recent(v)
            if rid:
                most_recent[f"per_model|{m}|"] = rid

    for m in models:
        for ser in ("s","ms","sq","sqm"):
            vname = f"{m}_instance_forecast_{ser}_vw_daily_best"
            if _exists(views, vname):
                rid = fetch_recent(f"engine.{vname}")
                if rid:
                    most_recent[f"per_table|{m}|{ser.upper()}"] = rid

    return {"scopes":["per_table","per_model","global"], "models":models, "series":["S","MS","SQ","SQM"], "most_recent": most_recent}

@router.get("/ids")
def ids(scope: str = Query(...), model: Optional[str] = None, series: Optional[str] = None, limit: int = 100):
    views = _discover_views()
    vname = _resolve_view(scope, model, series, views)
    sql = f"""
    SELECT forecast_id
    FROM (
      SELECT forecast_id, MAX(created_at) AS mc
      FROM {vname}
      GROUP BY forecast_id
    ) x
    ORDER BY mc DESC NULLS LAST
    LIMIT :lim
    """
    with _conn() as conn:
        vals = conn.execute(text(sql), {"lim": limit}).scalars().all()
    return [str(v) for v in vals if v]

class ViewsQueryBody(BaseModel):
    scope: str
    model: Optional[str] = None
    series: Optional[str] = None
    forecast_id: str
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    page: int = 1
    page_size: int = 2000

@router.post("/query")
def query(body: ViewsQueryBody):
    if not body.forecast_id:
        raise HTTPException(400, "forecast_id required")
    views = _discover_views()
    vname = _resolve_view(body.scope, body.model, body.series, views)
    where = ["forecast_id = :id"]
    params = {"id": body.forecast_id}
    if body.date_from:
        where.append("date >= :d1")
        params["d1"] = body.date_from
    if body.date_to:
        where.append("date <= :d2")
        params["d2"] = body.date_to
    where_sql = " AND ".join(where)
    limit = max(1, min(10000, int(body.page_size or 2000)))
    offset = max(0, (max(1, int(body.page or 1))-1) * limit)

    cols = "date, value, fv_l, fv, fv_u, fv_mean_mae, fv_interval_odds, fv_interval_sig, fv_variance_mean, fv_mean_mae_c, model_name, series, season, fmsr_series, created_at"
    sql = f"SELECT {cols} FROM {vname} WHERE {where_sql} ORDER BY date ASC LIMIT :lim OFFSET :off"
    cnt = f"SELECT COUNT(*) AS n FROM {vname} WHERE {where_sql}"
    with _conn() as conn:
        total = int(conn.execute(text(cnt), params).mappings().first()["n"])
        rows = conn.execute(text(sql), {**params, "lim": limit, "off": offset}).mappings().all()
    return {"rows": rows, "total": total}
