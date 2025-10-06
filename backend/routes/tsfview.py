
# backend/routes/tsfview.py
# Version: 2025-10-05 v2.1
# Adds GET "/" so /tsfview returns rows (not 404). Still provides /columns, /query, /export.

from typing import Optional, List
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
import os, datetime as dt
import psycopg
from psycopg.rows import dict_row

router = APIRouter(prefix="/tsfview", tags=["tsfview"])

def _db_url() -> str:
    return (
        os.getenv("ENGINE_DATABASE_URL_DIRECT")
        or os.getenv("ENGINE_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or ""
    )

def _connect():
    dsn = _db_url()
    if not dsn:
        raise RuntimeError("Database URL not configured")
    return psycopg.connect(dsn, autocommit=True)

def _parse_date(s: Optional[str]) -> Optional[dt.date]:
    return dt.date.fromisoformat(s) if s else None

@router.get("/")
def root(page_size: int = 100):
    """Quick browse: first N rows of engine.tsf_vw_full (all columns)."""
    limit = max(1, min(1000, int(page_size or 100)))
    sql = "SELECT v.* FROM engine.tsf_vw_full v ORDER BY v.date ASC LIMIT %s"
    with _connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, [limit])
        rows = [dict(r) for r in cur.fetchall()]
    return {"rows": rows, "limit": limit}

@router.get("/columns")
def columns():
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM engine.tsf_vw_full LIMIT 0")
        return {"columns": [d.name for d in cur.description]}

@router.post("/query")
def query_all(
    forecast_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    page_size: int = 5000,
):
    limit = max(1, min(20000, int(page_size or 5000)))
    offset = max(0, (max(1, int(page or 1)) - 1) * limit)

    conds = ["TRUE"]
    params: List[object] = []
    join = ""

    if forecast_id:
        join = "JOIN engine.forecast_registry fr ON fr.forecast_name = v.forecast_name"
        conds.append("fr.forecast_id = %s")
        params.append(forecast_id)

    if date_from:
        conds.append("v.date >= %s")
        params.append(_parse_date(date_from))
    if date_to:
        conds.append("v.date <= %s")
        params.append(_parse_date(date_to))

    where_clause = " AND ".join(conds)

    sql_count = f"SELECT COUNT(*) FROM engine.tsf_vw_full v {join} WHERE {where_clause}"
    sql = f"""
        SELECT v.*
        FROM engine.tsf_vw_full v
        {join}
        WHERE {where_clause}
        ORDER BY v.date ASC
        LIMIT %s OFFSET %s
    """

    with _connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_count, params)
        total = int(cur.fetchone()["count"])
        cur.execute(sql, params + [limit, offset])
        rows = [dict(r) for r in cur.fetchall()]

    return {"total": total, "rows": rows, "page": page, "page_size": limit}

@router.get("/export")
def export_csv(
    forecast_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    conds = ["TRUE"]
    params: List[object] = []
    join = ""

    if forecast_id:
        join = "JOIN engine.forecast_registry fr ON fr.forecast_name = v.forecast_name"
        conds.append("fr.forecast_id = %s")
        params.append(forecast_id)

    if date_from:
        conds.append("v.date >= %s")
        params.append(_parse_date(date_from))
    if date_to:
        conds.append("v.date <= %s")
        params.append(_parse_date(date_to))

    where_clause = " AND ".join(conds)
    sql = f"SELECT v.* FROM engine.tsf_vw_full v {join} WHERE {where_clause} ORDER BY v.date ASC"

    def row_iter():
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            headers = [d.name for d in cur.description]
            yield (",".join(headers) + "\\n").encode("utf-8")
            for rec in cur:
                out = []
                for val in rec:
                    if val is None:
                        out.append("")
                    elif isinstance(val, dt.date):
                        out.append(val.isoformat())
                    else:
                        s = str(val)
                        if any(ch in s for ch in [",", "\\n", '"']):
                            s = '"' + s.replace('"','""') + '"'
                        out.append(s)
                yield (",".join(out) + "\\n").encode("utf-8")

    fname = "tsf_vw_full.csv" if not forecast_id else f"tsf_vw_full_{forecast_id}.csv"
    return StreamingResponse(
        row_iter(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'}
    )
