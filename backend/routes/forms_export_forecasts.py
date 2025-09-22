# ==============================================================================
# backend/routes/forms_export_forecasts.py  (AIR_QUALITY_DEMO DSN verbatim)
# ==============================================================================
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
import os, io, csv, psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter()

def _demo_dsn() -> str:
    dsn = os.getenv("AIR_QUALITY_DEMO")
    if not dsn:
        raise RuntimeError("AIR_QUALITY_DEMO is not set")
    return dsn

def _connect_demo():
    return psycopg2.connect(_demo_dsn(), connect_timeout=10, sslmode="require")

@router.get("/forms/export-forecasts", response_class=HTMLResponse, tags=["forms"])
def export_form(request: Request):
    html = open("backend/templates/forms/export_forecasts.html", "r", encoding="utf-8").read()
    return HTMLResponse(html)

@router.get("/forms/export-forecasts/ping", response_class=JSONResponse, tags=["forms"])
def export_ping():
    try:
        with _connect_demo() as conn, conn.cursor() as cur:
            cur.execute("select current_database(), current_user")
            db, usr = cur.fetchone()
        return JSONResponse({"ok": True, "db": db, "user": usr})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@router.post("/forms/export-forecasts/run", tags=["forms"])
def export_run():
    sql = '''
    SELECT
      forecast_id::text AS forecast_id,
      forecast_name,
      COALESCE(updated_at, created_at) AS updated_at
    FROM engine.forecast_registry
    ORDER BY forecast_name
    '''
    try:
        with _connect_demo() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["forecast_id", "forecast_name", "updated_at"])
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    data = buf.getvalue().encode("utf-8")

    return StreamingResponse(
        io.BytesIO(data),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=forecasts.csv"}
    )
