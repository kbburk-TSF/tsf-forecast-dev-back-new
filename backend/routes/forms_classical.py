# ==============================================================================
# backend/routes/forms_classical.py  (Backend page + CSV generator)
# Provides:
#   - GET  /forms/classical        -> renders classical.html page
#   - POST /forms/classical/run    -> streams CSV
# Uses AIR_QUALITY_DEMO direct DSN only.
# ==============================================================================
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
import os, io, csv, psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter()

def _dsn() -> str:
    dsn = os.getenv("AIR_QUALITY_DEMO")
    if not dsn:
        raise RuntimeError("AIR_QUALITY_DEMO is not set")
    return dsn

def _connect():
    return psycopg2.connect(_dsn(), connect_timeout=10, sslmode="require")

@router.get("/forms/classical", response_class=HTMLResponse, tags=["forms"])
def classical_page(request: Request):
    html = open("backend/templates/forms/classical.html", "r", encoding="utf-8").read()
    return HTMLResponse(html)

@router.post("/forms/classical/run", tags=["forms"])
def classical_run(parameter: str = Form(default=""), state: str = Form(default="")):
    # Export structure expected by your frontend CSV: forecast_id, forecast_name, date, value
    # This pulls from engine.forecast_registry; extend the SQL when measurement table is ready.
    sql = '''
    SELECT
      forecast_id::text AS forecast_id,
      forecast_name,
      COALESCE(updated_at, created_at) AS date,
      NULL::numeric AS value
    FROM engine.forecast_registry
    ORDER BY forecast_name
    '''
    try:
        with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["forecast_id", "forecast_name", "date", "value"])
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    data = buf.getvalue().encode("utf-8")

    return StreamingResponse(
        io.BytesIO(data),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=classical_export.csv"}
    )
