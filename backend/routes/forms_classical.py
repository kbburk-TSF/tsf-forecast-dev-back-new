# ==============================================================================
# backend/routes/forms_classical.py  (RESTORE legacy endpoint)
# Provides POST /forms/classical/run to stream a CSV.
# Uses AIR_QUALITY_DEMO DSN only (direct connection), no pooling.
# ==============================================================================
from fastapi import APIRouter, Form
from fastapi.responses import StreamingResponse, JSONResponse
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

@router.post("/forms/classical/run", tags=["forms"])
def classical_run(parameter: str = Form(default=""), state: str = Form(default="")):
    # Minimal, safe export that unblocks the frontend.
    # Returns forecast_id, forecast_name, date (updated_at), value (NULL for now).
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
