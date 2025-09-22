# ==============================================================================
# forms_engine_kickoff.py  (Fixed)
# Date: 2025-09-21
# Change: Expose actual DB errors instead of swallowing them, so kickoff failures
#         are visible and you can confirm forecast_id is passed correctly.
# ==============================================================================

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.encoders import jsonable_encoder
from jinja2 import Environment, FileSystemLoader, select_autoescape
import os, psycopg2

router = APIRouter()

# Template loader
templates = Environment(
    loader=FileSystemLoader("backend/templates/forms"),
    autoescape=select_autoescape(["html", "xml"])
)

def _connect():
    dsn = os.environ.get("ENGINE_DATABASE_URL_DIRECT") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("No ENGINE_DATABASE_URL_DIRECT or DATABASE_URL set")
    return psycopg2.connect(dsn)

@router.get("/forms/engine-kickoff", response_class=HTMLResponse, tags=["forms"])
def engine_kickoff_form(request: Request):
    """Render kickoff form with forecast list."""
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT forecast_id::text, forecast_name
                FROM engine.forecast_registry
                ORDER BY forecast_name
            """)
            options = cur.fetchall()
    except Exception as e:
        return HTMLResponse(f"<pre>Error loading forecasts: {e}</pre>", status_code=500)

    template = templates.get_template("engine_kickoff.html")
    return template.render(options=options)

@router.post("/forms/engine-kickoff/start", tags=["forms"])
def engine_kickoff_start(forecast_id: str = Form(...)):
    """Kick off a run for the given forecast_id."""
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT engine.manual_kickoff_by_id(%s::uuid)", (forecast_id,))
            run_id = cur.fetchone()[0]
            conn.commit()
        return JSONResponse(content=jsonable_encoder({"ok": True, "run_id": run_id, "forecast_id": forecast_id}))
    except Exception as e:
        # FIX: return the actual DB error to frontend
        return JSONResponse(content=jsonable_encoder({"ok": False, "error": str(e)}), status_code=500)
