# backend/routes/forms_engine_kickoff.py
import os
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Query, Form
from fastapi.responses import HTMLResponse, JSONResponse
import psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter()

def _db_url() -> str:
    url = os.getenv("ENGINE_DATABASE_URL_DIRECT") or os.getenv("ENGINE_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("ENGINE_DATABASE_URL_DIRECT is not set")
    return url

def _connect():
    return psycopg2.connect(_db_url())

def _html(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

@router.get("/forms/engine-kickoff", response_class=HTMLResponse, tags=["forms"])
def engine_kickoff_form() -> str:
    options_html = ""
    try:
        with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT forecast_id, forecast_name
                FROM engine.forecast_registry
                ORDER BY COALESCE(updated_at, created_at) ASC NULLS FIRST, forecast_name
            """)
            rows = cur.fetchall()
            for r in rows:
                options_html += f'<option value="{r["forecast_id"]}">{r["forecast_name"]}</option>'
    except Exception as e:
        return HTMLResponse(f"<pre>Failed to load forecasts:\n{e}</pre>", status_code=500)

    html = _html("backend/templates/forms/engine_kickoff.html")
    html = html.replace("<!--__OPTIONS__-->", options_html)
    return html

@router.post("/forms/engine-kickoff/start", response_class=JSONResponse, tags=["forms"])
def engine_kickoff_start(forecast_id: str = Form(...)) -> JSONResponse:
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT engine.manual_kickoff_by_id(%s::uuid)", (forecast_id,))
            run_id = cur.fetchone()[0]
        return JSONResponse({"ok": True, "run_id": run_id, "forecast_id": forecast_id})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@router.get("/forms/engine-kickoff/status", response_class=JSONResponse, tags=["forms"])
def engine_kickoff_status(run_id: str = Query(...)) -> JSONResponse:
    try:
        with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT phase, status, started_at, finished_at, rows_written, message
                FROM engine.instance_run_phases
                WHERE run_id = %s::uuid
                ORDER BY CASE phase
                    WHEN 'sr_s' THEN 1 WHEN 'sr_sq' THEN 2 WHEN 'sr_sqm' THEN 3
                    WHEN 'fc_ms' THEN 4 WHEN 'fc_msq' THEN 5 WHEN 'fc_msqm' THEN 6
                    ELSE 999 END
            """, (run_id,))
            phases = cur.fetchall()

            cur.execute("""
                SELECT status, created_at, started_at, finished_at, overall_error
                FROM engine.instance_runs
                WHERE run_id = %s::uuid
            """, (run_id,))
            run_header = cur.fetchone()

        total = 6
        done = sum(1 for p in phases if p["status"] == "done")
        progress = int((done / total) * 100)

        return JSONResponse({
            "ok": True,
            "run": run_header,
            "phases": phases,
            "progress_percent": progress
        })
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
