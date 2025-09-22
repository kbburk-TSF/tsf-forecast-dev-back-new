# ==============================================================================
# backend/routes/forms_engine_kickoff.py  (TSF_ENGINE_APP DSN verbatim)
# ==============================================================================
import os, select, json, time
from fastapi import APIRouter, Query, Form
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.encoders import jsonable_encoder
import psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter()

def _dsn() -> str:
    dsn = os.getenv("TSF_ENGINE_APP")
    if not dsn:
        raise RuntimeError("TSF_ENGINE_APP is not set")
    return dsn

def _connect():
    # Use the DSN exactly as provided; enforce TLS and a short timeout.
    return psycopg2.connect(_dsn(), connect_timeout=10, sslmode="require")

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
            """ )
            for r in cur.fetchall():
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
        return JSONResponse(content=jsonable_encoder({"ok": True, "run_id": run_id, "forecast_id": forecast_id}))
    except Exception as e:
        return JSONResponse(content=jsonable_encoder({"ok": False, "error": str(e)}), status_code=500)

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

        return JSONResponse(content=jsonable_encoder({
            "ok": True,
            "run": run_header,
            "phases": phases,
            "progress_percent": progress
        }))
    except Exception as e:
        return JSONResponse(content=jsonable_encoder({"ok": False, "error": str(e)}), status_code=500)

@router.get("/forms/engine-kickoff/stream", tags=["forms"])
def engine_kickoff_stream(run_id: str):
    """SSE stream of NOTIFY payloads for this run_id."""
    def event_gen():
        try:
            conn = _connect()
            conn.set_session(autocommit=True)
            cur = conn.cursor()
            cur.execute("LISTEN engine_status;")
            yield "data: {\"ok\": true, \"event\": \"connected\"}\n\n"
            while True:
                if select.select([conn], [], [], 25) == ([], [], []):
                    yield "data: {\"ok\": true, \"event\": \"heartbeat\"}\n\n"
                    continue
                conn.poll()
                while conn.notifies:
                    n = conn.notifies.pop(0)
                    try:
                        payload = json.loads(n.payload)
                        if str(payload.get("run_id")) == str(run_id):
                            yield "data: " + json.dumps(payload) + "\n\n"
                    except Exception:
                        pass
        except Exception as e:
            yield "data: {\"ok\": false, \"error\": \"%s\"}\n\n" % str(e).replace('"','\\\"')
            time.sleep(0.5)
    return StreamingResponse(event_gen(), media_type="text/event-stream")
