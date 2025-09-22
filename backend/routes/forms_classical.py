# ==============================================================================
# backend/routes/forms_classical.py
# Populates <select> boxes from the database (server-side) and streams CSV.
# Uses AIR_QUALITY_DEMO DSN only.
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

def _query_listboxes(cur):
    # 1) Try JSON parameters in forecast_registry (preferred)
    cur.execute("""
        SELECT DISTINCT
               NULLIF(TRIM(parameters->>'parameter'), '') AS param,
               NULLIF(TRIM(parameters->>'state'), '')     AS state
        FROM engine.forecast_registry
    """)
    rows = cur.fetchall()
    params = sorted({r["param"] for r in rows if r and r.get("param")})
    states = sorted({r["state"] for r in rows if r and r.get("state")})

    # 2) Fallback: try explicit tables if they exist (states, parameters)
    if not params:
        try:
            cur.execute("SELECT DISTINCT name AS v FROM engine.parameters ORDER BY 1")
            params = [r["v"] for r in cur.fetchall()]
        except Exception:
            pass
    if not states:
        try:
            cur.execute("SELECT DISTINCT state_name AS v FROM engine.states ORDER BY 1")
            states = [r["v"] for r in cur.fetchall()]
        except Exception:
            pass

    # 3) Last resort (still from DB): use forecast_name values to populate both
    if not params or not states:
        cur.execute("""
            SELECT DISTINCT forecast_name AS v
            FROM engine.forecast_registry
            WHERE forecast_name IS NOT NULL AND forecast_name <> ''
            ORDER BY 1
        """)
        vals = [r["v"] for r in cur.fetchall()]
        if not params: params = vals[:]
        if not states: states = vals[:]

    return params, states

@router.get("/forms/classical", response_class=HTMLResponse, tags=["forms"])
def classical_page(request: Request):
    try:
        with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            params, states = _query_listboxes(cur)
    except Exception as e:
        return HTMLResponse(f"<pre>Failed to load options from DB:\n{e}</pre>", status_code=500)

    def _opts(values):
        return "".join(f'<option value="{v}">{v}</option>' for v in values) or '<option value="">(none)</option>'

    html = open("backend/templates/forms/classical.html", "r", encoding="utf-8").read()
    html = html.replace("<!--__PARAM_OPTIONS__-->", _opts(params))
    html = html.replace("<!--__STATE_OPTIONS__-->", _opts(states))
    return HTMLResponse(html)

@router.post("/forms/classical/run", tags=["forms"])
def classical_run(parameter: str = Form(default=""), state: str = Form(default="")):
    # Stream CSV — keep schema aligned with frontend expectation for now
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
