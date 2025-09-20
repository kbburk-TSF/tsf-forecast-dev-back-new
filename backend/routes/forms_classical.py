# FORMS CLASSICAL (INLINE_HTML_v2)
from typing import List, Optional
import os, shutil, glob
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
import psycopg
from psycopg.rows import tuple_row
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

router = APIRouter(prefix="/forms", tags=["forms"])

DATABASE_URL = os.getenv("DATABASE_URL")
SCHEMA = "air_quality_demo_data"
TABLE_FQ = f'{SCHEMA}."air_quality_raw"'
COL_PARAM = '"Parameter Name"'
COL_STATE = '"State Name"'
COL_DATE  = '"Date Local"'
COL_VALUE = '"Arithmetic Mean"'

OUTPUT_DIR = os.path.join(os.getcwd(), "backend", "data", "output")
STAGING_HISTORICAL_DIR = os.getenv("STAGING_HISTORICAL_DIR", os.path.join(os.getcwd(), "staging_historical"))
os.makedirs(STAGING_HISTORICAL_DIR, exist_ok=True)

def _clean_dsn(dsn: str) -> str:
    if not dsn:
        return dsn
    if "channel_binding=" in dsn and "://" not in dsn:
        parts = [tok for tok in dsn.split() if not tok.lower().startswith("channel_binding=")]
        return " ".join(parts)
    if "://" in dsn:
        p = urlparse(dsn)
        q = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True) if k.lower() != "channel_binding"]
        dsn = urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(q), p.fragment))
    return dsn

def _conn():
    dsn = _clean_dsn(DATABASE_URL or "")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")
    conn = psycopg.connect(dsn, row_factory=tuple_row)
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {SCHEMA}, public")
    return conn

def _esc(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"',"&quot;").replace("'","&#39;")

def _options(items: List[str], selected: Optional[str] = None) -> str:
    out = []
    for it in items:
        sel = " selected" if selected is not None and it == selected else ""
        out.append(f'<option value="{_esc(it)}"{sel}>{_esc(it)}</option>')
    return "\n".join(out)

def list_parameters() -> List[str]:
    sql = f'''
        SELECT DISTINCT {COL_PARAM}
        FROM {TABLE_FQ}
        WHERE {COL_PARAM} IS NOT NULL
        ORDER BY {COL_PARAM}
    '''
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return [r[0] for r in cur.fetchall()]

def list_states_for_param(param: Optional[str]) -> List[str]:
    if param:
        sql = f'''
            SELECT DISTINCT {COL_STATE}
            FROM {TABLE_FQ}
            WHERE {COL_PARAM} = %s AND {COL_STATE} IS NOT NULL
            ORDER BY {COL_STATE}
        '''
        args = (param,)
    else:
        sql = f'''
            SELECT DISTINCT {COL_STATE}
            FROM {TABLE_FQ}
            WHERE {COL_STATE} IS NOT NULL
            ORDER BY {COL_STATE}
        '''
        args = ()
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        return [r[0] for r in cur.fetchall()]

HTML = """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Classical Forecast - Backend Form</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; }
    .wrap { max-width: 760px; margin: 0 auto; }
    form { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    label { display: block; font-weight: 600; margin-bottom: 6px; }
    select { width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 6px; }
    .actions { grid-column: 1 / -1; display: flex; gap: 12px; }
    button { padding: 10px 16px; border: 0; border-radius: 6px; cursor: pointer; background: #1f6feb; color: #fff; }
    a.button { text-decoration: none; background: #eee; color: #111; padding: 10px 16px; border-radius: 6px; display: inline-block; }
    .err { background:#fee; border:1px solid #f88; padding:10px; border-radius:6px; margin-bottom:16px; }
  </style>
</head>
<body>
<div class="wrap">
  <h1>Classical Forecast - Backend Form</h1>
  {ERROR}
  <form method="post" action="/forms/classical/start">
    <div>
      <label for="parameter">Target (Parameter Name)</label>
      <select id="parameter" name="parameter" required>
        {PARAM_OPTIONS}
      </select>
    </div>
    <div>
      <label for="state">State Name</label>
      <select id="state" name="state" required>
        {STATE_OPTIONS}
      </select>
    </div>
    <div class="actions">
      <button type="submit">Run Classical</button>
      <a class="button" href="/forms/classical">Reset</a>
    </div>
  </form>
</div>
</body>
</html>"""

@router.get("/classical", response_class=HTMLResponse)
def get_form(request: Request, parameter: Optional[str] = None):
    try:
        params = list_parameters()
        states = list_states_for_param(parameter if parameter else None)
        html = HTML.replace("{PARAM_OPTIONS}", _options(params, selected=parameter)).replace("{STATE_OPTIONS}", _options(states)).replace("{ERROR}","")
        return HTMLResponse(html, status_code=200)
    except Exception as e:
        err = f'<div class="err"><strong>DB error:</strong> { _esc(str(e)) }</div>'
        html = HTML.replace("{PARAM_OPTIONS}","").replace("{STATE_OPTIONS}","").replace("{ERROR}", err)
        return HTMLResponse(html, status_code=200)

@router.post("/classical/start")
def start_from_form(parameter: str = Form(...), state: str = Form(...)):
    url = f"/forms/classical/next?parameter={parameter}&state={state}"
    return RedirectResponse(url, status_code=303)

@router.get("/classical/next", response_class=HTMLResponse)
def after_start(request: Request, parameter: str, state: str):
    body = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Classical - Run</title></head><body>
<h2>Run Classical</h2>
<form method="post" action="/classical/start">
  <input type="hidden" name="parameter" value="{_esc(parameter)}" />
  <input type="hidden" name="state" value="{_esc(state)}" />
  <button type="submit">Start Job</button>
</form>
<p>After you click "Start Job", you will receive JSON with <code>job_id</code>.</p>
<p>Then use: <code>/classical/status?job_id=...</code> and <code>/classical/download?job_id=...</code>.</p>
<p>When the CSV exists in <code>{_esc(OUTPUT_DIR)}</code>, copy it to staging via:<br/>
<code>/forms/classical/copy?parameter={_esc(parameter)}&state={_esc(state)}</code></p>
</body></html>"""
    return HTMLResponse(body, status_code=200)

@router.get("/classical/copy")
def copy_to_staging(parameter: str, state: str):
    safe = lambda s: "".join(c for c in s if c.isalnum() or c in (" ","-","_")).strip().replace(" ", "_")
    prefix = f"{safe(parameter)}_{safe(state)}"
    candidates = sorted(glob.glob(os.path.join(OUTPUT_DIR, f"{prefix}*.csv")), key=os.path.getmtime, reverse=True)
    if not candidates:
        return JSONResponse({"error": "No matching CSV found in output yet."}, status_code=404)
    src = candidates[0]
    dst = os.path.join(STAGING_HISTORICAL_DIR, os.path.basename(src))
    shutil.copy2(src, dst)
    return {"copied": True, "from": src, "to": dst}
