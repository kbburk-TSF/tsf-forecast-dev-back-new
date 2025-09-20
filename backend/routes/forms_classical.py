# forms_classical.py — templates/Jinja2 version
from typing import List, Optional
import os, shutil, glob
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from starlette.templating import Jinja2Templates
import psycopg
from psycopg.rows import tuple_row
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

router = APIRouter(prefix="/forms", tags=["forms"])

# Jinja templates under backend/templates
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # backend/
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

DATABASE_URL = os.getenv("DATABASE_URL")
SCHEMA = "public"
TABLE_FQ = f'{SCHEMA}."air_quality_raw"'
COL_PARAM = '"Parameter Name"'
COL_STATE = '"State Name"'
COL_DATE  = '"Date Local"'
COL_VALUE = '"Arithmetic Mean"'

OUTPUT_DIR = os.path.join(BASE_DIR, "data", "output")
STAGING_HISTORICAL_DIR = os.getenv("STAGING_HISTORICAL_DIR", os.path.join(BASE_DIR, "staging_historical"))
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

@router.get("/classical", response_class=HTMLResponse)
def form_classical(request: Request, parameter: Optional[str] = None):
    error = None
    parameters = []
    states = []
    try:
        parameters = list_parameters()
        states = list_states_for_param(parameter if parameter else None)
    except Exception as e:
        error = f"Database error: {e}"
    return templates.TemplateResponse(
        "forms/classical.html",
        {
            "request": request,
            "parameters": parameters,
            "states": states,
            "selected_parameter": parameter,
            "error": error,
        },
        status_code=200
    )

@router.post("/classical/start")
def start_from_form(parameter: str = Form(...), state: str = Form(...)):
    # Redirect to an intermediate page that posts into the existing classical pipeline
    return RedirectResponse(url=f"/forms/classical/next?parameter={parameter}&state={state}", status_code=303)

@router.get("/classical/next", response_class=HTMLResponse)
def next_step(request: Request, parameter: str, state: str):
    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Run Classical</title></head><body>
  <h2>Run Classical</h2>
  <form method="post" action="/classical/start">
    <input type="hidden" name="parameter" value="{parameter}"/>
    <input type="hidden" name="state" value="{state}"/>
    <button type="submit">Start Job</button>
  </form>
  <p>Use <code>/classical/status?job_id=...</code> and <code>/classical/download?job_id=...</code> after starting.</p>
</body></html>"""
    return HTMLResponse(html, status_code=200)

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
