# backend/routes/views.py
# Version: 2025-09-24 v5.0 (MINIMAL FORM -> tsf_vw_full only)
# Changes:
# - Stripped all options; single source: engine.tsf_vw_full
# - No radio buttons. Only forecast selector (shows forecast_name, value=forecast_id) and date range.
# - Queries engine.forecast_registry for the dropdown; reads engine.tsf_vw_full for results.
# - Uses DATABASE_URL env; trims whitespace/newlines to avoid channel_binding parse issues.
#
# Endpoints:
#   GET  /views          -> render form
#   GET  /views/results  -> run query and render basic table
#
# Drop-in: place at backend/routes/views.py and ensure this router is included in main.py

import os
from datetime import date, datetime
from typing import List, Dict, Optional

import psycopg
from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse

router = APIRouter(prefix="", tags=["views"])

# --- DB helpers ---

def _dsn() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("DB_DSN") or ""
    # Normalize common copy/paste issues (trailing newline/space)
    return dsn.strip()

def db():
    dsn = _dsn()
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg.connect(dsn, autocommit=True)

def list_forecasts() -> List[Dict]:
    sql = """
        SELECT forecast_id, forecast_name
        FROM engine.forecast_registry
        WHERE forecast_name IS NOT NULL
        ORDER BY created_at DESC NULLS LAST, forecast_name ASC
    """
    with db() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql)
        return cur.fetchall()

def run_query(forecast_id: int, start: Optional[str], end: Optional[str]) -> List[Dict]:
    params = {"fid": forecast_id}
    filters = ["forecast_id = %(fid)s"]
    if start:
        filters.append("date >= %(start)s")
        params["start"] = start
    if end:
        filters.append("date <= %(end)s")
        params["end"] = end

    sql = f"""
        SELECT *
        FROM engine.tsf_vw_full
        WHERE {' AND '.join(filters)}
        ORDER BY date ASC
        LIMIT 5000
    """
    with db() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

# --- HTML helpers (inline, no templates) ---

def _html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
         .replace("'", "&#39;")
    )

def _render_form(options: List[Dict], preset: Optional[int] = None) -> str:
    # Minimal styles inline to avoid external deps
    opts_html = []
    for row in options:
        sel = " selected" if preset and int(row["forecast_id"]) == preset else ""
        label = f"{row['forecast_name']}"
        opts_html.append(f'<option value="{row["forecast_id"]}"{sel}>{_html_escape(label)}</option>')
    return f"""
    <!doctype html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>TSF View Query</title>
        <style>
            body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; max-width: 980px; margin: 24px auto; padding: 0 16px; }}
            form  {{ display: grid; grid-template-columns: 1fr 1fr 1fr auto; gap: 12px; align-items: end; }}
            label {{ font-size: 12px; color: #444; display:block; margin-bottom: 6px; }}
            select, input[type=date] {{ padding: 8px; font-size: 14px; }}
            button {{ padding: 10px 14px; font-size: 14px; cursor: pointer; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 24px; }}
            th, td {{ border: 1px solid #ddd; padding: 6px 8px; font-size: 13px; }}
            th {{ background: #f6f6f6; text-align: left; position: sticky; top: 0; }}
            .muted {{ color:#666; font-size:12px; }}
        </style>
    </head>
    <body>
        <h1>TSF View: engine.tsf_vw_full</h1>
        <form action="/views/results" method="get">
            <div>
                <label for="forecast_id">Forecast</label>
                <select id="forecast_id" name="forecast_id" required>
                    <option value="" disabled {'' if preset else 'selected'}>Select a forecast...</option>
                    {''.join(opts_html)}
                </select>
            </div>
            <div>
                <label for="start_date">Start date</label>
                <input type="date" id="start_date" name="start_date">
            </div>
            <div>
                <label for="end_date">End date</label>
                <input type="date" id="end_date" name="end_date">
            </div>
            <div>
                <button type="submit">Run</button>
            </div>
        </form>
        <p class="muted">Shows only data from <code>engine.tsf_vw_full</code>. Filters: forecast_id, optional date range.</p>
    </body>
    </html>
    """

def _render_table(rows: List[Dict], info: Dict) -> str:
    if not rows:
        return f"""
        <!doctype html><html><head><meta charset="utf-8"><title>No Results</title></head>
        <body>
            <p><strong>No rows</strong> for forecast_id {info.get('forecast_id')} within the requested range.</p>
            <p><a href="/views">Back</a></p>
        </body></html>
        """
    # Header from first row keys
    cols = list(rows[0].keys())
    thead = ''.join(f"<th>{_html_escape(str(c))}</th>" for c in cols)
    trs = []
    for r in rows[:2000]:  # safety cap on render
        tds = ''.join(f"<td>{_html_escape(str(r.get(c, '')))}</td>" for c in cols)
        trs.append(f"<tr>{tds}</tr>")
    more = "" if len(rows) <= 2000 else f"<p class='muted'>Showing first 2000 of {len(rows)} rows.</p>"
    return f"""
    <!doctype html>
    <html>
    <head><meta charset="utf-8"><title>Results</title>
        <style>
            body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; max-width: 98%; margin: 24px auto; padding: 0 16px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 6px 8px; font-size: 13px; }}
            th {{ background: #f6f6f6; text-align: left; position: sticky; top: 0; }}
            .muted {{ color:#666; font-size:12px; margin: 8px 0; }}
        </style>
    </head>
    <body>
        <h1>Results: engine.tsf_vw_full</h1>
        <p class="muted">forecast_id={info.get('forecast_id')}, start={info.get('start_date') or '—'}, end={info.get('end_date') or '—'}</p>
        {more}
        <div style="overflow:auto; max-height: 78vh;">
            <table>
                <thead><tr>{thead}</tr></thead>
                <tbody>
                    {''.join(trs)}
                </tbody>
            </table>
        </div>
        <p><a href="/views">Back</a></p>
    </body>
    </html>
    """

# --- Routes ---

@router.get("/views", response_class=HTMLResponse)
def form(forecast_id: Optional[int] = None):
    options = list_forecasts()
    html = _render_form(options, preset=forecast_id)
    return HTMLResponse(content=html)

@router.get("/views/results", response_class=HTMLResponse)
def results(
    forecast_id: int = Query(..., description="forecast_id to filter"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    rows = run_query(forecast_id, start_date, end_date)
    html = _render_table(rows, {"forecast_id": forecast_id, "start_date": start_date, "end_date": end_date})
    return HTMLResponse(content=html)
