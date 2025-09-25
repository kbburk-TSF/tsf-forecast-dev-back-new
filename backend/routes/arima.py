# backend/routes/arima.py
# View Form & API for engine.tsf_vw_daily_best_arima_a0
# Version: 2025-09-25 v1.0
#
# PURPOSE
# - Serve a simple HTML form to query the ARIMA pre-baked daily-best view.
# - Provide JSON query + CSV export endpoints that ALWAYS read from
#   engine.tsf_vw_daily_best_arima_a0 and require forecast_id.
#
# CONTRACT
# - Columns returned match ChartsTab expectations (date, value, fv, etc.).
# - No low/high computation here; we simply select what's in the view.
#
# ROUTES (mounted under /arima)
#   GET  /arima/              -> HTML form
#   GET  /arima/ids           -> forecast ids (from engine.forecast_registry)
#   POST /arima/query         -> JSON rows from engine.tsf_vw_daily_best_arima_a0
#   GET  /arima/export        -> CSV stream of same query

from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query as FQuery
from fastapi.responses import HTMLResponse, StreamingResponse
import os, datetime as dt, traceback
import psycopg
from psycopg.rows import dict_row
from pydantic import BaseModel

VIEW_NAME = "engine.tsf_vw_daily_best_arima_a0"

router = APIRouter(prefix="/arima", tags=["arima-view"])

def _db_url() -> str:
    return (
        os.getenv("ENGINE_DATABASE_URL_DIRECT")
        or os.getenv("ENGINE_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or ""
    )

def _connect():
    dsn = _db_url()
    if not dsn:
        raise RuntimeError("Database URL not configured")
    return psycopg.connect(dsn, autocommit=True)

@router.get("/", response_class=HTMLResponse)
def arima_form():
    html = f"""
<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>TSF — View ({VIEW_NAME})</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
      body {{ margin:24px; font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; color:#111; }}
      .card {{ background:#f6f8fb; border:1px solid #dfe3ea; border-radius:10px; padding:16px; max-width:1100px; }}
      .row {{ display:flex; gap:14px; align-items:flex-end; flex-wrap:wrap; margin:12px 0; }}
      label {{ font-size:12px; color:#666; display:block; margin-bottom:4px; }}
      select,input[type=date] {{ padding:8px 10px; border:1px solid #dfe3ea; border-radius:8px; min-width:200px; }}
      .btn {{ padding:10px 14px; border-radius:8px; background:#111; color:white; border:none; cursor:pointer; }}
      .muted {{ color:#666; font-size:12px; }}
      table {{ border-collapse: collapse; width: 100%; margin-top: 10px; }}
      th, td {{ border:1px solid #e3e6eb; padding:6px 8px; font-size:13px; }}
      th {{ background:#f3f5f8; position: sticky; top: 0; }}
      #tableWrap {{ max-height: 72vh; overflow:auto; }}
    </style>
  </head>
  <body>
    <div class="card">
      <h2 style="margin:0 0 10px 0;">{VIEW_NAME}</h2>
      <div class="row">
        <div>
          <label for="fid">Forecast (forecast_name)</label>
          <select id="fid"></select>
        </div>
        <div>
          <label for="from">From</label>
          <input type="date" id="from">
        </div>
        <div>
          <label for="to">To</label>
          <input type="date" id="to">
        </div>
        <div>
          <button class="btn" id="load">Run</button>
        </div>
        <div>
          <button class="btn" id="csv" data-href="" disabled>Download CSV</button>
        </div>
      </div>
      <div class="row"><div class="muted" id="status"></div></div>
      <div id="tableWrap">
        <table>
          <thead><tr id="thead"></tr></thead>
          <tbody id="tbody"></tbody>
        </table>
      </div>
    </div>

    <script>
      const el = (id) => document.getElementById(id);
      const HEADERS = ["date","value","model_name","fv_l","fv","fv_u","fv_mean_mape","fv_mean_mape_c","fv_interval_odds","fv_interval_sig","fv_variance","fv_variance_mean","low","high"];
      function setStatus(msg){{ el('status').textContent = msg; }}

      async function ids() {{
        const r = await fetch('/arima/ids');
        if(!r.ok) throw new Error('ids ' + r.status);
        return r.json();
      }}

      function renderHead(){{
        el('thead').innerHTML = HEADERS.map(h => `<th>${{h}}</th>`).join('');
      }}
      function renderRows(rows){{
        const body = el('tbody');
        body.innerHTML = rows.map(r => `<tr>${{HEADERS.map(h => `<td>${{r[h] ?? ''}}</td>`).join('')}}` + '</tr>').join('');
      }}

      function buildPayload(){{
        const fid = el('fid').value;
        const from = el('from').value || null;
        const to = el('to').value || null;
        return {{ forecast_id: fid, date_from: from, date_to: to, page: 1, page_size: 2000 }};
      }}

      function updateExportHref(){{
        const p = buildPayload();
        if(!p.forecast_id){{ el('csv').disabled = true; el('csv').dataset.href=''; return; }}
        const q = new URLSearchParams(Object.entries(p).filter(([k,v]) => v !== null && v !== ''));
        el('csv').dataset.href = '/arima/export?' + q.toString();
        el('csv').disabled = false;
      }}

      async function doLoad(){{
        try{{
          setStatus('Loading…');
          const payload = buildPayload();
          const r = await fetch('/arima/query', {{ method:'POST', headers:{{'content-type':'application/json'}}, body: JSON.stringify(payload) }});
          if(!r.ok) throw new Error('query ' + r.status);
          const data = await r.json();
          renderRows(data.rows || []);
          setStatus((data.total||0) + ' rows');
          updateExportHref();
        }}catch(err){{
          console.error(err);
          setStatus('Error: ' + err.message);
        }}
      }}

      async function bootstrap(){{
        renderHead();
        const list = await ids();
        el('fid').innerHTML = `<option value="" selected disabled>Select forecast…</option>` + list.map(x => `<option value="${{x.id}}">${{x.name}}</option>`).join('');
      }}

      document.addEventListener('DOMContentLoaded', () => {{
        el('load').addEventListener('click', doLoad);
        el('csv').addEventListener('click', (ev) => {{
          const href = ev.currentTarget.dataset.href;
          if(!href){{ ev.preventDefault(); return; }}
          window.location.href = href;
        }});
        bootstrap();
      }});
    </script>
  </body>
</html>
    """
    return HTMLResponse(content=html)

@router.get("/ids")
def ids(limit: int = 200):
    try:
        with _connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT fr.forecast_id AS id,
                       COALESCE(fr.forecast_name, fr.forecast_id::text) AS name
                FROM engine.forecast_registry fr
                ORDER BY fr.forecast_id
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        return [{"id": str(r["id"]), "name": r["name"]} for r in rows]
    except Exception as e:
        return {"error": str(e), "trace": traceback.format_exc(), "ok": False, "step": "ids"}

class QueryBody(BaseModel):
    forecast_id: str
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    page: int = 1
    page_size: int = 2000

@router.post("/query")
def query(body: QueryBody):
    if not body.forecast_id:
        raise HTTPException(400, "forecast_id required")

    limit = max(1, min(10000, int(body.page_size or 2000)))
    offset = max(0, (max(1, int(body.page or 1))-1) * limit)

    cols = "date, value, model_name, fv_l, fv, fv_u, fv_mean_mape, fv_interval_odds, fv_interval_sig, fv_variance, fv_variance_mean, fv_mean_mape_c, low, high"
    conds = ["fr.forecast_id = %s"]
    params = [body.forecast_id]
    if body.date_from:
        conds.append("v.date >= %s")
        params.append(body.date_from)
    if body.date_to:
        conds.append("v.date <= %s")
        params.append(body.date_to)

    where_clause = " AND ".join(conds)
    sql = f"SELECT {cols} FROM {VIEW_NAME} v JOIN engine.forecast_registry fr ON fr.forecast_name = v.forecast_name WHERE {where_clause} ORDER BY date ASC LIMIT %s OFFSET %s"
    cnt = f"SELECT COUNT(*) AS n FROM {VIEW_NAME} v JOIN engine.forecast_registry fr ON fr.forecast_name = v.forecast_name WHERE {where_clause}"

    with _connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(cnt, params)
        total = int(cur.fetchone()["n"])
        cur.execute(sql, params + [limit, offset])
        rows = cur.fetchall()

    return { "rows": rows, "total": total }

@router.get("/export")
def export_csv(forecast_id: str = FQuery(...), date_from: Optional[str] = None, date_to: Optional[str] = None):
    if not forecast_id:
        raise HTTPException(400, "forecast_id required")

    cols = ["date","value","model_name","fv_l","fv","fv_u","fv_mean_mape","fv_interval_odds","fv_interval_sig","fv_variance","fv_variance_mean","fv_mean_mape_c","low","high"]
    conds = ["fr.forecast_id = %s"]
    params = [forecast_id]
    if date_from:
        conds.append("v.date >= %s"); params.append(date_from)
    if date_to:
        conds.append("v.date <= %s"); params.append(date_to)

    base = f"FROM {VIEW_NAME} v JOIN engine.forecast_registry fr ON fr.forecast_name = v.forecast_name WHERE " + " AND ".join(conds)
    sql = f"SELECT {', '.join(cols)} " + base + " ORDER BY date ASC"

    def row_iter():
        yield (",".join(cols) + "\n").encode("utf-8")
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            for rec in cur:
                line = []
                for v in rec:
                    if v is None:
                        line.append("")
                    elif isinstance(v, dt.date):
                        line.append(v.isoformat())
                    else:
                        s = str(v)
                        if any(ch in s for ch in [',','\n','"']):
                            s = '"' + s.replace('"','""') + '"'
                        line.append(s)
                yield (",".join(line) + "\n").encode("utf-8")

    filename = f"tsf_export_arima_{forecast_id}.csv"
    return StreamingResponse(row_iter(), media_type="text/csv",
                             headers={"Content-Disposition": f"attachment; filename={filename}"})
