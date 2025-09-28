# backend/routes/views.py
# Version: 2025-09-24 v4.1 (V11_14 views, fixed indentation)
# Changes in this drop:
# - Use quoted identifiers "ARIMA_M","HWES_M","SES_M" in SELECT/CSV to match uppercase columns.
# - Bind date_from/date_to as Python date objects (no date>=text errors).

from typing import Optional, Dict, List
from fastapi import APIRouter, HTTPException, Query as FQuery
from fastapi.responses import HTMLResponse, StreamingResponse
import os, traceback, datetime as dt
import psycopg
from psycopg.rows import dict_row

router = APIRouter(prefix="/views", tags=["views"])

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

def _discover_views(conn) -> List[Dict[str,str]]:
    sql = """
    SELECT schemaname, viewname
    FROM pg_catalog.pg_views
    WHERE schemaname='engine'
      AND viewname IN ('tsf_vw_full',
                       'tsf_vw_daily_best_arima_a0',
                       'tsf_vw_daily_best_ses_a0',
                       'tsf_vw_daily_best_hwes_a0')
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [dict(r) for r in rows]

def _exists(views, name: str) -> bool:
    return any(v["schemaname"] == "engine" and v["viewname"] == name for v in views)

def _resolve_view(scope: str, model: Optional[str], series: Optional[str], views) -> str:
    if not _exists(views, "tsf_vw_full"):
        raise HTTPException(404, "V11_14 view engine.tsf_vw_full not found")
    return "engine.tsf_vw_full"

@router.get("/", response_class=HTMLResponse)
def views_form():
    html = r"""
<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>TSF — View (tsf_vw_full)</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
      body { margin:24px; font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; color:#111; }
      .card { background:#f6f8fb; border:1px solid #dfe3ea; border-radius:10px; padding:16px; max-width:1100px; }
      .row { display:flex; gap:14px; align-items:flex-end; flex-wrap:wrap; margin:12px 0; }
      label { font-size:12px; color:#666; display:block; margin-bottom:4px; }
      select,input[type=date] { padding:8px 10px; border:1px solid #dfe3ea; border-radius:8px; min-width:200px; }
      .btn { padding:10px 14px; border-radius:8px; background:#111; color:white; border:none; cursor:pointer; }
      .muted { color:#666; font-size:12px; }
      table { border-collapse: collapse; width: 100%; margin-top: 10px; }
      th, td { border:1px solid #e3e6eb; padding:6px 8px; font-size:13px; }
      th { background:#f3f5f8; position: sticky; top: 0; }
      #tableWrap { max-height: 72vh; overflow:auto; }
    </style>
  </head>
  <body>
    <div class="card">
      <h2 style="margin:0 0 10px 0;">engine.tsf_vw_full</h2>
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
      const SCOPE = () => 'global';
      const el = (id) => document.getElementById(id);
      const HEADERS = ["date","value","ARIMA_M","HWES_M","SES_M","model_name","fv_l","fv","fv_u","fv_mean_mape","fv_mean_mape_c","fv_interval_odds","fv_interval_sig","fv_variance","fv_variance_mean","low","high"];

      function setStatus(msg){ el('status').textContent = msg; }

      async function ids(scope, model, series){
        const q = new URLSearchParams({scope, model: model||'', series: series||''});
        const r = await fetch('/views/ids?' + q.toString());
        if(!r.ok) throw new Error('ids ' + r.status);
        return r.json();
      }

      function renderHead(){
        el('thead').innerHTML = HEADERS.map(h => `<th>${h}</th>`).join('');
      }
      function renderRows(rows){
        const body = el('tbody');
        body.innerHTML = rows.map(r => {
          return `<tr>${HEADERS.map(h => `<td>${(r[h] ?? '')}</td>`).join('')}</tr>`;
        }).join('');
      }

      function buildPayload(){
        const fid = el('fid').value;
        const from = el('from').value;
        const to = el('to').value;
        return {
          scope: SCOPE(),
          model: '',
          series: '',
          forecast_id: fid || null,
          date_from: from || null,
          date_to: to || null,
          page: 1,
          page_size: 2000
        };
      }

      function updateExportHref(){
        const p = buildPayload();
        if(!p.forecast_id){ el('csv').disabled = true; el('csv').dataset.href=''; return; }
        const q = new URLSearchParams(Object.entries(p).filter(([k,v]) => v !== null && v !== ''));
        el('csv').dataset.href = '/views/export?' + q.toString();
        el('csv').disabled = false;
      }

      async function doLoad(){
        try{
          setStatus('Loading…');
          const payload = buildPayload();
          const r = await fetch('/views/query', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify(payload) });
          if(!r.ok) throw new Error('query ' + r.status);
          const data = await r.json();
          renderRows(data.rows || []);
          setStatus((data.total||0) + ' rows');
          updateExportHref();
        }catch(err){
          console.error(err);
          setStatus('Error: ' + err.message);
        }
      }

      async function bootstrap(){
        renderHead();
        const list = await ids(SCOPE(), '', '');
        el('fid').innerHTML = `<option value="" selected disabled>Select forecast…</option>` + list.map(x => `<option value="${x.id}">${x.name}</option>`).join('');
      }

      document.addEventListener('DOMContentLoaded', () => {
        el('load').addEventListener('click', doLoad);
        el('csv').addEventListener('click', (ev) => {
          const href = ev.currentTarget.dataset.href;
          if(!href){ ev.preventDefault(); return; }
          window.location.href = href;
        });
        bootstrap();
      });
    </script>
  </body>
</html>
    """
    return HTMLResponse(content=html)

@router.get("/ids")
def ids(scope: str = FQuery(...), model: Optional[str] = None, series: Optional[str] = None, limit: int = 100):
    try:
        with _connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT fr.forecast_id AS id,
                       COALESCE(fr.forecast_name, fr.forecast_id::text) AS name
                FROM engine.forecast_registry fr
                ORDER BY LOWER(fr.forecast_name) ASC LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        return [{"id": str(r["id"]), "name": r["name"]} for r in rows]
    except Exception as e:
        return {"error": str(e), "trace": traceback.format_exc(), "ok": False, "step": "ids"}

from pydantic import BaseModel

class ViewsQueryBody(BaseModel):
    scope: str
    model: Optional[str] = None
    series: Optional[str] = None
    forecast_id: str
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    page: int = 1
    page_size: int = 2000

def _d(s: Optional[str]) -> Optional[dt.date]:
    if not s: return None
    return dt.date.fromisoformat(s)

@router.post("/query")
def run_query(body: ViewsQueryBody):
    if not body.forecast_id:
        raise HTTPException(400, "forecast_id required")

    limit = max(1, min(10000, int(body.page_size or 2000)))
    offset = max(0, (max(1, int(body.page or 1))-1) * limit)

    with _connect() as conn:
        views = _discover_views(conn)
        vname = _resolve_view(body.scope, body.model, body.series, views)

        conds = ["fr.forecast_id = %s"]
        params = [body.forecast_id]
        if body.date_from:
            conds.append("v.date >= %s")
            params.append(_d(body.date_from))
        if body.date_to:
            conds.append("v.date <= %s")
            params.append(_d(body.date_to))

        cols = 'date, value, "ARIMA_M", "HWES_M", "SES_M", model_name, fv_l, fv, fv_u, fv_mean_mape, fv_interval_odds, fv_interval_sig, fv_variance, fv_variance_mean, fv_mean_mape_c, low, high'
        where_clause = " AND ".join(conds)
        sql = f'SELECT {cols} FROM {vname} v JOIN engine.forecast_registry fr ON fr.forecast_name = v.forecast_name WHERE {where_clause} ORDER BY date ASC LIMIT %s OFFSET %s'
        cnt = f'SELECT COUNT(*) AS n FROM {vname} v JOIN engine.forecast_registry fr ON fr.forecast_name = v.forecast_name WHERE {where_clause}'

        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(cnt, params)
            total = int(cur.fetchone()["n"])
            cur.execute(sql, params + [limit, offset])
            rows = cur.fetchall()

    return {"rows": rows, "total": total}

@router.get("/export")
def export_csv(scope: str, model: Optional[str] = None, series: Optional[str] = None,
               forecast_id: str = FQuery(...), date_from: Optional[str] = None, date_to: Optional[str] = None):
    with _connect() as conn:
        views = _discover_views(conn)
        vname = _resolve_view(scope, model, series, views)

        conds = ["fr.forecast_id = %s"]
        params = [forecast_id]
        if date_from:
            conds.append("v.date >= %s")
            params.append(_d(date_from))
        if date_to:
            conds.append("v.date <= %s")
            params.append(_d(date_to))

        cols = ['date','value','"ARIMA_M"','"HWES_M"','"SES_M"','model_name','fv_l','fv','fv_u','fv_mean_mape','fv_interval_odds','fv_interval_sig','fv_variance','fv_variance_mean','fv_mean_mape_c','low','high']
        base = f"FROM {vname} v JOIN engine.forecast_registry fr ON fr.forecast_name = v.forecast_name WHERE " + " AND ".join(conds)
        sql = f"SELECT {', '.join(cols)} " + base + " ORDER BY date ASC"

        def row_iter():
            headers = ["date","value","ARIMA_M","HWES_M","SES_M","model_name","fv_l","fv","fv_u","fv_mean_mape","fv_interval_odds","fv_interval_sig","fv_variance","fv_variance_mean","fv_mean_mape_c","low","high"]
            yield (",".join(headers) + "\n").encode("utf-8")
            with conn.cursor() as cur:
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

        fname_bits = [scope or 'view']
        if model: fname_bits.append(model)
        if series: fname_bits.append(series.upper())
        if forecast_id: fname_bits.append(str(forecast_id))
        filename = "tsf_export_" + "_".join(fname_bits) + ".csv"
        return StreamingResponse(row_iter(), media_type="text/csv",
                                 headers={"Content-Disposition": f"attachment; filename={filename}"})
