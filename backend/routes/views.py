
# backend/routes/views.py
# Version: 2025-09-24 v4.1 (V11_14 views, fixed indentation)
# Notes:
# - Routes target engine.tsf_vw_full (pre-baked cache view from V11_14).
# - Since the view hides forecast_id, filtering joins forecast_registry on forecast_name.
# - Columns returned are unchanged from the UI expectations.
# - Fixed indentation and parameter ordering.

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

def _extract_models(views) -> List[str]:
    # Kept for UI parity; may be empty for your current setup.
    models = set()
    for v in views:
        name = v["viewname"]
        if name.endswith("_vw_daily_best"):
            base = name[: -len("_vw_daily_best")]
            for sfx in ("_instance_forecast_ms", "_instance_forecast_msq", "_instance_forecast_msqm"):
                if base.endswith(sfx):
                    base = base[: -len(sfx)]
                    break
            models.add(base)
    return sorted(models)

def _resolve_view(scope: str, model: Optional[str], series: Optional[str], views) -> str:
    # Everything routes to the global pre-baked full view
    if not _exists(views, "tsf_vw_full"):
        raise HTTPException(404, "V11_14 view engine.tsf_vw_full not found")
    return "engine.tsf_vw_full"

@router.get("/", response_class=HTMLResponse)
def views_form():
    html = """
<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>TSF — Views Debugger</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
      :root { --card:#f6f8fb; --border:#dfe3ea; --fg:#111; --muted:#666; font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; }
      body { margin:24px; color:var(--fg); }
      .card { background:var(--card); border:1px solid var(--border); border-radius:10px; padding:16px; max-width:1100px; }
      .row { display:flex; gap:14px; align-items:flex-start; flex-wrap:wrap; margin:12px 0; }
      label { font-size:12px; color:var(--muted); display:block; margin-bottom:4px; }
      select,input[type=date] { padding:8px 10px; border:1px solid var(--border); border-radius:8px; min-width:170px; }
      .btn { padding:10px 14px; border-radius:8px; background:#111; color:white; border:none; cursor:pointer; }
      .muted { color:var(--muted); font-size:12px; }
      table { width:100%; border-collapse:separate; border-spacing:0; }
      th, td { border-bottom:1px solid var(--border); padding:8px 10px; text-align:left; }
      thead th { position:sticky; top:0; background:var(--card); }
      .scope { display:flex; gap:16px; }
      .err { color:#b00020; }
    </style>
  </head>
  <body>
    <h1>TSF — Views (Backend Form)</h1>
    <div class="card">
      <div class="row scope">
        <label><input type="radio" name="scope" value="per_table" checked> Per-table</label>
        <label><input type="radio" name="scope" value="per_model"> Per-model</label>
        <label><input type="radio" name="scope" value="global"> Global</label>
      </div>

      <div class="row">
        <div><label>Model</label><select id="model"></select></div>
        <div id="seriesWrap"><label>Series</label><select id="series"><option>S</option><option>SQ</option><option>SQM</option></select></div>
        <div><label>Forecast</label><select id="fid"></select></div>
        <div><label>Date Window</label>
          <select id="preset">
            <option value="30">Last 30 days</option>
            <option value="90" selected>Last 90 days</option>
            <option value="365">Last 365 days</option>
            <option value="all">All</option>
            <option value="custom">Custom…</option>
          </select>
        </div>
        <div id="customDates" style="display:none">
          <div><label>From</label><input type="date" id="from"/></div>
          <div><label>To</label><input type="date" id="to"/></div>
        </div>
        <div style="align-self:end; display:flex; gap:10px;">
          <button class="btn" id="load">Load</button>
          <button class="btn" id="csv" disabled>Export CSV</button>
        </div>
      </div>

      <div class="row" style="justify-content:space-between">
        <div class="muted" id="status">Loading metadata…</div>
      </div>

      <div style="overflow:auto; max-height:60vh; border:1px solid var(--border); border-radius:10px;">
        <table id="grid">
          <thead><tr id="head"></tr></thead>
          <tbody id="body"></tbody>
        </table>
      </div>
    </div>

    <script>
      const SCOPE = () => document.querySelector('input[name="scope"]:checked').value;
      const el = (id) => document.getElementById(id);
      const HEADERS = ["date","value","fv_l","fv","fv_u","fv_mean_mae","fv_interval_odds","fv_interval_sig","fv_variance_mean","fv_mean_mae_c"];

      function setStatus(msg){ el('status').textContent = msg; }

      async function meta(){
        const r = await fetch('/views/meta_form');
        if(!r.ok) throw new Error('meta ' + r.status);
        return r.json();
      }
      async function ids(scope, model, series){
        const q = new URLSearchParams({scope, model: model||'', series: series||''});
        const r = await fetch('/views/ids?' + q.toString());
        if(!r.ok) throw new Error('ids ' + r.status);
        return r.json();
      }
      async function query(payload){
        const r = await fetch('/views/query', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
        if(!r.ok) throw new Error('query ' + r.status);
        return r.json();
      }

      function fillSelectPairs(sel, pairs){
        sel.innerHTML = '';
        if(!pairs || !pairs.length){
          const o = document.createElement('option'); o.textContent = '(none)'; o.value=''; sel.appendChild(o);
          return;
        }
        pairs.forEach(({id, name}) => {
          const o = document.createElement('option');
          o.value = id; o.textContent = name || id;
          sel.appendChild(o);
        });
      }
      function fillSelect(sel, vals){
        sel.innerHTML = '';
        if(!vals || !vals.length){
          const o = document.createElement('option'); o.textContent = '(none)'; o.value=''; sel.appendChild(o);
          return;
        }
        vals.forEach(v => { const o = document.createElement('option'); o.value = v; o.textContent = v; sel.appendChild(o); });
      }

      function renderHead(){
        const tr = el('head'); tr.innerHTML = '';
        HEADERS.forEach(h => { const th = document.createElement('th'); th.textContent = h; tr.appendChild(th); });
      }

      function renderRows(rows){
        const tb = el('body'); tb.innerHTML = '';
        rows.forEach(r => {
          const tr = document.createElement('tr');
          HEADERS.forEach(k => {
            const td = document.createElement('td');
            td.textContent = (r[k] ?? '');
            tr.appendChild(td);
          });
          tb.appendChild(tr);
        });
      }

      function currentRange(){
        const p = el('preset').value;
        if(p === 'custom'){
          return {date_from: el('from').value || null, date_to: el('to').value || null};
        }
        if(p === 'all') return {date_from:null,date_to:null};
        const days = parseInt(p,10) || 90;
        const d = new Date();
        const date_to = d.toISOString().slice(0,10);
        d.setDate(d.getDate() - days);
        const date_from = d.toISOString().slice(0,10);
        return {date_from, date_to};
      }

      function buildPayload(){
        const scope = SCOPE();
        return {
          scope,
          model: scope==='global' ? '' : el('model').value,
          series: scope==='per_table' ? el('series').value : '',
          forecast_id: el('fid').value,
          ...currentRange(),
          page: 1, page_size: 2000
        };
      }

      function updateExportHref(){
        const p = buildPayload();
        if(!p.forecast_id){ el('csv').disabled = true; return; }
        const q = new URLSearchParams(p);
        el('csv').dataset.href = '/views/export?' + q.toString();
        el('csv').disabled = false;
      }

      async function refreshIds(){
        const scope = SCOPE();
        el('seriesWrap').style.display = (scope === 'per_table') ? 'block' : 'none';
        const model = (scope === 'global') ? '' : el('model').value;
        const series = (scope === 'per_table') ? el('series').value : '';
        try{
          const list = await ids(scope, model, series);
          fillSelectPairs(el('fid'), list);
          setStatus((list.length ? 'Pick a forecast and Load' : 'No forecasts for this selection'));
          updateExportHref();
        }catch(e){
          setStatus('IDs load failed: ' + e.message);
          fillSelectPairs(el('fid'), []);
          updateExportHref();
        }
      }

      async function bootstrap(){
        try{
          const m = await meta();
          setStatus('Meta loaded');
          fillSelect(el('model'), m.models);
          await refreshIds();
        }catch(e){
          setStatus('Meta load failed: ' + e.message);
        }
      }

      async function doLoad(){
        const payload = buildPayload();
        if(!payload.forecast_id){ setStatus('Select a forecast first'); return; }
        setStatus('Loading…');
        try{
          const res = await query(payload);
          renderHead(); renderRows(res.rows || []);
          setStatus(String((res.rows||[]).length) + ' / ' + String(res.total) + ' rows');
        }catch(e){
          setStatus('Query failed: ' + e.message);
        }
      }

      document.addEventListener('change', (ev) => {
        if(ev.target.name === 'scope'){ refreshIds(); }
        if(ev.target.id === 'model' || ev.target.id === 'series'){ refreshIds(); }
        if(ev.target.id === 'fid'){ updateExportHref(); }
        if(ev.target.id === 'preset'){
          el('customDates').style.display = (el('preset').value === 'custom') ? 'flex' : 'none';
          updateExportHref();
        }
        if(ev.target.id === 'from' || ev.target.id === 'to'){ updateExportHref(); }
      });

      document.addEventListener('DOMContentLoaded', () => {
        document.getElementById('load').addEventListener('click', doLoad);
        document.getElementById('csv').addEventListener('click', (ev) => {
          const href = ev.currentTarget.dataset.href;
          if(!href){ ev.preventDefault(); return; }
          window.location.href = href;
        });
        renderHead();
        bootstrap();
      });
    </script>
  </body>
</html>
    """
    return HTMLResponse(content=html)

@router.get("/meta_form")
def meta_form():
    try:
        with _connect() as conn:
            views = _discover_views(conn)
            models = _extract_models(views)
            return {"scopes":["per_table","per_model","global"], "models":models, "series":["S","SQ","SQM"], "most_recent": {}}
    except Exception as e:
        return {"error": str(e), "trace": traceback.format_exc(), "ok": False, "step": "meta_form"}

@router.get("/ids")
def ids(scope: str = FQuery(...), model: Optional[str] = None, series: Optional[str] = None, limit: int = 100):
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

@router.post("/query")
def run_query(body: ViewsQueryBody):
    if not body.forecast_id:
        raise HTTPException(400, "forecast_id required")

    limit = max(1, min(10000, int(body.page_size or 2000)))
    offset = max(0, (max(1, int(body.page or 1))-1) * limit)

    with _connect() as conn:
        views = _discover_views(conn)
        vname = _resolve_view(body.scope, body.model, body.series, views)

        # Build dynamic conditions and parameters
        conds = ["fr.forecast_id = %s"]
        params = [body.forecast_id]
        if body.date_from:
            conds.append("v.date >= %s")
            params.append(body.date_from)
        if body.date_to:
            conds.append("v.date <= %s")
            params.append(body.date_to)

        cols = "date, value, fv_l, fv, fv_u, fv_mean_mae, fv_interval_odds, fv_interval_sig, fv_variance_mean, fv_mean_mae_c"
        where_clause = " AND ".join(conds)
        sql = f"SELECT {cols} FROM {vname} v JOIN engine.forecast_registry fr ON fr.forecast_name = v.forecast_name WHERE {where_clause} ORDER BY date ASC LIMIT %s OFFSET %s"
        cnt = f"SELECT COUNT(*) AS n FROM {vname} v JOIN engine.forecast_registry fr ON fr.forecast_name = v.forecast_name WHERE {where_clause}"

        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(cnt, params)
            total = int(cur.fetchone()["n"])
            cur.execute(sql, params + [limit, offset])
            rows = cur.fetchall()

    return {"rows": rows, "total": total}

@router.get("/export")
def export_csv(scope: str, model: Optional[str] = None, series: Optional[str] = None,
               forecast_id: str = FQuery(...), date_from: Optional[str] = None, date_to: Optional[str] = None):
    # Stream full CSV (no pagination)
    with _connect() as conn:
        views = _discover_views(conn)
        vname = _resolve_view(scope, model, series, views)

        conds = ["fr.forecast_id = %s"]
        params = [forecast_id]
        if date_from:
            conds.append("v.date >= %s")
            params.append(date_from)
        if date_to:
            conds.append("v.date <= %s")
            params.append(date_to)

        cols = ["date","value","fv_l","fv","fv_u","fv_mean_mae","fv_interval_odds","fv_interval_sig","fv_variance_mean","fv_mean_mae_c"]
        base = f"FROM {vname} v JOIN engine.forecast_registry fr ON fr.forecast_name = v.forecast_name WHERE " + " AND ".join(conds)
        sql = f"SELECT {', '.join(cols)} " + base + " ORDER BY date ASC"

        def row_iter():
            yield (",".join(cols) + "\n").encode("utf-8")
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for rec in cur:
                    # rec is a tuple in declared column order
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
