# Version: patched-for-dsn-fix (2025-09-23)
# Version: patched-for-dsn-fix (2025-09-23)
# backend/routes/views.py
# Read-only Views API + simple HTML form.
# Uses ONLY TSF_ENGINE_APP for DB access.
# Forecast picker shows forecast_name but submits forecast_id.

from typing import Optional, Dict, List
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import os

router = APIRouter(prefix="/views", tags=["views"])

def _build_engine_from_env() -> Engine:
    url = os.getenv("TSF_ENGINE_APP")
    if not url:
        raise RuntimeError("TSF_ENGINE_APP not set")
    url = url.strip()
    if "channel_binding=require\\n" in url:
        url = url.replace("channel_binding=require\\n", "channel_binding=require")
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if url.startswith("postgresql://") and "+psycopg" not in url and "+psycopg2" not in url:
        try:
            import psycopg  # noqa: F401
            url = url.replace("postgresql://", "postgresql+psycopg://", 1)
        except Exception:
            url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return create_engine(url, pool_pre_ping=True)

_engine: Engine = None
def _conn():
    global _engine
    if _engine is None:
        _engine = _build_engine_from_env()
    return _engine.begin()

def _discover_views() -> List[Dict[str,str]]:
    sql = """
    SELECT schemaname, viewname
    FROM pg_catalog.pg_views
    WHERE schemaname='engine'
      AND (viewname = 'tsf_vw_daily_best' OR viewname LIKE '%_vw_daily_best')
    """
    with _conn() as conn:
        rows = conn.execute(text(sql)).mappings().all()
    return [dict(r) for r in rows]

def _exists(views, name: str) -> bool:
    return any(v["schemaname"] == "engine" and v["viewname"] == name for v in views)

def _extract_models(views) -> List[str]:
    models = set()
    for v in views:
        name = v["viewname"]
        if name == "tsf_vw_daily_best":
            continue
        if name.endswith("_vw_daily_best"):
            base = name[: -len("_vw_daily_best")]
            for sfx in ("_instance_forecast_s", "_instance_forecast_ms", "_instance_forecast_msq", "_instance_forecast_msqm"):
                if base.endswith(sfx):
                    base = base[: -len(sfx)]
                    break
            models.add(base)
    return sorted(models)

def _resolve_view(scope: str, model: Optional[str], series: Optional[str], views) -> str:
    s = (scope or "").lower()
    if s == "global":
        if not _exists(views, "tsf_vw_daily_best"):
            raise HTTPException(404, "Global view not found")
        return "engine.tsf_vw_daily_best"
    if s == "per_model":
        if not model:
            raise HTTPException(400, "Model required for per_model")
        name = f"{model}_vw_daily_best"
        if not _exists(views, name):
            raise HTTPException(404, f"Per-model view not found: {name}")
        return f"engine.{name}"
    if s == "per_table":
        if not (model and series):
            raise HTTPException(400, "Model and series required for per_table")
        ser = series.lower()
        if ser not in ("s","ms","sq","sqm"):
            raise HTTPException(400, "Series must be S/MS/SQ/SQM")
        name = f"{model}_instance_forecast_{ser}_vw_daily_best"
        if not _exists(views, name):
            raise HTTPException(404, f"Per-table view not found: {name}")
        return f"engine.{name}"
    raise HTTPException(400, "Invalid scope")

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
        <div id="seriesWrap"><label>Series</label><select id="series"><option>S</option><option>MS</option><option>SQ</option><option>SQM</option></select></div>
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
        <div style="align-self:end"><button class="btn" id="load">Load</button></div>
      </div>

      <div class="row" style="justify-content:space-between">
        <div class="muted" id="status">Loading metadata…</div>
        <div><button class="btn" id="csv" disabled>Export CSV</button></div>
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
      const HEADERS = ["date","value","fv_l","fv","fv_u","fv_mean_mae","fv_interval_odds","fv_interval_sig","fv_variance_mean","fv_mean_mae_c","model_name","series","season","fmsr_series"];

      function setStatus(msg){ el('status').textContent = msg; }

      async function meta(){
        const r = await fetch('/views/meta');
        if(!r.ok) throw new Error('meta ' + r.status);
        return r.json();
      }
      async function ids(scope, model, series){
        const q = new URLSearchParams({scope, model: model||'', series: series||''});
        const r = await fetch('/views/ids?' + q.toString());
        if(!r.ok) throw new Error('ids ' + r.status);
        return r.json(); // [{id, name}]
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

      async function refreshIds(){
        const scope = SCOPE();
        el('seriesWrap').style.display = (scope === 'per_table') ? 'block' : 'none';
        const model = (scope === 'global') ? '' : el('model').value;
        const series = (scope === 'per_table') ? el('series').value : '';
        try{
          const list = await ids(scope, model, series);
          fillSelectPairs(el('fid'), list);
          setStatus((list.length ? 'Pick a forecast and Load' : 'No forecasts for this selection'));
        }catch(e){
          setStatus('IDs load failed: ' + e.message);
          fillSelectPairs(el('fid'), []);
        }
      }

      function currentRange(){
        const p = el('preset').value;
        if(p === 'custom'){
          return {from: el('from').value || null, to: el('to').value || null};
        }
        if(p === 'all') return {from:null,to:null};
        const days = parseInt(p,10) || 90;
        const d = new Date();
        const to = d.toISOString().slice(0,10);
        d.setDate(d.getDate() - days);
        const from = d.toISOString().slice(0,10);
        return {from,to};
      }

      async function doLoad(){
        const scope = SCOPE();
        const payload = {
          scope,
          model: scope==='global' ? '' : el('model').value,
          series: scope==='per_table' ? el('series').value : '',
          forecast_id: el('fid').value,
          ...currentRange(),
          page: 1, page_size: 2000
        };
        if(!payload.forecast_id){ setStatus('Select a forecast first'); return; }
        setStatus('Loading…');
        try{
          const res = await query(payload);
          renderHead(); renderRows(res.rows || []);
          el('csv').disabled = !(res.rows && res.rows.length);
          setStatus(f"{(res.rows||[]).length} / {res.total} rows");  # noqa: E999
        }catch(e){
          setStatus('Query failed: ' + e.message);
        }
      }

      document.addEventListener('change', (ev) => {
        if(ev.target.name === 'scope'){ refreshIds(); }
        if(ev.target.id === 'model' || ev.target.id === 'series'){ refreshIds(); }
        if(ev.target.id === 'preset'){
          el('customDates').style.display = (el('preset').value === 'custom') ? 'flex' : 'none';
        }
      });

      el('load').addEventListener('click', doLoad);
      el('csv').addEventListener('click', () => {
        const rows = Array.from(el('body').querySelectorAll('tr')).map(tr => Array.from(tr.children).map(td => td.textContent));
        const head = HEADERS.join(',');
        const lines = rows.map(r => r.join(','));
        const blob = new Blob([head + "\n" + lines.join("\n")], {type: 'text/csv'});
        const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'view_slice.csv'; a.click();
        setTimeout(() => URL.revokeObjectURL(a.href), 500);
      });

      renderHead();
      bootstrap();
    </script>
  </body>
</html>
    """
    return HTMLResponse(content=html)

@router.get("/meta")
def meta():
    views = _discover_views()
    models = _extract_models(views)
    most_recent = {}
    def fetch_recent(vname: str):
        sql = f"SELECT forecast_id FROM {vname} ORDER BY created_at DESC NULLS LAST LIMIT 1"
        with _conn() as conn:
            row = conn.execute(text(sql)).mappings().first()
            return str(row["forecast_id"]) if row and row.get("forecast_id") else None

    if _exists(views, "tsf_vw_daily_best"):
        rid = fetch_recent("engine.tsf_vw_daily_best")
        if rid:
            most_recent["global||"] = rid
    for m in models:
        v = f"engine.{m}_vw_daily_best"
        if _exists(views, f"{m}_vw_daily_best"):
            rid = fetch_recent(v)
            if rid:
                most_recent[f"per_model|{m}|"] = rid
    for m in models:
        for ser in ("s","ms","sq","sqm"):
            vname = f"{m}_instance_forecast_{ser}_vw_daily_best"
            if _exists(views, vname):
                rid = fetch_recent(f"engine.{vname}")
                if rid:
                    most_recent[f"per_table|{m}|{ser.upper()}"] = rid

    return {"scopes":["per_table","per_model","global"], "models":models, "series":["S","MS","SQ","SQM"], "most_recent": most_recent}

@router.get("/ids")
def ids(scope: str = Query(...), model: Optional[str] = None, series: Optional[str] = None, limit: int = 100):
    views = _discover_views()
    vname = _resolve_view(scope, model, series, views)
    sql = f"""
    SELECT fr.forecast_id AS id, COALESCE(fr.forecast_name, fr.forecast_id::text) AS name, x.mc
    FROM (
      SELECT forecast_id, MAX(created_at) AS mc
      FROM {vname}
      GROUP BY forecast_id
    ) x
    JOIN engine.forecast_registry fr ON fr.forecast_id = x.forecast_id
    ORDER BY x.mc DESC NULLS LAST
    LIMIT :lim
    """
    with _conn() as conn:
        rows = conn.execute(text(sql), {"lim": limit}).mappings().all()
    return [{"id": str(r["id"]), "name": r["name"]} for r in rows]

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
def query(body: ViewsQueryBody):
    if not body.forecast_id:
        raise HTTPException(400, "forecast_id required")
    views = _discover_views()
    vname = _resolve_view(body.scope, body.model, body.series, views)
    where = ["forecast_id = :id"]
    params = {"id": body.forecast_id}
    if body.date_from:
        where.append("date >= :d1")
        params["d1"] = body.date_from
    if body.date_to:
        where.append("date <= :d2")
        params["d2"] = body.date_to
    where_sql = " AND ".join(where)
    limit = max(1, min(10000, int(body.page_size or 2000)))
    offset = max(0, (max(1, int(body.page or 1))-1) * limit)

    cols = "date, value, fv_l, fv, fv_u, fv_mean_mae, fv_interval_odds, fv_interval_sig, fv_variance_mean, fv_mean_mae_c, model_name, series, season, fmsr_series, created_at"
    sql = f"SELECT {cols} FROM {vname} WHERE {where_sql} ORDER BY date ASC LIMIT :lim OFFSET :off"
    cnt = f"SELECT COUNT(*) AS n FROM {vname} WHERE {where_sql}"
    with _conn() as conn:
        total = int(conn.execute(text(cnt), params).mappings().first()["n"])
        rows = conn.execute(text(sql), {**params, "lim": limit, "off": offset}).mappings().all()
    return {"rows": rows, "total": total}
