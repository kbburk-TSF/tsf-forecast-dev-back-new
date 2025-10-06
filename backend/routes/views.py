
# backend/routes/views.py
# Version: 2025-10-05 v7.0 — Month+Span UI; correct query against engine.tsf_vw_full
from typing import Dict, List
from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
import os, datetime as dt, json
import psycopg
from psycopg.rows import dict_row

router = APIRouter(prefix="/views", tags=["views"])

COLS: List[str] = ["forecast_name","date","value","model_name",
    "fv","fv_mape","fv_mean_mape","fv_mean_mape_c",
    "ci85_low","ci85_high","ci90_low","ci90_high","ci95_low","ci95_high", "ARIMA_M","HWES_M","SES_M"]

def _select_list(cols: list[str]) -> str:
    quoted = []
    for c in cols:
        if c in ("ARIMA_M","HWES_M","SES_M"):
            quoted.append(f'"{c}"')
        else:
            quoted.append(c)
    return ", ".join(quoted)

def _db_url() -> str:
    return os.getenv("ENGINE_DATABASE_URL_DIRECT") or os.getenv("ENGINE_DATABASE_URL") or os.getenv("DATABASE_URL") or ""

def _connect():
    dsn = _db_url()
    if not dsn:
        raise RuntimeError("Database URL not configured")
    return psycopg.connect(dsn, autocommit=True)

def _ym_first(ym: str) -> dt.date:
    y, m = ym.split("-")
    return dt.date(int(y), int(m), 1)

def _add_months(d: dt.date, n: int) -> dt.date:
    y = d.year + (d.month - 1 + n) // 12
    m = (d.month - 1 + n) % 12 + 1
    return dt.date(y, m, 1)

def _range_from_month_span(ym: str, span: int):
    span = 1 if span not in (1,2,3) else span
    start = _ym_first(ym) - dt.timedelta(days=7)  # include 7-day pre-roll
    stop = _add_months(_ym_first(ym), span)       # exclusive
    return start, stop

_HTML = """<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>engine.tsf_vw_full</title>
<style>
body{font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Arial;margin:24px}
.card{border:1px solid #e5e7eb;border-radius:12px;padding:16px;max-width:1100px}
.row{display:flex;gap:12px;align-items:flex-end;flex-wrap:wrap}
label{font-size:12px;color:#374151;display:block;margin-bottom:4px}
select,button{padding:8px 10px;border:1px solid #d1d5db;border-radius:8px}
table{border-collapse:collapse;width:100%;margin-top:12px;font-size:12px}
th,td{border-top:1px solid #e5e7eb;padding:6px 8px;white-space:nowrap;text-align:left}
th{background:#f9fafb;position:sticky;top:0}
.actions{display:flex;gap:8px}
.muted{color:#6b7280}
.err{color:#b91c1c;margin-top:8px}
</style>
</head>
<body>
<div class="card">
  <h2>engine.tsf_vw_full</h2>
  <div class="row">
    <div><label>Forecast (forecast_name)</label><select id="forecast"></select></div>
    <div><label>Month</label><select id="month"></select></div>
    <div><label>Span</label>
      <select id="span"><option value="1">1 month</option><option value="2">2 months</option><option value="3">3 months</option></select>
    </div>
    <div class="actions"><button id="run">Run</button><button id="csv">Download CSV</button></div>
    <div id="status" class="muted"></div>
  </div>
  <div style="overflow:auto;max-height:65vh">
    <table><thead id="thead"></thead><tbody id="tbody"></tbody></table>
  </div>
  <div id="error" class="err"></div>
</div>
<script>
const COLS = {{COLS_JSON}};
function el(id){return document.getElementById(id)}
function setHeaders(){document.getElementById('thead').innerHTML='<tr>'+COLS.map(h=>`<th>${h}</th>`).join('')+'</tr>'}
async function getJSON(u){const r=await fetch(u);if(!r.ok) throw new Error(await r.text());return r.json()}
async function postJSON(u,b){const r=await fetch(u,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)});if(!r.ok) throw new Error(await r.text());return r.json()}
async function loadForecasts(){const data=await getJSON('/views/forecasts');const s=el('forecast');s.innerHTML=data.map(v=>`<option value="${v}">${v}</option>`).join('');await loadMonths()}
async function loadMonths(){const fid=el('forecast').value;const data=await getJSON('/views/months?forecast_name='+encodeURIComponent(fid));const s=el('month');s.innerHTML=data.map(v=>`<option value="${v}">${v}</option>`).join('')}
function renderRows(rows){el('tbody').innerHTML=rows.map(r=>'<tr>'+COLS.map(c=>`<td>${(r[c]??'')}</td>`).join('')+'</tr>').join('')}
async function run(){el('error').textContent='';el('status').textContent='Running...';const payload={forecast_name:el('forecast').value,month:el('month').value,span:parseInt(el('span').value)};const out=await postJSON('/views/query',payload);renderRows(out.rows);el('status').textContent=`${out.rows.length} rows`}
function csv(){const qs=new URLSearchParams({forecast_name:el('forecast').value,month:el('month').value,span:el('span').value});window.location='/views/export?'+qs.toString()}
document.addEventListener('DOMContentLoaded',async()=>{setHeaders();await loadForecasts();el('forecast').addEventListener('change',loadMonths);el('run').addEventListener('click',run);el('csv').addEventListener('click',csv)})
</script>
</body></html>
"""

@router.get("/", response_class=HTMLResponse)
def page():
    html = _HTML.replace("{{COLS_JSON}}", json.dumps(COLS))
    return HTMLResponse(html)

@router.get("/forecasts")
def forecasts():
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT forecast_name FROM engine.tsf_vw_full ORDER BY 1")
        return [r[0] for r in cur.fetchall()]

@router.get("/months")
def months(forecast_name: str):
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT to_char(date_trunc('month', date), 'YYYY-MM') AS ym
            FROM engine.tsf_vw_full
            WHERE forecast_name = %s
            GROUP BY ym
            ORDER BY ym
        """, [forecast_name])
        return [r[0] for r in cur.fetchall()]

@router.post("/query")
def query(payload: Dict):
    forecast_name = payload.get("forecast_name")
    month = payload.get("month")
    span = int(payload.get("span") or 1)
    if not forecast_name or not month:
        raise HTTPException(status_code=400, detail="forecast_name and month are required")
    start, stop = _range_from_month_span(month, span)
    sql = f"""
        SELECT {_select_list(COLS)}
        FROM engine.tsf_vw_full
        WHERE forecast_name = %s
          AND date >= %s AND date < %s
        ORDER BY date ASC
    """
    with _connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, [forecast_name, start, stop])
        rows = [dict(r) for r in cur.fetchall()]
    return {"rows": rows, "total": len(rows)}

@router.get("/export")
def export(forecast_name: str, month: str, span: int = 1):
    start, stop = _range_from_month_span(month, int(span))
    sql = f"""
        SELECT {_select_list(COLS)}
        FROM engine.tsf_vw_full
        WHERE forecast_name = %s
          AND date >= %s AND date < %s
        ORDER BY date ASC
    """
    def row_iter():
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(sql, [forecast_name, start, stop])
            headers = [d.name for d in cur.description]
            yield (",".join(headers) + "\\n").encode("utf-8")
            for rec in cur:
                out = []
                for v in rec:
                    if v is None: out.append("")
                    elif hasattr(v, "isoformat"): out.append(v.isoformat())
                    else:
                        s = str(v)
                        if any(ch in s for ch in [",","\\n","\""]): s = '\"' + s.replace('\"','\"\"') + '\"'
                        out.append(s)
                yield (",".join(out) + "\\n").encode("utf-8")
    fname = f"tsf_vw_full_{forecast_name}_{month}_x{span}.csv".replace(" ","_")
    return StreamingResponse(row_iter(), media_type="text/csv",
                             headers={"Content-Disposition": f"attachment; filename=\"{fname}\""})
