# backend/routes/forms_upload_historical.py
# Direct-only + AUTO-RESOLVE the real staging_historical table inside air_quality_engine_research.
# No guessing at runtime: we enumerate information_schema and lock onto ONE fully-qualified table.
import os, io, uuid, asyncio, csv, time, threading
from typing import Dict, List, Tuple, Optional
from fastapi import APIRouter, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError

router = APIRouter()

ENGINE_DB_URL = (os.getenv("ENGINE_DATABASE_URL_DIRECT") or "").strip()
if not ENGINE_DB_URL:
    raise RuntimeError("ENGINE_DATABASE_URL_DIRECT must be set to the Neon DIRECT URL")

# Optional override. If set, we will validate and use exactly this.
ENV_TARGET = (os.getenv("ENGINE_TARGET_FQN") or "").strip()

CONNECT_TIMEOUT = int(os.getenv("NEON_CONNECT_TIMEOUT", "10"))
KEEPALIVE_SECS = int(os.getenv("NEON_KEEPALIVE_SECONDS", "25"))
RETRY_ATTEMPTS  = int(os.getenv("NEON_CONNECT_RETRIES", "10"))
RETRY_SLEEP     = float(os.getenv("NEON_CONNECT_RETRY_SLEEP", "3"))

def _make_engine(url: str):
    return create_engine(
        url,
        pool_pre_ping=True,
        future=True,
        connect_args={"connect_timeout": CONNECT_TIMEOUT},
    )

_engine = _make_engine(ENGINE_DB_URL)
PROGRESS: Dict[str, Dict] = {}
PING_STATE = {"running": False, "last_ok": None, "errors": 0}
RESOLVED: Dict[str, Optional[str]] = {"target_fqn": None, "why": None}

def _connect_with_retry(max_attempts: int = RETRY_ATTEMPTS, sleep_secs: float = RETRY_SLEEP):
    attempt = 0
    last_err = None
    while attempt < max_attempts:
        try:
            with _engine.connect() as conn:
                conn.exec_driver_sql("select 1")
            return attempt + 1
        except OperationalError as e:
            last_err = e
            time.sleep(sleep_secs)
            attempt += 1
    raise last_err

def _keepalive_loop():
    PING_STATE["running"] = True
    while True:
        try:
            _connect_with_retry()
            PING_STATE["last_ok"] = time.time()
        except Exception:
            PING_STATE["errors"] += 1
        time.sleep(KEEPALIVE_SECS)

threading.Thread(target=_keepalive_loop, daemon=True).start()

def _exists_table(conn, schema: str, table: str) -> bool:
    q = text("select 1 from information_schema.tables where table_schema=:s and table_name=:t")
    return conn.execute(q, {"s": schema, "t": table}).first() is not None

def _auto_resolve_target() -> str:
    """Resolve the fully-qualified name of the staging table once at startup."""
    with _engine.connect() as conn:
        if ENV_TARGET:
            if "." not in ENV_TARGET:
                raise RuntimeError(f"ENGINE_TARGET_FQN must be schema.table, got '{ENV_TARGET}'")
            s, t = ENV_TARGET.split(".", 1)
            if _exists_table(conn, s, t):
                RESOLVED["target_fqn"] = f"{s}.{t}"
                RESOLVED["why"] = "env_override_validated"
                return RESOLVED["target_fqn"]
            # fall through: env was wrong; enumerate to show facts

        # enumerate all candidates that look like staging_historical
        rows = conn.exec_driver_sql(
            "select table_schema, table_name "
            "from information_schema.tables "
            "where lower(table_name) = 'staging_historical' "
            "order by (case when table_schema='engine' then 0 else 1 end), table_schema"
        ).all()
        cands = [f"{r[0]}.{r[1]}" for r in rows]

        if not cands:
            RESOLVED["target_fqn"] = None
            RESOLVED["why"] = "no_candidates_found"
            raise RuntimeError("No table named 'staging_historical' found in any schema of current DB.")
        if len(cands) == 1:
            RESOLVED["target_fqn"] = cands[0]
            RESOLVED["why"] = "single_candidate"
            return RESOLVED["target_fqn"]

        # multiple candidates; prefer engine, else error with explicit list
        engine_pref = [c for c in cands if c.startswith("engine.")]
        if engine_pref:
            RESOLVED["target_fqn"] = engine_pref[0]
            RESOLVED["why"] = "multiple_candidates_prefer_engine"
            return RESOLVED["target_fqn"]

        RESOLVED["target_fqn"] = None
        RESOLVED["why"] = "multiple_candidates_no_engine"
        raise RuntimeError(f"Multiple 'staging_historical' tables found: {cands} — set ENGINE_TARGET_FQN to select one.")

# Resolve once on import
try:
    TARGET_FQN = _auto_resolve_target()
except Exception as e:
    TARGET_FQN = None
    RESOLVED["error"] = str(e)

@router.get("/forms/debug/engine-db")
def debug_engine_db():
    try:
        attempts_used = _connect_with_retry()
        with _engine.connect() as conn:
            db = conn.exec_driver_sql("select current_database()").scalar()
            sp = conn.exec_driver_sql("show search_path").scalar()
            ver = conn.exec_driver_sql("show server_version").scalar()
        return JSONResponse({
            "ok": True,
            "url_in_use": ENGINE_DB_URL,
            "attempts_used": attempts_used,
            "current_database": db,
            "search_path": sp,
            "server_version": ver,
            "resolved": RESOLVED,
            "target_fqn": TARGET_FQN,
        })
    except SQLAlchemyError as e:
        return JSONResponse({"ok": False, "url_in_use": ENGINE_DB_URL, "error": str(e.__cause__ or e)}, status_code=500)

@router.get("/forms/upload-historical", response_class=HTMLResponse)
async def upload_form():
    hdr = f"<h2>Upload to {TARGET_FQN or '[UNRESOLVED]'} </h2>"
    return HTMLResponse(
        "<!doctype html><meta charset='utf-8'>"
        "<title>Upload Historical CSV</title>"
        + hdr +
        "<p><a href='/forms/debug/engine-db' target='_blank'>Check connection</a></p>"
        "<form id=f method=post enctype=multipart/form-data action='/forms/upload-historical'>"
        "<input type=file name=file accept='.csv' required> "
        "<button>Upload</button></form>"
        "<pre id=out></pre>"
        "<script>"
        "const out=document.getElementById('out');"
        "document.getElementById('f').addEventListener('submit',async(e)=>{"
        " e.preventDefault(); out.textContent='Uploading...';"
        " const r=await fetch(e.target.action,{method:'POST',body:new FormData(e.target)});"
        " if(!r.ok){ out.textContent='HTTP '+r.status; return; }"
        " const {job_id}=await r.json(); out.textContent='Job '+job_id+' started. Monitoring...';"
        " const es=new EventSource('/forms/upload-historical/stream/'+job_id);"
        " es.onmessage=(ev)=>{ out.textContent=ev.data; if(ev.data.includes('state=done')||ev.data.includes('state=error')) es.close(); };"
        " es.onerror=()=>{ es.close(); };"
        "});"
        "</script>"
    )

@router.post("/forms/upload-historical")
async def upload(file: UploadFile = File(...)):
    if not TARGET_FQN:
        return JSONResponse({"error": f"Target not resolved: {RESOLVED}"}, status_code=500)
    job_id = str(uuid.uuid4())
    PROGRESS[job_id] = {"state":"queued","inserted":0,"total":0,"error":None}
    data = await file.read()
    asyncio.create_task(_ingest(job_id, data))
    return {"job_id": job_id}

async def _ingest(job_id: str, data: bytes):
    try:
        PROGRESS[job_id].update(state="reading")
        buf = io.StringIO(data.decode("utf-8", errors="ignore"))
        reader = csv.reader(buf)
        header = next(reader, None)
        if not header:
            raise ValueError("Empty file")

        total = sum(1 for _ in reader)
        PROGRESS[job_id]["total"] = total

        buf.seek(0); reader = csv.reader(buf); next(reader, None)

        insert_sql = text(f"INSERT INTO {TARGET_FQN} (parameter, state, date, value) VALUES (:p,:s,:d,:v)")

        BATCH = 1000
        batch: List[Tuple[str,str,str,str]] = []
        inserted = 0

        with _engine.begin() as conn:
            for row in reader:
                if not row: continue
                row = (row + [None, None, None, None])[:4]
                p, s, d, v = row[0], row[1], row[2], (row[3] if row[3] not in ('', None) else None)
                batch.append((p, s, d, v))
                if len(batch) >= BATCH:
                    conn.execute(insert_sql, [{"p":b[0], "s":b[1], "d":b[2], "v":b[3]} for b in batch])
                    inserted += len(batch); PROGRESS[job_id]["inserted"] = inserted; batch.clear()
            if batch:
                conn.execute(insert_sql, [{"p":b[0], "s":b[1], "d":b[2], "v":b[3]} for b in batch])
                inserted += len(batch); PROGRESS[job_id]["inserted"] = inserted

        PROGRESS[job_id].update(state="done")
    except Exception as e:
        PROGRESS[job_id].update(state="error", error=str(e))

@router.get("/forms/upload-historical/stream/{job_id}")
async def stream(job_id: str):
    async def gen():
        last = None
        while True:
            s = PROGRESS.get(job_id)
            if not s:
                yield "data: job not found\n\n"; return
            msg = f"state={s['state']} inserted={s['inserted']} total={s['total']} error={s['error']}"
            if msg != last: yield f"data: {msg}\n\n"; last = msg
            if s["state"] in ("done","error"): return
            await asyncio.sleep(0.5)
    return StreamingResponse(gen(), media_type="text/event-stream")
