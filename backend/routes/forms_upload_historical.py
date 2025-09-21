# backend/routes/forms_upload_historical.py
# Direct-only. Forces search_path=engine,public and inserts into engine.staging_historical.
import os, io, uuid, asyncio, csv, time, threading
from typing import Dict, List, Tuple
from fastapi import APIRouter, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError

router = APIRouter()

ENGINE_DB_URL = (os.getenv("ENGINE_DATABASE_URL_DIRECT") or "").strip()
if not ENGINE_DB_URL:
    raise RuntimeError("ENGINE_DATABASE_URL_DIRECT must be set (direct Neon URL)")

CONNECT_TIMEOUT = int(os.getenv("NEON_CONNECT_TIMEOUT", "10"))
KEEPALIVE_SECS = int(os.getenv("NEON_KEEPALIVE_SECONDS", "25"))
RETRY_ATTEMPTS  = int(os.getenv("NEON_CONNECT_RETRIES", "10"))
RETRY_SLEEP     = float(os.getenv("NEON_CONNECT_RETRY_SLEEP", "3"))

TARGET_FQN = "engine.staging_historical"

def _make_engine(url: str):
    # Force the server to use engine,public for name resolution
    return create_engine(
        url,
        pool_pre_ping=True,
        future=True,
        connect_args={
            "connect_timeout": CONNECT_TIMEOUT,
            "options": "-c search_path=engine,public"
        },
    )

_engine = _make_engine(ENGINE_DB_URL)
PROGRESS: Dict[str, Dict] = {}
PING_STATE = {"running": False, "last_ok": None, "errors": 0}

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

@router.get("/forms/debug/engine-db")
def debug_engine_db():
    try:
        attempts_used = _connect_with_retry()
        with _engine.connect() as conn:
            db = conn.exec_driver_sql("select current_database()").scalar()
            sp = conn.exec_driver_sql("show search_path").scalar()
            ver = conn.exec_driver_sql("show server_version").scalar()
            # Check target existence explicitly
            exists = conn.execute(text(
                "select exists (select 1 from information_schema.tables "
                " where table_schema='engine' and table_name='staging_historical')"
            )).scalar()
        return JSONResponse({
            "ok": True,
            "url_in_use": ENGINE_DB_URL,
            "attempts_used": attempts_used,
            "current_database": db,
            "search_path": sp,
            "server_version": ver,
            "target_fqn": TARGET_FQN,
            "target_exists": bool(exists),
            "keepalive": {"running": PING_STATE["running"], "last_ok_epoch": PING_STATE["last_ok"], "errors": PING_STATE["errors"]},
        })
    except SQLAlchemyError as e:
        return JSONResponse({"ok": False, "url_in_use": ENGINE_DB_URL, "error": str(e.__cause__ or e)}, status_code=500)

@router.get("/forms/upload-historical", response_class=HTMLResponse)
async def upload_form():
    return HTMLResponse(
        "<!doctype html><meta charset='utf-8'>"
        "<title>Upload Historical CSV</title>"
        f"<h2>Upload to {TARGET_FQN}</h2>"
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

        # Verify target exists (explicitly in engine schema)
        with _engine.connect() as conn:
            exists = conn.execute(text(
                "select 1 from information_schema.tables "
                "where table_schema='engine' and table_name='staging_historical'"
            )).first()
        if not exists:
            raise RuntimeError("Target table engine.staging_historical not found in air_quality_engine_research.")

        insert_sql = text("INSERT INTO engine.staging_historical (parameter, state, date, value) VALUES (:p,:s,:d,:v)")

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
