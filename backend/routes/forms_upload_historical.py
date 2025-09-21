# backend/routes/forms_upload_historical.py
import os, io, uuid, asyncio, csv, time
from typing import Dict, List, Tuple
from fastapi import APIRouter, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError

router = APIRouter()

ENGINE_DB_URL_DIRECT = os.getenv("ENGINE_DATABASE_URL_DIRECT", "").strip() or None
ENGINE_DB_URL = ENGINE_DB_URL_DIRECT or os.getenv("ENGINE_DATABASE_URL")
if not ENGINE_DB_URL:
    raise RuntimeError("ENGINE_DATABASE_URL (or ENGINE_DATABASE_URL_DIRECT) is not set")

def _make_engine(url: str):
    return create_engine(
        url,
        pool_pre_ping=True,
        future=True,
        connect_args={"connect_timeout": 10},
    )

_engine = _make_engine(ENGINE_DB_URL)
TARGET_FQN = "engine.staging_historical"
PROGRESS: Dict[str, Dict] = {}

def _connect_with_retry(max_attempts: int = 6, sleep_secs: float = 5.0):
    attempt = 0
    last_err = None
    while attempt < max_attempts:
        try:
            conn = _engine.connect()
            conn.exec_driver_sql("select 1")
            return conn, attempt + 1
        except OperationalError as e:
            msg = str(e).lower()
            last_err = e
            if "control plane request failed" in msg or "timeout" in msg or "connection refused" in msg:
                time.sleep(sleep_secs)
                attempt += 1
                continue
            raise
    raise last_err

@router.get("/forms/debug/engine-db")
def debug_engine_db():
    try:
        conn, used_attempts = _connect_with_retry()
        with conn.begin():
            db = conn.exec_driver_sql("select current_database()").scalar()
            sp = conn.exec_driver_sql("show search_path").scalar()
            ver = conn.exec_driver_sql("show server_version").scalar()
            exists = conn.exec_driver_sql(
                "select exists (select 1 from information_schema.tables "
                " where table_schema='engine' and table_name='staging_historical')"
            ).scalar()
        conn.close()
        return JSONResponse({
            "ok": True,
            "url_in_use": ENGINE_DB_URL,
            "attempts_used": used_attempts,
            "current_database": db,
            "search_path": sp,
            "server_version": ver,
            "engine.staging_historical_exists": bool(exists),
        })
    except SQLAlchemyError as e:
        return JSONResponse({"ok": False, "url_in_use": ENGINE_DB_URL, "error": str(e.__cause__ or e)}, status_code=500)

@router.get("/forms/warm-engine-db")
def warm_engine_db():
    try:
        conn, used_attempts = _connect_with_retry()
        with conn.begin():
            conn.exec_driver_sql("select 1")
        conn.close()
        return {"ok": True, "attempts_used": used_attempts}
    except SQLAlchemyError as e:
        return JSONResponse({"ok": False, "error": str(e.__cause__ or e)}, status_code=500)

@router.get("/forms/upload-historical", response_class=HTMLResponse)
async def upload_form():
    return HTMLResponse(
        "<!doctype html><meta charset='utf-8'>"
        "<title>Upload Historical CSV</title>"
        "<h2>Upload to air_quality_engine_research.engine.staging_historical</h2>"
        "<p><a href='/forms/debug/engine-db' target='_blank'>Check connection</a> · "
        "<a href='/forms/warm-engine-db' target='_blank'>Warm DB</a></p>"
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

        conn, _ = _connect_with_retry()
        with conn.begin():
            exists = conn.execute(text(
                "select 1 from information_schema.tables "
                "where table_schema='engine' and table_name='staging_historical'"
            )).first()
            if not exists:
                raise RuntimeError("engine.staging_historical not found in ENGINE DB")

        PROGRESS[job_id].update(state="inserting", inserted=0)

        insert_sql = text("INSERT INTO engine.staging_historical (parameter, state, date, value) VALUES (:p,:s,:d,:v)")

        BATCH = 1000
        batch: List[Tuple[str,str,str,str]] = []
        inserted = 0

        with conn.begin():
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

        conn.close()
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
