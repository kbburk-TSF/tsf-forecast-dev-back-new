# backend/routes/forms_upload_historical.py
# Drop‑in replacement: keeps existing /forms/upload-historical route,
# adds /debug/engine-db on the SAME router so it's automatically available
# wherever this module is already included.
import os, io, uuid, asyncio, csv
from typing import Dict, List, Tuple

from fastapi import APIRouter, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

router = APIRouter()

# Explicitly use the ENGINE (research) DB
ENGINE_DB_URL = os.getenv("ENGINE_DATABASE_URL")
if not ENGINE_DB_URL:
    raise RuntimeError("ENGINE_DATABASE_URL is not set")

_engine = create_engine(ENGINE_DB_URL, pool_pre_ping=True, future=True)
TARGET_FQN = "engine.staging_historical"
PROGRESS: Dict[str, Dict] = {}

@router.get("/debug/engine-db")
def debug_engine_db():
    """Prove connection + table existence against ENGINE_DATABASE_URL."""
    try:
        with _engine.begin() as conn:
            db = conn.exec_driver_sql("select current_database()").scalar()
            sp = conn.exec_driver_sql("show search_path").scalar()
            ver = conn.exec_driver_sql("show server_version").scalar()
            exists = conn.exec_driver_sql(
                "select exists (select 1 from information_schema.tables "
                " where table_schema='engine' and table_name='staging_historical')"
            ).scalar()
        return JSONResponse({
            "ok": True,
            "current_database": db,
            "search_path": sp,
            "server_version": ver,
            "engine.staging_historical_exists": bool(exists),
        })
    except SQLAlchemyError as e:
        return JSONResponse({"ok": False, "error": str(e.__cause__ or e)}, status_code=500)

@router.get("/forms/upload-historical", response_class=HTMLResponse)
async def upload_form():
    return HTMLResponse(
        "<!doctype html><meta charset='utf-8'>"
        "<title>Upload Historical CSV</title>"
        "<h2>Upload to air_quality_engine_research.engine.staging_historical</h2>"
        "<p><a href='/debug/engine-db' target='_blank'>Check connection</a></p>"
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

        # verify table on ENGINE DB
        with _engine.begin() as conn:
            exists = conn.execute(text(
                "select 1 from information_schema.tables "
                "where table_schema='engine' and table_name='staging_historical'"
            )).first()
            if not exists:
                raise RuntimeError("engine.staging_historical not found in ENGINE_DATABASE_URL DB")

        PROGRESS[job_id].update(state="inserting", inserted=0)

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
