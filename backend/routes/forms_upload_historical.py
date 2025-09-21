# backend/routes/forms_upload_historical.py
# Upload CSV into <ENGINE_DB_SCHEMA>.staging_historical with live SSE progress.
# Relies on existing env vars:
#   ENGINE_DATABASE_URL (Postgres DSN)
#   ENGINE_DB_SCHEMA    (schema name; default "engine")
#
# Endpoints:
#   GET  /forms/upload-historical
#   POST /forms/upload-historical
#   GET  /forms/upload-historical/stream/{job_id}

import os, io, uuid, asyncio
from typing import Dict

from fastapi import APIRouter, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
import psycopg
from psycopg.rows import tuple_row

router = APIRouter()
PROGRESS: Dict[str, Dict] = {}

def _engine_dsn() -> str:
    dsn = os.environ.get("ENGINE_DATABASE_URL")
    if not dsn:
        raise RuntimeError("ENGINE_DATABASE_URL not set")
    return dsn

def _engine_schema() -> str:
    return os.environ.get("ENGINE_DB_SCHEMA", "engine")

@router.get("/forms/upload-historical", response_class=HTMLResponse)
async def upload_form():
    return HTMLResponse((
        "<!doctype html><meta charset='utf-8'>"
        "<title>Upload Historical CSV</title>"
        "<h2>Upload to staging_historical</h2>"
        "<form id=f method=post enctype=multipart/form-data action='/forms/upload-historical'>"
        "<input type=file name=file accept='.csv' required> <button>Upload</button></form>"
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
    ))

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
        total_lines = data.count(b"\n")
        PROGRESS[job_id]["total"] = max(total_lines - 1, 0)

        dsn = _engine_dsn()
        schema = _engine_schema()
        target = f"{schema}.staging_historical"

        PROGRESS[job_id].update(state="inserting", inserted=0)

        buf = io.StringIO(data.decode("utf-8", errors="ignore"))

        async with await psycopg.AsyncConnection.connect(dsn) as conn:
            async with conn.cursor(row_factory=tuple_row) as cur:
                header = buf.readline()
                if not header:
                    raise ValueError("Empty file")
                await cur.execute("SET search_path TO " + schema)
                await cur.execute(f"COPY {target} (parameter,state,date,value) FROM STDIN WITH (FORMAT csv)")

                inserted = 0
                CHUNK = 5000
                cache = []

                for line in buf:
                    cache.append(line)
                    if len(cache) >= CHUNK:
                        await cur.copy_data(''.join(cache))
                        inserted += len(cache)
                        PROGRESS[job_id]["inserted"] = inserted
                        cache.clear()

                if cache:
                    await cur.copy_data(''.join(cache))
                    inserted += len(cache)
                    PROGRESS[job_id]["inserted"] = inserted

                await cur.copy_end()
            await conn.commit()

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
                yield "data: job not found\n\n"
                return
            msg = f"state={s['state']} inserted={s['inserted']} total={s['total']} error={s['error']}"
            if msg != last:
                yield f"data: {msg}\n\n"
                last = msg
            if s["state"] in ("done","error"):
                return
            await asyncio.sleep(0.5)
    return StreamingResponse(gen(), media_type="text/event-stream")
