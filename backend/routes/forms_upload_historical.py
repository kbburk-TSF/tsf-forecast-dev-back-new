# routes/forms_upload_historical.py
# Plug-in route for uploading a CSV directly into engine.staging_historical (ENGINE_DB_SCHEMA)
# Uses ENGINE_DATABASE_URL and ENGINE_DB_SCHEMA env vars (as in your Render screenshot).
#
# Endpoints:
#   GET  /forms/upload-historical          -> simple HTML to prove connectivity
#   POST /forms/upload-historical          -> accepts CSV, starts ingest, returns {job_id}
#   GET  /forms/upload-historical/stream/{job_id} -> SSE progress
#
# No validation is performed; CSV must match table (parameter,state,date,value) with a header row.

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
    schema = os.environ.get("ENGINE_DB_SCHEMA", "engine")
    return schema

@router.get("/forms/upload-historical", response_class=HTMLResponse)
async def upload_form():
    # Renders the template we ship, but as raw HTML to avoid templating dependencies.
    # If you prefer, drop this HTML into your templates/forms directory and render it there.
    return HTMLResponse((
        "<!doctype html><meta charset='utf-8'>"
        "<title>Upload Historical CSV</title>"
        "<h2>Upload to engine.staging_historical</h2>"
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

        PROGRESS[job_id].update(state="inserting", inserted=0)

        dsn = _engine_dsn()
        target_schema = _engine_schema()
        target_table = f"{target_schema}.staging_historical"

        buf = io.StringIO(data.decode("utf-8", errors="ignore"))

        async with await psycopg.AsyncConnection.connect(dsn) as conn:
            async with conn.cursor(row_factory=tuple_row) as cur:
                header = buf.readline()
                if not header:
                    raise ValueError("Empty file")
                # set search_path to ensure schema resolution
                await cur.execute("SET search_path TO " + target_schema)
                await cur.execute(f"COPY {target_table} (parameter,state,date,value) FROM STDIN WITH (FORMAT csv)")

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
