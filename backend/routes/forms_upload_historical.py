# backend/routes/forms_upload_historical.py
import io, uuid, asyncio, csv
from typing import Dict, List, Tuple
from fastapi import APIRouter, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse
from sqlalchemy import text
from backend.database import engine

TARGET = "engine.staging_historical"

router = APIRouter()
PROGRESS: Dict[str, Dict] = {}

@router.get("/forms/upload-historical", response_class=HTMLResponse)
async def upload_form():
    return HTMLResponse(
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

        buf.seek(0)
        reader = csv.reader(buf)
        next(reader, None)

        with engine.begin() as conn:
            exists = conn.execute(text(
                "select 1 from information_schema.tables "
                "where table_schema='engine' and table_name='staging_historical'"
            )).first()
            if not exists:
                raise RuntimeError("Table engine.staging_historical not found.")

        PROGRESS[job_id].update(state="inserting", inserted=0)

        insert_sql = text(
            f"INSERT INTO {TARGET} (parameter, state, date, value) "
            "VALUES (:p, :s, :d, :v)"
        )

        BATCH = 1000
        batch: List[Tuple[str,str,str,str]] = []
        inserted = 0

        with engine.begin() as conn:
            for row in reader:
                if not row:
                    continue
                row = (row + [None, None, None, None])[:4]
                p, s, d, v = row[0], row[1], row[2], row[3] if (row[3] not in ('', None)) else None
                batch.append((p, s, d, v))
                if len(batch) >= BATCH:
                    conn.execute(insert_sql, [{"p":b[0], "s":b[1], "d":b[2], "v":b[3]} for b in batch])
                    inserted += len(batch)
                    PROGRESS[job_id]["inserted"] = inserted
                    batch.clear()

            if batch:
                conn.execute(insert_sql, [{"p":b[0], "s":b[1], "d":b[2], "v":b[3]} for b in batch])
                inserted += len(batch)
                PROGRESS[job_id]["inserted"] = inserted

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
