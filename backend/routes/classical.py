
import os, json
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, HTTPException
from fastapi.params import Body
from fastapi.responses import FileResponse
from pydantic import BaseModel
from redis import Redis
from rq import Queue
from rq.job import Job

router = APIRouter(prefix="/classical", tags=["classical"])

# Job storage (fast local)
JOBS_DIR = Path(os.getenv("TSF_JOBS_DIR", "/tmp/tsf_jobs"))
JOBS_DIR.mkdir(parents=True, exist_ok=True)

def _csv_file(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.csv"

def _redis() -> Redis:
    url = os.getenv("REDIS_URL") or os.getenv("REDIS_TLS_URL")
    if not url:
        raise RuntimeError("REDIS_URL not set")
    return Redis.from_url(url)

def _queue() -> Queue:
    return Queue(os.getenv("TSF_RQ_QUEUE", "tsf"), connection=_redis())

class StartRequest(BaseModel):
    target_value: str
    state_name: str | None = None
    county_name: str | None = None
    city_name: str | None = None
    cbsa_name: str | None = None
    agg: str = "mean"
    ftype: str = "F"

@router.post("/start")
def start(req: StartRequest = Body(...)):
    job_id = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    payload = {
        "job_id": job_id,
        "target_value": req.target_value,
        "state_name": req.state_name,
        "county_name": req.county_name,
        "city_name": req.city_name,
        "cbsa_name": req.cbsa_name,
        "agg": req.agg,
        "ftype": req.ftype,
        "jobs_dir": str(JOBS_DIR),
    }
    q = _queue()
    job = q.enqueue("backend.worker.classical_worker.run_job", kwargs=payload, job_id=job_id, ttl=86400, result_ttl=86400)
    return {"job_id": job.id, "state": job.get_status(refresh=False) or "queued"}

@router.get("/status")
def status(job_id: str):
    try:
        job = Job.fetch(job_id, connection=_redis())
    except Exception:
        raise HTTPException(status_code=404, detail="job not found")
    state = job.get_status(refresh=False)
    meta = job.meta or {}
    resp = {"job_id": job_id, "state": state or "unknown"}
    if "progress" in meta:
        resp["progress"] = int(meta["progress"])
    if "message" in meta:
        resp["message"] = str(meta["message"])
    if state == "failed" and job.exc_info:
        resp["message"] = "failed"
    return resp

@router.get("/download")
def download(job_id: str):
    f = _csv_file(job_id)
    if not f.exists():
        raise HTTPException(status_code=404, detail="file not ready")
    return FileResponse(f, media_type="text/csv", filename=f.name)
