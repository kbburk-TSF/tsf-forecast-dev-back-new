# ==============================================================================
# backend/main.py  -- RESTORE classical page + keep other forms mounted
# Generated: 2025-09-22T02:03:29.272730Z
# ==============================================================================
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os, logging

log = logging.getLogger("uvicorn.error")
app = FastAPI(title="TSF Backend", version=os.getenv("APP_VERSION") or "dev")

env_origins = os.getenv("ALLOWED_ORIGINS", "").strip()
allowed = [o.strip() for o in env_origins.split(",") if o.strip()] or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def safe_include(module_path: str, attr: str):
    try:
        mod = __import__(module_path, fromlist=[attr])
        app.include_router(getattr(mod, attr))
        log.info(f"Mounted router: {module_path}.{attr}")
        return True
    except Exception as e:
        log.error(f"Failed to mount {module_path}.{attr}: {e}")
        return False

# Mount ALL known form routers (nothing removed)
safe_include("backend.routes.forms_upload_historical", "router")
safe_include("backend.routes.forms_engine_kickoff", "router")
safe_include("backend.routes.forms_export_forecasts", "router")
safe_include("backend.routes.forms_classical", "router")

@app.get("/health")
def health():
    return {"ok": True, "ts": "2025-09-22T02:03:29.272730Z"}

@app.get("/")
def root():
    return {"ok": True, "routes": [
        "/forms/classical",
        "/forms/classical/run",
        "/forms/upload-historical",
        "/forms/engine-kickoff",
        "/forms/export-forecasts"
    ]}
