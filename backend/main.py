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

# Mount existing known routers (kept generic; your app may mount more elsewhere)
safe_include("backend.routes.forms_upload_historical", "router")
safe_include("backend.routes.forms_engine_kickoff", "router")

# NEW: diagnostics router to isolate issues with kickoff
safe_include("backend.routes.engine_kickoff_diag", "router")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/")
def root():
    return {"ok": True, "routes": ["/forms/engine-kickoff", "/forms/engine-kickoff/ping", "/forms/engine-kickoff/force"]}
