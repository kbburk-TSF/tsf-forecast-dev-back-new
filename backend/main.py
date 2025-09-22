# ==============================================================================
# backend/main.py  -- RESTORED
# Generated: 2025-09-22T01:50:53.356267Z
# Purpose: Mount all required routers so the frontend works without edits.
# Routers:
#   - backend.routes.forms_upload_historical
#   - backend.routes.forms_engine_kickoff
#   - backend.routes.forms_export_forecasts
#   - backend.routes.forms_classical   (legacy endpoint for ClassicalTab.jsx)
# ==============================================================================

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os, logging

log = logging.getLogger("uvicorn.error")

app = FastAPI(title="TSF Backend", version=os.getenv("APP_VERSION") or "dev")

# ---- CORS ----
env_origins = os.getenv("ALLOWED_ORIGINS", "").strip()
allowed = [o.strip() for o in env_origins.split(",") if o.strip()] or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Helper to safely include routers ----
def safe_include(module_path: str, attr: str):
    try:
        mod = __import__(module_path, fromlist=[attr])
        app.include_router(getattr(mod, attr))
        log.info(f"Mounted router: {module_path}.{attr}")
        return True
    except Exception as e:
        log.error(f"Failed to mount {module_path}.{attr}: {e}")
        return False

# ---- Mount routers (no other edits required) ----
safe_include("backend.routes.forms_upload_historical", "router")
safe_include("backend.routes.forms_engine_kickoff", "router")
safe_include("backend.routes.forms_export_forecasts", "router")
safe_include("backend.routes.forms_classical", "router")   # legacy endpoint expected by the frontend

# ---- Health/endpoints ----
@app.get("/health")
def health():
    return {"ok": True, "ts": "2025-09-22T01:50:53.356267Z"}

@app.get("/")
def root():
    return {
        "ok": True,
        "routes": [
            "/forms/upload-historical",
            "/forms/engine-kickoff",
            "/forms/export-forecasts",
            "/forms/classical/run"
        ]
    }
