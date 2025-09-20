
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from backend.database import engine
from backend.routes.data import router as data_router
from backend.routes.aggregate import router as aggregate_router
from backend.routes.meta import router as meta_router
from backend.routes.classical import router as classical_router
from backend.routes.forms_classical_flow import router as forms_router
import os
from pathlib import Path

APP_VERSION = os.getenv("APP_VERSION", None)

app = FastAPI(title="TSF Backend", version=APP_VERSION or "dev")

env_origins = os.getenv("ALLOWED_ORIGINS", "").strip()
allowed = [o.strip() for o in env_origins.split(",") if o.strip()] or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(data_router)
app.include_router(aggregate_router)
app.include_router(meta_router)
app.include_router(classical_router)
app.include_router(forms_router)

@app.get("/", tags=["root"])
def root():
    return {
        "ok": True,
        "service": "tsf-backend",
        "forms": { "classical": "/forms/classical" },
        "docs": "/docs",
        "health": "/health",
    }

@app.get("/health", tags=["meta"])
def health():
    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))
        return {"ok": True, "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"ok": False, "database": "error", "error": str(e)})

@app.get("/version", tags=["meta"])
def version():
    if APP_VERSION:
        return {"version": APP_VERSION}
    try:
        here = Path(__file__).resolve().parent.parent
        vfile = here / "VERSION"
        if vfile.exists():
            return {"version": vfile.read_text().strip()}
    except Exception:
        pass
    return {"version": "unknown"}
