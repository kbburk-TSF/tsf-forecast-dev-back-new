# main.py — minimal app wired to backend.routes
# Version: 2025-10-05 v2.1 (2025-10-06 00:29) — add /health and /api/health endpoints (no other changes)
# Purpose: FastAPI app that includes routers from backend.routes.views and backend.routes.tsfview

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Import routers from the proper package paths
from backend.routes import views as _views
from backend.routes import tsfview as _tsfview

app = FastAPI(title="TSF Backend")

# CORS (permissive; match your existing policy if different)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routers
app.include_router(_views.router)
app.include_router(_tsfview.router)

@app.get("/", include_in_schema=False)
def root():
    return {"ok": True, "service": "tsf-backend"}

# --- Added health endpoints ---
@app.get("/health", include_in_schema=False)
def health_root():
    return {"status": "ok"}

@app.get("/api/health", include_in_schema=False)
def health_api():
    return {"status": "ok"}
