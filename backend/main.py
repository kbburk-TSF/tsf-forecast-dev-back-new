from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.routes.data import router as data_router
from backend.routes.aggregate import router as agg_router
from backend.routes.forecast import router as forecast_router
from backend.routes.meta import router as meta_router
from backend.routes.classical import router as classical_router
import os

app = FastAPI(title="TSF Backend", version="2.0.3")

# CORS (env-driven; defaults to "*")
env_origins = os.getenv("ALLOWED_ORIGINS", "").strip()
allowed = [o.strip() for o in env_origins.split(",") if o.strip()] or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed,
    allow_credentials=False,   # set True only if you configure specific origins
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routers exactly once
app.include_router(data_router)
app.include_router(agg_router)
app.include_router(forecast_router)
app.include_router(meta_router)
app.include_router(classical_router)

@app.get("/health")
def health():
    return {"status": "ok", "database": "up", "schema": "ready"}

@app.get("/version")
def version():
    try:
        with open("VERSION", "r") as f:
            return {"version": f.read().strip()}
    except Exception:
        return {"version": "unknown"}
