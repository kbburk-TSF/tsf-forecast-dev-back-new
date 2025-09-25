# main.py — TSF Backend (universal router loader)
# Version: 2025-09-23 v1.0 — Complete replacement; auto-mounts every router in backend.routes
# Notes: Discovers modules under backend.routes, includes APIRouters named `router` or iterables `routers`.
#        If a module exposes a FastAPI `app`, it mounts it at /<module> as a sub-app.
#        Safe: a failing module won't crash startup; errors are logged.

import os
import logging
import importlib
import pkgutil
from typing import Iterable, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

# Your project database engine
try:
    from backend.database import engine  # expects ENGINE_DATABASE_URL to be configured
except Exception:  # keep startup resilient even if DB import has issues
    engine = None

log = logging.getLogger("uvicorn.error")

app = FastAPI(title="TSF Backend", version=os.getenv("APP_VERSION") or "dev")

# ---------- CORS ----------
env_origins = os.getenv("ALLOWED_ORIGINS", "").strip()
allowed = [o.strip() for o in env_origins.split(",") if o.strip()] or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Router auto-loader ----------
def include_router_obj(obj: Any) -> bool:
    try:
        # APIRouter duck-typing: must have .routes attribute
        if obj is None:
            return False
        if hasattr(obj, "routes"):
            app.include_router(obj)
            return True
        return False
    except Exception as e:
        log.error(f"Failed to include router: {e}")
        return False

def mount_all_route_modules() -> None:
    try:
        routes_pkg = importlib.import_module("backend.routes")
    except Exception as e:
        log.error(f"Cannot import backend.routes: {e}")
        return

    for finder, name, ispkg in pkgutil.iter_modules(routes_pkg.__path__):
        if ispkg or name.startswith(("_", ".")):
            continue
        module_path = f"{routes_pkg.__name__}.{name}"
        try:
            mod = importlib.import_module(module_path)
        except Exception as e:
            log.error(f"Failed to import {module_path}: {e}")
            continue

        mounted = False

        # Prefer a single `router`
        router = getattr(mod, "router", None)
        if router is not None:
            mounted = include_router_obj(router)

        # Support multiple routers via `routers` iterable
        if not mounted:
            routers = getattr(mod, "routers", None)
            if isinstance(routers, Iterable):
                ok_any = False
                for r in routers:
                    ok_any = include_router_obj(r) or ok_any
                mounted = ok_any

        # If a sub-app is provided, mount at /<module>
        if not mounted:
            subapp = getattr(mod, "app", None)
            if subapp is not None:
                try:
                    app.mount(f"/{name}", subapp)
                    mounted = True
                except Exception as e:
                    log.error(f"Failed to mount sub-app from {module_path}: {e}")

        if mounted:
            log.info(f"Mounted routes from {module_path}")
        else:
            log.warning(f"No router/app found to mount in {module_path}")

mount_all_route_modules()

# ---------- Meta endpoints ----------
@app.api_route("/", methods=["GET","HEAD"], tags=["meta"])
def root():
    return {
        "ok": True,
        "service": "tsf-backend",
        "docs": "/docs",
        "health": "/health",
    }

@app.get("/health", tags=["meta"])
def health():
    if engine is None:
        return {"ok": True, "database": "unavailable (engine import failed)"}
    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))
        return {"ok": True, "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- explicit include for arima view (safe even with auto-loader) ---
try:
    from backend.routes import arima as _arima
    app.include_router(_arima.router)
except Exception as _e:
    log.error(f"Failed to include arima routes explicitly: {_e}")

# --- explicit include for hwes view (safe even with auto-loader) ---
try:
    from backend.routes import hwes as _hwes
    app.include_router(_hwes.router)
except Exception as _e:
    log.error(f"Failed to include hwes routes explicitly: {_e}")

# --- explicit include for ses view (safe even with auto-loader) ---
try:
    from backend.routes import ses as _ses
    app.include_router(_ses.router)
except Exception as _e:
    log.error(f"Failed to include ses routes explicitly: {_e}")
