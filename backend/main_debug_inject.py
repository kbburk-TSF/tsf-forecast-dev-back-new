# backend/main_debug_inject.py
# Version: v2.0 (2025-09-23)
# Purpose: Non-invasive shim that imports your existing FastAPI app and mounts the debug router without editing backend/main.py.
# Usage:
#   uvicorn backend.main_debug_inject:app --host 0.0.0.0 --port 8000
# Assumes your canonical app is exposed as `app` in `backend.main`.

from importlib import import_module

# Import your existing app from backend.main
_main = import_module("backend.main")
app = getattr(_main, "app")

# Now include the debug router
from backend.routes.views_debug import router as views_debug_router
app.include_router(views_debug_router)
