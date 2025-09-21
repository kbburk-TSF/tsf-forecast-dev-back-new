# backend/main_upload_debug_entry.py
# Wrapper that uses your existing FastAPI app and registers the new routes.
import importlib
from fastapi import FastAPI

# Try to reuse the project's existing app
app = None
for modname in ("backend.main", "main"):
    try:
        m = importlib.import_module(modname)
        if hasattr(m, "app") and isinstance(m.app, FastAPI):
            app = m.app
            break
    except Exception:
        pass

# If not found, create a minimal app so this can still boot.
if app is None:
    app = FastAPI(title="TSF Backend (upload+debug wrapper)")

# Include the routes
from backend.routes import forms_upload_historical as _u
from backend.routes import debug_engine_db as _d
app.include_router(_u.router)
app.include_router(_d.router)
