# sitecustomize.py
# Version: v5.0 (2025-09-23)
# Purpose: AUTO-MOUNT debug routers WITHOUT editing existing files and WITHOUT changing start commands.
# How it works: Python imports `sitecustomize` automatically at interpreter startup if present on sys.path.
# We import `backend.main` (your current entrypoint) and attach routers to its `app`.

import importlib
import sys

try:
    _main = importlib.import_module("backend.main")
    app = getattr(_main, "app", None)
    if app is None:
        raise RuntimeError("backend.main has no attribute 'app'")
    # Import routers and mount
    from backend.routes.views_meta_debug import router as views_meta_debug_router
    from backend.routes.views_debug import router as views_debug_router

    # Mount meta debug FIRST to override any existing /views/meta
    app.include_router(views_meta_debug_router)
    app.include_router(views_debug_router)

    # Optional: print to stdout so logs confirm activation
    print("[sitecustomize] Debug routers mounted: /views/meta and /views/debug/*", file=sys.stderr)
except Exception as e:
    # Fail safe: never crash app startup; only log
    print(f"[sitecustomize] Failed to mount debug routers: {e}", file=sys.stderr)
