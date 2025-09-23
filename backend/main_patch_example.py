# backend/main_patch_example.py
# Version: v3.0 (2025-09-23)
# Example of how to mount the debug router INSIDE your existing backend/main.py.
#
# Copy ONLY the two lines marked "ADD THIS" into your real backend/main.py
# after your `app = FastAPI(...)` is created.

from fastapi import FastAPI

app = FastAPI()

# --- ADD THIS (2 lines) ---
from backend.routes.views_debug import router as views_debug_router
app.include_router(views_debug_router)
# --- /ADD THIS ---

# ... keep the rest of your existing routes/middleware here ...
