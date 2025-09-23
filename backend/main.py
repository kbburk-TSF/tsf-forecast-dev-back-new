
from fastapi import FastAPI, HTTPException, Request, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import text
from backend.database import engine
import os, logging

log = logging.getLogger("uvicorn.error")

app = FastAPI(title="TSF Backend", version=os.getenv("APP_VERSION") or "dev")

# CORS
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

# Try to mount routers (won't crash on failure)
safe_include("backend.routes.data", "router")
safe_include("backend.routes.aggregate", "router")
safe_include("backend.routes.meta", "router")
safe_include("backend.routes.classical", "router")
forms_mounted = safe_include("backend.routes.forms_classical_flow", "router") or safe_include("backend.routes.forms_raw", "router")

# NEW: mount upload-historical routes (uses ENGINE_DATABASE_URL / ENGINE_DB_SCHEMA)
safe_include("backend.routes.forms_upload_historical", "router")

# NEW: Views API + Form (TSF_ENGINE_APP only)
safe_include("backend.routes.views", "router")

@app.get("/", tags=["root"])
def root():
    return {"ok": True, "service": "tsf-backend", "forms": {"classical": "/forms/classical", "upload": "/forms/upload-historical"}, "docs": "/docs", "health": "/health"}

@app.get("/health", tags=["meta"])
def health():
    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))
        return {"ok": True, "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

INLINE_FORM = '''
<!doctype html><html><head><meta charset="utf-8"><title>TSF Classical</title></head>
<body style="font-family:system-ui;max-width:720px;margin:40px auto">
<h1>Run Classical Forecast</h1>
<form method="post" action="/forms/classical/run">
  <label>Parameter Name<br><input name="parameter" required placeholder="e.g., CO"></label><br><br>
  <label>State Name<br><input name="state" required placeholder="e.g., California"></label><br><br>
  <button type="submit">Run</button>
</form>
<p>This is a fallback so the route never 404s. If your full form is mounted, it will take precedence.</p>
</body></html>
'''

@app.get("/forms/classical", response_class=HTMLResponse, tags=["forms"])
def classical_get():
    return HTMLResponse(INLINE_FORM)

@app.post("/forms/classical/run", tags=["forms"])
def classical_post(parameter: str = Form(...), state: str = Form(...)):
    return JSONResponse({"ok": False, "detail": "Fallback only. Forms router not mounted.", "parameter": parameter, "state": state}, status_code=501)
