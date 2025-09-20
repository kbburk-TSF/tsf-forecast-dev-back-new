
from fastapi import FastAPI, HTTPException, Request, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import text
from backend.database import engine
import os

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

# Mount known routers (best-effort)
def _try_include(path, attr):
    try:
        mod = __import__(path, fromlist=[attr])
        app.include_router(getattr(mod, attr))
        return True
    except Exception:
        return False

_try_include('backend.routes.data', 'router')
_try_include('backend.routes.aggregate', 'router')
_try_include('backend.routes.meta', 'router')
_try_include('backend.routes.classical', 'router')
_ext_forms = _try_include('backend.routes.forms_classical_flow', 'router')
_ext_forms2 = _try_include('backend.routes.forms_raw', 'router')

@app.get('/', tags=['root'])
def root():
    return {'ok': True, 'service': 'tsf-backend', 'forms': {'classical': '/forms/classical'}, 'docs': '/docs', 'health': '/health'}

@app.get('/health', tags=['meta'])
def health():
    try:
        with engine.begin() as conn:
            conn.execute(text('SELECT 1'))
        return {'ok': True, 'database': 'connected'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# INLINE, bulletproof fallback so /forms/classical always exists
INLINE_FORM = '''<!doctype html>
<html><head><meta charset="utf-8"><title>TSF Classical</title></head>
<body style="font-family:system-ui;max-width:720px;margin:40px auto">
<h1>Run Classical Forecast</h1>
<form method="post" action="/forms/classical/run">
  <label>Parameter Name<br><input name="parameter" required placeholder="e.g., CO"></label><br><br>
  <label>State Name<br><input name="state" required placeholder="e.g., California"></label><br><br>
  <button type="submit">Run</button>
</form>
<p>Note: This inline fallback exists so the route is always reachable. The full dropdown version will render if the external router is mounted.</p>
<p><a href="/docs">Open /docs</a></p>
</body></html>'''

@app.get('/forms/classical', response_class=HTMLResponse, tags=['forms'])
def forms_classical_fallback():
    # If external forms router is mounted, let it handle GET via its own template route.
    # Otherwise, serve a minimal fallback form so the route is NOT 404.
    return HTMLResponse(INLINE_FORM)

@app.post('/forms/classical/run', tags=['forms'])
def forms_classical_run_fallback(parameter: str = Form(...), state: str = Form(...)):
    # If external handler is mounted, it will also own this path and take precedence.
    # This fallback only confirms the route exists when external router is missing.
    return JSONResponse({'ok': False, 'detail': 'Fallback handler only. External forms handler not mounted.', 'parameter': parameter, 'state': state}, status_code=501)
