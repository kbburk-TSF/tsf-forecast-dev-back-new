from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Core routers already in your project
from backend.routes.data import router as data_router
from backend.routes.aggregate import router as agg_router
from backend.routes.meta import router as meta_router
from backend.routes.classical import router as classical_router

# Forms router is optional; include if present, otherwise ignore
try:
    from backend.routes.forms_classical import router as forms_classical_router  # type: ignore
    _HAS_FORMS = True
except Exception:
    forms_classical_router = None
    _HAS_FORMS = False

app = FastAPI(title="TSF Backend", version="CLASSICAL-STABLE")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Root and health — so hitting base URL returns 200 instead of 404
@app.get("/", include_in_schema=False)
def root():
    return {"service": "TSF Backend", "status": "ok"}

@app.get("/healthz", include_in_schema=False)
def healthz():
    return {"ok": True}

# Mount existing routers (unchanged)
app.include_router(data_router)
app.include_router(agg_router)
app.include_router(meta_router)
app.include_router(classical_router)

# Mount forms if available
if _HAS_FORMS and forms_classical_router is not None:
    app.include_router(forms_classical_router, prefix="/forms", tags=["forms"])