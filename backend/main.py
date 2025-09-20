# TSF BACKEND MAIN (FORMS_ROUTER_MOUNTED_v2)
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.routes.data import router as data_router
from backend.routes.aggregate import router as agg_router
from backend.routes.meta import router as meta_router
from backend.routes.classical import router as classical_router
from backend.routes.forms_classical import router as forms_classical_router  # <- ensures /forms/classical exists

app = FastAPI(title="TSF Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", include_in_schema=False)
def root():
    return {"service": "TSF Backend", "status": "ok"}

@app.get("/healthz", include_in_schema=False)
def healthz():
    return {"ok": True}

# Existing routers
app.include_router(data_router)
app.include_router(agg_router)
app.include_router(meta_router)
app.include_router(classical_router)

# REQUIRED: Forms router (inline HTML, no templates)
app.include_router(forms_classical_router, prefix="/forms", tags=["forms"])
