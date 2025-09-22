# backend/routes/engine_kickoff_diag.py
import os
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import psycopg2

router = APIRouter()

def _dsn() -> str:
    dsn = os.getenv("ENGINE_DATABASE_URL_DIRECT") or os.getenv("ENGINE_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("No ENGINE_DATABASE_URL set")
    return dsn

def _connect():
    return psycopg2.connect(_dsn())

@router.get("/forms/engine-kickoff/ping", tags=["diag"])
def ping():
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user")
            db, usr = cur.fetchone()
            cur.execute("SELECT to_regprocedure('engine.manual_kickoff_by_id(uuid)')")
            present = cur.fetchone()[0] is not None
        return JSONResponse(content=jsonable_encoder({"ok": True, "db": db, "user": usr, "kickoff_present": present}))
    except Exception as e:
        return JSONResponse(content=jsonable_encoder({"ok": False, "error": str(e)}), status_code=500)

@router.get("/forms/engine-kickoff/force", tags=["diag"])
def force(forecast_id: str = Query(...)):
    """Directly invoke the kickoff function via GET to isolate JS/form issues."""
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT engine.manual_kickoff_by_id(%s::uuid)", (forecast_id,))
            run_id = cur.fetchone()[0]
        return JSONResponse(content=jsonable_encoder({"ok": True, "run_id": run_id, "forecast_id": forecast_id}))
    except Exception as e:
        return JSONResponse(content=jsonable_encoder({"ok": False, "error": str(e)}), status_code=500)
