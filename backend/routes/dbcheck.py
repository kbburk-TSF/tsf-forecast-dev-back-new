# backend/routes/dbcheck.py
# Version: v1.0 (2025-09-23)
# Purpose: Minimal connectivity test endpoint for database.

from fastapi import APIRouter
import os, traceback
import psycopg

router = APIRouter(prefix="/views", tags=["dbcheck"])

def _db_url() -> str:
    return (
        os.getenv("ENGINE_DATABASE_URL_DIRECT")
        or os.getenv("ENGINE_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or ""
    )

@router.get("/dbcheck")
def dbcheck():
    dsn = _db_url()
    if not dsn:
        return {"ok": False, "error": "No database URL configured"}
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                row = cur.fetchone()
                return {"ok": True, "result": row[0] if row else None}
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "trace": traceback.format_exc(),
            "dsn_present": True,
        }
