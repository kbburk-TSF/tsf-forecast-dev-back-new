# backend/routes/debug_engine_db.py
import os
from fastapi import APIRouter
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

router = APIRouter()

def probe(url: str):
    if not url:
        return {"ok": False, "error": "URL not set"}
    try:
        eng = create_engine(url, pool_pre_ping=True, future=True)
        with eng.begin() as conn:
            db = conn.exec_driver_sql("select current_database()").scalar()
            sp = conn.exec_driver_sql("show search_path").scalar()
            ver = conn.exec_driver_sql("show server_version").scalar()
            exists = conn.exec_driver_sql("""
                select exists (
                    select 1 from information_schema.tables
                    where table_schema='engine' and table_name='staging_historical'
                )""").scalar()
        return {"ok": True, "current_database": db, "search_path": sp, "server_version": ver,
                "engine.staging_historical_exists": bool(exists), "error": None}
    except SQLAlchemyError as e:
        return {"ok": False, "error": str(e.__cause__ or e)}

@router.get("/debug/engine-db")
def debug_engine_db():
    return {
        "ENGINE_DATABASE_URL": probe(os.getenv("ENGINE_DATABASE_URL")),
        "DATABASE_URL": probe(os.getenv("DATABASE_URL")),
    }
