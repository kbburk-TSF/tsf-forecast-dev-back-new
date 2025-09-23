# backend/routes/views_meta_debug.py
# Version: v8.0 (2025-09-23)
# Purpose: /views/meta endpoint using ONLY psycopg (no SQLAlchemy).

from fastapi import APIRouter
from pydantic import BaseModel
import os, traceback
import psycopg
from psycopg.rows import dict_row

router = APIRouter(prefix="/views", tags=["views-meta-debug"])

class MetaResponse(BaseModel):
    ok: bool
    step: str
    details: dict

def _db_url() -> str:
    return (
        os.getenv("ENGINE_DATABASE_URL_DIRECT")
        or os.getenv("ENGINE_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or ""
    )

def _connect():
    return psycopg.connect(_db_url(), autocommit=True)

def _fetch_one(cur, sql, params=None):
    cur.execute(sql, params or ())
    return cur.fetchone()

def _fetch_all(cur, sql, params=None):
    cur.execute(sql, params or ())
    return cur.fetchall()

@router.get("/meta", response_model=MetaResponse)
def get_views_meta():
    dsn = _db_url()
    if not dsn:
        return MetaResponse(ok=False, step="config", details={"error": "No DB URL set"})

    try:
        conn = _connect()
    except Exception as e:
        return MetaResponse(ok=False, step="connect", details={
            "error": str(e),
            "trace": traceback.format_exc(),
            "dsn_present": True
        })

    try:
        with conn, conn.cursor(row_factory=dict_row) as cur:
            # Context
            try:
                ctx = _fetch_one(cur, """
                    select
                      current_user,
                      session_user,
                      current_database() as db,
                      (select current_schema()) as current_schema,
                      (select setting from pg_settings where name='search_path') as search_path
                """)
            except Exception as e:
                return MetaResponse(ok=False, step="context", details={
                    "error": str(e), "trace": traceback.format_exc()
                })

            # Existence + privileges
            try:
                chk = _fetch_one(cur, """
                    select
                      exists (
                        select 1
                        from pg_class c
                        join pg_namespace n on n.oid=c.relnamespace
                        where c.relkind in ('v','m') and n.nspname=%s and c.relname=%s
                      ) as exists,
                      has_schema_privilege(current_user, %s, 'USAGE') as has_usage,
                      has_table_privilege(current_user, %s, 'SELECT') as has_select
                """, ("engine","tsf_vw_daily_best","engine","engine.tsf_vw_daily_best"))
            except Exception as e:
                return MetaResponse(ok=False, step="privileges", details={
                    "error": str(e), "trace": traceback.format_exc(), "context": ctx
                })

            # Smoke test
            try:
                rows = _fetch_all(cur, "select * from engine.tsf_vw_daily_best limit 1")
            except Exception as e:
                return MetaResponse(ok=False, step="smoke", details={
                    "error": str(e), "trace": traceback.format_exc(),
                    "context": ctx, "checks": chk
                })

            return MetaResponse(ok=True, step="meta", details={
                "context": ctx,
                "checks": chk,
                "rows_returned": len(rows)
            })
    finally:
        try:
            conn.close()
        except Exception:
            pass
