# backend/routes/views_meta_debug.py
# Version: v5.0 (2025-09-23)
# Purpose: Hardens /views/meta endpoint to NEVER hang. Returns precise DB errors when metadata fails.

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
    url = os.getenv("ENGINE_DATABASE_URL_DIRECT") or os.getenv("ENGINE_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("ENGINE_DATABASE_URL_DIRECT is not set")
    return url

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
    try:
        conn = _connect()
    except Exception as e:
        return MetaResponse(ok=False, step="connect", details={
            "error": str(e),
            "sqlstate": getattr(e, "sqlstate", None),
            "trace": traceback.format_exc(),
            "dsn_present": True
        })

    try:
        with conn, conn.cursor(row_factory=dict_row) as cur:
            # Context info
            ctx = _fetch_one(cur, """
                select
                  current_user,
                  session_user,
                  current_database() as db,
                  (select current_schema()) as current_schema,
                  (select setting from pg_settings where name='search_path') as search_path
            ")

            # Existence + privs check for engine.tsf_vw_daily_best
            _ = _fetch_one(cur, """
                select
                  exists (
                    select 1
                    from pg_class c
                    join pg_namespace n on n.oid=c.relnamespace
                    where c.relkind in ('v','m') and n.nspname=%s and c.relname=%s
                  ) as exists,
                  has_schema_privilege(current_user, %s, 'USAGE') as has_usage,
                  has_table_privilege(current_user, %s, 'SELECT') as has_select
            """, ("engine", "tsf_vw_daily_best", "engine", "engine.tsf_vw_daily_best"))
            checks = [{"schema": "engine", "name": "tsf_vw_daily_best", **_}]

            # Smoke test select
            samples = []
            try:
                rows = _fetch_all(cur, "select * from engine.tsf_vw_daily_best limit 1")
                samples.append({"test": "engine.tsf_vw_daily_best", "ok": True, "rowcount": len(rows)})
            except Exception as e:
                return MetaResponse(ok=False, step="select engine.tsf_vw_daily_best", details={
                    "error": str(e), "sqlstate": getattr(e,"sqlstate",None), "trace": traceback.format_exc(),
                    "checks": checks, "context": ctx
                })

            schemas = _fetch_all(cur, """
                select nspname as schema
                from pg_namespace
                where nspname not like 'pg\\_%' and nspname <> 'information_schema'
                order by 1
            ")

            return MetaResponse(ok=True, step="meta", details={
                "context": ctx,
                "schemas": schemas,
                "view_smoke_tests": samples
            })
    finally:
        try:
            conn.close()
        except Exception:
            pass
