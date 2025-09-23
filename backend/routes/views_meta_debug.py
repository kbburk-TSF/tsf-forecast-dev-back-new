# backend/routes/views_meta_debug.py
# Version: v4.0 (2025-09-23)
# Purpose: Hardens /views/meta endpoint to NEVER hang. Returns precise DB errors (sqlstate + traceback) when metadata fails.
# This router mounts at prefix="/views" and defines GET /meta.
# Place BEFORE your existing /views router include so it takes precedence, or temporarily disable the old /views.meta include.

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
    """
    Drop-in replacement for /views/meta that either returns a compact metadata payload,
    or returns ok:false with exact failure info (sqlstate + traceback).
    """
    # 1) Connect
    try:
        conn = _connect()
    except Exception as e:
        return MetaResponse(ok=False, step="connect", details={
            "error": str(e),
            "sqlstate": getattr(e, "sqlstate", None),
            "trace": traceback.format_exc(),
            "dsn_present": bool(_dsn())
        })

    try:
        with conn, conn.cursor(row_factory=dict_row) as cur:
            # 2) Session context (who/where/search_path)
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
                    "error": str(e), "sqlstate": getattr(e,"sqlstate",None), "trace": traceback.format_exc()
                })

            # 3) Confirm target views exist & are readable (engine/ vw_daily_best)
            try:
                # existence + privs checks
                checks = []
                for schema, name in [("engine","vw_daily_best"), ("","vw_daily_best")]:
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
                    """, (schema, name, schema, f"{schema}.{name}"))
                    checks.append({"schema": schema, "name": name, **_})
                # Try minimal selects, capture the first failure
                samples = []
                for lbl, sql in [
                    ("engine_vw", "select * from engine.tsf_vw_daily_best limit 1"),
                    ("_vw", "select * from .vw_daily_best limit 1"),
                ]:
                    try:
                        rows = _fetch_all(cur, sql)
                        samples.append({"test": lbl, "ok": True, "rowcount": len(rows)})
                    except Exception as e:
                        return MetaResponse(ok=False, step=f"select {lbl}", details={
                            "error": str(e), "sqlstate": getattr(e,"sqlstate",None), "trace": traceback.format_exc(),
                            "checks": checks, "context": ctx
                        })
            except Exception as e:
                return MetaResponse(ok=False, step="privileges/existence", details={
                    "error": str(e), "sqlstate": getattr(e,"sqlstate",None), "trace": traceback.format_exc()
                })

            # 4) Minimal dropdown metadata (safe, generic)
            # If your real /views/meta returns more, you can re-add later—this is only to unblock.
            try:
                schemas = _fetch_all(cur, """
                    select nspname as schema
                    from pg_namespace
                    where nspname not like 'pg\_%' and nspname <> 'information_schema'
                    order by 1
                """)
            except Exception as e:
                return MetaResponse(ok=False, step="list_schemas", details={
                    "error": str(e), "sqlstate": getattr(e,"sqlstate",None), "trace": traceback.format_exc()
                })

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
