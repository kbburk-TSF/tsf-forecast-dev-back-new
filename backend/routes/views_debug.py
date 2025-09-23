# backend/routes/views_debug.py
# Version: v2.0 (2025-09-23)
# Purpose: Verbose, zero-guessing probes for /views/meta failures (connection, search_path, privileges, sample selects).
# Requirements: psycopg>=3, FastAPI, Pydantic

from fastapi import APIRouter
from pydantic import BaseModel
import os, traceback
import psycopg
from psycopg.rows import dict_row

router = APIRouter(prefix="/views/debug", tags=["views-debug"])

class ProbeResult(BaseModel):
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

def _probe_sql(cur, step, sql, params=None):
    try:
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        return True, ProbeResult(ok=True, step=step, details={"rowcount": len(rows), "rows": rows})
    except Exception as e:
        sqlstate = getattr(e, "sqlstate", None)
        return False, ProbeResult(
            ok=False,
            step=step,
            details={
                "error": str(e),
                "sqlstate": sqlstate,
                "trace": traceback.format_exc(),
            },
        )

@router.get("/connection", response_model=ProbeResult)
def probe_connection():
    try:
        with _connect() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("select 1 as ok")
                row = cur.fetchone()
                return ProbeResult(ok=True, step="connect", details={"select_1": row, "dsn_present": bool(_dsn())})
    except Exception as e:
        return ProbeResult(ok=False, step="connect", details={"error": str(e), "trace": traceback.format_exc(), "dsn_present": bool(_dsn())})

@router.get("/context", response_model=ProbeResult)
def probe_context():
    try:
        with _connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                select
                  current_user,
                  session_user,
                  current_database() as db,
                  (select current_schema()) as current_schema,
                  (select setting from pg_settings where name='search_path') as search_path
            """)
            ctx = cur.fetchone()

            cur.execute("""
                select nspname as schema
                from pg_namespace
                where nspname not like 'pg\\_%' and nspname <> 'information_schema'
                order by 1;
            """)
            schemas = cur.fetchall()

            cur.execute("""
                with t as (
                  select n.nspname as schema, c.relname as view_name, c.relkind
                  from pg_class c
                  join pg_namespace n on n.oid = c.relnamespace
                  where c.relkind in ('v','m') and n.nspname in ('pg_views','engine')
                )
                select * from t order by schema, view_name;
            """)
            views = cur.fetchall()

            return ProbeResult(ok=True, step="context", details={"context": ctx, "schemas": schemas, "views": views})
    except Exception as e:
        return ProbeResult(ok=False, step="context", details={"error": str(e), "trace": traceback.format_exc()})

@router.get("/privs", response_model=ProbeResult)
def probe_privileges():
    target_views = [
        ("pg_views", "vw_daily_best"),
        ("engine",   "vw_daily_best"),
    ]
    try:
        with _connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            checks = []
            for schema, name in target_views:
                cur.execute("""
                  select count(*)>0 as exists
                  from pg_class c
                  join pg_namespace n on n.oid=c.relnamespace
                  where c.relkind in ('v','m') and n.nspname=%s and c.relname=%s
                """, (schema, name))
                exists = cur.fetchone()["exists"]

                cur.execute("select has_schema_privilege(current_user, %s, 'USAGE') as has_usage;", (schema,))
                has_usage = cur.fetchone()["has_usage"]

                cur.execute("select has_table_privilege(current_user, %s, 'SELECT') as has_select;", (f'{schema}.{name}',))
                has_select = cur.fetchone()["has_select"]

                checks.append({
                    "schema": schema,
                    "name": name,
                    "exists": bool(exists),
                    "has_usage": bool(has_usage),
                    "has_select": bool(has_select),
                    "grant_hint": f"GRANT USAGE ON SCHEMA {schema} TO <role>; GRANT SELECT ON {schema}.{name} TO <role>;"
                })

            overall = all(c["exists"] and c["has_usage"] and c["has_select"] for c in checks)
            return ProbeResult(ok=overall, step="privileges", details={"checks": checks})
    except Exception as e:
        return ProbeResult(ok=False, step="privileges", details={"error": str(e), "trace": traceback.format_exc()})

@router.get("/sample", response_model=ProbeResult)
def probe_sample_query():
    tests = [
        ("select_engine_vw", "select * from engine.vw_daily_best limit 1"),
        ("select_pg_views_vw", "select * from pg_views.vw_daily_best limit 1"),
    ]
    try:
        with _connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            for name, sql in tests:
                ok, res = _probe_sql(cur, name, sql)
                if not ok:
                    return res
            return ProbeResult(ok=True, step="sample", details={"message": "All sample queries succeeded", "tests_run": [t[0] for t in tests]})
    except Exception as e:
        return ProbeResult(ok=False, step="sample", details={"error": str(e), "trace": traceback.format_exc()})

@router.get("/diagnose", response_model=ProbeResult)
def probe_diagnose():
    try:
        conn = _connect()
        conn.close()
    except Exception as e:
        return ProbeResult(ok=False, step="connect", details={"error": str(e), "trace": traceback.format_exc(), "dsn_present": bool(_dsn())})

    try:
        with _connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                select
                  current_user,
                  session_user,
                  current_database() as db,
                  (select current_schema()) as current_schema,
                  (select setting from pg_settings where name='search_path') as search_path
            """)
            ctx = cur.fetchone()
    except Exception as e:
        return ProbeResult(ok=False, step="context", details={"error": str(e), "trace": traceback.format_exc()})

    try:
        with _connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            def check(schema, name):
                cur.execute("""
                    select count(*)>0 as exists
                    from pg_class c
                    join pg_namespace n on n.oid=c.relnamespace
                    where c.relkind in ('v','m') and n.nspname=%s and c.relname=%s
                """, (schema, name))
                exists = cur.fetchone()["exists"]
                cur.execute("select has_schema_privilege(current_user, %s, 'USAGE') as has_usage;", (schema,))
                has_usage = cur.fetchone()["has_usage"]
                cur.execute("select has_table_privilege(current_user, %s, 'SELECT') as has_select;", (f'{schema}.{name}',))
                has_select = cur.fetchone()["has_select"]
                return {"schema": schema, "name": name, "exists": bool(exists), "has_usage": bool(has_usage), "has_select": bool(has_select)}

            checks = [check("engine","vw_daily_best"), check("pg_views","vw_daily_best")]
            if not all(c["exists"] and c["has_usage"] and c["has_select"] for c in checks):
                return ProbeResult(ok=False, step="privileges", details={"checks": checks})
    except Exception as e:
        return ProbeResult(ok=False, step="privileges", details={"error": str(e), "trace": traceback.format_exc()})

    try:
        with _connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute("select * from engine.vw_daily_best limit 1")
            _ = cur.fetchall()
    except Exception as e:
        return ProbeResult(ok=False, step="select engine.vw_daily_best", details={"error": str(e), "trace": traceback.format_exc()})
    try:
        with _connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute("select * from pg_views.vw_daily_best limit 1")
            _ = cur.fetchall()
    except Exception as e:
        return ProbeResult(ok=False, step="select pg_views.vw_daily_best", details={"error": str(e), "trace": traceback.format_exc()})

    return ProbeResult(ok=True, step="diagnose", details={"context": ctx, "message": "All checks passed"})
