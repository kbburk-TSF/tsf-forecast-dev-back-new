import os
import re
from typing import Dict, Any, List

from fastapi import APIRouter
import psycopg
from psycopg.rows import dict_row

router = APIRouter(prefix="/views", tags=["views"])

SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def _dsn() -> str:
    raw = (
        os.getenv("ENGINE_DATABASE_URL_DIRECT")
        or os.getenv("ENGINE_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or ""
    )
    dsn = raw.strip()
    if not dsn:
        raise RuntimeError("Database URL not configured (ENGINE_DATABASE_URL_DIRECT/ENGINE_DATABASE_URL/DATABASE_URL)")
    return dsn

def _valid_ident(s: str) -> bool:
    return bool(SAFE_IDENT.match(s or ""))

def check(schema: str, name: str) -> Dict[str, Any]:
    info: Dict[str, Any] = {"schema": schema, "name": name, "exists": False, "has_select": None, "error": None}

    if not (_valid_ident(schema) and _valid_ident(name)):
        info["error"] = "invalid identifiers"
        return info

    qual = f"{schema}.{name}"
    try:
        with psycopg.connect(_dsn(), autocommit=True, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass(%s) AS oid", (qual,))
                row = cur.fetchone()
                oid = row["oid"] if row else None
                if not oid:
                    info["exists"] = False
                    return info

                info["exists"] = True

                cur.execute("SELECT has_table_privilege(%s::regclass, 'SELECT') AS has_select", (qual,))
                row = cur.fetchone()
                info["has_select"] = bool(row["has_select"]) if row and "has_select" in row else False

        return info
    except Exception as e:
        info["error"] = str(e)
        return info

@router.get("/diagnose")
def probe_diagnose() -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = [
        check("engine", "tsf_vw_full"),
    ]
    ok = all(c.get("exists") and c.get("has_select") for c in checks if c.get("error") is None)
    return {"ok": ok, "step": "privileges", "details": {"checks": checks}}
