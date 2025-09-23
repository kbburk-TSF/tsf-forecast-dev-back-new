import os
from typing import Dict, List

from fastapi import APIRouter, HTTPException, Query
import psycopg
from psycopg.rows import tuple_row

router = APIRouter(prefix="/data", tags=["metadata"])

# Map of known data sources to their tables and columns
DB_SCHEMA_MAP: Dict[str, Dict[str, object]] = {
    "air_quality_demo_data": {
        "table": "air_quality_raw",
        "target_col": "Parameter Name",
        "value_col": "Arithmetic Mean",
        "filters": ["State Name", "County Name", "City Name", "CBSA Name"],
    },
}

def _db_url() -> str:
    """
    Resolve the database URL from environment variables.
    Keep the original precedence used elsewhere in the codebase.
    """
    url = (
        os.getenv("ENGINE_DATABASE_URL_DIRECT")
        or os.getenv("ENGINE_DATABASE_URL")
        or os.getenv("DATABASE_URL")
    )
    if not url:
        raise RuntimeError("ENGINE_DATABASE_URL_DIRECT is not set")
    return url

@router.get("/filters")
def get_filters(
    db: str = Query(..., description="Key in DB_SCHEMA_MAP, e.g. 'air_quality_demo_data'"),
    target: str = Query(..., description="Target/parameter name to filter on"),
) -> Dict[str, object]:
    """
    Return distinct values for each configured filter column given a target.
    """
    if db not in DB_SCHEMA_MAP:
        raise HTTPException(status_code=400, detail=f"Unknown db '{db}'")

    meta = DB_SCHEMA_MAP[db]
    table = meta["table"]
    target_col = meta["target_col"]
    filters: Dict[str, List[str]] = {}

    # Build and execute queries safely with parameters. Identifiers are whitelisted from the map.
    sql_template = 'SELECT DISTINCT "{fcol}" AS val FROM {table} WHERE "{tcol}" = %(target)s ORDER BY "{fcol}"'

    dsn = _db_url()
    # autocommit=True to avoid transaction overhead for simple reads
    with psycopg.connect(dsn, autocommit=True, row_factory=tuple_row) as conn:
        with conn.cursor() as cur:
            for fcol in meta["filters"]:
                sql = sql_template.format(fcol=fcol, table=table, tcol=target_col)
                cur.execute(sql, {"target": target})
                vals = [row[0] for row in cur.fetchall() if row and row[0] is not None]
                filters[fcol] = vals

    return {"target": target, "filters": filters}
