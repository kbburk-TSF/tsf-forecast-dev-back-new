from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text
def _db_url() -> str:
    url = os.getenv("ENGINE_DATABASE_URL_DIRECT") or os.getenv("ENGINE_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not url:
    raise RuntimeError("ENGINE_DATABASE_URL_DIRECT is not set")
    return url
    import psycopg
    from psycopg.rows import dict_row
    router = APIRouter(prefix="/data", tags=["metadata"])
    DB_SCHEMA_MAP = {
    "air_quality_demo_data": {
    "table": "air_quality_raw",
    "target_col": "Parameter Name",
    "value_col": "Arithmetic Mean",
    "filters": ["State Name", "County Name", "City Name", "CBSA Name"],
    }
    }
def _get_schema(db: str):
    if db not in DB_SCHEMA_MAP:
    raise HTTPException(status_code=404, detail=f"Unknown database {db}")
    return DB_SCHEMA_MAP[db]
    @router.get("/{db}/targets")
def get_targets(db: str):
    meta = _get_schema(db)
    table = f"{db}.{meta['table']}"
    target_col = meta["target_col"]
    sql = f'SELECT DISTINCT "{target_col}" as target FROM {table} ORDER BY "{target_col}"'
    with psycopg.connect(_db_url(), autocommit=True) as conn:
    cur = conn.cursor(row_factory=dict_row)
    return {"targets": [r["target"] for r in rows]}
    @router.get("/{db}/filters")
def get_filters(db: str, target: str = Query(...)):
    meta = _get_schema(db)
    table = f"{db}.{meta['table']}"
    target_col = meta["target_col"]
    filters = {}
    with psycopg.connect(_db_url(), autocommit=True) as conn:
    cur = conn.cursor(row_factory=dict_row)
    with conn.cursor(row_factory=dict_row) as cur:
    for fcol in meta["filters"]:
    sql = f'''
    SELECT DISTINCT "{fcol}" as val
    FROM {table}
    WHERE "{target_col}" = :target
    ORDER BY "{fcol}"
    '''
    vals = cur.execute(text(sql), {"target": target}).scalars().all()
    filters[fcol] = [v for v in vals if v is not None]
    return {"target": target, "filters": filters}