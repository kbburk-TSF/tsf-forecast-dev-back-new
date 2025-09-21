
# forms_upload_historical.py
# FastAPI router providing an HTML form and uploader that inserts CSV rows
# directly into engine.staging_historical with strict column matching.

import os
import io
import csv
from typing import List, Optional

from fastapi import APIRouter, UploadFile, Form
from fastapi.responses import HTMLResponse, PlainTextResponse

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

router = APIRouter()

EXPECTED_COLUMNS: List[str] = [
    "forecast_id",
    "forecast_name",
    "DATE",
    "VALUE",
    "SES-M",
    "SES-Q",
    "HWES-M",
    "HWES-Q",
    "ARIMA-M",
    "ARIMA-Q",
]

TABLE_SCHEMA = "engine"
TABLE_NAME = "staging_historical"

def _db_url() -> str:
    # Prefer direct engine DB URL
    url = os.getenv("ENGINE_DATABASE_URL_DIRECT") or os.getenv("ENGINE_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("ENGINE_DATABASE_URL_DIRECT is not set")
    return url

@router.get("/forms/upload-historical", response_class=HTMLResponse)
def upload_form() -> str:
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Upload to {TABLE_SCHEMA}.{TABLE_NAME}</title>
<style>
 body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,sans-serif;margin:24px}}
 code,pre{{font-family:ui-monospace,Menlo,Consolas,monospace}}
 button,input[type="submit"]{{padding:6px 12px}}
</style>
</head><body>
<h3>Upload to <code>{TABLE_SCHEMA}.{TABLE_NAME}</code></h3>
<p>The CSV must contain this header exactly, in this order:</p>
<pre>{",".join(EXPECTED_COLUMNS)}</pre>
<form action="/forms/upload-historical" method="post" enctype="multipart/form-data">
  <input name="file" type="file" accept=".csv" required />
  <input type="submit" value="Upload" />
</form>
</body></html>"""

@router.post("/forms/upload-historical", response_class=PlainTextResponse)
async def upload_csv(file: UploadFile):
    # Read file
    raw = await file.read()
    buf = io.StringIO(raw.decode("utf-8-sig"))  # handle BOM if present
    reader = csv.DictReader(buf)

    # Validate header strictly
    header = reader.fieldnames or []
    if header != EXPECTED_COLUMNS:
        return PlainTextResponse(
            f"state=error inserted=0 total=0 error=Header mismatch\n"
            f"expected={EXPECTED_COLUMNS}\n"
            f"found={header}",
            status_code=400,
        )

    rows = list(reader)
    total = len(rows)

    if total == 0:
        return PlainTextResponse("state=ok inserted=0 total=0 note=empty file")

    # Prepare rows in same order as EXPECTED_COLUMNS; blank -> None
    def norm(v: Optional[str]):
        if v is None: 
            return None
        v = v.strip()
        return None if v == "" else v

    data = [tuple(norm(r[col]) for col in EXPECTED_COLUMNS) for r in rows]

    # Compose INSERT with fully qualified, quoted identifiers to support hyphens
    cols_ident = [sql.Identifier(c) for c in EXPECTED_COLUMNS]
    insert_stmt = sql.SQL("INSERT INTO {schema}.{table} ({cols}) VALUES %s").format(
        schema=sql.Identifier(TABLE_SCHEMA),
        table=sql.Identifier(TABLE_NAME),
        cols=sql.SQL(", ").join(cols_ident),
    )

    # Execute in one batch
    try:
        with psycopg2.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_stmt.as_string(conn), data, page_size=1000)
        inserted = total
        return PlainTextResponse(f"state=ok inserted={inserted} total={total}")
    except Exception as e:
        return PlainTextResponse(f"state=error inserted=0 total={total} error={type(e)} {e}")
