# TSF Backend v1

FastAPI backend for Render + Neon Postgres.

## Deploy on Render
**Start command:**
```
uvicorn backend.main:app --host 0.0.0.0 --port $PORT
```

**Build command:**
```
pip install -r requirements.txt
```

**Environment variables:**
- `DATABASE_URL` = `postgresql+psycopg://USER:PASSWORD@HOST/DB?sslmode=require`
- `PYTHON_VERSION` = `3.10.12`
- `PYTHONUNBUFFERED` = `1`

## Create table in Neon
Run this SQL (also in `sql/air_quality.sql`):

```sql
-- Air Quality raw table
CREATE TABLE IF NOT EXISTS public.air_quality_raw (
    id BIGSERIAL PRIMARY KEY,
    date_local DATE NOT NULL,
    parameter_name TEXT NOT NULL,
    arithmetic_mean DOUBLE PRECISION NOT NULL,
    local_site_name TEXT,
    state_name TEXT NOT NULL,
    county_name TEXT,
    city_name TEXT,
    cbsa_name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Helpful query indexes (no UNIQUE constraints)
CREATE INDEX IF NOT EXISTS ix_air_quality_date ON public.air_quality_raw (date_local);
CREATE INDEX IF NOT EXISTS ix_air_quality_state_param_date ON public.air_quality_raw (state_name, parameter_name, date_local);
```

## Endpoints
- `GET /health` → `{"status":"ok"}`
- `GET /version` → repo version string
- `POST /upload/air_quality?on_conflict=ignore|fail` → CSV upload
- `GET /data/air_quality/last?limit=50` → last rows
- `GET /aggregate/state_daily?state=TX&parameter=CO&agg=mean|sum`
- `GET /forecast/state_daily?state=TX&parameter=CO&h=30&method=seasonal_naive_dow|ewma`

## CSV headers (flexible case/underscores)
- Date Local
- Parameter Name
- Arithmetic Mean
- Local Site Name
- State Name
- County Name
- City Name
- CBSA Name
