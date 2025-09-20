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
