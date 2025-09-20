import os
from sqlalchemy import create_engine

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL env var is required.")

url = DATABASE_URL.strip()

# Normalize driver so SQLAlchemy can import the right DBAPI.
# Prefer psycopg3 if available in requirements; otherwise fall back to psycopg2.
if url.startswith("postgres://"):
    # Heroku-style URLs; SQLAlchemy expects postgresql://
    url = "postgresql://" + url[len("postgres://"):]

if url.startswith("postgresql://") and "+psycopg" not in url and "+psycopg2" not in url:
    # Explicitly choose psycopg3; if that fails at runtime, switch to psycopg2.
    try:
        import psycopg  # noqa: F401
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    except Exception:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)

engine = create_engine(url, pool_pre_ping=True)
