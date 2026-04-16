"""
Database connection utilities for the ETL pipeline.

Provides:
  - get_engine()      : SQLAlchemy engine (connection pooling, ORM-compatible)
  - get_connection()  : Context manager for raw psycopg2 connections

Both helpers read credentials from the project .env file via python-dotenv.
"""

import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote_plus

import psycopg2
import psycopg2.extensions
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.utils.logger import logger

# ---------------------------------------------------------------------------
# Load .env from the project root (two levels up from this file).
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = _PROJECT_ROOT / ".env"

load_dotenv(dotenv_path=_ENV_FILE)


def _build_dsn() -> str:
    """Construct the PostgreSQL DSN from environment variables.

    Returns
    -------
    str
        A SQLAlchemy-compatible connection string of the form
        ``postgresql+psycopg2://user:pass@host:port/dbname``.

    Raises
    ------
    EnvironmentError
        If any required environment variable is missing.
    """
    required_vars = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD")
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        raise OSError(
            f"Missing required environment variable(s): {', '.join(missing)}. "
            f"Copy .env.example to .env and fill in your credentials."
        )

    host = os.environ["DB_HOST"]
    port = os.environ["DB_PORT"]
    name = os.environ["DB_NAME"]
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]

    return f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{name}"


def get_engine(pool_size: int = 5, max_overflow: int = 10) -> Engine:
    """Return a SQLAlchemy engine backed by a connection pool.

    The engine is created fresh on each call; callers that need a long-lived
    engine should cache the result themselves.

    Parameters
    ----------
    pool_size : int
        Number of persistent connections to keep in the pool.
    max_overflow : int
        Extra connections allowed beyond *pool_size* under load.

    Returns
    -------
    sqlalchemy.engine.Engine
    """
    dsn = _build_dsn()
    engine = create_engine(
        dsn,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,  # validate connections before use
        echo=False,
    )
    logger.debug(
        "SQLAlchemy engine created (pool_size={}, max_overflow={})", pool_size, max_overflow
    )
    return engine


@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Context manager that yields a raw psycopg2 connection.

    The connection is committed on clean exit and rolled back on exception.
    It is always closed when the ``with`` block exits.

    Yields
    ------
    psycopg2.extensions.connection

    Example
    -------
    >>> with get_connection() as conn:
    ...     with conn.cursor() as cur:
    ...         cur.execute("SELECT 1")
    """
    # Validate before accessing env vars so the error message is helpful
    required_vars = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD")
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        raise OSError(
            f"Missing required environment variable(s): {', '.join(missing)}. "
            "Copy .env.example to .env and fill in your credentials."
        )

    host = os.environ["DB_HOST"]
    port = int(os.environ["DB_PORT"])
    name = os.environ["DB_NAME"]
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]

    conn: psycopg2.extensions.connection = psycopg2.connect(
        host=host,
        port=port,
        dbname=name,
        user=user,
        password=password,
    )
    try:
        yield conn
        conn.commit()
        logger.debug("psycopg2 connection committed successfully")
    except Exception:
        conn.rollback()
        logger.warning("psycopg2 connection rolled back due to exception")
        raise
    finally:
        conn.close()
        logger.debug("psycopg2 connection closed")


def test_connection() -> bool:
    """Smoke-test the database connection.

    Returns
    -------
    bool
        True if the connection succeeds, False otherwise.
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection test passed")
        return True
    except Exception as exc:
        logger.error("Database connection test failed: {}", exc)
        return False


def init_schemas() -> None:
    """Execute all DDL init scripts in sql/ddl/ to create schemas and base tables.

    Globs ``sql/ddl/0*.sql`` and runs scripts in lexicographic (numeric) order:
      00_init.sql              — schemas + pipeline_metadata (canonical source)
      01_schemas.sql           — schema stubs (safe no-ops after 00_init.sql)
      02_pipeline_metadata.sql — table reference (safe no-op after 00_init.sql)

    All statements use ``IF NOT EXISTS`` so this function is fully idempotent.
    Runs inside a single ``get_connection()`` transaction — if any script fails,
    the entire batch is rolled back.
    """
    sql_dir = _PROJECT_ROOT / "sql" / "ddl"
    scripts = sorted(sql_dir.glob("0*.sql"))  # runs 01_, 02_, etc. in order

    if not scripts:
        logger.warning("No DDL scripts found in {}", sql_dir)
        return

    with get_connection() as conn:
        with conn.cursor() as cur:
            for script_path in scripts:
                sql = script_path.read_text()
                cur.execute(sql)
                logger.info("Executed DDL script: {}", script_path.name)

    logger.info("Schema initialisation complete ({} scripts run)", len(scripts))


def get_pipeline_config() -> dict[str, str]:
    """Return pipeline configuration from environment variables.

    Returns a dict with keys:
      - weather_provider : 'open-meteo' or 'openweathermap'
      - fx_provider      : 'frankfurter' or 'exchangeratehost'
      - start_date       : ISO date string
      - end_date         : ISO date string
      - log_level        : logging level string
    """
    return {
        "weather_provider": os.getenv("WEATHER_PROVIDER", "open-meteo"),
        "fx_provider": os.getenv("FX_PROVIDER", "frankfurter"),
        "start_date": os.getenv("PIPELINE_START_DATE", "2016-09-01"),
        "end_date": os.getenv("PIPELINE_END_DATE", "2018-10-31"),
        "weather_city_count": int(os.getenv("WEATHER_CITY_COUNT", "20")),
        "fx_base_currency": os.getenv("FX_BASE_CURRENCY", "USD"),
        "fx_quote_currency": os.getenv("FX_QUOTE_CURRENCY", "BRL"),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
    }


__all__ = ["get_engine", "get_connection", "test_connection", "init_schemas", "get_pipeline_config"]
