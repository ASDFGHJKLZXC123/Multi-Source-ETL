"""
Pytest version of the Stage 0 connectivity checks.
Requires a live PostgreSQL instance — skip if DB_HOST is not set.
"""
import os
import pytest
from pathlib import Path

# Skip the entire module if no DB config is present
pytestmark = pytest.mark.skipif(
    not os.getenv("DB_HOST"),
    reason="DB_HOST not set — skipping live DB tests",
)


def test_sqlalchemy_connection():
    from src.utils.db import get_engine
    from sqlalchemy import text
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).scalar()
    assert result == 1


def test_psycopg2_connection():
    from src.utils.db import get_connection
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            db_name = cur.fetchone()[0]
    assert db_name == os.environ.get("DB_NAME", "etl_pipeline")


def test_schemas_exist():
    from src.utils.db import get_engine
    from sqlalchemy import text
    engine = get_engine()
    with engine.connect() as conn:
        count = conn.execute(text(
            "SELECT COUNT(*) FROM information_schema.schemata "
            "WHERE schema_name IN ('source_system', 'analytics')"
        )).scalar()
    assert int(count) == 2, "Both ETL schemas must exist — run init_schemas() first"
