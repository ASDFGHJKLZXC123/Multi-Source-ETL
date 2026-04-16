#!/usr/bin/env python3
"""
Stage 0 connectivity smoke test.

Verifies that Python can reach the PostgreSQL instance, read back
server information, and confirm both ETL schemas are present.

Usage
-----
    python scripts/test_db_connection.py

Exit codes
----------
    0  — all checks passed
    1  — one or more checks failed
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to sys.path so src.* imports work without pip install -e
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from sqlalchemy import text

# ── Check results accumulator ───────────────────────────────────────────────
_CHECKS: list[tuple[str, bool, str]] = []  # (name, passed, detail)


def _check(name: str, passed: bool, detail: str = "") -> None:
    status = "PASS" if passed else "FAIL"
    symbol = "✓" if passed else "✗"
    print(f"  {symbol} [{status}] {name}" + (f" — {detail}" if detail else ""))
    _CHECKS.append((name, passed, detail))


def check_env_vars() -> None:
    """Verify all required environment variables are present."""
    import os
    required = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD")
    missing = [v for v in required if not os.getenv(v)]
    _check(
        "Environment variables",
        not missing,
        f"missing: {missing}" if missing else "all present",
    )


def check_sqlalchemy_connect() -> None:
    """Verify SQLAlchemy engine can open a connection."""
    try:
        from src.utils.db import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 AS ping")).scalar()
        _check("SQLAlchemy connection", result == 1, "SELECT 1 returned OK")
    except Exception as exc:
        _check("SQLAlchemy connection", False, str(exc))


def check_pg_version() -> None:
    """Log the PostgreSQL server version."""
    try:
        from src.utils.db import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version()")).scalar()
        _check("PostgreSQL version", True, version.split(",")[0])
    except Exception as exc:
        _check("PostgreSQL version", False, str(exc))


def check_schemas() -> None:
    """Verify source_system and analytics schemas exist."""
    try:
        from src.utils.db import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            count = conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.schemata "
                "WHERE schema_name IN ('source_system', 'analytics')"
            )).scalar()
        found = int(count)
        schemas_ok = found == 2
        _check(
            "ETL schemas present",
            schemas_ok,
            f"found {found}/2 (source_system, analytics)" if not schemas_ok
            else "source_system + analytics both present",
        )
    except Exception as exc:
        _check("ETL schemas present", False, str(exc))


def check_pipeline_metadata_table() -> None:
    """Verify the pipeline_metadata table exists in the analytics schema."""
    try:
        from src.utils.db import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            exists = conn.execute(text(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.tables "
                "  WHERE table_schema = 'analytics' "
                "  AND table_name = 'pipeline_metadata'"
                ")"
            )).scalar()
        _check(
            "pipeline_metadata table",
            bool(exists),
            "exists" if exists else "not found — run: python main.py --stage setup",
        )
    except Exception as exc:
        _check("pipeline_metadata table", False, str(exc))


def check_psycopg2_direct() -> None:
    """Verify raw psycopg2 connection works (bypasses SQLAlchemy pool)."""
    try:
        from src.utils.db import get_connection
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(), current_user")
                db_name, user = cur.fetchone()
        _check("psycopg2 direct connection", True, f"db={db_name}, user={user}")
    except Exception as exc:
        _check("psycopg2 direct connection", False, str(exc))


def main() -> int:
    print()
    print("=" * 60)
    print("  Stage 0 — PostgreSQL Connectivity Smoke Test")
    print("=" * 60)
    print()

    check_env_vars()
    check_sqlalchemy_connect()
    check_pg_version()
    check_psycopg2_direct()
    check_schemas()
    check_pipeline_metadata_table()

    print()
    passed = sum(1 for _, ok, _ in _CHECKS if ok)
    total = len(_CHECKS)
    all_ok = passed == total

    if all_ok:
        print(f"  All {total}/{total} checks passed — Stage 0 environment ready.")
    else:
        failed_names = [name for name, ok, _ in _CHECKS if not ok]
        print(f"  {passed}/{total} checks passed. FAILED: {failed_names}")
        print()
        print("  Troubleshooting:")
        print("    1. Is PostgreSQL running?  →  docker compose ps")
        print("    2. Is .env configured?     →  cp .env.example .env && edit .env")
        print("    3. Are schemas created?    →  python main.py --stage setup")

    print()
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
