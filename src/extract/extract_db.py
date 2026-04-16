"""
Stage 2 — Database Extraction (PostgreSQL source_system schema).

Snapshots all five operational tables from the ``source_system`` PostgreSQL
schema into date-stamped Parquet files under ``data/bronze/db/``.

Each table is written to its own subdirectory::

    data/bronze/db/{table_name}/{table_name}_{YYYYMMDD}.parquet

Re-runs on the same calendar day are idempotent: if today's file already
exists the extraction is skipped and the existing path is returned.  Pass
``--force`` (CLI) or ``force=True`` (API) to overwrite unconditionally.

Usage
-----
Run all tables::

    python -m src.extract.extract_db

Run a subset of tables::

    python -m src.extract.extract_db --tables customers orders

Force re-extraction (overwrite today's file)::

    python -m src.extract.extract_db --force

Specify an alternative schema::

    python -m src.extract.extract_db --schema staging --force
"""

from __future__ import annotations

import argparse
import sys
import traceback
from datetime import date
from pathlib import Path

import pandas as pd
import sqlalchemy.exc

from src.extract.config import BRONZE_DB, BRONZE_DB_TABLES, timestamp_suffix  # noqa: F401
from src.utils.db import get_engine
from src.utils.logger import logger

__all__ = ["extract_table", "extract_all_tables", "main"]

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _today_suffix() -> str:
    """Return today's date as ``YYYYMMDD`` string used in output file names."""
    return date.today().strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_table(
    table_name: str,
    schema: str = "source_system",
    force: bool = False,
) -> Path:
    """Extract a single table from PostgreSQL and write it to a Parquet file.

    Parameters
    ----------
    table_name : str
        Name of the table inside *schema*.
    schema : str
        PostgreSQL schema that owns the table.  Defaults to
        ``"source_system"``.
    force : bool
        When ``True``, overwrite today's file even if it already exists.
        Defaults to ``False`` (idempotent — skip if file is present).

    Returns
    -------
    Path
        Absolute path to the written (or pre-existing) Parquet file.

    Raises
    ------
    sqlalchemy.exc.OperationalError
        If a database connection cannot be established.
    Exception
        Any other unexpected error is logged with a full traceback before
        being re-raised so the caller can decide whether to abort.
    """
    table_dir: Path = BRONZE_DB / table_name
    table_dir.mkdir(parents=True, exist_ok=True)

    out_path: Path = table_dir / f"{table_name}_{_today_suffix()}.parquet"

    if out_path.exists() and not force:
        logger.info(
            "Skipping '{}' — today's file already exists: {}",
            table_name,
            out_path,
        )
        return out_path

    logger.info("Extracting table '{}.{}'...", schema, table_name)

    try:
        engine = get_engine()
        df: pd.DataFrame = pd.read_sql_table(table_name, engine, schema=schema)
    except sqlalchemy.exc.OperationalError as exc:
        # Log type + a sanitized summary only — the raw exception repr can
        # include host/port/user and occasionally the password.
        logger.error(
            "Connection error while reading '{}.{}': {} — check DB credentials and connectivity.",
            schema,
            table_name,
            type(exc).__name__,
        )
        raise
    except Exception as exc:
        logger.error(
            "Unexpected error reading '{}.{}': {}\n{}",
            schema,
            table_name,
            exc,
            traceback.format_exc(),
        )
        raise

    # Write atomically: .tmp file first, then rename so a partial write never
    # leaves a corrupt file that would be mistaken for a valid cache hit.
    tmp_path = out_path.with_suffix(".parquet.tmp")
    try:
        df.to_parquet(tmp_path, index=False, engine="pyarrow")
        tmp_path.rename(out_path)
    except Exception as exc:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        logger.error(
            "Failed to write Parquet for '{}': {}\n{}",
            table_name,
            exc,
            traceback.format_exc(),
        )
        raise

    file_kb: float = out_path.stat().st_size / 1024
    logger.info(
        "Extracted '{}': {:,} rows → {} ({:.1f} KB)",
        table_name,
        len(df),
        out_path,
        file_kb,
    )

    return out_path


def extract_all_tables(
    schema: str = "source_system",
    force: bool = False,
) -> dict[str, Path]:
    """Extract all configured tables from PostgreSQL to Parquet.

    Iterates over :data:`BRONZE_DB_TABLES` in order.  A failure on any
    individual table is caught, logged, and skipped so the remaining tables
    are still attempted.

    Parameters
    ----------
    schema : str
        PostgreSQL schema that owns all tables.  Defaults to
        ``"source_system"``.
    force : bool
        When ``True``, overwrite today's files even if they already exist.

    Returns
    -------
    dict[str, Path]
        Mapping of ``table_name -> path`` for every table that was
        successfully extracted (or already present when ``force=False``).
        Tables that raised an exception are omitted from the result.
    """
    results: dict[str, Path] = {}
    total: int = len(BRONZE_DB_TABLES)

    logger.info(
        "Starting DB extraction: {} table(s) from schema '{}' (force={})",
        total,
        schema,
        force,
    )

    for table_name in BRONZE_DB_TABLES:
        try:
            path = extract_table(table_name, schema=schema, force=force)
            results[table_name] = path
        except Exception as exc:
            logger.error(
                "Table '{}' failed — skipping. Reason: {}\n{}",
                table_name,
                exc,
                traceback.format_exc(),
            )

    success_count: int = len(results)
    if success_count == total:
        logger.info(
            "DB extraction complete: {}/{} tables extracted successfully.",
            success_count,
            total,
        )
    else:
        failed_count = total - success_count
        failed_tables = [t for t in BRONZE_DB_TABLES if t not in results]
        logger.warning(
            "DB extraction finished with errors: {}/{} tables succeeded, " "{} failed: {}",
            success_count,
            total,
            failed_count,
            failed_tables,
        )

    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for the DB extractor.

    Parameters
    ----------
    argv : list[str] | None
        Argument list override (defaults to ``sys.argv[1:]``).

    Returns
    -------
    int
        Exit code: 0 on success, 1 if any table failed.
    """
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.extract_db",
        description=("Snapshot source_system PostgreSQL tables into Bronze-layer Parquet files."),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  # Extract all tables\n"
            "  python -m src.extract.extract_db\n\n"
            "  # Extract only customers and orders\n"
            "  python -m src.extract.extract_db --tables customers orders\n\n"
            "  # Force overwrite of today's files for all tables\n"
            "  python -m src.extract.extract_db --force\n\n"
            "  # Force overwrite for a specific table against an alternate schema\n"
            "  python -m src.extract.extract_db --tables products --schema staging --force\n"
        ),
    )

    parser.add_argument(
        "--tables",
        nargs="*",
        default=None,
        metavar="TABLE",
        help=(
            "One or more table names to extract.  "
            "Defaults to all tables defined in BRONZE_DB_TABLES: "
            f"{BRONZE_DB_TABLES}."
        ),
    )
    parser.add_argument(
        "--schema",
        default="source_system",
        help="PostgreSQL schema to read from (default: source_system).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite today's Parquet files even if they already exist.",
    )

    args = parser.parse_args(argv)

    # Resolve which tables to process
    tables_to_run: list[str] = args.tables if args.tables else BRONZE_DB_TABLES

    # Validate requested table names against the known list
    unknown = [t for t in tables_to_run if t not in BRONZE_DB_TABLES]
    if unknown:
        logger.error(
            "Unknown table(s) requested: {}.  Valid tables: {}",
            unknown,
            BRONZE_DB_TABLES,
        )
        sys.exit(1)

    if tables_to_run == BRONZE_DB_TABLES:
        # Full run — use the batching helper for uniform summary logging
        extracted = extract_all_tables(schema=args.schema, force=args.force)
        failed_count = len(tables_to_run) - len(extracted)
    else:
        # Partial run — call extract_table individually
        extracted: dict[str, Path] = {}
        failed_count = 0
        for tbl in tables_to_run:
            try:
                path = extract_table(tbl, schema=args.schema, force=args.force)
                extracted[tbl] = path
            except Exception as exc:
                logger.error(
                    "Table '{}' failed: {}\n{}",
                    tbl,
                    exc,
                    traceback.format_exc(),
                )
                failed_count += 1

        success = len(extracted)
        total = len(tables_to_run)
        if failed_count == 0:
            logger.info(
                "DB extraction complete: {}/{} tables extracted successfully.",
                success,
                total,
            )
        else:
            logger.warning(
                "DB extraction finished with errors: {}/{} tables succeeded.",
                success,
                total,
            )

    return 1 if failed_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
