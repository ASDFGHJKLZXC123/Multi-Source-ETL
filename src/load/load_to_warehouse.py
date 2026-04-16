"""
Stage 5 — PostgreSQL Warehouse Loader
======================================

Loading strategy
----------------
**Dimensions** use a *truncate-and-reload* pattern:
  - FK trigger checks are disabled for the session via
    ``SET session_replication_role = 'replica'`` before the TRUNCATE so that
    fact rows already in the warehouse do not block the operation.
  - The table is truncated with ``RESTART IDENTITY`` and all rows from the
    Gold Parquet file are bulk-inserted fresh on every run.
  - ``session_replication_role`` is reset to ``'origin'`` immediately after
    the TRUNCATE (before the pandas ``to_sql`` insert) so normal constraint
    enforcement resumes for the insert phase.

**Facts** use an *upsert* pattern (no TRUNCATE):
  - The Gold Parquet is written to a temporary staging table in the
    ``public`` schema (``public._stg_<fact>``).
  - An ``INSERT … ON CONFLICT (pk_cols) DO UPDATE`` statement merges the
    staging data into the permanent ``analytics.<fact>`` table, making the
    loader fully idempotent — re-running it never produces duplicate rows.
  - The staging table is always dropped in a ``finally`` block so transient
    tables cannot accumulate from failed runs.

Both strategies stamp every loaded row with a ``_loaded_at`` UTC timestamp
that is set once per ``load_all()`` invocation, giving every row in the
warehouse a consistent batch timestamp.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy.engine import Engine

from src.transform.gold_utils import GOLD_DIMS_DIR, GOLD_FACTS_DIR
from src.utils.db import get_connection, get_engine
from src.utils.logger import logger

# ---------------------------------------------------------------------------
# Table registry
# ---------------------------------------------------------------------------

# (parquet_stem, analytics_table_name)
_DIM_TABLES: list[tuple[str, str]] = [
    ("dim_date", "dim_date"),
    ("dim_customer", "dim_customer"),
    ("dim_product", "dim_product"),
    ("dim_store", "dim_store"),
    ("dim_currency", "dim_currency"),
]

# (parquet_stem, analytics_table_name, pk_columns_list)
_FACT_TABLES: list[tuple[str, str, list[str]]] = [
    ("fact_sales", "fact_sales", ["order_item_id"]),
    ("fact_weather_daily", "fact_weather_daily", ["date_key", "city", "state"]),
    ("fact_fx_rates", "fact_fx_rates", ["date_key", "base_currency_key", "quote_currency_key"]),
]


# ---------------------------------------------------------------------------
# Dimension loader
# ---------------------------------------------------------------------------


def load_dimension(
    engine: Engine,
    parquet_path: Path,
    table: str,
    loaded_at: datetime,
) -> int:
    """Truncate an analytics dimension table and reload it from a Parquet file.

    The session replication role is set to ``'replica'`` to bypass FK trigger
    checks during the TRUNCATE, then immediately reset to ``'origin'`` before
    the bulk insert so constraint enforcement applies to the new rows.

    Args:
        engine: SQLAlchemy engine used for the pandas ``to_sql`` bulk insert.
        parquet_path: Absolute path to the Gold dimension Parquet file.
        table: Target table name inside the ``analytics`` schema.
        loaded_at: UTC timestamp to stamp on every loaded row.

    Returns:
        Number of rows inserted.

    Raises:
        FileNotFoundError: If *parquet_path* does not exist (caller should
            catch this and decide whether to skip or abort).
        Exception: Any database error propagates to the caller.
    """
    df: pd.DataFrame = pd.read_parquet(parquet_path)
    df["_loaded_at"] = loaded_at

    logger.debug(
        "Truncating analytics.{} and reloading {:,} rows from '{}'",
        table,
        len(df),
        parquet_path.name,
    )

    # Disable FK trigger checks, truncate, re-enable — single connection/txn.
    # session_replication_role is a session-level setting; it resets automatically
    # when the connection is closed, so no finally block is needed. Running
    # SET ORIGIN inside a finally block would mask the real TRUNCATE error if the
    # transaction were already aborted.
    # CASCADE handles any FK references from fact tables loaded in prior runs.
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SET session_replication_role = 'replica'")
            cur.execute(f"TRUNCATE analytics.{table} RESTART IDENTITY CASCADE")
            cur.execute("SET session_replication_role = 'origin'")

    # Bulk insert via SQLAlchemy (separate connection from the pool).
    df.to_sql(
        table,
        engine,
        schema="analytics",
        if_exists="append",
        index=False,
        chunksize=10_000,
        method="multi",
    )

    logger.info("Loaded dimension analytics.{}: {:,} rows", table, len(df))
    return len(df)


# ---------------------------------------------------------------------------
# Fact loader
# ---------------------------------------------------------------------------


def load_fact(
    engine: Engine,
    parquet_path: Path,
    table: str,
    pk_cols: list[str],
    loaded_at: datetime,
) -> int:
    """Upsert a Gold fact Parquet file into an analytics fact table.

    Rows are written to a temporary staging table in ``public`` schema and
    then merged into ``analytics.<table>`` via ``INSERT … ON CONFLICT … DO
    UPDATE``.  The staging table is always dropped afterwards.

    Args:
        engine: SQLAlchemy engine used for staging ``to_sql`` and the upsert.
        parquet_path: Absolute path to the Gold fact Parquet file.
        table: Target table name inside the ``analytics`` schema.
        pk_cols: Column name(s) that form the primary key / conflict target.
        loaded_at: UTC timestamp to stamp on every loaded row.

    Returns:
        Number of rows in the source Parquet (not necessarily net-new rows).

    Raises:
        FileNotFoundError: If *parquet_path* does not exist.
        Exception: Any database error propagates to the caller.
    """
    df: pd.DataFrame = pd.read_parquet(parquet_path)
    df["_loaded_at"] = loaded_at

    # PostgreSQL ON CONFLICT never matches rows where any PK column is NULL
    # (NULLs are never equal in a unique constraint).  Filter them out now so
    # re-runs do not silently insert duplicate rows instead of updating them.
    null_pk_mask: pd.Series = df[pk_cols].isna().any(axis=1)
    null_pk_count: int = int(null_pk_mask.sum())
    if null_pk_count > 0:
        logger.warning(
            "load_fact analytics.{}: dropping {:,} row(s) with NULL in pk column(s) {} "
            "— these cannot participate in ON CONFLICT matching",
            table,
            null_pk_count,
            pk_cols,
        )
        df = df.loc[~null_pk_mask].reset_index(drop=True)

    staging_table = f"_stg_{table}"

    logger.debug(
        "Writing {:,} rows to staging table public.{} for upsert into analytics.{}",
        len(df),
        staging_table,
        table,
    )

    # Write to public staging table (drop+recreate each run).
    df.to_sql(
        staging_table,
        engine,
        schema="public",
        if_exists="replace",
        index=False,
        chunksize=10_000,
        method="multi",
    )

    all_cols: list[str] = list(df.columns)
    non_pk_cols: list[str] = [c for c in all_cols if c not in pk_cols]

    # Build quoted column lists for the SQL statement.
    cols_sql = ", ".join(f'"{c}"' for c in all_cols)
    pk_conflict_sql = ", ".join(f'"{c}"' for c in pk_cols)
    update_sql = ",\n        ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_pk_cols)

    upsert_sql = (
        f'INSERT INTO analytics."{table}" ({cols_sql})\n'
        f'SELECT {cols_sql} FROM public."{staging_table}"\n'
        f"ON CONFLICT ({pk_conflict_sql}) DO UPDATE SET\n"
        f"        {update_sql}"
    )

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_sql)
                logger.info(
                    "Upserted analytics.{}: {:,} source rows (pk: {})",
                    table,
                    len(df),
                    pk_conflict_sql,
                )
    finally:
        # Always clean up the staging table, even on upsert failure.
        _drop_staging_table(engine, staging_table)

    return len(df)


def _drop_staging_table(engine: Engine, staging_table: str) -> None:
    """Drop the public staging table, suppressing any errors.

    This is called from a ``finally`` block so it must never raise.

    Args:
        engine: SQLAlchemy engine (used only to obtain a raw connection here).
        staging_table: Unqualified table name inside the ``public`` schema.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{staging_table}"')
        logger.debug("Dropped staging table public.{}", staging_table)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Could not drop staging table public.{}: {} — continuing",
            staging_table,
            exc,
        )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def load_all(
    dims_dir: Path | None = None,
    facts_dir: Path | None = None,
) -> dict[str, int]:
    """Load all dimension and fact tables from Gold Parquet into PostgreSQL.

    Dimensions are truncated-and-reloaded in registry order; facts are
    upserted.  A missing Parquet file is logged as a WARNING and that table
    is skipped — other tables continue loading.  Any database exception is
    logged at ERROR and re-raised immediately so the overall run fails fast.

    Args:
        dims_dir: Override for the Gold dimensions directory.  Defaults to
            ``GOLD_DIMS_DIR`` from ``src.transform.gold_utils``.
        facts_dir: Override for the Gold facts directory.  Defaults to
            ``GOLD_FACTS_DIR`` from ``src.transform.gold_utils``.

    Returns:
        Mapping of ``{table_name: row_count}`` for every table successfully
        loaded.  Tables that were skipped (missing Parquet) are absent.
    """
    dims_dir = Path(dims_dir) if dims_dir is not None else GOLD_DIMS_DIR
    facts_dir = Path(facts_dir) if facts_dir is not None else GOLD_FACTS_DIR

    engine: Engine = get_engine(pool_size=2, max_overflow=0)
    loaded_at: datetime = datetime.now(timezone.utc)
    results: dict[str, int] = {}

    logger.info(
        "Starting warehouse load — loaded_at={} | dims_dir={} | facts_dir={}",
        loaded_at.isoformat(),
        dims_dir,
        facts_dir,
    )

    # ---- Dimensions --------------------------------------------------------
    for parquet_stem, table in _DIM_TABLES:
        parquet_path = dims_dir / f"{parquet_stem}.parquet"
        if not parquet_path.exists():
            logger.warning(
                "Dimension Parquet not found, skipping analytics.{}: '{}'",
                table,
                parquet_path,
            )
            continue
        try:
            row_count = load_dimension(engine, parquet_path, table, loaded_at)
            results[table] = row_count
        except Exception as exc:
            logger.error("Failed to load dimension analytics.{}: {}", table, exc)
            raise

    # ---- Facts -------------------------------------------------------------
    for parquet_stem, table, pk_cols in _FACT_TABLES:
        parquet_path = facts_dir / f"{parquet_stem}.parquet"
        if not parquet_path.exists():
            logger.warning(
                "Fact Parquet not found, skipping analytics.{}: '{}'",
                table,
                parquet_path,
            )
            continue
        try:
            row_count = load_fact(engine, parquet_path, table, pk_cols, loaded_at)
            results[table] = row_count
        except Exception as exc:
            logger.error("Failed to load fact analytics.{}: {}", table, exc)
            raise

    # ---- Summary -----------------------------------------------------------
    _log_summary(results)
    return results


def _log_summary(results: dict[str, int]) -> None:
    """Emit a formatted INFO summary of row counts for each loaded table.

    Args:
        results: Mapping of table name to row count returned by ``load_all``.
    """
    if not results:
        logger.info("No tables were loaded.")
        return

    col_width = max(len(k) for k in results) + 2
    header = f"{'Table':<{col_width}}  {'Rows':>12}"
    separator = "-" * len(header)
    lines = [separator, header, separator]
    total = 0
    for table, count in results.items():
        lines.append(f"{table:<{col_width}}  {count:>12,}")
        total += count
    lines.append(separator)
    lines.append(f"{'TOTAL':<{col_width}}  {total:>12,}")
    lines.append(separator)
    logger.info("Warehouse load summary:\n{}", "\n".join(lines))


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def run() -> None:
    """Run the full warehouse load with default paths.

    This is the zero-argument entry point intended for use from pipeline
    orchestrators (e.g. ``pipeline_runner.py``) that chain stages together.
    """
    logger.info("Stage 5 — Warehouse loader starting")
    load_all()
    logger.info("Stage 5 — Warehouse loader complete")


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for the warehouse loader.

    Supports optional ``--dims-dir`` and ``--facts-dir`` overrides so that
    integration tests or one-off backfills can point at a custom Gold directory
    without modifying source code.

    Args:
        argv: Argument list (defaults to ``sys.argv[1:]`` when ``None``).

    Returns:
        Exit code — ``0`` on success, ``1`` on failure.
    """
    parser = argparse.ArgumentParser(
        description="Stage 5: Load Gold Parquet files into PostgreSQL analytics schema.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--dims-dir",
        type=Path,
        default=None,
        metavar="PATH",
        help="Override directory containing Gold dimension Parquet files.",
    )
    parser.add_argument(
        "--facts-dir",
        type=Path,
        default=None,
        metavar="PATH",
        help="Override directory containing Gold fact Parquet files.",
    )
    args = parser.parse_args(argv)

    logger.info("Stage 5 — Warehouse loader starting")
    try:
        load_all(dims_dir=args.dims_dir, facts_dir=args.facts_dir)
    except Exception as exc:
        logger.error("Warehouse loader failed: {}", exc)
        return 1

    logger.info("Stage 5 — Warehouse loader complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["load_all", "run"]
