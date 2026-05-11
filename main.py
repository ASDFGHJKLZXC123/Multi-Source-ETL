"""
Multi-Source ETL Pipeline — Main Orchestrator.

Runs the full pipeline or individual stages via CLI.

Execution modes
---------------
  --full-refresh   Extract → Silver → Gold → Warehouse (complete re-run from APIs)
  --incremental    Silver → Gold → Warehouse (Bronze already fresh; skip extract)
  --stage NAME     Run exactly one named stage

Individual stages
-----------------
  init       Stage 0a — Create PostgreSQL schemas + pipeline_metadata table
  setup      Stage 0b — Download Olist, create source_system schema, load CSVs
  extract    Stage 1  — Pull weather (Open-Meteo) + FX (Frankfurter) + flat files
  load       Stage 2  — Load source DB (alias for setup, kept for clarity)
  silver     Stage 3  — Transform Bronze → Silver (clean, validate, quarantine)
  gold       Stage 4  — Build Gold star schema (Parquet files)
  warehouse  Stage 5  — Load Gold Parquet into PostgreSQL analytics schema
  quality    Stage 7  — Run automated data quality checks; results → data_quality_log

Usage examples
--------------
    python main.py --full-refresh          # full run from API extract to warehouse
    python main.py --incremental           # re-transform + re-load (Bronze intact)
    python main.py --stage extract         # pull external APIs only
    python main.py --stage silver          # re-run Silver transform only
    python main.py --stage warehouse       # re-run warehouse load only
    python main.py --full-refresh --no-fail-fast  # run all stages, collect all errors
"""

from __future__ import annotations

import argparse
import sys
from typing import Callable

from src.utils.logger import logger

# ---------------------------------------------------------------------------
# Stage implementations
# ---------------------------------------------------------------------------


def stage_init() -> None:
    """Stage 0a: Run DDL init scripts — create schemas and pipeline_metadata.

    Safe to run against an empty database before any data arrives.
    All DDL uses IF NOT EXISTS so it is idempotent.
    """
    from src.utils.db import init_schemas

    init_schemas()


def stage_setup() -> None:
    """Stage 0b: Download Olist dataset, create source_system schema, load CSVs."""
    from src.setup.load_source_db import run as run_setup

    run_setup()


def stage_extract() -> None:
    """Stage 1: Extract layer — DB snapshot + API pulls + raw Olist Bronze snapshots."""
    from src.extract.extract_db import extract_all_tables
    from src.extract.extract_api import extract_all_apis
    from src.extract.extract_olist_csvs import snapshot_all as snapshot_raw_olist
    from src.utils.db import get_pipeline_config

    cfg = get_pipeline_config()
    start_date: str = cfg["start_date"]
    end_date: str = cfg["end_date"]

    logger.info("--- Stage 1a: DB snapshot (source_system → Parquet) ---")
    db_results = extract_all_tables()
    logger.info("DB tables extracted: {}/{}", len(db_results), 5)

    logger.info("--- Stage 1b: API extraction (weather + FX) ---")
    api_results = extract_all_apis(
        start_date,
        end_date,
        city_count=int(cfg["weather_city_count"]),
        base=cfg["fx_base_currency"],
        quote=cfg["fx_quote_currency"],
    )
    for source, manifest in api_results.items():
        logger.info("  {} → status={}", source, manifest.get("status", "unknown"))

    logger.info("--- Stage 1c: Raw Olist Bronze snapshots (payments/reviews/geo/translation) ---")
    snapshot_raw_olist()


def stage_load() -> None:
    """Stage 1 alias: Load raw CSVs into source_system (same as setup)."""
    stage_setup()


def stage_silver() -> None:
    """Stage 3: Transform Bronze → Silver (clean, validate, quarantine)."""
    from src.transform.transform_sales import run as run_sales
    from src.transform.transform_weather import run as run_weather
    from src.transform.transform_fx import run as run_fx

    logger.info("--- Stage 3a: Sales Silver transform ---")
    run_sales()

    logger.info("--- Stage 3b: Weather Silver transform ---")
    run_weather()

    logger.info("--- Stage 3c: FX Silver transform ---")
    run_fx()


def stage_gold() -> None:
    """Stage 4: Build the Gold star schema from Silver data.

    Outputs (data/gold/):
      dimensions/ — dim_date, dim_customer, dim_product, dim_store, dim_currency
      facts/      — fact_sales, fact_weather_daily, fact_fx_rates
    """
    from src.transform.build_dimensions import run as run_dimensions
    from src.transform.build_facts import run as run_facts

    logger.info("--- Stage 4a: Building Gold dimension tables ---")
    dims = run_dimensions()
    logger.info(
        "Dimensions complete: {}",
        {name: len(df) for name, df in dims.items()},
    )

    logger.info("--- Stage 4b: Building Gold fact tables ---")
    facts = run_facts()
    logger.info(
        "Facts complete: {}",
        {name: len(df) for name, df in facts.items()},
    )


def stage_warehouse() -> None:
    """Stage 5: Load Gold Parquet files into the PostgreSQL analytics schema.

    Dimensions: truncate-and-reload.
    Facts: INSERT … ON CONFLICT DO UPDATE (idempotent upsert).
    """
    from src.load.load_to_warehouse import run as run_load

    run_load()


def stage_quality(halt_on: str = "CRITICAL") -> None:
    """Stage 7: Run automated data quality checks against the analytics schema.

    Executes at least 5 checks per fact table covering row counts, null checks,
    uniqueness, value ranges, and referential integrity.  Results are persisted
    to ``analytics.data_quality_log`` and a summary table is logged.

    Raises
    ------
    RuntimeError
        When any check at or above *halt_on* severity fails, so the pipeline
        stage records a failure and stops execution.
    """
    from src.quality.runner import run as run_checks

    run_checks(halt_on=halt_on)


# ---------------------------------------------------------------------------
# Stage registry
# ---------------------------------------------------------------------------

#: All named stages available for --stage selection.
STAGES: dict[str, Callable[[], None]] = {
    "init": stage_init,
    "setup": stage_setup,
    "extract": stage_extract,
    "load": stage_load,
    "silver": stage_silver,
    "gold": stage_gold,
    "warehouse": stage_warehouse,
    "quality": stage_quality,
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Multi-Source ETL Pipeline — Olist Brazilian E-Commerce",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--full-refresh",
        action="store_true",
        default=False,
        help=(
            "Run the complete pipeline: extract → silver → gold → warehouse. "
            "Re-pulls all data from external APIs and the source DB."
        ),
    )
    mode_group.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help=(
            "Re-transform and re-load without re-extracting: "
            "silver → gold → warehouse. "
            "Use when Bronze Parquet is already up-to-date."
        ),
    )
    mode_group.add_argument(
        "--stage",
        choices=sorted(STAGES.keys()),
        metavar="STAGE",
        help=(f"Run exactly one stage. Choices: {', '.join(sorted(STAGES))}."),
    )

    parser.add_argument(
        "--no-fail-fast",
        action="store_true",
        default=False,
        help=(
            "Continue running remaining stages even after a failure. "
            "By default the pipeline stops at the first failed stage."
        ),
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point — parse args, build pipeline config, and run.

    Returns
    -------
    int
        Exit code: 0 on success, 1 on any stage failure.
    """
    from src.orchestration.pipeline import (
        PipelineConfig,
        PipelineMode,
        run_pipeline,
    )

    parser = _build_parser()
    args = parser.parse_args(argv)
    fail_fast = not args.no_fail_fast

    # Resolve execution mode
    if args.full_refresh:
        config = PipelineConfig.for_mode(PipelineMode.FULL_REFRESH, fail_fast=fail_fast)
    elif args.incremental:
        config = PipelineConfig.for_mode(PipelineMode.INCREMENTAL, fail_fast=fail_fast)
    elif args.stage:
        config = PipelineConfig.for_mode(
            PipelineMode.SINGLE,
            single_stage=args.stage,
            fail_fast=fail_fast,
        )
    else:
        # Default: full refresh (same as --full-refresh)
        logger.info(
            "No mode flag supplied — defaulting to --full-refresh. "
            "Use --incremental to skip the extract stage."
        )
        config = PipelineConfig.for_mode(PipelineMode.FULL_REFRESH, fail_fast=fail_fast)

    report = run_pipeline(STAGES, config)
    return 0 if report.success else 1


if __name__ == "__main__":
    sys.exit(main())
