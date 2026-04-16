"""
Data quality runner for the Multi-Source ETL pipeline — Stage 7.

Orchestrates the full quality check lifecycle:

1. Execute all fact-table check suites via ``run_all_checks``.
2. Log a formatted summary table to the console via loguru.
3. Persist every ``CheckResult`` to ``analytics.data_quality_log``.
4. Evaluate whether the pipeline should halt based on severity thresholds.

Entry points:

- ``run_quality_checks()``   — programmatic API, returns ``(results, should_halt)``.
- ``run()``                  — pipeline-stage entry point; raises ``RuntimeError``
                               when critical failures are found.
- ``main()``                 — CLI entry point for ``python -m src.quality.runner``.

Usage examples::

    python -m src.quality.runner                   # halt on CRITICAL (default)
    python -m src.quality.runner --halt-on WARNING  # halt on WARNING or higher
    python -m src.quality.runner --no-halt          # run all checks, never raise
"""

from __future__ import annotations

import argparse
import json
import sys

import psycopg2.extras

from src.quality.checks import CheckResult, run_all_checks
from src.utils.db import get_connection, get_engine
from src.utils.logger import logger

# ---------------------------------------------------------------------------
# Severity ordering — used by evaluate_halt
# ---------------------------------------------------------------------------
_SEVERITY_ORDER: dict[str, int] = {"INFO": 0, "WARNING": 1, "CRITICAL": 2}

# Column widths for the log_summary table
_COL_CHECK = 38
_COL_TABLE = 26
_COL_SEV = 8
_COL_STATUS = 6
_DIVIDER = "─" * (_COL_CHECK + _COL_TABLE + _COL_SEV + _COL_STATUS + 6)


# ---------------------------------------------------------------------------
# persist_results
# ---------------------------------------------------------------------------


def persist_results(results: list[CheckResult]) -> None:
    """Insert all ``CheckResult`` objects into ``analytics.data_quality_log``.

    Uses ``psycopg2.extras.execute_values`` for a single efficient batch
    insert.  The ``check_metadata`` column receives JSON-serialised metadata.

    Args:
        results: List of ``CheckResult`` instances to persist.

    Raises:
        Exception: Propagates any psycopg2 error after logging it.
    """
    if not results:
        logger.warning("persist_results called with an empty results list — nothing to insert")
        return

    insert_sql = """
        INSERT INTO analytics.data_quality_log
            (check_name, table_name, check_category, severity, status,
             expected_value, actual_value, rows_affected, message, check_metadata)
        VALUES %s
    """

    rows = [
        (
            r.check_name,
            r.table_name,
            r.category,
            r.severity,
            r.status,
            r.expected_value,
            r.actual_value,
            r.rows_affected,
            r.message,
            json.dumps(r.metadata) if r.metadata else None,
        )
        for r in results
    ]

    logger.debug("Persisting {} check result(s) to analytics.data_quality_log", len(rows))

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, insert_sql, rows)
        logger.info("Persisted {} check result(s) to analytics.data_quality_log", len(rows))
    except Exception as exc:
        logger.error("Failed to persist data quality results: {}", exc)
        raise


# ---------------------------------------------------------------------------
# log_summary
# ---------------------------------------------------------------------------


def log_summary(results: list[CheckResult]) -> None:
    """Emit a formatted summary table of all check results via loguru.

    The table is printed at INFO level and includes one row per check result
    plus a counts footer.  Column widths are fixed for readability.

    Args:
        results: List of ``CheckResult`` instances to summarise.
    """
    header = (
        f"{'Check':<{_COL_CHECK}}  "
        f"{'Table':<{_COL_TABLE}}  "
        f"{'Sev':<{_COL_SEV}}  "
        f"{'Status':<{_COL_STATUS}}"
    )

    lines: list[str] = [_DIVIDER, header, _DIVIDER]

    for r in results:
        sev_abbr = r.severity[:4]  # CRIT / WARN / INFO
        lines.append(
            f"{r.check_name:<{_COL_CHECK}}  "
            f"{r.table_name:<{_COL_TABLE}}  "
            f"{sev_abbr:<{_COL_SEV}}  "
            f"{r.status:<{_COL_STATUS}}"
        )

    lines.append(_DIVIDER)

    pass_count = sum(1 for r in results if r.status == "PASS")
    fail_count = sum(1 for r in results if r.status == "FAIL")
    warn_fails = sum(
        1 for r in results if r.status == "FAIL" and r.severity == "WARNING"
    )
    crit_fails = sum(
        1 for r in results if r.status == "FAIL" and r.severity == "CRITICAL"
    )

    lines.append(
        f"Summary: {pass_count} PASS  |  {fail_count} FAIL  "
        f"|  {warn_fails} WARNING  |  {crit_fails} CRITICAL"
    )
    lines.append(_DIVIDER)

    for line in lines:
        logger.info("{}", line)


# ---------------------------------------------------------------------------
# evaluate_halt
# ---------------------------------------------------------------------------


def evaluate_halt(
    results: list[CheckResult],
    halt_on: str = "CRITICAL",
) -> bool:
    """Return ``True`` when the pipeline should halt based on check results.

    A halt is triggered when at least one result has ``status == 'FAIL'``
    AND its severity is at or above the *halt_on* threshold.

    Severity order (ascending): INFO < WARNING < CRITICAL

    Args:
        results: List of ``CheckResult`` instances to evaluate.
        halt_on: Minimum severity level that triggers a halt.  One of
            ``'INFO'``, ``'WARNING'``, or ``'CRITICAL'``.

    Returns:
        ``True`` if the pipeline should halt, ``False`` otherwise.

    Raises:
        KeyError: If *halt_on* is not a recognised severity string.
    """
    threshold = _SEVERITY_ORDER[halt_on]
    return any(
        r.status == "FAIL" and _SEVERITY_ORDER.get(r.severity, 0) >= threshold
        for r in results
    )


# ---------------------------------------------------------------------------
# run_quality_checks — primary programmatic API
# ---------------------------------------------------------------------------


def run_quality_checks(
    halt_on: str = "CRITICAL",
) -> tuple[list[CheckResult], bool]:
    """Execute the full data quality check pipeline.

    Steps:

    1. Create a lightweight SQLAlchemy engine (pool_size=2).
    2. Run all fact-table check suites via ``run_all_checks``.
    3. Emit a formatted summary table to the log.
    4. Persist all results to ``analytics.data_quality_log``.
    5. Evaluate the halt condition.

    Args:
        halt_on: Minimum failure severity that should trigger a halt.
            One of ``'INFO'``, ``'WARNING'``, or ``'CRITICAL'`` (default).

    Returns:
        A tuple ``(results, should_halt)`` where *results* is the full list
        of ``CheckResult`` objects and *should_halt* is ``True`` when at
        least one failing check meets or exceeds the *halt_on* threshold.
    """
    logger.info("Stage 7 — Data Quality Framework starting (halt_on={})", halt_on)

    engine = get_engine(pool_size=2, max_overflow=0)
    results = run_all_checks(engine)

    log_summary(results)
    persist_results(results)

    should_halt = evaluate_halt(results, halt_on)

    if should_halt:
        failed_names = [r.check_name for r in results if r.status == "FAIL"]
        logger.warning(
            "Halt condition met (halt_on={}). Failed checks: {}",
            halt_on,
            failed_names,
        )
    else:
        logger.info("No halt condition triggered (halt_on={})", halt_on)

    return results, should_halt


# ---------------------------------------------------------------------------
# run — pipeline stage entry point
# ---------------------------------------------------------------------------


def run(halt_on: str = "CRITICAL") -> None:
    """Pipeline stage entry point — raises on quality failures.

    Calls ``run_quality_checks`` and raises ``RuntimeError`` if the halt
    condition is triggered.  Intended to be called from the main pipeline
    orchestrator as Stage 7.

    Args:
        halt_on: Minimum failure severity that should trigger a halt.

    Raises:
        RuntimeError: When at least one failing check meets or exceeds the
            *halt_on* severity threshold.
    """
    results, should_halt = run_quality_checks(halt_on=halt_on)
    if should_halt:
        failed = [r.check_name for r in results if r.status == "FAIL"]
        raise RuntimeError(
            f"Data quality checks failed (halt_on={halt_on!r}). "
            f"Failed checks: {failed}. See analytics.data_quality_log for details."
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point for the data quality runner.

    Usage::

        python -m src.quality.runner                     # halt on CRITICAL
        python -m src.quality.runner --halt-on WARNING   # halt on WARNING or higher
        python -m src.quality.runner --no-halt           # run all checks, never raise

    Args:
        argv: Argument list (defaults to ``sys.argv[1:]`` when ``None``).

    Returns:
        Exit code: ``0`` on success or when ``--no-halt`` is set, ``1`` when
        a halt condition is triggered.
    """
    parser = argparse.ArgumentParser(
        prog="python -m src.quality.runner",
        description="Run Stage 7 data quality checks for the Multi-Source ETL pipeline.",
    )
    parser.add_argument(
        "--halt-on",
        choices=["INFO", "WARNING", "CRITICAL"],
        default="CRITICAL",
        metavar="SEVERITY",
        help=(
            "Minimum failure severity that causes a non-zero exit. "
            "Choices: INFO, WARNING, CRITICAL (default: CRITICAL)."
        ),
    )
    parser.add_argument(
        "--no-halt",
        action="store_true",
        default=False,
        help="Run all checks but never raise or exit non-zero, regardless of failures.",
    )

    args = parser.parse_args(argv)

    if args.no_halt:
        logger.info("--no-halt flag set — pipeline will not halt on any failure")
        results, _ = run_quality_checks(halt_on="CRITICAL")
        return 0

    try:
        run(halt_on=args.halt_on)
        return 0
    except RuntimeError as exc:
        logger.error("{}", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["persist_results", "evaluate_halt", "run_quality_checks", "run"]
