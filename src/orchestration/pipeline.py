"""
src/orchestration/pipeline.py
------------------------------
Pipeline execution core for the Multi-Source ETL pipeline.

This module is intentionally decoupled from individual stage implementations.
It receives a stage registry (``dict[str, Callable]``) and a sequence of stage
names from the caller (``main.py``), so the orchestration logic is reusable
with any set of stages.

Prefect upgrade path
--------------------
The design mirrors Prefect's mental model so that upgrading later requires only
decorator additions, not structural rewrites:

    Current Python object      →  Prefect equivalent
    ─────────────────────────     ──────────────────────
    PipelineConfig             →  @flow parameters
    run_pipeline()             →  @flow
    _execute_stage()           →  @task (one per stage)
    StageResult                →  task return type
    PipelineReport             →  flow return type

To upgrade, wrap ``_execute_stage`` with ``@task`` and ``run_pipeline`` with
``@flow``.  ``PipelineConfig`` can be passed as-is to the flow function.

Execution modes
---------------
FULL_REFRESH  — Re-extract all data from APIs/DB, then transform and load.
                Stage sequence: extract → silver → gold → warehouse.

INCREMENTAL   — Bronze Parquet already fresh; only re-transform and re-load.
                Stage sequence: silver → gold → warehouse.
                Use when APIs/DB have not changed since the last extract run.

SINGLE        — Run exactly one named stage (used by ``--stage`` CLI flag).

Failure behaviour
-----------------
By default ``fail_fast=True``: the pipeline stops at the first stage failure
and returns a ``PipelineReport`` with ``success=False``.  Downstream stages
are recorded as SKIPPED so the report is always complete.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from src.utils.logger import logger

# ---------------------------------------------------------------------------
# Stage sequence constants
# ---------------------------------------------------------------------------

#: Stages run for --full-refresh: re-extract from APIs/DB, then transform/load/check.
FULL_REFRESH_STAGES: list[str] = ["extract", "silver", "gold", "warehouse", "quality"]

#: Stages run for --incremental: skip extract, re-transform, re-load, then check.
INCREMENTAL_STAGES: list[str] = ["silver", "gold", "warehouse", "quality"]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

class PipelineMode(str, Enum):
    """Execution mode controlling which stages are run."""
    FULL_REFRESH = "full-refresh"
    INCREMENTAL  = "incremental"
    SINGLE       = "single"


@dataclass
class PipelineConfig:
    """Runtime configuration for a single pipeline execution.

    Attributes
    ----------
    mode:
        Which stage sequence to execute.
    stage_sequence:
        Ordered list of stage names to run.  Populated automatically from
        *mode* by ``PipelineConfig.for_mode()``; override explicitly only
        when using ``PipelineMode.SINGLE``.
    fail_fast:
        Stop at the first stage failure (default ``True``).  Set to
        ``False`` to run all stages and collect all failures.
    """
    mode:           PipelineMode
    stage_sequence: list[str]
    fail_fast:      bool = True

    @classmethod
    def for_mode(
        cls,
        mode: PipelineMode,
        single_stage: str | None = None,
        fail_fast: bool = True,
    ) -> "PipelineConfig":
        """Construct a config for a named execution mode.

        Parameters
        ----------
        mode:
            Desired execution mode.
        single_stage:
            Required when *mode* is ``SINGLE``; the name of the one stage
            to execute.
        fail_fast:
            Passed through to the resulting config.
        """
        if mode is PipelineMode.FULL_REFRESH:
            sequence = list(FULL_REFRESH_STAGES)
        elif mode is PipelineMode.INCREMENTAL:
            sequence = list(INCREMENTAL_STAGES)
        elif mode is PipelineMode.SINGLE:
            if not single_stage:
                raise ValueError(
                    "single_stage must be provided when mode=SINGLE"
                )
            sequence = [single_stage]
        else:
            raise ValueError(f"Unknown PipelineMode: {mode!r}")

        return cls(mode=mode, stage_sequence=sequence, fail_fast=fail_fast)


@dataclass
class StageResult:
    """Result of executing one pipeline stage.

    Attributes
    ----------
    name:       Stage name (matches the key in the stage registry).
    status:     ``"success"`` | ``"failed"`` | ``"skipped"``.
    elapsed_s:  Wall-clock seconds consumed by the stage.
    error:      Exception instance if *status* is ``"failed"``; else ``None``.
    metadata:   Optional dict for stage-specific output (row counts, etc.).
    """
    name:      str
    status:    str
    elapsed_s: float
    error:     BaseException | None          = None
    metadata:  dict[str, Any]               = field(default_factory=dict)


@dataclass
class PipelineReport:
    """Full report returned by ``run_pipeline()``.

    Attributes
    ----------
    mode:           Execution mode used for this run.
    results:        Ordered list of ``StageResult`` objects.
    total_elapsed_s: Wall-clock seconds for the whole pipeline.
    success:         ``True`` only when every stage completed without error.
    """
    mode:             PipelineMode
    results:          list[StageResult]
    total_elapsed_s:  float
    success:          bool

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    @property
    def failed_stages(self) -> list[StageResult]:
        return [r for r in self.results if r.status == "failed"]

    @property
    def skipped_stages(self) -> list[StageResult]:
        return [r for r in self.results if r.status == "skipped"]

    def summary_table(self) -> str:
        """Return a human-readable timing table for all stages.

        Example output::

            ─────────────────────────────────────────────
            Pipeline run  │  mode=full-refresh
            ─────────────────────────────────────────────
            Stage          Status     Elapsed
            ─────────────────────────────────────────────
            extract        SUCCESS     42.3s
            silver         SUCCESS     18.7s
            gold           SUCCESS      6.1s
            warehouse      SUCCESS      9.4s
            ─────────────────────────────────────────────
            TOTAL                      76.5s  ✓
            ─────────────────────────────────────────────
        """
        col_stage   = max(len(r.name) for r in self.results) + 2
        col_status  = 10
        col_elapsed = 10

        bar = "─" * (col_stage + col_status + col_elapsed + 4)
        header = (
            f"{'Stage':<{col_stage}}"
            f"{'Status':<{col_status}}"
            f"{'Elapsed':>{col_elapsed}}"
        )

        rows = [
            bar,
            f"Pipeline run  │  mode={self.mode.value}",
            bar,
            header,
            bar,
        ]
        for r in self.results:
            status_label = r.status.upper()
            elapsed_str  = f"{r.elapsed_s:.1f}s"
            rows.append(
                f"{r.name:<{col_stage}}"
                f"{status_label:<{col_status}}"
                f"{elapsed_str:>{col_elapsed}}"
            )

        total_str = f"{self.total_elapsed_s:.1f}s"
        outcome   = "✓" if self.success else "✗"
        rows.append(bar)
        rows.append(
            f"{'TOTAL':<{col_stage}}"
            f"{'':<{col_status}}"
            f"{total_str:>{col_elapsed}}"
            f"  {outcome}"
        )
        rows.append(bar)
        return "\n".join(rows)


# ---------------------------------------------------------------------------
# Internal: single-stage executor
# ---------------------------------------------------------------------------

def _execute_stage(
    name: str,
    fn: Callable[[], None],
) -> StageResult:
    """Run *fn* for stage *name* and return a ``StageResult``.

    This function is the natural unit to annotate with ``@task`` when
    upgrading to Prefect.

    Parameters
    ----------
    name:   Stage name (for logging and result recording).
    fn:     Zero-argument callable that runs the stage.

    Returns
    -------
    StageResult
        Status is ``"success"`` on clean return, ``"failed"`` on any
        exception.  Elapsed time is always recorded.
    """
    logger.info("▶  Stage [{}] starting", name)
    t_start = time.perf_counter()
    try:
        fn()
        elapsed = time.perf_counter() - t_start
        logger.info("✔  Stage [{}] completed in {:.1f}s", name, elapsed)
        return StageResult(name=name, status="success", elapsed_s=elapsed)
    except Exception as exc:  # noqa: BLE001
        elapsed = time.perf_counter() - t_start
        logger.error("✘  Stage [{}] FAILED after {:.1f}s: {}", name, elapsed, exc)
        return StageResult(
            name=name, status="failed", elapsed_s=elapsed, error=exc
        )


# ---------------------------------------------------------------------------
# Public: pipeline runner
# ---------------------------------------------------------------------------

def run_pipeline(
    stage_registry: dict[str, Callable[[], None]],
    config: PipelineConfig,
) -> PipelineReport:
    """Execute the pipeline according to *config*.

    This function is the natural unit to annotate with ``@flow`` when
    upgrading to Prefect.

    Parameters
    ----------
    stage_registry:
        Mapping of stage name → zero-argument callable.  Typically the
        ``STAGES`` dict from ``main.py``.
    config:
        Runtime configuration controlling which stages run and how
        failures are handled.

    Returns
    -------
    PipelineReport
        Complete report including per-stage results, timing, and overall
        success/failure status.  Stage execution exceptions are always
        caught and recorded in the report — the caller always receives a
        complete report it can log and act on.

    Raises
    ------
    ValueError
        If any stage name in ``config.stage_sequence`` is not found in
        ``stage_registry``.
    """
    unknown = [s for s in config.stage_sequence if s not in stage_registry]
    if unknown:
        raise ValueError(
            f"Unknown stage(s) in sequence: {unknown}. "
            f"Valid stages: {sorted(stage_registry)}"
        )

    border = "═" * 62
    logger.info(border)
    logger.info(
        "  ETL Pipeline  │  mode={}  │  stages={}",
        config.mode.value,
        " → ".join(config.stage_sequence),
    )
    logger.info(border)

    pipeline_start = time.perf_counter()
    results: list[StageResult] = []
    abort = False

    for stage_name in config.stage_sequence:
        if abort:
            # fail_fast is active and a previous stage failed — mark as skipped
            results.append(
                StageResult(name=stage_name, status="skipped", elapsed_s=0.0)
            )
            logger.info("⊘  Stage [{}] skipped (pipeline aborted)", stage_name)
            continue

        result = _execute_stage(stage_name, stage_registry[stage_name])
        results.append(result)

        if result.status == "failed" and config.fail_fast:
            abort = True
            logger.error(
                "Pipeline abort: stage '{}' failed with fail_fast=True. "
                "Remaining stages will be skipped.",
                stage_name,
            )

    total_elapsed = time.perf_counter() - pipeline_start
    success = all(r.status == "success" for r in results)

    report = PipelineReport(
        mode=config.mode,
        results=results,
        total_elapsed_s=total_elapsed,
        success=success,
    )

    # Emit summary table
    logger.info("\n{}", report.summary_table())

    if success:
        logger.info(
            "Pipeline completed successfully in {:.1f}s  ✓",
            total_elapsed,
        )
    else:
        failed = [r.name for r in report.failed_stages]
        logger.error(
            "Pipeline FAILED in {:.1f}s  ✗  (failed stages: {})",
            total_elapsed,
            failed,
        )

    return report


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    "PipelineMode",
    "PipelineConfig",
    "StageResult",
    "PipelineReport",
    "FULL_REFRESH_STAGES",
    "INCREMENTAL_STAGES",
    "run_pipeline",
]
