"""
src/transform/transform_payments.py
-----------------------------------
Stage 3 Silver Transform — Payments domain.

Reads the latest Bronze Parquet snapshot for ``source_system.payments``,
filters rows with ``payment_type='not_defined'`` to quarantine (these are
missing-payment-method records, not a real instrument), coerces types,
validates the remainder against ``SilverPaymentsSchema``, routes rejected
rows to the Quarantine layer, and writes a clean Silver Parquet file.

Public entry-points
-------------------
* ``transform_payments()`` — cleans and promotes the payments table.
* ``run()``                — orchestrator (mirrors transform_sales.run shape).
"""

from __future__ import annotations

import sys

import pandas as pd

from src.extract.config import BRONZE_DB
from src.transform.schemas import SilverPaymentsSchema, validate_silver
from src.transform.utils import (
    log_transform_summary,
    quarantine_rows,
    read_latest_bronze_parquet,
    write_silver,
)
from src.utils.logger import logger

__all__ = ["transform_payments", "run"]


def transform_payments() -> tuple[pd.DataFrame, int]:
    """Clean Bronze payments → Silver. Returns ``(valid_df, quarantined_count)``."""
    logger.info("Starting Silver transform: payments")

    raw: pd.DataFrame = read_latest_bronze_parquet(BRONZE_DB / "payments")
    initial_count: int = len(raw)
    logger.info("payments: read {:,} rows from Bronze", initial_count)

    df: pd.DataFrame = raw.copy()

    # Type coercion — Bronze Parquet may carry payment_value as Decimal.
    df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce")
    df["payment_sequential"] = pd.to_numeric(df["payment_sequential"], errors="coerce")
    df["payment_installments"] = pd.to_numeric(df["payment_installments"], errors="coerce")
    df["ingested_at"] = pd.to_datetime(df["ingested_at"])

    total_quarantined: int = 0

    # Pre-validation quarantine: not_defined payment_type is a missing-method
    # marker, not a real instrument. Route to quarantine before pandera so the
    # schema's payment_type isin-check produces a clean valid set.
    not_defined_mask: pd.Series = df["payment_type"] == "not_defined"
    if not_defined_mask.any():
        not_defined_df: pd.DataFrame = df.loc[not_defined_mask].copy()
        reasons = pd.Series(
            ["payment_type='not_defined' (missing payment method)"] * len(not_defined_df),
            index=not_defined_df.index,
        )
        quarantine_rows(not_defined_df, reasons, "payments")
        total_quarantined += len(not_defined_df)
        df = df.loc[~not_defined_mask].copy()
        logger.info(
            "payments: quarantined {:,} rows with payment_type='not_defined'",
            len(not_defined_df),
        )

    # Pandera schema validation.
    keep = [
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
        "ingested_at",
    ]
    df = df[keep].reset_index(drop=True)

    valid_df, invalid_df = validate_silver(df, SilverPaymentsSchema, "payments")
    if len(invalid_df) > 0:
        quarantine_rows(
            invalid_df.drop(columns=["quarantine_reason"]),
            invalid_df["quarantine_reason"],
            "payments",
        )
        total_quarantined += len(invalid_df)

    log_transform_summary("payments", initial_count, len(valid_df), total_quarantined)
    write_silver(valid_df, "payments", "payments")

    return valid_df, total_quarantined


def run() -> None:
    """Orchestrate the Payments Silver transform."""
    logger.info("Starting Payments Silver transform")
    df, quarantined = transform_payments()
    logger.info(
        "Payments Silver transform complete: {:,} payments ({:,} quarantined)",
        len(df),
        quarantined,
    )


if __name__ == "__main__":
    try:
        run()
        sys.exit(0)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Payments transform failed: {}", exc)
        sys.exit(1)
