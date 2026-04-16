"""
src/transform/transform_fx.py
-------------------------------
Stage 3 Silver Transform — Foreign Exchange domain.

Reads daily USD/BRL spot rates from the Bronze cache (via ``extract_fx_rates``),
applies quality rules, and writes a clean Silver Parquet file.

Quality rules applied
---------------------
* Rows outside the pipeline date range are quarantined.
* Weekend / holiday gaps are forward-filled across a full calendar range;
  any leading nulls before the first trading day are back-filled.
* Rows with a null ``rate`` after gap-filling are quarantined.
* Rows with null ``base_currency`` or ``quote_currency`` are quarantined.
* Duplicate dates (same ``date`` value) are deduplicated — keeping the last
  occurrence so more-recent API data takes precedence.
* The resulting DataFrame is validated against ``SilverFxSchema``.

Public entry-points
-------------------
* ``transform_fx()`` — end-to-end transform.
* ``run()``          — thin wrapper used by the CLI.
"""

from __future__ import annotations

import sys

import pandas as pd

from src.extract.extract_fx import extract_fx_rates
from src.transform.schemas import SilverFxSchema, validate_silver
from src.transform.utils import (
    get_pipeline_date_range,
    log_transform_summary,
    quarantine_rows,
    write_silver,
)
from src.utils.logger import logger

__all__ = ["transform_fx", "run"]


# ---------------------------------------------------------------------------
# FX transform
# ---------------------------------------------------------------------------


def transform_fx(
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[pd.DataFrame, int]:
    """Clean and validate daily FX rates, writing to Silver.

    Parameters
    ----------
    start_date : str | None
        Inclusive pipeline start date (ISO format).  Resolved from
        ``get_pipeline_date_range()`` when *None*.
    end_date : str | None
        Inclusive pipeline end date (ISO format).  Resolved from
        ``get_pipeline_date_range()`` when *None*.

    Returns
    -------
    tuple[pd.DataFrame, int]
        ``(valid_df, total_quarantine_count)``
    """
    # ------------------------------------------------------------------
    # Step 1: Resolve date range
    # ------------------------------------------------------------------
    if start_date is None or end_date is None:
        start_date, end_date = get_pipeline_date_range()

    # ------------------------------------------------------------------
    # Step 2: Load from Bronze cache
    # ------------------------------------------------------------------
    df: pd.DataFrame = extract_fx_rates(start_date, end_date)
    initial_count: int = len(df)

    if df.empty:
        logger.warning("FX extract returned an empty DataFrame — nothing to transform")
        return df, 0

    quarantine_frames: list[pd.DataFrame] = []
    quarantine_reasons: list[pd.Series] = []

    # Ensure date column is proper datetime
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # ------------------------------------------------------------------
    # Step 4: Quarantine rows outside the pipeline date range
    # ------------------------------------------------------------------
    ts_start = pd.Timestamp(start_date)
    ts_end = pd.Timestamp(end_date)

    out_of_range_mask: pd.Series = (df["date"] < ts_start) | (df["date"] > ts_end)
    if out_of_range_mask.any():
        quarantine_frames.append(df.loc[out_of_range_mask])
        quarantine_reasons.append(
            pd.Series(
                "date outside pipeline range",
                index=df.index[out_of_range_mask],
            )
        )
        df = df.loc[~out_of_range_mask].copy()

    # ------------------------------------------------------------------
    # Step 5: Forward-fill missing calendar dates
    # extract_fx_rates already does this, but we re-apply here to guard
    # against partial or stale Bronze cache data.
    # ------------------------------------------------------------------
    # Capture currency values before reindexing (they're constant scalars)
    base_currency: str = df["base_currency"].iloc[0] if not df.empty else ""
    quote_currency: str = df["quote_currency"].iloc[0] if not df.empty else ""

    # Deduplicate on date before indexing — reindex raises ValueError if the
    # index contains duplicate labels.  Keep the last occurrence so that if
    # a Bronze cache has overlapping chunks the most-recent rate wins.
    df = df.drop_duplicates(subset=["date"], keep="last").copy()

    # Set date as index for reindex / fill operations
    # Name the index "date" before setting it so reset_index() produces a
    # column called "date" rather than the implicit "index" fallback name.
    df = df.set_index("date")
    df.index.name = "date"
    full_range: pd.DatetimeIndex = pd.date_range(
        start=start_date, end=end_date, freq="D", name="date"
    )
    df = df.reindex(full_range)

    # Restore currency columns that become NaN after reindex on new dates
    df["base_currency"] = df["base_currency"].fillna(base_currency)
    df["quote_currency"] = df["quote_currency"].fillna(quote_currency)

    df["rate"] = df["rate"].ffill()

    # Back-fill only leading NaNs (days before the first trading day)
    leading_null_count: int = int(df["rate"].isna().sum())
    if leading_null_count > 0:
        df["rate"] = df["rate"].bfill()
        logger.warning(
            "FX: {} leading null rate(s) before first trading day — back-filled",
            leading_null_count,
        )

    df = df.reset_index()  # "date" index → "date" column (index was named above)

    # ------------------------------------------------------------------
    # Step 6: Quarantine remaining null rates (should be none after fill)
    # ------------------------------------------------------------------
    null_rate_mask: pd.Series = df["rate"].isna()
    if null_rate_mask.any():
        quarantine_frames.append(df.loc[null_rate_mask])
        quarantine_reasons.append(
            pd.Series(
                "null exchange rate after ffill",
                index=df.index[null_rate_mask],
            )
        )
        df = df.loc[~null_rate_mask].copy()

    # ------------------------------------------------------------------
    # Step 7: Quarantine null base_currency / quote_currency
    # ------------------------------------------------------------------
    null_ccy_mask: pd.Series = df["base_currency"].isna() | df["quote_currency"].isna()
    if null_ccy_mask.any():
        quarantine_frames.append(df.loc[null_ccy_mask])
        quarantine_reasons.append(
            pd.Series(
                "null base_currency or quote_currency",
                index=df.index[null_ccy_mask],
            )
        )
        df = df.loc[~null_ccy_mask].copy()

    # ------------------------------------------------------------------
    # Step 8: Deduplicate on date — keep last (most recent API data wins)
    # ------------------------------------------------------------------
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    dedup_dropped = before_dedup - len(df)
    if dedup_dropped > 0:
        logger.info("FX: dropped {} duplicate date row(s)", dedup_dropped)

    # ------------------------------------------------------------------
    # Step 9: Pandera validation
    # ------------------------------------------------------------------
    valid_df, invalid_df = validate_silver(df, SilverFxSchema, "fx")

    if not invalid_df.empty:
        schema_reason = invalid_df["quarantine_reason"]
        invalid_df_without_reason = invalid_df.drop(columns=["quarantine_reason"])
        quarantine_frames.append(invalid_df_without_reason)
        quarantine_reasons.append(schema_reason.reset_index(drop=True))

    # ------------------------------------------------------------------
    # Quarantine flush + summary + write
    # ------------------------------------------------------------------
    total_quarantined: int = sum(len(f) for f in quarantine_frames)

    if quarantine_frames:
        combined_q = pd.concat(quarantine_frames, ignore_index=True)
        combined_reasons = pd.concat(
            [r.reset_index(drop=True) for r in quarantine_reasons],
            ignore_index=True,
        )
        quarantine_rows(combined_q, combined_reasons, "fx")

    log_transform_summary("fx", initial_count, len(valid_df), total_quarantined)
    write_silver(valid_df, "fx", "fx_rates")

    return valid_df, total_quarantined


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run() -> None:
    """Run the FX Silver transform end-to-end.

    Raises
    ------
    Exception
        Propagated so the CLI wrapper can exit with code 1.
    """
    logger.info("Starting FX Silver transform")
    valid_df, quarantined = transform_fx()
    logger.info(
        "FX Silver transform complete: {:,} rows written, {} quarantined",
        len(valid_df),
        quarantined,
    )


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        run()
        sys.exit(0)
    except Exception as exc:  # noqa: BLE001
        logger.exception("FX transform failed: {}", exc)
        sys.exit(1)
