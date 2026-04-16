"""
src/transform/transform_weather.py
------------------------------------
Stage 3 Silver Transform — Weather domain.

Reads daily weather observations from the Bronze layer (via the
``extract_weather`` cache-aware loader), applies quality rules, and writes a
clean Silver Parquet file.

Quality rules applied
---------------------
* Rows whose ``date`` falls outside the pipeline window are quarantined.
* Rows with a null ``city`` or ``state`` are quarantined.
* Rows with a null ``date`` are quarantined.
* ``weathercode`` is cast from float to nullable ``Int64``.
* City names are lowercased and stripped of leading/trailing whitespace
  (accents are preserved as-is).
* The resulting DataFrame is validated against ``SilverWeatherSchema``.

Public entry-points
-------------------
* ``transform_weather()`` — end-to-end transform.
* ``run()``               — thin wrapper used by the CLI.
"""

from __future__ import annotations

import sys

import pandas as pd

from src.utils.logger import logger
from src.extract.extract_weather import DEFAULT_CITIES, extract_weather
from src.transform.schemas import SilverWeatherSchema, validate_silver
from src.transform.utils import (
    get_pipeline_date_range,
    log_transform_summary,
    quarantine_rows,
    write_silver,
)

__all__ = ["transform_weather", "run"]


# ---------------------------------------------------------------------------
# Weather transform
# ---------------------------------------------------------------------------


def transform_weather(
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[pd.DataFrame, int]:
    """Clean and validate daily weather observations, writing to Silver.

    Parameters
    ----------
    start_date : str | None
        Inclusive pipeline start date (ISO format, e.g. ``"2016-09-01"``).
        Falls back to ``get_pipeline_date_range()`` when *None*.
    end_date : str | None
        Inclusive pipeline end date (ISO format, e.g. ``"2018-10-31"``).
        Falls back to ``get_pipeline_date_range()`` when *None*.

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
    df: pd.DataFrame = extract_weather(DEFAULT_CITIES, start_date, end_date)
    initial_count: int = len(df)

    if df.empty:
        logger.warning("Weather extract returned an empty DataFrame — nothing to transform")
        return df, 0

    quarantine_frames: list[pd.DataFrame] = []
    quarantine_reasons: list[pd.Series] = []

    # Ensure date column is datetime before range comparisons
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # ------------------------------------------------------------------
    # Step 3: Quarantine null date rows (must be done before range check)
    # ------------------------------------------------------------------
    null_date_mask: pd.Series = df["date"].isna()
    if null_date_mask.any():
        quarantine_frames.append(df.loc[null_date_mask])
        quarantine_reasons.append(
            pd.Series("null date", index=df.index[null_date_mask])
        )
        df = df.loc[~null_date_mask].copy()

    # ------------------------------------------------------------------
    # Step 4: Quarantine rows outside the pipeline date range
    # ------------------------------------------------------------------
    ts_start = pd.Timestamp(start_date)
    ts_end = pd.Timestamp(end_date)

    out_of_range_mask: pd.Series = (df["date"] < ts_start) | (df["date"] > ts_end)
    if out_of_range_mask.any():
        quarantine_frames.append(df.loc[out_of_range_mask])
        quarantine_reasons.append(
            pd.Series("date outside pipeline range", index=df.index[out_of_range_mask])
        )
        df = df.loc[~out_of_range_mask].copy()

    # ------------------------------------------------------------------
    # Step 5: Quarantine null city or state
    # ------------------------------------------------------------------
    null_city_state_mask: pd.Series = df["city"].isna() | df["state"].isna()
    if null_city_state_mask.any():
        quarantine_frames.append(df.loc[null_city_state_mask])
        quarantine_reasons.append(
            pd.Series(
                "null city or state",
                index=df.index[null_city_state_mask],
            )
        )
        df = df.loc[~null_city_state_mask].copy()

    # ------------------------------------------------------------------
    # Step 6: Cast weathercode to nullable Int64
    # ------------------------------------------------------------------
    df = df.copy()
    df["weathercode"] = df["weathercode"].astype("Int64")

    # ------------------------------------------------------------------
    # Step 7: Normalize city names (lowercase + strip; preserve accents)
    # ------------------------------------------------------------------
    df["city"] = df["city"].str.lower().str.strip()

    # ------------------------------------------------------------------
    # Step 8: Pandera validation
    # ------------------------------------------------------------------
    df = df.reset_index(drop=True)
    valid_df, invalid_df = validate_silver(df, SilverWeatherSchema, "weather")

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
        quarantine_rows(combined_q, combined_reasons, "weather")

    log_transform_summary("weather", initial_count, len(valid_df), total_quarantined)
    write_silver(valid_df, "weather", "weather")

    return valid_df, total_quarantined


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run() -> None:
    """Run the Weather Silver transform end-to-end.

    Raises
    ------
    Exception
        Propagated so the CLI wrapper can exit with code 1.
    """
    logger.info("Starting Weather Silver transform")
    valid_df, quarantined = transform_weather()
    logger.info(
        "Weather Silver transform complete: {:,} rows written, {} quarantined",
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
        logger.exception("Weather transform failed: {}", exc)
        sys.exit(1)
