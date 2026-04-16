"""
Stage 1b — Foreign Exchange Rate Extraction (Frankfurter API).

Fetches daily USD/BRL exchange rates over a specified date range using
the Frankfurter time-series endpoint (no API key required).

Weekend and public-holiday gaps are forward-filled so the resulting
DataFrame has a continuous daily index — essential for accurate join
operations with the order and weather datasets.

Usage
-----
    from src.extract.extract_fx import extract_fx_rates
    df = extract_fx_rates("2016-09-01", "2018-10-31")
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import requests

from src.utils.logger import logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_BRONZE_FX = _PROJECT_ROOT / "data" / "bronze" / "fx"

_API_BASE_URL = "https://api.frankfurter.dev/v2"

# Frankfurter enforces a limit on the number of days per request.
# Splitting into annual chunks avoids hitting that limit.
_MAX_DAYS_PER_REQUEST = 365


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def _get_json(url: str, params: dict | None = None) -> dict:
    """Perform a GET request and return the parsed JSON body.

    Parameters
    ----------
    url : str
        Full URL.
    params : dict, optional
        Query parameters.

    Returns
    -------
    dict

    Raises
    ------
    requests.HTTPError
        On any non-2xx response.
    """
    response = requests.get(url, params=params or {}, timeout=30)
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_fx_rates(
    start_date: str,
    end_date: str,
    base: str = "USD",
    quote: str = "BRL",
) -> pd.DataFrame:
    """Fetch daily FX rates from the Frankfurter time-series API.

    Gaps caused by weekends and public holidays are forward-filled using
    pandas ``reindex`` + ``ffill`` so every calendar day in the range
    has a rate.

    The raw JSON response is saved to::

        data/bronze/fx/fx_{base}_{quote}_{start_date}_{end_date}.json

    If this file already exists, the network call is skipped (idempotent).

    Parameters
    ----------
    start_date : str
        Inclusive start date in ISO format, e.g. ``"2016-09-01"``.
    end_date : str
        Inclusive end date in ISO format, e.g. ``"2018-10-31"``.
    base : str
        Base currency ticker (default ``"USD"``).
    quote : str
        Quote currency ticker (default ``"BRL"``).

    Returns
    -------
    pd.DataFrame
        Columns: ``date`` (datetime), ``base_currency``, ``quote_currency``,
        ``rate`` (float64).
        One row per calendar day; weekends/holidays are forward-filled.
    """
    _BRONZE_FX.mkdir(parents=True, exist_ok=True)

    raw_file = _BRONZE_FX / f"fx_{base}_{quote}_{start_date}_{end_date}.json"

    if raw_file.exists():
        logger.info("FX cache hit — loading from {}", raw_file)
        with open(raw_file, encoding="utf-8") as fh:
            raw_rates: dict[str, float] = json.load(fh)
    else:
        raw_rates = _fetch_fx_timeseries(start_date, end_date, base, quote)
        with open(raw_file, "w", encoding="utf-8") as fh:
            json.dump(raw_rates, fh, indent=2)
        logger.info("Saved raw FX data to {}", raw_file)

    return _build_dataframe(raw_rates, start_date, end_date, base, quote)


def _fetch_fx_timeseries(
    start_date: str,
    end_date: str,
    base: str,
    quote: str,
) -> dict[str, float]:
    """Fetch all rates from Frankfurter, splitting into annual chunks if needed.

    Parameters
    ----------
    start_date : str
        ISO date string.
    end_date : str
        ISO date string.
    base : str
        Base currency.
    quote : str
        Quote currency.

    Returns
    -------
    dict[str, float]
        Mapping of ISO date string -> float rate.
    """
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    all_rates: dict[str, float] = {}

    # Build date windows that fit within _MAX_DAYS_PER_REQUEST
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(
            chunk_start + pd.Timedelta(days=_MAX_DAYS_PER_REQUEST - 1),
            end,
        )
        # Frankfurter v2: /rates?from=&to=&base=&quotes=
        url = (
            f"{_API_BASE_URL}/rates"
            f"?from={chunk_start.date()}"
            f"&to={chunk_end.date()}"
            f"&base={base}"
            f"&quotes={quote}"
        )
        logger.debug(
            "Fetching FX rates: {} → {} ({} to {})",
            base, quote,
            chunk_start.date(), chunk_end.date(),
        )
        try:
            payload = _get_json(url)
        except requests.HTTPError as exc:
            logger.error("Frankfurter API error for chunk {}-{}: {}", chunk_start.date(), chunk_end.date(), exc)
            raise

        # Frankfurter v2 time-series response — two known shapes:
        #
        # Shape A (dict, older behaviour):
        #   {"rates": {"2016-09-01": {"BRL": 3.24}, ...}}
        #
        # Shape B (list, current v2 behaviour):
        #   [{"date": "2016-09-01", "base": "USD", "quote": "BRL", "rate": 3.23}, ...]
        #
        if isinstance(payload, list):
            # Shape B — list of daily objects with a flat "rate" field
            for item in payload:
                if not isinstance(item, dict):
                    continue
                date_str = item.get("date")
                rate = item.get("rate")
                if date_str and rate is not None:
                    all_rates[date_str] = float(rate)
        else:
            # Shape A — dict keyed by date
            rates_chunk = payload.get("rates", {})
            for date_str, value in rates_chunk.items():
                if isinstance(value, dict):
                    if quote in value:
                        all_rates[date_str] = float(value[quote])
                elif value is not None:
                    all_rates[date_str] = float(value)

        chunk_start = chunk_end + pd.Timedelta(days=1)

    logger.info(
        "Frankfurter returned {:,} trading-day rates ({} to {})",
        len(all_rates), start_date, end_date,
    )
    return all_rates


def _build_dataframe(
    raw_rates: dict[str, float],
    start_date: str,
    end_date: str,
    base: str,
    quote: str,
) -> pd.DataFrame:
    """Convert raw rates dict to a continuous daily DataFrame with gap filling.

    Parameters
    ----------
    raw_rates : dict[str, float]
        Trading-day-only rate map.
    start_date : str
    end_date : str
    base : str
    quote : str

    Returns
    -------
    pd.DataFrame
        Daily rows with forward-filled rates for non-trading days.
    """
    if not raw_rates:
        logger.warning("No FX rates to build DataFrame from — returning empty")
        return pd.DataFrame(columns=["date", "base_currency", "quote_currency", "rate"])

    # Build a Series indexed by date from the trading-day dict
    series = pd.Series(raw_rates, name="rate")
    series.index = pd.to_datetime(series.index)
    series = series.sort_index()

    # Reindex to full calendar-day range and forward-fill gaps
    full_range = pd.date_range(start=start_date, end=end_date, freq="D")
    series = series.reindex(full_range)

    gap_count = int(series.isna().sum())
    series = series.ffill()

    still_null = int(series.isna().sum())
    if still_null > 0:
        # Leading NaNs (before the first trading day) — back-fill as fallback
        series = series.bfill()
        logger.warning(
            "{} leading NaN rates found before first trading day — back-filled",
            still_null,
        )

    logger.info(
        "FX DataFrame built: {:,} calendar days, {:,} gaps forward-filled",
        len(series), gap_count,
    )

    df = pd.DataFrame({
        "date":           series.index,
        "base_currency":  base,
        "quote_currency": quote,
        "rate":           series.values,
    })
    return df


__all__ = ["extract_fx_rates"]
