"""
Stage 2 — Unified API Extraction Entry Point.

Orchestrates weather (Open-Meteo) and FX (Frankfurter) extractions, writes
per-source manifests to ``data/bronze/api/``, and exposes a CLI for
ad-hoc or scheduled runs.

Usage
-----
    # Run both sources with pipeline-config defaults:
    python -m src.extract.extract_api

    # Weather only, custom date window, force re-extraction:
    python -m src.extract.extract_api \\
        --source weather \\
        --start-date 2017-01-01 \\
        --end-date   2017-12-31 \\
        --force

    # FX only:
    python -m src.extract.extract_api --source fx

Programmatic usage:
    from src.extract.extract_api import extract_all_apis
    results = extract_all_apis("2016-09-01", "2018-10-31")
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.extract.config import BRONZE_API
from src.extract.extract_fx import extract_fx_rates
from src.extract.extract_weather import DEFAULT_CITIES, extract_weather
from src.utils.db import get_pipeline_config
from src.utils.logger import logger

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_manifest(path: Path) -> dict[str, Any]:
    """Read an existing manifest JSON from *path*.

    Parameters
    ----------
    path : Path
        Absolute path to the manifest file.

    Returns
    -------
    dict[str, Any]
        Parsed manifest contents.
    """
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _save_manifest(path: Path, manifest: dict[str, Any]) -> None:
    """Persist *manifest* to *path* as pretty-printed JSON.

    Parameters
    ----------
    path : Path
        Destination file path; parent directories are created if absent.
    manifest : dict[str, Any]
        Data to serialise.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, ensure_ascii=False, default=str)
    logger.debug("Manifest saved to {}", path)


# ---------------------------------------------------------------------------
# Public extraction functions
# ---------------------------------------------------------------------------


def extract_weather_to_bronze(
    start_date: str,
    end_date: str,
    city_count: int = 20,
) -> dict[str, Any]:
    """Extract weather data and write a Bronze-layer manifest.

    Delegates to :func:`src.extract.extract_weather.extract_weather` and
    then saves a JSON manifest summarising the extraction run.

    Idempotent: if the manifest file already exists it is loaded and returned
    immediately without re-fetching any data.

    Parameters
    ----------
    start_date : str
        Inclusive start date in ISO format, e.g. ``"2016-09-01"``.
    end_date : str
        Inclusive end date in ISO format, e.g. ``"2018-10-31"``.
    city_count : int
        Number of cities to extract from :data:`DEFAULT_CITIES` (head-slice).

    Returns
    -------
    dict[str, Any]
        Manifest with keys:
        ``source``, ``start_date``, ``end_date``, ``cities_requested``,
        ``cities_extracted``, ``total_records``, ``extracted_at``, ``status``.
    """
    manifest_path = BRONZE_API / f"weather_manifest_{start_date}_{end_date}.json"

    if manifest_path.exists():
        logger.info("Weather manifest already exists — loading from {}", manifest_path)
        return _load_manifest(manifest_path)

    cities_slice = DEFAULT_CITIES[:city_count]
    cities_requested = len(cities_slice)

    manifest: dict[str, Any] = {
        "source": "open-meteo",
        "start_date": start_date,
        "end_date": end_date,
        "cities_requested": cities_requested,
        "cities_extracted": 0,
        "total_records": 0,
        "extracted_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "status": "failed",
    }

    try:
        df: pd.DataFrame = extract_weather(cities_slice, start_date, end_date)
    except Exception as exc:
        logger.error("Weather extraction raised an unexpected error: {}", exc)
        _save_manifest(manifest_path, manifest)
        return manifest

    if df.empty:
        logger.warning("Weather extraction returned an empty DataFrame — status=failed")
        _save_manifest(manifest_path, manifest)
        return manifest

    cities_extracted: int = int(df["city"].nunique())
    total_records: int = len(df)

    if cities_extracted < cities_requested:
        status = "partial"
        logger.warning(
            "Weather extraction partial: {} of {} cities succeeded",
            cities_extracted,
            cities_requested,
        )
    else:
        status = "success"
        logger.info(
            "Weather extraction success: {:,} records for {} cities",
            total_records,
            cities_extracted,
        )

    manifest.update(
        {
            "cities_extracted": cities_extracted,
            "total_records": total_records,
            "extracted_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "status": status,
        }
    )

    _save_manifest(manifest_path, manifest)
    return manifest


def extract_fx_to_bronze(
    start_date: str,
    end_date: str,
    base: str = "USD",
    quote: str = "BRL",
) -> dict[str, Any]:
    """Extract FX rates and write a Bronze-layer manifest.

    Delegates to :func:`src.extract.extract_fx.extract_fx_rates` and saves a
    JSON manifest summarising the extraction run.

    Idempotent: if the manifest file already exists it is loaded and returned
    immediately without re-fetching any data.

    Parameters
    ----------
    start_date : str
        Inclusive start date in ISO format.
    end_date : str
        Inclusive end date in ISO format.
    base : str
        Base currency ticker (default ``"USD"``).
    quote : str
        Quote currency ticker (default ``"BRL"``).

    Returns
    -------
    dict[str, Any]
        Manifest with keys:
        ``source``, ``start_date``, ``end_date``, ``base_currency``,
        ``quote_currency``, ``trading_days``, ``calendar_days``,
        ``extracted_at``, ``status``.
    """
    manifest_path = BRONZE_API / f"fx_manifest_{base}_{quote}_{start_date}_{end_date}.json"

    if manifest_path.exists():
        logger.info("FX manifest already exists — loading from {}", manifest_path)
        return _load_manifest(manifest_path)

    # Compute expected calendar days for the manifest regardless of outcome.
    try:
        calendar_days = int((pd.Timestamp(end_date) - pd.Timestamp(start_date)).days + 1)
    except Exception:
        calendar_days = 0

    manifest: dict[str, Any] = {
        "source": "frankfurter",
        "start_date": start_date,
        "end_date": end_date,
        "base_currency": base,
        "quote_currency": quote,
        "trading_days": 0,
        "calendar_days": calendar_days,
        "extracted_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "status": "failed",
    }

    try:
        df: pd.DataFrame = extract_fx_rates(start_date, end_date, base, quote)
    except Exception as exc:
        logger.error("FX extraction raised an unexpected error: {}", exc)
        _save_manifest(manifest_path, manifest)
        return manifest

    if df.empty:
        logger.warning("FX extraction returned an empty DataFrame — status=failed")
        _save_manifest(manifest_path, manifest)
        return manifest

    # Forward-filled DataFrame has one row per calendar day; count non-ffill
    # (original trading-day) rows is not directly available here, so we proxy
    # trading days as the count of rows where the rate differs from the
    # previous row's rate (i.e. fresh data points), plus the very first row.
    rate_series = df["rate"]
    trading_days = int((rate_series != rate_series.shift()).sum())

    manifest.update(
        {
            "trading_days": trading_days,
            "calendar_days": len(df),
            "extracted_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "status": "success",
        }
    )

    logger.info(
        "FX extraction success: {:,} calendar days, ~{:,} trading days ({}/{})",
        len(df),
        trading_days,
        base,
        quote,
    )

    _save_manifest(manifest_path, manifest)
    return manifest


def extract_all_apis(
    start_date: str,
    end_date: str,
    **kwargs: Any,
) -> dict[str, dict[str, Any]]:
    """Run both weather and FX extractions and return combined manifests.

    Per-source failures are caught and logged; the other source continues
    regardless.

    Parameters
    ----------
    start_date : str
        Inclusive start date in ISO format.
    end_date : str
        Inclusive end date in ISO format.
    **kwargs : Any
        Optional overrides forwarded to each sub-function:
        ``city_count`` (int), ``base`` (str), ``quote`` (str).

    Returns
    -------
    dict[str, dict[str, Any]]
        ``{"weather": <manifest>, "fx": <manifest>}``.
        A source's manifest will have ``status="failed"`` if its extraction
        raised an unhandled exception.
    """
    city_count: int = kwargs.get("city_count", 20)
    base: str = kwargs.get("base", "USD")
    quote: str = kwargs.get("quote", "BRL")

    results: dict[str, dict[str, Any]] = {}

    # --- Weather ---
    try:
        results["weather"] = extract_weather_to_bronze(start_date, end_date, city_count)
    except Exception as exc:
        logger.error("Unhandled exception in weather extraction: {}", exc)
        results["weather"] = {
            "source": "open-meteo",
            "start_date": start_date,
            "end_date": end_date,
            "status": "failed",
            "error": str(exc),
        }

    # --- FX ---
    try:
        results["fx"] = extract_fx_to_bronze(start_date, end_date, base, quote)
    except Exception as exc:
        logger.error("Unhandled exception in FX extraction: {}", exc)
        results["fx"] = {
            "source": "frankfurter",
            "start_date": start_date,
            "end_date": end_date,
            "status": "failed",
            "error": str(exc),
        }

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    """Construct the argument parser for the CLI entry point."""
    config = get_pipeline_config()

    parser = argparse.ArgumentParser(
        prog="extract_api",
        description="Stage 2 — Extract API data (weather and/or FX) to Bronze layer.",
    )
    parser.add_argument(
        "--source",
        choices=["weather", "fx", "all"],
        default="all",
        help="Which API source(s) to extract (default: all).",
    )
    parser.add_argument(
        "--start-date",
        default=config["start_date"],
        metavar="YYYY-MM-DD",
        help=f"Extraction start date (default: {config['start_date']}).",
    )
    parser.add_argument(
        "--end-date",
        default=config["end_date"],
        metavar="YYYY-MM-DD",
        help=f"Extraction end date (default: {config['end_date']}).",
    )
    parser.add_argument(
        "--city-count",
        type=int,
        default=int(config["weather_city_count"]),
        metavar="N",
        help="Number of cities to extract for weather (default: 20).",
    )
    parser.add_argument(
        "--base",
        default=config["fx_base_currency"],
        metavar="CCY",
        help=f"FX base currency (default: {config['fx_base_currency']}).",
    )
    parser.add_argument(
        "--quote",
        default=config["fx_quote_currency"],
        metavar="CCY",
        help=f"FX quote currency (default: {config['fx_quote_currency']}).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Delete existing manifests before running (re-extract unconditionally).",
    )
    return parser


def _delete_manifest_if_exists(path: Path) -> None:
    """Remove *path* if it exists; log the action."""
    if path.exists():
        path.unlink()
        logger.info("--force: deleted existing manifest {}", path)


def _print_summary(results: dict[str, dict[str, Any]]) -> None:
    """Print a human-readable extraction summary to stdout."""
    print("\n" + "=" * 60)
    print("  API Extraction Summary")
    print("=" * 60)
    for source, manifest in results.items():
        status = manifest.get("status", "unknown").upper()
        print(f"  {source.upper():10s}  {status}")
    print("=" * 60 + "\n")


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Parameters
    ----------
    argv : list[str] | None
        Argument list override (defaults to ``sys.argv[1:]``).

    Returns
    -------
    int
        Exit code: 0 on full success, 1 if any source failed completely.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    start_date: str = args.start_date
    end_date: str = args.end_date
    source: str = args.source
    city_count: int = args.city_count
    base: str = args.base
    quote: str = args.quote
    force: bool = args.force

    # --force: wipe relevant manifests so idempotency check is bypassed.
    if force:
        if source in ("weather", "all"):
            _delete_manifest_if_exists(
                BRONZE_API / f"weather_manifest_{start_date}_{end_date}.json"
            )
        if source in ("fx", "all"):
            _delete_manifest_if_exists(
                BRONZE_API / f"fx_manifest_{base}_{quote}_{start_date}_{end_date}.json"
            )

    results: dict[str, dict[str, Any]] = {}

    if source == "weather":
        try:
            results["weather"] = extract_weather_to_bronze(start_date, end_date, city_count)
        except Exception as exc:
            logger.error("Weather extraction failed: {}", exc)
            results["weather"] = {"status": "failed", "error": str(exc)}

    elif source == "fx":
        try:
            results["fx"] = extract_fx_to_bronze(start_date, end_date, base, quote)
        except Exception as exc:
            logger.error("FX extraction failed: {}", exc)
            results["fx"] = {"status": "failed", "error": str(exc)}

    else:  # "all"
        results = extract_all_apis(
            start_date,
            end_date,
            city_count=city_count,
            base=base,
            quote=quote,
        )

    _print_summary(results)

    any_failed = any(m.get("status") == "failed" for m in results.values())
    return 1 if any_failed else 0


if __name__ == "__main__":
    sys.exit(main())


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    "extract_weather_to_bronze",
    "extract_fx_to_bronze",
    "extract_all_apis",
]
