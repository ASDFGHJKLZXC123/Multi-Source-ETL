"""
Stage 1c — Flat File Extraction (Brazilian Municipalities Reference).

Downloads the official Brazilian municipalities reference CSV from the
`kelvins/municipios-brasileiros` GitHub repository and caches it locally.

This file is used in the Silver layer to:
  - Enrich customer and seller records with standardised geographic attributes
  - Enable fuzzy city-name matching via RapidFuzz
  - Provide latitude/longitude coordinates for cities not in Olist's geolocation table

Usage
-----
    from src.extract.extract_flat_files import extract_municipios
    df = extract_municipios()
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from src.utils.logger import logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_BRONZE_MANUAL = _PROJECT_ROOT / "data" / "bronze" / "manual"
_MUNICIPIOS_URL = (
    "https://raw.githubusercontent.com/" "kelvins/municipios-brasileiros/main/csv/municipios.csv"
)
_MUNICIPIOS_PATH = _BRONZE_MANUAL / "municipios.csv"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_municipios() -> pd.DataFrame:
    """Download the Brazilian municipalities reference CSV and return it as a DataFrame.

    The file is saved to ``data/bronze/manual/municipios.csv``.
    If the file already exists on disk, the download is skipped (idempotent).

    Source
    ------
    https://github.com/kelvins/municipios-brasileiros

    Columns in the returned DataFrame (representative subset):
      - ``codigo_ibge``   : IBGE municipality code (int)
      - ``nome``          : Municipality name in Portuguese
      - ``latitude``      : Approximate centroid latitude
      - ``longitude``     : Approximate centroid longitude
      - ``capital``       : 1 if state capital, else 0
      - ``codigo_uf``     : IBGE state code
      - ``siafi_id``      : SIAFI financial system ID
      - ``ddd``           : Area code
      - ``fuso_horario``  : Time zone string
      - ``uf``            : Two-letter state abbreviation (added by this function)

    Returns
    -------
    pd.DataFrame
        Full municipalities reference table.

    Raises
    ------
    requests.HTTPError
        If the download fails and no local copy is available.
    """
    _BRONZE_MANUAL.mkdir(parents=True, exist_ok=True)

    if _MUNICIPIOS_PATH.exists():
        logger.info(
            "Municipios CSV already exists at {} — skipping download",
            _MUNICIPIOS_PATH,
        )
        df = pd.read_csv(_MUNICIPIOS_PATH, low_memory=False)
        logger.info("Loaded {:,} municipios records from cache", len(df))
        return _enrich(df)

    logger.info("Downloading Brazilian municipalities CSV from GitHub...")
    try:
        response = requests.get(_MUNICIPIOS_URL, timeout=30)
        response.raise_for_status()
    except requests.HTTPError as exc:
        logger.error("HTTP error downloading municipios: {}", exc)
        raise
    except requests.exceptions.ConnectionError as exc:
        logger.error("Network error downloading municipios: {}", exc)
        raise

    _MUNICIPIOS_PATH.write_bytes(response.content)
    logger.info(
        "Saved municipios CSV ({:.1f} KB) to {}",
        len(response.content) / 1024,
        _MUNICIPIOS_PATH,
    )

    df = pd.read_csv(_MUNICIPIOS_PATH, low_memory=False)
    logger.info("Loaded {:,} municipios records", len(df))
    return _enrich(df)


def _enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Apply lightweight enrichment to the municipalities DataFrame.

    Currently:
      - Normalises column names to lower_snake_case
      - Ensures ``nome`` (city name) is stripped of extra whitespace

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """
    df = df.copy()

    # Normalise column names: lowercase, spaces → underscores
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Trim city name whitespace
    if "nome" in df.columns:
        df["nome"] = df["nome"].str.strip()

    logger.debug("Municipios DataFrame shape after enrichment: {}", df.shape)
    return df


__all__ = ["extract_municipios"]
