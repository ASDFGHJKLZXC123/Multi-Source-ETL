"""
Stage 1a — Weather Data Extraction (Open-Meteo Archive API).

Fetches historical daily weather for a list of Brazilian cities over a
specified date range using the Open-Meteo archive endpoint (no API key
required).

Variables fetched per city:
  - temperature_2m_max   : Daily max air temperature at 2 m (°C)
  - temperature_2m_min   : Daily min air temperature at 2 m (°C)
  - precipitation_sum    : Total daily precipitation (mm)
  - windspeed_10m_max    : Daily max wind speed at 10 m (km/h)
  - weathercode          : WMO weather interpretation code

Usage
-----
    from src.extract.extract_weather import extract_weather, DEFAULT_CITIES
    df = extract_weather(DEFAULT_CITIES, "2016-09-01", "2018-10-31")
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from tqdm import tqdm

from src.utils.logger import logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_BRONZE_WEATHER = _PROJECT_ROOT / "data" / "bronze" / "weather"

_API_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
# Open-Meteo renamed the condition code variable to "weather_code" (with
# underscore) in their current API — "weathercode" (no underscore) is the
# legacy name and will 404 on updated endpoints.
_WEATHER_VARIABLES: list[str] = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "windspeed_10m_max",
    "weather_code",
]
# Joined once at module level; passed as a single comma-separated string so
# requests URL-encodes it correctly (daily=a,b,c rather than repeated daily=a).
_WEATHER_VARIABLES_PARAM: str = ",".join(_WEATHER_VARIABLES)

_MAX_RETRIES = 3
_BACKOFF_BASE_SECONDS = 2  # exponential: 2, 4, 8 seconds

DEFAULT_CITIES: list[dict[str, Any]] = [
    {"name": "sao paulo",      "state": "SP", "lat": -23.5505, "lng": -46.6333},
    {"name": "rio de janeiro", "state": "RJ", "lat": -22.9068, "lng": -43.1729},
    {"name": "brasilia",       "state": "DF", "lat": -15.7942, "lng": -47.8825},
    {"name": "salvador",       "state": "BA", "lat": -12.9714, "lng": -38.5014},
    {"name": "fortaleza",      "state": "CE", "lat":  -3.7172, "lng": -38.5434},
    {"name": "belo horizonte", "state": "MG", "lat": -19.9167, "lng": -43.9345},
    {"name": "manaus",         "state": "AM", "lat":  -3.1190, "lng": -60.0217},
    {"name": "curitiba",       "state": "PR", "lat": -25.4278, "lng": -49.2731},
    {"name": "recife",         "state": "PE", "lat":  -8.0539, "lng": -34.8811},
    {"name": "porto alegre",   "state": "RS", "lat": -30.0369, "lng": -51.2090},
    {"name": "belem",          "state": "PA", "lat":  -1.4558, "lng": -48.5044},
    {"name": "goiania",        "state": "GO", "lat": -16.6864, "lng": -49.2643},
    {"name": "guarulhos",      "state": "SP", "lat": -23.4543, "lng": -46.5333},
    {"name": "campinas",       "state": "SP", "lat": -22.9056, "lng": -47.0608},
    {"name": "sao luis",       "state": "MA", "lat":  -2.5297, "lng": -44.3028},
    {"name": "maceio",         "state": "AL", "lat":  -9.6658, "lng": -35.7350},
    {"name": "natal",          "state": "RN", "lat":  -5.7945, "lng": -35.2110},
    {"name": "teresina",       "state": "PI", "lat":  -5.0892, "lng": -42.8019},
    {"name": "campo grande",   "state": "MS", "lat": -20.4697, "lng": -54.6201},
    {"name": "joao pessoa",    "state": "PB", "lat":  -7.1195, "lng": -34.8450},
]


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _fetch_with_retry(url: str, params: dict[str, Any]) -> dict[str, Any]:
    """GET *url* with query *params*, retrying on transient HTTP errors.

    Implements exponential backoff: waits 2, 4, then 8 seconds between retries.

    Parameters
    ----------
    url : str
        Full URL to GET.
    params : dict
        Query parameters to append to the URL.

    Returns
    -------
    dict
        Parsed JSON response body.

    Raises
    ------
    requests.HTTPError
        After all retries are exhausted.
    """
    last_exc: Exception | None = None

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            # Do not retry client errors (4xx) except 429 (rate limit)
            if exc.response is not None and 400 <= exc.response.status_code < 500 and exc.response.status_code != 429:
                logger.error("Non-retryable HTTP {} error for {}", status, url)
                raise
            wait = _BACKOFF_BASE_SECONDS ** attempt
            logger.warning(
                "HTTP {} on attempt {}/{} — retrying in {}s",
                status, attempt, _MAX_RETRIES, wait,
            )
            last_exc = exc
            time.sleep(wait)
        except requests.exceptions.ConnectionError as exc:
            wait = _BACKOFF_BASE_SECONDS ** attempt
            logger.warning(
                "Connection error on attempt {}/{} — retrying in {}s: {}",
                attempt, _MAX_RETRIES, wait, exc,
            )
            last_exc = exc
            time.sleep(wait)
        except requests.exceptions.Timeout as exc:
            wait = _BACKOFF_BASE_SECONDS ** attempt
            logger.warning("Timeout on attempt {}/{} — retrying in {}s", attempt, _MAX_RETRIES, wait)
            last_exc = exc
            time.sleep(wait)

    raise requests.HTTPError(
        f"All {_MAX_RETRIES} attempts failed for {url}"
    ) from last_exc


def _save_raw_response(city_name: str, start_date: str, payload: dict[str, Any]) -> Path:
    """Persist the raw API JSON response to disk.

    Files are stored under ``data/bronze/weather/`` with the naming convention
    ``{city}_{start_date}.json``, where *city* has spaces replaced by
    underscores.

    Parameters
    ----------
    city_name : str
        City name (may contain spaces), e.g. ``"sao paulo"``.
    start_date : str
        ISO date string used as part of the filename.
    payload : dict
        The raw API response dict.

    Returns
    -------
    Path
        Path to the saved file.
    """
    _BRONZE_WEATHER.mkdir(parents=True, exist_ok=True)
    safe_city = city_name.replace(" ", "_")
    file_path = _BRONZE_WEATHER / f"{safe_city}_{start_date}.json"
    with open(file_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    logger.debug("Saved raw weather response to {}", file_path)
    return file_path


# ---------------------------------------------------------------------------
# Parse API response
# ---------------------------------------------------------------------------

def _parse_response(payload: dict[str, Any], city: dict[str, Any]) -> pd.DataFrame:
    """Convert an Open-Meteo archive response into a tidy DataFrame.

    Parameters
    ----------
    payload : dict
        Raw JSON response from the Open-Meteo archive API.
    city : dict
        City metadata dict with keys ``name`` and ``state``.

    Returns
    -------
    pd.DataFrame
        Columns: city, state, date, temp_max, temp_min,
        precipitation, windspeed, weathercode
    """
    daily = payload.get("daily", {})
    dates = daily.get("time", [])

    if not dates:
        logger.warning("Empty daily data for city '{}'", city["name"])
        return pd.DataFrame(
            columns=["city", "state", "date", "temp_max", "temp_min",
                     "precipitation", "windspeed", "weathercode"]
        )

    df = pd.DataFrame({
        "city":          city["name"],
        "state":         city["state"],
        "date":          pd.to_datetime(dates),
        "temp_max":      daily.get("temperature_2m_max"),
        "temp_min":      daily.get("temperature_2m_min"),
        "precipitation": daily.get("precipitation_sum"),
        "windspeed":     daily.get("windspeed_10m_max"),
        # Open-Meteo uses "weather_code" (with underscore) in current API
        "weathercode":   daily.get("weather_code"),
    })
    return df


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_weather(
    cities: list[dict[str, Any]],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Fetch historical daily weather for a list of cities from Open-Meteo.

    Parameters
    ----------
    cities : list[dict]
        List of city dicts. Each dict must contain:
        ``name`` (str), ``state`` (str), ``lat`` (float), ``lng`` (float).
    start_date : str
        Inclusive start date in ISO format, e.g. ``"2016-09-01"``.
    end_date : str
        Inclusive end date in ISO format, e.g. ``"2018-10-31"``.

    Returns
    -------
    pd.DataFrame
        Combined weather data for all cities with columns:
        city, state, date, temp_max, temp_min, precipitation,
        windspeed, weathercode.

    Notes
    -----
    - Raw API responses are saved under ``data/bronze/weather/``.
    - Cities that fail after all retries are skipped and logged as errors.
    """
    logger.info(
        "Extracting weather data for {} cities from {} to {}",
        len(cities), start_date, end_date,
    )

    all_frames: list[pd.DataFrame] = []

    for city in tqdm(cities, desc="Fetching weather", unit="city"):
        city_name: str = city["name"]
        state: str = city["state"]

        # Check whether raw file already exists (idempotency)
        safe_city = city_name.replace(" ", "_")
        raw_file = _BRONZE_WEATHER / f"{safe_city}_{start_date}.json"
        if raw_file.exists():
            logger.debug("Cache hit for '{}' — loading from disk", city_name)
            with open(raw_file, encoding="utf-8") as fh:
                payload = json.load(fh)
        else:
            params: dict[str, Any] = {
                "latitude":   city["lat"],
                "longitude":  city["lng"],
                "start_date": start_date,
                "end_date":   end_date,
                "daily":      _WEATHER_VARIABLES_PARAM,
                "timezone":   "America/Sao_Paulo",
            }
            try:
                payload = _fetch_with_retry(_API_BASE_URL, params)
                _save_raw_response(city_name, start_date, payload)
            except Exception as exc:
                logger.error("Failed to fetch weather for '{}' ({}): {}", city_name, state, exc)
                continue

        city_df = _parse_response(payload, city)
        if not city_df.empty:
            all_frames.append(city_df)
            logger.debug(
                "Parsed {:,} weather records for '{}'", len(city_df), city_name
            )

    if not all_frames:
        logger.warning("No weather data collected — returning empty DataFrame")
        return pd.DataFrame(
            columns=["city", "state", "date", "temp_max", "temp_min",
                     "precipitation", "windspeed", "weathercode"]
        )

    combined = pd.concat(all_frames, ignore_index=True)
    logger.info(
        "Weather extraction complete: {:,} total records for {} cities",
        len(combined),
        combined["city"].nunique(),
    )
    return combined


__all__ = ["extract_weather", "DEFAULT_CITIES"]
