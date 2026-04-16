"""
tests/conftest.py
-----------------
Shared pytest fixtures for the Multi-Source ETL test suite.

All fixtures produce fully in-memory pandas DataFrames with deterministic,
realistic column values that satisfy the Silver-layer schemas defined in
``src.transform.schemas``.  No file I/O, database connections, or network
calls are made by any fixture in this module.

Usage
-----
Fixtures declared here are automatically discovered by pytest and are
available in every test module under the ``tests/`` directory without any
explicit import.
"""

from __future__ import annotations

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Orders fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def minimal_orders_df() -> pd.DataFrame:
    """Return a 3-row orders DataFrame whose values satisfy SilverOrderSchema.

    All timestamp columns are genuine ``datetime64`` objects so tests that
    call ``validate_silver`` directly do not trigger coercion side-effects.
    The three rows use distinct ``order_id`` values and cover the three most
    common statuses: delivered, shipped, and invoiced.
    """
    return pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "order_code": ["aaa-111", "bbb-222", "ccc-333"],
            "customer_id": [10, 20, 30],
            "order_status": ["delivered", "shipped", "invoiced"],
            "order_date": pd.to_datetime(["2017-03-01", "2017-06-15", "2017-09-20"]),
            "order_timestamp": pd.to_datetime(
                ["2017-03-01 08:00:00", "2017-06-15 12:00:00", "2017-09-20 16:00:00"]
            ),
            "approved_at": pd.to_datetime(
                ["2017-03-01 09:00:00", "2017-06-15 13:00:00", "2017-09-20 17:00:00"]
            ),
            "estimated_delivery": pd.to_datetime(["2017-03-10", "2017-06-25", "2017-09-30"]),
            "actual_delivery": pd.to_datetime(["2017-03-09", "2017-06-24", None]),
            "delivery_days_actual": pd.array([8, 9, None], dtype="Int64"),
            "delivery_days_estimated": pd.array([9, 10, 10], dtype="Int64"),
            "source_channel": ["online", "online", "online"],
            "currency_code": ["BRL", "BRL", "BRL"],
            "ingested_at": pd.to_datetime(
                ["2017-03-02 00:00:00", "2017-06-16 00:00:00", "2017-09-21 00:00:00"]
            ),
        }
    )


# ---------------------------------------------------------------------------
# FX fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def minimal_fx_df() -> pd.DataFrame:
    """Return a 5-row FX DataFrame whose values satisfy SilverFxSchema.

    Rows span five consecutive trading days in January 2017 with a
    stable USD/BRL rate.  All rates are strictly positive and all currency
    codes are exactly 3 characters.
    """
    return pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2017-01-02",
                    "2017-01-03",
                    "2017-01-04",
                    "2017-01-05",
                    "2017-01-06",
                ]
            ),
            "base_currency": ["USD", "USD", "USD", "USD", "USD"],
            "quote_currency": ["BRL", "BRL", "BRL", "BRL", "BRL"],
            "rate": [3.25, 3.26, 3.24, 3.27, 3.28],
        }
    )


# ---------------------------------------------------------------------------
# Weather fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def minimal_weather_df() -> pd.DataFrame:
    """Return a 3-row weather DataFrame whose values satisfy SilverWeatherSchema.

    Cities and state abbreviations are already normalised (lowercase city,
    2-character uppercase state) as the Silver transform would produce them.
    All numeric observation columns are within their schema-defined bounds.
    The ``weathercode`` column uses the pandas nullable ``Int64`` dtype to
    match the cast applied in ``transform_weather``.
    """
    return pd.DataFrame(
        {
            "city": ["sao paulo", "rio de janeiro", "salvador"],
            "state": ["SP", "RJ", "BA"],
            "date": pd.to_datetime(["2017-01-01", "2017-01-01", "2017-01-01"]),
            "temp_max": [30.0, 32.5, 33.0],
            "temp_min": [18.0, 22.0, 24.0],
            "precipitation": [5.0, 0.0, 2.5],
            "windspeed": [10.0, 15.0, 8.0],
            "weathercode": pd.array([3, 1, 61], dtype="Int64"),
        }
    )
