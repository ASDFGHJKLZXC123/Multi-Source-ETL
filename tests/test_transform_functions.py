"""
tests/test_transform_functions.py
----------------------------------
Unit tests for the three Silver-layer transform entry-points:

    transform_sales.transform_orders()
    transform_weather.transform_weather(start_date, end_date)
    transform_fx.transform_fx(start_date, end_date)

All disk I/O and network calls are replaced by unittest.mock.patch so the
tests exercise the full transform logic — including pandera schema validation —
without touching the filesystem or any external API.

Mocking strategy
----------------
* read_latest_bronze_parquet  → returns an in-memory DataFrame
* write_silver                → returns a dummy Path; side-effect suppressed
* quarantine_rows             → returns None; side-effect suppressed
* extract_weather             → returns an in-memory DataFrame
* extract_fx_rates            → returns an in-memory DataFrame

pandera validation is NOT mocked — every test DataFrame is constructed to
satisfy (or deliberately violate) the relevant Silver schema so the
validation step remains meaningful.

Run all tests:
    pytest tests/test_transform_functions.py -v
Run a single test:
    pytest tests/test_transform_functions.py::test_transform_orders_happy_path -v
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Shared fixture factories
# ---------------------------------------------------------------------------

_ORDER_DEFAULTS: dict = {
    "order_id": 1,
    "order_code": "abc-def-123",
    "customer_id": 10,
    "order_status": "delivered",
    "order_date": pd.Timestamp("2017-06-01"),
    "order_timestamp": pd.Timestamp("2017-06-01 09:00"),
    "approved_at": pd.Timestamp("2017-06-01 10:00"),
    "estimated_delivery": pd.Timestamp("2017-06-10"),
    "actual_delivery": pd.Timestamp("2017-06-08"),
    "delivery_days_actual": 7,
    "delivery_days_estimated": 9,
    "source_channel": "online",
    "currency_code": "BRL",
    "ingested_at": pd.Timestamp("2017-06-02"),
}


def _make_orders(*overrides_list: dict) -> pd.DataFrame:
    """Return a DataFrame with one row per dict in *overrides_list*."""
    rows = []
    for i, overrides in enumerate(overrides_list):
        row = _ORDER_DEFAULTS.copy()
        row["order_id"] = i + 1
        row["order_code"] = f"abc-def-{i + 1:03d}"
        row.update(overrides)
        rows.append(row)
    return pd.DataFrame(rows)


def _make_weather(*overrides_list: dict) -> pd.DataFrame:
    """Return a minimal weather DataFrame, one row per dict."""
    defaults = {
        "city": "sao paulo",
        "state": "SP",
        "date": pd.Timestamp("2017-06-01"),
        "temp_max": 28.0,
        "temp_min": 18.0,
        "precipitation": 2.0,
        "windspeed": 12.0,
        "weathercode": 3.0,
    }
    rows = []
    for overrides in overrides_list:
        row = defaults.copy()
        row.update(overrides)
        rows.append(row)
    return pd.DataFrame(rows)


def _make_fx(*overrides_list: dict) -> pd.DataFrame:
    """Return a minimal FX DataFrame, one row per dict."""
    defaults = {
        "date": pd.Timestamp("2017-01-02"),
        "base_currency": "USD",
        "quote_currency": "BRL",
        "rate": 3.25,
    }
    rows = []
    for overrides in overrides_list:
        row = defaults.copy()
        row.update(overrides)
        rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Patch targets
# ---------------------------------------------------------------------------

_SALES_READ = "src.transform.transform_sales.read_latest_bronze_parquet"
_SALES_WRITE = "src.transform.transform_sales.write_silver"
_SALES_QUAR = "src.transform.transform_sales.quarantine_rows"

_WX_EXTRACT = "src.transform.transform_weather.extract_weather"
_WX_WRITE = "src.transform.transform_weather.write_silver"
_WX_QUAR = "src.transform.transform_weather.quarantine_rows"

_FX_EXTRACT = "src.transform.transform_fx.extract_fx_rates"
_FX_WRITE = "src.transform.transform_fx.write_silver"
_FX_QUAR = "src.transform.transform_fx.quarantine_rows"

_DUMMY_PATH = Path("/tmp/mock_silver.parquet")

# Explicit date ranges — avoid env-var resolution via get_pipeline_date_range()
_WX_START = "2017-01-01"
_WX_END = "2017-12-31"
_FX_START = "2017-01-02"  # Monday
_FX_END = "2017-01-08"    # Sunday


# ===========================================================================
# transform_orders tests
# ===========================================================================


@patch(_SALES_QUAR, return_value=None)
@patch(_SALES_WRITE, return_value=_DUMMY_PATH)
@patch(_SALES_READ)
def test_transform_orders_happy_path(
    mock_read: MagicMock,
    mock_write: MagicMock,
    mock_quar: MagicMock,
) -> None:
    """Three valid delivered orders must all pass through with 0 quarantined."""
    from src.transform.transform_sales import transform_orders

    mock_read.return_value = _make_orders(
        {"order_status": "delivered"},
        {"order_status": "delivered"},
        {"order_status": "delivered"},
    )

    valid_df, total_quarantined = transform_orders()

    assert len(valid_df) == 3, f"Expected 3 valid rows, got {len(valid_df)}"
    assert total_quarantined == 0
    mock_write.assert_called_once()
    mock_quar.assert_not_called()


@patch(_SALES_QUAR, return_value=None)
@patch(_SALES_WRITE, return_value=_DUMMY_PATH)
@patch(_SALES_READ)
def test_transform_orders_removes_canceled(
    mock_read: MagicMock,
    mock_write: MagicMock,
    mock_quar: MagicMock,
) -> None:
    """One delivered + one canceled → 1 valid, 1 quarantined."""
    from src.transform.transform_sales import transform_orders

    mock_read.return_value = _make_orders(
        {"order_status": "delivered"},
        {"order_status": "canceled"},
    )

    valid_df, total_quarantined = transform_orders()

    assert len(valid_df) == 1
    assert valid_df.iloc[0]["order_status"] == "delivered"
    assert total_quarantined == 1
    mock_quar.assert_called_once()
    reasons_arg: pd.Series = mock_quar.call_args[0][1]
    assert reasons_arg.iloc[0] == "canceled order excluded from Silver"


@patch(_SALES_QUAR, return_value=None)
@patch(_SALES_WRITE, return_value=_DUMMY_PATH)
@patch(_SALES_READ)
def test_transform_orders_removes_null_date(
    mock_read: MagicMock,
    mock_write: MagicMock,
    mock_quar: MagicMock,
) -> None:
    """A row with a null order_date must be quarantined."""
    from src.transform.transform_sales import transform_orders

    mock_read.return_value = _make_orders(
        {"order_date": pd.Timestamp("2017-06-01")},
        {"order_date": None},
    )

    valid_df, total_quarantined = transform_orders()

    assert len(valid_df) == 1
    assert total_quarantined == 1
    mock_quar.assert_called_once()
    reasons_arg: pd.Series = mock_quar.call_args[0][1]
    assert "null order_date" in reasons_arg.tolist()


@patch(_SALES_QUAR, return_value=None)
@patch(_SALES_WRITE, return_value=_DUMMY_PATH)
@patch(_SALES_READ)
def test_transform_orders_removes_null_customer(
    mock_read: MagicMock,
    mock_write: MagicMock,
    mock_quar: MagicMock,
) -> None:
    """A row with a null customer_id must be quarantined."""
    from src.transform.transform_sales import transform_orders

    mock_read.return_value = _make_orders(
        {"customer_id": 10},
        {"customer_id": None},
    )

    valid_df, total_quarantined = transform_orders()

    assert len(valid_df) == 1
    assert total_quarantined == 1
    mock_quar.assert_called_once()
    reasons_arg: pd.Series = mock_quar.call_args[0][1]
    assert "null customer_id" in reasons_arg.tolist()


@patch(_SALES_QUAR, return_value=None)
@patch(_SALES_WRITE, return_value=_DUMMY_PATH)
@patch(_SALES_READ)
def test_transform_orders_all_quarantined(
    mock_read: MagicMock,
    mock_write: MagicMock,
    mock_quar: MagicMock,
) -> None:
    """When every input row is invalid, valid_df must be empty."""
    from src.transform.transform_sales import transform_orders

    mock_read.return_value = _make_orders(
        {"order_status": "canceled"},
        {"order_status": "canceled"},
        {"order_status": "canceled"},
    )

    valid_df, total_quarantined = transform_orders()

    assert valid_df.empty
    assert total_quarantined == 3
    mock_write.assert_called_once()  # write_silver called even for empty valid_df
    mock_quar.assert_called_once()


# ===========================================================================
# transform_weather tests
# ===========================================================================


@patch(_WX_QUAR, return_value=None)
@patch(_WX_WRITE, return_value=_DUMMY_PATH)
@patch(_WX_EXTRACT)
def test_transform_weather_happy_path(
    mock_extract: MagicMock,
    mock_write: MagicMock,
    mock_quar: MagicMock,
) -> None:
    """Three valid weather rows within range must all pass through with 0 quarantined."""
    from src.transform.transform_weather import transform_weather

    mock_extract.return_value = _make_weather(
        {"city": "sao paulo", "state": "SP", "date": pd.Timestamp("2017-06-01")},
        {"city": "rio de janeiro", "state": "RJ", "date": pd.Timestamp("2017-06-02")},
        {"city": "brasilia", "state": "DF", "date": pd.Timestamp("2017-06-03")},
    )

    valid_df, total_quarantined = transform_weather(_WX_START, _WX_END)

    assert len(valid_df) == 3
    assert total_quarantined == 0
    mock_write.assert_called_once()
    mock_quar.assert_not_called()


@patch(_WX_QUAR, return_value=None)
@patch(_WX_WRITE, return_value=_DUMMY_PATH)
@patch(_WX_EXTRACT)
def test_transform_weather_empty_input_returns_early(
    mock_extract: MagicMock,
    mock_write: MagicMock,
    mock_quar: MagicMock,
) -> None:
    """An empty DataFrame from extract_weather must cause an early return of (empty, 0)."""
    from src.transform.transform_weather import transform_weather

    mock_extract.return_value = pd.DataFrame(
        columns=["city", "state", "date", "temp_max", "temp_min",
                 "precipitation", "windspeed", "weathercode"]
    )

    valid_df, total_quarantined = transform_weather(_WX_START, _WX_END)

    assert valid_df.empty
    assert total_quarantined == 0
    mock_write.assert_not_called()
    mock_quar.assert_not_called()


@patch(_WX_QUAR, return_value=None)
@patch(_WX_WRITE, return_value=_DUMMY_PATH)
@patch(_WX_EXTRACT)
def test_transform_weather_normalizes_city_names(
    mock_extract: MagicMock,
    mock_write: MagicMock,
    mock_quar: MagicMock,
) -> None:
    """City names must be lowercased and stripped: 'SAO PAULO' → 'sao paulo'."""
    from src.transform.transform_weather import transform_weather

    mock_extract.return_value = _make_weather(
        {"city": "SAO PAULO", "state": "SP", "date": pd.Timestamp("2017-06-01")},
    )

    valid_df, total_quarantined = transform_weather(_WX_START, _WX_END)

    assert len(valid_df) == 1
    assert valid_df.iloc[0]["city"] == "sao paulo"
    assert total_quarantined == 0


@patch(_WX_QUAR, return_value=None)
@patch(_WX_WRITE, return_value=_DUMMY_PATH)
@patch(_WX_EXTRACT)
def test_transform_weather_casts_weathercode_to_int64(
    mock_extract: MagicMock,
    mock_write: MagicMock,
    mock_quar: MagicMock,
) -> None:
    """weathercode must be cast from float64 to a non-float integer type."""
    from src.transform.transform_weather import transform_weather

    mock_extract.return_value = _make_weather(
        {"city": "sao paulo", "state": "SP", "date": pd.Timestamp("2017-03-15"),
         "weathercode": 61.0},
    )

    valid_df, total_quarantined = transform_weather(_WX_START, _WX_END)

    assert len(valid_df) == 1
    assert total_quarantined == 0
    wc_value = valid_df.iloc[0]["weathercode"]
    assert int(wc_value) == 61
    assert valid_df["weathercode"].dtype != "float64"


# ===========================================================================
# transform_fx tests
# ===========================================================================


@patch(_FX_QUAR, return_value=None)
@patch(_FX_WRITE, return_value=_DUMMY_PATH)
@patch(_FX_EXTRACT)
def test_transform_fx_happy_path(
    mock_extract: MagicMock,
    mock_write: MagicMock,
    mock_quar: MagicMock,
) -> None:
    """Five weekday trading rows must expand to 7 calendar rows after ffill."""
    from src.transform.transform_fx import transform_fx

    mock_extract.return_value = _make_fx(
        {"date": pd.Timestamp("2017-01-02"), "rate": 3.20},  # Mon
        {"date": pd.Timestamp("2017-01-03"), "rate": 3.21},  # Tue
        {"date": pd.Timestamp("2017-01-04"), "rate": 3.22},  # Wed
        {"date": pd.Timestamp("2017-01-05"), "rate": 3.23},  # Thu
        {"date": pd.Timestamp("2017-01-06"), "rate": 3.24},  # Fri
    )

    valid_df, total_quarantined = transform_fx(_FX_START, _FX_END)

    assert len(valid_df) == 7, f"Expected 7 rows after ffill, got {len(valid_df)}"
    assert total_quarantined == 0
    assert valid_df["rate"].isna().sum() == 0

    # Saturday (2017-01-07) must carry forward Friday's rate (3.24)
    sat_rate = valid_df.loc[
        valid_df["date"] == pd.Timestamp("2017-01-07"), "rate"
    ].iloc[0]
    assert sat_rate == pytest.approx(3.24)

    mock_write.assert_called_once()
    mock_quar.assert_not_called()


@patch(_FX_QUAR, return_value=None)
@patch(_FX_WRITE, return_value=_DUMMY_PATH)
@patch(_FX_EXTRACT)
def test_transform_fx_empty_returns_early(
    mock_extract: MagicMock,
    mock_write: MagicMock,
    mock_quar: MagicMock,
) -> None:
    """An empty DataFrame from extract_fx_rates must cause an early return of (empty, 0)."""
    from src.transform.transform_fx import transform_fx

    mock_extract.return_value = pd.DataFrame(
        columns=["date", "base_currency", "quote_currency", "rate"]
    )

    valid_df, total_quarantined = transform_fx(_FX_START, _FX_END)

    assert valid_df.empty
    assert total_quarantined == 0
    mock_write.assert_not_called()
    mock_quar.assert_not_called()


@patch(_FX_QUAR, return_value=None)
@patch(_FX_WRITE, return_value=_DUMMY_PATH)
@patch(_FX_EXTRACT)
def test_transform_fx_deduplicates_dates(
    mock_extract: MagicMock,
    mock_write: MagicMock,
    mock_quar: MagicMock,
) -> None:
    """Duplicate dates must be deduplicated, keeping the last occurrence's rate."""
    from src.transform.transform_fx import transform_fx

    mock_extract.return_value = _make_fx(
        {"date": pd.Timestamp("2017-01-02"), "rate": 3.10},  # stale
        {"date": pd.Timestamp("2017-01-03"), "rate": 3.20},
        {"date": pd.Timestamp("2017-01-02"), "rate": 3.15},  # fresh — must win
    )

    valid_df, total_quarantined = transform_fx("2017-01-02", "2017-01-03")

    assert len(valid_df) == 2
    assert valid_df["date"].duplicated().sum() == 0

    jan2_rate = valid_df.loc[
        valid_df["date"] == pd.Timestamp("2017-01-02"), "rate"
    ].iloc[0]
    assert jan2_rate == pytest.approx(3.15)
    assert total_quarantined == 0
