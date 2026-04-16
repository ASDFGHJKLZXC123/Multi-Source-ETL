"""
tests/test_schemas.py
---------------------
Unit tests for Silver-layer pandera schema validation via ``validate_silver``.

Tested schemas
--------------
* ``SilverOrderSchema``   — orders validation (status allow-list, delivery_days >= 0)
* ``SilverFxSchema``      — FX rate validation (rate strictly > 0)
* ``SilverWeatherSchema`` — weather validation (state must be 2-char abbreviation)

Each test constructs a minimal, fully in-memory DataFrame and calls
``validate_silver`` directly.  No file I/O, database connections, or network
calls are made.

Contract verified in every test
--------------------------------
* A valid row produces ``len(invalid_df) == 0`` (no quarantine output).
* An invalid row produces ``len(invalid_df) >= 1`` and the returned
  ``invalid_df`` contains a ``quarantine_reason`` column.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.transform.schemas import (
    SilverFxSchema,
    SilverOrderSchema,
    SilverWeatherSchema,
    validate_silver,
)


# ---------------------------------------------------------------------------
# Minimal row-builder helpers
# ---------------------------------------------------------------------------


def _make_order_row(**overrides) -> pd.DataFrame:
    """Return a single-row DataFrame satisfying ``SilverOrderSchema``."""
    defaults: dict = {
        "order_id": 1,
        "order_code": "abc-123",
        "customer_id": 10,
        "order_status": "delivered",
        "order_date": pd.Timestamp("2017-06-01"),
        "order_timestamp": pd.Timestamp("2017-06-01 09:00:00"),
        "approved_at": pd.Timestamp("2017-06-01 10:00:00"),
        "estimated_delivery": pd.Timestamp("2017-06-10"),
        "actual_delivery": pd.Timestamp("2017-06-08"),
        "delivery_days_actual": 7,
        "delivery_days_estimated": 9,
        "source_channel": "online",
        "currency_code": "BRL",
        "ingested_at": pd.Timestamp("2017-06-02 00:00:00"),
    }
    defaults.update(overrides)
    return pd.DataFrame([defaults])


def _make_fx_row(**overrides) -> pd.DataFrame:
    """Return a single-row DataFrame satisfying ``SilverFxSchema``."""
    defaults: dict = {
        "date": pd.Timestamp("2017-01-02"),
        "base_currency": "USD",
        "quote_currency": "BRL",
        "rate": 3.25,
    }
    defaults.update(overrides)
    return pd.DataFrame([defaults])


def _make_weather_row(**overrides) -> pd.DataFrame:
    """Return a single-row DataFrame satisfying ``SilverWeatherSchema``."""
    defaults: dict = {
        "city": "sao paulo",
        "state": "SP",
        "date": pd.Timestamp("2017-01-01"),
        "temp_max": 30.0,
        "temp_min": 18.0,
        "precipitation": 5.0,
        "windspeed": 10.0,
        "weathercode": pd.array([3], dtype="Int64")[0],
    }
    defaults.update(overrides)
    return pd.DataFrame([defaults])


# ===========================================================================
# SilverOrderSchema
# ===========================================================================


class TestSilverOrderSchema:
    """Tests for ``SilverOrderSchema`` via ``validate_silver``."""

    def test_valid_orders_passes(self, minimal_orders_df: pd.DataFrame) -> None:
        """A fully valid 3-row orders DataFrame must produce zero invalid rows."""
        valid_df, invalid_df = validate_silver(minimal_orders_df, SilverOrderSchema, "orders")

        assert len(invalid_df) == 0, (
            f"Expected 0 invalid rows; got {len(invalid_df)}. "
            f"Reasons: {list(invalid_df.get('quarantine_reason', []))}"
        )
        assert len(valid_df) == len(minimal_orders_df)

    def test_order_invalid_status_quarantined(self) -> None:
        """An order_status not in VALID_ORDER_STATUSES must be quarantined."""
        df = _make_order_row(order_status="mystery")
        _, invalid_df = validate_silver(df, SilverOrderSchema, "orders")

        assert len(invalid_df) >= 1
        assert "quarantine_reason" in invalid_df.columns

    def test_order_negative_delivery_days_quarantined(self) -> None:
        """delivery_days_actual=-1 violates Check.ge(0) and must be quarantined."""
        df = _make_order_row(delivery_days_actual=-1)
        _, invalid_df = validate_silver(df, SilverOrderSchema, "orders")

        assert len(invalid_df) >= 1
        assert "quarantine_reason" in invalid_df.columns

    def test_order_negative_estimated_delivery_days_quarantined(self) -> None:
        """delivery_days_estimated=-5 violates Check.ge(0) and must be quarantined."""
        df = _make_order_row(delivery_days_estimated=-5)
        _, invalid_df = validate_silver(df, SilverOrderSchema, "orders")

        assert len(invalid_df) >= 1
        assert "quarantine_reason" in invalid_df.columns

    def test_order_empty_order_code_quarantined(self) -> None:
        """An empty string order_code must fail the non-empty string check."""
        df = _make_order_row(order_code="")
        _, invalid_df = validate_silver(df, SilverOrderSchema, "orders")

        assert len(invalid_df) >= 1
        assert "quarantine_reason" in invalid_df.columns

    def test_order_invalid_currency_code_length_quarantined(self) -> None:
        """A currency_code that is not exactly 3 characters must be quarantined."""
        df = _make_order_row(currency_code="US")
        _, invalid_df = validate_silver(df, SilverOrderSchema, "orders")

        assert len(invalid_df) >= 1
        assert "quarantine_reason" in invalid_df.columns

    def test_order_valid_statuses_all_pass(self) -> None:
        """Every status in VALID_ORDER_STATUSES must pass schema validation."""
        from src.transform.schemas import VALID_ORDER_STATUSES

        for status in VALID_ORDER_STATUSES:
            df = _make_order_row(order_id=1, order_status=status)
            valid_df, invalid_df = validate_silver(df, SilverOrderSchema, f"orders_{status}")
            assert len(invalid_df) == 0, (
                f"Status '{status}' is in VALID_ORDER_STATUSES but was quarantined"
            )

    def test_validate_silver_invalid_df_always_has_quarantine_reason_column(self) -> None:
        """The quarantine_reason column must be present in invalid_df even when empty."""
        df = _make_order_row()
        _, invalid_df = validate_silver(df, SilverOrderSchema, "orders_col_check")

        assert "quarantine_reason" in invalid_df.columns


# ===========================================================================
# SilverFxSchema
# ===========================================================================


class TestSilverFxSchema:
    """Tests for ``SilverFxSchema`` via ``validate_silver``."""

    def test_fx_valid_row_passes(self, minimal_fx_df: pd.DataFrame) -> None:
        """A fully valid 5-row FX DataFrame must produce zero invalid rows."""
        valid_df, invalid_df = validate_silver(minimal_fx_df, SilverFxSchema, "fx")

        assert len(invalid_df) == 0, (
            f"Expected 0 invalid rows; got {len(invalid_df)}. "
            f"Reasons: {list(invalid_df.get('quarantine_reason', []))}"
        )
        assert len(valid_df) == len(minimal_fx_df)

    def test_fx_zero_rate_quarantined(self) -> None:
        """rate=0 violates Check.gt(0) and must be quarantined."""
        df = _make_fx_row(rate=0.0)
        _, invalid_df = validate_silver(df, SilverFxSchema, "fx")

        assert len(invalid_df) >= 1
        assert "quarantine_reason" in invalid_df.columns

    def test_fx_negative_rate_quarantined(self) -> None:
        """rate=-1 violates Check.gt(0) and must be quarantined."""
        df = _make_fx_row(rate=-1.0)
        _, invalid_df = validate_silver(df, SilverFxSchema, "fx")

        assert len(invalid_df) >= 1
        assert "quarantine_reason" in invalid_df.columns

    def test_fx_short_base_currency_quarantined(self) -> None:
        """A base_currency that is not exactly 3 characters must be quarantined."""
        df = _make_fx_row(base_currency="US")
        _, invalid_df = validate_silver(df, SilverFxSchema, "fx")

        assert len(invalid_df) >= 1
        assert "quarantine_reason" in invalid_df.columns

    def test_fx_short_quote_currency_quarantined(self) -> None:
        """A quote_currency that is not exactly 3 characters must be quarantined."""
        df = _make_fx_row(quote_currency="BR")
        _, invalid_df = validate_silver(df, SilverFxSchema, "fx")

        assert len(invalid_df) >= 1
        assert "quarantine_reason" in invalid_df.columns

    def test_fx_very_small_positive_rate_passes(self) -> None:
        """A very small but strictly positive rate must satisfy Check.gt(0)."""
        df = _make_fx_row(rate=1e-9)
        valid_df, invalid_df = validate_silver(df, SilverFxSchema, "fx")

        assert len(invalid_df) == 0


# ===========================================================================
# SilverWeatherSchema
# ===========================================================================


class TestSilverWeatherSchema:
    """Tests for ``SilverWeatherSchema`` via ``validate_silver``."""

    def test_weather_valid_row_passes(self, minimal_weather_df: pd.DataFrame) -> None:
        """A fully valid 3-row weather DataFrame must produce zero invalid rows."""
        valid_df, invalid_df = validate_silver(
            minimal_weather_df, SilverWeatherSchema, "weather"
        )

        assert len(invalid_df) == 0, (
            f"Expected 0 invalid rows; got {len(invalid_df)}. "
            f"Reasons: {list(invalid_df.get('quarantine_reason', []))}"
        )
        assert len(valid_df) == len(minimal_weather_df)

    def test_weather_bad_state_code_quarantined(self) -> None:
        """state='California' violates the len==2 check and must be quarantined."""
        df = _make_weather_row(state="California")
        _, invalid_df = validate_silver(df, SilverWeatherSchema, "weather")

        assert len(invalid_df) >= 1
        assert "quarantine_reason" in invalid_df.columns

    def test_weather_three_char_state_quarantined(self) -> None:
        """A 3-character state code must also violate the len==2 constraint."""
        df = _make_weather_row(state="SPX")
        _, invalid_df = validate_silver(df, SilverWeatherSchema, "weather")

        assert len(invalid_df) >= 1
        assert "quarantine_reason" in invalid_df.columns

    def test_weather_empty_city_quarantined(self) -> None:
        """An empty string city must fail the non-empty string check."""
        df = _make_weather_row(city="")
        _, invalid_df = validate_silver(df, SilverWeatherSchema, "weather")

        assert len(invalid_df) >= 1
        assert "quarantine_reason" in invalid_df.columns

    def test_weather_extreme_temp_max_quarantined(self) -> None:
        """temp_max=70 exceeds Check.le(60) and must be quarantined."""
        df = _make_weather_row(temp_max=70.0)
        _, invalid_df = validate_silver(df, SilverWeatherSchema, "weather")

        assert len(invalid_df) >= 1
        assert "quarantine_reason" in invalid_df.columns

    def test_weather_negative_precipitation_quarantined(self) -> None:
        """precipitation=-1 violates Check.ge(0) and must be quarantined."""
        df = _make_weather_row(precipitation=-1.0)
        _, invalid_df = validate_silver(df, SilverWeatherSchema, "weather")

        assert len(invalid_df) >= 1
        assert "quarantine_reason" in invalid_df.columns

    def test_weather_weathercode_out_of_range_quarantined(self) -> None:
        """weathercode=100 exceeds Check.le(99) and must be quarantined."""
        df = _make_weather_row()
        df["weathercode"] = pd.array([100], dtype="Int64")
        _, invalid_df = validate_silver(df, SilverWeatherSchema, "weather")

        assert len(invalid_df) >= 1
        assert "quarantine_reason" in invalid_df.columns

    def test_weather_null_weathercode_passes(self) -> None:
        """weathercode=NA is explicitly nullable in the schema and must pass."""
        df = _make_weather_row()
        df["weathercode"] = pd.array([pd.NA], dtype="Int64")
        valid_df, invalid_df = validate_silver(df, SilverWeatherSchema, "weather")

        assert len(invalid_df) == 0
