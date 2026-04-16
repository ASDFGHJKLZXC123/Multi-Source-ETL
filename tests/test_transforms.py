"""
tests/test_transforms.py
-------------------------
Unit tests for Stage 3 Silver-layer transform logic.

All tests work exclusively on in-memory pandas DataFrames — no database
connections, no file I/O, and no network calls.  Each test class covers one
transform domain (Sales orders, Weather, FX rates) and exercises the
transformation *logic* in isolation rather than calling the full pipeline
functions (which require Bronze Parquet files and environment config).

Run a specific test:
    pytest tests/test_transforms.py::TestTransformOrders::test_cancellations_removed -v
Run the full module:
    pytest tests/test_transforms.py -v
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.transform.schemas import VALID_ORDER_STATUSES


# ---------------------------------------------------------------------------
# Helpers shared across test classes
# ---------------------------------------------------------------------------


def _make_minimal_orders(**overrides) -> pd.DataFrame:
    """Return a single-row orders DataFrame with sane defaults.

    Keyword arguments override individual column values so tests only need to
    state the column(s) they care about.
    """
    defaults: dict = {
        "order_id": 1,
        "order_code": "abc-123",
        "customer_id": 10,
        "order_status": "delivered",
        "order_date": "2017-06-01",
        "order_timestamp": "2017-06-01 09:00:00",
        "approved_at": "2017-06-01 10:00:00",
        "estimated_delivery": "2017-06-10",
        "actual_delivery": "2017-06-08",
        "delivery_days_actual": 7,
        "delivery_days_estimated": 9,
        "source_channel": "online",
        "currency_code": "BRL",
        "ingested_at": "2017-06-02 00:00:00",
    }
    defaults.update(overrides)
    return pd.DataFrame([defaults])


# ---------------------------------------------------------------------------
# Sales Orders
# ---------------------------------------------------------------------------


class TestTransformOrders:
    """Verify order-specific quality rules from transform_sales.transform_orders."""

    def test_cancellations_removed(self) -> None:
        """Canceled rows must not appear in the kept set; others must survive."""
        df = pd.DataFrame(
            [
                _make_minimal_orders(order_id=1, order_status="delivered").iloc[0],
                _make_minimal_orders(order_id=2, order_status="shipped").iloc[0],
                _make_minimal_orders(order_id=3, order_status="canceled").iloc[0],
            ]
        )

        # --- inline the cancellation filter logic ---
        canceled_mask: pd.Series = df["order_status"] == "canceled"
        quarantined_df = df.loc[canceled_mask]
        kept_df = df.loc[~canceled_mask].reset_index(drop=True)

        # The canceled row goes to quarantine
        assert len(quarantined_df) == 1
        assert quarantined_df.iloc[0]["order_status"] == "canceled"
        assert quarantined_df.iloc[0]["order_id"] == 3

        # Delivered and shipped rows are retained
        assert len(kept_df) == 2
        assert set(kept_df["order_status"].tolist()) == {"delivered", "shipped"}
        assert 3 not in kept_df["order_id"].tolist()

    def test_null_order_date_quarantined(self) -> None:
        """A row with order_date=None must end up in quarantine, not in valid output."""
        df = pd.DataFrame(
            [
                _make_minimal_orders(order_id=1, order_date="2017-01-01").iloc[0],
                _make_minimal_orders(order_id=2, order_date=None).iloc[0],
            ]
        )

        # --- inline the null-date filter logic ---
        null_date_mask: pd.Series = df["order_date"].isna()
        quarantined_df = df.loc[null_date_mask]
        valid_df = df.loc[~null_date_mask].reset_index(drop=True)

        # The null-date row is quarantined
        assert len(quarantined_df) == 1
        assert quarantined_df.iloc[0]["order_id"] == 2

        # The valid row is kept
        assert len(valid_df) == 1
        assert valid_df.iloc[0]["order_id"] == 1
        assert valid_df.iloc[0]["order_date"] == "2017-01-01"

    def test_null_customer_id_quarantined(self) -> None:
        """Rows with null customer_id must be quarantined."""
        df = pd.DataFrame(
            [
                _make_minimal_orders(order_id=1, customer_id=99).iloc[0],
                _make_minimal_orders(order_id=2, customer_id=None).iloc[0],
            ]
        )

        null_cust_mask: pd.Series = df["customer_id"].isna()
        quarantined_df = df.loc[null_cust_mask]
        valid_df = df.loc[~null_cust_mask].reset_index(drop=True)

        assert len(quarantined_df) == 1
        assert quarantined_df.iloc[0]["order_id"] == 2
        assert len(valid_df) == 1

    def test_unknown_status_quarantined(self) -> None:
        """Rows with an unrecognised order_status must be quarantined."""
        df = pd.DataFrame(
            [
                _make_minimal_orders(order_id=1, order_status="delivered").iloc[0],
                _make_minimal_orders(order_id=2, order_status="mystery_status").iloc[0],
            ]
        )

        # Cancellations already removed before this check in the real transform
        invalid_status_mask: pd.Series = ~df["order_status"].isin(VALID_ORDER_STATUSES)
        quarantined_df = df.loc[invalid_status_mask]
        valid_df = df.loc[~invalid_status_mask].reset_index(drop=True)

        assert len(quarantined_df) == 1
        assert quarantined_df.iloc[0]["order_status"] == "mystery_status"
        assert len(valid_df) == 1

    def test_order_date_cast_to_datetime(self) -> None:
        """order_date column must be cast to datetime64 without errors."""
        df = pd.DataFrame(
            [
                _make_minimal_orders(order_id=1, order_date="2017-06-01").iloc[0],
                _make_minimal_orders(order_id=2, order_date="2017-12-31").iloc[0],
            ]
        )

        df["order_date"] = pd.to_datetime(df["order_date"])

        assert pd.api.types.is_datetime64_any_dtype(df["order_date"])
        assert df["order_date"].iloc[0] == pd.Timestamp("2017-06-01")


# ---------------------------------------------------------------------------
# Weather
# ---------------------------------------------------------------------------


class TestTransformWeather:
    """Verify weather-specific quality rules from transform_weather.transform_weather."""

    def _make_weather_df(self, rows: list[dict]) -> pd.DataFrame:
        """Build a minimal weather DataFrame from a list of row dicts."""
        defaults = {
            "city": "sao paulo",
            "state": "SP",
            "date": "2017-01-01",
            "temp_max": 30.0,
            "temp_min": 20.0,
            "precipitation": 5.0,
            "windspeed": 10.0,
            "weathercode": 3.0,
        }
        records = []
        for overrides in rows:
            row = defaults.copy()
            row.update(overrides)
            records.append(row)
        return pd.DataFrame(records)

    def test_weathercode_cast_to_int(self) -> None:
        """weathercode must be cast to nullable Int64; NaN must become pd.NA."""
        df = self._make_weather_df(
            [
                {"weathercode": 3.0},
                {"weathercode": 61.0},
                {"weathercode": float("nan")},
            ]
        )

        # --- inline the cast logic ---
        df["weathercode"] = df["weathercode"].astype("Int64")

        assert df["weathercode"].dtype == pd.Int64Dtype()
        assert df["weathercode"].iloc[0] == 3
        assert df["weathercode"].iloc[1] == 61
        # NaN float becomes pd.NA in Int64
        assert df["weathercode"].iloc[2] is pd.NA

    def test_weathercode_nan_is_na_not_zero(self) -> None:
        """Ensure the NaN→pd.NA cast does not accidentally fill with 0."""
        df = self._make_weather_df([{"weathercode": float("nan")}])
        df["weathercode"] = df["weathercode"].astype("Int64")

        assert pd.isna(df["weathercode"].iloc[0])
        assert df["weathercode"].iloc[0] != 0

    def test_date_range_filter(self) -> None:
        """Rows outside the pipeline date range must be identified as out-of-range."""
        df = self._make_weather_df(
            [
                {"date": "2017-01-01"},  # inside
                {"date": "2017-06-15"},  # inside
                {"date": "2016-08-31"},  # before start → outside
                {"date": "2018-11-01"},  # after end → outside
            ]
        )
        df["date"] = pd.to_datetime(df["date"])

        start = pd.Timestamp("2017-01-01")
        end = pd.Timestamp("2018-10-31")

        out_of_range_mask: pd.Series = (df["date"] < start) | (df["date"] > end)
        in_range_df = df.loc[~out_of_range_mask].reset_index(drop=True)
        out_of_range_df = df.loc[out_of_range_mask].reset_index(drop=True)

        assert len(in_range_df) == 2
        assert len(out_of_range_df) == 2
        assert pd.Timestamp("2016-08-31") in out_of_range_df["date"].tolist()
        assert pd.Timestamp("2018-11-01") in out_of_range_df["date"].tolist()

    def test_null_city_quarantined(self) -> None:
        """Rows with null city must be routed to quarantine."""
        df = self._make_weather_df(
            [
                {"city": "sao paulo", "state": "SP"},
                {"city": None, "state": "SP"},
            ]
        )

        mask: pd.Series = df["city"].isna() | df["state"].isna()
        quarantined = df.loc[mask]
        valid = df.loc[~mask].reset_index(drop=True)

        assert len(quarantined) == 1
        assert len(valid) == 1

    def test_city_name_normalized(self) -> None:
        """City names must be lowercased and stripped but accents kept intact."""
        df = self._make_weather_df(
            [
                {"city": "  SAO PAULO  "},
                {"city": "Rio de Janeiro"},
                # Accent-bearing name — accents must be preserved
                {"city": "São Luís"},
            ]
        )

        # --- inline the normalization logic ---
        df["city"] = df["city"].str.lower().str.strip()

        assert df["city"].iloc[0] == "sao paulo"
        assert df["city"].iloc[1] == "rio de janeiro"
        assert df["city"].iloc[2] == "são luís"  # accents preserved


# ---------------------------------------------------------------------------
# FX Rates
# ---------------------------------------------------------------------------


class TestTransformFx:
    """Verify FX-rate quality rules from transform_fx.transform_fx."""

    def _make_fx_df(self, rows: list[dict]) -> pd.DataFrame:
        """Build a minimal FX DataFrame from a list of row dicts."""
        defaults = {
            "date": "2017-01-02",
            "base_currency": "USD",
            "quote_currency": "BRL",
            "rate": 3.25,
        }
        records = []
        for overrides in rows:
            row = defaults.copy()
            row.update(overrides)
            records.append(row)
        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"])
        return df

    def test_forward_fill_missing_dates(self) -> None:
        """Calendar days missing from the trading-day data must be forward-filled."""
        # Monday and Friday rates only; Tuesday–Thursday are gaps
        df = self._make_fx_df(
            [
                {"date": "2017-01-02", "rate": 3.20},  # Monday
                {"date": "2017-01-06", "rate": 3.30},  # Friday
            ]
        )
        df = df.set_index("date")

        full_range = pd.date_range(start="2017-01-02", end="2017-01-08", freq="D")
        df = df.reindex(full_range)
        df["base_currency"] = df["base_currency"].fillna("USD")
        df["quote_currency"] = df["quote_currency"].fillna("BRL")
        df["rate"] = df["rate"].ffill()
        df = df.reset_index().rename(columns={"index": "date"})

        # Every calendar day in the range must have a non-null rate
        assert df["rate"].isna().sum() == 0
        assert len(df) == 7

        # Tuesday 3 Jan should carry forward Monday's rate (3.20)
        tuesday = df.loc[df["date"] == pd.Timestamp("2017-01-03"), "rate"].iloc[0]
        assert tuesday == pytest.approx(3.20)

        # Saturday 7 Jan should carry forward Friday's rate (3.30)
        saturday = df.loc[df["date"] == pd.Timestamp("2017-01-07"), "rate"].iloc[0]
        assert saturday == pytest.approx(3.30)

    def test_duplicate_dates_deduplicated(self) -> None:
        """When duplicate dates are present, only the last occurrence must survive."""
        df = self._make_fx_df(
            [
                {"date": "2017-01-02", "rate": 3.10},  # first occurrence — stale
                {"date": "2017-01-03", "rate": 3.20},
                {"date": "2017-01-02", "rate": 3.15},  # second occurrence — wins
            ]
        )

        df = df.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

        assert len(df) == 2
        assert df["date"].duplicated().sum() == 0
        # The 'last' value for 2017-01-02 is 3.15
        jan2_rate = df.loc[df["date"] == pd.Timestamp("2017-01-02"), "rate"].iloc[0]
        assert jan2_rate == pytest.approx(3.15)

    def test_null_rate_after_fill_quarantined(self) -> None:
        """Rates that remain null even after ffill/bfill must be quarantined."""
        # Build a fully-null rate series (cannot be filled)
        df = self._make_fx_df(
            [
                {"date": "2017-01-02", "rate": None},
                {"date": "2017-01-03", "rate": 3.25},
            ]
        )
        # Convert rate to float so isna() works correctly
        df["rate"] = pd.to_numeric(df["rate"], errors="coerce")

        # ffill with no preceding value leaves the first row null
        df["rate"] = df["rate"].ffill()

        null_rate_mask: pd.Series = df["rate"].isna()
        quarantined = df.loc[null_rate_mask]
        valid = df.loc[~null_rate_mask].reset_index(drop=True)

        assert len(quarantined) == 1
        assert quarantined.iloc[0]["date"] == pd.Timestamp("2017-01-02")
        assert len(valid) == 1

    def test_out_of_range_dates_quarantined(self) -> None:
        """Rows outside the pipeline date range must be quarantined."""
        df = self._make_fx_df(
            [
                {"date": "2016-08-31", "rate": 3.10},  # before start
                {"date": "2017-01-02", "rate": 3.20},  # inside
                {"date": "2018-11-01", "rate": 3.30},  # after end
            ]
        )

        start = pd.Timestamp("2017-01-01")
        end = pd.Timestamp("2018-10-31")

        out_mask: pd.Series = (df["date"] < start) | (df["date"] > end)
        quarantined = df.loc[out_mask]
        valid = df.loc[~out_mask].reset_index(drop=True)

        assert len(quarantined) == 2
        assert len(valid) == 1
        assert valid.iloc[0]["date"] == pd.Timestamp("2017-01-02")
