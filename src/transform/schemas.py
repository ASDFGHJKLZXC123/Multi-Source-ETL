"""
src/transform/schemas.py
------------------------
Pandera DataFrameSchema definitions for all Silver-layer outputs in the
Multi-Source ETL pipeline.  Each schema is used by ``validate_silver`` to
split an incoming DataFrame into a clean subset and a quarantine subset so
that downstream Gold-layer transforms never receive invalid rows.

Schema coverage
---------------
* SilverOrderSchema      – cleaned e-commerce orders (cancellations already
                           removed by the Bronze→Silver transform)
* SilverOrderItemSchema  – individual order line-items
* SilverWeatherSchema    – daily weather observations per city/state
* SilverFxSchema         – daily foreign-exchange spot rates
"""

from __future__ import annotations

import pandas as pd
import pandera as pa
import pandera.errors

from src.utils.logger import logger

# ---------------------------------------------------------------------------
# Allowed values
# ---------------------------------------------------------------------------

VALID_ORDER_STATUSES: frozenset[str] = frozenset(
    {
        "delivered",
        "shipped",
        "invoiced",
        "processing",
        "created",
        "approved",
        "unavailable",
    }
)
# Note: 'canceled' is intentionally excluded; cancellations are filtered out
# during the Bronze→Silver order transform before this schema is applied.

# ---------------------------------------------------------------------------
# Silver: Orders
# ---------------------------------------------------------------------------

SilverOrderSchema = pa.DataFrameSchema(
    columns={
        "order_id": pa.Column(
            pa.Int,
            nullable=False,
            description="Surrogate primary key for the order.",
        ),
        "order_code": pa.Column(
            pa.String,
            nullable=False,
            checks=pa.Check(
                lambda s: s.str.len() > 0,
                error="order_code must be a non-empty string (UUID expected)",
            ),
            description="Original UUID string from the source system.",
        ),
        "customer_id": pa.Column(
            pa.Int,
            nullable=False,
            description="Foreign key to the customer dimension.",
        ),
        "order_status": pa.Column(
            pa.String,
            nullable=False,
            checks=pa.Check.isin(VALID_ORDER_STATUSES),
            description="Current lifecycle status of the order.",
        ),
        "order_date": pa.Column(
            pa.DateTime,
            nullable=False,
            description="Calendar date on which the order was placed.",
        ),
        "order_timestamp": pa.Column(
            pa.DateTime,
            nullable=True,
            description="Full timestamp of the order (may be absent in older records).",
        ),
        "approved_at": pa.Column(
            pa.DateTime,
            nullable=True,
            description="Timestamp when payment/approval was confirmed.",
        ),
        "estimated_delivery": pa.Column(
            pa.DateTime,
            nullable=True,
            description="Promised delivery date communicated to the customer.",
        ),
        "actual_delivery": pa.Column(
            pa.DateTime,
            nullable=True,
            description="Date the order was actually delivered.",
        ),
        "delivery_days_actual": pa.Column(
            pa.Int,
            nullable=True,
            checks=pa.Check.ge(0),
            description="Number of calendar days from order to actual delivery.",
        ),
        "delivery_days_estimated": pa.Column(
            pa.Int,
            nullable=True,
            checks=pa.Check.ge(0),
            description="Number of calendar days from order to promised delivery.",
        ),
        "source_channel": pa.Column(
            pa.String,
            nullable=False,
            checks=pa.Check(
                lambda s: s.str.len() > 0,
                error="source_channel must be a non-empty string",
            ),
            description="Sales channel that originated the order (default 'online').",
        ),
        "currency_code": pa.Column(
            pa.String,
            nullable=False,
            checks=pa.Check(
                lambda s: s.str.len() == 3,
                error="currency_code must be exactly 3 characters (ISO 4217)",
            ),
            description="ISO 4217 currency code for order monetary values.",
        ),
        "ingested_at": pa.Column(
            pa.DateTime,
            nullable=False,
            description="Pipeline ingestion timestamp (set by the Bronze loader).",
        ),
    },
    coerce=True,
    name="SilverOrders",
)

# ---------------------------------------------------------------------------
# Silver: Order Items
# ---------------------------------------------------------------------------

SilverOrderItemSchema = pa.DataFrameSchema(
    columns={
        "order_item_id": pa.Column(
            pa.Int,
            nullable=False,
            description="Surrogate primary key for the order line-item.",
        ),
        "order_id": pa.Column(
            pa.Int,
            nullable=False,
            description="Foreign key to the parent order.",
        ),
        "product_id": pa.Column(
            pa.Int,
            nullable=False,
            description="Foreign key to the product dimension.",
        ),
        "store_id": pa.Column(
            pa.Int,
            nullable=False,
            description="Foreign key to the store/seller dimension.",
        ),
        "line_number": pa.Column(
            pa.Int,
            nullable=False,
            checks=pa.Check.ge(1),
            description="Position of this item within the order (1-based).",
        ),
        "unit_price": pa.Column(
            pa.Float,
            nullable=False,
            checks=pa.Check.gt(0),
            description="Selling price per unit in the order currency.",
        ),
        "freight_value": pa.Column(
            pa.Float,
            nullable=False,
            checks=pa.Check.ge(0),
            description="Shipping cost allocated to this line-item.",
        ),
        "quantity": pa.Column(
            pa.Int,
            nullable=False,
            checks=pa.Check.ge(1),
            description="Number of units ordered for this line-item.",
        ),
        "ingested_at": pa.Column(
            pa.DateTime,
            nullable=False,
            description="Pipeline ingestion timestamp (set by the Bronze loader).",
        ),
    },
    coerce=True,
    name="SilverOrderItems",
)

# ---------------------------------------------------------------------------
# Silver: Weather
# ---------------------------------------------------------------------------

SilverWeatherSchema = pa.DataFrameSchema(
    columns={
        "city": pa.Column(
            pa.String,
            nullable=False,
            checks=pa.Check(
                lambda s: s.str.len() > 0,
                error="city must be a non-empty string",
            ),
            description="City name corresponding to the weather observation.",
        ),
        "state": pa.Column(
            pa.String,
            nullable=False,
            checks=pa.Check(
                lambda s: s.str.len() == 2,
                error="state must be a 2-character abbreviation (e.g. 'SP')",
            ),
            description="2-character Brazilian state abbreviation.",
        ),
        "date": pa.Column(
            pa.DateTime,
            nullable=False,
            description="Calendar date of the weather observation.",
        ),
        "temp_max": pa.Column(
            pa.Float,
            nullable=True,
            checks=[
                pa.Check.ge(-50),
                pa.Check.le(60),
            ],
            description="Daily maximum temperature in degrees Celsius.",
        ),
        "temp_min": pa.Column(
            pa.Float,
            nullable=True,
            checks=[
                pa.Check.ge(-50),
                pa.Check.le(60),
            ],
            description="Daily minimum temperature in degrees Celsius.",
        ),
        "precipitation": pa.Column(
            pa.Float,
            nullable=True,
            checks=pa.Check.ge(0),
            description="Total daily precipitation in millimetres.",
        ),
        "windspeed": pa.Column(
            pa.Float,
            nullable=True,
            checks=pa.Check.ge(0),
            description="Mean daily wind speed in km/h.",
        ),
        "weathercode": pa.Column(
            # pa.Int with nullable=True and coerce=True correctly handles
            # pandas nullable Int64 (capital-I extension dtype) in pandera >= 0.18.
            pa.Int,
            nullable=True,
            checks=[
                pa.Check.ge(0),
                pa.Check.le(99),
            ],
            description="WMO weather interpretation code (0–99).",
        ),
    },
    coerce=True,
    name="SilverWeather",
)

# ---------------------------------------------------------------------------
# Silver: FX Rates
# ---------------------------------------------------------------------------

SilverFxSchema = pa.DataFrameSchema(
    columns={
        "date": pa.Column(
            pa.DateTime,
            nullable=False,
            description="Calendar date for this exchange-rate observation.",
        ),
        "base_currency": pa.Column(
            pa.String,
            nullable=False,
            checks=pa.Check(
                lambda s: s.str.len() == 3,
                error="base_currency must be exactly 3 characters (ISO 4217)",
            ),
            description="ISO 4217 code of the base currency (e.g. 'EUR').",
        ),
        "quote_currency": pa.Column(
            pa.String,
            nullable=False,
            checks=pa.Check(
                lambda s: s.str.len() == 3,
                error="quote_currency must be exactly 3 characters (ISO 4217)",
            ),
            description="ISO 4217 code of the quoted/target currency (e.g. 'BRL').",
        ),
        "rate": pa.Column(
            pa.Float,
            nullable=False,
            checks=pa.Check.gt(0),
            description="Spot exchange rate: 1 unit of base_currency in quote_currency.",
        ),
    },
    coerce=True,
    name="SilverFxRates",
)

# ---------------------------------------------------------------------------
# Validation helper
# ---------------------------------------------------------------------------


def validate_silver(
    df: pd.DataFrame,
    schema: pa.DataFrameSchema,
    transform_name: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate *df* against *schema* and split into valid/invalid subsets.

    Parameters
    ----------
    df:
        The Silver-layer DataFrame to validate.
    schema:
        One of the ``Silver*Schema`` objects defined in this module.
    transform_name:
        A short label used in log messages (e.g. ``"orders"``, ``"weather"``).

    Returns
    -------
    valid_df:
        Rows that satisfy every constraint in *schema*.  The DataFrame index
        is reset so downstream code can rely on a clean 0-based RangeIndex.
    invalid_df:
        Rows that violated at least one constraint.  An extra column
        ``quarantine_reason`` is appended that concatenates all failure
        descriptions for that row into a single pipe-separated string.
        Returns an empty DataFrame (with the same columns as *df* plus
        ``quarantine_reason``) when there are no violations.

    Notes
    -----
    ``lazy=True`` tells pandera to collect *all* schema errors before raising
    instead of stopping at the first failure.  This maximises the information
    captured in the quarantine table without requiring multiple validation
    passes.
    """
    try:
        validated = schema.validate(df, lazy=True)
        logger.info(
            "[{}] Silver validation passed — {:,} rows all valid.",
            transform_name,
            len(validated),
        )
        empty_invalid = df.iloc[:0].copy()
        empty_invalid["quarantine_reason"] = pd.Series(dtype="object")
        return validated.reset_index(drop=True), empty_invalid

    except pandera.errors.SchemaErrors as exc:
        error_report: pd.DataFrame = exc.failure_cases

        # ------------------------------------------------------------------
        # Build a mapping: original row index -> list of failure descriptions
        # ------------------------------------------------------------------
        # The failure_cases DataFrame from pandera (lazy validation) contains
        # at minimum the columns: 'schema_context', 'column', 'check',
        # 'check_number', 'failure_case', 'index'.
        # 'index' is the positional label of the failing row in *df*.
        # ------------------------------------------------------------------

        # Filter to row-level failures only (schema_context == 'Column').
        # DataFrameSchema-level checks use schema_context == 'DataFrameSchema'.
        row_failures = error_report[error_report["index"].notna()].copy()

        # Coerce 'index' to the same type as df.index for reliable .loc usage.
        row_failures["index"] = row_failures["index"].astype(df.index.dtype)

        # Group failure descriptions per row index.
        def _build_reason(group: pd.DataFrame) -> str:
            parts: list[str] = []
            for _, row in group.iterrows():
                col = row.get("column", "unknown_column")
                check = row.get("check", "unknown_check")
                case = row.get("failure_case", "")
                parts.append(f"{col}[{check}]={case!r}")
            return " | ".join(parts)

        reason_map: dict = (
            row_failures.groupby("index")
            .apply(_build_reason)
            .to_dict()
        )

        invalid_indices = set(reason_map.keys())
        valid_mask = ~df.index.isin(invalid_indices)

        valid_df = df.loc[valid_mask].reset_index(drop=True)
        invalid_df = df.loc[~valid_mask].copy()
        invalid_df["quarantine_reason"] = invalid_df.index.map(
            lambda idx: reason_map.get(idx, "unknown validation failure")
        )
        invalid_df = invalid_df.reset_index(drop=True)

        logger.warning(
            "[{}] Silver validation — {:,} valid rows, {:,} quarantined rows.",
            transform_name,
            len(valid_df),
            len(invalid_df),
        )
        logger.debug(
            "[{}] Quarantine sample:\n{}",
            transform_name,
            invalid_df[["quarantine_reason"]].head(10).to_string(),
        )

        return valid_df, invalid_df


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    "VALID_ORDER_STATUSES",
    "SilverOrderSchema",
    "SilverOrderItemSchema",
    "SilverWeatherSchema",
    "SilverFxSchema",
    "validate_silver",
]
