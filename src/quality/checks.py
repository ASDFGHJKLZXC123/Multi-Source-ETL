"""
Data quality check functions for the Multi-Source ETL pipeline.

Provides generic, reusable check primitives (row count, null, uniqueness,
range, referential integrity, column comparison) plus table-specific check
suites for the three Gold fact tables:

    - analytics.fact_sales
    - analytics.fact_weather_daily
    - analytics.fact_fx_rates

All checks return one or more ``CheckResult`` dataclass instances.  SQL is
executed via raw psycopg2 connections obtained from
``src.utils.db.get_connection``; the SQLAlchemy ``engine`` parameter is
accepted by every function for API consistency but is not used internally.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from src.utils.db import get_connection
from src.utils.logger import logger

# ---------------------------------------------------------------------------
# Allowlist — protects every SQL f-string from table-name injection
# ---------------------------------------------------------------------------

#: All tables that quality checks are permitted to query.
_ANALYTICS_TABLES: frozenset[str] = frozenset(
    {
        "analytics.fact_sales",
        "analytics.fact_weather_daily",
        "analytics.fact_fx_rates",
        "analytics.dim_date",
        "analytics.dim_customer",
        "analytics.dim_product",
        "analytics.dim_store",
        "analytics.dim_currency",
    }
)


def _validate_table(table: str) -> None:
    """Raise ``ValueError`` if *table* is not an allowed analytics table.

    Guards all generic check functions against SQL injection via the table
    name parameter.  Only tables in ``_ANALYTICS_TABLES`` may be queried.

    Args:
        table: Fully-qualified table name to validate.

    Raises:
        ValueError: When *table* is not in the allowlist.
    """
    if table not in _ANALYTICS_TABLES:
        raise ValueError(
            f"Table {table!r} is not in the quality-check allowlist. "
            f"Allowed tables: {sorted(_ANALYTICS_TABLES)}"
        )


# ---------------------------------------------------------------------------
# CheckResult — the universal result container
# ---------------------------------------------------------------------------


@dataclass
class CheckResult:
    """Captures the outcome of a single data quality check.

    Attributes:
        check_name: Unique human-readable identifier, e.g.
            ``"fact_sales.row_count_threshold"``.
        table_name: Fully-qualified table being checked, e.g.
            ``"analytics.fact_sales"``.
        category: Broad check type — one of ``'row_count'``, ``'null_check'``,
            ``'uniqueness'``, ``'range'``, or ``'referential_integrity'``.
        severity: Impact level if the check fails — ``'INFO'``, ``'WARNING'``,
            or ``'CRITICAL'``.
        status: Result verdict — ``'PASS'`` or ``'FAIL'``.
        expected_value: Human-readable description of what was expected
            (``None`` when not applicable).
        actual_value: Human-readable description of what was observed
            (``None`` when not applicable).
        rows_affected: Number of rows that violated the check; ``0`` on PASS.
        message: Single-line human-readable summary of the outcome.
        metadata: Arbitrary supplementary context (thresholds, sample values,
            etc.).
    """

    check_name: str
    table_name: str
    category: str
    severity: str
    status: str
    expected_value: str | None
    actual_value: str | None
    rows_affected: int
    message: str
    metadata: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Generic check primitives
# ---------------------------------------------------------------------------


def check_row_count(
    engine,
    table: str,
    min_rows: int,
    severity: str = "CRITICAL",
    check_name: str | None = None,
) -> CheckResult:
    """Fail if the actual row count in *table* is below *min_rows*.

    Args:
        engine: SQLAlchemy engine — accepted for API consistency, not used.
        table: Fully-qualified table name, e.g. ``"analytics.fact_sales"``.
        min_rows: Minimum acceptable row count (inclusive).
        severity: Severity level if the check fails (default ``"CRITICAL"``).
        check_name: Override for the auto-generated check name.

    Returns:
        A single ``CheckResult`` with status ``'PASS'`` or ``'FAIL'``.
    """
    _validate_table(table)
    name = check_name or f"{table}.row_count_threshold"
    sql = f"SELECT COUNT(*) FROM {table}"

    logger.debug("Running check '{}' — SQL: {}", name, sql)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            actual: int = cur.fetchone()[0]

    status = "PASS" if actual >= min_rows else "FAIL"
    rows_affected = 0 if status == "PASS" else max(0, min_rows - actual)
    message = (
        f"Row count {actual:,} >= {min_rows:,} — OK"
        if status == "PASS"
        else f"Row count {actual:,} is below minimum {min_rows:,}"
    )

    logger.debug("Check '{}': {} (actual={}, min={})", name, status, actual, min_rows)

    return CheckResult(
        check_name=name,
        table_name=table,
        category="row_count",
        severity=severity,
        status=status,
        expected_value=f">= {min_rows:,}",
        actual_value=str(actual),
        rows_affected=rows_affected,
        message=message,
        metadata={"min_rows": min_rows, "actual_rows": actual},
    )


def check_no_nulls(
    engine,
    table: str,
    columns: list[str],
    severity: str = "CRITICAL",
) -> list[CheckResult]:
    """Return one ``CheckResult`` per column — fail if any NULL is found.

    Args:
        engine: SQLAlchemy engine — accepted for API consistency, not used.
        table: Fully-qualified table name.
        columns: Column names to check for NULLs.
        severity: Severity level if any check fails (default ``"CRITICAL"``).

    Returns:
        A list of ``CheckResult`` objects, one per column in *columns*.
    """
    _validate_table(table)
    results: list[CheckResult] = []

    for col in columns:
        name = f"{table}.no_nulls.{col}"
        sql = f'SELECT COUNT(*) FROM {table} WHERE "{col}" IS NULL'

        logger.debug("Running check '{}' — SQL: {}", name, sql)

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                null_count: int = cur.fetchone()[0]

        status = "PASS" if null_count == 0 else "FAIL"
        message = (
            f"Column '{col}' has no NULLs — OK"
            if status == "PASS"
            else f"Column '{col}' has {null_count:,} NULL value(s)"
        )

        logger.debug("Check '{}': {} (null_count={})", name, status, null_count)

        results.append(
            CheckResult(
                check_name=name,
                table_name=table,
                category="null_check",
                severity=severity,
                status=status,
                expected_value="0 NULLs",
                actual_value=f"{null_count} NULL(s)",
                rows_affected=null_count if status == "FAIL" else 0,
                message=message,
                metadata={"column": col, "null_count": null_count},
            )
        )

    return results


def check_uniqueness(
    engine,
    table: str,
    columns: list[str],
    severity: str = "CRITICAL",
    check_name: str | None = None,
) -> CheckResult:
    """Fail if duplicate rows exist on the given column set.

    For a single column the check counts rows where the value appears more
    than once.  For composite keys it groups on all columns and counts groups
    with more than one member.

    Args:
        engine: SQLAlchemy engine — accepted for API consistency, not used.
        table: Fully-qualified table name.
        columns: Column(s) that together form the candidate key.
        severity: Severity level if the check fails (default ``"CRITICAL"``).
        check_name: Override for the auto-generated check name.

    Returns:
        A single ``CheckResult`` with status ``'PASS'`` or ``'FAIL'``.
    """
    _validate_table(table)
    col_str = ", ".join(f'"{c}"' for c in columns)
    name = check_name or f"{table}.uniqueness.{'+'.join(columns)}"

    sql = (
        f"SELECT COUNT(*) FROM ("
        f"  SELECT {col_str} FROM {table} GROUP BY {col_str} HAVING COUNT(*) > 1"
        f") sub"
    )

    logger.debug("Running check '{}' — SQL: {}", name, sql)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            dup_groups: int = cur.fetchone()[0]

    status = "PASS" if dup_groups == 0 else "FAIL"
    message = (
        f"No duplicate rows on ({', '.join(columns)}) — OK"
        if status == "PASS"
        else f"{dup_groups:,} duplicate group(s) found on ({', '.join(columns)})"
    )

    logger.debug("Check '{}': {} (dup_groups={})", name, status, dup_groups)

    return CheckResult(
        check_name=name,
        table_name=table,
        category="uniqueness",
        severity=severity,
        status=status,
        expected_value="0 duplicate groups",
        actual_value=f"{dup_groups} duplicate group(s)",
        rows_affected=dup_groups if status == "FAIL" else 0,
        message=message,
        metadata={"key_columns": columns, "duplicate_groups": dup_groups},
    )


def check_value_range(
    engine,
    table: str,
    column: str,
    min_val: float | None = None,
    max_val: float | None = None,
    allow_null: bool = True,
    severity: str = "WARNING",
    check_name: str | None = None,
) -> CheckResult:
    """Fail if any non-null value in *column* falls outside [*min_val*, *max_val*].

    At least one of *min_val* or *max_val* must be provided.  NULL values in
    the column are ignored when *allow_null* is ``True`` (default).

    Args:
        engine: SQLAlchemy engine — accepted for API consistency, not used.
        table: Fully-qualified table name.
        column: Column to range-check.
        min_val: Lower bound (inclusive).  ``None`` means no lower bound.
        max_val: Upper bound (inclusive).  ``None`` means no upper bound.
        allow_null: When ``True``, NULLs are excluded from the check.
        severity: Severity level if the check fails (default ``"WARNING"``).
        check_name: Override for the auto-generated check name.

    Returns:
        A single ``CheckResult`` with status ``'PASS'`` or ``'FAIL'``.
    """
    _validate_table(table)
    bounds = []
    if min_val is not None:
        bounds.append(f">= {min_val}")
    if max_val is not None:
        bounds.append(f"<= {max_val}")
    bounds_label = " and ".join(bounds) if bounds else "unbounded"

    name = check_name or f"{table}.value_range.{column}"

    # Build the WHERE clause
    conditions: list[str] = [f'"{column}" IS NOT NULL']
    range_parts: list[str] = []
    if min_val is not None:
        range_parts.append(f'"{column}" < {min_val}')
    if max_val is not None:
        range_parts.append(f'"{column}" > {max_val}')

    if not range_parts:
        # Nothing to check — trivial PASS
        return CheckResult(
            check_name=name,
            table_name=table,
            category="range",
            severity=severity,
            status="PASS",
            expected_value=bounds_label,
            actual_value="0 out-of-range",
            rows_affected=0,
            message=f"No range bounds specified for '{column}' — trivial PASS",
            metadata={"column": column},
        )

    conditions.append(f"({' OR '.join(range_parts)})")
    where_clause = " AND ".join(conditions)
    sql = f"SELECT COUNT(*) FROM {table} WHERE {where_clause}"

    logger.debug("Running check '{}' — SQL: {}", name, sql)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            violation_count: int = cur.fetchone()[0]

    status = "PASS" if violation_count == 0 else "FAIL"
    message = (
        f"Column '{column}' values all within [{bounds_label}] — OK"
        if status == "PASS"
        else f"Column '{column}' has {violation_count:,} value(s) outside [{bounds_label}]"
    )

    logger.debug("Check '{}': {} (violations={})", name, status, violation_count)

    return CheckResult(
        check_name=name,
        table_name=table,
        category="range",
        severity=severity,
        status=status,
        expected_value=bounds_label,
        actual_value=f"{violation_count} out-of-range",
        rows_affected=violation_count if status == "FAIL" else 0,
        message=message,
        metadata={
            "column": column,
            "min_val": min_val,
            "max_val": max_val,
            "allow_null": allow_null,
            "violations": violation_count,
        },
    )


def check_referential_integrity(
    engine,
    fact_table: str,
    fk_column: str,
    dim_table: str,
    pk_column: str,
    severity: str = "WARNING",
    check_name: str | None = None,
) -> CheckResult:
    """Fail if any non-null FK value has no matching PK in the dimension.

    Args:
        engine: SQLAlchemy engine — accepted for API consistency, not used.
        fact_table: Fully-qualified fact table name.
        fk_column: Foreign key column in the fact table.
        dim_table: Fully-qualified dimension table name.
        pk_column: Primary key column in the dimension table.
        severity: Severity level if the check fails (default ``"WARNING"``).
        check_name: Override for the auto-generated check name.

    Returns:
        A single ``CheckResult`` with status ``'PASS'`` or ``'FAIL'``.
    """
    _validate_table(fact_table)
    _validate_table(dim_table)
    name = check_name or f"{fact_table}.ri.{fk_column}->{dim_table}.{pk_column}"
    sql = (
        f"SELECT COUNT(*) FROM {fact_table} f "
        f'LEFT JOIN {dim_table} d ON f."{fk_column}" = d."{pk_column}" '
        f'WHERE d."{pk_column}" IS NULL AND f."{fk_column}" IS NOT NULL'
    )

    logger.debug("Running check '{}' — SQL: {}", name, sql)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            orphan_count: int = cur.fetchone()[0]

    status = "PASS" if orphan_count == 0 else "FAIL"
    message = (
        f"All non-null '{fk_column}' values match '{dim_table}.{pk_column}' — OK"
        if status == "PASS"
        else (
            f"{orphan_count:,} row(s) in '{fact_table}' have '{fk_column}' values "
            f"with no match in '{dim_table}.{pk_column}'"
        )
    )

    logger.debug("Check '{}': {} (orphans={})", name, status, orphan_count)

    return CheckResult(
        check_name=name,
        table_name=fact_table,
        category="referential_integrity",
        severity=severity,
        status=status,
        expected_value="0 orphan FK values",
        actual_value=f"{orphan_count} orphan(s)",
        rows_affected=orphan_count if status == "FAIL" else 0,
        message=message,
        metadata={
            "fk_column": fk_column,
            "dim_table": dim_table,
            "pk_column": pk_column,
            "orphan_count": orphan_count,
        },
    )


def check_column_gt_column(
    engine,
    table: str,
    col_a: str,
    col_b: str,
    allow_null: bool = True,
    severity: str = "WARNING",
    check_name: str | None = None,
) -> CheckResult:
    """Fail if ``col_a < col_b`` for any row (e.g. temp_max < temp_min).

    Rows where either column is NULL are excluded from the check when
    *allow_null* is ``True`` (default).

    Args:
        engine: SQLAlchemy engine — accepted for API consistency, not used.
        table: Fully-qualified table name.
        col_a: Column that must be greater than or equal to *col_b*.
        col_b: Column that must be less than or equal to *col_a*.
        allow_null: When ``True``, rows with NULL in either column are skipped.
        severity: Severity level if the check fails (default ``"WARNING"``).
        check_name: Override for the auto-generated check name.

    Returns:
        A single ``CheckResult`` with status ``'PASS'`` or ``'FAIL'``.
    """
    _validate_table(table)
    name = check_name or f"{table}.col_order.{col_a}>={col_b}"
    sql = (
        f"SELECT COUNT(*) FROM {table} "
        f'WHERE "{col_a}" IS NOT NULL AND "{col_b}" IS NOT NULL '
        f'AND "{col_a}" < "{col_b}"'
    )

    logger.debug("Running check '{}' — SQL: {}", name, sql)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            violation_count: int = cur.fetchone()[0]

    status = "PASS" if violation_count == 0 else "FAIL"
    message = (
        f"'{col_a}' >= '{col_b}' for all non-null rows — OK"
        if status == "PASS"
        else f"{violation_count:,} row(s) where '{col_a}' < '{col_b}'"
    )

    logger.debug("Check '{}': {} (violations={})", name, status, violation_count)

    return CheckResult(
        check_name=name,
        table_name=table,
        category="range",
        severity=severity,
        status=status,
        expected_value=f"{col_a} >= {col_b}",
        actual_value=f"{violation_count} violation(s)",
        rows_affected=violation_count if status == "FAIL" else 0,
        message=message,
        metadata={
            "col_a": col_a,
            "col_b": col_b,
            "allow_null": allow_null,
            "violations": violation_count,
        },
    )


# ---------------------------------------------------------------------------
# Table-specific check suites
# ---------------------------------------------------------------------------


def fact_sales_checks(engine) -> list[CheckResult]:
    """Run all data quality checks for ``analytics.fact_sales``.

    Checks performed:

    1. Row count >= 100,000 (CRITICAL).
    2. No NULLs on ``order_item_id``, ``order_id``, ``date_key`` (CRITICAL).
    3. Uniqueness on ``order_item_id`` (CRITICAL).
    4. ``unit_price`` > 0 (CRITICAL); ``freight_value`` >= 0 (WARNING);
       ``quantity`` >= 1 (WARNING).
    5. Referential integrity: ``date_key`` → ``analytics.dim_date.date_key``
       (WARNING).
    6. Referential integrity: ``customer_key`` →
       ``analytics.dim_customer.customer_key`` (INFO — nullable column,
       advisory only).

    Args:
        engine: SQLAlchemy engine — accepted for API consistency, not used.

    Returns:
        Combined list of ``CheckResult`` objects for this fact table.
    """
    table = "analytics.fact_sales"
    results: list[CheckResult] = []

    # 1 — Row count
    results.append(
        check_row_count(
            engine,
            table=table,
            min_rows=100_000,
            severity="CRITICAL",
            check_name="fact_sales.row_count_threshold",
        )
    )

    # 2 — No NULLs on critical columns
    results.extend(
        check_no_nulls(
            engine,
            table=table,
            columns=["order_item_id", "order_id", "date_key"],
            severity="CRITICAL",
        )
    )

    # 3 — Uniqueness on order_item_id (PK)
    results.append(
        check_uniqueness(
            engine,
            table=table,
            columns=["order_item_id"],
            severity="CRITICAL",
            check_name="fact_sales.uniqueness.order_item_id",
        )
    )

    # 4a — unit_price > 0
    results.append(
        check_value_range(
            engine,
            table=table,
            column="unit_price",
            min_val=0.000001,  # strictly greater than 0 via min just above 0
            severity="CRITICAL",
            check_name="fact_sales.range.unit_price_gt_0",
        )
    )

    # 4b — freight_value >= 0
    results.append(
        check_value_range(
            engine,
            table=table,
            column="freight_value",
            min_val=0,
            severity="WARNING",
            check_name="fact_sales.range.freight_value_gte_0",
        )
    )

    # 4c — quantity >= 1
    results.append(
        check_value_range(
            engine,
            table=table,
            column="quantity",
            min_val=1,
            severity="WARNING",
            check_name="fact_sales.range.quantity_gte_1",
        )
    )

    # 5 — RI: date_key → dim_date
    results.append(
        check_referential_integrity(
            engine,
            fact_table=table,
            fk_column="date_key",
            dim_table="analytics.dim_date",
            pk_column="date_key",
            severity="WARNING",
            check_name="fact_sales.ri.date_key->dim_date",
        )
    )

    # 6 — RI: customer_key → dim_customer (nullable, advisory INFO)
    results.append(
        check_referential_integrity(
            engine,
            fact_table=table,
            fk_column="customer_key",
            dim_table="analytics.dim_customer",
            pk_column="customer_key",
            severity="INFO",
            check_name="fact_sales.ri.customer_key->dim_customer",
        )
    )

    return results


def fact_weather_daily_checks(engine) -> list[CheckResult]:
    """Run all data quality checks for ``analytics.fact_weather_daily``.

    Checks performed:

    1. Row count >= 1,000 (CRITICAL).
    2. No NULLs on ``date_key``, ``city``, ``state`` (CRITICAL).
    3. Uniqueness on ``(date_key, city, state)`` composite key (CRITICAL).
    4. ``temp_max`` >= ``temp_min`` where both non-null (WARNING).
    5. ``weathercode`` in [0, 99] where non-null (WARNING).
    6. Referential integrity: ``date_key`` → ``analytics.dim_date.date_key``
       (WARNING).

    Args:
        engine: SQLAlchemy engine — accepted for API consistency, not used.

    Returns:
        Combined list of ``CheckResult`` objects for this fact table.
    """
    table = "analytics.fact_weather_daily"
    results: list[CheckResult] = []

    # 1 — Row count
    results.append(
        check_row_count(
            engine,
            table=table,
            min_rows=1_000,
            severity="CRITICAL",
            check_name="fact_weather_daily.row_count_threshold",
        )
    )

    # 2 — No NULLs on grain columns
    results.extend(
        check_no_nulls(
            engine,
            table=table,
            columns=["date_key", "city", "state"],
            severity="CRITICAL",
        )
    )

    # 3 — Uniqueness on composite key
    results.append(
        check_uniqueness(
            engine,
            table=table,
            columns=["date_key", "city", "state"],
            severity="CRITICAL",
            check_name="fact_weather_daily.uniqueness.date_key+city+state",
        )
    )

    # 4 — temp_max >= temp_min
    results.append(
        check_column_gt_column(
            engine,
            table=table,
            col_a="temp_max",
            col_b="temp_min",
            allow_null=True,
            severity="WARNING",
            check_name="fact_weather_daily.col_order.temp_max>=temp_min",
        )
    )

    # 5 — weathercode in [0, 99]
    results.append(
        check_value_range(
            engine,
            table=table,
            column="weathercode",
            min_val=0,
            max_val=99,
            allow_null=True,
            severity="WARNING",
            check_name="fact_weather_daily.range.weathercode_0_99",
        )
    )

    # 6 — RI: date_key → dim_date
    results.append(
        check_referential_integrity(
            engine,
            fact_table=table,
            fk_column="date_key",
            dim_table="analytics.dim_date",
            pk_column="date_key",
            severity="WARNING",
            check_name="fact_weather_daily.ri.date_key->dim_date",
        )
    )

    return results


def fact_fx_rates_checks(engine) -> list[CheckResult]:
    """Run all data quality checks for ``analytics.fact_fx_rates``.

    Checks performed:

    1. Row count >= 500 (CRITICAL).
    2. No NULLs on ``date_key``, ``base_currency``, ``quote_currency``,
       ``rate`` (CRITICAL).
    3. Uniqueness on ``(date_key, base_currency, quote_currency)`` (CRITICAL).
    4. ``rate`` > 0 (CRITICAL).
    5. Referential integrity: ``date_key`` → ``analytics.dim_date.date_key``
       (WARNING).

    Args:
        engine: SQLAlchemy engine — accepted for API consistency, not used.

    Returns:
        Combined list of ``CheckResult`` objects for this fact table.
    """
    table = "analytics.fact_fx_rates"
    results: list[CheckResult] = []

    # 1 — Row count
    results.append(
        check_row_count(
            engine,
            table=table,
            min_rows=500,
            severity="CRITICAL",
            check_name="fact_fx_rates.row_count_threshold",
        )
    )

    # 2 — No NULLs on mandatory columns
    results.extend(
        check_no_nulls(
            engine,
            table=table,
            columns=["date_key", "base_currency", "quote_currency", "rate"],
            severity="CRITICAL",
        )
    )

    # 3 — Uniqueness on grain key
    results.append(
        check_uniqueness(
            engine,
            table=table,
            columns=["date_key", "base_currency", "quote_currency"],
            severity="CRITICAL",
            check_name="fact_fx_rates.uniqueness.date_key+base_currency+quote_currency",
        )
    )

    # 4 — rate > 0
    results.append(
        check_value_range(
            engine,
            table=table,
            column="rate",
            min_val=0.000001,  # strictly greater than 0
            severity="CRITICAL",
            check_name="fact_fx_rates.range.rate_gt_0",
        )
    )

    # 5 — RI: date_key → dim_date
    results.append(
        check_referential_integrity(
            engine,
            fact_table=table,
            fk_column="date_key",
            dim_table="analytics.dim_date",
            pk_column="date_key",
            severity="WARNING",
            check_name="fact_fx_rates.ri.date_key->dim_date",
        )
    )

    return results


def run_all_checks(engine) -> list[CheckResult]:
    """Run all three fact check suites and return the combined results.

    Executes checks for ``fact_sales``, ``fact_weather_daily``, and
    ``fact_fx_rates`` in order and concatenates their ``CheckResult`` lists.

    Args:
        engine: SQLAlchemy engine passed through to each suite function.

    Returns:
        Flat list of all ``CheckResult`` objects across all three suites.
    """
    logger.info("Starting full data quality check run across all fact tables")

    all_results: list[CheckResult] = []

    all_results.extend(fact_sales_checks(engine))
    logger.info(
        "fact_sales checks complete — {} result(s)",
        len([r for r in all_results]),
    )

    weather_results = fact_weather_daily_checks(engine)
    all_results.extend(weather_results)
    logger.info("fact_weather_daily checks complete")

    fx_results = fact_fx_rates_checks(engine)
    all_results.extend(fx_results)
    logger.info("fact_fx_rates checks complete")

    pass_count = sum(1 for r in all_results if r.status == "PASS")
    fail_count = sum(1 for r in all_results if r.status == "FAIL")
    logger.info(
        "All checks done — total: {}, PASS: {}, FAIL: {}",
        len(all_results),
        pass_count,
        fail_count,
    )

    return all_results


__all__ = [
    "CheckResult",
    "check_row_count",
    "check_no_nulls",
    "check_uniqueness",
    "check_value_range",
    "check_referential_integrity",
    "check_column_gt_column",
    "fact_sales_checks",
    "fact_weather_daily_checks",
    "fact_fx_rates_checks",
    "run_all_checks",
]
