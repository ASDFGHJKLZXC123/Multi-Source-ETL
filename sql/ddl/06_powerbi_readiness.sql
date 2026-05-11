-- =============================================================
-- 06_powerbi_readiness.sql
-- Purpose : PostgreSQL readiness work for Power BI connection.
--
-- Sections
-- --------
--   1. Read-only role for Power BI (powerbi_reader)
--   2. Additional indexes not created by 04_gold_schema.sql
--      (FK indexes for new join paths + partial indexes on
--       nullable columns that 04 covers without partial conditions)
--   3. _loaded_at indexes on fact tables (incremental refresh)
--   4. Enriched sales view (pre-joined dimensions for BI import)
--   5. NUMERIC precision audit query
--   6. Write-privilege verification query
--
-- Safety: All statements use IF NOT EXISTS / CREATE OR REPLACE /
--         ALTER ROLE … SET — safe to re-run against a live database.
--
-- Index ownership note
-- --------------------
-- 04_gold_schema.sql owns the base FK indexes on fact_sales
-- (idx_fact_sales_date_key, idx_fact_sales_customer_key,
--  idx_fact_sales_product_key, idx_fact_sales_store_key) and the
-- dim geographic indexes.  This file adds only what 04 does not:
--   • currency_key index on fact_sales
--   • quote_currency_key index on fact_fx_rates
--   • date_key index on fact_weather_daily
--   • _loaded_at indexes on all three fact tables
--
-- Prerequisites
-- -------------
--   Run 04_gold_schema.sql first to ensure the analytics schema
--   and all dimension / fact tables exist.
--
-- Replace placeholders before running
-- ------------------------------------
--   <your_database>   — the target database name
--   <strong_password> — generated credential; store in a vault
-- =============================================================


-- =============================================================
-- 1. Read-only role — powerbi_reader
-- =============================================================

-- Create the login role if it does not exist.
-- DO block used because CREATE ROLE has no IF NOT EXISTS in PG < 16.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'powerbi_reader'
    ) THEN
        CREATE ROLE powerbi_reader
            WITH LOGIN
                 NOSUPERUSER
                 NOCREATEDB
                 NOCREATEROLE
                 -- NOINHERIT: ensures this role only has explicitly granted
                 -- privileges.  If added to a group role later, it will NOT
                 -- automatically inherit the group's grants.  This is
                 -- intentional — privilege escalation via group membership
                 -- is prevented for a dedicated BI service account.
                 NOINHERIT
                 CONNECTION LIMIT 10
                 PASSWORD '<strong_password>';

        RAISE NOTICE 'Role powerbi_reader created.';
    ELSE
        RAISE NOTICE 'Role powerbi_reader already exists — skipping CREATE.';
    END IF;
END;
$$;

-- Allow the role to connect to the target database.
GRANT CONNECT ON DATABASE etl_pipeline TO powerbi_reader;

-- Allow the role to see objects in the analytics schema.
GRANT USAGE ON SCHEMA analytics TO powerbi_reader;

-- Grant SELECT on all current analytics tables.
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO powerbi_reader;

-- Grant SELECT on all sequences (defensive; covers SERIAL columns).
GRANT SELECT ON ALL SEQUENCES IN SCHEMA analytics TO powerbi_reader;

-- Future-proof: any table or sequence created later in analytics
-- is automatically readable without a manual re-grant.
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics
    GRANT SELECT ON TABLES    TO powerbi_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA analytics
    GRANT SELECT ON SEQUENCES TO powerbi_reader;

-- Pin the search_path at the role level so every session resolves
-- unqualified names against analytics first.
ALTER ROLE powerbi_reader SET search_path = analytics, pg_catalog;

-- Set timezone to UTC to ensure consistent TIMESTAMPTZ presentation
-- for Power BI incremental refresh RangeStart / RangeEnd comparisons.
ALTER ROLE powerbi_reader SET timezone = 'UTC';

COMMENT ON ROLE powerbi_reader IS
    'Read-only Power BI service account. SELECT only on analytics schema. '
    'Created by 06_powerbi_readiness.sql.';


-- =============================================================
-- 2. Additional FK indexes
--    04_gold_schema.sql already owns idx_fact_sales_date_key,
--    idx_fact_sales_customer_key, idx_fact_sales_product_key,
--    idx_fact_sales_store_key, idx_fact_weather_state,
--    idx_fact_weather_city_state, and idx_fact_fx_rates_date_key.
--    This section adds only the indexes that 04 does not create.
-- =============================================================

-- fact_sales — currency_key (omitted from 04; added here)
CREATE INDEX IF NOT EXISTS idx_fact_sales_currency_key
    ON analytics.fact_sales (currency_key)
    WHERE currency_key IS NOT NULL;

-- fact_weather_daily — date_key lookup
-- (04 indexes city/state; this covers the fact→dim_date join)
CREATE INDEX IF NOT EXISTS idx_fact_weather_date_key
    ON analytics.fact_weather_daily (date_key);

-- fact_fx_rates — quote_currency_key
-- (PK covers date_key + base_currency_key; quote_currency alone is not)
CREATE INDEX IF NOT EXISTS idx_fact_fx_quote_currency
    ON analytics.fact_fx_rates (quote_currency_key);


-- =============================================================
-- 3. _loaded_at indexes on fact tables
--    Required for Power BI incremental refresh range scans.
--    Not created by 04_gold_schema.sql.
-- =============================================================

CREATE INDEX IF NOT EXISTS idx_fact_sales_loaded_at
    ON analytics.fact_sales (_loaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_fact_weather_loaded_at
    ON analytics.fact_weather_daily (_loaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_fact_fx_loaded_at
    ON analytics.fact_fx_rates (_loaded_at DESC);


-- =============================================================
-- 4. Enriched sales view
--    Pre-joins fact_sales to its four dimensions so Power BI can
--    import a single denormalised table for sales analysis.
--    All filters applied in Power BI fold to PostgreSQL SQL.
--
--    NOTE: This view does NOT include weather data.
--    Weather correlation requires a separate join path — see the
--    WeatherBridgeKey approach or TREATAS DAX pattern documented
--    in docs/stage8_powerbi.md Section 6.2.
-- =============================================================

CREATE OR REPLACE VIEW analytics.v_sales_enriched AS
SELECT
    fs.order_item_id,
    fs.order_id,
    fs.order_code,
    fs.line_number,
    fs.date_key,
    dd.year,
    dd.quarter,
    dd.month,
    dd.day_of_week,
    dd.is_weekend,
    dd.is_month_end,

    -- Customer geography
    dc.city             AS customer_city,
    dc.state            AS customer_state,

    -- Product
    dp.category_name_en AS category_english,
    dp.weight_g,

    -- Seller geography
    dst.state           AS seller_state,

    -- Measures
    fs.unit_price,
    fs.freight_value,
    fs.quantity,
    fs.delivery_days_actual,
    fs.delivery_days_estimated,
    fs.order_status,

    -- Audit
    fs._loaded_at

FROM  analytics.fact_sales     fs
JOIN  analytics.dim_date       dd  ON dd.date_key     = fs.date_key
LEFT JOIN analytics.dim_customer dc  ON dc.customer_key = fs.customer_key
LEFT JOIN analytics.dim_product  dp  ON dp.product_key  = fs.product_key
LEFT JOIN analytics.dim_store    dst ON dst.store_key   = fs.store_key;

-- Grant to powerbi_reader (view created after initial GRANT ALL, so explicit)
GRANT SELECT ON analytics.v_sales_enriched TO powerbi_reader;

COMMENT ON VIEW analytics.v_sales_enriched IS
    'Pre-joined sales view: fact_sales + dim_date + dim_customer + dim_product + dim_store. '
    'Designed for Power BI Import — all column filters fold to PostgreSQL SQL. '
    'For weather correlation see analytics.v_sales_with_weather; for USD reporting '
    'see analytics.v_sales_usd. Created by 06_powerbi_readiness.sql.';


-- =============================================================
-- 4b. analytics.v_sales_with_weather
--     Pre-joins fact_sales to dim_customer and fact_weather_daily so
--     the README's "weather is joined daily to orders by city" claim
--     is backed by a queryable artifact. Joins on (date_key,
--     normalized_city, state) because fact_weather_daily.city is
--     NFD-stripped lowercase per src.transform.transform_weather;
--     dim_customer.normalized_city is populated to match.
--     LEFT JOIN on weather preserves the ~5–8% of order-items in
--     cities without weather coverage (see Known Limitations).
-- =============================================================

CREATE OR REPLACE VIEW analytics.v_sales_with_weather AS
SELECT
    fs.order_item_id,
    fs.order_code,
    fs.date_key,
    fs.customer_key,
    fs.unit_price,
    fs.freight_value,
    fs.delivery_days_actual,
    dc.city             AS customer_city,
    dc.normalized_city  AS customer_city_normalized,
    dc.state            AS customer_state,
    fw.temp_max,
    fw.temp_min,
    fw.precipitation,
    fw.windspeed,
    fw.weathercode
FROM      analytics.fact_sales         fs
JOIN      analytics.dim_customer       dc USING (customer_key)
LEFT JOIN analytics.fact_weather_daily fw
       ON fw.date_key = fs.date_key
      AND fw.city     = dc.normalized_city
      AND fw.state    = dc.state;

GRANT SELECT ON analytics.v_sales_with_weather TO powerbi_reader;

COMMENT ON VIEW analytics.v_sales_with_weather IS
    'Pre-joined sales + customer + daily weather. LEFT JOIN on weather preserves '
    'sales rows where the customer city has no Open-Meteo coverage. Created by '
    '06_powerbi_readiness.sql; backs the "weather × sales" business question.';


-- =============================================================
-- 4c. analytics.v_sales_usd
--     Pre-joins fact_sales to fact_fx_rates for the USD/BRL pair so
--     the README's "FX rates are joined by order date to every
--     order-item" claim is backed by a queryable artifact.
--     Rate semantics: fact_fx_rates.rate where base_currency='USD'
--     and quote_currency='BRL' is "1 USD = rate BRL", so USD value
--     = BRL value / rate. LEFT JOIN with FX predicates in ON (not
--     WHERE) preserves sales rows on dates without FX (weekends/
--     holidays); usd_* columns are NULL for those rows.
-- =============================================================

CREATE OR REPLACE VIEW analytics.v_sales_usd AS
SELECT
    fs.order_item_id,
    fs.order_code,
    fs.date_key,
    fs.unit_price                                   AS unit_price_brl,
    fs.freight_value                                AS freight_value_brl,
    fx.rate                                         AS usd_to_brl_rate,
    ROUND((fs.unit_price    / fx.rate)::numeric, 4) AS unit_price_usd,
    ROUND((fs.freight_value / fx.rate)::numeric, 4) AS freight_value_usd
FROM      analytics.fact_sales    fs
LEFT JOIN analytics.fact_fx_rates fx
       ON fx.date_key       = fs.date_key
      AND fx.base_currency  = 'USD'
      AND fx.quote_currency = 'BRL';

GRANT SELECT ON analytics.v_sales_usd TO powerbi_reader;

COMMENT ON VIEW analytics.v_sales_usd IS
    'Pre-joined sales + USD/BRL FX rate. usd_* columns are NULL on dates '
    'outside Frankfurter''s trading-day coverage (weekends/holidays). '
    'Created by 06_powerbi_readiness.sql; backs the "USD-normalised reporting" '
    'business question.';


-- =============================================================
-- 5. NUMERIC precision audit
--    Verify that monetary columns have explicit precision/scale.
--    Expected: all rows show non-null numeric_precision.
--    Unconstrained NUMERIC (NULL precision) may cause silent
--    truncation in Power BI via Npgsql.
-- =============================================================

SELECT
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale
FROM information_schema.columns
WHERE table_schema = 'analytics'
  AND data_type    = 'numeric'
ORDER BY table_name, column_name;

-- Expected: numeric_precision IS NOT NULL for all rows.
-- If any row shows numeric_precision = NULL, update the column DDL:
--   ALTER TABLE analytics.<table>
--       ALTER COLUMN <col> TYPE NUMERIC(18, 4);


-- =============================================================
-- 6. Write-privilege verification
--    Must return zero rows after setup.
-- =============================================================

SELECT
    grantee,
    table_name,
    privilege_type
FROM information_schema.role_table_grants
WHERE grantee        = 'powerbi_reader'
  AND privilege_type IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE')
  AND table_schema   = 'analytics';

-- If this query returns any rows, revoke the write privileges:
--   REVOKE INSERT, UPDATE, DELETE, TRUNCATE
--       ON ALL TABLES IN SCHEMA analytics FROM powerbi_reader;
