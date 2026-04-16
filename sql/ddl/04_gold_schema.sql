-- =============================================================
-- sql/ddl/04_gold_schema.sql
--
-- Purpose  : Gold layer star-schema DDL for the analytics schema.
--            Companion to the Gold Parquet files written under
--            data/gold/ by the Python ETL pipeline.  Operators
--            load those Parquet files into these tables for BI
--            tool consumption (e.g. Power BI, Tableau, Metabase).
--
-- Stage    : Gold — analytics-ready, denormalised star schema.
--
-- Run order: Execute AFTER 00_init.sql (which creates the
--            analytics schema) and AFTER 03_source_system.sql.
--            Safe to re-run — every statement is idempotent via
--            IF NOT EXISTS / CREATE INDEX IF NOT EXISTS.
--
-- Load order (enforced by foreign-key dependencies):
--   1. analytics.dim_date          (no upstream Gold dependency)
--   2. analytics.dim_customer      (no upstream Gold dependency)
--   3. analytics.dim_product       (no upstream Gold dependency)
--   4. analytics.dim_store         (no upstream Gold dependency)
--   5. analytics.dim_currency      (no upstream Gold dependency)
--   6. analytics.fact_sales        (FK -> all five dimensions)
--   7. analytics.fact_weather_daily (FK -> dim_date)
--   8. analytics.fact_fx_rates     (FK -> dim_date, dim_currency x2)
--
-- Design principles:
--   • dim_date uses YYYYMMDD integer keys — self-documenting and
--     efficient as partition predicates without a lookup join
--   • All other dimensions use SERIAL surrogate keys — compact,
--     join-friendly integers consistent with source_system style
--   • Business keys preserved as *_code VARCHAR(32) UNIQUE columns
--     so ETL joins remain traceable to the source system
--   • SCD Type 1: all entity dimensions overwrite on each run;
--     no history columns maintained here
--   • Monetary values use NUMERIC(12,2); FX rates NUMERIC(16,6)
--     for precision across currency pairs
--   • All tables use IF NOT EXISTS — safe to run on existing DBs
-- =============================================================

CREATE SCHEMA IF NOT EXISTS analytics;


-- ------------------------------------------------------------
-- 1. analytics.dim_date
--    Calendar spine generated from the pipeline date range.
--    Surrogate key is YYYYMMDD integer (e.g. 20170601) — the
--    standard DW pattern: self-describing and usable directly
--    as a partition predicate without a join.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key        INT             PRIMARY KEY,
    date            DATE            NOT NULL,
    year            SMALLINT        NOT NULL,
    quarter         SMALLINT        NOT NULL,
    month           SMALLINT        NOT NULL,
    week            SMALLINT        NOT NULL,
    day_of_month    SMALLINT        NOT NULL,
    day_of_week     SMALLINT        NOT NULL,
    day_name        VARCHAR(10)     NOT NULL,
    month_name      VARCHAR(10)     NOT NULL,
    quarter_name    CHAR(2)         NOT NULL,
    is_weekend      BOOLEAN         NOT NULL,
    is_month_end    BOOLEAN         NOT NULL,
    _loaded_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE analytics.dim_date IS
    'Calendar spine. One row per day across the pipeline date range. '
    'date_key is YYYYMMDD integer — self-describing and usable directly as a partition predicate.';
COMMENT ON COLUMN analytics.dim_date.date_key IS
    'Surrogate key as YYYYMMDD integer (e.g. 20170601). Used as FK in all fact tables.';
COMMENT ON COLUMN analytics.dim_date.date IS
    'Calendar date value (DATE type). Matches the date_key encoded date exactly.';
COMMENT ON COLUMN analytics.dim_date.year IS
    'Four-digit calendar year (e.g. 2017).';
COMMENT ON COLUMN analytics.dim_date.quarter IS
    'Calendar quarter number: 1 = Jan–Mar, 2 = Apr–Jun, 3 = Jul–Sep, 4 = Oct–Dec.';
COMMENT ON COLUMN analytics.dim_date.month IS
    'Calendar month number 1–12.';
COMMENT ON COLUMN analytics.dim_date.week IS
    'ISO week-of-year number 1–53 (ISO 8601).';
COMMENT ON COLUMN analytics.dim_date.day_of_month IS
    'Day within the month, 1–31.';
COMMENT ON COLUMN analytics.dim_date.day_of_week IS
    'Day of the week: 0 = Monday … 6 = Sunday (pandas/ISO convention).';
COMMENT ON COLUMN analytics.dim_date.day_name IS
    'Full English day name (e.g. Monday, Tuesday).';
COMMENT ON COLUMN analytics.dim_date.month_name IS
    'Full English month name (e.g. January, February).';
COMMENT ON COLUMN analytics.dim_date.quarter_name IS
    'Quarter label: Q1, Q2, Q3, or Q4.';
COMMENT ON COLUMN analytics.dim_date.is_weekend IS
    'TRUE when day_of_week >= 5 (Saturday or Sunday).';
COMMENT ON COLUMN analytics.dim_date.is_month_end IS
    'TRUE when the date is the last calendar day of the month.';
COMMENT ON COLUMN analytics.dim_date._loaded_at IS
    'Pipeline load timestamp. Set by the Python loader at the start of each load run for all rows in that batch.';


-- ------------------------------------------------------------
-- 2. analytics.dim_customer
--    One row per unique customer (SCD Type 1, overwrite).
--    customer_code is the business key (maps to
--    source_system.customers.customer_code / olist
--    customer_unique_id).  customer_id is the source-system
--    surrogate preserved for ETL join traceability.
-- ------------------------------------------------------------
-- NOTE: SERIAL is used here for schema clarity; the ETL always does a
-- TRUNCATE + INSERT (not an incremental INSERT), so the sequence value is
-- never used at load time — Python assigns 1-based range keys explicitly.
-- If incremental inserts are ever introduced, switch to INT PRIMARY KEY.
CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_key    SERIAL          PRIMARY KEY,
    customer_id     INT             NOT NULL,
    customer_code   VARCHAR(32)     NOT NULL,
    zip_code_prefix VARCHAR(8),
    city            VARCHAR(100),
    state           CHAR(2),
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    _loaded_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_customer_code UNIQUE (customer_code)
);

COMMENT ON TABLE analytics.dim_customer IS
    'Customer dimension (SCD Type 1). One row per unique shopper. '
    'Rebuilt from Bronze on every pipeline run using the last-occurrence deduplication strategy.';
COMMENT ON COLUMN analytics.dim_customer.customer_key IS
    'Gold surrogate key (1-based SERIAL). Used as FK in fact_sales.';
COMMENT ON COLUMN analytics.dim_customer.customer_id IS
    'Source-system surrogate from source_system.customers.customer_id. '
    'Retained for ETL join traceability — not exposed to BI consumers.';
COMMENT ON COLUMN analytics.dim_customer.customer_code IS
    'Business key. Maps to olist customer_unique_id (one row per physical person).';
COMMENT ON COLUMN analytics.dim_customer.zip_code_prefix IS
    'First 5 digits of the Brazilian CEP postal code.';
COMMENT ON COLUMN analytics.dim_customer.state IS
    'Two-letter Brazilian state abbreviation (e.g. SP, RJ, MG).';
COMMENT ON COLUMN analytics.dim_customer.is_active IS
    'Soft-delete flag. FALSE when the customer account is deactivated in the source system.';
COMMENT ON COLUMN analytics.dim_customer._loaded_at IS
    'Pipeline load timestamp. Set by the Python loader at the start of each load run for all rows in that batch.';


-- ------------------------------------------------------------
-- 3. analytics.dim_product
--    One row per unique product (SCD Type 1, overwrite).
--    product_code is the business key (maps to
--    source_system.products.product_code / olist product_id).
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key         SERIAL          PRIMARY KEY,
    product_id          INT             NOT NULL,
    product_code        VARCHAR(32)     NOT NULL,
    category_name_en    VARCHAR(100),
    category_name_pt    VARCHAR(100),
    weight_g            NUMERIC(10,2),
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    _loaded_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_product_code UNIQUE (product_code)
);

COMMENT ON TABLE analytics.dim_product IS
    'Product dimension (SCD Type 1). One row per catalogue product. '
    'Rebuilt from Bronze on every pipeline run; attributes overwrite on each load.';
COMMENT ON COLUMN analytics.dim_product.product_key IS
    'Gold surrogate key (1-based SERIAL). Used as FK in fact_sales.';
COMMENT ON COLUMN analytics.dim_product.product_id IS
    'Source-system surrogate from source_system.products.product_id. '
    'Retained for ETL join traceability — not exposed to BI consumers.';
COMMENT ON COLUMN analytics.dim_product.product_code IS
    'Business key. Maps to olist_products_dataset.product_id (UUID string).';
COMMENT ON COLUMN analytics.dim_product.category_name_en IS
    'English category name joined from the Olist translation reference. '
    'NULL for the ~3 products with no translation available.';
COMMENT ON COLUMN analytics.dim_product.category_name_pt IS
    'Original Portuguese category name from olist_products_dataset.product_category_name.';
COMMENT ON COLUMN analytics.dim_product.weight_g IS
    'Product shipping weight in grams. NULL for digital/non-shippable products.';
COMMENT ON COLUMN analytics.dim_product.is_active IS
    'Soft-delete flag. FALSE when the product is delisted in the source system.';
COMMENT ON COLUMN analytics.dim_product._loaded_at IS
    'Pipeline load timestamp. Set by the Python loader at the start of each load run for all rows in that batch.';


-- ------------------------------------------------------------
-- 4. analytics.dim_store
--    One row per unique seller / store location (SCD Type 1,
--    overwrite).  store_code is the business key (maps to
--    source_system.stores.store_code / olist seller_id).
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_store (
    store_key       SERIAL          PRIMARY KEY,
    store_id        INT             NOT NULL,
    store_code      VARCHAR(32)     NOT NULL,
    zip_code_prefix VARCHAR(8),
    city            VARCHAR(100),
    state           CHAR(2),
    region          VARCHAR(30),
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    _loaded_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_store_code UNIQUE (store_code)
);

COMMENT ON TABLE analytics.dim_store IS
    'Store (seller) dimension (SCD Type 1). One row per merchant seller. '
    'Rebuilt from Bronze on every pipeline run; attributes overwrite on each load.';
COMMENT ON COLUMN analytics.dim_store.store_key IS
    'Gold surrogate key (1-based SERIAL). Used as FK in fact_sales.';
COMMENT ON COLUMN analytics.dim_store.store_id IS
    'Source-system surrogate from source_system.stores.store_id. '
    'Retained for ETL join traceability — not exposed to BI consumers.';
COMMENT ON COLUMN analytics.dim_store.store_code IS
    'Business key. Maps to olist_sellers_dataset.seller_id (UUID string).';
COMMENT ON COLUMN analytics.dim_store.zip_code_prefix IS
    'First 5 digits of the Brazilian CEP postal code for the seller warehouse.';
COMMENT ON COLUMN analytics.dim_store.state IS
    'Two-letter Brazilian state abbreviation where the seller is located.';
COMMENT ON COLUMN analytics.dim_store.region IS
    'Brazilian geographic macro-region derived from state code. '
    'Valid values: Norte, Nordeste, Sudeste, Sul, Centro-Oeste.';
COMMENT ON COLUMN analytics.dim_store.is_active IS
    'Soft-delete flag. FALSE when the seller account is deactivated in the source system.';
COMMENT ON COLUMN analytics.dim_store._loaded_at IS
    'Pipeline load timestamp. Set by the Python loader at the start of each load run for all rows in that batch.';


-- ------------------------------------------------------------
-- 5. analytics.dim_currency
--    One row per ISO 4217 currency code observed across Silver
--    orders and FX rates.  Derived on each pipeline run from
--    the union of all currency codes seen in those two sources.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_currency (
    currency_key    SERIAL          PRIMARY KEY,
    currency_code   CHAR(3)         NOT NULL,
    currency_name   VARCHAR(50)     NOT NULL,
    _loaded_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_currency_code UNIQUE (currency_code)
);

COMMENT ON TABLE analytics.dim_currency IS
    'Currency dimension. One row per ISO 4217 currency code observed in Silver orders or FX rates. '
    'Unknown codes fall back to the raw ISO code as the currency_name so the dimension is always '
    'complete with respect to the fact data.';
COMMENT ON COLUMN analytics.dim_currency.currency_key IS
    'Gold surrogate key (1-based SERIAL). Used as FK in fact_sales and fact_fx_rates.';
COMMENT ON COLUMN analytics.dim_currency.currency_code IS
    'ISO 4217 three-letter currency code (e.g. BRL, USD, EUR).';
COMMENT ON COLUMN analytics.dim_currency.currency_name IS
    'Human-readable currency name (e.g. Brazilian Real). Falls back to the code itself '
    'for any unrecognised ISO code to prevent silent dimension gaps.';
COMMENT ON COLUMN analytics.dim_currency._loaded_at IS
    'Pipeline load timestamp. Set by the Python loader at the start of each load run for all rows in that batch.';


-- ------------------------------------------------------------
-- 6. analytics.fact_sales
--    Transaction line-item fact — one row per order line item.
--    Grain: one order item (order_id + line_number).
--    FKs to all five dimensions.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    order_item_id           INT             NOT NULL,
    order_id                INT             NOT NULL,
    order_code              VARCHAR(32)     NOT NULL,
    line_number             SMALLINT        NOT NULL,
    date_key                INT             NOT NULL,
    customer_key            INT,            -- NULL when Silver customer_id has no dim_customer match
    product_key             INT,            -- NULL when Silver product_id has no dim_product match
    store_key               INT,            -- NULL when Silver store_id has no dim_store match
    currency_key            INT,            -- NULL when Silver currency_code has no dim_currency match
    order_status            VARCHAR(20)     NOT NULL,
    source_channel          VARCHAR(30)     NOT NULL,
    unit_price              NUMERIC(12,2)   NOT NULL,
    freight_value           NUMERIC(12,2)   NOT NULL,
    quantity                SMALLINT        NOT NULL DEFAULT 1,
    delivery_days_actual    INT,
    delivery_days_estimated INT,
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_item_id),
    CONSTRAINT fk_fact_sales_date
        FOREIGN KEY (date_key)      REFERENCES analytics.dim_date     (date_key),
    CONSTRAINT fk_fact_sales_customer
        FOREIGN KEY (customer_key)  REFERENCES analytics.dim_customer (customer_key),
    CONSTRAINT fk_fact_sales_product
        FOREIGN KEY (product_key)   REFERENCES analytics.dim_product  (product_key),
    CONSTRAINT fk_fact_sales_store
        FOREIGN KEY (store_key)     REFERENCES analytics.dim_store    (store_key),
    CONSTRAINT fk_fact_sales_currency
        FOREIGN KEY (currency_key)  REFERENCES analytics.dim_currency (currency_key)
);

COMMENT ON TABLE analytics.fact_sales IS
    'Sales fact table. Grain: one order line item. '
    'Approximately 112,650 rows covering the full Olist transaction history. '
    'All monetary values are in the currency indicated by currency_key (BRL in base Olist data).';
COMMENT ON COLUMN analytics.fact_sales.order_item_id IS
    'Degenerate dimension / natural key from source_system.order_items.order_item_id. '
    'Also serves as the fact table PK.';
COMMENT ON COLUMN analytics.fact_sales.order_id IS
    'Source-system order header surrogate. Join to order_code for the business key.';
COMMENT ON COLUMN analytics.fact_sales.order_code IS
    'Business key of the parent order. Maps to olist_orders_dataset.order_id (UUID string).';
COMMENT ON COLUMN analytics.fact_sales.line_number IS
    'Position of this item within the order (1-based). Maps to olist order_item_id.';
COMMENT ON COLUMN analytics.fact_sales.date_key IS
    'FK to dim_date. Encodes the order purchase date as YYYYMMDD integer.';
COMMENT ON COLUMN analytics.fact_sales.customer_key IS
    'FK to dim_customer surrogate key.';
COMMENT ON COLUMN analytics.fact_sales.product_key IS
    'FK to dim_product surrogate key.';
COMMENT ON COLUMN analytics.fact_sales.store_key IS
    'FK to dim_store surrogate key. Identifies the seller who fulfilled this line.';
COMMENT ON COLUMN analytics.fact_sales.currency_key IS
    'FK to dim_currency. Indicates the currency for unit_price and freight_value.';
COMMENT ON COLUMN analytics.fact_sales.order_status IS
    'Order lifecycle status at fact load time. '
    'Known values: delivered, shipped, canceled, unavailable, invoiced, processing, created, approved.';
COMMENT ON COLUMN analytics.fact_sales.source_channel IS
    'Sales channel. Defaults to ''online''; reserved for future multi-channel expansion.';
COMMENT ON COLUMN analytics.fact_sales.unit_price IS
    'Item sale price in the order currency at time of purchase.';
COMMENT ON COLUMN analytics.fact_sales.freight_value IS
    'Freight cost allocated to this line item in the order currency.';
COMMENT ON COLUMN analytics.fact_sales.quantity IS
    'Units ordered on this line. Always 1 in the current Olist dataset (one row per unit shipped).';
COMMENT ON COLUMN analytics.fact_sales.delivery_days_actual IS
    'Actual delivery duration in days (actual_delivery - order_date). NULL for undelivered orders.';
COMMENT ON COLUMN analytics.fact_sales.delivery_days_estimated IS
    'Estimated delivery duration in days (estimated_delivery - order_date). NULL when no estimate exists.';
COMMENT ON COLUMN analytics.fact_sales._loaded_at IS
    'Pipeline load timestamp. Set by the Python loader at the start of each load run for all rows in that batch.';


-- ------------------------------------------------------------
-- 7. analytics.fact_weather_daily
--    Daily weather observations keyed by date + city + state.
--    Sourced from the Open-Meteo API via the Bronze weather
--    extract and Silver weather transform stages.
--    Grain: one row per (date, city, state) combination.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_weather_daily (
    date_key        INT             NOT NULL,
    city            VARCHAR(100)    NOT NULL,
    state           CHAR(2)         NOT NULL,
    temp_max        NUMERIC(5,1),
    temp_min        NUMERIC(5,1),
    precipitation   NUMERIC(7,1),
    windspeed       NUMERIC(6,1),
    weathercode     SMALLINT,
    _loaded_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date_key, city, state),
    CONSTRAINT fk_fact_weather_date
        FOREIGN KEY (date_key) REFERENCES analytics.dim_date (date_key)
);

COMMENT ON TABLE analytics.fact_weather_daily IS
    'Daily weather fact table. Grain: one row per (date, city, state). '
    'Sourced from Open-Meteo historical weather API. Used to enrich sales analysis '
    'with weather conditions at customer and seller locations.';
COMMENT ON COLUMN analytics.fact_weather_daily.date_key IS
    'FK to dim_date. YYYYMMDD integer encoding the observation date.';
COMMENT ON COLUMN analytics.fact_weather_daily.city IS
    'City name matching the geographic location of the weather observation.';
COMMENT ON COLUMN analytics.fact_weather_daily.state IS
    'Two-letter Brazilian state abbreviation for the observation location.';
COMMENT ON COLUMN analytics.fact_weather_daily.temp_max IS
    'Maximum daily temperature in degrees Celsius (Open-Meteo temperature_2m_max).';
COMMENT ON COLUMN analytics.fact_weather_daily.temp_min IS
    'Minimum daily temperature in degrees Celsius (Open-Meteo temperature_2m_min).';
COMMENT ON COLUMN analytics.fact_weather_daily.precipitation IS
    'Total daily precipitation in millimetres (Open-Meteo precipitation_sum).';
COMMENT ON COLUMN analytics.fact_weather_daily.windspeed IS
    'Maximum daily wind speed in km/h (Open-Meteo windspeed_10m_max).';
COMMENT ON COLUMN analytics.fact_weather_daily.weathercode IS
    'WMO weather interpretation code for the dominant condition of the day '
    '(Open-Meteo weathercode). See WMO 4677 for code definitions.';
COMMENT ON COLUMN analytics.fact_weather_daily._loaded_at IS
    'Pipeline load timestamp. Set by the Python loader at the start of each load run for all rows in that batch.';


-- ------------------------------------------------------------
-- 8. analytics.fact_fx_rates
--    Daily foreign exchange rates keyed by date + currency pair.
--    Sourced from the Frankfurter API via the Bronze FX extract
--    and Silver FX transform stages.
--    Grain: one row per (date, base_currency, quote_currency).
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_fx_rates (
    date_key            INT             NOT NULL,
    base_currency_key   INT,            -- NULL when base_currency has no dim_currency match
    quote_currency_key  INT,            -- NULL when quote_currency has no dim_currency match
    base_currency       CHAR(3)         NOT NULL,
    quote_currency      CHAR(3)         NOT NULL,
    rate                NUMERIC(16,6)   NOT NULL,
    _loaded_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date_key, base_currency_key, quote_currency_key),
    CONSTRAINT fk_fact_fx_date
        FOREIGN KEY (date_key)           REFERENCES analytics.dim_date     (date_key),
    CONSTRAINT fk_fact_fx_base_currency
        FOREIGN KEY (base_currency_key)  REFERENCES analytics.dim_currency (currency_key),
    CONSTRAINT fk_fact_fx_quote_currency
        FOREIGN KEY (quote_currency_key) REFERENCES analytics.dim_currency (currency_key)
);

COMMENT ON TABLE analytics.fact_fx_rates IS
    'Foreign exchange rate fact table. Grain: one row per (date, base currency, quote currency). '
    'Sourced from the Frankfurter API. Used to convert BRL sales amounts into other currencies '
    'for multi-currency reporting.';
COMMENT ON COLUMN analytics.fact_fx_rates.date_key IS
    'FK to dim_date. YYYYMMDD integer encoding the rate effective date.';
COMMENT ON COLUMN analytics.fact_fx_rates.base_currency_key IS
    'FK to dim_currency for the base (denominator) currency of the exchange rate.';
COMMENT ON COLUMN analytics.fact_fx_rates.quote_currency_key IS
    'FK to dim_currency for the quote (numerator) currency of the exchange rate.';
COMMENT ON COLUMN analytics.fact_fx_rates.base_currency IS
    'Denormalised ISO 4217 base currency code (e.g. EUR). '
    'Retained for query convenience to avoid a join on the base leg.';
COMMENT ON COLUMN analytics.fact_fx_rates.quote_currency IS
    'Denormalised ISO 4217 quote currency code (e.g. BRL). '
    'Retained for query convenience to avoid a join on the quote leg.';
COMMENT ON COLUMN analytics.fact_fx_rates.rate IS
    'Exchange rate: 1 unit of base_currency = rate units of quote_currency. '
    'NUMERIC(16,6) provides sub-pip precision for all common currency pairs.';
COMMENT ON COLUMN analytics.fact_fx_rates._loaded_at IS
    'Pipeline load timestamp. Set by the Python loader at the start of each load run for all rows in that batch.';


-- =============================================================
-- INDEXES
-- Placed after all CREATE TABLE statements so the planner can
-- build statistics-accurate plans from the first ANALYZE run.
-- =============================================================

-- fact_sales — most common dashboard filter dimensions
CREATE INDEX IF NOT EXISTS idx_fact_sales_date_key
    ON analytics.fact_sales (date_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_key
    ON analytics.fact_sales (customer_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_product_key
    ON analytics.fact_sales (product_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_store_key
    ON analytics.fact_sales (store_key);

-- fact_weather_daily — geographic filter patterns
-- Single-column index supports state-level rollups across all cities.
CREATE INDEX IF NOT EXISTS idx_fact_weather_state
    ON analytics.fact_weather_daily (state);

-- Composite index supports the most common join pattern:
--   WHERE city = ? AND state = ?  (used when joining to dim_customer/dim_store)
CREATE INDEX IF NOT EXISTS idx_fact_weather_city_state
    ON analytics.fact_weather_daily (city, state);

-- fact_fx_rates — date-range rate lookups
CREATE INDEX IF NOT EXISTS idx_fact_fx_rates_date_key
    ON analytics.fact_fx_rates (date_key);

-- dim_customer — geographic filtering by state
CREATE INDEX IF NOT EXISTS idx_dim_customer_state
    ON analytics.dim_customer (state);

-- dim_store — regional and state-level aggregations
CREATE INDEX IF NOT EXISTS idx_dim_store_region
    ON analytics.dim_store (region);

CREATE INDEX IF NOT EXISTS idx_dim_store_state
    ON analytics.dim_store (state);


-- =============================================================
-- LOAD ORDER SUMMARY
--
-- Load dimensions first, then facts.  Within dimensions, load
-- dim_date before all others because no FK from another
-- dimension depends on it, but fact tables all reference it.
--
--   Step 1 — analytics.dim_date          (calendar spine; no deps)
--   Step 2 — analytics.dim_customer      (no Gold deps)
--   Step 3 — analytics.dim_product       (no Gold deps)
--   Step 4 — analytics.dim_store         (no Gold deps)
--   Step 5 — analytics.dim_currency      (no Gold deps)
--   Step 6 — analytics.fact_sales        (depends on all 5 dims)
--   Step 7 — analytics.fact_weather_daily (depends on dim_date)
--   Step 8 — analytics.fact_fx_rates     (depends on dim_date,
--                                          dim_currency x2)
--
-- All five dimensions must be fully loaded before any fact
-- table is loaded.  Steps 2–5 are order-independent among
-- themselves and may be loaded in parallel if the loader
-- supports it.
-- =============================================================
