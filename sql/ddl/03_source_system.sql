-- =============================================================
-- 03_source_system.sql
--
-- Purpose  : Enterprise source-system table DDL for the
--            Multi-Source ETL pipeline.
--
-- Dataset  : Olist Brazilian E-Commerce Public Dataset
--            (https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
--            Raw CSV files are staged, then loaded into these tables
--            by the Python ingestion layer (src/setup/load_source_db.py).
--
-- Run order: Execute AFTER 00_init.sql (which creates both schemas).
--            Safe to re-run — every statement is idempotent via
--            IF NOT EXISTS / CREATE OR REPLACE.
--
-- Design principles:
--   • Integer surrogate PKs (SERIAL) — no UUID strings in hot paths
--   • Business keys preserved as *_code VARCHAR columns with UNIQUE
--     constraints so source traceability is never lost
--   • ingested_at  — set once at load time, never updated
--   • created_at   — mirrors source system timestamp where available
--   • updated_at   — maintained by trigger (customers only for now;
--                    extend to other tables as CDC is introduced)
--   • All monetary values in BRL; currency_code column on orders
--     supports future multi-currency expansion
--
-- Tables (dependency order):
--   1. source_system.customers
--   2. source_system.stores
--   3. source_system.products
--   4. source_system.orders        (FK -> customers)
--   5. source_system.order_items   (FK -> orders, products, stores)
-- =============================================================


-- ------------------------------------------------------------
-- 1. source_system.customers
--    One row per physical person (deduplication key:
--    olist_customers.customer_unique_id, not the per-order
--    customer_id).
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS source_system.customers (
    customer_id       SERIAL          PRIMARY KEY,
    customer_code     VARCHAR(32)     NOT NULL,
    zip_code_prefix   VARCHAR(8),
    city              VARCHAR(100),
    state             CHAR(2),
    is_active         BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMP       NOT NULL DEFAULT NOW(),
    ingested_at       TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_customers_code UNIQUE (customer_code)
);

COMMENT ON TABLE source_system.customers IS
    'Unique shoppers. customer_code maps to olist customer_unique_id (not the per-order customer_id).';
COMMENT ON COLUMN source_system.customers.customer_id IS
    'Surrogate integer PK — never exposed to end users.';
COMMENT ON COLUMN source_system.customers.customer_code IS
    'Business key from source. Maps to olist_customers.customer_unique_id — one row per physical person.';
COMMENT ON COLUMN source_system.customers.zip_code_prefix IS
    'First 5 digits of Brazilian CEP postal code.';
COMMENT ON COLUMN source_system.customers.state IS
    'Two-letter Brazilian state abbreviation (e.g. SP, RJ, MG).';
COMMENT ON COLUMN source_system.customers.is_active IS
    'Soft-delete flag. Set to FALSE when a customer account is deactivated in the source system.';
COMMENT ON COLUMN source_system.customers.created_at IS
    'Record creation timestamp in the source system.';
COMMENT ON COLUMN source_system.customers.updated_at IS
    'Last modification timestamp. Maintained automatically by trg_customers_updated_at trigger.';
COMMENT ON COLUMN source_system.customers.ingested_at IS
    'Timestamp when this row was first loaded by the ETL pipeline. Set once; never updated.';


-- ------------------------------------------------------------
-- 2. source_system.stores
--    Merchant sellers acting as store/warehouse locations.
--    store_code maps to olist sellers.seller_id.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS source_system.stores (
    store_id          SERIAL          PRIMARY KEY,
    store_code        VARCHAR(32)     NOT NULL,
    zip_code_prefix   VARCHAR(8),
    city              VARCHAR(100),
    state             CHAR(2),
    region            VARCHAR(30),
    is_active         BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMP       NOT NULL DEFAULT NOW(),
    ingested_at       TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_stores_code UNIQUE (store_code)
);

COMMENT ON TABLE source_system.stores IS
    'Merchant sellers acting as store locations. store_code maps to olist seller_id.';
COMMENT ON COLUMN source_system.stores.store_id IS
    'Surrogate integer PK — never exposed to end users.';
COMMENT ON COLUMN source_system.stores.store_code IS
    'Business key from source. Maps to olist_sellers.seller_id (UUID string).';
COMMENT ON COLUMN source_system.stores.zip_code_prefix IS
    'First 5 digits of Brazilian CEP postal code for the seller warehouse.';
COMMENT ON COLUMN source_system.stores.state IS
    'Two-letter Brazilian state abbreviation where the seller is located.';
COMMENT ON COLUMN source_system.stores.region IS
    'Brazilian geographic macro-region derived from state code. '
    'Valid values: Norte, Nordeste, Sudeste, Sul, Centro-Oeste.';
COMMENT ON COLUMN source_system.stores.is_active IS
    'Soft-delete flag. Set to FALSE when a seller account is deactivated.';
COMMENT ON COLUMN source_system.stores.ingested_at IS
    'Timestamp when this row was first loaded by the ETL pipeline. Set once; never updated.';


-- ------------------------------------------------------------
-- 3. source_system.products
--    Product catalogue with both Portuguese and English
--    category names. English names joined from
--    product_category_name_translation.csv at load time.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS source_system.products (
    product_id        SERIAL          PRIMARY KEY,
    product_code      VARCHAR(32)     NOT NULL,
    category_name_pt  VARCHAR(100),
    category_name_en  VARCHAR(100),
    weight_g          NUMERIC(10,2),
    length_cm         NUMERIC(6,2),
    height_cm         NUMERIC(6,2),
    width_cm          NUMERIC(6,2),
    is_active         BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMP       NOT NULL DEFAULT NOW(),
    ingested_at       TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_products_code UNIQUE (product_code)
);

COMMENT ON TABLE source_system.products IS
    'Product catalogue. product_code maps to olist product_id. English categories joined from translation table.';
COMMENT ON COLUMN source_system.products.product_id IS
    'Surrogate integer PK — never exposed to end users.';
COMMENT ON COLUMN source_system.products.product_code IS
    'Business key from source. Maps to olist_products_dataset.product_id (UUID string).';
COMMENT ON COLUMN source_system.products.category_name_pt IS
    'Original Portuguese category name from olist_products_dataset.product_category_name.';
COMMENT ON COLUMN source_system.products.category_name_en IS
    'English translation of category name. Joined from product_category_name_translation.csv at ingest time. NULL when no translation exists.';
COMMENT ON COLUMN source_system.products.weight_g IS
    'Product weight in grams as provided by the seller.';
COMMENT ON COLUMN source_system.products.is_active IS
    'Soft-delete flag. Set to FALSE when a product is delisted.';
COMMENT ON COLUMN source_system.products.ingested_at IS
    'Timestamp when this row was first loaded by the ETL pipeline. Set once; never updated.';


-- ------------------------------------------------------------
-- 4. source_system.orders
--    Transaction header — one row per order, not per item.
--    FK to customers. order_code maps to olist order_id.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS source_system.orders (
    order_id                 SERIAL       PRIMARY KEY,
    order_code               VARCHAR(32)  NOT NULL,
    customer_id              INT          NOT NULL,
    order_status             VARCHAR(20)  NOT NULL,
    order_date               DATE         NOT NULL,
    order_timestamp          TIMESTAMP,
    approved_at              TIMESTAMP,
    estimated_delivery       DATE,
    actual_delivery          DATE,
    delivery_days_actual     INT          GENERATED ALWAYS AS (
                                 CASE WHEN actual_delivery IS NOT NULL
                                 THEN actual_delivery - order_date END
                             ) STORED,
    delivery_days_estimated  INT          GENERATED ALWAYS AS (
                                 CASE WHEN estimated_delivery IS NOT NULL
                                 THEN estimated_delivery - order_date END
                             ) STORED,
    source_channel           VARCHAR(30)  NOT NULL DEFAULT 'online',
    currency_code            CHAR(3)      NOT NULL DEFAULT 'BRL',
    created_at               TIMESTAMP    NOT NULL DEFAULT NOW(),
    ingested_at              TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_orders_code     UNIQUE (order_code),
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id)
        REFERENCES source_system.customers (customer_id)
);

COMMENT ON TABLE source_system.orders IS
    'Transaction header. One row per order (not per item). order_code = olist order_id.';
COMMENT ON COLUMN source_system.orders.order_id IS
    'Surrogate integer PK — referenced by order_items.order_id.';
COMMENT ON COLUMN source_system.orders.order_code IS
    'Business key from source. Maps to olist_orders_dataset.order_id (UUID string).';
COMMENT ON COLUMN source_system.orders.customer_id IS
    'FK to source_system.customers.customer_id. Resolved from olist customer_unique_id at ingest time.';
COMMENT ON COLUMN source_system.orders.order_status IS
    'Order lifecycle state from source. Known values: created, approved, invoiced, '
    'processing, shipped, delivered, unavailable, canceled.';
COMMENT ON COLUMN source_system.orders.order_date IS
    'Local order date (DATE only) extracted from order_purchase_timestamp (Brasilia time, UTC-3).';
COMMENT ON COLUMN source_system.orders.order_timestamp IS
    'Full purchase timestamp from source (order_purchase_timestamp). Stored in UTC.';
COMMENT ON COLUMN source_system.orders.approved_at IS
    'Payment approval timestamp from source (order_approved_at). NULL if not yet approved.';
COMMENT ON COLUMN source_system.orders.estimated_delivery IS
    'Carrier-promised delivery date (order_estimated_delivery_date).';
COMMENT ON COLUMN source_system.orders.actual_delivery IS
    'Actual delivery date (order_delivered_customer_date). NULL for undelivered orders.';
COMMENT ON COLUMN source_system.orders.delivery_days_actual IS
    'Computed: actual_delivery - order_date in days. NULL until order is delivered.';
COMMENT ON COLUMN source_system.orders.delivery_days_estimated IS
    'Computed: estimated_delivery - order_date in days. NULL when no estimate exists.';
COMMENT ON COLUMN source_system.orders.source_channel IS
    'Sales channel. Defaults to ''online''; extend for marketplace, app, etc.';
COMMENT ON COLUMN source_system.orders.currency_code IS
    'ISO 4217 currency code. Always BRL in Olist source data; column supports future multi-currency.';
COMMENT ON COLUMN source_system.orders.ingested_at IS
    'Timestamp when this row was first loaded by the ETL pipeline. Set once; never updated.';


-- ------------------------------------------------------------
-- 5. source_system.order_items
--    Transaction line items — one row per product per order.
--    FK to orders, products, and stores.
--    line_number maps to olist order_item_id.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS source_system.order_items (
    order_item_id       SERIAL          PRIMARY KEY,
    order_id            INT             NOT NULL,
    product_id          INT             NOT NULL,
    store_id            INT             NOT NULL,
    line_number         SMALLINT        NOT NULL,
    unit_price          NUMERIC(12,2)   NOT NULL,
    freight_value       NUMERIC(12,2)   NOT NULL DEFAULT 0,
    quantity            SMALLINT        NOT NULL DEFAULT 1,
    shipping_limit_date TIMESTAMP,
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    ingested_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_order_items_line    UNIQUE (order_id, line_number),
    CONSTRAINT fk_order_items_order   FOREIGN KEY (order_id)
        REFERENCES source_system.orders (order_id),
    CONSTRAINT fk_order_items_product FOREIGN KEY (product_id)
        REFERENCES source_system.products (product_id),
    CONSTRAINT fk_order_items_store   FOREIGN KEY (store_id)
        REFERENCES source_system.stores (store_id)
);

COMMENT ON TABLE source_system.order_items IS
    'Transaction line items. One row per product per order. line_number = olist order_item_id.';
COMMENT ON COLUMN source_system.order_items.order_item_id IS
    'Surrogate integer PK — not the Olist order_item_id sequence.';
COMMENT ON COLUMN source_system.order_items.order_id IS
    'FK to source_system.orders.order_id.';
COMMENT ON COLUMN source_system.order_items.product_id IS
    'FK to source_system.products.product_id.';
COMMENT ON COLUMN source_system.order_items.store_id IS
    'FK to source_system.stores.store_id. Identifies which seller fulfilled this line.';
COMMENT ON COLUMN source_system.order_items.line_number IS
    'Position within the order (1-based). Maps to olist_order_items_dataset.order_item_id.';
COMMENT ON COLUMN source_system.order_items.unit_price IS
    'Price in BRL at time of order. No explicit currency column in source — always BRL.';
COMMENT ON COLUMN source_system.order_items.freight_value IS
    'Freight cost in BRL allocated to this line item. 0 when shipping is free.';
COMMENT ON COLUMN source_system.order_items.quantity IS
    'Item quantity. Olist source always ships 1 unit per line; retained for schema extensibility.';
COMMENT ON COLUMN source_system.order_items.shipping_limit_date IS
    'Seller''s deadline to hand off to the carrier (shipping_limit_date from source).';
COMMENT ON COLUMN source_system.order_items.ingested_at IS
    'Timestamp when this row was first loaded by the ETL pipeline. Set once; never updated.';


-- =============================================================
-- INDEXES
-- Placed after all CREATE TABLE statements to allow the planner
-- to build statistics-accurate plans from the first ANALYZE run.
-- =============================================================

-- customers
CREATE INDEX IF NOT EXISTS idx_src_customers_city   ON source_system.customers (city);
CREATE INDEX IF NOT EXISTS idx_src_customers_state  ON source_system.customers (state);

-- stores
CREATE INDEX IF NOT EXISTS idx_src_stores_state   ON source_system.stores (state);
CREATE INDEX IF NOT EXISTS idx_src_stores_region  ON source_system.stores (region);

-- products
CREATE INDEX IF NOT EXISTS idx_src_products_category_en ON source_system.products (category_name_en);

-- orders
CREATE INDEX IF NOT EXISTS idx_src_orders_customer_id ON source_system.orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_src_orders_date        ON source_system.orders (order_date);
CREATE INDEX IF NOT EXISTS idx_src_orders_status      ON source_system.orders (order_status);
-- Composite index supports the most common dashboard filter pattern:
--   WHERE order_date BETWEEN x AND y AND order_status = 'delivered'
CREATE INDEX IF NOT EXISTS idx_src_orders_date_status ON source_system.orders (order_date, order_status);

-- order_items
CREATE INDEX IF NOT EXISTS idx_src_items_order_id   ON source_system.order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_src_items_product_id ON source_system.order_items (product_id);
CREATE INDEX IF NOT EXISTS idx_src_items_store_id   ON source_system.order_items (store_id);


-- =============================================================
-- TRIGGERS
-- Reusable function that stamps updated_at on every UPDATE.
-- Attach additional triggers to other tables as CDC is added.
-- =============================================================

-- Reusable trigger function for maintaining updated_at.
-- Defined in source_system schema to keep it co-located with
-- the tables it serves and avoid polluting the public schema.
CREATE OR REPLACE FUNCTION source_system.set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION source_system.set_updated_at() IS
    'Generic BEFORE UPDATE trigger function that sets updated_at = NOW(). '
    'Attach to any source_system table that carries an updated_at column.';

-- Attach to customers (the only source table with updated_at for now).
CREATE OR REPLACE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON source_system.customers
    FOR EACH ROW EXECUTE FUNCTION source_system.set_updated_at();
