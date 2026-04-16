-- =============================================================
-- 00_init.sql
-- Runs automatically on first container start via
--   /docker-entrypoint-initdb.d/
--
-- Also executed by src/utils/db.py::init_schemas() in numeric
-- order alongside 01_schemas.sql and 02_pipeline_metadata.sql.
-- All statements use IF NOT EXISTS — safe to re-run.
-- =============================================================

-- ------------------------------------------------------------
-- Schemas
-- ------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS source_system;
CREATE SCHEMA IF NOT EXISTS analytics;

COMMENT ON SCHEMA source_system IS
    'Raw source data loaded from Olist CSV files. Do not modify manually.';
COMMENT ON SCHEMA analytics IS
    'Analytics-ready Gold layer — star schema for Power BI consumption.';

-- ------------------------------------------------------------
-- analytics.pipeline_metadata
-- Tracks every ETL stage execution: stage name, target table,
-- row counts, status, timing, and any error detail.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.pipeline_metadata (
    run_id           SERIAL        PRIMARY KEY,
    stage_name       VARCHAR(50)   NOT NULL,
    table_name       VARCHAR(100),
    rows_processed   INT,
    status           VARCHAR(20)   NOT NULL
                         CHECK (status IN ('started', 'completed', 'failed', 'skipped')),
    started_at       TIMESTAMP     NOT NULL DEFAULT NOW(),
    completed_at     TIMESTAMP,
    duration_seconds NUMERIC(10,2) GENERATED ALWAYS AS (
                         EXTRACT(EPOCH FROM (completed_at - started_at))
                     ) STORED,
    error_message    TEXT,
    run_metadata     JSONB
);

-- Composite index for time-ordered stage lookups (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_stage
    ON analytics.pipeline_metadata (stage_name, started_at DESC);

-- Partial index — only indexes failed rows, keeping it small and fast for alerting
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_status
    ON analytics.pipeline_metadata (status) WHERE status = 'failed';

COMMENT ON TABLE analytics.pipeline_metadata IS
    'ETL run history — one row per stage execution. Used for monitoring and idempotency checks.';

-- ------------------------------------------------------------
-- Permissions (uncomment and adapt when a reporting role exists)
-- ------------------------------------------------------------
-- GRANT USAGE ON SCHEMA analytics      TO reporting_role;
-- GRANT USAGE ON SCHEMA source_system  TO reporting_role;
-- GRANT SELECT ON ALL TABLES IN SCHEMA analytics     TO reporting_role;
-- GRANT SELECT ON ALL TABLES IN SCHEMA source_system TO reporting_role;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA analytics
--     GRANT SELECT ON TABLES TO reporting_role;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA source_system
--     GRANT SELECT ON TABLES TO reporting_role;
