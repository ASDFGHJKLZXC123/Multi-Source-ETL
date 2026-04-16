-- =============================================================
-- 02_pipeline_metadata.sql  —  REFERENCE DOCUMENTATION ONLY
--
-- The canonical analytics.pipeline_metadata DDL lives in 00_init.sql,
-- which is the single source of truth for both the Docker first-run
-- path and the Python init_schemas() path.
--
-- Because 00_init.sql runs first (lexicographic order) and uses
-- IF NOT EXISTS, the statement below is always a safe no-op.
-- This file exists for developer reference — showing the table
-- design in isolation without opening the full 00_init.sql.
-- =============================================================

-- NOTE: If you update the table schema, update 00_init.sql — not this file.

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

CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_stage
    ON analytics.pipeline_metadata (stage_name, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_status
    ON analytics.pipeline_metadata (status) WHERE status = 'failed';

COMMENT ON TABLE analytics.pipeline_metadata IS
    'ETL run history — one row per stage execution. Used for monitoring and idempotency checks.';
