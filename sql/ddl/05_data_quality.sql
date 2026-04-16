-- =============================================================
-- 05_data_quality.sql
-- Purpose : Data quality check results log for the analytics
--           Gold layer.
--
-- Written by src/quality/runner.py after each warehouse load.
-- All statements use IF NOT EXISTS — safe to re-run.
-- =============================================================

-- ------------------------------------------------------------
-- analytics.data_quality_log
-- One row per check execution. Records whether a check passed
-- or failed, how many rows were affected, and optional JSONB
-- metadata for downstream alerting and reporting.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.data_quality_log (
    check_id        SERIAL          PRIMARY KEY,
    run_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    check_name      VARCHAR(100)    NOT NULL,
    table_name      VARCHAR(100)    NOT NULL,
    check_category  VARCHAR(30)     NOT NULL
                        CHECK (check_category IN (
                            'row_count',
                            'null_check',
                            'uniqueness',
                            'range',
                            'referential_integrity'
                        )),
    severity        VARCHAR(10)     NOT NULL
                        CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL')),
    status          VARCHAR(10)     NOT NULL
                        CHECK (status IN ('PASS', 'FAIL')),
    expected_value  TEXT,
    actual_value    TEXT,
    rows_affected   INT             NOT NULL DEFAULT 0,
    message         TEXT            NOT NULL,
    check_metadata  JSONB
);

-- Time-ordered query: show all recent failures
CREATE INDEX IF NOT EXISTS idx_dq_log_run_at
    ON analytics.data_quality_log (run_at DESC);

-- Filter by table for per-table quality reports
CREATE INDEX IF NOT EXISTS idx_dq_log_table_name
    ON analytics.data_quality_log (table_name, run_at DESC);

-- Partial index for fast failure-only queries (alerting, monitoring)
CREATE INDEX IF NOT EXISTS idx_dq_log_failures
    ON analytics.data_quality_log (run_at DESC, severity)
    WHERE status = 'FAIL';

COMMENT ON TABLE analytics.data_quality_log IS
    'Data quality check results — one row per check execution. Written by '
    'src/quality/runner.py after each warehouse load. Used for alerting, '
    'per-table quality reports, and pipeline health monitoring.';

-- ------------------------------------------------------------
-- Column comments
-- ------------------------------------------------------------
COMMENT ON COLUMN analytics.data_quality_log.check_id IS
    'Surrogate primary key.';

COMMENT ON COLUMN analytics.data_quality_log.run_at IS
    'UTC timestamp when this check was executed.';

COMMENT ON COLUMN analytics.data_quality_log.check_name IS
    'Human-readable check identifier, e.g. ''fact_sales.row_count_threshold''.';

COMMENT ON COLUMN analytics.data_quality_log.table_name IS
    'Fully qualified table being checked, e.g. ''analytics.fact_sales''.';

COMMENT ON COLUMN analytics.data_quality_log.check_category IS
    'Category of check: row_count | null_check | uniqueness | range | referential_integrity.';

COMMENT ON COLUMN analytics.data_quality_log.severity IS
    'Impact level if this check fails: INFO (advisory) | WARNING (investigate) | CRITICAL (halt pipeline).';

COMMENT ON COLUMN analytics.data_quality_log.status IS
    'Whether the check passed or failed: PASS | FAIL.';

COMMENT ON COLUMN analytics.data_quality_log.expected_value IS
    'What the check expected to find (human-readable string, may be NULL for pass-only checks).';

COMMENT ON COLUMN analytics.data_quality_log.actual_value IS
    'What the check actually found (human-readable string).';

COMMENT ON COLUMN analytics.data_quality_log.rows_affected IS
    'Number of rows that violated the check (0 on PASS).';

COMMENT ON COLUMN analytics.data_quality_log.message IS
    'Human-readable summary of the check result.';

COMMENT ON COLUMN analytics.data_quality_log.check_metadata IS
    'Optional JSONB for storing additional context (e.g. sample failing values, check thresholds).';
