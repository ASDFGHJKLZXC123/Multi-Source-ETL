-- =============================================================
-- 01_schemas.sql  —  REFERENCE DOCUMENTATION ONLY
--
-- The canonical schema DDL lives in 00_init.sql, which is the
-- single source of truth for both:
--   • Docker first-run  (mounted at /docker-entrypoint-initdb.d/)
--   • Python init path  (src/utils/db.py::init_schemas() runs all 0*.sql)
--
-- Because 00_init.sql runs first (lexicographic order) and uses
-- IF NOT EXISTS, the statements below are always safe no-ops.
-- They are kept here for developer reference — showing what each
-- schema is for without opening the larger 00_init.sql.
-- =============================================================

CREATE SCHEMA IF NOT EXISTS source_system;
CREATE SCHEMA IF NOT EXISTS analytics;

COMMENT ON SCHEMA source_system IS
    'Raw source data loaded from Olist CSV files. Do not modify manually.';
COMMENT ON SCHEMA analytics IS
    'Analytics-ready Gold layer — star schema for Power BI consumption.';
