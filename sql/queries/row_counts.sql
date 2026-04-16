-- Quick row count across all source_system tables.
SELECT
    schemaname,
    tablename,
    n_live_tup AS estimated_rows,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
WHERE schemaname IN ('source_system', 'analytics')
ORDER BY schemaname, tablename;
