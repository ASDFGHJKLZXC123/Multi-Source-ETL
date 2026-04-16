-- Run this to verify PostgreSQL connectivity and schema setup.
SELECT
    current_database()                          AS database_name,
    current_user                                AS connected_as,
    version()                                   AS pg_version,
    NOW()                                       AS server_time,
    (SELECT COUNT(*) FROM information_schema.schemata
     WHERE schema_name IN ('source_system', 'analytics'))  AS schemas_found;
