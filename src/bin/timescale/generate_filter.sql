-- use the following script to generate filter.ini
-- psql $PGCOPYDB_SOURCE_PGURI -f ~/dimitri/pgcopydb/src/bin/timescale/generate_filter.sql -t -A > filter.ini
SELECT '[exclude-table]';
SELECT CONCAT(table_schema, '.', table_name) FROM information_schema.tables
WHERE table_schema = '_timescaledb_internal' AND table_name LIKE '_compressed_hypertable_%';
SELECT '[include-only-schema]';
SELECT '_timescaledb_internal';
