from psql import psql

def init(conn):
    """
    Initializes the catalog table in the target database.
    """
    sql = """
    CREATE SCHEMA IF NOT EXISTS __live_migration;
    """
    psql(conn, sql)

    _init_matview_audit(conn)


def _init_matview_audit(conn):
    sql = """
    BEGIN;

    CREATE TABLE IF NOT EXISTS __live_migration.matview_audit (
        schemaname TEXT,
        matviewname TEXT,
        renamed BOOLEAN DEFAULT FALSE
    );

    CREATE OR REPLACE FUNCTION __live_migration.skip_dml_function() RETURNS TRIGGER AS $$
    BEGIN
        -- Do nothing and return NULL to skip the DML operation
        RAISE INFO 'live-migration: Skipping % DML in MATERIALIZED VIEW %.%', TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;

    COMMIT;
    """
    psql(conn, sql)

def clean(conn):
    """
    Cleans up the catalog table in the target database.
    """
    # also rename the materialized views back to their original names
    restore_matview(conn)

    # Drop the audit table
    _clean_matview_audit(conn)

    sql = """
    DROP SCHEMA IF EXISTS __live_migration CASCADE;
    """
    psql(conn, sql)


def _clean_matview_audit(conn):
    sql = """
    BEGIN;

    DROP FUNCTION IF EXISTS __live_migration.skip_dml_function() CASCADE;
    DROP TABLE IF EXISTS __live_migration.matview_audit CASCADE;

    COMMIT;
    """
    psql(conn, sql)


def convert_matview_to_view(conn):
    query = """
BEGIN;

DO $$
DECLARE
    mv RECORD;
    populated BOOLEAN;
BEGIN
    SELECT COUNT(*) > 0 INTO populated FROM __live_migration.matview_audit;

    -- Populate the audit table with materialized views if not already populated
    IF populated = FALSE THEN
        INSERT INTO __live_migration.matview_audit (schemaname, matviewname)
        SELECT schemaname, matviewname
        FROM pg_matviews;
    END IF;

    -- Loop through all materialized views in the current schema that have not been renamed
    FOR mv IN
        SELECT schemaname, matviewname
        FROM __live_migration.matview_audit
        WHERE NOT renamed
    LOOP
        -- Rename the materialized view with a live_migration_ prefix
        EXECUTE format('ALTER MATERIALIZED VIEW %I.%I RENAME TO %I', mv.schemaname, mv.matviewname, 'live_migration_' || mv.matviewname);

        -- Update the audit table to mark the view as renamed
        EXECUTE format('UPDATE __live_migration.matview_audit SET renamed = true WHERE schemaname = %L AND matviewname = %L', mv.schemaname, mv.matviewname);

        -- Create a view to replace the materialized view
        EXECUTE format('CREATE VIEW %I.%I AS SELECT * FROM %I.%I', mv.schemaname, mv.matviewname, mv.schemaname, 'live_migration_' || mv.matviewname);
        EXECUTE format('CREATE TRIGGER skip_dml_trigger
                        INSTEAD OF INSERT OR UPDATE OR DELETE ON %I.%I
                        FOR EACH ROW EXECUTE FUNCTION __live_migration.skip_dml_function()', mv.schemaname, mv.matviewname);
    END LOOP;
END $$;

COMMIT;
"""
    psql(conn, query)

def restore_matview(conn):
    query = """
BEGIN;

DO $$
DECLARE
    mv RECORD;
BEGIN
    -- Loop through all materialized views in the current schema that have not been renamed
    FOR mv IN
        SELECT schemaname, matviewname
        FROM __live_migration.matview_audit
        WHERE renamed
    LOOP
        -- Drop the view if it exists
        EXECUTE format('DROP VIEW IF EXISTS %I.%I', mv.schemaname, mv.matviewname);

        -- Rename the materialized view with a live_migration_ prefix
        EXECUTE format('ALTER MATERIALIZED VIEW %I.%I RENAME TO %I', mv.schemaname, 'live_migration_' || mv.matviewname, mv.matviewname);

        -- Update the audit table to mark the view as renamed
        EXECUTE format('UPDATE __live_migration.matview_audit SET renamed = false WHERE schemaname = %L AND matviewname = %L', mv.schemaname, mv.matviewname);

    END LOOP;
END $$;

COMMIT;
"""
    psql(conn, query)
