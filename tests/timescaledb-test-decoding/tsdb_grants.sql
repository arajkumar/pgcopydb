-- +migration Up

ALTER SCHEMA _timescaledb_internal OWNER TO tsdbadmin ;

GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA _timescaledb_catalog TO tsdbadmin;

GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA _timescaledb_config TO tsdbadmin;

GRANT USAGE, UPDATE, SELECT ON ALL SEQUENCES IN SCHEMA _timescaledb_catalog TO tsdbadmin;

GRANT USAGE, UPDATE, SELECT ON ALL SEQUENCES IN SCHEMA _timescaledb_config TO tsdbadmin;

GRANT SET ON PARAMETER timescaledb.restoring TO tsdbadmin;

-- +migration StatementBegin
DO LANGUAGE plpgsql $$
BEGIN
    IF NOT EXISTS (SELECT
                   FROM pg_catalog.pg_roles
                   WHERE rolname = 'ts_logical')
    THEN
        CREATE ROLE ts_logical;
    END IF;
END
$$;
-- +migration StatementEnd

-- Following GRANTs enable tsdbadmin to perform logical-decoding using Timescale Cloud
-- as a target database.
GRANT EXECUTE ON FUNCTION pg_catalog.pg_replication_origin_oid(text) TO ts_logical;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_replication_origin_create(text) TO ts_logical;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_replication_origin_advance(text, pg_lsn) TO ts_logical;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_replication_origin_progress(text, boolean) TO ts_logical;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_replication_origin_drop(text) TO ts_logical;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_replication_origin_session_setup(text) TO ts_logical;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_replication_origin_xact_setup(pg_lsn, timestamp WITH TIME ZONE) TO ts_logical;

-- +migration StatementBegin
CREATE OR REPLACE FUNCTION public.set_session_replication_role(TEXT) RETURNS VOID SECURITY DEFINER LANGUAGE SQL AS $$
   SELECT pg_catalog.set_config('session_replication_role', $1, false);
$$;
-- +migration StatementEnd

-- +migration StatementBegin
DO LANGUAGE plpgsql $$
BEGIN
    IF pg_catalog.current_setting('server_version_num')::int >= 150000
    THEN
        EXECUTE 'GRANT SET ON PARAMETER session_replication_role TO ts_logical';
        DROP FUNCTION public.set_session_replication_role;
    ELSE
        REVOKE EXECUTE ON FUNCTION public.set_session_replication_role FROM public;
        GRANT EXECUTE ON FUNCTION public.set_session_replication_role TO ts_logical;
    END IF;
END
$$;
-- +migration StatementEnd

GRANT EXECUTE ON FUNCTION _timescaledb_functions.stop_background_workers() TO ts_logical;

GRANT ts_logical TO tsdbadmin;
