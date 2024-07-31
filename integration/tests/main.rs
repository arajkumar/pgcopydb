use anyhow::Result;
use postgres::{Config, NoTls};
use rand::{thread_rng, Rng};
use std::process::Command;
use std::str::FromStr;
use std::thread::sleep;
use std::time::{Duration, Instant};
use tempfile::{tempdir, TempDir};
use test_common::PgVersion::{PG14, PG15};
use test_common::PsqlInput::Sql;
use test_common::TsVersion::{TS210, TS213, TS214};
use test_common::{
    configure_cloud_setup, psql, timescaledb, DbAssert, HasConnectionString,
    InternalConnectionString, PgVersion, TsVersion,
};
use testcontainers::{
    clients::Cli, core::WaitFor, images::generic::GenericImage, Container, RunnableImage,
};

fn generate_test_network_name() -> String {
    let mut rng = thread_rng();
    format!("network-{}", rng.gen_range(10_000..=99_999))
}

fn start_source<'a>(
    docker: &'a Cli,
    pg_version: PgVersion,
    ts_version: TsVersion,
    network_name: &'_ str,
) -> Container<'a, GenericImage> {
    docker.run(
        RunnableImage::from((
            timescaledb(pg_version, ts_version),
            vec![String::from("-c"), String::from("wal_level=logical")],
        ))
        .with_network(network_name),
    )
}

fn start_target<'a>(
    docker: &'a Cli,
    pg_version: PgVersion,
    ts_version: TsVersion,
    network_name: &'_ str,
) -> Container<'a, GenericImage> {
    let container = docker
        .run(RunnableImage::from(timescaledb(pg_version, ts_version)).with_network(network_name));
    configure_cloud_setup(&container).unwrap();
    container
}

fn live_migration_image(
    temp_dir: &TempDir,
    source_container: &Container<GenericImage>,
    target_container: &Container<GenericImage>,
) -> GenericImage {
    GenericImage::new("local/live-migration", "dev")
        .with_env_var(
            "PGCOPYDB_SOURCE_PGURI",
            source_container.internal_connection_string().as_str(),
        )
        .with_env_var(
            "PGCOPYDB_TARGET_PGURI",
            target_container.internal_connection_string().as_str(),
        )
        .with_volume(temp_dir.path().to_str().unwrap(), "/opt/timescale/ts_cdc")
}

fn wait_for_source_target_sync(
    source_container: &Container<'_, GenericImage>,
    target_container: &Container<'_, GenericImage>,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();
    let mut source_client =
        Config::from_str(source_container.connection_string().as_str())?.connect(NoTls)?;
    let mut target_client =
        Config::from_str(target_container.connection_string().as_str())?.connect(NoTls)?;
    let source_lsn: String = source_client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])?
        .get(0);
    loop {
        if start.elapsed() > timeout {
            panic!("wal did not sync after {}s", timeout.as_secs())
        }
        let target_has_caught_up = target_client.query_one("SELECT pg_wal_lsn_diff(pg_replication_origin_progress('pgcopydb', true), $1::text::pg_lsn) >= 0;", &[&source_lsn])?.get(0);
        if target_has_caught_up {
            break;
        }
        sleep(Duration::from_secs(1));
    }

    Ok(())
}

#[test]
fn test_fail_on_different_timescaledb_versions() {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let network_name = generate_test_network_name();

    let source_container = start_source(&docker, PG15, TS213, &network_name);
    let target_container = start_target(&docker, PG15, TS214, &network_name);

    let temp_dir = tempdir().unwrap();

    let image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stderr(
        "Source TimescaleDB version (2.13.1) does not match Target TimescaleDB version (2.14.2)",
    ));

    let target =
        RunnableImage::from((image, vec![String::from("snapshot")])).with_network(&network_name);
    docker.run(target);
}

#[test]
fn test_snapshot_creation() {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let network_name = generate_test_network_name();

    let source_container = start_source(&docker, PG15, TS214, &network_name);
    let target_container = start_target(&docker, PG15, TS214, &network_name);

    let temp_dir = tempdir().unwrap();

    let image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stdout(
            "You can now start the migration process",
        ));

    let target =
        RunnableImage::from((image, vec![String::from("snapshot")])).with_network(&network_name);
    docker.run(target);
}

#[test]
fn test_end_to_end_migration() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let network_name = generate_test_network_name();

    let source_container = start_source(&docker, PG15, TS214, &network_name);
    let target_container = start_target(&docker, PG15, TS214, &network_name);

    psql(
        &source_container,
        Sql(r"
		CREATE TABLE metrics(time timestamptz primary key, value float8);
		SELECT create_hypertable('metrics', 'time');
		INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-01-01 00:00:00', '2024-01-31 23:00:00', INTERVAL'1 hour') as time;
	"),
    )?;

    let temp_dir = tempdir().unwrap();

    let snapshot_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stdout(
            "You can now start the migration process",
        ));

    let snapshot = RunnableImage::from((snapshot_image, vec![String::from("snapshot")]))
        .with_network(&network_name);
    let _snapshot = docker.run(snapshot);

    let migrate_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stderr("Applying buffered transactions"));

    let migrate = RunnableImage::from((migrate_image, vec![String::from("migrate")]))
        .with_network(&network_name);
    let _migrate = docker.run(migrate);

    let mut target_assert = DbAssert::new(&target_container.connection_string())?;

    target_assert.has_table_count("public", "metrics", 744);

    psql(&source_container, Sql(r"
		INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-02-01 00:00:00', '2024-02-29 23:00:00', INTERVAL'1 hour') as time;
	")).expect("query should succeed");

    wait_for_source_target_sync(
        &source_container,
        &target_container,
        Duration::from_secs(60),
    )?;

    target_assert.has_table_count("public", "metrics", 1440);

    Ok(())
}

#[test]
fn test_end_to_end_migration_non_public_ext_schema() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let network_name = generate_test_network_name();

    let source_container = start_source(&docker, PG15, TS214, &network_name);
    let target_container = start_target(&docker, PG15, TS214, &network_name);

	// Drop and recreate timescaledb extension with schema "CustomSchema".
	psql(
		&source_container,
		Sql(r"
		DROP EXTENSION IF EXISTS timescaledb CASCADE;
	"),
	)?;

	// We need a different connection to create the extension.
	psql(
		&source_container,
		Sql(r#"
		CREATE SCHEMA IF NOT EXISTS "CustomSchema";
		CREATE EXTENSION timescaledb WITH SCHEMA "CustomSchema";
	"#),
	)?;

    psql(
        &source_container,
        Sql(r#"
		CREATE TABLE metrics(time timestamptz primary key, value float8);
		SELECT "CustomSchema".create_hypertable('metrics', 'time');
		INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-01-01 00:00:00', '2024-01-31 23:00:00', INTERVAL'1 hour') as time;
	"#),
    )?;

    let temp_dir = tempdir().unwrap();

    let snapshot_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stdout(
            "You can now start the migration process",
        ));

    let snapshot = RunnableImage::from((snapshot_image, vec![String::from("snapshot")]))
        .with_network(&network_name);
    let _snapshot = docker.run(snapshot);

    let migrate_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stderr("Applying buffered transactions"));

    let migrate = RunnableImage::from((migrate_image, vec![String::from("migrate")]))
        .with_network(&network_name);
    let _migrate = docker.run(migrate);

    let mut source_assert = DbAssert::new(&source_container.connection_string())?;
    let mut target_assert = DbAssert::new(&target_container.connection_string())?;

    source_assert.has_table_count("public", "metrics", 744);
    target_assert.has_table_count("public", "metrics", 744);

    psql(&source_container, Sql(r"
		INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-02-01 00:00:00', '2024-02-29 23:00:00', INTERVAL'1 hour') as time;
	")).expect("query should succeed");

    wait_for_source_target_sync(
        &source_container,
        &target_container,
        Duration::from_secs(60),
    )?;

    source_assert.has_table_count("_timescaledb_catalog", "hypertable", 1);
    target_assert.has_table_count("_timescaledb_catalog", "hypertable", 1);

    source_assert.has_table_count("public", "metrics", 1440);
    target_assert.has_table_count("public", "metrics", 1440);

    Ok(())
}

#[test]
fn test_cross_ext_version_ts2ts_migration() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let network_name = generate_test_network_name();

    let source_container = start_source(&docker, PG14, TS210, &network_name);
    let target_container = start_target(&docker, PG15, TS214, &network_name);

    // Drop and recreate timescaledb extension with older version using
    // a different connection.
    psql(
        &source_container,
        Sql(r"
		DROP EXTENSION timescaledb;
	"),
    )?;

    // Create timescaledb extension with older version.
    psql(
        &source_container,
        Sql(r#"
		CREATE EXTENSION timescaledb WITH VERSION "2.5.0";
	"#),
    )?;

    // TODO: Add tests to verify partitioning using functions
    psql(
        &source_container,
        Sql(r"
		CREATE TABLE metrics(time timestamptz,
							 id integer, value float8,
							 primary key (time, id));
		SELECT create_hypertable('metrics', 'time',
								 chunk_time_interval => '1hr'::interval);
		-- add 2nd dimension
		SELECT add_dimension('metrics', 'id', number_partitions => 4);

        INSERT INTO metrics(time, value, id) SELECT time, random(), (floor(random() * 10000) + 1) FROM generate_series('2024-01-01 00:00:00', '2024-02-01 00:00:00', INTERVAL'1 hour') as time;
	"),
    )?;

    let mut source_assert = DbAssert::new(&source_container.connection_string())?;
    let mut target_assert = DbAssert::new(&target_container.connection_string())?;

    // assert that the source and target extensions are different
    source_assert.is_true("SELECT EXISTS (SELECT extversion FROM pg_extension WHERE extname = 'timescaledb' AND extversion = '2.5.0')");
    target_assert.is_true("SELECT EXISTS (SELECT extversion FROM pg_extension WHERE extname = 'timescaledb' AND extversion = '2.14.2')");

    let temp_dir = tempdir().unwrap();

    let snapshot_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stdout(
            "You can now start the migration process",
        ));

    let snapshot = RunnableImage::from((
        snapshot_image,
        vec![
            String::from("snapshot"),
            String::from("--migrate-across-timescaledb-versions"),
        ],
    ))
    .with_network(&network_name);
    let _snapshot = docker.run(snapshot);

    let migrate_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stderr("Applying buffered transactions"));

    let migrate = RunnableImage::from((
        migrate_image,
        vec![
            String::from("migrate"),
            String::from("--migrate-across-timescaledb-versions"),
        ],
    ))
    .with_network(&network_name);
    let _migrate = docker.run(migrate);

    // Verify that the dimensions are equal to 2
    let dimension_query = "SELECT count(*) = 2 FROM timescaledb_information.dimensions WHERE hypertable_schema = 'public' AND hypertable_name = 'metrics'";
    source_assert.is_true(dimension_query);
    target_assert.is_true(dimension_query);

    source_assert.has_table_count("public", "metrics", 745);
    target_assert.has_table_count("public", "metrics", 745);

    psql(&source_container, Sql(r"
        INSERT INTO metrics(time, value, id) SELECT time, random(), (floor(random() * 10000) + 1) FROM generate_series('2024-02-01 00:00:00', '2024-03-01 00:00:00', INTERVAL'1 hour') as time;
	")).expect("query should succeed");

    wait_for_source_target_sync(
        &source_container,
        &target_container,
        Duration::from_secs(60),
    )?;

    source_assert.has_table_count("public", "metrics", 1442);
    target_assert.has_table_count("public", "metrics", 1442);

    Ok(())
}

#[test]
fn test_end_to_end_migration_with_extension_upgrade() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let network_name = generate_test_network_name();

    let source_container = start_source(&docker, PG15, TS214, &network_name);
    let target_container = start_target(&docker, PG15, TS214, &network_name);

    // Drop and recreate timescaledb extension with older version using
    // a different connection.
    psql(
        &source_container,
        Sql(r"
		DROP EXTENSION timescaledb;
	"),
    )?;

    // Create timescaledb extension with older version.
    psql(
        &source_container,
        Sql(r#"
		CREATE EXTENSION timescaledb WITH VERSION "2.13.0";
	"#),
    )?;

    // Create hypertable and insert data.
    psql(
        &source_container,
        Sql(r#"
		CREATE TABLE metrics(time timestamptz primary key, value float8);
		SELECT create_hypertable('metrics', 'time');
		INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-01-01 00:00:00', '2024-01-31 23:00:00', INTERVAL'1 hour') as time;
	"#),
    )?;

    // Upgrade timescaledb extension to 2.14.2.
    psql(
        &source_container,
        Sql(r#"
		ALTER EXTENSION timescaledb UPDATE TO "2.14.2";
	"#),
    )?;

    let mut source_assert = DbAssert::new(&source_container.connection_string())?;
    // Check oid of _timescaledb_catalog.hyptertable is greater than
    // _timescaledb_catalog.dimension due to extension upgrade.
    // The ordering of extension config tables must be taken care of during
    // extension migration.
    source_assert.is_true("SELECT '_timescaledb_catalog.hypertable'::regclass::oid > '_timescaledb_catalog.dimension'::regclass::oid");

    let temp_dir = tempdir().unwrap();

    let snapshot_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stdout(
            "You can now start the migration process",
        ));

    let snapshot = RunnableImage::from((snapshot_image, vec![String::from("snapshot")]))
        .with_network(&network_name);
    let _snapshot = docker.run(snapshot);

    let migrate_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stderr("Applying buffered transactions"));

    let migrate = RunnableImage::from((migrate_image, vec![String::from("migrate")]))
        .with_network(&network_name);
    let _migrate = docker.run(migrate);

    let mut target_assert = DbAssert::new(&target_container.connection_string())?;

    target_assert.has_table_count("public", "metrics", 744);

    psql(&source_container, Sql(r"
		INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-02-01 00:00:00', '2024-02-29 23:00:00', INTERVAL'1 hour') as time;
	")).expect("query should succeed");

    wait_for_source_target_sync(
        &source_container,
        &target_container,
        Duration::from_secs(60),
    )?;

    target_assert.has_table_count("public", "metrics", 1440);

    Ok(())
}

#[test]
fn test_exclude_existing_table_data() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let network_name = generate_test_network_name();

    let source_container = start_source(&docker, PG15, TS214, &network_name);
    let target_container = start_target(&docker, PG15, TS214, &network_name);

    psql(
        &source_container,
        Sql(r#"
		CREATE TABLE "Metrics_data_excluded"(time timestamptz primary key, value float8);
		INSERT INTO "Metrics_data_excluded"(time, value) SELECT time, random() FROM generate_series('2024-01-01 00:00:00', '2024-01-31 23:00:00', INTERVAL'1 hour') as time;

		CREATE TABLE metrics(time timestamptz primary key, value float8);
		INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-01-01 00:00:00', '2024-01-31 23:00:00', INTERVAL'1 hour') as time;
	"#),
    )?;

    let temp_dir = tempdir().unwrap();

    let snapshot_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stdout(
            "You can now start the migration process",
        ));

    let snapshot = RunnableImage::from((snapshot_image, vec![String::from("snapshot")]))
        .with_network(&network_name);
    let _snapshot = docker.run(snapshot);

    let migrate_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stderr("Applying buffered transactions"));

    let migrate = RunnableImage::from((
        migrate_image,
        vec![
            String::from("migrate"),
            String::from("--skip-table-data"),
            String::from("public.Metrics_data_excluded"),
        ],
    ))
    .with_network(&network_name);
    let _migrate = docker.run(migrate);

    let mut target_assert = DbAssert::new(&target_container.connection_string())?;

    target_assert.has_table_count("public", "Metrics_data_excluded", 0);
    target_assert.has_table_count("public", "metrics", 744);

    psql(
        &source_container,
        Sql(r#"
        INSERT INTO "Metrics_data_excluded"(time, value) SELECT time, random() FROM generate_series('2024-02-01 00:00:00', '2024-02-29 23:00:00', INTERVAL'1 hour') as time;
        INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-02-01 00:00:00', '2024-02-29 23:00:00', INTERVAL'1 hour') as time;
    "#),
    )?;

    wait_for_source_target_sync(
        &source_container,
        &target_container,
        Duration::from_secs(60),
    )?;

    // Currently, we do not skip data during live replay. This is why
    // "metrics_data_excluded" has count as 696 which was added during CDC.
    target_assert.has_table_count("public", "Metrics_data_excluded", 696);
    target_assert.has_table_count("public", "metrics", 1440);

    Ok(())
}

#[test]
fn test_skip_dml_on_matview() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let network_name = generate_test_network_name();

    let source_container = start_source(&docker, PG15, TS214, &network_name);
    let target_container = start_target(&docker, PG15, TS214, &network_name);

    psql(
        &source_container,
        Sql(r"
		CREATE TABLE metrics(time timestamptz primary key, value float8);
		INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-01-01 00:00:00', '2024-01-31 23:00:00', INTERVAL'1 hour') as time;

		CREATE MATERIALIZED VIEW metrics_count AS SELECT count(1) FROM metrics;

		CREATE SCHEMA IF NOT EXISTS metrics_matview;
		CREATE MATERIALIZED VIEW metrics_matview.metrics_count_1 AS SELECT count(1) FROM metrics;
	"),
    )?;

    let temp_dir = tempdir().unwrap();

    let snapshot_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stdout(
            "You can now start the migration process",
        ));

    let snapshot = RunnableImage::from((snapshot_image, vec![String::from("snapshot")]))
        .with_network(&network_name);
    let _snapshot = docker.run(snapshot);

    let migrate_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stderr("Applying buffered transactions"));
    let migrate = RunnableImage::from((migrate_image, vec![String::from("migrate")]))
        .with_network(&network_name);
    let _migrate = docker.run(migrate);

    let mut target_assert = DbAssert::new(&target_container.connection_string())?;

    target_assert.has_table_count("public", "metrics", 744);

    psql(
        &source_container,
        Sql(r"
        INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-02-01 00:00:00', '2024-02-29 23:00:00', INTERVAL'1 hour') as time;

		-- By default Postgres skips changes happening on materialized views due
		-- REFRESH MATERIALIZED VIEW CONCURRENTLY.
		-- However in some situations we see that Postgres does not skip the changes
		-- and logical messages are generated for the materialized view.
		-- Drop/Recreate is one such scenario which causes logical messages to
		-- be generated.
		-- Refer https://github.com/timescale/pgcopydb/pull/105.
		DROP MATERIALIZED VIEW IF EXISTS metrics_count;
		CREATE MATERIALIZED VIEW metrics_count AS SELECT count(1) FROM metrics;

		DROP MATERIALIZED VIEW IF EXISTS metrics_matview.metrics_count_1;
		CREATE MATERIALIZED VIEW metrics_matview.metrics_count_1 AS SELECT count(1) FROM metrics;
    "),
    )?;

    wait_for_source_target_sync(
        &source_container,
        &target_container,
        Duration::from_secs(60),
    )?;

    target_assert.has_table_count("public", "metrics", 1440);

    Ok(())
}

#[test]
fn test_case_sensitive_object_live_replication() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let network_name = generate_test_network_name();

    let source_container = start_source(&docker, PG15, TS214, &network_name);
    let target_container = start_target(&docker, PG15, TS214, &network_name);

    psql(
        &source_container,
        Sql(r#"
        CREATE SCHEMA "Sensitive_Schema0";
		CREATE TABLE "Sensitive_Schema0"."Sensitive_Metric1"(time timestamptz primary key, value float8);
        SELECT create_hypertable('"Sensitive_Schema0"."Sensitive_Metric1"', by_range('time'));
		INSERT INTO "Sensitive_Schema0"."Sensitive_Metric1"(time, value) SELECT time, random() FROM generate_series('2024-01-01 00:00:00', '2024-01-31 23:00:00', INTERVAL'1 hour') as time;
        CREATE ROLE "Sensitive_Role2" WITH LOGIN PASSWORD 'pass';
        GRANT ALL ON SCHEMA public TO "Sensitive_Role2";
	"#),
    )?;

    let temp_dir = tempdir().unwrap();

    let snapshot_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stdout(
            "You can now start the migration process",
        ));

    let snapshot = RunnableImage::from((snapshot_image, vec![String::from("snapshot")]))
        .with_network(&network_name);
    let _snapshot = docker.run(snapshot);

    let migrate_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stderr("Applying buffered transactions"));
    let migrate = RunnableImage::from((migrate_image, vec![String::from("migrate")]))
        .with_network(&network_name);
    let _migrate = docker.run(migrate);

    {
        let mut target_assert = DbAssert::new(&target_container.connection_string())?;
        target_assert.has_table("Sensitive_Schema0", "Sensitive_Metric1");
        target_assert.has_table_count("Sensitive_Schema0", "Sensitive_Metric1", 744);
    }

    // Perform CDC on sensitive SQL objects.
    // Live migration below v0.0.15 could not replicate the following INSERT statement.
    psql(
        &source_container,
        Sql(r#"
        INSERT INTO "Sensitive_Schema0"."Sensitive_Metric1"(time, value) SELECT time, random() FROM generate_series('2024-02-01 00:00:00', '2024-02-29 23:00:00', INTERVAL'1 hour') as time;
    "#),
    )?;

    wait_for_source_target_sync(
        &source_container,
        &target_container,
        Duration::from_secs(60),
    )?;

    let mut target_assert = DbAssert::new(&target_container.connection_string())?;
    target_assert.has_table_count("Sensitive_Schema0", "Sensitive_Metric1", 1440);

    Ok(())
}

#[test]
fn test_jobs_from_different_owner_migration() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let network_name = generate_test_network_name();

    let source_container = start_source(&docker, PG15, TS214, &network_name);
    let target_container = start_target(&docker, PG15, TS214, &network_name);

    // Postgres role will create test_role. We can imagine Postgres role here as a tsdbadmin role on MST.
    psql(
        &source_container,
        Sql(r"
		CREATE TABLE metrics(time timestamptz primary key, value float8);
		INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-01-01 00:00:00', '2024-01-31 23:00:00', INTERVAL'1 hour') as time;
        CREATE ROLE test_role WITH LOGIN PASSWORD 'pass';
        GRANT ALL ON SCHEMA public TO test_role;
	"),
    )?;

    // Create a job with test_role as job owner.
    psql(
        &source_container,
        Sql(r"
        SET SESSION ROLE 'test_role';
        CREATE OR REPLACE PROCEDURE test_action(job_id int, config jsonb) LANGUAGE PLPGSQL AS $$ BEGIN PERFORM 1; END $$;
        SELECT add_job('test_action', '1h')
    "),
    )?;

    {
        let mut source_assert = DbAssert::new(&source_container.connection_string())?;
        source_assert.has_user_defined_job("test_action", "test_role");
    }

    let temp_dir = tempdir().unwrap();

    let snapshot_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stdout(
            "You can now start the migration process",
        ));

    let snapshot = RunnableImage::from((snapshot_image, vec![String::from("snapshot")]))
        .with_network(&network_name);
    let _snapshot = docker.run(snapshot);

    let migrate_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stderr("Applying buffered transactions"));
    let migrate = RunnableImage::from((migrate_image, vec![String::from("migrate")]))
        .with_network(&network_name);
    let _migrate = docker.run(migrate);

    let mut target_assert = DbAssert::new(&target_container.connection_string())?;
    target_assert.has_user_defined_job("test_action", "test_role");

    Ok(())
}

#[test]
fn test_pg_to_ts_with_table_data_filtering() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let network_name = generate_test_network_name();

    let source_container = start_source(&docker, PG15, TS214, &network_name);
    let target_container = start_target(&docker, PG15, TS214, &network_name);

    psql(
        &source_container,
        Sql(r#"
		-- We want to test Postgres to TimescaleDB migration.
		DROP EXTENSION IF EXISTS timescaledb CASCADE;

		CREATE TABLE "Metrics_data_excluded"(time timestamptz primary key, value float8);
		INSERT INTO "Metrics_data_excluded"(time, value) SELECT time, random() FROM generate_series('2024-01-01 00:00:00', '2024-01-31 23:00:00', INTERVAL'1 hour') as time;

		CREATE TABLE metrics(id serial, time timestamptz, value float8, primary key(id, time));
		INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-01-01 00:00:00', '2024-01-31 23:00:00', INTERVAL'1 hour') as time;
	"#),
    )?;

    let temp_dir = tempdir().unwrap();

    let snapshot_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stdout(
            "You can now start the migration process",
        ));

    let snapshot = RunnableImage::from((snapshot_image, vec![String::from("snapshot")]))
        .with_network(&network_name);
    let _snapshot = docker.run(snapshot);

    let migrate_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stdout("send a SIGUSR1 signal with"));

    let migrate = RunnableImage::from((
        migrate_image,
        vec![
            String::from("migrate"),
            String::from("--skip-table-data"),
            String::from("public.Metrics_data_excluded"),
        ],
    ))
    .with_network(&network_name);

    let migrate = docker.run(migrate);

    // Wait for the signal handler to be installed.
    sleep(Duration::from_secs(5));

    psql(
        &target_container,
        Sql(r"
			SELECT create_hypertable('metrics', by_range('time', '1hr'::interval));
	"),
    )?;

    Command::new("docker")
        .args(["kill", "--signal", "SIGUSR1", migrate.id()])
        .spawn()
        .expect("failed to send signal");

    psql(
        &source_container,
        Sql(r#"
        INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-02-01 00:00:00', '2024-02-29 23:00:00', INTERVAL'1 hour') as time;
    "#),
    )?;

    wait_for_source_target_sync(
        &source_container,
        &target_container,
        Duration::from_secs(60),
    )?;

    let mut target_assert = DbAssert::new(&target_container.connection_string())?;
    target_assert.has_table_count("public", "Metrics_data_excluded", 0);
    target_assert.has_table_count("public", "metrics", 1440);

    Ok(())
}

#[test]
fn test_pg_to_pg_with_table_data_filtering() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let network_name = generate_test_network_name();

    let source_container = start_source(&docker, PG15, TS214, &network_name);
    let target_container = start_target(&docker, PG15, TS214, &network_name);

    psql(
        &target_container,
        Sql(r#"
		-- We want to test Postgres to Postgres migration.
		DROP EXTENSION IF EXISTS timescaledb CASCADE;
		"#),
    )?;

    psql(
        &source_container,
        Sql(r#"
		-- We want to test Postgres to TimescaleDB migration.
		DROP EXTENSION IF EXISTS timescaledb CASCADE;

		CREATE TABLE "Metrics_data_excluded"(time timestamptz primary key, value float8);
		INSERT INTO "Metrics_data_excluded"(time, value) SELECT time, random() FROM generate_series('2024-01-01 00:00:00', '2024-01-31 23:00:00', INTERVAL'1 hour') as time;

		CREATE TABLE metrics(id serial primary key, time timestamptz, value float8);
		INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-01-01 00:00:00', '2024-01-31 23:00:00', INTERVAL'1 hour') as time;
	"#),
    )?;

    let temp_dir = tempdir().unwrap();

    let snapshot_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stdout(
            "You can now start the migration process",
        ));

    let snapshot = RunnableImage::from((snapshot_image, vec![String::from("snapshot")]))
        .with_network(&network_name);
    let _snapshot = docker.run(snapshot);

    let migrate_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stderr("Applying buffered transactions"));

    let migrate = RunnableImage::from((
        migrate_image,
        vec![
            String::from("migrate"),
            String::from("--skip-table-data"),
            String::from("public.Metrics_data_excluded"),
        ],
    ))
    .with_network(&network_name);

    let _migrate = docker.run(migrate);

    psql(
        &source_container,
        Sql(r#"
        INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-02-01 00:00:00', '2024-02-29 23:00:00', INTERVAL'1 hour') as time;
    "#),
    )?;

    wait_for_source_target_sync(
        &source_container,
        &target_container,
        Duration::from_secs(60),
    )?;

    let mut target_assert = DbAssert::new(&target_container.connection_string())?;
    target_assert.has_table_count("public", "Metrics_data_excluded", 0);
    target_assert.has_table_count("public", "metrics", 1440);

    Ok(())
}

#[test]
fn test_compatible_migration_from_pg_to_ts() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let network_name = generate_test_network_name();

    let source_container = start_source(&docker, PG15, TS214, &network_name);
    let target_container = start_target(&docker, PG15, TS214, &network_name);

    psql(
        &source_container,
        Sql(r"
		-- We want to test Postgres to TimescaleDB migration.
		DROP EXTENSION IF EXISTS timescaledb CASCADE;

		-- Foreign key reference from a hypertable to a regular table
		CREATE TABLE metrics_type(id int primary key, type text);
		INSERT INTO metrics_type(id, type) SELECT id, 'type' FROM generate_series(1, 100) as id;

		CREATE TABLE metrics(id serial primary key, type_id int, time timestamptz, value float8, FOREIGN KEY (type_id) REFERENCES metrics_type(id));

		CREATE UNIQUE INDEX metrics_unique_idx ON metrics (time, id);

		INSERT INTO metrics(time, value, type_id) SELECT time, random(), (floor(random() * 100) + 1) FROM generate_series('2024-01-01 00:00:00', '2024-01-31 23:00:00', INTERVAL'1 hour') as time;
	"),
    )?;

    let temp_dir = tempdir().unwrap();

    let snapshot_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stdout(
            "You can now start the migration process",
        ));

    let snapshot = RunnableImage::from((snapshot_image, vec![String::from("snapshot")]))
        .with_network(&network_name);
    let _snapshot = docker.run(snapshot);

    let migrate_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stdout("send a SIGUSR1 signal with"));

    let migrate = RunnableImage::from((migrate_image, vec![String::from("migrate")]))
        .with_network(&network_name);

    let migrate = docker.run(migrate);

    // Wait for the signal handler to be installed.
    sleep(Duration::from_secs(5));

    psql(
        &target_container,
        Sql(r"
			SELECT create_hypertable('metrics', by_range('id', 100));
	"),
    )?;

    Command::new("docker")
        .args(["kill", "--signal", "SIGUSR1", migrate.id()])
        .spawn()
        .expect("failed to send signal");

    psql(
        &source_container,
        Sql(r"
        INSERT INTO metrics(time, value, type_id) SELECT time, random(), (floor(random() * 100) + 1) FROM generate_series('2024-02-01 00:00:00', '2024-02-29 23:00:00', INTERVAL'1 hour') as time;
    "),
    )?;

    wait_for_source_target_sync(
        &source_container,
        &target_container,
        Duration::from_secs(60),
    )?;

    let mut target_assert = DbAssert::new(&target_container.connection_string())?;
    target_assert.has_table_count("public", "metrics", 1440);

    target_assert.has_pk("public", "metrics", vec!["id"]);
    target_assert.has_fk_constraint_named("metrics_type_id_fkey");
    target_assert.has_unique_index("public", "metrics", vec!["time", "id"]);

    Ok(())
}

#[test]
fn test_skip_incompatible_objects_from_pg_to_ts() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let network_name = generate_test_network_name();

    let source_container = start_source(&docker, PG15, TS214, &network_name);
    let target_container = start_target(&docker, PG15, TS214, &network_name);

    psql(
        &source_container,
        Sql(r"
		-- We want to test Postgres to TimescaleDB migration.
		DROP EXTENSION IF EXISTS timescaledb CASCADE;

		CREATE TABLE metrics(id serial primary key, time timestamptz, value float8);
		CREATE UNIQUE INDEX metrics_unique_idx ON metrics (time, id);

		-- Foreign key reference from a regular table to a hypertable not allowed
		-- This must be ignored during migration.
		CREATE TABLE metrics_type(id int primary key, type text, FOREIGN KEY(id) REFERENCES metrics(id));

		INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-01-01 00:00:00', '2024-01-31 23:00:00', INTERVAL'1 hour') as time;
	"),
    )?;

    let temp_dir = tempdir().unwrap();

    let snapshot_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stdout(
            "You can now start the migration process",
        ));

    let snapshot = RunnableImage::from((snapshot_image, vec![String::from("snapshot")]))
        .with_network(&network_name);
    let _snapshot = docker.run(snapshot);

    let migrate_image = live_migration_image(&temp_dir, &source_container, &target_container)
        .with_wait_for(WaitFor::message_on_stdout("send a SIGUSR1 signal with"));

    let migrate = RunnableImage::from((
        migrate_image,
        vec![
            String::from("migrate"),
            String::from("--skip-hypertable-incompatible-objects"),
        ],
    ))
    .with_network(&network_name);

    let migrate = docker.run(migrate);

    // Wait for the signal handler to be installed.
    sleep(Duration::from_secs(5));

    psql(
        &target_container,
        Sql(r"
			SELECT create_hypertable('metrics', by_range('time', '1hr'::interval));
	"),
    )?;

    Command::new("docker")
        .args(["kill", "--signal", "SIGUSR1", migrate.id()])
        .spawn()
        .expect("failed to send signal");

    psql(
        &source_container,
        Sql(r"
        INSERT INTO metrics(time, value) SELECT time, random() FROM generate_series('2024-02-01 00:00:00', '2024-02-29 23:00:00', INTERVAL'1 hour') as time;
    "),
    )?;

    wait_for_source_target_sync(
        &source_container,
        &target_container,
        Duration::from_secs(60),
    )?;

    let mut target_assert = DbAssert::new(&target_container.connection_string())?;
    let mut source_assert = DbAssert::new(&source_container.connection_string())?;

    target_assert.has_table_count("public", "metrics", 1440);

    source_assert.has_pk("public", "metrics", vec!["id"]);
    target_assert.not_has_pk("public", "metrics", vec!["id"]);

    source_assert.has_fk_constraint_named("metrics_type_id_fkey");
    source_assert.has_fk(
        "public",
        "metrics_type",
        vec!["id"],
        "public",
        "metrics",
        vec!["id"],
    );
    target_assert.not_has_fk(
        "public",
        "metrics_type",
        vec!["id"],
        "public",
        "metrics",
        vec!["id"],
    );

    target_assert.has_unique_index("public", "metrics", vec!["time", "id"]);

    Ok(())
}
