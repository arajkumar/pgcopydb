use anyhow::Result;
use rand::{thread_rng, Rng};
use std::thread;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use test_common::PgVersion::PG15;
use test_common::PsqlInput::Sql;
use test_common::TsVersion::{TS213, TS214};
use test_common::{
    psql, timescaledb, DbAssert, HasConnectionString, InternalConnectionString, PgVersion,
    TsVersion,
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
    docker.run(RunnableImage::from(timescaledb(pg_version, ts_version)).with_network(network_name))
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

    // TODO: this is an arbitrary duration
    thread::sleep(Duration::from_secs(5));

    target_assert.has_table_count("public", "metrics", 1440);

    Ok(())
}
