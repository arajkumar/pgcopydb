use tempfile::tempdir;
use test_common::{timescaledb, HasConnectionString, PgVersion};
use testcontainers::{clients::Cli, core::WaitFor, images::generic::GenericImage, RunnableImage};

#[test]
fn test_snapshot_creation() {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_image = RunnableImage::from((
        timescaledb(PgVersion::PG15),
        vec![String::from("-c"), String::from("wal_level=logical")],
    ));
    let source_container = docker.run(source_image);
    let target_container = docker.run(timescaledb(PgVersion::PG15));

    let temp_dir = tempdir().unwrap();
    let image = GenericImage::new("timescale/live-migration", "main")
        .with_env_var(
            "PGCOPYDB_SOURCE_PGURI",
            source_container
                .connection_string()
                .as_str()
                .replace("127.0.0.1", "host.docker.internal"),
        )
        .with_env_var(
            "PGCOPYDB_TARGET_PGURI",
            target_container
                .connection_string()
                .as_str()
                .replace("127.0.0.1", "host.docker.internal"),
        )
        .with_volume(temp_dir.path().to_str().unwrap(), "/opt/timescale/ts_cdc")
        .with_wait_for(WaitFor::message_on_stdout(
            "You can now start the migration process",
        ));

    let target = RunnableImage::from((image, vec![String::from("snapshot")]));
    let _live_migration_snapshot = docker.run(target);
}
