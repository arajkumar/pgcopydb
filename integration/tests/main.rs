use rand::{thread_rng, Rng};
use tempfile::tempdir;
use test_common::{timescaledb, InternalConnectionString, PgVersion};
use testcontainers::{clients::Cli, core::WaitFor, images::generic::GenericImage, RunnableImage};

fn generate_test_network_name() -> String {
    let mut rng = thread_rng();
    format!("network-{}", rng.gen_range(10_000..=99_999))
}

#[test]
fn test_snapshot_creation() {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let network_name = generate_test_network_name();

    let source_container = docker.run(
        RunnableImage::from((
            timescaledb(PgVersion::PG15),
            vec![String::from("-c"), String::from("wal_level=logical")],
        ))
        .with_network(&network_name),
    );
    let target_container =
        docker.run(RunnableImage::from(timescaledb(PgVersion::PG15)).with_network(&network_name));

    let temp_dir = tempdir().unwrap();

    let image = GenericImage::new("local/live-migration", "dev")
        .with_env_var(
            "PGCOPYDB_SOURCE_PGURI",
            source_container.internal_connection_string().as_str(),
        )
        .with_env_var(
            "PGCOPYDB_TARGET_PGURI",
            target_container.internal_connection_string().as_str(),
        )
        .with_volume(temp_dir.path().to_str().unwrap(), "/opt/timescale/ts_cdc")
        .with_wait_for(WaitFor::message_on_stdout(
            "You can now start the migration process",
        ));

    let target =
        RunnableImage::from((image, vec![String::from("snapshot")])).with_network(&network_name);
    docker.run(target);
}
