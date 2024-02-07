# Live migration

The live migration docker repository:

https://hub.docker.com/r/timescale/live-migration

When new commits are made to the `team-data-ingest` branch, a new image will be
built and pushed to the `edge` tag.

To build the live-migration docker image locally:

```sh
docker buildx  build --tag live-migration  -f Dockerfile.live-migration .
```

To run the live-migration:

```sh
docker run --rm -it \
  -e PGCOPYDB_SOURCE_PGURI=$SOURCE  \
  -e PGCOPYDB_TARGET_PGURI=$TARGET \
  timescale/live-migration:edge
```

To persist the CDC replicated files to support resuming:

```sh
docker run --rm -it \
  -e PGCOPYDB_SOURCE_PGURI=$SOURCE  \
  -e PGCOPYDB_TARGET_PGURI=$TARGET \
  -v {path/on/host}:/opt/timescale/ts_cdc
  timescale/live-migration:edge
```

To use with docker-compose:

```yaml
networks:
  ts_ex_app:
    driver: bridge
services:
  source:
    image: timescale/timescaledb-ha:pg15.4-ts2.12.2-all
    ports:
      - 5432
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=custompassword1
      - POSTGRES_DB=ts_ex_app_repo
    networks:
      - ts_ex_app
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "postgres"]
      interval: 10s
      timeout: 10s
      retries: 3
      start_period: 10s
      start_interval: 10s
    command:
      - "postgres"
      - "-c"
      - "wal_level=logical"
  target:
    image: timescale/timescaledb-ha:pg15.4-ts2.12.2-all
    ports:
      - 5432
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=custompassword1
      - POSTGRES_DB=ts_ex_app_repo
    networks:
      - ts_ex_app
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "postgres"]
      interval: 10s
      timeout: 10s
      retries: 3
      start_period: 10s
      start_interval: 10s
  live:
    image: timescale/live-migration:edge
    networks:
      - ts_ex_app
    environment:
      PGCOPYDB_SOURCE_PGURI: postgresql://postgres:custompassword1@source/ts_ex_app_repo
      PGCOPYDB_TARGET_PGURI: postgresql://postgres:custompassword1@target/ts_ex_app_repo
    depends_on:
      source:
        condition: service_healthy
      target:
        condition: service_healthy
    volumes:
      - "/Users/adn/dev/timescale/pgcopydb/src/bin/timescale/:/opt/timescale"
```

## Release

1. Update SCRIPT_VERSION to the new version(e.g. 0.0.1).
2. Create a [pull request (PR)](https://github.com/timescale/pgcopydb/pull/29) and ensure it is successfully merged.
3. Create and push a release tag using the following commands:
	```sh
	git tag -a v0.0.1 -m "Release v0.0.1"
	git push <remote_name> v0.0.1
	```
	After the release tag is pushed to the fork, [GitHub Actions](https://github.com/timescale/pgcopydb/blob/main/.github/workflows/docker-publish-ts.yml) will automatically build and publish a Docker image to the docker.io/timescale/live-migration repository.

> [!IMPORTANT]
> In case of changes to version, flags, environment variables, or console messages, ensure that the following are updated to reflect changes:
> 1. Docs: Postgres to Timescale migration - https://docs.timescale.com/migrate/latest/live-migration/live-migration-from-postgres/
> 2. Docs: TimescaleDB to Timescale migration - https://docs.timescale.com/migrate/latest/live-migration/live-migration-from-timescaledb/
> 3. Web Console: https://github.com/timescale/web-cloud/blob/867adad25646ded38574c0aa16fc4e4a604bd9f8/src/pages/project/service/instructions/codeExamples.ts#L65
> 4. Blog: AWS RDS for PostgreSQL to Timescale - https://www.timescale.com/blog/how-to-migrate-from-aws-rds-for-postgresql-to-timescale/
> 5. Blog: PostgreSQL Database to Timescale With (Almost) Zero Downtime - https://www.timescale.com/blog/migrating-a-terabyte-scale-postgresql-database-to-timescale-with-zero-downtime/
