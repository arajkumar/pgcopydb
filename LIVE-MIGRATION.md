# Live migration

The live migration docker repository:

https://hub.docker.com/r/timescaledev/live-migration

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
  timescaledev/live-migration:edge
```

To persist the CDC replicated files to support resuming (currently not
implemented):

```sh
docker run --rm -it \
  -e PGCOPYDB_SOURCE_PGURI=$SOURCE  \
  -e PGCOPYDB_TARGET_PGURI=$TARGET \
  -v {path/on/host}:/opt/timescale/ts_cdc
  timescaledev/live-migration:edge
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
    image: timescaledev/live-migration:edge
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
