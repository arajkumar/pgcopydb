#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

# make sure source and target databases are ready
pgcopydb ping --source ${POSTGRES_SOURCE} --target ${POSTGRES_TARGET}

psql -a ${POSTGRES_SOURCE} <<EOF
create role tsdbadmin NOSUPERUSER CREATEDB REPLICATION NOCREATEROLE LOGIN PASSWORD '0wn3d';
create database tsdb owner tsdbadmin;
EOF

psql -o /tmp/c.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pgcopydb/rides.sql

copy="\COPY rides FROM nyc_data_rides.10k.csv CSV"
psql -d ${PGCOPYDB_SOURCE_PGURI} -1 -c "${copy}"

# take a snapshot using role tsdb on source database
coproc ( pgcopydb snapshot --follow --plugin wal2json )

# wait for snapshot to be created
while [ ! -f /tmp/pgcopydb/snapshot ]; do sleep 1; done

# copying roles needs superuser
# and we use the postgres database here still
pgcopydb copy roles --source ${POSTGRES_SOURCE} --target ${POSTGRES_TARGET} --resume

# now create the tsdb database on the target, owned by new role tsdb
psql ${POSTGRES_TARGET} <<EOF
create database tsdb owner tsdbadmin;
EOF

psql ${PGCOPYDB_TARGET_PGURI_SU} -f /usr/src/pgcopydb/tsdb_grants.sql

# copy the extensions separately, needs superuser (both on source and target)
pg_dump -d "$PGCOPYDB_SOURCE_PGURI_SU" \
  --format=plain \
  --quote-all-identifiers \
  --no-tablespaces \
  --no-owner \
  --no-privileges \
  --extension='timescaledb' \
  --exclude-schema='public' \
  --exclude-table='_timescaledb_internal.*' \
  --data-only \
  --file=/tmp/ext-dump.sql

psql -d "$PGCOPYDB_TARGET_PGURI_SU" \
    -c 'select public.timescaledb_pre_restore();' \
    -f /tmp/ext-dump.sql \
    -f - <<'EOF'
begin;
select public.timescaledb_post_restore();

-- disable all background jobs
select public.alter_job(id::integer, scheduled=>false)
from _timescaledb_config.bgw_job
where id >= 1000
;
commit;
EOF

pgcopydb stream setup

# now clone with superuser privileges, seems to be required for timescaledb
pgcopydb clone --resume \
         --skip-extensions \
         --source ${PGCOPYDB_SOURCE_PGURI_SU} \
         --target ${PGCOPYDB_TARGET_PGURI_SU}


# Insert some data into metrics hypertable. Since we have 1hr chunk,
# we will have 3 chunks after this.
psql -a ${PGCOPYDB_SOURCE_PGURI} <<EOF
BEGIN;
    INSERT INTO metrics (time, name, value) VALUES
    (now() - '1h'::interval, 'test-1', -1),
    (now(), 'test', 0),
    (now() + '1h'::interval, 'test+1', 1);
COMMIT;
EOF

# check that the source has 3 rows
sql="select count(*) from metrics"
test 3 -eq `psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"`
sql="select count(*) from show_chunks('metrics')"
test 3 -eq `psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"`

# Now drop the 1st chunk, which would generate TRUNCATE message.
# similar to postgres logical partitioning replication published via partition
# root, we should ignore truncate otherwise the mapping would cause the whole
# table to be truncated.
psql -a ${PGCOPYDB_SOURCE_PGURI} <<EOF
BEGIN;
    SELECT drop_chunks('metrics', older_than => now());
COMMIT;
EOF

pgcopydb stream sentinel set apply

pgcopydb stream sentinel set endpos --current

pgcopydb follow -vv --resume

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

s=/tmp/fares.out
t=/tmp/fares.out
psql -o $s -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/fares.sql
psql -o $t -d ${PGCOPYDB_TARGET_PGURI} -f /usr/src/pgcopydb/fares.sql

diff $s $t

# now check that the data is there and truncate is ignored.
sql="select count(*) from metrics"
test 2 -eq `psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"`
sql="select count(*) from show_chunks('metrics')"
test 2 -eq `psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"`

# now check that the data is there and truncate is ignored.
sql="select count(*) from metrics"
test 3 -eq `psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`
sql="select count(*) from show_chunks('metrics')"
test 3 -eq `psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`

pgcopydb stream cleanup
