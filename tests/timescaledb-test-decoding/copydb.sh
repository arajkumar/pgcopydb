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
coproc ( pgcopydb snapshot --follow --plugin test_decoding )

# wait for snapshot to be created
while [ ! -f /tmp/pgcopydb/snapshot ]; do sleep 1; done

dbf=/tmp/pgcopydb/schema/source.db

while [ ! -s ${dbf} ]
do
    sleep 1
done

# copying roles needs superuser
# and we use the postgres database here still
pgcopydb copy roles --source ${POSTGRES_SOURCE} --target ${POSTGRES_TARGET} --resume

# now create the tsdb database on the target, owned by new role tsdb
psql ${POSTGRES_TARGET} <<EOF
create database tsdb owner tsdbadmin;
EOF

psql ${PGCOPYDB_TARGET_PGURI_SU} -f /usr/src/pgcopydb/tsdb_grants.sql

psql -d "$PGCOPYDB_TARGET_PGURI_SU" \
    -c 'select public.timescaledb_pre_restore();'

pgcopydb copy extensions --resume

pgcopydb stream setup

# now clone with superuser privileges, seems to be required for timescaledb
pgcopydb clone --skip-extensions --no-acl --no-owner

psql -d "$PGCOPYDB_TARGET_PGURI_SU" \
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

# Insert some data into metrics hypertable. Since we have 1hr chunk,
# we will have 3 chunks after this.
psql -a ${PGCOPYDB_SOURCE_PGURI} <<EOF
BEGIN;
    INSERT INTO metrics (id, time, name, value) VALUES
    (1, now() - '1h'::interval, 'test-1', -1),
    (2, now(), 'test', 0),
    (3, now() + '1h'::interval, 'test+1', 1);
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

s=/tmp/fares-src.out
t=/tmp/fares-tgt.out
psql -o $s -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/fares.sql
psql -o $t -d ${PGCOPYDB_TARGET_PGURI} -f /usr/src/pgcopydb/fares.sql

diff $s $t

# check that source chunks are truncated
sql="select count(*) from metrics"
test 2 -eq `psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"`
sql="select count(*) from show_chunks('metrics')"
test 2 -eq `psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"`

# now check that the truncation on target is ignored.
sql="select count(*) from metrics"
test 3 -eq `psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`
sql="select count(*) from show_chunks('metrics')"
test 3 -eq `psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`

truncate_sql="delete from metrics"
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${truncate_sql}"
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${truncate_sql}"

function run_cdc() {
    pgcopydb stream sentinel set endpos --current
    pgcopydb follow -vv --resume
}

function diff_cdc() {
    s=/tmp/metrics-src.out
    t=/tmp/metrics-tgt.out
    numeric_sql="select * from metrics order by time, id"
    psql -o $s -d ${PGCOPYDB_SOURCE_PGURI} -c "${numeric_sql}"
    psql -o $t -d ${PGCOPYDB_TARGET_PGURI} -c "${numeric_sql}"

    diff -urN $s $t
}

# test with default replica identity i.e primary key
for sql in `ls /usr/src/pgcopydb/tests/*/*.sql`; do
    psql -d ${PGCOPYDB_SOURCE_PGURI} -f $sql
    run_cdc
    diff_cdc
done

# test with replica identity as full
truncate_sql="delete from metrics"
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${truncate_sql}"
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${truncate_sql}"

psql -d ${PGCOPYDB_SOURCE_PGURI} -c 'ALTER TABLE metrics REPLICA IDENTITY FULL'
for sql in `ls /usr/src/pgcopydb/tests/*/*.sql`; do
    psql -d ${PGCOPYDB_SOURCE_PGURI} -f $sql
    run_cdc
    diff_cdc
done

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

pgcopydb stream cleanup
