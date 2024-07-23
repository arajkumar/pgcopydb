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
coproc ( pgcopydb snapshot --follow --plugin wal2json)

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

# copy the extensions separately, needs superuser (both on source and target)

psql -d "$PGCOPYDB_TARGET_PGURI_SU" \
    -c 'select public.timescaledb_pre_restore();'

pgcopydb stream setup

pgcopydb copy extensions --resume --no-acl --no-owner

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

s=/tmp/fares-src.out
t=/tmp/fares-tgt.out
psql -o $s -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/fares.sql
psql -o $t -d ${PGCOPYDB_TARGET_PGURI} -f /usr/src/pgcopydb/fares.sql

diff $s $t

# Insert some data into metrics hypertable. Since we have 1hr chunk,
# we will have 3 chunks after this.
psql -a ${PGCOPYDB_SOURCE_PGURI} <<EOF
BEGIN;
    INSERT INTO metrics (id, time, name, value) VALUES
    (1, '2024-07-23 04:44:51.466335+00', 'test-1', -1),
    (2, '2024-07-23 05:44:51.466335+00', 'test', 0),
    (3, '2024-07-23 06:44:51.466335+00', 'test+1', 1);
COMMIT;
EOF

# Schema and chunk table prefix
psql -a ${PGCOPYDB_SOURCE_PGURI} <<EOF
BEGIN;
    INSERT INTO "Foo"."MetricsWithPrefix" (id, time, name, value) VALUES
    (1, '2024-07-23 04:44:51.466335+00', 'test-1', -1),
    (2, '2024-07-23 05:44:51.466335+00', 'test', 0),
    (3, '2024-07-23 06:44:51.466335+00', 'test+1', 1);
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
SELECT drop_chunks('metrics', older_than => '2024-07-23 05:44:51.466335+00'::timestamptz);
COMMIT;
EOF

# grab the current LSN, it's going to be our streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_lsn()'`

# and prefetch the changes captured in our replication slot
pgcopydb stream prefetch --resume --endpos "${lsn}" -vv

SHAREDIR=/var/lib/postgres/.local/share/pgcopydb
WALFILE=000000010000000000000002.json
SQLFILE=000000010000000000000002.sql

# now compare JSON output, skipping the lsn and nextlsn fields which are
# different at each run
expected=/tmp/expected.json
result=/tmp/result.json

JQSCRIPT='del(.lsn) | del(.nextlsn) | del(.timestamp) | del(.xid) | del(.message.xid)'

jq "${JQSCRIPT}" /usr/src/pgcopydb/${WALFILE} > ${expected}
jq "${JQSCRIPT}" ${SHAREDIR}/${WALFILE} > ${result}

# first command to provide debug information, second to stop when returns non-zero
diff ${expected} ${result} || cat ${SHAREDIR}/${WALFILE}
diff ${expected} ${result}

# now prefetch the changes again, which should be a noop
pgcopydb stream prefetch --resume --endpos "${lsn}" -vv

# now transform the JSON file into SQL
SQLFILENAME=`basename ${WALFILE} .json`.sql

pgcopydb stream transform -vv ${SHAREDIR}/${WALFILE} /tmp/${SQLFILENAME}

# we should get the same result as `pgcopydb stream prefetch`
diff ${SHAREDIR}/${SQLFILE} /tmp/${SQLFILENAME} || (cat /tmp/${SQLFILENAME} && exit 1)

# we should also get the same result as expected (discarding LSN numbers)
DIFFOPTS='-I BEGIN -I COMMIT -I KEEPALIVE -I SWITCH -I ENDPOS'

diff ${DIFFOPTS} /usr/src/pgcopydb/${SQLFILE} ${SHAREDIR}/${SQLFILENAME} || (cat ${SHAREDIR}/${SQLFILENAME} && exit 1)

# now allow for replaying/catching-up changes
pgcopydb stream sentinel set apply

# now apply the SQL file to the target database
pgcopydb stream catchup --resume --endpos "${lsn}" -vv

# now apply AGAIN the SQL file to the target database, skipping transactions
pgcopydb stream catchup --resume --endpos "${lsn}" -vv

# check replication of numeric data without precision loss.
sql="select count(*) from metrics"
test 2 -eq `psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"`
sql="select count(*) from show_chunks('metrics')"
test 2 -eq `psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"`

# now check that the data is there and truncate is ignored.
sql="select count(*) from metrics"
test 3 -eq `psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`
sql="select count(*) from show_chunks('metrics')"
test 3 -eq `psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`

# check that the schema and chunk table prefix is there
sql="select count(*) from \"Foo\".\"MetricsWithPrefix\""
test 3 -eq `psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

pgcopydb stream cleanup
