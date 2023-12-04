#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS
#  - PGCOPYDB_OUTPUT_PLUGIN

env | grep ^PGCOPYDB

# make sure source and target databases are ready
pgcopydb ping

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

# alter the pagila schema to allow capturing DDLs without pkey
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

slot=pgcopydb

# create the replication slot that captures all the changes
# PGCOPYDB_OUTPUT_PLUGIN is set to wal2json in docker-compose.yml
coproc ( pgcopydb snapshot --follow --slot-name ${slot})

sleep 1

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup

# pgcopydb copy db uses the environment variables
pgcopydb clone

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# now that the copying is done, inject some SQL DML changes to the source
psql -d ${PGCOPYDB_SOURCE_PGURI} -f - <<EOF
BEGIN;
INSERT INTO table_a(f1) VALUES (50011);
COMMIT;
EOF

pgcopydb stream sentinel set apply

pgcopydb stream sentinel set endpos --current

pgcopydb follow --resume --trace

# now check that all the new rows made it
sql="select count(*) from table_a"
test 1 -eq `psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`

for i in `seq 1 100`;
do
psql -d ${PGCOPYDB_SOURCE_PGURI} -f - <<EOF
BEGIN;
    SELECT pg_switch_wal();
    INSERT INTO table_a(f1) VALUES (50012);
    SELECT pg_switch_wal();
ROLLBACK;
EOF
done

psql -d ${PGCOPYDB_SOURCE_PGURI} -f - <<EOF
BEGIN;
INSERT INTO table_a(f1) VALUES (50013);
COMMIT;
EOF

pgcopydb stream sentinel set endpos --current

pgcopydb follow --resume --trace

# now check that all the new rows made it
sql="select count(*) from table_a"
test 2 -eq `psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`


# cleanup
pgcopydb stream cleanup
