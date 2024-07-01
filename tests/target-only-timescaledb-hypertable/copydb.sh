#! /bin/bash

set -x
set -e
set -o pipefail

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI


# make sure source and target databases are ready
pgcopydb ping

# create the table on the source and target databases
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# create the replication slot that captures all the changes
# PGCOPYDB_OUTPUT_PLUGIN is set to test_decoding in docker-compose.yml
coproc ( pgcopydb snapshot )

# wait for the snapshot to be created
while [ ! -f /tmp/pgcopydb/snapshot ]; do
  sleep 1
done

# dump only the schema
pgcopydb dump schema

# restore the pre-data schema
pgcopydb restore pre-data

psql -d ${PGCOPYDB_TARGET_PGURI} -f - <<EOF
-- convert the metrics table to a hypertable
SELECT create_hypertable('metrics', by_range('time', '1 day'::interval));
EOF

pgcopydb clone --resume

# verify hypertable exists
sql="select count(*) from timescaledb_information.hypertables where hypertable_name = 'metrics'"

count=`psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`
test ${count} -eq 1

# verify the data is the same
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "select * from metrics" > /tmp/pgcopydb/source_count
psql -d ${PGCOPYDB_TARGET_PGURI} -c "select * from metrics" > /tmp/pgcopydb/target_count

diff -urN /tmp/pgcopydb/source_count /tmp/pgcopydb/target_count || cat /tmp/pgcopydb/source_count /tmp/pgcopydb/target_count

kill -9 $COPROC_PID
