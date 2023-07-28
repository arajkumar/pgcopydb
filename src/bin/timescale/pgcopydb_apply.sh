set -e
cat << EOF | psql $PGCOPYDB_SOURCE_PGURI -t > /tmp/replace.sed
-- sed -u -E -f /tmp/replace.sed
SELECT concat('s/', '_timescaledb_internal','._hyper_', id,'_[^[:space:]]*/"', schema_name, '"."', table_name,'"', '/g') FROM _timescaledb_catalog.hypertable;
SELECT concat('s/', '"_timescaledb_internal"','."_hyper_', id,'_[^[:space:]]*/"', schema_name, '"."', table_name,'"', '/g') FROM _timescaledb_catalog.hypertable;
SELECT '/"_timescaledb_catalog"/d';
SELECT '/_timescaledb_catalog/d';
SELECT '/"_timescaledb_internal"."bgw_job_stat"/d';
SELECT 's/TRUNCATE ONLY/TRUNCATE/';
EOF

coproc apply ( pgcopydb stream apply --dir $PGCOPY_DIR - )
trap ctrl_c INT
function ctrl_c() {
	kill -INT ${apply_PID}
}
NEXT_LSN=$(psql $PGCOPYDB_TARGET_PGURI -c "select pg_replication_origin_progress('pgcopydb', true)" -t -A 2>/dev/null)
while true; do
	WAL_NAME=$(psql $PGCOPYDB_SOURCE_PGURI -c "select pg_walfile_name('$NEXT_LSN')" -t -A 2>/dev/null)
	WAL_FILE="${PGCOPY_DIR}/cdc/${WAL_NAME}.sql"
	if [ ! -f $WAL_FILE ]; then
		echo "Waiting for $WAL_FILE"
		sleep 5
		continue
	fi
	NEXT_LSN=$(tail -n 1 $WAL_FILE | sed -e 's/-- SWITCH WAL //g' | jq -r '.lsn')
	echo "$WAL_FILE | $NEXT_LSN"
	sed -i -u -E -f /tmp/replace.sed "$WAL_FILE"
	cat $WAL_FILE >& "${apply[1]}"
done
