#!/usr/bin/env bash
# Exercises pgshadow.validate() directly in SQL against a local Postgres.
set -u
P="psql -h ${PGHOST:-/tmp} -p ${PGPORT:-5433} -U postgres -d shadow -v ON_ERROR_STOP=1 -q"
fail=0
expect() { local want=$1 got=$2 what=$3; if [ "$got" = "$want" ]; then echo "ok    $what"; else echo "FAIL  $what (want $want, got '$got')"; fail=1; fi; }
$P -f "$(dirname "$0")/../sql/schema.sql" 2>/dev/null
$P -c "DROP TABLE IF EXISTS t1; DROP SCHEMA IF EXISTS early CASCADE; CREATE TABLE t1 (id int PRIMARY KEY, v int); INSERT INTO t1 VALUES (1, 1); SELECT pgshadow.enable('t1');" >/dev/null
OID=$($P -tAc "SELECT 't1'::regclass::oid")
state() { SNAP=$($P -tAc "SELECT pg_current_snapshot()"); FP=$($P -tAc "SELECT fp FROM pgshadow.rel_state('t1'::regclass)"); }
valid() { $P -tAc "SELECT valid FROM pgshadow.validate(ARRAY[1], ARRAY[$OID]::oid[], ARRAY['$FP'], ARRAY['$SNAP'], ARRAY['postgres'], ARRAY['t1'], ARRAY['${1:-pg_catalog,public}'])"; }

psql -h ${PGHOST:-/tmp} -p ${PGPORT:-5433} -U postgres -d shadow -q -c "BEGIN; UPDATE t1 SET v = 2; SELECT pg_sleep(1.5); COMMIT;" >/dev/null &
sleep 0.5; state
expect t "$(valid)" "writer in progress when cached: still valid (Postgres would not show it yet)"
wait
expect f "$(valid)" "writer committed after the snapshot: invalid"
state; expect t "$(valid)" "fresh snapshot, nothing changed: valid"
$P -c "UPDATE t1 SET v = v + 1"; expect f "$(valid)" "committed update: invalid"
state; $P -c "ALTER TABLE t1 ADD COLUMN w int"; expect f "$(valid)" "ALTER TABLE: invalid"
state; $P -c "ALTER TABLE t1 DISABLE TRIGGER pgshadow_capture"; expect f "$(valid)" "trigger disabled: invalid"
$P -c "ALTER TABLE t1 ENABLE ALWAYS TRIGGER pgshadow_capture"
state; $P -c "SET session_replication_role = replica; UPDATE t1 SET v = 9"; expect f "$(valid)" "write with session_replication_role=replica: invalid"
state; $P -c "TRUNCATE t1"; expect f "$(valid)" "TRUNCATE: invalid"
state; $P -c "CREATE SCHEMA early; CREATE TABLE early.t1 (id int)"; expect f "$(valid 'pg_catalog,early,public')" "name now resolves to another table: invalid"
expect t "$(valid)" "same name, other search path unaffected: valid"
$P -c "DROP SCHEMA early CASCADE"
$P -c "DROP ROLE IF EXISTS shadow_reader; CREATE ROLE shadow_reader; GRANT SELECT ON t1 TO shadow_reader" 2>/dev/null
state; R=$($P -tAc "SELECT valid FROM pgshadow.validate(ARRAY[1], ARRAY[$OID]::oid[], ARRAY['$FP'], ARRAY['$SNAP'], ARRAY['shadow_reader'], ARRAY['t1'], ARRAY['pg_catalog,public'])")
expect t "$R" "role with SELECT: valid"
$P -c "REVOKE SELECT ON t1 FROM shadow_reader"
R=$($P -tAc "SELECT valid FROM pgshadow.validate(ARRAY[1], ARRAY[$OID]::oid[], ARRAY['$FP'], ARRAY['$SNAP'], ARRAY['shadow_reader'], ARRAY['t1'], ARRAY['pg_catalog,public'])")
expect f "$R" "SELECT revoked: invalid"
psql -h ${PGHOST:-/tmp} -p ${PGPORT:-5433} -U postgres -d shadow -q -c "BEGIN; UPDATE t1 SET v = 3; SELECT pg_sleep(1); COMMIT;" >/dev/null &
sleep 0.4; state; wait
$P -c "SELECT pgshadow.trim('0 seconds')" >/dev/null
expect f "$(valid)" "in-progress write committed, then its log entry trimmed: invalid"
state; expect t "$(valid)" "fresh snapshot after trimming: valid"
# A long-running transaction elsewhere must not let trimming invalidate everything.
psql -h ${PGHOST:-/tmp} -p ${PGPORT:-5433} -U postgres -d shadow -q -c "BEGIN; SELECT txid_current(); SELECT pg_sleep(2); COMMIT;" >/dev/null &
sleep 0.3; $P -c "UPDATE t1 SET v = v + 1"; state
$P -c "SELECT pgshadow.trim('0 seconds')" >/dev/null
expect t "$(valid)" "trim while an unrelated long transaction is open: still valid"
wait
$P -c "ALTER TABLE t1 ENABLE ROW LEVEL SECURITY"; state; expect f "$(valid)" "row-level security: not eligible"
$P -c "ALTER TABLE t1 DISABLE ROW LEVEL SECURITY"
MS=$($P -tAc "SELECT 1 FROM pgshadow.validate(ARRAY[1], ARRAY[$OID]::oid[], ARRAY['$FP'], ARRAY['$SNAP'], ARRAY['postgres'], ARRAY['t1'], ARRAY['pg_catalog,public']); EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM pgshadow.validate(ARRAY[1], ARRAY[$OID]::oid[], ARRAY['$FP'], ARRAY['$SNAP'], ARRAY['postgres'], ARRAY['t1'], ARRAY['pg_catalog,public'])" | tail -n +2 | python3 -c 'import json,sys;print(json.load(sys.stdin)[0]["Execution Time"])')
echo "validate() server time for one item (warm session): ${MS} ms"
exit $fail
