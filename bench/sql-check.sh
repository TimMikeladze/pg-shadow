#!/usr/bin/env bash
# Exercises pgshadow.validate() directly in SQL against a local Postgres.
set -u
P="psql -h ${PGHOST:-/tmp} -p ${PGPORT:-5433} -U postgres -d shadow -v ON_ERROR_STOP=1 -q"
fail=0
expect() { local want=$1 got=$2 what=$3; if [ "$got" = "$want" ]; then echo "ok    $what"; else echo "FAIL  $what (want $want, got '$got')"; fail=1; fi; }
$P -f "$(dirname "$0")/../sql/schema.sql" 2>/dev/null
$P -c "DROP TABLE IF EXISTS t1; DROP SCHEMA IF EXISTS early CASCADE; CREATE TABLE t1 (id int PRIMARY KEY, v int); INSERT INTO t1 VALUES (1, 1); SELECT pgshadow.enable('t1');" >/dev/null
OID=$($P -tAc "SELECT 't1'::regclass::oid")
state() {
  read SNAP EPOCH SETTINGS < <($P -tAF ' ' -c "SELECT pgshadow.boot_token() IS NULL; SELECT f.snap, f.epoch, pgshadow.settings_hash() FROM pgshadow.fill_context() f" | tail -1)
  FP=$($P -tAc "SELECT fp FROM pgshadow.rel_state('t1'::regclass)")
}
# valid [search_path] [role] [schema-qualifier] [functions]
valid() {
  local path=${1:-'"\$user", public'} role=${2:-postgres} nsp=${3:-NULL} funcs=${4:-}
  [ "$nsp" != NULL ] && nsp="'$nsp'"
  $P -tAc "SELECT valid FROM pgshadow.validate(ARRAY[1], ARRAY[$OID]::oid[], ARRAY[\$f\$$FP\$f\$], ARRAY['$SNAP'], ARRAY['$role'],
           ARRAY[$nsp]::text[], ARRAY['t1'], ARRAY[\$p\$$path\$p\$], ARRAY['$EPOCH'], ARRAY['$SETTINGS'], ARRAY['$funcs'], pgshadow.settings_hash())"
}

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
state; $P -c "CREATE SCHEMA early; CREATE TABLE early.t1 (id int)"; expect f "$(valid 'early, public')" "name now resolves to another table: invalid"
expect t "$(valid)" "same name, other search path unaffected: valid"
$P -c "DROP SCHEMA early CASCADE"
state; $P -c "CREATE SCHEMA app_late; CREATE TABLE app_late.t1 (id int)"; expect f "$(valid 'app_late, public')" "schema created later on the search path: invalid"
$P -c "DROP SCHEMA app_late CASCADE"
state; expect t "$(valid '' postgres public)" "qualified reference: valid"
$P -c "ALTER SCHEMA public RENAME TO public_old; CREATE SCHEMA public; CREATE TABLE public.t1 (id int)"
expect f "$(valid '' postgres public)" "qualified reference after a schema swap: invalid"
$P -c "DROP SCHEMA public CASCADE; ALTER SCHEMA public_old RENAME TO public"
state; $P -c "CREATE FUNCTION public.audit_f(int) RETURNS int LANGUAGE sql AS 'SELECT 1'"; expect f "$(valid)" "user function created: invalid (catalog epoch)"
$P -c "DROP FUNCTION public.audit_f(int)"
$P -c "DROP ROLE IF EXISTS shadow_reader; CREATE ROLE shadow_reader; GRANT SELECT ON t1 TO shadow_reader" 2>/dev/null
state; expect t "$(valid 'public' shadow_reader)" "role with SELECT: valid"
$P -c "REVOKE USAGE ON SCHEMA public FROM PUBLIC"
expect f "$(valid 'public' shadow_reader)" "schema USAGE revoked: invalid"
$P -c "GRANT USAGE ON SCHEMA public TO PUBLIC"
$P -c "REVOKE SELECT ON t1 FROM shadow_reader"
expect f "$(valid 'public' shadow_reader)" "SELECT revoked: invalid"
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
$P -c "DROP TYPE IF EXISTS mood CASCADE; CREATE TYPE mood AS ENUM ('ok', 'sad'); ALTER TABLE t1 ADD COLUMN m mood"
state; $P -c "ALTER TYPE mood RENAME VALUE 'ok' TO 'fine'"; expect f "$(valid)" "enum label renamed: invalid"
state; $P -c "ALTER SYSTEM SET extra_float_digits = 2" -c "SELECT pg_reload_conf()" >/dev/null; sleep 0.3
expect f "$(valid)" "formatting setting changed by a config reload: invalid"
$P -c "ALTER SYSTEM RESET extra_float_digits" -c "SELECT pg_reload_conf()" >/dev/null; sleep 0.3
state; $P -c "ALTER SYSTEM SET search_path = 'other, public'" -c "SELECT pg_reload_conf()" >/dev/null; sleep 0.3
expect f "$(valid)" "search_path changed by a config reload: invalid"
$P -c "ALTER SYSTEM RESET search_path" -c "SELECT pg_reload_conf()" >/dev/null; sleep 0.3
$P -c "DROP TABLE IF EXISTS audit_par CASCADE; CREATE TABLE audit_par (id int) PARTITION BY RANGE (id); CREATE TABLE audit_p1 PARTITION OF audit_par FOR VALUES FROM (0) TO (100)"
R=$($P -tAc "SELECT pgshadow.enable('audit_p1')" 2>&1 | grep -c "partition or inheritance child"); expect 1 "$R" "partition cannot be enabled"
$P -c "DROP TABLE IF EXISTS audit_parent, audit_child CASCADE; CREATE TABLE audit_parent (id int); CREATE TABLE audit_child () INHERITS (audit_parent)"
R=$($P -tAc "SELECT pgshadow.enable('audit_child')" 2>&1 | grep -c "partition or inheritance child"); expect 1 "$R" "inheritance child cannot be enabled"
$P -c "DROP TABLE audit_par, audit_parent CASCADE"
# Findings of the second review.
$P -c "DROP TABLE IF EXISTS t1; DROP TYPE IF EXISTS comp, mood2 CASCADE; DROP DOMAIN IF EXISTS mood_d CASCADE;
       CREATE TYPE comp AS (x int, y int); CREATE TYPE mood2 AS ENUM ('a', 'b'); CREATE DOMAIN mood_d AS mood2;
       CREATE TABLE t1 (id int PRIMARY KEY, v int, c comp, md mood_d[]); SELECT pgshadow.enable('t1');" >/dev/null
OID=$($P -tAc "SELECT 't1'::regclass::oid")
state; $P -c "ALTER TYPE comp RENAME ATTRIBUTE x TO renamed"; expect f "$(valid)" "composite attribute renamed: invalid"
state; $P -c "ALTER TYPE mood2 RENAME VALUE 'a' TO 'z'"; expect f "$(valid)" "enum under domain under array renamed: invalid"
# A rename by a transaction older than the last enum change (count and max xmin unchanged).
psql -h ${PGHOST:-/tmp} -p ${PGPORT:-5433} -U postgres -d shadow -q -c "BEGIN; SELECT pg_current_xact_id(); SELECT pg_sleep(1.2); ALTER TYPE mood2 RENAME VALUE 'b' TO 'y'; COMMIT;" >/dev/null &
sleep 0.4; $P -c "ALTER TYPE mood2 ADD VALUE 'c'"; state; wait
expect f "$(valid)" "enum rename by an older transaction: invalid"
state; $P -c "DROP COLLATION IF EXISTS public.zcoll; CREATE COLLATION public.zcoll (locale = 'C')"; expect f "$(valid)" "user collation created: invalid (catalog epoch)"
state; $P -c "REVOKE EXECUTE ON FUNCTION pg_catalog.chr(int) FROM PUBLIC"; expect f "$(valid '' postgres NULL 'upper,chr')" "built-in function privileges changed: invalid"
expect t "$(valid '' postgres NULL 'upper')" "... other functions unaffected"
$P -c "UPDATE pg_catalog.pg_proc SET proacl = NULL WHERE oid = 'pg_catalog.chr(int)'::regprocedure" >/dev/null
state; $P -c "DELETE FROM pgshadow.boot"; expect f "$(valid)" "server rebooted (crash, failover, restore): invalid"
$P -c "DROP TABLE IF EXISTS u1; CREATE UNLOGGED TABLE u1 (id int)"
R=$($P -tAc "SELECT pgshadow.enable('u1')" 2>&1 | grep -c "unlogged"); expect 1 "$R" "unlogged table cannot be enabled"
$P -c "DROP TABLE IF EXISTS cc; CREATE TABLE cc (id int, tcol text, tsc timestamptz, dd mood_d)" >/dev/null
CC=$($P -tAc "SELECT pgshadow.clock_columns(ARRAY['cc'::regclass::oid], '{tcol}'), pgshadow.clock_columns(ARRAY['cc'::regclass::oid], '{tsc}'), pgshadow.clock_columns(ARRAY['cc'::regclass::oid], '{nope}')")
expect "t|f|t" "$CC" "date/time casts: text column or unknown name could read the clock, timestamptz column not"
SH=$($P -tAc "SELECT pgshadow.shadowed('{lower}', '{=}', '{zcoll,int4}')")
expect "{zcoll}" "$SH" "user collation name is reported as shadowing"
$P -c "DROP TYPE IF EXISTS zz_ct CASCADE; CREATE TYPE zz_ct AS (a int);
       CREATE FUNCTION zz_cf(zz_ct) RETURNS int LANGUAGE sql VOLATILE AS 'SELECT 1';
       CREATE CAST (zz_ct AS int) WITH FUNCTION zz_cf(zz_ct)" >/dev/null
SH=$($P -tAc "SELECT pgshadow.shadowed('{}', '{}', '{}')"); expect "{(cast)}" "$SH" "volatile user cast reported"
$P -c "DROP TYPE zz_ct CASCADE; DROP FUNCTION IF EXISTS zz_cf(zz_ct); DROP COLLATION public.zcoll; DROP TABLE cc" >/dev/null
$P -c "DROP VIEW IF EXISTS vw1; CREATE VIEW vw1 AS SELECT 1 AS x" >/dev/null
R=$($P -tAc "SELECT (pgshadow.rel_state('vw1'::regclass)).reason" 2>&1); expect "not a plain table (relkind v)" "$R" "view: refused without an error"
$P -c "DROP VIEW vw1" >/dev/null
state; ARGS="ARRAY[1], ARRAY[$OID]::oid[], ARRAY[\$f\$$FP\$f\$], ARRAY['$SNAP'], ARRAY['postgres'], ARRAY[NULL]::text[], ARRAY['t1'], ARRAY['public'], ARRAY['$EPOCH'], ARRAY['$SETTINGS'], ARRAY['lower'], pgshadow.settings_hash()"
MS=$( (for i in $(seq 8); do echo "SELECT 1 FROM pgshadow.validate($ARGS);"; done; echo "EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM pgshadow.validate($ARGS);") | $P -tA | tail -n +9 | python3 -c 'import json,sys;print(json.load(sys.stdin)[0]["Execution Time"])')
echo "validate() server time for one item (warm session): ${MS} ms"
exit $fail
