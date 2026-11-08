#!/usr/bin/env bash
# Runs on a bench Machine started by bench/fly.sh. Results go to /out/<target>/.
#
#   fly:  FLY_PG_HOST, FLY_PG_PASSWORD   Postgres Machine on the private network
#   neon: NEON_HOST, NEON_USER, NEON_PASSWORD, NEON_DB
#   PROXY_HOST: the proxy Machine running bench/proxy-ctl.py
#
# Runs bench/blog.sh (everything but the proxy-side micro benchmarks), then the staleness
# oracle for STALE_SECS (120), then on fly the integration suite. STALE_SECS=0 skips both.
set -uo pipefail
cd /pgshadow
T=$1; O=/out/$T; mkdir -p "$O"
STALE_SECS=${STALE_SECS:-120}
export PGSHADOW_VERSION=${PGSHADOW_VERSION:-unknown} ONLY=${ONLY:-sweep,cost,variants,pk,cpu,writes,mix,verify}
export PROXY_HOST PROXY_CTL="http://$PROXY_HOST:7000" PROXY_DESC="its own performance-2x Fly Machine (2 vCPU, 4 GB)"
echo "start $(date -u +%FT%TZ)"
case $T in
  fly)
    export PG_HOST=$FLY_PG_HOST PG_PORT=5432 PG_USER=postgres PGPASSWORD=$FLY_PG_PASSWORD PG_DB=bench \
      PG_SSLMODE=disable PG_CPU=procstat
    until pg_isready -q -h "$PG_HOST"; do sleep 1; done
    TEST_DSN="host=$PG_HOST user=postgres password=$PGPASSWORD dbname=shadow"
    ;;
  neon)
    export PG_HOST=$NEON_HOST PG_PORT=5432 PG_USER=$NEON_USER PGPASSWORD=$NEON_PASSWORD PG_DB=$NEON_DB \
      PG_SSLMODE=require PG_CPU=none CPUS_SQL="SELECT num_cpus()"
    psql -X -d "host=$PG_HOST user=$PG_USER dbname=$PG_DB sslmode=require" -qc "CREATE EXTENSION IF NOT EXISTS neon_utils"
    # Autoscaling: load the compute until it reports its cap (NEON_CPUS, 8 CU here), so the
    # first runs aren't measured on a smaller compute.
    echo "SELECT count(*) FROM generate_series(1, 3000000);" > /tmp/burn.sql
    for i in $(seq 1 16); do
      cpus=$(psql -X -d "host=$PG_HOST user=$PG_USER dbname=$PG_DB sslmode=require" -qAtc "SELECT num_cpus()")
      echo "warm-up: num_cpus() = $cpus"
      [ "${cpus%.*}" -ge "${NEON_CPUS:-8}" ] && break
      pgbench -n -c 48 -j 8 -T 15 -f /tmp/burn.sql "host=$PG_HOST user=$PG_USER dbname=$PG_DB sslmode=require" >/dev/null 2>&1
    done
    TEST_DSN="host=$PG_HOST user=$PG_USER password=$PGPASSWORD dbname=$PG_DB sslmode=require"
    ;;
  *) echo "target: fly | neon" >&2; exit 2 ;;
esac

OUT=$O bash bench/blog.sh
[ "$STALE_SECS" = 0 ] && { echo "done $(date -u +%FT%TZ)"; exit 0; }
echo "== staleness oracle, ${STALE_SECS}s"
# Neon: every write invalidates the table, and a read there takes a 24 ms round trip or
# more, so the writers pause longer between writes for reads to hit in between.
PAUSE=15; [ "$T" = neon ] && PAUSE=${STALE_PAUSE_MS:-300}
PGSHADOW_TEST_PG="$TEST_DSN" PGSHADOW_STALENESS_SECS=$STALE_SECS PGSHADOW_STALENESS_WRITE_PAUSE_MS=$PAUSE \
  target/release/it staleness --nocapture \
  > "$O/staleness.txt" 2>&1
tail -3 "$O/staleness.txt"
if [ "$T" = fly ]; then
  echo "== integration suite"
  PGSHADOW_TEST_PG="$TEST_DSN" target/release/it --test-threads 8 > "$O/it.txt" 2>&1
  grep '^test result' "$O/it.txt"
fi
echo "done $(date -u +%FT%TZ)"
