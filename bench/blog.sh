#!/usr/bin/env bash
# Benchmarks and correctness runs behind docs/benchmarks.md.
#
#   PGSHADOW_PG_PORT=55433 PGSHADOW_PG_CONTAINER=pgshadow-bench-pg \
#   PGSHADOW_PG_DOCKER_ARGS="--cpus 4 --memory 4g" bench/dev-postgres.sh
#   cargo build --release
#   PGSHADOW_PG_PORT=55433 PGSHADOW_PG_CONTAINER=pgshadow-bench-pg bench/blog.sh
#
# Any other Postgres (bench/fly.sh runs it on Fly against Fly Postgres and Neon):
#
#   PG_HOST=db.example.com PG_PORT=5432 PG_USER=app PG_DB=bench PG_SSLMODE=require PGPASSWORD=... \
#   PG_CPU=none bench/blog.sh
#
# PROXY_CTL=http://<host>:7000 runs each fresh proxy on another machine through bench/proxy-ctl.py
# instead of locally, and clients connect to it at PROXY_HOST:6432.
#
# Writes results.jsonl (one line per pgbench run) and the proxy logs to bench/results/<timestamp>/.
# Postgres CPU (PG_CPU): docker reads the container's cgroup; procstat reads /proc/stat through
# pg_read_file (needs superuser, and only means something on a VM that runs nothing but
# Postgres); none skips it. Postgres execution time from pg_stat_statements, for this database
# only, is recorded whenever the extension can be created.
set -euo pipefail
cd "$(dirname "$0")/.."
SECS=${SECS:-20}
RATE=${RATE:-100}                  # fixed request rate for the Postgres CPU comparison
PG_HOST=${PG_HOST:-127.0.0.1}
PG_PORT=${PG_PORT:-${PGSHADOW_PG_PORT:-5433}}
PG_USER=${PG_USER:-postgres}
PG_DB=${PG_DB:-shadow}
PG_SSLMODE=${PG_SSLMODE:-disable}  # disable | require (also the proxy's upstream TLS)
PG_CPU=${PG_CPU:-docker}
CPUS_SQL=${CPUS_SQL:-}             # e.g. "SELECT num_cpus()" on Neon: compute size before/after each run
CHECK_CONNECTIONS=${CHECK_CONNECTIONS:-2} # the proxy's check.connections (parallel validation batches)
WRITER_CLIENTS=${WRITER_CLIENTS:-16} # background writer connections; enough to hold the rate while Postgres is saturated
CONTAINER=${PGSHADOW_PG_CONTAINER:-pgshadow-test-pg}
BIN=${PGSHADOW_BIN:-./target/release/pgshadow}
PGB=${PGBENCH:-pgbench}
OUT=${OUT:-bench/results/$(date +%Y%m%d-%H%M%S)}
ONLY=${ONLY:-all}                  # comma list: sweep,cost,variants,pk,cpu,writes,mix,verify,micro
mkdir -p "$OUT"; W=$(mktemp -d)
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }
PROXY_CTL=${PROXY_CTL:-}
if [ -n "$PROXY_CTL" ]; then
  PROXY_HOST=${PROXY_HOST:?PROXY_HOST with PROXY_CTL}; PROXY_PORT=6432; ADMIN_PORT=9187; LISTEN="[::]:6432"
else
  PROXY_HOST=127.0.0.1; PROXY_PORT=$(free_port); ADMIN_PORT=$(free_port); LISTEN="127.0.0.1:$PROXY_PORT"
fi
DIRECT="host=$PG_HOST port=$PG_PORT user=$PG_USER dbname=$PG_DB sslmode=$PG_SSLMODE"
PROXY="host=$PROXY_HOST port=$PROXY_PORT user=$PG_USER dbname=$PG_DB sslmode=disable"
PSQL() { psql -X -d "$DIRECT" -qAt "$@"; }
PID=""
want() { [ "$ONLY" = all ] || [[ ",$ONLY," == *",$1,"* ]]; }

pg_cpu_us() {
  case "$PG_CPU" in
    docker) docker exec "$CONTAINER" cat /sys/fs/cgroup/cpu.stat 2>/dev/null | awk '/^usage_usec/{print $2}' || true ;;
    # busy jiffies (user nice system irq softirq; USER_HZ = 100) as microseconds
    # printf: mawk prints integers above 2^31 in scientific notation
    procstat) PSQL -c "SELECT pg_read_file('/proc/stat')" 2>/dev/null | awk '/^cpu /{printf "%.0f\n", ($2+$3+$4+$7+$8) * 10000; exit}' || true ;;
  esac
}
pg_exec_ms() {
  PSQL -c "SELECT round(sum(total_exec_time + total_plan_time)::numeric, 3) FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())" 2>/dev/null || true
}
pg_cpus() { [ -n "$CPUS_SQL" ] && PSQL -c "$CPUS_SQL" 2>/dev/null || true; }

start_proxy() { # label [cache.* toml lines]
  local label=$1; shift
  stop_proxy
  {
    printf '[listen]\naddr = "%s"\nmax_connections = 500\n' "$LISTEN"
    printf '[upstream]\nhost = "%s"\nport = %s\ntls = "%s"\n' "$PG_HOST" "$PG_PORT" "$PG_SSLMODE"
    printf '[check]\nconnections = %s\n' "$CHECK_CONNECTIONS"
    printf '[cache]\nmemory = "256MB"\n'; for l in "$@"; do echo "$l"; done
    printf '[admin]\naddr = "127.0.0.1:%s"\n' "$ADMIN_PORT"
    printf '[log]\nformat = "json"\nlevel = "info"\nsummary_interval = "1s"\n'
  } > "$W/proxy.toml"
  PLOG="$OUT/proxy-$label.log"
  # The check DSN goes through the environment so a password never lands in a file.
  local check="$DIRECT${PGPASSWORD:+ password=$PGPASSWORD}"
  if [ -n "$PROXY_CTL" ]; then
    python3 -c 'import json,sys; print(json.dumps({"toml": open(sys.argv[1]).read(), "env": {"PGSHADOW_CHECK_DSN": sys.argv[2]}}))' \
      "$W/proxy.toml" "$check" > "$W/start.json"
    curl -sf -X POST --data-binary @"$W/start.json" "$PROXY_CTL/start" >/dev/null && { PID=remote; return; }
    curl -s "$PROXY_CTL/log" > "$PLOG"
  else
    PGSHADOW_CHECK_DSN="$check" "$BIN" -c "$W/proxy.toml" serve > "$PLOG" 2>&1 & PID=$!
    for _ in $(seq 1 100); do curl -sf "http://127.0.0.1:$ADMIN_PORT/readyz" >/dev/null && return; sleep 0.1; done
  fi
  echo "proxy did not become ready, see $PLOG" >&2; exit 1
}
stop_proxy() {
  if [ "$PID" = remote ]; then curl -sf -X POST "$PROXY_CTL/stop" >/dev/null || true
  elif [ -n "$PID" ]; then kill "$PID" 2>/dev/null && wait "$PID" 2>/dev/null; fi
  PID=""; return 0
}
proxy_cpu() { [ "$PID" = remote ] && curl -sf "$PROXY_CTL/cpu" || python3 bench/report.py cpu "$PID"; }
proxy_rss() { [ "$PID" = remote ] && curl -sf "$PROXY_CTL/rss" || ps -o rss= -p "$PID"; }
trap 'stop_proxy; rm -rf "$W"' EXIT

run() { # label direct|pgshadow clients rate script[@weight]...
  local label=$1 target=$2 clients=$3 rate=$4; shift 4
  local conn=$DIRECT; [ "$target" = pgshadow ] && conn=$PROXY
  local args=() threads=$(( clients < 8 ? clients : 8 ))
  for s in "$@"; do args+=(-f "$W/${s%@*}@$( [[ $s == *@* ]] && echo "${s#*@}" || echo 1)"); done
  [ "$rate" -gt 0 ] && args+=(-R "$rate")
  local tag; tag=$(echo "$label-$target-$clients-$rate" | tr -c 'a-zA-Z0-9-' '_')
  local c0 p0 c1 p1 e0 e1 n0 n1; c0=$(pg_cpu_us); e0=$(pg_exec_ms); n0=$(pg_cpus)
  p0=$([ -n "$PID" ] && proxy_cpu || echo -)
  $PGB -n -c "$clients" -j "$threads" -T "$SECS" -M prepared -l --log-prefix "$W/log-$tag" "${args[@]}" "$conn" > "$W/out-$tag" 2>&1
  c1=$(pg_cpu_us); e1=$(pg_exec_ms); n1=$(pg_cpus)
  p1=$([ -n "$PID" ] && proxy_cpu || echo -)
  sleep 1.2 # let the last summary line land
  [ "$PID" = remote ] && curl -sf "$PROXY_CTL/log" > "$PLOG"
  local cpu=-; [ -n "$c0" ] && [ -n "$c1" ] && cpu=$((c1 - c0))
  local pcpu=-; [ "$p0" != - ] && pcpu=$(python3 -c "print($p1 - $p0)")
  local exec=-; [ -n "$e0" ] && [ -n "$e1" ] && exec=$(python3 -c "print($e1 - $e0)")
  local writer=-; [ -n "${WRITER_OUT:-}" ] && writer=$WRITER_OUT
  EXEC_MS=$exec PG_CPUS="${n0:+$n0 $n1}" WRITER_LOG=$writer \
  python3 bench/report.py pgbench "$label" "$target" "$clients" "$rate" "$W/out-$tag" "$W/log-$tag" "$cpu" "$pcpu" \
    "$([ "$target" = pgshadow ] && echo "$PLOG" || echo -)" | tee -a "$OUT/results.jsonl"
  rm -f "$W"/log-"$tag"*
}

both() { # label clients rate scripts...: direct, then through a fresh proxy
  local label=$1 clients=$2 rate=$3; shift 3
  stop_proxy; run "$label" direct "$clients" "$rate" "$@"
  start_proxy "$label-$clients-$rate"; run "$label" pgshadow "$clients" "$rate" "$@"; stop_proxy
}

echo "== setup: 200k-row bench_orders (opted in) and bench_orders_off (not opted in)"
PSQL -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" >/dev/null 2>&1 || echo "no pg_stat_statements: Postgres execution time not recorded"
PSQL <<'SQL' >/dev/null
\i sql/schema.sql
DROP TABLE IF EXISTS bench_orders, bench_orders_off;
CREATE TABLE bench_orders (id int PRIMARY KEY, customer_id int, status text, total numeric(12,2), created timestamptz);
INSERT INTO bench_orders SELECT g, g % 5000, (array['paid','pending','shipped','refunded'])[1 + g % 4], (g % 997) * 1.5,
       '2026-01-01'::timestamptz + g * interval '1 minute' FROM generate_series(1, 200000) g;
CREATE INDEX ON bench_orders (customer_id);
CREATE TABLE bench_orders_off (LIKE bench_orders INCLUDING ALL);
INSERT INTO bench_orders_off SELECT * FROM bench_orders;
VACUUM ANALYZE bench_orders; VACUUM ANALYZE bench_orders_off;
SELECT pgshadow.enable('bench_orders');
SQL
cat > "$W/pk.sql" <<'Q'
\set id random(1, 200000)
SELECT * FROM bench_orders WHERE id = :id;
Q
cat > "$W/report.sql" <<'Q'
SELECT status, count(*), sum(total), avg(total) FROM bench_orders GROUP BY status ORDER BY status;
Q
cat > "$W/customer.sql" <<'Q'
\set c random(1, 50)
SELECT date_trunc('month', created) AS m, count(*), sum(total) FROM bench_orders WHERE customer_id % 50 = :c GROUP BY 1 ORDER BY 1;
Q
for n in 2000 10000 50000 200000; do
  echo "SELECT status, count(*), sum(total) FROM bench_orders WHERE id <= $n GROUP BY status ORDER BY status;" > "$W/range$n.sql"
done
cat > "$W/write.sql" <<'Q'
\set id random(1, 200000)
UPDATE bench_orders SET total = total + 1 WHERE id = :id;
Q
cat > "$W/write_off.sql" <<'Q'
\set id random(1, 200000)
UPDATE bench_orders_off SET total = total + 1 WHERE id = :id;
Q
echo "SELECT 1;" > "$W/one.sql"
{
  echo "date: $(date -u +%FT%TZ)"
  echo "client: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | xargs), $(sysctl -n hw.ncpu 2>/dev/null || nproc) cpus"
  echo "postgres: $(PSQL -c 'select version()')"
  echo "postgres settings: $(PSQL -c "select string_agg(name || '=' || setting || coalesce(unit, ''), ' ' order by name) from pg_settings where name in ('max_connections', 'shared_buffers', 'synchronous_commit', 'jit')")"
  [ "$PG_CPU" = docker ] && echo "postgres container: $(docker inspect "$CONTAINER" --format 'cpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}}' 2>/dev/null || echo n/a)"
  [ -n "$CPUS_SQL" ] && echo "postgres cpus ($CPUS_SQL): $(pg_cpus)"
  echo "pgbench: $($PGB --version)"
  echo "pgshadow: ${PGSHADOW_VERSION:-$(git describe --always --dirty 2>/dev/null)}"
  echo "proxy: ${PROXY_CTL:+on $PROXY_HOST, }${PROXY_DESC:-same machine as the client}"
  echo "seconds per run: $SECS"
  echo "check.connections: $CHECK_CONNECTIONS"
  echo "round trip to Postgres: $($PGB -n -c 1 -T 5 -M prepared -f "$W/one.sql" "$DIRECT" 2>&1 | awk '/latency average/{print $4, $5}') (pgbench SELECT 1, one connection)"
} | tee "$OUT/env.txt"
for q in report customer range2000 range10000 range50000 range200000; do
  echo "$q query on Postgres: $(PSQL -c "EXPLAIN (ANALYZE, FORMAT JSON) $(sed 's/:c/7/' "$W/$q.sql" | grep -v '^\\')" | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["Execution Time"], "ms")')" | tee -a "$OUT/env.txt"
done

if want sweep; then
  echo "== report query, client sweep"
  for c in 1 8 32 64; do both report "$c" 0 report.sql; done
fi
if want cost; then
  echo "== query cost sweep: same aggregate over the first N rows, 32 clients"
  for n in 2000 10000 50000 200000; do both "range-$n" 32 0 "range$n.sql"; done
fi
if want variants; then
  echo "== per-customer report, 50 parameter variants"
  both customer 32 0 customer.sql
fi
if want pk; then
  echo "== primary-key lookup (below min_query_time: passes through)"
  for c in 8 32; do both pk "$c" 0 pk.sql; done
fi
if want cpu; then
  echo "== Postgres CPU at a fixed $RATE requests/s"
  both report-fixed-rate 8 "$RATE" report.sql
  both customer-fixed-rate 8 "$RATE" customer.sql
fi
if want writes; then
  echo "== write cost of the capture trigger (direct, single-row UPDATE)"
  stop_proxy
  run write-not-opted-in direct 8 0 write_off.sql
  run write-opted-in direct 8 0 write.sql
fi
if want mix; then
  echo "== reads under a steady write rate (32 readers, per-customer report; writer at N UPDATEs/s)"
  for wr in 0 1 10 100 1000; do
    for target in direct pgshadow; do
      [ "$target" = pgshadow ] && start_proxy "mix-$wr"
      conn=$DIRECT; [ "$target" = pgshadow ] && conn=$PROXY
      wpid=""; WRITER_OUT=""
      if [ "$wr" -gt 0 ]; then
        WRITER_OUT="$W/writer-$target-$wr"
        $PGB -n -c "$WRITER_CLIENTS" -j "$WRITER_CLIENTS" -T "$SECS" -R "$wr" -M prepared -f "$W/write.sql" "$conn" > "$WRITER_OUT" 2>&1 & wpid=$!
      fi
      run "mix-writes-$wr" "$target" 32 0 customer.sql
      [ -n "$wpid" ] && wait "$wpid"; WRITER_OUT=""
      stop_proxy
    done
  done
fi
if want verify; then
  echo "== verify mode: every hit re-run on Postgres and compared, with 20 UPDATEs/s"
  start_proxy verify "verify_sample_rate = 1.0" "invalidation_cooldown = \"0s\""
  WRITER_OUT="$W/writer-verify"
  $PGB -n -c "$WRITER_CLIENTS" -j "$WRITER_CLIENTS" -T "$(( SECS * 3 ))" -R 20 -M prepared -f "$W/write.sql" "$PROXY" > "$WRITER_OUT" 2>&1 & wpid=$!
  SECS=$(( SECS * 3 )) run verify pgshadow 16 0 customer.sql
  wait "$wpid"; WRITER_OUT=""; stop_proxy
  grep -c "VERIFY MISMATCH" "$OUT/proxy-verify.log" || true
fi
if want micro; then
  echo "== proxy-side micro benchmarks"
  ./target/release/examples/micro | tee "$OUT/micro.txt"
  start_proxy latency
  PGSHADOW_DIRECT_DSN="$DIRECT" PGSHADOW_PROXY_DSN="$PROXY" \
    ./target/release/examples/latency | tee "$OUT/latency.txt"
  echo "proxy rss after latency run: $(proxy_rss) KB" | tee -a "$OUT/latency.txt"
  stop_proxy
fi
echo "results: $OUT/results.jsonl"
