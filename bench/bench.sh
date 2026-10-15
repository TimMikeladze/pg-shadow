#!/usr/bin/env bash
# Compare direct Postgres with pgshadow using pgbench. Usage: bench/bench.sh [seconds] [clients]
set -e
SECS=${1:-10}; CLIENTS=${2:-8}
PGB=${PGBENCH:-/usr/lib/postgresql/16/bin/pgbench}
DIRECT="-h 127.0.0.1 -p 5433 -U postgres shadow"
PROXY="-h 127.0.0.1 -p 6432 -U postgres shadow"
W=$(mktemp -d)
psql -h 127.0.0.1 -p 5433 -U postgres -d shadow -q <<'SQL'
DROP TABLE IF EXISTS bench_orders;
CREATE TABLE bench_orders (id int PRIMARY KEY, customer_id int, status text, total numeric(12,2), created timestamptz);
INSERT INTO bench_orders SELECT g, g % 5000, (array['paid','pending','shipped','refunded'])[1 + g % 4], (g % 997) * 1.5,
       '2026-01-01'::timestamptz + g * interval '1 minute' FROM generate_series(1, 200000) g;
CREATE INDEX ON bench_orders (customer_id);
ANALYZE bench_orders;
SELECT pgshadow.enable('bench_orders');
SQL
cat > $W/pk.sql <<'Q'
\set id random(1, 200000)
SELECT * FROM bench_orders WHERE id = :id;
Q
cat > $W/report.sql <<'Q'
SELECT status, count(*), sum(total), avg(total) FROM bench_orders GROUP BY status ORDER BY status;
Q
cat > $W/customer.sql <<'Q'
\set c random(1, 50)
SELECT date_trunc('month', created) AS m, count(*), sum(total) FROM bench_orders WHERE customer_id % 50 = :c GROUP BY 1 ORDER BY 1;
Q
cat > $W/write.sql <<'Q'
\set id random(1, 200000)
UPDATE bench_orders SET total = total + 1 WHERE id = :id;
Q
run() { # label target mode script...
  local label=$1 target=$2 mode=$3; shift 3
  local args=""; for s in "$@"; do f=${s%@*}; w=${s#*@}; [ "$f" = "$s" ] && w=1; args="$args -f $W/$f@$w"; done
  $PGB $target -n -c $CLIENTS -j 2 -T $SECS -M $mode $args 2>/dev/null \
    | awk -v l="$label" '/latency average/{lat=$4} /^tps/{tps=$3} END{printf "%-46s %10.0f tps %9.2f ms avg\n", l, tps, lat}'
}
echo "== $CLIENTS clients, ${SECS}s each"
for mode in simple prepared; do
  run "primary-key lookup, direct ($mode)" "$DIRECT" $mode pk.sql
  run "primary-key lookup, pgshadow ($mode)" "$PROXY" $mode pk.sql
done
for mode in simple prepared; do
  run "report query, direct ($mode)" "$DIRECT" $mode report.sql
  run "report query, pgshadow ($mode)" "$PROXY" $mode report.sql
done
run "per-customer report (50 variants), direct" "$DIRECT" prepared customer.sql
run "per-customer report (50 variants), pgshadow" "$PROXY" prepared customer.sql
run "mixed 10:1 reports:writes, direct" "$DIRECT" prepared customer.sql@10 write.sql@1
run "mixed 10:1 reports:writes, pgshadow" "$PROXY" prepared customer.sql@10 write.sql@1
rm -rf $W
