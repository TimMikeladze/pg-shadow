# Benchmarks and correctness runs

The numbers behind the README's *Performance* section. Every pgbench run is one JSON line in a `results.jsonl` under [`bench/results/`](../bench/results):

| run | where | raw results |
|---|---|---|
| Fly Postgres | pgbench, pgshadow and Postgres on three Fly Machines in `sjc` | `cloud-2026-11-07-final/fly/` |
| Neon | the same client and proxy Machines in `sjc`, Neon in us-west-2 | `cloud-2026-11-07-final/neon/` |
| Fly variants | 8 check connections; pgshadow as a sidecar on the client Machine | `cloud-2026-11-07-final/fly-cc8/`, `fly-sidecar/` |
| local | Postgres in Docker on a laptop, pgshadow and pgbench next to it | `local-2026-11-07/` |

All of them ran the same build, `7a6907c`. The commit after it fixes a race that could make the request right after a fill miss (see the end); it doesn't change the measured paths otherwise. Older runs are kept for the history at the end: `cloud-2026-11-07-v1` to `v4`, `cloud-2026-11-07-v6-mix`, and `blog-2026-11-07` (the first local run).

## Setup

### Fly and Neon

Each request crosses the network twice, as it would in a deployment: client to pgshadow, pgshadow to Postgres. pgbench runs on one Fly Machine, pgshadow on another, Postgres on a third.

| | Fly | Neon |
|---|---|---|
| Postgres | 18.6 on a `performance-8x` Machine: 8 dedicated vCPU, 32 GB, `shared_buffers=256MB` | 18.6, autoscaling up to 8 CU (8 vCPU, 32 GB) |
| network to Postgres | Fly private network, 0.59 ms round trip | TLS from `sjc` to us-west-2, 23 ms round trip |
| pgshadow | `performance-2x` Machine (2 vCPU, 4 GB), a fresh proxy for every run | same |
| client | pgbench 18.6 on a `performance-4x` Machine | same |

The Fly Postgres is sized like the Neon compute under load: `num_cpus()` went from 1 to 8 within two minutes of load and stayed there. Before its first run, `bench/cloud-run.sh` loads the Neon compute until `num_cpus()` reports 8. Fly has no region in Oregon, so every Neon number carries a 23 ms round trip that an app in us-west-2 wouldn't have; a same-region run is still to do.

Postgres time comes from `pg_stat_statements` for the benchmark database only (`pg_exec_ms_per_tx`), which covers the proxy's check queries too. On Fly the Machine's `/proc/stat` CPU delta is also recorded (`pg_cpu_ms_per_tx`).

### Local

```
host:      Apple M5 Pro, 18 cores
postgres:  PostgreSQL 16.15 in Docker (OrbStack), --cpus 4 --memory 4g, stock settings
pgbench:   18.6
round trip to Postgres: 0.08 ms
```

pgshadow and pgbench run natively next to the container, outside its CPU limit, so the proxy has all 18 cores and there is no network hop. That is why local throughput is 3 to 4 times the Fly numbers; read it as the ceiling of the proxy itself, not as what a deployment gets. Postgres CPU is the container's cgroup `usage_usec` delta.

### Both

- Each run is 20 s of pgbench in prepared mode (`-M prepared`). Every proxied run starts a fresh proxy with an empty cache.
- `check.connections = 2`, `cache.memory = 256MB`, everything else at its default (`min_query_time = 3ms`, `invalidation_cooldown = 60s`).
- `bench_orders` has 200k rows. `bench_orders_off` is an identical copy that isn't opted in.
- The report query is a `GROUP BY` over all 200k rows: 33 ms on the Fly Postgres, 27 ms on Neon, 11 ms locally (`env.txt` in each run has these timings). The per-customer report filters to one customer, with 50 different customer ids.

## Running it

```bash
# local
PGSHADOW_PG_PORT=55433 PGSHADOW_PG_CONTAINER=pgshadow-bench-pg \
PGSHADOW_PG_DOCKER_ARGS="--cpus 4 --memory 4g" bench/dev-postgres.sh
cargo build --release --all-targets
PGSHADOW_PG_PORT=55433 PGSHADOW_PG_CONTAINER=pgshadow-bench-pg bench/blog.sh
python3 bench/charts.py bench/results/<run>/results.jsonl docs/img   # needs matplotlib
python3 bench/tables.py bench/results/<run>/results.jsonl            # the tables below

# Fly and Neon
bench/fly.sh up            # app, Postgres Machine, client and proxy Machines in sjc
bench/fly.sh run fly
NEON_URL=postgresql://user:password@ep-....aws.neon.tech/dbname bench/fly.sh run neon
bench/fly.sh status        # last lines of each run's log
bench/fly.sh fetch bench/results/cloud-<date>
python3 bench/charts.py cloud bench/results/cloud-<date>/fly/results.jsonl bench/results/cloud-<date>/neon/results.jsonl docs/img
bench/fly.sh down          # destroys the app, its Machines and the volume
```

`bench/dev-postgres.sh` defaults to port 5433 and container `pgshadow-test-pg`; the variables above move it. `ONLY=sweep,cost,...` runs a subset of `bench/blog.sh` and `SECS` sets the run length. `ONLY=mix STALE_SECS=0 bench/fly.sh run fly` runs one scenario in the cloud without the oracle and the integration suite. Use Neon's direct endpoint, not `-pooler`: PgBouncer in transaction mode can't hold pgshadow's per-session state. A run on both targets takes about 20 minutes; the five Machines cost about $1 an hour, so stop or destroy them afterwards.

`bench/fly.sh up` builds the image with Fly's remote builder. When that fails, `fly deploy --build-only --push --depot --image-label <tag>` and `fly machine update <id> --image registry.fly.io/<app>:<tag>` do the same. Replacing a Machine wipes its `/out`, so fetch first.

## Correctness

| run | result |
|---|---|
| `cargo test --lib` | 20 passed |
| `cargo test --test it`, local Postgres 16 | 43 passed |
| `cargo test --test it`, Fly Postgres 18 | 42 passed (before `a_request_right_after_a_fill_can_hit` was added) |
| `bench/sql-check.sh` | 38 ok, 0 FAIL |
| `py_tests` (psycopg, SQLAlchemy) | 11 passed |
| staleness oracle, local, 300 s | 1,281,037 reads, 916,949 cache hits, 118,458 writes, 0 stale reads |
| staleness oracle, Fly, 120 s | 198,691 reads, 98,999 cache hits, 42,176 writes, 0 stale reads |
| staleness oracle, Neon, 120 s | 12,001 reads, 6,661 cache hits, 2,416 writes, 0 stale reads |
| verify mode, 60 s, 16 readers, 20 UPDATEs/s | local 5,473 identical, 21 raced, 0 mismatches; Fly 4,680 / 9 / 0; Neon 2,192 / 23 / 0 |

Raw output: [`it.txt`](../bench/results/it.txt), [`sql-check.txt`](../bench/results/sql-check.txt), [`py-tests.txt`](../bench/results/py-tests.txt), [`staleness-300s.txt`](../bench/results/staleness-300s.txt), and `staleness.txt` and `it.txt` in each cloud run.

The oracle's final check binary-searches per-row write histories. To confirm it still fails, a read of all zeros was injected at the end of a 3 s run: it reported 16 violations, one per row. Against Neon the oracle's writers pause 300 ms between writes (`PGSHADOW_STALENESS_WRITE_PAUSE_MS`), otherwise every read 23 ms away lands after a write and nothing hits.

Verify mode sets `verify_sample_rate = 1.0` and `invalidation_cooldown = "0s"`, so every hit that passes validation is re-run on Postgres and compared. "Raced" means a write committed between the hit's validation and the re-run; see the README's *Verify mode*.

## Fly and Neon

![Report query throughput on Fly and Neon](img/cloud-clients.svg)

On Fly the report query runs 17 times faster through pgshadow at 8 clients, 52 times at 32 and 76 times at 64. Direct, Postgres runs out of CPU at about 150 to 170 tps. Through pgshadow every request after the first is a hit, and throughput is bounded by the two hops and the check round trip: a hit takes 2.5 ms at 1 client and 4.7 ms at 64.

On Neon every request through pgshadow costs one round trip for the check, 25 to 29 ms at the median, against 43 to 243 ms direct. At 64 clients that's 1,887 tps against 267. A single client gains less (38 against 23 tps), because the check's round trip is most of the cost.

![Read throughput under a steady write rate](img/cloud-write-rate.svg)

![Postgres execution time per request](img/cloud-postgres-time.svg)

At a fixed 100 requests/s, which Postgres handles on its own, a report costs Postgres 30 ms (Fly) or 37 ms (Neon) of execution direct, and 1.3 or 0.9 ms through pgshadow. What remains is the validation, which at this rate rarely shares a batch.

### Choosing check.connections and where to run the proxy

On Fly, pgshadow's hit throughput is bounded by its own round trips, not by Postgres: the cost sweep tops out at about 8,000 tps whatever the query costs. Two variants of the report sweep (`fly-cc8/`, `fly-sidecar/`):

| report query, tps | 1 client | 8 | 32 | 64 |
|---|---:|---:|---:|---:|
| pgshadow on its own Machine, `check.connections = 2` | 374 | 2,555 | 7,743 | 12,903 |
| same, `check.connections = 8` | 326 | 2,962 | 9,223 | 15,700 |
| pgshadow as a sidecar on the client Machine, 2 check connections | 492 | 2,929 | 10,260 | 18,521 |

More check connections help from 8 clients up: with 2, each check connection keeps one batch in flight on a fast network (see the README's *On a hit*), and the batches queue. A sidecar saves the client-to-proxy hop: 1.9 ms per hit at 1 client against 2.5 ms, and pass-through primary-key lookups at 32 clients go from 12,450 to 22,088 tps (direct: 24,496), with the proxy sharing 4 vCPU with pgbench.

### Fly Postgres, all runs

#### Report query by client count

| clients | direct tps | pgshadow tps | direct p50 / p99 | pgshadow p50 / p99 | proxy CPU (cores) |
|---:|---:|---:|---:|---:|---:|
| 1 | 35 | 374 | 27.7 ms / 36.6 ms | 2.48 ms / 5.33 ms | 0.07 |
| 8 | 148 | 2,555 | 52.2 ms / 86.9 ms | 2.95 ms / 6.37 ms | 0.17 |
| 32 | 150 | 7,743 | 219 ms / 333 ms | 3.94 ms / 7.36 ms | 0.28 |
| 64 | 170 | 12,903 | 380 ms / 579 ms | 4.71 ms / 8.66 ms | 0.39 |

#### Query cost sweep, 32 clients

| rows aggregated | direct tps | pgshadow tps | direct p50 | pgshadow p50 |
|---:|---:|---:|---:|---:|
| 2,000 | 9,180 | 8,146 | 2.95 ms | 3.75 ms |
| 10,000 | 2,425 | 8,249 | 12.5 ms | 3.71 ms |
| 50,000 | 511 | 7,886 | 61.3 ms | 3.86 ms |
| 200,000 | 152 | 7,981 | 222 ms | 3.80 ms |

#### Other workloads, 32 clients unless noted

| workload | direct tps | pgshadow tps | direct p50 / p99 | pgshadow p50 / p99 | proxy CPU (cores) |
|---|---:|---:|---:|---:|---:|
| per-customer report, 50 variants | 544 | 7,677 | 56.7 ms / 108 ms | 3.80 ms / 9.15 ms | 0.30 |
| primary-key lookup, 8 clients | 11,874 | 5,370 | 0.61 ms / 2.13 ms | 1.31 ms / 4.04 ms | 0.28 |
| primary-key lookup | 25,250 | 12,450 | 1.18 ms / 3.07 ms | 2.39 ms / 5.33 ms | 0.36 |

#### Reads under a steady write rate

| UPDATEs/s requested | reached (direct / pgshadow) | direct tps | pgshadow tps | pgshadow hit rate | hits | fills | invalidated |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 0 | 456 | 8,046 | 99.9% | 159,692 | 50 | 0 |
| 1 | 1.1 / 1.2 | 445 | 6,426 | 98.9% | 126,345 | 1,368 | 1,318 |
| 10 | 9.2 / 9.9 | 434 | 2,032 | 83.7% | 34,048 | 6,357 | 6,307 |
| 100 | 99.1 / 97.8 | 453 | 442 | 0.0% | 0 | 554 | 504 |
| 1,000 | 995.5 / 1,000.1 | 425 | 406 | 0.0% | 0 | 559 | 509 |

#### Postgres time at 100 requests/s, 8 clients

| workload | CPU per request (direct / pgshadow) | cores used (direct / pgshadow) | execution time per request (direct / pgshadow) |
|---|---:|---:|---:|
| report | 51.4 ms / 1.60 ms | 4.95 / 0.16 | 29.7 ms / 1.34 ms |
| per-customer report | 21.8 ms / 2.12 ms | 2.07 / 0.22 | 12.1 ms / 1.65 ms |

#### Write cost, single-row UPDATE, 8 clients, direct

| table | tps | p50 | Postgres time per write |
|---|---:|---:|---:|
| not opted in | 8,917 | 0.81 ms | 0.199 ms CPU, 0.034 ms exec |
| opted in | 8,715 | 0.84 ms | 0.239 ms CPU, 0.076 ms exec |

### Neon, all runs

#### Report query by client count

| clients | direct tps | pgshadow tps | direct p50 / p99 | pgshadow p50 / p99 | proxy CPU (cores) |
|---:|---:|---:|---:|---:|---:|
| 1 | 23 | 38 | 43.1 ms / 75.5 ms | 25.2 ms / 29.9 ms | 0.02 |
| 8 | 132 | 299 | 60.2 ms / 98.1 ms | 24.7 ms / 46.0 ms | 0.06 |
| 32 | 222 | 996 | 138 ms / 252 ms | 26.6 ms / 49.6 ms | 0.09 |
| 64 | 267 | 1,887 | 243 ms / 338 ms | 29.1 ms / 53.7 ms | 0.12 |

#### Query cost sweep, 32 clients

| rows aggregated | direct tps | pgshadow tps | direct p50 | pgshadow p50 |
|---:|---:|---:|---:|---:|
| 2,000 | 1,311 | 1,095 | 23.4 ms | 24.8 ms |
| 10,000 | 1,215 | 984 | 25.6 ms | 26.3 ms |
| 50,000 | 555 | 991 | 56.9 ms | 26.1 ms |
| 200,000 | 232 | 1,022 | 140 ms | 26.6 ms |

#### Other workloads, 32 clients unless noted

| workload | direct tps | pgshadow tps | direct p50 / p99 | pgshadow p50 / p99 | proxy CPU (cores) |
|---|---:|---:|---:|---:|---:|
| per-customer report, 50 variants | 626 | 939 | 49.1 ms / 110 ms | 29.0 ms / 50.5 ms | 0.08 |
| primary-key lookup, 8 clients | 336 | 309 | 23.5 ms / 28.8 ms | 24.8 ms / 29.3 ms | 0.04 |
| primary-key lookup | 1,336 | 1,190 | 23.5 ms / 31.0 ms | 24.5 ms / 35.6 ms | 0.13 |

#### Reads under a steady write rate

| UPDATEs/s requested | reached (direct / pgshadow) | direct tps | pgshadow tps | pgshadow hit rate | hits | fills | invalidated |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 0 | 322 | 992 | 99.6% | 19,112 | 50 | 0 |
| 1 | 1.0 / 0.9 | 577 | 924 | 95.1% | 17,052 | 817 | 767 |
| 10 | 9.1 / 10.2 | 592 | 560 | 2.3% | 260 | 311 | 261 |
| 100 | 102.3 / 99.7 | 584 | 553 | 0.0% | 4 | 552 | 502 |
| 1,000 | 564.1 / 557.9 | 564 | 530 | 0.0% | 0 | 570 | 520 |

#### Postgres time at 100 requests/s, 8 clients

| workload | execution time per request (direct / pgshadow) |
|---|---:|
| report | 36.9 ms / 0.90 ms |
| per-customer report | 11.0 ms / 1.16 ms |

#### Write cost, single-row UPDATE, 8 clients, direct

| table | tps | p50 | Postgres time per write |
|---|---:|---:|---:|
| not opted in | 318 | 24.9 ms | 0.037 ms exec |
| opted in | 321 | 24.7 ms | 0.097 ms exec |

Notes:

- On Neon, the 2,000- and 10,000-row aggregates (0.9 and 3.6 ms) and the primary-key lookups are below or near `min_query_time`, so they pass through, at 8 to 19% below direct: the extra hop costs 1 to 2 ms against a 23 ms round trip.
- Neon's direct run at 0 UPDATEs/s (322 tps) is lower than its runs with writes (564 to 592). I suspect the compute had scaled down between the previous scenario and this one; the warm-up only runs before the first scenario.
- From 10 UPDATEs/s on Neon, the per-customer statement stops being cached for `invalidation_cooldown` after its first 500 checks, because the failed checks cost more round trips than the hits save (see the README's *Its results keep validating*). It then passes through at 5% below direct. On Fly at 10 UPDATEs/s, 84% of reads still hit and throughput is 4.7 times direct.
- The opted-in table's trigger adds 0.04 ms (Fly) and 0.06 ms (Neon) of Postgres execution per single-row UPDATE. On Fly, UPDATE throughput went from 8,917 to 8,715 tps (2%).
- pgbench reports 0 failed transactions in every run.

## Local

![Report query throughput by client count](img/clients.svg)

#### Report query by client count

| clients | direct tps | pgshadow tps | direct p50 / p99 | pgshadow p50 / p99 | proxy CPU (cores) |
|---:|---:|---:|---:|---:|---:|
| 1 | 100 | 1,800 | 9.40 ms / 18.7 ms | 0.51 ms / 1.60 ms | 0.08 |
| 8 | 158 | 9,219 | 69.9 ms / 111 ms | 0.78 ms / 2.60 ms | 0.27 |
| 32 | 166 | 30,574 | 194 ms / 406 ms | 0.90 ms / 3.15 ms | 0.67 |
| 64 | 169 | 42,293 | 313 ms / 1,190 ms | 1.40 ms / 4.01 ms | 0.89 |

#### Query cost sweep, 32 clients

| rows aggregated | direct tps | pgshadow tps | direct p50 | pgshadow p50 |
|---:|---:|---:|---:|---:|
| 2,000 | 7,578 | 32,712 | 1.25 ms | 0.85 ms |
| 10,000 | 2,126 | 30,062 | 3.96 ms | 0.90 ms |
| 50,000 | 499 | 30,280 | 86.1 ms | 0.89 ms |
| 200,000 | 150 | 29,025 | 200 ms | 0.91 ms |

#### Other workloads, 32 clients unless noted

| workload | direct tps | pgshadow tps | direct p50 / p99 | pgshadow p50 / p99 | proxy CPU (cores) |
|---|---:|---:|---:|---:|---:|
| per-customer report, 50 variants | 502 | 28,487 | 86.0 ms / 175 ms | 0.92 ms / 3.44 ms | 0.65 |
| primary-key lookup, 8 clients | 42,256 | 32,658 | 0.17 ms / 0.55 ms | 0.22 ms / 0.69 ms | 0.94 |
| primary-key lookup | 63,206 | 46,760 | 0.41 ms / 1.38 ms | 0.60 ms / 1.73 ms | 1.85 |

#### Reads under a steady write rate

| UPDATEs/s requested | reached (direct / pgshadow) | direct tps | pgshadow tps | pgshadow hit rate | hits | fills | invalidated |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 0 | 420 | 29,530 | 100.0% | 590,307 | 50 | 0 |
| 1 | 1.1 / 0.9 | 429 | 25,285 | 99.8% | 504,326 | 1,149 | 1,099 |
| 10 | 9.6 / 9.4 | 419 | 5,548 | 93.6% | 104,017 | 6,876 | 6,826 |
| 100 | 99.7 / 101.8 | 423 | 420 | 0.2% | 22 | 547 | 497 |
| 1,000 | 548.2 / 509.7 | 410 | 403 | 0.0% | 0 | 570 | 520 |

#### Postgres time at 100 requests/s, 8 clients

| workload | CPU per request (direct / pgshadow) | cores used (direct / pgshadow) |
|---|---:|---:|
| report | 21.6 ms / 1.03 ms | 2.10 / 0.10 |
| per-customer report | 8.49 ms / 1.22 ms | 0.87 / 0.12 |

#### Write cost, single-row UPDATE, 8 clients, direct

| table | tps | p50 | Postgres time per write |
|---|---:|---:|---:|
| not opted in | 2,763 | 2.77 ms | 0.070 ms CPU |
| opted in | 5,044 | 0.95 ms | 0.074 ms CPU |

![Throughput by query cost](img/query-cost.svg)

![Read throughput under a steady write rate](img/write-rate.svg)

![Postgres CPU per request at a fixed rate](img/postgres-cpu.svg)

Notes:

- The 2,000-row aggregate (0.3 ms) was cached even though it's below `min_query_time`. A statement is timed on its first run, and here the first run happened with 32 clients arriving at once, queued behind each other, and took more than 3 ms. On Fly the same statement passed through.
- The local write-cost rows are noise: the opted-in table was faster than the plain one (5,044 against 2,763 tps), with both under 0.08 ms of CPU per write. Use the Fly numbers above.
- The writer at "1,000" UPDATEs/s reached only about 530/s against a Postgres saturated by the readers; the table shows the rate each run reached.

### Proxy-side costs

```
$ ./target/release/examples/micro
full classify (unique text):     15.1 us/query
pg_query::parse only:              6.4 us/query
pg_query::fingerprint only:        1.4 us/query
serde_json::to_value of tree:      2.6 us/query
memoized classify (same text):    0.01 us/query

$ ./target/release/examples/latency      # one connection, sequential round trips
direct    prepared pk lookup     83 us   simple SELECT 1    101 us
pgshadow  prepared pk lookup    105 us   simple SELECT 1    112 us
```

The proxy process used 13.6 MB RSS after the latency run, with a nearly empty cache. Cache memory is capped by `cache.memory`.

## What the cloud runs changed

The first local runs had no network between pgshadow and Postgres, which hid most of what follows. None of these served a stale result; each made pgshadow slower than it should have been.

- **`min_query_time` counted the network** (`cloud-2026-11-07-v1`). Statements were timed from send to last byte, so against Neon every statement looked slow enough to cache. Primary-key lookups with random ids, which never hit, kept getting filled: 335 tps through pgshadow against 1,402 direct. The proxy now measures the round trip with `SELECT 1` every 2 s and subtracts the fastest of the last 8.
- **Each check read the whole changelog** (`v1`). `validate()` plans generically, and the generic plan for "has this table changed since the snapshot" was a sequential scan. After the write-cost runs left 177,125 changelog entries, per-customer hits on Fly dropped from 9,001 to 1,471 tps. It's now one index probe (`ORDER BY xid DESC LIMIT 1`).
- **A miss took two round trips, a hit waited for a free check connection** (`v2`). The fill's snapshot now travels with the query, and check batches are pipelined. On Neon at 32 clients the hit median went from 49 ms to 27 ms (`v2` against `v3`).
- **A fixed pipeline depth of 4 hurt a fast network** (`v3`). On Fly, report hits at 32 clients fell from 8,379 to 5,471 tps and Postgres time per hit rose from 0.13 to 0.38 ms: small batches each paid the fixed cost of a check. Depth is now derived from the round trip and the measured batch time, which gives 1 on Fly and 8 on Neon (`v4`: 8,326 on Fly, Neon unchanged).
- **`invalidation_cooldown` fired during warm-up** (`v4`). It needed only 20 failed checks, so one write while the 50 per-customer variants were still filling paused the statement for the whole run: on Fly at 10 UPDATEs/s, 447 tps instead of about 2,000. It now waits for 500 checks.
- **Caching that costs round trips wasn't paused** (`v6-mix`). On Neon at 10 UPDATEs/s, 22% of per-customer reads hit and the rest paid an extra round trip: 454 tps through pgshadow against 639 direct. The cooldown now also fires when failed checks × round trip exceed hits × statement time.
- **Fills were timed at about 0 ms behind a distant database.** A fill's query was timed from its snapshot's response and then had the round trip subtracted a second time, so after a few fills a 10 ms statement looked cheaper than `min_query_time`. Found while checking the cooldown above, fixed before the final runs.

- **A request right after a fill could miss.** The proxy marked the session idle before storing the settings read it had queued behind the fill, so a request arriving in between skipped the cache. CI's slower runners hit it; locally a loop of 200 fill-then-hit pairs failed 3 runs out of 5. Fixed after the final runs.

Each fix has a test that fails on the old code; `tests/it/caching.rs` runs the proxy through a TCP relay that adds 10 ms each way.
