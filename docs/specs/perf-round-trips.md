# Fewer round trips and cheaper checks

The Fly and Neon runs showed where time goes once Postgres is a network hop away: a hit waits for a free check connection before its own round trip (about 2 round trips on Neon at 8+ clients), a miss spends a round trip on `fill_context()` before the query is sent, cheap statements get cached on their first run and then pay for a check on every hit, and each check batch spends about 0.8 ms of Postgres CPU fingerprinting every table it covers. Four changes, each keeping the never-stale rules as they are.

## Pipelined checks (validator.rs)

Each check connection keeps several batches in flight instead of one. A fixed depth of 4 cut hit throughput on Fly (0.6 ms round trip) by a third: small batches each pay the fixed cost of a check (about 0.2 ms of Postgres time), and on a fast network that costs more than waiting. Depth is therefore round trip / (2 x batch execution time), clamped to 1..8; execution time is the EWMA of batch round trip minus `rtt()`, sampled only from batches sent with nothing else in flight on their connection. Depth is 1 until both are measured, which gives 1 on Fly or next to Postgres and 8 against Neon 24 ms away. tokio-postgres already pipelines concurrent queries on one connection; the worker loop takes the next batch from the queue while earlier ones are outstanding and hands replies back as they complete. Postgres still runs them one after another on that connection; only the network wait overlaps. A request still waits for a batch that is sent after it was queued, so what a check sees is unchanged.

## Snapshot in the client's round trip (session.rs, validator.rs)

A fill needs a snapshot, the catalog epoch and the settings hash, all read before the query runs. Today they come from one extra round trip on a helper connection.

- Snapshot: `SELECT pg_current_snapshot()::text, pg_is_in_recovery()` goes on the client's own connection through `our_query` (the proxy's named statement and portal, as `TXN_SQL` already does), in the same write as the client's query. It runs first, so it is taken before the query, as now. Built-in functions only: no privileges needed, nothing that can fail in a healthy transaction. A NULL, an error or recovery means the result isn't stored.
- Epoch and settings hash: the latest values read on the proxy's connection, kept fresh by the context worker every second and after any check that reports a catalog or settings change. A value read before the query is older than or equal to the state the query saw, so a change in between makes the entry fail its first check, as an epoch change does today. Cold start (no value yet) falls back to reading one synchronously.

Saves one round trip per miss.

## Time before caching (session.rs)

With `min_query_time` above zero, a statement shape is forwarded and timed on its first run instead of being filled; from then on it is cached only while its average stays at or above `min_query_time`. A cheap statement then never enters the cache, instead of entering on its first run and staying while its tables don't change. With `min_query_time = 0` nothing changes (tests rely on that).

## Cheaper table fingerprint (schema.sql)

`rel_state()` builds the column part of the fingerprint by calling the PL/pgSQL `type_fp()` once per column. For built-in scalar types (`oid < 16384`, `typtype` b or p, no element type) `type_fp()` only returns `oid@xmin`; compute that inline in the same query and call `type_fp()` for everything else. The fingerprint string stays byte-identical, so existing cache entries stay valid and `bench/sql-check.sh` (enum relabel, domain, composite, array cases) keeps passing unchanged.

## Verification

- Unit/integration: relay tests (20 ms round trip) assert that hits from many concurrent clients each take about one round trip, that a miss takes one round trip to Postgres rather than two, and that a cheap statement is never filled; `sql-check.sh` and an fp equality check cover the fingerprint; the existing suite and the staleness oracle cover correctness.
- Benchmarks: local and Fly and Neon re-runs with the same scripts.

## Found by the v3 and v4 cloud runs

- **Fixed pipeline depth hurt a fast network.** See *Pipelined checks*: depth is now derived from the round trip and the measured batch time. Test: `checks_are_not_pipelined_on_a_fast_network` (depth 1 next to Postgres), next to the relay test (depth above 1).
- **Fills were timed at about 0 ms behind a distant database.** A fill's query is pipelined behind its snapshot read and was timed from the snapshot's response, then had `rtt()` subtracted again. A query pipelined behind another response is now timed from that response without subtracting. Test: `fills_behind_a_slow_network_keep_their_query_time` (9 of 11 variants filled before, 11 after).
- **`invalidation_cooldown` fired during warm-up.** It needed only 20 failed checks, so one write while 50 variants were still filling paused the statement for the whole run. It now needs 500 checks, and also pauses when failed checks × round trip exceed hits × statement time, which is what makes caching a loss against a distant database (Neon at 10 UPDATEs/s: 454 tps cached against 639 direct). Unit test: `cooldown_needs_enough_checks`.
- **The relay test harness lacked `TCP_NODELAY`.** On Linux, Nagle plus delayed ACKs added 40 ms to `a_miss_takes_one_round_trip_behind_a_slow_network`; the relay now sets it like the proxy and Postgres do.
