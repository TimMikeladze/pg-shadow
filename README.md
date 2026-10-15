# pgshadow

A caching proxy for Postgres that never serves stale data.

Point your applications (SQLAlchemy, psycopg, anything that speaks the Postgres protocol) at pgshadow instead of Postgres. **Only the host and port change.** Plain `SELECT`s over tables you opt in are answered from a memory/disk cache. Before any cached result is used, Postgres confirms that nothing it depends on has changed. Everything else passes through unchanged. Remove pgshadow and point the apps back at Postgres, and nothing else changes: it is purely opt-in.

```
app ──(Postgres protocol)──▶ pgshadow ──▶ Postgres / Timescale Cloud
                               │  ▲
                   foyer cache │  │ batched validity checks
                  (memory+disk)┘  └─ pgshadow.validate()
```

- **Never stale.** A cached result is used only when Postgres proves, at that moment, that the same query would return the same bytes. If the proof can't be completed (timeout, connection problem, anything unexpected), the query goes to Postgres.
- **Transparent.** Each client session gets its own Postgres session (session mode), and bytes are forwarded verbatim. Transactions, prepared statements, `COPY`, `LISTEN`, temp tables, errors, notices and cancels behave exactly as they do with Postgres directly.
- **Cheap for Postgres.** Validations are batched (one round trip checks up to 256 cached results). Queries cheaper than a check are never cached. Statements whose tables change constantly stop being cached for a while.
- **Rust**, built on [pgwire](https://crates.io/crates/pgwire) message types, [pg_query](https://crates.io/crates/pg_query) (Postgres's own parser) and [foyer](https://crates.io/crates/foyer) (hybrid memory + disk cache).

## Quick start

```bash
# 1. Install the pgshadow schema (one statement trigger per opted-in table, a change log,
#    and the validation functions). Idempotent; rerun after upgrading pgshadow.
pgshadow -c pgshadow.toml install

# 2. Opt tables in.
pgshadow -c pgshadow.toml enable orders customers public.invoices
pgshadow -c pgshadow.toml status

# 3. Run the proxy and point applications at it.
pgshadow -c pgshadow.toml serve
```

With SQLAlchemy, only the URL changes:

```python
engine = create_engine("postgresql+psycopg://app:secret@pgshadow-host:6432/tsdb")
```

All settings live in [`pgshadow.example.toml`](pgshadow.example.toml). Every key can also be set from the environment as `PGSHADOW_<SECTION>_<KEY>` (for example `PGSHADOW_CHECK_DSN`). With Docker:

```bash
docker build -t pgshadow .
docker run -p 6432:6432 -v $PWD/pgshadow.toml:/etc/pgshadow.toml pgshadow -c /etc/pgshadow.toml serve
```

## What gets cached

A request is cached only when all of these hold:

- **The statement** is a single plain `SELECT` (any shape: joins, CTEs, aggregates, window functions…) that uses only built-in deterministic functions, and only operators, types and collations that no user-defined object shadows.
  - Anything that reads the clock or session state is never cached: `now()`, `'today'::date`, time-word parameters, text cast to a date or time type, `random()`, sequences, `reg*` casts, and so on.
- **The tables** are all opted in, plain, logged tables.
  - Not allowed: partitions or inheritance children, row-level security, unlogged tables, logical-replication targets, or views.
  - Timescale hypertables are supported. Their set of chunks is part of their fingerprint.
- **The session** is in autocommit, or inside a READ COMMITTED transaction that has not written anything. Postgres itself confirms this, so writes made through triggers, functions or operators are caught too.
  - Sessions with temporary tables are never cached.
- **The query** takes at least `cache.min_query_time` (default 3 ms) on Postgres.

Results are keyed by the exact request bytes plus everything about the session that can affect them: role, database, `search_path`, output-format settings, encoding, and so on. The full key is compared on every lookup, so a hash collision cannot return the wrong result. Entries carry a CRC and live in foyer: memory first, with an optional disk tier that survives restarts. Even entries recovered from disk are validated before use.

Everything else (writes, DDL, `SET`, functions with side effects, non-opted-in tables, `SELECT … FOR UPDATE`, and so on) is forwarded as is, and the proxy tracks its effects on the session.

## How "never stale" works

**On every write.** A statement-level trigger (`pgshadow_capture`, `ENABLE ALWAYS` so it fires even in replica mode) records `(table, transaction id)` in `pgshadow.changelog`, once per writing transaction. That is one tiny insert per transaction and table, not per row. `TRUNCATE`, `COPY`, `MERGE`, `ON CONFLICT` and foreign-key cascades all fire it.

**On a miss**, before sending the query, the proxy reads on its own connection:

- the table states: how each name resolves for this session's `search_path` and role, each table's catalog fingerprint, and eligibility;
- a snapshot (`pg_current_snapshot()`);
- the catalog epoch: a hash over every user-defined function, operator, type, cast and collation;
- a hash of the server's result-affecting settings and its boot token.

Reading all of this *before* the query runs is what makes it safe. Anything that changes in between makes validation fail later; it can never make a stale result look valid.

**On a hit**, `pgshadow.validate()` re-checks, in one batched round trip:

- no transaction invisible to the stored snapshot has since committed a write to any of the tables. That means one that started later (xid ≥ xmax) or was in flight when the snapshot was taken (xid in xip). A writer that was open while the result was cached therefore invalidates it once it commits;
- every table is still eligible, with the same fingerprint. The fingerprint covers columns, their types followed recursively (composite attributes, domains, arrays, ranges, exact enum labels), the trigger and its enabled state, RLS, inheritance, and the hypertable chunk set;
- every reference still resolves to the same table for that `search_path` and role. This catches schemas created later, schemas renamed into place, and tables shadowed earlier in the path;
- the role still has `SELECT` on each table and `USAGE` on its schema, and the built-in functions used still have default privileges;
- the catalog epoch, the settings hash and the boot token are unchanged.

**Inside a transaction**, a hit first takes the same `ACCESS SHARE` locks the query would hold until commit, so it still blocks a concurrent `TRUNCATE` or DDL. It then asks Postgres whether the transaction is READ COMMITTED and has written nothing, and only then validates.

**Server changes:**
- **Config reloads.** The settings hash changes for every session without the client being told. Sessions then re-read their settings before using the cache again.
- **Crash recovery, failover, restore.** These can reuse transaction ids. The boot token lives in an `UNLOGGED` table, which they empty, so every earlier entry stops validating.
- **Standbys.** No caching on a standby, and only sessions on the same server and database as the check connection use the cache.

**Changelog trimming.** One proxy trims periodically. Removal is horizon-based, so it only drops entries every possible snapshot within `changelog.retention` can already see. `cache.ttl` must be shorter than the retention. A long-running transaction delays trimming (like vacuum); it never invalidates good results.

**Verify mode.** `cache.verify_sample_rate` re-runs a fraction of hits on Postgres and compares the results. A mismatch would mean a bug: it is logged as an error, and caching is turned off for those tables for an hour.

## Deploying on Timescale Cloud

1. Run `pgshadow install` as the service owner (`tsdbadmin`). It needs no superuser and no extensions; `gen_random_uuid()` is built in.
2. Optionally create a role for the proxy's own connections and grant it access:
   ```sql
   CREATE ROLE pgshadow LOGIN PASSWORD '...';
   SELECT pgshadow.grant_proxy('pgshadow');
   GRANT EXECUTE ON FUNCTION pgshadow.trim(interval) TO pgshadow;  -- for changelog trimming
   ```
3. Run pgshadow close to your applications, in the same region as the service. Set `upstream.host` and `check.dsn` to the **primary** endpoint, not a read replica (the proxy refuses to cache on a standby). Use `upstream.tls = "verify-full"`.
4. Applications connect to pgshadow with their usual credentials; the authentication exchange is relayed to Postgres.
   - **SCRAM channel binding** (`channel_binding=require`) cannot work through any TLS-terminating proxy. Leave libpq's default (`prefer`) or set `channel_binding=disable` on the app → pgshadow leg.
   - Keep that leg inside your private network, or give pgshadow a certificate (`listen.tls_cert`/`tls_key`).
5. Watch `/readyz` and the summary log line. To turn caching off instantly without dropping connections, use `POST /cache/disable`.

Several proxies can run against one database. They share nothing but Postgres; set `changelog.trim_interval = "0s"` on all but one (running it on all is merely redundant).

## Observability

Deliberately basic: structured logs and health endpoints, no metrics backend.

- **Summary line** every `log.summary_interval`: connections, queries, hits, fills, invalidations, failed checks, verification results, bytes served from cache, time saved in Postgres, cache memory, and the top bypass reasons.
- **Per-query lines** with `log.level = "info,pgshadow::query=debug"`: outcome (hit, fill, bypass, verify), reason, statement shape, Postgres time vs. proxy time, and bytes.
- **Admin HTTP** (`admin.addr`):
  - `GET /healthz`: the process is up.
  - `GET /readyz`: check connections are up and the schema matches.
  - `POST /cache/enable`, `POST /cache/disable`, `POST /cache/purge`.
- **In Postgres**: `SELECT * FROM pgshadow.tables` lists each opted-in table and, if one can't be cached, why.

## Performance

Measured with pgbench on a 2-vCPU sandbox, with Postgres 16, the proxy and pgbench sharing those CPUs. Setup: 8 clients, 200k-row table, `check.connections = 2`, release build. These numbers show the shape of the trade-off, not production capacity:

| workload | direct | through pgshadow |
|---|---:|---:|
| report query (GROUP BY over 200k rows, ~170 ms) | 45 tps | **~3,000 tps** (2.7 ms) |
| per-customer report, 50 parameter variants (~70 ms) | 117 tps | **~2,700 tps** (3.0 ms) |
| primary-key lookup (0.2 ms, below `min_query_time`: passes through) | 34k–46k tps | 19k–24k tps |
| single client, cached report query | ~6 tps | ~600 tps (1.6 ms) |

- **Hit cost.** Each hit costs one validation, about 1 ms of Postgres CPU per *batch* of hits, so under load it is shared across many hits. Each table and distinct snapshot in a batch adds an index lookup or two in the change log. This is why `min_query_time` defaults to 3 ms: below that, a check costs Postgres more than it saves.
- **Pass-through overhead** is one extra network hop plus parsing (memoised per statement text and per normalised shape). On this box that halves the throughput of 0.2 ms queries. Size the proxy's CPU like a connection pooler's.
- **Write cost.** Writes pay one extra small insert per transaction and table.

## Limitations

- **Session mode only.** Each client connection holds one Postgres connection, so there is no transaction pooling. Put PgBouncer *behind* pgshadow if you need pooling, or size `max_connections` like Postgres's.
- **One database per proxy.** Sessions in a different database than `check.dsn`'s pass through uncached.
- **Writes that bypass statement triggers are not seen.** Don't opt in tables written this way:
  - Writes directly into Timescale chunk tables instead of through the hypertable.
  - Third-party logical replication apply (pglogical and similar; built-in subscriptions are detected and refused).
  - `session_replication_role` tricks are fine, because the trigger is `ENABLE ALWAYS`.
- **Not tested against Timescale Cloud itself here.** The test suite runs against stock Postgres 16, with the hypertable code paths written to Timescale's catalog.
- **Cases that are bypassed, not cached:**
  - Roles that read a table only through column-level grants.
  - Any statement that runs while the database has a non-immutable user-defined cast.
  - Sessions in a transaction whose isolation level is above READ COMMITTED.
- **Small observable differences:**
  - Within an explicit transaction, re-executing or describing the unnamed portal after a cached result fails as if no portal existed.
  - Hits are not subject to `statement_timeout` (they are fast).
  - Rows of a query without `ORDER BY` come back in the order they were cached. Postgres doesn't guarantee an order either.
- **Requirements on client roles:** `USAGE` on PL/pgSQL (the default). In-transaction hits take their locks with a `DO` block.
- **Cancel race:** a cancel request that arrives while the proxy is re-reading a session's settings inside a transaction cancels that read. The transaction then fails, as it would have for the cancelled statement.

## Development

```bash
bench/dev-postgres.sh                      # local Postgres 16 on :5433 with test roles and TLS (Docker container when there is no system PG 16)
cargo test --lib                           # unit tests (classifier, protocol, cache encoding, ...)
cargo test --test it -- --test-threads 8   # integration: real Postgres through the proxy
PGHOST=127.0.0.1 bench/sql-check.sh        # pgshadow.validate() scenarios directly in SQL
python3 -m pytest -q py_tests              # psycopg + SQLAlchemy through the proxy
PGSHADOW_BIN=./target/release/pgshadow dev/proxy.sh restart dev/bench.toml && bench/bench.sh
```

The integration suite includes a staleness oracle: concurrent writers race cached readers, and every result is checked against what Postgres could have returned. It also has a regression test for every finding from two independent correctness reviews. Each of those tests was verified to fail with its fix removed.
