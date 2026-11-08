# Benchmarks on Fly and Neon

Run the same benchmarks and correctness checks as `bench/blog.sh` against hosted Postgres, with comparable database specs, so the post has numbers from setups people deploy.

## Targets

| | Fly | Neon |
|---|---|---|
| Postgres | `postgres:18` (18.6) on a Fly Machine, `performance-8x` (8 dedicated vCPU), 32 GB, 10 GB volume | existing project, us-west-2, Postgres 18.6, autoscaling up to 8 CU (8 vCPU, 32 GB) |
| Settings | stock, plus `shared_buffers=256MB` (Neon: 233 MB), `shared_preload_libraries=pg_stat_statements`, `max_connections=200` | as configured |
| Network | Fly private network (6PN), same region | public endpoint over TLS (direct, not `-pooler`) |
| Client | `performance-4x` Fly Machine in `sjc` running pgbench | its own `performance-4x` Machine |
| Proxy | pgshadow on its own `performance-2x` Machine (2 vCPU), started fresh for every run through `bench/proxy-ctl.py` | its own `performance-2x` Machine |

Neon's cap was measured, not read from settings: under 48 CPU-bound clients `num_cpus()` went from 1 to 8 within two minutes and stayed there. The proxy gets 2 vCPU on both targets, a quarter of the database, which is conservative for pgshadow; the tables report the proxy's CPU so a saturated proxy shows. The bench client runs in `sjc` for both. Fly has no region in Oregon, so Neon numbers include the sjc to us-west-2 round trip. The run records it. The Neon compute also serves other databases; the benchmark gets its own database, and Postgres time is filtered to it.

## Pieces

- `tests/it/common.rs`: the test DSN may carry a password and `sslmode=require`. The harness connects over TLS, and the proxy under test uses upstream TLS, so the staleness oracle runs against Neon.
- `bench/blog.sh`: target any Postgres through `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_DB`, `PG_SSLMODE` and `PGPASSWORD` instead of the local container. Postgres CPU comes from `PG_CPU=docker|procstat|none`. `procstat` reads `/proc/stat` through `pg_read_file`; that needs superuser and a VM that runs only Postgres, which is the case on Fly. Every run also records Postgres execution time from `pg_stat_statements` deltas for the benchmark database, which works on both. On Neon it records `num_cpus()` from `neon_utils`, to show the compute size during each run.
- `bench/Dockerfile.bench`: pgshadow and the integration test binary on `postgres:18-bookworm`, which supplies pgbench and psql.
- `bench/proxy-ctl.py`: a small HTTP service on the proxy Machine that starts a fresh pgshadow with the config `blog.sh` sends, and reports its CPU and log. `blog.sh` uses it when `PROXY_CTL` is set.
- `bench/fly.sh`: creates an app with the Postgres Machine and a client and proxy Machine per target, runs a target, copies the results back, and (`down`) destroys the app. The Neon DSN comes from the environment and is never written to the repo.

## Runs

Each target gets the `blog.sh` scenarios minus `micro`, plus a 120 s staleness oracle. On Fly it also gets the integration suite. Neon warms up first, until `num_cpus()` reports 8, so autoscaling doesn't skew the first runs; every run records `num_cpus()` before and after.

## Output

- `bench/results/fly-<date>/` and `bench/results/neon-<date>/`, each with `results.jsonl`, `env.txt` and the test output.
- Charts comparing direct and pgshadow per platform in `docs/img/cloud-*`.
- A section in `docs/benchmarks.md` and in the blog post.
