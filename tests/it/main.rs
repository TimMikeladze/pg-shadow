//! Integration tests. They need a Postgres to talk to:
//!   PGSHADOW_TEST_PG="host=127.0.0.1 port=5433 user=postgres dbname=shadow" cargo test --test it
//! (bench/dev-postgres.sh sets up a suitable local one.)

mod common;

mod audit;
mod basics;
mod caching;
mod gaps;
mod staleness;
