//! Integration tests. They need a Postgres to talk to:
//!   PGSHADOW_TEST_PG="host=127.0.0.1 port=5433 user=postgres dbname=shadow" cargo test --test it
//! (bench/dev-postgres.sh sets up a suitable local one.)
//!
//! The DSN may carry a password and `sslmode=require` (TLS from the tests and the proxy to
//! Postgres), e.g. for running the staleness oracle against a hosted database. There,
//! PGSHADOW_STALENESS_WRITE_PAUSE_MS (default 15) spaces the oracle's writes out so reads
//! still hit between them.

mod common;

mod audit;
mod basics;
mod caching;
mod gaps;
mod staleness;
