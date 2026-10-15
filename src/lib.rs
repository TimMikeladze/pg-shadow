//! pgshadow: a never-stale caching proxy for Postgres.
//!
//! Clients connect to pgshadow instead of Postgres (only host and port change).
//! Plain SELECTs over opted-in tables are answered from a memory/disk cache when
//! Postgres confirms nothing they depend on has changed; everything else is forwarded
//! unchanged. See README.md for the design and guarantees.

pub mod admin;
pub mod app;
pub mod cache;
pub mod classify;
pub mod config;
pub mod net;
pub mod observe;
pub mod protocol;
pub mod server;
pub mod session;
pub mod validator;
