//! Basic observability: structured logs, a periodic one-line summary, and counters the
//! summary and health endpoints read. No metrics backend.
//!
//! Per-query lines are logged at debug level on target `pgshadow::query` (enable with
//! log.level = "info,pgshadow::query=debug"). They carry the outcome, the reason for a
//! bypass, timings and a statement shape hash -- never SQL text or parameter values.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::config::Log;

pub fn init_logging(cfg: &Log) {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_new(&cfg.level).unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = fmt().with_env_filter(filter).with_target(true);
    let _ = if cfg.format == "pretty" {
        builder.try_init()
    } else {
        builder
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .try_init()
    };
}

#[derive(Default)]
pub struct Stats {
    pub connections: AtomicU64,
    pub connections_total: AtomicU64,
    pub queries: AtomicU64,
    pub hits: AtomicU64,
    pub fills: AtomicU64,
    pub invalidated: AtomicU64,
    pub check_failed: AtomicU64,
    pub verify_ok: AtomicU64,
    pub verify_mismatch: AtomicU64,
    pub hit_bytes: AtomicU64,
    /// Sum of the original Postgres execution time of every result served from cache.
    pub saved_micros: AtomicU64,
    bypass: Mutex<HashMap<&'static str, u64>>,
}

impl Stats {
    pub fn bypass(&self, reason: &'static str) {
        *self.bypass.lock().entry(reason).or_default() += 1;
    }

    pub fn inc(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }

    fn take_bypass(&self) -> HashMap<&'static str, u64> {
        std::mem::take(&mut *self.bypass.lock())
    }
}

#[derive(Default, Clone, Copy)]
struct Snapshot {
    queries: u64,
    hits: u64,
    fills: u64,
    invalidated: u64,
    check_failed: u64,
    verify_ok: u64,
    verify_mismatch: u64,
    hit_bytes: u64,
    saved_micros: u64,
}

fn snap(s: &Stats) -> Snapshot {
    let l = |c: &AtomicU64| c.load(Ordering::Relaxed);
    Snapshot {
        queries: l(&s.queries),
        hits: l(&s.hits),
        fills: l(&s.fills),
        invalidated: l(&s.invalidated),
        check_failed: l(&s.check_failed),
        verify_ok: l(&s.verify_ok),
        verify_mismatch: l(&s.verify_mismatch),
        hit_bytes: l(&s.hit_bytes),
        saved_micros: l(&s.saved_micros),
    }
}

/// Logs one summary line per interval with counts since the previous line.
pub fn spawn_summary(
    stats: Arc<Stats>,
    interval: Duration,
    memory_usage: impl Fn() -> usize + Send + 'static,
) {
    if interval.is_zero() {
        return;
    }
    tokio::spawn(async move {
        let mut prev = snap(&stats);
        let mut tick = tokio::time::interval(interval);
        tick.tick().await;
        loop {
            tick.tick().await;
            let now = snap(&stats);
            let d = |f: fn(&Snapshot) -> u64| f(&now).saturating_sub(f(&prev));
            let mut bypass: Vec<_> = stats.take_bypass().into_iter().collect();
            bypass.sort_by(|a, b| b.1.cmp(&a.1));
            let top: Vec<String> = bypass
                .iter()
                .take(5)
                .map(|(r, n)| format!("{r}={n}"))
                .collect();
            tracing::info!(
                target: "pgshadow::summary",
                connections = stats.connections.load(Ordering::Relaxed),
                queries = d(|s| s.queries),
                hits = d(|s| s.hits),
                fills = d(|s| s.fills),
                invalidated = d(|s| s.invalidated),
                check_failed = d(|s| s.check_failed),
                verify_ok = d(|s| s.verify_ok),
                verify_mismatch = d(|s| s.verify_mismatch),
                hit_bytes = d(|s| s.hit_bytes),
                postgres_ms_saved = d(|s| s.saved_micros) / 1000,
                cache_memory_bytes = memory_usage() as u64,
                top_bypass_reasons = %top.join(","),
                "summary"
            );
            prev = now;
        }
    });
}
