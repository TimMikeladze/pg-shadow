//! Batched validity checks.
//!
//! Every cache hit is confirmed with Postgres before it is served. Requests are queued;
//! each check connection takes everything waiting (up to max_batch) and validates it in
//! one round trip with pgshadow.validate(). Every request is answered by a round that
//! started after it was queued, so batching never makes a result older than a direct
//! query would be.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, oneshot, Mutex};

use crate::cache::Entry;
use crate::config::Config;
use crate::net;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Nothing the result depends on has changed: serve it.
    Valid,
    /// Something changed: do not serve.
    Invalid,
    /// The check could not be completed (timeout, connection problem): do not serve.
    Failed,
}

struct Request {
    entry: Arc<Entry>,
    reply: oneshot::Sender<Verdict>,
}

pub struct Validator {
    tx: mpsc::Sender<Request>,
    connected: Arc<AtomicUsize>,
    installed: Arc<AtomicBool>,
    timeout: std::time::Duration,
}

pub const SCHEMA_VERSION: i32 = 1;

const VALIDATE_SQL: &str = "SELECT item, valid FROM pgshadow.validate($1::int[], $2::oid[], $3::text[], \
                            $4::text[], $5::text[], $6::text[], $7::text[])";

impl Validator {
    pub fn start(cfg: &Config, tls: Arc<rustls::ClientConfig>) -> Arc<Self> {
        let (tx, rx) = mpsc::channel::<Request>(cfg.check.max_batch * 64);
        let rx = Arc::new(Mutex::new(rx));
        let connected = Arc::new(AtomicUsize::new(0));
        let installed = Arc::new(AtomicBool::new(false));
        for id in 0..cfg.check.connections {
            let rx = rx.clone();
            let cfg = cfg.clone();
            let tls = tls.clone();
            let connected = connected.clone();
            let installed = installed.clone();
            tokio::spawn(async move { worker(id, cfg, tls, rx, connected, installed).await });
        }
        Arc::new(Self { tx, connected, installed, timeout: cfg.check.timeout })
    }

    /// At least one check connection is up and the pgshadow schema is installed.
    pub fn ready(&self) -> bool {
        self.connected.load(Ordering::Relaxed) > 0 && self.installed.load(Ordering::Relaxed)
    }

    /// The pgshadow schema (right version) was found. Until it is, nothing is cached:
    /// the proxy's helper queries would fail.
    pub fn installed(&self) -> bool {
        self.installed.load(Ordering::Relaxed)
    }

    pub async fn check(&self, entry: Arc<Entry>) -> Verdict {
        if !self.ready() {
            return Verdict::Failed;
        }
        let (reply, rx) = oneshot::channel();
        if self.tx.try_send(Request { entry, reply }).is_err() {
            return Verdict::Failed; // queue full: shed load to Postgres directly
        }
        match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(v)) => v,
            _ => Verdict::Failed,
        }
    }
}

async fn worker(
    id: usize,
    cfg: Config,
    tls: Arc<rustls::ClientConfig>,
    rx: Arc<Mutex<mpsc::Receiver<Request>>>,
    connected: Arc<AtomicUsize>,
    installed: Arc<AtomicBool>,
) {
    let mut backoff = std::time::Duration::from_millis(200);
    loop {
        let client = match net::connect_check(&cfg.check.dsn, cfg.upstream.tls, &tls).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(worker = id, error = %format!("{e:#}"), "check connection failed; retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(std::time::Duration::from_secs(10));
                continue;
            }
        };
        match client
            .query_opt("SELECT schema_version FROM pgshadow.state", &[])
            .await
            .map(|r| r.map(|r| r.get::<_, i32>(0)))
        {
            Ok(Some(v)) if v == SCHEMA_VERSION => installed.store(true, Ordering::Relaxed),
            Ok(other) => {
                installed.store(false, Ordering::Relaxed);
                tracing::error!(found = ?other, expected = SCHEMA_VERSION,
                    "pgshadow schema missing or outdated; run `pgshadow install`. Caching is off until then.");
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                continue;
            }
            Err(e) => {
                installed.store(false, Ordering::Relaxed);
                tracing::error!(error = %e, "cannot read pgshadow.state; run `pgshadow install` and grant the check role access. Caching is off until then.");
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                continue;
            }
        }
        let stmt = match client.prepare(VALIDATE_SQL).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "preparing the check query failed");
                tokio::time::sleep(backoff).await;
                continue;
            }
        };
        backoff = std::time::Duration::from_millis(200);
        connected.fetch_add(1, Ordering::Relaxed);
        tracing::info!(worker = id, "check connection ready");

        loop {
            // Take one request (waiting), then everything else already queued.
            let mut batch = Vec::new();
            {
                let mut rx = rx.lock().await;
                match rx.recv().await {
                    Some(r) => batch.push(r),
                    None => return,
                }
                while batch.len() < cfg.check.max_batch {
                    match rx.try_recv() {
                        Ok(r) => batch.push(r),
                        Err(_) => break,
                    }
                }
            }
            batch.retain(|r| !r.reply.is_closed()); // callers that timed out
            if batch.is_empty() {
                continue;
            }
            let started = Instant::now();
            match run_batch(&client, &stmt, &batch).await {
                Ok(valid) => {
                    tracing::trace!(worker = id, items = batch.len(), micros = started.elapsed().as_micros() as u64, "check round");
                    for (r, v) in batch.into_iter().zip(valid) {
                        let _ = r.reply.send(if v { Verdict::Valid } else { Verdict::Invalid });
                    }
                }
                Err(e) => {
                    for r in batch {
                        let _ = r.reply.send(Verdict::Failed);
                    }
                    tracing::warn!(worker = id, error = %e, "check round failed");
                    if client.is_closed() {
                        break;
                    }
                }
            }
        }
        connected.fetch_sub(1, Ordering::Relaxed);
        tracing::warn!(worker = id, "check connection lost; reconnecting");
    }
}

async fn run_batch(
    client: &tokio_postgres::Client,
    stmt: &tokio_postgres::Statement,
    batch: &[Request],
) -> Result<Vec<bool>, tokio_postgres::Error> {
    let mut item = Vec::new();
    let mut rel = Vec::new();
    let mut fp = Vec::new();
    let mut snap = Vec::new();
    let mut role = Vec::new();
    let mut name: Vec<Option<String>> = Vec::new();
    let mut schemas: Vec<Option<String>> = Vec::new();
    for (i, r) in batch.iter().enumerate() {
        for t in &r.entry.tables {
            item.push(i as i32);
            rel.push(t.oid);
            fp.push(t.fp.clone());
            snap.push(r.entry.snapshot.clone());
            role.push(r.entry.role.clone());
            name.push(t.resolve.as_ref().map(|(n, _)| n.clone()));
            schemas.push(t.resolve.as_ref().map(|(_, s)| s.clone()));
        }
    }
    let rows = client.query(stmt, &[&item, &rel, &fp, &snap, &role, &name, &schemas]).await?;
    let mut out = vec![false; batch.len()];
    for row in rows {
        let i: i32 = row.get(0);
        let v: bool = row.get(1);
        if let Some(slot) = out.get_mut(i as usize) {
            *slot = v;
        }
    }
    // Entries with no tables never get a row and stay invalid (they are never created).
    Ok(out)
}
