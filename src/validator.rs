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
    /// Something changed: do not serve. `meta` is set when a table itself changed
    /// (schema, triggers, privileges, name resolution), not just its data.
    Invalid { meta: bool },
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
    meta: TableStates,
    ctx_tx: mpsc::Sender<oneshot::Sender<Option<FillContext>>>,
    shadow_aux: Aux,
    shadow: parking_lot::Mutex<ShadowCache>,
    /// The database and server the check connections are on. Only sessions on the same
    /// server and database can use the cache: changes are tracked, and results
    /// validated, per database.
    server: Arc<parking_lot::Mutex<Option<(String, String)>>>,
    generation: Arc<Generation>,
    clock_aux: Aux,
}

/// State of one referenced table as seen by the proxy's connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableState {
    pub oid: u32,
    pub eligible: bool,
    pub fp: String,
    /// Schema-qualified, quoted name.
    pub qualified: String,
}

/// What a fill records besides table state, read on the proxy's own connection before
/// the client's query is sent. Reading it early is conservative: anything that changes
/// before the query runs fails validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FillContext {
    pub snapshot: String,
    pub epoch: String,
    pub settings: String,
}

/// (search_path, role, schema, name)
type StateKey = (String, String, Option<String>, String);

/// A lazily (re)connected helper connection with one prepared statement.
struct Aux {
    cfg: Config,
    tls: Arc<rustls::ClientConfig>,
    sql: &'static str,
    conn: Mutex<Option<(tokio_postgres::Client, tokio_postgres::Statement)>>,
}

impl Aux {
    fn new(cfg: &Config, tls: &Arc<rustls::ClientConfig>, sql: &'static str) -> Self {
        Self { cfg: cfg.clone(), tls: tls.clone(), sql, conn: Mutex::new(None) }
    }

    async fn query(&self, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Option<Vec<tokio_postgres::Row>> {
        let mut guard = self.conn.lock().await;
        if guard.as_ref().is_none_or(|(c, _)| c.is_closed()) {
            let c = net::connect_check(&self.cfg.check.dsn, self.cfg.upstream.tls, &self.tls).await.ok()?;
            let st = c.prepare(self.sql).await.ok()?;
            *guard = Some((c, st));
        }
        let (c, st) = guard.as_ref()?;
        match tokio::time::timeout(self.cfg.check.timeout, c.query(st, params)).await {
            Ok(Ok(rows)) => Some(rows),
            Ok(Err(e)) => {
                tracing::debug!(error = %e, sql = self.sql, "helper query failed");
                None
            }
            Err(_) => {
                // A timed-out query may still be running: start over on a fresh connection.
                *guard = None;
                None
            }
        }
    }
}

/// Short-lived cache of table states. Reading a table's fingerprint some time before a
/// query runs is conservative: any change in between alters the fingerprint, and the
/// result fails validation.
struct TableStates {
    ttl: std::time::Duration,
    aux: Aux,
    cache: parking_lot::Mutex<std::collections::HashMap<StateKey, (Instant, Option<TableState>)>>,
}

const TABLE_STATE_SQL: &str = "SELECT ord, rel, eligible, fp, qualified FROM pgshadow.table_state(\
     $1::pg_catalog.text, $2::pg_catalog.text, $3::pg_catalog.text[], $4::pg_catalog.text[])";
const FILL_CONTEXT_SQL: &str = "SELECT snap, epoch, settings FROM pgshadow.fill_context()";
const SHADOWED_SQL: &str = "SELECT pgshadow.catalog_epoch(), \
     pgshadow.shadowed($1::pg_catalog.text[], $2::pg_catalog.text[], $3::pg_catalog.text[])";
const CLOCK_COLUMNS_SQL: &str =
    "SELECT pgshadow.clock_columns($1::pg_catalog.oid[], $2::pg_catalog.text[])";

impl TableStates {
    async fn lookup(&self, path: &str, role: &str, rels: &[(Option<String>, String)]) -> Option<Vec<Option<TableState>>> {
        let now = Instant::now();
        let key = |nsp: &Option<String>, name: &String| (path.to_string(), role.to_string(), nsp.clone(), name.clone());
        let mut out: Vec<Option<Option<TableState>>> = Vec::with_capacity(rels.len());
        let mut missing = Vec::new();
        {
            let c = self.cache.lock();
            for (i, (nsp, name)) in rels.iter().enumerate() {
                match c.get(&key(nsp, name)) {
                    Some((at, st)) if now.duration_since(*at) < self.ttl => out.push(Some(st.clone())),
                    _ => {
                        out.push(None);
                        missing.push(i);
                    }
                }
            }
        }
        if !missing.is_empty() {
            let nsps: Vec<Option<String>> = missing.iter().map(|&i| rels[i].0.clone()).collect();
            let names: Vec<String> = missing.iter().map(|&i| rels[i].1.clone()).collect();
            let rows = self.aux.query(&[&path, &role, &nsps, &names]).await?;
            let mut found = vec![None; names.len()];
            for r in rows {
                let ord: i32 = r.get(0);
                let oid: Option<u32> = r.get(1);
                let eligible: Option<bool> = r.get(2);
                let fp: Option<String> = r.get(3);
                let qualified: Option<String> = r.get(4);
                if let (Some(oid), Some(slot)) = (oid, found.get_mut(ord as usize - 1)) {
                    *slot = Some(TableState {
                        oid,
                        eligible: eligible.unwrap_or(false),
                        fp: fp.unwrap_or_default(),
                        qualified: qualified.unwrap_or_default(),
                    });
                }
            }
            let mut c = self.cache.lock();
            if c.len() > 50_000 {
                c.clear();
            }
            for (j, &i) in missing.iter().enumerate() {
                let st = found[j].clone();
                let (nsp, name) = &rels[i];
                c.insert(key(nsp, name), (now, st.clone()));
                out[i] = Some(st);
            }
        }
        out.into_iter().collect()
    }
}

/// Which built-in function, operator and type names are taken over by user-defined
/// objects, as of one catalog epoch. Only the latest epoch is kept.
#[derive(Default)]
struct ShadowCache {
    epoch: String,
    checked: std::collections::HashSet<String>,
    shadowed: std::collections::HashSet<String>,
}

pub const SCHEMA_VERSION: i32 = 5;

const VALIDATE_SQL: &str = "SELECT item, valid, meta_ok, settings FROM pgshadow.validate($1::pg_catalog.int4[], \
     $2::pg_catalog.oid[], $3::pg_catalog.text[], $4::pg_catalog.text[], $5::pg_catalog.text[], $6::pg_catalog.text[], \
     $7::pg_catalog.text[], $8::pg_catalog.text[], $9::pg_catalog.text[], $10::pg_catalog.text[], $11::pg_catalog.text[])";

/// The server the check connections are on: database, and postmaster start time (so a
/// session can confirm it is on the same server).
const SERVER_SQL: &str = "SELECT s.schema_version, pg_catalog.current_database()::pg_catalog.text, \
     EXTRACT(EPOCH FROM pg_catalog.pg_postmaster_start_time())::pg_catalog.text, pgshadow.settings_hash() \
     FROM pgshadow.state s";

/// Configuration generations: the settings hash last seen, and how often it changed.
#[derive(Default)]
struct Generation {
    seen: parking_lot::Mutex<Option<String>>,
    gen: std::sync::atomic::AtomicU64,
}

impl Generation {
    fn observe(&self, settings: &str) {
        let mut seen = self.seen.lock();
        if seen.as_deref() != Some(settings) {
            *seen = Some(settings.to_string());
            self.gen.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl Validator {
    pub fn start(cfg: &Config, tls: Arc<rustls::ClientConfig>) -> Arc<Self> {
        let (tx, rx) = mpsc::channel::<Request>(cfg.check.max_batch * 64);
        let rx = Arc::new(Mutex::new(rx));
        let connected = Arc::new(AtomicUsize::new(0));
        let installed = Arc::new(AtomicBool::new(false));
        let server = Arc::new(parking_lot::Mutex::new(None));
        let generation = Arc::new(Generation::default());
        for id in 0..cfg.check.connections {
            let rx = rx.clone();
            let cfg = cfg.clone();
            let tls = tls.clone();
            let connected = connected.clone();
            let installed = installed.clone();
            let server = server.clone();
            let generation = generation.clone();
            tokio::spawn(async move { worker(id, cfg, tls, rx, connected, installed, server, generation).await });
        }
        let meta = TableStates {
            ttl: cfg.cache.table_state_ttl,
            aux: Aux::new(cfg, &tls, TABLE_STATE_SQL),
            cache: parking_lot::Mutex::new(Default::default()),
        };
        let (ctx_tx, ctx_rx) = mpsc::channel(4096);
        tokio::spawn(context_worker(Aux::new(cfg, &tls, FILL_CONTEXT_SQL), ctx_rx, generation.clone()));
        Arc::new(Self {
            tx,
            connected,
            installed,
            timeout: cfg.check.timeout,
            meta,
            ctx_tx,
            shadow_aux: Aux::new(cfg, &tls, SHADOWED_SQL),
            shadow: parking_lot::Mutex::new(ShadowCache::default()),
            server,
            generation,
            clock_aux: Aux::new(cfg, &tls, CLOCK_COLUMNS_SQL),
        })
    }

    /// Whether a session in this database, on the server that started at this time, can
    /// use the cache.
    pub fn same_server(&self, db: &str, started: &str) -> bool {
        self.server.lock().as_ref().is_some_and(|(d, s)| d == db && s == started)
    }

    /// Increases whenever the server's configured settings are seen to change (a reload).
    /// Session settings read before then must be read again.
    pub fn settings_generation(&self) -> u64 {
        self.generation.gen.load(Ordering::SeqCst)
    }

    /// Whether casting any of these columns of these tables to a date/time type could
    /// read the clock (see pgshadow.clock_columns). None if unknown.
    pub async fn clock_columns(&self, rels: &[u32], cols: &[String]) -> Option<bool> {
        let rows = self.clock_aux.query(&[&rels, &cols]).await?;
        rows.first()?.get::<_, Option<bool>>(0)
    }

    /// Snapshot, catalog epoch and settings hash, read now on the proxy's connection.
    /// Concurrent fills share one round trip.
    pub async fn fill_context(&self) -> Option<FillContext> {
        let (tx, rx) = oneshot::channel();
        self.ctx_tx.try_send(tx).ok()?;
        rx.await.ok().flatten()
    }

    /// Whether any of these names could refer to a user-defined object instead of the
    /// built-in one, as of `epoch`. None if unknown (lookup failed or the epoch moved).
    pub async fn any_shadowed(&self, epoch: &str, funcs: &[String], ops: &[String], types: &[String]) -> Option<bool> {
        let tagged = |k: char, v: &[String]| v.iter().map(move |n| format!("{k}:{n}")).collect::<Vec<_>>();
        let all: Vec<String> = [tagged('f', funcs), tagged('o', ops), tagged('t', types)].concat();
        {
            let c = self.shadow.lock();
            if c.epoch == epoch {
                if all.iter().any(|n| c.shadowed.contains(n)) {
                    return Some(true);
                }
                if all.iter().all(|n| c.checked.contains(n)) {
                    return Some(false);
                }
            }
        }
        let rows = self.shadow_aux.query(&[&funcs, &ops, &types]).await?;
        let row = rows.first()?;
        let got_epoch: String = row.get(0);
        let names: Vec<String> = row.get(1);
        if got_epoch != epoch {
            return None;
        }
        let hit: std::collections::HashSet<&str> = names.iter().map(String::as_str).collect();
        let mut c = self.shadow.lock();
        if c.epoch != epoch {
            *c = ShadowCache { epoch: epoch.to_string(), ..Default::default() };
        }
        let mut any = false;
        for n in &all {
            // ambiguous across kinds is fine: a name is marked shadowed in every kind asked
            if hit.contains(&n[2..]) {
                c.shadowed.insert(n.clone());
                any = true;
            }
            c.checked.insert(n.clone());
        }
        Some(any)
    }

    /// Forget all cached table states (after a table itself changed).
    pub fn forget_table_states(&self) {
        self.meta.cache.lock().clear();
    }

    /// How each reference resolves for a session with this search_path and role, and the
    /// table's state. None if the lookup failed.
    pub async fn table_states(&self, path: &str, role: &str, rels: &[(Option<String>, String)]) -> Option<Vec<Option<TableState>>> {
        self.meta.lookup(path, role, rels).await
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
    server: Arc<parking_lot::Mutex<Option<(String, String)>>>,
    generation: Arc<Generation>,
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
            .query_opt(SERVER_SQL, &[])
            .await
            .map(|r| r.map(|r| (r.get::<_, i32>(0), r.get::<_, String>(1), r.get::<_, String>(2), r.get::<_, Option<String>>(3))))
        {
            Ok(Some((v, db, started, settings))) if v == SCHEMA_VERSION => {
                // Recorded before caching is turned on, so no session settings can have
                // been read before the first observation.
                if let Some(s) = settings {
                    generation.observe(&s);
                }
                *server.lock() = Some((db, started));
                installed.store(true, Ordering::Relaxed)
            }
            Ok(other) => {
                let other = other.map(|(v, _, _, _)| v);
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
                Ok((results, settings)) => {
                    if let Some(s) = settings {
                        generation.observe(&s);
                    }
                    tracing::trace!(worker = id, items = batch.len(), micros = started.elapsed().as_micros() as u64, "check round");
                    for (r, (valid, meta_ok)) in batch.into_iter().zip(results) {
                        let v = if valid { Verdict::Valid } else { Verdict::Invalid { meta: !meta_ok } };
                        let _ = r.reply.send(v);
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

async fn context_worker(
    aux: Aux,
    mut rx: mpsc::Receiver<oneshot::Sender<Option<FillContext>>>,
    generation: Arc<Generation>,
) {
    while let Some(first) = rx.recv().await {
        let mut waiting = vec![first];
        while let Ok(more) = rx.try_recv() {
            waiting.push(more);
        }
        waiting.retain(|w| !w.is_closed());
        if waiting.is_empty() {
            continue;
        }
        // Every waiter queued before this round started, so the snapshot is taken after it
        // asked, and before its query is sent.
        // All NULL during recovery: nothing is cached on a standby.
        let ctx = aux.query(&[]).await.and_then(|rows| {
            let r = rows.first()?;
            Some(FillContext { snapshot: r.get::<_, Option<String>>(0)?, epoch: r.get(1), settings: r.get::<_, Option<String>>(2)? })
        });
        if let Some(c) = &ctx {
            generation.observe(&c.settings);
        }
        for w in waiting {
            let _ = w.send(ctx.clone());
        }
    }
}

async fn run_batch(
    client: &tokio_postgres::Client,
    stmt: &tokio_postgres::Statement,
    batch: &[Request],
) -> Result<(Vec<(bool, bool)>, Option<String>), tokio_postgres::Error> {
    let mut item = Vec::new();
    let mut rel = Vec::new();
    let mut fp = Vec::new();
    let mut snap = Vec::new();
    let mut role = Vec::new();
    let mut nsp: Vec<Option<&str>> = Vec::new();
    let mut name: Vec<&str> = Vec::new();
    let mut path: Vec<&str> = Vec::new();
    let mut epoch: Vec<&str> = Vec::new();
    let mut settings: Vec<&str> = Vec::new();
    let mut funcs: Vec<&str> = Vec::new();
    for (i, r) in batch.iter().enumerate() {
        let e = &r.entry;
        for t in &e.tables {
            item.push(i as i32);
            rel.push(t.oid);
            fp.push(t.fp.as_str());
            snap.push(e.snapshot.as_str());
            role.push(e.role.as_str());
            nsp.push(t.nsp.as_deref());
            name.push(t.name.as_str());
            path.push(e.path.as_str());
            epoch.push(e.epoch.as_str());
            settings.push(e.settings.as_str());
            funcs.push(e.funcs.as_str());
        }
    }
    let rows = client
        .query(stmt, &[&item, &rel, &fp, &snap, &role, &nsp, &name, &path, &epoch, &settings, &funcs])
        .await?;
    let mut out = vec![(false, false); batch.len()];
    let mut current = None;
    for row in rows {
        if current.is_none() {
            current = row.get::<_, Option<String>>(3);
        }
        let i: i32 = row.get(0);
        let v: bool = row.get(1);
        let meta_ok: bool = row.get(2);
        if let Some(slot) = out.get_mut(i as usize) {
            *slot = (v, meta_ok);
        }
    }
    // Entries with no tables never get a row and stay invalid (they are never created).
    Ok((out, current))
}
