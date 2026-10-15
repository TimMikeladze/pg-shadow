//! One client connection.
//!
//! Each client session is paired with its own upstream Postgres session (like
//! PgBouncer's session mode), so transactions, prepared statements, temp tables, LISTEN,
//! COPY and session settings behave exactly as without the proxy. Two tasks:
//!
//! - the client reader (this module's main loop) decides, per request, whether to
//!   serve it from cache, fill the cache from it, or just forward it;
//! - the upstream reader routes Postgres's responses: to the client, into a capture
//!   buffer for the cache, or to the proxy itself for queries it injected.
//!
//! Responses from Postgres are split into "units", each ending with ReadyForQuery.
//! A request is only considered for the cache when no earlier unit is still in flight,
//! so served results always reflect the client's own earlier statements, statements are
//! never reordered, and the only writer to the client at that moment is the client
//! reader itself (the upstream reader writes everything else, in arrival order).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use tokio::io::{AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, oneshot, watch};

use crate::cache::{key_of, now_unix_ms, Entry, Store, TableDep};
use crate::classify::{Classified, Classifier, Effect, Isolation, RelName};
use crate::config::Config;
use crate::net::Stream;
use crate::observe::Stats;
use crate::protocol::{self, Frame, FrameReader};
use crate::validator::{Validator, Verdict};

// ---- state shared by all sessions ----------------------------------------------

/// Per-statement history, used to skip caching where it does not pay off.
#[derive(Default, Clone, Copy)]
struct Timing {
    ewma_micros: f64,
    samples: u32,
    valid: u32,
    invalid: u32,
    cooldown_until: Option<Instant>,
}

pub struct Shared {
    pub cfg: Config,
    pub store: Arc<Store>,
    pub validator: Arc<Validator>,
    pub classifier: Classifier,
    pub stats: Arc<Stats>,
    /// Runtime kill switch (admin endpoint). When false everything is forwarded.
    pub enabled: AtomicBool,
    pub upstream_tls: Arc<rustls::ClientConfig>,
    inflight: Mutex<HashMap<u128, watch::Receiver<bool>>>,
    timing: Mutex<HashMap<u64, Timing>>,
    /// Tables caching is turned off for after a verification mismatch, until the instant.
    disabled_rels: Mutex<HashMap<u32, Instant>>,
}

impl Shared {
    pub fn new(
        cfg: Config,
        store: Arc<Store>,
        validator: Arc<Validator>,
        stats: Arc<Stats>,
        upstream_tls: Arc<rustls::ClientConfig>,
    ) -> Arc<Self> {
        let classifier = Classifier::new(cfg.cache.classifier_capacity);
        let enabled = AtomicBool::new(cfg.cache.enabled);
        Arc::new(Self {
            cfg,
            store,
            validator,
            classifier,
            stats,
            enabled,
            upstream_tls,
            inflight: Mutex::new(HashMap::new()),
            timing: Mutex::new(HashMap::new()),
            disabled_rels: Mutex::new(HashMap::new()),
        })
    }

    fn record_time(&self, sql_hash: u64, elapsed: Duration) {
        let mut m = self.timing.lock();
        if m.len() > 100_000 {
            m.clear();
        }
        let t = m.entry(sql_hash).or_default();
        let us = elapsed.as_micros() as f64;
        t.ewma_micros = if t.samples == 0 { us } else { t.ewma_micros * 0.8 + us * 0.2 };
        t.samples = t.samples.saturating_add(1);
    }

    fn record_check(&self, sql_hash: u64, valid: bool) {
        let mut m = self.timing.lock();
        let t = m.entry(sql_hash).or_default();
        if valid {
            t.valid += 1;
        } else {
            t.invalid += 1;
        }
        // A statement whose cached results are nearly always invalid (its tables change
        // constantly) costs more to cache than it saves: pause it for a while.
        let cooldown = self.cfg.cache.invalidation_cooldown;
        if !cooldown.is_zero() && t.invalid >= 20 && t.invalid > 4 * t.valid {
            t.cooldown_until = Some(Instant::now() + cooldown);
            t.valid = 0;
            t.invalid = 0;
        }
        if t.valid + t.invalid > 1000 {
            t.valid /= 2;
            t.invalid /= 2;
        }
    }

    /// Reason to not cache this statement, from its history.
    fn policy_skip(&self, sql_hash: u64) -> Option<&'static str> {
        let mut m = self.timing.lock();
        let t = m.get_mut(&sql_hash)?;
        if let Some(until) = t.cooldown_until {
            if Instant::now() < until {
                return Some("frequently invalidated");
            }
            t.cooldown_until = None;
        }
        let min = self.cfg.cache.min_query_time.as_micros() as f64;
        if t.samples >= 3 && t.ewma_micros < min {
            return Some("faster than a cache check");
        }
        None
    }

    fn rel_disabled(&self, oids: impl IntoIterator<Item = u32>) -> bool {
        let m = self.disabled_rels.lock();
        if m.is_empty() {
            return false;
        }
        let now = Instant::now();
        oids.into_iter().any(|o| m.get(&o).is_some_and(|until| *until > now))
    }

    fn disable_rels(&self, oids: impl IntoIterator<Item = u32>, for_: Duration) {
        let until = Instant::now() + for_;
        let mut m = self.disabled_rels.lock();
        for o in oids {
            m.insert(o, until);
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }
}

/// Marks a key as being filled; waiters are released when this is dropped, whatever
/// happens to the fill.
struct InflightGuard {
    sh: Arc<Shared>,
    key: u128,
    tx: watch::Sender<bool>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.sh.inflight.lock().remove(&self.key);
        let _ = self.tx.send(true);
    }
}

enum Inflight {
    Lead(InflightGuard),
    Wait(watch::Receiver<bool>),
}

impl Shared {
    /// Become the session that fills `key`, or get a handle to wait for the one that is.
    fn claim(self: &Arc<Self>, key: u128) -> Inflight {
        let mut m = self.inflight.lock();
        if let Some(rx) = m.get(&key) {
            return Inflight::Wait(rx.clone());
        }
        let (tx, rx) = watch::channel(false);
        m.insert(key, rx);
        Inflight::Lead(InflightGuard { sh: self.clone(), key, tx })
    }
}

// ---- per-session state -----------------------------------------------------------

#[derive(Clone, Debug)]
struct StmtInfo {
    sql: Arc<str>,
    param_oids: Arc<[u32]>,
    /// State of the statement's tables when it was prepared (None: unknown). A later
    /// Bind-only request may only be answered with a result read from tables in exactly
    /// this state: otherwise Postgres would re-plan it or refuse ("cached plan must not
    /// change result type"), and the client's idea of the row shape could be wrong.
    deps: Option<Arc<Vec<(u32, String)>>>,
}

fn dep_set(tables: &[TableDep]) -> Arc<Vec<(u32, String)>> {
    let mut v: Vec<(u32, String)> = tables.iter().map(|t| (t.oid, t.fp.clone())).collect();
    v.sort();
    v.dedup();
    Arc::new(v)
}

/// What this session's settings look like, as far as results are concerned.
#[derive(Debug)]
struct SessionFp {
    hash: u128,
    current_user: String,
    database: String,
    /// The search_path setting as the session has it.
    path: String,
    temp_rels: i64,
    default_iso: Option<Isolation>,
    /// Postmaster start time: which server the session is on.
    server: String,
    in_recovery: bool,
    /// Server settings generation (Validator::settings_generation) when this was read.
    gen: u64,
}

struct State {
    /// Transaction status from the latest ReadyForQuery: I, T or E.
    status: u8,
    /// Upstream units sent but not yet finished.
    pending: usize,
    /// The current transaction wrote (or may have written) data.
    wrote: bool,
    /// Isolation level of the current transaction, if known.
    iso: Option<Isolation>,
    fp: Option<Arc<SessionFp>>,
    /// Settings may have changed since `fp` was read.
    fp_stale: bool,
    fp_refreshing: bool,
    /// Settings generation when the pending refresh was sent.
    fp_refresh_gen: u64,
    /// Prepared statements Postgres has confirmed.
    statements: HashMap<String, StmtInfo>,
}

/// Effects of one upstream unit, applied in order when its ReadyForQuery arrives.
#[derive(Default, Clone, Debug)]
struct UnitEffects {
    writes: bool,
    /// Last BEGIN seen: explicit isolation level, or None for the session default.
    begin: Option<Option<Isolation>>,
    /// COMMIT or ROLLBACK seen.
    txn_end: bool,
    /// Ran something that could have run user code (a view, trigger, function, operator):
    /// the session's settings must be read again before the cache is used.
    dirty: bool,
    settings: bool,
    parsed: Vec<(String, StmtInfo)>,
}

impl UnitEffects {
    fn from_effects(effects: &[Effect]) -> Self {
        let mut fx = UnitEffects::default();
        for e in effects {
            match e {
                Effect::Show | Effect::TxnControl | Effect::Deallocate(_) => {}
                Effect::Read => fx.dirty = true,
                Effect::TxnEnd => fx.txn_end = true,
                Effect::Begin(iso) => fx.begin = Some(*iso),
                Effect::Write => {
                    fx.writes = true;
                    fx.dirty = true;
                }
                Effect::DiscardAll | Effect::Other => {
                    fx.writes = true;
                    fx.settings = true;
                    fx.dirty = true;
                }
            }
        }
        fx
    }

    /// For units whose contents the proxy cannot fully see.
    fn unknown() -> Self {
        UnitEffects { writes: true, begin: None, txn_end: true, settings: true, dirty: true, parsed: vec![] }
    }

    fn merge(&mut self, o: UnitEffects) {
        self.writes |= o.writes;
        self.settings |= o.settings;
        self.txn_end |= o.txn_end;
        self.dirty |= o.dirty;
        if o.begin.is_some() {
            self.begin = o.begin;
        }
        self.parsed.extend(o.parsed);
    }
}

struct Capture {
    buf: BytesMut,
    limit: usize,
    clean: bool,
}

impl Capture {
    fn push(&mut self, f: &Frame) {
        if !self.clean {
            return;
        }
        // Only plain result messages may be cached. Notices, errors, parameter changes,
        // notifications, COPY, partial results: never.
        if !matches!(f.tag, b'1' | b'2' | b't' | b'T' | b'n' | b'D' | b'C') || self.buf.len() + f.wire_len() > self.limit {
            self.clean = false;
            self.buf = BytesMut::new();
            return;
        }
        f.encode_into(&mut self.buf);
    }
}

struct Done {
    response: Option<Bytes>,
    elapsed: Duration,
}

enum SwallowKind {
    Frames(oneshot::Sender<Vec<Frame>>),
    Fingerprint,
}

enum Up {
    Forward {
        /// Sent to the client ahead of the unit's responses.
        prefix: Bytes,
        fx: UnitEffects,
        capture: Option<Capture>,
        done: Option<oneshot::Sender<Done>>,
        started: Instant,
        /// Statement to record timing for (only when nothing was queued ahead of it).
        timing_key: Option<u64>,
    },
    Swallow {
        fx: UnitEffects,
        frames: Vec<Frame>,
        kind: SwallowKind,
    },
}

type ClientW = Arc<tokio::sync::Mutex<WriteHalf<Stream>>>;

async fn write_client(w: &ClientW, b: &[u8]) -> std::io::Result<()> {
    let mut w = w.lock().await;
    w.write_all(b).await?;
    w.flush().await
}

// ---- requests ------------------------------------------------------------------

enum Req {
    Simple {
        frame: Frame,
        sql: String,
        /// The session had an unnamed prepared statement, which this query destroys.
        drops_unnamed: bool,
    },
    Extended {
        /// The whole batch, ending with Sync.
        frames: Vec<Frame>,
        /// Index of the Parse in `frames`, if the batch has one.
        parse_idx: Option<usize>,
        stmt: StmtInfo,
        bind: protocol::Bind,
        describe: Option<u8>,
    },
}

impl Req {
    fn sql(&self) -> &str {
        match self {
            Req::Simple { sql, .. } => sql,
            Req::Extended { stmt, .. } => &stmt.sql,
        }
    }

    /// Everything that determines the response bytes, given the session fingerprint.
    fn canonical(&self, fp: &SessionFp) -> Vec<u8> {
        let mut b = Vec::with_capacity(self.sql().len() + 64);
        b.extend_from_slice(b"pgshadow-v1");
        b.extend_from_slice(&fp.hash.to_le_bytes());
        match self {
            Req::Simple { sql, .. } => {
                b.push(b'Q');
                b.extend_from_slice(sql.as_bytes());
            }
            Req::Extended { parse_idx, stmt, bind, describe, .. } => {
                b.push(b'E');
                b.push(parse_idx.is_some() as u8);
                b.push(describe.unwrap_or(0));
                put_bytes(&mut b, stmt.sql.as_bytes());
                b.extend_from_slice(&(stmt.param_oids.len() as u32).to_le_bytes());
                for o in stmt.param_oids.iter() {
                    b.extend_from_slice(&o.to_le_bytes());
                }
                b.extend_from_slice(&(bind.parameter_format_codes.len() as u32).to_le_bytes());
                for f in &bind.parameter_format_codes {
                    b.extend_from_slice(&f.to_le_bytes());
                }
                b.extend_from_slice(&(bind.parameters.len() as u32).to_le_bytes());
                for p in &bind.parameters {
                    match p {
                        None => b.extend_from_slice(&u64::MAX.to_le_bytes()),
                        Some(v) => put_bytes(&mut b, v),
                    }
                }
                b.extend_from_slice(&(bind.result_column_format_codes.len() as u32).to_le_bytes());
                for f in &bind.result_column_format_codes {
                    b.extend_from_slice(&f.to_le_bytes());
                }
            }
        }
        b
    }

    fn frames(&self, skip_parse: bool) -> Vec<Frame> {
        match self {
            Req::Simple { frame, .. } => vec![frame.clone()],
            Req::Extended { frames, parse_idx, .. } => frames
                .iter()
                .enumerate()
                .filter(|(i, _)| !(skip_parse && Some(*i) == *parse_idx))
                .map(|(_, f)| f.clone())
                .collect(),
        }
    }

    fn parse_frame(&self) -> Option<Frame> {
        match self {
            Req::Extended { frames, parse_idx: Some(i), .. } => Some(frames[*i].clone()),
            _ => None,
        }
    }

    fn parsed(&self) -> Vec<(String, StmtInfo)> {
        self.parsed_with(None)
    }

    fn parsed_with(&self, deps: Option<Arc<Vec<(u32, String)>>>) -> Vec<(String, StmtInfo)> {
        match self {
            Req::Extended { parse_idx: Some(_), stmt, bind, .. } => {
                let mut info = stmt.clone();
                info.deps = deps;
                vec![(bind.statement_name.clone().unwrap_or_default(), info)]
            }
            _ => vec![],
        }
    }

    /// For a Bind of a statement prepared earlier: its tables' state when prepared.
    fn prepared_deps(&self) -> Option<Option<&Arc<Vec<(u32, String)>>>> {
        match self {
            Req::Extended { parse_idx: None, stmt, .. } => Some(stmt.deps.as_ref()),
            _ => None,
        }
    }

    /// Parameters that Postgres could turn into the current time ('now', 'today 10:00',
    /// ...). Checked in every format and at any length: the binary form of a text value is
    /// the same bytes, and a false positive only means a cache bypass.
    fn has_time_param(&self) -> bool {
        let Req::Extended { bind, .. } = self else { return false };
        bind.parameters.iter().any(|p| p.as_ref().is_some_and(|v| crate::classify::is_time_word(&String::from_utf8_lossy(v))))
    }
}

fn put_bytes(b: &mut Vec<u8>, v: &[u8]) {
    b.extend_from_slice(&(v.len() as u64).to_le_bytes());
    b.extend_from_slice(v);
}

// ---- the session ---------------------------------------------------------------

const OUR_STMT: &str = "__pgshadow_q";
const OUR_PORTAL: &str = "__pgshadow_p";

/// The session's identity and the settings that affect results (hashed: columns 0-6),
/// then facts about its server and transaction (not hashed: 7-10).
const FP_SQL: &str = "SELECT current_user::pg_catalog.text, session_user::pg_catalog.text, \
     pg_catalog.current_database()::pg_catalog.text, pg_catalog.current_setting('search_path'), \
     CASE WHEN pg_catalog.pg_my_temp_schema() OPERATOR(pg_catalog.=) 0 THEN '0' \
          ELSE (SELECT pg_catalog.count(*) FROM pg_catalog.pg_class c \
                WHERE c.relnamespace OPERATOR(pg_catalog.=) pg_catalog.pg_my_temp_schema())::pg_catalog.text END, \
     pg_catalog.current_setting('default_transaction_isolation'), \
     pg_catalog.md5(pg_catalog.concat_ws(',', pg_catalog.current_setting('bytea_output'), \
       pg_catalog.current_setting('extra_float_digits'), pg_catalog.current_setting('lc_monetary'), \
       pg_catalog.current_setting('lc_numeric'), pg_catalog.current_setting('lc_time'), \
       pg_catalog.current_setting('xmloption'), pg_catalog.current_setting('xmlbinary'), \
       pg_catalog.current_setting('IntervalStyle'), pg_catalog.current_setting('DateStyle'), \
       pg_catalog.current_setting('TimeZone'), pg_catalog.current_setting('timezone_abbreviations'), \
       pg_catalog.current_setting('standard_conforming_strings'), pg_catalog.current_setting('backslash_quote'), \
       pg_catalog.current_setting('escape_string_warning'), pg_catalog.current_setting('default_text_search_config'), \
       pg_catalog.current_setting('array_nulls'), pg_catalog.current_setting('quote_all_identifiers'), \
       pg_catalog.current_setting('transform_null_equals'), pg_catalog.current_setting('gin_fuzzy_search_limit'), \
       pg_catalog.current_setting('client_encoding'), pg_catalog.current_setting('lc_messages'))), \
     EXTRACT(EPOCH FROM pg_catalog.pg_postmaster_start_time())::pg_catalog.text, \
     pg_catalog.pg_is_in_recovery()::pg_catalog.text, \
     (pg_catalog.pg_current_xact_id_if_assigned() IS NOT NULL)::pg_catalog.text, \
     pg_catalog.current_setting('transaction_isolation')";

/// Inside a transaction: its isolation level, and whether it has written anything (by
/// any route: a trigger, a function, a view calling one).
const TXN_SQL: &str = "SELECT (pg_catalog.pg_current_xact_id_if_assigned() IS NOT NULL)::pg_catalog.text, \
     pg_catalog.current_setting('transaction_isolation')";

/// Extended-protocol messages running one of the proxy's own queries on the client's
/// session, without touching the client's unnamed statement or portal.
fn our_query(sql: &str, params: &[Option<&str>]) -> Vec<Frame> {
    vec![
        protocol::close_msg(b'S', OUR_STMT),
        protocol::parse_msg(OUR_STMT, sql),
        protocol::bind_msg(OUR_PORTAL, OUR_STMT, params),
        protocol::execute_msg(OUR_PORTAL),
        protocol::close_msg(b'P', OUR_PORTAL),
        protocol::sync_msg(),
    ]
}

fn rows_of(frames: &[Frame]) -> Option<Vec<Vec<Option<String>>>> {
    if frames.iter().any(|f| f.tag == b'E') {
        return None;
    }
    Some(frames.iter().filter(|f| f.tag == b'D').map(|f| protocol::data_row_values(&f.body)).collect())
}

fn parse_fp(frames: &[Frame]) -> Option<SessionFp> {
    let rows = rows_of(frames)?;
    let r = rows.first()?;
    let col = |i: usize| r.get(i).cloned().flatten().unwrap_or_default();
    if r.len() < 11 {
        return None;
    }
    let mut all = String::new();
    for i in 0..7 {
        all.push_str(&col(i));
        all.push('\u{1}');
    }
    Some(SessionFp {
        hash: xxhash_rust::xxh3::xxh3_128(all.as_bytes()),
        current_user: col(0),
        database: col(2),
        path: col(3),
        temp_rels: col(4).parse().unwrap_or(1),
        default_iso: Isolation::parse(&col(5)),
        server: col(7),
        in_recovery: col(8) != "false",
        gen: 0,
    })
}

/// (isolation level, has written) from a TXN_SQL result, or from FP_SQL's last columns.
fn parse_txn(frames: &[Frame], first: usize) -> Option<(Option<Isolation>, bool)> {
    let rows = rows_of(frames)?;
    let r = rows.first()?;
    let iso = Isolation::parse(r.get(first + 1)?.as_deref()?);
    let wrote = r.get(first)?.as_deref()? != "false";
    Some((iso, wrote))
}

pub struct Session {
    sh: Arc<Shared>,
    st: Arc<Mutex<State>>,
    /// Signalled when no upstream units are in flight.
    idle: Arc<tokio::sync::Notify>,
    up_w: WriteHalf<Stream>,
    up_units: mpsc::UnboundedSender<Up>,
    client_w: ClientW,
    /// A forwarded batch is open: its Sync has not been sent yet.
    open_forward: bool,
    conn: u64,
}

pub async fn run(
    sh: Arc<Shared>,
    conn: u64,
    client_rd: FrameReader<ReadHalf<Stream>>,
    client_w: WriteHalf<Stream>,
    upstream_rd: FrameReader<ReadHalf<Stream>>,
    up_w: WriteHalf<Stream>,
    status: u8,
) {
    let st = Arc::new(Mutex::new(State {
        status,
        pending: 0,
        wrote: false,
        iso: None,
        fp: None,
        fp_stale: true,
        fp_refreshing: false,
        fp_refresh_gen: 0,
        statements: HashMap::new(),
    }));
    let client_w: ClientW = Arc::new(tokio::sync::Mutex::new(client_w));
    let (up_tx, up_rx) = mpsc::unbounded_channel();
    let (closed_tx, mut closed_rx) = watch::channel(false);
    let idle = Arc::new(tokio::sync::Notify::new());
    let reader = tokio::spawn({
        let st = st.clone();
        let sh = sh.clone();
        let idle = idle.clone();
        let client_w = client_w.clone();
        async move {
            if let Err(e) = upstream_reader(upstream_rd, up_rx, client_w, st, sh, idle).await {
                tracing::debug!(conn, error = %e, "upstream read ended");
            }
            let _ = closed_tx.send(true);
        }
    });
    let mut s = Session { sh, st, idle, up_w, up_units: up_tx, client_w: client_w.clone(), open_forward: false, conn };
    let mut rd = client_rd;
    let result = async {
        // Read the session's settings right away, so the first query can use the cache.
        s.refresh_fingerprint_if_needed().await?;
        loop {
            let frame = tokio::select! {
                f = rd.read_frame() => f?,
                _ = closed_rx.changed() => None,
            };
            let Some(frame) = frame else { break };
            if !s.on_frame(frame, &mut rd).await? {
                break;
            }
        }
        Ok::<_, std::io::Error>(())
    }
    .await;
    if let Err(e) = result {
        tracing::debug!(conn, error = %e, "client session ended");
    }
    let _ = s.up_w.shutdown().await;
    drop(s);
    let _ = reader.await;
    let _ = client_w.lock().await.shutdown().await;
}

impl Session {
    /// Handle one client message. Returns false when the session should end.
    async fn on_frame(&mut self, frame: Frame, rd: &mut FrameReader<ReadHalf<Stream>>) -> std::io::Result<bool> {
        match frame.tag {
            b'Q' => self.on_simple(frame).await?,
            b'P' | b'B' | b'D' | b'E' | b'C' if self.open_forward => {
                self.track_closes(std::slice::from_ref(&frame));
                self.write_up(&[frame]).await?;
            }
            b'P' | b'B' | b'D' | b'E' | b'C' => {
                // Collect an extended-protocol batch up to its Sync (or Flush).
                let mut batch = vec![frame];
                let mut bytes = batch[0].wire_len();
                loop {
                    let Some(next) = rd.read_frame().await? else { return Ok(false) };
                    bytes += next.wire_len();
                    match next.tag {
                        b'S' => {
                            batch.push(next);
                            self.on_batch(batch).await?;
                            break;
                        }
                        b'P' | b'B' | b'D' | b'E' | b'C' if batch.len() < 256 && bytes < (4 << 20) => batch.push(next),
                        _ => {
                            // Flush, very long pipelines, or anything unusual: forward as-is.
                            batch.push(next);
                            let ends = batch.last().map(|f| f.tag) == Some(b'S');
                            self.forward_batch(batch, ends).await?;
                            break;
                        }
                    }
                }
            }
            b'S' => {
                let ends = true;
                if self.open_forward {
                    self.write_up(&[frame]).await?;
                    self.open_forward = false;
                } else {
                    self.forward(vec![frame], ends, UnitEffects::default(), None, None, None, Bytes::new()).await?;
                }
            }
            b'H' => {
                if self.open_forward {
                    self.write_up(&[frame]).await?;
                } else {
                    self.forward(vec![frame], false, UnitEffects::unknown(), None, None, None, Bytes::new()).await?;
                }
            }
            b'X' => {
                self.write_up(&[frame]).await?;
                return Ok(false);
            }
            // COPY data and anything else belongs to the unit already in progress.
            b'd' | b'c' | b'f' => self.write_up(&[frame]).await?,
            b'F' => self.forward(vec![frame], true, UnitEffects::unknown(), None, None, None, Bytes::new()).await?,
            _ => self.write_up(&[frame]).await?,
        }
        Ok(true)
    }

    async fn write_up(&mut self, frames: &[Frame]) -> std::io::Result<()> {
        protocol::write_frames(&mut self.up_w, frames).await?;
        self.up_w.flush().await
    }

    fn push_unit(&mut self, unit: Up) {
        self.st.lock().pending += 1;
        let _ = self.up_units.send(unit);
    }

    /// Forward frames to Postgres with their responses going to the client.
    #[allow(clippy::too_many_arguments)]
    async fn forward(
        &mut self,
        frames: Vec<Frame>,
        ends_unit: bool,
        fx: UnitEffects,
        capture: Option<Capture>,
        done: Option<oneshot::Sender<Done>>,
        timing_key: Option<u64>,
        prefix: Bytes,
    ) -> std::io::Result<()> {
        if !self.open_forward {
            self.push_unit(Up::Forward { prefix, fx, capture, done, started: Instant::now(), timing_key });
        }
        self.open_forward = !ends_unit;
        self.write_up(&frames).await
    }

    /// Run frames on the session with the responses kept from the client.
    async fn swallow(&mut self, frames: Vec<Frame>, fx: UnitEffects) -> std::io::Result<oneshot::Receiver<Vec<Frame>>> {
        let (tx, rx) = oneshot::channel();
        self.push_unit(Up::Swallow { fx, frames: Vec::new(), kind: SwallowKind::Frames(tx) });
        self.write_up(&frames).await?;
        Ok(rx)
    }

    /// A response the proxy produces itself. Only called with nothing in flight upstream.
    async fn local(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        write_client(&self.client_w, bytes).await
    }

    fn apply_immediate(&mut self, effects: &[Effect]) {
        let mut st = self.st.lock();
        for e in effects {
            match e {
                Effect::Deallocate(Some(n)) => {
                    st.statements.remove(n);
                }
                Effect::Deallocate(None) | Effect::DiscardAll => st.statements.clear(),
                _ => {}
            }
        }
    }

    fn track_closes(&mut self, frames: &[Frame]) {
        for f in frames.iter().filter(|f| f.tag == b'C') {
            if let Some(c) = f.decode::<protocol::Close>() {
                if c.target_type == b'S' {
                    self.st.lock().statements.remove(c.name.as_deref().unwrap_or(""));
                }
            }
        }
    }

    /// Re-read the session's settings when they may have changed. Sent ahead of the
    /// client's next request (not inside a failed transaction), and not waited for.
    async fn refresh_fingerprint_if_needed(&mut self) -> std::io::Result<()> {
        if !self.sh.enabled() || !self.sh.validator.installed() || self.open_forward {
            return Ok(());
        }
        {
            let mut st = self.st.lock();
            if !(st.fp_stale && !st.fp_refreshing && matches!(st.status, b'I' | b'T') && st.pending == 0) {
                return Ok(());
            }
            st.fp_refreshing = true;
            st.fp_refresh_gen = self.sh.validator.settings_generation();
        }
        self.push_unit(Up::Swallow { fx: UnitEffects::default(), frames: Vec::new(), kind: SwallowKind::Fingerprint });
        self.write_up(&our_query(FP_SQL, &[])).await
    }

    // ---- simple protocol ----

    async fn on_simple(&mut self, frame: Frame) -> std::io::Result<()> {
        self.refresh_fingerprint_if_needed().await?;
        let Some(q) = frame.decode::<protocol::Query>() else {
            return self.forward(vec![frame], true, UnitEffects::unknown(), None, None, None, Bytes::new()).await;
        };
        let cls = self.sh.classifier.get(&q.query);
        self.apply_immediate(&cls.effects);
        // A simple query destroys the unnamed prepared statement.
        let drops_unnamed = self.st.lock().statements.remove("").is_some();
        if cls.cacheable.is_some() {
            let req = Req::Simple { frame, sql: q.query, drops_unnamed };
            return self.try_cache(req, cls).await;
        }
        self.bypass(&cls, cls.bypass_reason_or_effect());
        let key = Some(cls.shape);
        self.forward(vec![frame], true, UnitEffects::from_effects(&cls.effects), None, None, key, Bytes::new()).await
    }

    // ---- extended protocol ----

    async fn on_batch(&mut self, frames: Vec<Frame>) -> std::io::Result<()> {
        self.refresh_fingerprint_if_needed().await?;
        match self.cacheable_shape(&frames) {
            Some(req) => {
                let cls = self.sh.classifier.get(req.sql());
                self.apply_immediate(&cls.effects);
                if cls.cacheable.is_some() {
                    return self.try_cache(req, cls).await;
                }
                self.bypass(&cls, cls.bypass_reason_or_effect());
                let mut fx = UnitEffects::from_effects(&cls.effects);
                fx.parsed = req.parsed();
                let key = Some(cls.shape);
                self.forward(req.frames(false), true, fx, None, None, key, Bytes::new()).await
            }
            None => self.forward_batch(frames, true).await,
        }
    }

    /// Forward a batch that is not a cache candidate, tracking what it does.
    async fn forward_batch(&mut self, frames: Vec<Frame>, ends: bool) -> std::io::Result<()> {
        let mut fx = if ends { UnitEffects::default() } else { UnitEffects::unknown() };
        let mut local: HashMap<String, StmtInfo> = HashMap::new();
        // The last-read settings are good enough even if stale: the recorded state names
        // exact tables, so a different resolution can only cause a mismatch (no hit). Right
        // after connecting they may not have been read yet: wait for that.
        let mut fp = self.st.lock().fp.clone();
        if fp.is_none() && self.st.lock().fp_refreshing && !self.open_forward && self.sh.enabled() {
            self.wait_idle().await;
            fp = self.st.lock().fp.clone();
        }
        for f in &frames {
            match f.tag {
                b'P' => match f.decode::<protocol::Parse>() {
                    Some(p) => {
                        // Statements prepared now and bound later: remember their tables'
                        // state (read before the Parse reaches Postgres: conservative).
                        let deps = prepared_deps(&self.sh, fp.clone(), &p.query).await;
                        let info = StmtInfo { sql: Arc::from(p.query.as_str()), param_oids: Arc::from(p.type_oids.as_slice()), deps };
                        let name = p.name.unwrap_or_default();
                        local.insert(name.clone(), info.clone());
                        fx.parsed.push((name, info));
                    }
                    None => fx.merge(UnitEffects::unknown()),
                },
                b'B' => {
                    let stmt = f.decode::<protocol::Bind>().and_then(|b| {
                        let name = b.statement_name.unwrap_or_default();
                        local.get(&name).cloned().or_else(|| self.st.lock().statements.get(&name).cloned())
                    });
                    match stmt {
                        Some(info) => {
                            let cls = self.sh.classifier.get(&info.sql);
                            self.apply_immediate(&cls.effects);
                            fx.merge(UnitEffects::from_effects(&cls.effects));
                        }
                        None => fx.merge(UnitEffects::unknown()),
                    }
                }
                _ => {}
            }
        }
        self.track_closes(&frames);
        self.sh.stats.bypass("extended batch not cacheable");
        Stats::inc(&self.sh.stats.queries);
        self.forward(frames, ends, fx, None, None, None, Bytes::new()).await
    }

    /// [Parse] Bind [Describe] Execute Sync, unnamed portal, all rows.
    fn cacheable_shape(&self, frames: &[Frame]) -> Option<Req> {
        let tags: Vec<u8> = frames.iter().map(|f| f.tag).collect();
        let (parse_idx, rest) = match tags.as_slice() {
            [b'P', rest @ ..] => (Some(0), rest),
            rest => (None, rest),
        };
        let off = parse_idx.map_or(0, |_| 1);
        let describe = match rest {
            [b'B', b'D', b'E', b'S'] => true,
            [b'B', b'E', b'S'] => false,
            _ => return None,
        };
        let bind: protocol::Bind = frames[off].decode()?;
        if bind.portal_name.as_deref().unwrap_or("") != "" {
            return None;
        }
        let exec: protocol::Execute = frames[frames.len() - 2].decode()?;
        if exec.name.as_deref().unwrap_or("") != "" || exec.max_rows != 0 {
            return None;
        }
        let stmt_name = bind.statement_name.clone().unwrap_or_default();
        let stmt = match parse_idx {
            Some(i) => {
                let p: protocol::Parse = frames[i].decode()?;
                let pname = p.name.clone().unwrap_or_default();
                // Re-parsing an existing named statement is an error Postgres must report.
                if pname != stmt_name || (!pname.is_empty() && self.st.lock().statements.contains_key(&pname)) {
                    return None;
                }
                StmtInfo { sql: Arc::from(p.query.as_str()), param_oids: Arc::from(p.type_oids.as_slice()), deps: None }
            }
            None => self.st.lock().statements.get(&stmt_name)?.clone(),
        };
        let describe = if describe {
            let d: protocol::Describe = frames[off + 1].decode()?;
            let name = d.name.clone().unwrap_or_default();
            let ok = (d.target_type == b'P' && name.is_empty()) || (d.target_type == b'S' && name == stmt_name);
            if !ok {
                return None;
            }
            Some(d.target_type)
        } else {
            None
        };
        Some(Req::Extended { frames: frames.to_vec(), parse_idx, stmt, bind, describe })
    }

    fn bypass(&self, cls: &Classified, reason: &'static str) {
        Stats::inc(&self.sh.stats.queries);
        self.sh.stats.bypass(reason);
        tracing::debug!(target: "pgshadow::query", conn = self.conn, outcome = "bypass", reason, shape = %format!("{:016x}", cls.shape));
    }

    /// Why the session cannot use the cache right now, if it cannot.
    fn session_blocker(&self) -> Result<Arc<SessionFp>, &'static str> {
        if self.open_forward {
            return Err("pipeline in progress");
        }
        let st = self.st.lock();
        if st.pending > 0 {
            return Err("pipeline in progress");
        }
        let fp = match (&st.fp, st.fp_stale) {
            (Some(fp), false) => fp.clone(),
            _ => return Err("session settings changed"),
        };
        drop(st);
        if fp.gen != self.sh.validator.settings_generation() {
            self.st.lock().fp_stale = true;
            return Err("server settings changed");
        }
        if fp.in_recovery {
            return Err("server is a standby");
        }
        if !self.sh.validator.same_server(&fp.database, &fp.server) {
            return Err("not the server and database pgshadow checks");
        }
        let st = self.st.lock();
        if fp.temp_rels > 0 {
            return Err("session has temporary tables");
        }

        match st.status {
            b'I' => {
                if !fp.default_iso.is_some_and(Isolation::is_read_committed) {
                    return Err("isolation level is not READ COMMITTED");
                }
            }
            b'T' => {
                if st.wrote {
                    return Err("transaction has written");
                }
                if !st.iso.is_some_and(Isolation::is_read_committed) {
                    return Err("isolation level is not READ COMMITTED");
                }
            }
            _ => return Err("transaction failed"),
        }
        Ok(fp)
    }

    // ---- the cache path ----

    async fn try_cache(&mut self, req: Req, cls: Arc<Classified>) -> std::io::Result<()> {
        // Earlier requests (pipelined by the client, or the proxy's own settings refresh)
        // must finish first: their effects have to be visible to the check, and Postgres
        // would run them first anyway. Wait for them instead of skipping the cache.
        if !self.open_forward && self.sh.enabled() {
            self.wait_idle().await;
        }
        let read_fx = || {
            let mut fx = UnitEffects::from_effects(&cls.effects);
            fx.parsed = req.parsed();
            fx
        };
        let blocker = if !self.sh.enabled() {
            Err("cache disabled")
        } else if !self.sh.validator.installed() {
            Err("pgshadow not installed or unreachable")
        } else if req.has_time_param() {
            Err("time literal parameter")
        } else if let Some(r) = self.sh.policy_skip(cls.shape) {
            Err(r)
        } else {
            self.session_blocker()
        };
        let fp = match blocker {
            Ok(fp) => fp,
            Err(reason) => {
                self.bypass(&cls, reason);
                let fx = read_fx();
                return self.forward(req.frames(false), true, fx, None, None, Some(cls.shape), Bytes::new()).await;
            }
        };
        Stats::inc(&self.sh.stats.queries);
        let started = Instant::now();
        let request = req.canonical(&fp);
        let key = key_of(&request);

        let mut waited = false;
        loop {
            if let Some(handle) = self.sh.store.get(key, &request).await {
                let entry = Arc::new(handle.value().clone());
                let blocked = if self.sh.rel_disabled(entry.tables.iter().map(|t| t.oid)) {
                    Some("table disabled after verification mismatch")
                } else if req.prepared_deps().is_some_and(|d| d != Some(&dep_set(&entry.tables))) {
                    Some("statement was prepared before its tables last changed")
                } else {
                    None
                };
                if let Some(reason) = blocked {
                    self.bypass(&cls, reason);
                    let fx = read_fx();
                    return self.forward(req.frames(false), true, fx, None, None, Some(cls.shape), Bytes::new()).await;
                }
                // The client's Parse (if any) runs on Postgres alongside the check, so the
                // session's prepared statements stay exactly as without the proxy. A simple
                // query would have destroyed the unnamed statement: do that too.
                let parse_rx = match req.parse_frame() {
                    Some(p) => {
                        let fx = UnitEffects { parsed: req.parsed(), ..Default::default() };
                        Some(self.swallow(vec![p, protocol::sync_msg()], fx).await?)
                    }
                    None => None,
                };
                if matches!(req, Req::Simple { drops_unnamed: true, .. }) {
                    let _ = self.swallow(vec![protocol::close_msg(b'S', ""), protocol::sync_msg()], UnitEffects::default()).await?;
                }
                // Inside a transaction, the query would have locked its tables until the
                // transaction ends (blocking concurrent DDL and TRUNCATE): take the same
                // locks, before the check, so the check also sees anything committed first.
                // Then confirm the transaction has not written (by any route) and is READ
                // COMMITTED, so it sees exactly what the check sees.
                let (lock_rx, txn_rx) = if self.st.lock().status == b'T' {
                    let l = self.swallow(our_query(&lock_sql(&entry.tables), &[]), UnitEffects::default()).await?;
                    let t = self.swallow(our_query(TXN_SQL, &[]), UnitEffects::default()).await?;
                    (Some(l), Some(t))
                } else {
                    (None, None)
                };
                let parse_ok = match parse_rx {
                    Some(rx) => {
                        let frames = rx.await.unwrap_or_default();
                        if frames.iter().any(|f| f.tag == b'E') || frames.is_empty() {
                            // Postgres rejected the Parse: give the client exactly that. (In a
                            // transaction the lock then ran in an aborted transaction and
                            // failed too, which is what the client's Bind would have hit.)
                            if let Some(rx) = lock_rx {
                                let _ = rx.await;
                            }
                            if let Some(rx) = txn_rx {
                                let _ = rx.await;
                            }
                            let mut b = BytesMut::new();
                            for f in &frames {
                                f.encode_into(&mut b);
                            }
                            if frames.last().map(|f| f.tag) != Some(b'Z') {
                                protocol::ready_for_query(self.st.lock().status).encode_into(&mut b);
                            }
                            return self.local(&b).await;
                        }
                        Some(true)
                    }
                    None => None,
                };
                let parse_sent = parse_ok.is_some();
                if let (Some(lrx), Some(trx)) = (lock_rx, txn_rx) {
                    let frames = lrx.await.unwrap_or_default();
                    let txn = trx.await.ok().and_then(|f| parse_txn(&f, 0));
                    if let Some(err) = frames.iter().find(|f| f.tag == b'E') {
                        // Canceled or timed out while waiting for the lock: exactly what the
                        // client's query would have run into.
                        let mut b = BytesMut::new();
                        if parse_sent {
                            Frame::new(b'1', Bytes::new()).encode_into(&mut b);
                        }
                        without_context(err).encode_into(&mut b);
                        protocol::ready_for_query(self.st.lock().status).encode_into(&mut b);
                        return self.local(&b).await;
                    }
                    if frames.is_empty() || frames.iter().any(|f| f.tag == b'N' && notice_is(f, LOCK_FAILED)) {
                        // Any other problem (table gone, no privilege, deadlock): let Postgres
                        // run the query and report it.
                        return self.forward_instead(req, &cls, parse_sent, "could not lock tables").await;
                    }
                    let ok = match txn {
                        Some((iso, wrote)) => {
                            let mut st = self.st.lock();
                            st.iso = iso;
                            st.wrote |= wrote;
                            !wrote && iso.is_some_and(Isolation::is_read_committed)
                        }
                        None => false,
                    };
                    if !ok {
                        return self.forward_instead(req, &cls, parse_sent, "transaction has written or is not READ COMMITTED").await;
                    }
                }
                let verdict = self.sh.validator.check(entry.clone()).await;
                if verdict == Verdict::Valid && fp.gen != self.sh.validator.settings_generation() {
                    // The server's configuration changed (seen by this check): this
                    // session's settings must be read again first.
                    self.st.lock().fp_stale = true;
                    return self.forward_instead(req, &cls, parse_sent, "server settings changed").await;
                }
                if let Verdict::Invalid { meta: true } = verdict {
                    self.st.lock().fp_stale = true;
                }
                if matches!(verdict, Verdict::Valid) && parse_sent {
                    // The statement just prepared reads tables in the validated state.
                    if let Req::Extended { bind, .. } = &req {
                        let name = bind.statement_name.clone().unwrap_or_default();
                        if let Some(info) = self.st.lock().statements.get_mut(&name) {
                            info.deps = Some(dep_set(&entry.tables));
                        }
                    }
                }
                match verdict {
                    Verdict::Valid => {
                        self.sh.record_check(cls.shape, true);
                        let rate = self.sh.cfg.cache.verify_sample_rate;
                        if rate > 0.0 && rand::random::<f64>() < rate {
                            return self.fill(req, cls, fp, key, request, Some(entry), parse_sent, None).await;
                        }
                        let status = self.st.lock().status;
                        let mut b = BytesMut::with_capacity(entry.response.len() + 6);
                        b.extend_from_slice(&entry.response);
                        protocol::ready_for_query(status).encode_into(&mut b);
                        self.local(&b).await?;
                        let stats = &self.sh.stats;
                        Stats::inc(&stats.hits);
                        stats.hit_bytes.fetch_add(entry.response.len() as u64, Ordering::Relaxed);
                        stats.saved_micros.fetch_add(entry.exec_micros, Ordering::Relaxed);
                        tracing::debug!(target: "pgshadow::query", conn = self.conn, outcome = "hit",
                            shape = %format!("{:016x}", cls.shape), micros = started.elapsed().as_micros() as u64,
                            postgres_micros = entry.exec_micros, bytes = entry.response.len());
                        return Ok(());
                    }
                    Verdict::Invalid { meta } => {
                        Stats::inc(&self.sh.stats.invalidated);
                        self.sh.record_check(cls.shape, false);
                        self.sh.store.remove(key);
                        if meta {
                            // A table changed (not just its data): re-read table state.
                            self.sh.validator.forget_table_states();
                        }
                    }
                    Verdict::Failed => Stats::inc(&self.sh.stats.check_failed),
                }
                let guard = match self.sh.claim(key) {
                    Inflight::Lead(g) => Some(g),
                    Inflight::Wait(_) => None,
                };
                return self.fill(req, cls, fp, key, request, None, parse_sent, guard).await;
            }
            // Miss. Either fill it, or -- if another session is already running this exact
            // query -- wait for its result (which will still be validated) instead.
            match self.sh.claim(key) {
                Inflight::Lead(guard) => {
                    return self.fill(req, cls, fp, key, request, None, false, Some(guard)).await;
                }
                Inflight::Wait(mut rx) if !waited => {
                    let budget = self.sh.cfg.check.timeout.max(Duration::from_secs(5));
                    let _ = tokio::time::timeout(budget, rx.wait_for(|done| *done)).await;
                    waited = true;
                }
                Inflight::Wait(_) => break,
            }
        }
        self.fill(req, cls, fp, key, request, None, false, None).await
    }

    /// Wait (bounded) until no upstream unit is in flight.
    fn wait_idle(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let (idle, st) = (self.idle.clone(), self.st.clone());
        async move {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let notified = idle.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if st.lock().pending == 0 || Instant::now() >= deadline {
                    break;
                }
                let _ = tokio::time::timeout_at(deadline.into(), notified).await;
            }
        }
    }

    /// Let Postgres answer after all (the client's Parse, if `parse_sent`, already ran).
    async fn forward_instead(&mut self, req: Req, cls: &Classified, parse_sent: bool, reason: &'static str) -> std::io::Result<()> {
        self.bypass(cls, reason);
        let mut fx = UnitEffects::from_effects(&cls.effects);
        let prefix = if parse_sent {
            parse_complete()
        } else {
            fx.parsed = req.parsed();
            Bytes::new()
        };
        self.forward(req.frames(parse_sent), true, fx, None, None, Some(cls.shape), prefix).await
    }

    /// Run the request on Postgres, stream the result to the client, and store it.
    /// With `compare`, the fresh result is compared against a cached one instead.
    #[allow(clippy::too_many_arguments)]
    async fn fill(
        &mut self,
        req: Req,
        cls: Arc<Classified>,
        fp: Arc<SessionFp>,
        key: u128,
        request: Vec<u8>,
        compare: Option<Arc<Entry>>,
        parse_sent: bool,
        guard: Option<InflightGuard>,
    ) -> std::io::Result<()> {
        // If the Parse already ran (hit path that fell through), send the rest and put
        // ParseComplete back in front, so the client and the cached bytes see exactly
        // what Postgres would have sent for the whole batch.
        let prefix = if parse_sent { parse_complete() } else { Bytes::new() };
        let mut fx = UnitEffects::from_effects(&cls.effects);

        // 1. State of every table the query names, read before the query runs (from the
        //    proxy's own connection, briefly cached). Anything that changes afterwards
        //    changes the fingerprint and fails validation, so earlier is conservative.
        let rels: Vec<RelName> = cls.cacheable.as_ref().map(|c| c.relations.clone()).unwrap_or_default();
        let lookup: Vec<(Option<String>, String)> = rels.iter().map(|r| (r.schema.clone(), r.name.clone())).collect();
        let deps = match self.sh.validator.table_states(&fp.path, &fp.current_user, &lookup).await {
            Some(states) => table_deps(&rels, &states),
            None => Err("could not read table state"),
        };
        let deps = match deps {
            Ok(d) if self.sh.rel_disabled(d.iter().map(|t| t.oid)) => Err("table disabled after verification mismatch"),
            Ok(d) if req.prepared_deps().is_some_and(|p| p != Some(&dep_set(&d))) => {
                Err("statement was prepared before its tables last changed")
            }
            other => other,
        };
        let c = cls.cacheable.as_ref().expect("cacheable");
        let deps = match deps {
            Ok(d) if !c.clock_columns.is_empty() => {
                let oids: Vec<u32> = d.iter().map(|t| t.oid).collect();
                match self.sh.validator.clock_columns(&oids, &c.clock_columns).await {
                    Some(false) => Ok(d),
                    Some(true) => Err("casts a non-date/time column to a date/time type"),
                    None => Err("could not check column types"),
                }
            }
            other => other,
        };
        // 2. Snapshot, catalog epoch and settings, from the proxy's connection, before the
        //    query is sent; and that no name the query uses is taken over by a user object.
        let ready = match deps {
            Ok(d) => match self.sh.validator.fill_context().await {
                None => Err("could not take snapshot (or server is a standby)"),
                Some(_) if fp.gen != self.sh.validator.settings_generation() => {
                    self.st.lock().fp_stale = true;
                    Err("server settings changed")
                }
                Some(ctx) => {
                    match self.sh.validator.any_shadowed(&ctx.epoch, &c.functions, &c.operators, &c.types).await {
                        Some(false) => Ok((d, ctx)),
                        Some(true) => Err("uses a name a user-defined function, operator or type shares"),
                        None => Err("could not check function and operator names"),
                    }
                }
            },
            Err(e) => Err(e),
        };
        let (deps, ctx) = match ready {
            Ok(x) => x,
            Err(reason) => {
                // Not cacheable after all: just run it.
                self.sh.stats.bypass(reason);
                tracing::debug!(target: "pgshadow::query", conn = self.conn, outcome = "bypass", reason,
                    shape = %format!("{:016x}", cls.shape));
                drop(guard);
                if !parse_sent {
                    fx.parsed = req.parsed();
                }
                return self.forward(req.frames(parse_sent), true, fx, None, None, Some(cls.shape), prefix).await;
            }
        };
        if !parse_sent {
            fx.parsed = req.parsed_with(Some(dep_set(&deps)));
        } else if let Req::Extended { bind, .. } = &req {
            // The Parse already ran (hit path that fell through).
            let name = bind.statement_name.clone().unwrap_or_default();
            if let Some(info) = self.st.lock().statements.get_mut(&name) {
                info.deps = Some(dep_set(&deps));
            }
        }
        // 3. Inside a transaction: whether it has written (by any route) and its isolation
        //    level, asked right before the query (pipelined: no added latency).
        let txn_rx = if self.st.lock().status == b'T' {
            Some(self.swallow(our_query(TXN_SQL, &[]), UnitEffects::default()).await?)
        } else {
            None
        };

        let mut capture = Capture { buf: BytesMut::new(), limit: self.sh.cfg.cache.max_entry_size, clean: true };
        capture.buf.extend_from_slice(&prefix);
        let (done_tx, done_rx) = oneshot::channel();
        self.forward(req.frames(parse_sent), true, fx, Some(capture), Some(done_tx), Some(cls.shape), prefix).await?;

        let sh = self.sh.clone();
        let st = self.st.clone();
        let conn = self.conn;
        tokio::spawn(async move {
            let txn_ok = match txn_rx {
                None => true,
                Some(rx) => match rx.await.ok().and_then(|f| parse_txn(&f, 0)) {
                    Some((iso, wrote)) => {
                        let mut st = st.lock();
                        st.wrote |= wrote;
                        !wrote && iso.is_some_and(Isolation::is_read_committed)
                    }
                    None => false,
                },
            };
            let done = done_rx.await.ok();
            let outcome = if txn_ok {
                finish_fill(&sh, deps, &fp, ctx, done, request, key, compare, &cls)
            } else {
                sh.stats.bypass("transaction has written or is not READ COMMITTED");
                ("miss", "transaction has written", done.map_or(0, |d| d.elapsed.as_micros() as u64))
            };
            tracing::debug!(target: "pgshadow::query", conn, outcome = outcome.0, reason = outcome.1,
                shape = %format!("{:016x}", cls.shape), postgres_micros = outcome.2);
            drop(guard); // releases sessions waiting for this result
        });
        Ok(())
    }
}

/// State of the tables a cacheable statement reads, for a session with these settings.
async fn prepared_deps(sh: &Arc<Shared>, fp: Option<Arc<SessionFp>>, sql: &str) -> Option<Arc<Vec<(u32, String)>>> {
    let fp = fp?;
    if !sh.enabled() || !sh.validator.installed() {
        return None;
    }
    let cls = sh.classifier.get(sql);
    let rels = &cls.cacheable.as_ref()?.relations;
    let lookup: Vec<(Option<String>, String)> = rels.iter().map(|r| (r.schema.clone(), r.name.clone())).collect();
    let states = sh.validator.table_states(&fp.path, &fp.current_user, &lookup).await?;
    table_deps(rels, &states).ok().map(|d| dep_set(&d))
}

/// The tables a result depends on, or why it cannot be cached.
fn table_deps(rels: &[RelName], states: &[Option<crate::validator::TableState>]) -> Result<Vec<TableDep>, &'static str> {
    let mut deps = Vec::new();
    for (rel, st) in rels.iter().zip(states) {
        match st {
            None if rel.maybe_cte => continue, // a CTE, not a table
            None => return Err("relation not found"),
            Some(st) if !st.eligible => return Err("table not enabled for caching"),
            Some(st) => deps.push(TableDep {
                oid: st.oid,
                fp: st.fp.clone(),
                nsp: rel.schema.clone(),
                name: rel.name.clone(),
                lock_name: st.qualified.clone(),
            }),
        }
    }
    if deps.is_empty() {
        return Err("reads no tables");
    }
    Ok(deps)
}

/// Decide whether a fresh result can be stored (or, in verify mode, whether it matches).
/// Returns (outcome, reason, postgres micros) for the log line.
#[allow(clippy::too_many_arguments)]
fn finish_fill(
    sh: &Shared,
    tables: Vec<TableDep>,
    fp: &SessionFp,
    ctx: crate::validator::FillContext,
    done: Option<Done>,
    request: Vec<u8>,
    key: u128,
    compare: Option<Arc<Entry>>,
    cls: &Classified,
) -> (&'static str, &'static str, u64) {
    let Some(done) = done else { return ("miss", "connection closed", 0) };
    let micros = done.elapsed.as_micros() as u64;
    let Some(response) = done.response else {
        sh.stats.bypass("result not cacheable (error, notice, or too large)");
        return ("miss", "result not cacheable", micros);
    };
    let entry = Entry {
        request: Bytes::from(request),
        response,
        snapshot: ctx.snapshot,
        epoch: ctx.epoch,
        settings: ctx.settings,
        tables,
        role: fp.current_user.clone(),
        path: fp.path.clone(),
        funcs: cls.cacheable.as_ref().map(|c| c.functions.join(",")).unwrap_or_default(),
        created_unix_ms: now_unix_ms(),
        exec_micros: micros,
    };
    match compare {
        None => {
            sh.store.insert(key, entry);
            Stats::inc(&sh.stats.fills);
            ("fill", "", micros)
        }
        Some(cached) => {
            if same_result(&cached.response, &entry.response) {
                Stats::inc(&sh.stats.verify_ok);
                ("verify", "match", micros)
            } else {
                Stats::inc(&sh.stats.verify_mismatch);
                sh.store.remove(key);
                let oids: Vec<u32> = entry.tables.iter().map(|t| t.oid).collect();
                sh.disable_rels(oids.clone(), Duration::from_secs(3600));
                tracing::error!(
                    shape = %format!("{:016x}", cls.shape), tables = ?oids,
                    "VERIFY MISMATCH: a validated cached result differed from Postgres. \
                     Caching is disabled for these tables for 1 hour. Please report this."
                );
                ("verify", "mismatch", micros)
            }
        }
    }
}

/// Same messages, with data rows compared as a multiset (row order without ORDER BY
/// is not guaranteed by Postgres either).
fn same_result(a: &[u8], b: &[u8]) -> bool {
    fn split(mut x: &[u8]) -> Option<(Vec<&[u8]>, Vec<&[u8]>)> {
        let (mut other, mut rows) = (Vec::new(), Vec::new());
        while !x.is_empty() {
            if x.len() < 5 {
                return None;
            }
            let len = i32::from_be_bytes([x[1], x[2], x[3], x[4]]) as usize + 1;
            if len > x.len() {
                return None;
            }
            let (m, rest) = x.split_at(len);
            if m[0] == b'D' {
                rows.push(m);
            } else {
                other.push(m);
            }
            x = rest;
        }
        rows.sort_unstable();
        Some((other, rows))
    }
    matches!((split(a), split(b)), (Some(x), Some(y)) if x == y)
}

fn parse_complete() -> Bytes {
    let mut b = BytesMut::new();
    Frame::new(b'1', Bytes::new()).encode_into(&mut b);
    b.freeze()
}

const LOCK_FAILED: &str = "pgshadow-lock-failed";

/// Lock a cached result's tables the way reading them would. Problems other than a
/// cancel or timeout (which the client's query would have hit too) become a notice, so
/// the transaction stays usable and the query can run on Postgres to report them.
fn lock_sql(tables: &[TableDep]) -> String {
    let mut names: Vec<&str> = tables.iter().map(|t| t.lock_name.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    format!(
        "DO $pgshadow$ BEGIN LOCK TABLE {} IN ACCESS SHARE MODE; \
         EXCEPTION WHEN OTHERS THEN RAISE NOTICE '{LOCK_FAILED}'; END $pgshadow$",
        names.join(", ")
    )
}

/// An error without its CONTEXT field (which would name the proxy's DO block).
fn without_context(f: &Frame) -> Frame {
    let body = &f.body;
    let mut out = BytesMut::new();
    let mut i = 0;
    while i < body.len() && body[i] != 0 {
        let end = body[i + 1..].iter().position(|b| *b == 0).map(|p| i + 1 + p).unwrap_or(body.len());
        if body[i] != b'W' {
            out.extend_from_slice(&body[i..(end + 1).min(body.len())]);
        }
        i = end + 1;
    }
    out.extend_from_slice(&[0]);
    Frame::new(f.tag, out.freeze())
}

fn notice_is(f: &Frame, msg: &str) -> bool {
    protocol::error_fields(&f.body).1 == msg
}

trait ReasonExt {
    fn bypass_reason_or_effect(&self) -> &'static str;
}

impl ReasonExt for Classified {
    fn bypass_reason_or_effect(&self) -> &'static str {
        if self.is_read_only() {
            if self.bypass_reason.is_empty() {
                "not cacheable"
            } else {
                self.bypass_reason
            }
        } else {
            "write or session change"
        }
    }
}

// ---- tasks -----------------------------------------------------------------------

async fn upstream_reader(
    mut rd: FrameReader<ReadHalf<Stream>>,
    mut units: mpsc::UnboundedReceiver<Up>,
    client_w: ClientW,
    st: Arc<Mutex<State>>,
    sh: Arc<Shared>,
    idle: Arc<tokio::sync::Notify>,
) -> std::io::Result<()> {
    let mut cur: Option<Up> = None;
    let mut had_error = false;
    let mut saw_param_status = false;
    // Responses for the client are batched: everything already received goes out in one
    // send, instead of one per message.
    let mut batch = BytesMut::new();
    // When the previous unit finished: a unit's own time starts no earlier than this, so
    // timings exclude waiting behind earlier pipelined requests.
    let mut prev_end = Instant::now();
    loop {
        if !batch.is_empty() && (!rd.has_complete_frame() || batch.len() >= 64 * 1024) {
            write_client(&client_w, &batch.split()).await?;
        }
        let Some(f) = rd.read_frame().await? else {
            if !batch.is_empty() {
                let _ = write_client(&client_w, &batch).await;
            }
            return Ok(());
        };
        if cur.is_none() {
            match units.try_recv() {
                Ok(u) => {
                    if let Up::Forward { prefix, .. } = &u {
                        batch.extend_from_slice(prefix);
                    }
                    cur = Some(u);
                    had_error = false;
                    saw_param_status = false;
                }
                Err(_) => {
                    // Between units: asynchronous messages (NOTIFY, notices, parameter
                    // changes, or a FATAL error before Postgres closes the connection).
                    if f.tag == b'S' {
                        st.lock().fp_stale = true;
                    }
                    f.encode_into(&mut batch);
                    continue;
                }
            }
        }
        let unit = cur.as_mut().expect("current unit");
        match f.tag {
            b'E' => had_error = true,
            b'S' => saw_param_status = true,
            _ => {}
        }
        match unit {
            Up::Forward { capture, .. } => {
                if f.tag != b'Z' {
                    if let Some(c) = capture.as_mut() {
                        c.push(&f);
                    }
                }
                f.encode_into(&mut batch);
            }
            Up::Swallow { frames, .. } => match f.tag {
                // asynchronous: the client's
                b'A' | b'S' => f.encode_into(&mut batch),
                _ => frames.push(f.clone()),
            },
        }
        if f.tag != b'Z' {
            continue;
        }
        let status = f.body.first().copied().unwrap_or(b'I');
        let unit = cur.take().expect("current unit");
        let fx = match &unit {
            Up::Forward { fx, .. } | Up::Swallow { fx, .. } => fx.clone(),
        };
        {
            let mut s = st.lock();
            let prev = s.status;
            s.status = status;
            s.pending = s.pending.saturating_sub(1);
            if status == b'I' {
                s.wrote = false;
                s.iso = None;
            } else if prev == b'I' || fx.begin.is_some() || fx.txn_end {
                // A transaction started in this unit (possibly after others ended in it,
                // as in "COMMIT; BEGIN ISOLATION LEVEL ...").
                s.wrote = false;
                s.iso = match fx.begin {
                    Some(Some(level)) => Some(level),
                    Some(None) => s.fp.as_ref().and_then(|f| f.default_iso),
                    None => None,
                };
            }
            if fx.writes && status != b'I' {
                s.wrote = true;
            }
            if fx.settings || fx.dirty || saw_param_status {
                s.fp_stale = true;
            }
            for (name, info) in fx.parsed {
                if had_error {
                    s.statements.remove(&name);
                } else {
                    s.statements.insert(name, info);
                }
            }
        }
        match unit {
            Up::Forward { capture, done, started, timing_key, .. } => {
                if !batch.is_empty() {
                    write_client(&client_w, &batch.split()).await?;
                }
                let elapsed = started.max(prev_end).elapsed();
                if let Some(k) = timing_key {
                    if !had_error {
                        sh.record_time(k, elapsed);
                    }
                }
                if let Some(done) = done {
                    let response = capture.filter(|c| c.clean && !had_error && status != b'E').map(|c| c.buf.freeze());
                    let _ = done.send(Done { response, elapsed });
                }
            }
            Up::Swallow { frames, kind, .. } => match kind {
                SwallowKind::Frames(tx) => {
                    let _ = tx.send(frames);
                }
                SwallowKind::Fingerprint => {
                    let parsed = parse_fp(&frames);
                    let txn = parse_txn(&frames, 9);
                    let mut s = st.lock();
                    s.fp_refreshing = false;
                    match parsed {
                        Some(mut fp) => {
                            fp.gen = s.fp_refresh_gen;
                            if s.status == b'T' {
                                // What Postgres says about the transaction beats what the
                                // proxy inferred from the statements it saw.
                                match txn {
                                    Some((iso, wrote)) => {
                                        s.iso = iso;
                                        s.wrote |= wrote;
                                    }
                                    None => s.wrote = true,
                                }
                            }
                            s.fp = Some(Arc::new(fp));
                            s.fp_stale = false;
                        }
                        None => {
                            s.fp = None;
                            s.fp_stale = true;
                            tracing::debug!("could not read session settings; caching off for this session until they change");
                        }
                    }
                }
            },
        }
        prev_end = Instant::now();
        if st.lock().pending == 0 {
            idle.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn result_comparison_ignores_row_order_only() {
        let row = |v: &[u8]| {
            let mut b = BytesMut::new();
            let mut body = BytesMut::new();
            body.put_i16(1);
            body.put_i32(v.len() as i32);
            body.extend_from_slice(v);
            Frame::new(b'D', body.freeze()).encode_into(&mut b);
            b
        };
        let cc = |tag: &str| {
            let mut b = BytesMut::new();
            Frame::new(b'C', Bytes::from(format!("{tag}\0"))).encode_into(&mut b);
            b
        };
        let mk = |parts: Vec<BytesMut>| parts.into_iter().fold(BytesMut::new(), |mut a, p| {
            a.extend_from_slice(&p);
            a
        });
        let a = mk(vec![row(b"1"), row(b"2"), cc("SELECT 2")]);
        let b = mk(vec![row(b"2"), row(b"1"), cc("SELECT 2")]);
        let c = mk(vec![row(b"2"), row(b"3"), cc("SELECT 2")]);
        assert!(same_result(&a, &b));
        assert!(!same_result(&a, &c));
    }

    #[test]
    fn fingerprint_parsing() {
        let mut body = BytesMut::new();
        let cols = ["app", "app", "db", "pg_catalog,public", "0", "read committed", "abc", "1760561538.06", "false", "true", "repeatable read"];
        body.put_i16(cols.len() as i16);
        for c in cols {
            body.put_i32(c.len() as i32);
            body.extend_from_slice(c.as_bytes());
        }
        let frames = vec![Frame::new(b'D', body.freeze()), Frame::new(b'C', Bytes::from_static(b"SELECT 1\0"))];
        let fp = parse_fp(&frames).unwrap();
        assert_eq!(fp.current_user, "app");
        assert_eq!(fp.path, "pg_catalog,public");
        assert_eq!(fp.temp_rels, 0);
        assert_eq!(fp.default_iso, Some(Isolation::ReadCommitted));
        assert_eq!(fp.server, "1760561538.06");
        assert!(!fp.in_recovery);
        assert_eq!(parse_txn(&frames, 9), Some((Some(Isolation::RepeatableRead), true)));
        assert!(parse_fp(&[protocol::error_frame("ERROR", "42501", "denied")]).is_none());
    }
}
