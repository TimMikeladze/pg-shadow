//! One client connection.
//!
//! Each client session is paired with its own upstream Postgres session (like
//! PgBouncer's session mode), so transactions, prepared statements, temp tables, LISTEN,
//! COPY and session settings behave exactly as without the proxy. Three tasks:
//!
//! - the client reader (this module's main loop) decides, per request, whether to
//!   serve it from cache, fill the cache from it, or just forward it;
//! - the upstream reader routes Postgres's responses: to the client, into a capture
//!   buffer for the cache, or to the proxy itself for queries it injected;
//! - the client writer emits responses strictly in request order.
//!
//! Responses from Postgres are split into "units", each ending with ReadyForQuery.
//! A request is only considered for the cache when no earlier unit is still in flight,
//! so served results always reflect the client's own earlier statements, and statements
//! are never reordered.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
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
}

/// What this session's settings look like, as far as results are concerned.
#[derive(Debug)]
struct SessionFp {
    hash: u128,
    current_user: String,
    /// current_schemas(true), comma-separated.
    schemas: String,
    temp_rels: i64,
    default_iso: Option<Isolation>,
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
    /// Prepared statements Postgres has confirmed.
    statements: HashMap<String, StmtInfo>,
}

/// Effects of one upstream unit, applied in order when its ReadyForQuery arrives.
#[derive(Default, Clone, Debug)]
struct UnitEffects {
    writes: bool,
    /// BEGIN seen: explicit isolation level, or None for the session default.
    begin: Option<Option<Isolation>>,
    settings: bool,
    parsed: Vec<(String, StmtInfo)>,
}

impl UnitEffects {
    fn from_effects(effects: &[Effect]) -> Self {
        let mut fx = UnitEffects::default();
        for e in effects {
            match e {
                Effect::Read | Effect::TxnControl | Effect::Deallocate(_) => {}
                Effect::Begin(iso) => fx.begin = Some(*iso),
                Effect::Write => fx.writes = true,
                Effect::DiscardAll | Effect::Other => {
                    fx.writes = true;
                    fx.settings = true;
                }
            }
        }
        fx
    }

    /// For units whose contents the proxy cannot fully see.
    fn unknown() -> Self {
        UnitEffects { writes: true, begin: None, settings: true, parsed: vec![] }
    }

    fn merge(&mut self, o: UnitEffects) {
        self.writes |= o.writes;
        self.settings |= o.settings;
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
        out: mpsc::Sender<Bytes>,
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

enum Out {
    Stream { prefix: Bytes, rx: mpsc::Receiver<Bytes> },
    Local(Bytes),
}

// ---- requests ------------------------------------------------------------------

enum Req {
    Simple {
        frame: Frame,
        sql: String,
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
        match self {
            Req::Extended { parse_idx: Some(_), stmt, bind, .. } => {
                vec![(bind.statement_name.clone().unwrap_or_default(), stmt.clone())]
            }
            _ => vec![],
        }
    }

    /// Parameters that Postgres would turn into the current time ('now', 'today', ...).
    /// Checked in every format: the binary form of a text value is the same bytes.
    fn has_time_param(&self) -> bool {
        let Req::Extended { bind, .. } = self else { return false };
        bind.parameters.iter().any(|p| {
            p.as_ref().is_some_and(|v| v.len() <= 16 && crate::classify::is_time_word(&String::from_utf8_lossy(v)))
        })
    }
}

fn put_bytes(b: &mut Vec<u8>, v: &[u8]) {
    b.extend_from_slice(&(v.len() as u64).to_le_bytes());
    b.extend_from_slice(v);
}

// ---- the session ---------------------------------------------------------------

const OUR_STMT: &str = "__pgshadow_q";
const OUR_PORTAL: &str = "__pgshadow_p";

const FP_SQL: &str = "SELECT current_user::text, session_user::text, pg_catalog.current_database()::text, \
     pg_catalog.array_to_string(pg_catalog.current_schemas(true), ','), \
     (SELECT pg_catalog.count(*) FROM pg_catalog.pg_class c \
       WHERE c.relnamespace OPERATOR(pg_catalog.=) pg_catalog.pg_my_temp_schema())::text, \
     pg_catalog.current_setting('default_transaction_isolation'), \
     (SELECT pg_catalog.string_agg(s.name OPERATOR(pg_catalog.||) '=' OPERATOR(pg_catalog.||) s.setting, \
             pg_catalog.chr(10) ORDER BY s.name) \
        FROM pg_catalog.pg_settings s \
       WHERE s.source NOT IN ('default', 'override') \
         AND s.name NOT IN ('application_name', 'statement_timeout', 'lock_timeout', \
             'idle_in_transaction_session_timeout', 'idle_session_timeout', 'transaction_timeout', \
             'client_min_messages', 'work_mem', 'maintenance_work_mem', 'temp_buffers', \
             'default_transaction_read_only', 'transaction_read_only', 'tcp_keepalives_idle', \
             'tcp_keepalives_interval', 'tcp_keepalives_count', 'tcp_user_timeout'))";

const PREP_SQL: &str = "SELECT snap, ord, rel, eligible, fp, reason FROM pgshadow.prep($1::text[], $2::text[])";

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
    let mut all = String::new();
    for i in 0..7 {
        all.push_str(&col(i));
        all.push('\u{1}');
    }
    Some(SessionFp {
        hash: xxhash_rust::xxh3::xxh3_128(all.as_bytes()),
        current_user: col(0),
        schemas: col(3),
        temp_rels: col(4).parse().unwrap_or(1),
        default_iso: Isolation::parse(&col(5)),
    })
}

pub struct Session {
    sh: Arc<Shared>,
    st: Arc<Mutex<State>>,
    /// Signalled when no upstream units are in flight.
    idle: Arc<tokio::sync::Notify>,
    up_w: WriteHalf<Stream>,
    up_units: mpsc::UnboundedSender<Up>,
    out: mpsc::UnboundedSender<Out>,
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
        statements: HashMap::new(),
    }));
    let (out_tx, out_rx) = mpsc::unbounded_channel();
    let (async_tx, async_rx) = mpsc::unbounded_channel();
    let (up_tx, up_rx) = mpsc::unbounded_channel();
    let writer = tokio::spawn(client_writer(client_w, out_rx, async_rx));
    let (closed_tx, mut closed_rx) = watch::channel(false);
    let idle = Arc::new(tokio::sync::Notify::new());
    let reader = tokio::spawn({
        let st = st.clone();
        let sh = sh.clone();
        let idle = idle.clone();
        async move {
            if let Err(e) = upstream_reader(upstream_rd, up_rx, async_tx, st, sh, idle).await {
                tracing::debug!(conn, error = %e, "upstream read ended");
            }
            let _ = closed_tx.send(true);
        }
    });
    let mut s = Session { sh, st, idle, up_w, up_units: up_tx, out: out_tx, open_forward: false, conn };
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
    let _ = writer.await;
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
            let (tx, rx) = mpsc::channel(128);
            let idle = self.st.lock().pending == 0;
            let _ = self.out.send(Out::Stream { prefix, rx });
            self.push_unit(Up::Forward {
                out: tx,
                fx,
                capture,
                done,
                started: Instant::now(),
                timing_key: if idle { timing_key } else { None },
            });
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

    fn local(&self, bytes: Bytes) {
        let _ = self.out.send(Out::Local(bytes));
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
    /// client's next request (never inside a transaction), and not waited for.
    async fn refresh_fingerprint_if_needed(&mut self) -> std::io::Result<()> {
        if !self.sh.enabled() || !self.sh.validator.installed() || self.open_forward {
            return Ok(());
        }
        {
            let mut st = self.st.lock();
            if !(st.fp_stale && !st.fp_refreshing && st.status == b'I' && st.pending == 0) {
                return Ok(());
            }
            st.fp_refreshing = true;
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
        if cls.cacheable.is_some() {
            let req = Req::Simple { frame, sql: q.query };
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
        for f in &frames {
            match f.tag {
                b'P' => match f.decode::<protocol::Parse>() {
                    Some(p) => {
                        let info = StmtInfo { sql: Arc::from(p.query.as_str()), param_oids: Arc::from(p.type_oids.as_slice()) };
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
                StmtInfo { sql: Arc::from(p.query.as_str()), param_oids: Arc::from(p.type_oids.as_slice()) }
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
            let idle = self.idle.clone();
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let notified = idle.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.st.lock().pending == 0 || Instant::now() >= deadline {
                    break;
                }
                let _ = tokio::time::timeout_at(deadline.into(), notified).await;
            }
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
                if self.sh.rel_disabled(entry.tables.iter().map(|t| t.oid)) {
                    self.bypass(&cls, "table disabled after verification mismatch");
                    let fx = read_fx();
                    return self.forward(req.frames(false), true, fx, None, None, Some(cls.shape), Bytes::new()).await;
                }
                // The client's Parse (if any) runs on Postgres alongside the check, so the
                // session's prepared statements stay exactly as without the proxy.
                let parse_rx = match req.parse_frame() {
                    Some(p) => {
                        let fx = UnitEffects { parsed: req.parsed(), ..Default::default() };
                        Some(self.swallow(vec![p, protocol::sync_msg()], fx).await?)
                    }
                    None => None,
                };
                let verdict = self.sh.validator.check(entry.clone()).await;
                let parse_ok = match parse_rx {
                    Some(rx) => {
                        let frames = rx.await.unwrap_or_default();
                        if frames.iter().any(|f| f.tag == b'E') || frames.is_empty() {
                            // Postgres rejected the Parse: give the client exactly that.
                            let mut b = BytesMut::new();
                            for f in &frames {
                                f.encode_into(&mut b);
                            }
                            if frames.last().map(|f| f.tag) != Some(b'Z') {
                                protocol::ready_for_query(self.st.lock().status).encode_into(&mut b);
                            }
                            self.local(b.freeze());
                            return Ok(());
                        }
                        Some(true)
                    }
                    None => None,
                };
                let parse_sent = parse_ok.is_some();
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
                        self.local(b.freeze());
                        let stats = &self.sh.stats;
                        Stats::inc(&stats.hits);
                        stats.hit_bytes.fetch_add(entry.response.len() as u64, Ordering::Relaxed);
                        stats.saved_micros.fetch_add(entry.exec_micros, Ordering::Relaxed);
                        tracing::debug!(target: "pgshadow::query", conn = self.conn, outcome = "hit",
                            shape = %format!("{:016x}", cls.shape), micros = started.elapsed().as_micros() as u64,
                            postgres_micros = entry.exec_micros, bytes = entry.response.len());
                        return Ok(());
                    }
                    Verdict::Invalid => {
                        Stats::inc(&self.sh.stats.invalidated);
                        self.sh.record_check(cls.shape, false);
                        self.sh.store.remove(key);
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
        let rels: Vec<RelName> = cls.cacheable.as_ref().map(|c| c.relations.clone()).unwrap_or_default();
        let schemas: Vec<Option<&str>> = rels.iter().map(|r| r.schema.as_deref()).collect();
        let names: Vec<Option<&str>> = rels.iter().map(|r| Some(r.name.as_str())).collect();
        let a1 = protocol::text_array(&schemas);
        let a2 = protocol::text_array(&names);
        let prep_rx = self.swallow(our_query(PREP_SQL, &[Some(&a1), Some(&a2)]), UnitEffects::default()).await?;

        // If the Parse already ran (hit path that fell through), send the rest and put
        // ParseComplete back in front, so the client and the cached bytes see exactly
        // what Postgres would have sent for the whole batch.
        let prefix = if parse_sent {
            let mut b = BytesMut::new();
            Frame::new(b'1', Bytes::new()).encode_into(&mut b);
            b.freeze()
        } else {
            Bytes::new()
        };
        let mut capture = Capture { buf: BytesMut::new(), limit: self.sh.cfg.cache.max_entry_size, clean: true };
        capture.buf.extend_from_slice(&prefix);
        let (done_tx, done_rx) = oneshot::channel();
        let mut fx = UnitEffects::from_effects(&cls.effects);
        if !parse_sent {
            fx.parsed = req.parsed();
        }

        self.forward(req.frames(parse_sent), true, fx, Some(capture), Some(done_tx), Some(cls.shape), prefix).await?;

        let sh = self.sh.clone();
        let conn = self.conn;
        tokio::spawn(async move {
            let prep = prep_rx.await.ok().and_then(|f| rows_of(&f));
            let done = done_rx.await.ok();
            let outcome = finish_fill(&sh, &rels, &fp, prep, done, request, key, compare, &cls);
            tracing::debug!(target: "pgshadow::query", conn, outcome = outcome.0, reason = outcome.1,
                shape = %format!("{:016x}", cls.shape), postgres_micros = outcome.2);
            drop(guard); // releases sessions waiting for this result
        });
        Ok(())
    }
}

/// Decide whether a fresh result can be stored (or, in verify mode, whether it matches).
/// Returns (outcome, reason, postgres micros) for the log line.
#[allow(clippy::too_many_arguments)]
fn finish_fill(
    sh: &Shared,
    rels: &[RelName],
    fp: &SessionFp,
    prep: Option<Vec<Vec<Option<String>>>>,
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
    let Some(rows) = prep else {
        sh.stats.bypass("could not read table state");
        return ("miss", "could not read table state", micros);
    };
    // One row per relation, in order; ord 0 means prep caught an error.
    if rows.len() != rels.len() || rows.iter().any(|r| r.get(1).cloned().flatten().as_deref() == Some("0")) {
        sh.stats.bypass("could not read table state");
        return ("miss", "could not read table state", micros);
    }
    let snapshot = rows[0].first().cloned().flatten().unwrap_or_default();
    let mut tables = Vec::new();
    for (rel, row) in rels.iter().zip(&rows) {
        let oid: Option<u32> = row.get(2).cloned().flatten().and_then(|s| s.parse().ok());
        let eligible = row.get(3).cloned().flatten().as_deref() == Some("t");
        let tfp = row.get(4).cloned().flatten();
        match oid {
            None if rel.maybe_cte => continue, // a CTE, not a table
            None => {
                sh.stats.bypass("relation not found");
                return ("miss", "relation not found", micros);
            }
            Some(oid) => {
                if !eligible {
                    sh.stats.bypass("table not enabled for caching");
                    return ("miss", "table not enabled for caching", micros);
                }
                let Some(tfp) = tfp else { return ("miss", "could not read table state", micros) };
                let resolve = rel.schema.is_none().then(|| (rel.name.clone(), fp.schemas.clone()));
                tables.push(TableDep { oid, fp: tfp, resolve });
            }
        }
    }
    if tables.is_empty() || snapshot.is_empty() {
        return ("miss", "reads no tables", micros);
    }
    if sh.rel_disabled(tables.iter().map(|t| t.oid)) {
        return ("miss", "table disabled after verification mismatch", micros);
    }
    let entry = Entry {
        request: Bytes::from(request),
        response,
        snapshot,
        tables,
        role: fp.current_user.clone(),
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
    async_tx: mpsc::UnboundedSender<Bytes>,
    st: Arc<Mutex<State>>,
    sh: Arc<Shared>,
    idle: Arc<tokio::sync::Notify>,
) -> std::io::Result<()> {
    let mut cur: Option<Up> = None;
    let mut had_error = false;
    let mut saw_param_status = false;
    loop {
        let Some(f) = rd.read_frame().await? else { return Ok(()) };
        if cur.is_none() {
            match units.try_recv() {
                Ok(u) => {
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
                    let _ = async_tx.send(f.to_bytes());
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
            Up::Forward { out, capture, .. } => {
                if f.tag != b'Z' {
                    if let Some(c) = capture.as_mut() {
                        c.push(&f);
                    }
                }
                if out.send(f.to_bytes()).await.is_err() {
                    return Ok(()); // client gone
                }
            }
            Up::Swallow { frames, .. } => match f.tag {
                b'A' | b'S' => {
                    let _ = async_tx.send(f.to_bytes());
                }
                b'N' => {}
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
            } else if prev == b'I' {
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
            if fx.settings || saw_param_status {
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
            Up::Forward { out, capture, done, started, timing_key, .. } => {
                drop(out);
                let elapsed = started.elapsed();
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
                    let mut s = st.lock();
                    s.fp_refreshing = false;
                    match parsed {
                        Some(fp) => {
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
        if st.lock().pending == 0 {
            idle.notify_waiters();
        }
    }
}

async fn client_writer(
    mut w: WriteHalf<Stream>,
    mut units: mpsc::UnboundedReceiver<Out>,
    mut async_rx: mpsc::UnboundedReceiver<Bytes>,
) {
    let mut buf = BytesMut::with_capacity(64 * 1024);
    let result: std::io::Result<()> = async {
        loop {
            tokio::select! {
                biased;
                Some(a) = async_rx.recv() => {
                    w.write_all(&a).await?;
                    w.flush().await?;
                }
                u = units.recv() => match u {
                    None => return Ok(()),
                    Some(Out::Local(b)) => {
                        w.write_all(&b).await?;
                        w.flush().await?;
                    }
                    Some(Out::Stream { prefix, mut rx }) => {
                        buf.extend_from_slice(&prefix);
                        loop {
                            match rx.try_recv() {
                                Ok(b) => {
                                    buf.put_slice(&b);
                                    if buf.len() >= 64 * 1024 {
                                        w.write_all(&buf).await?;
                                        buf.clear();
                                    }
                                }
                                Err(mpsc::error::TryRecvError::Empty) => {
                                    if !buf.is_empty() {
                                        w.write_all(&buf).await?;
                                        buf.clear();
                                    }
                                    w.flush().await?;
                                    tokio::select! {
                                        biased;
                                        b = rx.recv() => match b {
                                            Some(b) => buf.put_slice(&b),
                                            None => break,
                                        },
                                        Some(a) = async_rx.recv() => {
                                            w.write_all(&a).await?;
                                        }
                                    }
                                }
                                Err(mpsc::error::TryRecvError::Disconnected) => break,
                            }
                        }
                        if !buf.is_empty() {
                            w.write_all(&buf).await?;
                            buf.clear();
                        }
                        w.flush().await?;
                    }
                }
            }
        }
    }
    .await;
    if let Err(e) = result {
        tracing::debug!(error = %e, "client write ended");
    }
    let _ = w.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let cols = ["app", "app", "db", "pg_catalog,public", "0", "read committed", "TimeZone=UTC"];
        body.put_i16(cols.len() as i16);
        for c in cols {
            body.put_i32(c.len() as i32);
            body.extend_from_slice(c.as_bytes());
        }
        let frames = vec![Frame::new(b'D', body.freeze()), Frame::new(b'C', Bytes::from_static(b"SELECT 1\0"))];
        let fp = parse_fp(&frames).unwrap();
        assert_eq!(fp.current_user, "app");
        assert_eq!(fp.schemas, "pg_catalog,public");
        assert_eq!(fp.temp_rels, 0);
        assert_eq!(fp.default_iso, Some(Isolation::ReadCommitted));
        assert!(parse_fp(&[protocol::error_frame("ERROR", "42501", "denied")]).is_none());
    }
}
