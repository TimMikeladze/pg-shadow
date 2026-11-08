use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use pgshadow::app::{self, App};
use pgshadow::config::{Config, TlsMode};
use tokio::sync::OnceCell;
use tokio_postgres::config::SslMode;
use tokio_postgres::types::{FromSql, Type};
use tokio_postgres::{Client, NoTls};

pub fn pg_dsn() -> String {
    std::env::var("PGSHADOW_TEST_PG")
        .unwrap_or_else(|_| "host=127.0.0.1 port=5433 user=postgres dbname=shadow".into())
}

fn pg_config() -> tokio_postgres::Config {
    pg_dsn().parse().expect("PGSHADOW_TEST_PG")
}

/// `sslmode=require` in PGSHADOW_TEST_PG: TLS to Postgres from the tests and the proxy.
pub fn pg_tls() -> bool {
    pg_config().get_ssl_mode() == SslMode::Require
}

/// Whether PGSHADOW_TEST_PG carries a password (so the server doesn't trust the user).
pub fn pg_has_password() -> bool {
    pg_config().get_password().is_some()
}

/// The test database, `shadow` unless PGSHADOW_TEST_PG names another.
pub fn pg_db() -> String {
    pg_config().get_dbname().unwrap_or("shadow").to_string()
}

/// A conninfo for the proxy on `port` as the test DSN's user (and password).
pub fn proxy_dsn(port: u16, db: &str) -> String {
    let cfg = pg_config();
    let mut dsn = format!(
        "host=127.0.0.1 port={port} user={} dbname={db}",
        cfg.get_user().unwrap_or("postgres")
    );
    if let Some(pw) = cfg.get_password() {
        let pw = String::from_utf8_lossy(pw)
            .replace('\\', "\\\\")
            .replace('\'', "\\'");
        dsn.push_str(&format!(" password='{pw}'"));
    }
    dsn
}

pub fn pg_host_port() -> (String, u16) {
    let cfg: tokio_postgres::Config = pg_dsn().parse().unwrap();
    let host = match &cfg.get_hosts()[0] {
        tokio_postgres::config::Host::Tcp(h) => h.clone(),
        #[allow(unreachable_patterns)]
        _ => "127.0.0.1".into(),
    };
    (host, cfg.get_ports().first().copied().unwrap_or(5432))
}

static INSTALLED: OnceCell<()> = OnceCell::const_new();

pub async fn direct() -> Client {
    connect_dsn(&pg_dsn()).await
}

pub async fn connect_dsn(dsn: &str) -> Client {
    let cfg: tokio_postgres::Config = dsn.parse().unwrap();
    // Never print a password in a panic message.
    let shown: Vec<&str> = dsn
        .split_whitespace()
        .filter(|kv| !kv.starts_with("password="))
        .collect();
    let shown = shown.join(" ");
    if cfg.get_ssl_mode() == SslMode::Require {
        // TlsMode::Require encrypts without verifying the certificate, like libpq's sslmode=require.
        let tls = pgshadow::net::client_tls_config(TlsMode::Require, None).unwrap();
        let (c, conn) = cfg
            .connect(tokio_postgres_rustls::MakeRustlsConnect::new(
                (*tls).clone(),
            ))
            .await
            .unwrap_or_else(|e| panic!("connect {shown}: {e}"));
        tokio::spawn(async move {
            let _ = conn.await;
        });
        return c;
    }
    let (c, conn) = cfg
        .connect(NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect {shown}: {e}"));
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

pub struct Proxy {
    pub app: App,
    pub port: u16,
    _dir: Option<tempfile::TempDir>,
}

/// A separate database (with pgshadow installed) for tests whose actions are global
/// to a database, like trimming the changelog.
pub async fn isolated_db(name: &str) -> String {
    let d = direct().await;
    let exists = d
        .query_opt("SELECT 1 FROM pg_database WHERE datname = $1", &[&name])
        .await
        .unwrap()
        .is_some();
    if !exists {
        let _ = d.batch_execute(&format!("CREATE DATABASE {name}")).await;
    }
    let dsn = pg_dsn().replace(&format!("dbname={}", pg_db()), &format!("dbname={name}"));
    let c = connect_dsn(&dsn).await;
    c.batch_execute(pgshadow::admin::SCHEMA_SQL)
        .await
        .expect("install pgshadow schema");
    dsn
}

impl Proxy {
    pub fn dsn(&self) -> String {
        proxy_dsn(self.port, &pg_db())
    }

    pub async fn client_db(&self, db: &str) -> Client {
        connect_dsn(&proxy_dsn(self.port, db)).await
    }

    pub async fn client(&self) -> Client {
        connect_dsn(&self.dsn()).await
    }

    pub fn hits(&self) -> u64 {
        self.app.shared.stats.hits.load(Ordering::Relaxed)
    }

    pub fn fills(&self) -> u64 {
        self.app.shared.stats.fills.load(Ordering::Relaxed)
    }

    /// Fills are stored by a background task right after the response is sent.
    pub async fn wait_fills(&self, n: u64) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.fills() < n {
            assert!(
                Instant::now() < deadline,
                "expected {n} fills, have {}",
                self.fills()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

pub fn base_config() -> Config {
    let (host, port) = pg_host_port();
    let mut cfg = Config::default();
    cfg.listen.addr = "127.0.0.1:0".into();
    cfg.upstream.host = host;
    cfg.upstream.port = port;
    cfg.upstream.tls = if pg_tls() {
        TlsMode::Require
    } else {
        TlsMode::Disable
    };
    cfg.check.dsn = pg_dsn();
    cfg.check.timeout = Duration::from_secs(2);
    cfg.cache.memory = 32 << 20;
    cfg.cache.min_query_time = Duration::ZERO;
    cfg.cache.table_state_ttl = Duration::from_millis(200);
    cfg.admin.addr = String::new();
    cfg.log.summary_interval = Duration::ZERO;
    cfg.changelog.trim_interval = Duration::ZERO;
    cfg
}

pub async fn proxy() -> Proxy {
    proxy_with(|_| {}).await
}

pub async fn proxy_with(tweak: impl FnOnce(&mut Config)) -> Proxy {
    INSTALLED
        .get_or_init(|| async {
            let c = direct().await;
            c.batch_execute(pgshadow::admin::SCHEMA_SQL)
                .await
                .expect("install pgshadow schema");
        })
        .await;
    let mut cfg = base_config();
    tweak(&mut cfg);
    proxy_from(cfg, None).await
}

pub async fn proxy_from(cfg: Config, dir: Option<tempfile::TempDir>) -> Proxy {
    INSTALLED
        .get_or_init(|| async {
            let c = direct().await;
            c.batch_execute(pgshadow::admin::SCHEMA_SQL)
                .await
                .expect("install pgshadow schema");
        })
        .await;
    let app = app::start(cfg).await.expect("start proxy");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !app.shared.validator.ready() {
        assert!(
            Instant::now() < deadline,
            "check connections never became ready"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    app.spawn(listener);
    Proxy {
        app,
        port,
        _dir: dir,
    }
}

/// A fresh table, filled and opted in. Names must be unique per test.
pub async fn table(c: &Client, name: &str, rows: i32, enable: bool) {
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {name} CASCADE;
         CREATE TABLE {name} (id int PRIMARY KEY, grp text NOT NULL, v bigint NOT NULL, amount numeric(12,2),
                              at timestamptz NOT NULL DEFAULT '2026-01-01 00:00+00', doc jsonb, tags text[], raw bytea);
         INSERT INTO {name} SELECT g, 'g' || (g % 5), g, g * 1.25, '2026-01-01 00:00+00'::timestamptz + g * interval '1 hour',
                                   jsonb_build_object('n', g), ARRAY['a', 'b' || g], decode(md5(g::text), 'hex')
         FROM generate_series(1, {rows}) g;
         ANALYZE {name};"
    ))
    .await
    .unwrap();
    if enable {
        c.execute("SELECT pgshadow.enable($1::text::regclass)", &[&name])
            .await
            .unwrap();
    }
}

/// Any column as raw bytes (whatever format the server sent), for exact comparisons.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Raw(pub Option<Vec<u8>>);

impl<'a> FromSql<'a> for Raw {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Raw(Some(raw.to_vec())))
    }
    fn from_sql_null(_: &Type) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Raw(None))
    }
    fn accepts(_: &Type) -> bool {
        true
    }
}

pub fn raw_rows(rows: &[tokio_postgres::Row]) -> Vec<Vec<Raw>> {
    rows.iter()
        .map(|r| (0..r.len()).map(|i| r.get::<_, Raw>(i)).collect())
        .collect()
}

pub async fn simple_rows(c: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    let mut out = Vec::new();
    for m in c.simple_query(sql).await.unwrap() {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            out.push((0..r.len()).map(|i| r.get(i).map(str::to_string)).collect());
        }
    }
    out
}

/// A TCP relay to the test Postgres that delivers every chunk `delay` after it arrived,
/// in each direction, like a link with that latency: the proxy sees a round trip of
/// about 2 * delay. Returns its port.
pub async fn slow_relay(delay: Duration) -> u16 {
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    fn link(
        mut r: impl AsyncRead + Unpin + Send + 'static,
        mut w: impl AsyncWrite + Unpin + Send + 'static,
        delay: Duration,
    ) {
        let (tx, mut rx) =
            tokio::sync::mpsc::unbounded_channel::<(tokio::time::Instant, Vec<u8>)>();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 << 10];
            while let Ok(n) = r.read(&mut buf).await {
                if n == 0
                    || tx
                        .send((tokio::time::Instant::now() + delay, buf[..n].to_vec()))
                        .is_err()
                {
                    break;
                }
            }
        });
        tokio::spawn(async move {
            while let Some((at, chunk)) = rx.recv().await {
                tokio::time::sleep_until(at).await;
                if w.write_all(&chunk).await.is_err() {
                    break;
                }
            }
        });
    }
    let (host, port) = pg_host_port();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let Ok(server) = tokio::net::TcpStream::connect((host.as_str(), port)).await else {
                continue;
            };
            // Like the proxy and Postgres. Without it Nagle holds a second write until the
            // first is acked, and Linux delays that ack by up to 40 ms.
            let _ = client.set_nodelay(true);
            let _ = server.set_nodelay(true);
            let (cr, cw) = client.into_split();
            let (sr, sw) = server.into_split();
            link(cr, sw, delay);
            link(sr, cw, delay);
        }
    });
    relay
}

/// A proxy whose upstream and check connections go through `slow_relay(delay)`, returned
/// once it has measured the round trip.
pub async fn slow_proxy(delay: Duration, tweak: impl FnOnce(&mut Config)) -> Proxy {
    let relay = slow_relay(delay).await;
    let mut check = proxy_dsn(relay, &pg_db());
    if pg_tls() {
        check.push_str(" sslmode=require");
    }
    let p = proxy_with(|c| {
        c.upstream.host = "127.0.0.1".into();
        c.upstream.port = relay;
        c.check.dsn = check;
        c.check.timeout = Duration::from_secs(2);
        tweak(c);
    })
    .await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while p.app.shared.validator.rtt() < delay * 3 / 2 {
        assert!(Instant::now() < deadline, "round trip never measured");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    p
}
