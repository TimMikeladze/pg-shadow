//! Configuration: a TOML file, with any value overridable by environment variables
//! named PGSHADOW_<SECTION>_<KEY> (e.g. PGSHADOW_UPSTREAM_HOST, PGSHADOW_CACHE_ENABLED).

use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub listen: Listen,
    pub upstream: Upstream,
    pub check: Check,
    pub cache: Cache,
    pub changelog: Changelog,
    pub admin: Admin,
    pub log: Log,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: Listen::default(),
            upstream: Upstream::default(),
            check: Check::default(),
            cache: Cache::default(),
            changelog: Changelog::default(),
            admin: Admin::default(),
            log: Log::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Listen {
    /// Where clients connect.
    pub addr: String,
    /// PEM certificate and key. When set, clients may use TLS (sslmode=require).
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    /// Maximum concurrent client connections (each holds one upstream connection).
    pub max_connections: usize,
}

impl Default for Listen {
    fn default() -> Self {
        Self { addr: "0.0.0.0:6432".into(), tls_cert: None, tls_key: None, max_connections: 1000 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TlsMode {
    Disable,
    Prefer,
    Require,
    VerifyFull,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Upstream {
    pub host: String,
    pub port: u16,
    /// disable | prefer | require | verify-full. `require` encrypts without checking the
    /// certificate (like libpq); `verify-full` checks it against the system roots or `ca_file`.
    pub tls: TlsMode,
    pub ca_file: Option<String>,
    #[serde(deserialize_with = "de_duration")]
    pub connect_timeout: Duration,
}

impl Default for Upstream {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 5432,
            tls: TlsMode::Prefer,
            ca_file: None,
            connect_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Check {
    /// Connection string for the proxy's own connections (validity checks, maintenance),
    /// e.g. "host=db.example.com user=pgshadow password=... dbname=app".
    /// TLS follows [upstream].tls. The role needs SELECT/EXECUTE on schema pgshadow and,
    /// for changelog trimming, EXECUTE on pgshadow.trim.
    pub dsn: String,
    /// Parallel check connections. Each runs one batched check at a time.
    pub connections: usize,
    /// A check slower than this counts as failed (the query goes to Postgres instead).
    #[serde(deserialize_with = "de_duration")]
    pub timeout: Duration,
    /// Most cached results validated in one round trip.
    pub max_batch: usize,
}

impl Default for Check {
    fn default() -> Self {
        Self {
            dsn: "host=127.0.0.1 user=postgres dbname=postgres".into(),
            connections: 2,
            timeout: Duration::from_millis(250),
            max_batch: 256,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Cache {
    /// Master switch. When false every query goes straight to Postgres.
    pub enabled: bool,
    /// In-memory tier size.
    #[serde(deserialize_with = "de_size")]
    pub memory: usize,
    /// Directory for the disk tier. Unset = memory only.
    pub disk_path: Option<String>,
    #[serde(deserialize_with = "de_size")]
    pub disk_capacity: usize,
    /// Results larger than this are streamed through and not stored.
    #[serde(deserialize_with = "de_size")]
    pub max_entry_size: usize,
    /// Cached results older than this are not used. Must be shorter than
    /// [changelog].retention; older results would fail validation anyway.
    #[serde(deserialize_with = "de_duration")]
    pub ttl: Duration,
    /// Only cache queries that take at least this long in Postgres. Cheaper queries cost
    /// less than the validity check, so caching them would add load, not remove it.
    #[serde(deserialize_with = "de_duration")]
    pub min_query_time: Duration,
    /// Fraction of cache hits that are also run on Postgres and compared (0.0 - 1.0).
    /// A mismatch is logged as an error and turns caching off for the tables involved.
    pub verify_sample_rate: f64,
    /// Remembered statement classifications.
    pub classifier_capacity: usize,
    /// How long the proxy reuses what it learned about a table (name resolution,
    /// eligibility, fingerprint) before asking Postgres again. A table enabled or changed
    /// is picked up within this time; validation never relies on it.
    #[serde(deserialize_with = "de_duration")]
    pub table_state_ttl: Duration,
    /// Statements whose cached results are nearly always invalid (their tables change
    /// constantly) stop being cached for this long. 0 disables the pause.
    #[serde(deserialize_with = "de_duration")]
    pub invalidation_cooldown: Duration,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            enabled: true,
            memory: 512 << 20,
            disk_path: None,
            disk_capacity: 8 << 30,
            max_entry_size: 8 << 20,
            ttl: Duration::from_secs(600),
            min_query_time: Duration::from_millis(1),
            verify_sample_rate: 0.0,
            classifier_capacity: 20_000,
            invalidation_cooldown: Duration::from_secs(60),
            table_state_ttl: Duration::from_secs(2),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Changelog {
    /// How long change records are kept. Bounds how old a cached result can be and
    /// still be validated.
    #[serde(deserialize_with = "de_duration")]
    pub retention: Duration,
    /// How often one proxy instance trims old records. 0 disables trimming here.
    #[serde(deserialize_with = "de_duration")]
    pub trim_interval: Duration,
}

impl Default for Changelog {
    fn default() -> Self {
        Self { retention: Duration::from_secs(900), trim_interval: Duration::from_secs(60) }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Admin {
    /// HTTP: GET /healthz, /readyz; POST /cache/enable, /cache/disable, /cache/purge.
    /// Empty disables it.
    pub addr: String,
}

impl Default for Admin {
    fn default() -> Self {
        Self { addr: "127.0.0.1:9187".into() }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Log {
    /// "json" or "pretty".
    pub format: String,
    /// tracing filter, e.g. "info" or "info,pgshadow::query=debug" for one line per query.
    pub level: String,
    /// Interval of the summary line (counts since the previous one). 0 disables it.
    #[serde(deserialize_with = "de_duration")]
    pub summary_interval: Duration,
}

impl Default for Log {
    fn default() -> Self {
        Self { format: "json".into(), level: "info".into(), summary_interval: Duration::from_secs(60) }
    }
}

impl Config {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut value: toml::Table = match path {
            Some(p) => toml::from_str(&std::fs::read_to_string(p).with_context(|| format!("reading {}", p.display()))?)
                .with_context(|| format!("parsing {}", p.display()))?,
            None => toml::Table::new(),
        };
        apply_env(&mut value);
        let cfg: Config = toml::Value::Table(value).try_into().context("invalid configuration")?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if !(0.0..=1.0).contains(&self.cache.verify_sample_rate) {
            bail!("cache.verify_sample_rate must be between 0 and 1");
        }
        if self.cache.ttl >= self.changelog.retention {
            bail!("cache.ttl must be shorter than changelog.retention");
        }
        if self.check.connections == 0 || self.check.max_batch == 0 {
            bail!("check.connections and check.max_batch must be at least 1");
        }
        if self.listen.tls_cert.is_some() != self.listen.tls_key.is_some() {
            bail!("listen.tls_cert and listen.tls_key must be set together");
        }
        if self.cache.memory < 1 << 20 {
            bail!("cache.memory must be at least 1MB");
        }
        Ok(())
    }
}

/// PGSHADOW_SECTION_KEY=value overrides. Values are parsed as TOML when possible
/// (numbers, booleans), otherwise taken as strings.
fn apply_env(root: &mut toml::Table) {
    for (k, v) in std::env::vars() {
        let Some(rest) = k.strip_prefix("PGSHADOW_") else { continue };
        let Some((section, key)) = rest.split_once('_') else { continue };
        let (section, key) = (section.to_ascii_lowercase(), key.to_ascii_lowercase());
        if !["listen", "upstream", "check", "cache", "changelog", "admin", "log"].contains(&section.as_str()) {
            continue;
        }
        let parsed = toml::from_str::<toml::Table>(&format!("v = {v}"))
            .ok()
            .and_then(|mut t| t.remove("v"))
            .unwrap_or(toml::Value::String(v));
        let entry = root.entry(section).or_insert_with(|| toml::Value::Table(Default::default()));
        if let toml::Value::Table(t) = entry {
            t.insert(key, parsed);
        }
    }
}

pub fn parse_size(s: &str) -> Result<usize> {
    let t = s.trim();
    let split = t.find(|c: char| !c.is_ascii_digit() && c != '.').unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let n: f64 = num.parse().with_context(|| format!("bad size {s:?}"))?;
    let mult = match unit.trim().to_ascii_uppercase().as_str() {
        "" | "B" => 1.0,
        "K" | "KB" | "KIB" => 1024.0,
        "M" | "MB" | "MIB" => 1024.0 * 1024.0,
        "G" | "GB" | "GIB" => 1024.0 * 1024.0 * 1024.0,
        "T" | "TB" | "TIB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        u => bail!("bad size unit {u:?} in {s:?}"),
    };
    Ok((n * mult) as usize)
}

pub fn parse_duration(s: &str) -> Result<Duration> {
    let t = s.trim();
    let split = t.find(|c: char| !c.is_ascii_digit() && c != '.').unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let n: f64 = num.parse().with_context(|| format!("bad duration {s:?}"))?;
    let secs = match unit.trim() {
        "ms" => n / 1000.0,
        "" | "s" => n,
        "m" => n * 60.0,
        "h" => n * 3600.0,
        "d" => n * 86400.0,
        u => bail!("bad duration unit {u:?} in {s:?} (use ms, s, m, h, d)"),
    };
    Ok(Duration::from_secs_f64(secs))
}

fn de_size<'de, D: serde::Deserializer<'de>>(d: D) -> Result<usize, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum V {
        N(u64),
        S(String),
    }
    match V::deserialize(d)? {
        V::N(n) => Ok(n as usize),
        V::S(s) => parse_size(&s).map_err(serde::de::Error::custom),
    }
}

fn de_duration<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum V {
        N(f64),
        S(String),
    }
    match V::deserialize(d)? {
        V::N(n) => Ok(Duration::from_secs_f64(n)),
        V::S(s) => parse_duration(&s).map_err(serde::de::Error::custom),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_and_durations() {
        assert_eq!(parse_size("8MB").unwrap(), 8 << 20);
        assert_eq!(parse_size("1.5 GB").unwrap(), 3 << 29);
        assert_eq!(parse_size("4096").unwrap(), 4096);
        assert!(parse_size("8 parsecs").is_err());
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_duration("15m").unwrap(), Duration::from_secs(900));
        assert!(parse_duration("5 fortnights").is_err());
    }

    #[test]
    fn file_and_validation() {
        let cfg: Config = toml::from_str(
            r#"
            [upstream]
            host = "db"
            tls = "verify-full"
            [cache]
            memory = "2GB"
            ttl = "5m"
            min_query_time = "2ms"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.upstream.tls, TlsMode::VerifyFull);
        assert_eq!(cfg.cache.memory, 2 << 30);
        assert_eq!(cfg.cache.min_query_time, Duration::from_millis(2));
        cfg.validate().unwrap();
        let bad: Config = toml::from_str("[cache]\nttl = \"1h\"").unwrap();
        assert!(bad.validate().is_err(), "ttl must be below changelog retention");
        assert!(toml::from_str::<Config>("[cache]\nnope = 1").is_err(), "unknown keys are rejected");
    }
}
