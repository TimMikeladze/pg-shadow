//! Cached results, stored in a foyer hybrid cache (memory, optionally spilling to disk).
//!
//! An entry holds Postgres's raw response bytes for one request plus what is needed to
//! prove it is still valid: the snapshot it was read under, the catalog epoch and
//! formatting settings, and for each table its catalog fingerprint and the reference
//! that resolved to it.
//!
//! Correctness never depends on this store: every hit is re-validated against Postgres
//! before it is served, entries carry a checksum, and the full request bytes are
//! compared on lookup so a hash collision can never return another query's result.
//! Whatever the store loses, keeps too long, or resurfaces after a restart, the worst
//! outcome is a miss.

use std::io::{Read, Write};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bytes::Bytes;
use foyer::{
    BlockEngineConfig, Code, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy, RecoverMode,
};

use crate::config;

/// One table a cached result depends on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDep {
    pub oid: u32,
    /// pgshadow.rel_state(oid).fp when the result was read.
    pub fp: String,
    /// The reference as written (schema if qualified, and name); the check confirms it
    /// still resolves to this table for the entry's search_path and role.
    pub nsp: Option<String>,
    pub name: String,
    /// Schema-qualified, quoted name, for locking the table inside transactions.
    pub lock_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Canonical request bytes; compared on every lookup.
    pub request: Bytes,
    /// Response frames exactly as Postgres sent them, without the final ReadyForQuery.
    pub response: Bytes,
    /// pg_current_snapshot(), taken before the query was sent.
    pub snapshot: String,
    /// pgshadow.catalog_epoch() and settings_hash() at the same time.
    pub epoch: String,
    pub settings: String,
    pub tables: Vec<TableDep>,
    /// Role whose privileges are re-checked before serving (current_user).
    pub role: String,
    /// The session's search_path setting, for re-resolving names.
    pub path: String,
    /// Built-in functions the query calls (comma-separated), whose privileges are re-checked.
    pub funcs: String,
    pub created_unix_ms: u64,
    /// How long Postgres took, for logs.
    pub exec_micros: u64,
}

const MAGIC: u32 = 0x5047_5348; // "PGSH"
const VERSION: u8 = 3;

fn io_err(e: impl std::fmt::Display) -> foyer::Error {
    foyer::Error::io_error(std::io::Error::other(e.to_string()))
}

struct Writer<'a, W: Write> {
    w: &'a mut W,
    crc: crc32fast::Hasher,
}

impl<W: Write> Writer<'_, W> {
    fn raw(&mut self, b: &[u8]) -> std::io::Result<()> {
        self.crc.update(b);
        self.w.write_all(b)
    }
    fn u64(&mut self, v: u64) -> std::io::Result<()> {
        self.raw(&v.to_le_bytes())
    }
    fn bytes(&mut self, b: &[u8]) -> std::io::Result<()> {
        self.u64(b.len() as u64)?;
        self.raw(b)
    }
}

struct Reader<'a, R: Read> {
    r: &'a mut R,
    crc: crc32fast::Hasher,
}

impl<R: Read> Reader<'_, R> {
    fn raw(&mut self, n: usize) -> std::io::Result<Vec<u8>> {
        let mut v = vec![0u8; n];
        self.r.read_exact(&mut v)?;
        self.crc.update(&v);
        Ok(v)
    }
    fn u64(&mut self) -> std::io::Result<u64> {
        let b = self.raw(8)?;
        Ok(u64::from_le_bytes(b.try_into().unwrap()))
    }
    fn bytes(&mut self) -> std::io::Result<Vec<u8>> {
        let n = self.u64()? as usize;
        if n > config::parse_size("2GB").unwrap() {
            return Err(std::io::Error::other("entry field too large"));
        }
        self.raw(n)
    }
    fn string(&mut self) -> std::io::Result<String> {
        String::from_utf8(self.bytes()?).map_err(std::io::Error::other)
    }
}

impl Code for Entry {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        let mut w = Writer {
            w: writer,
            crc: crc32fast::Hasher::new(),
        };
        (|| -> std::io::Result<()> {
            w.raw(&MAGIC.to_le_bytes())?;
            w.raw(&[VERSION])?;
            w.bytes(&self.request)?;
            w.bytes(&self.response)?;
            w.bytes(self.snapshot.as_bytes())?;
            w.bytes(self.epoch.as_bytes())?;
            w.bytes(self.settings.as_bytes())?;
            w.bytes(self.role.as_bytes())?;
            w.bytes(self.path.as_bytes())?;
            w.bytes(self.funcs.as_bytes())?;
            w.u64(self.created_unix_ms)?;
            w.u64(self.exec_micros)?;
            w.u64(self.tables.len() as u64)?;
            for t in &self.tables {
                w.u64(t.oid as u64)?;
                w.bytes(t.fp.as_bytes())?;
                match &t.nsp {
                    None => w.raw(&[0])?,
                    Some(nsp) => {
                        w.raw(&[1])?;
                        w.bytes(nsp.as_bytes())?;
                    }
                }
                w.bytes(t.name.as_bytes())?;
                w.bytes(t.lock_name.as_bytes())?;
            }
            let crc = w.crc.clone().finalize();
            w.w.write_all(&crc.to_le_bytes())
        })()
        .map_err(foyer::Error::io_error)
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self> {
        let mut r = Reader {
            r: reader,
            crc: crc32fast::Hasher::new(),
        };
        let entry = (|| -> std::io::Result<Entry> {
            let magic = u32::from_le_bytes(r.raw(4)?.try_into().unwrap());
            let version = r.raw(1)?[0];
            if magic != MAGIC || version != VERSION {
                return Err(std::io::Error::other("unknown entry format"));
            }
            let request = Bytes::from(r.bytes()?);
            let response = Bytes::from(r.bytes()?);
            let snapshot = r.string()?;
            let epoch = r.string()?;
            let settings = r.string()?;
            let role = r.string()?;
            let path = r.string()?;
            let funcs = r.string()?;
            let created_unix_ms = r.u64()?;
            let exec_micros = r.u64()?;
            let n = r.u64()?;
            if n > 10_000 {
                return Err(std::io::Error::other("too many tables"));
            }
            let mut tables = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let oid = r.u64()? as u32;
                let fp = r.string()?;
                let nsp = match r.raw(1)?[0] {
                    0 => None,
                    1 => Some(r.string()?),
                    _ => return Err(std::io::Error::other("bad entry")),
                };
                let name = r.string()?;
                let lock_name = r.string()?;
                tables.push(TableDep {
                    oid,
                    fp,
                    nsp,
                    name,
                    lock_name,
                });
            }
            Ok(Entry {
                request,
                response,
                snapshot,
                epoch,
                settings,
                tables,
                role,
                path,
                funcs,
                created_unix_ms,
                exec_micros,
            })
        })()
        .map_err(foyer::Error::io_error)?;
        let expected = r.crc.clone().finalize();
        let mut stored = [0u8; 4];
        r.r.read_exact(&mut stored)
            .map_err(foyer::Error::io_error)?;
        if u32::from_le_bytes(stored) != expected {
            return Err(io_err("checksum mismatch"));
        }
        Ok(entry)
    }

    fn estimated_size(&self) -> usize {
        64 + self.request.len()
            + self.response.len()
            + self.snapshot.len()
            + self.epoch.len()
            + self.settings.len()
            + self.role.len()
            + self.path.len()
            + self.funcs.len()
            + self
                .tables
                .iter()
                .map(|t| {
                    64 + t.fp.len()
                        + t.nsp.as_ref().map_or(0, String::len)
                        + t.name.len()
                        + t.lock_name.len()
                })
                .sum::<usize>()
    }
}

pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Cache key: a 128-bit hash of the canonical request.
pub fn key_of(request: &[u8]) -> u128 {
    xxhash_rust::xxh3::xxh3_128(request)
}

pub type Handle = foyer::HybridCacheEntry<u128, Entry>;

pub struct Store {
    cache: HybridCache<u128, Entry>,
    ttl_ms: u64,
    disk: bool,
}

impl Store {
    pub async fn open(cfg: &config::Cache) -> Result<Arc<Self>> {
        let builder = HybridCacheBuilder::new()
            .with_name("pgshadow")
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .with_flush_on_close(true)
            .memory(cfg.memory)
            .with_weighter(|_k: &u128, v: &Entry| v.estimated_size())
            .storage();
        let builder = match &cfg.disk_path {
            Some(path) => {
                std::fs::create_dir_all(path).with_context(|| format!("creating {path}"))?;
                let device = FsDeviceBuilder::new(path)
                    .with_capacity(cfg.disk_capacity)
                    .build()
                    .context("opening cache directory")?;
                builder
                    .with_engine_config(BlockEngineConfig::new(device).with_block_size(16 << 20))
                    // Entries found on disk after a restart are still validated before use.
                    .with_recover_mode(RecoverMode::Quiet)
            }
            None => builder,
        };
        let cache = builder.build().await.context("building cache")?;
        Ok(Arc::new(Self {
            cache,
            ttl_ms: cfg.ttl.as_millis() as u64,
            disk: cfg.disk_path.is_some(),
        }))
    }

    /// The entry for this request, if one is stored, matches exactly, and is within TTL.
    pub async fn get(&self, key: u128, request: &[u8]) -> Option<Handle> {
        match self.cache.get(&key).await {
            Ok(Some(e)) => {
                if e.value().request.as_ref() != request {
                    return None;
                }
                if now_unix_ms().saturating_sub(e.value().created_unix_ms) > self.ttl_ms {
                    self.cache.remove(&key);
                    return None;
                }
                Some(e)
            }
            Ok(None) => None,
            Err(err) => {
                tracing::warn!(error = %err, "cache read failed; treating as a miss");
                self.cache.remove(&key);
                None
            }
        }
    }

    pub fn insert(&self, key: u128, entry: Entry) {
        self.cache.insert(key, entry);
    }

    pub fn remove(&self, key: u128) {
        self.cache.remove(&key);
    }

    pub async fn clear(&self) -> Result<()> {
        self.cache.clear().await.context("clearing cache")
    }

    pub fn memory_usage(&self) -> usize {
        self.cache.memory().usage()
    }

    pub fn has_disk(&self) -> bool {
        self.disk
    }

    pub async fn close(&self) -> Result<()> {
        self.cache.close().await.context("closing cache")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Entry {
        Entry {
            request: Bytes::from_static(b"Q\0SELECT 1"),
            response: Bytes::from_static(b"T....D....C...."),
            snapshot: "100:105:101,103".into(),
            epoch: "3:900|0:0|1:880|0:0".into(),
            settings: "d41d8cd98f00b204e9800998ecf8427e".into(),
            tables: vec![
                TableDep {
                    oid: 16384,
                    fp: "abc|r|f".into(),
                    nsp: None,
                    name: "orders".into(),
                    lock_name: "public.orders".into(),
                },
                TableDep {
                    oid: 16390,
                    fp: "def".into(),
                    nsp: Some("app".into()),
                    name: "Items".into(),
                    lock_name: "app.\"Items\"".into(),
                },
            ],
            role: "app".into(),
            path: "\"$user\", public".into(),
            funcs: "lower,upper".into(),
            created_unix_ms: 1_700_000_000_000,
            exec_micros: 1234,
        }
    }

    #[test]
    fn entry_roundtrip_and_checksum() {
        let e = sample();
        let mut buf = Vec::new();
        e.encode(&mut buf).unwrap();
        assert_eq!(Entry::decode(&mut &buf[..]).unwrap(), e);
        // Flip one byte anywhere in the payload: decoding must fail, never return wrong data.
        for i in [5, 20, buf.len() / 2, buf.len() - 6] {
            let mut bad = buf.clone();
            bad[i] ^= 0x40;
            assert!(
                Entry::decode(&mut &bad[..]).is_err(),
                "corruption at byte {i} not detected"
            );
        }
    }

    #[tokio::test]
    async fn store_memory_and_disk() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config::Cache::default();
        cfg.memory = 4 << 20;
        cfg.disk_path = Some(dir.path().to_string_lossy().into_owned());
        cfg.disk_capacity = 64 << 20;
        let store = Store::open(&cfg).await.unwrap();
        let mut e = sample();
        e.created_unix_ms = now_unix_ms();
        let k = key_of(&e.request);
        store.insert(k, e.clone());
        assert_eq!(store.get(k, &e.request).await.unwrap().value(), &e);
        assert!(
            store.get(k, b"different request").await.is_none(),
            "request bytes are compared"
        );
        store.close().await.unwrap();
        drop(store);
        // Reopen: the entry comes back from disk.
        let store = Store::open(&cfg).await.unwrap();
        let got = store.get(k, &e.request).await;
        assert_eq!(got.map(|h| h.value().clone()), Some(e));
    }
}
