//! Concurrent staleness oracle.
//!
//! Writers increment counters (directly on Postgres and through the proxy, with short
//! and long transactions) and record the moment each commit was acknowledged. Readers
//! query through the proxy and record the moment each read started. A read is stale if
//! it returns a value older than one whose commit was acknowledged before the read
//! started -- exactly what Postgres itself guarantees. Any stale read fails the test.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rand::Rng;

use crate::common::*;

const ROWS: i32 = 16;

#[derive(Default)]
struct Log {
    /// (commit acknowledged at, row, value)
    writes: Vec<(Instant, i32, i64)>,
    /// (read started at, values by row)
    reads: Vec<(Instant, Vec<i64>)>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_stale_reads_under_concurrent_writes() {
    let secs: u64 = std::env::var("PGSHADOW_STALENESS_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(10);
    // Keep caching on even though this table is written constantly.
    let p = Arc::new(proxy_with(|c| c.cache.invalidation_cooldown = Duration::ZERO).await);
    let d = direct().await;
    d.batch_execute(&format!(
        "DROP TABLE IF EXISTS s_counters;
         CREATE TABLE s_counters (id int PRIMARY KEY, v bigint NOT NULL);
         INSERT INTO s_counters SELECT g, 0 FROM generate_series(1, {ROWS}) g;
         SELECT pgshadow.enable('s_counters');"
    ))
    .await
    .unwrap();
    let log = Arc::new(Mutex::new(Log::default()));
    let stop = Instant::now() + Duration::from_secs(secs);
    let mut tasks = Vec::new();

    // Writers. Each pauses a little between writes so readers get cache hits in between.
    for w in 0..4 {
        let log = log.clone();
        let p = p.clone();
        tasks.push(tokio::spawn(async move {
            let c = if w == 3 { p.client().await } else { direct().await };
            let mut n = 0u64;
            while Instant::now() < stop {
                let (a, b) = {
                    let mut r = rand::thread_rng();
                    (r.gen_range(1..=ROWS), r.gen_range(1..=ROWS))
                };
                match w {
                    // single autocommit update
                    0 | 3 => {
                        let v: i64 = c
                            .query_one("UPDATE s_counters SET v = v + 1 WHERE id = $1 RETURNING v", &[&a])
                            .await
                            .unwrap()
                            .get(0);
                        log.lock().writes.push((Instant::now(), a, v));
                    }
                    // two rows in one transaction
                    1 => {
                        c.batch_execute("BEGIN").await.unwrap();
                        let va: i64 = c.query_one("UPDATE s_counters SET v = v + 1 WHERE id = $1 RETURNING v", &[&a]).await.unwrap().get(0);
                        let vb: i64 = c.query_one("UPDATE s_counters SET v = v + 1 WHERE id = $1 RETURNING v", &[&b]).await.unwrap().get(0);
                        c.batch_execute("COMMIT").await.unwrap();
                        let now = Instant::now();
                        let mut l = log.lock();
                        l.writes.push((now, a, va));
                        if b != a {
                            l.writes.push((now, b, vb));
                        }
                    }
                    // a transaction held open for a while: the in-progress-at-snapshot case
                    _ => {
                        c.batch_execute("BEGIN").await.unwrap();
                        let v: i64 = c.query_one("UPDATE s_counters SET v = v + 1 WHERE id = $1 RETURNING v", &[&a]).await.unwrap().get(0);
                        tokio::time::sleep(Duration::from_millis(30)).await;
                        c.batch_execute("COMMIT").await.unwrap();
                        log.lock().writes.push((Instant::now(), a, v));
                    }
                }
                n += 1;
                let pause = rand::thread_rng().gen_range(0..15);
                tokio::time::sleep(Duration::from_millis(pause)).await;
            }
            n
        }));
    }

    // Readers through the proxy: extended protocol, simple protocol, inside transactions.
    for r in 0..6 {
        let log = log.clone();
        let p = p.clone();
        tasks.push(tokio::spawn(async move {
            let c = p.client().await;
            let q = "SELECT id, v FROM s_counters ORDER BY id";
            let mut n = 0u64;
            while Instant::now() < stop {
                let in_txn = r == 5 && n % 2 == 0;
                if in_txn {
                    c.batch_execute("BEGIN").await.unwrap();
                }
                let started = Instant::now();
                let values: Vec<i64> = if r % 2 == 0 {
                    c.query(q, &[]).await.unwrap().iter().map(|row| row.get::<_, i64>(1)).collect()
                } else {
                    simple_rows(&c, q).await.iter().map(|row| row[1].as_ref().unwrap().parse().unwrap()).collect()
                };
                if in_txn {
                    c.batch_execute("COMMIT").await.unwrap();
                }
                assert_eq!(values.len(), ROWS as usize);
                log.lock().reads.push((started, values));
                n += 1;
            }
            n
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let log = log.lock();
    let mut violations = Vec::new();
    for (started, values) in &log.reads {
        for (i, observed) in values.iter().enumerate() {
            let id = i as i32 + 1;
            let must_see = log
                .writes
                .iter()
                .filter(|(at, row, _)| *row == id && at < started)
                .map(|(_, _, v)| *v)
                .max()
                .unwrap_or(0);
            if *observed < must_see {
                violations.push(format!("row {id}: read {observed}, but {must_see} was committed before the read started"));
            }
        }
    }
    let hits = p.hits();
    eprintln!(
        "staleness: {} writes, {} reads, {} cache hits, {} fills, {} invalidated, {} violations",
        log.writes.len(),
        log.reads.len(),
        hits,
        p.fills(),
        p.app.shared.stats.invalidated.load(std::sync::atomic::Ordering::Relaxed),
        violations.len()
    );
    assert!(violations.is_empty(), "stale reads:\n{}", violations.iter().take(20).cloned().collect::<Vec<_>>().join("\n"));
    assert!(hits > 50, "the cache should have served reads between writes (hits = {hits})");
}
