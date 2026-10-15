//! Cache hits, exactness, invalidation and the rules about when the cache may be used.

use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::common::*;

#[tokio::test(flavor = "multi_thread")]
async fn hits_are_byte_identical_to_postgres() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "c_exact", 200, true).await;
    let c = p.client().await;
    let queries = [
        "SELECT * FROM c_exact ORDER BY id",
        "SELECT grp, count(*), sum(amount), avg(v), max(at), array_agg(id ORDER BY id) FROM c_exact GROUP BY grp ORDER BY grp",
        "SELECT doc, tags, raw, at, amount FROM c_exact WHERE id < 20 ORDER BY id",
        "SELECT jsonb_agg(doc ORDER BY id) FROM c_exact",
        "SELECT id FROM c_exact WHERE false",
    ];
    for (i, q) in queries.iter().enumerate() {
        let want_bin = raw_rows(&d.query(*q, &[]).await.unwrap());
        let want_txt = simple_rows(&d, q).await;
        // extended protocol, binary results: first a fill, then a hit
        assert_eq!(
            raw_rows(&c.query(*q, &[]).await.unwrap()),
            want_bin,
            "fill differs: {q}"
        );
        p.wait_fills((2 * i + 1) as u64).await;
        let hits = p.hits();
        assert_eq!(
            raw_rows(&c.query(*q, &[]).await.unwrap()),
            want_bin,
            "hit differs: {q}"
        );
        assert_eq!(p.hits(), hits + 1, "expected a hit: {q}");
        // simple protocol, text results
        assert_eq!(
            simple_rows(&c, q).await,
            want_txt,
            "fill differs (text): {q}"
        );
        p.wait_fills((2 * i + 2) as u64).await;
        assert_eq!(
            simple_rows(&c, q).await,
            want_txt,
            "hit differs (text): {q}"
        );
        assert_eq!(p.hits(), hits + 2, "expected a text hit: {q}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_invalidate_immediately() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "c_inv", 100, true).await;
    let c = p.client().await;
    let q = "SELECT sum(v) FROM c_inv";
    let sum = |rows: Vec<Vec<Option<String>>>| rows[0][0].clone().unwrap();
    assert_eq!(sum(simple_rows(&c, q).await), "5050");
    p.wait_fills(1).await;
    assert_eq!(sum(simple_rows(&c, q).await), "5050");
    assert_eq!(p.hits(), 1);

    // A write from another connection that never touched the proxy.
    d.execute("UPDATE c_inv SET v = v + 1 WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(sum(simple_rows(&c, q).await), "5051");
    // A write through the proxy, in a transaction.
    c.batch_execute("BEGIN; DELETE FROM c_inv WHERE id = 100; COMMIT")
        .await
        .unwrap();
    assert_eq!(sum(simple_rows(&c, q).await), "4951");
    // INSERT ... ON CONFLICT, COPY and TRUNCATE.
    d.execute(
        "INSERT INTO c_inv (id, grp, v) VALUES (1, 'x', 0) ON CONFLICT (id) DO UPDATE SET v = 1000",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(sum(simple_rows(&c, q).await), "5949");
    d.batch_execute("TRUNCATE c_inv").await.unwrap();
    assert_eq!(simple_rows(&c, q).await[0][0], None);
    assert_eq!(p.app.shared.stats.invalidated.load(Ordering::Relaxed), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_statements_and_parameters() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "c_prep", 100, true).await;
    let c = p.client().await;
    let st = c
        .prepare("SELECT count(*) FROM c_prep WHERE grp = $1 AND v > $2")
        .await
        .unwrap();
    let n = |r: tokio_postgres::Row| r.get::<_, i64>(0);
    assert_eq!(n(c.query_one(&st, &[&"g1", &10i64]).await.unwrap()), 18);
    p.wait_fills(1).await;
    assert_eq!(n(c.query_one(&st, &[&"g1", &10i64]).await.unwrap()), 18);
    assert_eq!(p.hits(), 1, "same parameters hit");
    assert_eq!(
        n(c.query_one(&st, &[&"g1", &50i64]).await.unwrap()),
        10,
        "other parameters are a separate entry"
    );
    assert_eq!(p.hits(), 1);
    // The statement still works for uncached use after hits, and after being closed.
    drop(st);
    let st2 = c
        .prepare("SELECT count(*) FROM c_prep WHERE grp = $1 AND v > $2")
        .await
        .unwrap();
    assert_eq!(n(c.query_one(&st2, &[&"g1", &10i64]).await.unwrap()), 18);
}

#[tokio::test(flavor = "multi_thread")]
async fn transaction_rules() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "c_txn", 10, true).await;
    let c = p.client().await;
    let q = "SELECT v FROM c_txn WHERE id = 1";
    let v = |rows: Vec<Vec<Option<String>>>| rows[0][0].clone().unwrap();
    assert_eq!(v(simple_rows(&c, q).await), "1");
    p.wait_fills(1).await;

    // READ COMMITTED transaction that has not written: cache is used.
    c.batch_execute("BEGIN").await.unwrap();
    let h = p.hits();
    assert_eq!(v(simple_rows(&c, q).await), "1");
    assert_eq!(p.hits(), h + 1);
    // ... a commit elsewhere is visible to the next statement, as in Postgres.
    d.execute("UPDATE c_txn SET v = 2 WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(v(simple_rows(&c, q).await), "2");
    // ... after this transaction writes, it must see its own uncommitted write.
    c.batch_execute("UPDATE c_txn SET v = 3 WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(v(simple_rows(&c, q).await), "3");
    c.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(v(simple_rows(&c, q).await), "2");

    // REPEATABLE READ: never served from cache, and keeps its snapshot.
    crate::gaps::warm(&p, &c, q).await;
    c.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .unwrap();
    let h = p.hits();
    assert_eq!(v(simple_rows(&c, q).await), "2");
    d.execute("UPDATE c_txn SET v = 4 WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(
        v(simple_rows(&c, q).await),
        "2",
        "repeatable read keeps its snapshot"
    );
    assert_eq!(p.hits(), h, "no cache use under REPEATABLE READ");
    c.batch_execute("COMMIT").await.unwrap();
    assert_eq!(v(simple_rows(&c, q).await), "4");

    // Session default isolation changed with SET: no cache use either.
    c.batch_execute("SET default_transaction_isolation = 'serializable'")
        .await
        .unwrap();
    let h = p.hits();
    simple_rows(&c, q).await;
    simple_rows(&c, q).await;
    assert_eq!(p.hits(), h);
}

#[tokio::test(flavor = "multi_thread")]
async fn only_safe_queries_are_cached() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "c_safe", 10, true).await;
    table(&d, "c_plain", 10, false).await;
    d.batch_execute("CREATE OR REPLACE VIEW c_view AS SELECT * FROM c_safe")
        .await
        .unwrap();
    let c = p.client().await;
    for q in [
        "SELECT now(), count(*) FROM c_safe",
        "SELECT count(*) FROM c_safe WHERE at < 'now'",
        "SELECT * FROM c_safe WHERE id = 1 FOR UPDATE",
        "SELECT count(*) FROM c_plain",
        "SELECT count(*) FROM c_view",
        "SELECT random(), id FROM c_safe",
    ] {
        c.batch_execute("BEGIN").await.unwrap();
        simple_rows(&c, q).await;
        simple_rows(&c, q).await;
        c.batch_execute("COMMIT").await.unwrap();
    }
    // 'now' as a bound parameter value
    let st = c
        .prepare("SELECT count(*) FROM c_safe WHERE at < $1::text::timestamptz")
        .await
        .unwrap();
    c.query(&st, &[&"now"]).await.unwrap();
    c.query(&st, &[&"now"]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(p.hits(), 0);
    assert_eq!(p.fills(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn disk_tier_survives_restart() {
    let d = direct().await;
    table(&d, "c_disk", 50, true).await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = base_config();
    cfg.cache.disk_path = Some(dir.path().to_string_lossy().into());
    cfg.cache.disk_capacity = 64 << 20;
    let q = "SELECT sum(amount) FROM c_disk";
    {
        let p = proxy_from(cfg.clone(), None).await;
        let c = p.client().await;
        simple_rows(&c, q).await;
        p.wait_fills(1).await;
        p.app.shutdown().await;
    }
    let p = proxy_from(cfg.clone(), None).await;
    let c = p.client().await;
    let want = simple_rows(&d, q).await;
    assert_eq!(simple_rows(&c, q).await, want);
    assert_eq!(
        p.hits(),
        1,
        "served from the disk tier after restart (and validated)"
    );
    // ... and still invalidated by writes made while it was down.
    p.app.shutdown().await;
    drop(p);
    d.execute("UPDATE c_disk SET amount = 0 WHERE id = 1", &[])
        .await
        .unwrap();
    let p = proxy_from(cfg, Some(dir)).await;
    let c = p.client().await;
    assert_eq!(simple_rows(&c, q).await, simple_rows(&d, q).await);
    assert_eq!(
        p.hits(),
        0,
        "a write while the proxy was down invalidates the stored result"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_mode_compares_hits_with_postgres() {
    let p = proxy_with(|c| c.cache.verify_sample_rate = 1.0).await;
    let d = direct().await;
    table(&d, "c_verify", 30, true).await;
    let c = p.client().await;
    let q = "SELECT grp, sum(v) FROM c_verify GROUP BY grp";
    let want = simple_rows(&d, q).await;
    simple_rows(&c, q).await;
    p.wait_fills(1).await;
    for _ in 0..3 {
        assert_eq!(simple_rows(&c, q).await, want);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let s = &p.app.shared.stats;
    assert_eq!(s.verify_ok.load(Ordering::Relaxed), 3);
    assert_eq!(s.verify_mismatch.load(Ordering::Relaxed), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn kill_switch() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "c_kill", 10, true).await;
    let c = p.client().await;
    let q = "SELECT sum(v) FROM c_kill";
    simple_rows(&c, q).await;
    p.wait_fills(1).await;
    p.app.shared.enabled.store(false, Ordering::Relaxed);
    simple_rows(&c, q).await;
    assert_eq!(p.hits(), 0);
    p.app.shared.enabled.store(true, Ordering::Relaxed);
    simple_rows(&c, q).await;
    assert_eq!(p.hits(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_misses_run_once() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "c_herd", 10, true).await;
    // Slow query: everyone arrives while the first is still running.
    let q = "SELECT count(*), pg_catalog.max(v) FROM c_herd, generate_series(1, 300000) g";
    let mut clients = Vec::new();
    for _ in 0..8 {
        clients.push(p.client().await);
    }
    // Warm each session's settings so all are cache-eligible.
    for c in &clients {
        simple_rows(c, "SELECT 1").await;
    }
    let futs = clients.iter().map(|c| simple_rows(c, q));
    let results = futures::future::join_all(futs).await;
    assert!(results.windows(2).all(|w| w[0] == w[1]));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (f, h) = (p.fills(), p.hits());
    let bypass = format!("{:?}", p.app.shared.stats.invalidated);
    assert_eq!((f, h), (1, 7), "one session should run the query and the others reuse it (fills, hits); invalidated={bypass}");
}
