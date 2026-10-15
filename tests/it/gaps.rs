//! Every way data or meaning can change without an ordinary write going through a
//! trigger. Each must make the cached result unusable.

use crate::common::*;

/// Run `q` through the proxy until it is served from cache.
pub async fn warm(p: &Proxy, c: &tokio_postgres::Client, q: &str) {
    for _ in 0..3 {
        let (hits, fills) = (p.hits(), p.fills());
        simple_rows(c, q).await;
        if p.hits() > hits {
            return;
        }
        if p.fills() == fills {
            // the fill is stored in the background; give it a moment
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while p.fills() == fills && std::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
    }
    panic!("warm-up never produced a cache hit: {q}");
}

/// The proxy's answer must equal Postgres's answer right now.
async fn agrees(p: &Proxy, c: &tokio_postgres::Client, d: &tokio_postgres::Client, q: &str, what: &str) {
    let hits = p.hits();
    let got = simple_rows(c, q).await;
    let want = simple_rows(d, q).await;
    assert_eq!(got, want, "{what}: proxy returned a different result than Postgres");
    assert_eq!(p.hits(), hits, "{what}: must not be a cache hit");
}

#[tokio::test(flavor = "multi_thread")]
async fn in_progress_writer_commits_after_fill() {
    // The subtle case: a write in progress when the result was cached, committing later.
    let p = proxy().await;
    let d = direct().await;
    table(&d, "g_xip", 10, true).await;
    let c = p.client().await;
    let w = direct().await;
    w.batch_execute("BEGIN; UPDATE g_xip SET v = 999 WHERE id = 1").await.unwrap();
    let q = "SELECT v FROM g_xip WHERE id = 1";
    warm(&p, &c, q).await; // cached while the writer is still open
    assert_eq!(simple_rows(&c, q).await[0][0].as_deref(), Some("1"), "uncommitted write not visible");
    w.batch_execute("COMMIT").await.unwrap();
    agrees(&p, &c, &d, q, "writer committed after the result was cached").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_changes() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "g_ddl", 10, true).await;
    let c = p.client().await;
    let q = "SELECT * FROM g_ddl ORDER BY id";
    warm(&p, &c, q).await;
    d.batch_execute("ALTER TABLE g_ddl ADD COLUMN extra int DEFAULT 7").await.unwrap();
    agrees(&p, &c, &d, q, "ADD COLUMN").await;
    warm(&p, &c, q).await;
    d.batch_execute("ALTER TABLE g_ddl DROP COLUMN extra").await.unwrap();
    agrees(&p, &c, &d, q, "DROP COLUMN").await;
    warm(&p, &c, q).await;
    d.batch_execute("ALTER TABLE g_ddl RENAME COLUMN grp TO grp2").await.unwrap();
    agrees(&p, &c, &d, q, "RENAME COLUMN").await;
    warm(&p, &c, q).await;
    d.batch_execute("ALTER TABLE g_ddl ALTER COLUMN v TYPE numeric").await.unwrap();
    agrees(&p, &c, &d, q, "ALTER COLUMN TYPE").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn trigger_tampering_and_replica_mode() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "g_trig", 10, true).await;
    let c = p.client().await;
    let q = "SELECT sum(v) FROM g_trig";
    warm(&p, &c, q).await;
    // Writes while the capture trigger is disabled are invisible to the changelog...
    d.batch_execute("ALTER TABLE g_trig DISABLE TRIGGER pgshadow_capture; UPDATE g_trig SET v = 0; ALTER TABLE g_trig ENABLE ALWAYS TRIGGER pgshadow_capture")
        .await
        .unwrap();
    // ... but the trigger change itself invalidates.
    agrees(&p, &c, &d, q, "write with the trigger disabled").await;
    warm(&p, &c, q).await;
    d.batch_execute("SET session_replication_role = replica; UPDATE g_trig SET v = 5; RESET session_replication_role")
        .await
        .unwrap();
    agrees(&p, &c, &d, q, "write under session_replication_role = replica").await;
    warm(&p, &c, q).await;
    d.batch_execute("ALTER TABLE g_trig DISABLE TRIGGER pgshadow_capture").await.unwrap();
    agrees(&p, &c, &d, q, "trigger left disabled").await;
    agrees(&p, &c, &d, q, "trigger left disabled (still no caching)").await;
    d.batch_execute("ALTER TABLE g_trig ENABLE ALWAYS TRIGGER pgshadow_capture").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn name_resolution_and_session_settings() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "g_names", 10, true).await;
    d.batch_execute("DROP SCHEMA IF EXISTS g_other CASCADE; CREATE SCHEMA g_other;").await.unwrap();
    let c = p.client().await;
    let q = "SELECT count(*) FROM g_names";
    warm(&p, &c, q).await;
    // A table of the same name appears earlier in the search path.
    c.batch_execute("SET search_path = g_other, public").await.unwrap();
    d.batch_execute("CREATE TABLE g_other.g_names (id int); INSERT INTO g_other.g_names VALUES (1)").await.unwrap();
    let got = simple_rows(&c, q).await;
    assert_eq!(got[0][0].as_deref(), Some("1"), "must read g_other.g_names now");
    c.batch_execute("RESET search_path").await.unwrap();
    // ... and the same thing created later, while a cached entry exists for a session
    // whose search path (from its role) already lists g_other first.
    d.batch_execute(
        "DROP TABLE g_other.g_names;
         DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgs_path') THEN CREATE ROLE pgs_path LOGIN; END IF; END $$;
         ALTER ROLE pgs_path SET search_path = g_other, public;
         GRANT USAGE ON SCHEMA g_other TO pgs_path; GRANT SELECT ON g_names TO pgs_path;",
    )
    .await
    .unwrap();
    let c2 = connect_dsn(&format!("host=127.0.0.1 port={} user=pgs_path dbname=shadow", p.port)).await;
    warm(&p, &c2, q).await;
    d.batch_execute("CREATE TABLE g_other.g_names (id int); GRANT SELECT ON g_other.g_names TO pgs_path").await.unwrap();
    let got = simple_rows(&c2, q).await;
    assert_eq!(got[0][0].as_deref(), Some("0"), "must read the new, empty g_other.g_names");

    // Temp table shadowing the cached one.
    warm(&p, &c, q).await;
    c.batch_execute("CREATE TEMP TABLE g_names (id int)").await.unwrap();
    assert_eq!(simple_rows(&c, q).await[0][0].as_deref(), Some("0"), "temp table shadows it");
    c.batch_execute("DROP TABLE pg_temp.g_names").await.unwrap();

    // Settings that change output format.
    let q2 = "SELECT at FROM g_names WHERE id = 1";
    warm(&p, &c, q2).await;
    c.batch_execute("SET TimeZone = 'Asia/Tokyo'").await.unwrap();
    let hits = p.hits();
    let got = simple_rows(&c, q2).await;
    assert!(got[0][0].as_deref().unwrap().ends_with("+09"), "{got:?}");
    assert_eq!(p.hits(), hits);
}

#[tokio::test(flavor = "multi_thread")]
async fn privileges() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "g_priv", 10, true).await;
    d.batch_execute("GRANT SELECT ON g_priv TO pgs_md5").await.unwrap();
    let c = connect_dsn(&format!("host=127.0.0.1 port={} user=pgs_md5 password=md5-pass dbname=shadow", p.port)).await;
    let q = "SELECT count(*) FROM g_priv";
    warm(&p, &c, q).await;
    d.batch_execute("REVOKE SELECT ON g_priv FROM pgs_md5").await.unwrap();
    let e = c.simple_query(q).await.unwrap_err();
    assert_eq!(e.code().unwrap().code(), "42501", "permission denied, as Postgres says");
    // Another role never sees results cached for this one.
    d.batch_execute("GRANT SELECT ON g_priv TO pgs_md5").await.unwrap();
    warm(&p, &c, q).await;
    let other = connect_dsn(&format!("host=127.0.0.1 port={} user=pgs_scram password=scram-pass dbname=shadow", p.port)).await;
    let e = other.simple_query(q).await.unwrap_err();
    assert_eq!(e.code().unwrap().code(), "42501");
}

#[tokio::test(flavor = "multi_thread")]
async fn cascades_and_ctes() {
    let p = proxy().await;
    let d = direct().await;
    d.batch_execute(
        "DROP TABLE IF EXISTS g_child, g_parent;
         CREATE TABLE g_parent (id int PRIMARY KEY);
         CREATE TABLE g_child (id int PRIMARY KEY, parent int REFERENCES g_parent ON DELETE CASCADE);
         INSERT INTO g_parent SELECT generate_series(1, 5);
         INSERT INTO g_child SELECT g, 1 + g % 5 FROM generate_series(1, 20) g;
         SELECT pgshadow.enable('g_parent'); SELECT pgshadow.enable('g_child');",
    )
    .await
    .unwrap();
    let c = p.client().await;
    let q = "SELECT count(*) FROM g_child";
    warm(&p, &c, q).await;
    d.execute("DELETE FROM g_parent WHERE id = 1", &[]).await.unwrap();
    agrees(&p, &c, &d, q, "ON DELETE CASCADE into a cached table").await;

    // A CTE named like a table that it also reads.
    let q2 = "WITH g_child AS (SELECT * FROM g_child WHERE id < 10) SELECT count(*) FROM g_child";
    warm(&p, &c, q2).await;
    d.execute("DELETE FROM g_child WHERE id = 2", &[]).await.unwrap();
    agrees(&p, &c, &d, q2, "CTE shadowing a cached table").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn changelog_trimming() {
    // Trimming with no retention invalidates everything in the database, so this test
    // gets a database of its own.
    let dsn = isolated_db("shadow_trim").await;
    let p = proxy_with(|c| c.check.dsn = dsn.clone()).await;
    let d = connect_dsn(&dsn).await;
    table(&d, "g_trim", 10, true).await;
    let c = p.client_db("shadow_trim").await;
    let q = "SELECT sum(v) FROM g_trim";
    // A writer in progress while the result is cached, then trimmed away after it commits.
    let w = connect_dsn(&dsn).await;
    w.batch_execute("BEGIN; UPDATE g_trim SET v = 0 WHERE id = 1").await.unwrap();
    warm(&p, &c, q).await;
    w.batch_execute("COMMIT").await.unwrap();
    d.batch_execute("SELECT pgshadow.trim('0 seconds')").await.unwrap();
    agrees(&p, &c, &d, q, "change record trimmed after commit").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tables_disabled_or_dropped() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "g_drop", 10, true).await;
    let c = p.client().await;
    let q = "SELECT count(*) FROM g_drop";
    warm(&p, &c, q).await;
    d.batch_execute("SELECT pgshadow.disable('g_drop'); DELETE FROM g_drop WHERE id = 1").await.unwrap();
    agrees(&p, &c, &d, q, "table opted out, then written").await;
    d.batch_execute("SELECT pgshadow.enable('g_drop')").await.unwrap();
    warm(&p, &c, q).await;
    d.batch_execute("DROP TABLE g_drop; CREATE TABLE g_drop (id int); SELECT pgshadow.enable('g_drop')").await.unwrap();
    agrees(&p, &c, &d, q, "table dropped and recreated").await;
}
