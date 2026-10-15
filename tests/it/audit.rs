//! Regression tests for findings from the correctness review. Each one is a way a cached
//! result could have been served, or stored, when Postgres would have answered
//! differently.

use std::time::Duration;

use bytes::BufMut;
use pgshadow::protocol::{self, Frame, FrameReader};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::common::*;
use crate::gaps::{agrees, warm};

/// Run `q` a few times and require that nothing was cached or served from cache.
async fn never_cached(
    p: &Proxy,
    c: &tokio_postgres::Client,
    d: &tokio_postgres::Client,
    q: &str,
    what: &str,
) {
    let (hits, fills) = (p.hits(), p.fills());
    for _ in 0..3 {
        assert_eq!(
            simple_rows(c, q).await,
            simple_rows(d, q).await,
            "{what}: differs from Postgres"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert_eq!(
        (p.hits(), p.fills()),
        (hits, fills),
        "{what}: must not be cached"
    );
}

fn code(e: &tokio_postgres::Error) -> String {
    e.code()
        .map(|c| c.code().to_string())
        .unwrap_or_else(|| format!("{e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn partitions_and_inheritance_children_are_refused() {
    // Writes through a parent fire the parent's statement trigger, not the child's, so
    // a child (or a partitioned parent) can never be opted in.
    let d = direct().await;
    d.batch_execute(
        "DROP TABLE IF EXISTS a_part, a_inh_child, a_inh_parent CASCADE;
         CREATE TABLE a_part (id int, k int) PARTITION BY RANGE (k);
         CREATE TABLE a_part_1 PARTITION OF a_part FOR VALUES FROM (0) TO (100);
         CREATE TABLE a_inh_parent (id int);
         CREATE TABLE a_inh_child () INHERITS (a_inh_parent);",
    )
    .await
    .unwrap();
    d.batch_execute("DROP TABLE IF EXISTS a_unlogged; CREATE UNLOGGED TABLE a_unlogged (id int)")
        .await
        .unwrap();
    for t in [
        "a_part",
        "a_part_1",
        "a_inh_parent",
        "a_inh_child",
        "a_unlogged",
    ] {
        let r = d
            .execute("SELECT pgshadow.enable($1::text::regclass)", &[&t])
            .await;
        assert!(r.is_err(), "{t} must not be accepted");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn user_defined_functions_operators_and_types() {
    // Creating functions and types changes the database's catalog epoch, which
    // invalidates everything cached in it: own database.
    let dsn = isolated_db("shadow_epoch").await;
    let p = proxy_with(|c| c.check.dsn = dsn.clone()).await;
    let d = connect_dsn(&dsn).await;
    d.batch_execute(
        "DROP FUNCTION IF EXISTS public.e_f(); DROP FUNCTION IF EXISTS public.lower(varchar);
         DROP OPERATOR IF EXISTS === (int, int); DROP TABLE IF EXISTS e_en; DROP TYPE IF EXISTS e_mood;",
    )
    .await
    .unwrap();
    table(&d, "e_t", 10, true).await;
    let c = p.client_db("shadow_epoch").await;

    // Any new function invalidates (it could change what the same SQL means).
    let q = "SELECT count(*) FROM e_t WHERE grp = 'g1'";
    warm(&p, &c, q).await;
    d.batch_execute("CREATE FUNCTION public.e_f() RETURNS int LANGUAGE sql AS 'SELECT 1'")
        .await
        .unwrap();
    agrees(&p, &c, &d, q, "user function created").await;

    // A user function that takes over a built-in's name is never cached.
    d.batch_execute(
        "ALTER TABLE e_t ADD COLUMN name varchar DEFAULT 'ABC';
         CREATE FUNCTION public.lower(varchar) RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT 'shadowed'$$;",
    )
    .await
    .unwrap();
    never_cached(
        &p,
        &c,
        &d,
        "SELECT lower(name) FROM e_t WHERE id = 1",
        "shadowed lower()",
    )
    .await;

    // Same for operators.
    d.batch_execute("CREATE OPERATOR === (LEFTARG = int, RIGHTARG = int, FUNCTION = int4eq)")
        .await
        .unwrap();
    never_cached(
        &p,
        &c,
        &d,
        "SELECT count(*) FROM e_t WHERE id === 1",
        "user operator",
    )
    .await;

    // Enum labels are part of the table's fingerprint.
    d.batch_execute(
        "CREATE TYPE e_mood AS ENUM ('happy', 'sad');
         CREATE TABLE e_en (id int PRIMARY KEY, m e_mood); INSERT INTO e_en VALUES (1, 'happy'), (2, 'sad');
         SELECT pgshadow.enable('e_en');",
    )
    .await
    .unwrap();
    let q = "SELECT m FROM e_en ORDER BY id";
    warm(&p, &c, q).await;
    d.batch_execute("ALTER TYPE e_mood RENAME VALUE 'happy' TO 'glad'")
        .await
        .unwrap();
    agrees(&p, &c, &d, q, "enum value renamed").await;

    // Attributes of a composite column type.
    d.batch_execute(
        "DROP TABLE IF EXISTS e_co; DROP TYPE IF EXISTS e_pair;
         CREATE TYPE e_pair AS (x int, y int);
         CREATE TABLE e_co (id int PRIMARY KEY, p e_pair); INSERT INTO e_co VALUES (1, ROW(1, 2));
         SELECT pgshadow.enable('e_co');",
    )
    .await
    .unwrap();
    let q = "SELECT to_jsonb(p) FROM e_co";
    warm(&p, &c, q).await;
    d.batch_execute("ALTER TYPE e_pair RENAME ATTRIBUTE x TO renamed")
        .await
        .unwrap();
    agrees(&p, &c, &d, q, "composite attribute renamed").await;

    // User-defined collations: created later (epoch), and used by name.
    let q = "SELECT grp FROM e_t ORDER BY grp, id";
    warm(&p, &c, q).await;
    d.batch_execute(
        "DROP COLLATION IF EXISTS public.e_coll; CREATE COLLATION public.e_coll (locale = 'C')",
    )
    .await
    .unwrap();
    agrees(&p, &c, &d, q, "collation created").await;
    never_cached(
        &p,
        &c,
        &d,
        "SELECT grp FROM e_t ORDER BY grp COLLATE e_coll, id",
        "user collation",
    )
    .await;

    // Privileges of a built-in function revoked: Postgres refuses, so must the proxy.
    d.batch_execute("GRANT SELECT ON e_t TO pgs_md5; UPDATE pg_proc SET proacl = NULL WHERE oid = 'upper(text)'::regprocedure")
        .await
        .unwrap();
    let m = connect_dsn(&format!(
        "host=127.0.0.1 port={} user=pgs_md5 password=md5-pass dbname=shadow_epoch",
        p.port
    ))
    .await;
    let q = "SELECT upper(grp) FROM e_t WHERE id = 1";
    warm(&p, &m, q).await;
    d.batch_execute("REVOKE EXECUTE ON FUNCTION upper(text) FROM PUBLIC")
        .await
        .unwrap();
    let e = m.simple_query(q).await.unwrap_err();
    assert_eq!(code(&e), "42501", "EXECUTE revoked");
    d.batch_execute("UPDATE pg_proc SET proacl = NULL WHERE oid = 'upper(text)'::regprocedure")
        .await
        .unwrap();

    // The server's boot token changes (crash recovery, failover, restore): nothing
    // cached before is used.
    let q = "SELECT count(*) FROM e_t";
    warm(&p, &c, q).await;
    d.batch_execute("DELETE FROM pgshadow.boot").await.unwrap();
    agrees(&p, &c, &d, q, "server rebooted").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_and_setting_changes_hidden_in_reads() {
    // A user-defined operator (or view, trigger, ...) can run code that writes or changes
    // settings inside what looks like a plain SELECT. Own database: creating operators
    // changes the catalog epoch.
    let dsn = isolated_db("shadow_hidden").await;
    let p = proxy_with(|c| c.check.dsn = dsn.clone()).await;
    let d = connect_dsn(&dsn).await;
    d.batch_execute(
        "DROP SCHEMA IF EXISTS h_a, h_b CASCADE; DROP VIEW IF EXISTS h_sp; DROP OPERATOR IF EXISTS === (int, int);
         DROP FUNCTION IF EXISTS h_bump(int, int);
         CREATE SCHEMA h_a; CREATE SCHEMA h_b;",
    )
    .await
    .unwrap();
    table(&d, "h_w", 3, true).await;
    d.batch_execute(
        "CREATE FUNCTION h_bump(int, int) RETURNS boolean LANGUAGE sql VOLATILE AS
           $$ UPDATE h_w SET v = v + 100 WHERE id = 1 RETURNING true $$;
         CREATE OPERATOR === (LEFTARG = int, RIGHTARG = int, FUNCTION = h_bump);
         CREATE VIEW h_sp AS SELECT set_config('search_path', 'h_b, public', false) AS sp;",
    )
    .await
    .unwrap();
    let c = p.client_db("shadow_hidden").await;
    let q = "SELECT v FROM h_w WHERE id = 1";
    warm(&p, &c, q).await;

    // Own write, hidden in an operator, inside a transaction: must be visible...
    c.batch_execute("BEGIN").await.unwrap();
    c.batch_execute("SELECT 1 === 1").await.unwrap();
    assert_eq!(
        simple_rows(&c, q).await[0][0].as_deref(),
        Some("101"),
        "own write must be visible"
    );
    assert_eq!(simple_rows(&c, q).await[0][0].as_deref(), Some("101"));
    // ... and never reach anyone else, before or after a rollback.
    let other = p.client_db("shadow_hidden").await;
    assert_eq!(
        simple_rows(&other, q).await[0][0].as_deref(),
        Some("1"),
        "uncommitted write leaked"
    );
    c.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        simple_rows(&other, q).await[0][0].as_deref(),
        Some("1"),
        "rolled-back write leaked"
    );
    assert_eq!(simple_rows(&c, q).await[0][0].as_deref(), Some("1"));

    // A view that changes search_path: the next query must resolve names with it.
    d.batch_execute(
        "CREATE TABLE h_a.h_t (v text); INSERT INTO h_a.h_t VALUES ('A'); SELECT pgshadow.enable('h_a.h_t');
         CREATE TABLE h_b.h_t (v text); INSERT INTO h_b.h_t VALUES ('B'); SELECT pgshadow.enable('h_b.h_t');",
    )
    .await
    .unwrap();
    c.batch_execute("SET search_path = h_a, public")
        .await
        .unwrap();
    let q = "SELECT v FROM h_t";
    warm(&p, &c, q).await;
    c.batch_execute("SELECT * FROM h_sp").await.unwrap();
    assert_eq!(
        simple_rows(&c, q).await[0][0].as_deref(),
        Some("B"),
        "search_path changed by a view"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn text_cast_to_dates_and_prepared_statement_shapes() {
    let p = proxy().await;
    let d = direct().await;
    d.batch_execute(
        "DROP TABLE IF EXISTS a_tc; CREATE TABLE a_tc (id int PRIMARY KEY, s text, j jsonb, ts timestamptz);
         INSERT INTO a_tc VALUES (1, 'now', '{\"ts\": \"now\"}', '2026-01-01 00:00+00');
         SELECT pgshadow.enable('a_tc');",
    )
    .await
    .unwrap();
    let c = p.client().await;
    for q in [
        "SELECT s::timestamptz FROM a_tc",
        "SELECT (j->>'ts')::timestamptz FROM a_tc",
    ] {
        let (hits, fills) = (p.hits(), p.fills());
        for _ in 0..3 {
            simple_rows(&c, q).await;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(
            (p.hits(), p.fills()),
            (hits, fills),
            "must not be cached: {q}"
        );
    }
    warm(&p, &c, "SELECT ts::date FROM a_tc").await; // a timestamptz column is fine

    // A statement prepared before a table change, executed after another session cached
    // the new shape: Postgres refuses ("cached plan must not change result type").
    table(&d, "a_ps", 5, true).await;
    let a = p.client().await;
    let q = "SELECT * FROM a_ps WHERE id = 1";
    let stmt = a.prepare(q).await.unwrap();
    a.query(&stmt, &[]).await.unwrap();
    d.batch_execute("ALTER TABLE a_ps ADD COLUMN extra int DEFAULT 1")
        .await
        .unwrap();
    let other = p.client().await;
    for _ in 0..3 {
        other.query(q, &[]).await.unwrap();
    }
    let direct_a = direct().await;
    let dstmt = direct_a.prepare(q).await.unwrap();
    direct_a.query(&dstmt, &[]).await.unwrap();
    d.batch_execute("ALTER TABLE a_ps ADD COLUMN extra2 int DEFAULT 2")
        .await
        .unwrap();
    let want = direct_a.query(&dstmt, &[]).await.err().map(|e| code(&e));
    let stmt2 = a.prepare(q).await.unwrap(); // a fresh statement before the second change
    let _ = stmt2;
    for _ in 0..3 {
        other.query(q, &[]).await.unwrap();
    }
    assert_eq!(
        want.as_deref(),
        Some("0A000"),
        "Postgres refuses the stale statement"
    );
    let got = a.query(&stmt, &[]).await.err().map(|e| code(&e));
    assert_eq!(
        got, want,
        "a statement prepared before the change must behave as on Postgres"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn clock_dependent_parameters_of_any_length() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "a_tp", 10, true).await;
    let c = p.client().await;
    let (hits, fills) = (p.hits(), p.fills());
    for v in [
        "today 10:00",
        "  Yesterday 23:59:59.999999 +00:00",
        "tomorrow",
    ] {
        for _ in 0..3 {
            c.query(
                "SELECT count(*) FROM a_tp WHERE at > $1::text::timestamptz",
                &[&v],
            )
            .await
            .unwrap();
        }
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!((p.hits(), p.fills()), (hits, fills));
}

#[tokio::test(flavor = "multi_thread")]
async fn schemas_renamed_created_or_revoked() {
    let p = proxy().await;
    let d = direct().await;
    d.batch_execute(
        "DROP SCHEMA IF EXISTS a_sw, a_sw_new, a_sw_old, a_later, a_us CASCADE;
         CREATE SCHEMA a_sw; CREATE SCHEMA a_sw_new; CREATE SCHEMA a_us;",
    )
    .await
    .unwrap();
    table(&d, "a_sw.t", 10, true).await;
    table(&d, "a_sw_new.t", 1, true).await;
    let c = p.client().await;

    // A schema-qualified name that comes to mean another table.
    let q = "SELECT count(*) FROM a_sw.t";
    warm(&p, &c, q).await;
    d.batch_execute("ALTER SCHEMA a_sw RENAME TO a_sw_old; ALTER SCHEMA a_sw_new RENAME TO a_sw")
        .await
        .unwrap();
    agrees(&p, &c, &d, q, "schemas swapped").await;

    // A schema listed in search_path that does not exist yet, then appears.
    table(&d, "a_lt", 10, true).await;
    c.batch_execute("SET search_path = a_later, public")
        .await
        .unwrap();
    let q = "SELECT count(*) FROM a_lt";
    warm(&p, &c, q).await;
    d.batch_execute("CREATE SCHEMA a_later; CREATE TABLE a_later.a_lt (id int)")
        .await
        .unwrap();
    assert_eq!(
        simple_rows(&c, q).await[0][0].as_deref(),
        Some("0"),
        "must read a_later.a_lt now"
    );
    c.batch_execute("RESET search_path").await.unwrap();

    // Schema USAGE revoked: Postgres would refuse the query.
    table(&d, "a_us.t", 10, true).await;
    d.batch_execute("GRANT USAGE ON SCHEMA a_us TO pgs_md5; GRANT SELECT ON a_us.t TO pgs_md5")
        .await
        .unwrap();
    let m = connect_dsn(&format!(
        "host=127.0.0.1 port={} user=pgs_md5 password=md5-pass dbname=shadow",
        p.port
    ))
    .await;
    let q = "SELECT count(*) FROM a_us.t";
    warm(&p, &m, q).await;
    d.batch_execute("REVOKE USAGE ON SCHEMA a_us FROM pgs_md5")
        .await
        .unwrap();
    let e = m.simple_query(q).await.unwrap_err();
    assert_eq!(code(&e), "42501");
}

#[tokio::test(flavor = "multi_thread")]
async fn transaction_boundaries_inside_one_message() {
    // "COMMIT; BEGIN ISOLATION LEVEL REPEATABLE READ" sent as one query: the new
    // transaction is REPEATABLE READ, so its (older) snapshot must neither be served from
    // nor stored in the cache.
    let p = proxy().await;
    let d = direct().await;
    table(&d, "a_rr", 10, true).await;
    let c = p.client().await;
    let q = "SELECT v FROM a_rr WHERE id = 1";
    warm(&p, &c, q).await;
    c.batch_execute("BEGIN").await.unwrap();
    c.batch_execute("COMMIT; BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .unwrap();
    assert_eq!(simple_rows(&c, q).await[0][0].as_deref(), Some("1"));
    d.batch_execute("UPDATE a_rr SET v = 777 WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(
        simple_rows(&c, q).await[0][0].as_deref(),
        Some("1"),
        "repeatable read keeps its snapshot"
    );
    c.batch_execute("COMMIT").await.unwrap();
    let other = p.client().await;
    for client in [&other, &c] {
        assert_eq!(
            simple_rows(client, q).await[0][0].as_deref(),
            Some("777"),
            "old snapshot was stored"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn hits_inside_transactions_take_table_locks() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "a_lk", 10, true).await;
    let c = p.client().await;
    let q = "SELECT count(*) FROM a_lk";
    warm(&p, &c, q).await;

    // The read holds its lock until the transaction ends, as it would on Postgres.
    c.batch_execute("BEGIN").await.unwrap();
    let hits = p.hits();
    assert_eq!(simple_rows(&c, q).await[0][0].as_deref(), Some("10"));
    assert!(p.hits() > hits, "served from cache");
    let e = d
        .batch_execute("SET lock_timeout = '200ms'; TRUNCATE a_lk")
        .await
        .unwrap_err();
    assert_eq!(
        code(&e),
        "55P03",
        "TRUNCATE must wait for the reading transaction"
    );
    c.batch_execute("COMMIT").await.unwrap();
    d.batch_execute("RESET lock_timeout").await.unwrap();

    // Lock not available: the client gets Postgres's own error.
    table(&d, "a_lk2", 10, true).await;
    let q = "SELECT count(*) FROM a_lk2";
    c.batch_execute("SET lock_timeout = '150ms'").await.unwrap();
    warm(&p, &c, q).await;
    let holder = direct().await;
    holder
        .batch_execute("BEGIN; LOCK TABLE a_lk2 IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    c.batch_execute("BEGIN").await.unwrap();
    let e = c.simple_query(q).await.unwrap_err();
    assert_eq!(code(&e), "55P03");
    c.batch_execute("ROLLBACK; RESET lock_timeout; SET statement_timeout = '150ms'")
        .await
        .unwrap();

    // Timed out (a cancel) while waiting: the same.
    warm(&p, &c, q).await;
    c.batch_execute("BEGIN").await.unwrap();
    let e = c.simple_query(q).await.unwrap_err();
    assert_eq!(code(&e), "57014");
    c.batch_execute("ROLLBACK; RESET statement_timeout")
        .await
        .unwrap();
    holder.batch_execute("ROLLBACK").await.unwrap();
    // The session is still in good shape.
    assert_eq!(simple_rows(&c, q).await[0][0].as_deref(), Some("10"));
}

#[tokio::test(flavor = "multi_thread")]
async fn other_databases_are_not_cached() {
    // The check connection is in "shadow"; a session in another database must never use
    // (or fill) the cache, even with an identically named, enabled table there.
    let dsn = isolated_db("shadow_otherdb").await;
    let od = connect_dsn(&dsn).await;
    table(&od, "a_db", 10, true).await;
    let p = proxy().await;
    let d = direct().await;
    table(&d, "a_db", 3, true).await;
    let c = p.client_db("shadow_otherdb").await;
    never_cached(
        &p,
        &c,
        &od,
        "SELECT count(*) FROM a_db",
        "session in another database",
    )
    .await;
}

// ---- a minimal raw protocol client, for message sequences drivers do not produce ----

struct RawConn {
    rd: FrameReader<OwnedReadHalf>,
    w: OwnedWriteHalf,
}

impl RawConn {
    async fn connect(port: u16) -> Self {
        let s = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let (r, mut w) = s.into_split();
        let mut body = bytes::BytesMut::new();
        body.put_i32(196608);
        for (k, v) in [("user", "postgres"), ("database", "shadow")] {
            body.extend_from_slice(k.as_bytes());
            body.put_u8(0);
            body.extend_from_slice(v.as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0);
        let mut pkt = bytes::BytesMut::new();
        pkt.put_i32(body.len() as i32 + 4);
        pkt.extend_from_slice(&body);
        w.write_all(&pkt).await.unwrap();
        let mut c = RawConn {
            rd: FrameReader::new(r),
            w,
        };
        let first = c.until_ready().await;
        assert!(!first.iter().any(|f| f.tag == b'E'), "startup failed");
        c
    }

    async fn send(&mut self, frames: &[Frame]) {
        protocol::write_frames(&mut self.w, frames).await.unwrap();
    }

    async fn until_ready(&mut self) -> Vec<Frame> {
        let mut out = Vec::new();
        loop {
            let f = tokio::time::timeout(Duration::from_secs(10), self.rd.read_frame())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let z = f.tag == b'Z';
            out.push(f);
            if z {
                return out;
            }
        }
    }

    async fn simple(&mut self, sql: &str) -> Vec<Frame> {
        let mut b = sql.as_bytes().to_vec();
        b.push(0);
        self.send(&[Frame::new(b'Q', b)]).await;
        self.until_ready().await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_query_hit_destroys_the_unnamed_statement() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "a_un", 10, true).await;
    let mut c = RawConn::connect(p.port).await;
    let q = "SELECT count(*) FROM a_un";
    let mut hit = false;
    for _ in 0..8 {
        let hits = p.hits();
        c.simple(q).await;
        if p.hits() > hits {
            hit = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(hit, "never served from cache");
    c.send(&[protocol::parse_msg("", "SELECT 1"), protocol::sync_msg()])
        .await;
    c.until_ready().await;
    let hits = p.hits();
    c.simple(q).await;
    assert!(p.hits() > hits, "served from cache");
    // On Postgres, the simple query destroyed the unnamed statement.
    c.send(&[
        protocol::bind_msg("", "", &[]),
        protocol::execute_msg(""),
        protocol::sync_msg(),
    ])
    .await;
    let resp = c.until_ready().await;
    let err = resp
        .iter()
        .find(|f| f.tag == b'E')
        .expect("Bind of the destroyed unnamed statement must fail");
    assert_eq!(protocol::error_fields(&err.body).0, "26000");
}
