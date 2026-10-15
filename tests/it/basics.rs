//! Everything the proxy does not cache must behave exactly as without it.

use std::time::Duration;

use crate::common::*;

#[tokio::test(flavor = "multi_thread")]
async fn queries_errors_and_transactions_pass_through() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "b_basic", 10, true).await;
    let c = p.client().await;

    // simple and extended protocol, parameters, NULLs
    assert_eq!(simple_rows(&c, "SELECT 1, NULL, 'x'").await, vec![vec![Some("1".into()), None, Some("x".into())]]);
    let r = c.query_one("SELECT $1::int + 1, $2::text", &[&41i32, &"hi"]).await.unwrap();
    assert_eq!((r.get::<_, i32>(0), r.get::<_, String>(1)), (42, "hi".into()));

    // errors carry the original SQLSTATE and the session keeps working
    let e = c.simple_query("SELEC 1").await.unwrap_err();
    assert_eq!(e.code().unwrap().code(), "42601");
    let e = c.query("SELECT 1/0", &[]).await.unwrap_err();
    assert_eq!(e.code().unwrap().code(), "22012");
    assert_eq!(simple_rows(&c, "SELECT 2").await, vec![vec![Some("2".into())]]);

    // transactions: rollback discards, commit keeps, failed transaction reports 25P02
    c.batch_execute("BEGIN; UPDATE b_basic SET v = -1 WHERE id = 1; ROLLBACK").await.unwrap();
    assert_eq!(d.query_one("SELECT v FROM b_basic WHERE id = 1", &[]).await.unwrap().get::<_, i64>(0), 1);
    c.batch_execute("BEGIN; UPDATE b_basic SET v = 100 WHERE id = 1; COMMIT").await.unwrap();
    assert_eq!(d.query_one("SELECT v FROM b_basic WHERE id = 1", &[]).await.unwrap().get::<_, i64>(0), 100);
    c.batch_execute("BEGIN").await.unwrap();
    assert!(c.simple_query("SELECT 1/0").await.is_err());
    let e = c.simple_query("SELECT 1").await.unwrap_err();
    assert_eq!(e.code().unwrap().code(), "25P02");
    c.batch_execute("ROLLBACK").await.unwrap();

    // multi-statement strings
    let msgs = c.simple_query("SELECT 1; SELECT 2; SELECT 3").await.unwrap();
    let rows = msgs.iter().filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))).count();
    assert_eq!(rows, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_in_and_out() {
    use futures::{SinkExt, StreamExt};
    let p = proxy().await;
    let d = direct().await;
    d.batch_execute("DROP TABLE IF EXISTS b_copy; CREATE TABLE b_copy (id int, t text)").await.unwrap();
    let c = p.client().await;
    let sink = c.copy_in("COPY b_copy FROM STDIN").await.unwrap();
    futures::pin_mut!(sink);
    sink.send(bytes::Bytes::from_static(b"1\tone\n2\ttwo\n")).await.unwrap();
    assert_eq!(sink.finish().await.unwrap(), 2);
    let out = c.copy_out("COPY (SELECT * FROM b_copy ORDER BY id) TO STDOUT").await.unwrap();
    futures::pin_mut!(out);
    let mut all = Vec::new();
    while let Some(chunk) = out.next().await {
        all.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(all, b"1\tone\n2\ttwo\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_requests_reach_postgres() {
    let p = proxy().await;
    let c = p.client().await;
    let token = c.cancel_token();
    let q = tokio::spawn(async move { c.simple_query("SELECT pg_sleep(20)").await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel_query(tokio_postgres::NoTls).await.unwrap();
    let res = tokio::time::timeout(Duration::from_secs(5), q).await.expect("query was not cancelled").unwrap();
    assert_eq!(res.unwrap_err().code().unwrap().code(), "57014");
}

#[tokio::test(flavor = "multi_thread")]
async fn password_logins_are_relayed() {
    let p = proxy().await;
    for (user, pass) in [("pgs_scram", "scram-pass"), ("pgs_md5", "md5-pass")] {
        let c = connect_dsn(&format!("host=127.0.0.1 port={} user={user} password={pass} dbname=shadow", p.port)).await;
        assert_eq!(simple_rows(&c, "SELECT current_user").await[0][0].as_deref(), Some(user));
        let bad = tokio_postgres::connect(
            &format!("host=127.0.0.1 port={} user={user} password=wrong dbname=shadow", p.port),
            tokio_postgres::NoTls,
        )
        .await;
        assert_eq!(bad.err().and_then(|e| e.code().cloned()).map(|c| c.code().to_string()), Some("28P01".into()));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_on_both_sides() {
    // Client -> proxy TLS with a throwaway certificate; proxy -> Postgres TLS.
    let dir = tempfile::tempdir().unwrap();
    let (cert, key) = (dir.path().join("c.pem"), dir.path().join("k.pem"));
    let ok = std::process::Command::new("openssl")
        .args(["req", "-new", "-x509", "-days", "1", "-nodes", "-subj", "/CN=localhost", "-keyout"])
        .arg(&key)
        .arg("-out")
        .arg(&cert)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !ok {
        eprintln!("openssl not available; skipping");
        return;
    }
    let p = proxy_with(|c| {
        c.listen.tls_cert = Some(cert.to_string_lossy().into());
        c.listen.tls_key = Some(key.to_string_lossy().into());
        c.upstream.tls = pgshadow::config::TlsMode::Require;
    })
    .await;
    let tls = pgshadow::net::client_tls_config(pgshadow::config::TlsMode::Require, None).unwrap();
    let connector = tokio_postgres_rustls::MakeRustlsConnect::new((*tls).clone());
    let (c, conn) = tokio_postgres::connect(
        &format!(
            "host=localhost port={} user=pgs_scram password=scram-pass dbname=shadow sslmode=require channel_binding=disable",
            p.port
        ),
        connector,
    )
    .await
    .expect("TLS connection through the proxy");
    tokio::spawn(conn);
    // Postgres sees an encrypted connection from the proxy.
    let rows = simple_rows(&c, "SELECT ssl::text FROM pg_stat_ssl WHERE pid = pg_backend_pid()").await;
    assert_eq!(rows[0][0].as_deref(), Some("true"));
}

#[tokio::test(flavor = "multi_thread")]
async fn session_state_is_preserved() {
    let p = proxy().await;
    let c = p.client().await;
    c.batch_execute("SET application_name = 'shadow-test'; CREATE TEMP TABLE b_tmp (x int); INSERT INTO b_tmp VALUES (7)")
        .await
        .unwrap();
    assert_eq!(simple_rows(&c, "SHOW application_name").await[0][0].as_deref(), Some("shadow-test"));
    assert_eq!(simple_rows(&c, "SELECT x FROM b_tmp").await[0][0].as_deref(), Some("7"));
    // prepared statements live across many executions
    let st = c.prepare("SELECT $1::int * 2").await.unwrap();
    for i in 0..20 {
        assert_eq!(c.query_one(&st, &[&i]).await.unwrap().get::<_, i32>(0), i * 2);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pipelined_queries_keep_their_order() {
    let p = proxy().await;
    let d = direct().await;
    table(&d, "b_pipe", 50, true).await;
    let c = p.client().await;
    // Warm the cache for one statement, then mix cached reads, uncached reads and writes
    // in a single pipeline.
    let sum = "SELECT sum(v) FROM b_pipe";
    c.query_one(sum, &[]).await.unwrap();
    p.wait_fills(1).await;
    let futs: Vec<_> = (0..30)
        .map(|i| {
            let c = &c;
            async move {
                match i % 3 {
                    0 => c.query_one(sum, &[]).await.unwrap().get::<_, rust_decimal_free::D>(0).0,
                    1 => c.query_one("SELECT $1::int8 + 0", &[&(i as i64)]).await.unwrap().get::<_, i64>(0).to_string(),
                    _ => c.execute("UPDATE b_pipe SET v = v WHERE id = 1", &[]).await.unwrap().to_string(),
                }
            }
        })
        .collect();
    let out = futures::future::join_all(futs).await;
    for (i, v) in out.iter().enumerate() {
        match i % 3 {
            1 => assert_eq!(v, &i.to_string()),
            2 => assert_eq!(v, "1"),
            _ => {}
        }
    }
}

/// numeric without a decimal crate: read the text of a numeric via its own cast.
mod rust_decimal_free {
    use tokio_postgres::types::{FromSql, Type};
    pub struct D(pub String);
    impl<'a> FromSql<'a> for D {
        fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
            Ok(D(format!("{raw:?}")))
        }
        fn accepts(_: &Type) -> bool {
            true
        }
    }
}
