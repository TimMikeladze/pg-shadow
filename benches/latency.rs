// Sequential round-trip latency through the proxy vs direct (single connection).
// PGSHADOW_DIRECT_DSN and PGSHADOW_PROXY_DSN override the default ports 5433 and 6432.
use std::time::Instant;
#[tokio::main]
async fn main() {
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    for (name, dsn) in [
        (
            "direct",
            env(
                "PGSHADOW_DIRECT_DSN",
                "host=127.0.0.1 port=5433 user=postgres dbname=shadow",
            ),
        ),
        (
            "pgshadow",
            env(
                "PGSHADOW_PROXY_DSN",
                "host=127.0.0.1 port=6432 user=postgres dbname=shadow",
            ),
        ),
    ] {
        let (c, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(conn);
        let st = c
            .prepare("SELECT id, status FROM bench_orders WHERE id = $1")
            .await
            .unwrap();
        for i in 0..500 {
            c.query_one(&st, &[&(i + 1)]).await.unwrap();
        }
        let n = 3000;
        let t = Instant::now();
        for i in 0..n {
            c.query_one(&st, &[&(i % 1000 + 1)]).await.unwrap();
        }
        let ext = t.elapsed().as_micros() as f64 / n as f64;
        let t = Instant::now();
        for _ in 0..n {
            c.simple_query("SELECT 1").await.unwrap();
        }
        let simple = t.elapsed().as_micros() as f64 / n as f64;
        println!("{name:9} prepared pk lookup {ext:6.0} us   simple SELECT 1 {simple:6.0} us");
    }
}
