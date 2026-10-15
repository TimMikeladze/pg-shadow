use std::time::Instant;
fn main() {
    let sqls: Vec<String> = (0..20000)
        .map(|i| format!("SELECT * FROM bench_orders WHERE id = {i}"))
        .collect();
    let t = Instant::now();
    for s in &sqls {
        let _ = pgshadow::classify::classify(s);
    }
    println!(
        "full classify (unique text):   {:6.1} us/query",
        t.elapsed().as_micros() as f64 / sqls.len() as f64
    );
    let t = Instant::now();
    for s in &sqls {
        let _ = pg_query::parse(s).unwrap();
    }
    println!(
        "pg_query::parse only:           {:6.1} us/query",
        t.elapsed().as_micros() as f64 / sqls.len() as f64
    );
    let t = Instant::now();
    for s in &sqls {
        let _ = pg_query::fingerprint(s).unwrap();
    }
    println!(
        "pg_query::fingerprint only:     {:6.1} us/query",
        t.elapsed().as_micros() as f64 / sqls.len() as f64
    );
    let p = pg_query::parse(&sqls[0]).unwrap();
    let t = Instant::now();
    for _ in 0..20000 {
        let _ = serde_json::to_value(&p.protobuf).unwrap();
    }
    println!(
        "serde_json::to_value of tree:   {:6.1} us/query",
        t.elapsed().as_micros() as f64 / 20000.0
    );
    let c = pgshadow::classify::Classifier::new(100000);
    let one = "SELECT * FROM bench_orders WHERE id = $1";
    c.get(one);
    let t = Instant::now();
    for _ in 0..200000 {
        let _ = c.get(one);
    }
    println!(
        "memoized classify (same text):  {:6.2} us/query",
        t.elapsed().as_micros() as f64 / 200000.0
    );
}
