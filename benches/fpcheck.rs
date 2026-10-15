// Pairs whose classification differs must have different fingerprints for the shape memo to be safe.
fn main() {
    let pairs = [
        ("SELECT now() FROM t", "SELECT lower(x) FROM t"),
        ("SELECT CURRENT_DATE FROM t", "SELECT CURRENT_USER FROM t"),
        ("SELECT CURRENT_TIMESTAMP FROM t", "SELECT CURRENT_SCHEMA FROM t"),
        ("SELECT a FROM t FOR UPDATE", "SELECT a FROM t"),
        ("SELECT a FROM t FOR UPDATE", "SELECT a FROM t FOR SHARE"),
        ("SELECT a FROM t TABLESAMPLE system(1)", "SELECT a FROM t"),
        ("SELECT a INTO x FROM t", "SELECT a FROM t"),
        ("SELECT a FROM t", "SELECT a FROM u"),
        ("SELECT a FROM s.t", "SELECT a FROM t"),
        ("SELECT a FROM s.t", "SELECT a FROM r.t"),
        ("SELECT pg_catalog.lower(a) FROM t", "SELECT public.lower(a) FROM t"),
        ("SELECT lower(a) FROM t", "SELECT upper(a) FROM t"),
        ("SELECT a FROM t WHERE a = random()", "SELECT a FROM t WHERE a = abs(1)"),
        ("WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d", "WITH d AS (SELECT * FROM t) SELECT * FROM d"),
        ("SELECT DISTINCT ON (now()) a FROM t", "SELECT DISTINCT ON (lower(a)) a FROM t"),
        ("SELECT a FROM db.s.t", "SELECT a FROM s.t"),
        ("SELECT nextval('s') FROM t", "SELECT lower('s') FROM t"),
        ("SELECT a FROM t WHERE a = $1", "SELECT a FROM t WHERE a = 1"),
    ];
    let mut bad = 0;
    for (a, b) in pairs {
        let (fa, fb) = (pg_query::fingerprint(a).unwrap().value, pg_query::fingerprint(b).unwrap().value);
        if fa == fb {
            println!("COLLISION: {a:?} vs {b:?}");
            bad += 1;
        }
    }
    // Same shape, different constants: must collide (that is the point).
    let same = pg_query::fingerprint("SELECT * FROM t WHERE id = 5").unwrap().value == pg_query::fingerprint("SELECT * FROM t WHERE id = 99").unwrap().value;
    println!("collisions among must-differ pairs: {bad}; constants ignored: {same}");
}
