"""Markdown tables for docs/benchmarks.md.

    python3 bench/tables.py results.jsonl[,rerun.jsonl]

A row in a later file replaces the same run in an earlier one.
"""
import json
import sys


def load(paths):
    rows = [json.loads(l) for p in paths.split(",") for l in open(p) if l.strip()]
    return {(r["label"], r["target"], r["clients"], r["rate"]): r for r in rows}


def n(v, d=0):
    return f"{v:,.{d}f}"


def ms(v):
    return f"{v:.2f} ms" if v < 10 else f"{v:.1f} ms" if v < 100 else f"{v:,.0f} ms"


def main(p):
    r = load(p)
    g = lambda l, t, c=32, rate=0: r.get((l, t, c, rate))
    print("### Report query by client count\n")
    print("| clients | direct tps | pgshadow tps | direct p50 / p99 | pgshadow p50 / p99 | proxy CPU (cores) |")
    print("|---:|---:|---:|---:|---:|---:|")
    secs = None
    for c in (1, 8, 32, 64):
        d, s = g("report", "direct", c), g("report", "pgshadow", c)
        if not d:
            continue
        secs = d["transactions"] / d["tps"]
        print(f"| {c} | {n(d['tps'])} | {n(s['tps'])} | {ms(d['p50_ms'])} / {ms(d['p99_ms'])} | "
              f"{ms(s['p50_ms'])} / {ms(s['p99_ms'])} | {s.get('proxy_cpu_s', 0) / secs:.2f} |")
    print("\n### Query cost sweep, 32 clients\n")
    print("| rows aggregated | direct tps | pgshadow tps | direct p50 | pgshadow p50 |")
    print("|---:|---:|---:|---:|---:|")
    for k in (2000, 10000, 50000, 200000):
        d, s = g(f"range-{k}", "direct"), g(f"range-{k}", "pgshadow")
        if d:
            print(f"| {n(k)} | {n(d['tps'])} | {n(s['tps'])} | {ms(d['p50_ms'])} | {ms(s['p50_ms'])} |")
    print("\n### Other workloads, 32 clients unless noted\n")
    print("| workload | direct tps | pgshadow tps | direct p50 / p99 | pgshadow p50 / p99 | proxy CPU (cores) |")
    print("|---|---:|---:|---:|---:|---:|")
    for label, name, c in (("customer", "per-customer report, 50 variants", 32), ("pk", "primary-key lookup, 8 clients", 8),
                           ("pk", "primary-key lookup", 32)):
        d, s = g(label, "direct", c), g(label, "pgshadow", c)
        if d:
            print(f"| {name} | {n(d['tps'])} | {n(s['tps'])} | {ms(d['p50_ms'])} / {ms(d['p99_ms'])} | "
                  f"{ms(s['p50_ms'])} / {ms(s['p99_ms'])} | {s.get('proxy_cpu_s', 0) / (s['transactions'] / s['tps']):.2f} |")
    print("\n### Reads under a steady write rate\n")
    print("| UPDATEs/s requested | reached (direct / pgshadow) | direct tps | pgshadow tps | pgshadow hit rate | hits | fills | invalidated |")
    print("|---:|---:|---:|---:|---:|---:|---:|---:|")
    for w in (0, 1, 10, 100, 1000):
        d, s = g(f"mix-writes-{w}", "direct"), g(f"mix-writes-{w}", "pgshadow")
        if not d:
            continue
        p = s["proxy"]
        reached = f"{d.get('writer_tps', 0):,.1f} / {s.get('writer_tps', 0):,.1f}" if w else "0"
        print(f"| {n(w)} | {reached} | {n(d['tps'])} | {n(s['tps'])} | {100 * p['hits'] / max(1, p['queries']):.1f}% | "
              f"{n(p['hits'])} | {n(p['fills'])} | {n(p['invalidated'])} |")
    rate = next((k[3] for k in r if k[0] == "report-fixed-rate"), None)
    if rate:
        print(f"\n### Postgres time at {rate} requests/s, 8 clients\n")
        has_cpu = "pg_cpu_ms_per_tx" in g("report-fixed-rate", "direct", 8, rate)
        has_exec = "pg_exec_ms_per_tx" in g("report-fixed-rate", "direct", 8, rate)
        cols = (["CPU per request (direct / pgshadow)", "cores used (direct / pgshadow)"] if has_cpu else []) + \
               (["execution time per request (direct / pgshadow)"] if has_exec else [])
        print("| workload | " + " | ".join(cols) + " |")
        print("|---|" + "---:|" * len(cols))
        for label, name in (("report-fixed-rate", "report"), ("customer-fixed-rate", "per-customer report")):
            d, s = g(label, "direct", 8, rate), g(label, "pgshadow", 8, rate)
            cells = []
            if has_cpu:
                cells.append(f"{ms(d['pg_cpu_ms_per_tx'])} / {ms(s['pg_cpu_ms_per_tx'])}")
                cells.append(f"{d['pg_cpu_s'] / (d['transactions'] / d['tps']):.2f} / {s['pg_cpu_s'] / (s['transactions'] / s['tps']):.2f}")
            if has_exec:
                cells.append(f"{ms(d['pg_exec_ms_per_tx'])} / {ms(s['pg_exec_ms_per_tx'])}")
            print(f"| {name} | " + " | ".join(cells) + " |")
    print("\n### Write cost, single-row UPDATE, 8 clients, direct\n")
    print("| table | tps | p50 | Postgres time per write |")
    print("|---|---:|---:|---:|")
    for label, name in (("write-not-opted-in", "not opted in"), ("write-opted-in", "opted in")):
        d = g(label, "direct", 8)
        if d:
            t = f"{d['pg_cpu_ms_per_tx']:.3f} ms CPU" if "pg_cpu_ms_per_tx" in d else ""
            if "pg_exec_ms_per_tx" in d:
                t += (", " if t else "") + f"{d['pg_exec_ms_per_tx']:.3f} ms exec"
            print(f"| {name} | {n(d['tps'])} | {ms(d['p50_ms'])} | {t} |")
    v = g("verify", "pgshadow", 16)
    if v:
        p = v["proxy"]
        print(f"\nverify: {v['tps']:,.0f} tps, verify_ok {p['verify_ok']:,}, verify_raced {p['verify_raced']:,}, "
              f"verify_mismatch {p['verify_mismatch']:,}, writer {v.get('writer_tps')}/s")
    cpus = sorted({c for row in r.values() for c in row.get("pg_cpus", [])})
    if cpus:
        print(f"\nnum_cpus() seen across runs: {cpus}")
    print("failed transactions:", sum(row.get("failed", 0) for row in r.values()))


main(sys.argv[1])
