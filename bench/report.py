#!/usr/bin/env python3
"""Helpers for bench/blog.sh. Standard library only.

  report.py pgbench <label> <target> <clients> <rate> <stdout> <log-prefix> <pg-cpu-us> <proxy-cpu-s> <proxy-log|->
      Prints one JSON result line: tps, latency percentiles from pgbench's per-transaction
      log, Postgres and proxy CPU, and the proxy's counters summed from its summary lines.
      From the environment: EXEC_MS (Postgres execution time from pg_stat_statements),
      PG_CPUS ("<before> <after>" compute size) and WRITER_LOG (stdout of a background
      pgbench writer, for the rate it achieved).
  report.py cpu <pid>     Cumulative CPU seconds of a process (from ps).
"""
import glob
import json
import os
import re
import subprocess
import sys


def percentiles(prefix):
    lat = []
    for f in glob.glob(prefix + "*"):
        with open(f) as fh:
            for line in fh:
                parts = line.split()
                # client_id transaction_no time script_no time_epoch time_us [schedule_lag]
                if len(parts) >= 6 and parts[2].isdigit():
                    lat.append(int(parts[2]))
    if not lat:
        return {}
    lat.sort()
    pick = lambda q: round(lat[min(len(lat) - 1, int(q * len(lat)))] / 1000, 3)
    return {"p50_ms": pick(0.50), "p95_ms": pick(0.95), "p99_ms": pick(0.99), "max_ms": round(lat[-1] / 1000, 3), "logged": len(lat)}


def counters(path):
    keys = ["queries", "hits", "fills", "invalidated", "check_failed", "verify_ok", "verify_raced", "verify_mismatch", "postgres_ms_saved"]
    out = {k: 0 for k in keys}
    if path == "-":
        return {}
    with open(path) as fh:
        for line in fh:
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            if ev.get("message") != "summary":
                continue
            for k in keys:
                out[k] += int(ev.get(k, 0))
    return out


def pgbench(label, target, clients, rate, stdout, prefix, pg_cpu_us, proxy_cpu_s, proxy_log):
    text = open(stdout).read()
    tps = re.search(r"^tps = ([\d.]+)", text, re.M)
    avg = re.search(r"latency average = ([\d.]+) ms", text)
    tx = re.search(r"number of transactions actually processed: (\d+)", text)
    failed = re.search(r"number of failed transactions: (\d+)", text)
    n = int(tx.group(1)) if tx else 0
    pg_cpu_us = int(pg_cpu_us) if pg_cpu_us not in ("", "-") else None
    r = {
        "label": label,
        "target": target,
        "clients": int(clients),
        "rate": int(rate),
        "tps": round(float(tps.group(1)), 1) if tps else None,
        "lat_avg_ms": float(avg.group(1)) if avg else None,
        "transactions": n,
        "failed": int(failed.group(1)) if failed else 0,
        **percentiles(prefix),
    }
    if pg_cpu_us is not None and n:
        r["pg_cpu_s"] = round(pg_cpu_us / 1e6, 2)
        r["pg_cpu_ms_per_tx"] = round(pg_cpu_us / 1000 / n, 3)
    if proxy_cpu_s not in ("", "-"):
        r["proxy_cpu_s"] = round(float(proxy_cpu_s), 2)
    exec_ms = os.environ.get("EXEC_MS", "-")
    if exec_ms not in ("", "-") and n:
        r["pg_exec_s"] = round(float(exec_ms) / 1000, 2)
        r["pg_exec_ms_per_tx"] = round(float(exec_ms) / n, 3)
    cpus = os.environ.get("PG_CPUS", "").split()
    if len(cpus) == 2:
        r["pg_cpus"] = [float(c) for c in cpus]
    writer = os.environ.get("WRITER_LOG", "-")
    if writer not in ("", "-") and os.path.exists(writer):
        w = re.search(r"^tps = ([\d.]+)", open(writer).read(), re.M)
        if w:
            r["writer_tps"] = round(float(w.group(1)), 1)
    c = counters(proxy_log)
    if c:
        r["proxy"] = c
    print(json.dumps(r))


def cpu(pid):
    t = subprocess.run(["ps", "-o", "time=", "-p", pid], capture_output=True, text=True).stdout.strip()
    if not t:
        print(0)
        return
    days = 0
    if "-" in t:
        d, t = t.split("-", 1)
        days = int(d)
    parts = [float(p) for p in t.split(":")]
    secs = 0.0
    for p in parts:
        secs = secs * 60 + p
    print(round(days * 86400 + secs, 2))


if __name__ == "__main__":
    cmd, args = sys.argv[1], sys.argv[2:]
    {"pgbench": pgbench, "cpu": cpu}[cmd](*args)
