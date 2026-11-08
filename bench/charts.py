#!/usr/bin/env python3
"""Draw the blog charts from bench/blog.sh runs. Needs matplotlib.

  python3 bench/charts.py bench/results/<run>/results.jsonl docs/img
  python3 bench/charts.py cloud <fly results.jsonl> <neon results.jsonl> docs/img
"""
import json
import os
import sys

import matplotlib

matplotlib.use("agg")
import matplotlib.pyplot as plt  # noqa: E402

SURFACE = "#fcfcfb"
TEXT = "#0b0b0b"
TEXT2 = "#52514e"
GRID = "#e6e5e1"
PGSHADOW = "#2a78d6"  # categorical slot 1
DIRECT = "#eb6834"  # categorical slot 2

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Inter", "Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
    "font.size": 11,
    "svg.fonttype": "none",
    "axes.edgecolor": GRID,
    "axes.labelcolor": TEXT2,
    "xtick.color": TEXT2,
    "ytick.color": TEXT2,
    "axes.facecolor": SURFACE,
    "figure.facecolor": SURFACE,
})


def save(fig, path):
    # SVG for the post, PNG for places that don't take SVG.
    fig.tight_layout()
    fig.savefig(path)
    fig.savefig(path[:-4] + ".png", dpi=200)
    plt.close(fig)


def load(paths):
    """results.jsonl files joined with commas; a row in a later file replaces the same run in an earlier one."""
    rows = [json.loads(l) for p in paths.split(",") for l in open(p) if l.strip()]
    return {(r["label"], r["target"], r["clients"], r["rate"]): r for r in rows}


def style(ax, title, subtitle):
    ax.set_title(title, loc="left", color=TEXT, fontsize=13, fontweight="bold", pad=26)
    ax.text(0, 1.02, subtitle, transform=ax.transAxes, color=TEXT2, fontsize=10, va="bottom")
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.tick_params(length=0)


def legend(ax, below=0.14):
    ax.legend(frameon=False, loc="upper left", bbox_to_anchor=(0, -below), ncol=2, labelcolor=TEXT2,
              handlelength=1.2, handleheight=1)


def fmt(v):
    return f"{v:,.0f}"


def hbars(path, title, subtitle, groups, value_fmt=fmt, log=False):
    """groups: [(label, direct, pgshadow)]"""
    fig, ax = plt.subplots(figsize=(8, 1.6 + 0.8 * len(groups)))
    h = 0.3
    ys = list(range(len(groups)))[::-1]
    top = max(max(d, p) for _, d, p in groups)
    for y, (label, d, p) in zip(ys, groups):
        ax.barh(y + h / 2 + 0.02, p, height=h, color=PGSHADOW, linewidth=0, label="pgshadow" if y == ys[0] else None)
        ax.barh(y - h / 2 - 0.02, d, height=h, color=DIRECT, linewidth=0, label="direct to Postgres" if y == ys[0] else None)
        pad = top * 0.01 if not log else 0
        ax.text(p * (1.04 if log else 1) + pad, y + h / 2 + 0.02, value_fmt(p), va="center", color=TEXT, fontsize=10)
        ax.text(d * (1.04 if log else 1) + pad, y - h / 2 - 0.02, value_fmt(d), va="center", color=TEXT2, fontsize=10)
    ax.set_yticks(ys)
    ax.set_yticklabels([g[0] for g in groups], color=TEXT)
    if log:
        ax.set_xscale("log")
        ax.minorticks_off()
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}" if v >= 100 or v == int(v) else f"{v:g}"))
    ax.set_xlim(right=top * (3 if log else 1.18))
    ax.xaxis.grid(True, color=GRID, linewidth=1)
    ax.set_axisbelow(True)
    style(ax, title, subtitle)
    legend(ax)
    save(fig, path)


def lines(path, title, subtitle, xs, series, xlabel, ylabel, xticks=None, logx=False, logy=False, label_at=-1,
          legend_below=0.14):
    """series: [(name, color, ys)]"""
    fig, ax = plt.subplots(figsize=(8, 4.2))
    for name, color, ys in series:
        ax.plot(xs, ys, color=color, linewidth=2, solid_capstyle="round", solid_joinstyle="round", label=name,
                marker="o", markersize=7, markeredgecolor=SURFACE, markeredgewidth=2)
        if label_at == -1:
            ax.text(xs[-1], ys[-1], "  " + fmt(ys[-1]), va="center", color=TEXT, fontsize=10)
        else:
            ax.annotate(fmt(ys[label_at]), (xs[label_at], ys[label_at]), xytext=(9, 7), textcoords="offset points",
                        ha="left", color=TEXT, fontsize=10)
    if logx:
        if 0 in xs:
            ax.set_xscale("symlog", linthresh=1, linscale=0.6)
        else:
            ax.set_xscale("log")
    if logy:
        ax.set_yscale("log")
        top = max(max(ys) for _, _, ys in series)
        ax.set_ylim(top=top * 1.5)
    ax.minorticks_off()
    if xticks:
        ax.set_xticks([x for x, _ in xticks])
        ax.set_xticklabels([l for _, l in xticks])
    ax.yaxis.grid(True, color=GRID, linewidth=1)
    ax.set_axisbelow(True)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    style(ax, title, subtitle)
    legend(ax, legend_below)
    save(fig, path)


def main(results, out):
    os.makedirs(out, exist_ok=True)
    r = load(results)
    g = lambda label, target, clients=32, rate=0: r[(label, target, clients, rate)]

    hbars(
        f"{out}/throughput.svg",
        "Throughput at 32 clients",
        "Transactions per second, higher is better.",
        [
            ("Report: GROUP BY over 200k rows", g("report", "direct")["tps"], g("report", "pgshadow")["tps"]),
            ("Per-customer report, 50 variants", g("customer", "direct")["tps"], g("customer", "pgshadow")["tps"]),
            ("Primary-key lookup (passes through)", g("pk", "direct")["tps"], g("pk", "pgshadow")["tps"]),
        ],
    )

    clients = [1, 8, 32, 64]
    lines(
        f"{out}/clients.svg",
        "Report query throughput by client count",
        "GROUP BY over 200k rows (11 ms on an idle Postgres). Postgres has 4 CPUs. Log scale.",
        clients,
        [
            ("pgshadow", PGSHADOW, [g("report", "pgshadow", c)["tps"] for c in clients]),
            ("direct to Postgres", DIRECT, [g("report", "direct", c)["tps"] for c in clients]),
        ],
        "concurrent clients",
        "transactions per second",
        xticks=[(c, str(c)) for c in clients],
        logx=True,
        logy=True,
    )

    costs = [2000, 10000, 50000, 200000]
    ms = exec_ms(results)
    lines(
        f"{out}/query-cost.svg",
        "Throughput by query cost",
        "Same aggregate over the first N rows, 32 clients. Log scales.",
        costs,
        [
            ("pgshadow", PGSHADOW, [g(f"range-{n}", "pgshadow")["tps"] for n in costs]),
            ("direct to Postgres", DIRECT, [g(f"range-{n}", "direct")["tps"] for n in costs]),
        ],
        "rows aggregated per query, and its execution time on an idle Postgres",
        "transactions per second",
        xticks=[(n, f"{n:,}\n{ms[f'range{n}']:.1f} ms") for n in costs],
        logx=True,
        logy=True,
        legend_below=0.24,
    )

    rates = [0, 1, 10, 100, 1000]
    lines(
        f"{out}/write-rate.svg",
        "Read throughput under a steady write rate",
        "Per-customer report at 32 clients while UPDATEs run at a fixed rate. Log scales.",
        rates,
        [
            ("pgshadow", PGSHADOW, [g(f"mix-writes-{w}", "pgshadow")["tps"] for w in rates]),
            ("direct to Postgres", DIRECT, [g(f"mix-writes-{w}", "direct")["tps"] for w in rates]),
        ],
        "UPDATEs per second requested on the table",
        "read transactions per second",
        xticks=[(w, rate_label(r, w)) for w in rates],
        logx=True,
        logy=True,
        label_at=0,
        legend_below=0.24,
    )

    hbars(
        f"{out}/postgres-cpu.svg",
        "Postgres CPU per request at a fixed rate",
        f"CPU milliseconds Postgres spends per request at {g('report-fixed-rate', 'direct', 8, rates_fixed(r))['rate']} requests/s, lower is better.",
        [
            ("Report: GROUP BY over 200k rows",
             g("report-fixed-rate", "direct", 8, rates_fixed(r))["pg_cpu_ms_per_tx"],
             g("report-fixed-rate", "pgshadow", 8, rates_fixed(r))["pg_cpu_ms_per_tx"]),
            ("Per-customer report, 50 variants",
             g("customer-fixed-rate", "direct", 8, rates_fixed(r))["pg_cpu_ms_per_tx"],
             g("customer-fixed-rate", "pgshadow", 8, rates_fixed(r))["pg_cpu_ms_per_tx"]),
        ],
        value_fmt=lambda v: f"{v:.2f} ms",
    )
    print(f"wrote {out}/throughput.svg clients.svg query-cost.svg write-rate.svg postgres-cpu.svg")


def exec_ms(results):
    """Execution times bench/blog.sh measured with EXPLAIN ANALYZE, from env.txt next to results."""
    out = {}
    for line in open(os.path.join(os.path.dirname(results), "env.txt")):
        if " query on Postgres: " in line:
            name, v = line.split(" query on Postgres: ")
            out[name] = float(v.split()[0])
    return out


def rate_label(r, w, label="mix-writes"):
    """The requested write rate, plus the rate the writer reached when Postgres held it back."""
    got = [r[k].get("writer_tps") for k in r if k[0] == f"{label}-{w}" and r[k].get("writer_tps")]
    if w and got and sum(got) / len(got) < 0.9 * w:
        return f"{w:,}\n({sum(got) / len(got):,.0f} reached)"
    return f"{w:,}"


def rates_fixed(r):
    return next(k[3] for k in r if k[0] == "report-fixed-rate")


def panels(path, title, subtitle, names, draw, height=4.2):
    """One row of panels sharing the y axis; draw(ax, i) fills panel i."""
    fig, axes = plt.subplots(1, len(names), figsize=(8, height), sharey=True)
    for i, (ax, name) in enumerate(zip(axes, names)):
        draw(ax, i)
        ax.set_title(name, loc="left", color=TEXT, fontsize=11, fontweight="bold")
        for side in ("top", "right", "left"):
            ax.spines[side].set_visible(False)
        ax.tick_params(length=0)
        ax.set_axisbelow(True)
    for ax in axes:
        ax.minorticks_off()
    top = 1 - 0.45 / height  # title and subtitle take a fixed height
    fig.text(0.02, 1 - 0.12 / height, title, color=TEXT, fontsize=13, fontweight="bold", va="top")
    fig.text(0.02, 1 - 0.42 / height, subtitle, color=TEXT2, fontsize=10, va="top")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, frameon=False, loc="lower left", bbox_to_anchor=(0.02, 0), ncol=2, labelcolor=TEXT2,
               handlelength=1.2, handleheight=1)
    fig.tight_layout(rect=(0, 0.35 / height, 1, top - 0.05 / height))
    fig.savefig(path)
    fig.savefig(path[:-4] + ".png", dpi=200)
    plt.close(fig)


def cloud(fly_path, neon_path, out):
    """Fly Postgres and Neon side by side, pgshadow on its own Machine in both."""
    os.makedirs(out, exist_ok=True)
    runs = [load(fly_path), load(neon_path)]
    names = ["Fly Postgres, same region", "Neon, us-west-2 from sjc"]
    g = lambda r, label, target, clients=32, rate=0: r[(label, target, clients, rate)]
    integer = matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}")

    def series(ax, xs, ys_p, ys_d, label_at=-1):
        for name, color, ys in (("pgshadow", PGSHADOW, ys_p), ("direct to Postgres", DIRECT, ys_d)):
            ax.plot(xs, ys, color=color, linewidth=2, solid_capstyle="round", label=name, marker="o", markersize=7,
                    markeredgecolor=SURFACE, markeredgewidth=2)
            i = label_at
            ax.annotate(fmt(ys[i]), (xs[i], ys[i]), xytext=(0, 9), textcoords="offset points", ha="center",
                        color=TEXT, fontsize=10)
        ax.yaxis.grid(True, color=GRID, linewidth=1)
        ax.set_yscale("log")
        ax.yaxis.set_major_formatter(integer)
        ax.minorticks_off()

    clients = [1, 8, 32, 64]

    def draw_clients(ax, i):
        r = runs[i]
        series(ax, clients, [g(r, "report", "pgshadow", c)["tps"] for c in clients],
               [g(r, "report", "direct", c)["tps"] for c in clients])
        ax.set_xscale("log")
        ax.set_xticks(clients)
        ax.set_xticklabels([str(c) for c in clients])
        ax.set_xlabel("concurrent clients")
        if i == 0:
            ax.set_ylabel("transactions per second")
        top = max(g(r, "report", "pgshadow", c)["tps"] for r in runs for c in clients)
        bottom = min(g(r, "report", t, c)["tps"] for r in runs for t in ("direct", "pgshadow") for c in clients)
        ax.set_ylim(bottom * 0.6, top * 2)

    panels(f"{out}/cloud-clients.svg", "Report query throughput on Fly and Neon",
           "GROUP BY over 200k rows. Postgres has 8 vCPU, pgshadow its own 2 vCPU Machine. Log scales.",
           names, draw_clients)

    rates = [0, 1, 10, 100, 1000]

    def draw_writes(ax, i):
        r = runs[i]
        series(ax, rates, [g(r, f"mix-writes-{w}", "pgshadow")["tps"] for w in rates],
               [g(r, f"mix-writes-{w}", "direct")["tps"] for w in rates], label_at=0)
        ax.set_xscale("symlog", linthresh=1, linscale=0.6)
        ax.set_xlim(-0.4, 1800)
        ax.set_xticks(rates)
        ax.set_xticklabels([rate_label(r, w) for w in rates], fontsize=9)
        ax.set_xlabel("UPDATEs per second requested")
        if i == 0:
            ax.set_ylabel("read transactions per second")
        top = max(g(r, "mix-writes-0", "pgshadow")["tps"] for r in runs)
        bottom = min(g(r, f"mix-writes-{w}", t)["tps"] for r in runs for t in ("direct", "pgshadow") for w in rates)
        ax.set_ylim(bottom * 0.7, top * 2)

    panels(f"{out}/cloud-write-rate.svg", "Read throughput under a steady write rate, Fly and Neon",
           "Per-customer report at 32 clients while UPDATEs run at a fixed rate. Log scales.",
           names, draw_writes, height=4.6)

    work = [("report-fixed-rate", "Report"), ("customer-fixed-rate", "Per-customer\nreport")]

    def draw_time(ax, i):
        r = runs[i]
        rate = next(k[3] for k in r if k[0] == "report-fixed-rate")
        h = 0.3
        ys = list(range(len(work)))[::-1]
        top = max(g(rr, w, "direct", 8, rate)["pg_exec_ms_per_tx"] for rr in runs for w, _ in work)
        for y, (w, _) in zip(ys, work):
            d = g(r, w, "direct", 8, rate)["pg_exec_ms_per_tx"]
            p = g(r, w, "pgshadow", 8, rate)["pg_exec_ms_per_tx"]
            ax.barh(y + h / 2 + 0.02, p, height=h, color=PGSHADOW, linewidth=0, label="pgshadow" if y == ys[0] else None)
            ax.barh(y - h / 2 - 0.02, d, height=h, color=DIRECT, linewidth=0,
                    label="direct to Postgres" if y == ys[0] else None)
            ax.text(p + top * 0.02, y + h / 2 + 0.02, f"{p:.2f} ms", va="center", color=TEXT, fontsize=10)
            ax.text(d + top * 0.02, y - h / 2 - 0.02, f"{d:.2f} ms", va="center", color=TEXT2, fontsize=10)
        ax.set_yticks(ys)
        ax.set_yticklabels([l for _, l in work], color=TEXT)
        ax.set_xlim(right=top * 1.35)
        ax.xaxis.grid(True, color=GRID, linewidth=1)
        ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}"))
        ax.set_xlabel("ms per request")

    rate = next(k[3] for k in runs[0] if k[0] == "report-fixed-rate")
    panels(f"{out}/cloud-postgres-time.svg", "Postgres execution time per request, Fly and Neon",
           f"From pg_stat_statements, at a fixed {rate} requests/s with 8 clients. Lower is better.",
           names, draw_time, height=3.4)
    print(f"wrote {out}/cloud-clients.svg cloud-write-rate.svg cloud-postgres-time.svg")


if __name__ == "__main__":
    if sys.argv[1] == "cloud":
        cloud(*sys.argv[2:5])
    else:
        main(*sys.argv[1:3])
