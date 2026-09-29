#!/usr/bin/env python3
"""plot_q2.py - turn Q2's benchmark CSVs into the report figures.

    python3 scripts/plot_q2.py

Reads   results/q2_bench.csv, results/q2_queries.csv
Writes  results/q2_plots.png

Every configuration is run several times; the plots show the MEDIAN with the
min-max range as an error bar, because a shared cluster produces outliers and a
single run would hide them.
"""

import csv
import os
import statistics

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# results/ next to this question's scripts/ directory.
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(HERE, "results")

COLORS = ["#4C6EF5", "#F06595", "#37B24D", "#F59F00", "#7048E8"]


def load(name, drop_incorrect=True):
    """Load a CSV. Runs whose output did not match log_seq are DISCARDED, not
    averaged in: a run that produced the wrong answer did not measure the system
    we are describing. How many were dropped is printed, never hidden."""
    path = os.path.join(RESULTS, name)
    if not os.path.exists(path):
        return []
    with open(path) as fh:
        rows = list(csv.DictReader(fh))
    if drop_incorrect and rows and "correct" in rows[0]:
        good = [r for r in rows if r["correct"] == "yes"]
        dropped = len(rows) - len(good)
        if dropped:
            print(f"{name}: dropped {dropped} of {len(rows)} runs that did not "
                  f"match log_seq (excluded from all plots)")
        return good
    return rows


def group(rows, key_fields, value_field, transform=float):
    """{key tuple: [values]} keeping every repeat."""
    out = {}
    for r in rows:
        try:
            value = transform(r[value_field])
        except (ValueError, KeyError):
            continue
        key = tuple(r[f] for f in key_fields)
        out.setdefault(key, []).append(value)
    return out


def bars(ax, labels, series, ylabel, title, color=COLORS[0], log=False):
    """Bar chart of medians with min-max whiskers."""
    medians = [statistics.median(v) for v in series]
    lower = [m - min(v) for m, v in zip(medians, series)]
    upper = [max(v) - m for m, v in zip(medians, series)]
    ax.bar(range(len(labels)), medians, yerr=[lower, upper], capsize=4,
           color=color, edgecolor="#212529", linewidth=0.6)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels)
    ax.set_ylabel(ylabel)
    ax.set_title(title, fontsize=11)
    if log:
        ax.set_yscale("log")
    ax.grid(axis="y", alpha=0.3)
    ax.set_axisbelow(True)

def plot_q2():
    rows = load("q2_bench.csv")
    if not rows:
        print("no q2_bench.csv - skipping Q2 plots")
        return
    queries = load("q2_queries.csv")

    # Runs on different datasets live in the same file, so each panel picks the
    # dataset its experiment used. Mixing them would average unlike runs.
    def only(rows_in, dataset):
        return [r for r in rows_in if r.get("dataset", dataset) == dataset]

    main_ds = "medium" if any(r.get("dataset") == "medium" for r in rows) else None
    query_ds = "small" if any(r.get("dataset") == "small" for r in rows) else None
    if main_ds:
        rows = only(rows, main_ds)

    fig, axes = plt.subplots(2, 2, figsize=(13, 9))
    fig.suptitle("Q2 - gRPC streaming analytics (Python)", fontsize=13, fontweight="bold")

    # 1. throughput vs worker count (no queries, batch 1000, round robin)
    ax = axes[0][0]
    sub = [r for r in rows if r["query_clients"] == "0"
           and r["strategy"] == "round_robin" and r["batch_size"] == "1000"]
    g = group(sub, ["workers"], "throughput")
    xs = sorted({int(k[0]) for k in g})
    if xs:
        series = [[v / 1e6 for v in g[(str(x),)]] for x in xs]
        bars(ax, [str(x) for x in xs], series, "throughput (M records/s)",
             "Throughput vs number of workers (batch 1000)", COLORS[0])
        ax.set_xlabel("workers")

    # Every panel below compares one variable, so the worker count is pinned to
    # the value the sweep used for those experiments (the largest tested).
    workers_used = max((int(r["workers"]) for r in rows), default=4)

    # 2. throughput vs message batch size
    ax = axes[0][1]
    sub = [r for r in rows if r["query_clients"] == "0" and r["strategy"] == "round_robin"
           and int(r["workers"]) == workers_used]
    g = group(sub, ["batch_size"], "throughput")
    xs = sorted({int(k[0]) for k in g})
    if xs:
        ys = [statistics.median(g[(str(x),)]) for x in xs]
        lo = [min(g[(str(x),)]) for x in xs]
        hi = [max(g[(str(x),)]) for x in xs]
        ax.errorbar(xs, ys, yerr=[[y - l for y, l in zip(ys, lo)],
                                  [h - y for y, h in zip(ys, hi)]],
                    marker="o", capsize=3, color=COLORS[1])
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("records per gRPC message")
        ax.set_ylabel("throughput (records/s)")
        ax.set_title("Message granularity dominates", fontsize=11)
        ax.grid(alpha=0.3, which="both")

    # 3. distribution strategy
    ax = axes[1][0]
    sub = [r for r in rows if r["query_clients"] == "0" and r["batch_size"] == "1000"
           and int(r["workers"]) == workers_used]
    g = group(sub, ["strategy"], "throughput")
    labels = [k[0] for k in sorted(g, key=lambda k: -statistics.median(g[k]))]
    if labels:
        series = [[v / 1e6 for v in g[(l,)]] for l in labels]
        bars(ax, labels, series, "throughput (M records/s)",
             "Record distribution strategy", COLORS[2])

    # 4. concurrent queries: ingest throughput and query latency
    ax = axes[1][1]
    all_rows = load("q2_bench.csv")
    query_rows = [r for r in all_rows if r.get("dataset") == query_ds] if query_ds else all_rows
    sub = [r for r in query_rows if r["strategy"] == "round_robin" and r["batch_size"] == "1000"
           and int(r["workers"]) == workers_used]
    g = group(sub, ["query_clients", "query_mode"], "throughput")
    order = [("0", "none"), ("1", "fresh"), ("4", "fresh"), ("16", "fresh"), ("16", "stale")]
    order = [o for o in order if o in g]
    if order:
        labels = ["none" if o[0] == "0" else f"{o[0]} {o[1]}" for o in order]
        series = [[v / 1e6 for v in g[o]] for o in order]
        bars(ax, labels, series, "ingest throughput (M records/s)",
             "Cost of concurrent queries during ingestion (%s)" % (query_ds or ""),
             COLORS[3])
        if queries:
            qg = group(queries, ["query_clients", "query_mode"], "p99_ms")
            twin = ax.twinx()
            xs, ys = [], []
            for i, o in enumerate(order):
                if o in qg:
                    xs.append(i)
                    ys.append(statistics.median(qg[o]))
            if xs:
                twin.plot(xs, ys, marker="D", color="#212529", linewidth=1.5,
                          label="query p99 latency")
                twin.set_ylabel("query p99 latency (ms)")
                twin.legend(fontsize=8, loc="upper left")

    fig.tight_layout(rect=[0, 0, 1, 0.96])
    out = os.path.join(RESULTS, "q2_plots.png")
    fig.savefig(out, dpi=140)
    print("wrote", out)

if __name__ == "__main__":
    plot_q2()
