#!/usr/bin/env python3
"""plot_compare.py - the two figures that put Section 2's numbers side by side.

    python3 scripts/plot_compare.py

Reads   results/q2_bench.csv, results/q2_queries.csv
        ../Q1_mapreduce/results/q1_bench.csv, ../Q1_mapreduce/results/q1_mpi.csv

Writes  results/q2_facts.png        every claim we make about gRPC, measured
        results/paradigm_compare.png  MapReduce vs MPI vs gRPC on the same data

Reading Q1's CSVs from here is deliberate: the comparison only means anything
because both questions ran the same analytics over the same dataset file.

Every configuration is run several times; the bars are MEDIANS, because a
shared cluster produces the occasional slow run.
"""

import csv
import os
import statistics

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(HERE, "results")
Q1_RESULTS = os.path.join(os.path.dirname(HERE), "Q1_mapreduce", "results")

BLUE, PINK, GREEN, AMBER, PURPLE = (
    "#4C6EF5", "#F06595", "#37B24D", "#F59F00", "#7048E8")

# The medium dataset is the common ground: 1,000,000 records, the same file for
# both questions.
RECORDS = 1_000_000


def load(path):
    if not os.path.exists(path):
        return []
    rows = list(csv.DictReader(open(path)))
    return [r for r in rows if r.get("correct", "yes") == "yes"]


def med(rows, field, **kw):
    sub = [r for r in rows if all(r[k] == str(v) for k, v in kw.items())]
    return statistics.median(float(r[field]) for r in sub) if sub else None


def label_bars(ax, bars, values, fmt="{:,.0f}"):
    """Write each bar's value above it - a reader should not have to guess."""
    for bar, value in zip(bars, values):
        ax.annotate(fmt.format(value),
                    (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                    ha="center", va="bottom", fontsize=9)


# --------------------------------------------------------------------------
# Figure 1: the gRPC claims, each with the measurement behind it
# --------------------------------------------------------------------------
def plot_facts():
    q2 = load(os.path.join(RESULTS, "q2_bench.csv"))
    if not q2:
        print("no q2_bench.csv - skipping the fact-check figure")
        return
    queries = load(os.path.join(RESULTS, "q2_queries.csv"))
    base = dict(strategy="round_robin", batch_size=1000, query_clients=0)

    fig, axes = plt.subplots(2, 2, figsize=(14, 9.5))
    fig.suptitle("Q2 gRPC - the four claims, and the measurements behind them",
                 fontsize=14, fontweight="bold")

    # 1. Does adding workers actually add throughput?
    ax = axes[0][0]
    ws = [1, 2, 4]
    ys = [med(q2, "throughput", workers=w, **base) for w in ws]
    ys = [y / 1e6 for y in ys if y is not None]
    bars = ax.bar([str(w) for w in ws], ys, color=BLUE, edgecolor="#212529", linewidth=0.6)
    label_bars(ax, bars, ys, "{:.2f}M")
    ideal = [ys[0] * w for w in ws]
    ax.plot([str(w) for w in ws], ideal, "--o", color="#868E96",
            linewidth=1.5, markersize=5, label="linear from 1 worker")
    ax.set_xlabel("workers (one per machine)")
    ax.set_ylabel("throughput (M records/s)")
    ax.set_title("CLAIM: workers scale nearly linearly\nMEASURED: %.2fx on 4 workers, ideal 4x"
                 % (ys[-1] / ys[0]), fontsize=10)
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.3); ax.set_axisbelow(True)

    # 2. Does message granularity dominate everything else?
    ax = axes[0][1]
    bs = [1, 10, 100, 1000, 10000]
    ys = [med(q2, "throughput", workers=4, strategy="round_robin",
              batch_size=b, query_clients=0) for b in bs]
    pairs = [(b, y) for b, y in zip(bs, ys) if y is not None]
    ax.plot([p[0] for p in pairs], [p[1] for p in pairs],
            marker="o", color=PINK, linewidth=2)
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlabel("records per gRPC message")
    ax.set_ylabel("throughput (records/s, log)")
    gain = pairs[-2][1] / pairs[0][1]
    ax.set_title("CLAIM: batching matters more than anything else\n"
                 "MEASURED: %.0fx from 1 to 1000 records per message" % gain,
                 fontsize=10)
    ax.grid(alpha=0.3, which="both")

    # 3. Does the routing strategy matter?
    ax = axes[1][0]
    strategies = ["round_robin", "least_loaded", "hash_server"]
    ys = [med(q2, "throughput", workers=4, strategy=s,
              batch_size=1000, query_clients=0) for s in strategies]
    pairs = [(s, y / 1e6) for s, y in zip(strategies, ys) if y is not None]
    bars = ax.bar([p[0] for p in pairs], [p[1] for p in pairs],
                  color=[GREEN, AMBER, PINK], edgecolor="#212529", linewidth=0.6)
    label_bars(ax, bars, [p[1] for p in pairs], "{:.2f}M")
    ax.set_ylabel("throughput (M records/s)")
    ax.set_xlabel("how records are routed to workers")
    ax.set_title("CLAIM: even splitting beats clever splitting\n"
                 "MEASURED: hashing by server id is %.0fx slower"
                 % (pairs[0][1] / pairs[-1][1]), fontsize=10)
    ax.tick_params(axis="x", labelsize=9)
    ax.grid(axis="y", alpha=0.3); ax.set_axisbelow(True)

    # 4. What do live queries cost the ingestion path?
    ax = axes[1][1]
    qs = [0, 1, 4, 16]
    ys = [med(q2, "throughput", workers=4, strategy="round_robin",
              batch_size=1000, query_clients=q) for q in qs]
    pairs = [(q, y / 1e6) for q, y in zip(qs, ys) if y is not None]
    bars = ax.bar([str(p[0]) for p in pairs], [p[1] for p in pairs],
                  color=AMBER, edgecolor="#212529", linewidth=0.6)
    label_bars(ax, bars, [p[1] for p in pairs], "{:.2f}M")
    ax.set_xlabel("query clients running during ingestion")
    ax.set_ylabel("ingest throughput (M records/s)")
    ax.set_title("CLAIM: queries are served while data streams in\n"
                 "MEASURED: they are - but 16 of them cost %.0fx the ingest rate"
                 % (pairs[0][1] / pairs[-1][1]), fontsize=10)
    ax.grid(axis="y", alpha=0.3); ax.set_axisbelow(True)

    # The query latency, on its own axis, so the cost is visible from both sides.
    if queries:
        twin = ax.twinx()
        xs, lat = [], []
        for i, (q, _) in enumerate(pairs):
            v = med(queries, "p99_ms", workers=4, strategy="round_robin",
                    batch_size=1000, query_clients=q)
            if v is not None:
                xs.append(i); lat.append(v)
        if xs:
            twin.plot(xs, lat, marker="D", color="#212529", linewidth=1.6,
                      label="query p99 latency")
            twin.set_ylabel("query p99 latency (ms)")
            twin.legend(fontsize=8, loc="upper center")

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    out = os.path.join(RESULTS, "q2_facts.png")
    fig.savefig(out, dpi=140)
    plt.close(fig)
    print("wrote", out)


# --------------------------------------------------------------------------
# Figure 2: the three paradigms on the same 1M records
# --------------------------------------------------------------------------
def plot_paradigms():
    q2 = load(os.path.join(RESULTS, "q2_bench.csv"))
    mr = load(os.path.join(Q1_RESULTS, "q1_bench.csv"))
    mpi = load(os.path.join(Q1_RESULTS, "q1_mpi.csv"))
    if not (q2 and mr and mpi):
        print("need q1_bench.csv, q1_mpi.csv and q2_bench.csv - skipping")
        return

    # Four machines each, on the same medium dataset.
    mr_s = med(mr, "total_s", dataset="medium", tasks=4)
    mpi_s = med(mpi, "total_s", dataset="medium", procs=4)
    grpc_rate = med(q2, "throughput", workers=4, strategy="round_robin",
                    batch_size=1000, query_clients=0)
    if None in (mr_s, mpi_s, grpc_rate):
        print("missing a 4-machine data point - skipping the paradigm figure")
        return

    names = ["MapReduce\n(Q1, batch)", "MPI\n(baseline, batch)", "gRPC\n(Q2, streaming)"]
    rates = [RECORDS / mr_s, RECORDS / mpi_s, grpc_rate]
    times = [mr_s, mpi_s, RECORDS / grpc_rate]
    colors = [BLUE, PINK, GREEN]

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Section 2 - the same analytics over the same 1,000,000 records, "
                 "on 4 machines", fontsize=14, fontweight="bold")

    ax = axes[0]
    bars = ax.bar(names, [r / 1e6 for r in rates], color=colors,
                  edgecolor="#212529", linewidth=0.6)
    label_bars(ax, bars, [r / 1e6 for r in rates], "{:.2f}M")
    ax.set_ylabel("records processed per second (millions)")
    ax.set_title("Throughput", fontsize=11)
    ax.grid(axis="y", alpha=0.3); ax.set_axisbelow(True)

    ax = axes[1]
    bars = ax.bar(names, times, color=colors, edgecolor="#212529", linewidth=0.6)
    label_bars(ax, bars, times, "{:.2f}s")
    ax.set_ylabel("seconds for 1,000,000 records")
    ax.set_title("Time for the same work", fontsize=11)
    ax.grid(axis="y", alpha=0.3); ax.set_axisbelow(True)

    # The honest caveat, on the figure itself rather than only in the report.
    fig.text(0.5, 0.005,
             "These are not interchangeable. MapReduce and MPI read the file from "
             "disk and finish; gRPC receives records over the network and keeps "
             "answering queries while it does so.\nThe gRPC figure excludes reading "
             "the file, because a real stream has no file to read.",
             ha="center", fontsize=8.5, style="italic", color="#495057")

    fig.tight_layout(rect=[0, 0.06, 1, 0.94])
    out = os.path.join(RESULTS, "paradigm_compare.png")
    fig.savefig(out, dpi=140)
    plt.close(fig)
    print("wrote", out)


if __name__ == "__main__":
    plot_facts()
    plot_paradigms()
