#!/usr/bin/env python3
"""plot_scaling.py - the strong-scaling figures for Section 1 Q1.

    python3 plot_scaling.py

Reads   perf_results/scaling.csv   (written by benchmark_scaling_slurm.sh)
Writes  perf_results/speedup.png
        perf_results/efficiency.png
        perf_results/stages_Small.png, stages_Medium.png, stages_Large.png

Every configuration is run several times; the plots use the MEDIAN, because a
shared cluster produces the occasional slow run and a single measurement would
hide it.

Speedup is T1 / Tp: how many times faster p tasks are than one task.
Efficiency is that speedup divided by p, as a percentage: 100% would mean every
added machine pulls its full weight.
"""

import csv
import os
import statistics
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(HERE, "perf_results")
CSV = os.path.join(RESULTS, "scaling.csv")

SIZES = ["Small", "Medium", "Large"]
COLORS = {"Small": "#1f77b4", "Medium": "#ff7f0e", "Large": "#2ca02c"}

# The four stages, in the order they are stacked. "Shuffle/Sort" is both sorts:
# the local one on each machine and the single global one before the reducer.
STAGES = [
    ("Map Computation",    ["mapper_s"],                  "#1f77b4"),
    ("Shuffle/Sort",       ["shuffle1_s", "shuffle2_s"],  "#ff7f0e"),
    ("Reduce Computation", ["reducer_s"],                 "#2ca02c"),
    ("Combiner",           ["combiner_s"],                "#d62728"),
]


def load():
    """Read the CSV, dropping any run whose answer did not match."""
    if not os.path.exists(CSV):
        sys.exit("no %s - run the benchmark first:\n    sbatch scripts/benchmark_scaling_slurm.sh" % CSV)
    rows = list(csv.DictReader(open(CSV)))
    good = [r for r in rows if r.get("correct") == "yes"]
    dropped = len(rows) - len(good)
    if dropped:
        print("dropped %d of %d runs whose answer did not match" % (dropped, len(rows)))
    return good


def median_total(rows, size, tasks):
    """Median total time for one size at one task count, or None."""
    vals = [float(r["total_s"]) for r in rows
            if r["size"] == size and int(r["tasks"]) == tasks]
    return statistics.median(vals) if vals else None


def task_counts(rows):
    return sorted({int(r["tasks"]) for r in rows})


def plot_speedup(rows):
    xs = task_counts(rows)
    fig, ax = plt.subplots(figsize=(10, 6.5))
    for size in SIZES:
        base = median_total(rows, size, xs[0])
        if base is None:
            continue
        ys = [base / median_total(rows, size, x) for x in xs]
        ax.plot(xs, ys, marker="o", linewidth=2.5, markersize=8,
                color=COLORS[size], label=size)
    ax.plot(xs, [x / xs[0] for x in xs], "--", color="gray",
            linewidth=1.5, label="Ideal (Linear)")
    ax.set_xscale("log", base=2)
    ax.set_xticks(xs)
    ax.set_xticklabels([str(x) for x in xs])
    ax.set_xlabel("Number of Tasks (p)")
    ax.set_ylabel("Speedup (T1 / Tp)")
    ax.set_title("Strong Scaling Speedup vs Number of Tasks")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend(title="Input Size", loc="upper left")
    save(fig, "speedup.png")


def plot_efficiency(rows):
    xs = task_counts(rows)
    fig, ax = plt.subplots(figsize=(10, 6.5))
    for size in SIZES:
        base = median_total(rows, size, xs[0])
        if base is None:
            continue
        ys = [100.0 * (base / median_total(rows, size, x)) / x for x in xs]
        ax.plot(xs, ys, marker="s", linewidth=2.5, markersize=8,
                color=COLORS[size], label=size)
    ax.axhline(100, linestyle="--", color="gray", linewidth=1.5,
               label="Ideal (100%)")
    ax.set_xscale("log", base=2)
    ax.set_xticks(xs)
    ax.set_xticklabels([str(x) for x in xs])
    ax.set_xlabel("Number of Tasks (p)")
    ax.set_ylabel("Parallel Efficiency (%)")
    ax.set_title("Parallel Efficiency vs Number of Tasks")
    ax.set_ylim(0, 110)
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend(title="Input Size", loc="lower left")
    save(fig, "efficiency.png")


def plot_stages(rows, size):
    xs = task_counts(rows)
    present = [x for x in xs if median_total(rows, size, x) is not None]
    if not present:
        return
    fig, ax = plt.subplots(figsize=(10, 6.5))
    bottoms = [0.0] * len(present)
    labels = ["T=%d" % x for x in present]
    for name, fields, color in STAGES:
        vals = []
        for x in present:
            sub = [r for r in rows if r["size"] == size and int(r["tasks"]) == x]
            vals.append(statistics.median(
                sum(float(r[f]) for f in fields) for r in sub))
        ax.bar(labels, vals, bottom=bottoms, color=color, label=name)
        bottoms = [b + v for b, v in zip(bottoms, vals)]
    ax.set_xlabel("Number of Tasks (p)")
    ax.set_ylabel("Time (seconds)")
    ax.set_title("Stage Time Breakdown vs Tasks\n(%s)" % size)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)
    ax.legend(title="Stage", loc="center left", bbox_to_anchor=(1.02, 0.5))
    save(fig, "stages_%s.png" % size)


def save(fig, name):
    fig.tight_layout()
    out = os.path.join(RESULTS, name)
    fig.savefig(out, dpi=140, bbox_inches="tight")
    plt.close(fig)
    print("wrote", out)


if __name__ == "__main__":
    data = load()
    plot_speedup(data)
    plot_efficiency(data)
    for s in SIZES:
        plot_stages(data, s)
