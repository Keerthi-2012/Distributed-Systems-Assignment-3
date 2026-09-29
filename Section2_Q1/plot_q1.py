#!/usr/bin/env python3
"""plot_q1.py - turn Q1's benchmark CSVs into the report figures.

    python3 plot_q1.py

Reads   results/q1_bench.csv, results/q1_mpi.csv
Writes  results/q1_plots.png, results/q1_vs_mpi.png

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

# results/ beside this file, inside the question's folder.
HERE = os.path.dirname(os.path.abspath(__file__))
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

def plot_q1():
    rows = load("q1_bench.csv")
    if not rows:
        print("no q1_bench.csv - skipping Q1 plots")
        return
    fig, axes = plt.subplots(2, 2, figsize=(13, 9))
    fig.suptitle("Q1 - MapReduce (HW2 Q7 analytics, C++ Hadoop Streaming)",
                 fontsize=13, fontweight="bold")

    # 1. total time vs map tasks, one line per dataset
    ax = axes[0][0]
    datasets = sorted({r["dataset"] for r in rows},
                      key=lambda d: int([x["records"] for x in rows if x["dataset"] == d][0]))
    for i, ds in enumerate(datasets):
        sub = [r for r in rows if r["dataset"] == ds]
        by_tasks = group(sub, ["tasks"], "total_s")
        xs = sorted({int(k[0]) for k in by_tasks})
        ys = [statistics.median(by_tasks[(str(x),)]) for x in xs]
        lo = [min(by_tasks[(str(x),)]) for x in xs]
        hi = [max(by_tasks[(str(x),)]) for x in xs]
        ax.errorbar(xs, ys, yerr=[[y - l for y, l in zip(ys, lo)],
                                  [h - y for y, h in zip(ys, hi)]],
                    marker="o", capsize=3, color=COLORS[i % len(COLORS)],
                    label=f"{ds} ({int(sub[0]['records']):,} recs)")
    ax.set_xlabel("map tasks")
    ax.set_ylabel("total pipeline time (s)")
    ax.set_yscale("log")
    ax.set_xscale("log", base=2)
    ax.set_xticks(xs)
    ax.set_xticklabels([str(x) for x in xs])
    ax.set_title("End-to-end time vs number of map tasks", fontsize=11)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8)

    # 2. speedup against 1 task, largest dataset
    ax = axes[0][1]
    big = datasets[-1]
    sub = [r for r in rows if r["dataset"] == big]
    by_tasks = group(sub, ["tasks"], "total_s")
    xs = sorted({int(k[0]) for k in by_tasks})
    base = statistics.median(by_tasks[(str(xs[0]),)])
    ys = [base / statistics.median(by_tasks[(str(x),)]) for x in xs]
    ax.plot(xs, ys, marker="o", color=COLORS[2], label="measured")
    ax.plot(xs, [x / xs[0] for x in xs], "--", color="#868E96", label="linear")
    ax.set_xlabel("map tasks")
    ax.set_ylabel(f"speedup vs {xs[0]} task(s)")
    ax.set_title(f"Scaling of the map phase ({big})", fontsize=11)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8)

    # 3. stage breakdown, largest dataset, stacked
    ax = axes[1][0]
    stages = ["map_s", "sort1_s", "combine_s", "sort2_s", "reduce_s"]
    names = ["map", "sort (local)", "combine", "sort (gather)", "reduce"]
    bottoms = [0.0] * len(xs)
    for i, (stage, label) in enumerate(zip(stages, names)):
        vals = []
        for x in xs:
            g = group([r for r in sub if r["tasks"] == str(x)], ["tasks"], stage)
            vals.append(statistics.median(g[(str(x),)]))
        ax.bar([str(x) for x in xs], vals, bottom=bottoms,
               color=COLORS[i % len(COLORS)], label=label,
               edgecolor="#212529", linewidth=0.5)
        bottoms = [b + v for b, v in zip(bottoms, vals)]
    ax.set_xlabel("map tasks")
    ax.set_ylabel("time (s)")
    ax.set_title(f"Where the time goes ({big})", fontsize=11)
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    ax.set_axisbelow(True)

    # 4. shuffle volume vs input size: the point of in-mapper combining
    ax = axes[1][1]
    sizes, shuffles, inputs = [], [], []
    for ds in datasets:
        one = [r for r in rows if r["dataset"] == ds][0]
        sizes.append(ds)
        shuffles.append(int(one["shuffle_bytes"]) / 1024.0)
        inputs.append(int(one["input_bytes"]) / 1024.0)
    width = 0.38
    idx = range(len(sizes))
    ax.bar([i - width / 2 for i in idx], inputs, width, label="input",
           color=COLORS[0], edgecolor="#212529", linewidth=0.6)
    ax.bar([i + width / 2 for i in idx], shuffles, width, label="shuffle",
           color=COLORS[1], edgecolor="#212529", linewidth=0.6)
    ax.set_xticks(list(idx))
    ax.set_xticklabels(sizes)
    ax.set_yscale("log")
    ax.set_ylabel("KiB (log scale)")
    ax.set_title("Shuffle volume vs input: in-mapper combining", fontsize=11)
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    ax.set_axisbelow(True)

    fig.tight_layout(rect=[0, 0, 1, 0.96])
    out = os.path.join(RESULTS, "q1_plots.png")
    fig.savefig(out, dpi=140)
    print("wrote", out)

def plot_comparison():
    """Q1 (MapReduce) against MPI on the same datasets and process counts."""
    mr = load("q1_bench.csv")
    mpi = load("q1_mpi.csv")
    if not mr or not mpi:
        print("need q1_bench.csv and q1_mpi.csv - skipping the comparison plot")
        return

    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    fig.suptitle("Q1 MapReduce vs MPI - same analytics, same data, same machines\n"
                 "(both: one process per node, as Assignment 2 measured Q7)",
                 fontsize=13, fontweight="bold")

    datasets = sorted({r["dataset"] for r in mpi},
                      key=lambda d: int([x["records"] for x in mpi if x["dataset"] == d][0]))

    # 1. time vs processes, both systems, largest dataset
    ax = axes[0]
    big = datasets[-1]
    for rows, label, color, field in ((mr, "MapReduce", COLORS[0], "tasks"),
                                      (mpi, "MPI", COLORS[1], "procs")):
        sub = [r for r in rows if r["dataset"] == big]
        if not sub:
            continue
        g = group(sub, [field], "total_s")
        xs = sorted({int(k[0]) for k in g})
        ys = [statistics.median(g[(str(x),)]) for x in xs]
        lo = [min(g[(str(x),)]) for x in xs]
        hi = [max(g[(str(x),)]) for x in xs]
        ax.errorbar(xs, ys, yerr=[[y - l for y, l in zip(ys, lo)],
                                  [h - y for y, h in zip(ys, hi)]],
                    marker="o", capsize=3, color=color, label=label)
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xlabel("processes / map tasks")
    ax.set_ylabel("wall-clock time (s)")
    ax.set_title("Time vs parallelism (%s)" % big, fontsize=11)
    ax.grid(alpha=0.3, which="both")
    ax.legend(fontsize=9)

    # 2. the ratio: how many times slower MapReduce is
    ax = axes[1]
    labels, ratios = [], []
    for ds in datasets:
        mr_sub = group([r for r in mr if r["dataset"] == ds], ["tasks"], "total_s")
        mpi_sub = group([r for r in mpi if r["dataset"] == ds], ["procs"], "total_s")
        common = sorted({int(k[0]) for k in mr_sub} & {int(k[0]) for k in mpi_sub})
        for x in common:
            labels.append("%s\n%d proc" % (ds, x))
            ratios.append(statistics.median(mr_sub[(str(x),)]) /
                          statistics.median(mpi_sub[(str(x),)]))
    if ratios:
        ax.bar(range(len(labels)), ratios, color=COLORS[2],
               edgecolor="#212529", linewidth=0.6)
        ax.axhline(1.0, color="#212529", linestyle="--", linewidth=1)
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels, fontsize=7, rotation=45, ha="right")
        ax.set_ylabel("MapReduce time / MPI time")
        ax.set_title("Relative time (below 1.0 = MapReduce faster)", fontsize=11)
        ax.grid(axis="y", alpha=0.3)
        ax.set_axisbelow(True)

    # 3. scaling efficiency of both
    ax = axes[2]
    for rows, label, color, field in ((mr, "MapReduce", COLORS[0], "tasks"),
                                      (mpi, "MPI", COLORS[1], "procs")):
        sub = [r for r in rows if r["dataset"] == big]
        if not sub:
            continue
        g = group(sub, [field], "total_s")
        xs = sorted({int(k[0]) for k in g})
        base = statistics.median(g[(str(xs[0]),)])
        ys = [base / statistics.median(g[(str(x),)]) for x in xs]
        ax.plot(xs, ys, marker="o", color=color, label=label)
    ax.plot(xs, [x / xs[0] for x in xs], "--", color="#868E96", label="linear")
    ax.set_xlabel("processes / map tasks")
    ax.set_ylabel("speedup vs 1")
    ax.set_title("Scaling (%s)" % big, fontsize=11)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=9)

    fig.tight_layout(rect=[0, 0, 1, 0.93])
    out = os.path.join(RESULTS, "q1_vs_mpi.png")
    fig.savefig(out, dpi=140)
    print("wrote", out)

if __name__ == "__main__":
    plot_q1()
    plot_comparison()
