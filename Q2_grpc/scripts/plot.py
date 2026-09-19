"""plot.py - turns results/bench_*.csv into results/q2_plots.png.

    python3 scripts/plot.py
"""

import csv
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(ROOT, "results")

BLUE, ORANGE, GREEN, GREY = "#2f6db5", "#d9822b", "#3a9a5b", "#8a8a8a"


def load(name):
    path = os.path.join(RESULTS, "bench_%s.csv" % name)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return list(csv.DictReader(f))


def num(rows, key):
    return [float(r[key]) if r.get(key) not in (None, "") else 0.0 for r in rows]


def err(rows, scale=1.0):
    """Asymmetric error bars: median down to min, up to max over the repeats."""
    med = num(rows, "e2e_throughput")
    lo = num(rows, "e2e_throughput_min") if "e2e_throughput_min" in rows[0] else med
    hi = num(rows, "e2e_throughput_max") if "e2e_throughput_max" in rows[0] else med
    return [[(m - l) / scale for m, l in zip(med, lo)], [(h - m) / scale for m, h in zip(med, hi)]]


def style(ax, title, xlabel, ylabel):
    ax.set_title(title, fontsize=11, loc="left")
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.3)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)


def main():
    fig, axes = plt.subplots(3, 2, figsize=(13, 14))
    axes = axes.ravel()

    rows = load("workers")
    if rows:
        ax = axes[0]
        w = num(rows, "workers")
        e2e = [x / 1000 for x in num(rows, "e2e_throughput")]
        ax.errorbar(w, e2e, yerr=err(rows, 1000), fmt="o-", color=BLUE, capsize=3,
                    label="end-to-end throughput (median, min-max)")
        base = e2e[0]
        ax.plot(w, [base * x for x in w], "--", color=GREY, label="ideal linear")
        ax.set_ylim(0, 5000)
        ax2 = ax.twinx()
        ax2.plot(w, num(rows, "cpu_coordinator"), "s:", color=ORANGE, label="coordinator CPU %")
        ax2.set_ylabel("coordinator CPU % (100 = 1 core)")
        ax2.set_ylim(0, 150)
        style(ax, "Effect of worker count (10M records, batch 1000)", "workers",
              "thousand records / s")
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        ax.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=8)

    rows = load("batch")
    if rows:
        ax = axes[1]
        b = num(rows, "batch_size")
        ax.errorbar(b, num(rows, "e2e_throughput"), yerr=err(rows), fmt="o-", color=BLUE,
                    capsize=3, label="end-to-end (median, min-max)")
        ax.plot(b, num(rows, "stream_throughput"), "s--", color=ORANGE, label="client send")
        ax.set_xscale("log")
        ax.set_yscale("log")
        style(ax, "Effect of message granularity (4 workers, 1M records)",
              "records per gRPC message (log)", "records / s (log)")
        ax.legend(fontsize=8)

    rows = load("strategy")
    if rows:
        ax = axes[2]
        names = [r["strategy"].replace("_", "\n") for r in rows]
        thr = [x / 1000 for x in num(rows, "e2e_throughput")]
        bars = ax.bar(names, thr, yerr=err(rows, 1000), capsize=6,
                      color=[BLUE, GREEN, ORANGE][:len(rows)])
        for bar_, r in zip(bars, rows):
            ax.text(bar_.get_x() + bar_.get_width() / 2, bar_.get_height(),
                    "imbalance %.2fx" % float(r["load_imbalance"]),
                    ha="left", va="bottom", fontsize=8)
        style(ax, "Record distribution strategy (4 workers, 10M records)", "",
              "thousand records / s")
        ax.set_ylim(0, max(num(rows, "e2e_throughput_max") or thr) / 1000 * 1.2)

    rows = load("queries")
    if rows:
        ax = axes[3]
        labels = ["%s%s" % (r["query_clients"], " stale" if r.get("stale_queries") == "1" else "")
                  for r in rows]
        x = range(len(rows))
        ax.bar([i - 0.2 for i in x], num(rows, "q_p50_ms"), 0.4, color=BLUE, label="p50 latency")
        ax.bar([i + 0.2 for i in x], num(rows, "q_p95_ms"), 0.4, color=ORANGE, label="p95 latency")
        ax.set_xticks(list(x))
        ax.set_xticklabels(labels)
        ax2 = ax.twinx()
        ax2.errorbar(list(x), [v / 1000 for v in num(rows, "e2e_throughput")],
                     yerr=err(rows, 1000), fmt="o-", color=GREEN, capsize=3,
                     label="ingest e2e throughput")
        ax2.set_ylabel("thousand records / s")
        ax2.set_ylim(0, max(num(rows, "e2e_throughput_max")) / 1000 * 1.3)
        style(ax, "Concurrent query clients during ingestion (10M records)", "query clients", "latency (ms)")
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        ax.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=8)

    rows = load("rate")
    if rows:
        ax = axes[4]
        target = [float(r["rate_target"]) for r in rows]
        labels = ["%dk" % (t / 1000) if t > 0 else "max" for t in target]
        x = range(len(rows))
        ax.bar(list(x), [v / 1000 for v in num(rows, "e2e_throughput")], color=BLUE,
               label="achieved (e2e)")
        ax.scatter([i for i, t in zip(x, target) if t > 0], [t / 1000 for t in target if t > 0],
                   marker="_", s=900, color="black", label="target", zorder=3)
        ax.set_xticks(list(x))
        ax.set_xticklabels(labels)
        ax2 = ax.twinx()
        ax2.plot(list(x), num(rows, "lag_mean"), "o-", color=ORANGE, label="records in flight (mean)")
        ax2.set_ylabel("records not yet in analytics")
        style(ax, "Stream rate vs. freshness (4 workers, 1M records)", "target rate", "thousand records / s")
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        ax.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=8)

    rows = load("size")
    if rows:
        ax = axes[5]
        n = num(rows, "records")
        ax.plot(n, num(rows, "e2e_seconds"), "o-", color=BLUE, label="end-to-end time")
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax2 = ax.twinx()
        ax2.plot(n, [v / 1000 for v in num(rows, "e2e_throughput")], "s--", color=ORANGE,
                 label="throughput")
        ax2.set_ylabel("thousand records / s")
        ax2.set_ylim(0, max(num(rows, "e2e_throughput")) / 1000 * 1.3)
        style(ax, "Scaling with input size (4 workers, batch 1000)", "records (log)",
              "seconds (log)")
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        ax.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=8)

    fig.tight_layout()
    out = os.path.join(RESULTS, "q2_plots.png")
    fig.savefig(out, dpi=130)
    print("wrote %s" % out)


if __name__ == "__main__":
    main()
