#!/usr/bin/env python3
"""Generate report.tex from the measured CSVs, so the PDF cannot drift from the data."""
import csv, collections, statistics, os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
S2 = os.path.join(ROOT, "Section2", "results")
B = os.path.dirname(os.path.abspath(__file__))

def rows(p): return list(csv.DictReader(open(p)))
def mean(rs, f): return statistics.mean(float(r[f]) for r in rs)
def sel(rs, **kw): return [r for r in rs if all(r[k] == str(v) for k, v in kw.items())]

q1   = rows(os.path.join(S2, "q1_bench.csv"))
mpi  = rows(os.path.join(S2, "q1_mpi.csv"))
q2   = rows(os.path.join(S2, "q2_bench.csv"))
q2q  = rows(os.path.join(S2, "q2_queries.csv"))
s1   = rows(os.path.join(B, "sec1_q1_bench.csv"))

def esc(s): return s.replace("_", r"\_").replace("&", r"\&").replace("%", r"\%")
def f(x, n=3): return ("%%.%df" % n) % x
def thousands(x): return "{:,}".format(int(round(x)))

# ---- Section 1 Q1 table -----------------------------------------------------
sec1_tbl = "\n".join(
    "%s & %s & %s & %s & %s & %s & %s & %s & \\textbf{%s} \\\\" % (
        esc(r["label"]), esc(r["shape_A"]), esc(r["shape_B"]),
        "yes" if r["divisible_by_4"] == "yes" else "no",
        f(float(r["mapper_s"]),2), f(float(r["shuffle1_s"]),2), f(float(r["combiner_s"]),2),
        f(float(r["reducer_s"]),2), f(float(r["total_s"]),2))
    for r in s1)

# ---- Section 2 Q1 scaling ---------------------------------------------------
def q1row(ds, t):
    rs = sel(q1, dataset=ds, tasks=t)
    return mean(rs, "total_s"), mean(rs, "map_s")
base = {ds: q1row(ds, 1)[0] for ds in ("small", "medium", "large")}
s2q1_tbl = "\n".join(
    "%d & %s & %s & %s & %s & %s \\\\" % (
        t, f(q1row("small", t)[0]), f(q1row("medium", t)[0]), f(q1row("large", t)[0]),
        f(q1row("large", t)[1]), f(base["large"] / q1row("large", t)[0], 2) + r"$\times$")
    for t in (1, 2, 4, 8))

def mpirow(ds, p): return mean(sel(mpi, dataset=ds, procs=p), "total_s")
mpi_tbl = "\n".join(
    "%d & %s & %s & %s & %s & %s \\\\" % (
        p, f(mpirow("large", p)), f(q1row("large", p)[0]),
        f(q1row("large", p)[0] / mpirow("large", p), 2) + r"$\times$",
        f(mpirow("large", 1) / mpirow("large", p), 2) + r"$\times$",
        f(base["large"] / q1row("large", p)[0], 2) + r"$\times$")
    for p in (1, 2, 4, 8))

# ---- Section 2 Q2 -----------------------------------------------------------
def q2t(**kw): return mean(sel(q2, **kw), "throughput")
rr = dict(strategy="round_robin", batch_size=1000, query_clients=0)
w1 = q2t(workers=1, **rr)
work_tbl = "\n".join("%d & %s & %s \\\\" % (w, thousands(q2t(workers=w, **rr)),
                      f(q2t(workers=w, **rr) / w1, 2) + r"$\times$") for w in (1, 2, 4))
b1 = q2t(workers=4, strategy="round_robin", batch_size=1, query_clients=0)
batch_tbl = "\n".join("%s & %s & %s & %s \\\\" % (
        thousands(b), thousands(1_000_000 / b),
        thousands(q2t(workers=4, strategy="round_robin", batch_size=b, query_clients=0)),
        f(q2t(workers=4, strategy="round_robin", batch_size=b, query_clients=0) / b1, 0) + r"$\times$")
    for b in (1, 10, 100, 1000, 10000))
rrv = q2t(workers=4, **rr)
strat_tbl = "\n".join("\\texttt{%s} & %s & %s \\\\" % (
        esc(st), thousands(q2t(workers=4, strategy=st, batch_size=1000, query_clients=0)),
        f(q2t(workers=4, strategy=st, batch_size=1000, query_clients=0) / rrv, 2) + r"$\times$")
    for st in ("round_robin", "least_loaded", "hash_server"))
qmap = {int(r["query_clients"]): r for r in q2q}
qlines = ["none & %s & --- & --- & --- & --- \\\\" % thousands(rrv)]
for q in (1, 4, 16):
    tp = q2t(workers=4, strategy="round_robin", batch_size=1000, query_clients=q)
    r = qmap[q]
    qlines.append("%d & %s & %s & %s & %s & %s \\\\" % (
        q, thousands(tp), f((tp / rrv - 1) * 100, 0) + r"\%", r["qps"], r["p50_ms"], r["p99_ms"]))
query_tbl = "\n".join(qlines)

vals = dict(
    sec1_tbl=sec1_tbl, s2q1_tbl=s2q1_tbl, mpi_tbl=mpi_tbl,
    work_tbl=work_tbl, batch_tbl=batch_tbl, strat_tbl=strat_tbl, query_tbl=query_tbl,
    q1_runs=len(q1), mpi_runs=len(mpi), q2_runs=len(q2),
    q1_ok=sum(1 for r in q1 if r["correct"] == "yes"),
    mpi_ok=sum(1 for r in mpi if r["correct"] == "yes"),
    q2_ok=sum(1 for r in q2 if r["correct"] == "yes"),
    large_best=f(min(q1row("large", t)[0] for t in (1,2,4,8))),
    large_speedup=f(base["large"] / q1row("large", 4)[0], 2),
    mpi_best=f(mpirow("large", 4)),
    mpi_speedup=f(mpirow("large", 1) / mpirow("large", 4), 2),
    q2_peak=thousands(rrv), q2_speedup=f(rrv / w1, 2),
    batch_gain=f(q2t(workers=4, strategy="round_robin", batch_size=1000, query_clients=0) / b1, 0),
    shuffle_ratio=f(float(sel(q1, dataset="large", tasks=1)[0]["input_bytes"]) /
                    float(sel(q1, dataset="large", tasks=1)[0]["shuffle_bytes"]), 0),
)
tpl = open(os.path.join(B, "report.tex.in")).read()
for k, v in vals.items():
    tpl = tpl.replace("@@%s@@" % k, str(v))
open(os.path.join(B, "report.tex"), "w").write(tpl)
print("report.tex generated from the CSVs")
for k in ("q1_ok","q1_runs","mpi_ok","mpi_runs","q2_ok","q2_runs","q2_peak","large_best"):
    print("  %-14s %s" % (k, vals[k]))
