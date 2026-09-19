"""bench.py - performance experiments for the gRPC streaming system.

Every run starts a fresh coordinator + W workers (scripts/cluster.py), replays a
dataset with stream_client.py --preload (so file parsing is not timed), waits
until every record is processed, and records:

    stream_throughput      records/s while the client was sending
    e2e_seconds            first record sent -> last record merged into the analytics
    e2e_throughput         records / e2e_seconds
    cpu_* / rss_*          CPU % (100 = one core) and peak memory per role (psutil)
    query latency          when concurrent query clients are running

Experiments (results/bench_<name>.csv):
    workers    W in 1,2,4,6,8                  round_robin, batch 1000, large (10M)
    batch      batch size 1..100000            W=4, medium (1M)
    strategy   round_robin/least_loaded/hash   W=4, batch 1000, large (+ load balance)
    queries    0,1,4,16 concurrent query clients (fresh) + 16 stale, W=4, large
    rate       target 100k..unlimited rec/s    W=4, medium, freshness (records in flight)
    size       20k .. 10M records              W=4, batch 1000

    python3 scripts/bench.py --exp all --repeats 3
    python3 scripts/bench.py --exp workers --repeats 3
"""

import argparse
import csv
import json
import os
import statistics
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cluster import ROOT, SRC, LocalCluster, py  # noqa: E402

sys.path.insert(0, SRC)
import grpc  # noqa: E402
import loganalytics_pb2 as pb  # noqa: E402
import loganalytics_pb2_grpc as rpc  # noqa: E402

try:
    import psutil
except ImportError:            # CPU / memory columns are left empty
    psutil = None

GEN = os.path.join(ROOT, "bin", "gen_dataset")
RESULTS = os.path.join(ROOT, "results")

DATASETS = {                   # name: (records, S, seed)
    "tiny": (20000, 32, 2000),
    "small": (100000, 64, 2001),
    "medium": (1000000, 128, 2002),
    "large": (10000000, 256, 2003),     # identical to HW2's large.in
}


def dataset(name):
    n, s, seed = DATASETS[name]
    path = os.path.join(ROOT, "data", name + ".in")
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        subprocess.run([GEN, str(n), "10", str(s), str(seed), path], check=True,
                       stdout=subprocess.DEVNULL)
    return path


class ResourceMonitor:
    """Samples CPU and RSS of the coordinator, workers and client every 0.25 s."""

    def __init__(self, roles):
        self.roles = roles            # role -> list of pids
        self.samples = {r: [] for r in roles}
        self.peak_rss = {r: 0 for r in roles}
        self.stop = threading.Event()
        self.procs = {}
        self.thread = threading.Thread(target=self.run, daemon=True)

    def add(self, role, pid):
        self.roles.setdefault(role, []).append(pid)
        self.samples.setdefault(role, [])
        self.peak_rss.setdefault(role, 0)

    def run(self):
        if psutil is None:
            return
        while not self.stop.wait(0.25):
            for role, pids in list(self.roles.items()):
                cpu = 0.0
                rss = 0
                for pid in pids:
                    try:
                        p = self.procs.get(pid)
                        if p is None:
                            p = self.procs[pid] = psutil.Process(pid)
                            p.cpu_percent(None)
                            continue
                        cpu += p.cpu_percent(None)
                        rss = max(rss, p.memory_info().rss)
                    except psutil.Error:
                        pass
                self.samples[role].append(cpu / max(1, len(pids)))
                self.peak_rss[role] = max(self.peak_rss[role], rss)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        self.thread.join()

    def summary(self):
        out = {}
        for role in self.samples:
            s = [x for x in self.samples[role] if x > 0] or [0.0]
            out["cpu_%s" % role] = round(statistics.mean(s), 1)
            out["rss_mb_%s" % role] = round(self.peak_rss[role] / 2 ** 20, 1)
        return out


def run_once(workers, strategy, data, batch_size, rate=0, query_clients=0, stale=False,
             lag_probe=False, queue_cap=64):
    with LocalCluster(workers=workers, strategy=strategy, queue_cap=queue_cap) as c:
        wpids, cpid = c.pids()
        mon = ResourceMonitor({"coordinator": [cpid], "worker": wpids})
        client = subprocess.Popen(
            py("stream_client.py", c.address, data, "--batch-size", batch_size,
               "--rate", rate, "--preload", "--wait", "--json", "--quiet"),
            stdout=subprocess.PIPE, text=True, cwd=SRC)
        mon.add("client", client.pid)

        # Wait for the preload to finish (the stream starts) before timing queries.
        ch = grpc.insecure_channel(c.address)
        stub = rpc.LogAnalyticsStub(ch)
        while client.poll() is None:
            if stub.GetAnalytics(pb.AnalyticsQuery()).active_streams > 0:
                break
            time.sleep(0.02)

        qproc = None
        if query_clients > 0:
            cmd = py("query_client.py", c.address, "--load", query_clients,
                     "--duration", 3600, "--until-complete", "--json")
            if stale:
                cmd.append("--stale")
            qproc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, cwd=SRC)

        lag = []

        def probe():
            while client.poll() is None:
                s = stub.GetAnalytics(pb.AnalyticsQuery(fresh=True))
                if s.active_streams > 0:
                    lag.append(s.records_ingested - s.records_processed)
                time.sleep(0.1)

        prober = threading.Thread(target=probe, daemon=True) if lag_probe else None
        with mon:
            if prober:
                prober.start()
            out, _ = client.communicate()
            if prober:
                prober.join()
        res = json.loads(out.strip().splitlines()[-1])
        final = stub.GetAnalytics(pb.AnalyticsQuery(wait_for_completion=True))
        ch.close()

        row = {
            "workers": workers, "strategy": strategy, "dataset": os.path.basename(data),
            "records": res["records"], "batch_size": batch_size, "rate_target": rate,
            "query_clients": query_clients, "stale_queries": int(stale),
            "stream_seconds": round(res["stream_seconds"], 4),
            "stream_throughput": round(res["stream_throughput"]),
            "e2e_seconds": round(res["end_to_end_seconds"], 4),
            "e2e_throughput": round(res["end_to_end_throughput"]),
        }
        shares = [w.records_processed for w in final.workers]
        mean = sum(shares) / len(shares) if shares else 0
        row["load_imbalance"] = round(max(shares) / mean, 3) if mean else 0
        row["worker_shares"] = "/".join(str(x) for x in shares)
        if qproc:
            q = json.loads(qproc.communicate()[0].strip().splitlines()[-1])
            row.update({"queries": q["queries"], "qps": round(q["queries_per_second"], 1),
                        "q_p50_ms": round(q["latency_ms_p50"], 2),
                        "q_p95_ms": round(q["latency_ms_p95"], 2),
                        "q_p99_ms": round(q["latency_ms_p99"], 2)})
        if lag_probe:
            row["lag_mean"] = round(statistics.mean(lag)) if lag else 0
            row["lag_max"] = max(lag) if lag else 0
        row.update(mon.summary())
        return row


def median_row(rows):
    """Median of the numeric timing columns over repeats (other columns from run 1)."""
    out = dict(rows[0])
    for key in ("stream_seconds", "stream_throughput", "e2e_seconds", "e2e_throughput",
                "q_p50_ms", "q_p95_ms", "q_p99_ms", "qps", "lag_mean", "lag_max",
                "cpu_coordinator", "cpu_worker", "cpu_client", "load_imbalance"):
        vals = [r[key] for r in rows if key in r]
        if vals:
            out[key] = statistics.median(vals)
    thr = [r["e2e_throughput"] for r in rows]
    out["e2e_throughput_min"] = min(thr)
    out["e2e_throughput_max"] = max(thr)
    out["repeats"] = len(rows)
    return out


def write_csv(name, rows):
    os.makedirs(RESULTS, exist_ok=True)
    path = os.path.join(RESULTS, "bench_%s.csv" % name)
    keys = []
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)
    print("wrote %s" % path, flush=True)


def experiment(name, configs, repeats):
    rows = []
    for cfg in configs:
        runs = []
        for r in range(repeats):
            row = run_once(**cfg)
            runs.append(row)
            print("  [%s] %s run %d: e2e %.2fs (%s rec/s)" %
                  (name, {k: v for k, v in cfg.items() if k != "data"}, r + 1,
                   row["e2e_seconds"], "{:,}".format(row["e2e_throughput"])), flush=True)
        rows.append(median_row(runs))
    write_csv(name, rows)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--exp", default="all",
                    help="workers,batch,strategy,queries,rate,size or all")
    ap.add_argument("--repeats", type=int, default=3)
    args = ap.parse_args()
    exps = ["workers", "batch", "strategy", "queries", "rate", "size"] \
        if args.exp == "all" else args.exp.split(",")

    medium, large = dataset("medium"), dataset("large")
    rr = "round_robin"

    for e in exps:
        print("== experiment: %s" % e, flush=True)
        if e == "workers":
            experiment(e, [dict(workers=w, strategy=rr, data=large, batch_size=1000)
                           for w in (1, 2, 4, 6, 8)], args.repeats)
        elif e == "batch":
            experiment(e, [dict(workers=4, strategy=rr, data=medium, batch_size=b)
                           for b in (1, 10, 100, 1000, 10000, 100000)], args.repeats)
        elif e == "strategy":
            experiment(e, [dict(workers=4, strategy=s, data=large, batch_size=1000)
                           for s in ("round_robin", "least_loaded", "hash_server")],
                       args.repeats)
        elif e == "queries":
            cfgs = [dict(workers=4, strategy=rr, data=large, batch_size=1000, query_clients=q)
                    for q in (0, 1, 4, 16)]
            cfgs.append(dict(workers=4, strategy=rr, data=large, batch_size=1000,
                             query_clients=16, stale=True))
            experiment(e, cfgs, args.repeats)
        elif e == "rate":
            experiment(e, [dict(workers=4, strategy=rr, data=medium, batch_size=1000,
                                rate=r, lag_probe=True)
                           for r in (100000, 250000, 500000, 1000000, 2000000, 0)],
                       args.repeats)
        elif e == "size":
            experiment(e, [dict(workers=4, strategy=rr, data=dataset(d), batch_size=1000)
                           for d in ("tiny", "small", "medium", "large")], args.repeats)
        else:
            sys.exit("unknown experiment %s" % e)


if __name__ == "__main__":
    main()
