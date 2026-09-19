"""verify.py - correctness verification of the gRPC streaming system.

The reference is HW2's sequential program (bin/log_seq, built from reference/).
For every input, the final analytics of the streaming system must be
BYTE-FOR-BYTE identical to log_seq's output, for every configuration:

  1. every input x workers {1,2,3,4} x strategies x batch sizes, with a Reset
     of the SAME cluster between runs (so Reset is exercised too)
  2. two sources streaming halves of a file at the same time
  3. queries while ingestion is in progress: every snapshot must be internally
     consistent and monotone (totals never go down, total == records processed)

    python3 scripts/verify.py              # full run
    python3 scripts/verify.py --quick      # fewer configurations
"""

import argparse
import os
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cluster import ROOT, SRC, LocalCluster  # noqa: E402

sys.path.insert(0, SRC)
import grpc  # noqa: E402
import loganalytics_pb2 as pb  # noqa: E402
import loganalytics_pb2_grpc as rpc  # noqa: E402

LOG_SEQ = os.path.join(ROOT, "bin", "log_seq")
GEN = os.path.join(ROOT, "bin", "gen_dataset")


def reference(path):
    return subprocess.run([LOG_SEQ, path], stdout=subprocess.PIPE, check=True,
                          text=True).stdout


def ensure_dataset(name, n, s, seed):
    path = os.path.join(ROOT, "data", name)
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        subprocess.run([GEN, str(n), "10", str(s), str(seed), path], check=True,
                       stdout=subprocess.DEVNULL)
    return path


def split_file(path, parts, tmpdir):
    with open(path) as f:
        n, k, s = f.readline().split()
        lines = f.readlines()
    out = []
    step = (len(lines) + parts - 1) // parts
    for i in range(parts):
        chunk = lines[i * step:(i + 1) * step]
        p = os.path.join(tmpdir, "part%d.in" % i)
        with open(p, "w") as f:
            f.write("%d %s %s\n" % (len(chunk), k, s))
            f.writelines(chunk)
        out.append(p)
    return out


class Results:
    def __init__(self):
        self.rows = []
        self.failed = 0

    def add(self, name, ok, detail=""):
        self.rows.append((name, ok, detail))
        if not ok:
            self.failed += 1
        print("%-4s %s %s" % ("PASS" if ok else "FAIL", name, detail), flush=True)


def check_live_queries(cluster, dataset, expected, res):
    """Streams a file while a thread queries continuously; checks every snapshot."""
    ch = grpc.insecure_channel(cluster.address)
    stub = rpc.LogAnalyticsStub(ch)
    snaps = []
    stop = threading.Event()

    def poll():
        while not stop.is_set():
            t0 = time.perf_counter()
            s = stub.GetAnalytics(pb.AnalyticsQuery(fresh=True))
            snaps.append((s, time.perf_counter() - t0))

    th = threading.Thread(target=poll)
    th.start()
    cluster.stream(dataset, batch_size=500, rate=150000)
    stop.set()
    th.join()
    final = cluster.final_report()

    problems = []
    prev = None
    during = 0
    for s, _ in snaps:
        if s.total_requests != s.records_processed:
            problems.append("total %d != processed %d" % (s.total_requests, s.records_processed))
        if s.records_processed > s.records_ingested:
            problems.append("processed > ingested")
        if s.successful_requests + s.failed_requests != s.total_requests:
            problems.append("success + failed != total")
        if prev is not None and (s.total_requests < prev.total_requests or
                                 s.snapshot_version < prev.snapshot_version):
            problems.append("snapshot went backwards")
        if 0 < s.total_requests < 1_000_000 and not s.complete:
            during += 1
        prev = s
    ok = not problems and final == expected and during > 0
    lat = sorted(l for _, l in snaps)
    detail = "(%d snapshots, %d taken mid-stream, p50 query %.1f ms)" % (
        len(snaps), during, 1000 * lat[len(lat) // 2] if lat else 0)
    if problems:
        detail += " " + "; ".join(sorted(set(problems))[:3])
    res.add("live queries during ingestion are consistent + final matches", ok, detail)
    ch.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    args = ap.parse_args()

    if not os.path.exists(LOG_SEQ):
        sys.exit("build the reference first: make tools")

    tests = os.path.join(ROOT, "tests")
    small_inputs = [os.path.join(tests, f) for f in sorted(os.listdir(tests)) if f.endswith(".in")]
    tiny = ensure_dataset("tiny.in", 20000, 32, 2000)
    small = ensure_dataset("small.in", 100000, 64, 2001)
    medium = ensure_dataset("medium.in", 1000000, 128, 2002)

    expected = {p: reference(p) for p in small_inputs + [tiny, small, medium]}
    res = Results()

    worker_counts = [1, 2, 4] if args.quick else [1, 2, 3, 4]
    strategies = ["round_robin", "least_loaded", "hash_server"]

    for w in worker_counts:
        for strategy in strategies:
            with LocalCluster(workers=w, strategy=strategy, sync_interval=0.2) as c:
                runs = [(p, 1) for p in small_inputs] + [(p, 3) for p in small_inputs]
                runs += [(tiny, 1), (tiny, 7), (small, 1000)]
                if not args.quick or w == 4:
                    runs += [(small, 64), (medium, 5000)]
                for path, bs in runs:
                    c.reset()
                    c.stream(path, batch_size=bs)
                    got = c.final_report()
                    name = "W=%d %-12s batch=%-5d %s" % (w, strategy, bs, os.path.basename(path))
                    res.add(name, got == expected[path])
                    if got != expected[path]:
                        print("---- expected\n%s---- got\n%s" % (expected[path], got))

    # two sources at once
    with tempfile.TemporaryDirectory() as tmp, LocalCluster(workers=3) as c:
        parts = split_file(small, 2, tmp)
        procs = [c.stream(p, batch_size=250, wait=False, source_id="src%d" % i, background=True)
                 for i, p in enumerate(parts)]
        for p in procs:
            p.wait()
        res.add("2 concurrent sources (halves of small.in) == whole file",
                c.final_report() == expected[small])

    # different K at query time (K from the query overrides the stream header)
    with LocalCluster(workers=2) as c:
        c.stream(small, batch_size=1000)
        got = c.final_report(k=3)
        with tempfile.NamedTemporaryFile("w", suffix=".in", delete=False) as f:
            with open(small) as src:
                n, _, s = src.readline().split()
                f.write("%s 3 %s\n" % (n, s))
                f.writelines(src)
        res.add("query-time K=3 == HW2 run with K=3", got == reference(f.name))
        os.unlink(f.name)

    with LocalCluster(workers=4, sync_interval=0.1) as c:
        check_live_queries(c, medium, expected[medium], res)

    total = len(res.rows)
    print("\n%d / %d checks passed" % (total - res.failed, total))
    out = os.path.join(ROOT, "results", "verification.txt")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        f.write("Correctness verification: gRPC system vs HW2 sequential log_seq\n")
        f.write("date: %s\n\n" % time.strftime("%Y-%m-%d %H:%M:%S"))
        for name, ok, detail in res.rows:
            f.write("%-4s %s %s\n" % ("PASS" if ok else "FAIL", name, detail))
        f.write("\n%d / %d checks passed\n" % (total - res.failed, total))
    sys.exit(1 if res.failed else 0)


if __name__ == "__main__":
    main()
