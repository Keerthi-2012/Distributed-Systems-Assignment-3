"""query_client.py - queries the current analytics from the coordinator.

Plain mode prints the analytics in EXACTLY the HW2 Q7 output format, so it can be
diffed against the sequential program from HW2:

    python3 query_client.py node01:50051              # current state, right now
    python3 query_client.py node01:50051 --final      # wait until the stream is fully processed
    python3 query_client.py node01:50051 --k 5

Load mode measures query latency with C concurrent query clients (separate
processes, so the clients do not share one Python interpreter lock):

    python3 query_client.py node01:50051 --load 8 --duration 10 --json
"""

import argparse
import json
import multiprocessing as mp
import sys
import time

import grpc

import loganalytics_pb2 as pb
import loganalytics_pb2_grpc as rpc
from analytics import format_report

GRPC_OPTIONS = [
    ("grpc.max_send_message_length", 256 * 1024 * 1024),
    ("grpc.max_receive_message_length", 256 * 1024 * 1024),
]


def connect(server):
    channel = grpc.insecure_channel(server, options=GRPC_OPTIONS)
    grpc.channel_ready_future(channel).result(timeout=60)
    return rpc.LogAnalyticsStub(channel)


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, int(round(p / 100.0 * (len(sorted_vals) - 1)))))
    return sorted_vals[idx]


def _load_worker(server, k, fresh, duration, think, stop_when_complete, out_q):
    stub = connect(server)
    lat = []
    end = time.time() + duration
    while time.time() < end:
        t0 = time.perf_counter()
        snap = stub.GetAnalytics(pb.AnalyticsQuery(k=k, fresh=fresh))
        lat.append(time.perf_counter() - t0)
        if stop_when_complete and snap.complete and snap.records_ingested > 0:
            break
        if think > 0:
            time.sleep(think)
    out_q.put(lat)


def run_load(args):
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    procs = [ctx.Process(target=_load_worker,
                         args=(args.server, args.k, not args.stale, args.duration,
                               args.think, args.until_complete, q))
             for _ in range(args.load)]
    t0 = time.time()
    for p in procs:
        p.start()
    lat = []
    for _ in procs:
        lat.extend(q.get())
    for p in procs:
        p.join()
    wall = time.time() - t0
    lat.sort()
    res = {
        "clients": args.load,
        "queries": len(lat),
        "wall_seconds": wall,
        "queries_per_second": len(lat) / wall if wall > 0 else 0.0,
        "latency_ms_mean": 1000 * sum(lat) / len(lat) if lat else 0.0,
        "latency_ms_p50": 1000 * percentile(lat, 50),
        "latency_ms_p95": 1000 * percentile(lat, 95),
        "latency_ms_p99": 1000 * percentile(lat, 99),
        "latency_ms_max": 1000 * lat[-1] if lat else 0.0,
    }
    if args.json:
        print(json.dumps(res))
    else:
        for key, v in res.items():
            print("%-20s %s" % (key, ("%.3f" % v) if isinstance(v, float) else v))


def main():
    ap = argparse.ArgumentParser(description="Query the log analytics coordinator")
    ap.add_argument("server", help="coordinator host:port")
    ap.add_argument("--k", type=int, default=0, help="Top-K size (0 = K from the stream header)")
    ap.add_argument("--final", action="store_true",
                    help="wait until every streamed record has been processed")
    ap.add_argument("--timeout", type=float, default=0.0)
    ap.add_argument("--stale", action="store_true",
                    help="do not pull fresh deltas; return the last merged state")
    ap.add_argument("--status", action="store_true",
                    help="also print system status (to stderr)")
    ap.add_argument("--load", type=int, default=0, metavar="C",
                    help="load-test mode with C concurrent query clients")
    ap.add_argument("--duration", type=float, default=10.0)
    ap.add_argument("--think", type=float, default=0.0,
                    help="seconds to wait between queries in load mode")
    ap.add_argument("--until-complete", action="store_true",
                    help="load mode: stop once the stream is fully processed")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.load > 0:
        run_load(args)
        return

    stub = connect(args.server)
    try:
        t0 = time.perf_counter()
        snap = stub.GetAnalytics(pb.AnalyticsQuery(
            k=args.k, fresh=not args.stale, wait_for_completion=args.final,
            timeout_seconds=args.timeout))
        latency = time.perf_counter() - t0
    except grpc.RpcError as e:
        print("[Error] %s: %s" % (e.code().name, e.details()), file=sys.stderr)
        sys.exit(1)

    sys.stdout.write(format_report(snap))
    if args.status:
        print("# processed %d / ingested %d records, complete=%s, version=%d, "
              "query %.1f ms" % (snap.records_processed, snap.records_ingested,
                                 snap.complete, snap.snapshot_version, latency * 1000),
              file=sys.stderr)


if __name__ == "__main__":
    main()
