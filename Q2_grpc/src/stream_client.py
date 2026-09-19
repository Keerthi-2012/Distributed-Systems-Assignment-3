"""stream_client.py - replays a pre-generated HW2 Q7 dataset as a live stream.

The file is read and sent batch by batch over one client-streaming RPC, as if
the records were arriving from a live source. How the stream behaves is
configurable, so the system can be studied under different conditions:

    --batch-size B    records per gRPC message (1 = one message per record)
    --rate R          target records per second (0 = as fast as possible)
    --speedup X       replay using the records' own timestamps, X times faster
                      than real time (overrides --rate)
    --preload         read and encode the whole file BEFORE streaming, so the
                      measured time is pure streaming (used for benchmarks)

Examples:
    python3 stream_client.py node01:50051 data/medium.in
    python3 stream_client.py node01:50051 data/medium.in --batch-size 100 --rate 20000
    python3 stream_client.py node01:50051 data/small.in --speedup 5000 --wait

Progress and timings go to stderr. With --json the final summary is printed to
stdout as one JSON object (used by bench.py).
"""

import argparse
import json
import os
import sys
import time

import grpc

import loganalytics_pb2 as pb
import loganalytics_pb2_grpc as rpc
from analytics import read_column_chunks, read_header

GRPC_OPTIONS = [
    ("grpc.max_send_message_length", 256 * 1024 * 1024),
    ("grpc.max_receive_message_length", 256 * 1024 * 1024),
]


def make_batches(path, batch_size):
    """Yields ("header", n, k, s) once, then ("batch", RecordBatch) items."""
    f = open(path, "r")
    n, k, s = read_header(f)
    yield ("header", n, k, s)
    seq = 0
    chunk_lines = batch_size * max(1, 65536 // batch_size)   # whole batches per chunk
    for cols in read_column_chunks(f, n, chunk_lines):
        m = len(cols[0])
        for start in range(0, m, batch_size):
            end = min(start + batch_size, m)
            b = pb.RecordBatch(seq=seq)
            b.timestamp.extend(cols[0][start:end])
            b.server_id.extend(cols[1][start:end])
            b.endpoint_id.extend(cols[2][start:end])
            b.user_id.extend(cols[3][start:end])
            b.status_code.extend(cols[4][start:end])
            b.response_time_micro.extend(cols[5][start:end])
            b.bytes_sent.extend(cols[6][start:end])
            seq += 1
            yield ("batch", b)
    f.close()


class Replayer:
    def __init__(self, args):
        self.args = args
        self.sent_records = 0
        self.sent_batches = 0
        self.header = None
        self.t_start = None
        self.t_end = None

    def messages(self, source):
        args = self.args
        first_ts = None
        last_report = 0.0
        for item in source:
            if item[0] == "header":
                _, n, k, s = item
                self.header = (n, k, s)
                self.t_start = time.perf_counter()
                yield pb.IngestMessage(header=pb.StreamHeader(
                    n=n, k=k, s=s, source_id=args.source_id))
                continue

            batch = item[1]
            # ---- pacing ----
            if args.speedup > 0 and len(batch.timestamp):
                ts0 = batch.timestamp[0]
                if first_ts is None:
                    first_ts = ts0
                due = self.t_start + (ts0 - first_ts) / args.speedup
                delay = due - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
            elif args.rate > 0:
                due = self.t_start + self.sent_records / args.rate
                delay = due - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)

            yield pb.IngestMessage(batch=batch)
            self.sent_records += len(batch.timestamp)
            self.sent_batches += 1

            if not args.quiet:
                now = time.perf_counter()
                if now - last_report >= 1.0:
                    last_report = now
                    el = now - self.t_start
                    print("[client] sent %d records (%.0f rec/s)" %
                          (self.sent_records, self.sent_records / el if el > 0 else 0),
                          file=sys.stderr, flush=True)
        self.t_end = time.perf_counter()


def main():
    ap = argparse.ArgumentParser(description="Replay a log dataset as a gRPC stream")
    ap.add_argument("server", help="coordinator host:port, e.g. node01:50051")
    ap.add_argument("dataset", help="HW2 Q7 input file (N K S header + N records)")
    ap.add_argument("--batch-size", type=int, default=1000)
    ap.add_argument("--rate", type=float, default=0.0, help="records/second, 0 = unlimited")
    ap.add_argument("--speedup", type=float, default=0.0,
                    help="timestamp-based replay, X times faster than real time")
    ap.add_argument("--preload", action="store_true",
                    help="encode the whole dataset before streaming starts")
    ap.add_argument("--wait", action="store_true",
                    help="after sending, wait until the system has processed everything")
    ap.add_argument("--reset", action="store_true",
                    help="clear the coordinator's state before streaming")
    ap.add_argument("--source-id", default="stream_client")
    ap.add_argument("--json", action="store_true", help="print the summary as JSON on stdout")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    if args.batch_size < 1:
        ap.error("--batch-size must be >= 1")
    if not os.path.isfile(args.dataset):
        ap.error("dataset not found: %s" % args.dataset)

    channel = grpc.insecure_channel(args.server, options=GRPC_OPTIONS)
    stub = rpc.LogAnalyticsStub(channel)
    grpc.channel_ready_future(channel).result(timeout=60)

    if args.reset:
        stub.Reset(pb.ResetRequest())

    source = make_batches(args.dataset, args.batch_size)
    t_load = 0.0
    if args.preload:
        t0 = time.perf_counter()
        source = list(source)
        t_load = time.perf_counter() - t0
        if not args.quiet:
            print("[client] preloaded %d batches in %.2fs" % (len(source) - 1, t_load),
                  file=sys.stderr, flush=True)

    rep = Replayer(args)
    t0 = time.perf_counter()
    summary = stub.StreamRecords(rep.messages(iter(source)))
    t_sent = time.perf_counter()
    stream_secs = t_sent - (rep.t_start or t0)

    result = {
        "records": rep.sent_records,
        "batches": rep.sent_batches,
        "batch_size": args.batch_size,
        "rate_target": args.rate,
        "load_seconds": t_load,
        "stream_seconds": stream_secs,
        "stream_throughput": rep.sent_records / stream_secs if stream_secs > 0 else 0.0,
        "server_records_received": summary.records_received,
    }

    if args.wait:
        stub.GetAnalytics(pb.AnalyticsQuery(wait_for_completion=True))
        t_done = time.perf_counter()
        e2e = t_done - (rep.t_start or t0)
        result["end_to_end_seconds"] = e2e
        result["end_to_end_throughput"] = rep.sent_records / e2e if e2e > 0 else 0.0

    if not args.quiet:
        print("[client] sent %d records in %d batches, stream %.3fs (%.0f rec/s)" %
              (rep.sent_records, rep.sent_batches, stream_secs, result["stream_throughput"]),
              file=sys.stderr)
        if args.wait:
            print("[client] fully processed after %.3fs (%.0f rec/s end-to-end)" %
                  (result["end_to_end_seconds"], result["end_to_end_throughput"]),
                  file=sys.stderr)
    if args.json:
        print(json.dumps(result))


if __name__ == "__main__":
    main()
