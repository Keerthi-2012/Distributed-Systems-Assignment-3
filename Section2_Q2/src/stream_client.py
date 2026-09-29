"""stream_client.py - replays a dataset file as a live stream.

    python3 stream_client.py node01:50051 ../../data/medium.in [options]

    --batch-size B   records per gRPC message (default 1000; B=1 sends record
                     by record, the most extreme granularity)
    --rate R         target records per second (0 = as fast as possible)
    --preload        encode everything before the timer starts, so a benchmark
                     measures streaming rather than reading the file
    --wait           return only when the system has counted every record
    --reset          clear the coordinator and the workers before streaming
    --json           print the summary as JSON, for the benchmark scripts
    --quiet          no progress lines
    --source-id NAME names this source, so several streams can be told apart

The whole stream is ONE client-streaming RPC, not one call per batch, so the
cost of setting up a call is paid once. gRPC's own flow control then gives us
backpressure for free: if the coordinator cannot keep up, sending simply slows
down here.

Response times are turned into whole numbers here, once, so nothing further
along ever has to parse a decimal or add up floats.
"""

import argparse
import json
import sys
import time

import grpc

import analytics
import loganalytics_pb2 as pb
import loganalytics_pb2_grpc as pb_grpc


def read_records(path):
    """Read the file. Gives the "N K S" header first, then one tuple per record."""
    with open(path) as fh:
        header = analytics.read_header(fh.readline())
        if header is None:
            raise SystemExit("the first line of the file should be: N K S")
        yield header
        for line in fh:
            record = analytics.read_record(line)
            if record is not None:
                yield record


def build_batches(records, batch_size):
    """Group records into batches. A batch keeps one list per column."""
    batch = pb.RecordBatch()
    count = 0
    for timestamp, server, endpoint, user, status, response_time, nbytes in records:
        batch.timestamp.append(timestamp)
        batch.server_id.append(server)
        batch.endpoint_id.append(endpoint)
        batch.user_id.append(user)
        batch.status_code.append(status)
        batch.response_time_micro.append(response_time)
        batch.bytes_sent.append(nbytes)
        count += 1
        if count == batch_size:
            yield batch
            batch = pb.RecordBatch()
            count = 0
    if count:
        yield batch


class Stream:
    """Sends the batches, and remembers how many and how long it took."""

    def __init__(self, header, batches, rate, quiet, source_id):
        self.header = header            # (n, k, s)
        self.source_id = source_id
        self.batches_to_send = batches
        self.rate = rate
        self.quiet = quiet
        self.sent = 0
        self.batches = 0
        self.started = None

    def messages(self):
        """What gRPC sends: the header first, then every batch."""
        n, k, s = self.header
        yield pb.IngestMessage(
            header=pb.StreamHeader(n=n, k=k, s=s, source_id=self.source_id))

        # The timer starts here, after the header, so it measures the records.
        self.started = time.time()

        for batch in self.batches_to_send:
            yield pb.IngestMessage(batch=batch)
            self.sent += len(batch.timestamp)
            self.batches += 1

            # --rate: if we are ahead of the target speed, wait a moment.
            if self.rate > 0:
                behind = self.sent / self.rate - (time.time() - self.started)
                if behind > 0:
                    time.sleep(behind)

            if not self.quiet and self.batches % 500 == 0:
                print("[client] sent %d records (%.0f rec/s)"
                      % (self.sent, self.sent / self.seconds()), file=sys.stderr)

    def seconds(self):
        """How long since the first record was sent (never zero, so we can
        divide by it)."""
        return max(time.time() - self.started, 1e-9)


def main():
    ap = argparse.ArgumentParser(description="replay a dataset as a gRPC stream")
    ap.add_argument("server", help="coordinator host:port, e.g. node01:50051")
    ap.add_argument("dataset", help="HW2 Q7 input file (N K S header + N records)")
    ap.add_argument("--batch-size", type=int, default=1000)
    ap.add_argument("--rate", type=float, default=0.0,
                    help="records/second, 0 = unlimited")
    ap.add_argument("--preload", action="store_true",
                    help="encode the whole dataset before streaming starts")
    ap.add_argument("--wait", action="store_true",
                    help="wait until the system has counted everything")
    ap.add_argument("--reset", action="store_true",
                    help="clear the coordinator and the workers first")
    ap.add_argument("--source-id", default="stream_client",
                    help="a name for this source (several may stream at once)")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    stub = pb_grpc.LogAnalyticsStub(
        grpc.insecure_channel(args.server, options=analytics.BIG_MESSAGES))

    records = read_records(args.dataset)
    header = next(records)              # the "N K S" line

    if args.reset:
        try:
            stub.Reset(pb.ResetRequest(), timeout=30)
        except grpc.RpcError as exc:
            print("reset failed: %s" % exc.details(), file=sys.stderr)

    batches = build_batches(records, max(1, args.batch_size))
    if args.preload:
        batches = list(batches)         # encode now, before the timer starts
        if not args.quiet:
            print("[client] prepared %d batches" % len(batches), file=sys.stderr)

    stream = Stream(header, batches, args.rate, args.quiet, args.source_id)
    try:
        stub.StreamRecords(stream.messages())
    except grpc.RpcError as exc:
        print("stream failed: %s" % exc.details(), file=sys.stderr)
        return 1

    stream_seconds = stream.seconds()
    end_to_end = stream_seconds

    if args.wait:
        # End to end means: until the last record shows up in the analytics.
        stub.GetAnalytics(pb.AnalyticsQuery(wait_for_completion=True,
                                            timeout_seconds=600), timeout=700)
        end_to_end = stream.seconds()

    if args.json:
        print(json.dumps({"records": stream.sent, "batches": stream.batches,
                          "batch_size": max(1, args.batch_size),
                          "stream_seconds": round(stream_seconds, 6),
                          "e2e_seconds": round(end_to_end, 6),
                          "throughput": round(stream.sent / end_to_end, 1)},
                         separators=(",", ":")))
    elif not args.quiet:
        print("[client] sent %d records in %d batches, stream %.3fs (%.0f rec/s)"
              % (stream.sent, stream.batches, stream_seconds,
                 stream.sent / stream_seconds), file=sys.stderr)
        if args.wait:
            print("[client] all counted after %.3fs (%.0f rec/s end to end)"
                  % (end_to_end, stream.sent / end_to_end), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
