"""worker.py - one analytics worker.

    python3 worker.py 0.0.0.0:50061

A worker does two things:

  Process     the coordinator sends it batches of records; it counts them.
  GetStats    the coordinator asks for its numbers; it sends them back.

It keeps its own running totals and always sends all of them. That makes asking
safe to repeat: if a reply gets lost, the coordinator just asks again and gets
the same (or newer) totals. The coordinator replaces that worker's numbers
rather than adding to them, so nothing is ever counted twice.

Workers never talk to each other, and never talk to the clients.
"""

import argparse
import sys
import threading
from concurrent import futures

import grpc

import analytics
import loganalytics_pb2 as pb
import loganalytics_pb2_grpc as pb_grpc


class WorkerService(pb_grpc.WorkerServicer):
    def __init__(self):
        self.stats = analytics.Stats()
        # One lock, because the batches arrive on one thread while the
        # coordinator asks for the numbers on another. A whole batch is counted
        # with the lock held, so nobody ever sees half a batch.
        self.lock = threading.Lock()

    def Process(self, request_iterator, context):
        """Count every batch the coordinator sends on this stream."""
        batches = 0
        for batch in request_iterator:
            with self.lock:
                self.stats.add_batch(batch)
            batches += 1
        with self.lock:
            return pb.ProcessAck(records_processed=self.stats.total,
                                 batches_processed=batches)

    def GetStats(self, request, context):
        """Send our running totals to the coordinator."""
        with self.lock:
            return stats_to_message(self.stats)

    def Reset(self, request, context):
        """Forget everything (used between test runs)."""
        with self.lock:
            self.stats = analytics.Stats()
        return pb.ResetResponse(ok=True)


def stats_to_message(stats):
    """Copy a Stats into the protobuf message we send back."""
    message = pb.PartialStats(
        total=stats.total,
        success=stats.success,
        failed=stats.failed,
        count_2xx=stats.count_2xx,
        count_3xx=stats.count_3xx,
        count_4xx=stats.count_4xx,
        count_5xx=stats.count_5xx,
        total_bytes=stats.total_bytes,
        time_sum_micro=stats.total_time,
        time_min_micro=stats.min_time if stats.min_time is not None else 0,
        time_max_micro=stats.max_time if stats.max_time is not None else 0,
    )

    # The three maps are sent as plain lists: the ids in one list, their numbers
    # in the others, in the same order.
    for server, (count, time_sum) in stats.servers.items():
        message.server_id.append(server)
        message.server_count.append(count)
        message.server_time_micro.append(time_sum)

    for endpoint, (count, nbytes) in stats.endpoints.items():
        message.endpoint_id.append(endpoint)
        message.endpoint_count.append(count)
        message.endpoint_bytes.append(nbytes)

    for minute, count in stats.intervals.items():
        message.interval_id.append(minute)
        message.interval_count.append(count)

    return message


def main():
    parser = argparse.ArgumentParser(description="Q7 analytics worker")
    parser.add_argument("address", nargs="?", default="0.0.0.0:50061",
                        help="host:port to listen on")
    args = parser.parse_args()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8),
                         options=analytics.BIG_MESSAGES)
    pb_grpc.add_WorkerServicer_to_server(WorkerService(), server)
    if server.add_insecure_port(args.address) == 0:
        print("worker: cannot listen on %s" % args.address, file=sys.stderr)
        return 1

    server.start()
    print("[worker] listening on %s" % args.address, file=sys.stderr)
    server.wait_for_termination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
