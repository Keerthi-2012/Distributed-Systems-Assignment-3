"""query_client.py - ask the coordinator for the analytics and print them.

    python3 query_client.py node01:50051              the numbers right now
    python3 query_client.py node01:50051 --final      wait for the stream to end
    python3 query_client.py node01:50051 --k 5        show 5 rows in the tops
    python3 query_client.py node01:50051 --status     also show what the system is doing
    python3 query_client.py node01:50051 --load 4 --duration 10    speed test

Only the required lines go to stdout, so the output can be compared with the
sequential program using diff. Anything else goes to stderr.
"""

import argparse
import json
import sys
import threading
import time

import grpc

import analytics
import loganalytics_pb2 as pb
import loganalytics_pb2_grpc as pb_grpc


def print_answer(answer):
    """Print the analytics in the format the assignment asks for."""
    total = answer.total_requests

    print("TOTAL_REQUESTS %d" % total)
    print("SUCCESSFUL_REQUESTS %d" % answer.successful_requests)
    print("FAILED_REQUESTS %d" % answer.failed_requests)

    # The average is worked out here, from the sum and the count.
    print("AVERAGE_RESPONSE_TIME %.6f"
          % analytics.average_ms(answer.time_sum_micro, total))
    print("MIN_RESPONSE_TIME %.6f"
          % analytics.to_ms(answer.time_min_micro if total else 0))
    print("MAX_RESPONSE_TIME %.6f"
          % analytics.to_ms(answer.time_max_micro if total else 0))

    print("TOTAL_BYTES %d" % answer.total_bytes)
    print("STATUS_2XX %d" % answer.status_2xx)
    print("STATUS_3XX %d" % answer.status_3xx)
    print("STATUS_4XX %d" % answer.status_4xx)
    print("STATUS_5XX %d" % answer.status_5xx)
    print("BUSIEST_INTERVAL %d %d" % (answer.busiest_interval, answer.busiest_count))

    print("TOP_SERVERS")
    for row in answer.top_servers:
        print("%d %d %.6f" % (row.server_id, row.count,
                              analytics.average_ms(row.time_sum_micro, row.count)))

    print("TOP_ENDPOINTS")
    for row in answer.top_endpoints:
        print("%d %d %d" % (row.endpoint_id, row.count, row.bytes))


def print_status(answer):
    """Print what the system is doing right now (to stderr)."""
    print("[status] streams %d  received %d  counted %d  still to count %d"
          % (answer.active_streams, answer.records_ingested,
             answer.records_processed,
             answer.records_ingested - answer.records_processed), file=sys.stderr)
    for worker in answer.workers:
        print("[status]   worker %d %s  sent %d  counted %d  waiting %d  %s"
              % (worker.index, worker.address, worker.records_routed,
                 worker.records_processed, worker.queue_depth,
                 "up" if worker.healthy else "DOWN"), file=sys.stderr)


def speed_test(server, how_many, seconds, k, as_json):
    """Ask lots of questions at once and report how fast the answers come."""
    times = []
    times_lock = threading.Lock()
    stop_at = time.time() + seconds

    def ask_over_and_over():
        stub = pb_grpc.LogAnalyticsStub(grpc.insecure_channel(server))
        while time.time() < stop_at:
            start = time.time()
            try:
                stub.GetAnalytics(pb.AnalyticsQuery(k=k), timeout=60)
            except grpc.RpcError:
                return
            with times_lock:
                times.append((time.time() - start) * 1000)

    threads = [threading.Thread(target=ask_over_and_over) for _ in range(how_many)]
    started = time.time()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    took = time.time() - started

    times.sort()

    def percent(fraction):
        if not times:
            return 0.0
        return times[int(fraction * (len(times) - 1))]

    if as_json:
        print(json.dumps({"clients": how_many, "queries": len(times),
                          "seconds": round(took, 3),
                          "qps": round(len(times) / took, 1) if took else 0,
                          "p50_ms": round(percent(0.50), 3),
                          "p95_ms": round(percent(0.95), 3),
                          "p99_ms": round(percent(0.99), 3)},
                         separators=(",", ":")))
    else:
        print("%d clients asked %d questions in %.1fs = %.0f per second "
              "(half under %.1f ms, 99%% under %.1f ms)"
              % (how_many, len(times), took,
                 len(times) / took if took else 0, percent(0.50), percent(0.99)),
              file=sys.stderr)
    return 0


def main():
    parser = argparse.ArgumentParser(description="ask for the analytics")
    parser.add_argument("server", help="coordinator host:port")
    parser.add_argument("--k", type=int, default=0,
                        help="rows in the top lists (0 = use the file's K)")
    parser.add_argument("--final", action="store_true",
                        help="wait until every record has been counted")
    parser.add_argument("--status", action="store_true",
                        help="also print what the system is doing")
    parser.add_argument("--load", type=int, default=0,
                        help="speed test: ask from this many clients at once")
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument("--timeout", type=float, default=0.0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if args.load > 0:
        return speed_test(args.server, args.load, args.duration, args.k, args.json)

    stub = pb_grpc.LogAnalyticsStub(
        grpc.insecure_channel(args.server, options=analytics.BIG_MESSAGES))

    question = pb.AnalyticsQuery(k=args.k, wait_for_completion=args.final,
                                 timeout_seconds=args.timeout)
    try:
        answer = stub.GetAnalytics(question, timeout=args.timeout or 700)
    except grpc.RpcError as error:
        print("could not ask the coordinator: %s" % error.details(), file=sys.stderr)
        return 1

    print_answer(answer)
    if args.status:
        print_status(answer)
    return 0


if __name__ == "__main__":
    sys.exit(main())
