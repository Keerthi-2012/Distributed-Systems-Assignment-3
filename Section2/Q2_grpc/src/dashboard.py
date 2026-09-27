"""dashboard.py - a live view of the analytics in the terminal.

    python3 dashboard.py node01:50051 [--interval 1.0] [--k 10] [--poll] [--once]

By default it calls WatchAnalytics, so the coordinator PUSHES a new snapshot
every interval and the dashboard never has to ask. That also means ten people
watching cost the coordinator one round of adding up per refresh, not ten.

--poll asks with GetAnalytics instead, and shows how long the answer took. That
is useful for comparing the two ways of querying. --once prints one screen and
stops.
"""

import argparse
import sys
import time

import grpc

import analytics
import loganalytics_pb2 as pb
import loganalytics_pb2_grpc as pb_grpc


def bar(part, whole, width=24):
    """A simple bar made of '#', as wide as part is of whole."""
    filled = int(width * part / whole) if whole else 0
    return "#" * filled + "." * (width - filled)


def commas(number):
    return "{:,}".format(number)


def draw(snap, query_ms):
    """Print the whole screen. Called again on every refresh."""
    if snap.active_streams > 0:
        state = "STREAMING from %d source(s)" % snap.active_streams
    elif snap.records_ingested > snap.records_processed:
        state = "CATCHING UP"
    elif snap.complete:
        state = "COMPLETE"
    else:
        state = "IDLE"

    total = snap.total_requests
    lines = []
    lines.append("\033[H\033[J")          # move to the top left and clear
    lines.append(" LOG ANALYTICS   strategy %s   %s"
                 % (snap.strategy, time.strftime("%H:%M:%S")))
    lines.append(" %s" % state)
    lines.append(" received %s   counted %s   %s"
                 % (commas(snap.records_ingested), commas(snap.records_processed),
                    bar(snap.records_processed, snap.records_ingested)))
    lines.append(" coming in %.0f rec/s   counted %.0f rec/s%s"
                 % (snap.ingest_rate, snap.process_rate,
                    "   query took %.1f ms" % query_ms if query_ms else ""))

    lines.append("")
    lines.append(" WORKERS")
    lines.append("  %-3s %-22s %12s %12s %7s %s"
                 % ("#", "address", "sent", "counted", "waiting", "state"))
    for w in snap.workers:
        lines.append("  %-3d %-22s %12s %12s %7d %s"
                     % (w.index, w.address, commas(w.records_routed),
                        commas(w.records_processed), w.queue_depth,
                        "up" if w.healthy else "DOWN"))

    lines.append("")
    lines.append(" REQUESTS")
    lines.append("  total        %14s" % commas(total))
    lines.append("  successful   %14s" % commas(snap.successful_requests))
    lines.append("  failed       %14s" % commas(snap.failed_requests))
    lines.append("  bytes sent   %14s" % commas(snap.total_bytes))

    lines.append("")
    lines.append(" RESPONSE TIME (ms)")
    lines.append("  average  %.6f" % analytics.average_ms(snap.time_sum_micro, total))
    lines.append("  minimum  %.6f" % analytics.to_ms(snap.time_min_micro if total else 0))
    lines.append("  maximum  %.6f" % analytics.to_ms(snap.time_max_micro if total else 0))

    lines.append("")
    lines.append(" BUSIEST 60s INTERVAL")
    lines.append("  interval %d, with %s requests"
                 % (snap.busiest_interval, commas(snap.busiest_count)))

    lines.append("")
    lines.append(" STATUS CODES")
    for name, value in (("2xx", snap.status_2xx), ("3xx", snap.status_3xx),
                        ("4xx", snap.status_4xx), ("5xx", snap.status_5xx)):
        lines.append("  %s %s %12s" % (name, bar(value, total), commas(value)))

    lines.append("")
    lines.append(" TOP %d SERVERS     requests    average ms" % snap.k)
    for row in snap.top_servers:
        lines.append("  server %-8d %10s %13.6f"
                     % (row.server_id, commas(row.count),
                        analytics.average_ms(row.time_sum_micro, row.count)))

    lines.append("")
    lines.append(" TOP %d ENDPOINTS   requests   total bytes" % snap.k)
    for row in snap.top_endpoints:
        lines.append("  endpoint %-6d %10s %13s"
                     % (row.endpoint_id, commas(row.count), commas(row.bytes)))

    lines.append("")
    lines.append(" Ctrl-C to quit (the system keeps running)")
    sys.stdout.write("\n".join(lines) + "\n")
    sys.stdout.flush()


def main():
    ap = argparse.ArgumentParser(description="live dashboard")
    ap.add_argument("server", help="coordinator host:port")
    ap.add_argument("--interval", type=float, default=1.0, help="seconds between refreshes")
    ap.add_argument("--k", type=int, default=0, help="rows in the tops (0 = K from the file)")
    ap.add_argument("--poll", action="store_true", help="ask instead of being pushed to")
    ap.add_argument("--once", action="store_true", help="print one screen and stop")
    args = ap.parse_args()

    stub = pb_grpc.LogAnalyticsStub(
        grpc.insecure_channel(args.server, options=analytics.BIG_MESSAGES))

    try:
        if args.poll or args.once:
            while True:
                asked_at = time.time()
                snap = stub.GetAnalytics(pb.AnalyticsQuery(k=args.k), timeout=60)
                draw(snap, (time.time() - asked_at) * 1000.0)
                if args.once:
                    break
                time.sleep(args.interval)
        else:
            request = pb.WatchRequest(k=args.k, interval_seconds=args.interval)
            for snap in stub.WatchAnalytics(request):
                draw(snap, 0.0)
    except KeyboardInterrupt:
        pass
    except grpc.RpcError as exc:
        print("dashboard stopped: %s" % exc.details(), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
