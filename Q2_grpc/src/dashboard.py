"""dashboard.py - live CLI dashboard for the streaming log analytics.

By default it subscribes to the coordinator's server-streaming WatchAnalytics RPC
and redraws whenever a new snapshot is pushed. --poll uses repeated GetAnalytics
calls instead (and shows the query latency of each call).

    python3 dashboard.py node01:50051
    python3 dashboard.py node01:50051 --interval 0.5 --k 10
    python3 dashboard.py node01:50051 --poll
    python3 dashboard.py node01:50051 --once          # print one frame and exit

Ctrl-C to quit. Only the view is closed; the system keeps running.
"""

import argparse
import collections
import shutil
import sys
import time

import grpc

import loganalytics_pb2 as pb
import loganalytics_pb2_grpc as rpc
from analytics import fmt_average, fmt_micro

GRPC_OPTIONS = [("grpc.max_receive_message_length", 256 * 1024 * 1024)]

SPARK = " ▁▂▃▄▅▆▇█"


class Style:
    def __init__(self, enabled):
        self.enabled = enabled

    def __call__(self, text, code):
        return "\033[%sm%s\033[0m" % (code, text) if self.enabled else text


def human(n):
    return "{:,}".format(n)


def human_bytes(n):
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return "%.1f %s" % (n, unit) if unit != "B" else "%d B" % n
        n /= 1024.0


def bar(frac, width):
    frac = max(0.0, min(1.0, frac))
    full = int(frac * width)
    part = int((frac * width - full) * 8)
    s = "█" * full
    if full < width:
        s += " ▏▎▍▌▋▊▉"[part] if part else " "
        s += " " * (width - full - 1)
    return s


def sparkline(values, width):
    vals = list(values)[-width:]
    if not vals:
        return ""
    top = max(vals) or 1.0
    return "".join(SPARK[min(8, int(round(v / top * 8)))] for v in vals)


def visible_len(s):
    """Length without ANSI escape codes."""
    out, i = 0, 0
    while i < len(s):
        if s[i] == "\033":
            j = s.find("m", i)
            i = j + 1 if j >= 0 else len(s)
        else:
            out += 1
            i += 1
    return out


def pad(s, width):
    return s + " " * max(0, width - visible_len(s))


def render(snap, server, st, rate_hist, latency_ms, width):
    lines = []
    now = time.strftime("%H:%M:%S")
    title = " LOG ANALYTICS  |  coordinator %s  |  %s " % (server, now)
    lines.append(st(title.ljust(width), "1;97;44"))

    if snap.active_streams > 0:
        state = st("STREAMING (%d source%s)" % (snap.active_streams,
                                                 "" if snap.active_streams == 1 else "s"), "1;32")
    elif snap.complete and snap.records_ingested > 0:
        state = st("COMPLETE", "1;36")
    elif snap.records_ingested > snap.records_processed:
        state = st("DRAINING", "1;33")
    else:
        state = st("IDLE", "2")
    up = int(snap.uptime_seconds)
    lines.append(" Status   %s   strategy %s   uptime %02d:%02d:%02d   snapshot v%d" %
                 (state, st(snap.strategy, "1"), up // 3600, (up % 3600) // 60, up % 60,
                  snap.snapshot_version))

    ing, proc = snap.records_ingested, snap.records_processed
    frac = proc / ing if ing else 0.0
    lines.append(" Progress %s %5.1f%%  processed %s / ingested %s  (in flight %s)" %
                 (st(bar(frac, 24), "32"), frac * 100, human(proc), human(ing),
                  human(ing - proc)))
    lat = ("   query %.1f ms" % latency_ms) if latency_ms is not None else ""
    lines.append(" Rates    ingest %s rec/s   process %s rec/s%s" %
                 (st("%9s" % human(int(snap.ingest_rate)), "1"),
                  st("%9s" % human(int(snap.process_rate)), "1"), lat))
    lines.append(" Activity %s" % st(sparkline(rate_hist, min(60, width - 12)), "36"))
    lines.append("")

    # workers
    lines.append(st(" WORKERS", "1;4"))
    lines.append("  %-3s %-22s %13s %13s %6s  %s" %
                 ("#", "address", "routed", "processed", "queue", "share of processed"))
    for i, w in enumerate(snap.workers):
        share = w.records_processed / proc if proc else 0.0
        health = "" if w.healthy else st(" DOWN", "1;31")
        lines.append("  %-3d %-22s %13s %13s %6d  %s %5.1f%%%s" %
                     (i, w.address[:22], human(w.records_routed), human(w.records_processed),
                      w.queue_depth, st(bar(share, 16), "34"), share * 100, health))
    lines.append("")

    # summary metrics, two columns
    total = snap.total_requests
    left = [st(" REQUESTS", "1;4"),
            "  total       %15s" % human(total),
            "  successful  %15s %s" % (human(snap.successful_requests),
                                       "(%5.1f%%)" % (100.0 * snap.successful_requests / total)
                                       if total else ""),
            "  failed      %15s %s" % (human(snap.failed_requests),
                                       st("(%5.1f%%)" % (100.0 * snap.failed_requests / total), "31")
                                       if total else ""),
            "  bytes sent  %15s (%s)" % (human(snap.total_bytes), human_bytes(snap.total_bytes)),
            "  servers %s  endpoints %s  intervals %s" % (
                human(snap.distinct_servers), human(snap.distinct_endpoints),
                human(snap.distinct_intervals))]
    right = [st(" RESPONSE TIME", "1;4"),
             "  average  %14s" % (fmt_average(snap.response_time_sum_micro, total)
                                  if total else "-"),
             "  minimum  %14s" % (fmt_micro(snap.min_response_time_micro) if total else "-"),
             "  maximum  %14s" % (fmt_micro(snap.max_response_time_micro) if total else "-"),
             "",
             st(" BUSIEST 60s INTERVAL", "1;4"),
             "  id %s  with %s requests" % (st(str(snap.busiest_interval), "1;33"),
                                            human(snap.busiest_count))]
    lines.extend(two_columns(left, right, width))
    lines.append("")

    lines.append(st(" STATUS CODES", "1;4"))
    colors = {"2xx": "32", "3xx": "36", "4xx": "33", "5xx": "31"}
    for name, val in (("2xx", snap.status_2xx), ("3xx", snap.status_3xx),
                      ("4xx", snap.status_4xx), ("5xx", snap.status_5xx)):
        f = val / total if total else 0.0
        lines.append("  %s %s %5.1f%%  %s" % (name, st(bar(f, 30), colors[name]), f * 100,
                                              human(val)))
    lines.append("")

    k = snap.k
    left = [st(" TOP %d SERVERS" % k, "1;4"),
            "  %-4s %-8s %12s %14s" % ("rank", "server", "requests", "avg resp")]
    for r, e in enumerate(snap.top_servers, 1):
        left.append("  %-4d %-8d %12s %14s" % (r, e.server_id, human(e.count),
                                               fmt_average(e.response_time_micro, e.count)))
    right = [st(" TOP %d ENDPOINTS" % k, "1;4"),
             "  %-4s %-8s %12s %18s" % ("rank", "endpoint", "requests", "total bytes")]
    for r, e in enumerate(snap.top_endpoints, 1):
        right.append("  %-4d %-8d %12s %18s" % (r, e.endpoint_id, human(e.count), human(e.bytes)))
    lines.extend(two_columns(left, right, width))
    lines.append("")
    lines.append(st(" Ctrl-C to quit the dashboard (the system keeps running)", "2"))
    return lines


def two_columns(left, right, width):
    col = 48
    if width < 2 * col + 4:                  # narrow terminal: stack them
        return left + [""] + right
    out = []
    for i in range(max(len(left), len(right))):
        a = left[i] if i < len(left) else ""
        b = right[i] if i < len(right) else ""
        out.append(pad(a, col + 4) + b)
    return out


def draw(lines, once):
    if once:
        sys.stdout.write("\n".join(lines) + "\n")
    else:
        # home the cursor, draw, clear anything left below
        sys.stdout.write("\033[H" + "\n".join(l + "\033[K" for l in lines) + "\n\033[J")
    sys.stdout.flush()


def main():
    ap = argparse.ArgumentParser(description="Live CLI dashboard")
    ap.add_argument("server", help="coordinator host:port")
    ap.add_argument("--interval", type=float, default=1.0, help="refresh period in seconds")
    ap.add_argument("--k", type=int, default=0, help="Top-K rows (0 = K from the stream)")
    ap.add_argument("--poll", action="store_true", help="poll GetAnalytics instead of streaming")
    ap.add_argument("--once", action="store_true", help="print one frame and exit")
    ap.add_argument("--no-color", action="store_true")
    args = ap.parse_args()

    channel = grpc.insecure_channel(args.server, options=GRPC_OPTIONS)
    stub = rpc.LogAnalyticsStub(channel)
    try:
        grpc.channel_ready_future(channel).result(timeout=30)
    except grpc.FutureTimeoutError:
        print("error: cannot reach coordinator at %s" % args.server, file=sys.stderr)
        sys.exit(1)

    st = Style(sys.stdout.isatty() and not args.no_color)
    rate_hist = collections.deque(maxlen=120)

    if args.once:
        t0 = time.perf_counter()
        snap = stub.GetAnalytics(pb.AnalyticsQuery(k=args.k, fresh=True))
        lat = (time.perf_counter() - t0) * 1000
        draw(render(snap, args.server, st, [snap.process_rate], lat, 110), True)
        return

    err = None
    if sys.stdout.isatty():
        sys.stdout.write("\033[?1049h\033[?25l\033[2J")   # alternate screen, hide cursor
    try:
        if args.poll:
            while True:
                t0 = time.perf_counter()
                snap = stub.GetAnalytics(pb.AnalyticsQuery(k=args.k, fresh=True))
                lat = (time.perf_counter() - t0) * 1000
                rate_hist.append(snap.process_rate)
                width = shutil.get_terminal_size((110, 40)).columns
                draw(render(snap, args.server, st, rate_hist, lat, width), False)
                time.sleep(args.interval)
        else:
            for snap in stub.WatchAnalytics(pb.WatchRequest(k=args.k,
                                                            interval_seconds=args.interval)):
                rate_hist.append(snap.process_rate)
                width = shutil.get_terminal_size((110, 40)).columns
                draw(render(snap, args.server, st, rate_hist, None, width), False)
    except KeyboardInterrupt:
        pass
    except grpc.RpcError as e:
        err = "connection lost: %s" % e.code().name
    finally:
        if sys.stdout.isatty():
            sys.stdout.write("\033[?25h\033[?1049l")
            sys.stdout.flush()
    if err:
        print(err, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
