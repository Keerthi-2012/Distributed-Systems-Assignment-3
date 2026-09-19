"""analytics.py - the HW2 Q7 analytics logic, shared by every component.

This is the Python counterpart of HW2's common.cpp. There is exactly ONE copy of
the counting, merging and printing rules, used by the worker (counting), the
coordinator (merging) and the query client / dashboard (printing), so the
components can never disagree about how anything is computed.

Same key idea as HW2: we only ever store COUNTS, SUMS, MINIMUMS and MAXIMUMS.
Those combine by adding (or min/max), so partial results from any number of
workers, pulled at any moment, merge into the exact global answer. Averages are
computed only when printing.

One improvement over HW2: response times are kept as integers in micro-units
(value * 1,000,000). Every sum is then an exact Python integer, so the merged
result does not depend on the order in which workers' sums are added.
"""

import heapq
from fractions import Fraction

MICRO = 1_000_000
INT64_MAX = (1 << 63) - 1
INT64_MIN = -(1 << 63)


# --------------------------------------------------------------------- parsing

def to_micro(text):
    """'76.393' -> 76393000. Exact for inputs with up to 6 decimal places."""
    return round(float(text) * MICRO)


def interval_of(timestamp):
    """timestamp / 60 with C semantics (truncation toward zero), like HW2."""
    if timestamp >= 0:
        return timestamp // 60
    return -((-timestamp) // 60)


def read_header(f):
    """Reads the 'N K S' line. Returns (n, k, s)."""
    line = f.readline()
    parts = line.split()
    if len(parts) < 3:
        raise ValueError("first line should be: N K S")
    return int(parts[0]), int(parts[1]), int(parts[2])


def read_column_chunks(f, n, chunk_lines=65536):
    """Yields the dataset as columnar chunks, reading at most n records.

    Each chunk is a tuple of 7 lists:
        (timestamp, server_id, endpoint_id, user_id, status_code,
         response_time_micro, bytes_sent)
    Converting whole columns with map() keeps the per-record Python work low.
    """
    remaining = n
    while remaining > 0:
        want = min(chunk_lines, remaining)
        rows = []
        for _ in range(want):
            line = f.readline()
            if not line:
                break
            parts = line.split()
            if len(parts) < 7:
                raise ValueError("bad data on record %d" % (n - remaining + len(rows) + 1))
            rows.append(parts)
        if not rows:
            break
        remaining -= len(rows)
        cols = list(zip(*rows))
        yield (list(map(int, cols[0])),
               list(map(int, cols[1])),
               list(map(int, cols[2])),
               list(map(int, cols[3])),
               list(map(int, cols[4])),
               list(map(to_micro, cols[5])),
               list(map(int, cols[6])))
        if len(rows) < want:
            break


# --------------------------------------------------------------------- state

class Stats:
    """Mergeable analytics state (one per worker delta, one global)."""

    __slots__ = ("total", "success", "failed", "c2xx", "c3xx", "c4xx", "c5xx",
                 "total_bytes", "time_sum", "time_min", "time_max",
                 "server_count", "server_time", "endpoint_count", "endpoint_bytes",
                 "interval_count", "busiest_interval", "busiest_count")

    def __init__(self):
        self.total = 0
        self.success = 0
        self.failed = 0
        self.c2xx = 0
        self.c3xx = 0
        self.c4xx = 0
        self.c5xx = 0
        self.total_bytes = 0
        self.time_sum = 0
        # A state with no records keeps min very high and max very low, so it
        # can never affect a merged answer (same trick as HW2).
        self.time_min = INT64_MAX
        self.time_max = INT64_MIN
        self.server_count = {}
        self.server_time = {}
        self.endpoint_count = {}
        self.endpoint_bytes = {}
        self.interval_count = {}
        # Maintained incrementally by merge(); counts only grow, so the running
        # maximum is always the true maximum.
        self.busiest_interval = 0
        self.busiest_count = 0

    # ---------------------------------------------------------------- counting

    def add_columns(self, ts, sid, eid, status, rt, nbytes):
        """Counts one batch of records given as columns (the worker's hot loop)."""
        success = c2 = c3 = c4 = c5 = 0
        tbytes = tsum = 0
        tmin = self.time_min
        tmax = self.time_max
        sc = self.server_count
        st = self.server_time
        ec = self.endpoint_count
        eb = self.endpoint_bytes
        ic = self.interval_count
        sc_get = sc.get
        st_get = st.get
        ec_get = ec.get
        eb_get = eb.get
        ic_get = ic.get

        for t, s, e, code, r, b in zip(ts, sid, eid, status, rt, nbytes):
            # successful if status_code < 400
            if code < 400:
                success += 1
                if 200 <= code <= 299:
                    c2 += 1
                elif 300 <= code <= 399:
                    c3 += 1
            elif code <= 499:
                c4 += 1
            elif code <= 599:
                c5 += 1

            tbytes += b
            tsum += r
            if r < tmin:
                tmin = r
            if r > tmax:
                tmax = r

            # HW2 ignores negative ids (they fall outside its arrays)
            if s >= 0:
                sc[s] = sc_get(s, 0) + 1
                st[s] = st_get(s, 0) + r
            if e >= 0:
                ec[e] = ec_get(e, 0) + 1
                eb[e] = eb_get(e, 0) + b

            iv = t // 60 if t >= 0 else -((-t) // 60)
            ic[iv] = ic_get(iv, 0) + 1

        n = len(ts)
        self.total += n
        self.success += success
        self.failed += n - success
        self.c2xx += c2
        self.c3xx += c3
        self.c4xx += c4
        self.c5xx += c5
        self.total_bytes += tbytes
        self.time_sum += tsum
        self.time_min = tmin
        self.time_max = tmax

    # ---------------------------------------------------------------- merging

    def merge(self, o):
        """Adds another Stats into this one (used for re-sent worker deltas)."""
        self.total += o.total
        self.success += o.success
        self.failed += o.failed
        self.c2xx += o.c2xx
        self.c3xx += o.c3xx
        self.c4xx += o.c4xx
        self.c5xx += o.c5xx
        self.total_bytes += o.total_bytes
        self.time_sum += o.time_sum
        if o.time_min < self.time_min:
            self.time_min = o.time_min
        if o.time_max > self.time_max:
            self.time_max = o.time_max
        for d, od in ((self.server_count, o.server_count),
                      (self.server_time, o.server_time),
                      (self.endpoint_count, o.endpoint_count),
                      (self.endpoint_bytes, o.endpoint_bytes),
                      (self.interval_count, o.interval_count)):
            get = d.get
            for key, v in od.items():
                d[key] = get(key, 0) + v

    def to_proto(self, msg):
        """Fills a PartialStats message."""
        msg.total = self.total
        msg.success = self.success
        msg.failed = self.failed
        msg.count_2xx = self.c2xx
        msg.count_3xx = self.c3xx
        msg.count_4xx = self.c4xx
        msg.count_5xx = self.c5xx
        msg.total_bytes = self.total_bytes
        msg.time_sum_micro = self.time_sum
        msg.time_min_micro = self.time_min
        msg.time_max_micro = self.time_max
        ids = list(self.server_count)
        msg.server_ids.extend(ids)
        msg.server_counts.extend([self.server_count[i] for i in ids])
        msg.server_time_micro.extend([self.server_time[i] for i in ids])
        ids = list(self.endpoint_count)
        msg.endpoint_ids.extend(ids)
        msg.endpoint_counts.extend([self.endpoint_count[i] for i in ids])
        msg.endpoint_bytes.extend([self.endpoint_bytes[i] for i in ids])
        ids = list(self.interval_count)
        msg.interval_ids.extend(ids)
        msg.interval_counts.extend([self.interval_count[i] for i in ids])
        return msg

    def merge_proto(self, p):
        """Adds a PartialStats message into this state (the coordinator's reduce)."""
        if p.total == 0 and not p.interval_ids:
            return
        self.total += p.total
        self.success += p.success
        self.failed += p.failed
        self.c2xx += p.count_2xx
        self.c3xx += p.count_3xx
        self.c4xx += p.count_4xx
        self.c5xx += p.count_5xx
        self.total_bytes += p.total_bytes
        self.time_sum += p.time_sum_micro
        if p.total > 0:
            if p.time_min_micro < self.time_min:
                self.time_min = p.time_min_micro
            if p.time_max_micro > self.time_max:
                self.time_max = p.time_max_micro

        sc, st = self.server_count, self.server_time
        for i, c, t in zip(p.server_ids, p.server_counts, p.server_time_micro):
            sc[i] = sc.get(i, 0) + c
            st[i] = st.get(i, 0) + t
        ec, eb = self.endpoint_count, self.endpoint_bytes
        for i, c, b in zip(p.endpoint_ids, p.endpoint_counts, p.endpoint_bytes):
            ec[i] = ec.get(i, 0) + c
            eb[i] = eb.get(i, 0) + b

        ic = self.interval_count
        best_i, best_c = self.busiest_interval, self.busiest_count
        for i, c in zip(p.interval_ids, p.interval_counts):
            v = ic.get(i, 0) + c
            ic[i] = v
            # HW2 rule: largest count, ties go to the earliest interval
            if v > best_c or (v == best_c and i < best_i):
                best_i, best_c = i, v
        self.busiest_interval, self.busiest_count = best_i, best_c

    # ---------------------------------------------------------------- queries

    def top_servers(self, k):
        if k <= 0:
            return []
        cnt = self.server_count
        ids = heapq.nsmallest(k, cnt, key=lambda i: (-cnt[i], i))
        return [(i, cnt[i], self.server_time[i]) for i in ids]

    def top_endpoints(self, k):
        if k <= 0:
            return []
        cnt = self.endpoint_count
        ids = heapq.nsmallest(k, cnt, key=lambda i: (-cnt[i], i))
        return [(i, cnt[i], self.endpoint_bytes[i]) for i in ids]


# --------------------------------------------------------------------- printing

def fmt_micro(v):
    """Integer micro-units -> '%.6f' text, exactly (no float involved)."""
    sign = "-" if v < 0 else ""
    v = abs(v)
    return "%s%d.%06d" % (sign, v // MICRO, v % MICRO)


def fmt_average(sum_micro, count):
    """'%.6f' of sum/count, like HW2's printf of a double average.

    Fraction -> float gives the double nearest the EXACT average, which is what
    the C program computes (up to its accumulated rounding), and Python's '%'
    formatting rounds that double the same way glibc's printf does.
    """
    if count <= 0:
        return "0.000000"
    return "%.6f" % float(Fraction(sum_micro, count * MICRO))


def format_report(snap):
    """The exact HW2 Q7 output block, from an AnalyticsSnapshot message."""
    total = snap.total_requests
    if total > 0:
        avg = fmt_average(snap.response_time_sum_micro, total)
        mn = fmt_micro(snap.min_response_time_micro)
        mx = fmt_micro(snap.max_response_time_micro)
    else:
        avg = mn = mx = "0.000000"

    out = [
        "TOTAL_REQUESTS %d" % total,
        "SUCCESSFUL_REQUESTS %d" % snap.successful_requests,
        "FAILED_REQUESTS %d" % snap.failed_requests,
        "AVERAGE_RESPONSE_TIME %s" % avg,
        "MIN_RESPONSE_TIME %s" % mn,
        "MAX_RESPONSE_TIME %s" % mx,
        "TOTAL_BYTES %d" % snap.total_bytes,
        "STATUS_2XX %d" % snap.status_2xx,
        "STATUS_3XX %d" % snap.status_3xx,
        "STATUS_4XX %d" % snap.status_4xx,
        "STATUS_5XX %d" % snap.status_5xx,
        "BUSIEST_INTERVAL %d %d" % (snap.busiest_interval, snap.busiest_count),
        "TOP_SERVERS",
    ]
    for e in snap.top_servers:
        out.append("%d %d %s" % (e.server_id, e.count,
                                 fmt_average(e.response_time_micro, e.count)))
    out.append("TOP_ENDPOINTS")
    for e in snap.top_endpoints:
        out.append("%d %d %d" % (e.endpoint_id, e.count, e.bytes))
    return "\n".join(out) + "\n"
