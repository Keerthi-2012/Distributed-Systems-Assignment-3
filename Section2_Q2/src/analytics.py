"""analytics.py - the Q7 analytics for the streaming system.

Same idea as the C++ version used by Q1: everything we keep is a total, a sum, a
smallest or a biggest, and all of those can be added together later. So each
worker counts its own records, and the coordinator adds the workers together.
We never store an average, because averages cannot be added - we store the sum
and the count, and divide only when printing.

Response times are kept as whole numbers, in millionths of a millisecond
(12.5 ms is stored as 12500000). Decimals lose a tiny amount each time you add
them, and workers finish in a different order every run, so the last digits
could come out different. Whole numbers always give exactly the same answer.
"""

# 12.5 ms is stored as 12500000, so the output can print 6 decimal places.
UNITS_PER_MS = 1000000

# Every channel and server in this system uses these options. A batch of 1000
# records, or a worker's whole reply, is bigger than gRPC's 4 MB default, so the
# limit is raised once here instead of in six different files.
BIG_MESSAGES = [("grpc.max_receive_message_length", 256 * 1024 * 1024),
                ("grpc.max_send_message_length", 256 * 1024 * 1024)]


def read_record(line):
    """Turn one line of the log file into a tuple. Returns None if it is not a
    record (the first line of the file, for example)."""
    parts = line.split()
    if len(parts) != 7:
        return None
    try:
        return (
            int(parts[0]),                                  # timestamp
            int(parts[1]),                                  # server id
            int(parts[2]),                                  # endpoint id
            int(parts[3]),                                  # user id
            int(parts[4]),                                  # status code
            round(float(parts[5]) * UNITS_PER_MS),          # response time
            int(parts[6]),                                  # bytes sent
        )
    except ValueError:
        return None


def read_header(line):
    """Read the "N K S" first line. Returns None if it is not that."""
    parts = line.split()
    if len(parts) != 3:
        return None
    try:
        return int(parts[0]), int(parts[1]), int(parts[2])
    except ValueError:
        return None


def minute_of(timestamp):
    """Which minute a timestamp belongs to."""
    # int() rounds towards zero, which is what the C++ version does with / 60.
    return int(timestamp / 60)


class Stats:
    """All the numbers the assignment asks for."""

    def __init__(self):
        self.total = 0
        self.success = 0
        self.failed = 0
        self.count_2xx = 0
        self.count_3xx = 0
        self.count_4xx = 0
        self.count_5xx = 0
        self.total_bytes = 0
        self.total_time = 0
        self.min_time = None        # None means nothing counted yet
        self.max_time = None
        self.servers = {}           # server id -> [count, total time]
        self.endpoints = {}         # endpoint id -> [count, total bytes]
        self.intervals = {}         # minute -> count

    def add_batch(self, batch):
        """Count a batch of records that arrived over gRPC.

        A batch holds one list per column (all the timestamps, then all the
        server ids, and so on), so we walk them together with zip.
        """
        for timestamp, server, endpoint, status, response_time, nbytes in zip(
                batch.timestamp, batch.server_id, batch.endpoint_id,
                batch.status_code, batch.response_time_micro, batch.bytes_sent):

            self.total += 1
            if status < 400:
                self.success += 1
            else:
                self.failed += 1

            if 200 <= status <= 299:
                self.count_2xx += 1
            elif 300 <= status <= 399:
                self.count_3xx += 1
            elif 400 <= status <= 499:
                self.count_4xx += 1
            elif 500 <= status <= 599:
                self.count_5xx += 1
            # A status outside all four counts in the totals but in no group.

            self.total_bytes += nbytes
            self.total_time += response_time

            if self.min_time is None or response_time < self.min_time:
                self.min_time = response_time
            if self.max_time is None or response_time > self.max_time:
                self.max_time = response_time

            # A negative id is not a real server or endpoint, so we skip it
            # here. It still counts in the totals above.
            if server >= 0:
                row = self.servers.get(server)
                if row is None:
                    self.servers[server] = [1, response_time]
                else:
                    row[0] += 1
                    row[1] += response_time

            if endpoint >= 0:
                row = self.endpoints.get(endpoint)
                if row is None:
                    self.endpoints[endpoint] = [1, nbytes]
                else:
                    row[0] += 1
                    row[1] += nbytes

            minute = minute_of(timestamp)
            self.intervals[minute] = self.intervals.get(minute, 0) + 1

    def add(self, other):
        """Add another Stats into this one."""
        self.total += other.total
        self.success += other.success
        self.failed += other.failed
        self.count_2xx += other.count_2xx
        self.count_3xx += other.count_3xx
        self.count_4xx += other.count_4xx
        self.count_5xx += other.count_5xx
        self.total_bytes += other.total_bytes
        self.total_time += other.total_time

        if other.min_time is not None:
            if self.min_time is None or other.min_time < self.min_time:
                self.min_time = other.min_time
        if other.max_time is not None:
            if self.max_time is None or other.max_time > self.max_time:
                self.max_time = other.max_time

        for server, (count, time_sum) in other.servers.items():
            row = self.servers.get(server)
            if row is None:
                self.servers[server] = [count, time_sum]
            else:
                row[0] += count
                row[1] += time_sum

        for endpoint, (count, nbytes) in other.endpoints.items():
            row = self.endpoints.get(endpoint)
            if row is None:
                self.endpoints[endpoint] = [count, nbytes]
            else:
                row[0] += count
                row[1] += nbytes

        for minute, count in other.intervals.items():
            self.intervals[minute] = self.intervals.get(minute, 0) + count

    def busiest_minute(self):
        """The minute with the most requests. If two are equal, the earlier."""
        best_minute = 0
        best_count = 0
        for minute in sorted(self.intervals):
            if self.intervals[minute] > best_count:
                best_count = self.intervals[minute]
                best_minute = minute
        if best_count == 0:
            return 0, 0
        return best_minute, best_count

    def top_servers(self, k):
        """The k servers with the most requests. Ties go to the smaller id."""
        rows = [(server, row[0], row[1]) for server, row in self.servers.items()]
        rows.sort(key=lambda row: (-row[1], row[0]))
        return rows[:max(k, 0)]

    def top_endpoints(self, k):
        rows = [(endpoint, row[0], row[1]) for endpoint, row in self.endpoints.items()]
        rows.sort(key=lambda row: (-row[1], row[0]))
        return rows[:max(k, 0)]


def to_ms(value):
    """Turn our whole numbers back into milliseconds."""
    return value / UNITS_PER_MS


def average_ms(total_time, count):
    """The average, worked out at the end from the sum and the count."""
    if not count:
        return 0.0
    return to_ms(total_time) / count
