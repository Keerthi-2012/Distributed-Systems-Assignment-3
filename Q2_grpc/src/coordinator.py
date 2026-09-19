"""coordinator.py - the gRPC server that receives the stream and owns the global view.

    stream_client --StreamRecords--> [route] --queue--> sender thread --Process--> worker i
    dashboard     --GetAnalytics---> [sync: PullDelta from all workers, merge] --> snapshot

Responsibilities
  * Ingestion: accepts any number of concurrent client streams and routes every
    batch to a worker with one of three strategies:
        round_robin   whole batches, in turn                      (default)
        least_loaded  whole batches, to the shortest queue
        hash_server   records split by server_id % W (key partitioning)
  * Backpressure: every worker has a bounded queue. When a worker falls behind
    its queue fills, put() blocks, the ingest handler stops reading, and gRPC
    flow control slows the client down. Memory stays bounded.
  * Global state: workers are pulled for DELTAS, which are merged into one
    global Stats (an incremental reduce). Pulls are "single-flight": concurrent
    queries share one sync instead of each hitting every worker.

Run:
    python3 coordinator.py 0.0.0.0:50051 --workers node02:50061,node03:50061
"""

import argparse
import collections
import queue
import signal
import sys
import threading
import time
from concurrent import futures

import grpc

import loganalytics_pb2 as pb
import loganalytics_pb2_grpc as rpc
from analytics import Stats

GRPC_OPTIONS = [
    ("grpc.max_send_message_length", 256 * 1024 * 1024),
    ("grpc.max_receive_message_length", 256 * 1024 * 1024),
]

STRATEGIES = ("round_robin", "least_loaded", "hash_server")


def log(msg):
    print("[coordinator] " + msg, file=sys.stderr, flush=True)


class WorkerLink:
    """Connection to one worker: a bounded queue drained into a Process stream."""

    def __init__(self, address, queue_cap):
        self.address = address
        self.channel = grpc.insecure_channel(address, options=GRPC_OPTIONS)
        self.stub = rpc.WorkerStub(self.channel)
        self.queue = queue.Queue(maxsize=queue_cap)
        self.routed = 0            # records handed to this worker
        self.processed = 0         # records the worker has counted (last pull)
        self.last_epoch = 0        # last delta epoch merged
        self.healthy = True
        self.thread = threading.Thread(target=self._sender, daemon=True,
                                       name="send-" + address)

    def wait_ready(self, timeout):
        grpc.channel_ready_future(self.channel).result(timeout=timeout)

    def start(self):
        self.thread.start()

    def _batches(self):
        while True:
            item = self.queue.get()
            if item is None:
                return
            yield item

    def _sender(self):
        try:
            self.stub.Process(self._batches())
        except grpc.RpcError as e:
            self.healthy = False
            log("worker %s stream failed: %s" % (self.address, e.code()))

    def stop(self):
        try:
            self.queue.put_nowait(None)
        except queue.Full:
            pass


class Coordinator(rpc.LogAnalyticsServicer):
    def __init__(self, worker_addrs, strategy, queue_cap, connect_timeout):
        self.strategy = strategy
        self.links = [WorkerLink(a, queue_cap) for a in worker_addrs]
        for link in self.links:
            log("connecting to worker %s ..." % link.address)
            link.wait_ready(connect_timeout)
        for link in self.links:
            link.start()
        log("%d workers connected, strategy=%s" % (len(self.links), strategy))

        self.pool = futures.ThreadPoolExecutor(max_workers=max(4, len(self.links)))

        # ingestion bookkeeping (protected by ingest_lock)
        self.ingest_lock = threading.Lock()
        self.rr_next = 0
        self.records_ingested = 0
        self.active_streams = 0
        self.k = 10

        # global analytics (protected by state_lock)
        self.state_lock = threading.Lock()
        self.state = Stats()
        self.version = 0
        # The analytics part of a snapshot only changes when a delta is merged,
        # so it is built once per (version, k) and copied for every other query.
        self.snap_cache = {}

        # single-flight sync
        self.sync_cv = threading.Condition()
        self.sync_running = False
        self.sync_started = 0
        self.sync_done = 0

        self.start_time = time.time()
        self.rate_hist = collections.deque(maxlen=64)   # (t, ingested, processed)

    # ------------------------------------------------------------------ ingest

    def _route(self, batch, context):
        n = len(batch.timestamp)
        if n == 0:
            return
        # A failed worker would never drain its queue, so stop routing to it.
        links = [l for l in self.links if l.healthy]
        if not links or (self.strategy == "hash_server" and len(links) != len(self.links)):
            context.abort(grpc.StatusCode.UNAVAILABLE, "a worker has failed")
        with self.ingest_lock:
            self.records_ingested += n

        if self.strategy == "hash_server":
            self._route_hash(batch)
            return

        with self.ingest_lock:
            if self.strategy == "least_loaded":
                link = min(links, key=lambda l: (l.queue.qsize(), l.routed - l.processed))
            else:
                link = links[self.rr_next % len(links)]
                self.rr_next += 1
            link.routed += n
        link.queue.put(batch)            # blocks when full -> backpressure

    def _route_hash(self, batch):
        w = len(self.links)
        buckets = [[] for _ in range(w)]
        for i, s in enumerate(batch.server_id):
            buckets[s % w].append(i)
        cols = (list(batch.timestamp), list(batch.server_id), list(batch.endpoint_id),
                list(batch.user_id), list(batch.status_code),
                list(batch.response_time_micro), list(batch.bytes_sent))
        for link, idx in zip(self.links, buckets):
            if not idx:
                continue
            part = pb.RecordBatch(seq=batch.seq)
            part.timestamp.extend([cols[0][i] for i in idx])
            part.server_id.extend([cols[1][i] for i in idx])
            part.endpoint_id.extend([cols[2][i] for i in idx])
            part.user_id.extend([cols[3][i] for i in idx])
            part.status_code.extend([cols[4][i] for i in idx])
            part.response_time_micro.extend([cols[5][i] for i in idx])
            part.bytes_sent.extend([cols[6][i] for i in idx])
            with self.ingest_lock:
                link.routed += len(idx)
            link.queue.put(part)

    def StreamRecords(self, request_iterator, context):
        with self.ingest_lock:
            self.active_streams += 1
        received = batches = 0
        t_first = None
        source = "?"
        try:
            for msg in request_iterator:
                if t_first is None:
                    t_first = time.time()
                if msg.HasField("batch"):
                    self._route(msg.batch, context)
                    received += len(msg.batch.timestamp)
                    batches += 1
                elif msg.HasField("header"):
                    source = msg.header.source_id or "?"
                    with self.ingest_lock:
                        self.k = msg.header.k
                    log("stream from %s: N=%d K=%d S=%d" %
                        (source, msg.header.n, msg.header.k, msg.header.s))
        finally:
            with self.ingest_lock:
                self.active_streams -= 1
        secs = time.time() - t_first if t_first else 0.0
        log("stream from %s finished: %d records in %d batches, %.3fs" %
            (source, received, batches, secs))
        return pb.IngestSummary(records_received=received, batches_received=batches,
                                seconds=secs)

    # ------------------------------------------------------------------ sync

    def sync(self):
        """Pulls deltas from every worker and merges them.

        Single-flight: a caller needs a sync that STARTS after it arrives. If one
        is already running, the caller waits and the waiters share the next one.
        """
        with self.sync_cv:
            target = self.sync_started + 1
            while self.sync_done < target:
                if not self.sync_running:
                    self.sync_running = True
                    self.sync_started += 1
                    gen = self.sync_started
                    break
                self.sync_cv.wait()
            else:
                return
        try:
            self._pull_all()
        finally:
            with self.sync_cv:
                self.sync_running = False
                self.sync_done = gen
                self.sync_cv.notify_all()

    def _pull_one(self, link):
        try:
            return link, link.stub.PullDelta(pb.PullRequest(ack_epoch=link.last_epoch),
                                             timeout=30)
        except grpc.RpcError as e:
            # Not fatal: the epoch protocol re-sends the lost delta on the next
            # pull. A worker that is really gone fails its Process stream too.
            log("pull from %s failed: %s" % (link.address, e.code()))
            return link, None

    def _pull_all(self):
        results = list(self.pool.map(self._pull_one, self.links))
        with self.state_lock:
            for link, delta in results:
                if delta is None:
                    continue
                if delta.epoch > link.last_epoch:     # merge each epoch exactly once
                    self.state.merge_proto(delta)
                    link.last_epoch = delta.epoch
                link.processed = delta.records_processed
            self.version += 1
        self._record_rates()

    def _record_rates(self):
        now = time.time()
        ingested = self.records_ingested
        processed = sum(l.processed for l in self.links)
        self.rate_hist.append((now, ingested, processed))

    def _rates(self):
        h = self.rate_hist
        if len(h) < 2:
            return 0.0, 0.0
        newest = h[-1]
        oldest = h[0]
        for item in h:                       # look back about 3 seconds
            if newest[0] - item[0] <= 3.0:
                oldest = item
                break
        dt = newest[0] - oldest[0]
        if dt <= 0:
            return 0.0, 0.0
        return (newest[1] - oldest[1]) / dt, (newest[2] - oldest[2]) / dt

    def is_complete(self):
        with self.ingest_lock:
            if self.active_streams > 0:
                return False
            ingested = self.records_ingested
        return all(l.healthy for l in self.links) and \
            sum(l.processed for l in self.links) == ingested

    # ------------------------------------------------------------------ queries

    def snapshot(self, k):
        with self.ingest_lock:
            if k <= 0:
                k = self.k
            ingested = self.records_ingested
            active = self.active_streams
        s = self.state
        with self.state_lock:
            key = (self.version, k)
            cached = self.snap_cache.get(key)
            if cached is None:
                cached = self._build_analytics(s, k)
                if len(self.snap_cache) > 16 or \
                        any(v != self.version for v, _ in self.snap_cache):
                    self.snap_cache.clear()
                self.snap_cache[key] = cached
            snap = pb.AnalyticsSnapshot()
            snap.CopyFrom(cached)
            # read under the same lock as the analytics, so processed == total
            processed = 0
            for l in self.links:
                snap.workers.add(address=l.address, records_routed=l.routed,
                                 records_processed=l.processed,
                                 queue_depth=l.queue.qsize(), healthy=l.healthy)
                processed += l.processed
        snap.records_ingested = ingested
        snap.records_processed = processed
        snap.active_streams = active
        snap.uptime_seconds = time.time() - self.start_time
        snap.ingest_rate, snap.process_rate = self._rates()
        snap.strategy = self.strategy
        snap.complete = (active == 0 and processed == ingested and
                         all(l.healthy for l in self.links))
        return snap

    def _build_analytics(self, s, k):
        """The Q7 part of a snapshot (caller holds state_lock)."""
        snap = pb.AnalyticsSnapshot(
            total_requests=s.total,
            successful_requests=s.success,
            failed_requests=s.failed,
            response_time_sum_micro=s.time_sum,
            min_response_time_micro=s.time_min if s.total else 0,
            max_response_time_micro=s.time_max if s.total else 0,
            total_bytes=s.total_bytes,
            status_2xx=s.c2xx, status_3xx=s.c3xx,
            status_4xx=s.c4xx, status_5xx=s.c5xx,
            busiest_interval=s.busiest_interval if s.busiest_count else 0,
            busiest_count=s.busiest_count,
            k=k,
            snapshot_version=self.version,
            distinct_servers=len(s.server_count),
            distinct_endpoints=len(s.endpoint_count),
            distinct_intervals=len(s.interval_count),
        )
        for i, c, t in s.top_servers(k):
            snap.top_servers.add(server_id=i, count=c, response_time_micro=t)
        for i, c, b in s.top_endpoints(k):
            snap.top_endpoints.add(endpoint_id=i, count=c, bytes=b)
        return snap

    def GetAnalytics(self, request, context):
        if request.wait_for_completion:
            deadline = time.time() + request.timeout_seconds \
                if request.timeout_seconds > 0 else None
            while True:
                self.sync()
                if self.is_complete():
                    break
                if deadline is not None and time.time() > deadline:
                    context.abort(grpc.StatusCode.DEADLINE_EXCEEDED,
                                  "stream not fully processed before timeout")
                if not context.is_active():
                    return pb.AnalyticsSnapshot()
                time.sleep(0.01)
        elif request.fresh:
            self.sync()
        return self.snapshot(request.k)

    def WatchAnalytics(self, request, context):
        interval = request.interval_seconds if request.interval_seconds > 0 else 1.0
        while context.is_active():
            self.sync()
            yield self.snapshot(request.k)
            time.sleep(interval)

    def Reset(self, request, context):
        with self.ingest_lock:
            if self.active_streams > 0:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                              "cannot reset while %d stream(s) are active"
                              % self.active_streams)
        # Drain in-flight work first so nothing counted before the reset leaks
        # into the state after it.
        deadline = time.time() + 60
        while not self.is_complete() and time.time() < deadline:
            self.sync()
            time.sleep(0.01)
        # Take the sync slot so no pull runs while workers are being reset.
        with self.sync_cv:
            while self.sync_running:
                self.sync_cv.wait()
            self.sync_running = True
        try:
            for link in self.links:
                link.stub.Reset(pb.ResetRequest(), timeout=10)
            with self.state_lock:
                self.state = Stats()
                self.version += 1
            with self.ingest_lock:
                self.records_ingested = 0
                for link in self.links:
                    link.routed = 0
                    link.processed = 0
            self.rate_hist.clear()
            self.start_time = time.time()
        finally:
            with self.sync_cv:
                self.sync_running = False
                self.sync_cv.notify_all()
        log("state reset")
        return pb.ResetResponse(ok=True, message="reset %d workers" % len(self.links))

    # ------------------------------------------------------------------ background

    def background_sync(self, interval, stop):
        while not stop.wait(interval):
            try:
                self.sync()
            except Exception as e:           # keep the loop alive
                log("background sync error: %s" % e)


def main():
    ap = argparse.ArgumentParser(description="Log analytics coordinator")
    ap.add_argument("address", nargs="?", default="0.0.0.0:50051",
                    help="host:port to listen on (default 0.0.0.0:50051)")
    ap.add_argument("--workers", required=True,
                    help="comma-separated worker addresses, e.g. node02:50061,node03:50061")
    ap.add_argument("--strategy", choices=STRATEGIES, default="round_robin")
    ap.add_argument("--queue-cap", type=int, default=64,
                    help="max batches waiting per worker (backpressure bound)")
    ap.add_argument("--sync-interval", type=float, default=0.5,
                    help="seconds between background delta pulls (0 = only on query)")
    ap.add_argument("--threads", type=int, default=32)
    ap.add_argument("--connect-timeout", type=float, default=120)
    args = ap.parse_args()

    addrs = [a.strip() for a in args.workers.split(",") if a.strip()]
    if not addrs:
        ap.error("need at least one worker")

    coord = Coordinator(addrs, args.strategy, args.queue_cap, args.connect_timeout)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=args.threads),
                         options=GRPC_OPTIONS)
    rpc.add_LogAnalyticsServicer_to_server(coord, server)
    if server.add_insecure_port(args.address) == 0:
        log("error: cannot listen on %s" % args.address)
        sys.exit(1)
    server.start()
    log("listening on %s" % args.address)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    if args.sync_interval > 0:
        threading.Thread(target=coord.background_sync, args=(args.sync_interval, stop),
                         daemon=True).start()
    while not stop.is_set():
        time.sleep(0.2)
    for link in coord.links:
        link.stop()
    server.stop(grace=1)


if __name__ == "__main__":
    main()
