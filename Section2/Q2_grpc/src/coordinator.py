"""coordinator.py - the server the clients talk to.

    python3 coordinator.py 0.0.0.0:50051 --workers node02:50061,node03:50061

It has three jobs:

 1. Take the records the stream client sends, and hand each batch to one worker.
 2. Ask the workers for their numbers and add them up.
 3. Answer questions about the analytics, even while records are still arriving.

How the answer is built: every worker keeps its own running totals, and the
coordinator asks all of them and adds the answers together. Asking again is
always safe, because we replace a worker's numbers rather than adding to them.
"""

import argparse
import queue
import sys
import threading
import time
from concurrent import futures

import grpc

import analytics
import loganalytics_pb2 as pb
import loganalytics_pb2_grpc as pb_grpc

STRATEGIES = ("round_robin", "least_loaded", "hash_server")


class Worker:
    """One worker, as seen from here."""

    def __init__(self, index, address, queue_size):
        self.index = index
        self.address = address
        self.stub = pb_grpc.WorkerStub(
            grpc.insecure_channel(address, options=analytics.BIG_MESSAGES))

        # Batches waiting to be sent to this worker. The size limit is what
        # stops us using unlimited memory: when a worker falls behind, its queue
        # fills up, put() waits, we stop reading from the client, and gRPC slows
        # the client down for us.
        self.queue = queue.Queue(maxsize=queue_size)

        self.records_sent = 0
        self.records_counted = 0     # what the worker told us last time we asked
        self.alive = True


class Coordinator(pb_grpc.LogAnalyticsServicer):
    def __init__(self, addresses, strategy, queue_size):
        self.workers = [Worker(i, address, queue_size)
                        for i, address in enumerate(addresses)]
        self.strategy = strategy
        self.started = time.time()

        self.next_worker = 0         # for round robin
        self.records_received = 0
        self.streams_running = 0
        self.streams_seen = 0        # how many streams have started, ever
        self.k = 10                  # how many rows in the top lists
        self.lock = threading.Lock()

        # One thread per worker, sending it the batches from its queue.
        for worker in self.workers:
            threading.Thread(target=self.send_batches, args=(worker,),
                             daemon=True).start()

    # ---------------------------------------------------------- taking data --

    def StreamRecords(self, request_iterator, context):
        """The stream client sends its records here, one batch at a time."""
        with self.lock:
            self.streams_running += 1
            self.streams_seen += 1
        start = time.time()
        records = 0
        batches = 0

        try:
            for message in request_iterator:
                if message.HasField("header"):
                    # The first message says how many records and what K is.
                    with self.lock:
                        self.k = message.header.k
                    continue

                batch = message.batch
                if len(batch.timestamp) == 0:
                    continue

                self.give_to_a_worker(batch)
                records += len(batch.timestamp)
                batches += 1
                with self.lock:
                    self.records_received += len(batch.timestamp)
        finally:
            with self.lock:
                self.streams_running -= 1

        return pb.IngestSummary(records_received=records, batches_received=batches,
                                seconds=time.time() - start, accepted=True)

    def give_to_a_worker(self, batch):
        """Choose a worker for this batch and put it on that worker's queue."""
        alive = [w for w in self.workers if w.alive]
        if not alive:
            return

        if self.strategy == "hash_server":
            self.split_by_server(batch, alive)
            return

        if self.strategy == "least_loaded":
            # Give it to whoever has the shortest queue.
            worker = min(alive, key=lambda w: w.queue.qsize())
        else:
            # round_robin: simply take turns.
            with self.lock:
                worker = alive[self.next_worker % len(alive)]
                self.next_worker += 1

        worker.records_sent += len(batch.timestamp)
        worker.queue.put(batch)          # waits here if the queue is full

    def split_by_server(self, batch, alive):
        """Send each record to the worker for its server id.

        This is what you need when a worker must keep all the information about
        one server. Our analytics do not need that, so this is here to compare
        against the other two. It is slower, because the coordinator has to look
        at every record instead of every batch.
        """
        pieces = [pb.RecordBatch() for _ in alive]
        for i in range(len(batch.timestamp)):
            piece = pieces[batch.server_id[i] % len(alive)]
            piece.timestamp.append(batch.timestamp[i])
            piece.server_id.append(batch.server_id[i])
            piece.endpoint_id.append(batch.endpoint_id[i])
            piece.user_id.append(batch.user_id[i])
            piece.status_code.append(batch.status_code[i])
            piece.response_time_micro.append(batch.response_time_micro[i])
            piece.bytes_sent.append(batch.bytes_sent[i])

        for worker, piece in zip(alive, pieces):
            if len(piece.timestamp) > 0:
                worker.records_sent += len(piece.timestamp)
                worker.queue.put(piece)

    def send_batches(self, worker):
        """Take batches off this worker's queue and send them, forever.

        All the batches for one worker travel on a single long-lived stream, so
        we do not pay to set up a call for every batch.
        """
        def batches_from_queue():
            while True:
                yield worker.queue.get()

        try:
            worker.stub.Process(batches_from_queue())
        except grpc.RpcError:
            # The worker died. Stop sending to it, so its queue cannot fill up
            # and block the whole stream.
            worker.alive = False

    # ------------------------------------------------------------ answering --

    def collect(self):
        """Ask every worker for its numbers and add them up."""
        total = analytics.Stats()

        for worker in self.workers:
            try:
                reply = worker.stub.GetStats(pb.StatsRequest(), timeout=30)
            except grpc.RpcError:
                worker.alive = False
                continue
            total.add(message_to_stats(reply))
            worker.records_counted = reply.total

        return total

    def GetAnalytics(self, request, context):
        """Answer a question about the analytics."""
        if request.wait_for_completion:
            # Wait until a stream has finished AND the workers have counted
            # everything it sent us.
            #
            # "streams_seen > 0" matters: without it, a query that arrives a
            # moment before the stream starts sees no stream running and nothing
            # received, decides it is finished, and answers with zeros.
            limit = time.time() + (request.timeout_seconds or 600)
            while time.time() < limit:
                stats = self.collect()
                with self.lock:
                    received = self.records_received
                    running = self.streams_running
                    seen = self.streams_seen
                if seen > 0 and running == 0 and stats.total >= received:
                    return self.make_answer(stats, request.k)
                time.sleep(0.05)

        return self.make_answer(self.collect(), request.k)

    def WatchAnalytics(self, request, context):
        """Send a new answer every few seconds, for the dashboard."""
        gap = request.interval_seconds or 1.0
        while context.is_active():
            yield self.make_answer(self.collect(), request.k)
            time.sleep(gap)

    def Reset(self, request, context):
        """Forget everything, in here and in the workers."""
        # Throw away batches that are still waiting to be sent. Without this
        # they would arrive AFTER the worker was reset, and be counted into the
        # next run.
        for worker in self.workers:
            while not worker.queue.empty():
                try:
                    worker.queue.get_nowait()
                except queue.Empty:
                    break

        for worker in self.workers:
            try:
                worker.stub.Reset(pb.ResetRequest(), timeout=10)
            except grpc.RpcError:
                pass
            worker.records_sent = 0
            worker.records_counted = 0
            worker.alive = True
        with self.lock:
            self.records_received = 0
            self.streams_seen = 0
        return pb.ResetResponse(ok=True)

    def make_answer(self, stats, k):
        """Build the reply message from the added-up numbers."""
        with self.lock:
            received = self.records_received
            running = self.streams_running
            if k <= 0:
                k = self.k

        busiest_minute, busiest_count = stats.busiest_minute()

        answer = pb.AnalyticsSnapshot(
            total_requests=stats.total,
            successful_requests=stats.success,
            failed_requests=stats.failed,
            time_sum_micro=stats.total_time,
            time_min_micro=stats.min_time if stats.total else 0,
            time_max_micro=stats.max_time if stats.total else 0,
            total_bytes=stats.total_bytes,
            status_2xx=stats.count_2xx,
            status_3xx=stats.count_3xx,
            status_4xx=stats.count_4xx,
            status_5xx=stats.count_5xx,
            busiest_interval=busiest_minute,
            busiest_count=busiest_count,
            records_ingested=received,
            records_processed=stats.total,
            active_streams=running,
            complete=(running == 0 and received > 0 and stats.total >= received),
            strategy=self.strategy,
            uptime_seconds=time.time() - self.started,
            k=k,
            distinct_servers=len(stats.servers),
            distinct_endpoints=len(stats.endpoints),
            distinct_intervals=len(stats.intervals),
        )

        # The top lists are worked out from the added-up numbers, never from
        # each worker's own top list: a server that is second on every worker
        # could still be first overall.
        for server, count, time_sum in stats.top_servers(k):
            answer.top_servers.add(server_id=server, count=count,
                                   time_sum_micro=time_sum)
        for endpoint, count, nbytes in stats.top_endpoints(k):
            answer.top_endpoints.add(endpoint_id=endpoint, count=count, bytes=nbytes)

        for worker in self.workers:
            answer.workers.add(index=worker.index, address=worker.address,
                               records_routed=worker.records_sent,
                               records_processed=worker.records_counted,
                               queue_depth=worker.queue.qsize(),
                               healthy=worker.alive)
        return answer


def message_to_stats(message):
    """Turn a worker's reply back into a Stats we can add up."""
    stats = analytics.Stats()
    stats.total = message.total
    stats.success = message.success
    stats.failed = message.failed
    stats.count_2xx = message.count_2xx
    stats.count_3xx = message.count_3xx
    stats.count_4xx = message.count_4xx
    stats.count_5xx = message.count_5xx
    stats.total_bytes = message.total_bytes
    stats.total_time = message.time_sum_micro
    if message.total > 0:
        stats.min_time = message.time_min_micro
        stats.max_time = message.time_max_micro

    for server, count, time_sum in zip(message.server_id, message.server_count,
                                       message.server_time_micro):
        stats.servers[server] = [count, time_sum]
    for endpoint, count, nbytes in zip(message.endpoint_id, message.endpoint_count,
                                       message.endpoint_bytes):
        stats.endpoints[endpoint] = [count, nbytes]
    for minute, count in zip(message.interval_id, message.interval_count):
        stats.intervals[minute] = count

    return stats


def main():
    parser = argparse.ArgumentParser(description="Q7 streaming coordinator")
    parser.add_argument("address", nargs="?", default="0.0.0.0:50051")
    parser.add_argument("--workers", required=True,
                        help="worker addresses, e.g. node02:50061,node03:50061")
    parser.add_argument("--strategy", choices=STRATEGIES, default="round_robin")
    parser.add_argument("--queue-cap", type=int, default=64,
                        help="how many batches may wait for one worker")
    args = parser.parse_args()

    addresses = [a.strip() for a in args.workers.split(",") if a.strip()]
    service = Coordinator(addresses, args.strategy, args.queue_cap)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32),
                         options=analytics.BIG_MESSAGES)
    pb_grpc.add_LogAnalyticsServicer_to_server(service, server)
    if server.add_insecure_port(args.address) == 0:
        print("coordinator: cannot listen on %s" % args.address, file=sys.stderr)
        return 1

    server.start()
    print("[coordinator] listening on %s, %d workers, strategy %s"
          % (args.address, len(addresses), args.strategy), file=sys.stderr)
    server.wait_for_termination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
