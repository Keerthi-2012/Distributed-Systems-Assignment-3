"""worker.py - an analytics worker.

Receives record batches from the coordinator over one long-lived client stream
(Worker.Process), counts them into a mergeable Stats object, and hands the
counts back when the coordinator pulls (Worker.PullDelta).

Delta protocol (exactly-once merge)
-----------------------------------
The worker keeps two Stats objects:
    current  - records counted since the last delta was cut
    pending  - the last delta sent, numbered by `epoch`, not yet acknowledged

PullDelta(ack_epoch):
    ack_epoch == pending epoch  -> the coordinator merged it: drop it, cut a new
                                   delta from `current` with epoch + 1, send it.
    otherwise                   -> the previous reply was lost: fold `current`
                                   into `pending` and re-send it with the SAME
                                   epoch. The coordinator merges each epoch once.

Run:
    python3 worker.py 0.0.0.0:50061
"""

import argparse
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


class WorkerService(rpc.WorkerServicer):
    def __init__(self, name):
        self.name = name
        self.lock = threading.Lock()
        self.current = Stats()
        self.pending = Stats()
        self.pending_epoch = 0          # epoch 0 = "nothing sent yet"
        self.records_processed = 0
        self.batches_processed = 0

    def Process(self, request_iterator, context):
        for batch in request_iterator:
            with self.lock:
                self.current.add_columns(batch.timestamp, batch.server_id,
                                         batch.endpoint_id, batch.status_code,
                                         batch.response_time_micro, batch.bytes_sent)
                self.records_processed += len(batch.timestamp)
                self.batches_processed += 1
        with self.lock:
            return pb.ProcessAck(records_processed=self.records_processed,
                                 batches_processed=self.batches_processed)

    def PullDelta(self, request, context):
        with self.lock:
            if request.ack_epoch == self.pending_epoch:
                self.pending = self.current
                self.pending_epoch += 1
            else:
                self.pending.merge(self.current)
            self.current = Stats()
            delta = self.pending
            epoch = self.pending_epoch
            processed = self.records_processed
            batches = self.batches_processed
        # Serialising outside the lock keeps ingestion running meanwhile. The
        # pending object is never modified again until the next PullDelta, and
        # pulls are serialised by the coordinator.
        msg = delta.to_proto(pb.PartialStats())
        msg.epoch = epoch
        msg.records_processed = processed
        msg.batches_processed = batches
        return msg

    def Reset(self, request, context):
        with self.lock:
            self.current = Stats()
            self.pending = Stats()          # epoch number is kept on purpose
            self.records_processed = 0
            self.batches_processed = 0
        return pb.ResetResponse(ok=True, message="%s reset" % self.name)


def main():
    ap = argparse.ArgumentParser(description="Log analytics worker")
    ap.add_argument("address", nargs="?", default="0.0.0.0:50061",
                    help="host:port to listen on (default 0.0.0.0:50061)")
    ap.add_argument("--threads", type=int, default=8)
    args = ap.parse_args()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=args.threads),
                         options=GRPC_OPTIONS)
    rpc.add_WorkerServicer_to_server(WorkerService(args.address), server)
    if server.add_insecure_port(args.address) == 0:
        print("error: cannot listen on %s" % args.address, file=sys.stderr)
        sys.exit(1)
    server.start()
    print("[worker] listening on %s" % args.address, file=sys.stderr, flush=True)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    while not stop.is_set():
        time.sleep(0.2)
    server.stop(grace=1)


if __name__ == "__main__":
    main()
