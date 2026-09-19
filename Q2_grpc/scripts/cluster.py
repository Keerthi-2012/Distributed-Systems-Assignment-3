"""cluster.py - starts a coordinator + W workers as local processes (tests and benchmarks).

    with LocalCluster(workers=4, strategy="round_robin") as c:
        c.stream("data/medium.in", batch_size=1000)
        print(c.final_report())

On the RCE cluster the same programs are started on separate nodes instead
(see scripts/rce_*.sh); nothing in the programs is specific to running locally.
"""

import json
import os
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(ROOT, "src")
PYTHON = os.environ.get("PYTHON", sys.executable)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def py(script, *args):
    return [PYTHON, os.path.join(SRC, script)] + [str(a) for a in args]


class LocalCluster:
    def __init__(self, workers=2, strategy="round_robin", queue_cap=64, sync_interval=0.5,
                 log_dir=None, host="127.0.0.1"):
        self.n_workers = workers
        self.strategy = strategy
        self.queue_cap = queue_cap
        self.sync_interval = sync_interval
        self.host = host
        self.log_dir = log_dir
        self.procs = []
        self.worker_procs = []
        self.coord_proc = None
        self.address = None

    def _spawn(self, cmd, name):
        if self.log_dir:
            os.makedirs(self.log_dir, exist_ok=True)
            err = open(os.path.join(self.log_dir, name + ".log"), "w")
        else:
            err = subprocess.DEVNULL
        p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=err, cwd=SRC)
        self.procs.append(p)
        return p

    def __enter__(self):
        addrs = []
        for i in range(self.n_workers):
            a = "%s:%d" % (self.host, free_port())
            addrs.append(a)
            self.worker_procs.append(self._spawn(py("worker.py", a), "worker%d" % i))
        self.address = "%s:%d" % (self.host, free_port())
        self.coord_proc = self._spawn(
            py("coordinator.py", self.address, "--workers", ",".join(addrs),
               "--strategy", self.strategy, "--queue-cap", self.queue_cap,
               "--sync-interval", self.sync_interval), "coordinator")
        self.wait_ready()
        return self

    def wait_ready(self, timeout=60):
        import grpc
        sys.path.insert(0, SRC)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.coord_proc.poll() is not None:
                raise RuntimeError("coordinator exited early")
            ch = grpc.insecure_channel(self.address)
            try:
                grpc.channel_ready_future(ch).result(timeout=1)
                ch.close()
                return
            except grpc.FutureTimeoutError:
                ch.close()
        raise RuntimeError("coordinator did not start")

    def __exit__(self, *exc):
        for p in self.procs:
            if p.poll() is None:
                p.terminate()
        for p in self.procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()

    # ---------------------------------------------------------------- helpers

    def stream(self, dataset, batch_size=1000, rate=0, preload=False, wait=True,
               source_id="client", background=False):
        cmd = py("stream_client.py", self.address, os.path.abspath(dataset),
                 "--batch-size", batch_size,
                 "--rate", rate, "--json", "--quiet", "--source-id", source_id)
        if preload:
            cmd.append("--preload")
        if wait:
            cmd.append("--wait")
        if background:
            return subprocess.Popen(cmd, stdout=subprocess.PIPE, cwd=SRC, text=True)
        out = subprocess.run(cmd, stdout=subprocess.PIPE, check=True, cwd=SRC, text=True)
        return json.loads(out.stdout.strip().splitlines()[-1])

    def final_report(self, k=0):
        cmd = py("query_client.py", self.address, "--final")
        if k:
            cmd += ["--k", str(k)]
        return subprocess.run(cmd, stdout=subprocess.PIPE, check=True, cwd=SRC,
                              text=True).stdout

    def reset(self):
        import grpc
        sys.path.insert(0, SRC)
        import loganalytics_pb2 as pb
        import loganalytics_pb2_grpc as rpc
        with grpc.insecure_channel(self.address) as ch:
            rpc.LogAnalyticsStub(ch).Reset(pb.ResetRequest(), timeout=120)

    def pids(self):
        return [p.pid for p in self.worker_procs], self.coord_proc.pid
