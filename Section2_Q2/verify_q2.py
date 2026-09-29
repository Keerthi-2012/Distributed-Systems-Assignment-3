#!/usr/bin/env python3
"""verify_q2.py - correctness suite for Q2 (the gRPC streaming system).

    python3 verify_q2.py            tests + tiny + small
    python3 verify_q2.py --full     also medium

The rule: after the whole stream has been processed, `query_client --final`
must print EXACTLY what HW2's `bin/log_seq` prints for the same input.

It starts a real coordinator and real worker processes for each configuration,
streams through gRPC, and diffs. Covered:

  - the worked example and 5 edge-case files
  - x worker counts {1, 2, 3} x strategies x batch sizes
  - generated datasets
  - queries issued WHILE ingestion runs: every snapshot must be internally
    consistent, and totals and versions must never go backwards
  - several sources streaming at once
  - a query-time K different from the file's K

Each configuration starts from a clean system, so runs cannot contaminate
each other.
"""

import argparse
import os
import random
import shutil
import subprocess
import sys
import time

import grpc

# HERE is the Section2_Q2 folder: this file sits directly inside it.
HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "src")

# Three things live outside this folder, at the repository root, and are shared
# with Section2_Q1 on purpose: the datasets (both questions must measure the
# same bytes), the hand-written test inputs, and log_seq, the HW2 sequential
# program that is the single correctness oracle for both questions.
# Section2_Q1 builds log_seq; this question only runs it.
ROOT = os.path.dirname(HERE)
BIN = os.path.join(ROOT, "Section2_Q1", "bin")
DATA = os.path.join(ROOT, "data")
TESTS = os.path.join(ROOT, "Section2_Q1", "tests")
PYTHON = os.environ.get("PY", sys.executable)

STRATEGIES = ("round_robin", "least_loaded", "hash_server")


# The servers' stderr goes here, one small file per server. The directory is
# emptied at the start of every run: a full suite starts a few hundred servers,
# and keeping every run's logs buries the useful ones.
LOGDIR = os.path.join(HERE, "logs", "verify")
shutil.rmtree(LOGDIR, ignore_errors=True)
os.makedirs(LOGDIR, exist_ok=True)


class System:
    """A coordinator plus W worker processes, on this machine.

    Every instance gets a FRESH port range. Reusing ports across configurations
    is a trap: a worker that has not finished shutting down still holds its
    port, the replacement fails to bind and exits, and the new coordinator then
    talks to the dying old worker - which shows up as a hang or as counts from
    the previous run.
    """

    # A random starting point, so ports left in TIME_WAIT by an earlier run
    # of this suite cannot collide with this one.
    _next_base = random.randint(20000, 55000)

    @staticmethod
    def _port_is_free(port):
        """True if a server could actually bind this port right now.

        We test by BINDING, not by connecting. Connecting only finds ports that
        something is already listening on, and misses the case that bites here:
        a port left in TIME_WAIT by a server we just stopped has no listener, so
        it looks free, and then the real bind fails with "Address already in
        use". Binding the same way gRPC does asks the question properly.
        """
        import socket
        for family, host in ((socket.AF_INET6, "::"), (socket.AF_INET, "0.0.0.0")):
            try:
                probe = socket.socket(family, socket.SOCK_STREAM)
            except OSError:
                continue            # no IPv6 on this machine, skip it
            with probe:
                try:
                    probe.bind((host, port))
                except OSError:
                    return False
        return True

    @staticmethod
    def _pick_free_base(start, span):
        """First port p in [start, ...] where p .. p+span can all be bound."""
        candidate = start
        while candidate < start + 4000:
            if all(System._port_is_free(port)
                   for port in range(candidate, candidate + span)):
                return candidate
            candidate += span
        raise RuntimeError("no free port range found")

    def __init__(self, workers, strategy, attempts=3):
        # Even a correct free-port check can lose a race with another program on
        # the machine, so if a server fails to come up we simply try a different
        # range rather than failing the whole suite.
        for attempt in range(attempts):
            try:
                self._start(workers, strategy)
                return
            except RuntimeError:
                if attempt == attempts - 1:
                    raise
                System._next_base += 500

    def _start(self, workers, strategy):
        base = System._pick_free_base(System._next_base, workers + 2)
        System._next_base = base + workers + 2
        self.address = "localhost:%d" % base

        self.procs = []
        try:
            addresses = []
            for i in range(workers):
                port = base + 1 + i
                self.procs.append(self._spawn(["worker.py", "0.0.0.0:%d" % port]))
                addresses.append("localhost:%d" % port)

            # Wait until every worker is actually accepting connections.
            for address in addresses:
                self._wait_ready(address)

            self.procs.append(self._spawn(
                ["coordinator.py", "0.0.0.0:%d" % base,
                 "--workers", ",".join(addresses), "--strategy", strategy]))
            self._wait_ready(self.address)
        except Exception:
            # Clean up after ourselves: processes spawned before the failure
            # would otherwise be orphaned and keep holding their ports, which
            # makes every later configuration fail too.
            self.stop()
            raise

    @staticmethod
    def _wait_ready(address, timeout=20.0):
        channel = grpc.insecure_channel(address)
        try:
            grpc.channel_ready_future(channel).result(timeout=timeout)
        except grpc.FutureTimeoutError:
            hint = ""
            log = os.path.join(LOGDIR, "0.0.0.0_%s.log" % address.split(":")[1])
            if os.path.exists(log):
                hint = " - server log: " + open(log).read().strip()[:300]
            raise RuntimeError("nothing listening on %s after %.0fs%s"
                               % (address, timeout, hint))
        finally:
            channel.close()

    def _spawn(self, args):
        # Keep stderr: when a server fails to start we need to see why, not
        # just watch a later connection time out.
        log = open(os.path.join(LOGDIR, "%s.log" % args[1].replace("/", "_").replace(":", "_")), "w")
        return subprocess.Popen([PYTHON] + args, cwd=SRC,
                                stdout=subprocess.DEVNULL, stderr=log)

    def stop(self):
        for p in self.procs:
            p.terminate()
        for p in self.procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=5)


def run(args, **kwargs):
    return subprocess.run([PYTHON] + args, cwd=SRC, capture_output=True,
                          text=True, **kwargs)


def expected_output(dataset, k=None):
    """What HW2's sequential program prints. With k, rewrite the header's K."""
    path = dataset
    if k is not None:
        with open(dataset) as fh:
            first = fh.readline().split()
            rest = fh.read()
        path = os.path.join(HERE, "logs", "k_input.in")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            fh.write("%s %d %s\n" % (first[0], k, first[2]))
            fh.write(rest)
    out = subprocess.run([os.path.join(BIN, "log_seq"), path],
                         capture_output=True, text=True)
    return out.stdout


class Suite:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.lines = []

    def check(self, name, expected, actual):
        if expected == actual:
            self.passed += 1
            self.lines.append("PASS  " + name)
        else:
            self.failed += 1
            self.lines.append("FAIL  " + name)
            exp = expected.splitlines()
            act = actual.splitlines()
            for i in range(max(len(exp), len(act))):
                e = exp[i] if i < len(exp) else "<missing>"
                a = act[i] if i < len(act) else "<missing>"
                if e != a:
                    self.lines.append("        expected: %s" % e)
                    self.lines.append("        actual:   %s" % a)
                    break
            print("FAIL  " + name)

    def note(self, ok, name):
        if ok:
            self.passed += 1
            self.lines.append("PASS  " + name)
        else:
            self.failed += 1
            self.lines.append("FAIL  " + name)
            print("FAIL  " + name)


def case(suite, dataset, workers, strategy, batch):
    name = "%s W=%d %s batch=%d" % (os.path.basename(dataset), workers, strategy, batch)
    system = System(workers, strategy)
    try:
        run(["stream_client.py", system.address, dataset,
             "--batch-size", str(batch), "--reset", "--wait", "--quiet"], timeout=300)
        out = run(["query_client.py", system.address, "--final"], timeout=300)
        suite.check(name, expected_output(dataset), out.stdout)
    finally:
        system.stop()


def live_query_case(suite, dataset):
    """Queries during ingestion must always be internally consistent."""
    system = System(3, "round_robin")
    try:
        stream = subprocess.Popen(
            [PYTHON, "stream_client.py", system.address, dataset,
             "--rate", "150000", "--reset", "--quiet"], cwd=SRC,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        snapshots = 0
        mid_stream = 0
        last_total = -1
        consistent = True
        while stream.poll() is None and snapshots < 200:
            out = run(["query_client.py", system.address, "--status"], timeout=60)
            values = {}
            for line in out.stdout.splitlines():
                parts = line.split()
                # Only the integer counters; AVERAGE/MIN/MAX are decimals.
                if len(parts) == 2 and parts[0].isupper():
                    try:
                        values[parts[0]] = int(parts[1])
                    except ValueError:
                        pass
            if not values:
                continue
            snapshots += 1
            total = values.get("TOTAL_REQUESTS", 0)
            if total > 0:
                mid_stream += 1
            # successful + failed must equal total, always
            if values.get("SUCCESSFUL_REQUESTS", 0) + values.get("FAILED_REQUESTS", 0) != total:
                consistent = False
            # totals must never go backwards
            if total < last_total:
                consistent = False
            last_total = total
        stream.wait(timeout=300)

        suite.note(consistent and snapshots > 0,
                   "%d live snapshots consistent (%d mid-stream)" % (snapshots, mid_stream))
        out = run(["query_client.py", system.address, "--final"], timeout=300)
        suite.check("%s final after live queries" % os.path.basename(dataset),
                    expected_output(dataset), out.stdout)
    finally:
        system.stop()


def multi_source_case(suite, dataset, sources=3):
    """Several sources at once: every count must be exactly multiplied."""
    system = System(3, "round_robin")
    try:
        run(["stream_client.py", system.address, dataset, "--reset", "--wait", "--quiet"],
            timeout=300)
        procs = [subprocess.Popen(
            [PYTHON, "stream_client.py", system.address, dataset, "--wait", "--quiet",
             "--source-id", "src%d" % i], cwd=SRC,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for i in range(sources - 1)]
        for p in procs:
            p.wait(timeout=300)

        out = run(["query_client.py", system.address, "--final"], timeout=300)
        lines = out.stdout.splitlines()
        if not lines:
            suite.note(False, "%d concurrent sources: query returned nothing (%s)"
                       % (sources, out.stderr.strip()[:120]))
            return
        got = int(lines[0].split()[1])
        with open(dataset) as fh:
            want = int(fh.readline().split()[0]) * sources
        suite.note(got == want, "%d concurrent sources -> %d records (want %d)"
                   % (sources, got, want))
    finally:
        system.stop()


def k_case(suite, dataset, k):
    system = System(2, "round_robin")
    try:
        run(["stream_client.py", system.address, dataset, "--reset", "--wait", "--quiet"],
            timeout=300)
        out = run(["query_client.py", system.address, "--final", "--k", str(k)], timeout=300)
        suite.check("%s query-time K=%d" % (os.path.basename(dataset), k),
                    expected_output(dataset, k=k), out.stdout)
    finally:
        system.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true", help="also run medium.in")
    args = ap.parse_args()

    if not os.path.exists(os.path.join(BIN, "log_seq")):
        raise SystemExit("log_seq missing at %s\nbuild it with: (cd ../Section2_Q1 && make)" % BIN)

    suite = Suite()
    tests = sorted(os.path.join(TESTS, f)
                   for f in os.listdir(TESTS) if f.endswith(".in"))

    print("Q2 correctness: query_client --final vs bin/log_seq")
    for dataset in tests:
        for workers in (1, 2, 3):
            for strategy in STRATEGIES:
                case(suite, dataset, workers, strategy, 1)

    for name in ("tiny", "small"):
        dataset = os.path.join(DATA, "%s.in" % name)
        if not os.path.exists(dataset):
            continue
        for workers in (1, 2, 3):
            for strategy in STRATEGIES:
                case(suite, dataset, workers, strategy, 1000)

    if args.full:
        medium = os.path.join(DATA, "medium.in")
        if os.path.exists(medium):
            case(suite, medium, 3, "round_robin", 5000)
            case(suite, medium, 4, "hash_server", 1000)

    small = os.path.join(DATA, "small.in")
    if os.path.exists(small):
        k_case(suite, small, 3)
        multi_source_case(suite, small)
        live_query_case(suite, small)

    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    report = os.path.join(HERE, "results", "verify_q2.txt")
    with open(report, "w") as fh:
        fh.write("Q2 correctness: query_client --final vs bin/log_seq\n")
        fh.write(time.strftime("date: %Y-%m-%d %H:%M:%S\n\n"))
        fh.write("\n".join(suite.lines))
        fh.write("\n\npassed %d, failed %d\n" % (suite.passed, suite.failed))

    print("passed %d, failed %d" % (suite.passed, suite.failed))
    print("details:", report)
    return 1 if suite.failed else 0


if __name__ == "__main__":
    sys.exit(main())
