# Section 2 Q2 — Real-time Log Analytics with gRPC

The same HW2 Question 7 analytics, but the data arrives as a **live stream**
instead of a file, and the answers can be asked for at any moment while it is
still arriving. Python, gRPC, several machines.

Cluster instructions: **[run.md](run.md)**. Full write-up: **[report.pdf](report.pdf)**.

---

## 1. What the question asks for

A distributed streaming system with three kinds of participant:

| Participant | Does |
| ----------- | ---- |
| **stream client** | sends log records in, continuously |
| **coordinator** | receives them, spreads them over the workers, keeps the running answer |
| **worker** | counts the records it is given |
| **query client / dashboard** | asks for the analytics *while data is still arriving* |

and four kinds of RPC, one of each shape gRPC offers:

| RPC | Shape | Used for |
| --- | ----- | -------- |
| `StreamRecords` | client-streaming | the whole log stream is **one** call |
| `GetAnalytics` | unary | one question, one answer |
| `WatchAnalytics` | server-streaming | the dashboard is pushed updates |
| `Worker.Process` | client-streaming (internal) | coordinator → worker |
| `Worker.GetStats` | unary (internal) | coordinator asks for totals |

The final answer must equal the HW2 sequential program's, byte for byte.

---

## 2. The design, and why

### The whole stream is one RPC

`StreamRecords` is a single client-streaming call, not one call per batch. The
cost of setting up a call is paid once. gRPC's own flow control then gives
backpressure for free: if the coordinator cannot keep up, sending simply slows
down at the client.

### Records travel in columnar batches

A `RecordBatch` holds one list per field — all the timestamps, then all the
server ids — rather than a list of record objects. Protobuf encodes a list of
integers far more compactly than the same integers spread across many small
messages. Batch size is the single biggest performance lever in the whole
system: **285×** between one record per message and a thousand.

### Workers send totals, not deltas

A worker always sends its **full running totals**, and the coordinator
**replaces** that worker's contribution rather than adding to it. This makes
asking safe to repeat: if a reply is lost, the coordinator just asks again and
nothing is double-counted. The earlier design used epoch-numbered deltas with
acknowledgements; it was the hardest part of the system to explain and deleting
it lost nothing.

### Mergeability, again

As in Q1, every figure is a count, sum, min or max, so partial results combine
in any order. Averages are never stored — sum and count are, and the division
happens at printing time. Response times are whole numbers of millionths of a
millisecond, so no ordering of workers can change the last digit.

### Bounded queues

Each worker has a `queue.Queue(maxsize=...)`. When a worker falls behind, its
queue fills, the coordinator blocks, and that blocking propagates back through
gRPC to the stream client. Memory stays bounded instead of growing until the
process dies.

---

## 3. Files

| File | What it is | Lines |
| ---- | ---------- | ----- |
| [proto/loganalytics.proto](../proto/loganalytics.proto) | the interface: `LogAnalytics` (public) and `Worker` (internal) | 211 |
| [src/analytics.py](../src/analytics.py) | the analytics themselves: a mergeable `Stats`, whole-number times, the exact output format | 207 |
| [src/coordinator.py](../src/coordinator.py) | the server: routing, bounded queues, adding workers up, queries, snapshots | 350 |
| [src/worker.py](../src/worker.py) | counts batches, hands back running totals | 116 |
| [src/stream_client.py](../src/stream_client.py) | replays a dataset as a live stream (rate, batch size, preload) | 185 |
| [src/query_client.py](../src/query_client.py) | prints the analytics in HW2 format; also a query load tester | 158 |
| [src/dashboard.py](../src/dashboard.py) | terminal dashboard, redrawing live | 146 |
| `scripts/` | setup, start, verify, benchmark, plots | |
| `results/` | verification output, CSVs, figures | |

`src/loganalytics_pb2.py` and `_pb2_grpc.py` are **generated** from the `.proto`
by `scripts/setup_python.sh` and are not in git: generated code refuses to load
unless it matches the installed grpcio version.

> **After editing the `.proto`, re-run `bash scripts/setup_python.sh`.**
> Otherwise the servers use stale stubs and clients fail with `Method not found`
> — which looks like a network problem but is not.

---

## 4. How it is laid out on the cluster

```
node01     coordinator            node02   the clients you run
node03..   one worker each                 (dashboard, stream, query)
```

One process per machine, so nothing shares a CPU with anything else.

---

## 5. Correctness

```bash
python3 verify_q2.py
```

→ **passed 76, failed 0** (Slurm job 99850).

It checks that the streaming system's answer equals `log_seq`'s across worker
counts, all three routing strategies, batch sizes from 1 to 10000, several
concurrent stream sources, and queries issued mid-stream. The oracle is Q1's
`bin/log_seq` — one baseline for both questions, not two that could disagree.

---

## 6. What the measurements show

1,000,000 records, 4 workers unless stated, median of 3 runs,
**36 of 36 correct**.

### Workers

| Workers | Throughput | Speedup |
| ------: | ---------: | ------: |
| 1 | 823,037 rec/s | 1.00× |
| 2 | 1,573,037 rec/s | 1.91× |
| 4 | 2,909,799 rec/s | **3.54×** |

Near-linear, because workers never talk to each other. The shortfall from 4× is
the coordinator: it parses and routes every record itself.

### Message granularity — the dominant factor

| Records per message | Throughput | vs one at a time |
| ------------------: | ---------: | ---------------: |
| 1 | 10,211 rec/s | 1× |
| 10 | 99,199 rec/s | 10× |
| 100 | 832,628 rec/s | 82× |
| 1,000 | 2,909,799 rec/s | **285×** |
| 10,000 | 3,124,137 rec/s | 306× |

Each message carries a fixed cost — a protobuf header, a length prefix, a trip
through gRPC's machinery — and at one record per message that cost *is* the
workload. The curve flattens after 1000, so 1000 is the default.

### Routing strategy

| Strategy | Throughput |
| -------- | ---------: |
| `round_robin` | 2,909,799 rec/s |
| `least_loaded` | 1,822,886 rec/s |
| `hash_server` | 221,908 rec/s |

**Even splitting beats clever splitting.** `hash_server` sends every record for
a given server to the same worker; the dataset has a few busy servers, so one
worker gets most of the traffic while the rest idle — **13× slower**.
`least_loaded` inspects queue depths on every batch, and that inspection costs
more than the imbalance it avoids.

### Queries during ingestion

| Query clients | Ingest throughput | Query p99 |
| ------------: | ----------------: | --------: |
| 0 | 2,909,799 rec/s | — |
| 1 | 2,388,961 rec/s | 32 ms |
| 4 | 779,990 rec/s | 42 ms |
| 16 | 197,263 rec/s | 192 ms |

Queries *are* served while data streams in — that is the point of the question —
but they are not free. Sixteen closed-loop clients cost **15×** the ingest rate.
Each query makes the coordinator collect and merge every worker's totals, and
that competes directly with routing. `WatchAnalytics` exists for this reason:
the dashboard is *pushed* updates, so ten people watching cost one round of work
rather than ten.

### Figures

| File | Shows |
| ---- | ----- |
| `results/q2_plots.png` | the four sweeps above |
| `results/q2_facts.png` | each claim set against the measurement behind it |
| `results/paradigm_compare.png` | MapReduce, MPI and gRPC on the same 1M records |

---

## 7. One honest comparison

On four machines over the same million records: MapReduce 1.35M rec/s, MPI
2.09M rec/s, this system 2.91M rec/s.

That ordering is **not** a ranking of the technologies. MapReduce and MPI read
the file from disk and terminate; this system receives records over the network
and keeps answering queries throughout, and its figure excludes reading a file
because a real stream has none. What the comparison does show is that the
streaming design pays **no penalty for being incremental** — maintaining a
queryable answer at all times costs nothing in throughput.
