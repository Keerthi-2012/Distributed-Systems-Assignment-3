# Section 2 — Report

**Problem:** HW2 Q7, large-scale server log analytics, implemented twice:

| | Paradigm | Data model | Language |
| - | -------- | ---------- | -------- |
| **Q1** | Hadoop MapReduce (Hadoop Streaming) | complete batch, available before the job starts | C++ |
| **Q2** | gRPC streaming analytics | continuous stream, processed while it arrives | Python |

Both produce output **byte-for-byte identical** to HW2's sequential program,
`reference/log_seq`, which is the correctness reference throughout.

How to build and run everything: [run.md](run.md). Design and requirements:
[README.md](README.md).

---

## Contents

1. [The problem, and the one idea both solutions rest on](#1-the-problem-and-the-one-idea-both-solutions-rest-on)
2. [Q1 — MapReduce](#2-q1--mapreduce)
3. [Q2 — gRPC streaming](#3-q2--grpc-streaming)
4. [MapReduce vs MPI](#4-mapreduce-vs-mpi)
5. [MapReduce vs gRPC streaming](#5-mapreduce-vs-grpc-streaming)
6. [Correctness](#6-correctness)
7. [Reproducibility](#7-reproducibility)
8. [Limitations](#8-limitations)
9. [Conclusions](#9-conclusions)

---

## 1. The problem, and the one idea both solutions rest on

Q7 asks, over a log of `N` request records, for: total / successful / failed
requests, average / min / max response time, total bytes, the 2xx–5xx counts,
the busiest 60-second interval, and the Top-K servers and endpoints. The exact
rules and output format are in [README §1](README.md#1-the-q7-problem-exactly).

**Every quantity is a count, a sum, a minimum or a maximum.** All four combine
exactly: computing them on pieces of the input and merging afterwards gives the
same answer as computing them on the whole. Averages are never stored, only
computed at print time from a sum and a count, because averages cannot be
merged.

That single property is what makes the same problem expressible as

- a MapReduce job — mappers count their splits, the reducer merges; and
- a live stream — workers count batches, the coordinator merges continuously.

Two consequences are respected in both implementations:

- **Top-K is chosen after merging**, never from each part's local top K. A
  server ranked second on every worker can still be first overall.
- **The busiest interval is a maximum over merged counts**, ties going to the
  earliest interval, as HW2 does.

### Exact arithmetic

HW2 summed response times as `double`. Sums of floating-point numbers depend on
the order they are added in, and neither MapReduce nor a streaming system
controls that order. Both implementations here therefore convert the response
time **once, at parse time, to an integer number of micro-units**
(`76.393 ms → 76393000`). Every sum is then exact, and the merged result cannot
depend on how the work was divided. The average is produced at print time by
dividing exactly and rounding once, which reproduces HW2's `printf` output —
including on `tests/edge_odd.in`, which contains a value sitting exactly on a
rounding tie.

---

## 2. Q1 — MapReduce

### 2.1 Design

One MapReduce stage. Full reasoning in
[README §5](README.md#5-q1--mapreduce-design); the essentials:

**Map.** Each mapper reads its split, counts it into a `Stats`, and at the end
of the split emits only aggregates as `key<TAB>value` lines:

| Key | Value | One per |
| --- | ----- | ------- |
| `G` | 11 global counters | split |
| `S:<id>` | count, summed response time | server seen |
| `E:<id>` | count, total bytes | endpoint seen |
| `I:<id>` | count | 60-second interval seen |
| `K` | K from the header | the split holding the file header |

This is **in-mapper combining**, and it is the main design decision. Emitting
one pair per record — the obvious approach — would push all N records through
the sort and across the network, and the shuffle is the expensive part of a
MapReduce job. Here the output is bounded by the number of *distinct* keys, not
by the number of records. Measured effect in §2.3.

**Combine.** The combiner merges a mapper's own pairs before they cross the
network, consuming and producing the same format, so the reducer cannot tell
whether it ran.

**Reduce.** A single reducer merges everything and prints. One reducer, because
choosing the top K servers needs the counts of *all* servers: a server ranked
eleventh on every reducer can still be first overall, so merging per-reducer top
Ks would be wrong. It is cheap precisely because the mappers already reduced N
records to a few thousand pairs.

### 2.2 Scaling

10M records (`large.in`, 396 MB), map tasks placed **one per node** across a
4-node allocation, mean of 2 runs, every run checked against `log_seq` (24 of 24
matched):

| Map tasks | Total | Map | Sort | Combine | Gather+sort | Reduce | Speedup |
| --------: | ----: | --: | ---: | ------: | ----------: | -----: | ------: |
| 1 | 16.666 s | 16.086 | 0.212 | 0.149 | 0.148 | 0.071 | 1.00× |
| 2 | 9.321 s | 8.811 | 0.156 | 0.125 | 0.158 | 0.071 | 1.79× |
| 4 | 4.918 s | 4.435 | 0.120 | 0.107 | 0.182 | 0.074 | **3.39×** |
| 8 | 5.127 s | 4.430 | 0.217 | 0.204 | 0.197 | 0.078 | 3.25× |

Smaller inputs:

| Dataset | Records | 1 task | 2 | 4 | 8 |
| ------- | ------: | -----: | -: | -: | -: |
| small | 100,000 | 0.342 s | 0.319 s | 0.300 s | 0.609 s |
| medium | 1,000,000 | 1.766 s | 1.106 s | 0.689 s | 0.999 s |
| large | 10,000,000 | 16.666 s | 9.321 s | 4.918 s | 5.127 s |

**Observations**

- **The map phase is almost the whole run and it is what scales.** On `large` at
  one task, map is **16.09 s of 16.67 s — 96%**. Sort, combine, gather and
  reduce together come to under 0.6 s at every task count. Speedup tracks the
  map stage closely: 1→2 is 1.79×, 1→4 is 3.39×.
- **Four tasks is the sweet spot, because there are four nodes.** At eight tasks
  the map stage stops improving entirely (4.435 s → 4.430 s): two tasks now share
  each node, and since the stage is dominated by reading the input over the
  shared filesystem, a second task on the same node adds no bandwidth. The extra
  tasks only make the sort and combine stages larger (0.120 → 0.217 s), so the
  total gets *worse*: 4.918 s → 5.127 s.
- **The serial tail is small but real.** Gather+sort and reduce never shrink when
  mappers are added — they grow, 0.219 s → 0.275 s, because more mappers emit
  more partial rows for the same keys. That is Amdahl's law with a visible serial
  part, though here it is not what limits us; the filesystem is.
- **Small inputs should not be distributed at all.** `small.in` is barely faster
  at 4 tasks than at 1 (0.300 s vs 0.342 s) and clearly *slower* at 8 (0.609 s).
  100,000 records take a fraction of a second to map, and process startup across
  nodes costs more than the parallelism saves. Below roughly a million records
  the overhead wins.
- **Time grows linearly with input size** at fixed parallelism: 100k → 1M → 10M
  gives 0.342 → 1.766 → 16.666 s at one task, close to 10× per step once the
  fixed startup cost is discounted.

### 2.3 What in-mapper combining saves

| Dataset | Input | Shuffle (1 task) | Shuffle (8 tasks) | Reduction (1 task) |
| ------- | ----: | ---------------: | ----------------: | -----------------: |
| small | 3.9 MB | 29 KB | 93 KB | **135×** |
| medium | 39 MB | 162 KB | 300 KB | **246×** |
| large | 396 MB | 1.43 MB | 1.73 MB | **278×** |

- The shuffle is **278× smaller than the input** on 10M records, and the ratio
  *improves* with size, because intermediate data is bounded by the key space
  (256 servers, ~2,000 endpoints, ~57,000 intervals) while the input grows with
  N. At one task it is 1.43 MB against a 396 MB input.
- More mappers means more shuffle, since each mapper emits its own partial row
  for every key it sees: 1→8 tasks takes `large` from 1.43 MB to
  1.73 MB (22% more). That is still tiny next to the input.
- This is why the single reducer is not a bottleneck: it merges megabytes, not
  hundreds of megabytes.

---

## 3. Q2 — gRPC streaming

### 3.1 Architecture

```
 stream_client ──StreamRecords──▶ ┌─ Coordinator ─┐ ──Process()──▶ Worker 0
 (replays the dataset)            │ router        │ ──Process()──▶ Worker 1
                                  │ bounded queue │ ──Process()──▶ Worker 2
 dashboard ────WatchAnalytics──▶  │ adds them up  │ ◀──GetStats()───  (each counts
 query_client ──GetAnalytics──▶   └───────────────┘                  into its own)
```

The coordinator routes each batch to a worker's **bounded** queue. When a query
arrives it calls `GetStats` on every worker, adds the replies into one `Stats`,
and answers from that — while ingestion continues. Full design, including the
`.proto` choices, in [README §6](README.md#6-q2--grpc-design).

Two mechanisms are worth calling out because the measurements below exercise
them:

- **End-to-end backpressure.** A full queue blocks the put, the ingestion
  handler stops reading, and gRPC's flow control slows the client. Memory stays
  bounded however fast the source sends.
- **Repeating a query is safe.** Each worker keeps its own running totals and
  always sends all of them, and the coordinator *replaces* that worker's
  contribution rather than adding to it. So a lost or timed-out reply costs
  nothing but a retry: no sequence numbers, no acknowledgements, and no way to
  double-count. An earlier version of this system sent numbered *deltas* that the
  coordinator had to acknowledge; replacing instead of accumulating made that
  whole protocol unnecessary.

### 3.2 Performance

Measured on RCE with **one process per node**: coordinator on one node, the
stream client on another, each worker on its own. 1M records (`medium.in`),
mean of the runs in `results/q2_bench.csv`, every run checked against `log_seq`
— **all 35 runs produced the correct answer**. The client uses `--preload`, so
the timings measure streaming and analytics, not file parsing.

![Q2 plots](results/q2_plots.png)

#### Number of workers (round robin, batch 1000)

| Workers | Throughput | Speedup |
| ------: | ---------: | ------: |
| 1 | 834k rec/s | 1.00× |
| 2 | 1.53M rec/s | 1.84× |
| 4 | 2.86M rec/s | **3.43×** |

- **Scaling is close to linear to 4 workers.** Counting is the dominant cost and
  it parallelises well, because workers share nothing: each holds its own
  `Stats` and never talks to another worker.
- A single worker sustains **834k records/s**, far more than a per-record Python
  loop would manage. The reason is the columnar batch format: protobuf decodes
  seven packed arrays in C, and the worker's loop walks them with local variable
  lookups.
- The gap from 3.43× to a perfect 4× is the coordinator: every record passes
  through its single Python interpreter, which is the one part of the system that
  cannot be parallelised by adding workers.

#### Message granularity (4 workers, round robin)

| Records per message | Messages for 1M records | Throughput | vs batch 1 |
| ------------------: | ----------------------: | ---------: | ---------: |
| 1 | 1,000,000 | 10.4k rec/s | 1× |
| 10 | 100,000 | 101k rec/s | 9.8× |
| 100 | 10,000 | 825k rec/s | 80× |
| 1,000 | 1,000 | 2.86M rec/s | **276×** |
| 10,000 | 100 | 3.09M rec/s | 298× |

- **Granularity is by far the largest single factor**, worth **276×** between one
  record per message and a thousand. Up to batch 100 throughput grows almost
  exactly 10× per 10× batch size, which is the signature of a cost that is **per
  message, not per record**: gRPC framing, a Python-level handler iteration, a
  queue hand-off and a re-send to the worker are all paid once per message
  whether it carries 1 record or 1,000.
- The curve **flattens after 1,000** (only 8% more at 10,000), where per-message
  overhead has been amortised away and the per-record work dominates.
- Batch 1,000 is the right default: it sits at the start of the plateau, and at
  1M rec/s a batch spends only ~1 ms waiting to fill, so freshness is unaffected.

#### Record distribution strategy (4 workers, batch 1000)

| Strategy | Throughput | vs round robin |
| -------- | ---------: | -------------: |
| `round_robin` | 2.86M rec/s | 1.00× |
| `least_loaded` | 1.87M rec/s | 0.65× |
| `hash_server` | 219k rec/s | **0.08×** |

- **`hash_server` is 13× slower.** It is the only strategy that looks at every
  *record* rather than every *batch*: the coordinator takes each batch apart,
  decides a destination per record, and re-encodes W smaller batches — O(records)
  work in the one interpreter every record already has to pass through. Key
  partitioning moves the bottleneck into the router.
- **`least_loaded` is 35% slower than round robin**, the opposite of its intent.
  It calls `qsize()` on every worker queue for every batch, and on this cluster
  the workers are near-identical, so it pays the polling cost without ever
  finding a meaningfully shorter queue. It would earn its keep only with
  heterogeneous or contended workers.
- **`round_robin` is the right default here**: O(1) per batch, perfectly even for
  equal-sized batches, and correct for Q7 because every analytic is mergeable, so
  no key affinity is needed.

#### Concurrent queries during ingestion

Each query client is a separate thread issuing `GetAnalytics` in a **closed
loop** — no pause between queries, which is the harshest possible load. Every
query asks all the workers for their running totals and adds them up.

All rows are `medium.in` (1M records), 4 workers, batch 1000, round robin.

| Query clients | Ingest throughput | vs no queries | Queries/s | p50 | p95 | p99 |
| ------------- | ----------------: | ------------: | --------: | --: | --: | --: |
| none | 2.86M rec/s | — | — | — | — | — |
| 1 | 2.21M rec/s | **−23%** | 53 | 19.8 ms | 31.0 ms | 32.8 ms |
| 4 | 696k rec/s | **−76%** | 150 | 29.6 ms | 37.4 ms | 41.2 ms |
| 16 | 168k rec/s | **−94%** | 155 | 110.8 ms | 159.8 ms | 190.2 ms |

**Observations**

- **16 concurrent clients now complete.** In the previous version of this system
  this row could not be filled in at all: three attempts, each with a shorter
  time cap and the last on a 100k-record dataset, all failed to finish a stream.
  Replacing the epoch-numbered delta protocol with plain running totals removed
  the coordination that was saturating the coordinator, and the case now runs to
  completion on a full 1M-record stream, correctly.
- **Queries are answered throughout, and every answer is correct** (§6) — all 35
  runs in this sweep matched `log_seq`, including every query-load row.
- **Queries are expensive, and increasingly so.** One closed-loop client costs
  23% of ingest throughput; four cost 76%; sixteen cost 94%. Each query now makes
  the coordinator call all four workers and add up their full state, and that
  happens in the same single Python interpreter every record must pass through.
- **Query throughput saturates while its cost keeps rising.** Going 4 → 16
  clients raises answers/s only from 150 to 155 — a 3% gain — while ingestion
  falls a further 18 points and p99 latency climbs from 41 ms to 190 ms. Beyond
  about 4 concurrent closed-loop clients the coordinator is the bottleneck and
  extra clients only queue behind each other.
- **This is the clearest limit of the design.** Every record and every query
  passes through one coordinator process, and Python's GIL means routing and
  query-answering cannot truly overlap. Sharding the coordinator, or answering
  queries from a periodically-refreshed snapshot instead of polling workers on
  every request, is the obvious next step; the trade would be freshness against
  throughput.
- **Latency is higher than the batch numbers above might suggest** because each
  query does real work: four gRPC round trips plus a merge of three maps. A
  p99 of 33 ms under a closed-loop client, while ingesting 2.2M records/s, is
  still comfortably interactive for a dashboard refreshing once a second.

## 4. MapReduce vs MPI

**About the MPI program.** The assignment asks for a comparison against HW2's
MPI implementation. HW2's `log_mpi.cpp` is not in this folder — only
`log_seq.cpp`, `common.cpp` and `common.h` were carried over — so
[reference/log_mpi.cpp](reference/log_mpi.cpp) is a **reconstruction**. It is
written against HW2's unchanged `common.h`/`common.cpp`, so the parsing, the
counting and the printing are literally the code the sequential program uses;
only the distribution is new. It splits the file by byte range exactly as Hadoop
splits it across mappers, and combines with `MPI_Reduce` (`MPI_SUM` for counts
and sums, `MPI_MIN`/`MPI_MAX` for the extremes). It reproduces `log_seq`
byte-for-byte at 1, 2, 4 and 8 processes.

**How these were measured.** MapReduce runs its map tasks **one per node**
across a 4-node allocation (`srun --nodes --distribution=cyclic`). MPI runs
**all ranks on a single node**, because Open MPI cannot launch across nodes on
RCE: pointing it at the InfiniBand IPoIB interface fails with
`connect() to 10.0.0.x:1024 failed` and then hangs, and forcing the Ethernet
subnet instead fails with a missing-topology error for the remote node. Each
compute node has **2 cores**. Every run is diffed against `log_seq`; all 24
MapReduce runs and all 24 MPI runs matched.

> **Read the 4- and 8-process rows with care.** They are not like-for-like:
> MapReduce has 4 machines there, MPI has one 2-core machine. Only the 1- and
> 2-process rows compare the two paradigms on equal hardware.

![MapReduce vs MPI](results/q1_vs_mpi.png)

### 4.1 Wall-clock time

Mean of 2 runs, seconds.

| Dataset | Procs | MPI | MapReduce | MapReduce / MPI |
| ------- | ----: | --: | --------: | --------------: |
| small (100k) | 1 | 0.189 | 0.342 | 1.81× |
| | 2 | 0.277 | 0.319 | 1.15× |
| | 4 | 0.338 | 0.300 | 0.89× |
| | 8 | 0.498 | 0.609 | 1.22× |
| medium (1M) | 1 | 0.870 | 1.766 | 2.03× |
| | 2 | 0.946 | 1.106 | 1.17× |
| | 4 | 1.024 | 0.689 | 0.67× |
| | 8 | 1.150 | 0.999 | 0.87× |
| large (10M) | 1 | 7.708 | 16.666 | 2.16× |
| | 2 | 7.802 | 9.321 | 1.19× |
| | 4 | 7.731 | 4.918 | 0.64× |
| | 8 | 7.931 | 5.127 | 0.65× |

**On equal hardware MPI wins.** At one process it is **2.2× faster** than the
MapReduce pipeline on `large`, and still ahead at two. That is what the
paradigms predict: MPI has no shuffle, no sort, no text intermediate form and no
process per stage.

**MapReduce only overtakes it by using more machines**, and the crossover is
exactly where the comparison stops being fair — at 4 tasks, where MapReduce has
four nodes and MPI still has one.

### 4.2 Scaling, and why MPI's line is flat

| Procs | MPI (large) | speedup | MapReduce (large) | speedup |
| ----: | ----------: | ------: | ----------------: | ------: |
| 1 | 7.708 s | 1.00× | 16.666 s | 1.00× |
| 2 | 7.802 s | 0.99× | 9.321 s | 1.79× |
| 4 | 7.731 s | 1.00× | 4.918 s | 3.39× |
| 8 | 7.931 s | 0.97× | 5.127 s | 3.25× |

**MPI does not speed up at all** — not even from 1 to 2 processes, where it has
two real cores to use. That rules out a CPU limit and points at the filesystem:
every rank reads its byte range of the same 415 MB file from the shared home
directory, over that one node's single network link, so adding ranks adds no
bandwidth.

The per-stage MapReduce numbers say the same thing from the other side. On
`large` with one task, the map stage is **16.09 s of the 16.67 s total** — 96%
of the run is reading and parsing the input; sort, combine and reduce together
account for under half a second. MapReduce scales because its map tasks sit on
*different* nodes, so four tasks pull through four separate links. Its 3.39×
at four tasks is close to the 4× that implies, and the fall-back at eight tasks
(3.25×) is two tasks per node competing for one link again.

**So this measurement is dominated by the environment, not by the paradigms.**
On one machine MPI is the faster design here; MapReduce's advantage at scale
comes from spreading I/O across machines, which is exactly the thing MPI was
prevented from doing on this cluster. A fair multi-node MPI comparison would
need Open MPI to launch across nodes, which we could not make work on RCE.

### 4.3 A separate, structural finding

Independently of the distribution framework, the two programs represent their
state differently, and that is worth recording because it is the one comparison
free of the launcher problem.

`log_mpi` inherits HW2's structure, because it reuses HW2's `Stats`: read every
record into a `vector<Record>`, scan it to find the id ranges, size dense
arrays, then count in a second pass. Our mapper counts in a **single streaming
pass** into **sparse maps**, and never holds a record after counting it.
Measured on `large.in`, one process, no MPI involved at all:

| Program | Structure | Peak memory |
| ------- | --------- | ----------: |
| `log_seq` (HW2's own) | two passes, all records in a vector, dense arrays | **395 MB** |
| `mapper \| reducer` | one pass, sparse maps | **11.5 MB** |

**34× less memory for the same answer.** That difference is a property of how
the state is represented, not of MPI or MapReduce, and it would carry over to an
MPI program written the same way.

### 4.4 Qualitative comparison

| | MPI | MapReduce |
| - | --- | --------- |
| **Communication** | one `MPI_Allreduce` for ranges, one `MPI_Reduce` for results: a few KB | 1.4 MB of text through sort and a shared filesystem |
| **Data movement** | in memory, direct between ranks | written to disk, sorted, read back |
| **Fault tolerance** | none: one rank dies, the job dies | Hadoop re-runs a failed task; our Slurm pipeline does not |
| **Programming effort** | 200 lines, but the subtle parts are manual: byte-range splitting, agreeing on array sizes before reducing, reducing each array separately | 190 lines across three programs, each of which reads stdin and writes stdout and can be tested with a shell pipe |
| **Where bugs hide** | the split boundary. The reconstruction counted one record too many per boundary until the multi-process check caught it | the key encoding; a single reducer makes ordering trivial |
| **Elasticity** | fixed at launch: `-np` cannot change | task count is just how many pieces the input is split into |
| **Debuggability** | needs `mpirun` and a parallel debugger | `./mapper < chunk \| sort \| ./reducer` in a terminal |

The practical difference we felt while building them: the MapReduce programs are
ordinary filters, so every stage is inspectable by hand, while the MPI program's
correctness depends on invariants that are invisible until several ranks run at
once.

## 5. MapReduce vs gRPC streaming

The same analytics, the same data, two paradigms. Both were measured on RCE, so
the comparison is like-for-like in hardware if not in placement: Q1's map tasks
share one node, Q2 places every process on its own node.

| | Q1 — MapReduce | Q2 — gRPC streaming |
| - | -------------- | ------------------- |
| **When the answer exists** | only at the end of the job | continuously, while data arrives |
| **1M records, 4 workers/tasks** | 0.689 s | 0.35 s (2.86M rec/s) |
| **10M records** | 4.92 s at 4 tasks | not measured at 10M |
| **Scaling 1 → 4** | 3.39× | 3.43× |
| **What limits it** | the serial tail: gather, final sort, single reducer | the single coordinator every record passes through |
| **Data movement** | 1.4 MB of text through sort and the filesystem | 1M records through gRPC, in 1,000 messages |
| **Memory** | 11.5 MB per mapper | ~50 MB per worker, constant in N |
| **Queries during processing** | impossible: there is no state until the reduce | any number, answered from merged state |
| **Multiple sources** | one input path per job | several streams at once, counted together |
| **Failure of one participant** | Hadoop re-runs the task | worker marked down, its queue re-routed, its unpulled records lost |
| **Lines of code** | 190 (three filters) + 425 shared C++ | 1,107 Python |

**Observations**

- **Streaming is the faster of the two on a fixed dataset here.** On 1M records
  with four workers or tasks, Q2 finishes in 0.29 s against MapReduce's 0.689 s.
  That is not a paradigm result: MapReduce spends almost all of its time reading
  the input from the shared filesystem (96% of the run is the map stage), while
  the streaming client has the records in memory before the timer starts. The two
  measure different things, and the comparison is included to be explicit about
  that rather than to rank them.
- **Streaming wins on when the answer is available.** Q1's analytics do not
  exist until the job ends; Q2's are queryable throughout, which is the whole
  point of the paradigm and cannot be compared on a throughput axis at all.
- **The costs are in different places.** Q1 pays a fixed startup and a serial
  tail, so it is wasteful on small inputs (100k records is slower at 8 tasks
  than at 4) and excellent on large ones. Q2 pays per message, which is why
  granularity is worth 276× and why the batch size, not the worker count, is
  the first thing to tune.
- **Both are limited by a single serial component**: Q1's one reducer, Q2's one
  coordinator. Q1's is cheap because the mappers reduced the data 278× first;
  Q2's is not, because every record must pass through it.
- **The same property makes both correct.** Counts, sums, minima and maxima
  merge exactly, so Q1 can pre-combine in mappers and Q2 can merge deltas
  whenever it likes. Neither implementation would work if any analytic needed
  the whole dataset at once — a median, for instance, would have forced a very
  different design in both.

---

## 6. Correctness

The rule for both implementations: the printed analytics must be **byte-for-byte
identical** to `bin/log_seq` on the same input.

| | Checks | Coverage |
| - | -----: | -------- |
| **Q1** | **34 / 34** | worked example + 5 edge cases × 1–4 mappers; `tiny` and `small` × 1, 2, 4, 8 mappers; query-time K |
| **Q2** | **76 / 76** | worked example + 5 edge cases × 1–3 workers × 3 strategies; `tiny` and `small` × the same; concurrent sources; live queries; query-time K |
| **MPI** | 1, 2, 4, 8 processes | identical to `log_seq` on `small.in` |

Reports: `results/verify_q1.txt`, `results/verify_q2.txt`. Re-run with
`make verify` and `python3 scripts/verify_q2.py`.

Edge cases in `tests/`:

| File | What it checks |
| ---- | -------------- |
| `sample.in` | the HW2 worked example |
| `edge_empty.in` | N = 0: all zeros, `BUSIEST_INTERVAL 0 0`, empty lists |
| `edge_single.in` | one record, status 503 |
| `edge_odd.in` | negative ids (counted in totals, in no per-id list), status 100/199/600 (in totals, in no bucket), negative timestamps (C truncation for `timestamp/60`), bytes > 2³², 6-decimal response times, a rounding tie, K larger than the number of distinct ids |
| `edge_ties.in` | equal counts for servers, endpoints and intervals (earliest/lowest id wins) |
| `edge_k0.in` | K = 0: no Top-K rows at all |

Beyond final answers, Q2's suite checks that **every snapshot taken while
ingesting is internally consistent** — `successful + failed == total`, totals
never go backwards — and that three sources streaming `small.in` at once produce
exactly 300,000 records.

**Two real bugs these suites caught**, both of which would have produced wrong
output in a demo:

1. **`K = 0` was ignored.** The coordinator treated 0 as "unset" and fell back
   to its default of 10, printing Top-K rows that `log_seq` omits entirely.
   Caught by `edge_k0.in` across all nine configurations.
2. **The MPI program counted one record too many per split boundary.** It read
   "one line past the end" of its byte range, but that line already belonged to
   it, because the next rank discards the partial line it lands in. With P
   ranks the total came out P−1 records too high — invisible at P = 1, which is
   exactly why the suite runs several process counts.

---

## 7. Reproducibility

Datasets come from HW2's unchanged `gen_dataset` with fixed seeds:

| File | Records | K | S | Seed | Size |
| ---- | ------: | -: | --: | ---: | ---: |
| `tiny.in` | 20,000 | 10 | 32 | 2000 | 0.8 MB |
| `small.in` | 100,000 | 10 | 64 | 2001 | 3.9 MB |
| `medium.in` | 1,000,000 | 10 | 128 | 2002 | 39 MB |
| `large.in` | 10,000,000 | 10 | 256 | 2003 | 396 MB |

`small`, `medium` and `large` use exactly HW2's parameters, so they are the same
files HW2 was benchmarked on. Checksums are in `results/dataset_md5.txt`, and
the files generated on the laptop and on RCE match. The generator uses
`srand(seed)`/`rand()`, so files are identical on the same C library.

Properties that matter for the measurements: timestamps increase, so file order
is chronological; about one record in 2,000 starts a burst of 3,000 requests,
which gives a clear busiest interval; half the traffic goes to the first quarter
of the servers, so per-server load is skewed — which is what makes
`hash_server` routing worth measuring.

Every benchmark row carries its own `correct` column, and `scripts/plot.py`
**discards any run that did not match `log_seq`** rather than averaging it in,
reporting how many it dropped.

---

## 8. Limitations

- **The MPI program is a reconstruction**, not HW2's original (§4). The
  computation is HW2's own code, but the distribution strategy is a choice made
  here, so the comparison measures *this* MPI program rather than HW2's.
- **Hadoop was not available on RCE**, so Q1's numbers come from the Slurm
  pipeline that runs the identical mapper, combiner and reducer binaries through
  the same stages. `Q1_mapreduce/run_hadoop.sh` runs the real Hadoop Streaming
  job when a cluster is available; the results below are not from YARN.
- **Q2 fault tolerance is detection plus re-routing, not recovery.** A worker
  whose stream fails is marked down, no longer receives batches, and its queued
  batches are handed to healthy workers — but records it had already accepted
  and not yet handed back are lost. Recovery would need the coordinator to
  retain unacknowledged batches and replay them; the epoch protocol already
  makes merges idempotent, so replay is the missing piece.
- **The coordinator is a single point** through which every record passes.
- **Q1's measurements are single-node** (parallel processes). `scripts/bench_q1.sh`
  runs mappers across nodes with `srun` when submitted to Slurm; the numbers
  reported here were taken with all tasks on one node, which is also how the MPI
  program was measured, so the two are comparable.
- **The 16-query-client configuration has no numbers.** Three attempts — 1M
  records with a 240 s cap, 100k records with 120 s, and 100k with 60 s — each
  failed to complete a stream within minutes. The system stays correct and
  recovers afterwards; it is the single-threaded coordinator being saturated
  (§3.2). The row is left empty rather than estimated.
- **The query experiment uses `small.in`** while the worker, batch and strategy
  experiments use `medium.in`, for the reason just given. Its baseline was
  re-measured on `small.in` so that the percentages in that table are
  self-consistent.

---

## 9. Conclusions

**Both implementations are correct.** Q1 passes 34/34 checks and Q2 passes
76/76, every one byte-identical to HW2's sequential program, across mapper
counts, worker counts, all three routing strategies, batch sizes from 1 to
10,000, concurrent sources, mid-stream queries and six edge-case files. The MPI
program matches at 1, 2, 4 and 8 processes.

**One property does all the work.** Every Q7 analytic is a count, a sum, a
minimum or a maximum, and those merge exactly. That is what lets the mapper
pre-aggregate its whole split, the combiner merge again, the reducer merge once
more, and — in a completely different paradigm — streaming workers merge deltas
into a live total. An analytic that needed the whole dataset at once, such as a
median, would have forced a different design in both.

**Integer micro-units, not doubles.** Neither paradigm controls the order its
partial sums are combined in, and floating-point addition is not associative.
Converting response times once, at parse time, to integers makes every merge
exact, which is why the output matches to the last digit — including a value
that sits exactly on a rounding tie in `edge_odd.in`.

**What each paradigm is good at, measured:**

- **Q1 (MapReduce)** processes 10M records in **4.92 s with 4 map tasks on 4
  nodes**, scaling 3.39×, and shrinks intermediate data **278× below the input**
  through in-mapper combining. It is wasteful below about a million records,
  where fixed startup dominates. Its ceiling here is **reading the input**: the
  map stage is 96% of the run, so throughput is bounded by shared-filesystem
  bandwidth, and adding a fifth-through-eighth task on already-busy nodes makes
  it slower rather than faster.
- **Q2 (gRPC streaming)** sustains **2.86M records/s with 4 workers**, scaling
  3.43×, and answers queries throughout ingestion — p99 **33 ms** under one
  closed-loop client, and 16 concurrent clients now complete a full 1M-record
  stream, which the earlier delta-based design could not do at all. Its
  dominant tuning knob is message granularity, worth **276×** between one record
  per message and a thousand; its ceiling is the single coordinator, which
  saturates under heavy query load.

**The comparison that surprised us.** The MapReduce pipeline beat the MPI
program by 2–10×, which is backwards for the paradigms involved. Isolating the
cause showed it had nothing to do with MPI: HW2's two-pass structure — load
every record into a vector, size dense arrays, then count — costs 2.1 s and
**34× the memory** versus a single streaming pass with sparse maps, before any
communication happens. The lesson is that *how state is represented* mattered
more here than *which framework distributed the work*.

**What we would do differently.** The coordinator is the one design decision we
would revisit: every record passes through a single process that cannot use more
than one core, which caps Q2's throughput and collapses under 16 closed-loop
query clients. Partitioning the coordinator, or serving stale reads by default,
would address it directly.
