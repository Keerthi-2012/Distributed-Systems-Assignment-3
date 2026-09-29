# Section 2 Q1 — Log Analytics as a MapReduce job

The HW2 Question 7 log analytics, rewritten as a MapReduce pipeline in C++ and
run across several machines with Slurm.

Cluster instructions: **[run.md](run.md)**. Full write-up: **[report.pdf](report.pdf)**.

---

## 1. What the question asks for

Given a log file of `N` records, each

```
timestamp  server_id  endpoint_id  user_id  status_code  response_time_ms  bytes_sent
```

produce these figures, in this exact format:

| Output | Meaning |
| ------ | ------- |
| `TOTAL_REQUESTS` | how many records |
| `SUCCESSFUL_REQUESTS` / `FAILED_REQUESTS` | `status_code < 400` / `>= 400` |
| `AVERAGE_RESPONSE_TIME` | sum ÷ count, printed to 6 decimals |
| `MIN_RESPONSE_TIME`, `MAX_RESPONSE_TIME` | smallest and largest |
| `TOTAL_BYTES` | sum of `bytes_sent` |
| `STATUS_2XX/3XX/4XX/5XX` | counts per range; a code outside all four counts in the totals but in no bucket |
| `BUSIEST_INTERVAL id count` | the 60-second window with the most requests; **ties go to the earliest** |
| `TOP_SERVERS` | up to K rows: `server_id count avg_response_time` |
| `TOP_ENDPOINTS` | up to K rows: `endpoint_id count total_bytes` |

The answer must be **byte-identical** to the HW2 sequential program, whatever
the input is split into.

---

## 2. Why this can be a MapReduce job at all

Every figure above is a **count, a sum, a minimum or a maximum**. All four have
the same property: you can compute them on two halves of the data separately and
combine the halves afterwards, in any order, and get the same answer.

Averages do **not** have that property, so an average is never stored. The
mapper keeps a sum and a count; the division happens once, in the reducer, at
printing time.

Response times are stored as **whole numbers of millionths of a millisecond**
(12.5 ms → 12500000). Floating-point addition is not associative, and mappers
finish in a different order every run, so decimals could give a different last
digit each time. Whole numbers cannot. `tests/edge_odd.in` contains values as
small as 0.000001 ms, which is why the unit is that fine.

---

## 3. Files

| File | What it is | Lines |
| ---- | ---------- | ----- |
| [analytics.h](../analytics.h) | the `Stats` box: every counter, and the functions over it | 48 |
| [analytics.cpp](../analytics.cpp) | merging two `Stats`, parsing a record, and the `key<TAB>value` wire format | 125 |
| [mapper.cpp](../mapper.cpp) | **map**: read a split, count every record, emit one line per distinct key | 60 |
| [combiner.cpp](../combiner.cpp) | merge a mapper's own lines before they travel | 20 |
| [reducer.cpp](../reducer.cpp) | **reduce**: add all mappers' lines, then work out averages, the busiest interval and the Top-K lists | 95 |
| [Makefile](../Makefile) | builds all of the above plus the reference programs | |
| `reference/` | HW2's sequential program, MPI program and dataset generator, unchanged | |
| `scripts/` | data, verify, run, benchmark, plot, Hadoop | |
| `tests/` | the worked example and five edge-case inputs | |
| `results/` | verification output, benchmark CSVs, figures | |

`bin/` and `logs/` are produced by the build and are not in git.

---

## 4. How the work is split

```
        input file
            │  split -n l/N          (stands in for HDFS placing blocks)
   ┌────────┼────────┐
 chunk_0  chunk_1  chunk_2           one per machine
   │        │        │
 mapper   mapper   mapper            count records  →  key <TAB> value
   │        │        │
  sort     sort     sort             group identical keys, per machine
   │        │        │
combiner combiner combiner           merge this machine's own lines
   └────────┼────────┘
         sort                        the shuffle: one global ordering
            │
         reducer                     add everything, print the answer
```

**Four kinds of key** travel between the stages:

| Key | Carries |
| --- | ------- |
| `G` | the global counters: totals, status buckets, bytes, time sum/min/max |
| `S:<id>` | one server: count and summed response time |
| `E:<id>` | one endpoint: count and total bytes |
| `T:<minute>` | one 60-second window: count |

### In-mapper combining

The mapper does **not** emit one line per record. It keeps a `Stats` in memory
and emits one line per *distinct key* when it reaches the end of its split. On
the 10M-record input this makes the shuffle **278× smaller than the input**
(396 MB in, 1.43 MB out). Moving data between machines is the expensive part of
MapReduce, so this is the single most important design decision here.

### Why there is exactly one reducer

Top-K needs to see **every** server's count before it can say which ten are
biggest. Two reducers would each produce their own top ten, and merging those is
not the same answer. The reducer is therefore single, and it is cheap: 0.07 s
against a 16 s map stage.

---

## 5. Correctness

```bash
bash scripts/verify_q1.sh
```

→ **passed 34, failed 0** (Slurm job 99818).

| Group | What it checks |
| ----- | -------------- |
| the worked example | matches the figures in the assignment |
| 5 edge cases | empty input, one record, ties in Top-K, `K=0`, response times of 0.000001 ms |
| same input split 1, 2, 4 and 8 ways | **splitting differently must never change the answer** |

Everything is diffed byte-for-byte against `bin/log_seq`, HW2's sequential
program, which is kept unchanged in `reference/` precisely so it can be trusted
as the oracle.

---

## 6. What the measurements show

10M records, one map task per machine, median of 3 runs, **36 of 36 correct**.

| Nodes | Total | Map | Sort | Combine | Gather | Reduce | Speedup |
| ----: | ----: | --: | ---: | ------: | -----: | -----: | ------: |
| 1 | 18.044 s | 17.456 | 0.222 | 0.162 | 0.146 | 0.070 | 1.00× |
| 2 | 10.321 s | 9.795 | 0.162 | 0.134 | 0.158 | 0.072 | 1.75× |
| 4 | 5.273 s | 4.800 | 0.124 | 0.104 | 0.169 | 0.075 | 3.42× |
| 6 | 3.736 s | 3.258 | 0.114 | 0.105 | 0.179 | 0.077 | **4.83×** |

**The map stage is the job.** 16.35 s of 16.94 s — 97%. And it is CPU-bound, not
I/O-bound: reading the same file with `cat` takes 0.26 s, while the mapper takes
13.26 s of which 13.16 s is user CPU. The cost is parsing ten million lines.

**The serial tail is real but small.** Gather + reduce do not shrink as mappers
are added — they grow slightly, 0.21 s → 0.25 s, because more mappers emit more
partial rows for the same keys. That is 1.2% of the one-node run but 6.9% of the
six-node run, and it is what bends the curve away from linear.

**Small inputs should not be distributed.** At 100k records, six machines buy
1.21× over one. Process startup and five pipeline stages cost more than the work
saved.

### Against MPI

The same analytics written with MPI, on the same machines, one process each:

| Nodes | MPI | MapReduce | MapReduce is |
| ----: | --: | --------: | -----------: |
| 1 | 7.740 s | 18.044 s | 2.33× slower |
| 2 | 4.654 s | 10.321 s | 2.22× slower |
| 4 | 2.527 s | 5.273 s | 2.09× slower |
| 6 | 1.777 s | 3.736 s | 2.10× slower |

MPI is faster everywhere, but **the gap narrows** as machines are added, and
MapReduce actually scales slightly better (4.83× against MPI's 4.37×) because
its larger constant costs are the part that parallelises.

The constant factor comes from how state is represented, not from MapReduce:

| Program | State | Peak memory | Time |
| ------- | ----- | ----------: | ---: |
| HW2 sequential | two passes, dense arrays | 395 MB | 4.65 s |
| MPI, one rank | the same, byte-range split | 403 MB | 6.31 s |
| our mapper | one streaming pass, sparse maps | **10.1 MB** | 13.00 s |

A mapper sees only its own split, so it cannot know the global id ranges in
advance and cannot allocate dense arrays. It counts into sparse maps instead:
a tree lookup per record rather than an array subscript. That is **39× less
memory for about 2.8× the time** — and the low memory is exactly what makes the
work splittable in the first place.

### Figures

`results/q1_plots.png` and `results/q1_vs_mpi.png`, drawn by
`plot_q1.py` from the CSVs.
