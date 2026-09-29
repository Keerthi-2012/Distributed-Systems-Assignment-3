# Distributed Systems — Homework 3

Four distributed programs: two on the MapReduce model, two on gRPC.
The full write-up with design reasoning, measurements and analysis is
**[report.pdf](report.pdf)** in this folder.

| Folder | Assignment | What it is | Language |
| ------ | ---------- | ---------- | -------- |
| [Q1/](Q1/) | Section 1, Q1 | Distributed matrix multiplication (Row–Row method) | Python |
| [Section2/](Section2/) | Section 2, Q1 + Q2 | The HW2 log analytics, as a MapReduce batch job **and** as a gRPC streaming system | C++ and Python |
| [Q3/](Q3/) | Section 3, Q2 | Food ordering system | Python |

---

## Before anything: three rules for the RCE cluster

1. **Never run on the login node.** Ask Slurm for compute nodes with `salloc`
   (interactive) or `sbatch` (batch).
2. **You can only `ssh` to a node you currently hold in an allocation.** Without one
   you get `Access denied by pam_slurm_adopt`. So `salloc` first, then `ssh`.
3. **Never use `localhost` when client and server are on different nodes.** Use the
   node's name, e.g. `node01:51087`.

Also: always pass **`--cpus-per-task`**. Without it, `--ntasks-per-node=1` gives you
*one physical core per node* (`nproc` says 2, but those are two hyperthreads of one
core), and every timing you take will be wrong.

### A note on Hadoop

The assignment specifies Hadoop 3.3.6 on YARN. It is not usable on RCE: the shared
HDFS is read-only for a student account and no ResourceManager is configured. The
course has confirmed the Hadoop environment has a known issue and that a **Slurm-based
script** is acceptable meanwhile. Both MapReduce programs therefore run through shell
scripts that reproduce the Hadoop Streaming flow exactly — `split → map → sort →
combine → sort → reduce` — with mapper, combiner and reducer as standalone programs
reading stdin and writing stdout, which is the Hadoop Streaming contract.

---

## Section 1, Q1 — Distributed Matrix Multiplication (`Q1/`)

Computes `C = A × B` by the Row–Row method. `B` is replicated into every mapper's
memory; `A` is split by rows.

| File | What it does |
| ---- | ------------ |
| `mapper.py` | for each row of `A`, emits one partial vector `a[i,k] × B[k,:]` per `k`, keyed by row index |
| `combiner.py` | sums partial vectors for the same row locally, before the shuffle |
| `reducer.py` | sums the remaining partial vectors and writes the rows of `C` in order |
| `verify.py` | checks the output against a direct triple-loop multiplication |
| `generate_input.py` | builds test matrices, including the assignment's two examples and the edge cases |
| `run_local.py` | runs the whole pipeline on one machine |
| `run_cluster_test.sh` | correctness: 9 configurations, distributed over 4 nodes |
| `benchmark_dist_slurm.sh` | performance sweep over all test shapes |
| `test_data/` | the generated matrices (`*_a.txt` is A, `*_b.txt` is B) |

**Run locally**

```bash
cd Q1
python3 run_local.py test_data/p1_small_a.txt test_data/p1_small_b.txt out.txt
python3 verify.py    test_data/p1_small_a.txt test_data/p1_small_b.txt out.txt
```

**Run on RCE**

```bash
cd Q1
sbatch --cpus-per-task=8 run_cluster_test.sh       # correctness, 9 cases on 4 nodes
sbatch --cpus-per-task=8 benchmark_dist_slurm.sh   # performance
squeue -u $USER
cat test_results_<jobid>.out
```

---

## Section 2 — Log Analytics, two ways (`Section2/`)

Same HW2 Q7 analytics (request counts, status classes, response-time min/max/mean,
bytes, busiest 60-second interval, Top-K servers and endpoints) implemented twice.
Full instructions: **[Section2/run.md](Section2/run.md)**. Design notes:
[Section2/README.md](Section2/README.md). Detailed results: each question's own
report.pdf — [Section2/Q1_mapreduce/report.pdf](Section2/Q1_mapreduce/report.pdf) and
[Section2/Q2_grpc/report.pdf](Section2/Q2_grpc/report.pdf).

### Q1 — MapReduce (C++)

| File | What it does |
| ---- | ------------ |
| `Q1_mapreduce/mapper.cpp` | counts a whole split, then emits one pair per distinct key (in-mapper combining) |
| `Q1_mapreduce/combiner.cpp` | merges a mapper's own pairs before the shuffle; same format in and out |
| `Q1_mapreduce/reducer.cpp` | merges every mapper's pairs, then computes averages, the busiest interval and the Top-K lists |
| `Q1_mapreduce/analytics.h`, `Q1_mapreduce/analytics.cpp` | the shared `Stats` structure, how to add two together, and the text format they travel in |
| `Q1_mapreduce/reference/log_seq.cpp` | HW2's sequential program — the correctness baseline |
| `Q1_mapreduce/reference/log_mpi.cpp` | the MPI version, for the comparison |

### Q2 — gRPC streaming (Python)

| File | What it does |
| ---- | ------------ |
| `Q2_grpc/proto/loganalytics.proto` | the interface: `LogAnalytics` (public) and `Worker` (internal) |
| `Q2_grpc/src/coordinator.py` | accepts streams, routes batches to workers, combines their results to answer queries |
| `Q2_grpc/src/worker.py` | counts the batches it is sent; hands back its running totals when asked |
| `Q2_grpc/src/analytics.py` | the analytics themselves (the Python counterpart of Q1's `analytics.h`) |
| `Q2_grpc/src/stream_client.py` | replays a dataset file as a live stream |
| `Q2_grpc/src/query_client.py` | prints the current analytics; also the query load tester |
| `Q2_grpc/src/dashboard.py` | the live terminal dashboard |

### Scripts

| Script | Purpose |
| ------ | ------- |
| `Q2_grpc/scripts/setup_python.sh` | **one-time on RCE for Q2**: Python 3.12 venv, grpcio, gRPC stubs |
| `Q1_mapreduce/scripts/make_data.sh` | regenerate the datasets (fixed seeds) |
| `Q1_mapreduce/scripts/run_q1.sh` | run the MapReduce pipeline once, any number of map tasks, self-checking |
| `Q1_mapreduce/scripts/verify_q1.sh` | Q1 correctness — 34 checks |
| `Q2_grpc/scripts/verify_q2.py` | Q2 correctness — 76 checks |
| `Q1_mapreduce/scripts/bench_q1.sh` | Q1 benchmark sweep (`sbatch`, 6 nodes, one task each) — also runs the MPI comparison |
| `Q2_grpc/scripts/bench_q2.sh` | Q2 benchmark sweep (`sbatch`, 6 nodes) |
| `Q1_mapreduce/scripts/bench_mpi.sh` | times the MPI program on its own |
| `Q2_grpc/scripts/rce_start.sh` | start / stop the Q2 system across an allocation |
| `Q2_grpc/scripts/run_local.sh` | start / stop the Q2 system on one machine |
| `Q1_mapreduce/scripts/plot_q1.py`, `Q2_grpc/scripts/plot_q2.py` | build each question's plots from its own CSVs |

**Setup (once, on the RCE login node)**

```bash
cd ~/HW3/Section2/Q1_mapreduce && make && bash scripts/make_data.sh
cd ~/HW3/Section2/Q2_grpc     && bash scripts/setup_python.sh
```

RCE's default `python3` is 3.6 and too old for grpcio, so Q2 always uses
`~/HW3/venv/bin/python3`.

**Run Q1 (MapReduce)**

```bash
salloc --nodes=1 --ntasks-per-node=1 --cpus-per-task=8 --time=00:30:00
cd ~/HW3/Section2/Q1_mapreduce
bash scripts/run_q1.sh ../data/medium.in out.txt 4  # 4 map tasks, checks itself
bash scripts/verify_q1.sh                           # 34 checks
exit

sbatch scripts/bench_q1.sh                          # the benchmark, 6 nodes
```

**Run Q2 (gRPC)** — server on one compute node, clients on others:

```bash
cd ~/HW3/Section2/Q2_grpc
salloc --nodes=4 --ntasks-per-node=1 --cpus-per-task=4 --time=01:00:00
bash scripts/rce_start.sh                     # coordinator on node 1, a worker on each other node
COORD=$(cat logs/coord_addr.txt)              # e.g. node01:51087

# from another terminal, on a node you hold:
ssh node02 ; cd ~/HW3/Section2/Q2_grpc/src
~/HW3/venv/bin/python3 dashboard.py     $(cat ../logs/coord_addr.txt)
~/HW3/venv/bin/python3 stream_client.py $(cat ../logs/coord_addr.txt) ../../data/medium.in --rate 100000 --wait
~/HW3/venv/bin/python3 query_client.py  $(cat ../logs/coord_addr.txt) --final

bash scripts/rce_start.sh stop
```

> **Do not use port 50051.** It is the example port in the RCE guide, so everyone tries
> to bind it. If another user holds it on your node, your server fails to bind and your
> client silently connects to *theirs* — which shows up as `Method not found`, not as a
> recognisable error. `rce_start.sh` derives a port from your user id and writes the
> address it actually bound to `logs/coord_addr.txt`.

---

## Section 3, Q2 — Food Ordering System (`Q3/`)

A central server holding restaurants and orders; customer and restaurant clients are
separate processes. See also [Q3/README.md](Q3/README.md).

| File | What it does |
| ---- | ------------ |
| `food_ordering.proto` | the service: list, place, query, update, and stream order updates |
| `server.py` | restaurant catalogue, order state, transition validation, subscriber notification |
| `customer.py` | customer CLI: list restaurants, place an order, check status, track live, cancel |
| `restaurant.py` | restaurant CLI: view pending orders, accept, start preparing, mark ready |

**Run locally** (three terminals)

```bash
cd Q3
python3 server.py     localhost:50051
python3 customer.py   localhost:50051
python3 restaurant.py localhost:50051 "Pizza House"
```

**Run on RCE** — one terminal per node, as the execution guide requires:

```bash
salloc --nodes=3 --ntasks-per-node=1 --cpus-per-task=4 --time=01:00:00
scontrol show hostnames $SLURM_JOB_NODELIST      # e.g. node01 node02 node03

ssh node01 ; cd ~/HW3/Q3 && ~/HW3/venv/bin/python3 server.py     0.0.0.0:51387
ssh node02 ; cd ~/HW3/Q3 && ~/HW3/venv/bin/python3 customer.py   node01:51387
ssh node03 ; cd ~/HW3/Q3 && ~/HW3/venv/bin/python3 restaurant.py node01:51387 "Pizza House"
```

If you edit the `.proto`, regenerate the stubs:

```bash
~/HW3/venv/bin/python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. food_ordering.proto
```

---

## Rebuilding the report

`report.pdf` is generated from the recorded result files, so its figures cannot drift
from the measurements:

```bash
cd report_build && python3 gen.py && pdflatex report.tex && cp report.pdf ../report.pdf
```

---

## What is not in this repository

Generated or machine-specific things are deliberately excluded: built binaries
(`Section2/Q1_mapreduce/bin/`), datasets (`Section2/data/`, rebuilt by `Q1_mapreduce/scripts/make_data.sh`), logs,
the Python virtualenv, generated gRPC stubs for Section 2 (they must match the installed
grpcio version, so `Q2_grpc/scripts/setup_python.sh` regenerates them), and personal notes.
