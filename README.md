# Distributed Systems — Homework 3

Three questions, one from each section of `hw3-final.pdf`.

---

## Which question is which

The folder names in this repository do **not** match the numbering in the
assignment PDF, because the PDF numbers questions within each section. This
table is the mapping, and it is the first thing to read.

| This repository | In `hw3-final.pdf` | Problem | Language |
| --------------- | ------------------ | ------- | -------- |
| **[Section1_Q1/](Section1_Q1/)** | Section 1, **Q1** | Distributed Matrix Multiplication using MapReduce (Row-Row method) | Python |
| **[Section2_Q1/](Section2_Q1/)** | Section 2, **Q1** | Batch analytics with MapReduce | C++ |
| **[Section2_Q2/](Section2_Q2/)** | Section 2, **Q2** | The same analytics as a real-time gRPC stream | Python |
| **[Section3_Q2/](Section3_Q2/)** | Section 3, **Problem 2** | Food Ordering System using gRPC | Python |

Each folder is named for its place in the assignment, so the name alone says
which question it answers.

Two points about that table:

**Section 2 is one problem answered two ways.** The assignment says each team
continues with the problem it was assigned in Homework 2. Ours was **Homework 2
Question 7**, the server log analytics. Section 2 Q1 implements it as a batch
MapReduce job; Section 2 Q2 implements the same analytics as a real-time gRPC
streaming system. Both produce byte-identical output to the Homework 2
sequential program, which is what makes the comparison between them meaningful.

**Section 3 offers two problems**, Collaborative Document Editing (Problem 1)
and the Food Ordering System (Problem 2). **Problem 2 is ours**, and it lives in
`Q3/` — the folder is named for the section, not for the problem number.
Likewise, Section 1 lists three MapReduce questions and **Q1 is ours**. The
questions not present here were not assigned to this team.

---

## Layout

```
.
├── Section1_Q1/            Section 1 Q1 — matrix multiplication
├── Section2_Q1/            Section 2 Q1 — batch analytics, C++ MapReduce
├── Section2_Q2/            Section 2 Q2 — streaming analytics, Python gRPC
├── Section3_Q2/            Section 3 Problem 2 — food ordering, gRPC
├── data/                   the shared datasets (generated, not in git)
└── report.pdf              the combined report covering all four questions
```

Inside every question folder the same shape repeats:

```
<question>/
├── doc/                    readme.md, run.md, report.pdf (Section3_Q2: readme.md only)
├── scripts/                the shell scripts: build, run, benchmark, submit
├── *.py / *.cpp            the code itself
└── results/                CSVs and figures produced by the benchmarks
```

Every question folder is self-contained, and its `doc/` folder carries the same
documents:

| File | What it is |
| ---- | ---------- |
| `doc/readme.md` | what the question asks, what was built, and why |
| `doc/run.md` | the commands, the cluster rules, and a symptom/cause table |
| `doc/report.pdf` | the write-up, with figures — the three measured questions only |

Section 3 Problem 2 is the exception: it has only `doc/readme.md`. The
assignment asks there for a README that documents how to run the system and
demonstrates it with multiple clients, so the commands, the cluster rules and
the symptom/cause table all live inside that one file rather than in a separate
`run.md`, and there is no separate report.

Shell scripts live in `scripts/`; Python and C++ are code and sit at the
question's top level.

So there are **four** reports: one for each of the three measured questions,
plus `report.pdf` at the root covering all of them including Section 3. Start
with the root one; go to a question's own report for the detail.

---

## Results at a glance

| Question | Correctness | Headline measurement |
| -------- | ----------- | -------------------- |
| Section 1 Q1 | **9 / 9** cases, and all 36 benchmark runs | 4.14× on 7 machines (largest input) |
| Section 2 Q1 | **34 / 34** checks | 4.66× on 6 machines; shuffle 278× smaller than input |
| Section 2 Q2 | **76 / 76** checks | 2.9M records/s on 4 machines, while serving queries |
| Section 3 P2 | all 6 exception cases return correct gRPC codes | 40 concurrent order trackers, server stays responsive |

Everything above was measured on the RCE cluster, one process per machine.

---

## Running any of it

Each question's `run.md` has the full instructions — for Section 3 Problem 2,
its `readme.md`. The short version:

```bash
# Section 1 Q1
cd Q1 && sbatch scripts/run_cluster_test.sh && sbatch scripts/benchmark_scaling_slurm.sh

# Section 2 Q1
cd Section2_Q1 && make && bash scripts/make_data.sh
sbatch scripts/bench_q1.sh

# Section 2 Q2
cd Section2_Q2 && bash scripts/setup_python.sh
bash scripts/rce_q2.sh bench

# Section 3 Problem 2
cd Q3 && python3 server.py localhost:50051     # then customer.py / restaurant.py
```

### Three rules for the RCE cluster

1. **Never compute on the login node.** Use `sbatch` or `salloc`.
2. **You can only `ssh` to a node you currently hold**, or you get
   `Access denied by pam_slurm_adopt`.
3. **`node08` is drained**, so only 7 nodes are usable. A job asking for 8 will
   queue for ever.

> **`--cpus-per-task` is not optional.** Without it Slurm gives each task one
> physical core. `nproc` reports 2, but those are the two hyperthreads of a
> single core, and two processes on them take twice as long each. Leaving it out
> makes every multi-task measurement wrong in the same direction.

### About Hadoop

The assignment names Apache Hadoop with YARN for Section 2 Q1. On RCE the shared
HDFS is read-only for a student account and no ResourceManager is configured;
the course confirmed the Hadoop environment is unavailable and directed students
to a Slurm-based script. The benchmark therefore follows the shape of the
course's own `Mapreduce_distributed.sh`. The three programs are ordinary
executables reading stdin and writing stdout — Hadoop Streaming's contract — so
`Section2_Q1/scripts/run_hadoop.sh` runs them unchanged on a working
cluster.

---

## What is not in this repository

| Not tracked | Why |
| ----------- | --- |
| `data/` | generated datasets, 460 MB; rebuilt with fixed seeds by `Section2_Q1/scripts/make_data.sh`, so the files come back byte-identical |
| `bin/`, `logs/`, `__pycache__/` | build and run products |
| `*_pb2.py`, `*_pb2_grpc.py` | generated gRPC stubs — the generated code refuses to load unless it matches the installed grpcio version, so it belongs on each machine rather than in git |
| `*.tex`, `report_build/` | the LaTeX the PDFs were built from; each question ships only its finished `report.pdf` |
| `EXPLAIN.md` | personal working notes, one per question |

The datasets sit at the repository root rather than inside a question because the
report compares Section 2's two questions on **the same bytes**; a separate copy
per question could silently drift apart. For the same reason Q2 uses Q1's
`bin/log_seq` as its correctness oracle — one baseline, not two that could
disagree.
