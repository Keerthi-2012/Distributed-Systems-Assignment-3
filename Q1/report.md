# MapReduce Distributed Matrix Multiplication — Report
### Distributed Systems — Assignment 3, Q1

---

## 1. Problem Statement

Compute **C = A × B** using the MapReduce Row-Row method where each row of C is:

```
c_i = a_{i,1}·B_{1,:} + a_{i,2}·B_{2,:} + ... + a_{i,n}·B_{n,:}
```

- A is m×n, B is n×p, C is m×p
- A is distributed row-by-row across mapper nodes
- B is made available to every mapper through the shared RCE filesystem

---

## 2. Design

### Pipeline

```
matrix_a.txt ──► SPLIT ──► mapper.py ──► sort ──► combiner.py
                                                        │
matrix_b.txt ──► (loaded by every mapper)          sort (global)
                                                        │
                                                  reducer.py ──► output.txt
```

| Stage | Role |
|---|---|
| **Mapper** | For each row i of A, for each element k, emits `(i, a[i,k] × B[k,:])` |
| **Shuffle/Sort 1** | Local Unix `sort` groups emissions by key i on each node |
| **Combiner** | Sums partial vectors with the same key locally — reduces data before global shuffle |
| **Shuffle/Sort 2** | Global Unix `sort` merges all combiner outputs on master node |
| **Reducer** | Final sum per key → row i of C |

> **Note on shuffle implementation:** Since Hadoop was unavailable on RCE, the MapReduce shuffle and grouping stages were explicitly orchestrated using Unix `sort` within the SLURM workflow. Stage 2 sorts locally per node; Stage 4 merges globally on the master. This is functionally equivalent to the Hadoop shuffle/sort phase.

### Key-Value Schema

```
Mapper emits:
  key   →  zero-padded row index i     e.g.  "0000000"
  value →  a[i,k] * B[k,:]            e.g.  "2.0,3.0,4.0"

  For row i=0 of A=[1,2], B=[[2,3,4],[1,0,-1]]:
    0000000    2,3,4       (1 × B[0,:])
    0000000    2,0,-2      (2 × B[1,:])

Combiner emits (partial sum per key):
    0000000    4,3,2

Reducer emits (final row of C):
    4 3 2
```

### Data Distribution

Matrix A is split across nodes using `split -n l/N` which distributes rows evenly,
with any remainder rows assigned to the first nodes (uneven splits handled gracefully).

Matrix B is replicated logically to every mapper by making the same B file available
on the shared RCE filesystem. Each mapper loads B locally before processing its
assigned rows, via the `MATRIX_B_FILE` environment variable.

**Other design decisions:**
- Keys are zero-padded to 7 digits so plain `sort` gives correct numeric order for up to 9,999,999 rows
- Global row index embedded as column 0 of matrix_a.txt preserves correct output ordering after SLURM file splits
- Empty chunks from uneven splits are detected with `[ -s chunk ]` before invoking mapper

---

## 3. Complexity Analysis

For A of size m×n and B of size n×p:

- Each row of A generates **n** intermediate key-value pairs, each containing **p** values
- Total mapper output: **O(m·n·p)** values
- The combiner aggregates partial vectors per row per node, reducing intermediate data to at most **m** entries before the global shuffle
- Without the combiner, global Sort 2 would process O(m·n·p) data; with it, only O(m·p)
- The reducer performs O(m·p) work on the combined data

This explains the observed timing: the square case (m=1000, n=100, p=1000) generates
100M intermediate values (~400MB text), making Shuffle 1 the dominant cost at 20s,
while the tall case (m=5000, n=10, p=10) generates only 500K values and completes in under 1s.

---

## 4. Correctness Verification

All tests run distributed across **4 nodes** (node[01-03,06], Python 3.6.8, SLURM job 95280).

### Example 1 — Even Split (3 Mappers)

```
A (3×2) = | 1  2 |     B (2×3) = | 2  3  4 |
           | 0  3 |               | 1  0 -1 |
           |-1  4 |
```

3 rows across 4 tasks — task 4 receives empty chunk (handled gracefully)

```
c_0 = 1·(2,3,4) + 2·(1,0,-1) = (4,  3,  2)
c_1 = 0·(2,3,4) + 3·(1,0,-1) = (3,  0, -3)
c_2 =-1·(2,3,4) + 4·(1,0,-1) = (2, -3, -8)
```

**MapReduce output:** `4 3 2 / 3 0 -3 / 2 -3 -8` ✓ PASSED

---

### Example 2 — Uneven Split (4 rows, 4 tasks — 1 row each)

```
A (4×2) = | 1  0 |     B (2×2) = | 1  2 |
           | 2 -1 |               | 0  1 |
           | 0  3 |
           | 1  1 |
```

```
c_0=(1,2)  c_1=(2,3)  c_2=(0,3)  c_3=(1,3)
```

**MapReduce output:** `1 2 / 2 3 / 0 3 / 1 3` ✓ PASSED

---

### All Correctness Test Cases (9 total, all on 4 nodes, SLURM job 95280)

| Case | Shape | Rows | Tasks | m÷tasks | Result |
|---|---|---|---|---|---|
| Example 1 | A(3×2)×B(2×3) | 3 | 4 | uneven | ✓ PASSED |
| Example 2 | A(4×2)×B(2×2) | 4 | 4 | even | ✓ PASSED |
| Edge m=1 | A(1×3)×B(3×2) | 1 | 4 | uneven | ✓ PASSED |
| Edge n=1 | A(3×1)×B(1×3) | 3 | 4 | uneven | ✓ PASSED |
| Scale small | A(100×50)×B(50×50) | 100 | 4 | even | ✓ PASSED |
| Scale square | A(1000×100)×B(100×1000) | 1000 | 4 | even | ✓ PASSED |
| Scale square uneven | A(1001×100)×B(100×1000) | 1001 | 4 | uneven | ✓ PASSED |
| Scale tall | A(5000×10)×B(10×10) | 5000 | 4 | even | ✓ PASSED |
| Scale wide | A(10×500)×B(500×10) | 10 | 4 | uneven | ✓ PASSED |

---

## 5. Performance Results

### Cluster Configuration

| Item | Value |
|---|---|
| Cluster | RCE IIIT (SLURM) |
| Nodes | 4 compute nodes (node[01-03,06]) |
| Tasks | 4 (1 per node) |
| Python | 3.6.8 |

### Benchmark Results — SLURM job 95285, 4 nodes

| Shape | A | B | Output C | m÷4? | Mapper | Shuffle1 | Combiner | Shuffle2 | Reducer | **Total** |
|---|---|---|---|---|---|---|---|---|---|---|
| p1_small | 100×50 | 50×50 | 100×50 | ✓ | 0.251s | 0.179s | 0.219s | 0.007s | 0.020s | **0.688s** |
| p1_medium | 500×50 | 50×50 | 500×50 | ✓ | 0.428s | 0.239s | 0.329s | 0.009s | 0.034s | **1.051s** |
| p1_large | 2000×50 | 50×50 | 2000×50 | ✓ | 1.118s | 0.400s | 0.766s | 0.015s | 0.085s | **2.398s** |
| p1_square | 1000×100 | 100×1000 | 1000×1000 | ✓ | 17.575s | 20.143s | 11.144s | 0.068s | 0.660s | **49.607s** |
| p1_square_uneven | 1001×100 | 100×1000 | 1001×1000 | ✗ | 17.428s | 21.606s | 11.147s | 0.067s | 0.662s | **50.925s** |
| p1_tall | 5000×10 | 10×10 | 5000×10 | ✓ | 0.331s | 0.232s | 0.271s | 0.015s | 0.057s | **0.920s** |
| p1_wide | 10×500 | 500×10 | 10×10 | ✗ | 0.217s | 0.176s | 0.197s | 0.005s | 0.016s | **0.622s** |

### Analysis

**1. Intermediate data volume dominates at large n·p**
The square cases (~1000×1000 output) take ~50s vs 2.4s for p1_large despite similar
input sizes. Each row emits n=100 KV pairs each with p=1000 values → ~400MB intermediate
text. Shuffle 1 alone takes ~20s. For baseline (fixed n=50), total scales linearly with m.

**2. Uneven split adds minimal overhead**
p1_square (1000 rows, even) vs p1_square_uneven (1001 rows, uneven) differ by only
1.3s total (49.6s vs 50.9s). The extra Shuffle 1 cost (+1.5s) comes from one node
receiving one extra row. Uneven splits are handled correctly with negligible penalty.

**3. Combiner reduces global shuffle by O(n) factor**
For square cases, combiner reduces each node's ~25,000 KV pairs to ~250 rows before
the global merge, making Shuffle 2 take only 0.068s despite ~400MB intermediate data.

**4. Tall (m>>n): efficient — small intermediate data per row**
p1_tall (5000×10) finishes in 0.92s. With n=10, each row emits only 10 KV pairs.

**5. Wide (n>>m): fast but underutilises nodes**
p1_wide (10×500) is fastest at 0.62s but only 10 rows means most nodes sit idle.

---

## 6. Distributed Execution

### SLURM Job Architecture

```
Login node — sbatch only, no computation
       │
  SLURM scheduler — allocates 4 nodes
       │
  node01  node02  node03  node06
  mapper  mapper  mapper  mapper    ← Stage 1: srun parallel
  sort    sort    sort    sort      ← Stage 2: srun parallel (local shuffle)
  comb    comb    comb    comb      ← Stage 3: srun parallel
       │
  Master node (node01)
  sort comb_*.out (global shuffle) ← Stage 4: single node
  reducer → output                 ← Stage 5: single node
```

### Actual Runs Performed

| Job | Script | Nodes | Cases | Outcome |
|---|---|---|---|---|
| 95280 | run_cluster_test.sh | 4 | 9 correctness cases | All PASSED |
| 95285 | benchmark_dist_slurm.sh | 4 | 7 benchmark shapes | Complete |

These job IDs are specific to the RCE cluster runs performed during development.
The scripts are fully reproducible by resubmitting with `sbatch`.

### Commands

```bash
sbatch run_cluster_test.sh          # correctness — 4 nodes, 9 cases
sbatch benchmark_dist_slurm.sh     # benchmark  — 4 nodes, 7 shapes
column -t -s',' perf_results/dist_benchmark_summary.csv
```