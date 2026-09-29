# Section 1 Q1 — Matrix Multiplication with MapReduce

Multiply two matrices, A (m×n) and B (n×p), using the MapReduce pattern:
independent map tasks on separate machines, a sort that groups related values
together, and a single reducer that produces the answer.

Written in Python 3, run on the RCE cluster with Slurm. **Step-by-step cluster
instructions are in [run.md](run.md).**

---

## 1. Files

| File | What it is | Lines |
| ---- | ---------- | ----- |
| [mapper.py](../mapper.py) | **the map step**: reads rows of A, emits one partial product per output cell | 45 |
| [combiner.py](../combiner.py) | adds up a mapper's own partial products before they travel | 30 |
| [reducer.py](../reducer.py) | **the reduce step**: sums the partials for each cell and prints the matrix | 40 |
| [verify.py](../verify.py) | multiplies the matrices directly, with no MapReduce — the answer everything is checked against | 35 |
| [generate_input.py](../generate_input.py) | builds the test matrices in `test_data/` | 60 |
| [run_local.py](../run_local.py) | runs the whole pipeline on one machine, then checks itself | 55 |
| [scripts/run_cluster_test.sh](../scripts/run_cluster_test.sh) | Slurm: the 9 correctness cases across 4 nodes | |
| [scripts/benchmark_dist_slurm.sh](../scripts/benchmark_dist_slurm.sh) | Slurm: 7 matrix shapes at a fixed 4 nodes | |
| [scripts/benchmark_scaling_slurm.sh](../scripts/benchmark_scaling_slurm.sh) | Slurm: 3 sizes × 1, 2, 4, 7 tasks — the speedup sweep | |
| [plot_scaling.py](../plot_scaling.py) | the speedup, efficiency and stage-breakdown figures | 140 |
| `test_data/` | the input matrices | |
| `perf_results/` | everything the benchmarks produce (not in git) | |

---

## 2. How the multiplication is decomposed

To compute `C[i][j]`, you need row `i` of A and column `j` of B. The trick is
that each mapper is given **whole rows of A** and reads **all of B**, so one
mapper can compute every cell in its own rows without talking to any other
mapper. That is what makes the work splittable.

**Map.** For each row `i` of A that this task holds, and each column `j` of B,
emit

```
i,j <TAB> partial value
```

**Combine.** A mapper usually produces several partials for the same `i,j`. The
combiner adds them up locally, so less data travels to the reducer. This is the
same idea as Section 2's in-mapper combining.

**Sort.** `sort -k1,1` puts all the values for one `i,j` next to each other.
This is the shuffle: no program does it, the sort does.

**Reduce.** Walk the sorted stream, add the values sharing a key, print the
finished matrix.

The pipeline is therefore five stages:

```
split A by rows  →  mapper  →  sort (per node)  →  combiner
                                                       ↓
                            answer  ←  reducer  ←  sort (global)
```

Splitting A by rows is what allows any number of tasks: a task that receives no
rows simply produces nothing, which is why a 1001-row matrix over 4 tasks works
as well as a 1000-row one.

`MATRIX_B_FILE` tells the mapper where B is; every mapper reads all of B.

---

## 3. Correctness

`sbatch scripts/run_cluster_test.sh` → **9 PASSED / 0 FAILED** (job 99847, 4 nodes).

| Case | Why it is there |
| ---- | --------------- |
| the assignment's worked example | matches the expected output in the question |
| square, rows divisible by tasks | the easy case |
| square, rows **not** divisible (1001 rows / 4 tasks) | uneven splits must still be right |
| tall, 5000×10 | many rows, few columns |
| wide, 10×500 | few rows, many columns — most tasks get nothing to do |
| edge cases | single row, single column, and so on |

Every answer is compared against `verify.py`, which does the multiplication
directly. The scaling benchmark additionally checks that **splitting the input
differently never changes the answer** — all 36 of its runs matched.

---

## 4. What the measurements show

### 4.1 Adding machines helps, but only if there is enough work

From `perf_results/scaling.csv`, median of 3 runs, one task per machine:

| Input | 1 node | 2 nodes | 4 nodes | 7 nodes | Speedup at 7 |
| ----- | ------ | ------- | ------- | ------- | ------------ |
| Small (100×50) | 0.414 s | 0.353 s | 0.355 s | 0.326 s | **1.27×** |
| Medium (500×50) | 1.141 s | 0.720 s | 0.529 s | 0.446 s | **2.56×** |
| Large (2000×50) | 3.801 s | 2.140 s | 1.274 s | 0.918 s | **4.14×** |

The pattern is the point: **the bigger the input, the better it scales.** Large
reaches 4.14× on 7 machines (59% efficiency), while Small barely moves at 1.27×
(18% efficiency). Every run has a fixed cost — starting Python on each node,
reading all of B, five stages of process startup — and that cost does not shrink
when you add machines. On a 100×50 matrix there is so little real work that the
fixed cost is most of the run.

The practical conclusion: **do not distribute small inputs.** One machine is
faster than seven, once you count the coordination.

### 4.2 Shape matters more than size

From `perf_results/dist_benchmark_summary.csv`, all at 4 nodes:

| Shape | A | B | Total |
| ----- | - | - | ----- |
| wide | 10×500 | 500×10 | 0.65 s |
| small | 100×50 | 50×50 | 0.71 s |
| tall | 5000×10 | 10×10 | 0.93 s |
| medium | 500×50 | 50×50 | 1.07 s |
| large | 2000×50 | 50×50 | 2.45 s |
| **square** | 1000×100 | 100×1000 | **50.22 s** |
| square uneven | 1001×100 | 100×1000 | 53.96 s |

Square is **twenty times slower than large**, despite having only half as many
rows. The reason is the number of output cells: `large` produces 2000×50 =
100,000 of them, while `square` produces 1000×1000 = **1,000,000**, and each one
needs 100 partial products. The cost follows m×n×p, not the size of A.

This is also where the shuffle becomes the bottleneck. For `square`, sorting
takes 20.5 s of the 50.2 s — more than the mapping. For every other shape the
sort is under half a second.

**Uneven splits cost almost nothing.** 1001 rows over 4 tasks takes 53.96 s
against 50.22 s for 1000 rows: the single extra row lands on one node, and the
other three wait for it. That is a 7% penalty, and it is the expected behaviour
of a barrier, not a bug.

### 4.3 Figures

`python3 plot_scaling.py` writes into `perf_results/`:

| File | Shows |
| ---- | ----- |
| `speedup.png` | speedup vs task count, against the linear ideal |
| `efficiency.png` | parallel efficiency, against 100% |
| `stages_Small.png`, `stages_Medium.png`, `stages_Large.png` | time per stage, stacked |

---

## 5. One thing to be careful about

The sweep is **1, 2, 4, 7** and not 1, 2, 4, 8, because `node08` is drained and
only 7 nodes are usable.

This is not cosmetic. An earlier run did ask for 8 tasks on 7 nodes. The
allocation grants one task slot per node, so `srun --ntasks=8` quietly started
only **seven**: one chunk was never mapped, and the pipeline produced a wrong
answer **faster** than the correct one — 0.308 s at "p=8" against 0.327 s at
p=4. A missing chunk looks exactly like a speedup.

`benchmark_scaling_slurm.sh` now checks that the number of map outputs equals
the number of chunks and stops if they differ, so this cannot be recorded as a
result again.
