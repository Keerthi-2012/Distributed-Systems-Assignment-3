# Section 1 Q1 — how to run it on the RCE cluster

Matrix multiplication as a MapReduce pipeline, driven by Slurm.

Everything below is run from `~/HW3/Section1_Q1` on the cluster. If you are impatient,
the three commands that matter are at the bottom under [Everything at once](#6-everything-at-once).

---

## 1. Three rules for this cluster

1. **Never compute on the login node.** It is shared by everyone. Ask Slurm for
   compute nodes with `sbatch` (submit and walk away) or `salloc` (hold nodes
   and type commands yourself).
2. **You can only `ssh` to a node you currently hold.** Without an allocation
   you get `Access denied by pam_slurm_adopt`.
3. **`node08` is drained**, so only **7 nodes** are usable. A job asking for 8
   will queue for ever. This is why the scaling sweep goes 1, 2, 4, **7** and
   not 1, 2, 4, 8.

---

## 2. Copy the code up

From your laptop:

```bash
rsync -avz --exclude results/ Section1_Q1/ <your-username>@rce.iiit.ac.in:~/HW3/Section1_Q1/
```

Then log in:

```bash
ssh <your-username>@rce.iiit.ac.in
cd ~/HW3/Section1_Q1
```

Nothing needs building — the whole pipeline is Python 3, which the compute
nodes already have. The test matrices are in `test_data/` and are copied up with
the code, so there is no generation step either.

> If you ever need to rebuild the matrices:
> `python3 generate_input.py` writes them back into `test_data/`.

---

## 3. Correctness — 9 cases across 4 nodes

```bash
cd ~/HW3/Section1_Q1
sbatch scripts/run_cluster_test.sh
squeue -u $USER                          # wait for it to disappear
cat results/test_results_<jobid>.out
```

Expected last line: `Results: 9 PASSED / 0 FAILED`.

The nine cases cover the assignment's worked example, square matrices whose row
count divides evenly by the task count and one that does not (1001 rows over 4
tasks), a tall matrix (5000×10), a wide one (10×500), and the edge cases. Each
result is checked against `verify.py`, which multiplies the matrices directly
with no MapReduce at all.

> **Submit from `~/HW3/Section1_Q1`.** The scripts now find the code by their own
> location, so they run correctly from anywhere — but Slurm writes the job log
> relative to the directory you submitted from, and cannot expand variables in
> `#SBATCH` lines. Submit from the question folder and the log lands in
> `results/` beside the results it describes; submit from `~/HW3` and it
> lands loose in your home directory.
>
> Intermediate files (`chunk_*`, `map_*.out`, …) go to
> `results/work_<jobid>/`, which is removed when the job ends, so nothing
> is left beside the source.

---

## 4. The two benchmarks

They answer different questions, so both are worth running.

### 4.1 Shapes — does the matrix shape matter?

```bash
sbatch scripts/benchmark_dist_slurm.sh
cat results/benchmark_dist_results_<jobid>.out
```

Seven matrix shapes, all at a fixed 4 nodes, timed per stage. Writes
`results/dist_benchmark_summary.csv`.

### 4.2 Scaling — does adding machines help?

```bash
sbatch scripts/benchmark_scaling_slurm.sh
cat results/scaling_<jobid>.out
```

Three input sizes (Small 100×50, Medium 500×50, Large 2000×50), each run with
**1, 2, 4 and 7 map tasks**, three repeats each, timed per stage. Writes
`results/scaling.csv`.

Seven nodes with **one task on each**, so a task count of N means N separate
machines. This is the only honest way to measure speedup: packing several tasks
onto one node makes them share a physical core, and the resulting curve
describes the scheduler rather than the program.

Every run is compared against the single-task answer, because splitting the
input differently must never change the result. The script also checks that the
number of map outputs equals the number of chunks — if Slurm quietly starts
fewer tasks than asked, a chunk goes unmapped and the pipeline produces a wrong
answer **faster** than the right one, which otherwise looks like a good result.

---

## 5. Plots

```bash
python3 plot_scaling.py
```

Reads `results/scaling.csv` and writes five figures into `results/`:

| File | Shows |
| ---- | ----- |
| `speedup.png` | speedup against the linear ideal, one line per input size |
| `efficiency.png` | parallel efficiency against 100% |
| `stages_Small.png`, `stages_Medium.png`, `stages_Large.png` | where the time goes, stacked by stage |

It needs matplotlib. On RCE:

```bash
~/HW3/venv/bin/python3 -m pip install matplotlib
~/HW3/venv/bin/python3 plot_scaling.py
```

Bring everything home:

```bash
rsync -avz <your-username>@rce.iiit.ac.in:~/HW3/Section1_Section1_Q1/results/ Section1_Q1/results/
```

---

## 6. Everything at once

Submitted as a chain, so each job waits for the one before it and no two
benchmarks ever share a node:

```bash
cd ~/HW3/Section1_Q1
J1=$(sbatch --parsable run_cluster_test.sh)
J2=$(sbatch --parsable --dependency=afterany:$J1 benchmark_dist_slurm.sh)
J3=$(sbatch --parsable --dependency=afterany:$J2 benchmark_scaling_slurm.sh)
sbatch --dependency=afterany:$J3 --nodes=1 --time=00:10:00 \
       --wrap "cd ~/HW3/Section1_Q1 && ~/HW3/venv/bin/python3 plot_scaling.py"
squeue -u $USER
```

Two jobs running at the same time would contend for machines and make every
timing look worse than it is, so the dependencies are not optional.

---

## 7. Running it on your own machine

No cluster, no Slurm — useful while developing:

```bash
python3 run_local.py test_data/p1_small_a.txt test_data/p1_small_b.txt
```

It runs mapper → sort → combiner → sort → reducer in one shell pipeline and
checks the answer itself.

---

## 8. If something goes wrong

| Symptom | Cause |
| ------- | ----- |
| `0 PASSED / 9 FAILED` in nine seconds, `can't open file 'combiner.py'` | submitted from the wrong directory — see §3 |
| Job stuck `PENDING (ReqNodeNotAvail)` | asked for 8 nodes; only 7 are usable |
| Job stuck `PENDING (Priority)` | someone else is ahead of you; just wait |
| `Access denied by pam_slurm_adopt` | you tried to `ssh` to a node you do not hold |
| A speedup that looks too good | check the `correct` column — a dropped task makes it fast **and wrong** |
| `\r: command not found` | the script has Windows line endings; fix with `dos2unix *.sh` |
