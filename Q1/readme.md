# MapReduce — Distributed Matrix Multiplication

## Files

| File | Purpose |
|---|---|
| `mapper.py` | Map phase — emits partial row contributions |
| `combiner.py` | Combiner — local partial sums before global shuffle |
| `reducer.py` | Reduce phase — final aggregation, writes rows of C |
| `generate_input.py` | Generates all matrix input files |
| `verify.py` | Checks output against direct matrix multiply |
| `run_local.py` | Run full pipeline locally (Windows / Mac / Linux) |
| `run_cluster_test.sh` | SLURM correctness test — 1 node |
| `benchmark_dist_slurm.sh` | SLURM distributed benchmark — 4 nodes |

---

## How to Run

### Step 1 — Generate test data (on your machine)

```powershell
# Assignment examples
python generate_input.py --preset example1
python generate_input.py --preset example2

# Edge cases
python generate_input.py --preset edge

# All benchmark shapes (saves to test_data/)
python generate_input.py --preset full_benchmark
```

### Step 2 — Run locally (on your machine)

```powershell
python generate_input.py --preset example1
python run_local.py
# runs pipeline, prints result, auto-verifies

# Custom files
python run_local.py matrix_a.txt matrix_b.txt output.txt

# Edge cases
python run_local.py edge_single_row_a.txt edge_single_row_b.txt out.txt
python run_local.py edge_single_col_a.txt edge_single_col_b.txt out.txt
```

### Step 3 — Transfer to RCE cluster

```powershell
# Create directory
ssh cs3401.26@rce.iiit.ac.in "mkdir -p ~/HW3/Q1"

# Copy all scripts
scp mapper.py combiner.py reducer.py generate_input.py verify.py `
    run_cluster_test.sh benchmark_dist_slurm.sh `
    cs3401.26@rce.iiit.ac.in:~/HW3/Q1/

# Copy benchmark test data
scp -r test_data cs3401.26@rce.iiit.ac.in:~/HW3/Q1/
```

### Step 4 — SSH into cluster (login node only — no computation here)

```bash
ssh cs3401.26@rce.iiit.ac.in
cd ~/HW3/Q1
dos2unix run_cluster_test.sh benchmark_dist_slurm.sh
```

### Step 5 — Submit correctness test (1 node)

```bash
sbatch run_cluster_test.sh
squeue -u $USER
cat test_results_<JOBID>.out
```

### Step 6 — Submit distributed benchmark (4 nodes)

```bash
sbatch benchmark_dist_slurm.sh
squeue -u $USER
cat benchmark_dist_results_<JOBID>.out
column -t -s',' perf_results/dist_benchmark_summary.csv
```

---

## Environment Variable

| Variable | Default | Description |
|---|---|---|
| `MATRIX_B_FILE` | `matrix_b.txt` | Path to B matrix read by every mapper |

Set before running locally:
```powershell
$env:MATRIX_B_FILE = "matrix_b.txt"
python run_local.py
```