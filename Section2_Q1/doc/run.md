# Section 2 Q1 — how to run it

Everything below is run from this folder, `Section2_Q1`.

---

## 1. Three rules for the RCE cluster

1. **Never compute on the login node.** Ask Slurm for machines with `sbatch`
   (submit and walk away) or `salloc` (hold machines and type commands).
2. **You can only `ssh` to a node you currently hold.** Otherwise you get
   `Access denied by pam_slurm_adopt`.
3. **`node08` is drained**, so only **7 nodes** are usable. A job asking for 8
   will queue for ever.

> **`--cpus-per-task` is not optional.** Without it Slurm gives each task a
> single physical core. `nproc` then reports 2, but those are the two
> hyperthreads of one core: two mappers take 2.49 s where one takes 1.27 s. With
> `--cpus-per-task=8` both finish in 1.22 s. Leaving it out makes every
> multi-task measurement wrong in the same direction.

---

## 2. Build

```bash
cd ~/HW3/Section2_Q1
make                                   # mapper, combiner, reducer, log_seq, gen_dataset
module load openmpi/4.1.5 && make mpi   # bin/log_mpi, for the comparison
```

Nothing but `g++` is needed for the MapReduce side.

## 3. Datasets

```bash
bash scripts/make_data.sh              # writes ../data/{tiny,small,medium,large}.in
```

They land in `data/`, one level up, because **Q2 reads the same files**
— the report compares the two questions on identical bytes. Fixed seeds, so the
files are byte-identical every time; checksums go to
`results/dataset_md5.txt`.

`large.in` is 415 MB and takes a few minutes.

---

## 4. Run it once, locally

The quickest way to see the pipeline work:

```bash
bash scripts/run_q1.sh ../data/small.in out.txt 4
```

That runs 4 map tasks as parallel processes on this machine, prints the timing,
and checks the answer against `bin/log_seq` itself.

By hand, which is exactly what the stages are:

```bash
./bin/mapper < ../data/small.in | sort | ./bin/combiner | sort | ./bin/reducer
```

---

## 5. Correctness — 34 checks

```bash
salloc --nodes=1 --ntasks-per-node=1 --cpus-per-task=8 --time=00:30:00
cd ~/HW3/Section2_Q1 && bash scripts/verify_q1.sh
exit
```

Expected last line: **`passed 34, failed 0`**. Add `full` to include the medium
and large datasets.

Or as a batch job:

```bash
sbatch --nodes=1 --ntasks=1 --cpus-per-task=8 --time=01:00:00 \
       --output=results/verify_%j.log \
       --wrap "cd $PWD && bash scripts/verify_q1.sh"
```

---

## 6. The benchmark

```bash
sbatch scripts/bench_q1.sh
squeue -u $USER
cat results/q1_bench_<jobid>.log
```

**6 nodes, one task on each**, sweeping 1, 2, 4 and 6 nodes over three datasets,
three repeats. It measures the MapReduce pipeline **and** the MPI program at
each point, and diffs both against `log_seq`.

Writes `results/q1_bench.csv` and `results/q1_mpi.csv`.

To change the sweep:

```bash
sbatch --export=ALL,DATASETS="small medium",TASKS_LIST="1 2 4 6",REPEATS=2 \
       scripts/bench_q1.sh
```

> **Why one task per machine.** A task count of N means N separate machines, for
> the MapReduce side and the MPI side alike. That is the shape Assignment 2
> measured Q7 with (`--nodes=N --ntasks-per-node=1`), which is what lets the
> numbers be compared. Packing tasks onto fewer nodes makes them share cores and
> the resulting curve describes the scheduler, not the program.

> **Multi-node MPI needs one environment variable.** RCE's compute nodes reach
> each other over `172.16.0.0/24`. Left alone, Open MPI picks an address on
> `10.0.0.x` that they cannot route between, and `mpirun` hangs for ever. The
> script sets `OMPI_MCA_btl_tcp_if_include` and uses `--map-by node`.

---

## 7. Plots

```bash
~/HW3/venv/bin/python3 plot_q1.py
```

Reads `results/q1_bench.csv` and `results/q1_mpi.csv`, writes
`results/q1_plots.png` and `results/q1_vs_mpi.png`. Needs matplotlib:

```bash
~/HW3/venv/bin/python3 -m pip install matplotlib
```

Bring it home:

```bash
rsync -avz <you>@rce.iiit.ac.in:~/HW3/Section2_Q1/results/ results/
```

---

## 8. Everything at once

```bash
cd ~/HW3/Section2_Q1
make && bash scripts/make_data.sh
J1=$(sbatch --parsable --nodes=1 --ntasks=1 --cpus-per-task=8 --time=01:00:00 \
     --output=results/verify_%j.log --wrap "cd $PWD && bash scripts/verify_q1.sh")
J2=$(sbatch --parsable --dependency=afterany:$J1 scripts/bench_q1.sh)
sbatch --dependency=afterany:$J2 --nodes=1 --time=00:15:00 \
       --wrap "cd $PWD && ~/HW3/venv/bin/python3 plot_q1.py"
```

The dependencies matter: two benchmarks running at once would share machines and
every timing would come out worse than it is.

---

## 9. About Hadoop

The assignment names Apache Hadoop with YARN, and `scripts/run_hadoop.sh` will
run this unchanged as a Hadoop Streaming job if you have a cluster — the three
programs are ordinary executables reading stdin and writing stdout, which is
exactly Hadoop Streaming's contract.

On RCE that is not possible: the shared HDFS is read-only for a student account
and no ResourceManager is configured. The TA confirmed the Hadoop environment is
broken and said to use a **Slurm-based script** instead. `scripts/bench_q1.sh`
follows the shape of the course's own `Mapreduce_distributed.sh`: `split` the
input, `srun` a mapper per node, sort locally, combine, sort globally, reduce
once.

---

## 10. If something goes wrong

| Symptom | Cause |
| ------- | ----- |
| Adding tasks does not help | `--cpus-per-task` missing — see §1 |
| `mpirun` hangs for ever | the network-interface variable is unset; see §6 |
| Job stuck `PENDING (ReqNodeNotAvail)` | asked for 8 nodes; only 7 are usable |
| `Access denied by pam_slurm_adopt` | you tried to `ssh` to a node you do not hold |
| A speedup that looks too good | check the `correct` column: a dropped task is fast **and wrong** |
| `bin/log_seq missing` | run `make` first |
