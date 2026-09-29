# How to run Section 2 on the RCE cluster

Q1 is **MapReduce in C++**, Q2 is **real-time analytics over gRPC in Python**.
Both produce exactly the same numbers as HW2's sequential program, `bin/log_seq`,
and every step below ends by checking that.

Everything here has been run on RCE as written.

---

## 0. Before anything: three rules

1. **Never run the servers or the mappers on the login node.** Ask Slurm for
   compute nodes with `salloc` (interactive) or `sbatch` (batch). The login node
   also caps memory, so big runs die there.
2. **You can only `ssh` to a node you currently hold in an allocation.** Without
   one you get `Access denied by pam_slurm_adopt`. So `salloc` first, then `ssh`.
3. **Never use `localhost` when the client and the server are on different
   nodes.** Use the node's name, e.g. `node01:51087`.
4. **Always ask for `--cpus-per-task`.** This one is easy to miss and it silently
   ruins timings. `--nodes=N --ntasks-per-node=1` on its own gives you **one
   physical core per node** — `nproc` says 2, but those are the two hyperthreads
   of a single core, so two programs on a node take twice as long as one. With
   `--cpus-per-task=8` two mappers on the same node both finish in 1.22 s; without
   it they take 2.49 s against 1.27 s for one. Every benchmark below asks for it.

---

## 1. One-time setup

From your laptop, copy the code up:

```bash
rsync -avz --exclude data --exclude bin --exclude logs --exclude results \
      --exclude __pycache__ --exclude .venv \
      Section2/ <your-username>@rce.iiit.ac.in:~/HW3/Section2/
```

Then on the RCE **login node**:

Each question sets itself up, in its own folder.

```bash
ssh <your-username>@rce.iiit.ac.in

# Q1 - C++. Also builds the datasets, which both questions read.
cd ~/HW3/Section2/Q1_mapreduce
make                            # bin/{mapper,combiner,reducer,log_seq,gen_dataset}
bash scripts/make_data.sh       # ../data/{tiny,small,medium,large}.in

# Q2 - Python.
cd ~/HW3/Section2/Q2_grpc
bash scripts/setup_python.sh    # virtualenv + grpcio + the gRPC stubs
```

| Step | Why |
| ---- | --- |
| `make` in `Q1_mapreduce` | Q1 is C++ and needs nothing but `g++` |
| `scripts/make_data.sh` | fixed seeds, so `data/` is byte-identical every time; the checksums are recorded in `Q1_mapreduce/results/dataset_md5.txt` |
| `setup_python.sh` creates `~/HW3/venv` from Python 3.12 | RCE's default `python3` is **3.6**, too old for grpcio |
| it then generates the gRPC stubs | the generated code must match the installed grpcio version, so it is generated here rather than shipped |

The datasets sit in `Section2/data/`, one level above both questions, because the
report compares Q1 and Q2 on **the same bytes** — a separate copy per question
could silently drift apart. `make_data.sh` builds `bin/log_mpi` too if you first
`module load openmpi/4.1.5 && make mpi`.

Home is shared by every compute node, so one setup serves them all.

> **After changing `Q2_grpc/proto/loganalytics.proto`, re-run `bash Q2_grpc/scripts/setup_python.sh`.**
> Otherwise the servers use stale stubs and the clients fail with
> `Method not found`.

The Python for Q2 is always `~/HW3/venv/bin/python3`. Plain `python3` is 3.6 and
will not import grpc.

---

## 2. Q1 — MapReduce, on the cluster

### 2.1 What actually runs

Hadoop Streaming means the mapper and reducer are **ordinary programs** that read
stdin and write stdout, so the pipeline is just:

```
split the input  →  mapper (one per node, in parallel)  →  sort
                 →  combiner (one per node)             →  sort (all together)
                 →  reducer (one, sees everything)
```

On RCE the parallel steps run through `srun`, one task per node. This is the same
shape as the course's `Mapreduce_distributed.sh`.

### 2.2 Quick check on one node

```bash
salloc --nodes=1 --ntasks-per-node=1 --cpus-per-task=8 --time=00:20:00
cd ~/HW3/Section2/Q1_mapreduce
./bin/mapper < ../data/small.in | sort | ./bin/combiner | sort | ./bin/reducer
```

Compare it with the sequential program — the two must be byte for byte the same:

```bash
./bin/mapper < ../data/small.in | sort | ./bin/combiner | sort | ./bin/reducer > /tmp/mr.txt
./bin/log_seq ../data/small.in | diff - /tmp/mr.txt && echo SAME
exit                      # releases the allocation
```

### 2.3 Correctness — 34 checks

```bash
salloc --nodes=1 --ntasks-per-node=1 --cpus-per-task=8 --time=00:30:00
cd ~/HW3/Section2/Q1_mapreduce && bash scripts/verify_q1.sh      # add "full" for medium+large
exit
```

Expected last line: `passed 34, failed 0`. It runs the worked example, five edge
cases (empty input, one record, ties, K=0, odd values) and the same input split
1, 2, 4 and 8 ways — splitting differently must never change the answer.

### 2.4 The real multi-node run and the benchmark

```bash
cd ~/HW3/Section2/Q1_mapreduce
sbatch scripts/bench_q1.sh
squeue -u $USER                                   # wait for it to disappear
cat results/q1_bench_<jobid>.log
```

It asks for **6 nodes with one task on each** and sweeps input size × number of
map tasks, timing each stage and checking every run against `log_seq`.
A task count of N therefore means **N separate machines**, for the MapReduce
pipeline and for the MPI baseline alike — the same shape Assignment 2 Q7 was
measured with (`--nodes=N --ntasks-per-node=1`), which is what lets the two be
put side by side. Results land in
`results/q1_bench.csv`, one row per run, with a `correct` column.

To change the sweep:

```bash
sbatch --export=ALL,DATASETS="small medium",TASKS_LIST="1 2 4 6",REPEATS=2 \
       scripts/bench_q1.sh
```

> **Why tasks are spread with `--nodes` and `--distribution=cyclic`.** Plain
> `srun --ntasks=4` packs all four tasks onto one or two nodes, so the run is not
> really distributed and the timings are wrong. `bench_q1.sh` passes both flags
> so you get one task per node. You can see the difference yourself:
>
> ```bash
> srun --ntasks=4 --overlap bash -c 'echo $SLURM_PROCID $(hostname)'
> srun --ntasks=4 --nodes=4 --distribution=cyclic --overlap bash -c 'echo $SLURM_PROCID $(hostname)'
> ```

---

## 3. Q2 — gRPC streaming, on the cluster

### 3.1 The layout

This is what the RCE execution guide asks for: the server on one compute node,
clients on other compute nodes.

```
        node01                  node02              node03, node06
   coordinator :PORT        stream client           worker :PORT+11
                            query client
                            dashboard
```

**About the port.** Do **not** use 50051. It is the example port in the guide, so
every student tries to bind it, and if someone else already holds it on your node
your coordinator dies and your client silently connects to *their* server — which
shows up as `Method not found`, not as an error you would recognise.
`rce_start.sh` therefore picks a port from your user id and writes the address it
actually bound to `logs/coord_addr.txt`.

### 3.2 Start it

**Terminal A** — allocate the nodes and start the servers. Keep this terminal
open; closing it releases the nodes and kills everything.

```bash
ssh <your-username>@rce.iiit.ac.in
cd ~/HW3/Section2/Q2_grpc
salloc --nodes=4 --ntasks-per-node=1 --cpus-per-task=4 --time=01:00:00

scontrol show hostnames $SLURM_JOB_NODELIST    # e.g. node01 node02 node03 node06
bash scripts/rce_start.sh                      # coordinator on node 1,
                                               # one worker on each other node
```

It prints, and refuses to continue if the coordinator did not come up:

```
coordinator : node01:51087
workers     : node02:51098,node03:51098,node06:51098
strategy    : round_robin
```

Options: `bash scripts/rce_start.sh 2 least_loaded` = 2 workers per node, a
different distribution strategy (`round_robin`, `least_loaded`, `hash_server`).

Note the coordinator address — every client needs it:

```bash
COORD=$(cat logs/coord_addr.txt)
```

### 3.2b The short way: one driver script

Everything in 3.2–3.6 is wrapped in a single script, if you would rather not
type the steps:

```bash
cd ~/HW3/Section2/Q2_grpc
bash scripts/rce_q2.sh setup     # once: venv, grpcio, gRPC stubs
bash scripts/rce_q2.sh bench     # submit the benchmark sweep, 6 nodes
bash scripts/rce_q2.sh verify    # submit the correctness run, 4 nodes
bash scripts/rce_q2.sh status    # your jobs + the tail of the newest log
```

For the live demo it cannot hold the nodes for you — `salloc` has to come from
your own login shell, because a script exits and the allocation is freed with
it. So:

```bash
salloc --nodes=4 --ntasks-per-node=1 --cpus-per-task=4 --time=01:00:00
cd ~/HW3/Section2/Q2_grpc
bash scripts/rce_q2.sh demo
```

`demo` starts the coordinator and the workers, then **prints the exact
`ssh` + command lines** for the dashboard, the stream and the correctness check,
with your real node names and port already filled in. Copy them into two more
terminals. `bash scripts/rce_q2.sh stop` shuts the servers down.

### 3.3 Watch it live (Terminal B)

```bash
ssh <your-username>@rce.iiit.ac.in
ssh node02                                     # allowed: you hold the allocation
cd ~/HW3/Section2/Q2_grpc/src
~/HW3/venv/bin/python3 dashboard.py $(cat ../logs/coord_addr.txt)
```

The dashboard redraws about once a second and shows records received against
records counted, per-worker progress and queue depth, and every analytic. It is
pushed to by the coordinator (`WatchAnalytics`), so ten people watching cost one
round of work, not ten. Ctrl-C quits the dashboard; the system keeps running.

### 3.4 Stream the data (Terminal C)

```bash
ssh <your-username>@rce.iiit.ac.in
ssh node03
cd ~/HW3/Section2/Q2_grpc/src
~/HW3/venv/bin/python3 stream_client.py $(cat ../logs/coord_addr.txt) \
    ../../data/medium.in --rate 100000 --wait
```

`--rate 100000` makes the 1M-record file take about 10 seconds, so you can watch
the dashboard fill up. Drop `--rate` to go as fast as possible. `--wait` returns
only once every record has been counted.

| Option | Effect |
| ------ | ------ |
| `--rate R` | records per second (0 = as fast as possible) |
| `--batch-size B` | records per gRPC message (default 1000; `1` = one at a time) |
| `--reset` | clear all state first — **use this between runs** |
| `--wait` | return only when everything has been counted |
| `--preload` | encode the file before the timer starts (for benchmarks) |

### 3.5 Query it, and check the answer

While the stream is still running, from any node:

```bash
~/HW3/venv/bin/python3 query_client.py $(cat ../logs/coord_addr.txt)
~/HW3/venv/bin/python3 query_client.py $(cat ../logs/coord_addr.txt) --status
```

After it finishes, the final answer must equal the sequential program's:

```bash
cd ~/HW3/Section2/Q2_grpc
~/HW3/venv/bin/python3 src/query_client.py $(cat logs/coord_addr.txt) --final > /tmp/grpc.txt
../Q1_mapreduce/bin/log_seq ../data/medium.in | diff - /tmp/grpc.txt && echo SAME
```

### 3.6 Stop

```bash
bash scripts/rce_start.sh stop
exit                    # releases the allocation
```

### 3.7 All of 3.2–3.6 in one command

```bash
cd ~/HW3/Section2/Q2_grpc
salloc --nodes=4 --ntasks-per-node=1 --cpus-per-task=4 --time=00:20:00 bash -c '
  bash scripts/rce_start.sh
  C=$(cat logs/coord_addr.txt)
  N=($(scontrol show hostnames $SLURM_JOB_NODELIST))
  srun --nodes=1 --ntasks=1 --nodelist=${N[1]} --overlap bash -c \
      "cd src && ~/HW3/venv/bin/python3 stream_client.py $C ../../data/medium.in --reset --wait"
  srun --nodes=1 --ntasks=1 --nodelist=${N[2]} --overlap bash -c \
      "cd src && ~/HW3/venv/bin/python3 query_client.py $C --final" > logs/final.txt
  ./bin/log_seq data/medium.in | diff - logs/final.txt && echo "RESULT: SAME as log_seq"
  bash scripts/rce_start.sh stop'
```

### 3.8 Correctness — 76 checks

```bash
salloc --nodes=1 --ntasks-per-node=1 --cpus-per-task=8 --time=00:40:00
cd ~/HW3/Section2/Q2_grpc && ~/HW3/venv/bin/python3 scripts/verify_q2.py
exit
```

Expected last line: `passed 76, failed 0`. It covers 1–3 workers × 3 strategies ×
several batch sizes, two sources streaming at once, queries issued while data is
still arriving, and K given at query time.

### 3.9 The benchmark sweep

```bash
sbatch scripts/bench_q2.sh ../data/medium.in
squeue -u $USER
cat results/q2_bench_<jobid>.log
```

Six nodes: coordinator, clients, and one worker per remaining node. It sweeps
worker count, message batch size, distribution strategy and number of concurrent
query clients, checking every run against `log_seq`. Results go to
`results/q2_bench.csv` and `results/q2_queries.csv`.

---

## 4. Plots

```bash
cd ~/HW3/Section2/Q1_mapreduce
~/HW3/venv/bin/python3 scripts/plot_q1.py     # -> results/q1_plots.png, q1_vs_mpi.png

cd ~/HW3/Section2/Q2_grpc
~/HW3/venv/bin/python3 scripts/plot_q2.py     # -> results/q2_plots.png
```

Each question plots only its own CSVs, from its own `results/` folder.

Then copy the results back to your laptop:

```bash
rsync -avz <your-username>@rce.iiit.ac.in:~/HW3/Section2/Q1_mapreduce/results/ \
      Section2/Q1_mapreduce/results/
rsync -avz <your-username>@rce.iiit.ac.in:~/HW3/Section2/Q2_grpc/results/ \
      Section2/Q2_grpc/results/
```

---

## 5. Running locally instead

Everything except Slurm works on a laptop.

```bash
make                                   # build Q1
bash scripts/make_data.sh              # datasets
bash scripts/verify_q1.sh              # 34 checks

# Q1 pipeline
./bin/mapper < data/small.in | sort | ./bin/combiner | sort | ./bin/reducer

# Q2: 4 workers + coordinator on this machine
bash scripts/run_local.sh 4
cd src
python3 stream_client.py localhost:50051 ../../data/medium.in --wait
python3 query_client.py  localhost:50051 --final
cd .. && bash scripts/run_local.sh stop
```

---

## 6. When something goes wrong

| What you see | What it means |
| ------------ | ------------- |
| `Access denied by pam_slurm_adopt` | you `ssh`'d to a node you do not hold — `salloc` first |
| `Method not found` | the server you reached is not ours. Either the stubs are stale (re-run `Q2_grpc/scripts/setup_python.sh`), or another user holds your port — re-run `rce_start.sh`, or set `PORT_BASE=<something else>` |
| `Failed to bind to address ...: Address already in use` | that port is taken. `bash scripts/rce_start.sh stop`, then start again, or use another `PORT_BASE` |
| `ModuleNotFoundError: No module named 'grpc'` | you used the system `python3` (3.6). Use `~/HW3/venv/bin/python3` |
| the query returns zeros | the stream has not started yet, or you queried a coordinator that was reset. Use `--final` to wait for the stream to finish |
| `can't honor --ntasks-per-node` | harmless: more tasks than nodes, so some nodes run more than one |
| timings do not improve when you add tasks | you probably did not ask for `--cpus-per-task`, so the whole node is giving you one core. Check with `taskset -pc $$` — two cpu numbers 24 apart (like `10,34`) are one core's two hyperthreads |
| a script that starts a server over ssh never returns | write `cd X; nohup … &`, not `cd X && nohup … &` — with `&&` the whole list is backgrounded and the subshell holds ssh's streams open |
| the benchmark says `correct=no` | state left over from an earlier run. Stream with `--reset`, and `bash scripts/rce_start.sh stop` between runs |

---

## 7. Every script, in one table

| Script | What it does |
| ------ | ------------ |
| `Q2_grpc/scripts/setup_python.sh` | one-time: build, venv, gRPC stubs, datasets |
| `scripts/make_data.sh` | regenerate the datasets (fixed seeds) |
| `scripts/verify_q1.sh` | Q1 correctness — 34 checks against `log_seq` |
| `scripts/verify_q2.py` | Q2 correctness — 76 checks against `log_seq` |
| `scripts/bench_q1.sh` | Q1 benchmark sweep (`sbatch`, 4 nodes) |
| `scripts/bench_q2.sh` | Q2 benchmark sweep (`sbatch`, 6 nodes) |
| `scripts/bench_mpi.sh` | times the MPI program, for the Q1-vs-MPI comparison |
| `scripts/rce_start.sh` | start / stop Q2 across an allocation |
| `scripts/run_local.sh` | start / stop Q2 on one machine |
| `scripts/plot_q1.py` | build the plots from the CSVs in `results/` |
