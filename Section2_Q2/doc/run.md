# Section 2 Q2 — how to run it

Everything below is run from this folder, `Section2_Q2` — **not from
`src/`**. The programs are `src/...` and the coordinator's address is
`logs/coord_addr.txt`.

---

## 1. Three rules for the RCE cluster

1. **Never compute on the login node.** Ask Slurm for machines with `salloc`
   (hold them and type commands) or `sbatch` (submit and walk away).
2. **You can only `ssh` to a node you currently hold.** Otherwise:
   `Access denied by pam_slurm_adopt`.
3. **Do not use port 50051.** It is the example port in the RCE guide, so every
   student tries to bind it. If somebody else already has it, your coordinator
   dies and your client happily connects to *their* server — which shows up as
   `Method not found`, not as an error you would recognise. The scripts derive a
   port from your user id instead.

---

## 2. Setup, once

```bash
cd ~/HW3/Section2_Q2
bash scripts/setup_python.sh
```

This builds `~/HW3/venv` from Python 3.12 — RCE's default `python3` is 3.6,
too old for grpcio — installs grpcio, and generates the gRPC stubs from the
`.proto`.

The datasets come from Q1, because both questions must measure the same bytes:

```bash
(cd ../Section2_Q1 && make && bash scripts/make_data.sh)
```

> **Re-run `setup_python.sh` after any change to `proto/loganalytics.proto`.**

---

## 3. The short way

One script drives everything:

```bash
bash scripts/rce_q2.sh setup     # once
bash scripts/rce_q2.sh bench     # submit the benchmark, 6 nodes
bash scripts/rce_q2.sh verify    # submit the correctness run, 4 nodes
bash scripts/rce_q2.sh status    # your jobs + the tail of the newest log
bash scripts/rce_q2.sh stop      # shut a running demo down
```

---

## 4. The live demo

`salloc` has to come from **your own login shell** — a script exits and takes
the allocation with it.

### Terminal A — start the system

```bash
salloc --nodes=4 --ntasks-per-node=1 --cpus-per-task=4 --time=01:00:00
cd ~/HW3/Section2_Q2
bash scripts/rce_q2.sh demo
```

It puts the coordinator on the first node and one worker on each other node,
then prints the exact commands for the next two terminals with your real node
names and port already filled in.

### Terminal B — watch the analytics live

```bash
ssh <your-username>@rce.iiit.ac.in
ssh node02                                # allowed: you hold the allocation
cd ~/HW3/Section2_Q2
~/HW3/venv/bin/python3 src/dashboard.py $(cat logs/coord_addr.txt)
```

The dashboard redraws about once a second and shows records received against
records counted, per-worker progress and queue depth, and every analytic. It is
**pushed** to by the coordinator over `WatchAnalytics`, so ten people watching
cost one round of work, not ten. Ctrl-C quits the dashboard; the system keeps
running.

### Terminal C — feed the stream

```bash
ssh <your-username>@rce.iiit.ac.in
ssh node03
cd ~/HW3/Section2_Q2
~/HW3/venv/bin/python3 src/stream_client.py $(cat logs/coord_addr.txt) \
    ../data/medium.in --rate 100000 --reset --wait
```

`--rate 100000` stretches the 1M-record file over about ten seconds, so the
dashboard visibly fills. Drop it to go as fast as possible.

| Option | Effect |
| ------ | ------ |
| `--rate R` | records per second (0 = unlimited) |
| `--batch-size B` | records per gRPC message (default 1000; `1` = one at a time) |
| `--reset` | clear all state first — **use this between runs** |
| `--wait` | return only when every record has been counted |
| `--preload` | encode the file before the timer starts, for benchmarks |

### Query it, and check the answer

```bash
cd ~/HW3/Section2_Q2
~/HW3/venv/bin/python3 src/query_client.py $(cat logs/coord_addr.txt)
~/HW3/venv/bin/python3 src/query_client.py $(cat logs/coord_addr.txt) --status

~/HW3/venv/bin/python3 src/query_client.py $(cat logs/coord_addr.txt) --final > /tmp/grpc.txt
../Section2_Q1/bin/log_seq ../data/medium.in | diff - /tmp/grpc.txt && echo SAME
```

### Stop

```bash
bash scripts/rce_q2.sh stop
exit                                      # releases the nodes
```

> **Do not run `stop` while a benchmark job is running.** It kills by process
> name, and the batch job's workers match the same pattern.

---

## 5. Correctness — 76 checks

```bash
sbatch --nodes=1 --ntasks=1 --cpus-per-task=8 --time=01:00:00 \
       --output=results/verify_%j.log \
       --wrap "cd $PWD && ~/HW3/venv/bin/python3 verify_q2.py"
```

Expected: **`passed 76, failed 0`**, written to `results/verify_q2.txt`.

It sweeps worker counts, all three routing strategies, batch sizes 1 to 10000,
several concurrent stream sources and mid-stream queries, and compares every
final answer against Q1's `bin/log_seq`. Build that first:
`(cd ../Section2_Q1 && make)`.

---

## 6. The benchmark

```bash
sbatch scripts/bench_q2.sh ../data/medium.in
squeue -u $USER
cat results/q2_bench_<jobid>.log
```

**6 nodes, one process per machine**: coordinator on node 1, clients on node 2,
one worker on each of the rest. Sweeps worker count, batch size, routing
strategy and concurrent query clients, three repeats, each checked against
`log_seq`.

Writes `results/q2_bench.csv` and `results/q2_queries.csv`.

Useful knobs:

```bash
sbatch --export=ALL,REPEATS=2,BLOCKS="workers batch" scripts/bench_q2.sh ../data/medium.in
```

`BLOCKS` picks which experiments to run from `workers batch strategy queries`.

---

## 7. Plots

```bash
~/HW3/venv/bin/python3 plot_q2.py        # results/q2_plots.png
~/HW3/venv/bin/python3 plot_compare.py   # q2_facts.png, paradigm_compare.png
```

`plot_compare.py` also reads Q1's CSVs, because the comparison only means
anything if both questions ran over the same dataset.

Bring everything home:

```bash
rsync -avz <you>@rce.iiit.ac.in:~/HW3/Section2_Q2/results/ results/
```

---

## 8. Running it on one machine

```bash
bash scripts/run_local.sh 4                      # coordinator + 4 workers
cd src
python3 stream_client.py localhost:50051 ../../data/medium.in --wait
python3 query_client.py  localhost:50051 --final
cd .. && bash scripts/run_local.sh stop
```

`run_local.sh` finds the project's `.venv` automatically, since the system
`python3` usually has no grpcio.

---

## 9. If something goes wrong

| Symptom | Cause |
| ------- | ----- |
| `Method not found` | stale generated stubs — re-run `setup_python.sh`; or you connected to another student's server on port 50051 |
| `can't open file '.../Section2_Q2/query_client.py'` | you are in the wrong folder — run from `Section2_Q2`, with `src/...` |
| `cat: logs/coord_addr.txt: No such file` | the system was never started; `rce_start.sh` writes it, from inside an allocation |
| `ModuleNotFoundError: No module named 'grpc'` | using the system `python3`; use `~/HW3/venv/bin/python3` |
| `log_seq missing` | build it: `(cd ../Section2_Q1 && make)` |
| Throughput far lower than expected | check `--batch-size`; 1 record per message is 285× slower |
| A benchmark dies halfway | a `stop` was run while it was going — see §4 |
