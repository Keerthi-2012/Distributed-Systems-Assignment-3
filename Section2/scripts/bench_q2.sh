#!/bin/bash
#SBATCH --job-name=q7_q2_bench
#SBATCH --partition=debug
#SBATCH --nodes=6
#SBATCH --ntasks-per-node=1
#SBATCH --time=02:00:00
#SBATCH --output=results/q2_bench_%j.log
#
# bench_q2.sh - gRPC streaming benchmark sweep, one process per node.
#
#   sbatch scripts/bench_q2.sh [dataset]            as a batch job
#   salloc --nodes=6 --ntasks-per-node=1            or interactively
#   bash   scripts/bench_q2.sh [dataset]
#
# Node layout: node 1 = coordinator, node 2 = clients, nodes 3.. = one worker
# each, so nothing shares a CPU with anything else.
#
# Sweeps, each repeated REPEATS times, each checked against bin/log_seq:
#   - number of workers          (G11, G12)
#   - message batch size         (granularity)
#   - distribution strategy
#   - concurrent query clients
#
# Output: results/q2_bench.csv and results/q2_queries.csv

cd "${SLURM_SUBMIT_DIR:-$(dirname "$0")/..}"
ROOT=$(pwd)
mkdir -p results logs
DATA=${1:-data/medium.in}
REPEATS=${REPEATS:-3}
# Which experiments to run: any of "workers batch strategy queries".
BLOCKS=${BLOCKS:-"workers batch strategy queries"}
# The query experiment is far slower per run (a closed-loop query client starves
# ingestion), so it gets its own repeat count and its own cap.
QUERY_REPEATS=${QUERY_REPEATS:-1}
QUERY_MAX_SECONDS=${QUERY_MAX_SECONDS:-300}
# Which query-client counts to test.
QC_LIST=${QC_LIST:-"1 4 16"}

# Port 50051 is the example port in the RCE guide, so every student tries to bind
# it. If someone else already holds it on our node, our coordinator dies and our
# client silently connects to THEIR server instead. Derive our own ports from our
# user id; override with PORT_BASE=... if one is ever taken anyway.
PORT_BASE=${PORT_BASE:-$((50000 + $(id -u) % 9000))}

NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
if [ ${#NODES[@]} -lt 3 ]; then echo "need at least 3 nodes"; exit 1; fi
COORD=${NODES[0]}
CLIENT=${NODES[1]}
WORKER_NODES=("${NODES[@]:2}")
MAXW=${#WORKER_NODES[@]}

echo "coordinator $COORD   client $CLIENT   workers ${WORKER_NODES[*]}   data $DATA"
./bin/log_seq "$DATA" > logs/expected_bench.txt

CSV=results/q2_bench.csv
QCSV=results/q2_queries.csv
# The dataset is recorded in every row: runs on different datasets end up in
# the same file, and without this column they cannot be told apart afterwards.
DATASET_NAME=$(basename "$DATA" .in)
[ -f "$CSV" ]  || echo "dataset,workers,strategy,batch_size,query_clients,query_mode,run,records,stream_s,e2e_s,throughput,correct" > "$CSV"
[ -f "$QCSV" ] || echo "dataset,workers,strategy,batch_size,query_clients,query_mode,run,queries,qps,p50_ms,p95_ms,p99_ms" > "$QCSV"

PY=${PY:-$HOME/HW3/venv/bin/python3}   # RCE's default python3 is 3.6
SRC=$ROOT/Q2_grpc/src

on() { local h=$1; shift; srun --nodes=1 --ntasks=1 --nodelist="$h" --overlap "$@"; }

run_config() {   # run_config <W> <strategy> <batch> <query_clients> <mode> <run>
    local W=$1 STRATEGY=$2 BATCH=$3 QC=$4 MODE=$5 RUN=$6
    local PIDS=() WORKERS=""

    for i in $(seq 0 $((W - 1))); do
        h=${WORKER_NODES[$((i % MAXW))]}
        port=$((PORT_BASE + 10 + i / MAXW))
        on "$h" "$PY" "$SRC/worker.py" 0.0.0.0:$port > logs/w_$i.log 2>&1 &
        PIDS+=($!)
        WORKERS="$WORKERS,$h:$port"
    done
    WORKERS=${WORKERS#,}
    sleep 3
    on "$COORD" "$PY" "$SRC/coordinator.py" 0.0.0.0:$PORT_BASE --workers "$WORKERS" \
        --strategy "$STRATEGY" > logs/coord.log 2>&1 &
    PIDS+=($!)
    sleep 3

    local QPID=""
    if [ "$QC" -gt 0 ]; then
        # Every answer is built from the workers' current running totals, so
        # there is no "stale" mode to ask for any more.
        on "$CLIENT" "$PY" "$SRC/query_client.py" "$COORD:$PORT_BASE" --load "$QC" \
            --duration "$QUERY_MAX_SECONDS" --json > logs/q.json 2>/dev/null &
        QPID=$!
    fi

    # Do not time a run whose workers are not all up: a batch routed to a dead
    # worker sits on a queue nobody drains, which stalls the stream and produces
    # a meaningless number. Wait for all W to report healthy first.
    ready=0
    for attempt in $(seq 1 20); do
        up=$(on "$CLIENT" "$PY" "$SRC/query_client.py" "$COORD:$PORT_BASE" --status 2>&1 >/dev/null \
             | grep -c " up$")
        if [ "$up" = "$W" ]; then ready=1; break; fi
        sleep 2
    done
    if [ "$ready" != 1 ]; then
        echo "  !! only $up/$W workers up - skipping this run"
        for h in "$COORD" "${WORKER_NODES[@]}"; do
            ssh -n "$h" "pkill -u $USER -f 'Q2_grpc/src/(worker|coordinator).py'" 2>/dev/null
        done
        kill "${PIDS[@]}" 2>/dev/null
        sleep 3
        return
    fi

    # --reset clears the coordinator AND every worker before the stream, so a
    # process left over from an earlier configuration cannot skew this run.
    RES=$(on "$CLIENT" "$PY" "$SRC/stream_client.py" "$COORD:$PORT_BASE" "$DATA" \
          --batch-size "$BATCH" --reset --preload --wait --json --quiet 2>/dev/null)
    [ -n "$QPID" ] && wait "$QPID" 2>/dev/null

    on "$CLIENT" "$PY" "$SRC/query_client.py" "$COORD:$PORT_BASE" --final > logs/final.txt 2>/dev/null
    if diff -q logs/expected_bench.txt logs/final.txt > /dev/null; then
        CORRECT=yes
    else
        CORRECT=no
        echo "  !! MISMATCH (W=$W $STRATEGY batch=$BATCH q=$QC/$MODE run=$RUN)"
        diff logs/expected_bench.txt logs/final.txt | head -4
    fi

    RECORDS=$(echo "$RES" | sed 's/.*"records": *\([0-9]*\).*/\1/')
    STREAM=$(echo "$RES"  | sed 's/.*"stream_seconds": *\([0-9.]*\).*/\1/')
    E2E=$(echo "$RES"     | sed 's/.*"e2e_seconds": *\([0-9.]*\).*/\1/')
    THRU=$(echo "$RES"    | sed 's/.*"throughput": *\([0-9.]*\).*/\1/')
    if [ -z "$THRU" ]; then
        echo "  !! no throughput parsed from stream client output: $RES"
    fi
    echo "$DATASET_NAME,$W,$STRATEGY,$BATCH,$QC,$MODE,$RUN,$RECORDS,$STREAM,$E2E,$THRU,$CORRECT" >> "$CSV"
    echo "  W=$W $STRATEGY batch=$BATCH q=$QC/$MODE run=$RUN -> ${THRU} rec/s correct=$CORRECT"

    if [ "$QC" -gt 0 ] && [ -s logs/q.json ]; then
        Q=$(cat logs/q.json)
        qn=$(echo "$Q"  | sed 's/.*"queries": *\([0-9]*\).*/\1/')
        qps=$(echo "$Q" | sed 's/.*"qps": *\([0-9.]*\).*/\1/')
        p50=$(echo "$Q" | sed 's/.*"p50_ms": *\([0-9.]*\).*/\1/')
        p95=$(echo "$Q" | sed 's/.*"p95_ms": *\([0-9.]*\).*/\1/')
        p99=$(echo "$Q" | sed 's/.*"p99_ms": *\([0-9.]*\).*/\1/')
        echo "$DATASET_NAME,$W,$STRATEGY,$BATCH,$QC,$MODE,$RUN,$qn,$qps,$p50,$p95,$p99" >> "$QCSV"
    fi

    kill "${PIDS[@]}" 2>/dev/null
    wait "${PIDS[@]}" 2>/dev/null
    # srun's wrapper dying does not always take the server with it.
    for h in "$COORD" "${WORKER_NODES[@]}"; do
        ssh -n "$h" "pkill -u $USER -f 'Q2_grpc/src/(worker|coordinator).py'" 2>/dev/null
    done
    sleep 3
}

W_DEFAULT=$(( MAXW < 4 ? MAXW : 4 ))

if [[ " $BLOCKS " == *" workers "* ]]; then
echo "== worker count =="
for W in 1 2 4; do
    [ "$W" -le "$((MAXW * 2))" ] || continue
    for r in $(seq 1 $REPEATS); do run_config "$W" round_robin 1000 0 none "$r"; done
done
fi

if [[ " $BLOCKS " == *" batch "* ]]; then
echo "== message granularity =="
for B in 1 10 100 1000 10000; do
    for r in $(seq 1 $REPEATS); do run_config "$W_DEFAULT" round_robin "$B" 0 none "$r"; done
done
fi

if [[ " $BLOCKS " == *" strategy "* ]]; then
echo "== distribution strategy =="
for S in round_robin least_loaded hash_server; do
    for r in $(seq 1 $REPEATS); do run_config "$W_DEFAULT" "$S" 1000 0 none "$r"; done
done
fi

if [[ " $BLOCKS " == *" queries "* ]]; then
echo "== concurrent queries =="
for QC in $QC_LIST; do
    for r in $(seq 1 $QUERY_REPEATS); do run_config "$W_DEFAULT" round_robin 1000 "$QC" fresh "$r"; done
done
# (A "stale" variant used to be measured here. It no longer exists: with
# running totals, every answer already reflects everything counted so far.)
fi

echo "results in $CSV and $QCSV"
