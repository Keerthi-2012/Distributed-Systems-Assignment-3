#!/bin/bash
#SBATCH --job-name=q2grpc
#SBATCH --partition=debug
#SBATCH --nodes=6
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=01:00:00
#SBATCH --output=q2grpc_%j.log

# rce_bench.sh - multi-node benchmark on the RCE cluster (every process on its own node).
#
#   sbatch scripts/rce_bench.sh [dataset]          default data/medium.in
#
# Layout for N allocated nodes:
#   node 1          coordinator
#   node 2          stream client + query clients
#   nodes 3..N      one worker each  -> worker counts swept: 1, 2, 4 (up to N-2)
#
# Also verifies correctness on the cluster: the final analytics of every run are
# diffed against the HW2 sequential program.
# Results: results/rce_<jobid>.jsonl (one JSON object per run) + this log.

cd "${SLURM_SUBMIT_DIR:-$(dirname "$0")/..}"
ROOT=$(pwd)
DATA=${1:-data/medium.in}
DATA=$(realpath "$DATA")
OUT=$ROOT/results/rce_${SLURM_JOB_ID:-local}.jsonl
mkdir -p results logs
PY=${PY:-$HOME/HW3/venv/bin/python3}   # virtualenv made by rce_setup.sh (system python3 is 3.6)

NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
COORD=${NODES[0]}
CLIENT=${NODES[1]}
WORKER_NODES=("${NODES[@]:2}")
MAXW=${#WORKER_NODES[@]}

echo "coordinator: $COORD   client: $CLIENT   worker nodes: ${WORKER_NODES[*]}"
echo "dataset: $DATA"
./bin/log_seq "$DATA" > results/expected_$SLURM_JOB_ID.txt

on() {   # on <host> <command...>  - run a job step on one node
    local h=$1; shift
    srun --nodes=1 --ntasks=1 --nodelist="$h" --overlap "$@"
}

run_config() {   # run_config <W> <strategy> <batch> <query_clients>
    local W=$1 STRATEGY=$2 BATCH=$3 QC=$4
    local PIDS=() WORKERS=""
    for i in $(seq 0 $((W - 1))); do
        h=${WORKER_NODES[$i]}
        on "$h" "$PY" src/worker.py 0.0.0.0:50061 2> logs/w_${h}.log &
        PIDS+=($!)
        WORKERS="$WORKERS,$h:50061"
    done
    WORKERS=${WORKERS#,}
    on "$COORD" "$PY" src/coordinator.py 0.0.0.0:50051 --workers "$WORKERS" \
        --strategy "$STRATEGY" 2> logs/coord.log &
    PIDS+=($!)
    sleep 5

    local QPID=""
    if [ "$QC" -gt 0 ]; then
        on "$CLIENT" "$PY" src/query_client.py "$COORD:50051" --load "$QC" \
            --duration 3600 --until-complete --json > logs/q.json &
        QPID=$!
    fi
    RES=$(on "$CLIENT" "$PY" src/stream_client.py "$COORD:50051" "$DATA" \
          --batch-size "$BATCH" --preload --wait --json --quiet)
    [ -n "$QPID" ] && wait "$QPID"
    on "$CLIENT" "$PY" src/query_client.py "$COORD:50051" --final > logs/final.txt
    if diff -q logs/final.txt results/expected_$SLURM_JOB_ID.txt > /dev/null; then
        OK=true
    else
        OK=false
    fi
    Q=$( [ "$QC" -gt 0 ] && cat logs/q.json || echo null )
    printf '{"workers":%d,"strategy":"%s","batch_size":%d,"query_clients":%d,"correct":%s,"stream":%s,"queries":%s}\n' \
        "$W" "$STRATEGY" "$BATCH" "$QC" "$OK" "$RES" "$Q" | tee -a "$OUT"

    kill "${PIDS[@]}" 2>/dev/null
    wait "${PIDS[@]}" 2>/dev/null
    sleep 2
}

for W in 1 2 4; do
    [ "$W" -le "$MAXW" ] && run_config "$W" round_robin 1000 0
done
W=$(( MAXW < 4 ? MAXW : 4 ))
for B in 10 100 10000; do run_config "$W" round_robin "$B" 0; done
for S in least_loaded hash_server; do run_config "$W" "$S" 1000 0; done
for QC in 1 4 16; do run_config "$W" round_robin 1000 "$QC"; done

echo "results in $OUT"
