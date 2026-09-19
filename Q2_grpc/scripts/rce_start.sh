#!/bin/bash
# rce_start.sh - starts the system across the nodes of an interactive allocation.
#
# Inside `salloc --nodes=4 --ntasks-per-node=1` (run from the Q2_grpc folder):
#
#   bash scripts/rce_start.sh [workers_per_node] [strategy]
#
# Node layout (hostnames come from $SLURM_JOB_NODELIST):
#   node 1        coordinator on :50051
#   nodes 2..N    workers on :50061, :50062, ... (workers_per_node each, default 1)
#
# The client and dashboard are then started by hand, as in the RCE guide:
#   ssh <any node>; source ~/HW3/venv/bin/activate; cd ~/HW3/Q2_grpc/src
#   python3 dashboard.py <node1>:50051
#   python3 stream_client.py <node1>:50051 ../data/medium.in --rate 50000
#   python3 query_client.py <node1>:50051 --final
#
# Stop everything with:  bash scripts/rce_start.sh stop

cd "$(dirname "$0")/.."
ROOT=$(pwd)
mkdir -p logs
PY=${PY:-$HOME/HW3/venv/bin/python3}   # virtualenv made by rce_setup.sh (system python3 is 3.6)

if [ "$1" = "stop" ]; then
    for h in $(scontrol show hostnames "$SLURM_JOB_NODELIST"); do
        ssh -n "$h" "pkill -u $USER -f 'Q2_grpc/src/(worker|coordinator).py'" 2>/dev/null
    done
    echo "stopped"
    exit 0
fi

PER_NODE=${1:-1}
STRATEGY=${2:-round_robin}
NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
if [ ${#NODES[@]} -lt 2 ]; then
    echo "need at least 2 nodes (1 coordinator + workers)"; exit 1
fi
COORD=${NODES[0]}

WORKERS=""
for h in "${NODES[@]:1}"; do
    for j in $(seq 1 "$PER_NODE"); do
        PORT=$((50060 + j))
        ssh -n "$h" "cd $ROOT/src; nohup $PY ${ROOT}/src/worker.py 0.0.0.0:$PORT \
            > $ROOT/logs/worker_${h}_$PORT.log 2>&1 < /dev/null &"
        WORKERS="$WORKERS,$h:$PORT"
    done
done
WORKERS=${WORKERS#,}

ssh -n "$COORD" "cd $ROOT/src; nohup $PY ${ROOT}/src/coordinator.py 0.0.0.0:50051 \
    --workers $WORKERS --strategy $STRATEGY > $ROOT/logs/coordinator.log 2>&1 < /dev/null &"

echo "coordinator : $COORD:50051"
echo "workers     : $WORKERS"
echo "strategy    : $STRATEGY"
echo ""
echo "next:  source ~/HW3/venv/bin/activate"
echo "       cd src && python3 dashboard.py $COORD:50051"
echo "       cd src && python3 stream_client.py $COORD:50051 ../data/medium.in --wait"
echo "       cd src && python3 query_client.py $COORD:50051 --final"
