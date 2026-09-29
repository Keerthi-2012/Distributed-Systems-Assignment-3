#!/bin/bash
# rce_start.sh - start Q2's coordinator and workers across an allocation.
#
# Inside `salloc --nodes=4 --ntasks-per-node=1`, from the Section2 folder:
#
#   bash scripts/rce_start.sh [workers_per_node] [strategy]
#
#   node 1        coordinator on $PORT_BASE
#   nodes 2..N    workers on $PORT_BASE+11, +12, ...
#
# PORT 50051 IS NOT SAFE HERE. It is the port the RCE guide uses as its example,
# so every student on the cluster tries to bind it. If somebody else already has
# it on our node, our coordinator dies and our client happily connects to THEIR
# server instead - which shows up as "Method not found", not as an error we can
# recognise. So the ports are derived from our own user id by default.
#
# Then, as the RCE execution guide describes, run the clients from other nodes:
#   ssh <node>; cd ~/HW3/Section2
#   source ~/HW3/venv/bin/activate; cd ~/HW3/Section2/Q2_grpc/src
#   python3 dashboard.py     <node1>:<port>
#   python3 stream_client.py <node1>:<port> ../../data/medium.in --rate 100000
#   python3 query_client.py  <node1>:<port> --final
#
# Stop everything:  bash scripts/rce_start.sh stop

cd "$(dirname "$0")/.."
ROOT=$(pwd)
mkdir -p logs

# A base port of our own: 50000 + (uid mod 9000), so two students almost never
# choose the same one. Override with PORT_BASE=... if it is ever taken anyway.
PORT_BASE=${PORT_BASE:-$((50000 + $(id -u) % 9000))}

if [ "$1" = "stop" ]; then
    for h in $(scontrol show hostnames "$SLURM_JOB_NODELIST"); do
        ssh -n "$h" "pkill -u $USER -f 'src/(worker|coordinator).py'" 2>/dev/null
    done
    echo "stopped"
    exit 0
fi

PY=${PY:-$HOME/HW3/venv/bin/python3}   # RCE's default python3 is 3.6, too old for grpcio
SRC=$ROOT/src

PER_NODE=${1:-1}
STRATEGY=${2:-round_robin}
NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
if [ ${#NODES[@]} -lt 2 ]; then
    echo "need at least 2 nodes (1 coordinator + workers)"; exit 1
fi
COORD=${NODES[0]}

# Note: "cd X; nohup ... &" and ssh -n, never "cd X && nohup ... &" - with &&
# the & backgrounds the whole list, the subshell keeps ssh's streams open, and
# ssh then never returns.
WORKERS=""
for h in "${NODES[@]:1}"; do
    for j in $(seq 1 "$PER_NODE"); do
        PORT=$((PORT_BASE + 10 + j))
        ssh -n "$h" "cd $SRC; nohup $PY $SRC/worker.py 0.0.0.0:$PORT \
            > $ROOT/logs/worker_${h}_$PORT.log 2>&1 < /dev/null &"
        WORKERS="$WORKERS,$h:$PORT"
    done
done
WORKERS=${WORKERS#,}

ssh -n "$COORD" "cd $SRC; nohup $PY $SRC/coordinator.py 0.0.0.0:$PORT_BASE \
    --workers $WORKERS --strategy $STRATEGY \
    > $ROOT/logs/coordinator.log 2>&1 < /dev/null &"
sleep 3

# Say plainly whether the coordinator is actually listening. Silently carrying on
# after a failed bind is how we ended up querying a stranger's server.
if ! $PY - "$COORD" "$PORT_BASE" <<'PYCHECK'
import socket, sys
s = socket.socket(); s.settimeout(5)
sys.exit(0 if s.connect_ex((sys.argv[1], int(sys.argv[2]))) == 0 else 1)
PYCHECK
then
    echo "ERROR: nothing is listening on $COORD:$PORT_BASE - see logs/coordinator.log"
    exit 1
fi

# Record the address so the demo and the benchmarks use the same one.
echo "$COORD:$PORT_BASE" > "$ROOT/logs/coord_addr.txt"

echo "coordinator : $COORD:$PORT_BASE"
echo "workers     : $WORKERS"
echo "strategy    : $STRATEGY"
echo
echo "next:  cd src"
echo "       ~/HW3/venv/bin/python3 dashboard.py     $COORD:$PORT_BASE"
echo "       ~/HW3/venv/bin/python3 stream_client.py $COORD:$PORT_BASE ../../data/medium.in --rate 100000 --wait"
echo "       ~/HW3/venv/bin/python3 query_client.py  $COORD:$PORT_BASE --final"
