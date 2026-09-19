#!/bin/bash
# run_local.sh - starts W workers and the coordinator on THIS machine, in the background.
#
#   bash scripts/run_local.sh [W] [strategy]      default: 4 workers, round_robin
#   bash scripts/run_local.sh stop
#
# Then, in other terminals (from the src/ folder):
#   python3 dashboard.py localhost:50051
#   python3 stream_client.py localhost:50051 ../data/medium.in --rate 50000
#   python3 query_client.py localhost:50051 --final

cd "$(dirname "$0")/../src"
PY=${PYTHON:-python3}
PIDFILE=../.local_pids

if [ "$1" = "stop" ]; then
    [ -f $PIDFILE ] && kill $(cat $PIDFILE) 2>/dev/null
    rm -f $PIDFILE
    echo "stopped"
    exit 0
fi

W=${1:-4}
STRATEGY=${2:-round_robin}
mkdir -p ../logs
: > $PIDFILE

WORKERS=""
for i in $(seq 1 "$W"); do
    PORT=$((50060 + i))
    $PY worker.py 0.0.0.0:$PORT 2> ../logs/worker$i.log &
    echo $! >> $PIDFILE
    WORKERS="$WORKERS,localhost:$PORT"
done
WORKERS=${WORKERS#,}

$PY coordinator.py 0.0.0.0:50051 --workers "$WORKERS" --strategy "$STRATEGY" \
    2> ../logs/coordinator.log &
echo $! >> $PIDFILE

echo "coordinator on localhost:50051 with $W workers ($WORKERS), strategy $STRATEGY"
echo "logs in logs/, stop with: bash scripts/run_local.sh stop"
