#!/bin/bash
# run_local.sh - start Q2's coordinator and W workers on this machine, in the
# background, for local testing.
#
#   bash scripts/run_local.sh [workers] [strategy]
#   bash scripts/run_local.sh stop
#
# Ports: coordinator 50051, workers 50061, 50062, ...
# Logs:  logs/coordinator.log, logs/worker<N>.log

cd "$(dirname "$0")/.."
ROOT=$(pwd)
# Which Python to use. grpcio is usually not installed in the system python3,
# so prefer a virtualenv: the project's .venv locally, or ~/HW3/venv on RCE.
# Override with PY=... if yours lives somewhere else.
if [ -z "${PY:-}" ]; then
    for candidate in "$ROOT/../../.venv/bin/python3" "$HOME/HW3/venv/bin/python3"; do
        [ -x "$candidate" ] && { PY=$candidate; break; }
    done
fi
PY=${PY:-python3}
if ! "$PY" -c "import grpc" 2>/dev/null; then
    echo "$PY cannot import grpc - install it with: $PY -m pip install grpcio grpcio-tools"
    exit 1
fi
SRC=$ROOT/src
mkdir -p logs

if [ "$1" = "stop" ]; then
    # The servers are started from inside src/, so their command line is
    # "python3 worker.py <port>" without the directory - match that.
    pkill -f "python3 worker\.py" 2>/dev/null
    pkill -f "python3 coordinator\.py" 2>/dev/null
    echo "stopped"
    exit 0
fi

W=${1:-4}
STRATEGY=${2:-round_robin}

WORKERS=""
for i in $(seq 0 $((W - 1))); do
    PORT=$((50061 + i))
    (cd "$SRC" && nohup $PY worker.py "0.0.0.0:$PORT" \
        > "$ROOT/logs/worker$i.log" 2>&1 &)
    WORKERS="$WORKERS,localhost:$PORT"
done
WORKERS=${WORKERS#,}
sleep 1

(cd "$SRC" && nohup $PY coordinator.py 0.0.0.0:50051 --workers "$WORKERS" \
    --strategy "$STRATEGY" > "$ROOT/logs/coordinator.log" 2>&1 &)
sleep 1

echo "coordinator : localhost:50051"
echo "workers     : $WORKERS"
echo "strategy    : $STRATEGY"
echo
echo "next:  cd src && python3 dashboard.py localhost:50051"
echo "       cd src && python3 stream_client.py localhost:50051 ../../data/medium.in --wait"
echo "       cd src && python3 query_client.py localhost:50051 --final"
echo "stop:  bash scripts/run_local.sh stop"
