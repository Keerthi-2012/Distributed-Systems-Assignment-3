#!/bin/bash
# rce_q2.sh - the one script to drive Q2 on the RCE cluster.
#
# Log in to RCE, go to this folder, and run one of:
#
#   bash scripts/rce_q2.sh setup      once: Python 3.12 venv, grpcio, gRPC stubs
#   bash scripts/rce_q2.sh bench      submit the benchmark sweep (6 nodes)
#   bash scripts/rce_q2.sh verify     submit the correctness run (4 nodes)
#   bash scripts/rce_q2.sh demo       hold 4 nodes and start the live system
#   bash scripts/rce_q2.sh status     show your jobs and the newest log
#   bash scripts/rce_q2.sh stop       kill the servers of a running demo
#
# EVERYTHING HERE IS MULTI-NODE. One process per machine, never several
# processes sharing one machine's cores:
#
#   node 1   the coordinator
#   node 2   the clients you run by hand (dashboard, stream, query)
#   node 3.. one worker each
#
# That is the same shape Q1 and Assignment 2 were measured with, so the two
# questions can be compared.

set -u
# Remember where this script is BEFORE cd, so the help text can read it back
# whatever directory you called it from.
SELF=$(cd "$(dirname "$0")" && pwd)/$(basename "$0")
cd "$(dirname "$0")/.."
ROOT=$(pwd)
WHAT=${1:-help}
PY=${PY:-$HOME/HW3/venv/bin/python3}

say() { echo ""; echo "== $* =="; }

case "$WHAT" in

setup)
    bash scripts/setup_python.sh
    say "Q2 is ready"
    echo "Datasets come from Q1 (shared, so both questions measure the same bytes):"
    echo "    (cd ../Q1_mapreduce && make && bash scripts/make_data.sh)"
    ;;

bench)
    # The sweep allocates its own 6 nodes - see the #SBATCH lines in bench_q2.sh.
    [ -x "$PY" ] || { echo "no venv at $PY - run: bash scripts/rce_q2.sh setup"; exit 1; }
    mkdir -p results logs
    say "submitting the Q2 benchmark: 6 nodes, one process per node"
    sbatch scripts/bench_q2.sh ../data/medium.in
    echo ""
    echo "Watch it with:  bash scripts/rce_q2.sh status"
    echo "It writes results/q2_bench.csv and results/q2_queries.csv."
    ;;

verify)
    [ -x "$PY" ] || { echo "no venv at $PY - run: bash scripts/rce_q2.sh setup"; exit 1; }
    mkdir -p results logs
    say "submitting the Q2 correctness run: 4 nodes"
    sbatch --nodes=4 --ntasks-per-node=1 --cpus-per-task=4 --time=01:00:00 \
           --partition=debug --job-name=q7_q2_verify \
           --output=results/q2_verify_%j.log \
           --wrap "cd $ROOT && $PY scripts/verify_q2.py"
    echo ""
    echo "Watch it with:  bash scripts/rce_q2.sh status"
    echo "It writes results/verify_q2.txt."
    ;;

demo)
    # An interactive demo: hold the nodes yourself, then drive the system by
    # hand from other terminals. salloc must come from a login shell, so this
    # branch expects you to already be inside an allocation.
    if [ -z "${SLURM_JOB_ID:-}" ]; then
        echo "You are not inside an allocation. Get one first:"
        echo ""
        echo "    salloc --nodes=4 --ntasks-per-node=1 --cpus-per-task=4 --time=01:00:00"
        echo "    cd ~/HW3/Section2/Q2_grpc"
        echo "    bash scripts/rce_q2.sh demo"
        echo ""
        echo "salloc must be run from your login shell - a script cannot hold"
        echo "the allocation for you, because it exits and the nodes are freed."
        exit 1
    fi
    [ -x "$PY" ] || { echo "no venv at $PY - run: bash scripts/rce_q2.sh setup"; exit 1; }

    say "starting the coordinator and one worker per node"
    bash scripts/rce_start.sh || exit 1

    COORD=$(cat logs/coord_addr.txt)
    NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))

    say "running"
    echo "coordinator : $COORD   (on ${NODES[0]})"
    echo "workers     : one on each of ${NODES[*]:1}"
    echo ""
    echo "Open TWO more terminals, ssh to rce, then into a node you hold."
    echo ""
    echo "  --- Terminal B: watch the analytics live ---------------------"
    echo "  ssh ${NODES[1]}"
    echo "  cd ~/HW3/Section2/Q2_grpc/src"
    echo "  $PY dashboard.py $COORD"
    echo ""
    echo "  --- Terminal C: feed the stream ------------------------------"
    echo "  ssh ${NODES[2]:-${NODES[1]}}"
    echo "  cd ~/HW3/Section2/Q2_grpc/src"
    echo "  $PY stream_client.py $COORD ../../data/medium.in --rate 100000 --reset --wait"
    echo ""
    echo "  --rate 100000 makes the 1M-record file take about 10 seconds, so"
    echo "  the dashboard visibly fills up. Drop it to go full speed."
    echo ""
    echo "  --- check the answer, from anywhere --------------------------"
    echo "  cd ~/HW3/Section2/Q2_grpc"
    echo "  $PY src/query_client.py $COORD --final > /tmp/grpc.txt"
    echo "  ../Q1_mapreduce/bin/log_seq ../data/medium.in | diff - /tmp/grpc.txt && echo SAME"
    echo ""
    echo "  --- when you are done ----------------------------------------"
    echo "  bash scripts/rce_q2.sh stop      (then 'exit' to release the nodes)"
    ;;

status)
    say "your jobs"
    squeue -u "$USER" -o "%i %j %T %M %D %R"
    say "newest Q2 logs"
    ls -t results/q2_*_[0-9]*.log 2>/dev/null | head -3
    NEWEST=$(ls -t results/q2_*_[0-9]*.log 2>/dev/null | head -1)
    if [ -n "$NEWEST" ]; then
        say "tail of $NEWEST"
        tail -20 "$NEWEST"
    fi
    ;;

stop)
    bash scripts/rce_start.sh stop
    ;;

*)
    sed -n '2,22p' "$SELF" | sed 's/^# \{0,1\}//'
    ;;
esac
