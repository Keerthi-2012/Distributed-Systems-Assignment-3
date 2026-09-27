#!/bin/bash
# rce_setup.sh - one-time setup on the RCE login node.
#
#   cd ~/HW3/Section2 && bash scripts/rce_setup.sh
#
# Q1 is C++ and needs nothing but g++.
# Q2 is Python and needs grpcio, which RCE's default python3 (3.6) is too old
# for, so we build a virtualenv from the cluster's Python 3.12. Home is shared
# by every compute node, so one venv serves them all.

set -e
cd "$(dirname "$0")/.."
ROOT=$(pwd)

BASE_PY=${BASE_PY:-/usr/local/apps/python-3.12.5/bin/python3}
VENV=${VENV:-$HOME/HW3/venv}

echo "== Q1: building the C++ programs and the HW2 reference =="
make                                  # mapper, combiner, reducer, log_seq, gen_dataset

echo "== Q2: Python environment =="
[ -x "$VENV/bin/python3" ] || "$BASE_PY" -m venv "$VENV"
PY=$VENV/bin/python3
$PY -m pip install --quiet --upgrade pip
$PY -m pip install --quiet --upgrade grpcio grpcio-tools protobuf
$PY -c "import grpc; print('   grpcio', grpc.__version__)"

echo "== Q2: generating the gRPC stubs =="
# Generated here rather than shipped, because the generated code checks that it
# matches the installed grpcio version.
$PY -m grpc_tools.protoc -IQ2_grpc/proto \
    --python_out=Q2_grpc/src --grpc_python_out=Q2_grpc/src \
    Q2_grpc/proto/loganalytics.proto

echo "== datasets =="
bash scripts/make_data.sh

echo
echo "setup done."
echo "  Q1:  ./bin/mapper | sort | ./bin/combiner | sort | ./bin/reducer"
echo "  Q2:  source $VENV/bin/activate   (then python3 Q2_grpc/src/...)"
