#!/bin/bash
# setup_python.sh - one-time setup for Q2 on the RCE login node.
#
#   cd ~/HW3/Section2/Q2_grpc && bash scripts/setup_python.sh
#
# Q2 needs grpcio, and RCE's default python3 is 3.6, which is too old for it.
# So we build a virtualenv from the cluster's Python 3.12. Your home directory
# is shared by every compute node, so one virtualenv serves them all.
#
# It also regenerates the two Python files that grpc builds from the .proto.
# RE-RUN THIS after any change to proto/loganalytics.proto.
#
# Q1 is separate and needs nothing but g++:  cd ../Q1_mapreduce && make

set -e
cd "$(dirname "$0")/.."
ROOT=$(pwd)

BASE_PY=${BASE_PY:-/usr/local/apps/python-3.12.5/bin/python3}
VENV=${VENV:-$HOME/HW3/venv}

echo "== Python environment =="
[ -x "$VENV/bin/python3" ] || "$BASE_PY" -m venv "$VENV"
PY=$VENV/bin/python3
$PY -m pip install --quiet --upgrade pip
$PY -m pip install --quiet --upgrade grpcio grpcio-tools protobuf

echo "== generating the gRPC stubs from the .proto =="
$PY -m grpc_tools.protoc -Iproto \
    --python_out=src --grpc_python_out=src \
    proto/loganalytics.proto

echo ""
echo "done."
echo "  activate with : source $VENV/bin/activate"
echo "  then run      : cd src && python3 coordinator.py --help"
echo ""
echo "Datasets are shared with Q1 and are generated there:"
echo "  (cd ../Q1_mapreduce && make && bash scripts/make_data.sh)"
echo "They land in Section2/data/, which both questions read."
