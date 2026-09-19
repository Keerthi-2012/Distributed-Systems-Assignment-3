#!/bin/bash
# rce_setup.sh - one-time setup on the RCE login node.
#
#   cd ~/HW3/Q2_grpc && bash scripts/rce_setup.sh
#
# RCE's default python3 is 3.6, too old for current grpcio, so this creates a
# virtualenv ~/HW3/venv from the cluster's Python 3.12 (home is shared by all
# compute nodes, so every node sees it), installs the packages there, regenerates the gRPC stubs with the installed grpcio-tools version (the
# generated files check that version), builds the HW2 reference tools and
# generates the datasets.

set -e
cd "$(dirname "$0")/.."

BASE_PY=${BASE_PY:-/usr/local/apps/python-3.12.5/bin/python3}
VENV=${VENV:-$HOME/HW3/venv}
[ -x "$VENV/bin/python3" ] || "$BASE_PY" -m venv "$VENV"
PY=$VENV/bin/python3
$PY -m pip install --upgrade pip
$PY -m pip install --upgrade grpcio grpcio-tools protobuf psutil
$PY -c "import grpc; print('grpcio', grpc.__version__)"

make proto PYTHON=$PY
make tools
bash scripts/make_data.sh
echo "setup done - use:  source $VENV/bin/activate"
