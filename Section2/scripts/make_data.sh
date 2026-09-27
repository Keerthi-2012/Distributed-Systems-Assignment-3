#!/bin/bash
# make_data.sh - generate the datasets with fixed seeds, so anyone can rebuild
# exactly the same files. Parameters are "N K S seed", as HW2 used them.
#
#   bash scripts/make_data.sh [which]     which = tiny|small|medium|large|all
#
# small, medium and large use exactly HW2's parameters, so the benchmark
# numbers are comparable with the HW2 MPI results.

set -e
cd "$(dirname "$0")/.."
mkdir -p data
GEN=bin/gen_dataset
[ -x "$GEN" ] || { echo "build first: make tools"; exit 1; }

want=${1:-all}
gen() {   # gen <name> <N> <K> <S> <seed>
    if [ "$want" = "all" ] || [ "$want" = "$1" ]; then
        $GEN "$2" "$3" "$4" "$5" "data/$1.in"
    fi
}

gen tiny      20000 10  32 2000
gen small    100000 10  64 2001
gen medium  1000000 10 128 2002
gen large  10000000 10 256 2003

echo "--- md5 ---"
md5sum data/*.in | tee results/dataset_md5.txt
ls -la data/
