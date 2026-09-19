#!/bin/bash
# make_data.sh - generates the datasets used for testing and benchmarks.
#
# Uses the HW2 generator (reference/gen_dataset.cpp) with fixed seeds, so running
# this again produces byte-identical files. small/medium/large use the same parameters
# as HW2, so HW2 results and these results refer to the same data.
#
#   bash scripts/make_data.sh          (run `make tools` first)

cd "$(dirname "$0")/.."
mkdir -p data

#                      N        K  S    seed
./bin/gen_dataset      20000   10  32   2000 data/tiny.in
./bin/gen_dataset     100000   10  64   2001 data/small.in
./bin/gen_dataset    1000000   10 128   2002 data/medium.in
./bin/gen_dataset   10000000   10 256   2003 data/large.in

ls -lh data/
