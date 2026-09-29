#!/bin/bash
#SBATCH --job-name=q7_mpi_bench
#SBATCH --partition=debug
#SBATCH --nodes=6
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --time=01:00:00
#SBATCH --output=results/q1_mpi_%j.log
#
# bench_mpi.sh - times the MPI program, for the MapReduce-vs-MPI comparison.
#
#   sbatch scripts/bench_mpi.sh            on the cluster
#   bash   scripts/bench_mpi.sh            locally, if MPI is installed
#
# Build first:  module load openmpi/4.1.5 && make mpi
#
# ONE RANK PER MACHINE, across nodes. -np 4 means four separate machines, not
# four cores of one. That is the shape Assignment 2 measured Q7 with
# (--nodes=N --ntasks-per-node=1), and the shape the MapReduce pipeline in
# bench_q1.sh runs in, so all three sets of numbers can be compared directly.
#
# OMPI_MCA_btl_tcp_if_include below is what makes a multi-node run work at all
# on RCE; without it Open MPI tries an unroutable 10.0.0.x address and hangs.
#
# Every run is diffed against bin/log_seq; a run that does not match is recorded
# as correct=no and discarded by the plots.
#
# Output: results/q1_mpi.csv

cd "${SLURM_SUBMIT_DIR:-$(dirname "$0")/..}"
ROOT=$(pwd)
mkdir -p results logs
module load openmpi/4.1.5 2>/dev/null

# The network the RCE compute nodes actually share. Required for multi-node MPI.
export OMPI_MCA_btl_tcp_if_include=172.16.0.0/24
export OMPI_MCA_btl_vader_single_copy_mechanism=none

DATASETS=${DATASETS:-"small medium large"}
PROCS_LIST=${PROCS_LIST:-"1 2 4 6"}   # node counts, matching Assignment 2
REPEATS=${REPEATS:-3}

[ -x ./bin/log_mpi ] || { echo "bin/log_mpi missing - run: module load openmpi/4.1.5 && make mpi"; exit 1; }

CSV=results/q1_mpi.csv
echo "dataset,records,procs,run,total_s,correct" > "$CSV"

for name in $DATASETS; do
    INPUT=../data/$name.in
    [ -f "$INPUT" ] || { echo "skip $name (missing)"; continue; }
    RECORDS=$(head -1 "$INPUT" | awk '{print $1}')
    ./bin/log_seq "$INPUT" > "logs/expected_$name.txt"

    for P in $PROCS_LIST; do
        for run in $(seq 1 $REPEATS); do
            START=$(date +%s.%N)
            # --map-by node: one rank on each machine, round robin.
            timeout 300 mpirun -np "$P" --map-by node \
                ./bin/log_mpi "$INPUT" > "logs/mpi_out.txt" 2> "logs/mpi_err.txt"
            END=$(date +%s.%N)

            if diff -q "logs/expected_$name.txt" "logs/mpi_out.txt" > /dev/null 2>&1; then
                CORRECT=yes
            else
                CORRECT=no
                echo "  !! MPI mismatch: $name procs=$P run=$run"
                head -3 logs/mpi_err.txt | sed 's/^/     /'
            fi
            T=$(echo "scale=6; $END - $START" | bc)
            echo "$name,$RECORDS,$P,$run,$T,$CORRECT" >> "$CSV"
            echo "  MPI $name procs=$P run=$run total=${T}s correct=$CORRECT"
        done
    done
done

echo "results in $CSV"
