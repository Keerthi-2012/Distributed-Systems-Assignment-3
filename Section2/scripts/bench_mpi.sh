#!/bin/bash
#SBATCH --job-name=q7_mpi_bench
#SBATCH --partition=debug
#SBATCH --nodes=1
#SBATCH --ntasks=1
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
# ALL RANKS ON ONE NODE, deliberately. Two reasons:
#   - the MapReduce numbers in results/q1_bench.csv were measured the same way
#     (parallel processes on one node), so the comparison is like-for-like;
#   - cross-node `mpirun` startup hangs on this cluster, so a multi-node run
#     would measure the launcher rather than the program.
#
# Every run is diffed against bin/log_seq; a run that does not match is recorded
# as correct=no and discarded by the plots.
#
# Output: results/q1_mpi.csv

cd "${SLURM_SUBMIT_DIR:-$(dirname "$0")/..}"
ROOT=$(pwd)
mkdir -p results logs
module load openmpi/4.1.5 2>/dev/null

DATASETS=${DATASETS:-"small medium large"}
PROCS_LIST=${PROCS_LIST:-"1 2 4 8"}
REPEATS=${REPEATS:-3}

[ -x ./bin/log_mpi ] || { echo "bin/log_mpi missing - run: module load openmpi/4.1.5 && make mpi"; exit 1; }

CSV=results/q1_mpi.csv
echo "dataset,records,procs,run,total_s,correct" > "$CSV"

for name in $DATASETS; do
    INPUT=data/$name.in
    [ -f "$INPUT" ] || { echo "skip $name (missing)"; continue; }
    RECORDS=$(head -1 "$INPUT" | awk '{print $1}')
    ./bin/log_seq "$INPUT" > "logs/expected_$name.txt"

    for P in $PROCS_LIST; do
        for run in $(seq 1 $REPEATS); do
            START=$(date +%s.%N)
            timeout 300 mpirun --oversubscribe --bind-to none -np "$P" \
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
