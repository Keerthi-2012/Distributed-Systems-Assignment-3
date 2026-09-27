#!/bin/bash
#SBATCH --job-name=q7_q1_bench
#SBATCH --partition=debug
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --time=02:00:00
#SBATCH --output=results/q1_bench_%j.log
#
# bench_q1.sh - MapReduce benchmark sweep, AND the MapReduce-vs-MPI comparison.
#
#   sbatch scripts/bench_q1.sh                  on the cluster: mappers on
#                                               different nodes, via srun
#   bash   scripts/bench_q1.sh                  locally: parallel processes
#
# On the cluster the map tasks run with srun, one per node, so the mappers
# really are on separate machines - the same shape as the MPI program they are
# compared against, which is what makes the comparison fair. The input and the
# intermediate files sit in the shared home directory that every node can see,
# standing in for HDFS.
#
# For each dataset and task count it runs, REPEATS times:
#   - the MapReduce pipeline: map (parallel) -> sort -> combine -> gather -> reduce
#   - the MPI program with the same number of processes (if bin/log_mpi exists)
# and diffs both against bin/log_seq. A run that does not match is recorded as
# correct=no and discarded by the plots.
#
# Output: results/q1_bench.csv  MapReduce, one row per run, timed per stage
#         results/q1_mpi.csv    MPI, one row per run
#
# Environment: DATASETS, TASKS_LIST, REPEATS, MPI_ONLY=1 (skip the MapReduce
# sweep and only measure MPI, for when the MapReduce numbers already exist).

cd "${SLURM_SUBMIT_DIR:-$(dirname "$0")/..}"
ROOT=$(pwd)
mkdir -p results logs
REPEATS=${REPEATS:-3}
DATASETS=${DATASETS:-"small medium large"}
TASKS_LIST=${TASKS_LIST:-"1 2 4 8"}
MPI_ONLY=${MPI_ONLY:-0}

CSV=results/q1_bench.csv
MPICSV=results/q1_mpi.csv
if [ "$MPI_ONLY" != 1 ]; then
    echo "dataset,records,tasks,run,map_s,sort1_s,combine_s,sort2_s,reduce_s,total_s,shuffle_bytes,input_bytes,correct" > "$CSV"
fi
echo "dataset,records,procs,run,total_s,correct" > "$MPICSV"

# Per-task files must be visible from every node, so they live in the shared
# home directory, not /tmp, which is node-local.
WORK=$ROOT/logs/bench_work
mkdir -p "$WORK"

ON_CLUSTER=0
if [ -n "$SLURM_JOB_ID" ]; then
    ON_CLUSTER=1
    NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
    echo "nodes: ${NODES[*]}"
    module load openmpi/4.1.5 2>/dev/null
fi

now() { date +%s.%N; }
d() { echo "scale=6; $2 - $1" | bc; }

# One command per task, in parallel: across nodes under Slurm, as background
# processes otherwise. $TID is the task number, zero padded to 3 digits.
run_tasks() {   # run_tasks <ntasks> <command using $TID>
    local ntasks=$1 cmd=$2
    if [ "$ON_CLUSTER" = 1 ]; then
        # --nodes and --distribution matter. Without them srun packs every task
        # onto ONE node of the allocation, and the "distributed" run is really a
        # single machine. --nodes spreads the tasks over as many nodes as we have
        # (or as many as we have tasks), and cyclic distribution deals them out
        # round robin, one per node, instead of filling a node before moving on.
        local use=$ntasks
        [ "$use" -gt "${#NODES[@]}" ] && use=${#NODES[@]}
        srun --ntasks="$ntasks" --nodes="$use" --distribution=cyclic --overlap \
             bash -c "TID=\$(printf %03d \$SLURM_PROCID); $cmd"
    else
        for i in $(seq 0 $((ntasks - 1))); do
            bash -c "TID=$(printf %03d $i); $cmd" &
        done
        wait
    fi
}

for name in $DATASETS; do
    INPUT=$ROOT/data/$name.in
    [ -f "$INPUT" ] || { echo "skip $name (no $INPUT)"; continue; }
    RECORDS=$(head -1 "$INPUT" | awk '{print $1}')
    INPUT_BYTES=$(stat -c %s "$INPUT")
    ./bin/log_seq "$INPUT" > "$ROOT/logs/expected_$name.txt"

    for TASKS in $TASKS_LIST; do
        for run in $(seq 1 $REPEATS); do

            # ---- MapReduce ----
            if [ "$MPI_ONLY" != 1 ]; then
                rm -f "$WORK"/chunk_* "$WORK"/map_* "$WORK"/sorted_* "$WORK"/comb_*
                # Splitting stands in for HDFS placing blocks; not timed,
                # because under Hadoop the file is already split.
                split -d -a 3 -n "l/$TASKS" "$INPUT" "$WORK/chunk_"

                T0=$(now)
                run_tasks "$TASKS" "$ROOT/bin/mapper < $WORK/chunk_\$TID > $WORK/map_\$TID 2>/dev/null"
                T1=$(now)
                run_tasks "$TASKS" "sort $WORK/map_\$TID > $WORK/sorted_\$TID"
                T2=$(now)
                run_tasks "$TASKS" "$ROOT/bin/combiner < $WORK/sorted_\$TID > $WORK/comb_\$TID 2>/dev/null"
                T3=$(now)
                cat "$WORK"/comb_* | sort > "$WORK/shuffled"
                T4=$(now)
                ./bin/reducer < "$WORK/shuffled" > "$WORK/out.txt" 2>/dev/null
                T5=$(now)

                SHUFFLE=$(cat "$WORK"/map_* | wc -c)
                if diff -q "$ROOT/logs/expected_$name.txt" "$WORK/out.txt" > /dev/null; then
                    CORRECT=yes
                else
                    CORRECT=no
                    echo "  !! MapReduce mismatch: $name tasks=$TASKS run=$run"
                fi
                echo "$name,$RECORDS,$TASKS,$run,$(d $T0 $T1),$(d $T1 $T2),$(d $T2 $T3),$(d $T3 $T4),$(d $T4 $T5),$(d $T0 $T5),$SHUFFLE,$INPUT_BYTES,$CORRECT" >> "$CSV"
                echo "  MR  $name tasks=$TASKS run=$run total=$(d $T0 $T5)s correct=$CORRECT"
            fi

            # ---- the same work with MPI, same process count ----
            if [ -x ./bin/log_mpi ]; then
                # mpirun, not srun: srun starts the ranks but does not wire
                # up MPI here, and the program then fails silently.
                M0=$(now)
                mpirun --oversubscribe -np "$TASKS" ./bin/log_mpi "$INPUT" \
                    > "$WORK/mpi_out.txt" 2> "$WORK/mpi_err.txt"
                M1=$(now)
                if diff -q "$ROOT/logs/expected_$name.txt" "$WORK/mpi_out.txt" > /dev/null; then
                    MPI_CORRECT=yes
                else
                    MPI_CORRECT=no
                    echo "  !! MPI mismatch: $name procs=$TASKS run=$run"
                    head -3 "$WORK/mpi_err.txt" 2>/dev/null | sed 's/^/     /'
                    diff "$ROOT/logs/expected_$name.txt" "$WORK/mpi_out.txt" | head -4 | sed 's/^/     /' 
                fi
                echo "$name,$RECORDS,$TASKS,$run,$(d $M0 $M1),$MPI_CORRECT" >> "$MPICSV"
                echo "  MPI $name procs=$TASKS run=$run total=$(d $M0 $M1)s correct=$MPI_CORRECT"
            fi
        done
    done
done

rm -rf "$WORK"
echo "results in $CSV and $MPICSV"
