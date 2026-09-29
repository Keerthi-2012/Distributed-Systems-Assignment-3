#!/bin/bash
#SBATCH --job-name=q7_q1_bench
#SBATCH --partition=debug
#SBATCH --nodes=6
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --time=02:00:00
#
# SIX nodes, ONE task on each. This is the same allocation shape the Q7 MPI
# program was measured with in Assignment 2:
#
#     sbatch --nodes=1 --ntasks-per-node=1 scripts/simple_q7.sh data/large.in
#     sbatch --nodes=2 --ntasks-per-node=1 scripts/simple_q7.sh data/large.in
#     sbatch --nodes=4 --ntasks-per-node=1 scripts/simple_q7.sh data/large.in
#     sbatch --nodes=6 --ntasks-per-node=1 scripts/simple_q7.sh data/large.in
#
# So a task count of N here means N machines with one process each, for BOTH
# sides of the comparison - never N processes sharing one machine's cores. That
# is the only way the MapReduce timings and the Assignment 2 MPI timings mean
# the same thing. TASKS_LIST is 1 2 4 6 for the same reason.
#
# --cpus-per-task=8 gives each node real cores rather than one core's two
# hyperthreads. Without it Slurm hands out a single physical core per node
# (nproc reports 2, but they are siblings: two mappers then take 2.49 s where
# one takes 1.27 s, and with 8 cpus both finish in 1.22 s). One mapper cannot
# use eight cores, but the sort stage and the reducer can, and the headroom
# stops a second process on a node from halving the speed of the first.
#SBATCH --output=results/q1_bench_%j.log
#
# bench_q1.sh - MapReduce benchmark sweep, AND the MapReduce-vs-MPI comparison.
#
#   sbatch scripts/bench_q1.sh                  on the cluster: mappers on
#                                               different nodes, via srun
#   bash   scripts/bench_q1.sh                  locally: parallel processes
#
# On the cluster BOTH sides run one process per machine: the map tasks with
# srun --distribution=cyclic, the MPI ranks with mpirun --map-by node. So
# "tasks=4" always means four separate machines, and the numbers can be put
# beside the Assignment 2 Q7 results, which were taken the same way. The input and the
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
TASKS_LIST=${TASKS_LIST:-"1 2 4 6"}
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
    echo "nodes: ${NODES[*]} (${#NODES[@]} machines, one task each)"
    module load openmpi/4.1.5 2>/dev/null

    # REQUIRED for running MPI across nodes on RCE. Left alone, Open MPI picks
    # an address on 10.0.0.x that the compute nodes cannot reach each other on,
    # and the run either hangs for ever or dies with "failed to TCP connect to
    # a peer MPI process". 172.16.0.0/24 is the network they actually share.
    # (Taken from the Assignment 2 Q7 script, which had already solved this.)
    export OMPI_MCA_btl_tcp_if_include=172.16.0.0/24
    # Silences a harmless warning about copying memory between processes.
    export OMPI_MCA_btl_vader_single_copy_mechanism=none
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
        srun --ntasks="$ntasks" --nodes="$use" --cpus-per-task=1 \
             --distribution=cyclic --overlap \
             bash -c "TID=\$(printf %03d \$SLURM_PROCID); $cmd"
    else
        for i in $(seq 0 $((ntasks - 1))); do
            bash -c "TID=$(printf %03d $i); $cmd" &
        done
        wait
    fi
}

for name in $DATASETS; do
    INPUT=$ROOT/../data/$name.in
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
                #
                # --map-by node puts ONE rank on each machine, going round the
                # nodes one at a time, instead of filling the first node's cores
                # before touching the second. With -np 4 in this 6 node
                # allocation that is 4 machines with one process each - exactly
                # what Assignment 2 measured with
                # --nodes=4 --ntasks-per-node=1.
                #
                # This used to be confined to one node with --host, because
                # multi-node runs hung. The cause was the network interface, not
                # the node count, and OMPI_MCA_btl_tcp_if_include above fixes
                # it; a single-node MPI number could not fairly be compared with
                # MapReduce timings taken across several machines.
                #
                # timeout stays, because a hang here used to stall the whole
                # sweep: one stuck MPI run meant no MapReduce results at all.
                M0=$(now)
                timeout 600 mpirun -np "$TASKS" --map-by node \
                    ./bin/log_mpi "$INPUT" \
                    > "$WORK/mpi_out.txt" 2> "$WORK/mpi_err.txt"
                MPI_RC=$?
                M1=$(now)
                if [ "$MPI_RC" -eq 124 ]; then
                    MPI_CORRECT=timeout
                    echo "  !! MPI timed out: $name procs=$TASKS run=$run"
                    head -3 "$WORK/mpi_err.txt" 2>/dev/null | sed 's/^/     /'
                elif diff -q "$ROOT/logs/expected_$name.txt" "$WORK/mpi_out.txt" > /dev/null; then
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
