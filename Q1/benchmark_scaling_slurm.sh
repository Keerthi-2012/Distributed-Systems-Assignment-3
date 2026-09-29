#!/bin/bash
#SBATCH --job-name=mapreduce_scaling
#SBATCH --output=perf_results/scaling_%j.out
#SBATCH --error=perf_results/scaling_%j.err
#SBATCH --nodes=7
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --time=01:00:00

# Strong-scaling benchmark for Section 1 Q1.
#
#   sbatch benchmark_scaling_slurm.sh
#
# benchmark_dist_slurm.sh runs 7 matrix shapes at a FIXED 4 nodes, so it cannot
# show speedup. This one does the opposite: 3 input sizes, each run with
# 1, 2, 4 and 8 map tasks, so speedup and parallel efficiency can be worked out.
#
# SEVEN nodes, ONE task on each, so p tasks means p separate machines. Packing
# several tasks onto one node instead would make them share a physical core and
# the "speedup" would measure the scheduler, not the program.
#
# Seven, not eight, because node08 is drained and an 8-node job would queue for
# ever. The sweep is therefore 1, 2, 4, 7 rather than 1, 2, 4, 8.
#
# Do NOT put 8 back while only 7 nodes are usable. The allocation grants one
# task slot per node, so `srun --ntasks=8` across 7 nodes silently starts only
# SEVEN tasks: the eighth chunk is never mapped, and the pipeline then produces
# a wrong answer FASTER than the correct one, which looks like a good result.
# Job 99808 did exactly this - every p=8 run came out wrong. The check below
# now catches it instead of relying on the output comparison.
#
# Output: perf_results/scaling.csv - one row per run, timed per stage.

cd "${SLURM_SUBMIT_DIR:-$(dirname "$0")}"
mkdir -p perf_results
WORK=$PWD/perf_results/work_$$
mkdir -p "$WORK"

REPEATS=${REPEATS:-3}
TASKS_LIST=${TASKS_LIST:-"1 2 4 7"}

CSV=perf_results/scaling.csv
echo "size,rows_A,cols_A,rows_B,cols_B,tasks,run,mapper_s,shuffle1_s,combiner_s,shuffle2_s,reducer_s,total_s,correct" > "$CSV"

# label | A file | B file
CASES=(
    "Small|test_data/p1_small_a.txt|test_data/p1_small_b.txt"
    "Medium|test_data/p1_medium_a.txt|test_data/p1_medium_b.txt"
    "Large|test_data/p1_large_a.txt|test_data/p1_large_b.txt"
)

NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
echo "nodes: ${NODES[*]}  (${#NODES[@]} machines, one task each)"
echo ""

now() { date +%s%N; }
secs() { echo "scale=6; ($2 - $1) / 1000000000" | bc; }

# Run one command per task, one task per machine. --nodes and
# --distribution=cyclic matter: without them srun packs every task onto the
# first node and the run is not distributed at all.
run_tasks() {   # run_tasks <ntasks> <command using $TID>
    local n=$1 cmd=$2 use=$1
    [ "$use" -gt "${#NODES[@]}" ] && use=${#NODES[@]}
    srun --ntasks="$n" --nodes="$use" --cpus-per-task=1 \
         --distribution=cyclic --overlap \
         bash -c "TID=\$(printf '%02d' \$SLURM_PROCID); $cmd"
}

for ENTRY in "${CASES[@]}"; do
    LABEL=${ENTRY%%|*}
    REST=${ENTRY#*|}
    A_FILE=$PWD/${REST%%|*}
    B_FILE=$PWD/${REST#*|}
    [ -f "$A_FILE" ] || { echo "SKIP $LABEL - no $A_FILE"; continue; }

    RA=$(wc -l < "$A_FILE"); CA=$(head -1 "$A_FILE" | awk '{print NF-1}')
    RB=$(wc -l < "$B_FILE"); CB=$(head -1 "$B_FILE" | awk '{print NF}')
    export MATRIX_B_FILE="$B_FILE"

    # The answer must not depend on how many ways the input is split, so the
    # 1-task result is kept and every other run is compared against it.
    EXPECTED=$WORK/expected_$LABEL.txt

    for T in $TASKS_LIST; do
        for RUN in $(seq 1 "$REPEATS"); do
            rm -f "$WORK"/chunk_* "$WORK"/map_* "$WORK"/shuf1_* "$WORK"/comb_*
            split -d -a 2 -n "l/$T" "$A_FILE" "$WORK/chunk_"

            T0=$(now)
            run_tasks "$T" "[ -s $WORK/chunk_\$TID ] && MATRIX_B_FILE='$B_FILE' python3 $PWD/mapper.py < $WORK/chunk_\$TID > $WORK/map_\$TID.out || touch $WORK/map_\$TID.out"
            T1=$(now)

            # Every chunk must have produced a map output. If srun quietly ran
            # fewer tasks than asked, a chunk goes unmapped and the answer is
            # wrong but fast - so stop rather than record a misleading row.
            NCHUNK=$(ls "$WORK"/chunk_* 2>/dev/null | wc -l)
            NMAP=$(ls "$WORK"/map_*.out 2>/dev/null | wc -l)
            if [ "$NCHUNK" -ne "$NMAP" ]; then
                echo "  !! $LABEL tasks=$T run=$RUN: $NCHUNK chunks but $NMAP map outputs."
                echo "     srun could not start $T tasks in this allocation - skipping."
                continue
            fi

            run_tasks "$T" "sort -k1,1 $WORK/map_\$TID.out > $WORK/shuf1_\$TID.out"
            T2=$(now)
            run_tasks "$T" "python3 $PWD/combiner.py < $WORK/shuf1_\$TID.out > $WORK/comb_\$TID.out"
            T3=$(now)
            sort -k1,1 "$WORK"/comb_*.out > "$WORK/global.out"
            T4=$(now)
            python3 reducer.py < "$WORK/global.out" > "$WORK/actual.txt"
            T5=$(now)

            # Correctness: the 1-task, first-run output is the reference.
            if [ ! -f "$EXPECTED" ]; then
                cp "$WORK/actual.txt" "$EXPECTED"
                CORRECT=yes
            elif sort "$EXPECTED" | diff -q - <(sort "$WORK/actual.txt") > /dev/null; then
                CORRECT=yes
            else
                CORRECT=no
                echo "  !! MISMATCH $LABEL tasks=$T run=$RUN"
            fi

            echo "$LABEL,$RA,$CA,$RB,$CB,$T,$RUN,$(secs $T0 $T1),$(secs $T1 $T2),$(secs $T2 $T3),$(secs $T3 $T4),$(secs $T4 $T5),$(secs $T0 $T5),$CORRECT" >> "$CSV"
            echo "  $LABEL ${RA}x${CA}  tasks=$T run=$RUN  total=$(secs $T0 $T5)s  correct=$CORRECT"
        done
    done
    echo ""
done

rm -rf "$WORK"
echo "============================================"
column -t -s',' "$CSV"
echo "============================================"
echo "wrote $CSV"
