#!/bin/bash
#SBATCH --job-name=mapreduce_benchmark
#SBATCH --output=perf_results/benchmark_dist_results_%j.out
#SBATCH --error=perf_results/benchmark_dist_results_%j.err
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1

# Slurm writes the two files above relative to the directory you SUBMIT from,
# not to this script's location, and it cannot expand variables in #SBATCH
# lines. So submit from the question folder:
#
#     cd ~/HW3/Section1_Q1 && sbatch scripts/benchmark_dist_slurm.sh
#
# and the log lands in perf_results/ beside the results it describes. Submitting
# from ~/HW3 instead used to scatter test_results_*.out across the home
# directory.
#SBATCH --time=00:30:00

# Distributed benchmark — 7 shapes across 4 nodes
# Covers: baseline, square divisible, square NOT divisible, tall (m>>n), wide (n>>m)

# Work from the question folder, whatever directory this was submitted from.
# Resolving against the script's own location rather than $SLURM_SUBMIT_DIR is
# deliberate: submitting from the wrong directory used to make the run "succeed"
# in nine seconds with 0 PASSED / 9 FAILED and "can't open file 'combiner.py'",
# because $SLURM_SUBMIT_DIR pointed somewhere with no code in it.
QDIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$QDIR"
mkdir -p perf_results

# Intermediate files go in a per-job scratch directory, not in the question
# folder. They used to be written beside mapper.py, so a run left chunk_*,
# map_*.out, shuf1_*.out and comb_*.out lying next to the source, and two jobs
# running at once would overwrite each other's. Home is shared by every compute
# node, so the scratch directory is visible from all of them.
WORK=$QDIR/perf_results/work_${SLURM_JOB_ID:-$$}
mkdir -p "$WORK"
trap 'rm -rf "$WORK"' EXIT
mkdir -p perf_results
SUMMARY="perf_results/dist_benchmark_summary.csv"
echo "label,shape_A,shape_B,divisible_by_4,mapper_s,shuffle1_s,combiner_s,shuffle2_s,reducer_s,total_s" > "$SUMMARY"

# Test cases: label | A_file | B_file | divisible?
TEST_CASES=(
    "p1_small|test_data/p1_small_a.txt|test_data/p1_small_b.txt|yes"
    "p1_medium|test_data/p1_medium_a.txt|test_data/p1_medium_b.txt|yes"
    "p1_large|test_data/p1_large_a.txt|test_data/p1_large_b.txt|yes"
    "p1_square|test_data/p1_square_a.txt|test_data/p1_square_b.txt|yes"
    "p1_square_uneven|test_data/p1_square_uneven_a.txt|test_data/p1_square_b.txt|no"
    "p1_tall|test_data/p1_tall_a.txt|test_data/p1_tall_b.txt|yes"
    "p1_wide|test_data/p1_wide_a.txt|test_data/p1_wide_b.txt|no"
)

echo "Nodes: $SLURM_JOB_NODELIST | Tasks: $SLURM_NTASKS | Date: $(date)"
echo ""

for ENTRY in "${TEST_CASES[@]}"; do
    LABEL=$(echo "$ENTRY" | cut -d'|' -f1)
    A_FILE="$QDIR/$(echo "$ENTRY" | cut -d'|' -f2)"
    B_FILE="$QDIR/$(echo "$ENTRY" | cut -d'|' -f3)"
    DIV=$(echo "$ENTRY" | cut -d'|' -f4)

    [ ! -f "$A_FILE" ] && echo "SKIP $LABEL — file not found" && continue

    ROWS=$(wc -l < "$A_FILE")
    COLS_A=$(head -1 "$A_FILE" | awk '{print NF-1}')
    ROWS_B=$(wc -l < "$B_FILE")
    COLS_B=$(head -1 "$B_FILE" | awk '{print NF}')
    export MATRIX_B_FILE="$B_FILE"

    echo "[$LABEL]  A:${ROWS}x${COLS_A} x B:${ROWS_B}x${COLS_B}  divisible_by_4=$DIV"

    TOTAL_START=$(date +%s%N)
    split -d -a 2 -n l/$SLURM_NTASKS "$A_FILE" "$WORK/chunk_"

    # Stage 1: Mapper — distributed, handles empty chunks from uneven splits
    S=$(date +%s%N)
    srun --ntasks=$SLURM_NTASKS bash -c "
        TID=\$(printf '%02d' \$SLURM_PROCID)
        [ -s $WORK/chunk_\${TID} ] && MATRIX_B_FILE=\"$B_FILE\" python3 $QDIR/mapper.py < $WORK/chunk_\${TID} > $WORK/map_\${TID}.out || touch $WORK/map_\${TID}.out"
    MAPPER=$(echo "scale=6; ($(date +%s%N) - $S) / 1000000000" | bc)

    # Stage 2: Shuffle/Sort 1 — local sort per node
    S=$(date +%s%N)
    srun --ntasks=$SLURM_NTASKS bash -c '
        TID=$(printf "%02d" $SLURM_PROCID); sort -k1,1 $WORK/map_${TID}.out > $WORK/shuf1_${TID}.out'
    SHUF1=$(echo "scale=6; ($(date +%s%N) - $S) / 1000000000" | bc)

    # Stage 3: Combiner — distributed
    S=$(date +%s%N)
    srun --ntasks=$SLURM_NTASKS bash -c '
        TID=$(printf "%02d" $SLURM_PROCID); python3 $QDIR/combiner.py < $WORK/shuf1_${TID}.out > $WORK/comb_${TID}.out'
    COMB=$(echo "scale=6; ($(date +%s%N) - $S) / 1000000000" | bc)

    # Stage 4: Global sort on master
    S=$(date +%s%N)
    sort -k1,1 "$WORK"/comb_*.out > "$WORK/global_shuf2.out"
    SHUF2=$(echo "scale=6; ($(date +%s%N) - $S) / 1000000000" | bc)

    # Stage 5: Reducer on master
    S=$(date +%s%N)
    python3 reducer.py < "$WORK/global_shuf2.out" > "perf_results/output_${LABEL}.txt"
    RED=$(echo "scale=6; ($(date +%s%N) - $S) / 1000000000" | bc)

    TOTAL=$(echo "scale=6; ($(date +%s%N) - $TOTAL_START) / 1000000000" | bc)
    echo "  mapper=${MAPPER}s  shuf1=${SHUF1}s  comb=${COMB}s  shuf2=${SHUF2}s  red=${RED}s  TOTAL=${TOTAL}s"
    echo ""

    echo "${LABEL},${ROWS}x${COLS_A},${ROWS_B}x${COLS_B},${DIV},${MAPPER},${SHUF1},${COMB},${SHUF2},${RED},${TOTAL}" >> "$SUMMARY"
    rm -f "$WORK"/chunk_* "$WORK"/map_*.out "$WORK"/shuf1_*.out "$WORK"/comb_*.out "$WORK/global_shuf2.out"
done

echo "============================================"
column -t -s',' "$SUMMARY"
echo "============================================"