#!/bin/bash
#SBATCH --job-name=mapreduce_benchmark
#SBATCH --output=benchmark_dist_results_%j.out
#SBATCH --error=benchmark_dist_results_%j.err
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --time=00:30:00

# Distributed benchmark — 7 shapes across 4 nodes
# Covers: baseline, square divisible, square NOT divisible, tall (m>>n), wide (n>>m)

cd "$SLURM_SUBMIT_DIR"
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
    A_FILE="$SLURM_SUBMIT_DIR/$(echo "$ENTRY" | cut -d'|' -f2)"
    B_FILE="$SLURM_SUBMIT_DIR/$(echo "$ENTRY" | cut -d'|' -f3)"
    DIV=$(echo "$ENTRY" | cut -d'|' -f4)

    [ ! -f "$A_FILE" ] && echo "SKIP $LABEL — file not found" && continue

    ROWS=$(wc -l < "$A_FILE")
    COLS_A=$(head -1 "$A_FILE" | awk '{print NF-1}')
    ROWS_B=$(wc -l < "$B_FILE")
    COLS_B=$(head -1 "$B_FILE" | awk '{print NF}')
    export MATRIX_B_FILE="$B_FILE"

    echo "[$LABEL]  A:${ROWS}x${COLS_A} x B:${ROWS_B}x${COLS_B}  divisible_by_4=$DIV"

    TOTAL_START=$(date +%s%N)
    split -d -a 2 -n l/$SLURM_NTASKS "$A_FILE" chunk_

    # Stage 1: Mapper — distributed, handles empty chunks from uneven splits
    S=$(date +%s%N)
    srun --ntasks=$SLURM_NTASKS bash -c "
        TID=\$(printf '%02d' \$SLURM_PROCID)
        [ -s chunk_\${TID} ] && MATRIX_B_FILE=\"$B_FILE\" python3 mapper.py < chunk_\${TID} > map_\${TID}.out || touch map_\${TID}.out"
    MAPPER=$(echo "scale=6; ($(date +%s%N) - $S) / 1000000000" | bc)

    # Stage 2: Shuffle/Sort 1 — local sort per node
    S=$(date +%s%N)
    srun --ntasks=$SLURM_NTASKS bash -c '
        TID=$(printf "%02d" $SLURM_PROCID); sort -k1,1 map_${TID}.out > shuf1_${TID}.out'
    SHUF1=$(echo "scale=6; ($(date +%s%N) - $S) / 1000000000" | bc)

    # Stage 3: Combiner — distributed
    S=$(date +%s%N)
    srun --ntasks=$SLURM_NTASKS bash -c '
        TID=$(printf "%02d" $SLURM_PROCID); python3 combiner.py < shuf1_${TID}.out > comb_${TID}.out'
    COMB=$(echo "scale=6; ($(date +%s%N) - $S) / 1000000000" | bc)

    # Stage 4: Global sort on master
    S=$(date +%s%N)
    sort -k1,1 comb_*.out > global_shuf2.out
    SHUF2=$(echo "scale=6; ($(date +%s%N) - $S) / 1000000000" | bc)

    # Stage 5: Reducer on master
    S=$(date +%s%N)
    python3 reducer.py < global_shuf2.out > "perf_results/output_${LABEL}.txt"
    RED=$(echo "scale=6; ($(date +%s%N) - $S) / 1000000000" | bc)

    TOTAL=$(echo "scale=6; ($(date +%s%N) - $TOTAL_START) / 1000000000" | bc)
    echo "  mapper=${MAPPER}s  shuf1=${SHUF1}s  comb=${COMB}s  shuf2=${SHUF2}s  red=${RED}s  TOTAL=${TOTAL}s"
    echo ""

    echo "${LABEL},${ROWS}x${COLS_A},${ROWS_B}x${COLS_B},${DIV},${MAPPER},${SHUF1},${COMB},${SHUF2},${RED},${TOTAL}" >> "$SUMMARY"
    rm -f chunk_* map_*.out shuf1_*.out comb_*.out global_shuf2.out
done

echo "============================================"
column -t -s',' "$SUMMARY"
echo "============================================"