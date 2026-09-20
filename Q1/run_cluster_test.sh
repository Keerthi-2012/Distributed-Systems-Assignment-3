#!/bin/bash
#SBATCH --job-name=mapreduce_test
#SBATCH --output=test_results_%j.out
#SBATCH --error=test_results_%j.err
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --time=00:10:00

# Correctness test — ALL cases run distributed across 4 nodes using srun
# Covers: even split, uneven split, edge cases (m=1, n=1), large scale, tall, wide

cd "$SLURM_SUBMIT_DIR"

PASS=0; FAIL=0

run_test() {
    # Args: label A_file B_file output_file
    local LABEL="$1" A_FILE="$2" B_FILE="$3" OUT="$4"
    local B_ABS="$SLURM_SUBMIT_DIR/$B_FILE"
    echo "--- $LABEL ---"

    # Split A rows across 4 nodes (handles uneven: some nodes get fewer/zero rows)
    split -d -a 2 -n l/$SLURM_NTASKS "$A_FILE" chunk_

    # Stage 1: Mapper — distributed across all 4 nodes
    srun --ntasks=$SLURM_NTASKS bash -c "
        TID=\$(printf '%02d' \$SLURM_PROCID)
        [ -s chunk_\${TID} ] && MATRIX_B_FILE=\"$B_ABS\" python3 mapper.py < chunk_\${TID} > map_\${TID}.out || touch map_\${TID}.out"

    # Stage 2: Shuffle/Sort 1 — local sort per node
    srun --ntasks=$SLURM_NTASKS bash -c '
        TID=$(printf "%02d" $SLURM_PROCID)
        sort -k1,1 map_${TID}.out > shuf1_${TID}.out'

    # Stage 3: Combiner — distributed
    srun --ntasks=$SLURM_NTASKS bash -c '
        TID=$(printf "%02d" $SLURM_PROCID)
        python3 combiner.py < shuf1_${TID}.out > comb_${TID}.out'

    # Stage 4: Global sort on master
    sort -k1,1 comb_*.out > global_shuf2.out

    # Stage 5: Reducer on master
    python3 reducer.py < global_shuf2.out > "$OUT"

    echo "  Output: $(cat $OUT | tr '\n' '|' | sed 's/|$//')"

    # Verify against direct matrix multiply
    RESULT=$(python3 verify.py "$A_FILE" "$B_FILE" "$OUT")
    echo "  $RESULT"
    if echo "$RESULT" | grep -q "PASSED"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi
    echo ""

    rm -f chunk_* map_*.out shuf1_*.out comb_*.out global_shuf2.out
}

echo "Node: $SLURMD_NODENAME | Tasks: $SLURM_NTASKS | Date: $(date)"
echo ""

# ── Assignment examples ────────────────────────────────────────────────────────
python3 generate_input.py --preset example1 --out-dir .
run_test "Example 1 — even split     A(3x2) x B(2x3) | 3 rows / 4 tasks (uneven)" \
    matrix_a.txt matrix_b.txt out_ex1.txt

python3 generate_input.py --preset example2 --out-dir .
run_test "Example 2 — uneven split   A(4x2) x B(2x2) | 4 rows / 4 tasks (even)" \
    matrix_a.txt matrix_b.txt out_ex2.txt

# ── Edge cases ─────────────────────────────────────────────────────────────────
python3 generate_input.py --preset edge --out-dir .
run_test "Edge — single row  m=1     A(1x3) x B(3x2) | 1 row  / 4 tasks (uneven)" \
    edge_single_row_a.txt edge_single_row_b.txt out_edge_row.txt

run_test "Edge — single col  n=1     A(3x1) x B(1x3) | 3 rows / 4 tasks (uneven)" \
    edge_single_col_a.txt edge_single_col_b.txt out_edge_col.txt

# ── Scale tests (uses benchmark test_data/) ────────────────────────────────────
run_test "Scale — small (divisible)   A(100x50)  x B(50x50)    | 100 rows / 4 tasks" \
    test_data/p1_small_a.txt test_data/p1_small_b.txt out_small.txt

run_test "Scale — square divisible    A(1000x100) x B(100x1000) | 1000 rows / 4 tasks" \
    test_data/p1_square_a.txt test_data/p1_square_b.txt out_square.txt

run_test "Scale — square NOT divisible A(1001x100) x B(100x1000)| 1001 rows / 4 tasks" \
    test_data/p1_square_uneven_a.txt test_data/p1_square_b.txt out_square_uneven.txt

run_test "Scale — tall  m>>n          A(5000x10) x B(10x10)     | 5000 rows / 4 tasks" \
    test_data/p1_tall_a.txt test_data/p1_tall_b.txt out_tall.txt

run_test "Scale — wide  n>>m          A(10x500) x B(500x10)     | 10 rows  / 4 tasks" \
    test_data/p1_wide_a.txt test_data/p1_wide_b.txt out_wide.txt

# ── Summary ────────────────────────────────────────────────────────────────────
echo "============================================"
echo "Results: $PASS PASSED / $FAIL FAILED"
echo "============================================"