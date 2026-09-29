#!/bin/bash
#SBATCH --job-name=mapreduce_test
#SBATCH --output=results/test_results_%j.out
#SBATCH --error=results/test_results_%j.err
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1

# Slurm writes the two files above relative to the directory you SUBMIT from,
# not to this script's location, and it cannot expand variables in #SBATCH
# lines. So submit from the question folder:
#
#     cd ~/HW3/Section1_Q1 && sbatch scripts/run_cluster_test.sh
#
# and the log lands in results/ beside the results it describes. Submitting
# from ~/HW3 instead used to scatter test_results_*.out across the home
# directory.
#SBATCH --time=00:10:00

# Correctness test — ALL cases run distributed across 4 nodes using srun
# Covers: even split, uneven split, edge cases (m=1, n=1), large scale, tall, wide

# Find the question folder.
#
# Under sbatch, $0 is Slurm's own COPY of this script in /var/spool/slurmd, not
# the path you submitted, so deriving the folder from $0 alone lands in an
# unwritable system directory - that is exactly how job 99933 failed with
# "mkdir: cannot create directory /var/spool/slurmd/results: Permission denied".
#
# $SLURM_SUBMIT_DIR is the directory you submitted FROM, which is the right
# answer when you submit from the question folder as intended. The two fallbacks
# cover submitting from ~/HW3 by mistake, and running the script directly with
# bash outside Slurm. If none of them contains the code, stop and say so rather
# than running on and reporting nine failures.
QDIR=${SLURM_SUBMIT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}
[ -f "$QDIR/mapper.py" ] || QDIR="$QDIR/Section1_Q1"
[ -f "$QDIR/mapper.py" ] || QDIR=$(cd "$(dirname "$0")/.." && pwd)
if [ ! -f "$QDIR/mapper.py" ]; then
    echo "ERROR: cannot find mapper.py. Submit from the question folder:" >&2
    echo "    cd ~/HW3/Section1_Q1 && sbatch scripts/$(basename "$0")" >&2
    exit 1
fi
cd "$QDIR"
mkdir -p results

# Intermediate files go in a per-job scratch directory, not in the question
# folder. They used to be written beside mapper.py, so a run left chunk_*,
# map_*.out, shuf1_*.out and comb_*.out lying next to the source, and two jobs
# running at once would overwrite each other's. Home is shared by every compute
# node, so the scratch directory is visible from all of them.
WORK=$QDIR/results/work_${SLURM_JOB_ID:-$$}
mkdir -p "$WORK"

# EXPORT them. The srun stages below are written with single quotes so that
# $SLURM_PROCID is expanded on each compute node rather than here - but that
# also stops $WORK and $QDIR being expanded, and without exporting them the
# remote shell sees empty strings. That is how job 99945 failed, trying to
# write /shuf1_00.out at the filesystem root:
#     /usr/bin/bash: line 2: /shuf1_00.out: Permission denied
export QDIR WORK
trap 'rm -rf "$WORK"' EXIT

PASS=0; FAIL=0

run_test() {
    # Args: label A_file B_file output_file
    local LABEL="$1" A_FILE="$2" B_FILE="$3" OUT="$4"
    # The mapper is launched on other nodes, so B must be given as an absolute
    # path. Some callers already pass one (the generated examples live in the
    # scratch directory), others pass a path relative to the question folder.
    local B_ABS
    case "$B_FILE" in
        /*) B_ABS="$B_FILE" ;;
        *)  B_ABS="$QDIR/$B_FILE" ;;
    esac
    echo "--- $LABEL ---"

    # Split A rows across 4 nodes (handles uneven: some nodes get fewer/zero rows)
    split -d -a 2 -n l/$SLURM_NTASKS "$A_FILE" "$WORK/chunk_"

    # Stage 1: Mapper — distributed across all 4 nodes
    srun --ntasks=$SLURM_NTASKS bash -c "
        TID=\$(printf '%02d' \$SLURM_PROCID)
        [ -s $WORK/chunk_\${TID} ] && MATRIX_B_FILE=\"$B_ABS\" python3 $QDIR/mapper.py < $WORK/chunk_\${TID} > $WORK/map_\${TID}.out || touch $WORK/map_\${TID}.out"

    # Stage 2: Shuffle/Sort 1 — local sort per node
    srun --ntasks=$SLURM_NTASKS bash -c '
        TID=$(printf "%02d" $SLURM_PROCID)
        sort -k1,1 $WORK/map_${TID}.out > $WORK/shuf1_${TID}.out'

    # Stage 3: Combiner — distributed
    srun --ntasks=$SLURM_NTASKS bash -c '
        TID=$(printf "%02d" $SLURM_PROCID)
        python3 $QDIR/combiner.py < $WORK/shuf1_${TID}.out > $WORK/comb_${TID}.out'

    # Stage 4: Global sort on master
    sort -k1,1 "$WORK"/comb_*.out > "$WORK/global_shuf2.out"

    # Stage 5: Reducer on master
    python3 reducer.py < "$WORK/global_shuf2.out" > "$OUT"

    echo "  Output: $(cat $OUT | tr '\n' '|' | sed 's/|$//')"

    # Verify against direct matrix multiply
    RESULT=$(python3 verify.py "$A_FILE" "$B_FILE" "$OUT")
    echo "  $RESULT"
    if echo "$RESULT" | grep -q "PASSED"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi
    echo ""

    rm -f "$WORK"/chunk_* "$WORK"/map_*.out "$WORK"/shuf1_*.out "$WORK"/comb_*.out "$WORK/global_shuf2.out"
}

echo "Node: $SLURMD_NODENAME | Tasks: $SLURM_NTASKS | Date: $(date)"
echo ""

# ── Assignment examples ────────────────────────────────────────────────────────
python3 generate_input.py --preset example1 --out-dir "$WORK"
run_test "Example 1 — even split     A(3x2) x B(2x3) | 3 rows / 4 tasks (uneven)" \
    "$WORK/matrix_a.txt" "$WORK/matrix_b.txt" "$WORK/out_ex1.txt"

python3 generate_input.py --preset example2 --out-dir "$WORK"
run_test "Example 2 — uneven split   A(4x2) x B(2x2) | 4 rows / 4 tasks (even)" \
    "$WORK/matrix_a.txt" "$WORK/matrix_b.txt" "$WORK/out_ex2.txt"

# ── Edge cases ─────────────────────────────────────────────────────────────────
python3 generate_input.py --preset edge --out-dir "$WORK"
run_test "Edge — single row  m=1     A(1x3) x B(3x2) | 1 row  / 4 tasks (uneven)" \
    "$WORK/edge_single_row_a.txt" "$WORK/edge_single_row_b.txt" "$WORK/out_edge_row.txt"

run_test "Edge — single col  n=1     A(3x1) x B(1x3) | 3 rows / 4 tasks (uneven)" \
    "$WORK/edge_single_col_a.txt" "$WORK/edge_single_col_b.txt" "$WORK/out_edge_col.txt"

# ── Scale tests (uses benchmark test_data/) ────────────────────────────────────
run_test "Scale — small (divisible)   A(100x50)  x B(50x50)    | 100 rows / 4 tasks" \
    test_data/p1_small_a.txt test_data/p1_small_b.txt "$WORK/out_small.txt"

run_test "Scale — square divisible    A(1000x100) x B(100x1000) | 1000 rows / 4 tasks" \
    test_data/p1_square_a.txt test_data/p1_square_b.txt "$WORK/out_square.txt"

run_test "Scale — square NOT divisible A(1001x100) x B(100x1000)| 1001 rows / 4 tasks" \
    test_data/p1_square_uneven_a.txt test_data/p1_square_b.txt "$WORK/out_square_uneven.txt"

run_test "Scale — tall  m>>n          A(5000x10) x B(10x10)     | 5000 rows / 4 tasks" \
    test_data/p1_tall_a.txt test_data/p1_tall_b.txt "$WORK/out_tall.txt"

run_test "Scale — wide  n>>m          A(10x500) x B(500x10)     | 10 rows  / 4 tasks" \
    test_data/p1_wide_a.txt test_data/p1_wide_b.txt "$WORK/out_wide.txt"

# ── Summary ────────────────────────────────────────────────────────────────────
echo "============================================"
echo "Results: $PASS PASSED / $FAIL FAILED"
echo "============================================"