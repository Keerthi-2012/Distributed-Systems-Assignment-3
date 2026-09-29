#!/bin/bash
#SBATCH --job-name=mapreduce_benchmark
#SBATCH --output=results/benchmark_dist_results_%j.out
#SBATCH --error=results/benchmark_dist_results_%j.err
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1

# Slurm writes the two files above relative to the directory you SUBMIT from,
# not to this script's location, and it cannot expand variables in #SBATCH
# lines. So submit from the question folder:
#
#     cd ~/HW3/Section1_Q1 && sbatch scripts/benchmark_dist_slurm.sh
#
# and the log lands in results/ beside the results it describes. Submitting
# from ~/HW3 instead used to scatter test_results_*.out across the home
# directory.
#SBATCH --time=00:30:00

# Distributed benchmark — 7 shapes across 4 nodes
# Covers: baseline, square divisible, square NOT divisible, tall (m>>n), wide (n>>m)

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
mkdir -p results
SUMMARY="results/dist_benchmark_summary.csv"
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
    python3 reducer.py < "$WORK/global_shuf2.out" > "results/output_${LABEL}.txt"
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