#!/bin/bash
# verify_q1.sh - correctness check for Q1.
#
# The rule: the MapReduce pipeline must print EXACTLY what HW2's sequential
# program prints for the same input, byte for byte.
#
#   bash scripts/verify_q1.sh            tests + tiny + small
#   bash scripts/verify_q1.sh full       also medium and large
#
# Every input is run with several mapper counts, because the point of the
# exercise is that splitting the input differently must not change the answer.
# The file is split with "split -n l/M", which divides it on line boundaries the
# way Hadoop divides a file into splits; the header line lands in chunk 0, which
# is what happens under Hadoop too.

cd "$(dirname "$0")/.."
ROOT=$(pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
mkdir -p results
LOG=results/verify_q1.txt
: > $LOG

pass=0
fail=0

run_case() {   # run_case <input> <mappers> [extra reducer args]
    local input=$1 mappers=$2 extra=$3
    local name="$(basename "$input") M=$mappers ${extra}"

    ./bin/log_seq "$input" > "$WORK/expected.txt" 2>/dev/null
    if [ -n "$extra" ]; then
        # --k N: compare against log_seq on a copy whose header asks for that K
        local k=${extra#--k }
        { head -1 "$input" | awk -v k="$k" '{print $1, k, $3}'; tail -n +2 "$input"; } > "$WORK/kin.in"
        ./bin/log_seq "$WORK/kin.in" > "$WORK/expected.txt" 2>/dev/null
    fi

    rm -f "$WORK"/chunk_*
    if [ "$mappers" -eq 1 ]; then
        cp "$input" "$WORK/chunk_00"
    else
        split -d -a 2 -n "l/$mappers" "$input" "$WORK/chunk_"
    fi

    # map + local sort + combine, once per mapper, exactly as Hadoop would
    rm -f "$WORK"/comb_*
    for chunk in "$WORK"/chunk_*; do
        ./bin/mapper < "$chunk" 2>/dev/null | sort | ./bin/combiner 2>/dev/null \
            > "$WORK/comb_$(basename "$chunk")"
    done
    # shuffle + reduce
    cat "$WORK"/comb_* | sort | ./bin/reducer $extra 2>/dev/null > "$WORK/actual.txt"

    if diff -q "$WORK/expected.txt" "$WORK/actual.txt" > /dev/null; then
        pass=$((pass + 1))
        echo "PASS  $name" >> $LOG
    else
        fail=$((fail + 1))
        echo "FAIL  $name" >> $LOG
        diff "$WORK/expected.txt" "$WORK/actual.txt" | head -10 >> $LOG
        echo "FAIL  $name"
    fi
}

echo "Q1 MapReduce correctness: pipeline output vs HW2 bin/log_seq" | tee -a $LOG
echo "date: $(date)" >> $LOG
echo >> $LOG

# hand-made inputs: the worked example and the edge cases
for f in tests/*.in; do
    for m in 1 2 3 4; do run_case "$f" $m; done
done

# a different query K, to check K is not hard-coded
run_case tests/sample.in 2 "--k 1"
run_case ../data/tiny.in 3 "--k 3"

# generated datasets
for f in ../data/tiny.in ../data/small.in; do
    [ -f "$f" ] || continue
    for m in 1 2 4 8; do run_case "$f" $m; done
done

if [ "$1" = "full" ]; then
    for f in ../data/medium.in ../data/large.in; do
        [ -f "$f" ] || continue
        for m in 4 16; do run_case "$f" $m; done
    done
fi

echo >> $LOG
echo "passed $pass, failed $fail" | tee -a $LOG
echo "details: $LOG"
[ "$fail" -eq 0 ]
