#!/bin/bash
# run_q1.sh - run the MapReduce pipeline once, the simple way.
#
#   bash scripts/run_q1.sh [input] [output] [map_tasks]
#
#   bash scripts/run_q1.sh data/small.in                 1 mapper, prints the answer
#   bash scripts/run_q1.sh data/medium.in out.txt 4      4 mappers in parallel
#
# This is the same five stages Hadoop Streaming would run, orchestrated by a
# shell script instead (the Hadoop service on RCE is not usable at the moment):
#
#   split -> mapper (parallel) -> sort -> combiner -> sort -> reducer
#
# With one map task it is just the pipe:
#
#   ./bin/mapper < input | sort | ./bin/combiner | sort | ./bin/reducer
#
# It finishes by checking the answer against HW2's sequential program, so a run
# either prints "SAME as log_seq" or shows the difference.

set -e
cd "$(dirname "$0")/.."

INPUT=${1:-data/small.in}
OUTPUT=${2:-}
TASKS=${3:-1}

[ -f "$INPUT" ] || { echo "no such input: $INPUT (run: bash scripts/make_data.sh)"; exit 1; }
for b in bin/mapper bin/combiner bin/reducer bin/log_seq; do
    [ -x "$b" ] || { echo "missing $b - run: make"; exit 1; }
done

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
RESULT=${OUTPUT:-$WORK/out.txt}

START=$(date +%s.%N)

if [ "$TASKS" -le 1 ]; then
    ./bin/mapper < "$INPUT" | sort | ./bin/combiner | sort | ./bin/reducer > "$RESULT"
else
    # Split on line boundaries, the way Hadoop divides a file into input splits.
    split -d -a 3 -n "l/$TASKS" "$INPUT" "$WORK/chunk_"

    # Map: one process per split, all at once.
    for c in "$WORK"/chunk_[0-9][0-9][0-9]; do
        ( ./bin/mapper < "$c" | sort | ./bin/combiner > "$c.map" ) &
    done
    wait

    # Shuffle: one sort over everything the mappers emitted, then one reducer,
    # because Top-K needs to see every server's total count.
    sort "$WORK"/chunk_[0-9][0-9][0-9].map | ./bin/reducer > "$RESULT"
fi

END=$(date +%s.%N)

./bin/log_seq "$INPUT" > "$WORK/expected.txt"
if diff -q "$WORK/expected.txt" "$RESULT" > /dev/null; then
    echo "RESULT: SAME as log_seq"
else
    echo "RESULT: DIFFERENT"
    diff "$WORK/expected.txt" "$RESULT" | head -10
fi
echo "input $INPUT, $TASKS map task(s), $(echo "$END - $START" | bc) s"
[ -n "$OUTPUT" ] && echo "answer written to $OUTPUT" || cat "$RESULT"
