#!/bin/bash
# run_hadoop.sh - run Q1 as a real Hadoop Streaming job on YARN.
#
#   bash scripts/run_hadoop.sh ../data/medium.in [output_dir]
#
# Requires Hadoop 3.3.6 with HDFS and YARN running, and $HADOOP_HOME set.
# The mapper, combiner and reducer are the C++ executables built by `make q1`;
# -files ships them to every node, and Hadoop pipes the data through their
# stdin/stdout. That is what "Hadoop Streaming" means: the framework is Java,
# the tasks are ordinary programs.
#
# One reducer is used on purpose (see README §5, "Why a single reducer").

set -e
cd "$(dirname "$0")/.."
ROOT=$(pwd)

INPUT=${1:-../data/medium.in}
OUTDIR=${2:-q7_out}
NAME=$(basename "$INPUT" .in)
HDFS_IN=${HDFS_IN:-/user/$USER/q7/$NAME.in}
HDFS_OUT=${HDFS_OUT:-/user/$USER/q7/$OUTDIR}

: "${HADOOP_HOME:?set HADOOP_HOME to the Hadoop 3.3.6 installation}"
STREAM_JAR=$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar | head -1)

for b in bin/mapper bin/combiner bin/reducer; do
    [ -x "$b" ] || { echo "missing $b - run: make q1"; exit 1; }
done

echo "== putting the input into HDFS =="
hdfs dfs -mkdir -p "$(dirname "$HDFS_IN")"
hdfs dfs -test -e "$HDFS_IN" || hdfs dfs -put "$INPUT" "$HDFS_IN"
hdfs dfs -rm -r -f "$HDFS_OUT"

echo "== running the job =="
# mapreduce.job.maps is a hint: the real number of mappers follows the split
# size, so we set the split size to control it (see run_hadoop_sweep below).
START=$(date +%s.%N)
hadoop jar "$STREAM_JAR" \
    -D mapreduce.job.name="q7_log_analytics_${NAME}" \
    -D mapreduce.job.reduces=1 \
    ${SPLIT_SIZE:+-D mapreduce.input.fileinputformat.split.maxsize=$SPLIT_SIZE} \
    -files "$ROOT/bin/mapper,$ROOT/bin/combiner,$ROOT/bin/reducer" \
    -input  "$HDFS_IN" \
    -output "$HDFS_OUT" \
    -mapper   mapper \
    -combiner combiner \
    -reducer  reducer
END=$(date +%s.%N)

echo "== result =="
hdfs dfs -cat "$HDFS_OUT/part-00000"
echo "elapsed $(echo "$END - $START" | bc) s" >&2

# Correctness: the job output must equal the HW2 sequential program's output.
if [ -x bin/log_seq ]; then
    hdfs dfs -cat "$HDFS_OUT/part-00000" > /tmp/q7_hadoop_$$.txt
    ./bin/log_seq "$INPUT" > /tmp/q7_seq_$$.txt
    if diff -q /tmp/q7_seq_$$.txt /tmp/q7_hadoop_$$.txt > /dev/null; then
        echo "CORRECT: identical to bin/log_seq" >&2
    else
        echo "MISMATCH against bin/log_seq" >&2
        diff /tmp/q7_seq_$$.txt /tmp/q7_hadoop_$$.txt | head >&2
    fi
    rm -f /tmp/q7_hadoop_$$.txt /tmp/q7_seq_$$.txt
fi
