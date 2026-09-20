#!/usr/bin/env python3
# Combiner — sums partial vectors sharing the same row key locally before global shuffle

import sys

def main():
    current_key = None
    current_sum = None

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        key, vals_str = line.split("\t", 1)
        values = list(map(float, vals_str.split(",")))

        if key == current_key:
            # Accumulate into running sum for this row
            for i in range(len(values)):
                current_sum[i] += values[i]
        else:
            # Flush previous row and start new one
            if current_key is not None:
                print("{}\t{}".format(current_key, ",".join("{:.10g}".format(v) for v in current_sum)))
            current_key = key
            current_sum = values[:]

    if current_key is not None:
        print("{}\t{}".format(current_key, ",".join("{:.10g}".format(v) for v in current_sum)))

if __name__ == "__main__":
    main()