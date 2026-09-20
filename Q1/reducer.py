#!/usr/bin/env python3
# Reducer — final aggregation of globally sorted combiner output; writes rows of C

import sys

def fmt(v):
    # Print as integer if whole number, else 6dp float
    r = round(v)
    return str(r) if abs(v - r) < 1e-9 else "{:.6f}".format(v)

def main():
    results    = {}
    current_key = None
    current_sum = None

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        key, vals_str = line.split("\t", 1)
        values = list(map(float, vals_str.split(",")))

        if key == current_key:
            for i in range(len(values)):
                current_sum[i] += values[i]
        else:
            if current_key is not None:
                results[int(current_key)] = current_sum
            current_key = key
            current_sum = values[:]

    if current_key is not None:
        results[int(current_key)] = current_sum

    # Output rows of C in ascending order
    for row_idx in sorted(results.keys()):
        print(" ".join(fmt(v) for v in results[row_idx]))

if __name__ == "__main__":
    main()