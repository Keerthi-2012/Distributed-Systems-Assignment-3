#!/usr/bin/env python3
# Mapper — Row-Row method: emits (row_idx, a[i,k]*B[k,:]) for every element k in each row of A

import sys
import os

def load_b(filepath):
    # Load matrix B fully into memory — replicated to every mapper node
    matrix = []
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if line:
                matrix.append(list(map(float, line.split())))
    return matrix

def main():
    B = load_b(os.environ.get("MATRIX_B_FILE", "matrix_b.txt"))
    n = len(B)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        row_idx = int(parts[0])   # global row index embedded in input file
        a_row   = list(map(float, parts[1:]))

        # Emit one partial vector per element — key is zero-padded for correct sort order
        for k, a_ik in enumerate(a_row):
            if k >= n:
                break
            partial = [a_ik * b_j for b_j in B[k]]
            print("{:07d}\t{}".format(row_idx, ",".join("{:.10g}".format(v) for v in partial)))

if __name__ == "__main__":
    main()