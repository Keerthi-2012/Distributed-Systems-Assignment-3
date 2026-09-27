#!/usr/bin/env python3
# Verifies MapReduce output against direct matrix multiplication
# Usage: python verify.py matrix_a.txt matrix_b.txt output.txt

import sys, os

def load_a(path):
    # A file has row index as first column
    rows = {}
    with open(path) as f:
        for line in f:
            p = list(map(float, line.split()))
            rows[int(p[0])] = p[1:]
    m = max(rows) + 1
    return [[rows[i][j] for j in range(len(rows[0]))] for i in range(m)]

def load_b(path):
    with open(path) as f:
        return [list(map(float, l.split())) for l in f if l.strip()]

def load_out(path):
    with open(path) as f:
        return [list(map(float, l.split())) for l in f if l.strip()]

def matmul(A, B):
    m, n, pp = len(A), len(A[0]), len(B[0])
    C = [[0.0]*pp for _ in range(m)]
    for i in range(m):
        for k in range(n):
            for j in range(pp):
                C[i][j] += A[i][k] * B[k][j]
    return C

def main():
    a_file  = sys.argv[1] if len(sys.argv) > 1 else "matrix_a.txt"
    b_file  = sys.argv[2] if len(sys.argv) > 2 else "matrix_b.txt"
    out_file = sys.argv[3] if len(sys.argv) > 3 else "output.txt"

    A, B, out = load_a(a_file), load_b(b_file), load_out(out_file)
    expected  = matmul(A, B)

    ok = True
    for i in range(len(expected)):
        for j in range(len(expected[0])):
            if abs(expected[i][j] - out[i][j]) > 1e-6:
                print("MISMATCH C[{}][{}]: expected {} got {}".format(
                    i, j, expected[i][j], out[i][j]))
                ok = False

    print("PASSED" if ok else "FAILED")

if __name__ == "__main__":
    main()