#!/usr/bin/env python3
# Generates matrix A and B input files for all test cases
# Usage: python generate_input.py --preset [example1|example2|edge|full_benchmark]

import argparse, os, random

def write_a(matrix, path):
    # Write A with row index as first column (preserves global order after SLURM split)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        for i, row in enumerate(matrix):
            f.write("{} {}\n".format(i, " ".join(map(str, row))))
    print("  A ({}x{}) -> {}".format(len(matrix), len(matrix[0]), path))

def write_b(matrix, path):
    # Write B as plain rows — loaded entirely by each mapper node
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        for row in matrix:
            f.write(" ".join(map(str, row)) + "\n")
    print("  B ({}x{}) -> {}".format(len(matrix), len(matrix[0]), path))

def rand(rows, cols, lo=-10, hi=10, seed=42):
    rng = random.Random(seed)
    return [[rng.randint(lo, hi) for _ in range(cols)] for _ in range(rows)]

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--preset", choices=["example1","example2","edge","full_benchmark"])
    p.add_argument("--m",  type=int, default=5)
    p.add_argument("--n",  type=int, default=4)
    p.add_argument("--p",  type=int, default=4)
    p.add_argument("--out-dir", default=".")
    args = p.parse_args()
    d = args.out_dir

    if args.preset == "example1":
        # Assignment Example 1 — even split (3 mappers, 3 rows)
        print("Example 1: A(3x2) x B(2x3) => expected C: [[4,3,2],[3,0,-3],[2,-3,-8]]")
        write_a([[1,2],[0,3],[-1,4]],    os.path.join(d, "matrix_a.txt"))
        write_b([[2,3,4],[1,0,-1]],       os.path.join(d, "matrix_b.txt"))

    elif args.preset == "example2":
        # Assignment Example 2 — uneven split (3 mappers, 4 rows)
        print("Example 2: A(4x2) x B(2x2) => expected C: [[1,2],[2,3],[0,3],[1,3]]")
        write_a([[1,0],[2,-1],[0,3],[1,1]], os.path.join(d, "matrix_a.txt"))
        write_b([[1,2],[0,1]],              os.path.join(d, "matrix_b.txt"))

    elif args.preset == "edge":
        # Edge cases: single row (m=1) and single column in A (n=1)
        write_a([[2,-1,3]],       os.path.join(d, "edge_single_row_a.txt"))
        write_b([[1,0],[0,1],[2,-1]], os.path.join(d, "edge_single_row_b.txt"))
        write_a([[2],[-1],[3]],   os.path.join(d, "edge_single_col_a.txt"))
        write_b([[4,5,6]],        os.path.join(d, "edge_single_col_b.txt"))

    elif args.preset == "full_benchmark":
        # All benchmark shapes: baseline + square divisible + square uneven + tall + wide
        d = d if d != "." else "test_data"
        cases = [
            # (label,              m,    n,   p,    seed_a, seed_b)
            ("p1_small",           100,  50,  50,   1, 10),
            ("p1_medium",          500,  50,  50,   2, 10),
            ("p1_large",           2000, 50,  50,   3, 10),
            ("p1_square",          1000, 100, 1000, 4, 20),  # divisible by 4
            ("p1_square_uneven",   1001, 100, 1000, 7, 20),  # NOT divisible by 4
            ("p1_tall",            5000, 10,  10,   5, 30),
            ("p1_wide",            10,   500, 10,   6, 40),
        ]
        for label, m, n, pp, sa, sb in cases:
            print("{}  A({}x{}) x B({}x{}):".format(label, m, n, n, pp))
            write_a(rand(m, n, seed=sa), os.path.join(d, "{}_a.txt".format(label)))
            # square_uneven reuses p1_square_b.txt (same B dimensions)
            if label != "p1_square_uneven":
                write_b(rand(n, pp, seed=sb), os.path.join(d, "{}_b.txt".format(label)))
    else:
        # Custom random matrix
        write_a(rand(args.m, args.n, seed=42), os.path.join(d, "matrix_a.txt"))
        write_b(rand(args.n, args.p,  seed=43), os.path.join(d, "matrix_b.txt"))

    print("Done.")

if __name__ == "__main__":
    main()