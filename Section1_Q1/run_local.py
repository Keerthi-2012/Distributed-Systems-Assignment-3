#!/usr/bin/env python3
# Local MapReduce pipeline runner — works on Windows, Mac, Linux
# Usage: python run_local.py [matrix_a.txt] [matrix_b.txt] [output.txt]

import sys, os, subprocess, time

A_FILE = sys.argv[1] if len(sys.argv) > 1 else "matrix_a.txt"
B_FILE = sys.argv[2] if len(sys.argv) > 2 else "matrix_b.txt"
OUTPUT = sys.argv[3] if len(sys.argv) > 3 else "output.txt"
PY     = sys.executable

# Build env with MATRIX_B_FILE set — passed to every subprocess call
env = os.environ.copy()
env["MATRIX_B_FILE"] = B_FILE

def run(cmd, stdin=None):
    # env is explicitly passed so MATRIX_B_FILE reaches mapper.py
    return subprocess.run(
        cmd,
        input=stdin,
        stdout=subprocess.PIPE,
        env=env
    ).stdout

def sort_bytes(data):
    lines = [l for l in data.split(b"\n") if l.strip()]
    return b"\n".join(sorted(lines)) + b"\n"

print("A={} B={} -> {}".format(A_FILE, B_FILE, OUTPUT))
with open(A_FILE, "rb") as f:
    a = f.read()

t0 = time.perf_counter()
mapped   = run([PY, "mapper.py"],   stdin=a)        # Stage 1: Mapper
shuf1    = sort_bytes(mapped)                        # Stage 2: Shuffle/Sort 1
combined = run([PY, "combiner.py"], stdin=shuf1)    # Stage 3: Combiner
shuf2    = sort_bytes(combined)                      # Stage 4: Shuffle/Sort 2
result   = run([PY, "reducer.py"],  stdin=shuf2)    # Stage 5: Reducer
total    = time.perf_counter() - t0

with open(OUTPUT, "wb") as f:
    f.write(result)

print("Output:\n" + result.decode().strip())
print("Total: {:.3f}s".format(total))

if os.path.exists("verify.py"):
    subprocess.run([PY, "verify.py", A_FILE, B_FILE, OUTPUT], env=env)