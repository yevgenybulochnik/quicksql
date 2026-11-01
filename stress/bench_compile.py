"""Benchmark compiling the kitchen-sink notebook in-process: warm up, then
print the median wall time of RUNS compiles in milliseconds."""

import statistics
import time
from pathlib import Path

from quicksql.bootstrap import compile_file  # with the notebook's own registries, as the CLI and TUI compile

NOTEBOOK = Path(__file__).resolve().parent.parent / "demo" / "kitchen-sink" / "kitchen-sink.qsql"
WARMUP, RUNS = 5, 50

if __name__ == "__main__":
    for _ in range(WARMUP):
        compile_file(NOTEBOOK)
    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        compile_file(NOTEBOOK)
        times.append(time.perf_counter() - t0)
    print(f"{NOTEBOOK.name}: median {statistics.median(times) * 1000:.1f} ms over {RUNS} compiles")
