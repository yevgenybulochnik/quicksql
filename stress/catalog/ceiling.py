"""Find the stress emulator's ceiling: raise one dimension of the BigQuery seed
at a time (datasets, shards, wide columns), restart the emulator on it, and
time what the catalog asks of it. A dimension stops at its first failure.

    uv run python stress/catalog/ceiling.py            # every dimension
    uv run python stress/catalog/ceiling.py shards     # one
    uv run python stress/catalog/ceiling.py datasets=200,300,400   # chosen steps

Prints a table and writes ``.seed/ceiling.json``; record the result in
README.md. Takes several minutes: the emulator loads ~20 ms per table at
startup. Leaves the emulator on the last seed: ``stack.py up`` restores the
default one.
"""

from __future__ import annotations

import json
import statistics
import subprocess
import sys
import time
import urllib.request
from typing import Any

import stack

BASE = ["--datasets", "10", "--shards", "10", "--wide-columns", "100", "--wide-records", "0"]
STEPS: dict[str, tuple[str, list[int]]] = {
    "datasets": ("--datasets", [150, 500, 1000, 2000]),
    "shards": ("--shards", [400, 1000, 2500, 5000]),
    # BigQuery allows 10,000 columns, nested fields included
    "wide-columns": ("--wide-columns", [1500, 3000, 6000, 9900]),
}
API = f"{stack.ENDPOINT}/bigquery/v2/projects/{stack.PROJECT}"


def get(path: str) -> tuple[float, dict[str, Any]]:
    """Median seconds of three GETs, and the last body."""
    times, body = [], {}
    for _ in range(3):
        start = time.perf_counter()
        with urllib.request.urlopen(f"{API}{path}", timeout=300) as resp:
            body = json.load(resp)
        times.append(time.perf_counter() - start)
    return statistics.median(times), body


def memory() -> str:
    out = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}",
         f"quicksql-{stack.SERVICE}-1"],
        capture_output=True, text=True,
    )
    return out.stdout.strip().split(" / ")[0] or "?"


def step(flag: str, value: int) -> dict[str, Any]:
    args = [*BASE, flag, str(value)]
    if flag == "--wide-columns":
        args += ["--wide-records", "0"]
    row: dict[str, Any] = {"seed_mb": round(stack.seed_bq(args) / 1e6, 2)}
    try:
        row["startup_s"] = round(stack.up_bq(wait=900), 1)
        row["create_wide_s"] = round(stack.create_wide(args), 2)
        secs, body = get("/datasets?maxResults=100000")
        row["list_datasets_ms"], row["datasets"] = round(secs * 1000), len(body.get("datasets", []))
        secs, body = get("/datasets/sharded/tables?maxResults=100000")
        row["list_shards_ms"], row["shards"] = round(secs * 1000), len(body.get("tables", []))
        secs, body = get("/datasets/wide/tables/wide_table")
        row["get_wide_ms"], row["wide_fields"] = round(secs * 1000), len(body["schema"]["fields"])
        row["memory"] = memory()
    except Exception as exc:  # the ceiling: record why, stop this dimension
        row["error"] = f"{type(exc).__name__}: {exc}"[:300]
        row["container"] = died()
    return row


def died() -> str:
    """How the stress emulator's container ended: its exit code, and whether
    the memory cap killed it."""
    out = subprocess.run(
        ["docker", "inspect", "--format", "exit={{.State.ExitCode}} oom={{.State.OOMKilled}}",
         f"quicksql-{stack.SERVICE}-1"],
        capture_output=True, text=True,
    )
    return out.stdout.strip() or "?"


def main(argv: list[str]) -> None:
    """``DIM`` runs that dimension's steps; ``DIM=V1,V2`` runs those values."""
    dims = argv or list(STEPS)
    results: dict[str, list[dict[str, Any]]] = {}
    for spec in dims:
        dim, _, values_given = spec.partition("=")
        flag, values = STEPS[dim]
        if values_given:
            values = [int(v) for v in values_given.split(",")]
        results[dim] = []
        for value in values:
            row = {"value": value, **step(flag, value)}
            results[dim].append(row)
            print(f"{dim}={value}: {row}", flush=True)
            if "error" in row:
                break
    out = stack.SEED.parent / "ceiling.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"wrote {out}")


if __name__ == "__main__":
    main(sys.argv[1:])
