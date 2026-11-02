"""Stand up the catalog stress backends: the stress BigQuery emulator (:9052,
seeded from a generated YAML at startup) and the stress schemas in the compose
Postgres's ``quicksql`` database.

    uv run python stress/catalog/stack.py up            # both, default sizes
    uv run python stress/catalog/stack.py up --shards 2000
    uv run python stress/catalog/stack.py seed-pg       # Postgres only
    uv run python stress/catalog/stack.py down          # stop the stress emulator

``up`` recreates the emulator, so it always reloads the seed: that is also the
only safe way to clean it up. Extra arguments go to ``gen_bq_seed.py``.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
SEED = HERE / ".seed" / "bq-seed.yaml"
SERVICE = "bigquery-emulator-stress"
ENDPOINT = "http://localhost:9052"
PROJECT = "quicksql-stress"


def compose(*args: str, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True, **kw)


def seed_bq(gen_args: list[str]) -> int:
    """Write the YAML seed; returns its size in bytes."""
    SEED.parent.mkdir(parents=True, exist_ok=True)
    with SEED.open("w") as out:
        subprocess.run(
            [sys.executable, str(HERE / "gen_bq_seed.py"), *gen_args], stdout=out, check=True
        )
    return SEED.stat().st_size


def create_wide(gen_args: list[str]) -> float:
    """Create ``wide.wide_table`` through the REST API (the YAML loader drops
    descriptions); returns seconds."""
    body = subprocess.run(
        [sys.executable, str(HERE / "gen_bq_seed.py"), *gen_args, "--wide-json"],
        capture_output=True, check=True,
    ).stdout
    url = f"{ENDPOINT}/bigquery/v2/projects/{PROJECT}/datasets/wide/tables"
    request = urllib.request.Request(
        url, data=body, method="POST", headers={"Content-Type": "application/json"}
    )
    start = time.perf_counter()
    with urllib.request.urlopen(request, timeout=300) as resp:
        resp.read()
    return time.perf_counter() - start


def datasets_answering(timeout: float = 2.0) -> int | None:
    """How many datasets the stress emulator lists, or None while it doesn't answer."""
    url = f"{ENDPOINT}/bigquery/v2/projects/{PROJECT}/datasets?maxResults=100000"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return len(json.load(resp).get("datasets", []))
    except (urllib.error.URLError, OSError, ValueError):
        return None


def running() -> bool:
    out = compose("--profile", "stress", "ps", "-q", SERVICE, capture_output=True, text=True)
    return bool(out.stdout.strip())


def up_bq(wait: float = 600.0) -> float:
    """Recreate the stress emulator on the current seed; returns seconds until
    it answers. Raises if it dies or stays silent past ``wait``."""
    compose("--profile", "stress", "up", "-d", "--force-recreate", SERVICE,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    start = time.perf_counter()
    while time.perf_counter() - start < wait:
        if datasets_answering() is not None:
            return time.perf_counter() - start
        if not running():
            raise RuntimeError(f"{SERVICE} exited while loading the seed (docker compose logs {SERVICE})")
        time.sleep(0.5)
    raise TimeoutError(f"{SERVICE} didn't answer within {wait:.0f} s")


def seed_pg() -> float:
    """Load the Postgres stress schemas into ``quicksql``; returns seconds."""
    sql = subprocess.run(
        [sys.executable, str(HERE / "gen_pg_seed.py")], capture_output=True, text=True, check=True
    ).stdout
    start = time.perf_counter()
    compose("exec", "-T", "postgres", "psql", "-q", "-v", "ON_ERROR_STOP=1",
            "-U", "quicksql", "-d", "quicksql", input=sql, text=True, stdout=subprocess.DEVNULL)
    return time.perf_counter() - start


def main(argv: list[str]) -> None:
    if not argv or argv[0] not in ("up", "seed-bq", "seed-pg", "down"):
        sys.exit(__doc__)
    cmd, rest = argv[0], argv[1:]
    if cmd in ("up", "seed-bq"):
        size = seed_bq(rest)
        print(f"bq seed: {SEED.relative_to(ROOT)} ({size / 1e6:.1f} MB)")
    if cmd == "up":
        secs = up_bq()
        print(f"bq emulator: {datasets_answering()} datasets at {ENDPOINT}, answering after {secs:.1f} s")
        print(f"bq wide table: created in {create_wide(rest):.1f} s")
    if cmd in ("up", "seed-pg"):
        print(f"pg stress schemas: loaded in {seed_pg():.1f} s")
    if cmd == "down":
        compose("--profile", "stress", "stop", SERVICE)


if __name__ == "__main__":
    main(sys.argv[1:])
