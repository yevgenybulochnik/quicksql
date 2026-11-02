"""Compare a stress run (report.json) with the Phase 0 baseline (baseline.json):
round trips, local wall time and response bytes per scenario and engine.

    uv run python stress/catalog/compare.py [REPORT] [BASELINE]

Exit 1 when any scenario now takes more round trips than the baseline did.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def load(path: Path) -> dict[tuple[str, str], dict]:
    return {(r["scenario"], r["engine"]): r for r in json.loads(path.read_text())["records"]}


def cell(before, after, unit: str = "") -> str:
    if before is None and after is None:
        return ""
    if before == after or before is None or after is None:
        return f"{after if after is not None else '—'}{unit}"
    return f"{before}{unit} → {after}{unit}"


def main(argv: list[str]) -> int:
    report = Path(argv[0]) if argv else HERE / "report.json"
    baseline = Path(argv[1]) if len(argv) > 1 else HERE / "baseline.json"
    now, base = load(report), load(baseline)
    worse = []
    print(f"{'scenario':8} {'engine':24} {'round trips':>18} {'local ms':>20} {'bytes':>22}")
    for key in sorted(set(now) | set(base), key=lambda k: (int("".join(c for c in k[0] if c.isdigit())), k)):
        b, n = base.get(key, {}), now.get(key, {})
        trips_b, trips_n = b.get("round_trips"), n.get("round_trips")
        if trips_b is not None and trips_n is not None and trips_n > trips_b:
            worse.append(key)
        print(f"{key[0]:8} {key[1]:24} {cell(trips_b, trips_n):>18} "
              f"{cell(b.get('local_ms'), n.get('local_ms')):>20} {cell(b.get('bytes'), n.get('bytes')):>22}"
              + ("" if n else "   (not run)"))
    if worse:
        print(f"\nmore round trips than the baseline: {', '.join(' '.join(k) for k in worse)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
