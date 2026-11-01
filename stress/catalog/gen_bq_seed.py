"""The BigQuery stress seed, shaped like a large production project:

* ``ds_000`` … — many datasets, each holding a few small tables;
* ``sharded`` — date-sharded ``events_YYYYMMDD`` tables, as exports produce;
* ``wide.wide_table`` — very wide: STRING columns with descriptions, plus
  REPEATED RECORDs whose fields add one field path each.

Two halves, because the emulator's YAML loader (``--data-from-yaml``) rejects
column descriptions, and descriptions are most of a wide table's bytes:

    uv run python stress/catalog/gen_bq_seed.py              # YAML: datasets + shards
    uv run python stress/catalog/gen_bq_seed.py --wide-json  # the wide table, for the REST API

``stack.py up`` loads the YAML at startup, then creates the wide table through
the API. Deterministic, no data rows: the catalog reads metadata only. The
defaults stay inside the emulator's ceiling (100 datasets, 400 shards,
2050 field paths); ``ceiling.py`` raises them one dimension at a time.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from typing import Any

PROJECT = "quicksql-stress"


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--datasets", type=int, default=100)
    ap.add_argument("--tables-per-dataset", type=int, default=3)
    ap.add_argument("--shards", type=int, default=400)
    ap.add_argument("--wide-columns", type=int, default=1500)
    ap.add_argument("--wide-records", type=int, default=50)
    ap.add_argument("--record-fields", type=int, default=10)
    ap.add_argument("--description-len", type=int, default=200)
    ap.add_argument("--wide-json", action="store_true", help="Print the wide table's REST resource")
    return ap


def small_table(name: str) -> dict[str, Any]:
    return {
        "id": name,
        "columns": [
            {"name": "id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING"},
            {"name": "amount", "type": "NUMERIC"},
            {"name": "created_at", "type": "TIMESTAMP"},
            {"name": "tags", "type": "STRING", "mode": "REPEATED"},
        ],
    }


def seed(args: argparse.Namespace) -> dict[str, Any]:
    """The YAML document: every dataset, every table but the wide one."""
    sets: list[dict[str, Any]] = [
        {"id": f"ds_{d:03d}", "tables": [small_table(f"t_{t:02d}") for t in range(args.tables_per_dataset)]}
        for d in range(args.datasets)
    ]
    start = date(2024, 1, 1)
    shards = [small_table(f"events_{start + timedelta(days=i):%Y%m%d}") for i in range(args.shards)]
    sets.append({"id": "sharded", "tables": shards})
    sets.append({"id": "wide", "tables": []})
    return {"projects": [{"id": PROJECT, "datasets": sets}]}


def wide_fields(args: argparse.Namespace) -> list[dict[str, Any]]:
    desc = ("lorem ipsum dolor sit amet " * (args.description_len // 27 + 1))[: args.description_len]
    fields: list[dict[str, Any]] = [
        {"name": f"col_{i:04d}", "type": "STRING", "mode": "NULLABLE", "description": desc}
        for i in range(args.wide_columns)
    ]
    for r in range(args.wide_records):
        fields.append(
            {
                "name": f"rec_{r:03d}",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [{"name": f"f{j:02d}", "type": "INT64"} for j in range(args.record_fields)],
            }
        )
    return fields


def wide_table(args: argparse.Namespace) -> dict[str, Any]:
    """``wide.wide_table`` as a REST Table resource (tables.insert)."""
    return {
        "tableReference": {"projectId": PROJECT, "datasetId": "wide", "tableId": "wide_table"},
        "schema": {"fields": wide_fields(args)},
    }


def field_paths(args: argparse.Namespace) -> int:
    """How many field-path rows the wide table's catalog leaf has."""
    return args.wide_columns + args.wide_records * (1 + args.record_fields)


def main(argv: list[str] | None = None) -> None:
    args = parser().parse_args(argv)
    if args.wide_json:
        json.dump(wide_table(args), sys.stdout)
        return
    import yaml

    yaml.safe_dump(seed(args), sys.stdout, sort_keys=False, width=120)


if __name__ == "__main__":
    main()
