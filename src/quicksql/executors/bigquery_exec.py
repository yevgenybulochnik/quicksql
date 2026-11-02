"""BigQuery executor: extracts to Arrow, which DuckDB registers and lands anywhere.

Needs the ``bigquery`` extra (google-cloud-bigquery, pyarrow).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from ..errors import ExecutorError
from ..models import CatalogNode, RunContext
from ..registry import executor
from .base import (
    EngineSession,
    Executor,
    SessionLost,
    field_path_frame,
    strip_trailing_semicolon,
)


def _expired(exc: Exception) -> bool:
    """BigQuery's words for a session past its limits (24 hours idle, 7 days
    old): "Session <id> has expired and is no longer available"."""
    message = str(exc)
    return "Session" in message and (
        "has expired" in message or "is no longer available" in message
    )


def bq_field_paths(fields: Iterable[Any]) -> Any:
    """A table's schema as a field-path frame, from BigQuery SchemaFields
    (.name/.field_type/.mode/.fields/.description), depth-first."""
    rows: list[tuple[str, str, str, str, str]] = []

    def walk(field: Any, column: str, prefix: str) -> None:
        path = f"{prefix}.{field.name}" if prefix else field.name
        rows.append((column, path, field.field_type, field.mode or "", field.description or ""))
        for sub in field.fields or ():
            walk(sub, column, path)

    for field in fields:
        walk(field, field.name, "")
    return field_path_frame(rows)


class BigQuerySession(EngineSession):
    """One client per context. The BigQuery session that holds temp tables
    opens with the first scoped statement; unscoped ones run as plain jobs.
    Sessions expire server-side, so close() has nothing to release."""

    def __init__(self, client: Any, query_opts: dict[str, Any]) -> None:
        self.client = client
        self.query_opts = query_opts
        self.session_id: str | None = None

    def execute(self, sql: str, *, scoped: bool = True) -> None:
        if scoped:
            self._query(sql)
        else:
            self.client.query(sql, **self.query_opts).result()

    def fetch(self, sql: str, *, scoped: bool = True) -> Any:
        if scoped:
            return self._query(sql).to_arrow()
        return self.client.query(sql, **self.query_opts).to_arrow()

    def materialize(self, name: str, sql: str) -> str:
        # the body goes in bare: wrapped in parens, a trailing line comment
        # would swallow the closing one
        self._query(f"CREATE OR REPLACE TEMP TABLE {name} AS {sql}")
        return f"SELECT * FROM {name}"

    def drop(self, name: str) -> None:
        if self.session_id is not None:  # no session, no temps
            self._query(f"DROP TABLE IF EXISTS {name}")

    def _job_config(self) -> Any:
        """Split out so offline tests avoid the google import."""
        from google.cloud import bigquery

        if self.session_id is None:
            return bigquery.QueryJobConfig(create_session=True)
        return bigquery.QueryJobConfig(
            connection_properties=[bigquery.ConnectionProperty("session_id", self.session_id)]
        )

    def _query(self, sql: str) -> Any:
        try:
            job = self.client.query(sql, job_config=self._job_config(), **self.query_opts)
            # query() only *submits*; sessions allow one active job at a time, so
            # wait for completion before the next statement goes in
            job.result()
        except Exception as exc:
            if self.session_id is not None and _expired(exc):
                raise SessionLost(str(exc)) from exc
            raise
        if self.session_id is None:
            info = getattr(job, "session_info", None)
            self.session_id = getattr(info, "session_id", None)
        return job


@executor("bigquery")
class BigQueryExecutor(Executor):
    dialect = "bigquery"
    supports_context_refs = True
    # bigquery runs multi-statement scripts server-side in one job; splitting
    # client-side would run the pieces as separate jobs and lose session state,
    # and a script can't be the body of CREATE TEMP TABLE AS.
    supports_multistatement_materialization = False
    input_shorthand = "project"
    # listings ask for full pages: the API's own default page is smaller, and
    # each page is a round trip
    page_size = 1000
    catalog_persists = True

    def split_statements(self, sql: str) -> list[str]:
        return [strip_trailing_semicolon(sql)]

    def context_key(self, config: Any) -> str:
        return f"bigquery:{self.input_spec(config).get('project')}"

    def make_client(self, spec: dict[str, Any]) -> Any:
        """Split out so tests can substitute a fake client."""
        try:
            from google.cloud import bigquery
        except ImportError as exc:
            raise ExecutorError(
                "bigquery engine needs the 'bigquery' extra: uv sync --extra bigquery"
            ) from exc
        endpoint = spec.get("endpoint")
        if endpoint:
            # a local emulator (e.g. goccy/bigquery-emulator): anonymous
            # credentials, or the client goes hunting for ADC and dies offline
            from google.api_core.client_options import ClientOptions
            from google.auth.credentials import AnonymousCredentials

            return bigquery.Client(
                project=spec.get("project"),
                client_options=ClientOptions(api_endpoint=endpoint),
                credentials=AnonymousCredentials(),
            )
        return bigquery.Client(project=spec.get("project"))

    def _query_opts(self, spec: dict[str, Any]) -> dict[str, Any]:
        """Fail fast against a local emulator: goccy reports every execution
        failure with a retryable-looking reason (jobInternalError), so the
        client's default 600s job retry re-submits a failing query for ten
        minutes — and a down emulator blocks the same way via the API retry.
        Real BigQuery (no endpoint) keeps the client defaults."""
        if not spec.get("endpoint"):
            return {}
        return {"retry": None, "job_retry": None}

    def open_session(self, config: Any, ctx: RunContext) -> EngineSession:
        spec = self.input_spec(config)
        return BigQuerySession(self.make_client(spec), self._query_opts(spec))

    def catalog(self, config: Any, root: Path) -> CatalogNode:
        return self.context_node(self.input_spec(config))

    def catalog_target(self, target: str, root: Path) -> tuple[str, CatalogNode]:
        """``project``, or ``project.dataset`` to start at that dataset's tables."""
        project, _, dataset = target.partition(".")
        if dataset:
            return f"bigquery:{project}/{dataset}", self.context_node(
                {"project": project}, dataset=dataset
            )
        return f"bigquery:{project}", self.context_node({"project": project})

    def catalog_table(self, config: Any, root: Path, parts: list[str]) -> CatalogNode | None:
        """``table`` in the context's dataset, ``dataset.table``, or
        ``project.dataset.table``: that project, the endpoint carrying over."""
        spec = self.input_spec(config)
        if len(parts) >= 3:
            spec = {**spec, "project": parts[-3]}
        dataset = parts[-2] if len(parts) >= 2 else spec.get("dataset")
        if not dataset:
            return None
        tables = self.context_node(spec, dataset=dataset)
        return tables.child({"table": parts[-1]}) if tables.child else None

    def catalog_relations(self, config: Any, root: Path, parts: list[str]) -> list[tuple[str, str]]:
        """With no typed segments, the context's datasets; with one, that
        dataset's tables — falling back to the *project* of that name's datasets
        when it isn't a dataset; with two (project.dataset.), that project's
        dataset's tables."""
        spec = self.input_spec(config)

        def datasets(s: dict[str, Any]) -> list[tuple[str, str]]:
            frame = self.context_node(s).load()
            return [(d, "dataset") for d in frame["dataset"].to_list()]

        def tables(s: dict[str, Any], ds: str) -> list[tuple[str, str]]:
            frame = self.context_node(s, dataset=ds).load()
            return [(row["table"], row["type"] or "table") for row in frame.iter_rows(named=True)]

        if len(parts) == 0:
            return datasets(spec)
        if len(parts) == 2:
            return tables({**spec, "project": parts[0]}, parts[1])
        if len(parts) > 2:
            return []
        try:
            return tables(spec, parts[0])
        except Exception:
            return datasets({**spec, "project": parts[0]})

    def context_node(self, spec: dict[str, Any], dataset: str | None = None) -> CatalogNode:
        """datasets -> tables -> field paths, on one client memoized per chain.
        With ``dataset``, the chain starts at that dataset's tables instead."""
        import polars as pl

        holder: dict[str, Any] = {}

        def client() -> Any:
            if "client" not in holder:
                holder["client"] = self.make_client(spec)
            return holder["client"]

        title = f"bigquery:{spec.get('project')}"
        # the key names the endpoint too: two emulators can hold a project of
        # the same name, and the disk keeps levels across sessions
        endpoint = spec.get("endpoint")
        base = f"{title}@{endpoint}" if endpoint else title

        def dataset_node(ds: str) -> CatalogNode:
            def tables_load() -> pl.DataFrame:
                tables = list(client().list_tables(ds, page_size=self.page_size))
                return pl.DataFrame(
                    {
                        "table": [t.table_id for t in tables],
                        "type": [getattr(t, "table_type", None) or "" for t in tables],
                    },
                    schema={"table": pl.Utf8, "type": pl.Utf8},
                )

            def tables_child(trow: dict[str, Any]) -> CatalogNode:
                table = trow["table"]
                return CatalogNode(
                    title=f"{ds}.{table}",
                    load=lambda: bq_field_paths(client().get_table(f"{ds}.{table}").schema),
                    key=f"{base}/{ds}.{table}",
                )

            return CatalogNode(
                title=f"{title}/{ds}",
                load=tables_load,
                child=tables_child,
                key=f"{base}/{ds}",
                name_columns=("table",),
            )

        if dataset is not None:
            return dataset_node(dataset)

        def load() -> pl.DataFrame:
            datasets = [d.dataset_id for d in client().list_datasets(page_size=self.page_size)]
            return pl.DataFrame({"dataset": datasets}, schema={"dataset": pl.Utf8})

        def child(row: dict[str, Any]) -> CatalogNode:
            return dataset_node(row["dataset"])

        return CatalogNode(title=title, load=load, child=child, key=base, name_columns=("dataset",))
