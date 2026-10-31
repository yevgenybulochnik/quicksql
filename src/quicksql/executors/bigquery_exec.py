"""BigQuery executor: extracts to Arrow, which DuckDB registers and lands anywhere.

Needs the ``bigquery`` extra (google-cloud-bigquery, pyarrow).
"""

from __future__ import annotations

from typing import Any

from ..config import input_spec
from ..errors import ExecutorError
from ..models import RunContext
from ..registry import executor
from .base import EngineSession, Executor, strip_trailing_semicolon


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
        job = self.client.query(sql, job_config=self._job_config(), **self.query_opts)
        # query() only *submits*; sessions allow one active job at a time, so
        # wait for completion before the next statement goes in
        job.result()
        if self.session_id is None:
            info = getattr(job, "session_info", None)
            self.session_id = getattr(info, "session_id", None)
        return job


@executor("bigquery")
class BigQueryExecutor(Executor):
    supports_context_refs = True
    # bigquery runs multi-statement scripts server-side in one job; splitting
    # client-side would run the pieces as separate jobs and lose session state,
    # and a script can't be the body of CREATE TEMP TABLE AS.
    supports_multistatement_materialization = False
    input_shorthand = "project"

    def split_statements(self, sql: str) -> list[str]:
        return [strip_trailing_semicolon(sql)]

    def context_key(self, config: Any) -> str:
        return f"bigquery:{input_spec(config, 'bigquery').get('project')}"

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
        spec = input_spec(config, "bigquery")
        return BigQuerySession(self.make_client(spec), self._query_opts(spec))
