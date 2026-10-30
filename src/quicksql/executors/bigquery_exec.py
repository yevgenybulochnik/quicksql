"""BigQuery executor: extracts to Arrow, which DuckDB registers and lands anywhere.

Needs the ``bigquery`` extra (google-cloud-bigquery, pyarrow).
"""

from __future__ import annotations

from typing import Any

from ..errors import ExecutorError
from ..models import RenderedCell, RunContext
from ..registry import executor
from .base import Executor, register_frame, result_view, strip_trailing_semicolon


@executor("bigquery")
class BigQueryExecutor(Executor):
    supports_context_refs = True
    # bigquery runs multi-statement scripts server-side in one job; splitting
    # client-side would run the pieces as separate jobs and lose session state,
    # and a script can't be the body of CREATE TEMP TABLE AS.
    supports_multistatement_materialization = False

    def split_statements(self, sql: str) -> list[str]:
        return [strip_trailing_semicolon(sql)]

    def context_key(self, config: Any) -> str:
        spec = (config.input or {}).get("bigquery") or {}
        return f"bigquery:{spec.get('project')}"

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

    def _session_state(self, cell: RenderedCell, ctx: RunContext, spec: dict[str, Any]) -> dict:
        """Per-context BigQuery session (temp tables live in it; sessions
        auto-expire server-side, so close() is a no-op)."""
        if ctx.session is None:
            return {
                "client": self.make_client(spec),
                "session_id": None,
                "query_opts": self._query_opts(spec),
            }
        key = self.context_key(cell.config)
        if key not in ctx.session.engine_sessions:
            ctx.session.engine_sessions[key] = {
                "client": self.make_client(spec),
                "session_id": None,
                "query_opts": self._query_opts(spec),
            }
        return ctx.session.engine_sessions[key]

    def _job_config(self, state: dict) -> Any:
        """Split out so offline tests avoid the google import."""
        from google.cloud import bigquery

        if state["session_id"] is None:
            return bigquery.QueryJobConfig(create_session=True)
        return bigquery.QueryJobConfig(
            connection_properties=[
                bigquery.ConnectionProperty("session_id", state["session_id"])
            ]
        )

    def _query(self, state: dict, sql: str) -> Any:
        job = state["client"].query(
            sql, job_config=self._job_config(state), **state["query_opts"]
        )
        # query() only *submits*; sessions allow one active job at a time, so
        # wait for completion before the next statement goes in
        job.result()
        if state["session_id"] is None:
            info = getattr(job, "session_info", None)
            session_id = getattr(info, "session_id", None)
            if session_id:
                state["session_id"] = session_id
        return job

    def execute(self, cell: RenderedCell, ctx: RunContext) -> str:
        spec = (cell.config.input or {}).get("bigquery") or {}
        sql = strip_trailing_semicolon(cell.sql)  # bigquery runs the whole script in one job
        if cell.sink_type == "none":
            # effect-only: run the (possibly multi-statement) script, land nothing
            if cell.context_refs:
                self._query(self._session_state(cell, ctx, spec), sql)
            else:
                self.make_client(spec).query(sql, **self._query_opts(spec)).result()
            return ""
        if cell.reffed_in_context:
            # materialize once; the temp serves consumers, the read lands parquet.
            # body goes in bare: wrapped in parens, a trailing line comment
            # would swallow the closing one
            state = self._session_state(cell, ctx, spec)
            self._query(state, f"CREATE OR REPLACE TEMP TABLE {cell.name} AS {sql}")
            table = self._query(state, f"SELECT * FROM {cell.name}").to_arrow()
        elif cell.context_refs:
            # consumes session temps: run in-session, no temp of its own
            state = self._session_state(cell, ctx, spec)
            table = self._query(state, sql).to_arrow()
        else:  # no in-context participation: one plain job, as before
            table = self.make_client(spec).query(sql, **self._query_opts(spec)).to_arrow()
        return register_frame(ctx, result_view(cell.name), table)
