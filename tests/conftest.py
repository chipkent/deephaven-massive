"""Shared pytest fixtures for deephaven-massive including an embedded Deephaven Server."""

from __future__ import annotations

from collections.abc import Callable, Mapping
import atexit
import gzip
import io
import os
import sys
from typing import Any, BinaryIO

import pyarrow as pa
import pytest

from deephaven_massive.session import MassiveSession, ensure_deephaven_server

_EXIT_STATUS: int = 0


def _final_jvm_exit() -> None:
    """Flush stdio and exit cleanly before JVM/jpy finalizer teardown segfaults on macOS."""
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass
    os._exit(_EXIT_STATUS)


@pytest.hookimpl(trylast=True)
def pytest_terminal_summary(
    terminalreporter: Any, exitstatus: int, config: Any
) -> None:
    """Record the final session exitstatus after pytest-cov evaluates --cov-fail-under."""
    global _EXIT_STATUS
    _EXIT_STATUS = int(getattr(terminalreporter._session, "exitstatus", exitstatus))


@pytest.fixture(scope="session", autouse=True)
def dh_exec_ctx() -> Any:
    """Start the embedded Deephaven server once per test session with a fast 25ms UpdateGraph tick."""
    ctx = ensure_deephaven_server(
        auto_start=True,
        port=10099,
        jvm_args=[
            "-Xmx2g",
            "-DPeriodicUpdateGraph.defaultRefreshIntervalMillis=25",
        ],
    )
    atexit.register(_final_jvm_exit)
    return ctx


def dh_to_pydict(table: Any) -> dict[str, list[Any]]:
    """Convert a Deephaven Table into a Python dict of column lists via PyArrow."""
    from deephaven import arrow as dh_arrow

    pa_tbl = dh_arrow.to_arrow(table)
    return pa_tbl.to_pydict()  # type: ignore[no-any-return]


def dh_to_pylist(table: Any) -> list[dict[str, Any]]:
    """Convert a Deephaven Table into a list of row dicts via PyArrow."""
    from deephaven import arrow as dh_arrow

    pa_tbl = dh_arrow.to_arrow(table)
    return pa_tbl.to_pylist()  # type: ignore[no-any-return]


def make_csv_gz_stream(csv_text: str) -> BinaryIO:
    """Compress CSV text into an in-memory `.csv.gz` binary stream."""
    compressed = gzip.compress(csv_text.encode("utf-8"))
    return io.BytesIO(compressed)


@pytest.fixture()
def mock_session(dh_exec_ctx: Any) -> Callable[..., MassiveSession]:
    """Factory fixture returning a configured MassiveSession with injectable HTTP and S3 handlers."""
    created: list[MassiveSession] = []

    def _create(
        *,
        http_handler: Callable[
            [str, Mapping[str, Any] | None, Mapping[str, str]],
            tuple[int, Any],
        ]
        | None = None,
        s3_opener: Callable[[str, str], BinaryIO] | None = None,
        s3_access_key_id: str | None = "test-s3-key",
        s3_secret_access_key: str | None = "test-s3-secret",
        flat_file_ticker_threshold: int = 3,
        auto_flush: bool = False,
    ) -> MassiveSession:
        def _default_http(
            url: str,
            params: Mapping[str, Any] | None,
            headers: Mapping[str, str],
        ) -> tuple[int, Any]:
            return 200, {"status": "OK", "results": [{"ticker": "EURUSD", "p": 150.0, "s": 100, "t": 1700000000000000000}]}

        sess = MassiveSession(
            api_key="test-massive-key",
            s3_access_key_id=s3_access_key_id,
            s3_secret_access_key=s3_secret_access_key,
            flat_file_ticker_threshold=flat_file_ticker_threshold,
            auto_flush=auto_flush,
            http_handler=http_handler or _default_http,
            s3_opener=s3_opener,
            ws_send_hook=lambda _u, _p: None,
            sleep_fn=lambda _: None,
        )
        created.append(sess)
        return sess

    yield _create

    for s in created:
        s.close()
