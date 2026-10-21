"""Shared pytest fixtures for deephaven-massive including an embedded Deephaven Server."""

from __future__ import annotations

import atexit
import gzip
import io
import os
import sys
from collections.abc import Callable, Iterator, Mapping
from typing import Any, BinaryIO

import pytest

from deephaven_massive.session import MassiveSession, _ensure_deephaven_server

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
def pytest_terminal_summary(terminalreporter: Any, exitstatus: int, config: Any) -> None:
    """Record the final session exitstatus after pytest-cov evaluates --cov-fail-under.

    Args:
        terminalreporter: The pytest terminal reporter plugin instance.
        exitstatus: The integer exit code reported by pytest.
        config: The pytest Config object.
    """
    global _EXIT_STATUS
    _EXIT_STATUS = int(getattr(terminalreporter._session, "exitstatus", exitstatus))


@pytest.fixture(scope="session", autouse=True)
def dh_exec_ctx() -> Any:
    """Start the embedded Deephaven server once per test session with a fast 25ms UpdateGraph tick.

    Returns:
        The systemic Deephaven ExecutionContext bound to the embedded JVM server.
    """
    ctx = _ensure_deephaven_server(
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
    """Convert a Deephaven Table into a Python dict of column lists via PyArrow.

    Args:
        table: The Deephaven Table to convert.

    Returns:
        A dictionary mapping column names to Python lists of values.
    """
    from deephaven import arrow as dh_arrow

    pa_tbl = dh_arrow.to_arrow(table)
    return pa_tbl.to_pydict()


def dh_to_pylist(table: Any) -> list[dict[str, Any]]:
    """Convert a Deephaven Table into a list of row dicts via PyArrow.

    Args:
        table: The Deephaven Table to convert.

    Returns:
        A list of dictionaries representing each row in the table.
    """
    from deephaven import arrow as dh_arrow

    pa_tbl = dh_arrow.to_arrow(table)
    return pa_tbl.to_pylist()


def make_csv_gz_stream(csv_text: str) -> BinaryIO:
    """Compress CSV text into an in-memory `.csv.gz` binary stream.

    Args:
        csv_text: Uncompressed UTF-8 CSV content.

    Returns:
        A binary stream containing the gzip-compressed bytes.
    """
    compressed = gzip.compress(csv_text.encode("utf-8"))
    return io.BytesIO(compressed)


@pytest.fixture()
def mock_session(dh_exec_ctx: Any) -> Iterator[Callable[..., MassiveSession]]:
    """Factory fixture returning a configured MassiveSession with injectable HTTP and S3 handlers.

    Every created session also exposes `sess.recorded_requests`, a list of
    `(url, params_dict, headers_dict)` tuples capturing every outbound REST call.

    Args:
        dh_exec_ctx: Session-scoped Deephaven ExecutionContext fixture.

    Yields:
        A factory callable accepting optional `http_handler`, `s3_opener`, and config overrides.
    """
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
        recorded_requests: list[tuple[str, dict[str, Any], dict[str, str]]] = []

        def _default_http(
            url: str,
            params: Mapping[str, Any] | None,
            headers: Mapping[str, str],
        ) -> tuple[int, Any]:
            return (
                200,
                {
                    "status": "OK",
                    "results": [
                        {
                            "ticker": "EURUSD",
                            "p": 150.0,
                            "s": 100,
                            "t": 1800000000000000000,
                        }
                    ],
                },
            )

        delegate = http_handler or _default_http

        def _recording_http(
            url: str,
            params: Mapping[str, Any] | None,
            headers: Mapping[str, str],
        ) -> tuple[int, Any]:
            recorded_requests.append((url, dict(params or {}), dict(headers)))
            return delegate(url, params, headers)

        sess = MassiveSession(
            api_key="test-massive-key",
            s3_access_key_id=s3_access_key_id,
            s3_secret_access_key=s3_secret_access_key,
            flat_file_ticker_threshold=flat_file_ticker_threshold,
            auto_flush=auto_flush,
            http_handler=_recording_http,
            s3_opener=s3_opener,
            ws_send_hook=lambda _u, _p: None,
            sleep_fn=lambda _: None,
        )
        sess.recorded_requests = recorded_requests  # type: ignore[attr-defined]
        created.append(sess)
        return sess

    yield _create

    for s in created:
        s.close()
