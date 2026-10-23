"""Test-suite configuration: the embedded Deephaven JVM and the shared fixtures.

The embedded server starts in `pytest_configure`, before collection, so test modules can import
`deephaven` at module level, exactly as the library does. This module itself is imported
before `pytest_configure` runs, so it reaches `deephaven` only through `importlib` inside
fixtures, never through a module-level import.

Test modules never import from this file; they request its fixtures by name. Fakes from
`tests/_fakes/` are handed to tests through thin fixtures defined here.
"""

from __future__ import annotations

import atexit
import gzip
import importlib
import itertools
import os
import sys
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from deephaven_server import Server

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from deephaven.execution_context import ExecutionContext
    from deephaven.stream.table_publisher import TablePublisher
    from deephaven.table import Table

    from deephaven_massive import MassiveSession
    from deephaven_massive._catalog import DatasetSpec
    from deephaven_massive._liveness import StreamHandle
    from deephaven_massive._query import Request

collect_ignore_glob = ["legacy/*"]  # LEGACY-EXCLUSION: legacy tests, deleted in P7

_DATA_DIR = Path(__file__).parent / "data"

_JVM_ARGS = [
    "-Xmx2g",
    # A 25 ms update-graph cycle (default 1 s) keeps ticking-table tests fast.
    "-DPeriodicUpdateGraph.defaultRefreshIntervalMillis=25",
]


# `trylast` matters: pytest's faulthandler plugin installs its SIGSEGV handler in its own
# `pytest_configure`. The JVM must install its signal handlers after that, chaining to the
# faulthandler, because the JVM raises SIGSEGV on purpose (implicit null checks, safepoints).
# Started first, the JVM's handlers would be replaced and the first such signal would kill
# the process.
@pytest.hookimpl(trylast=True)
def pytest_configure(config: pytest.Config) -> None:
    """Start the embedded Deephaven server once per process, before collection.

    Args:
        config: The pytest configuration (unused; required by the hook signature).
    """
    # Port 0 asks the OS for an ephemeral port, so two test runs can share a machine.
    Server(port=0, jvm_args=_JVM_ARGS).start()
    atexit.register(_final_jvm_exit)


# --- JVM shutdown workaround -------------------------------------------------------------------
# On macOS with Python 3.11+, normal interpreter shutdown after an embedded JVM has run can
# segfault (exit code 139) while C++ static destructors in jpy and the JVM are finalized. The
# workaround: once pytest has finished (after pytest-cov has applied `fail_under` to the
# session's exit status), flush stdio and leave through `os._exit` with pytest's own exit
# status, skipping the finalizers. `atexit` handlers run last-in, first-out, so this one,
# registered after the server's, runs first. Do not remove it without reproducing the crash.

_SESSIONS: list[pytest.Session] = []


def pytest_sessionstart(session: pytest.Session) -> None:
    """Remember the session so the exit handler can read its final exit status.

    Args:
        session: The pytest session that is starting.
    """
    _SESSIONS.append(session)


def _final_jvm_exit() -> None:
    """Flush stdio and exit with pytest's exit status, skipping JVM and jpy finalizers."""
    status = int(_SESSIONS[-1].exitstatus) if _SESSIONS else int(pytest.ExitCode.INTERNAL_ERROR)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(status)


# --- End of JVM shutdown workaround ------------------------------------------------------------


@pytest.fixture(scope="session")
def dh_exec_ctx() -> ExecutionContext:
    """Return the embedded server's `ExecutionContext`.

    Returns:
        The execution context of the main thread's script session.
    """
    execution_context = importlib.import_module("deephaven.execution_context")
    ctx: ExecutionContext = execution_context.get_exec_ctx()
    return ctx


@pytest.fixture
def dh_to_pylist() -> Callable[[Table], list[dict[str, Any]]]:
    """Return a function that snapshots a Deephaven table into a list of row dictionaries.

    Returns:
        A function mapping a table to one `{column: value}` dictionary per row.
    """
    dh_arrow = importlib.import_module("deephaven.arrow")

    def to_pylist(table: Table) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = dh_arrow.to_arrow(table).to_pylist()
        return rows

    return to_pylist


@pytest.fixture
def load_fixture() -> Callable[[str, str], bytes]:
    """Return a function that reads a payload fixture from `tests/data/<owner>/<name>`.

    Each work package keeps its payloads under its own `owner` directory.

    Returns:
        A function mapping `(owner, name)` to the fixture file's bytes.
    """

    def load(owner: str, name: str) -> bytes:
        return (_DATA_DIR / owner / name).read_bytes()

    return load


@pytest.fixture
def make_csv_gz() -> Callable[[str], bytes]:
    """Return a function that gzip-compresses CSV text, as a flat file is stored in S3.

    Returns:
        A function mapping CSV text to the bytes of a `.csv.gz` file.
    """

    def compress(csv_text: str) -> bytes:
        return gzip.compress(csv_text.encode())

    return compress


# --- Session fixtures (P2) ---------------------------------------------------------------------


@pytest.fixture
def massive_session() -> Iterator[MassiveSession]:
    """Yield a `MassiveSession` with a test key, closed at teardown.

    Yields:
        An open session on the embedded server's execution context.
    """
    dm = importlib.import_module("deephaven_massive")
    session: MassiveSession = dm.MassiveSession(dm.MassiveConfig(api_key="test-key"))
    yield session
    session.close()


class FakeStreamTransport:
    """A WebSocket transport stand-in: each stream is a bare `table_publisher` blink table.

    Records every stream it opens and every `on_shutdown_callback` that fires, so liveness tests
    can observe exactly when Deephaven tears a stream down.
    """

    def __init__(self) -> None:
        self.opened: list[tuple[str, str | None]] = []
        self.publishers: dict[int, TablePublisher] = {}
        self.shutdown: dict[int, threading.Event] = {}
        self.shutdown_calls: dict[int, int] = {}
        self._lock = threading.Lock()
        self._ids = itertools.count(1)

    def _on_shutdown(self, stream_id: int) -> None:
        with self._lock:
            self.shutdown_calls[stream_id] = self.shutdown_calls.get(stream_id, 0) + 1
        self.shutdown[stream_id].set()

    def open_stream(self, spec: DatasetSpec, request: Request, channel: str | None) -> StreamHandle:
        liveness = importlib.import_module("deephaven_massive._liveness")
        publisher_module = importlib.import_module("deephaven.stream.table_publisher")
        assert spec.schema is not None
        stream_id = next(self._ids)
        self.shutdown[stream_id] = threading.Event()
        scope = liveness.new_scope()
        with scope.open():
            blink, publisher = publisher_module.table_publisher(
                f"test.{spec.name}#{stream_id}",
                spec.schema.col_defs(),
                on_shutdown_callback=lambda: self._on_shutdown(stream_id),
            )
        self.opened.append((spec.name, channel))
        self.publishers[stream_id] = publisher
        handle: StreamHandle = liveness.StreamHandle(
            stream_id, blink, scope, "socket.test", "/stocks"
        )
        return handle


@pytest.fixture
def fake_streams(
    massive_session: MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> FakeStreamTransport:
    """Install a `FakeStreamTransport` as the session's WebSocket transport hook.

    Returns:
        The fake, for inspecting opened streams and shutdown callbacks.
    """
    fake = FakeStreamTransport()
    monkeypatch.setattr(massive_session._core, "stream_transport", lambda: fake)
    return fake
