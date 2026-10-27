"""A stand-in for the WebSocket transport's stream factory, for the liveness tests.

Each stream is a bare `table_publisher` blink table created in its own stream scope, exactly as
the real transport creates one, without any network. The fake records every stream it opens and
every `on_shutdown_callback` Deephaven runs, so tests can observe exactly when a stream is torn
down. Like the real transport, the callback first tells the session
(`SessionCore.stream_shut_down`), which drops the stream's registry record. Imported lazily by
the `fake_streams` fixture in `tests/conftest.py`, after the embedded JVM has started.
"""

from __future__ import annotations

import threading
import weakref
from typing import TYPE_CHECKING

from deephaven.stream.table_publisher import TablePublisher, table_publisher

from deephaven_massive._tables.columns import col_defs
from deephaven_massive._tables.liveness import StreamHandle, new_scope

if TYPE_CHECKING:
    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._tables.core import SessionCore


class FakeStreamTransport:
    """Opens publisher-backed streams and records their lifecycle.

    The session's core is held weakly, so the fake adds no reference that could keep a session
    alive in the garbage-collection tests.
    """

    def __init__(self, core: SessionCore) -> None:
        self._core = weakref.ref(core)
        self.opened: list[tuple[str, str | None]] = []
        self.publishers: dict[int, TablePublisher] = {}
        self.shutdown: dict[int, threading.Event] = {}
        self.shutdown_calls: dict[int, int] = {}
        self._lock = threading.Lock()

    def _on_shutdown(self, stream_id: int) -> None:
        core = self._core()
        if core is not None:
            core.stream_shut_down(stream_id)
        with self._lock:
            self.shutdown_calls[stream_id] = self.shutdown_calls.get(stream_id, 0) + 1
        self.shutdown[stream_id].set()

    def open_stream(
        self, spec: DatasetSpec, request: Request, channel: str | None, *, stream_id: int
    ) -> StreamHandle:
        assert spec.schema is not None
        with self._lock:
            self.shutdown[stream_id] = threading.Event()
        scope = new_scope()
        with scope.open():
            blink, publisher = table_publisher(
                f"massive.{spec.name}#{stream_id}",
                col_defs(spec.schema),
                on_shutdown_callback=lambda: self._on_shutdown(stream_id),
            )
        with self._lock:
            self.opened.append((spec.name, channel))
            self.publishers[stream_id] = publisher
        return StreamHandle(stream_id, blink, scope, "socket.test", "/stocks")
