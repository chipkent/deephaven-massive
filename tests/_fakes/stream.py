"""A stand-in for the WebSocket transport's stream factory, for the P2 liveness tests.

Each stream is a bare `table_publisher` blink table created in its own stream scope, exactly as
the real transport creates one, without any network. The fake records every stream it opens and
every `on_shutdown_callback` Deephaven runs, so tests can observe exactly when a stream is torn
down. Imported lazily by the `fake_streams` fixture in `tests/conftest.py`, after the embedded
JVM has started.
"""

from __future__ import annotations

import itertools
import threading
from typing import TYPE_CHECKING

from deephaven.stream.table_publisher import TablePublisher, table_publisher

from deephaven_massive._liveness import StreamHandle, new_scope

if TYPE_CHECKING:
    from deephaven_massive._catalog import DatasetSpec
    from deephaven_massive._query import Request


class FakeStreamTransport:
    """Opens publisher-backed streams and records their lifecycle."""

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
        assert spec.schema is not None
        with self._lock:
            stream_id = next(self._ids)
            self.shutdown[stream_id] = threading.Event()
        scope = new_scope()
        with scope.open():
            blink, publisher = table_publisher(
                f"test.{spec.name}#{stream_id}",
                spec.schema.col_defs(),
                on_shutdown_callback=lambda: self._on_shutdown(stream_id),
            )
        with self._lock:
            self.opened.append((spec.name, channel))
            self.publishers[stream_id] = publisher
        return StreamHandle(stream_id, blink, scope, "socket.test", "/stocks")
