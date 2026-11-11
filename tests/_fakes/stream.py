"""A stand-in for the WebSocket transport's stream factory, for the liveness tests.

Each stream is a `blink_publisher` blink table created in its own stream scope, exactly as the
real transport creates one, without any network. The fake records every stream it opens and
every `on_shutdown_callback` Deephaven runs, so tests can observe exactly when a stream is torn
down. Like the real transport, the callback first tells the session
(`SharedStreams.shut_down`), which drops the stream's registry record, and the handle's
`fail` records the failure and then fails the table, once, as `StreamSink.fail` does. Imported
lazily by
the `fake_streams` fixture in `tests/conftest.py`, after the embedded JVM has started.
"""

from __future__ import annotations

import functools
import threading
import types
import weakref
from typing import TYPE_CHECKING, Any

from deephaven_massive._enums import Recency
from deephaven_massive._tables.columns import col_defs
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive._tables.publisher import BlinkPublisher, blink_publisher
from deephaven_massive._tables.transports import StreamHandle

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._errors import MassiveError
    from deephaven_massive._tables.session_core import SessionCore


class FakeStreamTransport:
    """Opens publisher-backed streams and records their lifecycle.

    The session's core is held weakly, so the fake adds no reference that could keep a session
    alive in the garbage-collection tests.
    """

    def __init__(self, core: SessionCore) -> None:
        self._core = weakref.ref(core)
        self.opened: list[tuple[str, str]] = []
        self.publishers: dict[int, BlinkPublisher] = {}
        self.shutdown: dict[int, threading.Event] = {}
        self.shutdown_calls: dict[int, int] = {}
        self.failed: dict[int, str] = {}
        # The stream of each `publish_held` call, in order.
        self.published_held: list[int] = []
        self._lock = threading.Lock()

    def _on_shutdown(self, stream_id: int) -> None:
        core = self._core()
        if core is not None:
            core.streams.shut_down(stream_id)
        with self._lock:
            self.shutdown_calls[stream_id] = self.shutdown_calls.get(stream_id, 0) + 1
        self.shutdown[stream_id].set()

    def _fail(self, stream_id: int, error: MassiveError) -> bool:
        with self._lock:
            if stream_id in self.failed:
                return False
            self.failed[stream_id] = str(error)
        core = self._core()
        assert core is not None  # a stream's record keeps its session alive
        core.fail_stream(stream_id, str(error))
        self.publishers[stream_id].publish_failure(error)
        return True

    def _publish_held(self, stream_id: int) -> None:
        with self._lock:
            self.published_held.append(stream_id)

    def tradable_pairs(self, spec: DatasetSpec, symbols: tuple[str, ...]) -> tuple[str, ...]:
        # Every pair is listed here; `tests/ingest/test_ws.py` tests the real lookups.
        return symbols

    def open_stream(
        self, spec: DatasetSpec, request: Request, channel: str, *, stream_id: int
    ) -> StreamHandle:
        core = self._core()
        assert core is not None  # a verb holds its session's core while it opens a stream
        schema = core.dataset_schema(spec)
        with self._lock:
            self.shutdown[stream_id] = threading.Event()
        scope = new_scope()
        with scope.open():
            blink, publisher = blink_publisher(
                f"massive.{spec.name}#{stream_id}",
                col_defs(schema),
                on_shutdown=lambda: self._on_shutdown(stream_id),
                release_later=core.release_later,
            )
        with self._lock:
            self.opened.append((spec.name, channel))
            self.publishers[stream_id] = publisher
        fail = functools.partial(self._fail, stream_id)
        return StreamHandle(
            stream_id,
            blink,
            scope,
            "socket.test",
            "/stocks",
            "test host",
            fail,
            functools.partial(self._publish_held, stream_id),
            recency=Recency.REALTIME,
        )


def transport_of(open_stream: Callable[..., StreamHandle]) -> Callable[[], Any]:
    """Return a `SessionCore.stream_transport` hook whose transport opens with `open_stream`.

    A test that needs its own open patches the hook with this rather than with a local class:
    a class is a reference cycle, so a local one whose method closes over the session would
    keep the session alive until the cyclic collector ran.

    Args:
        open_stream: The transport's `open_stream`.

    Returns:
        The hook: each call returns a transport with that `open_stream`.
    """
    return lambda: types.SimpleNamespace(open_stream=open_stream)
