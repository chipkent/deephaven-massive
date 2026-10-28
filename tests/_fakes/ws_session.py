"""A real session whose WebSocket connections reach the fake Massive server, for stream tests.

`WsSession` wraps a `MassiveSession` whose WebSocket transport is a real `WsConnections`: its
host decision is fixed (no REST request decides it) and each connection's client is a real
`WsClient`, pointed at the fake server's endpoint of the same path (`tests/_fakes/ws.py`). Every
client callback is recorded, so a test waits for exactly the event it needs instead of
sleeping, and flushes are explicit: the flush interval is an hour unless a test asks for
another. The transport tests and every namespace's end-to-end stream tests share it; tests get
the default one from the `ws_session` fixture in `tests/conftest.py`, and build their own only
when they need other options. It imports the library, which imports Deephaven, so it is
imported only once the embedded JVM has started.
"""

from __future__ import annotations

import inspect
import json
import threading
from typing import TYPE_CHECKING, Any, cast

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._datasets.request import HostDecision
from deephaven_massive._enums import Recency
from deephaven_massive._ingest import ws
from deephaven_massive._namespaces.base import build_query
from deephaven_massive._query import StreamQuery
from deephaven_massive._types import override

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._enums import AssetClass, ChannelFamily
    from deephaven_massive._massive import DriftReport, Reject, Source, Status, WireError, WsState
    from tests._fakes.ws import FakeMassiveServer

TIMEOUT = 15.0
"""Seconds any wait on the session or the server may take before the test fails."""

REALTIME = HostDecision("socket.test", Recency.REALTIME, "test evidence")
"""The host decision every stream gets unless a test passes its own `decide`."""


class SpyClient(_massive.WsClient):
    """A real client that also records the loop-thread calls the library makes on it.

    Each `set_required` and `subscribe` call is passed to `record`, in call order, before the
    real method runs; `WsSession` appends it to its journal under its lock.
    """

    def __init__(
        self,
        url: str,
        record: Callable[[tuple[object, ...]], None],
        *,
        sources: Sequence[Source],
        callbacks: dict[str, Callable[..., None]],
        backoff: float,
    ) -> None:
        super().__init__(
            url,
            api_key="test-key",
            sources=sources,
            on_open=callbacks["on_open"],
            on_status=callbacks["on_status"],
            on_data=callbacks["on_data"],
            on_state=callbacks["on_state"],
            reconnect_backoff=backoff,
            max_reconnect_backoff=backoff * 2,
        )
        self._record = record

    @override
    def set_required(
        self, channel: str, keys: frozenset[str], *, symbol: str | None = None
    ) -> None:
        self._record(("set_required", channel, keys, symbol))
        super().set_required(channel, keys, symbol=symbol)

    @override
    def subscribe(self, generation: int, topics: Sequence[str]) -> None:
        self._record(("subscribe", tuple(topics)))
        super().subscribe(generation, topics)


class WsSession:
    """A session whose connections reach the fake server, with every client event recorded.

    Created on the test thread, by the `ws_session` fixture or directly, and closed with
    `close()`. What it records is written on the test thread and on the clients' loop threads,
    always under `cond`, a reentrant condition. Each attribute below reads it under `cond` too,
    and a list attribute returns a snapshot, so a test may read one anywhere; to wait for a
    change, pass a predicate to `wait`, which holds `cond` while it tests it.

    Attributes:
        session: The session; build queries from its namespaces (`session.stocks.trades`).
        connections: The session's WebSocket transport.
        cond: The condition every recorded event is written and notified under.
    """

    def __init__(
        self,
        server: FakeMassiveServer,
        *,
        decide: Callable[[AssetClass, ChannelFamily], HostDecision] | None = None,
        flush_interval: float = 3600.0,
        backoff: float = 0.05,
        **config: object,
    ) -> None:
        """Open the session; nothing connects until a stream verb runs.

        Args:
            server: The fake server every connection reaches.
            decide: The host decision of each `(asset_class, family)`; None for `REALTIME`.
            flush_interval: The session's flush interval, in seconds; an hour by default, so
                rows are published only by `flush()`.
            backoff: Each client's first reconnect backoff, in seconds (twice it at most).
            **config: Further `MassiveConfig` options.
        """
        self.server = server
        self.backoff = backoff
        self._decide = decide or (lambda *_: REALTIME)
        self.cond = threading.Condition(threading.RLock())
        self._decisions: list[tuple[AssetClass, ChannelFamily]] = []
        self._verdicts: list[tuple[AssetClass, ChannelFamily, HostDecision]] = []
        self._journal: list[tuple[object, ...]] = []
        self._urls: list[str] = []
        self._items = 0
        self._statuses: list[Status] = []
        self._states: list[WsState] = []
        self._opens = 0
        self.session = dm.MassiveSession(
            dm.MassiveConfig(
                api_key="test-key",
                flush_interval=flush_interval,
                **cast("dict[str, Any]", config),
            )
        )
        clients: Any = self.session._core._ports
        self.connections = ws.WsConnections(
            self.session._core,
            resolve_host=self._resolve,
            record_verdict=self._record_verdict,
            client_factory=self.factory,
        )
        clients._streams = self.connections
        self.session._core.on_close(self.connections.close)

    @property
    def decisions(self) -> list[tuple[AssetClass, ChannelFamily]]:
        """Each `(asset_class, family)` the transport asked a host decision for, in order."""
        with self.cond:
            return list(self._decisions)

    @property
    def verdicts(self) -> list[tuple[AssetClass, ChannelFamily, HostDecision]]:
        """Each real-time verdict recorded, as `(asset_class, family, decision)`, in order.

        A recorded verdict answers the later decisions of its pair.
        """
        with self.cond:
            return list(self._verdicts)

    @property
    def journal(self) -> list[tuple[object, ...]]:
        """The `set_required` and `subscribe` calls on every client, in order."""
        with self.cond:
            return list(self._journal)

    @property
    def urls(self) -> list[str]:
        """The URL of each client the transport created, in order."""
        with self.cond:
            return list(self._urls)

    @property
    def items(self) -> int:
        """How many data items the clients have delivered, rows and rejects alike."""
        with self.cond:
            return self._items

    @property
    def statuses(self) -> list[Status]:
        """Every status the clients have delivered, in order."""
        with self.cond:
            return list(self._statuses)

    @property
    def states(self) -> list[WsState]:
        """Every connection state the clients have reported, in order."""
        with self.cond:
            return list(self._states)

    @property
    def opens(self) -> int:
        """How many times a client has opened a connection."""
        with self.cond:
            return self._opens

    def _resolve(self, asset_class: AssetClass, family: ChannelFamily) -> HostDecision:
        """Decide a host as the REST transport's cache does: a recorded verdict wins.

        Returns:
            The decision.
        """
        with self.cond:
            self._decisions.append((asset_class, family))
            recorded = [
                decision
                for verdict_class, verdict_family, decision in self._verdicts
                if (verdict_class, verdict_family) == (asset_class, family)
            ]
        return recorded[0] if recorded else self._decide(asset_class, family)

    def _record_verdict(
        self, asset_class: AssetClass, family: ChannelFamily, decision: HostDecision
    ) -> None:
        """Record a real-time verdict, which answers the pair's later decisions."""
        with self.cond:
            self._verdicts.append((asset_class, family, decision))
            self.cond.notify_all()

    def factory(
        self,
        url: str,
        *,
        sources: Sequence[Source],
        on_open: Callable[[int], None],
        on_status: Callable[[int, Status], None],
        on_data: Callable[[int, str, list[dict[str, object]], list[Reject], DriftReport], None],
        on_state: Callable[[WsState, float | WireError | None], None],
    ) -> SpyClient:
        """Create the client of `url` on the fake server's endpoint of the same path.

        Returns:
            The client, not started; its callbacks are the transport's, each also recorded.
        """
        with self.cond:
            self._urls.append(url)
            self.cond.notify_all()
        path = "/" + url.split("/", 3)[3]
        callbacks = {
            "on_open": self._noting(on_open, "opens"),
            "on_status": self._noting(on_status, "statuses"),
            "on_data": self._noting(on_data, "items"),
            "on_state": self._noting(on_state, "states"),
        }
        return SpyClient(
            self.server.url(path),
            self._called,
            sources=sources,
            callbacks=callbacks,
            backoff=self.backoff,
        )

    def _called(self, call: tuple[object, ...]) -> None:
        with self.cond:
            self._journal.append(call)
            self.cond.notify_all()

    def _noting(self, callback: Callable[..., None], what: str) -> Callable[..., None]:
        def call(*event: object) -> None:
            callback(*event)
            self._note(what, event)

        return call

    def _note(self, what: str, event: tuple[object, ...]) -> None:
        with self.cond:
            if what == "opens":
                self._opens += 1
            elif what == "items":
                rows, rejects = cast("tuple[list[object], list[object]]", event[2:4])
                self._items += len(rows) + len(rejects)
            elif what == "statuses":
                self._statuses.append(cast("Status", event[1]))
            else:
                self._states.append(cast("WsState", event[0]))
            self.cond.notify_all()

    def wait(self, predicate: Callable[[WsSession], bool], what: str | None = None) -> None:
        """Wait, holding `cond`, until `predicate(self)` is true; fail after `TIMEOUT`.

        Args:
            predicate: The condition, tested whenever a recorded event arrives.
            what: What the wait is for, in the failure message; the predicate's source line
                when None.
        """
        with self.cond:
            assert self.cond.wait_for(lambda: predicate(self), TIMEOUT), (
                f"waited {TIMEOUT} s for {what or inspect.getsource(predicate).strip()}; "
                f"recorded items={self._items}, opens={self._opens}, "
                f"states={self._states}, statuses={self._statuses}"
            )

    def query(self, spec: DatasetSpec, symbols: object) -> StreamQuery:
        """Build the stream query of a dataset no namespace needs to declare.

        Returns:
            The query.
        """
        q = build_query(self.session._core, spec, symbols=cast("list[str]", symbols))
        assert isinstance(q, StreamQuery)
        return q

    def connection(self, url: str = "wss://socket.test/stocks") -> ws.Connection:
        """Return the session's connection to `url`.

        Returns:
            The connection.
        """
        with self.session._core.registry_lock:
            return self.connections._connections[url]

    def send(self, *items: dict[str, object]) -> None:
        """Send one frame of `items` and wait until the clients have delivered every data item.

        A client delivers a frame one channel at a time, so the wait counts items, rows and
        rejects alike, not deliveries: it returns only once every channel of the frame has
        reached the transport. A status object (`"ev": "status"`) is not a data item and is not
        waited for, and every other item must belong to a channel the connection has a source
        for, since the client drops any other.
        """
        expected = sum(1 for item in items if item.get("ev") != "status")
        with self.cond:
            before = self._items
        self.server.send(json.dumps(list(items)))
        self.wait(
            lambda s: s.items >= before + expected,
            f"{expected} data items of the frame to be delivered",
        )

    def publish(self, *items: dict[str, object]) -> None:
        """Send one frame of `items`, wait until every data item is delivered, then flush."""
        self.send(*items)
        self.flush()

    def flush(self) -> None:
        """Publish the rows every connection holds, on the test thread."""
        with self.session._core.registry_lock:
            connections = list(self.connections._connections.values())
        for connection in connections:
            connection.flusher.flush()

    def on_loop(self, connection: ws.Connection, action: Callable[[], object]) -> object:
        """Run `action` on the connection's loop thread and wait for it.

        Returns:
            What `action` returned.
        """
        done = threading.Event()
        result: list[object] = []

        def run() -> None:
            result.append(action())
            done.set()

        assert connection.post(run)
        assert done.wait(TIMEOUT)
        return result[0]

    def subscribed(self, *topics: str) -> None:
        """Wait until the latest connection is authenticated and every topic was subscribed."""

        def named() -> bool:
            sent = {
                t
                for f in self.server.frames()
                if json.loads(f)["action"] == "subscribe"
                for t in json.loads(f)["params"].split(",")
            }
            return set(topics) <= sent

        self.server.wait_for(lambda s: bool(s.connections) and s.connections[-1].auth is not None)
        self.server.wait_for(lambda _: named())

    def unsubscribed(self, *topics: str) -> None:
        """Wait until every topic was unsubscribed."""

        def named() -> bool:
            gone = {
                t
                for f in self.server.frames()
                if json.loads(f)["action"] == "unsubscribe"
                for t in json.loads(f)["params"].split(",")
            }
            return set(topics) <= gone

        self.server.wait_for(lambda _: named())

    def close(self) -> None:
        """Close the session, which closes its connections."""
        self.session.close()
