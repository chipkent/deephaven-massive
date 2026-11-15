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

import dataclasses
import inspect
import json
import threading
from typing import TYPE_CHECKING

from typing_extensions import override

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._enums import Recency
from deephaven_massive._errors import UnlistedPairError
from deephaven_massive._ingest import ws, ws_connection
from deephaven_massive._ingest.clients import SessionClients
from deephaven_massive._tables.transports import HostDecision
from deephaven_massive.namespaces._query_building import build_query
from deephaven_massive.queries import SymbolStreamQuery

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from collections.abc import Set as AbstractSet

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.symbols import SymbolsLike
    from deephaven_massive._enums import AssetClass, ChannelFamily
    from deephaven_massive._massive import DriftReport, Reject, Source, Status, WireError, WsState
    from tests._fakes.ws import FakeMassiveServer

TIMEOUT = 15.0
"""Seconds any wait on the session or the server may take before the test fails."""

_DEFAULTS = dm.MassiveConfig(api_key="test-key")
"""The options a session gets unless a test passes its own."""

REALTIME = HostDecision("socket.test", Recency.REALTIME, "test evidence")
"""The host decision every stream gets unless a test passes its own `decide`."""

PAIRS: dict[str, tuple[str, str]] = {
    "X:ADAUSD": ("ADA", "USD"),
    "X:BCDUSD": ("BCD", "USD"),
    "X:BTCUSD": ("BTC", "USD"),
    "X:ETHUSD": ("ETH", "USD"),
    "X:LINKUSD": ("LINK", "USD"),
    "X:SOLUSD": ("SOL", "USD"),
}
"""The crypto pairs Massive's ticker list holds for every `WsSession`, as the stream tests use
them; a test adds others to `WsSession.pair_answers`."""


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
        reconnect_deadline: Callable[[], float | None],
        thread_prefix: str,
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
            reconnect_deadline=reconnect_deadline,
            thread_prefix=thread_prefix,
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
        rows: The rows the futures schedule calendar's REST lookups get, by path template;
            a path not listed answers no rows.
        fetched: Each REST lookup of the futures schedule calendar, `(path, query)`, in
            order; appended under `cond`, so read it under `cond` (in a `wait` predicate) or
            once the calendar is idle.
        pair_answers: What Massive's crypto ticker list says of each crypto ticker, as the
            REST transport's `crypto_pair` would read it: its `(base, quote)`, or an exception
            to raise. A ticker missing from it is refused as unlisted. Change it under `cond`,
            or before the stream that looks the ticker up.
    """

    def __init__(
        self,
        server: FakeMassiveServer,
        *,
        decide: Callable[[AssetClass, ChannelFamily], HostDecision] | None = None,
        flush_interval: float = 3600.0,
        backoff: float = 0.05,
        rest_base_url: str = _DEFAULTS.rest_base_url,
        subscription_confirmation_timeout: float = _DEFAULTS.subscription_confirmation_timeout,
        connection_taken: dm.ConnectionTaken = _DEFAULTS.connection_taken,
        max_outage: float = _DEFAULTS.max_outage,
        noncanonical_symbols: dm.OnError = _DEFAULTS.noncanonical_symbols,
        unlisted_symbols: dm.OnError = _DEFAULTS.unlisted_symbols,
        unreadable_symbols: dm.OnError = _DEFAULTS.unreadable_symbols,
        delayed_subscriptions: AbstractSet[dm.Subscription] = _DEFAULTS.delayed_subscriptions,
    ) -> None:
        """Open the session; nothing connects until a stream verb runs.

        Args:
            server: The fake server every connection reaches.
            decide: The host decision of each `(asset_class, family)`; None for `REALTIME`.
            flush_interval: The session's flush interval, in seconds; an hour by default, so
                rows are published only by `flush()`.
            backoff: Each client's first reconnect backoff, in seconds (twice it at most).
            rest_base_url: As `MassiveConfig` takes it.
            subscription_confirmation_timeout: As `MassiveConfig` takes it.
            connection_taken: As `MassiveConfig` takes it.
            max_outage: As `MassiveConfig` takes it.
            noncanonical_symbols: As `MassiveConfig` takes it.
            unlisted_symbols: As `MassiveConfig` takes it.
            unreadable_symbols: As `MassiveConfig` takes it.
            delayed_subscriptions: As `MassiveConfig` takes it.
        """
        self.server = server
        self.backoff = backoff
        self._decide = decide or (lambda *_: REALTIME)
        self.cond = threading.Condition(threading.RLock())
        self._decisions: list[tuple[AssetClass, ChannelFamily]] = []
        self._journal: list[tuple[object, ...]] = []
        self._urls: list[str] = []
        self._items = 0
        self._statuses: list[Status] = []
        self._states: list[WsState] = []
        self._opens = 0
        self.fetched: list[tuple[str, dict[str, object]]] = []
        self.rows: dict[str, list[dict[str, object]]] = {}
        self.pair_answers: dict[str, tuple[str, str] | Exception] = dict(PAIRS)
        self.config = dataclasses.replace(
            _DEFAULTS,
            flush_interval=flush_interval,
            rest_base_url=rest_base_url,
            subscription_confirmation_timeout=subscription_confirmation_timeout,
            connection_taken=connection_taken,
            max_outage=max_outage,
            noncanonical_symbols=noncanonical_symbols,
            unlisted_symbols=unlisted_symbols,
            unreadable_symbols=unreadable_symbols,
            delayed_subscriptions=delayed_subscriptions,
        )
        self.session = dm.MassiveSession(self.config)
        clients = self.session._core._ports
        assert isinstance(clients, SessionClients)
        self.connections = ws.WsConnections(
            self.session._core,
            resolve_host=self._resolve,
            fetch_rows=self._fetch_rows,
            crypto_pair=self._crypto_pair,
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
    def journal(self) -> list[tuple[object, ...]]:
        """The `set_required` and `subscribe` calls on every client, and each crypto lookup.

        A lookup is `("crypto_pair", ticker, thread name)`, in order with the rest.
        """
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

    def _fetch_rows(
        self, template: str, query: Mapping[str, object], *, bucket: str, cancel: object
    ) -> list[dict[str, object]]:
        """Answer a futures schedule calendar lookup from `rows`, recording it.

        The rate governor's `bucket` and `cancel` are not used: the fake makes no request.

        Returns:
            The rows listed for the path, or none.
        """
        with self.cond:
            self.fetched.append((template, dict(query)))
            self.cond.notify_all()
            return list(self.rows.get(template, []))

    def _crypto_pair(self, symbol: str) -> tuple[str, str]:
        """Split a crypto ticker as the REST transport's lookup in the ticker list does.

        Returns:
            The ticker's `(base, quote)` from `pair_answers`.

        Raises:
            UnlistedPairError: If `pair_answers` does not hold the ticker.
        """
        with self.cond:
            self._journal.append(("crypto_pair", symbol, threading.current_thread().name))
            answer = self.pair_answers.get(symbol)
        if answer is None:
            raise UnlistedPairError(
                f"{symbol!r} cannot be split into its two currencies: Massive's reference data "
                "does not list it"
            )
        if isinstance(answer, Exception):
            raise answer
        return answer

    def _resolve(self, asset_class: AssetClass, family: ChannelFamily) -> HostDecision:
        """Decide a host with the test's `decide`, recording the request.

        Returns:
            The decision.
        """
        with self.cond:
            self._decisions.append((asset_class, family))
        return self._decide(asset_class, family)

    def factory(
        self,
        url: str,
        *,
        sources: Sequence[Source],
        on_open: Callable[[int], None],
        on_status: Callable[[int, Status], None],
        on_data: Callable[[int, str, list[tuple[object, ...]], list[Reject], DriftReport], None],
        on_state: Callable[[WsState, float | WireError | None], None],
        reconnect_deadline: Callable[[], float | None],
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
            reconnect_deadline=reconnect_deadline,
            thread_prefix=self.connections.group.label,
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
                rows, rejects = event[2:4]
                assert isinstance(rows, list)
                assert isinstance(rejects, list)
                self._items += len(rows) + len(rejects)
            elif what == "statuses":
                status = event[1]
                assert isinstance(status, _massive.Status)
                self._statuses.append(status)
            else:
                state = event[0]
                assert isinstance(state, _massive.WsState)
                self._states.append(state)
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

    def query(
        self, spec: DatasetSpec, symbols: SymbolsLike, session: dm.MassiveSession | None = None
    ) -> SymbolStreamQuery:
        """Build the stream query of a dataset no namespace needs to declare.

        Args:
            spec: The dataset.
            symbols: Its symbols.
            session: The session to build it on; None for this wrapper's own.

        Returns:
            The query.
        """
        core = (session or self.session)._core
        return build_query(core, spec, query_type=SymbolStreamQuery, symbols=symbols)

    def connection(self, url: str = "wss://socket.test/stocks") -> ws_connection.Connection:
        """Return the session's connection to `url`.

        Returns:
            The connection.
        """
        with self.connections.group.lock:
            return self.connections.group.connections[url]

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
        with self.connections.group.lock:
            connections = list(self.connections.group.connections.values())
        for connection in connections:
            connection.flusher.flush()

    def on_loop(self, connection: ws_connection.Connection, action: Callable[[], object]) -> object:
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
