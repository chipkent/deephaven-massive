"""Tests for the WebSocket transport: the reconcile loop, connections, and live streams.

`Subscriptions` is tested alone, with a recording sender and a manual timer queue, so every step
of the reconcile loop is deterministic. Connections and streams are tested end to end: a real
session whose `WsConnections` reaches the fake Massive server (`tests/_fakes/ws.py`) through
real `WsClient`s, with every client callback recorded so a test waits for exactly the event it
needs. Flushes are explicit (`Harness.flush`): the flush interval is an hour unless a test needs
the flush thread itself.
"""

from __future__ import annotations

import gc
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

import pytest
from deephaven import dtypes, input_table, new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.request import HostDecision
from deephaven_massive._enums import AssetClass, ChannelFamily, Recency
from deephaven_massive._ingest import ws
from deephaven_massive._massive import Status, StatusKind, Transport, WsState
from deephaven_massive._namespaces.base import build_query
from deephaven_massive._query import StreamQuery
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive._tables.watchlist import SymbolWatch
from deephaven_massive._types import override

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

    from deephaven.table import Table

    from deephaven_massive._massive import DriftReport, Reject, Source, WireError
    from tests._fakes.ws import FakeMassiveServer

TIMEOUT = 15.0
W = Transport.WS


# --- Subscriptions: the reconcile loop alone ---------------------------------------------------


class FakeSender:
    """Records what `Subscriptions` asks the client to send, in order."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, int, tuple[str, ...]]] = []

    def subscribe(self, generation: int, topics: Sequence[str]) -> None:
        self.calls.append(("subscribe", generation, tuple(topics)))

    def unsubscribe(self, generation: int, topics: Sequence[str]) -> None:
        self.calls.append(("unsubscribe", generation, tuple(topics)))

    def reconnect(self) -> None:
        self.calls.append(("reconnect", 0, ()))

    def take(self) -> list[tuple[str, int, tuple[str, ...]]]:
        calls, self.calls = self.calls, []
        return calls


@dataclass
class FakeTimer:
    callback: Callable[..., None]
    args: tuple[object, ...]
    cancelled: bool = False

    def cancel(self) -> None:
        self.cancelled = True


@dataclass
class FakeTimers:
    """A manual `call_later`: timers run only when a test fires them."""

    timers: list[FakeTimer] = field(default_factory=list)

    def call_later(self, delay: float, callback: Callable[..., None], *args: object) -> FakeTimer:
        assert delay == 5.0
        timer = FakeTimer(callback, args)
        self.timers.append(timer)
        return timer

    def live(self) -> list[FakeTimer]:
        return [t for t in self.timers if not t.cancelled]

    def fire(self) -> None:
        due, self.timers = self.live(), []
        for timer in due:
            if not timer.cancelled:
                timer.callback(*timer.args)


@dataclass
class Loop:
    subs: ws.Subscriptions
    sender: FakeSender
    timers: FakeTimers
    refused: list[tuple[tuple[str, ...], str]]
    verdicts: list[Status]


def make_loop(*, cap: int | None = None, verdict: bool = False) -> Loop:
    sender, timers = FakeSender(), FakeTimers()
    refused: list[tuple[tuple[str, ...], str]] = []
    verdicts: list[Status] = []

    def step_refused(status: Status) -> bool:
        verdicts.append(status)
        return verdict

    subs = ws.Subscriptions(
        sender,
        call_later=timers.call_later,
        ack_timeout=5.0,
        cap=cap,
        on_refused=lambda topics, message: refused.append((topics, message)),
        on_step_refused=step_refused,
        on_change=lambda: None,
    )
    return Loop(subs, sender, timers, refused, verdicts)


def ack(*topics: str) -> Status:
    return Status(kind=StatusKind.ACK, topics=topics, message="subscribed to: ...")


def refusal(*topics: str, message: str = "not authorized") -> Status:
    return Status(kind=StatusKind.REFUSED, topics=topics, message=message)


def test_nothing_is_sent_without_a_generation() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    loop.subs.reconcile()
    assert loop.sender.take() == []
    assert loop.subs.counts() == (1, 0, 0, 0)


def test_each_generation_sends_the_whole_desired_set() -> None:
    loop = make_loop()
    loop.subs.add(["T.MSFT", "T.AAPL"])
    loop.subs.opened(1)
    assert loop.sender.take() == [("subscribe", 1, ("T.AAPL", "T.MSFT"))]
    loop.subs.status(ack("T.AAPL", "T.MSFT"))
    assert loop.subs.counts() == (2, 2, 0, 0)
    loop.subs.ended()
    loop.subs.opened(2)
    assert loop.sender.take() == [("subscribe", 2, ("T.AAPL", "T.MSFT"))]


def test_one_step_is_in_flight_at_a_time() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    loop.subs.opened(1)
    loop.subs.add(["T.MSFT"])
    loop.subs.reconcile()
    assert loop.sender.take() == [("subscribe", 1, ("T.AAPL",))]
    assert loop.subs.counts() == (2, 0, 1, 0)
    loop.subs.status(ack("T.AAPL"))
    assert loop.sender.take() == [("subscribe", 1, ("T.MSFT",))]


def test_an_acknowledgement_of_an_unsent_topic_is_ignored() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    loop.subs.opened(1)
    loop.subs.status(ack("T.MSFT"))
    assert loop.subs.counts() == (1, 0, 1, 0)


def test_an_unparsed_acknowledgement_acknowledges_the_step_and_warns_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL", "T.MSFT"])
    with caplog.at_level(logging.WARNING, logger=ws.__name__):
        loop.subs.opened(1)
        loop.subs.status(ack())
        loop.subs.add(["T.IBM"])
        loop.subs.reconcile()
        loop.subs.status(ack())
    assert loop.subs.counts() == (3, 3, 0, 0)
    assert len([r for r in caplog.records if "named no topic" in r.message]) == 1


def test_removed_topics_are_unsubscribed_after_the_step() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL", "T.MSFT"])
    loop.subs.opened(1)
    loop.subs.remove(["T.MSFT"])
    loop.subs.reconcile()
    assert loop.sender.take() == [("subscribe", 1, ("T.AAPL", "T.MSFT"))]  # in flight: waits
    loop.subs.status(ack("T.AAPL"))  # T.MSFT left D: its watchdog went with it
    assert loop.sender.take() == [("unsubscribe", 1, ("T.MSFT",))]
    assert loop.subs.counts() == (1, 1, 0, 0)


def test_topics_are_counted_per_stream() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    loop.subs.add(["T.AAPL"])
    loop.subs.opened(1)
    loop.subs.status(ack("T.AAPL"))
    loop.subs.remove(["T.AAPL"])
    loop.subs.reconcile()
    assert loop.sender.take() == [("subscribe", 1, ("T.AAPL",))]
    assert loop.subs.wanted
    loop.subs.remove(["T.AAPL"])
    loop.subs.reconcile()
    assert loop.sender.take() == [("unsubscribe", 1, ("T.AAPL",))]
    assert not loop.subs.wanted


def test_topics_are_exact_strings() -> None:
    # T.A is not a prefix of T.AAPL, and T.* covers nothing until acknowledged.
    loop = make_loop()
    loop.subs.add(["T.A", "T.AAPL"])
    loop.subs.opened(1)
    loop.subs.status(ack("T.A"))
    assert loop.subs.counts() == (2, 1, 1, 0)


def test_adding_a_wildcard_makes_before_it_breaks() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    loop.subs.opened(1)
    loop.subs.status(ack("T.AAPL"))
    loop.sender.take()
    loop.subs.add(["T.*"])
    loop.subs.reconcile()
    assert loop.sender.take() == [("subscribe", 1, ("T.*",))]  # T.AAPL stays on the wire
    loop.subs.status(ack("T.*"))
    assert loop.sender.take() == [("unsubscribe", 1, ("T.AAPL",))]
    assert loop.subs.counts() == (2, 1, 0, 0)


def test_a_refused_wildcard_keeps_the_explicit_topics() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    loop.subs.opened(1)
    loop.subs.status(ack("T.AAPL"))
    loop.sender.take()
    loop.subs.add(["T.*"])
    loop.subs.reconcile()
    loop.subs.status(refusal("T.*", message="not authorized: T.*"))
    assert loop.refused == [(("T.*",), "not authorized: T.*")]
    assert loop.sender.take() == [("subscribe", 1, ("T.*",))]  # no unsubscription
    assert loop.subs.counts() == (1, 1, 0, 0)


def test_removing_a_wildcard_waits_for_the_explicit_topics() -> None:
    loop = make_loop()
    loop.subs.add(["T.*", "T.AAPL", "T.MSFT", "T.IBM"])
    loop.subs.opened(1)
    assert loop.sender.take() == [("subscribe", 1, ("T.*", "T.AAPL", "T.IBM", "T.MSFT"))]
    loop.subs.status(ack("T.*", "T.AAPL", "T.IBM", "T.MSFT"))
    assert loop.sender.take() == [("unsubscribe", 1, ("T.AAPL", "T.IBM", "T.MSFT"))]
    loop.subs.remove(["T.*"])
    loop.subs.reconcile()
    # The explicit topics come back first; the wildcard stays until each is resolved.
    assert loop.sender.take() == [("subscribe", 1, ("T.AAPL", "T.IBM", "T.MSFT"))]
    loop.subs.status(ack("T.AAPL"))
    loop.subs.status(refusal("T.IBM"))
    assert loop.sender.take() == []
    loop.timers.fire()  # T.MSFT: the single re-send
    assert loop.sender.take() == [("subscribe", 1, ("T.MSFT",))]
    loop.timers.fire()  # T.MSFT: NO_ACK resolves it, and the wildcard can go
    assert loop.sender.take() == [("reconnect", 0, ()), ("unsubscribe", 1, ("T.*",))]


def test_a_wildcard_removed_before_its_acknowledgement_leaves_after_the_step() -> None:
    loop = make_loop()
    loop.subs.add(["T.*"])
    loop.subs.opened(1)
    loop.subs.remove(["T.*"])
    loop.subs.reconcile()
    loop.subs.status(ack("T.*"))
    assert loop.sender.take() == [
        ("subscribe", 1, ("T.*",)),
        ("unsubscribe", 1, ("T.*",)),
    ]


def test_a_missing_acknowledgement_is_resent_once_then_no_ack_reconnects() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    loop.subs.opened(1)
    loop.sender.take()
    loop.timers.fire()
    assert loop.sender.take() == [("subscribe", 1, ("T.AAPL",))]
    loop.timers.fire()
    assert loop.sender.take() == [("reconnect", 0, ())]
    assert loop.subs.counts() == (1, 0, 0, 1)  # still desired; the stream never fails
    assert loop.refused == []


def test_a_second_no_ack_of_a_topic_does_not_reconnect(caplog: pytest.LogCaptureFixture) -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    for generation in (1, 2):
        loop.subs.opened(generation)
        loop.timers.fire()
        loop.timers.fire()
        loop.subs.ended()
    calls = loop.sender.take()
    assert [c for c in calls if c[0] == "reconnect"] == [("reconnect", 0, ())]
    assert [r.levelno for r in caplog.records if "no acknowledgement" in r.message] == [
        logging.ERROR,
        logging.ERROR,
    ]


def test_a_watchdog_of_an_ended_generation_does_not_escalate() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    loop.subs.opened(1)
    stale = loop.timers.live()
    loop.subs.ended()
    assert all(t.cancelled for t in stale)
    loop.subs.opened(2)
    loop.sender.take()
    stale[0].callback(*stale[0].args)  # a watchdog that ran despite its cancellation
    assert loop.sender.take() == []


def test_a_watchdog_whose_topic_left_the_wire_does_not_escalate() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL", "T.MSFT"])
    loop.subs.opened(1)
    loop.subs.status(ack("T.AAPL"))
    [timer] = loop.timers.live()
    loop.subs.remove(["T.MSFT"])  # cancels T.MSFT's watchdog
    assert timer.cancelled
    timer.callback(*timer.args)
    assert loop.sender.take() == [
        ("subscribe", 1, ("T.AAPL", "T.MSFT")),
        ("unsubscribe", 1, ("T.MSFT",)),
    ]


def test_a_refused_topic_leaves_d_until_a_stream_wants_it_again() -> None:
    loop = make_loop()
    loop.subs.add(["Q.AAPL", "T.AAPL"])
    loop.subs.opened(1)
    loop.subs.status(refusal("Q.AAPL", "T.IBM"))  # T.IBM is nobody's: not reported
    loop.subs.status(ack("T.AAPL"))
    assert loop.refused == [(("Q.AAPL",), "not authorized")]
    assert loop.subs.counts() == (1, 1, 0, 0)
    loop.subs.add(["Q.AAPL"])  # a new stream tries it again
    loop.subs.reconcile()
    assert loop.sender.take()[-1] == ("subscribe", 1, ("Q.AAPL",))


def test_a_refusal_naming_no_topic_refuses_the_step_in_flight() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL", "T.MSFT"])
    loop.subs.opened(1)
    loop.subs.status(ack("T.AAPL", "T.MSFT"))
    loop.subs.add(["Q.AAPL", "Q.MSFT"])
    loop.subs.reconcile()
    loop.subs.status(refusal())
    assert loop.refused == [(("Q.AAPL", "Q.MSFT"), "not authorized")]
    assert len(loop.verdicts) == 1  # the whole step was unresolved: the verdict was asked


def test_a_partly_resolved_step_is_never_a_verdict() -> None:
    loop = make_loop(verdict=True)
    loop.subs.add(["T.AAPL", "T.MSFT"])
    loop.subs.opened(1)
    loop.subs.status(ack("T.AAPL"))
    loop.subs.status(refusal(message="real-time data needs a plan"))
    assert loop.verdicts == []
    assert loop.refused == [(("T.MSFT",), "real-time data needs a plan")]


def test_a_verdict_the_connection_takes_changes_nothing_here() -> None:
    loop = make_loop(verdict=True)
    loop.subs.add(["T.ESZ5"])
    loop.subs.opened(1)
    loop.sender.take()
    loop.subs.status(refusal(message="real-time data needs a plan"))
    assert loop.refused == []
    assert loop.sender.take() == []


def test_a_refusal_with_nothing_in_flight_is_only_logged(caplog: pytest.LogCaptureFixture) -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    loop.subs.opened(1)
    loop.subs.status(ack("T.AAPL"))
    with caplog.at_level(logging.WARNING, logger=ws.__name__):
        loop.subs.status(refusal(message="huh"))
    assert loop.refused == []
    assert "no subscription was in flight: huh" in caplog.text


def test_other_statuses_change_nothing() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    loop.subs.opened(1)
    for kind in (StatusKind.MAX_CONNECTIONS, StatusKind.OTHER):
        loop.subs.status(Status(kind=kind, message="x"))
    assert loop.subs.counts() == (1, 0, 1, 0)


def test_the_cap_admits_contracts_in_sorted_order_and_defers_the_rest() -> None:
    loop = make_loop(cap=2)
    loop.subs.add(["T.O:C", "T.O:A", "T.O:B", "Q.*"])  # another channel's wildcard counts zero
    loop.subs.opened(1)
    assert loop.sender.take() == [("subscribe", 1, ("Q.*", "T.O:A", "T.O:B"))]
    assert loop.subs.deferred == {"T.O:C"}
    assert loop.subs.explicit == 3
    loop.subs.status(ack("Q.*", "T.O:A", "T.O:B"))
    loop.subs.remove(["T.O:A"])  # room frees: the deferred contract is admitted
    loop.subs.reconcile()
    assert loop.subs.deferred == frozenset()
    assert loop.sender.take() == [
        ("subscribe", 1, ("T.O:C",)),
        ("unsubscribe", 1, ("T.O:A",)),
    ]


def test_covered_contracts_count_only_while_on_the_wire() -> None:
    loop = make_loop(cap=1)
    loop.subs.add(["T.*", "T.O:A", "T.O:B"])
    loop.subs.opened(1)
    loop.subs.status(ack("T.*", "T.O:A"))  # T.* covers both: neither counts
    assert loop.subs.deferred == frozenset()
    assert loop.sender.take()[-1] == ("unsubscribe", 1, ("T.O:A",))
    loop.subs.remove(["T.*"])
    loop.subs.reconcile()
    # They re-enter through the cap in sorted order.
    assert loop.subs.deferred == {"T.O:B"}
    assert loop.sender.take() == [("subscribe", 1, ("T.O:A",))]


# --- The live harness: a session on the fake server --------------------------------------------

STOCK_KEYS = SourceKeys(symbol=KeySpec("Ticker"), time=KeySpec("SipTimestamp"))
TRADES = DatasetSpec(
    name="test.trades",
    summary="A test dataset.",
    asset_class=AssetClass.STOCKS,
    channels={None: "T"},
    keys={(W, "stocks/T"): STOCK_KEYS},
)
QUOTES = DatasetSpec(
    name="test.quotes",
    summary="A test dataset.",
    asset_class=AssetClass.STOCKS,
    channels={None: "Q"},
    keys={(W, "stocks/Q"): STOCK_KEYS},
)
FOREX = DatasetSpec(
    name="test.forex",
    summary="A test dataset.",
    asset_class=AssetClass.FOREX,
    channels={None: "C"},
    keys={(W, "forex/C"): SourceKeys(KeySpec("Ticker"), KeySpec("ParticipantTimestamp"))},
)
OPTIONS = DatasetSpec(
    name="test.options",
    summary="A test dataset.",
    asset_class=AssetClass.OPTIONS,
    channels={None: "T"},
    keys={(W, "options/T"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
)
FUTURES = DatasetSpec(
    name="test.futures",
    summary="A test dataset.",
    asset_class=AssetClass.FUTURES,
    channels={None: "T"},
    keys={(W, "futures/T"): SourceKeys(KeySpec("Ticker"), KeySpec("Timestamp"))},
)

QUOTES_FUTURES = DatasetSpec(
    name="test.futures_quotes",
    summary="A test dataset.",
    asset_class=AssetClass.FUTURES,
    channels={None: "Q"},
    keys={(W, "futures/Q"): SourceKeys(KeySpec("Ticker"), KeySpec("Timestamp"))},
)

REALTIME = HostDecision("socket.test", Recency.REALTIME, "test evidence")


class SpyClient(_massive.WsClient):
    """A real client that also records the loop-thread calls the library makes on it."""

    def __init__(
        self,
        url: str,
        journal: list[tuple[object, ...]],
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
        self._journal = journal

    @override
    def set_required(
        self, channel: str, keys: frozenset[str], *, symbol: str | None = None
    ) -> None:
        self._journal.append(("set_required", channel, keys, symbol))
        super().set_required(channel, keys, symbol=symbol)

    @override
    def subscribe(self, generation: int, topics: Sequence[str]) -> None:
        self._journal.append(("subscribe", tuple(topics)))
        super().subscribe(generation, topics)


class Harness:
    """A session whose connections reach the fake server, with every client event recorded."""

    def __init__(
        self,
        server: FakeMassiveServer,
        *,
        decide: Callable[[AssetClass, ChannelFamily], HostDecision] | None = None,
        flush_interval: float = 3600.0,
        backoff: float = 0.05,
        **config: object,
    ) -> None:
        self.server = server
        self.backoff = backoff
        self.decisions: list[tuple[AssetClass, ChannelFamily]] = []
        self._decide = decide or (lambda *_: REALTIME)
        self.cond = threading.Condition()
        self.journal: list[tuple[object, ...]] = []
        self.urls: list[str] = []
        self.frames = 0
        self.statuses: list[Status] = []
        self.states: list[WsState] = []
        self.opens = 0
        self.session = dm.MassiveSession(
            dm.MassiveConfig(
                api_key="test-key",
                flush_interval=flush_interval,
                **cast("dict[str, Any]", config),
            )
        )
        clients: Any = self.session._core._ports
        self.connections = ws.WsConnections(
            self.session._core, resolve_host=self.resolve, client_factory=self.factory
        )
        clients._streams = self.connections
        self.session._core.on_close(self.connections.close)

    def resolve(self, asset_class: AssetClass, family: ChannelFamily) -> HostDecision:
        self.decisions.append((asset_class, family))
        return self._decide(asset_class, family)

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
        self.urls.append(url)
        path = "/" + url.split("/", 3)[3]

        callbacks = {
            "on_open": self._noting(on_open, "opens"),
            "on_status": self._noting(on_status, "statuses"),
            "on_data": self._noting(on_data, "frames"),
            "on_state": self._noting(on_state, "states"),
        }
        return SpyClient(
            self.server.url(path),
            self.journal,
            sources=sources,
            callbacks=callbacks,
            backoff=self.backoff,
        )

    def _noting(self, callback: Callable[..., None], what: str) -> Callable[..., None]:
        def call(*event: object) -> None:
            callback(*event)
            self._note(what, event)

        return call

    def _note(self, what: str, event: tuple[object, ...]) -> None:
        with self.cond:
            if what == "opens":
                self.opens += 1
            elif what == "frames":
                self.frames += 1
            elif what == "statuses":
                self.statuses.append(cast("Status", event[1]))
            else:
                self.states.append(cast("WsState", event[0]))
            self.cond.notify_all()

    def wait(self, predicate: Callable[[Harness], bool]) -> None:
        with self.cond:
            assert self.cond.wait_for(lambda: predicate(self), TIMEOUT)

    def query(self, spec: DatasetSpec, symbols: object) -> StreamQuery:
        q = build_query(self.session._core, spec, symbols=cast("list[str]", symbols))
        assert isinstance(q, StreamQuery)
        return q

    def connection(self, url: str = "wss://socket.test/stocks") -> ws.Connection:
        with self.session._core.registry_lock:
            return self.connections._connections[url]

    def send(self, *items: dict[str, object]) -> None:
        before = self.frames
        self.server.send(json.dumps(list(items)))
        self.wait(lambda h: h.frames > before)

    def flush(self) -> None:
        with self.session._core.registry_lock:
            connections = list(self.connections._connections.values())
        for connection in connections:
            connection.flusher.flush()

    def on_loop(self, connection: ws.Connection, action: Callable[[], object]) -> object:
        done = threading.Event()
        result: list[object] = []

        def run() -> None:
            result.append(action())
            done.set()

        assert connection.post(run)
        assert done.wait(TIMEOUT)
        return result[0]

    def subscribed(self, *topics: str) -> None:
        # Wait until the latest connection has received a subscribe frame naming each topic.
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
        self.session.close()


@pytest.fixture(autouse=True)
def _no_connection_outlives_its_test() -> Iterator[None]:
    # A client left running reconnects forever, and through whatever proxy a later test sets.
    yield
    wait_for(
        lambda: (
            not [
                t
                for t in threading.enumerate()
                if t.name.startswith(("massive-ws ", "massive-flush ")) and t.is_alive()
            ]
        )
    )


@pytest.fixture
def harness(fake_ws_server: FakeMassiveServer) -> Iterator[Harness]:
    h = Harness(fake_ws_server)
    yield h
    h.close()


def trade(symbol: str, price: float = 1.0, **fields: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ev": "T",
        "sym": symbol,
        "x": 4,
        "i": "1",
        "p": price,
        "q": 1,
        "t": 1_700_000_000_000,
        "s": 100,
        "z": 3,
        "ds": "100",
    }
    row.update(fields)
    return row


def wait_for(condition: Callable[[], bool]) -> None:
    # Bounded wait for an update-graph cycle to apply a change.
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(300):
        if condition():
            return
        pause.wait(0.02)
    assert condition()


def column(
    table: Table, name: str, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> list[object]:
    return [row[name] for row in dh_to_pylist(table)]


def streams_of(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    assert harness.session._core.diagnostics.barrier().wait(TIMEOUT)
    return sorted(dh_to_pylist(harness.session.streams()), key=lambda r: r["StreamId"])


def connections_of(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    assert harness.session._core.diagnostics.barrier().wait(TIMEOUT)
    return [r for r in dh_to_pylist(harness.session.connections()) if r["Transport"] == "WS"]


# --- Connections and streams, end to end ------------------------------------------------------


def test_the_channel_keys_are_declared_before_its_first_subscribe(harness: Harness) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    required = ("set_required", "T", frozenset({"Ticker", "SipTimestamp"}), "Ticker")
    assert harness.journal[:2] == [required, ("subscribe", ("T.AAPL",))]
    harness.session.release(result)


def test_a_stream_publishes_only_its_symbols(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    aapl = harness.query(TRADES, ["AAPL"]).append_only()
    every = harness.query(TRADES, dm.ALL).append_only()
    harness.subscribed("T.AAPL", "T.*")
    harness.send(trade("AAPL", 1.5), trade("MSFT", 2.0))
    harness.flush()
    wait_for(lambda: aapl.size == 1 and every.size == 2)
    assert column(aapl, "Price", dh_to_pylist) == [1.5]
    assert sorted(cast("list[str]", column(every, "Symbol", dh_to_pylist))) == ["AAPL", "MSFT"]
    rows = streams_of(harness, dh_to_pylist)
    assert [(r["Symbols"], r["RowsPublished"], r["Host"], r["Path"]) for r in rows] == [
        ("AAPL", 1, "socket.test", "/stocks"),
        ("ALL", 2, "socket.test", "/stocks"),
    ]


def test_equal_queries_share_one_stream_and_one_connection(harness: Harness) -> None:
    first = harness.query(TRADES, ["AAPL"]).blink()
    second = harness.query(TRADES, ["aapl"]).last_by()
    quotes = harness.query(QUOTES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL", "Q.AAPL")
    assert len(harness.urls) == 1
    assert harness.decisions == [(AssetClass.STOCKS, ChannelFamily.TICK)] * 2
    assert harness.connection().leases == 2
    for result in (first, second, quotes):
        harness.session.release(result)


def test_wire_symbols_become_canonical(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    result = harness.query(FOREX, ["C:EURUSD"]).append_only()
    harness.subscribed("C.EUR/USD")
    harness.send({"ev": "C", "p": "EUR/USD", "a": 1.1, "b": 1.0, "t": 1_700_000_000_000, "x": 1})
    harness.connection("wss://socket.test/forex").flusher.flush()
    wait_for(lambda: result.size == 1)
    assert column(result, "Symbol", dh_to_pylist) == ["C:EURUSD"]


def test_a_refused_topic_fails_only_its_streams(
    harness: Harness,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    fake_ws_server.refuse["Q.AAPL"] = "not authorized to access Q.AAPL"
    trades = harness.query(TRADES, ["AAPL"]).append_only()
    harness.subscribed("T.AAPL")
    quotes = harness.query(QUOTES, ["AAPL"]).blink()
    harness.wait(lambda h: any(s.kind is StatusKind.REFUSED for s in h.statuses))
    harness.flush()
    rows = streams_of(harness, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED"]
    assert "socket.test/stocks refused Q.AAPL: not authorized to access Q.AAPL" in rows[1]["Error"]
    assert "MassiveConfig.ws_hosts[AssetClass.STOCKS]" in rows[1]["Error"]
    harness.send(trade("AAPL"))
    harness.flush()
    wait_for(lambda: trades.size == 1)  # the connection kept running
    del quotes


def test_a_rejected_row_fails_only_the_streams_of_its_symbol(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    aapl = harness.query(TRADES, ["AAPL"]).append_only()
    msft = harness.query(TRADES, ["MSFT"]).blink()
    every = harness.query(TRADES, dm.ALL).blink()
    harness.subscribed("T.AAPL", "T.MSFT", "T.*")
    bad = trade("MSFT")
    del bad["t"]  # a required key
    harness.send(bad, trade("AAPL"))
    harness.flush()
    wait_for(lambda: aapl.size == 1)
    rows = streams_of(harness, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED", "FAILED"]
    assert "no SipTimestamp" in rows[1]["Error"]
    del msft, every


def test_a_row_without_a_symbol_fails_only_the_all_streams(
    harness: Harness,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    aapl = harness.query(TRADES, ["AAPL"]).blink()
    every = harness.query(TRADES, dm.ALL).blink()
    harness.subscribed("T.AAPL", "T.*")
    nameless = trade("AAPL")
    del nameless["sym"]
    unreadable = trade("not a symbol")
    del unreadable["t"]
    with caplog.at_level(logging.ERROR, logger=ws.__name__):
        harness.send(nameless, unreadable)
    harness.flush()
    rows = streams_of(harness, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED"]
    assert caplog.text.count("has no valid symbol; failing the channel's ALL streams") == 2
    del aapl, every


def test_a_timestamp_out_of_bounds_fails_only_its_streams(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    aapl = harness.query(TRADES, ["AAPL"]).append_only()
    msft = harness.query(TRADES, ["MSFT"]).blink()
    harness.subscribed("T.AAPL", "T.MSFT")
    harness.send(trade("MSFT", t=5), trade("AAPL"))
    harness.flush()
    wait_for(lambda: aapl.size == 1)
    rows = streams_of(harness, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED"]
    assert "timestamp unit mismatch for test.trades.Timestamp: 5 ms" in rows[1]["Error"]
    del msft


def test_drift_is_logged_once_and_shown(
    harness: Harness,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    with caplog.at_level(logging.WARNING):
        harness.send(trade("AAPL", zz=1))
        harness.send(trade("AAPL", zz=2))
        harness.flush()
    assert caplog.text.count("field 'zz' is not in the schema") == 1
    [connection] = connections_of(harness, dh_to_pylist)
    assert connection["SchemaDrift"] == "T:zz+"
    [stream] = streams_of(harness, dh_to_pylist)
    assert stream["SchemaDrift"] == "WS:zz+"
    harness.session.release(result)


def test_releasing_the_last_result_unsubscribes(harness: Harness) -> None:
    first = harness.query(TRADES, ["AAPL"]).blink()
    second = harness.query(TRADES, ["AAPL"]).ring(5)
    harness.subscribed("T.AAPL")
    harness.session.release(first)
    harness.session.release(second)
    harness.unsubscribed("T.AAPL")
    assert harness.connection().leases == 0


def test_a_derived_table_keeps_the_stream_alive(
    harness: Harness, fake_ws_server: FakeMassiveServer
) -> None:
    result = harness.query(TRADES, ["AAPL"]).append_only()
    harness.subscribed("T.AAPL")
    scope = new_scope()  # the user's own scope, released below
    with scope.open():
        derived = [result.where("Price > 0")]
    harness.session.release(result)
    del result
    gc.collect()
    harness.send(trade("AAPL"))
    harness.flush()
    wait_for(lambda: derived[0].size == 1)
    assert not any(json.loads(f)["action"] == "unsubscribe" for f in fake_ws_server.frames())
    derived.clear()
    scope.release()
    harness.unsubscribed("T.AAPL")


def test_close_returns_promptly_during_a_backoff_wait(fake_ws_server: FakeMassiveServer) -> None:
    harness = Harness(fake_ws_server, backoff=60.0)
    fake_ws_server.reject_next(503)
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.wait(lambda h: WsState.RECONNECTING in h.states)
    started = time.monotonic()
    harness.close()
    assert time.monotonic() - started < 5.0
    assert WsState.CLOSED in harness.states
    del result


def test_a_rejected_key_fails_every_stream(
    harness: Harness,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    fake_ws_server.auth_replies.append("auth_failed")
    q = harness.query(TRADES, ["AAPL"])
    result = q.blink()
    harness.wait(lambda h: WsState.FAILED in h.states)
    with harness.session._core.registry_lock:
        failed = harness.connections._streams[1]
        assert harness.connections._connections == {}  # retired: the next stream opens anew
    assert failed.flusher.join(TIMEOUT)  # its last flush applied the failures, unasked
    [row] = connections_of(harness, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert "rejected the API key" in row["LastError"]
    [stream] = streams_of(harness, dh_to_pylist)
    assert stream["State"] == "FAILED"
    again = q.blink()  # a fresh stream, on a fresh connection
    assert len(harness.urls) == 2
    del result, again


def test_a_fatal_handshake_names_the_status_and_host(
    harness: Harness,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    fake_ws_server.reject_next(404)
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.wait(lambda h: WsState.FAILED in h.states)
    [row] = connections_of(harness, dh_to_pylist)
    assert row["LastError"].startswith("the WebSocket connection to socket.test/stocks failed:")
    assert "404" in row["LastError"]
    del result


def test_a_reconnect_resends_the_desired_set(
    harness: Harness,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    fake_ws_server.close(1011)
    harness.wait(lambda h: h.opens == 2)
    harness.subscribed("T.AAPL")  # on the new connection
    harness.wait(lambda h: sum(s.kind is StatusKind.ACK for s in h.statuses) == 2)
    [row] = connections_of(harness, dh_to_pylist)
    assert (row["State"], row["Reconnects"], row["Topics"], row["AckedTopics"]) == (
        "CONNECTED",
        1,
        1,
        1,
    )
    assert WsState.RECONNECTING in harness.states
    harness.session.release(result)


def test_an_idle_connection_closes_and_a_new_stream_opens_another(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    monkeypatch.setattr(ws, "IDLE_GRACE", 0.05)
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    harness.session.release(result)
    harness.wait(lambda h: WsState.CLOSED in h.states)
    [row] = connections_of(harness, dh_to_pylist)
    assert row["State"] == "CLOSED"
    again = harness.query(TRADES, ["MSFT"]).blink()
    harness.subscribed("T.MSFT")
    assert len(harness.urls) == 2
    harness.session.release(again)


def test_an_opening_stream_cancels_the_idle_close(harness: Harness) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    connection = harness.connection()
    harness.subscribed("T.AAPL")
    harness.session.release(result)
    harness.unsubscribed("T.AAPL")
    with harness.session._core.registry_lock:
        connection.leases += 1  # a stream is opening
    retired = harness.on_loop(connection, lambda: harness.connections.retire(connection, idle=True))
    assert retired is False
    with harness.session._core.registry_lock:
        connection.leases -= 1


def test_a_missing_acknowledgement_reconnects(fake_ws_server: FakeMassiveServer) -> None:
    harness = Harness(fake_ws_server, ack_timeout=0.05)
    try:
        fake_ws_server.silent.add("T.AAPL")
        result = harness.query(TRADES, ["AAPL"]).blink()
        harness.wait(lambda h: h.opens >= 2)
        assert [json.loads(f)["params"] for f in fake_ws_server.frames(0)] == ["T.AAPL", "T.AAPL"]
        del result
    finally:
        harness.close()


def test_a_reactive_table_follows_its_rows(
    harness: Harness,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["aapl", "not a symbol!"])]))
    result = harness.query(TRADES, watchlist).append_only()
    harness.subscribed("T.AAPL")
    harness.send(trade("AAPL"), trade("MSFT"))
    harness.flush()
    wait_for(lambda: result.size == 1)
    watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
    harness.subscribed("T.MSFT")
    watchlist.delete(new_table([string_col("Symbol", ["aapl"])]))
    harness.unsubscribed("T.AAPL")
    harness.send(trade("AAPL", 2.0), trade("MSFT", 3.0))
    harness.flush()
    wait_for(lambda: result.size == 2)
    assert column(result, "Symbol", dh_to_pylist) == ["AAPL", "MSFT"]
    # The row that is no valid symbol never became a topic.
    sent = [json.loads(f)["params"] for f in fake_ws_server.frames()]
    assert sent == ["T.AAPL", "T.MSFT", "T.AAPL"]


def test_a_derived_table_keeps_following_the_watchlist(harness: Harness) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    result = harness.query(TRADES, watchlist).append_only()
    harness.subscribed("T.AAPL")
    derived = result.where("Price > 0")
    harness.session.release(result)
    del result
    gc.collect()
    watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
    harness.subscribed("T.MSFT")
    harness.send(trade("MSFT"))
    harness.flush()
    wait_for(lambda: derived.size == 1)


def test_static_options_beyond_the_cap_raise(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 2)
    contracts = ["O:SPY251219C00600000", "O:SPY251219C00610000", "O:SPY251219C00620000"]
    with pytest.raises(ValueError, match="3 new option contracts and 0 in use exceed the 2"):
        harness.query(OPTIONS, contracts).blink()
    first = harness.query(OPTIONS, contracts[:2]).blink()
    with pytest.raises(ValueError, match="1 new option contracts and 2 in use"):
        harness.query(OPTIONS, contracts[2:]).blink()
    every = harness.query(OPTIONS, dm.ALL).blink()  # a wildcard counts zero
    harness.session.release(first)
    harness.session.release(every)


def test_reactive_options_beyond_the_cap_are_deferred(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 1)
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    contracts = ["O:SPY251219C00610000", "O:SPY251219C00600000"]
    watchlist.add(new_table([string_col("Symbol", contracts)]))
    result = harness.query(OPTIONS, watchlist).blink()
    harness.subscribed("T.O:SPY251219C00600000")
    connection = harness.connection("wss://socket.test/options")
    harness.on_loop(connection, lambda: None)
    [row] = streams_of(harness, dh_to_pylist)
    assert (row["State"], row["Deferred"], row["Error"]) == (
        "RUNNING",
        1,
        "over capacity: 2/1; 1 deferred",
    )
    watchlist.delete(new_table([string_col("Symbol", ["O:SPY251219C00600000"])]))
    harness.subscribed("T.O:SPY251219C00610000")
    harness.on_loop(connection, lambda: None)
    [row] = streams_of(harness, dh_to_pylist)
    assert (row["Deferred"], row["Error"]) == (0, None)
    del result


def unknown_futures(*_: object) -> HostDecision:
    return HostDecision("socket.test", Recency.UNKNOWN, "no timeframe in futures snapshot")


def test_the_real_time_verdict_moves_the_connection_once(
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    harness = Harness(fake_ws_server, decide=unknown_futures)
    try:
        harness.connections._reconnect_backoff = 0.01
        fake_ws_server.step_refusals.append("Real-time data requires an upgrade")
        result = harness.query(FUTURES, ["ESZ5"]).blink()
        harness.wait(lambda h: h.opens == 2)
        assert harness.urls == ["wss://socket.test/futures", "wss://delayed.test/futures"]
        harness.subscribed("T.ESZ5")
        [row] = connections_of(harness, dh_to_pylist)
        assert (row["Host"], row["Recency"]) == ("delayed.test", "DELAYED")
        assert row["Evidence"] == "server verdict: Real-time data requires an upgrade"
        [stream] = streams_of(harness, dh_to_pylist)
        assert stream["State"] == "RUNNING"
        other = harness.query(FUTURES, ["NQZ5"]).blink()  # the verdict is cached
        assert len(harness.decisions) == 1
        harness.session.release(result)
        harness.session.release(other)
    finally:
        harness.close()


def realtime_futures(*_: object) -> HostDecision:
    return HostDecision("socket.test", Recency.REALTIME, "timeframe=REAL-TIME")


@pytest.mark.parametrize(
    ("decide", "pinned"),
    [(unknown_futures, {AssetClass.FUTURES: "socket.test"}), (realtime_futures, {})],
)
def test_a_verdict_otherwise_fails_the_step(
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    decide: Callable[[AssetClass, ChannelFamily], HostDecision],
    pinned: dict[AssetClass, str],
) -> None:
    harness = Harness(fake_ws_server, decide=decide, ws_hosts=pinned)
    try:
        fake_ws_server.step_refusals.append("Real-time data requires an upgrade")
        result = harness.query(FUTURES, ["ESZ5"]).blink()
        harness.wait(lambda h: any(s.kind is StatusKind.REFUSED for s in h.statuses))
        harness.connection("wss://socket.test/futures").flusher.flush()
        [stream] = streams_of(harness, dh_to_pylist)
        assert stream["State"] == "FAILED"
        assert len(harness.urls) == 1
        del result
    finally:
        harness.close()


def test_a_failed_open_returns_its_lease(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(*_: object, **__: object) -> object:
        raise RuntimeError("no tables")

    monkeypatch.setattr(ws, "open_stream_tables", broken)
    with pytest.raises(RuntimeError, match="no tables"):
        harness.query(TRADES, ["AAPL"]).blink()
    assert harness.connection().leases == 0
    assert harness.connections._streams == {}


def test_a_watch_that_cannot_start_releases_the_stream(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(self: object) -> None:
        raise RuntimeError("no watch")

    monkeypatch.setattr(SymbolWatch, "start", broken)
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    with pytest.raises(RuntimeError, match="no watch"):
        harness.query(TRADES, watchlist).blink()
    assert harness.connection().leases == 0


def test_a_lost_race_closes_the_spare_connection(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    barrier = threading.Barrier(2)
    built: list[ws.Connection] = []
    real: Callable[..., ws.Connection] = ws.Connection

    def racing(*args: object, **kwargs: object) -> ws.Connection:
        connection = real(*args, **kwargs)
        built.append(connection)
        barrier.wait(TIMEOUT)
        return connection

    monkeypatch.setattr(ws, "Connection", racing)
    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(lambda s: harness.query(TRADES, [s]).blink(), ["AAPL", "MSFT"]))
    assert len(built) == 2
    assert len(harness.connections._connections) == 1
    spare = next(c for c in built if c not in harness.connections._connections.values())
    assert spare.join(TIMEOUT)
    for result in results:
        harness.session.release(result)


def test_a_closed_transport_opens_nothing(harness: Harness) -> None:
    request = harness.query(TRADES, ["AAPL"])._request
    harness.connections.close()
    with pytest.raises(dm.MassiveError, match="is closed"):
        harness.connections.open_stream(
            harness.session._core, TRADES, request, channel="T", stream_id=99
        )


def test_a_shutdown_after_the_connection_ended_is_logged(
    harness: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    connection = harness.connection()
    connection.close()
    assert connection.join(TIMEOUT)
    with caplog.at_level(logging.INFO, logger=ws.__name__):
        harness.session.release(result)
        wait_for(lambda: "has closed; dropped release_stream" in caplog.text)


def test_a_failing_shutdown_is_logged(
    harness: Harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def broken(stream_id: int) -> None:
        raise RuntimeError(f"registry {stream_id}")

    monkeypatch.setattr(harness.session._core, "stream_shut_down", broken)
    harness.connections._shut_down(42)
    assert "the shutdown of stream 42 failed" in caplog.text


def test_the_flush_thread_publishes_by_itself(fake_ws_server: FakeMassiveServer) -> None:
    harness = Harness(fake_ws_server, flush_interval=0.01)
    try:
        result = harness.query(TRADES, ["AAPL"]).append_only()
        harness.subscribed("T.AAPL")
        harness.send(trade("AAPL"))
        wait_for(lambda: result.size == 1)
    finally:
        harness.close()


def spare_connection(harness: Harness, host: str) -> ws.Connection:
    return ws.Connection(
        harness.connections,
        harness.session._core,
        url=f"wss://{host}/stocks",
        asset_class=AssetClass.STOCKS,
        family=ChannelFamily.TICK,
        decision=REALTIME,
        client_factory=harness.factory,
    )


def test_a_delayed_start_waits_and_close_cancels_it(harness: Harness) -> None:
    cancelled = spare_connection(harness, "never.test")
    cancelled.start(delay=60.0)
    cancelled.start()  # a second start does nothing
    cancelled.close()
    cancelled.start()  # nor does a start after close
    assert cancelled.join(TIMEOUT)
    assert harness.opens == 0
    late = spare_connection(harness, "late.test")
    late.start(delay=0.01)
    harness.wait(lambda h: h.opens == 1)
    late.close()
    assert late.join(TIMEOUT)


def test_changes_for_a_moved_stream_follow_it(harness: Harness) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    result = harness.query(TRADES, watchlist).blink()
    old = harness.connection()
    successor = spare_connection(harness, "next.test")
    successor.start()
    with harness.session._core.registry_lock:
        harness.connections._streams[1] = successor
    harness.on_loop(old, lambda: old.hand_over(successor))
    assert old.join(TIMEOUT)
    # The old loop has ended, so its methods run here, as a late callback would have there.
    old.change_symbols(1, frozenset({"AAPL"}), frozenset())
    harness.subscribed("T.AAPL")  # on the successor
    old.release_stream(1)
    harness.unsubscribed("T.AAPL")
    successor.close()
    del result


def test_stress_concurrent_verbs_and_releases(harness: Harness) -> None:
    # 16 threads open and release overlapping streams at once; every topic is released.
    symbols = [f"S{i}" for i in range(8)]

    def churn(worker: int) -> None:
        for round_ in range(10):
            symbol = symbols[(worker + round_) % len(symbols)]
            result = harness.query(TRADES, [symbol]).blink()
            harness.session.release(result)

    with ThreadPoolExecutor(16) as pool:
        list(pool.map(churn, range(16)))
    core = harness.session._core
    wait_for(lambda: not harness.connections._streams)
    connection = harness.connection()
    assert harness.on_loop(connection, lambda: connection.subscriptions.wanted) is False
    assert connection.leases == 0
    wait_for(lambda: core._streams == {})


# --- Edge cases of the connection's callbacks -------------------------------------------------


def test_a_refusal_of_topics_nobody_wants_reports_nothing() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    loop.subs.opened(1)
    loop.subs.status(refusal("T.IBM"))
    assert loop.refused == []


def test_late_callbacks_of_an_unknown_stream_are_ignored(harness: Harness) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    connection = harness.connection()
    harness.subscribed("T.AAPL")
    harness.on_loop(connection, lambda: connection.release_stream(999))
    harness.on_loop(connection, lambda: connection.change_symbols(999, frozenset(), frozenset()))
    harness.connections._symbols_changed(999, frozenset({"X"}), frozenset())
    harness.on_loop(connection, connection._idle_close)  # a stream is open: it stays
    harness.on_loop(
        connection, lambda: connection._on_status(999, ack("T.AAPL"))
    )  # another generation's status
    assert harness.connection() is connection
    harness.session.release(result)


def test_a_rejected_row_nobody_receives_fails_nothing(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    bad = trade("MSFT")
    del bad["t"]
    harness.send(bad)
    harness.flush()
    [row] = streams_of(harness, dh_to_pylist)
    assert row["State"] == "RUNNING"
    harness.session.release(result)


def test_last_message_rows_are_throttled(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    connection = harness.connection()
    written: list[int] = []
    monkeypatch.setattr(connection, "_write_row", lambda: written.append(1))
    harness.send(trade("AAPL"))
    harness.send(trade("AAPL"))
    assert len(written) <= 1
    harness.session.release(result)


def test_an_index_value_refusal_names_the_business_host(harness: Harness) -> None:
    connection = ws.Connection(
        harness.connections,
        harness.session._core,
        url="wss://socket.test/indices",
        asset_class=AssetClass.INDICES,
        family=ChannelFamily.TICK,
        decision=REALTIME,
        client_factory=harness.factory,
    )
    connection._refused(("V.I:SPX",), "not authorized")
    assert connection._last_error is not None
    assert "index values may need business.massive.com" in connection._last_error
    assert "ws_hosts[AssetClass.INDICES]" in connection._last_error
    connection.close()
    assert connection.join(TIMEOUT)


def test_a_connection_whose_session_is_gone_writes_nothing(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = harness.query(OPTIONS, ["O:SPY251219C00600000"]).blink()
    harness.subscribed("T.O:SPY251219C00600000")
    connection = harness.connection("wss://socket.test/options")
    monkeypatch.setattr(connection, "_core", lambda: None)
    monkeypatch.setattr(harness.connections, "_core", lambda: None)
    harness.on_loop(connection, connection._write_row)
    harness.on_loop(connection, connection._show_deferred)
    drift = _massive.DriftReport(extra=frozenset({"zz"}))
    harness.on_loop(connection, lambda: connection._record_drift("T", drift))
    harness.connections._shut_down(1)
    assert harness.on_loop(connection, lambda: harness.connections.retire(connection, idle=False))
    assert harness.connections.reroute(connection, "real-time") is False
    connection.close()  # retired: the session's close no longer reaches it
    assert connection.join(TIMEOUT)
    del result


def test_retiring_a_replaced_connection_keeps_its_replacement(harness: Harness) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    connection = harness.connection()
    spare = spare_connection(harness, "spare.test")
    assert harness.connections.retire(spare, idle=False)
    assert harness.connection() is connection
    spare.close()
    assert spare.join(TIMEOUT)
    harness.session.release(result)


def test_a_verdict_is_taken_once(fake_ws_server: FakeMassiveServer) -> None:
    harness = Harness(fake_ws_server, decide=unknown_futures)
    try:
        result = harness.query(FUTURES, ["ESZ5"]).blink()
        other = harness.query(TRADES, ["AAPL"]).blink()  # a stream the move leaves alone
        connection = harness.connection("wss://socket.test/futures")
        harness.connections._verdicts[AssetClass.FUTURES, ChannelFamily.TICK] = REALTIME
        assert harness.connections.reroute(connection, "real-time") is False
        harness.connections._verdicts.clear()
        with harness.session._core.registry_lock:
            del harness.connections._connections[connection.url]  # already replaced
        harness.connections._reconnect_backoff = 0.01
        assert harness.on_loop(connection, lambda: harness.connections.reroute(connection, "x"))
        harness.wait(lambda h: h.urls[-1] == "wss://delayed.test/futures")
        with harness.session._core.registry_lock:
            owners = dict(harness.connections._streams)
        assert owners[1].url == "wss://delayed.test/futures"
        assert owners[2].url == "wss://socket.test/stocks"
        harness.session.release(result)
        harness.session.release(other)
    finally:
        harness.close()


def test_a_connection_that_does_not_end_is_reported(
    harness: Harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    connection = harness.connection()
    monkeypatch.setattr(ws, "JOIN_TIMEOUT", 0.01)
    monkeypatch.setattr(connection, "join", lambda _: False)
    harness.connections.close()
    assert "has not ended 0 s after close()" in caplog.text
    del result


def test_the_default_factory_builds_real_clients(harness: Harness) -> None:
    factory = ws._WsClientFactory("test-key", backoff=15.0, max_backoff=30.0)
    client = factory(
        "wss://socket.test/stocks",
        sources=[],
        on_open=lambda *_: None,
        on_status=lambda *_: None,
        on_data=lambda *_: None,
        on_state=lambda *_: None,
    )
    assert isinstance(client, _massive.WsClient)
    client.close()
    client.start()
    assert client.join(TIMEOUT)
    assert "test-key" not in repr(factory)


def test_a_timer_start_after_close_does_nothing(harness: Harness) -> None:
    connection = spare_connection(harness, "never.test")
    connection.close()
    connection._start_later()  # the timer fired just as close() cancelled it
    assert connection.join(TIMEOUT)
    assert harness.opens == 0


def test_a_new_stream_cancels_a_pending_idle_close(harness: Harness) -> None:
    first = harness.query(TRADES, ["AAPL"]).blink()
    connection = harness.connection()
    harness.subscribed("T.AAPL")
    harness.session.release(first)
    harness.unsubscribed("T.AAPL")
    assert harness.on_loop(connection, lambda: connection._idle is not None)
    second = harness.query(TRADES, ["MSFT"]).blink()
    harness.subscribed("T.MSFT")
    assert harness.on_loop(connection, lambda: connection._idle is None)
    harness.session.release(second)


def test_every_message_rewrites_the_row_once_the_interval_passed(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    connection = harness.connection()
    written: list[int] = []
    monkeypatch.setattr(ws, "_MESSAGE_ROW_NS", 0)
    monkeypatch.setattr(connection, "_write_row", lambda: written.append(1))
    harness.send(trade("AAPL"))
    assert written  # the message alone rewrote the row
    harness.session.release(result)


def test_contracts_of_opening_streams_count_against_the_cap(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 2)
    first = harness.query(OPTIONS, ["O:SPY251219C00600000"]).blink()
    connection = harness.connection("wss://socket.test/options")
    with harness.session._core.registry_lock:
        connection.cap_pending[99] = frozenset({"T.O:SPY251219C00610000"})  # still opening
        with pytest.raises(ValueError, match="1 new option contracts and 2 in use"):
            ws.WsConnections._check_cap(OPTIONS, connection, frozenset({"T.O:SPY251219C00620000"}))
        del connection.cap_pending[99]
    harness.session.release(first)


def test_the_documented_trade_frame_is_published(
    harness: Harness,
    fake_ws_server: FakeMassiveServer,
    *,
    load_fixture: Callable[[str, str], bytes],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    # The frame of Massive's WebSocket quickstart: no `q`, `ds`, or `trfi`, which read as null.
    result = harness.query(TRADES, ["MSFT"]).append_only()
    harness.subscribed("T.MSFT")
    before = harness.frames
    fake_ws_server.send(load_fixture("ingest/ws", "stocks_trades.json").decode())
    harness.wait(lambda h: h.frames > before)
    harness.flush()
    wait_for(lambda: result.size == 2)
    rows = dh_to_pylist(result)
    assert [(r["Symbol"], r["Price"], r["Size"], r["Conditions"]) for r in rows] == [
        ("MSFT", 215.9721, 100, None),
        ("MSFT", 215.989, 1, [37]),
    ]
    assert rows[0]["Timestamp"].value == 1_611_082_428_813_000_000


def test_a_verdict_while_closing_fails_the_step(
    fake_ws_server: FakeMassiveServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = Harness(fake_ws_server, decide=unknown_futures)
    try:
        result = harness.query(FUTURES, ["ESZ5"]).blink()
        connection = harness.connection("wss://socket.test/futures")
        closed = iter([False, True])  # open at the first check, closed by the second
        monkeypatch.setattr(harness.connections, "_is_closed", lambda: next(closed))
        assert harness.connections.reroute(connection, "real-time") is False
        del result
    finally:
        harness.close()


def test_a_verdict_joins_an_open_delayed_connection(fake_ws_server: FakeMassiveServer) -> None:
    decisions = iter(
        [
            HostDecision("socket.test", Recency.UNKNOWN, "no timeframe"),
            HostDecision("delayed.test", Recency.DELAYED, "a stream already reads it"),
        ]
    )
    harness = Harness(fake_ws_server, decide=lambda *_: next(decisions))
    try:
        harness.connections._reconnect_backoff = 0.01
        result = harness.query(FUTURES, ["ESZ5"]).blink()
        harness.connections._verdicts.clear()
        delayed = harness.query(QUOTES_FUTURES, ["ESZ5"]).blink()
        moved = harness.connection("wss://socket.test/futures")
        existing = harness.connection("wss://delayed.test/futures")
        assert harness.on_loop(moved, lambda: harness.connections.reroute(moved, "real-time"))
        assert harness.connection("wss://delayed.test/futures") is existing
        assert existing.leases == 2
        harness.session.release(result)
        harness.session.release(delayed)
    finally:
        harness.close()
