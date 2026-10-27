"""Tests for the WebSocket transport: the reconcile loop, connections, and live streams.

`Subscriptions` is tested alone, with a recording sender and a manual timer queue, so every step
of the reconcile loop is deterministic. Connections and streams are tested end to end: a real
session whose `WsConnections` reaches the fake Massive server (`tests/_fakes/ws.py`) through
real `WsClient`s, with every client callback recorded so a test waits for exactly the event it
needs. Flushes are explicit (`Harness.flush`): the flush interval is an hour unless a test needs
the flush thread itself.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

import pytest

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.request import HostDecision
from deephaven_massive._enums import AssetClass, ChannelFamily, Recency
from deephaven_massive._ingest import ws
from deephaven_massive._massive import Status, StatusKind, Transport, WsState
from deephaven_massive._namespaces.base import build_query
from deephaven_massive._query import StreamQuery
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
    asset_class=AssetClass.STOCKS,
    channels={None: "T"},
    keys={(W, "stocks/T"): STOCK_KEYS},
)
QUOTES = DatasetSpec(
    name="test.quotes",
    asset_class=AssetClass.STOCKS,
    channels={None: "Q"},
    keys={(W, "stocks/Q"): STOCK_KEYS},
)
FOREX = DatasetSpec(
    name="test.forex",
    asset_class=AssetClass.FOREX,
    channels={None: "C"},
    keys={(W, "forex/C"): SourceKeys(KeySpec("Ticker"), KeySpec("ParticipantTimestamp"))},
)
OPTIONS = DatasetSpec(
    name="test.options",
    asset_class=AssetClass.OPTIONS,
    channels={None: "T"},
    keys={(W, "options/T"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
)
FUTURES = DatasetSpec(
    name="test.futures",
    asset_class=AssetClass.FUTURES,
    channels={None: "T"},
    keys={(W, "futures/T"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
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
