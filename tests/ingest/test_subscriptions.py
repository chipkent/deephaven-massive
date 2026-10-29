"""Tests for the subscription protocol: topics and the reconcile loop of one connection.

`Subscriptions` is tested alone, with a recording sender and a manual timer queue, so every step
of the reconcile loop is deterministic.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from deephaven_massive._enums import AssetClass
from deephaven_massive._ingest.subscriptions import (
    Subscriptions,
    channel_of,
    is_wildcard,
    stream_topics,
    wildcard,
)
from deephaven_massive._massive import Status, StatusKind

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    import pytest


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
    subs: Subscriptions
    sender: FakeSender
    timers: FakeTimers
    refused: list[tuple[tuple[str, ...], str]]
    verdicts: list[Status]


class NoAcked:
    """The session's record of `NO_ACK` topics, as `WsConnections.first_no_ack` keeps it."""

    def __init__(self) -> None:
        self.topics: set[str] = set()

    def first(self, topic: str) -> bool:
        first = topic not in self.topics
        self.topics.add(topic)
        return first


def make_loop(
    *, cap: int | None = None, verdict: bool = False, no_acked: NoAcked | None = None
) -> Loop:
    no_acked = no_acked or NoAcked()
    sender, timers = FakeSender(), FakeTimers()
    refused: list[tuple[tuple[str, ...], str]] = []
    verdicts: list[Status] = []

    def step_refused(status: Status) -> bool:
        verdicts.append(status)
        return verdict

    subs = Subscriptions(
        sender,
        call_later=timers.call_later,
        ack_timeout=5.0,
        cap=cap,
        on_refused=lambda topics, message: refused.append((topics, message)),
        on_step_refused=step_refused,
        first_no_ack=no_acked.first,
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
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._ingest.subscriptions"):
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
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._ingest.subscriptions"):
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


def test_covered_contracts_count_against_the_cap() -> None:
    # A wildcard keeps covered contracts off the wire, but they count: it may go at any time.
    loop = make_loop(cap=1)
    loop.subs.add(["T.*"])
    loop.subs.add(["T.O:B"], static=True)
    loop.subs.add(["T.O:A"])
    loop.subs.opened(1)
    assert loop.sender.take() == [("subscribe", 1, ("T.*", "T.O:B"))]
    loop.subs.status(ack("T.*", "T.O:B"))  # T.* now covers T.O:B, which leaves the wire
    assert loop.sender.take() == [("unsubscribe", 1, ("T.O:B",))]
    assert loop.subs.admitted == {"T.O:B"}
    assert loop.subs.deferred == {"T.O:A"}
    loop.subs.remove(["T.*"])
    loop.subs.reconcile()  # the static contract keeps its place; the reactive one waits
    assert loop.subs.deferred == {"T.O:A"}
    assert loop.sender.take() == [("subscribe", 1, ("T.O:B",))]


def test_an_admitted_reactive_contract_keeps_its_place_across_a_reconnect() -> None:
    # Sorted order decides only between new contracts: one admitted before keeps its place.
    loop = make_loop(cap=1)
    loop.subs.add(["T.O:B"])
    loop.subs.opened(1)
    loop.subs.add(["T.O:A"])  # sorts first, but the room is taken
    loop.subs.reconcile()
    assert loop.subs.deferred == {"T.O:A"}
    loop.subs.ended()
    loop.subs.opened(2)
    assert loop.subs.deferred == {"T.O:A"}
    assert loop.sender.take()[-1] == ("subscribe", 2, ("T.O:B",))


def test_a_refusal_of_topics_nobody_wants_reports_nothing() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    loop.subs.opened(1)
    loop.subs.status(refusal("T.IBM"))
    assert loop.refused == []


def test_static_contracts_are_admitted_first_and_never_reshuffled() -> None:
    # A reconnect must not let a reactive table's contracts push static ones off the wire.
    loop = make_loop(cap=3)
    static = ["T.O:C", "T.O:D"]
    loop.subs.add(static, static=True)
    loop.subs.opened(1)
    loop.subs.status(ack(*static))
    loop.subs.add(["T.O:A", "T.O:B"])  # a reactive table's contracts
    loop.subs.reconcile()
    assert loop.subs.deferred == {"T.O:B"}
    assert loop.subs.admitted == {"T.O:A", "T.O:C", "T.O:D"}
    loop.subs.ended()
    loop.subs.opened(2)
    assert loop.subs.deferred == {"T.O:B"}
    loop.subs.add(["T.O:C"], static=True)  # a second static stream of one contract
    loop.subs.remove(["T.O:C"], static=True)  # still static for the first
    loop.subs.reconcile()
    assert loop.subs.deferred == {"T.O:B"}
    loop.subs.remove(static, static=True)  # room frees for the deferred contract
    loop.subs.reconcile()
    assert loop.subs.deferred == frozenset()


def test_the_cap_counts_wanted_topics_before_any_generation() -> None:
    loop = make_loop(cap=1)
    loop.subs.add(["T.O:B"], static=True)
    loop.subs.add(["T.O:A"])
    loop.subs.reconcile()  # no generation: nothing is sent, but the admitted set is known
    assert loop.subs.admitted == {"T.O:B"}
    assert loop.subs.deferred == {"T.O:A"}
    assert loop.sender.take() == []


def test_static_contracts_beyond_the_cap_are_deferred_too() -> None:
    # Only a race past the verb's check can get here; the frame still never crosses the cap.
    loop = make_loop(cap=1)
    loop.subs.add(["T.O:B", "T.O:A"], static=True)
    loop.subs.opened(1)
    assert loop.sender.take() == [("subscribe", 1, ("T.O:A",))]
    assert loop.subs.deferred == {"T.O:B"}


def test_a_no_ack_reconnects_once_per_session_across_connections() -> None:
    session = NoAcked()
    for _connection in range(2):
        loop = make_loop(no_acked=session)
        loop.subs.add(["T.AAPL"])
        loop.subs.opened(1)
        loop.timers.fire()
        loop.timers.fire()
        reconnects = [c for c in loop.sender.take() if c[0] == "reconnect"]
        assert len(reconnects) == (1 if _connection == 0 else 0)


def test_topic_helpers() -> None:
    assert wildcard("T") == "T.*"
    assert is_wildcard("T.*")
    assert not is_wildcard("T.AAPL")
    assert channel_of("C.EUR-USD") == "C"
    assert stream_topics(AssetClass.FOREX, "C", ["C:EURUSD"], pairs={}) == {"C.EUR-USD"}
