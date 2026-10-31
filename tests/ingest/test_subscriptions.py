"""Tests for the subscription protocol: topics and the reconcile loop of one connection.

`Subscriptions` is tested alone, with a recording sender and a manual timer queue, so every step
of the reconcile loop is deterministic.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from deephaven_massive._config import NO_ACK_RESEND
from deephaven_massive._enums import AssetClass
from deephaven_massive._ingest import subscriptions
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

    def take(self) -> list[tuple[str, int, tuple[str, ...]]]:
        calls, self.calls = self.calls, []
        return calls


@dataclass
class FakeTimer:
    callback: Callable[..., None]
    args: tuple[object, ...]
    delay: float
    cancelled: bool = False

    def cancel(self) -> None:
        self.cancelled = True


@dataclass
class FakeTimers:
    """A manual `call_later`: timers run only when a test fires them."""

    timers: list[FakeTimer] = field(default_factory=list)

    def call_later(self, delay: float, callback: Callable[..., None], *args: object) -> FakeTimer:
        assert delay in {5.0, NO_ACK_RESEND}  # the ack timeout, or a re-send
        timer = FakeTimer(callback, args, delay)
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
    changes: list[tuple[int, int, int, int]]


def make_loop(*, cap: int | None = None) -> Loop:
    sender, timers = FakeSender(), FakeTimers()
    refused: list[tuple[tuple[str, ...], str]] = []
    changes: list[tuple[int, int, int, int]] = []
    subs = Subscriptions(
        sender,
        call_later=timers.call_later,
        ack_timeout=5.0,
        cap=cap,
        on_refused=lambda topics, message: refused.append((topics, message)),
        on_change=lambda: changes.append(subs.counts()),
    )
    return Loop(subs, sender, timers, refused, changes)


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
    assert loop.sender.take() == [("unsubscribe", 1, ("T.*",))]


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


def test_a_missing_acknowledgement_is_resent_once_then_counted_without_a_reconnect(
    caplog: pytest.LogCaptureFixture,
) -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL", "T.IBM", "T.MSFT"])
    loop.subs.opened(1)
    loop.sender.take()
    loop.subs.status(ack("T.MSFT"))
    loop.timers.fire()
    assert loop.sender.take() == [("subscribe", 1, ("T.AAPL",)), ("subscribe", 1, ("T.IBM",))]
    with caplog.at_level(logging.WARNING, "deephaven_massive._ingest.subscriptions"):
        loop.timers.fire()
    # No reconnect: that would cut T.MSFT, and every other stream on the connection, over one
    # late answer. The topics stay wanted and subscribed, and count as NO_ACK.
    assert loop.sender.take() == []
    assert loop.subs.counts() == (3, 1, 0, 2)
    assert loop.subs.no_ack == {"T.AAPL", "T.IBM"}
    assert loop.refused == []
    # One WARNING for the step, however many of its topics went unacknowledged.
    (record,) = caplog.records
    assert record.levelno == logging.WARNING
    assert record.getMessage() == (
        "generation 1: no acknowledgement from Massive after 10.0 s and one re-send for the "
        "topics 2 [T.AAPL, T.IBM]; they stay subscribed and are re-sent every 60 s, they count "
        "in NoAckTopics of m.connections() and in their streams' Error, and the connection is "
        "not reconnected for them"
    )


def no_ack(*topics: str) -> Loop:
    # A loop whose first step's topics all went `NO_ACK`, with what it sent taken.
    loop = make_loop()
    loop.subs.add(topics)
    loop.subs.opened(1)
    loop.timers.fire()  # each topic re-sent alone
    loop.timers.fire()  # each marked NO_ACK; one round of re-sends is scheduled
    loop.sender.take()
    assert loop.subs.no_ack == frozenset(topics)
    return loop


def delays(loop: Loop) -> list[float]:
    return sorted(t.delay for t in loop.timers.live())


def test_a_no_ack_topic_is_resent_periodically_until_acknowledged() -> None:
    loop = no_ack("T.AAPL")
    # Each round waits `NO_ACK_RESEND`, sends the topic, awaits an answer for the ack timeout,
    # and never reconnects.
    assert delays(loop) == [NO_ACK_RESEND]
    for _ in range(3):
        loop.timers.fire()
        assert loop.sender.take() == [("subscribe", 1, ("T.AAPL",))]
        assert delays(loop) == [5.0, NO_ACK_RESEND]
    loop.subs.status(ack("T.AAPL"))
    assert delays(loop) == [NO_ACK_RESEND]  # the answer ends the wait
    assert loop.subs.no_ack == frozenset()
    loop.timers.fire()  # the next round finds nothing to re-send, and stops
    assert loop.sender.take() == []
    assert loop.timers.live() == []


def test_every_no_ack_topic_is_resent_in_one_frame_each_round() -> None:
    loop = no_ack("T.AAPL", "T.IBM", "T.MSFT")
    for _ in range(3):
        loop.timers.fire()
        assert loop.sender.take() == [("subscribe", 1, ("T.AAPL", "T.IBM", "T.MSFT"))]


def test_a_round_waits_while_a_step_is_in_flight() -> None:
    # An answer naming no topic is credited to the step in flight, so a round never overlaps
    # one: it tries again after the ack timeout.
    loop = no_ack("T.AAPL")
    loop.subs.add(["T.MSFT"])
    loop.subs.reconcile()
    assert loop.sender.take() == [("subscribe", 1, ("T.MSFT",))]
    [round_] = [t for t in loop.timers.live() if t.delay == NO_ACK_RESEND]
    loop.timers.timers.remove(round_)
    round_.callback(*round_.args)
    assert loop.sender.take() == []
    loop.subs.status(ack("T.MSFT"))  # the step resolves
    [retry] = [t for t in loop.timers.live() if t.args == round_.args]
    assert retry.delay == 5.0
    loop.timers.timers.remove(retry)
    retry.callback(*retry.args)
    assert loop.sender.take() == [("subscribe", 1, ("T.AAPL",))]


def test_a_step_waits_while_a_round_awaits_its_answer() -> None:
    loop = no_ack("T.AAPL", "T.IBM")
    loop.timers.fire()
    loop.sender.take()
    loop.subs.add(["T.MSFT"])
    loop.subs.reconcile()
    assert loop.sender.take() == []  # an answer naming no topic would have two owners
    loop.subs.status(ack())  # credited to the round, which resolves
    assert loop.subs.no_ack == frozenset()
    assert loop.sender.take() == [("subscribe", 1, ("T.MSFT",))]


def test_a_round_whose_answer_never_comes_lets_the_held_step_go() -> None:
    loop = no_ack("T.AAPL")
    loop.timers.fire()
    loop.sender.take()
    loop.subs.add(["T.MSFT"])
    loop.subs.reconcile()
    assert loop.sender.take() == []
    [wait] = [t for t in loop.timers.live() if t.delay == 5.0]
    loop.timers.timers.remove(wait)
    wait.callback(*wait.args)
    assert loop.sender.take() == [("subscribe", 1, ("T.MSFT",))]


def refused_nothing(loop: Loop, caplog: pytest.LogCaptureFixture, awaiting: str) -> None:
    # A refusal naming no topic, logged at INFO with what awaits an answer, and failing nothing.
    with caplog.at_level(logging.INFO, logger="deephaven_massive._ingest.subscriptions"):
        loop.subs.status(refusal(message="huh"))
    assert loop.refused == []
    assert f"a refusal named no topic while {awaiting}; it fails nothing: huh" in (caplog.messages)


def test_a_refusal_naming_no_topic_after_a_round_fails_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Even a round of one topic: the refusal may answer an older subscribe of that topic.
    loop = no_ack("T.AAPL")
    loop.timers.fire()
    refused_nothing(
        loop, caplog, "2 topics await an answer (0 in flight, 1 never acknowledged, 1 re-sent)"
    )
    assert loop.subs.no_ack == frozenset({"T.AAPL"})  # it keeps its note


def test_a_late_refusal_of_an_old_topic_never_fails_a_new_step(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # T.AAPL never got an answer; a watchlist adds T.MSFT, whose step is in flight alone when
    # a refusal naming no topic arrives. It may be T.AAPL's, so it fails neither.
    loop = no_ack("T.AAPL")
    loop.subs.add(["T.MSFT"])
    loop.subs.reconcile()
    assert loop.sender.take() == [("subscribe", 1, ("T.MSFT",))]
    refused_nothing(
        loop, caplog, "2 topics await an answer (1 in flight, 1 never acknowledged, 0 re-sent)"
    )
    loop.subs.status(ack("T.MSFT"))
    assert loop.subs.acknowledged == frozenset({"T.MSFT"})


def test_a_round_waits_while_the_previous_round_awaits_its_answer() -> None:
    loop = no_ack("T.AAPL")
    loop.timers.fire()
    loop.sender.take()
    [round_] = [t for t in loop.timers.live() if t.delay == NO_ACK_RESEND]
    [wait] = [t for t in loop.timers.live() if t.delay == 5.0]
    loop.timers.timers.remove(round_)
    round_.callback(*round_.args)  # due before the previous round's wait ended
    assert loop.sender.take() == []
    assert not wait.cancelled
    [retry] = [t for t in loop.timers.live() if t.args == round_.args]
    assert retry.delay == 5.0
    loop.timers.timers.remove(wait)
    wait.callback(*wait.args)  # the previous round stops awaiting its answer
    loop.timers.timers.remove(retry)
    retry.callback(*retry.args)
    assert loop.sender.take() == [("subscribe", 1, ("T.AAPL",))]


def test_an_unparsed_acknowledgement_after_a_round_acknowledges_its_topics() -> None:
    loop = no_ack("T.AAPL", "T.MSFT")
    loop.timers.fire()
    loop.subs.status(ack())
    assert loop.subs.acknowledged == frozenset({"T.AAPL", "T.MSFT"})
    assert loop.subs.no_ack == frozenset()


def test_a_late_answer_naming_no_topic_acknowledges_but_never_refuses_the_step(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A large first step outlasts its watchdogs: its topics are `NO_ACK` and nothing is in
    # flight when its answer, naming no topic, finally comes. A refusal could be anyone's
    # and fails nothing; an acknowledgement clears the step's `NO_ACK` topics.
    loop = no_ack("T.AAPL", "T.MSFT")
    refused_nothing(
        loop, caplog, "2 topics await an answer (0 in flight, 2 never acknowledged, 0 re-sent)"
    )
    loop.subs.status(ack())
    assert loop.subs.acknowledged == frozenset({"T.AAPL", "T.MSFT"})


def test_an_answer_crossing_a_finished_round_is_credited_to_nothing() -> None:
    # The round went out (so the step is no longer an answer's target), its wait ended, and
    # then an answer naming no topic arrives: it has no owner.
    loop = no_ack("T.AAPL")
    loop.timers.fire()
    [wait] = [t for t in loop.timers.live() if t.delay == 5.0]
    loop.timers.timers.remove(wait)
    wait.callback(*wait.args)
    loop.subs.status(ack())
    loop.subs.status(refusal(message="late"))
    assert loop.subs.no_ack == frozenset({"T.AAPL"})
    assert loop.refused == []


def test_releasing_a_resent_topic_stops_awaiting_its_answer() -> None:
    loop = no_ack("T.AAPL")
    loop.timers.fire()
    loop.subs.remove(["T.AAPL"])
    loop.subs.reconcile()
    assert delays(loop) == [NO_ACK_RESEND]


def test_a_no_ack_topic_no_longer_wanted_is_not_resent() -> None:
    loop = no_ack("T.AAPL")
    loop.subs.remove(["T.AAPL"])
    loop.subs.reconcile()
    assert loop.sender.take() == [("unsubscribe", 1, ("T.AAPL",))]
    assert loop.subs.no_ack == frozenset()
    loop.timers.fire()
    assert loop.sender.take() == []


def test_a_round_of_an_ended_generation_does_nothing() -> None:
    loop = no_ack("T.AAPL")
    [round_] = loop.timers.live()
    loop.subs.ended()
    assert round_.cancelled
    loop.subs.opened(2)
    loop.sender.take()
    round_.callback(*round_.args)  # a round that ran despite its cancellation
    assert loop.sender.take() == []


def test_a_new_generation_clears_the_no_acks() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    loop.subs.opened(1)
    loop.timers.fire()
    loop.timers.fire()
    resend = loop.timers.live()
    loop.subs.ended()
    assert all(t.cancelled for t in resend)
    loop.subs.opened(2)
    assert loop.subs.no_ack == frozenset()


def test_a_late_acknowledgement_clears_a_no_ack() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    loop.subs.opened(1)
    loop.timers.fire()
    loop.timers.fire()
    assert loop.subs.counts() == (1, 0, 0, 1)
    loop.subs.status(ack("T.AAPL"))
    assert loop.subs.counts() == (1, 1, 0, 0)
    assert loop.sender.take() == [("subscribe", 1, ("T.AAPL",)), ("subscribe", 1, ("T.AAPL",))]


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


def test_a_refusal_naming_no_topic_refuses_the_one_topic_awaiting_an_answer() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL", "T.MSFT"])
    loop.subs.opened(1)
    loop.subs.status(ack("T.AAPL", "T.MSFT"))
    loop.subs.add(["Q.AAPL"])
    loop.subs.reconcile()
    loop.subs.status(refusal())
    assert loop.refused == [(("Q.AAPL",), "not authorized")]


def test_a_refusal_naming_no_topic_of_a_step_of_several_fails_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    loop = make_loop()
    loop.subs.add(["Q.AAPL", "Q.MSFT"])
    loop.subs.opened(1)
    refused_nothing(
        loop, caplog, "2 topics await an answer (2 in flight, 0 never acknowledged, 0 re-sent)"
    )


def test_a_refusal_naming_no_topic_skips_the_resolved_topics() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL", "T.MSFT"])
    loop.subs.opened(1)
    loop.subs.status(ack("T.AAPL"))
    loop.subs.status(refusal(message="real-time data needs a plan"))
    assert loop.refused == [(("T.MSFT",), "real-time data needs a plan")]


def test_a_refusal_with_nothing_in_flight_is_only_logged(caplog: pytest.LogCaptureFixture) -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    loop.subs.opened(1)
    loop.subs.status(ack("T.AAPL"))
    with caplog.at_level(logging.INFO, logger="deephaven_massive._ingest.subscriptions"):
        loop.subs.status(refusal(message="huh"))
    assert loop.refused == []
    [record] = caplog.records
    assert record.levelno == logging.INFO  # nothing failed, so nothing to act on
    assert record.getMessage() == (
        "a refusal named no topic while 0 topics await an answer (0 in flight, 0 never "
        "acknowledged, 0 re-sent); it fails nothing: huh"
    )


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


def test_a_refusal_fails_only_the_refused_topics_of_an_admitted_contract() -> None:
    # A server that refuses some topics past its own limit fails only those, with its message;
    # the contract's other topic keeps running.
    loop = make_loop(cap=2)
    loop.subs.add(["T.O:A", "Q.O:A"], static=True)
    loop.subs.opened(1)
    loop.sender.take()
    loop.subs.status(ack("T.O:A"))
    loop.subs.status(refusal("Q.O:A", message="Maximum number of subscriptions exceeded"))
    assert loop.refused == [(("Q.O:A",), "Maximum number of subscriptions exceeded")]
    assert loop.subs.counts() == (1, 1, 0, 0)
    assert loop.sender.take() == []


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


def test_topic_helpers() -> None:
    assert wildcard("T") == "T.*"
    assert is_wildcard("T.*")
    assert not is_wildcard("T.AAPL")
    assert channel_of("C.EUR-USD") == "C"
    assert stream_topics(AssetClass.FOREX, "C", ["C:EURUSD"], pairs={}) == {"C.EUR-USD"}


def test_each_step_and_the_acknowledged_topics_are_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    loop = make_loop()
    loop.subs.add(["T.MSFT", "T.AAPL"])
    with caplog.at_level(logging.DEBUG, logger=subscriptions.__name__):
        loop.subs.opened(1)
        loop.subs.status(ack("T.AAPL", "T.MSFT"))
        loop.subs.remove(["T.MSFT"])
        loop.subs.reconcile()
        loop.subs.reconcile()  # nothing changed: no line
    assert loop.subs.acknowledged == frozenset({"T.AAPL"})
    assert caplog.messages == [
        (
            "generation 1: subscribe ['T.AAPL', 'T.MSFT'], unsubscribe [] (desired 2, "
            "acknowledged 0, deferred by the options cap 0)"
        ),
        (
            "generation 1: subscribe [], unsubscribe ['T.MSFT'] (desired 1, acknowledged 1, "
            "deferred by the options cap 0)"
        ),
    ]


def test_every_resolution_is_reported_while_the_step_is_in_flight() -> None:
    # The connection's row follows each acknowledgement, refusal, and NO_ACK of a step as it
    # happens, not only once the whole step has resolved.
    loop = make_loop()
    loop.subs.add(["T.A", "T.B", "T.C"])
    loop.subs.opened(1)
    assert loop.changes[-1] == (3, 0, 3, 0)  # (topics, acked, pending, no_ack)
    loop.subs.status(ack("T.A"))
    assert loop.changes[-1] == (3, 1, 2, 0)
    loop.subs.status(refusal("T.B"))
    assert loop.changes[-1] == (2, 1, 1, 0)
    loop.timers.fire()  # T.C is re-sent alone
    loop.timers.fire()  # and reaches NO_ACK
    assert loop.changes[-1] == (2, 1, 0, 1)


def test_a_change_with_no_generation_open_is_reported() -> None:
    loop = make_loop()
    loop.subs.add(["T.AAPL"])
    loop.subs.reconcile()
    assert loop.changes == [(1, 0, 0, 0)]


def test_acknowledgements_do_not_recompute_the_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    # An acknowledgement changes no stream's demand, so the admitted set it reads is the same.
    # Recomputing it per acknowledgement sorted every topic once per topic of the step, which
    # stalled the connection's loop for seconds on a large options subscription.
    computed: list[int] = []
    recompute = Subscriptions._defer

    def counted(self: Subscriptions) -> None:
        computed.append(1)
        recompute(self)

    monkeypatch.setattr(Subscriptions, "_defer", counted)
    loop = make_loop(cap=50)
    topics = [f"T.O:{n:03d}" for n in range(100)]
    loop.subs.add(topics, static=True)
    loop.subs.opened(1)
    admitted, deferred = loop.subs.admitted, loop.subs.deferred
    for topic in sorted(admitted):
        loop.subs.status(ack(topic))
    assert len(computed) == 1
    assert (loop.subs.admitted, loop.subs.deferred) == (admitted, deferred)
    assert loop.subs.counts() == (50, 50, 0, 0)


def test_a_refused_contract_frees_its_place_under_the_cap() -> None:
    loop = make_loop(cap=1)
    loop.subs.add(["T.O:A", "T.O:B"])
    loop.subs.opened(1)
    assert loop.sender.take() == [("subscribe", 1, ("T.O:A",))]
    loop.subs.status(refusal("T.O:A"))
    assert loop.subs.admitted == {"T.O:B"}
    assert loop.sender.take() == [("subscribe", 1, ("T.O:B",))]
