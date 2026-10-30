"""Tests for `RequestGovernor`: pacing, pauses after a 429, give-up, cancellation, diagnostics.

Time is virtual (`FakeClock`): automatic where one thread runs, manual where a test needs to
see threads wait before it lets them go.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.net.cancel import Cancel
from deephaven_massive._massive.net.errors import WireError
from deephaven_massive._massive.rest.governor import (
    BASIC_REQUESTS_PER_MINUTE,
    MAX_RETRY_AFTER,
    RATE_WINDOW,
    UNLIMITED,
    Admission,
    BucketState,
    Patience,
    Permit,
    RequestGovernor,
    _Bucket,
    _Waiter,
    is_basic_refusal,
)
from deephaven_massive._types import override
from tests._fakes.clock import FakeClock

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable

BUCKETS = ("stocks", "options", "other")
BASIC = "You've exceeded the maximum requests per minute, please wait or upgrade."
TIMEOUT = 10.0
_GOVERNOR_LOGGER = "deephaven_massive._massive.rest.governor"


def _governor(clock: FakeClock | None = None, *, basic: set[str] | None = None) -> RequestGovernor:
    governor = RequestGovernor(BUCKETS, clock=clock or FakeClock())
    if basic:
        governor.declare(basic)
    return governor


def _state(governor: RequestGovernor, bucket: str = "stocks") -> BucketState:
    return next(state for state in governor.snapshot() if state.bucket == bucket)


def _requests(governor: RequestGovernor, bucket: str, count: int) -> None:
    """Make `count` requests of a bucket, each answered at once."""
    for _ in range(count):
        governor.done(governor.admission(bucket, lane=object()).acquire(Cancel()), answered=True)


def _refuse(
    governor: RequestGovernor,
    permit: Permit,
    *,
    retry_after: float | None = None,
    message: str = "HTTP 429: slow down",
) -> bool:
    return governor.refused(permit, retry_after=retry_after, message=message)


class _Waiting:
    """A thread that acquires a permit and records the outcome."""

    def __init__(
        self,
        governor: RequestGovernor,
        bucket: str,
        cancel: Cancel | None = None,
        *,
        lane: Hashable,
        patience: Patience = UNLIMITED,
    ) -> None:
        self.cancel = cancel or Cancel()
        self.lane = lane
        self.patience = patience
        self.permit: Permit | None = None
        self.error: WireError | None = None
        self.started: float | None = None
        self._governor = governor
        self._bucket = bucket
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        try:
            self.permit = self._governor.admission(
                self._bucket, lane=self.lane, patience=self.patience
            ).acquire(self.cancel)
            self.started = self._governor.clock.monotonic()
        except WireError as error:
            self.error = error

    def join(self) -> None:
        self.thread.join(TIMEOUT)
        assert not self.thread.is_alive()


# --- Construction and buckets ------------------------------------------------------------------


@pytest.mark.parametrize("buckets", [(), ("stocks", "stocks")])
def test_the_buckets_are_distinct_and_at_least_one(buckets: tuple[str, ...]) -> None:
    with pytest.raises(ValueError, match="distinct names"):
        RequestGovernor(buckets)


def test_an_unknown_bucket_is_refused_everywhere() -> None:
    governor = _governor()
    assert governor.buckets == BUCKETS
    with pytest.raises(ValueError, match="unknown bucket 'crypto'"):
        governor.admission("crypto", lane=object()).acquire(Cancel())
    with pytest.raises(ValueError, match="unknown bucket 'crypto'"):
        governor.estimate("crypto", 1)
    with pytest.raises(ValueError, match=r"unknown buckets \['crypto'\]"):
        governor.declare({"crypto"})


def test_the_basic_rate_is_five_a_minute_with_a_second_of_margin() -> None:
    assert BASIC_REQUESTS_PER_MINUTE == 5
    assert RATE_WINDOW == 61.0


def test_is_basic_refusal_matches_massives_words_ignoring_case() -> None:
    assert is_basic_refusal(BASIC)
    assert is_basic_refusal("HTTP 429 ...: YOU'VE EXCEEDED THE MAXIMUM REQUESTS PER MINUTE")
    assert not is_basic_refusal("HTTP 429: too many requests")


# --- Pacing ------------------------------------------------------------------------------------


def test_an_unpaced_bucket_never_waits() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    _requests(governor, "stocks", 50)
    assert clock.waits == []
    assert _state(governor).attempts == 50


def test_a_declared_bucket_starts_its_sixth_request_a_window_after_the_first_answer() -> None:
    clock = FakeClock()
    governor = _governor(clock, basic={"stocks"})
    _requests(governor, "stocks", 5)
    assert clock.waits == []
    _requests(governor, "stocks", 1)
    assert clock.waits == [61.0]
    _requests(governor, "options", 20)  # another bucket is not paced
    assert clock.waits == [61.0]


def test_the_window_slides_with_each_answer() -> None:
    clock = FakeClock()
    governor = _governor(clock, basic={"stocks"})
    _requests(governor, "stocks", 1)  # its slot frees at 61
    clock.advance(30.0)
    _requests(governor, "stocks", 4)  # theirs free at 91
    _requests(governor, "stocks", 1)  # waits until 61
    assert clock.monotonic() == 61.0
    _requests(governor, "stocks", 1)  # waits until 91: four slots are still held
    assert clock.monotonic() == 91.0


def test_a_request_in_flight_holds_its_slot_until_a_window_after_its_answer() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock, basic={"stocks"})
    permits = [governor.admission("stocks", lane=object()).acquire(Cancel()) for _ in range(5)]
    waiting = _Waiting(governor, "stocks", lane=object())
    # Every slot is in flight: nothing says when one frees, so it sleeps until woken.
    assert clock.wait_for_sleepers(1) == [math.inf]
    clock.advance(10.0)
    governor.done(permits[0], answered=True)  # frees at 71
    assert clock.wait_for_sleepers(1) == [71.0]
    clock.advance(61.0)
    waiting.join()
    assert waiting.started == 71.0
    for permit in permits[1:]:
        governor.done(permit, answered=False)


def test_requests_start_first_in_first_out() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock, basic={"stocks"})
    _requests(governor, "stocks", 1)  # frees at 61
    clock.advance(30.0)
    _requests(governor, "stocks", 4)  # free at 91
    first = _Waiting(governor, "stocks", lane=object())
    assert clock.wait_for_sleepers(1) == [61.0]
    second = _Waiting(governor, "stocks", lane=object())
    # Not at the head, the second only sleeps until woken; it never computes a start.
    assert clock.wait_for_sleepers(2) == [61.0, math.inf]
    clock.advance(31.0)  # 61: one slot frees; the head takes it
    first.join()
    assert first.started == 61.0
    assert clock.wait_for_sleepers(1) == [91.0]  # the second is the head now
    clock.advance(30.0)
    second.join()
    assert second.started == 91.0


def test_a_change_of_declaration_takes_effect_for_the_waiting_head() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock, basic={"stocks"})
    _requests(governor, "stocks", 5)
    waiting = _Waiting(governor, "stocks", lane=object())
    assert clock.wait_for_sleepers(1) == [61.0]
    governor.declare(set())  # no longer Basic: it starts at once
    waiting.join()
    assert waiting.started == 0.0


# --- Cancellation --------------------------------------------------------------------------------


def test_a_cancelled_wait_gives_up_its_place_and_wakes_the_next() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock, basic={"stocks"})
    _requests(governor, "stocks", 5)
    head = _Waiting(governor, "stocks", lane=object())
    clock.wait_for_sleepers(1)
    behind = _Waiting(governor, "stocks", lane=object())
    clock.wait_for_sleepers(2)
    head.cancel.set()
    head.join()
    assert head.error is not None
    assert head.error.status is None
    assert str(head.error) == "stocks: the wait for a request slot was cancelled"
    assert clock.wait_for_sleepers(1) == [61.0]  # the next became the head
    assert _state(governor).waiting == 1
    behind.cancel.set()
    behind.join()
    assert _state(governor).waiting == 0


def test_a_cancelled_waiter_behind_the_head_leaves_the_head_alone() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock, basic={"stocks"})
    _requests(governor, "stocks", 5)
    head = _Waiting(governor, "stocks", lane=object())
    clock.wait_for_sleepers(1)
    behind = _Waiting(governor, "stocks", lane=object())
    clock.wait_for_sleepers(2)
    behind.cancel.set()
    behind.join()
    assert clock.wait_for_sleepers(1) == [61.0]
    clock.advance(61.0)
    head.join()
    assert head.permit is not None


def test_an_already_cancelled_token_never_waits() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    cancel = Cancel()
    cancel.set()
    with pytest.raises(WireError, match="cancelled"):
        governor.admission("stocks", lane=object()).acquire(cancel)
    assert _state(governor).waiting == 0
    assert clock.waits == []


# --- Refusals ------------------------------------------------------------------------------------


def test_an_unpaced_refusal_pauses_only_its_bucket_15_30_60_then_60() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    for _ in range(4):
        _refuse(governor, governor.admission("stocks", lane=object()).acquire(Cancel()))
        _requests(governor, "options", 1)  # never held back
    _requests(governor, "stocks", 1)
    assert clock.waits == [15.0, 30.0, 60.0, 60.0]


def test_an_answer_after_the_pause_ends_the_run() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    _refuse(governor, governor.admission("stocks", lane=object()).acquire(Cancel()))
    _requests(governor, "stocks", 1)  # waits 15, then is answered
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel())
    )  # a new run: 15 again
    _requests(governor, "stocks", 1)
    assert clock.waits == [15.0, 15.0]


def test_a_connection_failure_does_not_end_the_run() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    _refuse(governor, governor.admission("stocks", lane=object()).acquire(Cancel()))
    governor.done(governor.admission("stocks", lane=object()).acquire(Cancel()), answered=False)
    _refuse(governor, governor.admission("stocks", lane=object()).acquire(Cancel()))
    _requests(governor, "stocks", 1)
    assert clock.waits == [15.0, 30.0]


def test_a_burst_of_refusals_in_flight_is_one_refusal_and_stale_answers_keep_the_run() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    burst = [governor.admission("stocks", lane=object()).acquire(Cancel()) for _ in range(3)]
    _refuse(governor, burst[0])  # pause 15
    _refuse(governor, burst[1])  # started before the pause: not counted
    governor.done(burst[2], answered=True)  # nor does its success end the run
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel())
    )  # the run's second: 30
    _requests(governor, "stocks", 1)
    assert clock.waits == [15.0, 30.0]
    assert _state(governor).refusals == 3


def test_retry_after_sets_the_pause_and_a_stale_one_only_lengthens_it() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    burst = [governor.admission("stocks", lane=object()).acquire(Cancel()) for _ in range(3)]
    _refuse(governor, burst[0], retry_after=20.0)
    _refuse(governor, burst[1], retry_after=5.0)  # shorter: the pause stays 20
    _refuse(governor, burst[2], retry_after=40.0)  # longer: it grows to 40
    _requests(governor, "stocks", 1)
    assert clock.waits == [40.0]


def test_a_refusal_naming_the_per_minute_maximum_makes_the_bucket_learned_basic() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    assert _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel()), message=BASIC
    )
    state = _state(governor)
    assert (state.limit, state.learned, state.declared) == (5, True, False)
    _requests(governor, "stocks", 1)  # a paced bucket pauses a whole window
    assert clock.waits == [61.0]
    governor.declare(set())  # learned stays learned
    assert _state(governor).limit == 5


def test_a_refusal_without_those_words_only_pauses() -> None:
    governor = _governor()
    assert not _refuse(governor, governor.admission("stocks", lane=object()).acquire(Cancel()))
    assert _state(governor).limit is None


def test_a_paced_bucket_pauses_a_window_and_counts_the_refused_request() -> None:
    clock = FakeClock()
    governor = _governor(clock, basic={"stocks"})
    _refuse(governor, governor.admission("stocks", lane=object()).acquire(Cancel()))
    assert _state(governor).recent == 1
    _requests(governor, "stocks", 1)
    assert clock.waits == [61.0]


def test_a_refusal_wakes_a_head_whose_pause_it_lengthens() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock)
    in_flight = governor.admission("stocks", lane=object()).acquire(Cancel())
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel())
    )  # pause until 15
    waiting = _Waiting(governor, "stocks", lane=object())
    assert clock.wait_for_sleepers(1) == [15.0]
    _refuse(governor, in_flight, retry_after=40.0)  # stale, but lengthens the pause
    assert clock.wait_for_sleepers(1) == [40.0]
    clock.advance(40.0)
    waiting.join()
    assert waiting.started == 40.0


# --- Giving up is each request's own -------------------------------------------------------------


def test_a_request_whose_patience_ends_before_the_pause_fails_at_once() -> None:
    clock = FakeClock(wall=1_800_000_000.0)
    governor = _governor(clock)
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel()), retry_after=301.0
    )
    with pytest.raises(WireError) as caught:
        governor.admission("stocks", lane=object(), patience=Patience(300.0)).acquire(Cancel())
    assert caught.value.status == 429
    local = datetime.fromtimestamp(1_800_000_301, UTC).astimezone().isoformat()
    assert str(caught.value) == (
        "Massive is refusing stocks requests with HTTP 429 (latest refusal: HTTP 429: slow "
        "down). Its next request, a probe, goes at 2027-01-15T08:05:01+00:00 "
        f"({local} local time), but this read waits out at most 300 s of pauses and has "
        "spent 0 s of its patience. The usual cause is another program or process using the "
        "same Massive account, whose limit they share. To go on: retry after that time; stop "
        "the other program; or, if this subscription is on the Basic tier, declare it Basic "
        "(rest_basic_subscriptions) so its requests are paced, or upgrade it"
    )
    assert clock.waits == []  # it failed without waiting
    # The bucket has not given up: a request with more patience waits the pause out.
    _requests(governor, "stocks", 1)
    assert clock.monotonic() == 301.0


@pytest.mark.parametrize("asked", [600.5, 1e10, 1e12, math.inf])
def test_a_retry_after_beyond_ten_minutes_pauses_ten_minutes_and_says_so(
    asked: float, caplog: pytest.LogCaptureFixture
) -> None:
    clock = FakeClock(wall=1_800_000_000.0)
    governor = _governor(clock)
    with caplog.at_level(logging.DEBUG, logger=_GOVERNOR_LOGGER):
        _refuse(
            governor,
            governor.admission("stocks", lane=object()).acquire(Cancel()),
            retry_after=asked,
        )
    assert MAX_RETRY_AFTER == 600.0
    assert governor.snapshot()[0].paused_until == 1_800_000_600.0
    # A diagnostics writer sleeps until the next change: always a bounded, finite wait.
    assert governor.next_change() == 600.0
    [record] = caplog.records
    assert record.levelname == "WARNING"
    assert record.getMessage() == (
        f"stocks: a refusal's Retry-After asks for {asked} s; pausing 600.0 s, the most honoured"
    )
    # A read with ten minutes of patience waits the clamped pause out.
    permit = governor.admission("stocks", lane=object(), patience=Patience(600.0)).acquire(Cancel())
    governor.done(permit, answered=True)
    assert clock.waits == [600.0]


def test_a_retry_after_of_ten_minutes_is_honoured_without_a_log_line(
    caplog: pytest.LogCaptureFixture,
) -> None:
    governor = _governor(FakeClock())
    with caplog.at_level(logging.DEBUG, logger=_GOVERNOR_LOGGER):
        _refuse(
            governor,
            governor.admission("stocks", lane=object()).acquire(Cancel()),
            retry_after=600.0,
        )
    assert caplog.messages == []
    assert governor.next_change() == 600.0


def test_a_request_whose_patience_ends_with_the_pause_waits_for_it() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel()), retry_after=600.0
    )
    permit = governor.admission("stocks", lane=object(), patience=Patience(600.0)).acquire(Cancel())
    governor.done(permit, answered=True)
    assert clock.waits == [600.0]


def test_a_waiting_request_fails_when_a_refusal_pushes_the_pause_past_its_patience() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock)
    in_flight = governor.admission("stocks", lane=object()).acquire(Cancel())
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel())
    )  # paused until 15
    impatient = _Waiting(governor, "stocks", lane=object(), patience=Patience(100.0))
    clock.wait_for_sleepers(1)
    _refuse(governor, in_flight, retry_after=700.0)  # stale: the pause grows to 700
    impatient.join()
    assert impatient.error is not None
    assert impatient.error.status == 429
    assert "Its next request, a probe, goes at" in str(impatient.error)


def test_a_read_waiting_on_another_reads_requests_charges_their_pauses_to_its_own_patience() -> (
    None
):
    clock = FakeClock(manual=True)
    governor = _governor(clock)
    patience = Patience(90.0)
    assert governor.charge_pause("other", patience) is None  # no pause: nothing to charge
    assert patience.spent == 0.0
    _refuse(governor, governor.admission("other", lane=object()).acquire(Cancel()), retry_after=60)
    assert governor.charge_pause("other", patience) is None
    assert governor.charge_pause("other", patience) is None  # the same pause counts once
    assert patience.spent == 60.0
    clock.advance(60.0)
    _refuse(governor, governor.admission("other", lane=object()).acquire(Cancel()), retry_after=60)
    failure = governor.charge_pause("other", patience)  # 120 s in all: more than it waits out
    assert failure is not None
    assert failure.status == 429
    assert "this read waits out at most 90 s of pauses and has spent 60 s" in str(failure)
    assert patience.spent == 60.0  # nothing is charged for a pause it will not wait out
    assert governor.charge_pause("other", Patience(600.0)) is None  # another read's own patience
    with pytest.raises(ValueError, match="unknown bucket 'crypto'"):
        governor.charge_pause("crypto", patience)


def test_the_bucket_never_gives_up_however_long_massive_refuses() -> None:
    # Ninety refusals take far longer than any read waits; a request with no deadline still
    # waits each pause out, so another session's reads never fail for this one's patience.
    clock = FakeClock()
    governor = _governor(clock)
    for _ in range(90):
        _refuse(governor, governor.admission("stocks", lane=object()).acquire(Cancel()))
    _requests(governor, "stocks", 1)
    assert clock.monotonic() == 15.0 + 30.0 + 60.0 * 88
    assert _state(governor).refusals == 90


# --- Snapshots, estimates, idleness, listeners ---------------------------------------------------


def test_snapshot_reports_every_bucket() -> None:
    clock = FakeClock(wall=1_800_000_000.0)
    governor = _governor(clock, basic={"options"})
    held = governor.admission("stocks", lane=object()).acquire(Cancel())
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel()), retry_after=30.0
    )
    clock.advance(10.0)
    stocks, options, other = governor.snapshot()
    assert stocks.bucket == "stocks"
    assert (stocks.limit, stocks.declared, stocks.learned) == (None, False, False)
    assert (stocks.in_flight, stocks.waiting, stocks.recent) == (1, 0, 2)
    assert (stocks.attempts, stocks.refusals) == (2, 1)
    assert stocks.paused_until == 1_800_000_030.0
    assert stocks.last_refusal == "HTTP 429: slow down"
    assert (options.limit, options.declared, options.paused_until) == (5, True, None)
    assert other.last_refusal is None
    governor.done(held, answered=True)


def test_estimate_is_a_lower_bound_from_the_slots_held() -> None:
    clock = FakeClock()
    governor = _governor(clock, basic={"stocks"})
    assert governor.estimate("options", 100) is None  # unpaced
    assert governor.estimate("stocks", 0) is None
    assert governor.estimate("stocks", 5) == 0.0
    assert governor.estimate("stocks", 6) == 61.0
    assert governor.estimate("stocks", 250) == 49 * 61.0
    in_flight = governor.admission("stocks", lane=object()).acquire(
        Cancel()
    )  # frees no sooner than 61 from now
    _requests(governor, "stocks", 1)
    clock.advance(30.0)  # that slot frees at 61, 31 from now
    assert governor.estimate("stocks", 3) == 0.0
    assert governor.estimate("stocks", 4) == 31.0
    assert governor.estimate("stocks", 5) == 61.0
    governor.done(in_flight, answered=True)


def test_estimate_counts_the_pause() -> None:
    clock = FakeClock()
    governor = _governor(clock, basic={"stocks"})
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel()), retry_after=100.0
    )
    assert governor.estimate("stocks", 1) == 100.0


def test_idle_once_nothing_is_held() -> None:
    clock = FakeClock()
    governor = _governor(clock, basic={"stocks"})
    assert governor.idle()
    permit = governor.admission("stocks", lane=object()).acquire(Cancel())
    assert not governor.idle()  # in flight
    governor.done(permit, answered=True)
    assert not governor.idle()  # its slot is held
    clock.advance(61.0)
    assert governor.idle()
    _refuse(
        governor, governor.admission("options", lane=object()).acquire(Cancel()), retry_after=200.0
    )
    clock.advance(61.0)
    assert not governor.idle()  # paused
    clock.advance(200.0)
    assert governor.idle()


def test_a_waiting_request_keeps_the_governor_busy() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock, basic={"stocks"})
    _requests(governor, "stocks", 5)
    clock.advance(61.0)
    permits = [governor.admission("stocks", lane=object()).acquire(Cancel()) for _ in range(5)]
    for permit in permits:
        governor.done(permit, answered=True)
    waiting = _Waiting(governor, "stocks", lane=object())
    clock.wait_for_sleepers(1)
    clock.advance(61.0)
    waiting.join()
    assert not governor.idle()


def test_listeners_hear_every_change_until_unsubscribed() -> None:
    governor = _governor()
    heard: list[str] = []

    def listener() -> None:
        heard.append("x")

    governor.subscribe(listener)
    _requests(governor, "stocks", 1)  # admitted without waiting: only its answer is a change
    assert len(heard) == 1
    governor.declare({"stocks"})
    assert len(heard) == 2
    governor.unsubscribe(listener)
    governor.unsubscribe(listener)  # unknown now: ignored
    _requests(governor, "stocks", 1)
    assert len(heard) == 2


def test_a_bucket_listener_hears_only_its_bucket() -> None:
    governor = _governor()
    heard: list[str] = []

    def listener() -> None:
        heard.append("x")

    governor.subscribe_bucket("other", listener)
    _requests(governor, "stocks", 1)  # another subscription's change
    assert heard == []
    _requests(governor, "other", 1)
    assert len(heard) == 1
    governor.declare({"stocks"})  # a declaration changes every bucket
    assert len(heard) == 2
    governor.unsubscribe(listener)
    _requests(governor, "other", 1)
    assert len(heard) == 2
    with pytest.raises(ValueError, match="unknown bucket 'crypto'"):
        governor.subscribe_bucket("crypto", listener)


# --- Many threads --------------------------------------------------------------------------------


def _drive(clock: FakeClock, alive: Callable[[], int]) -> None:
    """Advance virtual time to the earliest deadline whenever every live worker sleeps."""
    deadline = time.monotonic() + 60.0
    while alive():
        assert time.monotonic() < deadline, "the workers stopped making progress"
        before = alive()
        sleeping = clock.sleeping()
        # Every live worker sleeps only if the count held still around the read: a worker
        # that finished meanwhile would let one still awake pass for asleep.
        if sleeping and len(sleeping) == before == alive():
            clock.advance(sleeping[0] - clock.monotonic())
        else:
            time.sleep(0.0005)


def test_16_threads_never_start_more_than_five_requests_in_a_window() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock, basic={"stocks"})
    starts: list[float] = []
    lock = threading.Lock()
    running = [16]

    def worker() -> None:
        try:
            for _ in range(10):
                permit = governor.admission("stocks", lane=object()).acquire(Cancel())
                with lock:
                    starts.append(clock.monotonic())
                governor.done(permit, answered=True)
        finally:
            with lock:
                running[0] -= 1

    def alive() -> int:
        with lock:
            return running[0]

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(16)]
    for thread in threads:
        thread.start()
    _drive(clock, alive)
    for thread in threads:
        thread.join(TIMEOUT)
    assert len(starts) == 160
    starts.sort()
    # Each request holds its slot a window from its answer, which came at once: no six
    # requests ever start within one window.
    assert all(later - first >= 61.0 for first, later in zip(starts, starts[5:], strict=False))
    assert starts[-1] == 31 * 61.0  # and no slot is wasted
    state = _state(governor)
    assert (state.attempts, state.in_flight, state.waiting) == (160, 0, 0)


def test_a_counted_refusal_never_shortens_a_pause_a_stale_one_lengthened() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    slow = governor.admission("stocks", lane=object()).acquire(
        Cancel()
    )  # still in flight through what follows
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel())
    )  # pause until 15
    current = governor.admission("stocks", lane=object()).acquire(
        Cancel()
    )  # starts at 15, after the pause
    _refuse(governor, slow, retry_after=100.0)  # stale: the pause grows to 115
    _refuse(governor, current)  # the run's second refusal asks for 30 s, to 45: not shorter
    _requests(governor, "stocks", 1)
    assert clock.monotonic() == 115.0


# --- Lanes ---------------------------------------------------------------------------------------


def _spend_five_slots_ten_seconds_apart(clock: FakeClock, governor: RequestGovernor) -> None:
    """Make five answered stocks requests at 0, 10, 20, 30, and 40 s.

    Their slots free one at a time, at 61, 71, 81, 91, and 101 s, so every later start comes
    at a time of its own.
    """
    for index in range(5):
        if index:
            clock.advance(10.0)
        governor.done(governor.admission("stocks", lane=object()).acquire(Cancel()), answered=True)


class _Lanes:
    """Threads that each take permits in a lane, recording who started when, in order."""

    def __init__(self, clock: FakeClock, governor: RequestGovernor) -> None:
        self.clock = clock
        self.governor = governor
        self.starts: list[tuple[str, float]] = []
        self.threads: list[threading.Thread] = []
        self._lock = threading.Lock()
        self._running = 0

    def add(self, name: str, lane: object, *, times: int = 1) -> None:
        """Start a thread that takes `times` permits in `lane`, one after the other.

        Returns once the thread sleeps in the governor, so the lanes fill in call order.
        """

        def run() -> None:
            try:
                for _ in range(times):
                    permit = self.governor.admission("stocks", lane=lane).acquire(Cancel())
                    with self._lock:
                        self.starts.append((name, self.clock.monotonic()))
                    self.governor.done(permit, answered=True)
            finally:
                with self._lock:
                    self._running -= 1

        with self._lock:
            self._running += 1
            running = self._running
        thread = threading.Thread(target=run, daemon=True)
        self.threads.append(thread)
        thread.start()
        self.clock.wait_for_sleepers(running)

    def alive(self) -> int:
        with self._lock:
            return self._running

    def run(self) -> list[str]:
        """Drive time until every thread is done.

        Returns:
            The names of the threads, one per start, in start order.
        """
        _drive(self.clock, self.alive)
        for thread in self.threads:
            thread.join(TIMEOUT)
        return [name for name, _ in self.starts]


def test_lanes_take_turns_and_each_lane_keeps_its_order() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock, basic={"stocks"})
    _spend_five_slots_ten_seconds_apart(clock, governor)
    lanes = _Lanes(clock, governor)
    history, probe = object(), object()
    for name in ("h1", "h2", "h3"):
        lanes.add(name, history)
    lanes.add("probe", probe)
    assert lanes.run() == ["h1", "probe", "h2", "h3"]
    assert [moment for _, moment in lanes.starts] == [61.0, 71.0, 81.0, 91.0]


def test_a_host_probe_behind_two_histories_waits_for_one_request_of_each() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock, basic={"stocks"})
    _spend_five_slots_ten_seconds_apart(clock, governor)
    lanes = _Lanes(clock, governor)
    first, second = object(), object()
    for index in range(8):  # two histories of eight workers each
        lanes.add(f"a{index}", first)
    for index in range(8):
        lanes.add(f"b{index}", second)
    lanes.add("probe", object())
    order = lanes.run()
    assert order[:3] == ["a0", "b0", "probe"]
    assert dict(lanes.starts)["probe"] == 81.0  # not after all sixteen history requests
    assert [name for name in order if name.startswith("a")] == [f"a{i}" for i in range(8)]


def test_a_poller_gets_every_third_request_beside_two_histories() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock, basic={"stocks"})
    _spend_five_slots_ten_seconds_apart(clock, governor)
    lanes = _Lanes(clock, governor)
    first, second = object(), object()
    for index in range(8):
        lanes.add(f"a{index}", first)
    for index in range(8):
        lanes.add(f"b{index}", second)
    lanes.add("poll", object(), times=3)  # one cycle after another, each joining again
    order = lanes.run()
    assert [index for index, name in enumerate(order) if name == "poll"] == [2, 5, 8]


# --- Retry-After floor ---------------------------------------------------------------------------


@pytest.mark.parametrize(("basic", "pause"), [(None, 15.0), ({"stocks"}, 61.0)])
def test_a_retry_after_of_zero_or_in_the_past_falls_back_to_the_schedule(
    basic: set[str] | None, pause: float
) -> None:
    clock = FakeClock()
    governor = _governor(clock, basic=basic)
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel()), retry_after=0.0
    )
    _requests(governor, "stocks", 1)
    assert clock.monotonic() == pause


# --- Reports, learning, timing --------------------------------------------------------------------


def test_a_permit_counts_its_first_report_only() -> None:
    clock = FakeClock()
    governor = _governor(clock, basic={"stocks"})
    permit = governor.admission("stocks", lane=object()).acquire(Cancel())
    governor.release(permit)  # never sent: no slot held
    governor.done(permit, answered=True)
    governor.release(permit)
    assert not _refuse(governor, permit)
    state = _state(governor)
    assert (state.in_flight, state.recent, state.refusals) == (0, 0, 0)
    sent = governor.admission("stocks", lane=object()).acquire(Cancel())
    governor.done(sent, answered=False)
    governor.release(sent)  # too late: it may have reached Massive
    assert _state(governor).recent == 1


def test_a_release_wakes_the_head_at_once() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock, basic={"stocks"})
    permits = [governor.admission("stocks", lane=object()).acquire(Cancel()) for _ in range(5)]
    waiting = _Waiting(governor, "stocks", lane=object())
    clock.wait_for_deadlines([math.inf])  # every slot in flight
    governor.release(permits[0])
    waiting.join()  # admitted without any time passing
    assert waiting.started == 0.0
    for permit in permits[1:]:
        governor.release(permit)


def test_learning_paces_a_bucket_and_wakes_its_head() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock)
    _requests(governor, "stocks", 5)  # unpaced: five slots held until 61, but no limit
    stale = governor.admission("stocks", lane=object()).acquire(Cancel())  # in flight from before
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel())
    )  # pause until 15
    waiting = _Waiting(governor, "stocks", lane=object())
    clock.wait_for_deadlines([15.0])
    assert _refuse(governor, stale, message=BASIC)  # learned Basic: paced from now on
    state = _state(governor)
    assert (state.limit, state.learned) == (5, True)
    clock.wait_for_deadlines([61.0])  # woken at once, it now waits for a slot
    clock.advance(61.0)
    waiting.join()


def test_next_change_is_the_earliest_pause_end_or_slot_release() -> None:
    clock = FakeClock()
    governor = _governor(clock, basic={"stocks"})
    assert governor.next_change() is None
    _requests(governor, "stocks", 1)  # frees at 61
    assert governor.next_change() == 61.0
    _refuse(
        governor, governor.admission("options", lane=object()).acquire(Cancel()), retry_after=20.0
    )
    assert governor.next_change() == 20.0
    clock.advance(30.0)
    assert governor.next_change() == 31.0  # both slots free at 61
    clock.advance(31.0)
    assert governor.next_change() is None


def test_a_listener_that_raises_is_logged_and_the_permit_still_granted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    governor = _governor()
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel()), retry_after=1.0
    )

    def broken() -> None:
        raise RuntimeError("listener broke")

    governor.subscribe(broken)
    with caplog.at_level(logging.ERROR):
        permit = governor.admission("stocks", lane=object()).acquire(
            Cancel()
        )  # queued for the pause
    assert "a rate-limit listener failed" in caplog.text
    governor.done(permit, answered=True)
    assert _state(governor).in_flight == 0


def test_an_interrupt_after_the_grant_hands_the_permit_back() -> None:
    governor = _governor()
    # A pause makes the request queue, so its admission sends a change notice.
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel()), retry_after=1.0
    )
    calls: list[int] = []

    def interrupting() -> None:
        calls.append(1)
        if len(calls) == 2:  # the notice of the grant, after that of the queuing
            raise KeyboardInterrupt

    governor.subscribe(interrupting)
    with pytest.raises(KeyboardInterrupt):
        governor.admission("stocks", lane=object()).acquire(Cancel())
    state = _state(governor)
    # Only the refused attempt holds a slot: the interrupted grant was handed back.
    assert (state.in_flight, state.waiting, state.recent) == (0, 0, 1)


def test_messages_name_the_buckets_requests() -> None:
    governor = RequestGovernor(
        ("stocks", "other"), clock=FakeClock(), labels={"other": "requests elsewhere"}
    )
    for bucket in ("other", "stocks"):
        _refuse(
            governor, governor.admission(bucket, lane=object()).acquire(Cancel()), retry_after=700.0
        )
    with pytest.raises(WireError, match=r"^Massive is refusing requests elsewhere with HTTP 429"):
        governor.admission("other", lane=object(), patience=Patience(0.0)).acquire(Cancel())
    with pytest.raises(WireError, match=r"^Massive is refusing stocks requests with HTTP 429"):
        governor.admission("stocks", lane=object(), patience=Patience(0.0)).acquire(Cancel())


def test_a_request_leaving_its_lane_keeps_the_lane_and_its_turn() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock, basic={"stocks"})
    _requests(governor, "stocks", 5)
    read = object()
    head = _Waiting(governor, "stocks", lane=read)
    clock.wait_for_sleepers(1)
    behind = _Waiting(governor, "stocks", lane=read)  # the same read: the same lane
    clock.wait_for_deadlines([61.0, math.inf])
    behind.cancel.set()  # it leaves; the lane keeps its head, which is not woken
    behind.join()
    clock.wait_for_deadlines([61.0])
    assert _state(governor).waiting == 1
    clock.advance(61.0)
    head.join()
    assert head.started == 61.0


def test_a_late_shorter_refusal_leaves_a_longer_pause_alone() -> None:
    clock = FakeClock(wall=1_800_000_000.0)
    governor = _governor(clock)
    slow = governor.admission("stocks", lane=object()).acquire(
        Cancel()
    )  # in flight before the pause
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel()), retry_after=100.0
    )
    clock.advance(10.0)
    _refuse(governor, slow, retry_after=5.0)  # stale, and ends sooner: changes nothing
    assert _state(governor).paused_until == 1_800_000_100.0
    _requests(governor, "stocks", 1)
    assert clock.monotonic() == 100.0


def test_an_interrupted_lane_change_leaves_a_state_the_bucket_repairs() -> None:
    bucket = _Bucket("stocks requests", declarable=True)
    first, second = _Waiter("a"), _Waiter("b")
    bucket.join(first)
    bucket.join(second)
    # Lane "a" is empty but keeps the front turn, as `start_head` leaves it, or an
    # interruption of `leave`. `head` drops it and serves "b".
    bucket.lanes["a"].popleft()
    assert bucket.head() is second
    assert list(bucket.turns) == ["b"]
    assert "a" not in bucket.lanes
    # As if it struck between dropping a turn and its lane: lane "b" has no turn. A request
    # joining it gives the lane a turn again, and the one already there is served first.
    bucket.turns.clear()
    third = _Waiter("b")
    bucket.join(third)
    assert bucket.head() is second
    assert bucket.waiting == 2


def test_a_request_that_gives_up_its_place_sends_a_change_notice() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock, basic={"stocks"})
    _requests(governor, "stocks", 5)
    heard = threading.Semaphore(0)
    waiting = _Waiting(governor, "stocks", lane=object())
    clock.wait_for_sleepers(1)
    governor.subscribe(heard.release)
    waiting.cancel.set()
    waiting.join()
    assert heard.acquire(timeout=TIMEOUT)  # the waiting count changed: listeners hear it
    assert _state(governor).waiting == 0


def test_a_queued_request_alone_keeps_the_governor_busy() -> None:
    # A request just woken to start, its slot freed and nothing in flight or paused: a
    # registry pruning the governor now would strand it on a governor nobody else shares.
    governor = _governor()
    governor._buckets["stocks"].join(_Waiter(object()))
    assert not governor.idle()


# --- Expiring runs, probes after a pause, never-paced buckets ------------------------------------


def test_a_refusal_after_a_quiet_hour_starts_a_new_run() -> None:
    # One refusal, then the read is abandoned: no answer ever ends the run. A refusal an hour
    # later is the first of a new run, so it pauses instead of giving up.
    clock = FakeClock(manual=True)
    governor = _governor(clock)
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel())
    )  # paused until 15
    clock.advance(3600.0)
    assert not _refuse(governor, governor.admission("stocks", lane=object()).acquire(Cancel()))
    state = _state(governor)
    assert state.paused_until == 1_800_000_000.0 + 3600.0 + 15.0


def test_a_refusal_within_a_window_of_the_pause_continues_its_run() -> None:
    # A refusal soon after its pause ended is part of the same run: its pause grows to 30 s.
    clock = FakeClock(manual=True)
    governor = _governor(clock)
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel())
    )  # paused until 15
    clock.advance(15.0 + RATE_WINDOW)
    _refuse(governor, governor.admission("stocks", lane=object()).acquire(Cancel()))
    assert _state(governor).paused_until == 1_800_000_000.0 + 76.0 + 30.0


def test_a_pause_ends_with_one_probe_while_the_others_wait_for_its_answer() -> None:
    # Massive's 429 lacks the Basic words, so the bucket stays unpaced. Without the probe,
    # every waiting request would start when the pause ends and be refused again.
    clock = FakeClock(manual=True)
    governor = _governor(clock)
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel())
    )  # paused until 15
    waiting = [_Waiting(governor, "stocks", lane=object()) for _ in range(5)]
    assert clock.wait_for_sleepers(5) == [15.0, *[math.inf] * 4]
    clock.advance(15.0)
    probe = next(w for w in waiting if _joined(w))  # one starts, alone
    assert probe.permit is not None
    assert clock.wait_for_sleepers(4) == [math.inf] * 4
    _refuse(governor, probe.permit)  # refused: the run's second pause, 30 s
    assert clock.wait_for_sleepers(4) == [45.0, *[math.inf] * 3]
    clock.advance(30.0)
    second = next(w for w in waiting if w is not probe and _joined(w))
    assert second.permit is not None
    assert clock.wait_for_sleepers(3) == [math.inf] * 3
    governor.done(second.permit, answered=True)  # answered: the others go at once
    for each in waiting:
        each.join()
    assert _state(governor).refusals == 2  # one per pause, never a burst
    for each in waiting:
        if each.permit is not None and each not in (probe, second):
            governor.done(each.permit, answered=True)


def _joined(waiting: _Waiting) -> bool:
    waiting.thread.join(TIMEOUT / 10)
    return not waiting.thread.is_alive()


def test_a_probe_that_is_never_answered_lets_the_next_request_probe() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock)
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel())
    )  # paused until 15
    clock.advance(15.0)
    probe = governor.admission("stocks", lane=object()).acquire(Cancel())
    waiting = _Waiting(governor, "stocks", lane=object())
    assert clock.wait_for_sleepers(1) == [math.inf]  # behind the probe
    governor.release(probe)  # never sent: the waiting request probes instead
    waiting.join()
    assert waiting.permit is not None
    third = _Waiting(governor, "stocks", lane=object())
    assert clock.wait_for_sleepers(1) == [math.inf]  # behind the new probe
    governor.done(waiting.permit, answered=True)
    third.join()
    assert third.permit is not None
    governor.done(third.permit, answered=True)


def test_a_learn_only_bucket_is_paced_by_a_basic_refusal_and_cannot_be_declared() -> None:
    governor = RequestGovernor(BUCKETS, learn_only={"other"}, clock=FakeClock())
    with pytest.raises(ValueError, match=r"buckets \['other'\] cannot be declared Basic"):
        governor.declare({"other"})
    assert _refuse(
        governor, governor.admission("other", lane=object()).acquire(Cancel()), message=BASIC
    )
    other = _state(governor, "other")
    assert (other.limit, other.learned, other.declared) == (5, True, False)
    with pytest.raises(ValueError, match=r"learn-only buckets \['crypto'\] are not buckets"):
        RequestGovernor(BUCKETS, learn_only={"crypto"})


def test_an_interrupt_while_it_stops_listening_hands_the_permit_back() -> None:
    # Ctrl-C landing in the unregistering of the cancel callback, after the grant.
    class Interrupting(Cancel):
        @override
        def on_set(self, callback: Callable[[], None]) -> Callable[[], None]:
            super().on_set(callback)

            def stop() -> None:
                raise KeyboardInterrupt

            return stop

    governor = _governor(basic={"stocks"})
    for _ in range(5):
        _requests(governor, "stocks", 5)  # every slot held: the next request queues
        with pytest.raises(KeyboardInterrupt):
            governor.admission("stocks", lane=object()).acquire(Interrupting())
        state = _state(governor)
        assert (state.in_flight, state.waiting) == (0, 0)
    _requests(governor, "stocks", 1)  # nothing leaked: the bucket still admits requests
    assert governor.idle() is False


def test_slots_of_an_unpaced_bucket_change_nothing_with_time() -> None:
    governor = _governor()
    _requests(governor, "stocks", 3)
    assert _state(governor).recent == 3
    assert governor.next_change() is None  # a paid plan's diagnostics do not churn
    governor.declare({"stocks"})
    assert governor.next_change() == RATE_WINDOW


class _GrantClock(FakeClock):
    """A fake clock that remembers, per thread, the time it last told that thread.

    The governor reads the clock under its lock and grants on that reading, with no later
    read before `acquire` returns, so a thread's last reading is the time of its grant. A
    reading taken after `acquire` returns is not: while the thread is descheduled, another
    thread's wait for the next free slot may already have moved virtual time a window on.
    """

    def __init__(self) -> None:
        super().__init__()
        self._read = threading.local()

    @override
    def monotonic(self) -> float:
        now = super().monotonic()
        self._read.last = now
        return now

    def last_read(self) -> float:
        """Return the time this clock last told the calling thread."""
        last: float = self._read.last
        return last


def test_concurrent_paced_requests_on_the_automatic_clock_do_not_race_virtual_time() -> None:
    # No wait without a deadline moves virtual time: only a slot's release does, so time
    # advances by windows.
    clock = _GrantClock()
    governor = _governor(clock, basic={"stocks"})
    lane = object()
    starts: list[float] = []
    lock = threading.Lock()

    def work() -> None:
        for _ in range(5):
            permit = governor.admission("stocks", lane=lane).acquire(Cancel())
            with lock:
                starts.append(clock.last_read())  # the time the governor granted it
            governor.done(permit, answered=True)

    threads = [threading.Thread(target=work, daemon=True) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(TIMEOUT)
        assert not thread.is_alive()
    assert sorted(starts)[:5] == [0.0] * 5
    assert len(starts) == 20
    assert all(wait == math.inf or 0 < wait <= RATE_WINDOW for wait in clock.waits)
    # 20 requests at 5 a window need three more windows; a slot answered late may need one more.
    assert clock.monotonic() <= 4 * RATE_WINDOW


def test_refusals_without_the_basic_words_never_pace_a_bucket_however_many() -> None:
    # The tier is learned from the refusal's words alone, never inferred from a count.
    clock = FakeClock()
    governor = _governor(clock)
    for _ in range(3):
        _refuse(governor, governor.admission("stocks", lane=object()).acquire(Cancel()))
    state = _state(governor)
    assert (state.limit, state.learned, state.refusals) == (None, False, 3)


def test_an_interrupt_before_the_grant_leaves_its_place_and_holds_nothing() -> None:
    governor = _governor()
    # A pause makes the request queue; the notice of its queuing is interrupted.
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel()), retry_after=1.0
    )

    def interrupting() -> None:
        raise KeyboardInterrupt

    governor.subscribe(interrupting)
    with pytest.raises(KeyboardInterrupt):
        governor.admission("stocks", lane=object()).acquire(Cancel())
    governor.unsubscribe(interrupting)
    state = _state(governor)
    assert (state.in_flight, state.waiting, state.attempts) == (0, 0, 1)


def test_a_stale_refusal_only_lengthens_the_pause() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock)
    stale = governor.admission("stocks", lane=object()).acquire(Cancel())  # in flight from before
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel()), retry_after=590.0
    )
    clock.advance(100.0)
    _refuse(governor, stale, retry_after=500.0)  # until 600
    assert _state(governor).paused_until == 1_800_000_000.0 + 600.0


def test_a_request_never_jumps_a_queue_that_may_start() -> None:
    # A request finding others queued joins the queue, even when a start is possible now.
    clock = FakeClock(manual=True)
    governor = _governor(clock)
    queued = _Waiter(object())
    with governor._lock:
        governor._buckets["stocks"].join(queued)
    waiting = _Waiting(governor, "stocks", lane=object())
    assert clock.wait_for_sleepers(1) == [math.inf]  # behind the queued request
    with governor._lock:
        governor._buckets["stocks"].leave(queued)  # it gives up its place: the next one starts
    waiting.join()
    assert waiting.permit is not None
    governor.done(waiting.permit, answered=True)


# --- Claims --------------------------------------------------------------------------------------


def test_a_claim_reports_its_permit_whatever_interrupts_after_the_grant() -> None:
    # The window between the grant's return and the caller's `try` is gone: the claim is open
    # before the grant, and its exit reports the permit.
    clock = FakeClock(manual=True)
    governor = _governor(clock)
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel())
    )  # paused until 15
    clock.advance(15.0)
    claims: list[Admission] = []

    def interrupted_just_after_the_grant() -> None:
        with governor.admission("stocks", lane=object()) as claim:
            claims.append(claim)
            claim.acquire(Cancel())  # the pause's probe
            raise KeyboardInterrupt  # lands just after the grant returns

    with pytest.raises(KeyboardInterrupt):
        interrupted_just_after_the_grant()
    assert claims[0].permit is not None
    state = _state(governor)
    assert (state.in_flight, state.waiting) == (0, 0)
    # The probe's report came, so the next request probes in its place at once.
    governor.done(governor.admission("stocks", lane=object()).acquire(Cancel()), answered=True)
    assert _state(governor).in_flight == 0


def test_a_claim_reports_nothing_twice_and_refuses_an_unknown_bucket() -> None:
    governor = _governor()
    with governor.admission("stocks", lane=object()) as claim:
        permit = claim.acquire(Cancel())
        governor.done(permit, answered=True)  # reported in the block: the exit does nothing
    assert _state(governor).recent == 1
    with governor.admission("stocks", lane=object()) as unused:
        pass  # nothing granted: nothing to report
    assert unused.permit is None
    with pytest.raises(ValueError, match="unknown bucket 'crypto'"):
        governor.admission("crypto", lane=object())


def test_a_slow_probe_refused_after_a_window_continues_its_run() -> None:
    # The probe is still in flight more than a window after the pause ended: its 429 belongs
    # to the run, so the run's patience is not reset and its next pause grows to 30 s.
    clock = FakeClock(manual=True)
    governor = _governor(clock)
    _refuse(
        governor, governor.admission("stocks", lane=object()).acquire(Cancel())
    )  # paused until 15
    clock.advance(15.0)
    probe = governor.admission("stocks", lane=object()).acquire(Cancel())
    clock.advance(RATE_WINDOW + 10.0)  # answered 71 s after the pause ended
    _refuse(governor, probe)
    assert _state(governor).paused_until == 1_800_000_000.0 + 86.0 + 30.0


def test_a_longer_pause_fails_an_impatient_request_behind_a_patient_one() -> None:
    # Only the head works out its start, but every waiter checks a new pause against its own
    # patience: the impatient request must not sleep behind the patient one.
    clock = FakeClock(manual=True)
    governor = _governor(clock)
    in_flight = governor.admission("stocks", lane=object()).acquire(Cancel())
    _refuse(governor, governor.admission("stocks", lane=object()).acquire(Cancel()))  # until 15
    patient = _Waiting(governor, "stocks", lane=object(), patience=Patience(1000.0))
    clock.wait_for_sleepers(1)
    impatient = _Waiting(governor, "stocks", lane=object(), patience=Patience(90.0))
    assert clock.wait_for_sleepers(2) == [15.0, math.inf]
    _refuse(governor, in_flight, retry_after=500.0)  # stale: the pause grows to 500
    impatient.join()
    assert impatient.error is not None
    assert impatient.error.status == 429
    assert clock.wait_for_sleepers(1) == [500.0]  # the patient one waits the pause out
    clock.advance(500.0)
    patient.join()
    assert patient.permit is not None
    governor.done(patient.permit, answered=True)


# --- Patience: only pauses count ------------------------------------------------------------------


def test_a_patience_counts_each_stretch_of_pause_once() -> None:
    patience = Patience(20.0)
    assert patience.charge(0.0, 15.0)
    assert patience.charge(5.0, 15.0)  # a second request waiting the same pause: no more
    assert patience.spent == 15.0
    assert not patience.charge(15.0, 21.0)  # six more would make 21 > 20
    assert patience.spent == 15.0  # a refused charge charges nothing
    assert patience.charge(15.0, 20.0)
    assert patience.spent == 20.0
    assert UNLIMITED.charge(0.0, 10.0**9)
    assert UNLIMITED.spent == 0.0  # shared by every governor and thread, so never changed


@pytest.mark.parametrize("name", ["seconds", "advice", "spent"])
def test_a_patience_is_read_only_so_the_shared_unlimited_one_never_changes(name: str) -> None:
    with pytest.raises(AttributeError, match="has no setter"):
        setattr(UNLIMITED, name, 1.0)
    with pytest.raises(AttributeError, match="has no deleter"):
        delattr(UNLIMITED, name)
    assert (UNLIMITED.seconds, UNLIMITED.advice, UNLIMITED.spent) == (math.inf, "", 0.0)


def test_a_long_paced_read_still_waits_out_a_short_pause() -> None:
    # Fifteen windows of slot waits spend none of the read's patience: a one-second pause
    # after them is waited out, not refused.
    clock = FakeClock()
    governor = _governor(clock, basic={"stocks"})
    read, patience = object(), Patience(600.0)
    for _ in range(75):  # 5 a window for 15 windows: about 915 s of pacing
        with governor.admission("stocks", lane=read, patience=patience) as admission:
            governor.done(admission.acquire(Cancel()), answered=True)
    assert clock.monotonic() >= 14 * RATE_WINDOW
    with governor.admission("stocks", lane=object()) as other:
        governor.refused(other.acquire(Cancel()), retry_after=1.0, message="slow down")
    with governor.admission("stocks", lane=read, patience=patience) as admission:
        governor.done(admission.acquire(Cancel()), answered=True)
    assert patience.spent <= RATE_WINDOW  # only the pause counted


def test_a_spent_patience_still_starts_a_request_when_nothing_is_paused() -> None:
    # Patience only matters while the bucket is paused: with none spare, an unpaused bucket
    # still admits the read's requests.
    governor = _governor()
    patience = Patience(0.0)
    with governor.admission("stocks", lane=object(), patience=patience) as admission:
        governor.done(admission.acquire(Cancel()), answered=True)
    assert patience.spent == 0.0


def test_the_give_up_message_ends_with_the_patience_s_advice() -> None:
    # The caller knows one more remedy (a console read can read in the background): it comes
    # last, after the governor's own.
    governor = RequestGovernor(BUCKETS, learn_only={"other"}, clock=FakeClock())
    with governor.admission("other", lane=object()) as claim:
        _refuse(governor, claim.acquire(Cancel()), retry_after=60.0)
    patience = Patience(0.0, advice="read in the background")
    with pytest.raises(WireError) as caught:
        governor.admission("other", lane=object(), patience=patience).acquire(Cancel())
    assert str(caught.value).endswith(
        "To go on: retry after that time, or stop the other program; read in the background"
    )


def test_the_give_up_message_offers_no_declaration_for_a_learn_only_bucket() -> None:
    governor = RequestGovernor(BUCKETS, learn_only={"other"}, clock=FakeClock())
    with governor.admission("other", lane=object()) as claim:
        _refuse(governor, claim.acquire(Cancel()), retry_after=60.0)
    with pytest.raises(WireError) as caught:
        governor.admission("other", lane=object(), patience=Patience(0.0)).acquire(Cancel())
    assert str(caught.value).endswith("To go on: retry after that time, or stop the other program")
    assert "declare" not in str(caught.value)


def test_a_shorter_pause_of_another_bucket_never_lets_a_read_count_a_longer_one_twice() -> None:
    # A read spanning two buckets waits out stocks until 100, then other until 50, then
    # stocks again: the stretch up to 100 is counted once.
    patience = Patience(150.0)
    assert patience.charge(0.0, 100.0)
    assert patience.charge(0.0, 50.0)
    assert patience.charge(0.0, 100.0)
    assert patience.spent == 100.0
