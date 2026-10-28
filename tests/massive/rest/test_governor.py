"""Tests for `RequestGovernor`: pacing, pauses after a 429, give-up, cancellation, diagnostics.

Time is virtual (`FakeClock`): automatic where one thread runs, manual where a test needs to
see threads wait before it lets them go.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.net.cancel import Cancel
from deephaven_massive._massive.net.errors import WireError
from deephaven_massive._massive.rest.governor import (
    BASIC_REQUESTS_PER_MINUTE,
    RATE_WINDOW,
    BucketState,
    Permit,
    RequestGovernor,
    is_basic_refusal,
)
from tests._fakes.clock import FakeClock

if TYPE_CHECKING:
    from collections.abc import Callable

BUCKETS = ("stocks", "options", "other")
BASIC = "You've exceeded the maximum requests per minute, please wait or upgrade."
TIMEOUT = 10.0


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
        governor.done(governor.acquire(bucket, Cancel()), answered=True)


def _refuse(
    governor: RequestGovernor,
    permit: Permit,
    *,
    retry_after: float | None = None,
    message: str = "HTTP 429: slow down",
) -> bool:
    return governor.refused(permit, retry_after=retry_after, message=message, request_id="r1")


class _Waiting:
    """A thread that acquires a permit and records the outcome."""

    def __init__(
        self, governor: RequestGovernor, bucket: str, cancel: Cancel | None = None
    ) -> None:
        self.cancel = cancel or Cancel()
        self.permit: Permit | None = None
        self.error: WireError | None = None
        self.started: float | None = None
        self._governor = governor
        self._bucket = bucket
        self.thread = threading.Thread(target=self._run)
        self.thread.start()

    def _run(self) -> None:
        try:
            self.permit = self._governor.acquire(self._bucket, self.cancel)
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
        governor.acquire("crypto", Cancel())
    with pytest.raises(ValueError, match="unknown bucket 'crypto'"):
        governor.estimate("crypto", 1)
    with pytest.raises(ValueError, match=r"unknown buckets \['crypto'\]"):
        governor.declare({"crypto"})


def test_the_basic_rate_is_five_a_minute_with_a_second_of_margin() -> None:
    assert BASIC_REQUESTS_PER_MINUTE == RequestGovernor.BASIC_REQUESTS_PER_MINUTE == 5
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
    assert _state(governor).requests == 50


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
    permits = [governor.acquire("stocks", Cancel()) for _ in range(5)]
    waiting = _Waiting(governor, "stocks")
    # Every slot is in flight: nothing says when one frees, so it sleeps until woken.
    assert clock.wait_for_sleepers(1) == [60.0]
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
    first = _Waiting(governor, "stocks")
    assert clock.wait_for_sleepers(1) == [61.0]
    second = _Waiting(governor, "stocks")
    # Not at the head, the second only sleeps until woken; it never computes a start.
    assert clock.wait_for_sleepers(2) == [61.0, 90.0]
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
    waiting = _Waiting(governor, "stocks")
    assert clock.wait_for_sleepers(1) == [61.0]
    governor.declare(set())  # no longer Basic: it starts at once
    waiting.join()
    assert waiting.started == 0.0


# --- Cancellation --------------------------------------------------------------------------------


def test_a_cancelled_wait_gives_up_its_place_and_wakes_the_next() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock, basic={"stocks"})
    _requests(governor, "stocks", 5)
    head = _Waiting(governor, "stocks")
    clock.wait_for_sleepers(1)
    behind = _Waiting(governor, "stocks")
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
    head = _Waiting(governor, "stocks")
    clock.wait_for_sleepers(1)
    behind = _Waiting(governor, "stocks")
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
        governor.acquire("stocks", cancel)
    assert _state(governor).waiting == 0
    assert clock.waits == []


# --- Refusals ------------------------------------------------------------------------------------


def test_an_unpaced_refusal_pauses_only_its_bucket_15_30_60_then_60() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    for _ in range(4):
        _refuse(governor, governor.acquire("stocks", Cancel()))
        _requests(governor, "options", 1)  # never held back
    _requests(governor, "stocks", 1)
    assert clock.waits == [15.0, 30.0, 60.0, 60.0]


def test_an_answer_after_the_pause_ends_the_run() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    _refuse(governor, governor.acquire("stocks", Cancel()))
    _requests(governor, "stocks", 1)  # waits 15, then is answered
    _refuse(governor, governor.acquire("stocks", Cancel()))  # a new run: 15 again
    _requests(governor, "stocks", 1)
    assert clock.waits == [15.0, 15.0]


def test_a_connection_failure_does_not_end_the_run() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    _refuse(governor, governor.acquire("stocks", Cancel()))
    governor.done(governor.acquire("stocks", Cancel()), answered=False)
    _refuse(governor, governor.acquire("stocks", Cancel()))
    _requests(governor, "stocks", 1)
    assert clock.waits == [15.0, 30.0]


def test_a_burst_of_refusals_in_flight_is_one_refusal_and_stale_answers_keep_the_run() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    burst = [governor.acquire("stocks", Cancel()) for _ in range(3)]
    _refuse(governor, burst[0])  # pause 15
    _refuse(governor, burst[1])  # started before the pause: not counted
    governor.done(burst[2], answered=True)  # nor does its success end the run
    _refuse(governor, governor.acquire("stocks", Cancel()))  # the run's second: 30
    _requests(governor, "stocks", 1)
    assert clock.waits == [15.0, 30.0]
    assert _state(governor).refusals == 3


def test_retry_after_sets_the_pause_and_a_stale_one_only_lengthens_it() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    burst = [governor.acquire("stocks", Cancel()) for _ in range(3)]
    _refuse(governor, burst[0], retry_after=20.0)
    _refuse(governor, burst[1], retry_after=5.0)  # shorter: the pause stays 20
    _refuse(governor, burst[2], retry_after=40.0)  # longer: it grows to 40
    _requests(governor, "stocks", 1)
    assert clock.waits == [40.0]


def test_a_refusal_naming_the_per_minute_maximum_makes_the_bucket_learned_basic() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    assert _refuse(governor, governor.acquire("stocks", Cancel()), message=BASIC)
    state = _state(governor)
    assert (state.limit, state.learned, state.declared) == (5, True, False)
    _requests(governor, "stocks", 1)  # a paced bucket pauses a whole window
    assert clock.waits == [61.0]
    governor.declare(set())  # learned stays learned
    assert _state(governor).limit == 5


def test_a_refusal_without_those_words_only_pauses() -> None:
    governor = _governor()
    assert not _refuse(governor, governor.acquire("stocks", Cancel()))
    assert _state(governor).limit is None


def test_a_paced_bucket_pauses_a_window_and_counts_the_refused_request() -> None:
    clock = FakeClock()
    governor = _governor(clock, basic={"stocks"})
    _refuse(governor, governor.acquire("stocks", Cancel()))
    assert _state(governor).recent == 1
    _requests(governor, "stocks", 1)
    assert clock.waits == [61.0]


def test_a_refusal_wakes_a_head_whose_pause_it_lengthens() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock)
    in_flight = governor.acquire("stocks", Cancel())
    _refuse(governor, governor.acquire("stocks", Cancel()))  # pause until 15
    waiting = _Waiting(governor, "stocks")
    assert clock.wait_for_sleepers(1) == [15.0]
    _refuse(governor, in_flight, retry_after=40.0)  # stale, but lengthens the pause
    assert clock.wait_for_sleepers(1) == [40.0]
    clock.advance(40.0)
    waiting.join()
    assert waiting.started == 40.0


# --- Giving up -----------------------------------------------------------------------------------


def test_a_retry_after_beyond_ten_minutes_gives_up_at_once_but_still_pauses() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    with pytest.raises(WireError) as caught:
        _refuse(governor, governor.acquire("stocks", Cancel()), retry_after=601.0)
    assert (caught.value.status, caught.value.request_id) == (429, "r1")
    assert str(caught.value) == (
        "Massive refused stocks requests with HTTP 429, and resuming would take until 601 s "
        "after the first refusal, more than the 600 s the library waits (latest refusal: "
        "HTTP 429: slow down)"
    )
    state = _state(governor)
    assert state.gave_up
    # Every new request of the bucket fails at once, without waiting, until the pause ends.
    with pytest.raises(WireError, match="resuming would take until 601 s") as again:
        governor.acquire("stocks", Cancel())
    assert (again.value.status, again.value.request_id) == (429, None)
    assert clock.waits == []
    _requests(governor, "options", 1)
    clock.advance(601.0)
    _requests(governor, "stocks", 1)  # the pause is over: one more try
    assert not _state(governor).gave_up


def test_a_retry_after_of_exactly_ten_minutes_is_waited_for() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    _refuse(governor, governor.acquire("stocks", Cancel()), retry_after=600.0)
    _requests(governor, "stocks", 1)
    assert clock.waits == [600.0]


def test_a_run_gives_up_once_resuming_would_pass_ten_minutes_from_its_first_refusal() -> None:
    clock = FakeClock()
    governor = _governor(clock)
    refusals: list[Permit] = []

    def refuse_forever() -> None:
        while True:
            refusals.append(governor.acquire("stocks", Cancel()))
            _refuse(governor, refusals[-1])

    with pytest.raises(WireError, match="resuming would take until 645 s"):
        refuse_forever()
    # 15 + 30 + 60 x 9 = 585 s of pauses; the twelfth refusal, at 585 s, would pause to 645 s.
    assert len(refusals) == 12
    assert clock.monotonic() == 585.0


def test_a_waiting_request_gives_up_when_a_refusal_outlasts_the_patience() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock)
    in_flight = governor.acquire("stocks", Cancel())
    _refuse(governor, governor.acquire("stocks", Cancel()))
    waiting = _Waiting(governor, "stocks")
    clock.wait_for_sleepers(1)
    with pytest.raises(WireError):
        _refuse(governor, in_flight, retry_after=700.0)
    waiting.join()
    assert waiting.error is not None
    assert waiting.error.status == 429


# --- Snapshots, estimates, idleness, listeners ---------------------------------------------------


def test_snapshot_reports_every_bucket() -> None:
    clock = FakeClock(wall=1_800_000_000.0)
    governor = _governor(clock, basic={"options"})
    held = governor.acquire("stocks", Cancel())
    _refuse(governor, governor.acquire("stocks", Cancel()), retry_after=30.0)
    clock.advance(10.0)
    stocks, options, other = governor.snapshot()
    assert stocks.bucket == "stocks"
    assert (stocks.limit, stocks.declared, stocks.learned) == (None, False, False)
    assert (stocks.in_flight, stocks.waiting, stocks.recent) == (1, 0, 2)
    assert (stocks.requests, stocks.refusals) == (2, 1)
    assert stocks.paused_until == 1_800_000_030.0
    assert not stocks.gave_up
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
    in_flight = governor.acquire("stocks", Cancel())  # frees no sooner than 61 from now
    _requests(governor, "stocks", 1)
    clock.advance(30.0)  # that slot frees at 61, 31 from now
    assert governor.estimate("stocks", 3) == 0.0
    assert governor.estimate("stocks", 4) == 31.0
    assert governor.estimate("stocks", 5) == 61.0
    governor.done(in_flight, answered=True)


def test_estimate_counts_the_pause() -> None:
    clock = FakeClock()
    governor = _governor(clock, basic={"stocks"})
    _refuse(governor, governor.acquire("stocks", Cancel()), retry_after=100.0)
    assert governor.estimate("stocks", 1) == 100.0


def test_idle_once_nothing_is_held() -> None:
    clock = FakeClock()
    governor = _governor(clock, basic={"stocks"})
    assert governor.idle()
    permit = governor.acquire("stocks", Cancel())
    assert not governor.idle()  # in flight
    governor.done(permit, answered=True)
    assert not governor.idle()  # its slot is held
    clock.advance(61.0)
    assert governor.idle()
    _refuse(governor, governor.acquire("options", Cancel()), retry_after=200.0)
    clock.advance(61.0)
    assert not governor.idle()  # paused
    clock.advance(200.0)
    assert governor.idle()


def test_a_waiting_request_keeps_the_governor_busy() -> None:
    clock = FakeClock(manual=True)
    governor = _governor(clock, basic={"stocks"})
    _requests(governor, "stocks", 5)
    clock.advance(61.0)
    permits = [governor.acquire("stocks", Cancel()) for _ in range(5)]
    for permit in permits:
        governor.done(permit, answered=True)
    waiting = _Waiting(governor, "stocks")
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
    _requests(governor, "stocks", 1)  # queued, admitted, answered
    assert len(heard) == 3
    governor.declare({"stocks"})
    assert len(heard) == 4
    governor.unsubscribe(listener)
    governor.unsubscribe(listener)  # unknown now: ignored
    _requests(governor, "stocks", 1)
    assert len(heard) == 4


# --- Many threads --------------------------------------------------------------------------------


def _drive(clock: FakeClock, alive: Callable[[], int]) -> None:
    """Advance virtual time to the earliest deadline whenever every live worker sleeps."""
    deadline = time.monotonic() + 60.0
    while alive():
        assert time.monotonic() < deadline, "the workers stopped making progress"
        sleeping = clock.sleeping()
        if sleeping and len(sleeping) == alive():
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
                permit = governor.acquire("stocks", Cancel())
                with lock:
                    starts.append(clock.monotonic())
                governor.done(permit, answered=True)
        finally:
            with lock:
                running[0] -= 1

    def alive() -> int:
        with lock:
            return running[0]

    threads = [threading.Thread(target=worker) for _ in range(16)]
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
    assert (state.requests, state.in_flight, state.waiting) == (160, 0, 0)
