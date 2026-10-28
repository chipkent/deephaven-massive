"""`RequestGovernor`: when each REST request may start, per Massive subscription.

Massive limits REST requests per subscription, not per account or per key. It sells five
subscriptions (stocks, options, indices, currencies, futures), each bought on its own tier: on
the free Basic tier a subscription allows `BASIC_REQUESTS_PER_MINUTE` (5) requests per minute,
and every paid tier has no request limit. All the API keys of an account share these counters,
and a request that exceeds one is refused with HTTP 429 and the message "You've exceeded the
maximum requests per minute". Massive offers no way to ask which tier a subscription is on.

The governor holds one *bucket* per subscription (and one for the datasets Massive assigns to
none), each named by an opaque string fixed at construction. Every attempt of a REST request
first takes a *permit* of its bucket (`acquire`) and reports how it ended, exactly once:
`done` (answered, or failed on the wire), `refused` (HTTP 429), or `release` (never sent). A
bucket is:

- **Paced** once it is declared Basic (`declare`) or once Massive's refusal names the
  per-minute maximum (it is *learned* Basic, `learn`). A paced bucket lets a request start only
  while fewer than its limit of *slots* are held. A slot is held by a request in flight, and by
  a finished one until `RATE_WINDOW` (61 s: Massive's minute plus one second of margin for
  latency and clock skew) after its response arrived, which bounds when Massive counted it.
- **Unpaced** otherwise: requests start at once, except during a pause.

**Lanes.** Each waiting request belongs to a *lane*: the requests of one read, one poller's
cycle, or one host decision share a lane (the caller names it, usually by its cancellation
token). A bucket keeps one first-in, first-out queue per lane and serves the lanes round-robin:
the request at the front of the next lane in turn is the bucket's *head*, and once it starts,
its lane goes to the back of the turn. So a host decision or a `poll()` cycle waits for one
request of each other lane, not behind every page of a long history with eight workers. Only
the head works out when it may start; it is woken when that may change (a request finished or
was released, the head before it started or gave up its place, a refusal, a new limit).

After a 429 the bucket pauses: for the `Retry-After` the response asked for; without one, or
with one of zero or in the past, a paced bucket for a whole window and an unpaced one for 15,
30, then 60 s by the number of refusals in the run (`net.retry.rate_limit_wait`). A pause only
ever grows. A run of refusals starts with a counted refusal, or with a refusal of an earlier
burst that lengthens the pause; it ends with the first answer other than a 429 to a request
that started after the latest pause. A burst of 429s to requests already in flight when the
pause began is one refusal, and their late successes do not end the run. Once the run has
lasted `RATE_LIMIT_PATIENCE` (10 minutes), or its pause would end later than that after its
first refusal, the bucket *gives up*: the refused request, every waiting request, and every new
request of the bucket fail with status 429 until that pause has passed, and the first request
after it starts a new run. Waiting for a slot never gives up by itself: a long history at 5
requests per minute takes as long as it takes, and a `Cancel` token ends the wait.

One governor serves every session of a process that uses the same API key and REST origin,
so their requests share the counters as Massive's do (the library's registry builds and shares
them). Thread-safe: one leaf lock guards every bucket, held only to read or change state, never
for a wait, a request, or a callback. Listeners (`subscribe`) are called after the lock is
released and must only enqueue; one that raises is logged and does not disturb the governor.
Time comes from an injectable `Clock`, so tests never sleep.

This module imports only the standard library and `net`.
"""

from __future__ import annotations

import heapq
import logging
import threading
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, Final

from deephaven_massive._massive.net.errors import WireError
from deephaven_massive._massive.net.retry import (
    RATE_LIMIT_PATIENCE,
    SYSTEM_CLOCK,
    rate_limit_wait,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable, Iterable, Mapping
    from collections.abc import Set as AbstractSet

    from deephaven_massive._massive.net.cancel import Cancel
    from deephaven_massive._massive.net.retry import Clock

__all__ = [
    "BASIC_REQUESTS_PER_MINUTE",
    "RATE_WINDOW",
    "BucketState",
    "Permit",
    "RequestGovernor",
    "is_basic_refusal",
]

logger = logging.getLogger(__name__)

BASIC_REQUESTS_PER_MINUTE: Final = 5
"""The requests per minute Massive allows a subscription on its free Basic tier."""

RATE_WINDOW: Final = 61.0
"""Seconds a finished request holds its slot of a paced bucket: Massive's minute plus 1 s."""

_BASIC_REFUSAL: Final = "exceeded the maximum requests per minute"
"""The words of Massive's 429 message that mark a subscription on the Basic tier."""

_IDLE_WAIT: Final = 60.0
"""Seconds a waiting request sleeps when nothing tells it when to look again (it is not the
head of its bucket, or every slot is held by a request in flight); any change wakes it sooner."""


def is_basic_refusal(message: str) -> bool:
    """Tell whether a 429's message says the subscription is on the Basic tier.

    Massive's knowledge base names the message "You've exceeded the maximum requests per
    minute" as the mark of a Basic subscription; a paid subscription has no request limit.

    Args:
        message: The refusal's text: the server's `message`, or a `WireError` message that
            quotes it.

    Returns:
        True when the text contains "exceeded the maximum requests per minute", ignoring case.
    """
    return _BASIC_REFUSAL in message.casefold()


@dataclass(frozen=True, eq=False, slots=True)
class Permit:
    """The right of one attempt of a request to be sent, from `RequestGovernor.acquire`.

    The caller reports how the attempt ended with `RequestGovernor.done`, `refused`, or
    `release`. Only the first report counts: a later one does nothing, so a caller can always
    report in a `finally` without risking a second count. Until the first report the permit
    holds a slot of its bucket, in flight.

    Attributes:
        bucket: The bucket the attempt is charged to.
        episode: The pause the attempt started after (a count of the bucket's pauses), which
            tells a response to the latest pause apart from one to an earlier burst.
    """

    bucket: str
    episode: int


@dataclass(frozen=True, slots=True)
class BucketState:
    """A snapshot of one bucket, for diagnostics (`RequestGovernor.snapshot`).

    Attributes:
        bucket: The bucket's name.
        limit: The requests per minute it is paced at, or None when it is unpaced.
        declared: Whether it is declared Basic (`RequestGovernor.declare`).
        learned: Whether Massive's refusal showed it is Basic.
        in_flight: Requests sent and not yet answered.
        waiting: Requests waiting for a slot or for the end of a pause.
        recent: Slots held: requests in flight, and finished ones less than `RATE_WINDOW`
            seconds after their response.
        requests: Attempts started since the governor was created.
        refusals: HTTP 429 responses since the governor was created.
        paused_until: When the current pause ends, in seconds since the Unix epoch; None when
            no pause is active.
        gave_up: Whether the bucket has given up on its current run of refusals, so its
            requests fail at once until `paused_until`.
        last_refusal: The message of the latest 429, with its request id; None before any.
    """

    bucket: str
    limit: int | None
    declared: bool
    learned: bool
    in_flight: int
    waiting: int
    recent: int
    requests: int
    refusals: int
    paused_until: float | None
    gave_up: bool
    last_refusal: str | None


class _Waiter:
    """One waiting request: its lane and the event that wakes it."""

    __slots__ = ("lane", "wake")

    def __init__(self, lane: Hashable | None) -> None:
        """Create the waiter.

        Args:
            lane: The lane it waits in; None for a lane of its own.
        """
        self.lane: Hashable = self if lane is None else lane
        self.wake = threading.Event()


class _Bucket:
    """The mutable state of one bucket; every field is guarded by the governor's lock."""

    __slots__ = (
        "declared",
        "episode",
        "given_up",
        "in_flight",
        "label",
        "lanes",
        "last_refusal",
        "learned",
        "pause_until",
        "refusals",
        "requests",
        "run_refusals",
        "run_start",
        "slots",
        "turns",
    )

    def __init__(self, label: str) -> None:
        """Start unpaced, idle, and with no refusal.

        Args:
            label: How messages name the bucket's requests, such as `"stocks requests"`.
        """
        self.label = label
        self.declared = False
        self.learned = False
        # The waiting requests of each lane, first in first out, and the lanes' turns.
        self.lanes: dict[Hashable, deque[_Waiter]] = {}
        self.turns: deque[Hashable] = deque()
        self.in_flight = 0
        # When each finished request's slot frees (monotonic seconds), oldest first.
        self.slots: deque[float] = deque()
        self.requests = 0
        self.refusals = 0
        self.pause_until = float("-inf")
        self.episode = 0
        self.run_start: float | None = None
        self.run_refusals = 0
        self.given_up = False
        self.last_refusal: str | None = None

    @property
    def limit(self) -> int | None:
        """The requests per minute the bucket is paced at, or None."""
        return BASIC_REQUESTS_PER_MINUTE if self.declared or self.learned else None

    @property
    def waiting(self) -> int:
        """How many requests wait, in every lane."""
        return sum(len(lane) for lane in self.lanes.values())

    def head(self) -> _Waiter | None:
        """Return the request whose turn it is, or None when none waits."""
        return self.lanes[self.turns[0]][0] if self.turns else None

    def join(self, waiter: _Waiter) -> None:
        """Queue a request at the back of its lane; a new lane takes the last turn.

        Args:
            waiter: The request.
        """
        lane = self.lanes.get(waiter.lane)
        if lane is None:
            lane = self.lanes[waiter.lane] = deque()
            self.turns.append(waiter.lane)
        lane.append(waiter)

    def leave(self, waiter: _Waiter) -> None:
        """Take a request out of its lane (it gave up), and wake the head if that changed.

        Args:
            waiter: The request; one no longer queued (it started) is ignored.
        """
        lane = self.lanes.get(waiter.lane)
        if lane is None or waiter not in lane:
            return
        was_head = self.head() is waiter
        lane.remove(waiter)
        if not lane:
            del self.lanes[waiter.lane]
            self.turns.remove(waiter.lane)
        if was_head:
            self.wake_head()

    def start_head(self) -> None:
        """Remove the head, which starts now, and wake the next head.

        The head's lane takes the last turn if it still has requests waiting.
        """
        lane_key = self.turns.popleft()
        lane = self.lanes[lane_key]
        lane.popleft()
        if lane:
            self.turns.append(lane_key)
        else:
            del self.lanes[lane_key]
        self.wake_head()

    def wake_head(self) -> None:
        """Wake the head, if any, to work out its start again."""
        head = self.head()
        if head is not None:
            head.wake.set()

    def expire(self, now: float) -> None:
        """Free the slots whose window has passed.

        Args:
            now: The monotonic time.
        """
        while self.slots and self.slots[0] <= now:
            self.slots.popleft()

    def gave_up(self, now: float) -> bool:
        """Tell whether the bucket's requests fail at once now.

        Args:
            now: The monotonic time.

        Returns:
            True while the bucket has given up and its pause has not ended.
        """
        return self.given_up and now < self.pause_until

    def end_run(self) -> None:
        """Forget the run of refusals: an answer came, or a given-up pause has ended."""
        self.run_start = None
        self.run_refusals = 0
        self.given_up = False

    def judge_run(self, start: float) -> None:
        """Give up once the run's pause would end past the patience from its first refusal.

        A refusal always pauses for at least 15 s (a `Retry-After` of zero counts as none), so
        a run that has lasted the patience has a pause ending past it: this one rule also gives
        up after ten minutes of refusals, whatever each pause.

        Args:
            start: When the run's first refusal came.
        """
        if self.pause_until > start + RATE_LIMIT_PATIENCE:
            self.given_up = True

    def start_at(self, now: float) -> float | None:
        """Return when the bucket's head may start, or None to wait for a finish.

        Args:
            now: The monotonic time; the expired slots are already freed.

        Returns:
            The monotonic time it may start (`now` or earlier means at once), or None when
            every slot is held by a request in flight, so only a finish can free one.
        """
        start = max(now, self.pause_until)
        limit = self.limit
        if limit is None:
            return start
        excess = self.in_flight + len(self.slots) - limit
        if excess < 0:
            return start
        if excess >= len(self.slots):
            return None
        return max(start, self.slots[excess])

    def finish(self, now: float, *, hold: bool) -> None:
        """End a request's flight.

        Args:
            now: The monotonic time its response arrived (or its attempt failed).
            hold: True to keep its slot for a window (it may have reached Massive), False to
                free it at once (it was never sent).
        """
        self.in_flight -= 1
        if hold:
            self.slots.append(now + RATE_WINDOW)
        self.wake_head()


class RequestGovernor:
    """Admits the REST requests of each subscription at its rate, and pauses them after a 429.

    The library's registry creates one per (API key, REST origin) of the process and shares it
    between the sessions that use them; each session's `RestClient` takes a permit for every
    attempt. Tests create their own with a fake clock. The buckets are fixed at construction;
    every method that names another raises `ValueError`. See the module docstring for the
    rules and the thread-safety.

    Example:
        ::

            from deephaven_massive._massive import Cancel, RequestGovernor

            governor = RequestGovernor(("stocks", "options", "other"))
            governor.declare({"stocks"})  # Basic: 5 requests per minute
            closed = Cancel()
            permit = governor.acquire("stocks", closed)  # blocks while no slot is free
            try:
                ...  # send the request
            finally:
                governor.done(permit, answered=True)
    """

    BASIC_REQUESTS_PER_MINUTE: ClassVar[int] = BASIC_REQUESTS_PER_MINUTE
    """The requests per minute of a Basic subscription, the pace of a paced bucket."""

    RATE_WINDOW: ClassVar[float] = RATE_WINDOW
    """Seconds a finished request of a paced bucket holds its slot."""

    __slots__ = ("_buckets", "_clock", "_listeners", "_lock", "_open")

    def __init__(
        self,
        buckets: Iterable[str],
        *,
        clock: Clock = SYSTEM_CLOCK,
        labels: Mapping[str, str] | None = None,
    ) -> None:
        """Create the buckets, each unpaced and idle.

        Args:
            buckets: The bucket names, such as `("stocks", "options", ..., "other")`.
            clock: The time source of every wait and pause; the clients that use the
                governor wait on it too. Tests pass a fake.
            labels: How messages name each bucket's requests, such as `{"other": "requests
                outside Massive's subscriptions"}`; a bucket left out is `"<name> requests"`.

        Raises:
            ValueError: If `buckets` is empty or names one twice.
        """
        names = tuple(buckets)
        if not names or len(set(names)) != len(names):
            raise ValueError(f"buckets must be distinct names, at least one: {names!r}")
        named = labels or {}
        self._clock = clock
        self._lock = threading.Lock()
        self._buckets = {name: _Bucket(named.get(name, f"{name} requests")) for name in names}
        self._listeners: tuple[Callable[[], None], ...] = ()
        # The permits granted and not yet reported; guarded by the lock.
        self._open: set[Permit] = set()

    @property
    def clock(self) -> Clock:
        """The governor's time source, which its clients also wait on."""
        return self._clock

    @property
    def buckets(self) -> tuple[str, ...]:
        """The bucket names, in construction order."""
        return tuple(self._buckets)

    def declare(self, basic: AbstractSet[str]) -> None:
        """Set which buckets are declared Basic, and so paced from their first request.

        A bucket left out is no longer declared; it stays paced only if it was learned Basic.
        An unknown name raises `ValueError` (`_check`).

        Args:
            basic: The buckets declared Basic.
        """
        self._check(basic)
        with self._lock:
            for name, bucket in self._buckets.items():
                bucket.declared = name in basic
                bucket.wake_head()
        self._notify()

    def learn(self, basic: AbstractSet[str]) -> None:
        """Mark buckets learned Basic, as a refusal naming the per-minute maximum would.

        The library's registry calls it to carry what an earlier governor of the same key
        learned into a new one. Learned stays learned for the governor's life. An unknown name
        raises `ValueError` (`_check`).

        Args:
            basic: The buckets known to be Basic.
        """
        self._check(basic)
        with self._lock:
            for name in basic:
                bucket = self._buckets[name]
                bucket.learned = True
                bucket.wake_head()
        self._notify()

    def acquire(self, bucket: str, cancel: Cancel, *, lane: Hashable | None = None) -> Permit:
        """Wait until a request of `bucket` may start, and return its permit.

        Blocks the calling thread, outside every lock, for as long as the bucket's lanes,
        pace, and pause demand; waiting for a slot never times out. Report the attempt's end
        with `done`, `refused`, or `release`. If this thread is interrupted after the permit
        was granted, the permit is released before the exception propagates. An unknown
        `bucket` raises `ValueError`.

        Args:
            bucket: The bucket the request is charged to.
            cancel: Ends the wait when set: the request gives up its place.
            lane: The lane the request waits in, such as its read's cancellation token: the
                requests of one lane start in order, and the lanes of a bucket take turns.
                None for a lane of its own.

        Returns:
            The permit.

        The wait (`_wait`) raises `WireError` when `cancel` is set (no status; the message
        says the wait was cancelled), and with status 429 while the bucket has given up on a
        run of refusals (the message says how long Massive has refused the bucket's requests
        and how long they fail at once, and quotes the latest refusal).
        """
        state = self._bucket(bucket)
        waiter = _Waiter(lane)
        granted: list[Permit] = []
        stop_listening = cancel.on_set(waiter.wake.set)
        try:
            with self._lock:
                state.join(waiter)
            self._notify()
            granted.append(self._wait(bucket, waiter, cancel))
            self._notify()
        except BaseException:
            for permit in granted:  # interrupted after the grant: hand it back
                self.release(permit)
            raise
        finally:
            stop_listening()
            with self._lock:
                state.leave(waiter)
        return granted[0]

    def _wait(self, bucket: str, waiter: _Waiter, cancel: Cancel) -> Permit:
        """Sleep until a queued request is admitted, and return its permit.

        Args:
            bucket: The bucket's name, a known one.
            waiter: The queued request.
            cancel: The request's cancellation.

        Returns:
            The permit.

        Raises:
            WireError: When the wait must fail, as `acquire` describes.
        """
        while True:
            permit, delay, failure = self._step(bucket, waiter, cancel)
            if failure is not None:
                message, status = failure
                raise WireError(message, status=status)
            if permit is not None:
                return permit
            self._clock.wait(waiter.wake, delay)

    def done(self, permit: Permit, *, answered: bool) -> None:
        """Report an attempt that ended without a 429; its slot stays held for a window.

        Does nothing for a permit already reported.

        Args:
            permit: The attempt's permit.
            answered: True when Massive answered (any status but 429), which ends the
                bucket's run of refusals if the attempt started after the latest pause; False
                when no response came (a connection failure, an interruption), which changes
                nothing else.
        """
        state = self._bucket(permit.bucket)
        with self._lock:
            if not self._report(permit):
                return
            state.finish(self._clock.monotonic(), hold=True)
            if answered and permit.episode == state.episode:
                state.end_run()
        self._notify()

    def release(self, permit: Permit) -> None:
        """Report an attempt that was never sent: its slot frees at once.

        Does nothing for a permit already reported.

        Args:
            permit: The attempt's permit.
        """
        state = self._bucket(permit.bucket)
        with self._lock:
            if not self._report(permit):
                return
            state.finish(self._clock.monotonic(), hold=False)
        self._notify()

    def refused(
        self,
        permit: Permit,
        *,
        retry_after: float | None,
        message: str,
        request_id: str | None,
    ) -> bool:
        """Report an attempt that Massive refused with HTTP 429, and pause its bucket.

        A refusal whose message names the per-minute maximum (`is_basic_refusal`) makes the
        bucket learned Basic, so it is paced from now on. A refusal to an attempt that
        started after the latest pause counts toward the run and sets a new pause; one to an
        attempt that started before it only lengthens the pause to its own `Retry-After` (and
        starts a run if none is going). A `Retry-After` of zero counts as none. The caller
        then asks for a new permit, which waits out the pause. Does nothing for a permit
        already reported.

        Args:
            permit: The attempt's permit.
            retry_after: The wait the response's `Retry-After` asked for, in seconds, or None
                when it gave none.
            message: The refusal's text, quoted in diagnostics and in the error of a give-up.
            request_id: The response's request id, or None.

        Returns:
            Whether the message showed the bucket is Basic.

        Raises:
            WireError: With status 429 and `request_id` when the bucket gives up (the run has
                lasted, or would last, more than `RATE_LIMIT_PATIENCE`); the pause is set
                first, so the bucket's other requests stop too.
        """
        state = self._bucket(permit.bucket)
        basic = is_basic_refusal(message)
        asked = retry_after if retry_after is not None and retry_after > 0 else None
        give_up = None
        with self._lock:
            if not self._report(permit):
                return basic
            now = self._clock.monotonic()
            state.finish(now, hold=True)
            state.refusals += 1
            state.last_refusal = message
            if basic:
                state.learned = True
            if permit.episode == state.episode:
                state.episode += 1
                state.run_refusals += 1
                if state.run_start is None:
                    state.run_start = now
                if asked is not None or state.limit is None:
                    pause = rate_limit_wait(state.run_refusals, asked)
                else:
                    pause = RATE_WINDOW
                state.pause_until = max(state.pause_until, now + pause)
                state.judge_run(state.run_start)
            elif asked is not None and now + asked > state.pause_until:
                state.pause_until = now + asked
                if state.run_start is None:
                    state.run_start = now
                state.judge_run(state.run_start)
            if state.gave_up(now):
                give_up = self._give_up_text(state, now)
        self._notify()
        if give_up is not None:
            raise WireError(give_up, status=429, request_id=request_id)
        return basic

    def snapshot(self) -> tuple[BucketState, ...]:
        """Return the state of every bucket, in construction order.

        Returns:
            One `BucketState` per bucket.
        """
        with self._lock:
            now = self._clock.monotonic()
            wall = self._clock.time()
            states = []
            for name, bucket in self._buckets.items():
                bucket.expire(now)
                paused = bucket.pause_until > now
                states.append(
                    BucketState(
                        bucket=name,
                        limit=bucket.limit,
                        declared=bucket.declared,
                        learned=bucket.learned,
                        in_flight=bucket.in_flight,
                        waiting=bucket.waiting,
                        recent=bucket.in_flight + len(bucket.slots),
                        requests=bucket.requests,
                        refusals=bucket.refusals,
                        paused_until=wall + bucket.pause_until - now if paused else None,
                        gave_up=bucket.gave_up(now),
                        last_refusal=bucket.last_refusal,
                    )
                )
        return tuple(states)

    def next_change(self) -> float | None:
        """Return the seconds until a bucket's state changes with time alone.

        A pause ends, or a held slot frees, without any request or response; diagnostics
        that show the state read it again then.

        Returns:
            Seconds from now until the earliest pause end or slot release, or None when no
            bucket has either.
        """
        with self._lock:
            now = self._clock.monotonic()
            moments = []
            for bucket in self._buckets.values():
                bucket.expire(now)
                if bucket.pause_until > now:
                    moments.append(bucket.pause_until)
                if bucket.slots:
                    moments.append(bucket.slots[0])
        return min(moments) - now if moments else None

    def estimate(self, bucket: str, requests: int) -> float | None:
        """Return a lower bound on how long `requests` new requests of a bucket take to start.

        The bound assumes every response arrives at once and no other request joins the
        bucket: the slots already held free at their times (a request in flight a full window
        from now), the current pause holds every start back, and each new request holds its
        slot a window from its start. Further pages of a request and other sessions' requests
        only add time.

        Args:
            bucket: The bucket.
            requests: How many requests will be made.

        Returns:
            Seconds from now until the last of them can start (0.0 when all can start at
            once), or None when the bucket is unpaced or `requests` is less than 1. An
            unknown `bucket` raises `ValueError`.
        """
        state = self._bucket(bucket)
        with self._lock:
            limit = state.limit
            if limit is None or requests < 1:
                return None
            now = self._clock.monotonic()
            state.expire(now)
            held = [*state.slots, *[now + RATE_WINDOW] * state.in_flight]
            start = max(now, state.pause_until)
        heapq.heapify(held)
        for _ in range(requests):
            while len(held) >= limit:
                start = max(start, heapq.heappop(held))
            heapq.heappush(held, start + RATE_WINDOW)
        return start - now

    def idle(self) -> bool:
        """Tell whether the governor holds nothing worth keeping but what it learned.

        Returns:
            True when no bucket has a request in flight or waiting, a slot held, or a pause
            active.
        """
        with self._lock:
            now = self._clock.monotonic()
            for bucket in self._buckets.values():
                bucket.expire(now)
                if bucket.in_flight or bucket.lanes or bucket.slots or bucket.pause_until > now:
                    return False
            return True

    def subscribe(self, listener: Callable[[], None]) -> None:
        """Call `listener` after every change of any bucket.

        Args:
            listener: Called without arguments and without the governor's lock, on the thread
                that made the change (a request's thread, any session's). It must only note
                the change (enqueue, set an event) and return; an `Exception` it raises is
                logged at ERROR and ignored.
        """
        with self._lock:
            self._listeners = (*self._listeners, listener)

    def unsubscribe(self, listener: Callable[[], None]) -> None:
        """Stop calling a listener; an unknown one is ignored.

        Args:
            listener: A listener given to `subscribe` (compared by equality, so a bound
                method may be passed again).
        """
        with self._lock:
            self._listeners = tuple(each for each in self._listeners if each != listener)

    def _report(self, permit: Permit) -> bool:
        """Mark a permit reported; the lock is held.

        Args:
            permit: The permit.

        Returns:
            True for its first report, False for any later one.
        """
        if permit not in self._open:
            return False
        self._open.remove(permit)
        return True

    def _check(self, names: Iterable[str]) -> None:
        """Refuse bucket names the governor does not have.

        Args:
            names: The names.

        Raises:
            ValueError: If one is unknown.
        """
        unknown = set(names) - self._buckets.keys()
        if unknown:
            raise ValueError(f"unknown buckets {sorted(unknown)}; known: {list(self._buckets)}")

    def _bucket(self, bucket: str) -> _Bucket:
        """Return a bucket's state.

        Args:
            bucket: The bucket's name.

        Returns:
            Its state.

        Raises:
            ValueError: If the bucket is unknown.
        """
        state = self._buckets.get(bucket)
        if state is None:
            raise ValueError(f"unknown bucket {bucket!r}; known: {list(self._buckets)}")
        return state

    def _step(
        self, bucket: str, waiter: _Waiter, cancel: Cancel
    ) -> tuple[Permit | None, float, tuple[str, int | None] | None]:
        """Admit a waiting request if it may start now, or say how long it should sleep.

        Args:
            bucket: The bucket's name, a known one.
            waiter: The waiting request, queued in the bucket.
            cancel: The request's cancellation.

        Returns:
            `(permit, 0.0, None)` when the request starts now (it leaves its lane, and the
            next head is woken); `(None, seconds, None)` to sleep that long unless woken
            sooner; `(None, 0.0, (message, status))` when the wait must fail: cancelled (no
            status) or given up (429).
        """
        state = self._buckets[bucket]
        with self._lock:
            waiter.wake.clear()  # before reading the state, so no change is missed
            if cancel.is_set():
                return None, 0.0, (f"{bucket}: the wait for a request slot was cancelled", None)
            now = self._clock.monotonic()
            if state.given_up and now >= state.pause_until:
                state.end_run()  # the given-up pause is over: the next refusal starts a run
            if state.gave_up(now):
                return None, 0.0, (self._give_up_text(state, now), 429)
            if state.head() is not waiter:
                return None, _IDLE_WAIT, None
            state.expire(now)
            start = state.start_at(now)
            if start is None:
                return None, _IDLE_WAIT, None
            if start > now:
                return None, start - now, None
            state.start_head()
            state.in_flight += 1
            state.requests += 1
            permit = Permit(bucket, state.episode)
            self._open.add(permit)
            return permit, 0.0, None

    @staticmethod
    def _give_up_text(state: _Bucket, now: float) -> str:
        """Return the message of a request that will not wait out its bucket's pause.

        Called with the lock held, while the bucket has given up (so its run has started).

        Args:
            state: The bucket.
            now: The monotonic time.

        Returns:
            The message: how long Massive has refused the bucket's requests, how long they
            fail at once, and the latest refusal.
        """
        start = now if state.run_start is None else state.run_start
        return (
            f"Massive has refused {state.label} with HTTP 429 for {now - start:.0f} s and "
            f"asks to wait {state.pause_until - now:.0f} s more, past the "
            f"{RATE_LIMIT_PATIENCE:.0f} s the library waits from the first refusal; they fail "
            f"at once until the pause ends (latest refusal: {state.last_refusal})"
        )

    def _notify(self) -> None:
        """Call every listener, outside the lock; one that raises is logged and skipped."""
        with self._lock:
            listeners = self._listeners
        for listener in listeners:
            try:
                listener()
            except Exception:
                logger.exception("a rate-limit listener failed")
