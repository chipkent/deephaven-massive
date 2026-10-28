"""`RequestGovernor`: when each REST request may start, per Massive subscription.

Massive limits REST requests per subscription, not per account or per key. It sells five
subscriptions (stocks, options, indices, currencies, futures), each bought on its own tier: on
the free Basic tier a subscription allows `BASIC_REQUESTS_PER_MINUTE` (5) requests per minute,
and every paid tier has no request limit. All the API keys of an account share these counters,
and a request that exceeds one is refused with HTTP 429 and the message "You've exceeded the
maximum requests per minute". Massive offers no way to ask which tier a subscription is on.

The governor holds one *bucket* per subscription (and one for the datasets Massive assigns to
none), each named by an opaque string fixed at construction. Every attempt of a REST request
first takes a *permit* of its bucket (`acquire`) and reports how it ended (`done`, or
`refused` for a 429). A bucket is:

- **Paced** once it is declared Basic (`declare`) or once Massive's refusal names the
  per-minute maximum (it is *learned* Basic). A paced bucket lets a request start only while
  fewer than its limit of *slots* are held. A slot is held by a request in flight, and by a
  finished one until `RATE_WINDOW` (61 s: Massive's minute plus one second of margin for
  latency and clock skew) after its response arrived, which bounds when Massive counted it.
  Requests start in the order they asked, first in, first out: only the head of a bucket's
  queue works out when it may start, and it is woken when that may change (a request
  finished, a request ahead of it gave up its place, a refusal, a new limit).
- **Unpaced** otherwise: requests start at once, except during a pause.

After a 429 the bucket pauses: for the `Retry-After` the response asked for; without one, a
paced bucket for a whole window, and an unpaced one for 15, 30, then 60 s by the number of
refusals in the run (`net.retry.rate_limit_wait`). A pause only ever grows. A run of refusals
ends with the first answer other than a 429 to a request that started after the latest pause;
a burst of 429s to requests already in flight when the pause began is one refusal, and their
late successes do not end the run. When resuming would take more than `RATE_LIMIT_PATIENCE`
(10 minutes) from the run's first refusal, the refused request, every waiting request, and
every new request of the bucket fail with status 429 until that pause has passed. Waiting for
a slot never gives up by itself: a long history at 5 requests per minute takes as long as it
takes, and a `Cancel` token ends the wait.

One governor serves every session of a process that uses the same API key and REST origin,
so their requests share the counters as Massive's do (the library's registry builds and shares
them). Thread-safe: one leaf lock guards every bucket, held only to read or change state, never
for a wait, a request, or a callback. Listeners (`subscribe`) are called after the lock is
released and must only enqueue. Time comes from an injectable `Clock`, so tests never sleep.

This module imports only the standard library and `net`.
"""

from __future__ import annotations

import heapq
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
    from collections.abc import Callable, Iterable
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

BASIC_REQUESTS_PER_MINUTE: Final = 5
"""The requests per minute Massive allows a subscription on its free Basic tier."""

RATE_WINDOW: Final = 61.0
"""Seconds a finished request holds its slot of a paced bucket: Massive's minute plus 1 s."""

_BASIC_REFUSAL: Final = "exceeded the maximum requests per minute"
"""The words of Massive's 429 message that mark a subscription on the Basic tier."""

_IDLE_WAIT: Final = 60.0
"""Seconds a waiting request sleeps when nothing tells it when to look again (it is not at the
head of its queue, or every slot is held by a request in flight); any change wakes it sooner."""


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


@dataclass(frozen=True, slots=True)
class Permit:
    """The right of one attempt of a request to be sent, from `RequestGovernor.acquire`.

    The caller reports how the attempt ended exactly once, with `RequestGovernor.done` or
    `RequestGovernor.refused`; until then it holds a slot of its bucket.

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
        gave_up: Whether the current pause ends more than `RATE_LIMIT_PATIENCE` after the
            run's first refusal, so the bucket's requests fail at once until it ends.
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
    """One request in a bucket's queue, with the event that wakes it."""

    __slots__ = ("wake",)

    def __init__(self) -> None:
        """Create the waiter's wake event, set by whatever may let it start sooner."""
        self.wake = threading.Event()


class _Bucket:
    """The mutable state of one bucket; every field is guarded by the governor's lock."""

    __slots__ = (
        "declared",
        "episode",
        "in_flight",
        "last_refusal",
        "learned",
        "pause_until",
        "queue",
        "refusals",
        "requests",
        "run_refusals",
        "run_start",
        "slots",
    )

    def __init__(self) -> None:
        """Start unpaced, idle, and with no refusal."""
        self.declared = False
        self.learned = False
        self.queue: deque[_Waiter] = deque()
        self.in_flight = 0
        # When each finished request's slot frees (monotonic seconds), oldest first.
        self.slots: deque[float] = deque()
        self.requests = 0
        self.refusals = 0
        self.pause_until = float("-inf")
        self.episode = 0
        self.run_start: float | None = None
        self.run_refusals = 0
        self.last_refusal: str | None = None

    @property
    def limit(self) -> int | None:
        """The requests per minute the bucket is paced at, or None."""
        return BASIC_REQUESTS_PER_MINUTE if self.declared or self.learned else None

    def expire(self, now: float) -> None:
        """Free the slots whose window has passed.

        Args:
            now: The monotonic time.
        """
        while self.slots and self.slots[0] <= now:
            self.slots.popleft()

    def gave_up(self, now: float) -> bool:
        """Tell whether the current pause outlasts the run's patience.

        Args:
            now: The monotonic time.

        Returns:
            True while a pause is active that ends more than `RATE_LIMIT_PATIENCE` after the
            run's first refusal.
        """
        return (
            self.run_start is not None
            and now < self.pause_until
            and self.pause_until > self.run_start + RATE_LIMIT_PATIENCE
        )

    def start_at(self, now: float) -> float | None:
        """Return when the bucket's head request may start, or None to wait for a finish.

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

    def finish(self, now: float) -> None:
        """Release a request's flight; its slot stays held for a window.

        Args:
            now: The monotonic time its response arrived (or its attempt failed).
        """
        self.in_flight -= 1
        self.slots.append(now + RATE_WINDOW)
        if self.queue:
            self.queue[0].wake.set()


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
            # ... send the request ...
            governor.done(permit, answered=True)
    """

    BASIC_REQUESTS_PER_MINUTE: ClassVar[int] = BASIC_REQUESTS_PER_MINUTE
    """The requests per minute of a Basic subscription, the pace of a paced bucket."""

    __slots__ = ("_buckets", "_clock", "_listeners", "_lock")

    def __init__(self, buckets: Iterable[str], *, clock: Clock = SYSTEM_CLOCK) -> None:
        """Create the buckets, each unpaced and idle.

        Args:
            buckets: The bucket names, such as `("stocks", "options", ..., "other")`.
            clock: The time source of every wait and pause; the clients that use the
                governor wait on it too. Tests pass a fake.

        Raises:
            ValueError: If `buckets` is empty or names one twice.
        """
        names = tuple(buckets)
        if not names or len(set(names)) != len(names):
            raise ValueError(f"buckets must be distinct names, at least one: {names!r}")
        self._clock = clock
        self._lock = threading.Lock()
        self._buckets = {name: _Bucket() for name in names}
        self._listeners: tuple[Callable[[], None], ...] = ()

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

        Args:
            basic: The buckets declared Basic.

        Raises:
            ValueError: If `basic` names an unknown bucket.
        """
        unknown = set(basic) - self._buckets.keys()
        if unknown:
            raise ValueError(f"unknown buckets {sorted(unknown)}; known: {list(self._buckets)}")
        with self._lock:
            for name, bucket in self._buckets.items():
                bucket.declared = name in basic
                if bucket.queue:
                    bucket.queue[0].wake.set()
        self._notify()

    def acquire(self, bucket: str, cancel: Cancel) -> Permit:
        """Wait until a request of `bucket` may start, and return its permit.

        Blocks the calling thread, outside every lock, for as long as the bucket's queue,
        pace, and pause demand; waiting for a slot never times out. Report the attempt's end
        with `done` or `refused`.

        Args:
            bucket: The bucket the request is charged to.
            cancel: Ends the wait when set: the request gives up its place.

        Returns:
            The permit.

        The wait raises `WireError` when `cancel` is set (no status; the message says the wait
        was cancelled), and with status 429 while the bucket's pause outlasts
        `RATE_LIMIT_PATIENCE` from its run's first refusal (the message names the bucket and
        quotes the latest refusal). An unknown `bucket` raises `ValueError`.
        """
        state = self._bucket(bucket)
        waiter = _Waiter()
        stop_listening = cancel.on_set(waiter.wake.set)
        with self._lock:
            state.queue.append(waiter)
        self._notify()
        try:
            while True:
                permit, delay = self._step(bucket, waiter, cancel)
                if permit is not None:
                    break
                self._clock.wait(waiter.wake, delay)
        finally:
            stop_listening()
            with self._lock:
                if waiter in state.queue:
                    head = state.queue[0] is waiter
                    state.queue.remove(waiter)
                    if head and state.queue:
                        state.queue[0].wake.set()
        self._notify()
        return permit

    def _step(self, bucket: str, waiter: _Waiter, cancel: Cancel) -> tuple[Permit | None, float]:
        """Admit a waiting request if it may start now, or say how long it should sleep.

        Args:
            bucket: The bucket's name, a known one.
            waiter: The waiting request, in the bucket's queue.
            cancel: The request's cancellation.

        Returns:
            The permit and 0.0 when the request starts now (it leaves the queue, and the next
            request in it is woken to take its turn); otherwise None and the seconds to sleep
            unless woken sooner.

        Raises:
            WireError: When `cancel` is set (no status), or with status 429 while the bucket's
                pause outlasts `RATE_LIMIT_PATIENCE`.
        """
        state = self._buckets[bucket]
        with self._lock:
            waiter.wake.clear()  # before reading the state, so no change is missed
            if cancel.is_set():
                raise WireError(f"{bucket}: the wait for a request slot was cancelled")
            now = self._clock.monotonic()
            if state.gave_up(now):
                raise self._give_up(bucket, state)
            if state.queue[0] is not waiter:
                return None, _IDLE_WAIT
            state.expire(now)
            start = state.start_at(now)
            if start is None:
                return None, _IDLE_WAIT
            if start > now:
                return None, start - now
            state.queue.popleft()
            state.in_flight += 1
            state.requests += 1
            if state.queue:
                state.queue[0].wake.set()
            return Permit(bucket, state.episode), 0.0

    def done(self, permit: Permit, *, answered: bool) -> None:
        """Report an attempt that ended without a 429; its slot stays held for a window.

        Args:
            permit: The attempt's permit.
            answered: True when Massive answered (any status but 429), which ends the
                bucket's run of refusals if the attempt started after the latest pause; False
                when no response came (a connection failure), which changes nothing else.
        """
        state = self._bucket(permit.bucket)
        with self._lock:
            state.finish(self._clock.monotonic())
            if answered and permit.episode == state.episode:
                state.run_start = None
                state.run_refusals = 0
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
        started after the latest pause counts toward the run and sets a new pause (see the
        module docstring); one to an attempt that started before it only lengthens the pause
        to its own `Retry-After`. The caller then asks for a new permit, which waits out the
        pause.

        Args:
            permit: The attempt's permit.
            retry_after: The wait the response's `Retry-After` asked for, in seconds, or None
                when it gave none.
            message: The refusal's text, quoted in diagnostics and in the error of a give-up.
            request_id: The response's request id, or None.

        Returns:
            Whether the message showed the bucket is Basic.

        When resuming would take more than `RATE_LIMIT_PATIENCE` from the run's first refusal,
        this raises `WireError` with status 429 and `request_id` instead; the pause is set
        first, so the bucket's other requests stop too.
        """
        state = self._bucket(permit.bucket)
        basic = is_basic_refusal(message)
        give_up = None
        with self._lock:
            now = self._clock.monotonic()
            state.finish(now)
            state.refusals += 1
            state.last_refusal = message
            if basic:
                state.learned = True
            if permit.episode == state.episode:
                state.episode += 1
                state.run_refusals += 1
                if state.run_start is None:
                    state.run_start = now
                if retry_after is not None or state.limit is None:
                    pause = rate_limit_wait(state.run_refusals, retry_after)
                else:
                    pause = RATE_WINDOW
                state.pause_until = max(state.pause_until, now + pause)
            elif retry_after is not None:
                state.pause_until = max(state.pause_until, now + retry_after)
            if state.gave_up(now):
                give_up = self._give_up(permit.bucket, state, request_id)
        self._notify()
        if give_up is not None:
            raise give_up
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
                        waiting=len(bucket.queue),
                        recent=bucket.in_flight + len(bucket.slots),
                        requests=bucket.requests,
                        refusals=bucket.refusals,
                        paused_until=wall + bucket.pause_until - now if paused else None,
                        gave_up=bucket.gave_up(now),
                        last_refusal=bucket.last_refusal,
                    )
                )
        return tuple(states)

    def estimate(self, bucket: str, requests: int) -> float | None:
        """Return a lower bound on how long `requests` new requests of a bucket take to start.

        The bound assumes every response arrives at once and no other request joins the
        queue: the slots already held free at their times (a request in flight a full window
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
        """Tell whether the governor holds nothing worth keeping.

        Returns:
            True when no bucket has a request in flight or waiting, a slot held, or a pause
            active: a new governor would behave the same.
        """
        with self._lock:
            now = self._clock.monotonic()
            for bucket in self._buckets.values():
                bucket.expire(now)
                if bucket.in_flight or bucket.queue or bucket.slots or bucket.pause_until > now:
                    return False
            return True

    def subscribe(self, listener: Callable[[], None]) -> None:
        """Call `listener` after every change of any bucket.

        Args:
            listener: Called without arguments and without the governor's lock, on the thread
                that made the change (a request's thread, any session's). It must only note
                the change (enqueue, set an event) and return: never block, raise, or call
                back into the governor.
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

    @staticmethod
    def _give_up(bucket: str, state: _Bucket, request_id: str | None = None) -> WireError:
        """Return the error of a request that will not wait out its bucket's pause.

        Called with the lock held, while `state.gave_up` is true.

        Args:
            bucket: The bucket's name.
            state: Its state.
            request_id: The refusal's request id, when a refusal gave up; None for a waiting
                request.

        Returns:
            A `WireError` with status 429.
        """
        run_start = state.run_start if state.run_start is not None else state.pause_until
        total = state.pause_until - run_start
        return WireError(
            f"Massive refused {bucket} requests with HTTP 429, and resuming would take until "
            f"{total:.0f} s after the first refusal, more than the {RATE_LIMIT_PATIENCE:.0f} s "
            f"the library waits (latest refusal: {state.last_refusal})",
            status=429,
            request_id=request_id,
        )

    def _notify(self) -> None:
        """Call every listener, outside the lock."""
        with self._lock:
            listeners = self._listeners
        for listener in listeners:
            listener()
