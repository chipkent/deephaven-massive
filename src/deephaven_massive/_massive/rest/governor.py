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
- **Unpaced** otherwise: requests start at once, except during a pause. A bucket named in
  `unpaced` at construction (the library's bucket for data of no subscription) is never paced:
  it cannot be declared, and a refusal naming the per-minute maximum only pauses it.

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
ever grows. When it ends, the bucket sends one *probe*: the first request to start, alone,
while every other request of the bucket waits until Massive answers it. An answer other than a
429 releases them; a 429 pauses again. So a bucket whose refusals do not say it is Basic still
sends one refused request per pause, never a burst.

A bucket is learned Basic only from the words of a refusal (`is_basic_refusal`), never from
how many refusals it gets: a subscription refused twice in one run is not taken for Basic.
Inferring the tier from a count of refusals would tie the library to details of Massive's plans
that it cannot check; the probe after each pause is what bounds the cost when the words are
missing.

A run of refusals starts with a counted refusal, or with a refusal of an earlier burst that
lengthens the pause; it ends with the first answer other than a 429 to a request that started
after the latest pause, or once more than `RATE_WINDOW` has passed since its pause ended with
no refusal (the run was abandoned: its read was cancelled or its session closed). A burst of
429s to requests already in flight when the pause began is one refusal, and their late
successes do not end the run. Once the run's pauses add up to more than `RATE_LIMIT_PATIENCE`
(10 minutes), the bucket *gives up*: the refused request, every waiting request, and every new
request of the bucket fail with status 429 until that pause has passed, and the first refusal
after it starts a new run. Waiting for a slot never gives up by itself: a long history at 5
requests per minute takes as long as it takes, and a `Cancel` token ends the wait.

A request that may start at once, with nothing queued before it, is granted without queuing:
no waiter, no event, and no change notice (its answer gives one). Every other request waits
on events: a wait without a deadline (behind another request, or for a slot held in flight)
is woken by the change that can let it start, never by a timer. A wait behind a probe ends
at the probe's answer, or after `PROBE_TIMEOUT` if its report was lost, when the next
request probes instead. `admission` opens a claim before anything is granted and reports its
permit on exit, so a caller that takes permits through it never leaves one unreported.

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
import math
import threading
from collections import deque
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, Self, cast

from deephaven_massive._massive.net.errors import WireError
from deephaven_massive._massive.net.retry import (
    RATE_LIMIT_PATIENCE,
    SYSTEM_CLOCK,
    rate_limit_wait,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable, Iterable, Mapping
    from types import TracebackType

    from deephaven_massive._massive.net.cancel import Cancel
    from deephaven_massive._massive.net.retry import Clock

__all__ = [
    "BASIC_REQUESTS_PER_MINUTE",
    "PROBE_TIMEOUT",
    "RATE_WINDOW",
    "Admission",
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

PROBE_TIMEOUT: Final = 300.0
"""Seconds a pause's probe may stay unanswered before the bucket stops waiting for it.

Far longer than a page takes (a connection times out after 10 s, and each read after 30 s), so
it ends only a probe whose report was lost, which would otherwise hold the bucket forever."""

_UNTIL_WOKEN: Final = math.inf
"""The wait of a request with no deadline: only a change of its bucket wakes it."""


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


class Admission:
    """One attempt's claim on its bucket, open for a `with` block (`RequestGovernor.admission`).

    The block's exit reports the permit, if one was granted and not yet reported, as an
    attempt that ended without an answer (`RequestGovernor.done(..., answered=False)`). The
    permit is recorded here under the governor's lock in the same step as its grant, and the
    `with` block is entered before anything is granted, so no interrupt, at any instruction,
    can leave a granted permit without a report: one in a `with` block of a lock aside, which
    no Python code can make safe. A caller that reports the attempt itself (`done`,
    `refused`, `release`) inside the block makes the exit's report do nothing.

    Not thread-safe: one attempt, on one thread.

    Attributes:
        bucket: The bucket the attempt is charged to.
        lane: The lane it waits in.
        permit: The granted permit, or None before the grant.

    Example:
        ::

            with governor.admission("stocks", lane=read_token) as admission:
                permit = admission.acquire(read_token)
                ...  # send the request; report it with done, refused, or release
    """

    __slots__ = ("_governor", "bucket", "lane", "permit")

    def __init__(self, governor: RequestGovernor, bucket: str, lane: Hashable) -> None:
        """Hold nothing yet.

        Args:
            governor: The governor.
            bucket: The bucket the attempt is charged to.
            lane: The lane it waits in (see `RequestGovernor.acquire`).
        """
        self._governor = governor
        self.bucket = bucket
        self.lane = lane
        # The granted permit, set under the governor's lock as it is granted; None before.
        self.permit: Permit | None = None

    def __enter__(self) -> Self:
        """Open the claim; nothing is granted yet.

        Returns:
            The admission.
        """
        return self

    def acquire(self, cancel: Cancel) -> Permit:
        """Wait until the attempt may start, as `RequestGovernor.acquire` describes.

        Args:
            cancel: Ends the wait when set.

        Returns:
            The permit, also kept in `permit`.

        Raises:
            WireError: As `RequestGovernor.acquire` raises it.
        """
        permit, failure = self._governor.admit(self, cancel)
        if failure is not None:
            raise WireError(failure.message, status=failure.status)
        return cast("Permit", permit)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Report a granted permit not yet reported, as an attempt that got no answer.

        Args:
            exc_type: The exception type, if the block raised.
            exc: The exception, if the block raised.
            traceback: Its traceback.
        """
        if self.permit is not None:
            self._governor.done(self.permit, answered=False)


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
        attempts: Attempts started since the governor was created: every retry and refused
            attempt counts, so one request may count several times.
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
    attempts: int
    refusals: int
    paused_until: float | None
    gave_up: bool
    last_refusal: str | None


class _Waiter:
    """One waiting request: its lane, and the event that wakes it."""

    __slots__ = ("lane", "wake")

    def __init__(self, lane: Hashable) -> None:
        """Create the waiter.

        Args:
            lane: The lane it waits in.
        """
        self.lane = lane
        self.wake = threading.Event()


class _Bucket:
    """The mutable state of one bucket; every field is guarded by the governor's lock."""

    __slots__ = (
        "attempts",
        "declared",
        "episode",
        "given_up",
        "label",
        "lanes",
        "last_refusal",
        "learned",
        "open",
        "pace",
        "pause_until",
        "probe",
        "probe_since",
        "probing",
        "refusals",
        "run_paused",
        "run_refusals",
        "run_start",
        "slots",
        "turns",
    )

    def __init__(self, label: str, *, pace: bool) -> None:
        """Start unpaced, idle, and with no refusal.

        Args:
            label: How messages name the bucket's requests, such as `"stocks requests"`.
            pace: Whether the bucket may be paced at all: False for one that is never paced,
                however it is declared or refused.
        """
        self.label = label
        self.pace = pace
        self.declared = False
        self.learned = False
        # The waiting requests of each lane, first in first out, and the lanes' turns.
        self.lanes: dict[Hashable, deque[_Waiter]] = {}
        self.turns: deque[Hashable] = deque()
        # The permits granted and not yet reported: the requests in flight. Adding one is the
        # grant itself, a single step, so no interruption can leave a count out of step.
        self.open: set[Permit] = set()
        # When each finished request's slot frees (monotonic seconds), oldest first.
        self.slots: deque[float] = deque()
        self.attempts = 0
        self.refusals = 0
        self.pause_until = float("-inf")
        self.episode = 0
        self.run_start: float | None = None
        self.run_refusals = 0
        # Seconds the run's pauses have covered, the measure of the library's patience.
        self.run_paused = 0.0
        self.given_up = False
        self.last_refusal: str | None = None
        # Set by a pause: the next request to start is its probe, alone (`probe`) until
        # Massive answers it with anything but a 429.
        self.probing = False
        self.probe: Permit | None = None
        self.probe_since = 0.0  # when the probe was granted (monotonic)

    @property
    def limit(self) -> int | None:
        """The requests per minute the bucket is paced at, or None."""
        paced = self.pace and (self.declared or self.learned)
        return BASIC_REQUESTS_PER_MINUTE if paced else None

    @property
    def in_flight(self) -> int:
        """How many granted requests have not been reported."""
        return len(self.open)

    @property
    def waiting(self) -> int:
        """How many requests wait, in every lane."""
        return sum(len(lane) for lane in self.lanes.values())

    def head(self) -> _Waiter | None:
        """Return the request whose turn it is, or None when none waits.

        A turn whose lane is empty or gone is dropped on the way: `start_head` leaves an
        emptied lane's turn for this to drop, and an interruption between two steps of a lane
        change can leave one too.
        """
        while self.turns:
            lane = self.lanes.get(self.turns[0])
            if lane:
                return lane[0]
            self.lanes.pop(self.turns.popleft(), None)
        return None

    def join(self, waiter: _Waiter) -> None:
        """Queue a request at the back of its lane; a lane without a turn takes the last one.

        Args:
            waiter: The request.
        """
        if waiter.lane not in self.turns:
            self.turns.append(waiter.lane)
        self.lanes.setdefault(waiter.lane, deque()).append(waiter)

    def leave(self, waiter: _Waiter) -> bool:
        """Take a request out of its lane (it gave up), and wake the head if that changed.

        An emptied lane loses its turn when `head` next reaches it.

        Args:
            waiter: The request; one no longer queued (it started) is ignored.

        Returns:
            True when the request was still queued and has left.
        """
        lane = self.lanes.get(waiter.lane)
        if lane is None or waiter not in lane:
            return False
        was_head = self.head() is waiter
        lane.remove(waiter)
        if was_head:
            self.wake_head()
        return True

    def start_head(self) -> None:
        """Remove the head, which starts now, and wake the next head.

        The head's lane takes the last turn; once empty, it loses that turn when `head` next
        reaches it. Each step leaves a state the others accept: the waiter leaves its lane
        first, then the turn moves in one step.
        """
        self.lanes[self.turns[0]].popleft()
        self.turns.rotate(-1)
        self.wake_head()

    def wake_head(self) -> None:
        """Wake the head, if any, to work out its start again."""
        head = self.head()
        if head is not None:
            head.wake.set()

    def expire(self, now: float) -> None:
        """Free the slots whose window has passed, and stop waiting for an overdue probe.

        A probe unanswered for `PROBE_TIMEOUT` lost its report; the next request probes in
        its place, so no lost report can hold the bucket forever.

        Args:
            now: The monotonic time.
        """
        while self.slots and self.slots[0] <= now:
            self.slots.popleft()
        if self.probe is not None and now >= self.probe_since + PROBE_TIMEOUT:
            self.probe = None

    def gave_up(self, now: float) -> bool:
        """Tell whether the bucket's requests fail at once now.

        Args:
            now: The monotonic time.

        Returns:
            True while the bucket has given up and its pause has not ended.
        """
        return self.given_up and now < self.pause_until

    def end_run(self) -> None:
        """Forget the run of refusals: an answer came, or the run is over by time."""
        self.run_start = None
        self.run_refusals = 0
        self.run_paused = 0.0
        self.given_up = False

    def settle(self, now: float) -> None:
        """End a run that time has ended, before anything reads or extends it.

        A given-up pause that has passed ends its run, so the next refusal starts a new one.
        A run whose pause ended more than `RATE_WINDOW` ago, with no refusal since and no
        probe still waiting for its answer, was abandoned (its read was cancelled, or its
        session closed), so a refusal hours later is the first of a new run, not the last
        straw of an old one; a slow probe's own refusal still belongs to its run.

        Args:
            now: The monotonic time.
        """
        if self.run_start is None or now < self.pause_until:
            return
        idle = self.probe is None and now - self.pause_until > RATE_WINDOW
        if self.given_up or idle:
            self.end_run()

    def pause(self, now: float, until: float) -> None:
        """Extend the pause to `until`, count the time it adds to the run, and probe after it.

        The run gives up once its pauses add up to more than `RATE_LIMIT_PATIENCE`: the
        time spent paused, not the time since the first refusal, which an idle gap would
        stretch.

        Args:
            now: The monotonic time.
            until: When the new pause ends; a pause only ever grows.
        """
        if self.run_start is None:
            self.run_start = now
        if until > self.pause_until:
            self.run_paused += until - max(now, self.pause_until)
            self.pause_until = until
        self.probing = True
        if self.run_paused > RATE_LIMIT_PATIENCE:
            self.given_up = True

    def start_at(self, now: float) -> float:
        """Return when the bucket's head may start.

        Args:
            now: The monotonic time; the expired slots are already freed.

        Returns:
            The monotonic time it may start (`now` or earlier means at once): behind a probe
            waiting for its answer, when the probe would be overdue (its answer wakes it
            sooner); or `math.inf` when every slot is held by a request in flight, so only a
            change of the bucket can let it start.
        """
        start = max(now, self.pause_until)
        if self.probe is not None:
            return self.probe_since + PROBE_TIMEOUT
        limit = self.limit
        if limit is None:
            return start
        excess = self.in_flight + len(self.slots) - limit
        if excess < 0:
            return start
        if excess >= len(self.slots):
            return _UNTIL_WOKEN
        return max(start, self.slots[excess])

    def grant(self, name: str, admission: Admission, now: float) -> Permit:
        """Grant a request that may start now, and record it; the caller holds the lock.

        Args:
            name: The bucket's name.
            admission: The request's claim, which records the permit in the same step.
            now: The monotonic time.

        Returns:
            The permit, in flight; the pause's probe when the bucket is probing.
        """
        permit = admission.permit = Permit(name, self.episode)
        self.open.add(permit)  # the grant
        self.attempts += 1
        if self.probing:
            self.probe = permit
            self.probe_since = now
        return permit

    def finish(self, permit: Permit, now: float, *, hold: bool) -> bool:
        """End a request's flight, if it has not ended already.

        Args:
            permit: The request's permit.
            now: The monotonic time its response arrived (or its attempt failed).
            hold: True to keep its slot for a window (it may have reached Massive), False to
                free it at once (it was never sent).

        Returns:
            True for the permit's first report, False (and no change) for any later one.
        """
        if permit not in self.open:
            return False
        self.open.remove(permit)
        if permit is self.probe:  # the next request probes, unless its answer ended the run
            self.probe = None
        if hold:
            self.slots.append(now + RATE_WINDOW)
        self.wake_head()
        return True


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
            # Blocks while no slot is free; the lane keys the requests that take turns together.
            permit = governor.acquire("stocks", closed, lane=object())
            try:
                ...  # send the request
            finally:
                governor.done(permit, answered=True)
    """

    __slots__ = ("_buckets", "_clock", "_listeners", "_lock")

    def __init__(
        self,
        buckets: Iterable[str],
        *,
        unpaced: Iterable[str] = (),
        clock: Clock = SYSTEM_CLOCK,
        labels: Mapping[str, str] = MappingProxyType({}),
    ) -> None:
        """Create the buckets, each unpaced and idle.

        Args:
            buckets: The bucket names, such as `("stocks", "options", ..., "other")`.
            unpaced: The buckets that are never paced, such as `("other",)` for data of no
                subscription: they pause after a 429 like any bucket, but a refusal naming
                the per-minute maximum does not pace them, and declaring one raises.
            clock: The time source of every wait and pause; the clients that use the
                governor wait on it too. Tests pass a fake.
            labels: How messages name each bucket's requests, such as `{"other": "requests
                outside Massive's subscriptions"}`; a bucket left out is `"<name> requests"`.

        Raises:
            ValueError: If `buckets` is empty or names one twice, or `unpaced` names a bucket
                not in `buckets`.
        """
        names = tuple(buckets)
        if not names or len(set(names)) != len(names):
            raise ValueError(f"buckets must be distinct names, at least one: {names!r}")
        never = frozenset(unpaced)
        if not never <= set(names):
            raise ValueError(f"unpaced buckets {sorted(never - set(names))} are not buckets")
        self._clock = clock
        self._lock = threading.Lock()
        self._buckets = {
            name: _Bucket(labels.get(name, f"{name} requests"), pace=name not in never)
            for name in names
        }
        self._listeners: tuple[Callable[[], None], ...] = ()

    @property
    def clock(self) -> Clock:
        """The governor's time source, which its clients also wait on."""
        return self._clock

    @property
    def buckets(self) -> tuple[str, ...]:
        """The bucket names, in construction order."""
        return tuple(self._buckets)

    def declare(self, basic: Iterable[str]) -> None:
        """Set which buckets are declared Basic, and so paced from their first request.

        A bucket left out is no longer declared; it stays paced only if it was learned Basic.

        Args:
            basic: The buckets declared Basic.

        Raises:
            ValueError: If a name is not a bucket, or is a bucket that is never paced.
        """
        basic = frozenset(basic)
        unknown = basic - self._buckets.keys()
        if unknown:
            raise ValueError(f"unknown buckets {sorted(unknown)}; known: {list(self._buckets)}")
        never = sorted(name for name in basic if not self._buckets[name].pace)
        if never:
            raise ValueError(f"buckets {never} are never paced, so they cannot be declared Basic")
        with self._lock:
            for name, bucket in self._buckets.items():
                bucket.declared = name in basic
                bucket.wake_head()
        self._notify()

    def learn(self, basic: Iterable[str]) -> None:
        """Mark buckets learned Basic, as a refusal naming the per-minute maximum would.

        The library's registry calls it to carry what an earlier governor of the same key
        learned into a new one. Learned stays learned for the governor's life; a bucket that
        is never paced is left as it is.

        Args:
            basic: The buckets known to be Basic.

        Raises:
            ValueError: If a name is not a bucket.
        """
        basic = frozenset(basic)
        unknown = basic - self._buckets.keys()
        if unknown:
            raise ValueError(f"unknown buckets {sorted(unknown)}; known: {list(self._buckets)}")
        with self._lock:
            for name in basic:
                bucket = self._buckets[name]
                bucket.learned = bucket.pace
                bucket.wake_head()
        self._notify()

    def admission(self, bucket: str, *, lane: Hashable) -> Admission:
        """Open one attempt's claim on a bucket, for a `with` block; nothing is granted yet.

        The safe way to take a permit: `Admission.acquire` inside the block takes it, and the
        block's exit reports it if nothing else did, so a permit is never left unreported (see
        `Admission`). The REST client takes every permit this way.

        Args:
            bucket: The bucket the attempt is charged to.
            lane: The lane it waits in (see `acquire`).

        Returns:
            The claim.

        Raises:
            ValueError: If `bucket` is not a bucket.
        """
        if bucket not in self._buckets:
            raise ValueError(f"unknown bucket {bucket!r}; known: {list(self._buckets)}")
        return Admission(self, bucket, lane)

    def acquire(self, bucket: str, cancel: Cancel, *, lane: Hashable) -> Permit:
        """Wait until a request of `bucket` may start, and return its permit.

        Blocks the calling thread, outside every lock, for as long as the bucket's lanes,
        pace, pause, and probe demand; waiting for a slot never times out. A request that may
        start at once, with nothing queued before it, is granted without waiting. Report the
        attempt's end with `done`, `refused`, or `release`. A cancellation, a failure, or an
        interrupt anywhere after the grant inside this method (`KeyboardInterrupt` while the
        thread sleeps, in a listener, or while it stops listening to `cancel`) releases the
        permit before the exception propagates. An interrupt between this method's return and
        the caller's `try` can still leave the permit unreported; `admission` closes that
        window. A probe whose report is lost stops holding its bucket after `PROBE_TIMEOUT`.

        Args:
            bucket: The bucket the request is charged to.
            cancel: Ends the wait when set: the request gives up its place.
            lane: The lane the request waits in, such as its read's cancellation token: the
                requests of one lane start in order, and the lanes of a bucket take turns.
                A request alone in its lane passes a key of its own, such as a fresh
                `object()`.

        Returns:
            The permit.

        Raises:
            WireError: Without a status when `cancel` is set (the message says the wait was
                cancelled), and with status 429 while the bucket has given up on a run of
                refusals (the message says how long Massive has refused the bucket's
                requests and how long they fail at once, and quotes the latest refusal).
        """
        admission = self.admission(bucket, lane=lane)
        permit, failure = self.admit(admission, cancel)
        if failure is not None:
            raise WireError(failure.message, status=failure.status)
        return cast("Permit", permit)

    def admit(self, admission: Admission, cancel: Cancel) -> tuple[Permit | None, WireError | None]:
        """Grant an attempt at once or after waiting, recording the permit on its claim.

        `Admission.acquire` and `acquire` call it; it raises nothing of its own, so each
        caller turns a failure into its `WireError`.

        Args:
            admission: The attempt's claim, for a known bucket.
            cancel: Ends the wait when set.

        Returns:
            `(permit, None)` once granted, or `(None, error)` when the attempt must fail, as
            `acquire` describes.
        """
        bucket, lane = admission.bucket, admission.lane
        state = self._buckets[bucket]
        permit, failure = self._grant_at_once(admission, cancel)
        if failure is not None or permit is not None:
            return permit, failure
        waiter = _Waiter(lane)
        stop_listening = cancel.on_set(waiter.wake.set)
        try:
            try:
                with self._lock:
                    state.join(waiter)
                self._notify()
                permit, failure = self._wait(admission, waiter, cancel)
                if failure is None:
                    self._notify()
            finally:
                stop_listening()
                with self._lock:
                    left = state.leave(waiter)
                if left:  # it gave up its place: the bucket's waiting count changed
                    self._notify()
        except BaseException:
            if admission.permit is not None:  # interrupted after the grant: hand it back
                self.release(admission.permit)
            raise
        return permit, failure

    def _grant_at_once(
        self, admission: Admission, cancel: Cancel
    ) -> tuple[Permit | None, WireError | None]:
        """Grant a request that may start now with nothing queued before it, without waiting.

        Args:
            admission: The request's claim, for a known bucket.
            cancel: The request's cancellation.

        Returns:
            `(permit, None)` for a request granted now; `(None, error)` when it must fail at
            once (cancelled, or the bucket has given up); `(None, None)` when it must queue.
        """
        bucket = admission.bucket
        state = self._buckets[bucket]
        with self._lock:
            now = self._clock.monotonic()
            failure = self._failure(bucket, cancel, now=now)
            if failure is not None:
                return None, failure
            if state.head() is not None:
                return None, None
            state.expire(now)
            if state.start_at(now) > now:
                return None, None
            return state.grant(bucket, admission, now), None

    def _wait(
        self, admission: Admission, waiter: _Waiter, cancel: Cancel
    ) -> tuple[Permit | None, WireError | None]:
        """Sleep until a queued request is admitted or must fail.

        Args:
            admission: The request's claim, for a known bucket.
            waiter: The queued request.
            cancel: The request's cancellation.

        Returns:
            `(permit, None)` once it is admitted, or `(None, error)` when it must fail, as
            `acquire` describes.
        """
        while True:
            permit, delay, failure = self._step(admission, waiter, cancel)
            if failure is not None or permit is not None:
                return permit, failure
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
            if not state.finish(permit, self._clock.monotonic(), hold=True):
                return
            if answered and permit.episode == state.episode:
                state.end_run()
                state.probing = False  # Massive answered after the pause: no more probe
        self._notify()

    def release(self, permit: Permit) -> None:
        """Report an attempt that was never sent: its slot frees at once.

        Does nothing for a permit already reported.

        Args:
            permit: The attempt's permit.
        """
        state = self._bucket(permit.bucket)
        with self._lock:
            if not state.finish(permit, self._clock.monotonic(), hold=False):
                return
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
        bucket learned Basic, so it is paced from now on, unless the bucket is never paced. A
        refusal to an attempt that started after the latest pause counts toward the run and
        sets a new pause; one to an attempt that started before it only lengthens the pause
        to its own `Retry-After` (and starts a run if none is going). A `Retry-After` of zero
        counts as none. A run that time has ended (see the module) ends first, so this
        refusal starts a new one. The caller then asks for a new permit, which waits out the
        pause. Does nothing for a permit already reported.

        Args:
            permit: The attempt's permit.
            retry_after: The wait the response's `Retry-After` asked for, in seconds, or None
                when it gave none.
            message: The refusal's text, quoted in diagnostics and in the error of a give-up.
            request_id: The response's request id, or None.

        Returns:
            Whether the bucket is paced as Basic because of this message: False for a bucket
            that is never paced, whatever the message says.

        Raises:
            WireError: With status 429 and `request_id` when the bucket gives up (the run's
                pauses add up to more than `RATE_LIMIT_PATIENCE`); the pause is set first, so
                the bucket's other requests stop too.
        """
        state = self._bucket(permit.bucket)
        basic = is_basic_refusal(message) and state.pace
        asked = retry_after if retry_after is not None and retry_after > 0 else None
        give_up = None
        with self._lock:
            now = self._clock.monotonic()
            state.settle(now)  # before `finish`, so a slow probe still holds its run open
            if not state.finish(permit, now, hold=True):
                return basic
            state.refusals += 1
            state.last_refusal = message
            if basic:
                state.learned = True
            if permit.episode == state.episode:
                state.episode += 1
                state.run_refusals += 1
                if asked is not None or state.limit is None:
                    pause = rate_limit_wait(state.run_refusals, asked)
                else:
                    pause = RATE_WINDOW
                state.pause(now, now + pause)
            elif asked is not None and now + asked > state.pause_until:
                state.pause(now, now + asked)
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
                        attempts=bucket.attempts,
                        refusals=bucket.refusals,
                        paused_until=wall + bucket.pause_until - now if paused else None,
                        gave_up=bucket.gave_up(now),
                        last_refusal=bucket.last_refusal,
                    )
                )
        return tuple(states)

    def next_change(self) -> float | None:
        """Return the seconds until a bucket's state changes with time alone.

        A pause ends, or a held slot of a paced bucket frees, without any request or
        response; diagnostics that show the state read it again then. An unpaced bucket's
        slots are left out: they change only its count of recent requests, which the next
        request or response refreshes, so a paid plan's diagnostics do not churn with time.

        Returns:
            Seconds from now until the earliest pause end or paced slot release, or None when
            no bucket has either.
        """
        with self._lock:
            now = self._clock.monotonic()
            moments = []
            for bucket in self._buckets.values():
                bucket.expire(now)
                if bucket.pause_until > now:
                    moments.append(bucket.pause_until)
                if bucket.slots and bucket.limit is not None:
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
            once), or None when the bucket is unpaced or `requests` is less than 1.

        Raises:
            ValueError: If `bucket` is not a bucket.
        """
        state = self._buckets.get(bucket)
        if state is None:
            raise ValueError(f"unknown bucket {bucket!r}; known: {list(self._buckets)}")
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
                if bucket.open or bucket.waiting or bucket.slots or bucket.pause_until > now:
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
        self, admission: Admission, waiter: _Waiter, cancel: Cancel
    ) -> tuple[Permit | None, float, WireError | None]:
        """Admit a waiting request if it may start now, or say how long it should sleep.

        Args:
            admission: The request's claim, for a known bucket.
            waiter: The waiting request, queued in the bucket.
            cancel: The request's cancellation.

        Returns:
            `(permit, 0.0, None)` when the request starts now (it leaves its lane, and the
            next head is woken); `(None, seconds, None)` to sleep that long unless woken
            sooner (`math.inf`: until woken); `(None, 0.0, error)` when the wait must fail:
            cancelled (no status) or given up (429).
        """
        bucket = admission.bucket
        state = self._buckets[bucket]
        with self._lock:
            waiter.wake.clear()  # before reading the state, so no change is missed
            now = self._clock.monotonic()
            failure = self._failure(bucket, cancel, now=now)
            if failure is not None:
                return None, 0.0, failure
            if state.head() is not waiter:
                return None, _UNTIL_WOKEN, None
            state.expire(now)
            start = state.start_at(now)
            if start > now:
                return None, start - now, None
            # Recorded on the claim in the same step as the grant, so an interrupted
            # `acquire` always knows what to hand back.
            permit = state.grant(bucket, admission, now)
            state.start_head()
            return permit, 0.0, None

    def _failure(self, bucket: str, cancel: Cancel, *, now: float) -> WireError | None:
        """Return the error a request of the bucket must fail with now, if any; lock held.

        Ends a run that time has ended first (`settle`).

        Args:
            bucket: The bucket's name, a known one.
            cancel: The request's cancellation.
            now: The monotonic time.

        Returns:
            A `WireError` without a status when `cancel` is set, one with status 429 while the
            bucket has given up, or None.
        """
        if cancel.is_set():
            return WireError(f"{bucket}: the wait for a request slot was cancelled")
        state = self._buckets[bucket]
        state.settle(now)
        if state.gave_up(now):
            return WireError(self._give_up_text(state, now), status=429)
        return None

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
            f"Massive has refused {state.label} with HTTP 429 for {now - start:.0f} s, and "
            f"its pauses come to {state.run_paused:.0f} s, past the "
            f"{RATE_LIMIT_PATIENCE:.0f} s the library waits out; they fail at once for "
            f"{state.pause_until - now:.0f} s more, until the pause ends (latest refusal: "
            f"{state.last_refusal})"
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
