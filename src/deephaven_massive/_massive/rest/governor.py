"""`RequestGovernor`: when each REST request may start, per Massive subscription.

Massive limits REST requests per subscription (stocks, options, indices, currencies, futures),
not per account or per key, and sets each limit by the account's plans, which change over
time; the governor holds no limit of its own. All the API keys of an account share these
counters, and a request that exceeds one is refused with HTTP 429 and the message "You've
exceeded the maximum requests per minute". Massive offers no way to ask a subscription's limit,
and its 429 does not say it. Massive's knowledge base states these rules, and that reference
and cross-asset endpoints are counted in a bucket of their own, in the article "What is the
request limit for Massive's RESTful APIs?". The refresh vendors it, and its drift check reports
any change to it:
`vendor/massive/knowledge-base/what-is-the-request-limit-for-massives-restful-apis.md`.

The governor holds one *bucket* per subscription (and one for the datasets Massive assigns to
none), each named by an opaque string fixed at construction. Every attempt of a REST request
first takes a *permit* of its bucket (`admission`, then `Admission.acquire`) and reports how
it ended, exactly once:
`done` (answered, or failed on the wire), `refused` (HTTP 429), or `release` (never sent). A
bucket is:

- **Paced** at the limit the user declares (`declare`), or else at the limit it *learned* from
  Massive's refusals (see **Learning**). A paced bucket lets a request start only while fewer
  than its limit of *slots* are held. A slot is held by a request in flight, and by
  a finished one until `RATE_WINDOW` (61 s: Massive's minute plus one second of margin for
  latency and clock skew) after its response arrived, which bounds when Massive counted it.
  Massive's minute is the clock minute (a live check saw a spent allowance freed as the
  minute turned, not 60 s after the first request); no clock minute ever holds more starts
  than a 61 s window does, so the hold is safe for either, at worst a minute slower. Waiting
  for the next minute instead would trust the local clock to agree with Massive's to the
  second, and a fast clock would draw a 429 and a whole window's pause.
- **Unpaced** otherwise: requests start at once, except during a pause. A bucket named in
  `learn_only` at construction (the library's bucket for reference and other data of no
  subscription, which Massive counts in a bucket of its own) cannot be declared; it is paced
  only once it learns a limit.

**Learning.** A refusal whose message names the per-minute maximum (`is_rate_limit_refusal`)
says the bucket has a limit, but not what it is. The limit is the number of the bucket's
requests Massive answered (any status but 429; a wire failure is no answer) among those sent in
the clock minute that held the send of the run's earliest such refusal: Massive counts a request
when it arrives, nearest its send. A request's send is when the client sends it (`sending`),
after any wait for a connection, not when its permit was granted. When a refusal arrives, some
of that minute's answers may still be in flight (with eight workers, the 429 can come back
before the successes), so the bucket learns once the run has ended, when Massive answers a
request sent after the latest pause, and once no request sent in that minute is still in flight:
a slow download of the minute counts when it is answered. Counting by the clock minute, not by a
window, keeps a burst that straddles two minutes from counting both. The bucket is then paced at
that count. A later run re-learns only downward, when its minute holds fewer answers, so an
over-count corrects itself and the pace never rises by itself. A run whose minute holds no
answer (another client of the account spent it before this process sent anything) learns
nothing, nor does a run that ends without an answer after its pause (abandoned, see below).
Other clients of the account that share the minute make the count lower than the true limit,
never higher: slower, never faster. Since a paced bucket never sends more than its limit in a
minute, a learned limit never rises on its own, for the governor's life; the log line that
learns it says so, and that declaring the limit raises it. A declared limit wins over a learned
one: the user knows the limit.

**Lanes.** Each waiting request belongs to a *lane*: the requests of one read, one poller's
cycle, or one host decision share a lane (the caller names it, usually by its cancellation
token). A bucket keeps one first-in, first-out queue per lane and serves the lanes round-robin:
the request at the front of the next lane in turn is the bucket's *head*, and once it starts,
its lane goes to the back of the turn. So a host decision or a `poll()` cycle waits for one
request of each other lane, not behind every page of a long history with eight workers. Only
the head works out when it may start; it is woken when that may change (a request finished or
was released, the head before it started or gave up its place, a refusal, a new limit).

After a 429 the bucket pauses: for the `Retry-After` the response asked for, up to
`MAX_RETRY_AFTER` (10 minutes, so a broken server's header cannot stop a subscription's
requests for days, and every pause ends at a time a timestamp can hold); without one, or
with one of zero or in the past, a paced bucket for a whole window and an unpaced one for 15,
30, then 60 s by the number of refusals in the run (`net.retry.rate_limit_wait`). A pause only
ever grows. When it ends, the bucket sends one *probe*: the first request to start, alone,
while every other request of the bucket waits until Massive answers it. An answer other than a
429 releases them; a 429 pauses again. So a bucket without a limit still sends one refused
request per pause, never a burst. Without a `Retry-After`, a refusal that names the per-minute
maximum, or any refusal of a paced bucket, pauses a whole window: that minute's allowance is
spent.

A bucket learns only from refusals that name the per-minute maximum, never from how many
refusals it gets, which say nothing of the limit; the probe after each pause is what bounds
the cost when the words are missing.

A run of refusals starts with a counted refusal, or with a refusal of an earlier burst that
lengthens the pause; it ends with the first answer other than a 429 to a request that started
after the latest pause, or once more than `RATE_WINDOW` has passed since its pause ended with
no refusal and no probe in flight (the run was abandoned: its read was cancelled or its session
closed). A burst of 429s to requests already in flight when the pause began is one refusal,
and their late successes do not end the run. The run only sets the fallback pause (15, 30,
then 60 s): the bucket itself never gives up, and keeps pausing and probing for as long as
Massive refuses it.

**Giving up is each read's own.** Every request names its read's `Patience`: the most time the
read waits out pauses, shared by all its requests. Only pauses count: a read paced at a few
requests a minute for an hour spends none of its patience on its slot waits. A request that
would wait for a pause its read's patience cannot cover fails at once with status 429, saying
when the bucket's next request goes; its read stops, while the requests of other reads, other
sessions included, keep their own patience. A read that waits on a lookup another read is making
sends no request, so it charges the bucket's pauses to its own patience itself (`charge_pause`)
and gives up the same way. Waiting for a slot never gives up by itself: a long history at a few
requests per minute takes as long as it takes, and a `Cancel` token ends the wait.

A request that may start at once, with nothing queued before it, is granted without queuing:
no waiter, no event, and no change notice (its answer gives one). Every other request waits
on events: a wait without a deadline (behind another request, or for a slot held in flight)
is woken by the change that can let it start, never by a timer; a wait behind a probe ends at
the probe's answer. Every permit is taken through `admission`, a claim opened before anything
is granted that reports its permit on exit, so no interrupt leaves a permit unreported, short
of one inside a lock wait or a second one during the claim's exit, which no Python code can
make safe.

One governor serves every session of a process that uses the same API key and REST origin,
so their requests share the counters as Massive's do (the library's registry builds and shares
them). Thread-safe: one leaf lock guards every bucket, held only to read or change state, never
for a wait, a request, or a callback. Listeners (`subscribe`, `subscribe_bucket`) are called
after the lock is released and must only enqueue; one that raises is logged and does not disturb
the governor. Time comes from an injectable `Clock`, so tests never sleep.

This module imports only the standard library and `net`.
"""

from __future__ import annotations

import heapq
import logging
import math
import threading
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, Self

from deephaven_massive._massive.net.errors import LIBRARY_BUG_REPORT, WireError, WireFailure
from deephaven_massive._massive.net.retry import RATE_LIMIT_WAITS, SYSTEM_CLOCK, rate_limit_wait

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable, Iterable, Mapping
    from types import TracebackType

    from deephaven_massive._massive.net.cancel import Cancel
    from deephaven_massive._massive.net.retry import Clock

__all__ = [
    "MAX_RETRY_AFTER",
    "RATE_WINDOW",
    "UNLIMITED",
    "Admission",
    "BucketState",
    "Patience",
    "Permit",
    "RequestGovernor",
    "is_rate_limit_refusal",
]

logger = logging.getLogger(__name__)

RATE_WINDOW: Final = 61.0
"""Seconds a finished request holds its slot of a paced bucket: Massive's minute plus 1 s.

Massive counts by the clock minute; holding a slot this long after each response is never
faster than that allows, whatever the local clock reads."""

MAX_RETRY_AFTER: Final = 600.0
"""The longest pause a 429's `Retry-After` sets, in seconds: 10 minutes. A longer one is clamped
to it, and the refusal's log line gives both; the bucket probes again when it ends."""

_PER_MINUTE_REFUSAL: Final = "exceeded the maximum requests per minute"
"""The words of Massive's 429 message that say a subscription has a limit per minute."""

_MINUTE: Final = 60
"""Seconds in a clock minute, the span Massive counts a limit over."""

_SCHEDULE: Final = ", ".join(f"{wait:g}" for wait in RATE_LIMIT_WAITS)
"""The pauses of an unpaced bucket's run of refusals without a `Retry-After`, for the log."""

_UNTIL_WOKEN: Final = math.inf
"""The wait of a request with no deadline: only a change of its bucket wakes it."""


def is_rate_limit_refusal(message: str) -> bool:
    """Tell whether a 429's message says the subscription has a limit of requests per minute.

    Massive's knowledge base quotes "You've exceeded the maximum requests per minute" as its
    answer to a request over a subscription's limit (the article vendored as
    `vendor/massive/knowledge-base/what-is-the-request-limit-for-massives-restful-apis.md`).
    The message does not give the limit.

    Args:
        message: The refusal's text: the server's `message`, or a `WireError` message that
            quotes it.

    Returns:
        True when the text contains "exceeded the maximum requests per minute", ignoring case.
    """
    return _PER_MINUTE_REFUSAL in message.casefold()


def _learning_text(
    label: str, count: int, before: int | None, *, declared: int | None, declarable: bool
) -> tuple[int, str]:
    """Return the log level and text of a run's learning (`RequestGovernor.done`).

    Args:
        label: How messages name the bucket's requests, such as `"stocks requests"`.
        count: The requests Massive answered of those sent in the refused minute.
        before: The limit learned before this run, or None.
        declared: The declared limit, or None.
        declarable: Whether a limit may be declared for the bucket, which the remedy names.

    Returns:
        Without a declared limit: INFO and the line when the minute held no answer or the
        learned limit changed; DEBUG when the count left it as it was. With one: INFO when the
        count is below the declared limit and above zero; DEBUG otherwise, since the declared
        limit paces the bucket either way.
    """
    said = f"Massive answered {count} {label} sent in the clock minute of its refusal"
    if declared is not None:
        if count == 0 or count >= declared:
            return logging.DEBUG, f"{said}; the declared {declared} per minute still paces them"
        return logging.INFO, (
            f"{said}; the declared {declared} per minute still paces them; if refusals repeat, "
            f"lower it toward {count}"
        )
    declare = (
        "declare the limit Massive gives this subscription (rest_requests_per_minute)"
        if declarable
        else ""
    )
    if count == 0:
        kept = "they stay unpaced" if before is None else f"they keep the {before} learned before"
        advice = f"; if this repeats, {declare}" if declarable and before is None else ""
        return logging.INFO, (
            f"{said}, so nothing is learned and {kept}: another program of the account likely "
            f"spent that minute{advice}"
        )
    if before is not None and count >= before:
        return logging.DEBUG, f"{said}, so they keep the {before} per minute learned before"
    lowered = "" if before is None else f", fewer than the {before} learned before"
    rises = (
        f"; this pace never rises on its own: if your limit is higher (another program of the "
        f"account shared that minute), {declare}"
        if declarable
        else "; this pace never rises on its own"
    )
    paced = f"so they are paced at {count} per minute from now on"
    return logging.INFO, f"{said}{lowered}, {paced}{rises}"


@dataclass(frozen=True, eq=False, slots=True)
class Permit:
    """The right of one attempt of a request to be sent, from `Admission.acquire`.

    The caller reports how the attempt ended with `RequestGovernor.done`, `refused`, or
    `release`. Only the first report counts: a later one does nothing, so a caller can always
    report in a `finally` without risking a second count. Until the first report the permit
    holds a slot of its bucket, in flight.

    Attributes:
        bucket: The bucket the attempt is charged to.
        episode: The pause the attempt started after (a count of the bucket's pauses), which
            tells a response to the latest pause apart from one to an earlier burst.
        probe: True for the first attempt to start after a pause, which goes alone while
            the bucket's other requests wait for its answer.
    """

    bucket: str
    episode: int
    probe: bool = False


class Patience:
    """How long one read waits out its buckets' pauses, shared by every request of the read.

    The library makes one at the start of each read (a `fetch()`, a `history()`, one `poll()`
    cycle, one lookup) and passes it to every request of the read, on every thread of a
    fan-out. Each time a request of the read must wait for a pause, the part of that pause the
    read has not already counted is charged to it; slot waits are never charged. A request
    whose pause would take the read past `seconds` fails instead (`RequestGovernor.admission`).
    The governor reads and charges it only under its own lock, so a read's patience must be
    used with one governor.

    Its `seconds`, `advice`, and `spent` are read-only; only `charge` changes it, and never a
    patience without limit, so the one `UNLIMITED` serves every governor and thread.

    Example:
        ::

            patience = Patience(600.0)  # ten minutes of pauses for this read
            with governor.admission("stocks", lane=read, patience=patience) as admission:
                permit = admission.acquire(read)
    """

    __slots__ = ("_advice", "_counted_until", "_seconds", "_spent")

    def __init__(self, seconds: float, *, advice: str = "") -> None:
        """Start with nothing charged.

        Args:
            seconds: The most pause time the read waits out, in seconds; `math.inf` for none.
            advice: One more remedy the caller knows, which ends the message of a request that
                gives up (`RequestGovernor` adds it after its own remedies); empty for none.
        """
        self._seconds = seconds
        self._advice = advice
        self._spent = 0.0
        # The end of the pause time already charged (monotonic): a pause several requests of
        # the read wait out together is charged once.
        self._counted_until = -math.inf

    @property
    def seconds(self) -> float:
        """The most pause time the read waits out, in seconds; `math.inf` for no limit."""
        return self._seconds

    @property
    def advice(self) -> str:
        """The caller's remedy that ends a give-up message; empty for none."""
        return self._advice

    @property
    def spent(self) -> float:
        """The pause time charged so far, in seconds."""
        return self._spent

    def charge(self, now: float, until: float) -> bool:
        """Charge a wait for a pause ending at `until`, if the patience covers it.

        Called by the governor with its lock held.

        Args:
            now: The monotonic time.
            until: When the pause ends.

        Returns:
            True when the read waits it out (the part not yet counted is charged), False when
            that part would take the read past `seconds` (nothing is charged). A patience
            without limit returns True and charges nothing, as `UNLIMITED` needs.
        """
        if self._seconds == math.inf:
            return True
        added = max(0.0, until - max(now, self._counted_until))
        if self._spent + added > self._seconds:
            return False
        self._spent += added
        self._counted_until = max(self._counted_until, until)
        return True


UNLIMITED: Final = Patience(math.inf)
"""The patience of a request that waits out any pause; the default of `admission`. It is
read-only, so every governor and thread can share it."""


class Admission:
    """One attempt's claim on its bucket, open for a `with` block (`RequestGovernor.admission`).

    The only way to take a permit. `acquire` inside the block waits for it.

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
        patience: The patience of the attempt's read, which the attempt's pause waits charge;
            it does not wait for a pause the patience cannot cover.
        permit: The granted permit, or None before the grant.

    Example:
        ::

            with governor.admission("stocks", lane=read_token) as admission:
                permit = admission.acquire(read_token)
                ...  # send the request; report it with done, refused, or release
    """

    __slots__ = ("_governor", "bucket", "lane", "patience", "permit")

    def __init__(
        self, governor: RequestGovernor, bucket: str, *, lane: Hashable, patience: Patience
    ) -> None:
        """Hold nothing yet.

        Args:
            governor: The governor.
            bucket: The bucket the attempt is charged to.
            lane: The lane it waits in (see `RequestGovernor.admission`).
            patience: The patience of the attempt's read (see `RequestGovernor.admission`).
        """
        self._governor = governor
        self.bucket = bucket
        self.lane = lane
        self.patience = patience
        # The granted permit, set under the governor's lock as it is granted; None before.
        self.permit: Permit | None = None

    def __enter__(self) -> Self:
        """Open the claim; nothing is granted yet.

        Returns:
            The admission.
        """
        return self

    def acquire(self, cancel: Cancel) -> Permit:
        """Wait until the attempt may start, and return its permit.

        Blocks the calling thread, outside every lock, for as long as the bucket's lanes,
        pace, pause, and probe demand; waiting for a slot never times out. An attempt that may
        start at once, with nothing queued before it, is granted without waiting. Report the
        attempt's end with `RequestGovernor.done`, `refused`, or `release`; the block's exit
        reports it otherwise. A cancellation, a failure, or an interrupt anywhere after the
        grant (`KeyboardInterrupt` while the thread sleeps, in a listener, or while it stops
        listening to `cancel`) releases the permit before the exception propagates. The lane
        (see `RequestGovernor.admission`) orders the attempt among the bucket's others. The
        first attempt after a pause, its probe, logs one DEBUG line saying the pause is over.

        Args:
            cancel: Ends the wait when set: the attempt gives up its place.

        Returns:
            The permit, also kept in `permit`.

        Raises:
            WireError: Without a status when `cancel` is set (the message says the wait was
                cancelled), and with status 429 when the bucket is paused for longer than the
                read's patience can still wait out (the message names the bucket's requests,
                says when its next request, a probe, goes, gives the patience and what it has
                spent, the usual cause and the remedies, and quotes the latest refusal).
        """
        permit = self._governor.admit(self, cancel)
        if isinstance(permit, WireError):
            logger.debug("%s: the wait for a permit ends: %s", self.bucket, permit.message)
            raise WireError(permit.message, status=permit.status, kind=permit.kind)
        if permit.probe:
            logger.debug(
                "%s: the pause is over; this request probes alone, and the bucket's other "
                "requests wait for its answer",
                self.bucket,
            )
        return permit

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
        limit: The requests per minute it is paced at: `declared` when set, else `learned`;
            None when it is unpaced.
        declared: The limit the user declared (`RequestGovernor.declare`), or None.
        learned: The limit learned from Massive's refusals (see the module), or None.
        in_flight: Requests sent and not yet answered.
        waiting: Requests waiting for a slot or for the end of a pause.
        recent: Slots held: requests in flight, and finished ones less than `RATE_WINDOW`
            seconds after their response.
        attempts: Attempts started since the governor was created: every retry and refused
            attempt counts, so one request may count several times.
        refusals: HTTP 429 responses since the governor was created.
        paused_until: When the current pause ends, in seconds since the Unix epoch; None when
            no pause is active.
        last_refusal: The message of the latest 429, with its request id; None before any.
    """

    bucket: str
    limit: int | None
    declared: int | None
    learned: int | None
    in_flight: int
    waiting: int
    recent: int
    attempts: int
    refusals: int
    paused_until: float | None
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
        "answered",
        "attempts",
        "declarable",
        "declared",
        "due",
        "episode",
        "label",
        "lanes",
        "last_refusal",
        "learned",
        "open",
        "pause_until",
        "probe",
        "probing",
        "refusals",
        "refused_minute",
        "run_refusals",
        "slots",
        "turns",
    )

    def __init__(self, label: str, *, declarable: bool) -> None:
        """Start unpaced, idle, and with no refusal.

        Args:
            label: How messages name the bucket's requests, such as `"stocks requests"`.
            declarable: Whether a limit may be declared for the bucket: False for one paced
                only once it learns a limit.
        """
        self.label = label
        self.declarable = declarable
        self.declared: int | None = None
        self.learned: int | None = None
        # The requests Massive answered (any status but 429) in each recent clock minute, keyed
        # by the minute's number (wall seconds // 60); only the minutes learning may still read
        # are kept (`count_answer`).
        self.answered: dict[int, int] = {}
        # The clock minute that held the send of the current run's earliest refusal naming the
        # per-minute maximum, which the run's end learns from; None when there is none.
        self.refused_minute: int | None = None
        # The refused minutes of runs that have ended, each learned from once no request sent
        # in it is still in flight (`learn_ready`).
        self.due: set[int] = set()
        # The waiting requests of each lane, first in first out, and the lanes' turns.
        self.lanes: dict[Hashable, deque[_Waiter]] = {}
        self.turns: deque[Hashable] = deque()
        # The permits granted and not yet reported, the requests in flight, each with when it
        # was sent (seconds since the Unix epoch): its grant until the client says it is sending
        # (`RequestGovernor.sending`). Adding one is the grant itself, a single step, so no
        # interruption can leave a count out of step.
        self.open: dict[Permit, float] = {}
        # When each finished request's slot frees (monotonic seconds), oldest first.
        self.slots: deque[float] = deque()
        self.attempts = 0
        self.refusals = 0
        self.pause_until = float("-inf")
        self.episode = 0
        self.run_refusals = 0  # counted refusals of the current run; 0 when none is going
        self.last_refusal: str | None = None
        # Set by a pause: the next request to start is its probe, alone (`probe`) until
        # Massive answers it with anything but a 429.
        self.probing = False
        self.probe: Permit | None = None

    @property
    def limit(self) -> int | None:
        """The requests per minute the bucket is paced at: the declared limit, else the learned."""
        return self.learned if self.declared is None else self.declared

    def count_answer(self, sent: float, wall: float) -> None:
        """Count an answer of Massive in the clock minute of its send, and forget stale minutes.

        Massive counts a request when it arrives, nearest its send, and a refusal is placed by
        its send too, so an answer that comes back after the minute turned still counts in the
        minute it was sent. A minute is kept while learning may still read it: the run's
        refused minute, the minutes due to be learned, the minute of any request still in
        flight (its 429 may yet come), and the minute before this one.

        Args:
            sent: When the answered request was sent, in seconds since the Unix epoch.
            wall: The time now, in seconds since the Unix epoch.
        """
        minute = int(sent // _MINUTE)
        self.answered[minute] = self.answered.get(minute, 0) + 1
        kept = [int(wall // _MINUTE) - 1, *self.due]
        kept.extend(int(other // _MINUTE) for other in self.open.values())
        if self.refused_minute is not None:
            kept.append(self.refused_minute)
        oldest = min(kept)
        for old in [m for m in self.answered if m < oldest]:
            del self.answered[old]

    def end_learning(self) -> None:
        """Mark the ended run's refused minute, if it has one, as due to be learned from.

        Called at the end of a run, when Massive answered a request sent after the pause.
        """
        if self.refused_minute is not None:
            self.due.add(self.refused_minute)
            self.refused_minute = None

    def learn_ready(self) -> list[tuple[int, int | None]]:
        """Learn from each due minute that no request still in flight was sent in.

        Returns:
            For each minute learned from, oldest first, its count and the learned limit before
            it. The bucket takes the count as its learned limit when it had none or the count
            is lower; a count of zero learns nothing.
        """
        in_flight = {int(sent // _MINUTE) for sent in self.open.values()}
        results = []
        for minute in sorted(self.due - in_flight):
            self.due.discard(minute)
            count = self.answered.get(minute, 0)
            before = self.learned
            if count > 0 and (before is None or count < before):
                self.learned = count
            results.append((count, before))
        return results

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
        """Free the slots whose window has passed.

        Args:
            now: The monotonic time.
        """
        while self.slots and self.slots[0] <= now:
            self.slots.popleft()

    def settle(self, now: float) -> None:
        """End a run that time has ended, before anything reads or extends it.

        A run whose pause ended more than `RATE_WINDOW` ago, with no refusal since and no
        probe still waiting for its answer, was abandoned (its read was cancelled, or its
        session closed), so a refusal hours later is the first of a new run and pauses for
        15 s again; a slow probe's own refusal still belongs to its run.

        Args:
            now: The monotonic time.
        """
        if self.probe is None and now - self.pause_until > RATE_WINDOW:
            self.run_refusals = 0
            self.refused_minute = None  # an abandoned run learns nothing

    def pause(self, until: float) -> None:
        """Extend the pause to `until`, probe after it, and wake every waiting request.

        Every waiter, not only the head, checks the new pause against its own read's patience:
        a request behind a more patient one must not sleep past its own patience.

        Args:
            until: When the new pause ends; a pause only ever grows.
        """
        if until > self.pause_until:
            self.pause_until = until
            for lane in self.lanes.values():
                for waiter in lane:
                    waiter.wake.set()
        self.probing = True

    def fallback_pause(self, *, per_minute: bool) -> tuple[float, str]:
        """Return the pause of a counted refusal without a `Retry-After`, and why, for the log.

        A paced bucket, or a refusal naming the per-minute maximum, has spent its minute, so it
        pauses a whole window; any other refusal takes the run's step of the 15, 30, 60 s
        schedule (`net.retry.rate_limit_wait`).

        Args:
            per_minute: Whether the refusal named the per-minute maximum.

        Returns:
            The pause in seconds and its reason.
        """
        if self.limit is not None:
            return RATE_WINDOW, f"one window, paced at {self.limit} per minute"
        if per_minute:
            return RATE_WINDOW, "one window: Massive limits these requests per minute"
        step = min(self.run_refusals, len(RATE_LIMIT_WAITS))
        return rate_limit_wait(self.run_refusals, None), f"schedule step {step} of {_SCHEDULE} s"

    def start_at(self, now: float) -> float:
        """Return when the bucket's head may start.

        Args:
            now: The monotonic time; the expired slots are already freed.

        Returns:
            The monotonic time it may start (`now` or earlier means at once), or `math.inf`
            when no time alone lets it: a probe is waiting for its answer, or every slot is
            held by a request in flight, so only a change of the bucket can.
        """
        start = max(now, self.pause_until)
        if self.probe is not None:
            return _UNTIL_WOKEN
        limit = self.limit
        if limit is None:
            return start
        excess = self.in_flight + len(self.slots) - limit
        if excess < 0:
            return start
        if excess >= len(self.slots):
            return _UNTIL_WOKEN
        return max(start, self.slots[excess])

    def grant(self, name: str, admission: Admission, wall: float) -> Permit:
        """Grant a request that may start now, and record it; the caller holds the lock.

        Args:
            name: The bucket's name.
            admission: The request's claim, which records the permit in the same step.
            wall: The time now, in seconds since the Unix epoch.

        Returns:
            The permit, in flight; the pause's probe when the bucket is probing.
        """
        permit = admission.permit = Permit(name, self.episode, probe=self.probing)
        self.open[permit] = wall  # the grant
        self.attempts += 1
        if self.probing:
            self.probe = permit
        return permit

    def finish(self, permit: Permit, now: float, *, hold: bool) -> float | None:
        """End a request's flight, if it has not ended already.

        Args:
            permit: The request's permit.
            now: The monotonic time its response arrived (or its attempt failed).
            hold: True to keep its slot for a window (it may have reached Massive), False to
                free it at once (it was never sent).

        Returns:
            When the request was sent, in seconds since the Unix epoch, for the permit's first
            report; None (and no change) for any later one.
        """
        sent = self.open.pop(permit, None)
        if sent is None:
            return None
        if permit is self.probe:  # the next request probes, unless its answer ended the run
            self.probe = None
        if hold:
            self.slots.append(now + RATE_WINDOW)
        self.wake_head()
        return sent


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
            governor.declare({"stocks": 5})  # the limit Massive gives the account
            closed = Cancel()
            # The lane keys the requests that take turns together.
            with governor.admission("stocks", lane=object()) as admission:
                permit = admission.acquire(closed)  # blocks while no slot is free
                ...  # send the request
                governor.done(permit, answered=True)
    """

    __slots__ = ("_buckets", "_clock", "_listeners", "_lock")

    def __init__(
        self,
        buckets: Iterable[str],
        *,
        learn_only: Iterable[str] = (),
        clock: Clock = SYSTEM_CLOCK,
        labels: Mapping[str, str] = MappingProxyType({}),
    ) -> None:
        """Create the buckets, each unpaced and idle.

        Args:
            buckets: The bucket names, such as `("stocks", "options", ..., "other")`.
            learn_only: The buckets whose limit cannot be declared, such as `("other",)` for
                data of no subscription, which no setting names: they are paced only once they
                learn a limit, and declaring one raises.
            clock: The time source of every wait and pause; the clients that use the
                governor wait on it too. Tests pass a fake.
            labels: How messages name each bucket's requests, such as `{"other": "requests
                outside Massive's subscriptions"}`; a bucket left out is `"<name> requests"`.

        Raises:
            ValueError: If `buckets` is empty or names one twice, or `learn_only` names a
                bucket not in `buckets`.
        """
        names = tuple(buckets)
        if not names or len(set(names)) != len(names):
            raise ValueError(f"buckets must be distinct names, at least one: {names!r}")
        fixed = frozenset(learn_only)
        if not fixed <= set(names):
            raise ValueError(f"learn-only buckets {sorted(fixed - set(names))} are not buckets")
        self._clock = clock
        self._lock = threading.Lock()
        self._buckets = {
            name: _Bucket(labels.get(name, f"{name} requests"), declarable=name not in fixed)
            for name in names
        }
        # Each listener, with the buckets whose changes it hears.
        self._listeners: tuple[tuple[Callable[[], None], frozenset[str]], ...] = ()

    @property
    def clock(self) -> Clock:
        """The governor's time source, which its clients also wait on."""
        return self._clock

    @property
    def buckets(self) -> tuple[str, ...]:
        """The bucket names, in construction order."""
        return tuple(self._buckets)

    def declare(self, limits: Mapping[str, int]) -> None:
        """Set each bucket's declared limit, which paces it from its first request.

        A declared limit wins over a learned one. A bucket left out is no longer declared; it
        stays paced only at a limit it learned.

        Args:
            limits: The requests per minute of each declared bucket, each at least 1.

        Raises:
            ValueError: If a name is not a bucket, is a bucket that cannot be declared, or has a
                limit below 1.
        """
        limits = dict(limits)
        unknown = limits.keys() - self._buckets.keys()
        if unknown:
            raise ValueError(f"unknown buckets {sorted(unknown)}; known: {list(self._buckets)}")
        fixed = sorted(name for name in limits if not self._buckets[name].declarable)
        if fixed:
            raise ValueError(
                f"buckets {fixed} cannot be declared; they are paced once they learn a limit "
                "from Massive's refusal"
            )
        low = {name: limit for name, limit in limits.items() if limit < 1}
        if low:
            raise ValueError(f"declared limits must be at least 1 request per minute: {low}")
        with self._lock:
            for name, bucket in self._buckets.items():
                bucket.declared = limits.get(name)
                bucket.wake_head()
        self._notify(*self._buckets)

    def admission(
        self, bucket: str, *, lane: Hashable, patience: Patience = UNLIMITED
    ) -> Admission:
        """Open one attempt's claim on a bucket, for a `with` block; nothing is granted yet.

        The only way to take a permit: `Admission.acquire` inside the block takes it, and the
        block's exit reports it if nothing else did (see `Admission`).

        Args:
            bucket: The bucket the attempt is charged to.
            lane: The lane the attempt waits in, such as its read's cancellation token: the
                attempts of one lane start in order, and the lanes of a bucket take turns. An
                attempt alone in its lane passes a key of its own, such as a fresh `object()`.
            patience: The patience of the attempt's read, shared by all its requests: the
                attempt's waits for a pause are charged to it, and a pause it cannot cover
                fails the attempt at once. `UNLIMITED`, the default, waits however long
                Massive refuses the bucket.

        Returns:
            The claim.

        Raises:
            ValueError: If `bucket` is not a bucket.
        """
        if bucket not in self._buckets:
            raise ValueError(f"unknown bucket {bucket!r}; known: {list(self._buckets)}")
        return Admission(self, bucket, lane=lane, patience=patience)

    def charge_pause(self, bucket: str, patience: Patience) -> WireError | None:
        """Charge a bucket's current pause to a read that waits on another read's requests.

        A read that shares a lookup another read started sends no request of its own, so no
        admission ever charges its patience. It calls this instead, when it starts waiting and
        after every change of that bucket (`subscribe_bucket`), and stops waiting once the
        bucket's pause outlasts what its own patience still covers, as a request of its own
        would (`Admission.acquire`). The read that started the lookup keeps its own patience.

        Args:
            bucket: The bucket the shared lookup's requests are charged to.
            patience: The waiting read's patience; the part of the pause it has not already
                counted is charged to it.

        Returns:
            None when the bucket is not paused or the patience covers its pause; otherwise a
            `WireError` with status 429 and the message of a request that gives up (when the
            bucket's next request goes, the patience and what it has spent, and the remedies),
            and nothing is charged.

        Raises:
            ValueError: If `bucket` is not a bucket.
        """
        state = self._buckets.get(bucket)
        if state is None:
            raise ValueError(f"unknown bucket {bucket!r}; known: {list(self._buckets)}")
        with self._lock:
            now = self._clock.monotonic()
            state.settle(now)
            return self._beyond_patience(state, patience, now)

    def admit(self, admission: Admission, cancel: Cancel) -> Permit | WireError:
        """Grant an attempt at once or after waiting, recording the permit on its claim.

        `Admission.acquire` calls it, and turns a failure into its `WireError`.

        Args:
            admission: The attempt's claim, for a known bucket.
            cancel: Ends the wait when set.

        Returns:
            The permit once granted, or the error when the attempt must fail, as
            `Admission.acquire` describes.
        """
        bucket, lane = admission.bucket, admission.lane
        state = self._buckets[bucket]
        at_once = self._grant_at_once(admission, cancel)
        if at_once is not None:
            return at_once
        waiter = _Waiter(lane)
        stop_listening = cancel.on_set(waiter.wake.set)
        try:
            try:
                with self._lock:
                    state.join(waiter)
                self._notify(bucket)
                outcome = self._wait(admission, waiter, cancel)
                if not isinstance(outcome, WireError):
                    self._notify(bucket)
            finally:
                stop_listening()
                with self._lock:
                    left = state.leave(waiter)
                if left:  # it gave up its place: the bucket's waiting count changed
                    self._notify(bucket)
        except BaseException:
            if admission.permit is not None:  # interrupted after the grant: hand it back
                self.release(admission.permit)
            raise
        return outcome

    def _grant_at_once(self, admission: Admission, cancel: Cancel) -> Permit | WireError | None:
        """Grant a request that may start now with nothing queued before it, without waiting.

        Args:
            admission: The request's claim, for a known bucket.
            cancel: The request's cancellation.

        Returns:
            The permit for a request granted now; the error when it must fail at once
            (cancelled, or a pause longer than its read's patience covers); None when it must
            queue.
        """
        bucket = admission.bucket
        state = self._buckets[bucket]
        with self._lock:
            now = self._clock.monotonic()
            failure = self._failure(admission, cancel, now=now)
            if failure is not None:
                return failure
            if state.head() is not None:
                return None
            state.expire(now)
            if state.start_at(now) > now:
                return None
            return state.grant(bucket, admission, self._clock.time())

    def _wait(self, admission: Admission, waiter: _Waiter, cancel: Cancel) -> Permit | WireError:
        """Sleep until a queued request is admitted or must fail.

        Args:
            admission: The request's claim, for a known bucket.
            waiter: The queued request.
            cancel: The request's cancellation.

        Returns:
            The permit once it is admitted, or the error when it must fail, as
            `Admission.acquire` describes.
        """
        while True:
            permit, delay, failure = self._step(admission, waiter, cancel)
            if failure is not None:
                return failure
            if permit is not None:
                return permit
            self._clock.wait(waiter.wake, delay)

    def done(self, permit: Permit, *, answered: bool) -> None:
        """Report an attempt that ended without a 429; its slot stays held for a window.

        Does nothing for a permit already reported. An answer that ends a pause's probing
        (the bucket's requests resume) logs one DEBUG line, and makes the run's refused minute
        due to be learned from (see the module). Any report, this one, `refused`, or `release`,
        learns from each due minute no request in flight was sent in, and logs one line for it
        with the count and the remedy, at the level `_learning_text` chooses.

        Args:
            permit: The attempt's permit.
            answered: True when Massive answered (any status but 429), which counts toward the
                answers of its clock minute and ends the bucket's run of refusals if the
                attempt started after the latest pause; False when no response came (a
                connection failure, an interruption), which changes nothing else.
        """
        state = self._buckets[permit.bucket]
        resumed = False
        with self._lock:
            sent = state.finish(permit, self._clock.monotonic(), hold=True)
            if sent is None:
                return
            if answered:
                state.count_answer(sent, self._clock.time())
            if answered and permit.episode == state.episode:
                resumed = state.probing
                state.run_refusals = 0  # the run is over
                state.probing = False  # Massive answered after the pause: no more probe
                if resumed:
                    state.end_learning()
            learned = state.learn_ready()
            declared = state.declared
        if resumed:
            logger.debug(
                "%s: Massive answered the first request after the pause; requests resume",
                permit.bucket,
            )
        self._log_learning(permit.bucket, learned, declared=declared)
        self._notify(permit.bucket)

    def sending(self, permit: Permit) -> None:
        """Record that an attempt is being sent now, which places it in Massive's minute.

        A permit is granted before the client waits for a connection, so its grant can fall in
        an earlier clock minute than its send; the client calls this just before it sends, and
        learning counts the attempt in the minute of this call. Does nothing for a permit
        already reported.

        Args:
            permit: The attempt's permit.
        """
        state = self._buckets[permit.bucket]
        with self._lock:
            if permit in state.open:
                state.open[permit] = self._clock.time()

    def _log_learning(
        self, bucket: str, learned: list[tuple[int, int | None]], *, declared: int | None
    ) -> None:
        """Log each minute a bucket learned from, outside the lock, as `_learning_text` says.

        Args:
            bucket: The bucket.
            learned: Each minute's count and the learned limit before it (`learn_ready`).
            declared: The bucket's declared limit when it learned, or None.
        """
        state = self._buckets[bucket]
        for count, before in learned:
            level, text = _learning_text(
                state.label, count, before, declared=declared, declarable=state.declarable
            )
            logger.log(level, "%s: %s", bucket, text)

    def release(self, permit: Permit) -> None:
        """Report an attempt that was never sent: its slot frees at once.

        Does nothing for a permit already reported.

        Args:
            permit: The attempt's permit.
        """
        state = self._buckets[permit.bucket]
        with self._lock:
            if state.finish(permit, self._clock.monotonic(), hold=False) is None:
                return
            learned = state.learn_ready()
            declared = state.declared
        self._log_learning(permit.bucket, learned, declared=declared)
        self._notify(permit.bucket)

    def refused(
        self,
        permit: Permit,
        *,
        retry_after: float | None,
        message: str,
    ) -> bool:
        """Report an attempt that Massive refused with HTTP 429, and pause its bucket.

        A refusal whose message names the per-minute maximum (`is_rate_limit_refusal`), while a
        run is going, marks the clock minute of the attempt's send for the run's learning (see
        the module), which ends at the first answer after the pause. A refusal
        to an attempt that started after the latest pause counts toward the run and sets a
        new pause; one to an attempt that started before it only lengthens the pause to its
        own `Retry-After`. A `Retry-After` of zero counts as none, and one longer than
        `MAX_RETRY_AFTER` pauses that long. A run that time has ended (see the module) ends
        first, so this refusal starts a new one. The caller then asks for a new permit, which
        waits out the pause, or fails if the pause ends past its own read's patience. Does
        nothing for a permit already reported.

        Each refusal logs one INFO line, after the listeners are called: the bucket, how long
        it is now paused and why (the `Retry-After`, as asked and as honoured; the step of the
        15, 30, 60 s schedule; one window of a paced bucket or of a refusal naming the
        per-minute maximum; or a request sent before the pause, which leaves the pause as it
        was unless its `Retry-After` is longer), and the refusal's message.

        Args:
            permit: The attempt's permit.
            retry_after: The wait the response's `Retry-After` asked for, in seconds (any
                length, `math.inf` included), or None when it gave none.
            message: The refusal's text, with its request id, quoted in diagnostics and in the
                error of a request that stops waiting.

        Returns:
            Whether the message names the per-minute maximum (`is_rate_limit_refusal`).
        """
        state = self._buckets[permit.bucket]
        per_minute = is_rate_limit_refusal(message)
        asked = retry_after if retry_after is not None and retry_after > 0 else None
        honoured = ""
        if asked is not None:
            honoured = f"Retry-After {asked:g} s"
            if asked > MAX_RETRY_AFTER:
                honoured = f"{honoured}, cut to {MAX_RETRY_AFTER:g} s, the most honoured"
                asked = MAX_RETRY_AFTER
        with self._lock:
            now = self._clock.monotonic()
            state.settle(now)  # before `finish`, so a slow probe still holds its run open
            sent = state.finish(permit, now, hold=True)
            if sent is None:
                return per_minute
            state.refusals += 1
            state.last_refusal = message
            if permit.episode == state.episode:
                state.episode += 1
                state.run_refusals += 1
                if asked is not None:
                    pause, reason = asked, honoured
                else:
                    pause, reason = state.fallback_pause(per_minute=per_minute)
                state.pause(now + pause)
            else:
                reason = "a request sent before the pause"
                if asked is not None:
                    reason = f"{reason}; {honoured}"
                    state.pause(now + asked)
            # Learning reads the earliest refused minute of a run still going; a late refusal
            # after its run ended teaches nothing, even when its Retry-After pauses the bucket.
            if per_minute and state.run_refusals > 0:
                minute = int(sent // _MINUTE)
                if state.refused_minute is None or minute < state.refused_minute:
                    state.refused_minute = minute
            # A request sent before a pause that has already ended leaves no pause.
            paused = max(0.0, state.pause_until - now)
            learned = state.learn_ready()
            declared = state.declared
        self._log_learning(permit.bucket, learned, declared=declared)
        self._notify(permit.bucket)
        # One INFO line per refusal, with the pause it chose: a decision, not something to act
        # on. The paused-poll WARNING and the read's own error say when a pause matters.
        logger.info(
            "%s: paused %.1f s (%s) after HTTP 429; the refusal: %s",
            permit.bucket,
            paused,
            reason,
            message,
        )
        return per_minute

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
                        last_refusal=bucket.last_refusal,
                    )
                )
        return tuple(states)

    def next_change(self) -> float | None:
        """Return the seconds until a bucket's state changes with time alone.

        A pause ends, or a held slot of a paced bucket frees, without any request or
        response; diagnostics that show the state read it again then. An unpaced bucket's
        slots are left out: they change only its count of recent requests, which the next
        request or response refreshes, so an unpaced bucket's diagnostics do not churn with time.

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
            self._listeners = (*self._listeners, (listener, frozenset(self._buckets)))

    def subscribe_bucket(self, bucket: str, listener: Callable[[], None]) -> None:
        """Call `listener` after every change of one bucket, and of no other.

        For a caller that waits on one bucket only, such as a read waiting on another read's
        lookup (`charge_pause`), so a change of another subscription never wakes it.

        Args:
            bucket: The bucket whose changes it hears.
            listener: As for `subscribe`.

        Raises:
            ValueError: If `bucket` is not a bucket.
        """
        if bucket not in self._buckets:
            raise ValueError(f"unknown bucket {bucket!r}; known: {list(self._buckets)}")
        with self._lock:
            self._listeners = (*self._listeners, (listener, frozenset((bucket,))))

    def unsubscribe(self, listener: Callable[[], None]) -> None:
        """Stop calling a listener, for every bucket it hears; an unknown one is ignored.

        Args:
            listener: A listener given to `subscribe` or `subscribe_bucket` (compared by
                equality, so a bound method may be passed again).
        """
        with self._lock:
            self._listeners = tuple(each for each in self._listeners if each[0] != listener)

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
            cancelled (no status), or a pause longer than its read's patience covers (429).
        """
        bucket = admission.bucket
        state = self._buckets[bucket]
        with self._lock:
            waiter.wake.clear()  # before reading the state, so no change is missed
            now = self._clock.monotonic()
            failure = self._failure(admission, cancel, now=now)
            if failure is not None:
                return None, 0.0, failure
            if state.head() is not waiter:
                return None, _UNTIL_WOKEN, None
            state.expire(now)
            start = state.start_at(now)
            if start > now:
                return None, start - now, None
            # Recorded on the claim in the same step as the grant, so an interrupted
            # `admit` always knows what to hand back.
            permit = state.grant(bucket, admission, self._clock.time())
            state.start_head()
            return permit, 0.0, None

    def _failure(self, admission: Admission, cancel: Cancel, *, now: float) -> WireError | None:
        """Return the error a request must fail with now, if any; the lock is held.

        Ends a run that time has ended first (`settle`).

        Args:
            admission: The request's claim, for a known bucket.
            cancel: The request's cancellation.
            now: The monotonic time.

        Returns:
            A `WireError` without a status, `WireFailure.STOPPED`, when `cancel` is set; one
            with status 429 when the bucket is paused for longer than the read's patience still
            covers (only a pause counts: an unpaused bucket never fails a request); or None. A
            pause the patience covers is charged to it.
        """
        bucket = admission.bucket
        if cancel.is_set():
            return WireError(
                f"{bucket}: the wait for a request slot was cancelled",
                kind=WireFailure.STOPPED,
            )
        state = self._buckets[bucket]
        state.settle(now)
        return self._beyond_patience(state, admission.patience, now)

    def _beyond_patience(self, state: _Bucket, patience: Patience, now: float) -> WireError | None:
        """Charge a bucket's pause to a read, or return the error of a read that gives up.

        Called with the lock held, after `settle`.

        Args:
            state: The bucket.
            patience: The read's patience.
            now: The monotonic time.

        Returns:
            None when the patience covers the pause (the part not yet counted is charged), or
            a `WireError` with status 429 when it does not (nothing is charged).
        """
        # A pause already over adds nothing to charge, so an unpaused bucket never fails.
        if not patience.charge(now, state.pause_until):
            # Massive refused the bucket with HTTP 429; the read will not wait out the pause.
            return WireError(
                self._give_up_text(state, patience, now), status=429, kind=WireFailure.REFUSED
            )
        return None

    def _give_up_text(self, state: _Bucket, patience: Patience, now: float) -> str:
        """Return the message of a request whose read will not wait out its bucket's pause.

        Called with the lock held, while the bucket is paused.

        Args:
            state: The bucket.
            patience: The read's patience.
            now: The monotonic time.

        Returns:
            The message: the bucket's requests and the latest refusal; when the next request
            goes, in UTC and in local time; the read's patience and what it has spent; the
            usual cause; and the remedies, declaring the limit only for a bucket that can be
            declared, then the patience's `advice`, when it has one.
        """
        goes = datetime.fromtimestamp(self._clock.time() + state.pause_until - now, UTC)
        remedies = "retry after that time, or stop the other program"
        if state.declarable:
            remedies = (
                "retry after that time; stop the other program; or declare the limit Massive "
                "gives this subscription (rest_requests_per_minute), or upgrade it"
            )
        if patience.advice:
            remedies = f"{remedies}; {patience.advice}"
        return (
            f"Massive is refusing {state.label} with HTTP 429 (latest refusal: "
            f"{state.last_refusal}). Its next request, a probe, goes at "
            f"{goes.isoformat(timespec='seconds')} "
            f"({goes.astimezone().isoformat(timespec='seconds')} local time), but this read "
            f"waits out at most {patience.seconds:.0f} s of pauses and has spent "
            f"{patience.spent:.0f} s of its patience. The usual cause is another program or "
            f"process using the same Massive account, whose limit they share. To go on: "
            f"{remedies}"
        )

    def _notify(self, *changed: str) -> None:
        """Call every listener of a changed bucket, outside the lock; one that raises is logged.

        Args:
            *changed: The buckets that changed.
        """
        with self._lock:
            listeners = self._listeners
        for listener, buckets in listeners:
            if buckets.isdisjoint(changed):
                continue
            try:
                listener()
            except Exception:
                logger.exception("a rate-limit listener failed. %s", LIBRARY_BUG_REPORT)
