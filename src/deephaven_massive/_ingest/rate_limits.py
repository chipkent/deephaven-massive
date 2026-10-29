"""Massive's REST rate limits on the library side: shared governors, warnings, and diagnostics.

Massive limits REST requests per subscription (stocks, options, indices, currencies, futures),
5 per minute on the free Basic tier and none on a paid one, and every API key of an account
shares the counters. Reference and cross-asset requests are counted in a bucket of their own
(Massive's knowledge base, "What is the request limit for Massive's RESTful APIs?", at
`https://massive.com/knowledge-base/article/` and the slug
`what-is-the-request-limit-for-massives-restful-apis`). The wire package's `RequestGovernor`
enforces that for the requests it admits; this module decides which requests share one, how
long each read waits, and what the user is told:

- **Sharing.** `GOVERNORS`, the process's `GovernorRegistry`, keeps one governor per (API key,
  REST origin). Each session takes a `GovernorLease` at construction and releases it when it
  closes or is collected, so the sessions of a process that use one key share its counters.
  The registry keys entries by the key's SHA-256 digest, never the key itself. What a governor
  learned is Basic lives and dies with it: a governor created again for the same key and origin
  starts unpaced, and relearns a Basic subscription from its first refusal. The governor
  paces every subscription that any live session declares Basic
  (`MassiveConfig.rest_basic_subscriptions`: the stricter wins), and logs one WARNING when two
  sessions declare different non-empty sets. An entry outlives its last lease until its
  governor is idle (no slot held, no pause active), so a notebook cell that makes a new session
  does not start with an empty window.
- **Buckets.** A request is charged to its dataset's subscription (`bucket_of`); reference
  data and the other datasets Massive assigns to no subscription go to `OTHER`, which is paced
  once a refusal names the per-minute limit, like a subscription, but cannot be declared.
- **Patience.** Each read decides how long it waits out pauses (`read_patience`): `READ_PATIENCE`
  (10 minutes of pauses), or `CONSOLE_PATIENCE` (90 s) for a read made while its thread holds the
  Deephaven update graph's lock, as a console command does, since every table of the process
  stops updating meanwhile. A read that gives up fails alone; the bucket keeps pausing and
  probing for every other read.
- **Warnings.** `RateLimitWarnings` is the REST client's refusal hook: the first 429 of each
  subscription in a session logs one WARNING that says what happened and what to do.
- **Diagnostics.** `rate_limit_rows` turns the governor's snapshot into the rows of
  `m.rate_limits()`; the session's diagnostics writer pulls them whenever the governor
  changes.
- **Notices.** `announce` logs, at INFO, how many requests a long static read makes and the
  least time pacing makes them take, and `PaceCheck` warns once when a `poll()` cycle needs
  more requests than its interval allows.

Locks: the registry has one leaf lock. It calls into a governor (`declare`, `idle`) while
holding it, so the order is the registry's lock, then the governor's, then whatever a governor
listener takes (the diagnostics queue's leaf lock). None of them is ever taken under
`SessionCore.registry_lock`, and none is held across a Deephaven call, a request, or a wait.
"""

from __future__ import annotations

import hashlib
import itertools
import logging
import math
import threading
from collections import Counter
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final

from deephaven_massive import _massive
from deephaven_massive._enums import Subscription
from deephaven_massive._tables.diagnostics import LimitSource, RateLimitRow, RateLimitState
from deephaven_massive._tables.liveness import holds_update_graph_lock

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping
    from collections.abc import Set as AbstractSet

    from deephaven.execution_context import ExecutionContext

    from deephaven_massive._massive import Patience, RequestGovernor, WireError

__all__ = [
    "BUCKETS",
    "CONSOLE_PATIENCE",
    "GOVERNORS",
    "LABELS",
    "LEARN_ONLY",
    "OTHER",
    "READ_PATIENCE",
    "GovernorLease",
    "GovernorRegistry",
    "PaceCheck",
    "RateLimitWarnings",
    "announce",
    "bucket_of",
    "rate_limit_pull",
    "rate_limit_rows",
    "read_patience",
]

logger = logging.getLogger(__name__)

OTHER: Final = "other"
"""The bucket of reference data and the other datasets Massive assigns to no subscription
(cross-market reference data, economy, partner data, the unified snapshot), which Massive
counts in a bucket of its own; it is paced once a refusal names the per-minute limit."""

BUCKETS: Final = (*(subscription.value for subscription in Subscription), OTHER)
"""Every bucket of a governor: one per subscription, then `OTHER`."""

LEARN_ONLY: Final = frozenset({OTHER})
"""The buckets that cannot be declared Basic, since no setting names them; a refusal naming
the per-minute limit paces them as it does a subscription."""

READ_PATIENCE: Final = 600.0
"""Seconds a read waits out its bucket's pauses before it fails with status 429."""

CONSOLE_PATIENCE: Final = 90.0
"""Seconds a read waits out pauses while its thread holds the update graph's lock: every table
of the process stops updating meanwhile, so it gives up sooner."""

LABELS: Final = {OTHER: "requests outside Massive's subscriptions"}
"""How the governor's messages name the requests of `OTHER`; a subscription's bucket is
`"<subscription> requests"`."""


_MINUTE: Final = 60.0
"""Seconds in the minute Massive's limits count."""


class ReadShape(StrEnum):
    """How a static read's requests are counted, which decides its INFO notice (`announce`).

    The notice's wording for each lives in `announce`, not in the values.

    Attributes:
        PAGED: The pages of a read whose request count follows its data, not its window. Only
            a paced bucket's notice is logged.
        PER_DAY: One request per day of the window, such as a grouped-daily download.
        PER_SESSION: One request per trading session and symbol, more where a session spans
            several pages.
    """

    PAGED = "paged"
    PER_DAY = "per_day"
    PER_SESSION = "per_session"


_SHAPE_WORDS: Final = {
    ReadShape.PER_DAY: "one per day",
    ReadShape.PER_SESSION: "one per session and symbol, more where a session spans several pages",
}
"""How the INFO notice says a read counts its requests; a paged read says nothing."""


def _real_clock_governor() -> RequestGovernor:
    """Make a governor of `BUCKETS`, `OTHER` learn-only, named by `LABELS`, on the real clock.

    Returns:
        A new `RequestGovernor` with nothing declared.
    """
    return _massive.RequestGovernor(BUCKETS, learn_only=LEARN_ONLY, labels=LABELS)


def read_patience(exec_ctx: ExecutionContext) -> Patience:
    """Return the patience of a read starting now: how long it waits out its buckets' pauses.

    A read waits out `READ_PATIENCE` (10 minutes) of pauses, or `CONSOLE_PATIENCE` (90 s)
    when its thread holds the Deephaven update graph's lock, as a console command does: no
    table of the process updates meanwhile. Only pauses count; waiting for a paced slot never
    does. Call it on the read's own thread, before any fan-out and outside every library lock,
    since it asks Deephaven (a Java call); pass the result to every request of the read.

    Args:
        exec_ctx: The session's execution context, whose update graph is asked.

    Returns:
        A new `Patience` for the read.
    """
    console = holds_update_graph_lock(exec_ctx)
    seconds = CONSOLE_PATIENCE if console else READ_PATIENCE
    logger.debug("a read waits out %.0f s of pauses (update-graph lock held: %s)", seconds, console)
    return _massive.Patience(seconds)


def bucket_of(subscription: Subscription | None) -> str:
    """Return the governor bucket a request of a subscription is charged to.

    Args:
        subscription: The request's subscription, or None for data of none.

    Returns:
        The subscription's value, or `OTHER`.
    """
    return OTHER if subscription is None else subscription.value


@dataclass(frozen=True, slots=True)
class _Holder:
    """One session's hold on a governor.

    Attributes:
        label: The session's label, such as `"MassiveSession #1"`.
        basic: The subscriptions it declares Basic.
    """

    label: str
    basic: frozenset[Subscription]


class _Entry:
    """A governor and the sessions that hold it; guarded by the registry's lock."""

    __slots__ = ("governor", "holders")

    def __init__(self, governor: RequestGovernor) -> None:
        """Hold a new governor, with no session yet.

        Args:
            governor: The governor.
        """
        self.governor = governor
        self.holders: dict[int, _Holder] = {}

    def declare(self) -> None:
        """Declare Basic every subscription that any holder declares (the stricter wins)."""
        basic = {subscription.value for h in self.holders.values() for subscription in h.basic}
        self.governor.declare(basic)


class GovernorLease:
    """One session's hold on its shared governor, from `GovernorRegistry.lease`.

    The session registers `release` as a closer, so the hold ends when the session closes or
    is collected unclosed. The lease refers to the registry, never to the session. Thread-safe.
    """

    __slots__ = ("_holder", "_key", "_listener", "_registry", "governor")

    def __init__(
        self,
        registry: GovernorRegistry,
        *,
        key: tuple[str, tuple[str, str, int]],
        holder: int,
        governor: RequestGovernor,
        listener: Callable[[], None],
    ) -> None:
        """Bind the lease; the registry has already recorded the hold.

        Args:
            registry: The registry that holds the entry.
            key: The entry's key.
            holder: The hold's number in the entry.
            governor: The shared governor.
            listener: The session's governor listener, removed by `release`.
        """
        self._registry = registry
        self._key = key
        self._holder = holder
        self._listener = listener
        self.governor: Final = governor

    def release(self) -> None:
        """End the hold: stop the session's listener and drop its declarations. Idempotent.

        The subscriptions only this session declared become unpaced again (unless learned
        Basic). The entry stays while its governor still holds slots or a pause.
        """
        self.governor.unsubscribe(self._listener)
        self._registry.release(self._key, self._holder)


class GovernorRegistry:
    """The governors of a process, one per (API key, REST origin), shared by its sessions.

    `GOVERNORS` is the process's registry; tests create their own, whose governors wait on a
    fake clock. Thread safety and lock order: see the module docstring.

    Example:
        ::

            from deephaven_massive._enums import Subscription
            from deephaven_massive._ingest.rate_limits import GovernorRegistry

            registry = GovernorRegistry()
            one = registry.lease(
                api_key="k",
                base_url="https://api.massive.com",
                basic={Subscription.STOCKS},
                label="MassiveSession #1",
                listener=lambda: None,
            )
            two = registry.lease(
                api_key="k",
                base_url="https://api.massive.com",
                basic=frozenset(),
                label="MassiveSession #2",
                listener=lambda: None,
            )
            assert one.governor is two.governor
            one.release()
            two.release()
    """

    __slots__ = ("_create", "_entries", "_holders", "_lock")

    def __init__(self, create: Callable[[], RequestGovernor] = _real_clock_governor) -> None:
        """Start with no governor.

        Args:
            create: Makes each new governor, with the buckets `BUCKETS` and the `LABELS`; by
                default on the real clock. Tests pass one whose governors wait on a fake clock.
        """
        self._create = create
        self._lock = threading.Lock()
        self._entries: dict[tuple[str, tuple[str, str, int]], _Entry] = {}
        self._holders = itertools.count(1)

    def __len__(self) -> int:
        """Count the governors kept, with or without a lease.

        Returns:
            The number of entries.
        """
        with self._lock:
            return len(self._entries)

    def leases(self) -> int:
        """Count the leases not yet released, over every governor.

        Each open session holds one, so a count above the number of open sessions means a
        session ended without releasing its lease.

        Returns:
            The number of leases held.
        """
        with self._lock:
            return sum(len(entry.holders) for entry in self._entries.values())

    def lease(
        self,
        *,
        api_key: str,
        base_url: str,
        basic: AbstractSet[Subscription],
        label: str,
        listener: Callable[[], None],
    ) -> GovernorLease:
        """Take a hold on the governor of an API key and REST origin, creating it if needed.

        Idle entries without a lease are dropped first, and with them what their governors
        learned; a new governor starts unpaced. The governor then paces the union of
        every holder's Basic subscriptions. When this session and another holder each declare
        a non-empty set and the sets differ, one WARNING names both sessions and the union.

        Args:
            api_key: The session's API key; only its SHA-256 digest is kept.
            base_url: The session's REST base URL; its scheme, host, and port are kept
                (`origin_of`, which the REST client also checks it with).
            basic: The subscriptions the session declares Basic.
            label: The session's label, for the WARNING.
            listener: Called after every change of the governor (`RequestGovernor.subscribe`);
                it must only enqueue.

        Returns:
            The lease, whose `governor` the session's REST client uses.
        """
        digest = hashlib.sha256(api_key.encode()).hexdigest()
        key = (digest, _massive.origin_of(base_url))
        mine = frozenset(basic)
        with self._lock:
            self._prune()
            entry = self._entries.get(key)
            if entry is None:
                entry = self._entries[key] = _Entry(self._create())
            conflict = next(
                (h for h in entry.holders.values() if h.basic and mine and h.basic != mine),
                None,
            )
            number = next(self._holders)
            entry.holders[number] = _Holder(label, mine)
            entry.declare()
            entry.governor.subscribe(listener)
            union = frozenset().union(*(h.basic for h in entry.holders.values()))
        if conflict is not None:
            logger.warning(
                "%s declares rest_basic_subscriptions %s, but %s, which uses the same API key, "
                "declares %s; Massive counts their requests together, so both pace %s",
                label,
                _names(mine),
                conflict.label,
                _names(conflict.basic),
                _names(union),
            )
        return GovernorLease(
            self, key=key, holder=number, governor=entry.governor, listener=listener
        )

    def release(self, key: tuple[str, tuple[str, str, int]], holder: int) -> None:
        """End one hold, and pace only what the remaining holders declare; idempotent.

        Args:
            key: The entry's key.
            holder: The hold's number; an unknown one is ignored.
        """
        with self._lock:
            entry = self._entries.get(key)
            if entry is None or entry.holders.pop(holder, None) is None:
                return
            entry.declare()

    def _prune(self) -> None:
        """Drop the entries that have no holder and an idle governor; the lock is held.

        What a dropped governor learned goes with it.
        """
        for key, entry in list(self._entries.items()):
            if not entry.holders and entry.governor.idle():
                del self._entries[key]


def _names(subscriptions: Iterable[Subscription]) -> str:
    """Spell a set of subscriptions for a message.

    Args:
        subscriptions: The subscriptions.

    Returns:
        Their values, sorted, in braces, such as `"{options, stocks}"`.
    """
    return "{" + ", ".join(sorted(s.value for s in subscriptions)) + "}"


GOVERNORS: Final = GovernorRegistry()
"""The process's registry of governors, used by every session."""


class RateLimitWarnings:
    """One session's WARNING at the first HTTP 429 of each bucket: the REST client's hook.

    Thread-safe: a leaf lock guards the buckets already warned about, and the WARNING is
    logged after it is released.
    """

    __slots__ = ("_basic", "_label", "_lock", "_warned")

    def __init__(self, label: str, basic: AbstractSet[Subscription]) -> None:
        """Start with nothing warned.

        Args:
            label: The session's label.
            basic: The subscriptions the session declares Basic.
        """
        self._label = label
        self._basic = frozenset(subscription.value for subscription in basic)
        self._lock = threading.Lock()
        self._warned: set[str] = set()

    def refused(self, bucket: str, error: WireError, *, basic: bool) -> None:
        """Log the session's WARNING for a bucket's first 429; later ones log nothing here.

        The WARNING says what the refusal means: a subscription the session declared Basic is
        refused because other sessions, processes, or keys of the account share its limit; a
        refusal that names the per-minute maximum shows the Basic limit, which is paced from
        now on (a subscription could be declared; `OTHER` cannot); any other refusal pauses
        the requests, and a
        paid subscription that keeps getting them is a matter for Massive's support.

        Args:
            bucket: The refused request's bucket.
            error: The refusal.
            basic: Whether its message named the per-minute maximum.
        """
        with self._lock:
            if bucket in self._warned:
                return
            self._warned.add(bucket)
        rate = _massive.BASIC_REQUESTS_PER_MINUTE
        what = f"{'an' if bucket[0] in 'aeiou' else 'a'} {bucket} request"
        if bucket == OTHER:
            what = "a request outside Massive's subscriptions"
        if bucket in self._basic:
            advice = (
                f"its requests are already paced at {rate} per minute, so other sessions, "
                "processes, or API keys of the account are using the same limit; requests "
                "pause and resume"
            )
        elif basic and bucket == OTHER:
            advice = (
                f"that is Massive's limit of {rate} requests per minute on reference and other "
                "data outside the subscriptions, which it counts on their own, so these "
                "requests are paced at that rate from now on"
            )
        elif basic:
            advice = (
                f"that is the Basic tier's limit of {rate} requests per minute, so these "
                "requests are paced at that rate from now on; declare "
                f"MassiveConfig(rest_basic_subscriptions={{dm.Subscription.{bucket.upper()}}}) "
                "to pace them from the first request"
            )
        else:
            advice = (
                "requests pause and resume; a paid subscription has no request limit, so if "
                "this persists, send the request id to Massive's support"
            )
        logger.warning(
            "%s: Massive refused %s with HTTP 429 (%s); %s", self._label, what, error, advice
        )


def rate_limit_rows(governor: RequestGovernor) -> list[RateLimitRow]:
    """Return the rows of `m.rate_limits()`: the governor's state, one row per bucket.

    Args:
        governor: The session's governor.

    Returns:
        One row per bucket, in `BUCKETS` order, stamped now on the governor's clock, the one
        `PausedUntil` is read on.
    """
    now = round(governor.clock.time() * 1e9)  # the clock `PausedUntil` is read on
    rows = []
    for state in governor.snapshot():
        if state.paused_until is not None:
            status = RateLimitState.PAUSED
        elif state.limit is not None and (state.waiting or state.recent >= state.limit):
            status = RateLimitState.PACED
        else:
            status = RateLimitState.READY
        basis = (
            LimitSource.DECLARED
            if state.declared
            else LimitSource.LEARNED
            if state.learned
            else None
        )
        until = None if state.paused_until is None else round(state.paused_until * 1e9)
        rows.append(
            RateLimitRow(
                bucket=state.bucket,
                state=status,
                requests_per_minute=state.limit,
                limit_source=basis,
                in_flight=state.in_flight,
                waiting=state.waiting,
                recent_requests=state.recent,
                attempts=state.attempts,
                refusals=state.refusals,
                paused_until=until,
                last_refusal=state.last_refusal,
                updated_at=now,
            )
        )
    return rows


def rate_limit_pull(governor: RequestGovernor) -> tuple[list[RateLimitRow], float | None]:
    """Return the rows of `m.rate_limits()` and when they next change with time alone.

    The session's diagnostics writer calls it (`Diagnostics.watch_rate_limits`) whenever the
    governor signals a change, and again when the time it returns has passed, so a pause that
    ends or a slot that frees with no request around still shows.

    Args:
        governor: The session's governor.

    Returns:
        `rate_limit_rows(governor)` and `governor.next_change()`.
    """
    return rate_limit_rows(governor), governor.next_change()


def announce(
    governor: RequestGovernor,
    read: str,
    buckets: Iterable[str],
    *,
    shape: ReadShape = ReadShape.PAGED,
    pauses_live_tables: bool = False,
) -> None:
    """Log at INFO, before a long static read, how many requests it makes and how long they take.

    One line per bucket of the read:

    - a paced bucket whose requests cannot all start within a minute gets the least time they
      take (`RequestGovernor.estimate`), such as "stocks.bars history: 250 requests; paced at
      5 requests a minute (stocks); at that pace they take at least 49 min";
    - an unpaced subscription's bucket of a download that makes one request per day or per
      session (`shape`) and more than 5 requests gets the time it would take were the
      subscription on the Basic tier, which the library learns only at Massive's first refusal
      unless it is declared: "stocks.bars history: 11 requests, one per day; if the stocks
      subscription is on the Basic tier (5 requests a minute), they take at least 2 min".

    Both are lower bounds: pages beyond the first, slow responses, and other sessions'
    requests only add time.

    A read made on a thread that holds the update graph's lock (`pauses_live_tables`), as every
    Deephaven console command does, holds every live table of the worker still while it runs,
    so its paced line is a WARNING instead, which says so and names the remedy: "stocks.bars
    history: 250 requests; paced at 5 requests a minute (stocks); this command will pause every
    live table in the worker for at least 49 min: run long downloads from a thread of your own,
    outside a console command".

    Args:
        governor: The session's governor.
        read: What is read, such as `"stocks.bars history"`.
        buckets: The bucket of each planned request, one entry per request.
        shape: How the read counts its requests. The default, `ReadShape.PAGED`, announces
            a read only when it is paced.
        pauses_live_tables: True when the read runs on a thread that holds the update graph's lock.
    """
    rate = _massive.BASIC_REQUESTS_PER_MINUTE
    for bucket, count in Counter(buckets).items():
        seconds = governor.estimate(bucket, count)
        words = _SHAPE_WORDS.get(shape)
        what = f"{count} requests" if words is None else f"{count} requests, {words}"
        if seconds is not None:
            if seconds >= _MINUTE and pauses_live_tables:
                logger.warning(
                    "%s: %s; paced at %d requests a minute (%s); this command will pause every "
                    "live table in the worker for at least %d min: run long downloads from a "
                    "thread of your own, outside a console command",
                    read,
                    what,
                    rate,
                    bucket,
                    math.floor(seconds / _MINUTE),
                )
            elif seconds >= _MINUTE:
                logger.info(
                    "%s: %s; paced at %d requests a minute (%s); at that pace they take at "
                    "least %d min",
                    read,
                    what,
                    rate,
                    bucket,
                    math.floor(seconds / _MINUTE),
                )
        elif shape is not ReadShape.PAGED and bucket != OTHER and count > rate:
            logger.info(
                "%s: %s; if the %s subscription is on the Basic tier (%d requests a minute), "
                "they take at least %d min",
                read,
                what,
                bucket,
                rate,
                math.ceil((count - rate) / rate),
            )


class PaceCheck:
    """Warns once when a `poll()` table's cycles need more requests than their interval allows.

    `RestEndpoints.poll` makes one per table, and the poller's reader calls `check` with each
    cycle's planned requests. Only the poller's thread calls it.
    """

    __slots__ = ("_interval", "_read", "_warned")

    def __init__(self, read: str, interval: float) -> None:
        """Start unwarned.

        Args:
            read: What is polled, such as `"stocks.snapshots poll()"`.
            interval: Seconds from one cycle's start to the next.
        """
        self._read = read
        self._interval = interval
        self._warned = False

    def check(self, governor: RequestGovernor, requests: Mapping[str, int]) -> None:
        """Log one WARNING the first time a cycle's paced requests outlast the interval.

        A paced bucket starts at most its limit of requests per window of 61 s (the minute
        plus the governor's margin), so `count` requests take `count * 61 / limit` seconds.

        Args:
            governor: The session's governor.
            requests: The cycle's planned requests per bucket.
        """
        if self._warned:
            return
        limits = {state.bucket: state.limit for state in governor.snapshot()}
        for bucket, count in requests.items():
            limit = limits[bucket]
            needs = count * _massive.RATE_WINDOW / limit if limit else 0.0
            if needs <= self._interval:
                continue
            self._warned = True
            logger.warning(
                "%s: each cycle makes %d %s requests, but they are paced at %d per minute, so a "
                "cycle takes at least %.0f s, longer than the %g s interval; cycles run late",
                self._read,
                count,
                bucket,
                limit,
                needs,
                self._interval,
            )
            return
