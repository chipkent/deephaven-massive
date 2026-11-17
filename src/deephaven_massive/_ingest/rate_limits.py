"""Massive's REST rate limits on the library side: shared governors, warnings, and diagnostics.

Massive limits REST requests per subscription (stocks, options, indices, currencies, futures),
at a limit its plans set, and every API key of an account shares the counters. Reference and
cross-asset requests are counted in a bucket of their own.
Massive's knowledge base states these rules in the article "What is the request limit for
Massive's RESTful APIs?", which the refresh vendors, and whose changes its drift check reports:
`vendor/massive/knowledge-base/what-is-the-request-limit-for-massives-restful-apis.md`.
The wire package's `RequestGovernor` enforces them for the requests it admits; this module
decides which requests share one, how long each read waits, and what the user is told:

- **Sharing.** `GOVERNORS`, the process's `GovernorRegistry`, keeps one governor per (API key,
  REST origin). Each session takes a `GovernorLease` at construction and releases it when it
  closes or is collected, so the sessions of a process that use one key share its counters.
  The registry keys entries by the key's SHA-256 digest, never the key itself. The limits a
  governor learned live and die with it: a governor created again for the same key and origin
  starts unpaced, and learns again from Massive's refusals. The governor paces every
  subscription that any live session declares a limit for
  (`MassiveConfig.rest_requests_per_minute`: the lowest limit declared wins), and logs one
  WARNING when two sessions declare different non-empty mappings. An entry outlives its last
  lease until its
  governor is idle (no slot held, no pause active), so a notebook cell that makes a new session
  does not start with an empty window.
- **Buckets.** A request is charged to its dataset's subscription (`bucket_of`); reference
  data and the other datasets Massive assigns to no subscription go to `OTHER`, which learns
  its limit from Massive's refusals, like a subscription, but cannot be declared.
- **Patience.** Each read decides how long it waits out pauses (`read_patience`), by who waits
  for it (`Waiter`). A read its caller waits for waits out `READ_PATIENCE` (10 minutes of
  pauses). A read that feeds a live table (a background `fetch()` or `history()`, a `poll()`
  cycle, a watchlist's reads after the call, a stream's lookups) waits out
  `MassiveConfig.max_outage`, the time any live table may stay cut off from Massive, and its
  give-up error says so (`LIVE_TABLE_ADVICE`). Any read whose thread holds the Deephaven update
  graph's lock, as a console command does, waits out only `CONSOLE_PATIENCE` (90 s), since
  every table of the process stops updating meanwhile; a `fetch()` or `history()` call's
  give-up error then advises leaving out `wait=True` (`console_advice`), and no other read's
  does, since no other call takes `wait=True`. A read that gives up fails alone; the bucket
  keeps pausing and probing for every other read.
- **Refusal notices.** `RateLimitNotices` is the REST client's refusal hook: the first 429 of
  each subscription in a session logs one INFO line that says what happened and what to do.
  Requests pause and resume by themselves, so the refusal alone is nothing to act on; a read
  it makes give up raises, and a `poll()` it makes late warns (`Poller`).
- **Diagnostics.** `rate_limit_rows` turns the governor's snapshot into the rows of
  `m.rate_limits()`; the session's diagnostics writer pulls them whenever the governor
  changes. Each pause also reaches the session's `m.problems()` as it starts, lengthens, and
  ends: the lease hands the governor's pause notices to the session (`GovernorLease`).
- **Notices.** `announce` logs, at INFO, how many requests a long static read makes and the
  least time pacing makes them take, and `PaceCheck` warns once when a `poll()` cycle needs
  more requests than its interval allows.

Locks: the registry has one leaf lock. It calls into a governor (`declare`, `idle`,
`subscribe_pauses`) while holding it, so the order is the registry's lock, then the governor's,
then whatever a governor listener takes (the diagnostics queue's leaf lock), which a pause
listener takes under the governor's lock; the diagnostics never call the governor while
holding it. Neither the registry's lock nor the
governor's is ever taken under `SessionCore.registry_lock`; the diagnostics queue's lock is, by
every diagnostics write, which is safe because that lock is a leaf on both paths. None of them
is held across a Deephaven call, a request, or a wait.
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
from typing import TYPE_CHECKING, Final, assert_never

from deephaven_massive import _massive
from deephaven_massive._enums import Subscription
from deephaven_massive._tables.diagnostics import LimitSource, RateLimitRow, RateLimitState
from deephaven_massive._tables.execution_context import holds_update_graph_lock

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from deephaven.execution_context import ExecutionContext

    from deephaven_massive._massive import Patience, PauseNotice, RequestGovernor, WireError

__all__ = [
    "BUCKETS",
    "CONSOLE_PATIENCE",
    "GOVERNORS",
    "LABELS",
    "LEARN_ONLY",
    "LIVE_TABLE_ADVICE",
    "OTHER",
    "READ_PATIENCE",
    "GovernorLease",
    "GovernorRegistry",
    "PaceCheck",
    "RateLimitNotices",
    "Waiter",
    "announce",
    "bucket_of",
    "console_advice",
    "rate_limit_pull",
    "rate_limit_rows",
    "read_patience",
]

logger = logging.getLogger(__name__)

OTHER: Final = "other"
"""The bucket of reference data and the other datasets Massive assigns to no subscription
(cross-market reference data, economy, partner data, the unified snapshot), which Massive
counts in a bucket of its own; it is paced once it learns a limit from Massive's refusals."""

BUCKETS: Final = (*(subscription.value for subscription in Subscription), OTHER)
"""Every bucket of a governor: one per subscription, then `OTHER`."""

LEARN_ONLY: Final = frozenset({OTHER})
"""The buckets whose limit cannot be declared, since no setting names them; they learn a limit
from Massive's refusals as a subscription does."""

READ_PATIENCE: Final = 600.0
"""Seconds a read its caller waits for waits out its bucket's pauses before it fails with status
429."""

LIVE_TABLE_ADVICE: Final = "raise MassiveConfig.max_outage to wait longer"
"""The remedy the give-up error of a read that feeds a live table ends with (`Patience.advice`)."""

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


class Waiter(StrEnum):
    """Who waits for a read, which decides how long it waits out pauses (`read_patience`).

    Attributes:
        CALL: The caller of `fetch()` or `history()` with `wait=True`, which gets the rows.
        STREAM_OPEN: The caller of a stream verb, which waits for the stream's WebSocket host
            decision. A stream verb takes no `wait=True`, so its give-up error advises
            nothing about it.
        LIVE_TABLE: No caller: the read feeds a live table, as a background `fetch()` or
            `history()`, a `poll()` cycle, a watchlist's reads after the call, and a stream's
            lookups do.
    """

    CALL = "call"
    STREAM_OPEN = "stream_open"
    LIVE_TABLE = "live_table"


def console_advice(max_outage: float) -> str:
    """Return the remedy a console read's give-up error ends with (`Patience.advice`).

    It advises leaving out `wait=True`, so `fetch()` or `history()` reads in the background,
    which waits out `MassiveConfig.max_outage` of pauses, only when that is longer than the
    console's `CONSOLE_PATIENCE`; otherwise reading in the background would wait no longer.

    Args:
        max_outage: The session's `MassiveConfig.max_outage`, in seconds.

    Returns:
        The remedy, or empty when there is none to give.
    """
    if max_outage <= CONSOLE_PATIENCE:
        return ""
    return (
        f"this read ran inside a Deephaven console command, which waits out only "
        f"{CONSOLE_PATIENCE:.0f} s of pauses; leave out wait=True to let fetch() or history() "
        f"read in the background, which waits out MassiveConfig.max_outage ({max_outage:g} s)"
    )


def read_patience(exec_ctx: ExecutionContext, *, max_outage: float, waiter: Waiter) -> Patience:
    """Return the patience of a read starting now: how long it waits out its buckets' pauses.

    A read whose thread holds the Deephaven update graph's lock, as a console command does,
    waits out `CONSOLE_PATIENCE` (90 s), since no table of the process updates meanwhile; a
    `fetch()` or `history()` call's give-up error then advises leaving out `wait=True`
    (`console_advice`). Otherwise a read its caller waits for waits out `READ_PATIENCE` (10
    minutes), and a read that feeds a live table waits out `MassiveConfig.max_outage`, the time
    any live table may stay cut off from Massive, so with zero it fails at its first pause. Only
    pauses count; waiting for a paced slot never does. Call it on the read's own thread, before
    any fan-out and outside every library lock, since it asks Deephaven (a Java call); pass the
    result to every request of the read. It logs nothing: the line that starts the read (a
    `fetch()` call's, or a `poll()` cycle's) names its seconds.

    Args:
        exec_ctx: The session's execution context, whose update graph is asked.
        max_outage: The session's `MassiveConfig.max_outage`, in seconds.
        waiter: Who waits for the read.

    Returns:
        A new `Patience` for the read.
    """
    console = holds_update_graph_lock(exec_ctx)
    match waiter:
        case Waiter.CALL:
            if console:
                return _massive.Patience(CONSOLE_PATIENCE, advice=console_advice(max_outage))
            return _massive.Patience(READ_PATIENCE)
        case Waiter.STREAM_OPEN:
            return _massive.Patience(CONSOLE_PATIENCE if console else READ_PATIENCE)
        case Waiter.LIVE_TABLE:
            if console:
                return _massive.Patience(CONSOLE_PATIENCE)
            return _massive.Patience(max_outage, advice=LIVE_TABLE_ADVICE)
        case _:
            assert_never(waiter)


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
        limits: The requests per minute it declares for each subscription.
    """

    label: str
    limits: Mapping[Subscription, int]


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
        """Declare every subscription's limit that any holder declares (the lowest wins)."""
        self.governor.declare(
            {subscription.value: limit for subscription, limit in _lowest(self.holders).items()}
        )


class GovernorLease:
    """One session's hold on its shared governor, from `GovernorRegistry.lease`.

    The session registers `release` as a closer, so the hold ends when the session closes or
    is collected unclosed. The lease refers to the registry, never to the session. Thread-safe.
    """

    __slots__ = ("_holder", "_key", "_listener", "_on_pause", "_registry", "governor")

    def __init__(
        self,
        registry: GovernorRegistry,
        *,
        key: tuple[str, tuple[str, str, int]],
        holder: int,
        governor: RequestGovernor,
        listener: Callable[[], None],
        on_pause: Callable[[PauseNotice], None],
    ) -> None:
        """Bind the lease; the registry has already recorded the hold.

        Args:
            registry: The registry that holds the entry.
            key: The entry's key.
            holder: The hold's number in the entry.
            governor: The shared governor.
            listener: The session's governor listener, removed by `release`.
            on_pause: The session's pause listener, removed by `release`.
        """
        self._registry = registry
        self._key = key
        self._holder = holder
        self._listener = listener
        self._on_pause = on_pause
        self.governor: Final = governor

    def release(self) -> None:
        """End the hold: stop the session's listeners and drop its declarations. Idempotent.

        The subscriptions only this session declared become unpaced again, unless they learned
        a limit. The entry stays while its governor still holds slots or a pause.
        """
        self.governor.unsubscribe(self._listener)
        self.governor.unsubscribe_pauses(self._on_pause)
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
                limits={Subscription.STOCKS: 5},
                label="MassiveSession #1",
                listener=lambda: None,
                on_pause=lambda notice: None,
            )
            two = registry.lease(
                api_key="k",
                base_url="https://api.massive.com",
                limits={},
                label="MassiveSession #2",
                listener=lambda: None,
                on_pause=lambda notice: None,
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
        limits: Mapping[Subscription, int],
        label: str,
        listener: Callable[[], None],
        on_pause: Callable[[PauseNotice], None],
    ) -> GovernorLease:
        """Take a hold on the governor of an API key and REST origin, creating it if needed.

        Idle entries without a lease are dropped first, and with them what their governors
        learned; a new governor starts unpaced. The governor then paces each subscription any
        holder declares, at the lowest limit declared for it. When this session and another
        holder each declare a non-empty mapping and the mappings differ, one WARNING names both
        sessions and the limits paced.

        Args:
            api_key: The session's API key; only its SHA-256 digest is kept.
            base_url: The session's REST base URL; its scheme, host, and port are kept
                (`origin_of`, which the REST client also checks it with).
            limits: The requests per minute the session declares for each subscription.
            label: The session's label, for the WARNING.
            listener: Called after every change of the governor (`RequestGovernor.subscribe`);
                it must only enqueue.
            on_pause: Told of every pause of the governor, from those open now
                (`RequestGovernor.subscribe_pauses`); it must only enqueue.

        Returns:
            The lease, whose `governor` the session's REST client uses.
        """
        digest = hashlib.sha256(api_key.encode()).hexdigest()
        key = (digest, _massive.origin_of(base_url))
        mine = dict(limits)
        with self._lock:
            self._prune()
            entry = self._entries.get(key)
            if entry is None:
                entry = self._entries[key] = _Entry(self._create())
            conflict = next(
                (h for h in entry.holders.values() if h.limits and mine and h.limits != mine),
                None,
            )
            number = next(self._holders)
            entry.holders[number] = _Holder(label, mine)
            entry.declare()
            entry.governor.subscribe(listener)
            entry.governor.subscribe_pauses(on_pause)
            paced = _lowest(entry.holders)
        if conflict is not None:
            logger.warning(
                "%s declares rest_requests_per_minute %s, but %s, which uses the same API key, "
                "declares %s; Massive counts their requests together, so both pace %s, the "
                "lowest limit declared for each subscription",
                label,
                _names(mine),
                conflict.label,
                _names(conflict.limits),
                _names(paced),
            )
        return GovernorLease(
            self,
            key=key,
            holder=number,
            governor=entry.governor,
            listener=listener,
            on_pause=on_pause,
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


def _lowest(holders: Mapping[int, _Holder]) -> dict[Subscription, int]:
    """Return each subscription any holder declares, with the lowest limit declared for it.

    Args:
        holders: The holds on one governor.

    Returns:
        The requests per minute to pace each declared subscription at.
    """
    lowest: dict[Subscription, int] = {}
    for holder in holders.values():
        for subscription, limit in holder.limits.items():
            lowest[subscription] = min(limit, lowest.get(subscription, limit))
    return lowest


def _names(limits: Mapping[Subscription, int]) -> str:
    """Spell declared limits for a message.

    Args:
        limits: The requests per minute of each subscription.

    Returns:
        Each subscription's value and limit, sorted, in braces, such as
        `"{options: 5, stocks: 10}"`.
    """
    return "{" + ", ".join(f"{s.value}: {n}" for s, n in sorted(limits.items())) + "}"


GOVERNORS: Final = GovernorRegistry()
"""The process's registry of governors, used by every session."""


class RateLimitNotices:
    """One session's INFO line at the first HTTP 429 of each bucket: the REST client's hook.

    Thread-safe: a leaf lock guards the buckets already reported, and the line is logged after
    it is released.
    """

    __slots__ = ("_declared", "_label", "_lock", "_warned")

    def __init__(self, label: str, limits: Mapping[Subscription, int]) -> None:
        """Start with nothing reported.

        Args:
            label: The session's label.
            limits: The requests per minute the session declares for each subscription.
        """
        self._label = label
        self._declared = {subscription.value: limit for subscription, limit in limits.items()}
        self._lock = threading.Lock()
        self._warned: set[str] = set()

    def refused(self, bucket: str, error: WireError, *, per_minute: bool) -> None:
        """Log the session's INFO line for a bucket's first 429; later ones log nothing here.

        The line says what the refusal means: a subscription the session declared a limit for
        is refused because other sessions, processes, or keys of the account share its limit;
        a refusal that names the per-minute maximum says Massive limits the requests per
        minute, so once the pause ends they are paced at the number Massive answered in the
        refusal's minute, if any (a subscription's limit could be declared; `OTHER`'s cannot);
        a declared subscription's refusal means its limit is shared or declared too high; any other
        refusal pauses the requests, and one that keeps coming is a matter for Massive's
        support. The governor's own INFO line gives the number once it is learned.

        Args:
            bucket: The refused request's bucket.
            error: The refusal.
            per_minute: Whether its message named the per-minute maximum.
        """
        with self._lock:
            if bucket in self._warned:
                return
            self._warned.add(bucket)
        what = f"{'an' if bucket[0] in 'aeiou' else 'a'} {bucket} request"
        if bucket == OTHER:
            what = "a request outside Massive's subscriptions"
        learns = (
            "once the pause ends, they are paced at the number of them Massive answered of "
            "those sent in the refusal's clock minute, if it answered any (the next log line "
            "and m.rate_limits() give it)"
        )
        if bucket in self._declared:
            declared = self._declared[bucket]
            advice = (
                f"its requests are already paced at the {declared} per minute declared, so "
                "either other sessions, processes, or API keys of the account share the limit, "
                f"or {declared} is above Massive's limit; requests pause and resume"
            )
        elif per_minute and bucket == OTHER:
            advice = (
                "that is Massive's limit on reference and other data outside the "
                f"subscriptions, which it counts on their own; {learns}"
            )
        elif per_minute:
            advice = (
                f"Massive limits these requests per minute; {learns}; declare "
                f"MassiveConfig(rest_requests_per_minute={{dm.Subscription.{bucket.upper()}: "
                "<limit>}) to pace them from the first request, with the limit Massive gives "
                "you"
            )
        else:
            advice = (
                "requests pause and resume; if this persists, send the request id to Massive's "
                "support"
            )
        logger.info(
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
            if state.declared is not None
            else LimitSource.LEARNED
            if state.learned is not None
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

    a paced bucket whose requests cannot all start within a minute gets the least time they
    take at its limit (`RequestGovernor.estimate`), such as "stocks.bars history: 250
    requests; paced at 5 requests a minute (stocks); at that pace they take at least 49 min".
    It is a lower bound: pages beyond the first, slow responses, and other sessions' requests
    only add time. An unpaced bucket gets no line: its limit, if any, is unknown until Massive
    refuses one of its requests.

    A read made on a thread that holds the update graph's lock (`pauses_live_tables`), as every
    Deephaven console command does, holds every live table of the server still while it runs,
    so its paced line is a WARNING instead, which says so and names the remedy: "stocks.bars
    history: 250 requests; paced at 5 requests a minute (stocks); this command will pause every
    live table on the server for at least 49 min: leave out wait=True to read in the
    background".

    Args:
        governor: The session's governor.
        read: What is read, such as `"stocks.bars history"`.
        buckets: The bucket of each planned request, one entry per request.
        shape: How the read counts its requests, which the line says; the default,
            `ReadShape.PAGED`, says nothing of it.
        pauses_live_tables: True when the read runs on a thread that holds the update graph's lock.
    """
    limits = {state.bucket: state.limit for state in governor.snapshot()}
    for bucket, count in Counter(buckets).items():
        seconds = governor.estimate(bucket, count)
        limit = limits[bucket]
        if seconds is None or limit is None or seconds < _MINUTE:
            continue
        # A paged read says nothing; every other shape must say how it counts its requests.
        words = None if shape is ReadShape.PAGED else _SHAPE_WORDS[shape]
        what = f"{count} requests" if words is None else f"{count} requests, {words}"
        if pauses_live_tables:
            logger.warning(
                "%s: %s; paced at %d requests a minute (%s); this command will pause every "
                "live table on the server for at least %d min: leave out wait=True to read "
                "in the background",
                read,
                what,
                limit,
                bucket,
                math.floor(seconds / _MINUTE),
            )
        else:
            logger.info(
                "%s: %s; paced at %d requests a minute (%s); at that pace they take at "
                "least %d min",
                read,
                what,
                limit,
                bucket,
                math.floor(seconds / _MINUTE),
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
