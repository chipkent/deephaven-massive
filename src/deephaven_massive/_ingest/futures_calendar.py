"""The futures session calendar of streamed rows: Massive's schedules, loaded as streams need them.

Massive's REST and flat-file futures rows carry their session (`session_end_date`, the
trading date); WebSocket futures messages carry none. The flush therefore dates each streamed
futures row by the rule the library uses everywhere (the session ends at the next 17:00
Chicago time, a Saturday's or Sunday's moving to Monday) and asks this calendar to correct it.
The rule is wrong in two places, which the schedules (`/futures/v1/schedules`) settle: a
holiday folds its trading into the next session, which then opens days early, and a product
whose pre-open starts before 17:00 Chicago time (16:45 on CBOT) already trades for the next
session.

`FuturesCalendar` keeps, per session:

- the product of each ticker a stream names or receives, single contract or combination alike,
  as Massive states it: the snapshot's `details.product_code` (`/futures/v1/snapshot`, by
  `ticker.any_of`), and for a ticker the snapshot lacks, such as one listed today, the
  contracts list's `product_code` (`/futures/v1/contracts`, by `ticker.any_of` over the week up
  to today's session, so each ticker answers a row a day, not its whole history). The product
  is never derived from the ticker: Massive's tickers do not spell their product in one way.
  Each request names at most `TICKERS_PER_REQUEST` tickers or products.
- each product's sessions from a week back to ten days ahead (`/futures/v1/schedules`): one
  session per `session_end_date`, open from its first event (a pre-open or the open) to its
  last (the close).

Nothing is loaded for a ticker no stream has named or received.

**Lookup states.** Each ticker is in one state, which `states` reports to the flush:

- *Waiting*: a lookup it needs is queued, on the wire, failing transiently
  (`MassiveError.is_retryable`: the network, a timeout, a 5xx, a 429), or answered without it
  (in neither list). A transient failure or a missing ticker is asked again after
  `RETRY_DELAYS`, then every `RETRY_EVERY`; the ask happens when a flush asks for the state,
  so only tickers whose rows wait are asked again.
- *Unlisted*: neither list has held the ticker for `UNLISTED_AFTER` since Massive first
  answered without it. The flush discards its rows (`dm.ErrorPolicy.unlisted_contracts`); it is
  asked again every `RELOAD_SPACING`, and once a list holds it, its later rows are dated.
- *Refused*: Massive refused a lookup it needs twice, the second time `RETRY_DELAYS[0]` after
  the first: the contracts list refused the key or its plan (`AuthenticationError`,
  `EntitlementError`), or the schedules refused in any way that is not retryable. The streams
  whose rows wait on it fail with Massive's error. It is asked again after `RELOAD_SPACING`.
  The contracts list refusing several tickers in another way is asked again for each half, so
  one ticker it refuses costs the others nothing; that ticker waits as one neither list holds,
  with Massive's text. The snapshot refusing anything refuses nothing: its tickers go to the
  contracts list, and after it refuses the key or its plan twice, the calendar uses only the
  contracts list for the rest of its life.
- *Unscheduled*: Massive's schedules hold no session of its product at all: the flush discards
  its rows (`dm.ErrorPolicy.unscheduled_products`).
- *Ready*: its product's schedule is loaded. A schedule that loaded once stays ready while its
  daily reload is asked for or fails.

`correct` runs on the connection's flush thread and never waits: a row inside a loaded session
gets that session's date, and a row in the gap between a close and the next loaded session's
first event gets the next session's date. Any other row gets a null `Date`, never the rule's,
which can be wrong on any weekday (a holiday folds its trading into the next session), and the
flush holds it (`_tables/held_rows.py`): a row still waiting, or a ready product's row no loaded
session holds, before the first or past the last. Such a row asks for a reload of its product,
at most once per `RETRY_EVERY`; `unplaced` says which sessions are nearest it once the flush
withholds it. A product whose schedule holds no session in the window is asked once more with
no window; Massive answering none at all makes it *unscheduled*, and its rows are withheld.
The sessions are reloaded once a day as the dates move on, unscheduled products' too.

**Rate limits.** Every lookup is charged to the futures subscription's rate governor bucket, in
a lane of its own, so it waits its turn with the session's other futures reads. Each lookup
listens to a cancellation token that follows the calendar's, which `close` sets: a lookup still
waiting for the governor then stops at once.

**Threads and locks.** One daemon worker thread makes every request, through the session's REST
transport (`fetch`), and publishes each round's answers under the calendar's leaf lock;
`correct`, `states`, and `watch` only read the state and queue work under it, so the flush
thread and the update-graph thread that reports a watchlist change never block on the network.
The worker makes no Deephaven call. `close` stops it; a request on the wire finishes first, one
waiting for the rate governor stops at once, and a lookup that fails because the session or the
calendar closed under it is not logged.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, Protocol

import pyarrow as pa

from deephaven_massive._datasets.epoch import epoch_ns
from deephaven_massive._datasets.futures_sessions import futures_session
from deephaven_massive._enums import Subscription
from deephaven_massive._errors import (
    LIBRARY_BUG,
    AuthenticationError,
    EntitlementError,
    MassiveError,
    failure_text,
)
from deephaven_massive._ingest.rate_limits import bucket_of
from deephaven_massive._ingest.rest_requests import largest_page
from deephaven_massive._massive import Cancel, Transport, source
from deephaven_massive._tables.futures_dates import UNLISTED_AFTER, LookupState, Readiness

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

__all__ = [
    "RELOAD_SPACING",
    "RETRY_DELAYS",
    "RETRY_EVERY",
    "TICKERS_PER_REQUEST",
    "FuturesCalendar",
    "RowFetch",
    "ScheduledSession",
    "contracts_query",
]

logger = logging.getLogger(__name__)

_BUCKET: Final = bucket_of(Subscription.FUTURES)
"""The rate governor bucket every lookup is charged to: the futures subscription's."""


class RowFetch(Protocol):
    """Reads every raw row of a REST path for a query, following its pages.

    The session's REST transport implements it (`RestEndpoints.fetch_rows`), and the
    WebSocket connection group passes it on to the calendar.
    """

    def __call__(
        self, template: str, query: Mapping[str, object], *, bucket: str, cancel: Cancel
    ) -> list[dict[str, object]]:
        """Return the rows of every page.

        Args:
            template: The path template; it takes no path parameter.
            query: The query parameters, by wire name.
            bucket: The rate governor bucket the requests are charged to.
            cancel: Ends the requests' waits for the rate governor when set; also their lane.

        Returns:
            The rows of every page, every key kept.

        A failed or cancelled read raises its public `MassiveError`.
        """
        ...


RELOAD_SPACING: Final = timedelta(minutes=10)
"""The least time between two lookups of a ticker once it is unlisted or refused: Massive
updates its lists daily, so asking more often spends the session's request budget for
nothing."""

RETRY_DELAYS: Final = (2.0, 8.0, 30.0)
"""Seconds before a lookup that failed transiently, or answered without the ticker, is asked
again: after its first, second, and third such answer; then every `RETRY_EVERY`."""

RETRY_EVERY: Final = 60.0
"""Seconds between later asks of a lookup that keeps failing or keeps lacking the ticker."""

TICKERS_PER_REQUEST: Final = 100
"""The most tickers, or products, one lookup request names in its `any_of` filter."""

_REFUSALS: Final = 2
"""The refusals in a row after which a lookup is refused."""
_RELOAD_DAILY: Final = timedelta(days=1)
"""How old a product's loaded schedule may get before it is loaded again, so the sessions
ahead of today stay loaded as the days pass."""
_DAYS_BACK: Final = timedelta(days=7)
_DAYS_AHEAD: Final = timedelta(days=10)
_LISTED_DAYS: Final = timedelta(days=7)
"""The days before today's session whose contracts-list rows a ticker's lookup reads: one row
per ticker and day, so a contract is found whichever of those days Massive's list has reached."""
_JOIN_TIMEOUT: Final = 5.0
_STOP: Final = object()


@dataclass(slots=True)
class _Lookup:
    """One ticker's or product's lookup, while it has no answer. Guarded by the calendar's lock.

    Attributes:
        queued: Whether it is queued or on the wire.
        due: When it may be asked again, seconds since the epoch.
        attempts: Its failures, or answers without the ticker, so far.
        failing_since: When it began failing transiently; None when its last answer was none.
        error: The last failure's text, for the reason.
        refusals: Its refusals in a row.
        refused: Massive's refusal, once it has refused `_REFUSALS` times.
        unlisted_since: When Massive's lists first answered without the ticker.
    """

    queued: bool = True
    due: float = 0.0
    attempts: int = 0
    failing_since: float | None = None
    error: str = ""
    refusals: int = 0
    refused: MassiveError | None = None
    unlisted_since: float | None = None


@dataclass(frozen=True, slots=True)
class _Known:
    """What the calendar knew when one `correct` call began: a consistent snapshot.

    Attributes:
        products: The product of each ticker found.
        sessions: Each loaded product's sessions, by first event.
        stale: The products whose schedule is a day old or older.
        unscheduled: The products Massive's schedules hold no session of at all.
    """

    products: Mapping[str, str]
    sessions: Mapping[str, tuple[ScheduledSession, ...]]
    stale: frozenset[str]
    unscheduled: frozenset[str]


@dataclass(slots=True)
class _Batch:
    """What one `correct` call gathers as it places its rows, so each is done once per call.

    Attributes:
        tickers: The tickers without a product.
        products: The products whose schedules are not loaded.
        reloads: The loaded products whose schedules to load again.
        opens: Each product's session first events, for the bisection, built once per call.
    """

    tickers: set[str] = field(default_factory=set)
    products: set[str] = field(default_factory=set)
    reloads: set[str] = field(default_factory=set)
    opens: dict[str, list[int]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ScheduledSession:
    """One futures session of one product, from Massive's schedule.

    Attributes:
        day: The session's `session_end_date`, its trading date.
        opens: Its first event (a pre-open or the open), epoch nanoseconds.
        closes: Its last event (the close), epoch nanoseconds.
    """

    day: date
    opens: int
    closes: int


@dataclass(slots=True)
class _Answers:
    """What one round of the worker learned, published at once under the lock.

    Attributes:
        products: The product of each ticker found.
        sessions: The sessions of each product loaded.
        unscheduled: The products Massive's schedules hold no session of at all.
        unlisted: The tickers Massive's lists answered without, each with Massive's refusal of
            it alone, or empty when the lists simply lack it.
        failed: The failed keys, by kind (`"tickers"` or `"products"`) and key, with the error.
        logged: Whether the round's failures were logged already: a defect, with its trace.
        started: When the round's requests began, seconds since the epoch: the time a schedule
            it loads counts as loaded at, so a row held while it ran waits for the next load.
    """

    products: dict[str, str] = field(default_factory=dict)
    sessions: dict[str, tuple[ScheduledSession, ...]] = field(default_factory=dict)
    unlisted: dict[str, str] = field(default_factory=dict)
    unscheduled: set[str] = field(default_factory=set)
    failed: dict[tuple[str, str], MassiveError] = field(default_factory=dict)
    logged: bool = False
    started: float = 0.0


def _lookup_keys(items: list[object]) -> tuple[set[str], set[str], list[object]]:
    """Sort queued lookups into the tickers and the products whose schedules to load.

    Args:
        items: What the worker took from its queue at once, `_STOP` excepted.

    Returns:
        The tickers, the products, and every item that is not a lookup
        `FuturesCalendar._put_lookups` queues. Such an item is a defect of the library: it is
        set apart, never taken for a lookup, so the caller logs it and still loads the others.
    """
    tickers: set[str] = set()
    products: set[str] = set()
    strays: list[object] = []
    for entry in items:
        match entry:
            case ("tickers", frozenset() as keys):
                tickers.update(keys)
            case ("products", frozenset() as keys):
                products.update(keys)
            case _:
                strays.append(entry)
    return tickers, products, strays


def contracts_query(tickers: tuple[str, ...], today: date, *, page: int) -> dict[str, object]:
    """Return the query of one contracts-list lookup of tickers' products.

    It reads the week up to today's session, so each ticker answers a row a day, not every day
    of its history, which made a lookup of 100 tickers take seconds or time out.

    Args:
        tickers: At most `TICKERS_PER_REQUEST` tickers.
        today: Today's futures session.
        page: The page size: the path's largest.

    Returns:
        The query, by wire name.
    """
    return {
        "ticker.any_of": tickers,
        "date.gte": today - _LISTED_DAYS,
        "date.lte": today,
        "limit": page,
    }


def _chunks(keys: Iterable[str]) -> list[tuple[str, ...]]:
    """Split keys, sorted, into the `any_of` lists of one request each.

    Args:
        keys: Tickers or products.

    Returns:
        Lists of at most `TICKERS_PER_REQUEST` keys.
    """
    ordered = sorted(keys)
    return [
        tuple(ordered[start : start + TICKERS_PER_REQUEST])
        for start in range(0, len(ordered), TICKERS_PER_REQUEST)
    ]


def _retry_delay(attempts: int) -> float:
    """Return the seconds before a lookup's next ask, after its answers so far.

    Args:
        attempts: Its failures, or answers without the ticker, so far; at least 1.

    Returns:
        `RETRY_DELAYS` for the first answers, then `RETRY_EVERY`.
    """
    return RETRY_DELAYS[attempts - 1] if attempts <= len(RETRY_DELAYS) else RETRY_EVERY


class FuturesCalendar:
    """The futures session calendar of one session's WebSocket streams (see the module).

    Created by the session's `WsConnections` with its first futures connection, and closed
    with it. Thread-safe: the maps are immutable and replaced whole, and the lookups' state
    changes, under one leaf lock, which nothing holds while it waits; one worker thread makes
    every request.

    Example:
        ::

            sessions = FuturesCalendar(
                rest.fetch_rows,
                snapshot_path="/futures/v1/snapshot",
                contracts_path="/futures/v1/contracts",
                schedules_path="/futures/v1/schedules",
                label="MassiveSession #1",
            )
            sessions.watch(["ESZ5"])  # looks up ESZ5's product and its schedule
            states = sessions.states(["ESZ5"])  # READY once its schedule is in
            dates = sessions.correct(symbols, instants, rule_dates)
            sessions.close()
    """

    def __init__(
        self,
        fetch: RowFetch,
        *,
        snapshot_path: str,
        contracts_path: str,
        schedules_path: str,
        label: str,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Create the calendar and start its worker; no request is made yet.

        Args:
            fetch: Returns every row of a REST path for a query, following its pages, on the
                session's REST client; it raises `MassiveError` on a failure. Each lookup
                passes the futures bucket and a token that the calendar's close sets.
            snapshot_path: The REST path that names each ticker's product first, the
                `futures.snapshots` dataset's (`/futures/v1/snapshot`).
            contracts_path: The REST path that names the product of a ticker the snapshot
                lacks, the `futures.contracts` dataset's (`/futures/v1/contracts`).
            schedules_path: The REST path of each product's timed session events, the
                `futures.schedules` dataset's (`/futures/v1/schedules`).
            label: The session's label, for the worker's name and messages.
            clock: The current time in seconds since the epoch; tests replace it.
        """
        self._fetch = fetch
        self._snapshot_path = snapshot_path
        self._contracts_path = contracts_path
        self._schedules_path = schedules_path
        # Each lookup asks for the largest page its path documents.
        self._snapshot_page = largest_page(source(Transport.REST, snapshot_path))
        self._contracts_page = largest_page(source(Transport.REST, contracts_path))
        self._schedules_page = largest_page(source(Transport.REST, schedules_path))
        self._label = label
        self._clock = clock
        self._lock = threading.Lock()
        # Guarded by the lock; each map is immutable and replaced whole.
        self._products: Mapping[str, str] = MappingProxyType({})
        self._sessions: Mapping[str, tuple[ScheduledSession, ...]] = MappingProxyType({})
        # Guarded by the lock: each product's last load, the products with no session at all,
        # each product's last reload for a row no session holds, and the lookups without an
        # answer.
        self._loaded: dict[str, float] = {}
        self._unscheduled: frozenset[str] = frozenset()
        self._reloads: dict[str, float] = {}
        self._tickers: dict[str, _Lookup] = {}
        self._schedules: dict[str, _Lookup] = {}
        self._version = 0
        self._closed = False
        # Only the worker reads or writes it.
        self._snapshot_refusals = 0
        # Set by `close`: every lookup's token follows it, so none waits on after the close.
        self._stop = Cancel()
        self._queue: queue.SimpleQueue[object] = queue.SimpleQueue()
        self._thread = threading.Thread(
            target=self._run, name=f"{label} futures schedules", daemon=True
        )
        self._thread.start()

    # --- any thread ---------------------------------------------------------------------------

    @property
    def version(self) -> int:
        """A count that grows each time the worker publishes a round's answers.

        The flush reads the states of its waiting tickers again when it changed, or when time
        alone may have changed one (a retry due, an unlisted ticker's discard).
        """
        with self._lock:
            return self._version

    def alive(self) -> bool:
        """Return whether the worker still serves lookups, or the calendar was closed.

        Returns:
            False only when the worker ended without `close`: a defect, after which no
            waiting ticker would ever be answered.
        """
        with self._lock:
            closed = self._closed
        return closed or self._thread.is_alive()

    def watch(self, symbols: Iterable[str]) -> None:
        """Ask for the products and schedules of tickers a stream will receive. Never blocks.

        Called when a futures stream opens with explicit symbols and when a reactive symbol
        table adds some, so their lookups start before their first rows arrive.

        Args:
            symbols: Canonical futures tickers, single contracts or combinations.
        """
        self.states(symbols)

    def states(self, symbols: Iterable[str]) -> dict[str, LookupState]:
        """Return each ticker's lookup state, asking for what it needs. Never blocks.

        Runs on a flush thread for the tickers whose rows wait: a lookup not asked yet, or
        whose next ask is due, is queued here.

        Args:
            symbols: Canonical futures tickers.

        Returns:
            The state of each, by ticker.
        """
        now = self._clock()
        found: dict[str, LookupState] = {}
        tickers: set[str] = set()
        products: set[str] = set()
        with self._lock:
            closed = self._closed
            for symbol in symbols:
                found[symbol] = self._state(symbol, now, tickers=tickers, products=products)
        if not closed:
            self._put_lookups(tickers, products)
        return found

    def correct(self, symbols: pa.Array, instants: pa.Array, dates: pa.Array) -> pa.Array:
        """Return streamed futures rows' session dates, corrected by the schedules.

        Runs on a flush thread and never waits: what is not loaded yet is asked for, and its
        rows get a null date meanwhile, which the flush holds (`states`). Each row is placed by
        its product's schedule (see the module docstring).

        Args:
            symbols: The rows' canonical symbols, `string`.
            instants: The rows' instants (a tick's time, a bar's start), epoch nanoseconds.
            dates: The 17:00 Chicago rule's session dates, ISO text.

        Returns:
            The dates to keep, ISO text, one per row; null for a row whose product's schedule
            is not loaded, and for a miss, which is logged at WARNING the first time it is
            seen per product and rule date.
        """
        now = self._clock()
        day_old = now - _RELOAD_DAILY.total_seconds()
        with self._lock:
            known = _Known(
                self._products,
                self._sessions,
                frozenset(p for p, at in self._loaded.items() if at <= day_old),
                self._unscheduled,
            )
        names = symbols.to_pylist()
        moments = instants.to_pylist()
        ruled = dates.to_pylist()
        fixed: list[str | None] = list(ruled)
        batch = _Batch()
        for row, (symbol, moment, day) in enumerate(zip(names, moments, ruled, strict=True)):
            if symbol is None or moment is None:
                # No ticker or no instant to place: the rule's date may be wrong, so none.
                fixed[row] = None
                continue
            if day is None:
                continue
            fixed[row] = self._place(symbol, known, moment=moment, batch=batch)
        self._ask(batch, stale=known.stale)
        return pa.array(fixed, pa.string())

    def close(self, *, wait: bool = True) -> None:
        """Stop the worker after its current request; idempotent.

        A lookup still waiting for the rate governor stops at once; one on the wire finishes.

        Args:
            wait: True to wait for the worker for at most `_JOIN_TIMEOUT` seconds; False to
                return at once, as a thread that holds the update graph's lock must. The
                worker is a daemon that never needs that lock, so it ends on its own.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._stop.set()
        self._queue.put(_STOP)
        if wait:
            self._thread.join(_JOIN_TIMEOUT)

    # --- states, under the lock ---------------------------------------------------------------

    def _state(
        self, symbol: str, now: float, *, tickers: set[str], products: set[str]
    ) -> LookupState:
        """Return one ticker's state, adding the lookups it needs now. Under the lock.

        Args:
            symbol: The ticker.
            now: The clock's time.
            tickers: Receives the tickers to look up.
            products: Receives the products whose schedules to load.

        Returns:
            The ticker's state.
        """
        product = self._products.get(symbol)
        if product is None:
            return _ticker_state(self._due(self._tickers, symbol, now=now, asks=tickers), now)
        if product in self._unscheduled:
            return LookupState(
                Readiness.UNSCHEDULED,
                f"Massive's futures schedules hold no session of its product {product}",
            )
        if product in self._sessions:
            # A reload still failing or refused concerns the rows the loaded sessions lack.
            reload = self._schedules.get(product)
            return LookupState(
                Readiness.READY,
                reload.error if reload is not None else "",
                failing_since=None if reload is None else reload.failing_since,
                error=None if reload is None else reload.refused,
                loaded_at=self._loaded[product],
            )
        lookup = self._due(self._schedules, product, now=now, asks=products)
        if lookup.refused is not None:
            return LookupState(
                Readiness.REFUSED,
                f"Massive refused the schedule of its product {product}",
                error=lookup.refused,
            )
        if lookup.failing_since is not None:
            return LookupState(
                Readiness.WAITING,
                f"loading the schedule of its product {product} failed: {lookup.error}",
                failing_since=lookup.failing_since,
            )
        return LookupState(Readiness.WAITING, f"the schedule of its product {product} is loading")

    @staticmethod
    def _due(lookups: dict[str, _Lookup], key: str, *, now: float, asks: set[str]) -> _Lookup:
        """Return a key's lookup: started now, asked again when its next ask is due, or as is.

        Under the lock.

        Args:
            lookups: The ticker or product lookups.
            key: The ticker or product.
            now: The clock's time.
            asks: Receives the key when it is to be queued.

        Returns:
            Its lookup.
        """
        lookup = lookups.get(key)
        if lookup is None:
            lookup = lookups[key] = _Lookup()
            asks.add(key)
        elif not lookup.queued and now >= lookup.due:
            lookup.queued = True
            asks.add(key)
        return lookup

    # --- correction ---------------------------------------------------------------------------

    def _place(
        self,
        symbol: str,
        known: _Known,
        *,
        moment: int,
        batch: _Batch,
    ) -> str | None:
        """Return the session date of one ticker's row, recording its miss in the batch.

        Args:
            symbol: The row's ticker.
            known: What the calendar knows.
            moment: The row's instant, epoch nanoseconds.
            batch: Gathers the lookups and reloads to ask for.

        Returns:
            The date to keep, ISO text; None when the row cannot be dated now.
        """
        product = known.products.get(symbol)
        if product is None:
            batch.tickers.add(symbol)
            return None
        sessions = known.sessions.get(product)
        if sessions is None:
            batch.products.add(product)
            return None
        if product in known.unscheduled:  # its rows are withheld, never dated
            return None
        opens = batch.opens.get(product)
        if opens is None:
            opens = batch.opens[product] = [s.opens for s in sessions]
        index = bisect_right(opens, moment) - 1
        if index >= 0 and moment < sessions[index].closes:
            return sessions[index].day.isoformat()
        if 0 <= index < len(sessions) - 1:
            # Between a close and the next session's first event: the next session's.
            return sessions[index + 1].day.isoformat()
        batch.reloads.add(product)  # no loaded session holds the row: Massive's may have changed
        return None

    def unplaced(self, symbol: str, instant: int) -> str:
        """Say why no loaded session of a ready ticker's product holds one of its rows.

        Args:
            symbol: The ticker.
            instant: The row's instant, epoch nanoseconds.

        Returns:
            The product, the row's instant, and the loaded sessions nearest it, such as
            `no session of its product ES holds 2024-04-01T22:00:00Z; the nearest are those of
            2024-04-01 (before) and none after`.
        """
        with self._lock:
            product = self._products.get(symbol)
            sessions = () if product is None else self._sessions.get(product, ())
        at = datetime.fromtimestamp(instant / 1e9, UTC).isoformat().replace("+00:00", "Z")
        index = bisect_right([s.opens for s in sessions], instant) - 1
        before = sessions[index].day.isoformat() if index >= 0 else "none"
        after = sessions[index + 1].day.isoformat() if index + 1 < len(sessions) else "none"
        return (
            f"no session of its product {product} holds {at}; the nearest Massive's schedule "
            f"gives are those of {before} (before) and {after} (after)"
        )

    # --- requests -----------------------------------------------------------------------------

    def _ask(self, batch: _Batch, *, stale: frozenset[str]) -> None:
        """Queue what one `correct` call needs: lookups, and reloads of loaded products.

        A day-old schedule is loaded again once a day; a row no loaded session holds asks for
        its product's reload at most once per `RETRY_EVERY`. Either keeps the loaded sessions
        until the new ones replace them.

        Args:
            batch: The call's tickers and products without an answer, and the products of its
                rows no loaded session holds.
            stale: The products whose schedule is a day old or older.
        """
        now = self._clock()
        spacing = RETRY_EVERY
        tickers: set[str] = set()
        products: set[str] = set()
        with self._lock:
            if self._closed:
                return
            # Without what the worker published since the call's snapshot.
            for ticker in batch.tickers - self._products.keys():
                self._due(self._tickers, ticker, now=now, asks=tickers)
            for product in batch.products - self._sessions.keys():
                self._due(self._schedules, product, now=now, asks=products)
            # A row no session holds asks for a reload once per spacing; a reload that failed
            # follows its own retries, as any lookup does.
            failing = {p for p in batch.reloads if p in self._schedules}
            reloads = {
                p
                for p in batch.reloads - failing
                if now - self._reloads.get(p, -spacing) >= spacing
            }
            for product in reloads:
                self._reloads[product] = now
            for product in reloads | failing | stale:
                self._due(self._schedules, product, now=now, asks=products)
        self._put_lookups(tickers, products)

    def _put_lookups(self, tickers: set[str], products: set[str]) -> None:
        """Queue the lookups of tickers and products for the worker.

        Args:
            tickers: Tickers whose products to look up.
            products: Products whose schedules to load.
        """
        if tickers:
            self._queue.put(("tickers", frozenset(tickers)))
        if products:
            self._queue.put(("products", frozenset(products)))

    def _run(self) -> None:
        """Serve queued lookups until `close`, coalescing what is queued at once."""
        while True:
            item = self._queue.get()
            items = [item]
            while True:
                try:
                    items.append(self._queue.get_nowait())
                except queue.Empty:
                    break
            if any(entry is _STOP for entry in items):
                return
            tickers, products, strays = _lookup_keys(items)
            for stray in strays:
                logger.error(
                    "%s: futures schedules: the lookup queue holds %r, which is no lookup; the "
                    "worker skips it and loads the others. %s",
                    self._label,
                    stray,
                    LIBRARY_BUG,
                )
            if tickers:
                found = self._round("tickers", tickers, self._load_tickers)
                self._publish(tickers, set(), found)
                products |= self._unloaded(set(found.products.values()))
            if products:
                self._publish(
                    set(), products, self._round("products", products, self._load_schedules)
                )

    def _round(
        self, kind: str, keys: set[str], load: Callable[[set[str], _Answers], None]
    ) -> _Answers:
        """Run one kind of lookup for some keys, and gather its answers.

        A failure that is no `MassiveError` is a defect: it is logged at ERROR with its trace,
        unless the calendar closed under it, and every key the lookup has no answer for fails
        with it, to be asked again.

        Args:
            kind: `"tickers"` or `"products"`.
            keys: The tickers or products.
            load: The lookup: `_load_tickers` or `_load_schedules`.

        Returns:
            What it learned.
        """
        answers = _Answers(started=self._clock())
        try:
            load(keys, answers)
        except Exception as error:  # a defect or a malformed answer: the worker carries on
            with self._lock:
                closed = self._closed
            if not closed:  # a lookup cut short by the close is no failure
                logger.exception(
                    "%s: futures schedules: looking up %s failed. %s",
                    self._label,
                    ", ".join(sorted(keys)),
                    LIBRARY_BUG,
                )
            defect = MassiveError(f"{error!r}", is_bug=True)
            answers.logged = True
            answered = {*answers.products, *answers.unlisted, *answers.sessions}
            for key in keys - answered:
                answers.failed.setdefault((kind, key), defect)
        return answers

    def _unloaded(self, found: set[str]) -> set[str]:
        """Return the products just found whose schedules no other lookup loads, marking them.

        Args:
            found: The products of the tickers a round found.

        Returns:
            Those not loaded, not queued, and due: this round loads them. A product whose
            load failed waits for its retry, as any lookup does.
        """
        now = self._clock()
        wanted: set[str] = set()
        with self._lock:
            for product in found - self._sessions.keys():
                self._due(self._schedules, product, now=now, asks=wanted)
        return wanted

    def _load_tickers(self, tickers: set[str], answers: _Answers) -> None:
        """Look up tickers' products: the snapshot first, then the contracts list.

        Args:
            tickers: The tickers.
            answers: Receives the products found, the tickers neither list holds, and the
                failures, by ticker.
        """
        missing = set(tickers)
        if self._snapshot_refusals < _REFUSALS:
            for chunk in _chunks(tickers):
                self._from_snapshot(chunk, answers)
            missing.difference_update(answers.products)
        if missing:
            today = futures_session(datetime.fromtimestamp(self._clock(), UTC))
            for chunk in _chunks(missing):
                self._from_contracts(chunk, today, answers)

    def _from_snapshot(self, chunk: tuple[str, ...], answers: _Answers) -> None:
        """Look up one request's tickers in the snapshot.

        A ticker it does not answer goes to the contracts list, whatever the reason: the
        snapshot lacks it, it failed, or Massive refused it. A refusal of the key or its plan
        counts toward giving the snapshot up (`_snapshot_refused`).

        Args:
            chunk: The tickers.
            answers: Receives the products found.
        """
        query = {"ticker.any_of": chunk, "limit": self._snapshot_page}
        try:
            rows = self._read(self._snapshot_path, query)
        except MassiveError as error:
            if isinstance(error, (AuthenticationError, EntitlementError)):
                self._snapshot_refused(error)
            else:
                logger.debug(
                    "%s: futures schedules: the snapshot failed for %d tickers (%s); asking the "
                    "contracts list",
                    self._label,
                    len(chunk),
                    failure_text(error),
                )
            return
        for row in rows:
            details = row.get("details")
            if isinstance(details, dict):
                _record(details.get("ticker"), details.get("product_code"), answers)

    def _from_contracts(self, chunk: tuple[str, ...], today: date, answers: _Answers) -> None:
        """Look up one request's tickers in the contracts list, over the week up to today.

        A refusal of several tickers that is not about the key or its plan asks each half
        again, so one ticker Massive refuses costs the others nothing; a refusal of one ticker
        is Massive's answer about it, which leaves it without a product like a ticker the list
        lacks, with Massive's text as the reason.

        Args:
            chunk: The tickers.
            today: Today's futures session.
            answers: Receives the products found, the tickers the list lacks, and the failures.
        """
        query = contracts_query(chunk, today, page=self._contracts_page)
        try:
            rows = self._read(self._contracts_path, query)
        except MassiveError as error:
            transient = error.is_retryable or error.is_bug
            if transient or isinstance(error, (AuthenticationError, EntitlementError)):
                answers.failed.update({("tickers", t): error for t in chunk})
            elif len(chunk) == 1:
                answers.unlisted[chunk[0]] = f"Massive refused its lookup: {failure_text(error)}"
            else:
                half = len(chunk) // 2
                self._from_contracts(chunk[:half], today, answers)
                self._from_contracts(chunk[half:], today, answers)
            return
        for row in rows:
            _record(row.get("ticker"), row.get("product_code"), answers)
        for ticker in chunk:
            if ticker not in answers.products:
                answers.unlisted[ticker] = ""

    def _snapshot_refused(self, error: MassiveError) -> None:
        """Count a refusal of the snapshot; after the second, use only the contracts list.

        Args:
            error: Massive's refusal.
        """
        self._snapshot_refusals += 1
        if self._snapshot_refusals == _REFUSALS:
            logger.warning(
                "%s: futures schedules: Massive refused the futures snapshot twice (%s); "
                "futures products are looked up in the contracts list only",
                self._label,
                failure_text(error),
            )

    def _load_schedules(self, products: set[str], answers: _Answers) -> None:
        """Load the sessions of products around today.

        A product whose schedule holds no session in the window loads with none: its rows are
        misses.

        Args:
            products: Product codes.
            answers: Receives the sessions loaded and the failures, by product.
        """
        today = futures_session(datetime.fromtimestamp(self._clock(), UTC))
        for chunk in _chunks(products):
            query = {
                "product_code.any_of": chunk,
                "session_end_date.gte": today - _DAYS_BACK,
                "session_end_date.lte": today + _DAYS_AHEAD,
                "limit": self._schedules_page,
            }
            try:
                rows = self._read(self._schedules_path, query)
            except MassiveError as error:
                answers.failed.update({("products", p): error for p in chunk})
                continue
            events: dict[str, dict[date, list[int]]] = {product: {} for product in chunk}
            for row in rows:
                product, day, stamp = (
                    row.get(k) for k in ("product_code", "session_end_date", "timestamp")
                )
                if product in events and isinstance(day, str) and isinstance(stamp, str):
                    events[product].setdefault(date.fromisoformat(day), []).append(
                        epoch_ns(datetime.fromisoformat(stamp))
                    )
            for product, found in events.items():
                answers.sessions[product] = tuple(
                    ScheduledSession(day, min(stamps), max(stamps))
                    for day, stamps in sorted(found.items())
                )
                if not found:
                    self._confirm_unscheduled(product, answers)

    def _confirm_unscheduled(self, product: str, answers: _Answers) -> None:
        """Ask Massive whether a product with no session in the window has any session at all.

        One request for the product's latest session, whatever its date. An empty answer is
        Massive's word that it has none, and its rows are withheld (`Readiness.UNSCHEDULED`);
        a session outside the window leaves its rows misses. A failure fails the product's
        load, to be asked again.

        Args:
            product: The product.
            answers: Receives the product as unscheduled, or its failure.
        """
        query = {"product_code": product, "sort": "session_end_date.desc", "limit": 1}
        try:
            rows = self._read(self._schedules_path, query)
        except MassiveError as error:
            answers.failed[("products", product)] = error
            del answers.sessions[product]
            return
        if not rows:
            answers.unscheduled.add(product)

    def _read(self, path: str, query: Mapping[str, object]) -> list[dict[str, object]]:
        """Read every row of one lookup request, in a lane of its own.

        Args:
            path: The REST path.
            query: The query.

        Returns:
            The rows.

        A failed request raises its `MassiveError`.
        """
        with Cancel(self._stop) as lane:
            return self._fetch(path, query, bucket=_BUCKET, cancel=lane)

    def _publish(self, tickers: set[str], products: set[str], answers: _Answers) -> None:
        """Publish one round's answers, and set each unanswered lookup's next ask.

        Args:
            tickers: The tickers the round looked up.
            products: The products whose schedules it loaded.
            answers: What it learned.
        """
        now = self._clock()
        with self._lock:
            closed = self._closed
            self._products = MappingProxyType({**self._products, **answers.products})
            self._sessions = MappingProxyType({**self._sessions, **answers.sessions})
            self._unscheduled = (self._unscheduled - answers.sessions.keys()) | answers.unscheduled
            for product in answers.sessions:
                self._loaded[product] = answers.started
                self._schedules.pop(product, None)
            for ticker in answers.products:
                self._tickers.pop(ticker, None)
            for ticker, refusal in answers.unlisted.items():
                _unlisted(self._tickers.setdefault(ticker, _Lookup()), refusal, now)
            logged = self._failures(answers.failed, now)
            for lookups, keys in ((self._tickers, tickers), (self._schedules, products)):
                for key in keys & lookups.keys():
                    lookups[key].queued = False
            self._version += 1
        if not closed and not answers.logged:
            self._log_failures(logged)

    def _failures(
        self, failed: Mapping[tuple[str, str], MassiveError], now: float
    ) -> list[tuple[str, MassiveError]]:
        """Record a round's failed lookups. Under the lock.

        Args:
            failed: The failed keys, by kind and key, with their errors.
            now: The clock's time.

        Returns:
            The failures new to the log.
        """
        logged = []
        for (kind, key), error in failed.items():
            lookups = self._tickers if kind == "tickers" else self._schedules
            if _failed(lookups.setdefault(key, _Lookup()), error, now):
                logged.append((key, error))
        return logged

    def _log_failures(self, failures: list[tuple[str, MassiveError]]) -> None:
        """Log a round's new failures, one line per error text.

        A transient failure or a refusal logs a WARNING, a defect of the library an ERROR. A
        failure is new the first time its lookup fails, or when a refusal settles.

        Args:
            failures: The failed tickers or products, with their errors.
        """
        by_text: dict[str, tuple[MassiveError, list[str]]] = {}
        for key, error in failures:
            by_text.setdefault(failure_text(error), (error, []))[1].append(key)
        for text, (error, keys) in by_text.items():
            logger.log(
                logging.ERROR if error.is_bug else logging.WARNING,
                "%s: futures schedules: looking up %s failed: %s; their rows wait for it",
                self._label,
                ", ".join(sorted(keys)),
                text,
                exc_info=error if error.is_bug else None,
            )


def _record(ticker: object, product: object, answers: _Answers) -> None:
    """Record a ticker's product from one row of Massive's answer.

    Args:
        ticker: The row's ticker.
        product: The row's product code.
        answers: Receives the product, unless the ticker has one already.
    """
    if isinstance(ticker, str) and isinstance(product, str) and product:
        answers.products.setdefault(ticker, product)


def _ticker_state(lookup: _Lookup, now: float) -> LookupState:
    """Return the state of a ticker without a product.

    Args:
        lookup: Its lookup.
        now: The clock's time.

    Returns:
        Refused, unlisted, or waiting.
    """
    if lookup.refused is not None:
        return LookupState(
            Readiness.REFUSED, "Massive refused the lookup of its product", error=lookup.refused
        )
    if lookup.failing_since is not None:  # an outage now outranks an earlier answer
        return LookupState(
            Readiness.WAITING,
            f"looking up its product failed: {lookup.error}",
            failing_since=lookup.failing_since,
        )
    if lookup.unlisted_since is not None:
        missing = now - lookup.unlisted_since
        why = lookup.error or ("neither Massive's futures snapshot nor its contracts list holds it")
        why = f"{why} (asked over {missing / 60:.0f} min)"
        if missing >= UNLISTED_AFTER.total_seconds():
            return LookupState(Readiness.UNLISTED, why)
        return LookupState(Readiness.WAITING, why)
    return LookupState(Readiness.WAITING, "its product is being looked up")


def _unlisted(lookup: _Lookup, refusal: str, now: float) -> None:
    """Record that Massive's lists answered without a ticker, and set its next ask.

    Args:
        lookup: The ticker's lookup.
        refusal: Massive's refusal of the ticker alone, or empty when the lists lack it.
        now: The clock's time.
    """
    if lookup.unlisted_since is None:
        lookup.unlisted_since = now
    lookup.failing_since = None
    lookup.error = refusal
    lookup.refusals = 0
    lookup.refused = None  # Massive answered: an older refusal no longer holds
    lookup.attempts += 1
    discarded = now - lookup.unlisted_since >= UNLISTED_AFTER.total_seconds()
    lookup.due = now + (
        RELOAD_SPACING.total_seconds() if discarded else _retry_delay(lookup.attempts)
    )


def _failed(lookup: _Lookup, error: MassiveError, now: float) -> bool:
    """Record one failed answer of a lookup and set its next ask. Under the calendar's lock.

    Args:
        lookup: The lookup.
        error: Its failure.
        now: The clock's time.

    Returns:
        Whether the failure is new to the log: its first in a run, or the refusal that settles.
    """
    lookup.error = failure_text(error)
    if error.is_retryable or error.is_bug:
        first = lookup.failing_since is None
        if first:
            lookup.failing_since = now
        lookup.refusals = 0
        lookup.refused = None  # the latest answer is an outage, not a refusal
        lookup.attempts += 1
        lookup.due = now + _retry_delay(lookup.attempts)
        return first
    lookup.refusals += 1
    if lookup.refusals < _REFUSALS:
        lookup.due = now + RETRY_DELAYS[0]
        return False
    lookup.refused = error
    lookup.failing_since = None
    lookup.due = now + RELOAD_SPACING.total_seconds()
    return True
