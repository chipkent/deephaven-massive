"""The futures session calendar of streamed rows: Massive's schedules, kept warm off the flush.

Massive's REST and flat-file futures rows carry their session (`session_end_date`, the
trading date); WebSocket futures messages carry none. The flush therefore dates each streamed
futures row by the rule the library uses everywhere (the session ends at the next 17:00
Chicago time, a Saturday's or Sunday's moving to Monday) and asks this calendar to correct it.
The rule is wrong in two places, which the schedules (`/futures/v1/schedules`, on every
futures plan) settle: a holiday folds its trading into the next session, which then opens
days early, and a product whose pre-open starts before 17:00 Chicago time (16:45 on CBOT)
already trades for the next session.

`FuturesCalendar` keeps, per session:

- the product of each contract (`/futures/v1/contracts`, looked up by `ticker.any_of`), and
- each product's sessions from a week back to ten days ahead (`/futures/v1/schedules`): one
  session per `session_end_date`, open from its first event (a pre-open or the open) to its
  last (the close).

Only a single contract ticker is looked up (`symbols.is_futures_contract`). Every other futures
ticker is a spread or combination (`GCQ6-GCJ7`, `BOB 02-01 U6`, `BZ:BF F7-G7-H7`,
`6A:XF:AUDUSD:Q6`): `/futures/v1/contracts` lists none of them, and Massive documents no grammar
that names their legs. The schedules cannot place their rows, so they are misses (below).

A futures stream's verb asks for its contracts (`watch`) and waits, bounded, until their
products and schedules are loaded (`wait_loaded`), before the stream subscribes, so its first
rows are dated by the schedules; the contracts a symbol table holds or adds wait the same way
on the session's `symbol changes` worker (`_ingest/ws.py`).

`correct` runs on the connection's flush thread and never waits: a row inside a loaded session
gets that session's date, and a row in the gap between a close and the next loaded session's
first event gets the next session's date. A row the calendar cannot place is a miss: a spread,
a contract or product still loading (a contract a `dm.ALL` stream adds, or one whose wait
ran out), a row before the first or past the last loaded session, a
contract Massive's contracts do not list, or a lookup that failed. A miss gets a null `Date`,
never the 17:00 rule's: without the schedule the rule can be wrong on any weekday (a holiday
folds its trading into the next session), and the library has no holiday calendar to rule that
out. Its `Timestamp` and every other column are kept, and the stream's `Error` is not touched:
the stream is healthy, only the session date of some rows is unknown. A miss whose lookup is
still running logs at DEBUG; any other is logged at WARNING once per product (or contract) and
rule date. A lookup that fails in any way, an answer the calendar cannot read included, logs a
WARNING (or an ERROR for a defect), makes its contracts' rows misses, and the worker carries
on. A miss past the loaded sessions asks for a reload of its product, at most once per
`RELOAD_SPACING`, and the sessions are reloaded once a day as the dates move on.

**Rate limits.** Every lookup is charged to the futures subscription's rate governor bucket,
in a lane of its own, so on a Basic subscription it waits its turn with the session's other
futures reads. Each lookup listens to a cancellation token that follows the calendar's, which
`close` sets: a lookup still waiting for the governor then stops at once.

**Threads and locks.** One daemon worker thread makes every request, through the session's
REST transport (`fetch`), and swaps in each new map whole under the calendar's leaf lock;
`correct` and `watch` only read the maps and queue work under it, so the flush thread and the
update-graph thread that reports a watchlist change never block on the network. The worker
makes no Deephaven call. `close` stops it; a request on the wire finishes first, one waiting
for the rate governor stops at once, and a lookup that fails because the session or the
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
from deephaven_massive._datasets.symbols import is_futures_contract
from deephaven_massive._enums import Subscription
from deephaven_massive._errors import (
    LIBRARY_BUG,
    ErrorSource,
    MassiveError,
    failure_text,
)
from deephaven_massive._ingest.rate_limits import bucket_of
from deephaven_massive._ingest.rest_requests import largest_page
from deephaven_massive._massive import Cancel, Transport, source

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

__all__ = [
    "RELOAD_SPACING",
    "FuturesCalendar",
    "RowFetch",
    "ScheduledSession",
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
"""The least time between two loads of one product's schedule, or two lookups of one
contract: a miss asks for a reload, and a stream of misses must not spend the session's
request budget (Basic keys allow 5 requests a minute)."""

_RELOAD_DAILY: Final = timedelta(days=1)
"""How old a product's loaded schedule may get before it is loaded again, so the sessions
ahead of today stay loaded as the days pass."""
_DAYS_BACK: Final = timedelta(days=7)
_DAYS_AHEAD: Final = timedelta(days=10)
_TICKERS_PER_REQUEST: Final = 100
"""The most tickers one contracts lookup sends in `ticker.any_of`."""
_JOIN_TIMEOUT: Final = 5.0
_STOP: Final = object()


@dataclass(frozen=True, slots=True)
class _Known:
    """What the calendar knew when one `correct` call began: a consistent snapshot.

    Attributes:
        products: The product of each contract looked up; None for a contract Massive's
            contracts do not list.
        sessions: Each loaded product's sessions, by first event.
        failed: Why each contract or product failed to load, until it loads.
        stale: The products whose schedule is a day old or older.
    """

    products: Mapping[str, str | None]
    sessions: Mapping[str, tuple[ScheduledSession, ...]]
    failed: Mapping[str, str]
    stale: frozenset[str]


@dataclass(slots=True)
class _Batch:
    """What one `correct` call gathers as it places its rows, so each is done once per call.

    Attributes:
        misses: The batch's misses, `(symbol, key, rule date, reason)`, each once; their
            messages are built and logged once per call (`FuturesCalendar._report`).
        contracts: The contracts to look up.
        products: The products whose schedules to load.
        opens: Each product's session first events, for the bisection, built once per call.
    """

    misses: dict[tuple[str, str, str, str], None] = field(default_factory=dict)
    contracts: set[str] = field(default_factory=set)
    products: set[str] = field(default_factory=set)
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


def _lookup_keys(items: list[object]) -> tuple[set[str], set[str], list[object]]:
    """Sort queued lookups into the contracts and the products whose schedules to load.

    Args:
        items: What the worker took from its queue at once, `_STOP` excepted.

    Returns:
        The contracts, the products, and every item that is not a lookup
        `FuturesCalendar._put` queues. Such an item is a defect of the library: it is set
        apart, never taken for a lookup, so the caller logs it and still loads the others.
    """
    contracts: set[str] = set()
    products: set[str] = set()
    strays: list[object] = []
    for entry in items:
        match entry:
            case ("contracts", frozenset() as keys):
                contracts.update(keys)
            case ("schedules", frozenset() as keys):
                products.update(keys)
            case _:
                strays.append(entry)
    return contracts, products, strays


class FuturesCalendar:
    """The futures session calendar of one session's WebSocket streams (see the module).

    Created by the session's `WsConnections` with its first futures connection, and closed
    with it. Thread-safe: the maps are immutable and replaced whole under one leaf lock, which
    nothing holds while it waits; one worker thread makes every request.

    Example:
        ::

            sessions = FuturesCalendar(
                rest.fetch_rows,
                contracts_path="/futures/v1/contracts",
                schedules_path="/futures/v1/schedules",
                label="MassiveSession #1",
            )
            sessions.watch(["ESZ5"])  # looks up ESZ5's product and its schedule
            sessions.wait_loaded(["ESZ5"], 5.0, stop=threading.Event())  # before it subscribes
            dates = sessions.correct(symbols, instants, rule_dates)
            sessions.close()
    """

    def __init__(
        self,
        fetch: RowFetch,
        *,
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
            contracts_path: The REST path that maps a contract ticker to its product, the
                `futures.contracts` dataset's (`/futures/v1/contracts`).
            schedules_path: The REST path of each product's timed session events, the
                `futures.schedules` dataset's (`/futures/v1/schedules`).
            label: The session's label, for the worker's name and messages.
            clock: The current time in seconds since the epoch; tests replace it.
        """
        self._fetch = fetch
        self._contracts_path = contracts_path
        self._schedules_path = schedules_path
        # Each lookup asks for the largest page its path documents.
        self._contracts_page = largest_page(source(Transport.REST, contracts_path))
        self._schedules_page = largest_page(source(Transport.REST, schedules_path))
        self._label = label
        self._clock = clock
        self._lock = threading.Lock()
        # Notified, under the lock, each time the worker has served what it took off the queue.
        self._served = threading.Condition(self._lock)
        # Guarded by the lock; each map is immutable and replaced whole.
        self._products: Mapping[str, str | None] = MappingProxyType({})
        self._sessions: Mapping[str, tuple[ScheduledSession, ...]] = MappingProxyType({})
        self._loaded: dict[str, float] = {}  # product -> when its schedule last loaded
        self._asked: dict[str, float] = {}  # contract or product -> when last queued
        self._failed: dict[str, str] = {}  # contract or product -> why its load failed
        self._reported: set[tuple[str, date]] = set()
        self._closed = False
        # Set by `close`: every lookup's token follows it, so none waits on after the close.
        self._stop = Cancel()
        self._queue: queue.SimpleQueue[object] = queue.SimpleQueue()
        self._thread = threading.Thread(
            target=self._run, name=f"{label} futures schedules", daemon=True
        )
        self._thread.start()

    # --- any thread ---------------------------------------------------------------------------

    def watch(self, symbols: Iterable[str]) -> None:
        """Ask for the products and schedules of contracts a stream will receive. Never blocks.

        Called when a futures stream opens with explicit symbols and when a reactive symbol
        table adds some, so their rows are usually placed from the first one.

        Args:
            symbols: Canonical futures symbols, contracts or spreads; spreads are not looked
                up.
        """
        self._ask_contracts({symbol for symbol in symbols if is_futures_contract(symbol)})

    def wait_loaded(self, symbols: Iterable[str], timeout: float, *, stop: threading.Event) -> bool:
        """Wait until the products and schedules of some contracts are loaded, or failed.

        A futures stream's verb calls it after `watch`, before the stream subscribes, so its
        first rows are dated by the schedules. It waits only on the calendar's worker, which
        makes no Deephaven call and never needs the update graph's lock, so it is safe on a
        console command that holds that lock; a lookup paced by the rate governor may use up
        the timeout. Never call it on a flush or update-graph thread, which must not wait.

        Args:
            symbols: Canonical futures symbols; spreads are skipped, as they are never looked
                up.
            timeout: The most seconds to wait.
            stop: Ends the wait early: set it, then call `wake()`. The waiting session's close
                does so, while the calendar, which other sessions share, stays open.

        Returns:
            True once every contract's product is known (or known to be unlisted, or failed)
            and every product's schedule is loaded (or failed), or the calendar has closed;
            False when `timeout` passed first or `stop` ended the wait.
        """
        contracts = {symbol for symbol in symbols if is_futures_contract(symbol)}

        def settled() -> bool:
            if self._closed:
                return True
            for contract in contracts:
                if contract not in self._products:
                    return False
                product = self._products[contract]
                loading = product not in self._sessions and product not in self._failed
                if product is not None and loading:
                    return False
            return True

        def ended() -> bool:
            return stop.is_set() or settled()

        with self._served:
            return self._served.wait_for(ended, timeout) and settled()

    def wake(self) -> None:
        """Wake every `wait_loaded` call, so each checks its `stop` event again. Never blocks.

        A session's close sets its stop event and then calls this, so its `symbol changes`
        worker stops waiting for schedules at once rather than after up to the full wait.
        """
        with self._served:
            self._served.notify_all()

    def correct(self, symbols: pa.Array, instants: pa.Array, dates: pa.Array) -> pa.Array:
        """Return streamed futures rows' session dates, corrected by the schedules.

        Runs on a flush thread and never waits: what is not loaded yet is asked for, and its
        rows get a null date meanwhile. Each row is placed by its product's schedule (see the
        module docstring).

        Args:
            symbols: The rows' canonical symbols, `string`.
            instants: The rows' instants (a tick's time, a bar's start), epoch nanoseconds.
            dates: The 17:00 Chicago rule's session dates, ISO text.

        Returns:
            The dates to keep, ISO text, one per row; null for a miss, a row the calendar
            cannot place, whose rule date may be wrong. A miss is logged at WARNING the first
            time it is seen per product (or contract) and rule date.
        """
        now = self._clock()
        day_old = now - _RELOAD_DAILY.total_seconds()
        with self._lock:
            known = _Known(
                self._products,
                self._sessions,
                dict(self._failed),
                frozenset(p for p, at in self._loaded.items() if at <= day_old),
            )
        if known.stale:
            self._ask_products(set(known.stale))
        names = symbols.to_pylist()
        moments = instants.to_pylist()
        ruled = dates.to_pylist()
        fixed: list[str | None] = list(ruled)
        batch = _Batch()
        for row, (symbol, moment, day) in enumerate(zip(names, moments, ruled, strict=True)):
            if symbol is None or moment is None:
                # No contract or no instant to place: the rule's date may be wrong, so none.
                fixed[row] = None
                continue
            if day is None:
                continue
            if not is_futures_contract(symbol):
                batch.misses[symbol, symbol, day, _SPREAD] = None
                fixed[row] = None
                continue
            fixed[row] = self._place(symbol, known, moment=moment, day=day, batch=batch)
        if batch.contracts:
            self._ask_contracts(batch.contracts)
        if batch.products:
            self._ask_products(batch.products)
        self._report(batch)
        return pa.array(fixed, pa.string())

    def close(self, *, wait: bool = True) -> None:
        """Stop the worker after its current request; idempotent.

        A lookup still waiting for the rate governor stops at once; one on the wire finishes.

        Args:
            wait: True to wait for the worker for at most `_JOIN_TIMEOUT` seconds; False to
                return at once, as a thread that holds the update graph's lock must. The
                worker is a daemon that never needs that lock, so it ends on its own.
        """
        with self._served:
            if self._closed:
                return
            self._closed = True
            self._served.notify_all()  # `wait_loaded` returns once closed
        self._stop.set()
        self._queue.put(_STOP)
        if wait:
            self._thread.join(_JOIN_TIMEOUT)

    # --- correction ---------------------------------------------------------------------------

    def _place(
        self,
        symbol: str,
        known: _Known,
        *,
        moment: int,
        day: str,
        batch: _Batch,
    ) -> str | None:
        """Return the session date of one contract's row, recording its miss in the batch.

        Args:
            symbol: The row's contract.
            known: What the calendar knows.
            moment: The row's instant, epoch nanoseconds.
            day: The rule's date, ISO text.
            batch: Gathers the call's misses and the lookups to ask for.

        Returns:
            The date to keep, ISO text; None for a miss.
        """
        product = known.products.get(symbol)
        sessions = None if product is None else known.sessions.get(product)
        if product is None or sessions is None:
            key, why = self._unloaded(symbol, product, known=known, batch=batch)
            batch.misses[symbol, key, day, why] = None
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
        batch.products.add(product)
        batch.misses[symbol, product, day, _outside(product, sessions, before=index < 0)] = None
        return None

    @staticmethod
    def _unloaded(
        symbol: str, product: str | None, *, known: _Known, batch: _Batch
    ) -> tuple[str, str]:
        """Ask for what a contract's row needs, and tell why the row is a miss.

        Args:
            symbol: The row's contract, whose product or product's schedule is not loaded.
            product: Its product, or None when the contract's lookup has not found one.
            known: What the calendar knows.
            batch: Gathers the lookups to ask for.

        Returns:
            The miss's key (the product, or the contract when its product is not known) and
            reason.
        """
        if symbol not in known.products:
            batch.contracts.add(symbol)
            return symbol, _LOOKING_UP
        if product is None:
            batch.contracts.add(symbol)
            return symbol, known.failed.get(symbol, "Massive's futures contracts do not list it")
        batch.products.add(product)
        return product, known.failed.get(product, _SCHEDULE_LOADING)

    def _report(self, batch: _Batch) -> None:
        """Log a batch's misses at WARNING, each the first time per key and rule date.

        One lock acquisition for the whole batch, and one message per distinct miss, however
        many rows share it.

        Args:
            batch: The call's misses: `(symbol, key, rule date, reason)`, where the key is the
                product, or the contract when its product is not known.
        """
        if not batch.misses:
            return
        logged = []
        loading = []
        with self._lock:
            for symbol, key, day, why in batch.misses:
                if why in _LOADING:  # logged at DEBUG, once per key, rule date, and reason
                    noted = (f"{key} {why}", date.fromisoformat(day))
                    if noted not in self._reported:
                        self._reported.add(noted)
                        loading.append((symbol, why))
                    continue
                reported = (key, date.fromisoformat(day))
                if reported not in self._reported:
                    self._reported.add(reported)
                    logged.append(
                        f"futures session of {symbol} not confirmed: {why}; its rows get a null "
                        f"Date, since the 17:00 Chicago rule's {day} may be wrong (a holiday or "
                        "an early pre-open moves the session); their Timestamp and every other "
                        "column are kept, and the stream keeps running"
                    )
        for message in logged:
            logger.warning("%s: %s", self._label, message)
        for symbol, why in loading:
            logger.debug(
                "%s: futures session of %s: %s; its rows get a null Date", self._label, symbol, why
            )

    # --- requests -----------------------------------------------------------------------------

    def _ask_contracts(self, contracts: set[str]) -> None:
        """Queue the product lookup of contracts without a product and not asked for lately.

        A contract Massive's contracts did not list, or whose lookup failed, is asked for
        again once `RELOAD_SPACING` has passed.

        Args:
            contracts: Contract tickers.
        """
        with self._lock:
            products = self._products
        found = {contract: product for contract, product in products.items() if product}
        wanted = self._due(contracts, found)
        if wanted:
            self._put(("contracts", frozenset(wanted)))

    def _ask_products(self, products: set[str]) -> None:
        """Queue the schedule load of products not loaded lately.

        Args:
            products: Product codes.
        """
        wanted = self._due(products, {})
        if wanted:
            self._put(("schedules", frozenset(wanted)))

    def _put(self, item: tuple[str, frozenset[str]]) -> None:
        """Queue a lookup for the worker.

        Args:
            item: The kind (`"contracts"` or `"schedules"`) and its keys.
        """
        self._queue.put(item)

    def _due(self, keys: set[str], known: Mapping[str, object]) -> set[str]:
        """Return the keys to ask for now, and mark them asked.

        Args:
            keys: Contracts or products.
            known: The keys already answered, which need no request.

        Returns:
            The keys neither answered nor asked for within `RELOAD_SPACING`.
        """
        now = self._clock()
        spacing = RELOAD_SPACING.total_seconds()
        with self._lock:
            if self._closed:
                return set()
            due = {
                key
                for key in keys
                if key not in known and now - self._asked.get(key, -spacing) >= spacing
            }
            for key in due:
                self._asked[key] = now
        return due

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
            contracts, products, strays = _lookup_keys(items)
            for stray in strays:
                logger.error(
                    "%s: futures schedules: the lookup queue holds %r, which is no lookup; the "
                    "worker skips it and loads the others. %s",
                    self._label,
                    stray,
                    LIBRARY_BUG,
                )
            try:
                if contracts:
                    products |= self._load_contracts(contracts)
                if products:
                    self._load_schedules(products)
            except Exception as error:  # a defect or a malformed answer: the worker carries on
                with self._lock:
                    closed = self._closed
                if not closed:
                    logger.exception(
                        "%s: futures schedules: a lookup failed. %s", self._label, LIBRARY_BUG
                    )
                self._failure(contracts | products, f"looking it up failed: {error!r}")
                with self._lock:
                    unknown = {c: None for c in contracts if c not in self._products}
                    self._products = MappingProxyType({**self._products, **unknown})
            with self._served:
                self._served.notify_all()

    def _load_contracts(self, contracts: set[str]) -> set[str]:
        """Look up the products of contracts and publish them.

        Args:
            contracts: Contract tickers.

        Returns:
            The products found whose schedules are not loaded yet.
        """
        found: dict[str, str | None] = dict.fromkeys(contracts)
        ordered = sorted(contracts)
        try:
            with Cancel(self._stop) as lane:  # the lookup's own lane in the futures bucket
                for start in range(0, len(ordered), _TICKERS_PER_REQUEST):
                    chunk = ordered[start : start + _TICKERS_PER_REQUEST]
                    query = {"ticker.any_of": tuple(chunk), "limit": self._contracts_page}
                    for row in self._fetch(
                        self._contracts_path, query, bucket=_BUCKET, cancel=lane
                    ):
                        ticker, product = row.get("ticker"), row.get("product_code")
                        if isinstance(ticker, str) and isinstance(product, str) and ticker in found:
                            found[ticker] = found[ticker] or product
        except MassiveError as error:
            self._failure(contracts, f"looking up its product failed: {error}", error)
            with self._lock:
                unknown = {c: None for c in contracts if c not in self._products}
                self._products = MappingProxyType({**self._products, **unknown})
            return set()
        with self._lock:
            self._products = MappingProxyType({**self._products, **found})
            for contract in contracts:
                self._failed.pop(contract, None)
            loaded = set(self._sessions)
        return {product for product in found.values() if product is not None} - loaded

    def _load_schedules(self, products: set[str]) -> None:
        """Load the sessions of products around today and publish them.

        Args:
            products: Product codes.
        """
        today = futures_session(datetime.fromtimestamp(self._clock(), UTC))
        query = {
            "product_code.any_of": tuple(sorted(products)),
            "session_end_date.gte": today - _DAYS_BACK,
            "session_end_date.lte": today + _DAYS_AHEAD,
            "limit": self._schedules_page,
        }
        try:
            with Cancel(self._stop) as lane:  # the lookup's own lane in the futures bucket
                rows = self._fetch(self._schedules_path, query, bucket=_BUCKET, cancel=lane)
        except MassiveError as error:
            self._failure(products, f"loading its schedule failed: {error}", error)
            return
        events: dict[str, dict[date, list[int]]] = {product: {} for product in products}
        for row in rows:
            product, day, stamp = (
                row.get(k) for k in ("product_code", "session_end_date", "timestamp")
            )
            if product in events and isinstance(day, str) and isinstance(stamp, str):
                events[product].setdefault(date.fromisoformat(day), []).append(
                    epoch_ns(datetime.fromisoformat(stamp))
                )
        loaded = {
            product: tuple(
                ScheduledSession(day, min(stamps), max(stamps))
                for day, stamps in sorted(found.items())
            )
            for product, found in events.items()
        }
        now = self._clock()
        with self._lock:
            self._sessions = MappingProxyType({**self._sessions, **loaded})
            for product in loaded:
                # A schedule just loaded counts as asked for: a miss reloads it only after
                # `RELOAD_SPACING`, however its load was asked for.
                self._loaded[product] = self._asked[product] = now
                self._failed.pop(product, None)

    def _failure(self, keys: set[str], why: str, error: MassiveError | None = None) -> None:
        """Record that loading some contracts or products failed, and log it once.

        Their rows are misses, with a null `Date`; they are asked for again after
        `RELOAD_SPACING`. Nothing is logged once the calendar is closed: a lookup cut short by
        the session closing is no failure. The streams keep running, so an expected failure
        (a refusal, a network failure) logs a WARNING; a defect, such as an answer the library
        cannot read, logs an ERROR saying whose it is.

        Args:
            keys: The contracts or products.
            why: The failure, for the log and the misses.
            error: The `MassiveError` that failed the lookup, whose `is_bug` sets the level; None
                for any other failure, which the caller has logged with its trace.
        """
        with self._lock:
            closed = self._closed
            for key in keys:
                self._failed[key] = why
        if not closed:
            logger.log(
                logging.ERROR if error is not None and error.is_bug else logging.WARNING,
                "%s: futures schedules: %s",
                self._label,
                failure_text(
                    MassiveError(
                        f"{why} ({', '.join(sorted(keys))})",
                        source=ErrorSource.MASSIVE if error is None else error.source,
                        is_bug=error is not None and error.is_bug,
                    )
                ),
                exc_info=error if error is not None and error.is_bug else None,
            )


_LOOKING_UP: Final = "its product is still being looked up"
_SCHEDULE_LOADING: Final = "its schedule is still loading"
_LOADING: Final = frozenset({_LOOKING_UP, _SCHEDULE_LOADING})
"""The reasons of a miss whose lookup is still running: logged at DEBUG only. A stream's verb
waits for its contracts' schedules (`wait_loaded`), so these are the rows of contracts a
reactive table or a `dm.ALL` stream adds, until their lookup ends; a lookup that fails logs a
WARNING of its own (`_failure`)."""

_SPREAD: Final = "a spread or combination is never looked up in Massive's schedules"
"""The reason of a miss whose symbol is not a single contract (`symbols.is_futures_contract`)."""


def _outside(product: str, sessions: tuple[ScheduledSession, ...], *, before: bool) -> str:
    """Return why a row outside a product's loaded sessions is a miss.

    The text names the loaded schedule's first or last session, not the row's instant, so the
    rows of one product and rule date share one miss, however many instants they hold.

    Args:
        product: The product.
        sessions: Its loaded sessions, oldest first; possibly none.
        before: True for a row before the first session, False for one after the last.

    Returns:
        The reason, such as `its loaded schedule (ES) ends with the session of 2024-04-01`.
    """
    if not sessions:
        return f"its loaded schedule ({product}) holds no session"
    if before:
        return f"its loaded schedule ({product}) begins with the session of {sessions[0].day}"
    return f"its loaded schedule ({product}) ends with the session of {sessions[-1].day}"
