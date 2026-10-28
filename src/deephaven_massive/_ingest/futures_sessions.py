"""The futures session calendar of streamed rows: Massive's schedules, kept warm off the flush.

Massive's REST and flat-file futures rows carry their session (`session_end_date`, the
trading date); WebSocket futures messages carry none. The flush therefore dates each streamed
futures row by the rule the library uses everywhere (the session ends at the next 17:00
Chicago time, a Saturday's or Sunday's moving to Monday) and asks this calendar to correct it.
The rule is wrong in two places, which the schedules (`/futures/v1/schedules`, on every
futures plan) settle: a holiday folds its trading into the next session, which then opens
days early, and a product whose pre-open starts before 17:00 Chicago time (16:45 on CBOT)
already trades for the next session.

`FuturesSessions` keeps, per session:

- the product of each contract (`/futures/v1/contracts`, looked up by `ticker.any_of`; a spread
  such as `GCQ6-GCJ7` takes its first leg's product), and
- each product's sessions from a week back to ten days ahead (`/futures/v1/schedules`): one
  session per `session_end_date`, open from its first event (a pre-open or the open) to its
  last (the close).

`correct` runs on the connection's flush thread and never waits: a row inside the session the
rule gave it is kept, a row the schedules place in another session moves there, and a row in the
gap between a close and the next session's first event keeps the rule's date. What the calendar
does not know yet (a contract or product being looked up) keeps the rule's date silently. A row
the loaded schedules cannot place (past the last loaded session, a contract Massive's contracts
do not list, or a lookup that failed) is a miss: it keeps the rule's date, the miss is logged
at ERROR once per product (or contract) and session date, and the flush shows it in the `Error`
of every stream that receives the contract. A miss past the loaded sessions asks for a
reload of its product, at most once per `RELOAD_SPACING`, and the sessions are reloaded once a
day as the dates move on.

**Threads and locks.** One daemon worker thread makes every request, through the session's
REST transport (`fetch`), and swaps in each new map whole under the calendar's leaf lock;
`correct` and `watch` only read the maps and queue work under it, so the flush thread and the
update-graph thread that reports a watchlist change never block on the network. The worker
makes no Deephaven call. `close` stops it; a request on the wire finishes first.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from bisect import bisect_right
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

import pyarrow as pa

from deephaven_massive._datasets.dates import futures_session
from deephaven_massive._errors import MassiveError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

__all__ = [
    "CONTRACTS_PATH",
    "RELOAD_SPACING",
    "SCHEDULES_PATH",
    "FuturesSessions",
    "Session",
]

logger = logging.getLogger(__name__)

CONTRACTS_PATH: Final = "/futures/v1/contracts"
"""The REST path that maps a contract ticker to its product."""
SCHEDULES_PATH: Final = "/futures/v1/schedules"
"""The REST path of each product's timed session events."""

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
_PAGE: Final = 1000
"""The largest page both paths serve (their documented `limit` maximum)."""
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
    sessions: Mapping[str, tuple[Session, ...]]
    failed: Mapping[str, str]
    stale: frozenset[str]


@dataclass(frozen=True, slots=True)
class Session:
    """One futures session of one product, from Massive's schedule.

    Attributes:
        day: The session's `session_end_date`, its trading date.
        opens: Its first event (a pre-open or the open), epoch nanoseconds.
        closes: Its last event (the close), epoch nanoseconds.
    """

    day: date
    opens: int
    closes: int


def _ns(text: str) -> int:
    """Return an ISO-8601 instant of a schedule as epoch nanoseconds.

    Args:
        text: Such as `2024-06-09T21:00:00+00:00`.

    Returns:
        The instant, to the microsecond.
    """
    moment = datetime.fromisoformat(text)
    return (moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1) * 1000


def _leg(symbol: str) -> str:
    """Return the contract whose product names a symbol: a spread's first leg, else itself.

    Args:
        symbol: A contract (`ESZ5`) or a spread (`GCQ6-GCJ7`).

    Returns:
        The contract.
    """
    return symbol.partition("-")[0]


class FuturesSessions:
    """The futures session calendar of one session's WebSocket streams (see the module).

    Created by the session's `WsConnections` with its first futures connection, and closed
    with it. Thread-safe: the maps are immutable and replaced whole under one leaf lock, which
    nothing holds while it waits; one worker thread makes every request.

    Example:
        ::

            sessions = FuturesSessions(rest.fetch_rows, label="MassiveSession #1")
            sessions.watch(["ESZ5"])  # looks up ESZ5's product and its schedule
            dates, misses = sessions.correct(symbols, instants, rule_dates)
            sessions.close()
    """

    def __init__(
        self,
        fetch: Callable[[str, Mapping[str, object]], list[dict[str, object]]],
        *,
        label: str,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Create the calendar and start its worker; no request is made yet.

        Args:
            fetch: Returns every row of a REST path for a query, following its pages, on the
                session's REST client; it raises `MassiveError` on a failure.
            label: The session's label, for the worker's name and messages.
            clock: The current time in seconds since the epoch; tests replace it.
        """
        self._fetch = fetch
        self._label = label
        self._clock = clock
        self._lock = threading.Lock()
        # Guarded by the lock; each map is immutable and replaced whole.
        self._products: Mapping[str, str | None] = MappingProxyType({})
        self._sessions: Mapping[str, tuple[Session, ...]] = MappingProxyType({})
        self._loaded: dict[str, float] = {}  # product -> when its schedule last loaded
        self._asked: dict[str, float] = {}  # contract or product -> when last queued
        self._failed: dict[str, str] = {}  # contract or product -> why its load failed
        self._reported: set[tuple[str, date]] = set()
        self._closed = False
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
            symbols: Canonical futures symbols, contracts or spreads.
        """
        self._ask_contracts({_leg(symbol) for symbol in symbols})

    def correct(
        self, symbols: pa.Array, instants: pa.Array, dates: pa.Array
    ) -> tuple[pa.Array, list[tuple[str, str]]]:
        """Return streamed futures rows' session dates, corrected by the schedules.

        Runs on a flush thread and never waits: what is not loaded yet is asked for and keeps
        the rule's date. Each row whose instant lies inside the session the rule gave it keeps
        it; the others are placed by their product's schedule.

        Args:
            symbols: The rows' canonical symbols, `string`.
            instants: The rows' instants (a tick's time, a bar's start), epoch nanoseconds.
            dates: The 17:00 Chicago rule's session dates, ISO text.

        Returns:
            The dates to keep, ISO text, one per row; and each miss reported for the first
            time, as `(symbol, message)`, which is logged at ERROR here.
        """
        day_old = self._clock() - _RELOAD_DAILY.total_seconds()
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
        misses: list[tuple[str, str]] = []
        unknown: set[str] = set()
        for row, (symbol, moment, day) in enumerate(zip(names, moments, ruled, strict=True)):
            if symbol is None or moment is None or day is None:
                continue
            contract = _leg(symbol)
            if contract not in known.products:
                unknown.add(contract)
                continue
            fixed[row] = self._place(symbol, known, moment=moment, day=day, misses=misses)
        if unknown:
            self._ask_contracts(unknown)
        return pa.array(fixed, pa.string()), misses

    def close(self) -> None:
        """Stop the worker after its current request and wait a bounded time; idempotent."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._queue.put(_STOP)
        self._thread.join(_JOIN_TIMEOUT)

    # --- correction ---------------------------------------------------------------------------

    def _place(
        self,
        symbol: str,
        known: _Known,
        *,
        moment: int,
        day: str,
        misses: list[tuple[str, str]],
    ) -> str:
        """Return the session date of one row, recording a miss the first time it is seen.

        Args:
            symbol: The row's symbol, whose contract `known` has looked up.
            known: What the calendar knows.
            moment: The row's instant, epoch nanoseconds.
            day: The rule's date, ISO text.
            misses: Receives the misses reported for the first time.

        Returns:
            The date to keep, ISO text.
        """
        ruled = date.fromisoformat(day)
        contract = _leg(symbol)
        product = known.products[contract]
        if product is None:
            self._ask_contracts({contract})
            why = known.failed.get(contract, "Massive's futures contracts do not list it")
            self._miss(symbol, contract, ruled=ruled, why=why, misses=misses)
            return day
        sessions = known.sessions.get(product)
        if sessions is None:
            self._ask_products({product})
            reason = known.failed.get(product)
            if reason is not None:
                self._miss(symbol, product, ruled=ruled, why=reason, misses=misses)
            return day
        index = bisect_right([s.opens for s in sessions], moment) - 1
        if index >= 0 and moment < sessions[index].closes:
            return sessions[index].day.isoformat()
        if 0 <= index < len(sessions) - 1:
            return day  # between a close and the next session's first event: no session
        self._ask_products({product})
        where = "before" if index < 0 else "after"
        why = f"its schedule ({product}) holds no session {where} {_iso(moment)}"
        self._miss(symbol, product, ruled=ruled, why=why, misses=misses)
        return day

    def _miss(
        self,
        symbol: str,
        key: str,
        *,
        ruled: date,
        why: str,
        misses: list[tuple[str, str]],
    ) -> None:
        """Log and return a miss once per product (or contract) and rule date.

        Args:
            symbol: The row's symbol.
            key: The product, or the contract when its product is not known.
            ruled: The rule's session date, which the row keeps.
            why: Why the schedules cannot place the row.
            misses: Receives the miss when it is new.
        """
        with self._lock:
            new = (key, ruled) not in self._reported
            self._reported.add((key, ruled))
        if not new:
            return
        message = (
            f"futures session of {symbol} not confirmed: {why}; its rows keep the session "
            f"the 17:00 Chicago rule gives them, {ruled.isoformat()}"
        )
        logger.error("%s: %s", self._label, message)
        misses.append((symbol, message))

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
            self._queue.put(("contracts", frozenset(wanted)))

    def _ask_products(self, products: set[str]) -> None:
        """Queue the schedule load of products not loaded lately.

        Args:
            products: Product codes.
        """
        wanted = self._due(products, {})
        if wanted:
            self._queue.put(("schedules", frozenset(wanted)))

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
            contracts: set[str] = set()
            products: set[str] = set()
            for kind, keys in (entry for entry in items if isinstance(entry, tuple)):
                (contracts if kind == "contracts" else products).update(keys)
            if contracts:
                products |= self._load_contracts(contracts)
            if products:
                self._load_schedules(products)

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
            for start in range(0, len(ordered), _TICKERS_PER_REQUEST):
                chunk = ordered[start : start + _TICKERS_PER_REQUEST]
                query = {"ticker.any_of": tuple(chunk), "limit": _PAGE}
                for row in self._fetch(CONTRACTS_PATH, query):
                    ticker, product = row.get("ticker"), row.get("product_code")
                    if isinstance(ticker, str) and isinstance(product, str) and ticker in found:
                        found[ticker] = found[ticker] or product
        except MassiveError as error:
            self._failure(contracts, f"looking up its product failed: {error}")
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
            "limit": _PAGE,
        }
        try:
            rows = self._fetch(SCHEDULES_PATH, query)
        except MassiveError as error:
            self._failure(products, f"loading its schedule failed: {error}")
            return
        events: dict[str, dict[date, list[int]]] = {product: {} for product in products}
        for row in rows:
            product, day, stamp = (
                row.get(k) for k in ("product_code", "session_end_date", "timestamp")
            )
            if product in events and isinstance(day, str) and isinstance(stamp, str):
                events[product].setdefault(date.fromisoformat(day), []).append(_ns(stamp))
        loaded = {
            product: tuple(
                Session(day, min(stamps), max(stamps)) for day, stamps in sorted(found.items())
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

    def _failure(self, keys: set[str], why: str) -> None:
        """Record that loading some contracts or products failed, and log it once.

        Their rows keep the rule's date and are reported as misses; they are asked for again
        after `RELOAD_SPACING`.

        Args:
            keys: The contracts or products.
            why: The failure, for the log and the misses.
        """
        logger.error("%s: futures schedules: %s (%s)", self._label, why, ", ".join(sorted(keys)))
        with self._lock:
            for key in keys:
                self._failed[key] = why


def _iso(ns: int) -> str:
    """Return epoch nanoseconds as an ISO-8601 UTC instant, for a message.

    Args:
        ns: The instant.

    Returns:
        Such as `2025-03-03T22:00:00+00:00`.
    """
    return (datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=ns // 1000)).isoformat()
