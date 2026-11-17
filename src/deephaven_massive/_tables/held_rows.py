"""Streamed futures rows held until their session date is known, then published in order.

WebSocket futures messages carry no session date, and the flush dates each row from its
product's schedule (`futures_dates.FuturesSessionDates`), which loads only once a stream needs
it. A row whose ticker is not ready yet is valid data, so it is neither published undated nor
dropped: `HeldRows` keeps it, in its ticker's arrival order, and the flush publishes it, dated,
before any newer row of that ticker once the schedule is in. One `HeldRows` serves one
connection's flush thread, and only that thread touches it, under the flush lock: no lock of
its own, no Deephaven call, and no wait.

`settle` takes one flush's converted rows of one dataset and returns what to publish, what to
discard, and which streams to fail. It reads the calendar's state only for the tickers that
need it: those with rows that got no date in this flush, held tickers that have rows in it, and
every held ticker once the calendar has learned something or `RECHECK_EVERY` has passed. A
ticker still waiting costs one state lookup, never a pass over its held rows. Each step dates
rows through one `correct` call, so what it publishes and what it holds come from one answer.

What a ticker's rows come to, by its state:

- *Ready*: dated by the dataset's time rule with the calendar's dates and published, held rows
  first. A bar whose `Date` that rule leaves null (one spanning two sessions) publishes as it
  is. A row no loaded session holds waits for its product's schedule to load again; once a
  load that began after the row was held still has no session for it, it is discarded
  (`Readiness.UNSCHEDULED`).
- *Waiting*: held, with every newer row of the ticker. Once its oldest held row has waited
  `DELAYED_AFTER`, the wait is reported once (`Settled.delayed`).
- *Waiting* on a lookup failing for longer than `max_outage` since its oldest row was held, or
  *refused*: dropped, and the streams that receive it fail (`Settled.failed`).
- *Unlisted*, or its product *unscheduled*: discarded and counted (`Settled.discarded`).

Held rows are copies of the flush's rows (`Table.take`), so a ticker's few rows never keep a
whole flush alive, and a ticker's tables are merged once they pass `MERGE_AT`, so a long wait
holds a few tables, not one per flush. Their buffers' size counts toward `HELD_BYTES_LIMIT`;
past it, the dataset holding the most fails with the reason (`Settled.overflow`), so a long
outage of Massive's reference data never takes the server's memory. A WARNING at a tenth of it
names the wait first.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, Generic, TypeVar, assert_never

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.row_times import DATE, INSERT, time_column
from deephaven_massive._errors import BrokenGuaranteeError, MassiveError, failure_text, library_bug
from deephaven_massive._tables.futures_dates import LookupState, Readiness

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from deephaven_massive._datasets.row_times import TimeRule
    from deephaven_massive._tables.futures_dates import FuturesSessionDates

__all__ = [
    "DELAYED_AFTER",
    "HELD_BYTES_LIMIT",
    "MERGE_AT",
    "RECHECK_EVERY",
    "Delayed",
    "HeldRows",
    "Settled",
]

logger = logging.getLogger(__name__)

_K = TypeVar("_K")
"""A dataset's key: its plan on the channel."""

DELAYED_AFTER: Final = 15.0
"""Seconds a ticker's oldest held row waits before the wait is reported as a problem: an order
of magnitude above a lookup's usual two round trips, and below a hung request's timeouts."""

RECHECK_EVERY: Final = 1.0
"""Seconds between two reads of every held ticker's state when the calendar learned nothing:
time alone moves a retry, a discard, or an outage's limit."""

HELD_BYTES_LIMIT: Final = 2**30
"""The most bytes of held rows one connection keeps: 1 GiB. Past it, the dataset holding the
most fails, so its rows stop growing the server's memory while Massive's reference data is out
of reach."""

MERGE_AT: Final = 32
"""The held tables of one ticker past which they are merged into one."""

_SYMBOL: Final = "Symbol"
_UNPLACED: Final = "no loaded session holds it; its product's schedule is loading again"


@dataclass(frozen=True, slots=True)
class _Part:
    """Some held rows of one ticker, copied out of one flush or merged from several.

    Attributes:
        rows: The rows, in arrival order.
        generation: The newest socket generation of the rows.
        held_at: When each of them was held, seconds since the epoch, one per row: merging
            parts keeps each row's own.
        nbytes: The size of the rows' buffers and of their held times.
    """

    rows: pa.Table
    generation: int
    held_at: pa.Array
    nbytes: int


@dataclass(slots=True)
class _Waiting:
    """One ticker's held rows, in arrival order.

    Attributes:
        parts: The held rows, oldest first.
        since: When its oldest held row was held, seconds since the epoch.
        reported: Whether this wait was reported (`DELAYED_AFTER`).
        reason: Why it waits, as the calendar last said.
    """

    parts: list[_Part] = field(default_factory=list)
    since: float = 0.0
    reported: bool = False
    reason: str = ""

    @property
    def rows(self) -> int:
        """How many rows it holds."""
        return sum(part.rows.num_rows for part in self.parts)

    @property
    def nbytes(self) -> int:
        """The size of its rows' buffers."""
        return sum(part.nbytes for part in self.parts)


@dataclass(frozen=True, slots=True)
class Delayed:
    """A ticker whose rows have waited `DELAYED_AFTER`, reported once per wait.

    Attributes:
        rows: How many rows it holds.
        reason: Why they wait.
    """

    rows: int
    reason: str


@dataclass(frozen=True, slots=True)
class Settled:
    """What one dataset's rows of one flush come to, after the held rows are settled.

    Attributes:
        rows: The flush's rows to publish, after `released`; None when there are none.
        released: Held rows to publish first, dated, one table per socket generation, oldest
            first, each ticker's rows in arrival order.
        discarded: The rows discarded now, by why (`Readiness.UNLISTED` or
            `Readiness.UNSCHEDULED`) and ticker, each with their count and the reason.
        failed: Each ticker whose streams fail, with the error.
        delayed: Each ticker whose wait passed `DELAYED_AFTER` in this flush.
        resumed: The tickers whose reported wait ended in this flush: their rows published.
        overflow: Why the dataset fails for holding the most bytes past `HELD_BYTES_LIMIT`;
            empty when it does not.
    """

    rows: pa.Table | None
    released: tuple[tuple[pa.Table, int], ...] = ()
    discarded: dict[Readiness, dict[str, tuple[int, str]]] = field(default_factory=dict)
    failed: dict[str, MassiveError] = field(default_factory=dict)
    delayed: dict[str, Delayed] = field(default_factory=dict)
    resumed: frozenset[str] = frozenset()
    overflow: str = ""


@dataclass(slots=True)
class _Sorted:
    """One flush's rows sorted into what `settle` does with them, ticker by ticker."""

    released: dict[int, list[pa.Table]] = field(default_factory=dict)
    discarded: dict[Readiness, dict[str, tuple[int, str]]] = field(default_factory=dict)
    failed: dict[str, MassiveError] = field(default_factory=dict)
    resumed: set[str] = field(default_factory=set)
    dropped: set[str] = field(default_factory=set)

    def release(self, rows: pa.Table, generation: int) -> None:
        """Publish held rows of one socket generation, after those released before them.

        Args:
            rows: The rows, dated.
            generation: Their socket generation.
        """
        if rows.num_rows:
            self.released.setdefault(generation, []).append(rows)

    def discard(self, state: LookupState, ticker: str, rows: int) -> None:
        """Count rows of a ticker discarded now.

        Args:
            state: The ticker's state: unlisted, or its product unscheduled.
            ticker: The ticker.
            rows: How many of its rows.
        """
        found = self.discarded.setdefault(state.readiness, {})
        count = found.get(ticker, (0, ""))[0]
        found[ticker] = (count + rows, state.reason)


class HeldRows(Generic[_K]):
    """The held futures rows of one connection's datasets (see the module).

    Only the connection's flush thread calls it, under the flush lock.

    Example:
        ::

            held = HeldRows(calendar, max_outage=300.0)
            settled = held.settle(plan, converted.batch, generation=1, times=plan.times)
            publish(settled.rows)  # dated, a ticker's held rows before its newer rows
    """

    __slots__ = ("_calendar", "_checked", "_clock", "_held", "_max_outage", "_warned")

    def __init__(
        self,
        calendar: FuturesSessionDates,
        *,
        max_outage: float,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Create an empty hold.

        Args:
            calendar: The connection's futures session calendar.
            max_outage: Seconds a ticker's lookup may fail transiently while its rows wait,
                `MassiveConfig.max_outage`; past it the streams that receive it fail.
            clock: The current time in seconds since the epoch; tests replace it.
        """
        self._calendar = calendar
        self._max_outage = max_outage
        self._clock = clock
        self._held: dict[_K, dict[str, _Waiting]] = {}
        self._checked: dict[_K, tuple[int, float]] = {}  # the version and time last read
        self._warned = False

    @property
    def total_bytes(self) -> int:
        """The size of every held row's buffers."""
        return sum(self._bytes_of(key) for key in self._held)

    def holding(self) -> tuple[_K, ...]:
        """Return the datasets that hold rows."""
        return tuple(key for key, held in self._held.items() if held)

    def rows_of(self, key: _K) -> int:
        """Return how many rows a dataset holds.

        Args:
            key: The dataset's plan.

        Returns:
            Its held rows.
        """
        return sum(w.rows for w in self._held.get(key, {}).values())

    def waiting(self, key: _K) -> frozenset[str]:
        """Return the tickers whose rows a dataset holds.

        Args:
            key: The dataset's plan.

        Returns:
            The tickers.
        """
        return frozenset(self._held.get(key, {}))

    def keep(self, keys: Iterable[_K]) -> None:
        """Drop the held rows of every dataset not among `keys`: no stream receives them now.

        Args:
            keys: The datasets still routed.
        """
        live = set(keys)
        for key in [key for key in self._held if key not in live]:
            self.drop(key, why="no stream reads them any more")

    def prune(self, key: _K, wanted: Callable[[str], bool]) -> None:
        """Drop the held rows of a dataset's tickers that no stream receives any more.

        A watchlist that removes a ticker while its rows wait leaves them to no one: they are
        dropped, and logged at INFO, rather than looked up and counted against the limit.

        Args:
            key: The dataset's plan.
            wanted: Whether some stream of the dataset still receives a ticker.
        """
        held = self._held.get(key, {})
        gone = [ticker for ticker in held if not wanted(ticker)]
        for ticker in gone:
            logger.info(
                "%d held futures rows of %s are dropped: no stream receives it any more",
                held.pop(ticker).rows,
                ticker,
            )

    def drop(self, key: _K, *, why: str) -> None:
        """Drop every held row of one dataset, and log how many at INFO.

        Args:
            key: The dataset's plan.
            why: Why, for the log.
        """
        held = self._held.pop(key, {})
        self._checked.pop(key, None)
        if held:
            logger.info(
                "%d held futures rows of %d tickers are dropped: %s",
                sum(w.rows for w in held.values()),
                len(held),
                why,
            )

    def settle(
        self, key: _K, batch: pa.Table | None, *, generation: int, times: TimeRule
    ) -> Settled:
        """Settle one dataset's rows of one flush with its held rows.

        Args:
            key: The dataset's plan on the channel.
            batch: The flush's converted rows, futures rows with a null `Date` where the
                calendar could not date them; None when the flush brought the dataset no rows,
                and only its held rows are settled.
            generation: The socket generation of the batch's rows.
            times: How the dataset fills its rows' time columns: the flush filled them with
                the calendar's dates, and held rows are filled again once they can be dated.

        Returns:
            What to publish, discard, and fail.
        """
        now = self._clock()
        held = self._held.setdefault(key, {})
        undated = _undated(batch)
        names = set() if undated is None else set(pc.unique(undated.column(_SYMBOL)).to_pylist())
        present = (
            set()
            if batch is None or not held
            else set(pc.unique(batch.column(_SYMBOL)).to_pylist()) & held.keys()
        )
        version = self._calendar.version
        last = self._checked.get(key)
        due = last is None or last[0] != version or now - last[1] >= RECHECK_EVERY
        asked = names | present | (set(held) if due else set())
        if due:
            self._checked[key] = (version, now)
        states = self._calendar.states(asked) if asked else {}
        out = _Sorted()
        for ticker in [t for t in held if t in states]:
            self._settle_held(held, ticker, states[ticker], out=out, now=now, times=times)
        rows = None
        if batch is not None:
            rows = self._take(
                held,
                batch,
                names=names,
                states=states,
                out=out,
                now=now,
                generation=generation,
                times=times,
            )
        delayed = self._delayed(held, now)
        if not held:
            del self._held[key]
        return Settled(
            rows if rows is not None and rows.num_rows else None,
            tuple(
                (pa.concat_tables(tables), released)
                for released, tables in sorted(out.released.items())
            ),
            out.discarded,
            out.failed,
            delayed,
            frozenset(out.resumed),
            self._overflow(key),
        )

    # --- one flush ---------------------------------------------------------------------------

    def _settle_held(
        self,
        held: dict[str, _Waiting],
        ticker: str,
        state: LookupState,
        *,
        out: _Sorted,
        now: float,
        times: TimeRule,
    ) -> None:
        """Settle one held ticker by its state: keep, publish, discard, or fail its rows.

        Args:
            held: The dataset's held tickers.
            ticker: The ticker.
            state: Its state, read this flush.
            out: Receives what to publish, discard, and fail.
            now: The clock's time.
            times: How the dataset fills its rows' time columns.
        """
        waiting = held[ticker]
        match state.readiness:
            case Readiness.WAITING:
                outage = self._outage(ticker, state, since=waiting.since, now=now)
                if outage is None:
                    waiting.reason = state.reason
                    return
                del held[ticker]
                out.failed[ticker] = outage
            case Readiness.REFUSED:
                del held[ticker]
                out.failed[ticker] = _refused(ticker, state)
            case Readiness.UNLISTED | Readiness.UNSCHEDULED:
                del held[ticker]
                out.discard(state, ticker, waiting.rows)
            case Readiness.READY:
                del held[ticker]
                self._ready(
                    held, ticker, waiting=waiting, state=state, out=out, now=now, times=times
                )
                if waiting.reported and ticker not in held:
                    out.resumed.add(ticker)
                    logger.info(
                        "the held futures rows of %s are settled after %.0f s",
                        ticker,
                        now - waiting.since,
                    )
            case _:
                assert_never(state.readiness)

    def _ready(
        self,
        held: dict[str, _Waiting],
        ticker: str,
        *,
        waiting: _Waiting,
        state: LookupState,
        out: _Sorted,
        now: float,
        times: TimeRule,
    ) -> None:
        """Date a ready ticker's held rows: publish them, or hold or withhold those left undated.

        A row no loaded session holds waits for its product's schedule to load again, and
        every later row of the ticker waits behind it, so none overtakes it. Once a load that
        began after the row was held answers without such a session, the row is withheld
        (`Readiness.UNSCHEDULED`), with the sessions nearest it. A reload that Massive refused,
        or that has failed past `max_outage`, fails the ticker's streams instead.

        Args:
            held: The dataset's held tickers, which no longer hold this one.
            ticker: The ticker.
            waiting: Its held rows.
            state: Its state, ready.
            out: Receives what to publish, discard, and fail.
            now: The clock's time.
            times: How the dataset fills its rows' time columns.
        """
        kept: list[_Part] = []
        lost = 0
        why = ""
        for part in waiting.parts:
            rows, placed = self._fill(part.rows, times)
            if kept:  # an earlier row waits: every later one waits behind it
                kept.append(_part(rows, part.generation, held_at=part.held_at))
                continue
            # A row held before the last load began is final: that load answered for it.
            final = (
                pc.less_equal(part.held_at, state.loaded_at)
                if state.loaded_at is not None
                else pa.repeat(pa.scalar(value=False), rows.num_rows)
            )
            first = _first(pc.and_(pc.invert(placed), pc.invert(final)))
            head = rows if first is None else rows.slice(0, first)
            head_placed = placed if first is None else placed.slice(0, first)
            out.release(head.filter(head_placed), part.generation)
            left = head.filter(pc.invert(head_placed))
            if left.num_rows:  # a load since the rows were held has no session for them
                column = left.column(time_column(times.row_times)).combine_chunks()
                instant = pc.cast(column, pa.int64())
                why = why or self._calendar.unplaced(ticker, instant[0].as_py())
                lost += left.num_rows
            if first is not None:
                tail = rows.slice(first)
                kept.append(
                    _part(
                        tail.take(pa.array(range(tail.num_rows))),
                        part.generation,
                        held_at=part.held_at.slice(first).take(pa.array(range(tail.num_rows))),
                    )
                )
        if lost:
            out.discard(LookupState(Readiness.UNSCHEDULED, why), ticker, lost)
        if not kept:
            return
        # The rows still waiting depend on a reload that may be refused, or out of reach.
        failure = (
            _refused(ticker, state)
            if state.error is not None
            else self._outage(ticker, state, since=waiting.since, now=now)
        )
        if failure is not None:
            out.failed[ticker] = failure
            return
        held[ticker] = _Waiting(
            kept, since=waiting.since, reported=waiting.reported, reason=_UNPLACED
        )

    def _take(
        self,
        held: dict[str, _Waiting],
        batch: pa.Table,
        *,
        names: set[str],
        states: dict[str, LookupState],
        out: _Sorted,
        now: float,
        generation: int,
        times: TimeRule,
    ) -> pa.Table:
        """Sort a flush's rows: publish the ready ones, hold or drop the others, in order.

        Every row of a ticker that is not ready, or still holds rows, is held, dated or not,
        so none overtakes an older one. A ready ticker's row the calendar does not place waits
        for its product's schedule to load again.

        Args:
            held: The dataset's held tickers.
            batch: The flush's rows.
            names: The tickers of its rows that have no date.
            states: The states read this flush.
            out: Receives what to discard and fail.
            now: The clock's time.
            generation: The socket generation of the batch's rows.
            times: How the dataset fills its rows' time columns.

        Returns:
            The batch's rows to publish, in arrival order.
        """
        if not names and not held:
            return batch
        ready = _sort_tickers(names, batch, states=states, out=out)
        symbols = batch.column(_SYMBOL).combine_chunks()
        waiting = sorted((names - ready) | held.keys())
        waits = pc.is_in(symbols, value_set=pa.array(waiting, pa.string()))
        if ready:  # dated again, through one answer of the calendar
            batch, placed = self._fill(batch, times)
            named = pc.is_in(symbols, value_set=pa.array(sorted(ready), pa.string()))
            waits = pc.or_(waits, _behind(symbols, pc.and_(named, pc.invert(placed))))
        waits = pc.fill_null(waits, fill_value=False)
        _hold(held, batch.filter(waits), states=states, out=out, now=now, generation=generation)
        return batch.filter(pc.invert(waits))

    def _fill(self, rows: pa.Table, times: TimeRule) -> tuple[pa.Table, pa.Array]:
        """Fill rows' time columns again, and tell which rows the calendar placed.

        The dataset's own rule fills them, as the flush did (`TimeRule.fill`), through one
        call of the calendar's `correct`, whose answer also decides which rows are placed: a
        bar that spans two sessions keeps its null `Date` and still counts as placed.

        Args:
            rows: The rows, filled once.
            times: How the dataset fills its rows' time columns.

        Returns:
            The rows, filled again, and one flag per row: True unless it has a ticker and the
            calendar placed no session for it.
        """
        answers: list[pa.Array] = []

        def correct(symbols: pa.Array, instants: pa.Array, dates: pa.Array) -> pa.Array:
            found = self._calendar.correct(symbols, instants, dates)
            answers.append(found)
            return found

        received = rows.column(INSERT).combine_chunks()
        filled = times.fill(rows, received=received, session_dates=correct)
        dated = pc.is_valid(filled.column(DATE).combine_chunks())
        if not answers:  # every row kept a date it had: the calendar was not asked
            return filled, dated
        unplaced = pc.and_(
            pc.is_null(answers[-1]), pc.is_valid(filled.column(_SYMBOL).combine_chunks())
        )
        return filled, pc.invert(pc.and_(unplaced, pc.invert(dated)))

    def _outage(
        self, ticker: str, state: LookupState, *, since: float, now: float
    ) -> MassiveError | None:
        """Return the failure of a waiting ticker's streams once its outage passes the limit.

        The outage counts from the later of when its lookup began failing and when its oldest
        row waiting now was held: a ticker whose rows only just arrived gets the whole
        `max_outage`.

        Args:
            ticker: The ticker.
            state: Its state, waiting.
            since: When its oldest row waiting now was held.
            now: The clock's time.

        Returns:
            The failure past `max_outage`, or None while the ticker may still wait.
        """
        if state.failing_since is None:
            return None
        down = now - max(state.failing_since, since)
        if down <= self._max_outage:
            return None
        return MassiveError(
            f"the rows of {ticker} have waited {down:.0f} s for its trading session, longer "
            f"than MassiveConfig.max_outage ({self._max_outage:g} s): {state.reason}"
        )

    def _delayed(self, held: dict[str, _Waiting], now: float) -> dict[str, Delayed]:
        """Return the held tickers whose wait passed `DELAYED_AFTER` now, marking them reported.

        Args:
            held: The dataset's held tickers.
            now: The clock's time.

        Returns:
            Each newly delayed ticker.
        """
        found = {}
        for ticker, waiting in held.items():
            if not waiting.reported and now - waiting.since >= DELAYED_AFTER:
                waiting.reported = True
                found[ticker] = Delayed(waiting.rows, waiting.reason)
        return found

    def _bytes_of(self, key: _K) -> int:
        """Return the size of one dataset's held rows' buffers.

        Args:
            key: The dataset's plan.

        Returns:
            The bytes.
        """
        return sum(w.nbytes for w in self._held.get(key, {}).values())

    def _overflow(self, key: _K) -> str:
        """Check the held bytes against `HELD_BYTES_LIMIT`; past it, fail the largest dataset.

        Args:
            key: The dataset just settled.

        Returns:
            Why `key` fails, when it holds the most past the limit; empty otherwise.
        """
        sizes = {k: self._bytes_of(k) for k in self._held}
        total = sum(sizes.values())
        if total < HELD_BYTES_LIMIT // 20:
            self._warned = False
        elif total >= HELD_BYTES_LIMIT // 10 and not self._warned:
            self._warned = True
            logger.warning(
                "%d MB of streamed futures rows wait for their trading sessions on one "
                "connection, a tenth of the %d MB the library keeps; Massive's futures "
                "reference data is slow or out of reach",
                total // 2**20,
                HELD_BYTES_LIMIT // 2**20,
            )
        if total < HELD_BYTES_LIMIT or max(sizes, key=sizes.__getitem__) != key:
            return ""
        held = self._held[key]
        reasons = sorted({w.reason for w in held.values()})
        return (
            f"{sum(w.rows for w in held.values())} streamed futures rows "
            f"({sizes[key] // 2**20} MB) waited for their trading sessions, past the "
            f"{HELD_BYTES_LIMIT // 2**20} MB the library keeps: {'; '.join(reasons[:3])}"
        )


def _undated(batch: pa.Table | None) -> pa.Table | None:
    """Return the rows of a batch that have a ticker and no date, or None when none do.

    Args:
        batch: The flush's rows, or None.

    Returns:
        Those rows, or None.
    """
    if batch is None or not batch.num_rows:
        return None
    mask = pc.and_(pc.is_null(batch.column(DATE)), pc.is_valid(batch.column(_SYMBOL)))
    undated = batch.filter(mask)
    return undated if undated.num_rows else None


def _sort_tickers(
    names: set[str],
    batch: pa.Table,
    *,
    states: dict[str, LookupState],
    out: _Sorted,
) -> set[str]:
    """Decide what each ticker of a flush's undated rows comes to.

    Args:
        names: The tickers of the flush's rows that have no date.
        batch: The flush's rows.
        states: The states read this flush.
        out: Receives the tickers whose streams fail, and the rows discarded; both are
            dropped, not held.

    Returns:
        The ready tickers, whose rows the calendar dates now.
    """
    ready: set[str] = set()
    symbols = batch.column(_SYMBOL)
    for ticker in sorted(names):
        state = states[ticker]
        match state.readiness:
            case Readiness.WAITING:
                continue  # its rows wait; `_settle_held` judges its outage once held
            case Readiness.REFUSED:
                out.failed[ticker] = _refused(ticker, state)
                out.dropped.add(ticker)
            case Readiness.UNLISTED | Readiness.UNSCHEDULED:
                out.discard(state, ticker, pc.sum(pc.equal(symbols, ticker)).as_py())
                out.dropped.add(ticker)
            case Readiness.READY:
                ready.add(ticker)
            case _:
                assert_never(state.readiness)
    return ready


def _hold(
    held: dict[str, _Waiting],
    rows: pa.Table,
    *,
    states: dict[str, LookupState],
    out: _Sorted,
    now: float,
    generation: int,
) -> None:
    """Hold the rows of tickers that wait, each ticker's rows in arrival order.

    The rows are copied ticker by ticker (`take`), so each held table holds only its own, and a
    ticker's tables are merged once they pass `MERGE_AT`. A ticker whose streams fail, or whose
    rows are discarded, holds none.

    Args:
        held: The dataset's held tickers.
        rows: The flush's rows of tickers that wait, in arrival order.
        states: The states read this flush.
        out: The tickers whose rows of this flush are dropped.
        now: The clock's time.
        generation: The socket generation of the rows.
    """
    if not rows.num_rows:
        return
    # A stable sort keeps each ticker's rows in arrival order.
    ordered = rows.take(pc.sort_indices(rows, sort_keys=[(_SYMBOL, "ascending")]))
    start = 0
    for entry in pc.value_counts(ordered.column(_SYMBOL).combine_chunks()).to_pylist():
        ticker, count = entry["values"], entry["counts"]
        part = ordered.slice(start, count)
        start += count
        if ticker in out.dropped or ticker in out.failed:  # its streams fail, or rows discarded
            continue
        waiting = held.get(ticker)
        if waiting is None:
            state = states.get(ticker)
            reason = state.reason if state is not None and state.reason else _UNPLACED
            waiting = held[ticker] = _Waiting(since=now, reason=reason)
        # Its own buffers, not the flush's.
        held_at = pa.repeat(pa.scalar(now, pa.float64()), count)
        waiting.parts.append(_part(part.take(pa.array(range(count))), generation, held_at=held_at))
        if len(waiting.parts) > MERGE_AT:
            merged = pa.concat_tables([p.rows for p in waiting.parts]).combine_chunks()
            newest = max(p.generation for p in waiting.parts)
            each = pa.concat_arrays([p.held_at for p in waiting.parts])
            waiting.parts = [_part(merged, newest, held_at=each)]


def _part(rows: pa.Table, generation: int, *, held_at: pa.Array) -> _Part:
    """Return held rows with their size.

    Args:
        rows: The rows, a copy of their own.
        generation: Their newest socket generation.
        held_at: When each of them was held, one per row.

    Returns:
        The part.
    """
    size = rows.get_total_buffer_size() + held_at.get_total_buffer_size()
    return _Part(rows, generation, held_at, size)


def _first(flags: pa.Array) -> int | None:
    """Return the index of the first set flag, or None when none is set.

    Args:
        flags: Booleans, one per row.

    Returns:
        The index, or None.
    """
    found = pc.index(flags, value=True).as_py()
    return None if found < 0 else found


def _behind(symbols: pa.Array, waits: pa.Array) -> pa.Array:
    """Extend the rows that wait to every later row of the same ticker.

    Args:
        symbols: The rows' tickers.
        waits: Which rows must wait.

    Returns:
        Which rows wait: each ticker's first waiting row and every row of it after.
    """
    if not pc.any(waits).as_py():
        return waits
    positions = pa.array(range(len(symbols)), pa.int64())
    behind = waits
    for ticker in pc.unique(symbols.filter(waits)).to_pylist():
        mine = pc.equal(symbols, ticker)
        start = pc.min(positions.filter(pc.and_(mine, waits))).as_py()
        behind = pc.or_(behind, pc.and_(mine, pc.greater_equal(positions, start)))
    return behind


def _refused(ticker: str, state: LookupState) -> MassiveError:
    """Return the failure of the streams that receive a ticker whose lookup Massive refused.

    Args:
        ticker: The ticker.
        state: Its state, refused.

    Returns:
        Massive's refusal, its status and source kept, naming the lookup it refused.

    Raises:
        BrokenGuaranteeError: If the state carries no refusal, which the calendar always gives
            a refused ticker.
    """
    error = state.error
    if error is None:
        raise BrokenGuaranteeError(library_bug(f"the refused lookup of {ticker} has no refusal"))
    return MassiveError(
        f"looking up the trading session of {ticker} failed: {failure_text(error)}",
        status=error.status,
        request_id=error.request_id,
        source=error.source,
    )
