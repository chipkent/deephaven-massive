"""Each symbol of a `poll()` table: which ones a cycle reads, and what a symbol's failure does.

A `poll()` of symbols, or of a table of values, reads each symbol (or value) on its own, so one
symbol's failure never takes another's rows. The poller keeps one `SymbolRun` per symbol
(`_tables/symbol_retry.py`, the rule a followed `fetch()` or `history()` applies too) and asks
this module, at each cycle's start, which symbols the cycle reads (`PollSymbols.due`), and at
its end what their outcomes mean (`PollSymbols.settle`):

- A symbol that answered is read again next cycle; HTTP 404 (no data) is an answer.
- A failure that may pass (a 5xx after the client's retries, a timeout) makes the symbol
  `RETRYING`: it is read again 2, 8, and 30 seconds later, then every minute, at the first
  cycle that starts once its retry is due, and its keys stay in a `REPLACE` table. Once it has
  failed for `MassiveConfig.max_outage` it is `FAILED`.
- Any other failure (Massive's refusal of the symbol, rows the library withheld every one of)
  makes it `FAILED` at once. A `FAILED` symbol is not read again, and its keys leave a
  `REPLACE` table, until it leaves the symbol list and comes back.

A symbol leaves the list when a cycle finds it gone, or, for a refreshing table of symbols or
values, when the table's `SymbolWatch` reports its last row deleted (`DroppedNames`), even if a
row of it was added back before the next cycle: deleting a symbol's row and adding it back
always starts its run afresh, so the next cycle reads it again.

Every symbol that is `RETRYING` or `FAILED` is a span of `m.problems()` under its cause
(`symbol_retry.span_cause`), from the start of the cycle it first failed in until it answers
or leaves the list, so no symbol stops getting rows without a row that says why. The symbols of
the first cycle are the call's: when every one of them has failed, the table fails, as a call
that waits raises; once one of them answers, or leaves before failing, a symbol's failure is
its own.

A `PollSymbols` belongs to one poller, touched only by the poller's thread, so it takes no
lock. A `DroppedNames` is also fed by an update-graph thread, under its own leaf lock.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, assert_never

from deephaven_massive._errors import copy_error, every_symbol_failed, failure_text, new_live_table
from deephaven_massive._log_limits import bounded_list
from deephaven_massive._tables.problem_events import ProblemCause, SpanTracker
from deephaven_massive._tables.symbol_read_rows import SymbolReadState
from deephaven_massive._tables.symbol_retry import SPAN_CAUSES, Failed, Retry, SymbolRun

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._enums import OnError
    from deephaven_massive._errors import MassiveError
    from deephaven_massive._tables.problem_events import ProblemSpans
    from deephaven_massive._tables.watchlist import SymbolWatch

__all__ = ["DroppedNames", "PollSymbols", "Settled"]

logger = logging.getLogger(__name__)


class DroppedNames:
    """The symbols (or values) a refreshing table dropped since the poller last took them.

    The `on_change` of the table's `SymbolWatch`, which reports a symbol once its last row is
    deleted; the poller takes the set at each cycle's start (`take`) and hands it to
    `PollSymbols.due`. A symbol deleted and added back between two cycles is still dropped, so
    its run starts afresh.

    Thread-safety: `on_change` runs on an update-graph thread holding the update-graph lock, and
    `take` on the poller's thread holding no lock; a leaf lock guards the set, under which
    nothing else is called.
    """

    __slots__ = ("_dropped", "_lock")

    def __init__(self) -> None:
        """Start with nothing dropped."""
        self._lock = threading.Lock()
        self._dropped: set[str] = set()

    def on_change(self, added: frozenset[str], removed: frozenset[str]) -> None:
        """Note the symbols whose last row left the table; never blocks.

        Args:
            added: Unused: a cycle reads the table's current symbols itself.
            removed: The symbols the table no longer holds.
        """
        del added
        with self._lock:
            self._dropped |= removed

    def take(self) -> frozenset[str]:
        """Return the symbols dropped since the last call, and forget them.

        Returns:
            The dropped symbols; empty when none was.
        """
        with self._lock:
            dropped, self._dropped = frozenset(self._dropped), set()
        return dropped


@dataclass(frozen=True, slots=True)
class Settled:
    """What a cycle's symbol outcomes mean for the table (`PollSymbols.settle`).

    Attributes:
        spans: The snapshots of the symbols' spans that changed, for `m.problems()`.
        fatal: The error the table fails with, or None: every symbol of the first cycle has
            failed.
        answered: True when some symbol the cycle read answered, or the table lists no symbol:
            a good cycle. False when every listed symbol is waiting or has failed.
        failure: When no symbol answered, the first failure that may pass, which makes the
            cycle a failed one for the table's run of failed cycles; None otherwise.
    """

    spans: tuple[ProblemSpans, ...]
    fatal: MassiveError | None
    answered: bool
    failure: MassiveError | None


class PollSymbols:
    """The run of each symbol (or value) of one `poll()` table, and the call-time check.

    Not thread-safe: only the poller's thread calls it.
    """

    __slots__ = (
        "_call_time",
        "_dataset",
        "_dropped",
        "_field",
        "_limit",
        "_runs",
        "_started",
        "_trackers",
        "_watch",
    )

    def __init__(
        self,
        *,
        dataset: str,
        stream_id: int,
        field: str | None,
        refused_symbols: OnError,
        limit: float,
        watched: tuple[SymbolWatch, DroppedNames] | None = None,
    ) -> None:
        """Start with no symbol.

        Args:
            dataset: The dataset, for the spans and the log.
            stream_id: The table's `StreamId`, which its spans carry.
            field: The column of the value table for a poll of values, which the spans name;
                None for a poll of symbols.
            refused_symbols: The table's `refused_symbols` setting, which decides how loudly a
                refused symbol's span is logged.
            limit: `MassiveConfig.max_outage`, in seconds.
            watched: For a refreshing table of symbols or values, its started watch and the
                symbols the watch saw leave, which `due` forgets; `stop_watch` stops the
                watch. None for any other poll.
        """
        self._watch: SymbolWatch | None = None if watched is None else watched[0]
        self._dropped: DroppedNames | None = None if watched is None else watched[1]
        self._dataset = dataset
        self._field = field
        self._limit = limit
        self._runs: dict[str, SymbolRun] = {}
        self._started = False
        # Each first-cycle symbol with its final failure, or None while it has none; None once
        # the check is settled, or when the first cycle had no symbol.
        self._call_time: dict[str, MassiveError | None] | None = None
        where = f"{dataset} poll() stream {stream_id}"
        self._trackers = {
            cause: SpanTracker(
                cause,
                dataset=dataset,
                stream_id=stream_id,
                where=where,
                policy=refused_symbols if cause is ProblemCause.REFUSED_SYMBOLS else None,
            )
            for cause in SPAN_CAUSES
        }

    def due(self, names: tuple[str, ...], *, now: float) -> tuple[str, ...]:
        """Take the cycle's symbols and return those it reads, each counted as started.

        A symbol no longer listed, or dropped from the table since the last cycle, is
        forgotten: its span ends at the next `settle`, and a symbol listed again starts a new
        run. A symbol is read unless it is `FAILED`, or `RETRYING` with its retry not yet due.

        Args:
            names: The symbols (or values) the table lists now, sorted.
            now: The cycle's start, monotonic seconds.

        Returns:
            The symbols to read, in the order given.
        """
        listed = set(names)
        dropped = frozenset() if self._dropped is None else self._dropped.take()
        for gone in [name for name in self._runs if name not in listed or name in dropped]:
            logger.debug("%s poll(): %s left the symbol list; its run ends", self._dataset, gone)
            del self._runs[gone]
            self._call_time_left(gone)
        if not self._started:
            self._started = True
            self._call_time = dict.fromkeys(names) or None
        due: list[str] = []
        for name in names:
            run = self._runs.setdefault(name, SymbolRun())
            if _is_due(run, now):
                run.started()
                due.append(name)
        if len(due) < len(names):
            logger.debug(
                "%s poll(): %d of %d symbols are read this cycle; waiting or failed: %s",
                self._dataset,
                len(due),
                len(names),
                bounded_list([name for name in names if name not in set(due)]),
            )
        return tuple(due)

    def stop_watch(self) -> None:
        """Stop the watch of the table of symbols or values, if the poll has one.

        A Deephaven call: the poller's thread makes it as it ends, with the session's execution
        context and no library lock held.
        """
        if self._watch is not None:
            self._watch.stop()

    def holds_keys(self, name: str) -> bool:
        """Tell whether a `REPLACE` table keeps a symbol's keys although the cycle had none.

        Args:
            name: The symbol the keys' rows were given to.

        Returns:
            True only for a listed symbol waiting to be read again (`RETRYING`): its last rows
            stay, and its span says they are stale. A symbol that answered, that has failed,
            or that left the list loses its keys.
        """
        run = self._runs.get(name)  # a symbol no longer listed has no run
        return run is not None and run.state is SymbolReadState.RETRYING

    def settle(
        self,
        read: tuple[str, ...],
        failures: Mapping[str, MassiveError],
        *,
        now: float,
        at: int,
        since: int,
    ) -> Settled:
        """Take the outcomes of the symbols a cycle read.

        Args:
            read: The symbols the cycle read (`due`).
            failures: Each failed symbol's error; every other symbol read answered.
            now: When the outcomes landed, monotonic seconds.
            at: The same time as epoch nanoseconds, for the spans.
            since: The cycle's start, epoch nanoseconds: where a new span starts.

        Returns:
            What the outcomes mean for the table (`Settled`).
        """
        answered = [name for name in read if name not in failures]
        for name in answered:
            self._runs[name].succeeded()
        if answered and self._call_time is not None and set(answered) & set(self._call_time):
            logger.debug(
                "%s poll(): a symbol of the first cycle answered; the table no longer fails "
                "for those symbols alone",
                self._dataset,
            )
            self._call_time = None
        retryable: MassiveError | None = None
        for name in read:
            error = failures.get(name)  # only the failed symbols have an error
            if error is None:
                continue
            step = self._runs[name].failed(error, name=name, now=now, limit=self._limit)
            match step:
                case Retry():
                    pass
                case Failed():
                    self._call_time_failed(name, step.error)
                case _:
                    assert_never(step)
            if retryable is None and error.is_retryable:
                retryable = error
        return Settled(
            spans=self._spans(at=at, since=since),
            fatal=self._every_call_time_symbol_failed(),
            answered=bool(answered) or (not read and not self._runs),
            failure=None if answered else retryable,
        )

    def _spans(self, *, at: int, since: int) -> tuple[ProblemSpans, ...]:
        """Return the snapshot of each cause's spans that changed.

        Args:
            at: Now, epoch nanoseconds.
            since: Where a new span starts, epoch nanoseconds.

        Returns:
            The changed snapshots.
        """
        held: dict[ProblemCause, dict[str, tuple[str | None, str | None, str, int]]] = {
            cause: {} for cause in SPAN_CAUSES
        }
        for name, run in self._runs.items():
            cause = run.cause
            if cause is not None:
                assert run.error is not None  # a run holds a span only after a failure
                held[cause][name] = (name, self._field, failure_text(run.error), 0)
        changed = (
            self._trackers[cause].update(spans, at=at, since=since) for cause, spans in held.items()
        )
        return tuple(spans for spans in changed if spans is not None)

    def _call_time_left(self, name: str) -> None:
        """A symbol left the list: one of the first cycle's that had not failed settles the check.

        Args:
            name: The symbol.
        """
        pending = self._call_time
        if pending is not None and name in pending and (pending.pop(name) is None or not pending):
            self._call_time = None

    def _call_time_failed(self, name: str, error: MassiveError) -> None:
        """A symbol failed for good: record it, if it is one of the first cycle's.

        Args:
            name: The symbol.
            error: Its final error.
        """
        if self._call_time is not None and name in self._call_time:
            self._call_time[name] = error

    def _every_call_time_symbol_failed(self) -> MassiveError | None:
        """Return the table's failure once every symbol of the first cycle has failed.

        Returns:
            The error every symbol of the first cycle failed with (`every_symbol_failed`,
            the first in name order), once; None while one has not failed, and once settled.
            When every symbol failed for `max_outage` on failures that may pass, the error
            ends with the call that makes the table again (`new_live_table`), whose first
            cycle reads them afresh.
        """
        pending = self._call_time
        if pending is None or any(error is None for error in pending.values()):
            return None
        self._call_time = None
        errors = [error for _, error in sorted(pending.items()) if error is not None]
        logger.debug(
            "%s poll(): all %d symbols of the first cycle failed; the table fails",
            self._dataset,
            len(errors),
        )
        failure = every_symbol_failed(errors)
        if not failure.is_retryable:
            return failure  # a refusal or a bug: a new table would fail the same way
        return copy_error(failure, f"{failure} {new_live_table(self._dataset)}")


def _is_due(run: SymbolRun, now: float) -> bool:
    """Tell whether a cycle reads a symbol in its run's state.

    Args:
        run: The symbol's run.
        now: The cycle's start, monotonic seconds.

    Returns:
        False for a `FAILED` symbol, and for a `RETRYING` one whose retry is not due yet; True
        otherwise.
    """
    match run.state:
        case SymbolReadState.FAILED:
            return False
        case SymbolReadState.RETRYING:
            assert run.due is not None  # a retrying run has its due time
            return run.due <= now
        case (
            SymbolReadState.QUEUED
            | SymbolReadState.LOADING
            | SymbolReadState.DONE
            | SymbolReadState.REMOVED
            | SymbolReadState.STOPPED
            | SymbolReadState.CLOSED
        ):
            return True
        case _:
            assert_never(run.state)
