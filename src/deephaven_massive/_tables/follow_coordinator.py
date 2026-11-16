"""The follow coordinator: the one owner of every followed read's per-symbol state in a session.

A `history()` or `fetch()` whose `symbols` is a refreshing table follows it
(`_tables/followed_read.py`): each symbol added to the table is read (by `history()` over the
call's fixed window, by `fetch()` as Massive answers at that read) and lands in the result
whole, in one update cycle, and each symbol removed leaves it. The coordinator adds, replaces,
and frees a symbol's rows by publishing its download (`FollowedRead.publish`);
`followed_read.py` explains that model. This module decides, for every
such result of a session, which symbol is read when, what its read's outcome does, and what the
diagnostics show. It owns:

- **One coordinator thread per session** (`massive #1 follow`), started by the session's first
  followed read and kept until the session closes or is collected (a `QueueWorker`), so one
  thread only ever owns the state. It takes every event from
  one queue, in order: the symbol table's changes (from the watch's listener, which only
  queues), each read's start and outcome (from the read pool), and each result's stop (from its
  blink table's shutdown). It is the only thread that reads or changes per-symbol state, so that
  state needs no lock. It makes no request and waits on nothing: it publishes each symbol's
  download (`FollowedRead.publish`, which never waits for an update cycle), times the
  retries, and queues the `m.streams()` and `m.symbol_reads()` rows.
- **The session's read pool** (`_tables/read_pool.py`), at most `MassiveConfig.max_concurrency`
  threads (`massive #1 read_0`), shared with the background reads, so every download of the
  session counts against one limit. The session owns and stops it; the coordinator only submits
  to it.
  A read's outcome that arrives once its result's stop token is set is dropped: the result was
  released and is unused, or failed. Each task
  reads some symbols of one result (`FollowPlan.read`) in the session's execution context,
  listening to that result's stop token, and queues the outcome. A REST read reads up to
  `FollowPlan.symbols_per_read` symbols (one, or a `fetch()` list parameter's batch); a
  flat-file read reads every symbol waiting at once, in one scan, and a result has at most
  one scan in flight, since a scan downloads the same files whatever the number of symbols.

**A symbol's life.** Added, it is `QUEUED`; a read of it makes it `LOADING`; its rows make it
`DONE`. A failure that may pass (its error `is_retryable`) makes it `RETRYING`, read again 1, 5,
and 15 minutes after the first, second, and third failure (`RETRY_DELAYS`), and `FAILED` after the
fourth; any other failure makes it `FAILED` at once. Removed, it is `REMOVED`: under `Reread.ALWAYS`
an empty download replaces its rows in the result, freeing them, and a re-add reads it again;
under `Reread.NEVER` its download is kept, so a re-add shows the
earlier download at once, with no request. A `FAILED` symbol is read again on every re-add. Each add
and removal moves the symbol to a new *generation*: the outcome of a read of an older one is
dropped, so no removed or overtaken read ever lands, and a symbol is never read twice at once (a
re-add during a read waits for it to end). When the result stops (released and unused, or failed),
every symbol still following is `STOPPED`, and stays so: a read that starts after the stop
changes nothing.

**Whole-table failures.** A read that raises (`AuthenticationError`, a session closed during
the read, or a defect) and a symbol table that fails or, under
`noncanonical_symbols=RAISE`, gains a bad row fail the result (`FollowedRead.fail_table`):
the coordinator records the failure and publishes it, never the listener. So does a call that
did not wait whose call-time symbols (the table's symbols when the watch started) all end
`FAILED`, such as on a plan without the dataset, which refuses every symbol: the table fails
with the first symbol's error, in name order, retryable only when every symbol's error is, as
a call that waits raises it (`_call_time_outcome`). One of them with rows, or one deleted from
the table before it failed, settles the check: from then on a symbol's failure is its own.

**Time.** The coordinator reads `CLOCK`: the wall clock for the diagnostics' instants and the
monotonic clock for the retries. Its queue wait ends at the next retry; tests replace `CLOCK`
with a virtual one and wake the coordinator (`wake`) after moving it.

**Cost.** The retries wait in a heap ordered by due time, and each result keeps its
`m.streams()` progress as running counts (`_Progress`), so a batch costs work in the symbols it
changed, never a pass over every symbol: a read of 10,000 symbols that land one by one stays
linear in them.

The module is one subject although it runs past 1,000 lines: every event of a followed read
(a change of its symbol table, a read's start or outcome, a retry coming due, a stop) changes
the same per-symbol state on the one coordinator thread, so the state machine and the rows it
shows read whole only here.
"""

from __future__ import annotations

import functools
import heapq
import itertools
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, Protocol, TypeAlias

from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._enums import Reread
from deephaven_massive._errors import (
    LIBRARY_BUG,
    BrokenGuaranteeError,
    ErrorSource,
    MassiveError,
    every_symbol_failed,
    failure_text,
    library_bug,
    required,
)
from deephaven_massive._log_limits import bounded_list, log_failure, mark_failure_logged
from deephaven_massive._tables import poll
from deephaven_massive._tables.diagnostics import (
    FLAT_FILES_AND_REST,
    SYMBOLS_LISTED,
    NotePart,
    StreamUpdate,
    symbol_list_text,
)
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import QueueWorker
from deephaven_massive._tables.poll import SymbolRowError
from deephaven_massive._tables.symbol_read_rows import SymbolReadRow, SymbolReadState
from deephaven_massive._tables.transports import SymbolFailure, SymbolRows

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive._config import MassiveConfig
    from deephaven_massive._tables.followed_read import FollowPlan
    from deephaven_massive._tables.poll import PollClock
    from deephaven_massive._tables.read_pool import ReadPool
    from deephaven_massive._tables.transports import SymbolOutcome

__all__ = [
    "CLOCK",
    "RETRY_DELAYS",
    "CoordinatedRead",
    "FirstRead",
    "FollowCoordinator",
    "FollowedReadRows",
    "FollowingCore",
]

logger = logging.getLogger(__name__)

CLOCK: PollClock = poll.CLOCK
"""The coordinator's time source; tests replace it with a virtual clock."""

RETRY_DELAYS: Final = (60.0, 300.0, 900.0)
"""Seconds after a symbol's first, second, and third transient failure before it is read again;
the fourth failure is final."""

_FOLLOWING: Final = frozenset(
    {SymbolReadState.QUEUED, SymbolReadState.LOADING, SymbolReadState.RETRYING}
)
"""The states of a symbol still waiting for its rows: the `Loading` count of `m.streams()`."""

_ENDED: Final = frozenset({SymbolReadState.FAILED, SymbolReadState.REMOVED, SymbolReadState.CLOSED})
"""The states a stop leaves as they are; every other becomes `STOPPED`."""

_CAUSES_SHOWN: Final = 3
"""The most causes one `Error` part of `m.streams()` names."""

_SYMBOLS_SHOWN: Final = 5
"""The most symbols one cause of an `Error` part lists."""

_CAUSE_SHOWN: Final = 200
"""The longest text of one cause in `Error`, in characters."""


class FollowedReadRows(Protocol):
    """The session's worker tables, as the coordinator shows a followed read's progress."""

    def update_followed_read(
        self, stream_id: int, update: StreamUpdate, /, *, loading: bool
    ) -> None:
        """Apply a read's progress to its `m.streams()` row (`WorkerTables.update_followed_read`).

        Args:
            stream_id: The read's `StreamId`.
            update: The row's change.
            loading: True while any symbol still waits for its rows.
        """
        ...


class FollowingCore(Protocol):
    """A followed read's session core, as the coordinator reads and reports to it."""

    @property
    def closed(self) -> bool:
        """True once the session has closed (`SessionCore.closed`)."""
        ...

    @property
    def config(self) -> MassiveConfig:
        """The session's settings."""
        ...

    @property
    def workers(self) -> FollowedReadRows:
        """The session's worker tables."""
        ...

    def write_symbol_reads(self, rows: Sequence[SymbolReadRow], /) -> None:
        """Write symbol rows to `m.symbol_reads()` (`SessionCore.write_symbol_reads`).

        Args:
            rows: The rows.
        """
        ...


class CoordinatedRead(Protocol):
    """A followed read, as the coordinator drives it: a `FollowedRead`.

    The coordinator reads its settings, hands it each download to publish, and fails it; it
    calls nothing else, so a test can drive the coordinator with a read of its own.
    """

    @property
    def core(self) -> FollowingCore:
        """The session's core."""
        ...

    @property
    def call(self) -> int:
        """The call's id: the row's `StreamId`."""
        ...

    @property
    def dataset(self) -> str:
        """The dataset's name, such as `"stocks.bars"`."""
        ...

    @property
    def reread(self) -> Reread:
        """Whether a returning symbol is read again."""
        ...

    @property
    def plan(self) -> FollowPlan:
        """How the symbols are read."""
        ...

    @property
    def stop_token(self) -> threading.Event:
        """The result's stop token; every read listens to it."""
        ...

    @property
    def built_step(self) -> int:
        """The update cycle the result was built at."""
        ...

    @property
    def shut_down(self) -> bool:
        """True once the result's blink table has shut down."""
        ...

    def fail_table(self, error: MassiveError, /) -> bool:
        """Fail the result, once (`FollowedRead.fail_table`).

        Args:
            error: The failure.

        Returns:
            False when it had already failed.
        """
        ...

    def publish(
        self,
        symbol: str,
        seq: int,
        table: Table | None,
        /,
        *,
        decided_at_step: int,
        parts_before: int,
    ) -> int:
        """Publish a symbol's download, or an empty one (`FollowedRead.publish`).

        Args:
            symbol: The symbol.
            seq: The download's sequence number.
            table: The rows, or None for an empty download.
            decided_at_step: The update cycle the download was decided at.
            parts_before: The symbol's row groups before this download.

        Returns:
            The symbol's row groups after it.
        """
        ...


@dataclass(frozen=True, slots=True)
class FirstRead:
    """The outcome of the `wait=True` read of a followed read's call-time symbols.

    Made on the caller thread before the result exists, and handed to the coordinator with
    the result (`FollowCoordinator.follow`), which records each symbol's outcome as if its
    own pool had read it. Immutable.

    Attributes:
        outcomes: Each call-time symbol's rows (already in the result's seed) or failure.
        began_at: When the read began, epoch nanoseconds: each `DONE` symbol's `ReadAt`.
        seconds: How long the read took, for the log.
    """

    outcomes: Mapping[str, SymbolOutcome]
    began_at: int
    seconds: float


@dataclass(frozen=True, slots=True)
class _Follow:
    """A new followed read, with what its caller already read."""

    read: CoordinatedRead
    first: FirstRead | None


@dataclass(frozen=True, slots=True)
class _Baseline:
    """The symbol table's whole set of symbols when its watch started.

    Every later change is relative to it. `step` is the update cycle the watch replayed the
    table at, or 0 when the table held no symbol then.
    """

    read: CoordinatedRead
    symbols: frozenset[str]
    step: int


@dataclass(frozen=True, slots=True)
class _Changed:
    """Symbols the table gained and lost in one update cycle, the cycle `step`."""

    read: CoordinatedRead
    added: frozenset[str]
    removed: frozenset[str]
    step: int


@dataclass(frozen=True, slots=True)
class _Skipped:
    """The table's values the result skips now, because they are not Massive's tickers."""

    read: CoordinatedRead
    text: str | None


@dataclass(frozen=True, slots=True)
class _Invalid:
    """A bad row under `noncanonical_symbols=RAISE`, or the symbol table's own failure."""

    read: CoordinatedRead
    message: str


@dataclass(frozen=True, slots=True)
class _Began:
    """A pool thread began reading some symbols of one result, at these generations."""

    read: CoordinatedRead
    generations: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class _Outcome:
    """A read's outcome: each symbol's rows or failure, or the error that failed the table."""

    read: CoordinatedRead
    generations: Mapping[str, int]
    outcomes: Mapping[str, SymbolOutcome]
    began_at: int
    seconds: float
    table_error: MassiveError | None = None


@dataclass(frozen=True, slots=True)
class _Stop:
    """The result stopped: released and unused, failed, or closed."""

    read: CoordinatedRead


@dataclass(frozen=True, slots=True)
class _Wake:
    """Nothing but a wake-up: the coordinator checks the retries that are due."""


_Message: TypeAlias = (
    "_Follow | _Baseline | _Changed | _Skipped | _Invalid | _Began | _Outcome | _Stop | _Wake"
)


@dataclass(slots=True)
class _Symbol:
    """One symbol of one followed read; touched only by the coordinator thread.

    Attributes:
        state: Its `m.symbol_reads()` state.
        in_table: Whether the symbol table holds it now.
        generation: Moves on every add and removal; a read's outcome counts only at the
            generation it was started for.
        reading: The generation a read in flight was started for, or None.
        waiting: Whether it waits for a read slot (its result's queue).
        attempts: Reads begun since it was last added with a read queued.
        due: When its next read is due (monotonic seconds), while `RETRYING`.
        next_attempt_at: The same instant, epoch nanoseconds.
        added_at: When its row last entered the table, epoch nanoseconds.
        read_at: When the download whose rows the result holds began, or None.
        rows: Its rows in the result (0 for none).
        kept: Whether the result's newest download of it holds rows (is not empty).
        parts: How many row groups of its newest published download hold rows: every part
            from this one up is empty or was never sent (`FollowedRead.publish`).
        error: Its latest failure's text, until a read succeeds.
        cause: Its latest failure's cause, which groups it in `Error`.
        transport: The source of its latest read, as `m.symbol_reads()` shows it.
        counted: What its result's progress last counted of it (`_Counted`), or None while
            it is not counted: it was never shown, or it left the table.
    """

    state: SymbolReadState
    in_table: bool
    generation: int
    added_at: int
    reading: int | None = None
    waiting: bool = False
    attempts: int = 0
    due: float | None = None
    next_attempt_at: int | None = None
    read_at: int | None = None
    rows: int = 0
    kept: bool = False
    parts: int = 0
    error: str | None = None
    cause: str | None = None
    transport: str | None = None
    counted: _Counted | None = None


@dataclass(frozen=True, slots=True)
class _Counted:
    """What one symbol in the table adds to its result's `m.streams()` progress.

    Attributes:
        state: Its state, which counts it as loading, retrying, or failed.
        rows: Its rows in the result.
        cause: Its latest failure's cause, which groups a retrying or failed symbol.
    """

    state: SymbolReadState
    rows: int
    cause: str


@dataclass(slots=True)
class _Progress:
    """A followed read's `m.streams()` progress, kept current one changed symbol at a time.

    `_show` updates it from the symbols whose row changed since it last ran (`_Result.dirty`),
    so writing a result's row costs work in the symbols that changed, not in all of them: a
    read of 10,000 symbols that land one by one would otherwise cost 10,000 passes over all
    10,000. Touched only by the coordinator thread.

    Attributes:
        members: The symbols the table holds now.
        loading: How many of them still wait for their rows (`QUEUED`, `LOADING`, `RETRYING`).
        rows: Their rows in the result, in all.
        troubled: Of them, the `RETRYING` and `FAILED` ones, by state, then by cause.
        members_changed: Whether `members` changed since `symbols` was last made.
        troubled_changed: Whether `troubled` changed since `notes` were last made.
        symbols: The `SymbolCount` and `Symbols` shown (`symbol_list_text`).
        notes: The `Error` parts shown (`FollowCoordinator._note`).
    """

    members: set[str] = field(default_factory=set)
    loading: int = 0
    rows: int = 0
    troubled: dict[SymbolReadState, dict[str, set[str]]] = field(
        default_factory=lambda: {SymbolReadState.RETRYING: {}, SymbolReadState.FAILED: {}}
    )
    members_changed: bool = True
    troubled_changed: bool = True
    symbols: tuple[int | None, str] = (0, "")
    notes: dict[NotePart, str | None] = field(default_factory=dict)

    def count(self, name: str, symbol: _Symbol) -> None:
        """Replace what a symbol added to the progress with what it adds now.

        Args:
            name: The symbol.
            symbol: Its state.
        """
        before = symbol.counted
        after = _Counted(symbol.state, symbol.rows, symbol.cause or "") if symbol.in_table else None
        if before == after:
            return
        for counted, sign in ((before, -1), (after, 1)):
            if counted is None:
                continue
            self.loading += sign * (counted.state in _FOLLOWING)
            self.rows += sign * counted.rows
            causes = self.troubled.get(counted.state)
            if causes is None:
                continue
            self.troubled_changed = True
            names = causes.setdefault(counted.cause, set())
            if sign > 0:
                names.add(name)
                continue
            names.discard(name)
            if not names:
                del causes[counted.cause]
        if (before is None) != (after is None):
            self.members_changed = True
            if after is None:
                self.members.discard(name)
            else:
                self.members.add(name)
        symbol.counted = after


@dataclass(slots=True)
class _Result:
    """The coordinator's state of one followed read.

    Attributes:
        read: The result's handle.
        symbols: Every symbol it has followed, by name.
        queue: The symbols waiting for a read, in the order they began to wait: a dict used as
            an ordered set, so taking a symbol out costs no scan.
        scanning: Whether a flat-file scan of the result is in flight.
        seq: The number of the result's latest download.
        ended: Whether it failed or stopped: no read starts, and no download is published.
        dirty: The symbols whose `m.symbol_reads()` row changed since it was last written.
        warned: The causes of failure already logged at WARNING for the result.
        transports: The sources its reads have used.
        skipped: The text of its skipped symbol-table values, or None.
        progress: Its `m.streams()` progress, kept current by `_show`.
        shown: The `m.streams()` progress last written, to skip an unchanged write.
        decided_at_step: The latest update cycle of the symbol table whose change the
            coordinator has applied (the result's build cycle until the first), stamped on
            every download it publishes; see `FollowedRead.publish`.
        call_time: For a call that did not wait, until its call-time symbols settle: each of
            them still in the table, in name order, with its final failure once it has
            one. None until the watch's first set of symbols arrives (`awaits_call_time`), for
            a call that waited, and once settled (`_call_time_outcome`).
        awaits_call_time: True from the start of a call that did not wait until the watch's
            first set of symbols arrives.
        call_time_open: How many symbols of `call_time` have not failed yet, so each outcome
            updates the check in constant time.
    """

    read: CoordinatedRead
    decided_at_step: int
    symbols: dict[str, _Symbol] = field(default_factory=dict)
    queue: dict[str, None] = field(default_factory=dict)
    scanning: bool = False
    seq: int = 0
    ended: bool = False
    dirty: set[str] = field(default_factory=set)
    warned: set[str] = field(default_factory=set)
    transports: set[str] = field(default_factory=set)
    skipped: str | None = None
    progress: _Progress = field(default_factory=_Progress)
    shown: object = None
    call_time: dict[str, MassiveError | None] | None = None
    awaits_call_time: bool = False
    call_time_open: int = 0


def _now_ns() -> int:
    """Return the wall-clock time of `CLOCK` in epoch nanoseconds."""
    return int(CLOCK.time() * 1e9)


def _cause(error: MassiveError) -> str:
    """Return the key that groups failures of one cause: the error's type, source, and status.

    A bug is a cause of its own, so it never shares a key, or its single WARNING or ERROR, with
    a failure that is none.

    Args:
        error: A symbol's failure.

    Returns:
        Such as `"EntitlementError massive 403"`, `"MassiveError network"` for a failure
        without a status, or `"MassiveError library bug"`.
    """
    parts = [type(error).__name__, error.source.value]
    if error.status is not None:
        parts.append(str(error.status))
    if error.is_bug:
        parts.append("bug")
    return " ".join(parts)


def _transport(read: CoordinatedRead, outcome: SymbolOutcome) -> str:
    """Return the source a symbol's read used, as `m.symbol_reads()` shows it.

    Args:
        read: The result, whose plan names its source.
        outcome: The symbol's outcome.

    Returns:
        The source the rows name (`SymbolRows.source`), such as `"flat files and REST"`, or
        else the plan's source, such as `"REST"`.
    """
    if isinstance(outcome, SymbolRows) and outcome.source is not None:
        return outcome.source
    return TRANSPORT_LABELS[read.plan.transport]


def _shown_transport(transports: Iterable[str]) -> str | None:
    """Return the `Transport` of a followed read's `m.streams()` row: the sources used so far.

    Args:
        transports: The sources its reads used, as `_transport` names them.

    Returns:
        The one source, `FLAT_FILES_AND_REST` when both REST and flat files served rows, or
        None before any read.
    """
    used = set(transports)
    if len(used) > 1:
        return FLAT_FILES_AND_REST
    return used.pop() if used else None


class FollowCoordinator:
    """The session's follow coordinator thread and read pool; see the module docstring.

    Created with the session's core, which holds it; it starts no thread until the session's
    first followed read (`follow`), and keeps its thread and pool until `stop` (the session's
    close, or its collection), so one thread owns the per-symbol state for the session's life.
    It never refers to the core. `follow`, `post`, `wake`, and `stop` are safe from any thread
    and never block: they take its lock only around a queue put. Every handler runs on the
    coordinator thread.
    """

    __slots__ = (
        "_closed",
        "_exec_ctx",
        "_following",
        "_lock",
        "_pool",
        "_results",
        "_retries",
        "_retry_numbers",
        "_thread_prefix",
        "_worker",
    )

    def __init__(self, exec_ctx: ExecutionContext, thread_prefix: str, pool: ReadPool) -> None:
        """Prepare the coordinator; no thread starts yet.

        Args:
            exec_ctx: The session's execution context, entered by the coordinator thread and by
                each of its reads.
            thread_prefix: The session's thread prefix, such as `"massive #1"`.
            pool: The session's read pool, which runs the reads.
        """
        self._exec_ctx = exec_ctx
        self._thread_prefix = thread_prefix
        self._pool = pool
        # Guards the thread, the followed reads, and the stop. Held only around a queue put
        # or a pool submit, which never waits on another thread's lock.
        self._lock = threading.Lock()
        self._worker: QueueWorker[_Message] | None = None
        # Every followed read handed over (`follow`) and not yet forgotten, by `id`: what a
        # dying thread fails, including a read whose start never reached it.
        self._following: dict[int, CoordinatedRead] = {}
        self._closed = False
        # Touched only by the coordinator thread.
        self._results: dict[int, _Result] = {}
        # A heap of the retries waiting for their time: (due, number, result key, symbol),
        # due in `CLOCK.monotonic()` seconds; the number breaks ties in the order they were
        # made. An entry outlives a retry that a removal, a stop, or a re-add cancelled: when
        # its time comes, `_queue_retry` finds the symbol no longer waiting for it and drops
        # it. So a due retry costs a heap pop, and a batch with none costs a look at the top.
        self._retries: list[tuple[float, int, int, str]] = []
        self._retry_numbers = itertools.count()

    @property
    def thread(self) -> threading.Thread | None:
        """The coordinator thread, or None before the session's first followed read."""
        with self._lock:
            return None if self._worker is None else self._worker.thread

    @property
    def following(self) -> int:
        """How many followed reads the coordinator serves: counted until each one stops."""
        with self._lock:
            return len(self._following)

    def follow(self, read: CoordinatedRead, first: FirstRead | None) -> None:
        """Start coordinating a new followed read, starting the thread and pool if needed.

        Called on the caller thread once the result is registered, before its watch starts,
        so the result's own events queue behind it.

        Args:
            read: The result.
            first: The outcome of the `wait=True` read of the call-time symbols, or None
                for `wait=False`.
        """
        with self._lock:
            if self._closed:  # the session closed: the result's own check refuses it
                return
            if self._worker is None:
                self._worker = QueueWorker(
                    self._exec_ctx,
                    f"{self._thread_prefix} follow",
                    handle=self._handle,
                    timeout=self._next_due,
                    on_timeout=self._retry_due,
                    on_exit=self._exited,
                    on_death=self._died,
                )
            started = not self._following
            self._following[id(read)] = read
            self._worker.put(_Follow(read, first))
        if started:
            logger.debug("%s follow: a followed read starts", self._thread_prefix)

    def stop(self) -> None:
        """End the coordinator thread, without waiting; idempotent.

        Called by `SessionCore.close` and, through the reaper, by the collection of a session
        never closed. The events queued before it are handled first; later ones are dropped.
        """
        with self._lock:
            self._closed = True
            worker = self._worker
            self._worker = None
        if worker is not None:
            worker.stop()

    def post(self, message: _Message) -> None:
        """Queue an event for the coordinator; never blocks. Dropped when no result follows.

        Args:
            message: The event.
        """
        with self._lock:
            worker = self._worker
            if worker is not None:
                worker.put(message)

    def wake(self) -> None:
        """Make the coordinator check the retries that are due now (after `CLOCK` moved)."""
        self.post(_Wake())

    # --- Events from other threads, for a result -------------------------------------------

    def baseline(self, read: CoordinatedRead, symbols: frozenset[str], step: int) -> None:
        """Queue the symbol table's set of symbols when its watch started.

        Args:
            read: The result.
            symbols: The set.
            step: The update cycle the watch replayed the table at, or 0 for an empty set.
        """
        self.post(_Baseline(read, symbols, step))

    def changed(
        self, read: CoordinatedRead, added: frozenset[str], removed: frozenset[str], *, step: int
    ) -> None:
        """Queue one change of the symbol table's set (the watch's listener calls it).

        Args:
            read: The result.
            added: The symbols gained.
            removed: The symbols lost.
            step: The update cycle that made the change.
        """
        self.post(_Changed(read, added, removed, step))

    def skipped(self, read: CoordinatedRead, text: str | None) -> None:
        """Queue the text of the table's skipped values, or None once none is skipped.

        Args:
            read: The result.
            text: As `skipped_rows_text` writes it, or None.
        """
        self.post(_Skipped(read, text))

    def invalid(self, read: CoordinatedRead, message: str) -> None:
        """Queue a bad row under `RAISE`, or the symbol table's failure: the result fails.

        Args:
            read: The result.
            message: Why, as the watch says it.
        """
        self.post(_Invalid(read, message))

    def stop_following(self, read: CoordinatedRead) -> None:
        """Queue the result's stop (its blink table shut down, or it failed or closed).

        Args:
            read: The result.
        """
        self.post(_Stop(read))

    # --- The coordinator thread -----------------------------------------------------------

    def _handle(self, batch: list[_Message]) -> None:
        """Handle one batch of events in order, then start reads and write the rows.

        Each step for one result runs guarded (`_guarded`): an error fails that result alone,
        and the rest of the batch still runs, for it and for every other result.

        Args:
            batch: The events.
        """
        touched: dict[int, _Result] = {}
        for message in batch:
            if isinstance(message, _Wake):
                continue
            if isinstance(message, _Follow):
                result = _Result(message.read, decided_at_step=message.read.built_step)
                self._results[id(message.read)] = result
                self._guarded(result, "starting", functools.partial(self._start, result, message))
            else:
                found = self._results.get(id(message.read))
                if found is None:  # an event of a result already stopped
                    logger.debug(
                        "follow: an event for call %d after it stopped: %s",
                        message.read.call,
                        type(message).__name__,
                    )
                    continue
                result = found
                self._guarded(
                    result,
                    f"handling {type(message).__name__}",
                    functools.partial(self._apply, result, message),
                )
            touched[id(result.read)] = result
        self._retry_due(touched)
        self._schedule_and_show(touched)
        for result in touched.values():
            self._guarded(result, "forgetting it", functools.partial(self._forget, result))

    def _guarded(self, result: _Result, step: str, action: Callable[[], None]) -> None:
        """Run one step for one result; an error fails that result alone.

        The error is logged at ERROR with its traceback, the result's call and dataset, and the
        step, and the result's table fails with it (`_defect`). If failing the table raises too,
        that is logged as well. Either way the coordinator goes on with its next step.

        Args:
            result: The result the step is for.
            step: What the step does, for the log and the table's error, such as
                `"handling _Changed"`.
            action: The step.
        """
        try:
            action()
        except Exception as error:  # one result's defect; the others go on
            logger.exception(  # the traceback names the error and its text
                "call %d follow: %s failed (%s). %s",
                result.read.call,
                step,
                result.read.dataset,
                LIBRARY_BUG,
            )
            try:
                self._defect(result, step, error)
            except Exception:
                logger.exception(
                    "call %d follow: failing its table after %s failed raised too. %s",
                    result.read.call,
                    step,
                    LIBRARY_BUG,
                )

    def _schedule_and_show(self, touched: Mapping[int, _Result]) -> None:
        """Start each touched result's reads and write its rows, each step guarded.

        Args:
            touched: The results whose state changed.
        """
        for result in touched.values():
            self._guarded(result, "starting its reads", functools.partial(self._schedule, result))
            self._guarded(result, "writing its rows", functools.partial(self._show, result))

    def _apply(self, result: _Result, message: _Message) -> None:
        """Apply one event of a known result.

        Args:
            result: The result's state.
            message: The event.

        Raises:
            BrokenGuaranteeError: If the event is of a kind this method has no rule for.
        """
        if isinstance(message, (_Baseline, _Changed)):
            self._membership(result, message)
        elif isinstance(message, _Skipped):
            result.skipped = message.text
        elif isinstance(message, _Invalid):
            self._fail(result, SymbolRowError(message.message))
        elif isinstance(message, _Began):
            if result.ended:  # a read that started as the result stopped leaves it STOPPED
                logger.debug(
                    "call %d follow: the read of %s started after the stop; its symbols stay "
                    "as the stop left them",
                    result.read.call,
                    bounded_list(sorted(message.generations)),
                )
                return
            for name, generation in message.generations.items():
                symbol = result.symbols[name]
                if symbol.generation == generation and symbol.in_table:
                    symbol.state = SymbolReadState.LOADING
                    symbol.attempts += 1
                    result.dirty.add(name)
        elif isinstance(message, _Outcome):
            self._landed(result, message)
        elif isinstance(message, _Stop):
            self._stopped(result)
        else:
            raise BrokenGuaranteeError(library_bug(f"no rule applies {message!r} to a result"))

    def _defect(self, result: _Result, step: str, error: Exception) -> None:
        """Fail a result one of whose steps raised: its state may no longer be whole.

        The caller logged the defect with its traceback; this fails the result's table with a
        `MassiveError` that names it (`ErrorSource.LIBRARY`), marked as logged. The
        table's failure stops the result, so none of its symbols stays queued or loading.

        Args:
            result: The result's state.
            step: The step that raised, such as `"handling _Changed"`.
            error: What it raised.
        """
        failure = MassiveError(
            library_bug(
                f"{result.read.dataset}: the followed read failed {step}: "
                f"{type(error).__name__}: {error}"
            ),
            source=ErrorSource.LIBRARY,
        )
        mark_failure_logged(failure)  # the caller logged it, with its trace
        self._fail(result, failure)

    def _membership(self, result: _Result, message: _Baseline | _Changed) -> None:
        """Apply a change of the symbol table's set: removals first, then adds.

        Args:
            result: The result's state.
            message: The whole set when the watch started, or one change.
        """
        if isinstance(message, _Baseline):
            held = {name for name, symbol in result.symbols.items() if symbol.in_table}
            added, removed = message.symbols - held, frozenset(held - message.symbols)
            if result.awaits_call_time:
                result.awaits_call_time = False
                result.call_time = dict.fromkeys(sorted(message.symbols)) or None
                result.call_time_open = len(message.symbols)
        else:
            added, removed = message.added, message.removed
        result.decided_at_step = max(result.decided_at_step, message.step)
        now = _now_ns()
        for name in sorted(removed):
            self._remove(result, name)
        for name in sorted(added):
            self._add(result, name, now)
        logger.debug(
            "call %d follow: the symbol table gained %s and lost %s",
            result.read.call,
            bounded_list(sorted(added)),
            bounded_list(sorted(removed)),
        )

    def _start(self, result: _Result, message: _Follow) -> None:
        """Start following a new result: record what its caller read, if anything.

        Args:
            result: The result's new, empty state, already registered.
            message: The new result and its first read.
        """
        read = message.read
        if read.core.closed:  # the close failed the table while it was handed over
            result.ended = True
            logger.debug("call %d follow: the session closed before it started", read.call)
            return
        first = message.first
        if first is None:  # the call did not wait: its call-time symbols come with the watch
            result.awaits_call_time = True
        else:
            # The call warned of its failed symbols already: once per result and cause.
            result.warned.update(
                _cause(outcome.error)
                for outcome in first.outcomes.values()
                if isinstance(outcome, SymbolFailure)
            )
            for name, outcome in first.outcomes.items():
                symbol = _Symbol(
                    state=SymbolReadState.LOADING,
                    in_table=True,
                    generation=1,
                    added_at=first.began_at,
                    attempts=1,
                )
                result.symbols[name] = symbol
                if isinstance(outcome, SymbolRows):  # its rows are in the result's seed
                    symbol.kept = True
                self._record(result, name, outcome, began_at=first.began_at, seconds=first.seconds)
        logger.debug(
            "call %d follow: started (%s, reread %s, %d symbols read by the call)",
            read.call,
            TRANSPORT_LABELS[read.plan.transport],
            read.reread.name,
            0 if first is None else len(first.outcomes),
        )

    def _add(self, result: _Result, name: str, now: int) -> None:
        """A symbol entered the table: reuse its kept download under `NEVER`, or queue a read.

        A result that has failed or stopped reads nothing, so the symbol is `STOPPED` at once:
        queued, it would wait for a read that never starts.

        Args:
            result: The result's state.
            name: The symbol.
            now: The instant of the add, epoch nanoseconds.
        """
        symbol = result.symbols.get(name)
        if symbol is None:
            symbol = result.symbols[name] = _Symbol(
                state=SymbolReadState.QUEUED, in_table=True, generation=0, added_at=now
            )
        symbol.in_table = True
        symbol.generation += 1
        symbol.added_at = now
        result.dirty.add(name)
        if result.ended:
            symbol.state = SymbolReadState.STOPPED
            logger.debug(
                "call %d follow %s: added after the result failed or stopped; not read",
                result.read.call,
                name,
            )
            return
        if result.read.reread is Reread.NEVER and symbol.kept:
            symbol.state = SymbolReadState.DONE
            logger.debug(
                "call %d follow %s: back in the table; its download of %s is kept (NEVER), "
                "no request",
                result.read.call,
                name,
                _ns_text(required(symbol.read_at, f"{name}'s kept download's start")),
            )
            return
        symbol.state = SymbolReadState.QUEUED
        symbol.attempts = 0
        symbol.due = symbol.next_attempt_at = None
        self._enqueue(result, name)

    def _remove(self, result: _Result, name: str) -> None:
        """A symbol left the table: drop its pending read and retry, and its rows under `ALWAYS`.

        Its rows leave the result in the cycle its row left the table (the result joins the
        table), so the empty download only frees them.

        Args:
            result: The result's state.
            name: The symbol.
        """
        symbol = result.symbols[name]
        symbol.in_table = False
        symbol.generation += 1
        symbol.state = SymbolReadState.REMOVED
        # A call-time symbol deleted before it failed settles the check; a failed one only
        # leaves it.
        pending = result.call_time
        if pending is not None and name in pending and (pending.pop(name) is None or not pending):
            result.call_time = None
        symbol.due = symbol.next_attempt_at = None
        result.dirty.add(name)
        if symbol.waiting:
            symbol.waiting = False
            del result.queue[name]
        if result.read.reread is Reread.ALWAYS and symbol.kept:
            result.seq += 1
            symbol.parts = result.read.publish(
                name,
                result.seq,
                None,
                decided_at_step=result.decided_at_step,
                parts_before=symbol.parts,
            )
            symbol.kept = False
            symbol.read_at = None
            symbol.rows = 0

    def _enqueue(self, result: _Result, name: str) -> None:
        """Put a symbol at the end of its result's queue of symbols waiting for a read.

        Only a symbol that was not waiting comes here: a new add, or a retry come due.

        Args:
            result: The result's state.
            name: The symbol.
        """
        result.symbols[name].waiting = True
        result.queue[name] = None

    def _landed(self, result: _Result, message: _Outcome) -> None:
        """Apply a read's outcome: each symbol whose generation still matches takes it.

        Args:
            result: The result's state.
            message: The outcome.
        """
        if result.read.plan.one_scan:
            result.scanning = False
        for name in message.generations:
            result.symbols[name].reading = None  # its only read in flight has ended
        if result.read.stop_token.is_set():  # released and unused, or failed: drop it
            logger.debug("call %d follow: a read ended after the stop; dropped", result.read.call)
            return
        if message.table_error is not None:
            self._fail(result, message.table_error)
            return
        for name, generation in message.generations.items():
            symbol = result.symbols[name]
            if symbol.generation != generation or not symbol.in_table or result.ended:
                logger.debug(
                    "call %d follow %s: a read overtaken by a later add or removal ended; "
                    "its outcome is dropped",
                    result.read.call,
                    name,
                )
                continue
            self._record(
                result,
                name,
                message.outcomes[name],
                began_at=message.began_at,
                seconds=message.seconds,
                publish=True,
            )

    def _record(
        self,
        result: _Result,
        name: str,
        outcome: SymbolOutcome,
        *,
        began_at: int,
        seconds: float,
        publish: bool = False,
    ) -> None:
        """Record one symbol's outcome: its rows land, or it retries, or it fails.

        Args:
            result: The result's state.
            name: The symbol.
            outcome: Its rows or failure.
            began_at: When the read began, epoch nanoseconds.
            seconds: How long it took.
            publish: True to publish its download; False for a symbol already in the seed.
        """
        symbol = result.symbols[name]
        read = result.read
        transport = _transport(read, outcome)
        symbol.transport = transport
        result.transports.add(transport)
        result.dirty.add(name)
        if isinstance(outcome, SymbolRows):
            if publish:
                result.seq += 1
                symbol.parts = read.publish(
                    name,
                    result.seq,
                    outcome.table,
                    decided_at_step=result.decided_at_step,
                    parts_before=symbol.parts,
                )
            self._call_time_outcome(result, name, None)
            symbol.state = SymbolReadState.DONE
            symbol.kept = True
            symbol.rows = outcome.table.size
            symbol.read_at = began_at
            symbol.error = outcome.skipped  # why a read that succeeded gave it no rows, if so
            symbol.cause = None
            symbol.due = symbol.next_attempt_at = None
            logger.debug(
                "call %d %s: %d rows in %.1f s from %s (download %d, decided at step %d)",
                read.call,
                name,
                symbol.rows,
                seconds,
                transport,
                result.seq,
                result.decided_at_step,
            )
            return
        self._failed(result, name, outcome)

    def _failed(self, result: _Result, name: str, outcome: SymbolFailure) -> None:
        """Make a failed symbol `RETRYING` or `FAILED`, and log it.

        Args:
            result: The result's state.
            name: The symbol.
            outcome: Its failure.
        """
        symbol = result.symbols[name]
        error = outcome.error
        symbol.error = failure_text(error)
        symbol.cause = _cause(error)
        attempts = max(symbol.attempts, 1)
        if error.is_retryable and attempts <= len(RETRY_DELAYS):
            delay = RETRY_DELAYS[attempts - 1]
            symbol.state = SymbolReadState.RETRYING
            symbol.due = CLOCK.monotonic() + delay
            heapq.heappush(
                self._retries, (symbol.due, next(self._retry_numbers), id(result.read), name)
            )
            symbol.next_attempt_at = _now_ns() + int(delay * 1e9)
            logger.debug(
                "call %d follow %s: attempt %d failed in a way that may pass; read again in "
                "%.0f s: %s",
                result.read.call,
                name,
                attempts,
                delay,
                symbol.error,
            )
            return
        symbol.state = SymbolReadState.FAILED
        symbol.due = symbol.next_attempt_at = None
        first = symbol.cause not in result.warned
        result.warned.add(symbol.cause)
        mark_failure_logged(error)
        level = logging.WARNING if first else logging.DEBUG
        if error.is_bug and first:
            level = logging.ERROR
        logger.log(
            level,
            "%s call %d: %s failed after %d %s (%s), and is left out of the result; "
            "delete and re-add its row to read it again: %s",
            result.read.dataset,
            result.read.call,
            name,
            attempts,
            "attempt" if attempts == 1 else "attempts",
            "retryable" if error.is_retryable else "not retryable",
            symbol.error,
            exc_info=error if level == logging.ERROR else None,  # a bug, with its traceback
        )
        self._call_time_outcome(result, name, error)

    def _call_time_outcome(self, result: _Result, name: str, error: MassiveError | None) -> None:
        """Take a symbol's outcome toward the call-time check of a call that did not wait.

        The check fails the table when every call-time symbol ends `FAILED`, as a call that
        waits raises when every one of them failed (`_query.QueryBase._first_read`). Rows
        settle the check without failing; a failure read again later counts only once it is
        final.

        Args:
            result: The result's state.
            name: The symbol.
            error: Its final failure, or None for rows.
        """
        pending = result.call_time
        if pending is None or name not in pending:
            return
        if error is None:
            result.call_time = None
            logger.debug(
                "call %d follow %s: a call-time symbol has rows; the table no longer fails "
                "for its call-time symbols alone",
                result.read.call,
                name,
            )
            return
        pending[name] = error  # final: the symbol's next outcome needs a re-add, which drops it
        result.call_time_open -= 1
        if result.call_time_open:
            return
        # Every call-time symbol still in the table has failed.
        result.call_time = None
        logger.debug(
            "call %d follow: all %d call-time symbols failed; the table fails, as a call "
            "that waits raises",
            result.read.call,
            len(pending),
        )
        self._fail(result, every_symbol_failed([e for e in pending.values() if e is not None]))

    def _retry_due(self, touched: dict[int, _Result] | None = None) -> None:
        """Queue every retry whose time has come, taking each off the heap of retries.

        Args:
            touched: Receives each result with a retry queued; None when the coordinator woke
                for the retries alone, which then starts their reads itself.
        """
        now = CLOCK.monotonic()
        woke = touched is None
        touched = {} if touched is None else touched
        retries = self._retries
        while retries and retries[0][0] <= now:
            due, _, key, name = heapq.heappop(retries)
            result = self._results.get(key)
            if result is None:  # forgotten: the retry ended with its result
                logger.debug("follow %s: a retry of a forgotten result is dropped", name)
                continue
            self._guarded(
                result,
                "queuing a due retry",
                functools.partial(self._queue_retry, result, name, due=due, touched=touched),
            )
        if woke:
            self._schedule_and_show(touched)

    def _queue_retry(
        self, result: _Result, name: str, *, due: float, touched: dict[int, _Result]
    ) -> None:
        """Queue one symbol's retry that came due, unless it was cancelled meanwhile.

        Args:
            result: The result's state.
            name: The symbol.
            due: When the retry was due, monotonic seconds: a symbol whose retry a removal,
                a stop, or a re-add replaced no longer waits for this one.
            touched: Receives the result when the retry is queued.
        """
        # `get`: a result started after this retry's result was forgotten may reuse its key.
        symbol = result.symbols.get(name)
        if (
            result.ended
            or symbol is None
            or symbol.state is not SymbolReadState.RETRYING
            or symbol.due != due
        ):
            logger.debug(
                "call %d follow %s: a cancelled or replaced retry came due; dropped",
                result.read.call,
                name,
            )
            return
        symbol.state = SymbolReadState.QUEUED
        symbol.due = symbol.next_attempt_at = None
        result.dirty.add(name)
        self._enqueue(result, name)
        touched[id(result.read)] = result

    def _next_due(self) -> float | None:
        """Return the seconds until the next retry is due, for the queue wait.

        A cancelled retry still in the heap wakes the coordinator early, once, at its time.

        Returns:
            The seconds (0 or less: at once), or None when no retry waits.
        """
        return self._retries[0][0] - CLOCK.monotonic() if self._retries else None

    def _schedule(self, result: _Result) -> None:
        """Start the reads the result's queue allows: REST groups, or one flat-file scan.

        A REST read takes up to `FollowPlan.symbols_per_read` waiting symbols, in queue order.
        A symbol whose earlier read is still in flight waits for it.

        Args:
            result: The result's state.
        """
        if result.ended or (result.read.plan.one_scan and result.scanning):
            return
        ready = [name for name in result.queue if result.symbols[name].reading is None]
        if not ready:
            return
        for name in ready:
            del result.queue[name]
            result.symbols[name].waiting = False
        size = len(ready) if result.read.plan.one_scan else result.read.plan.symbols_per_read
        batches = [ready[i : i + size] for i in range(0, len(ready), size)]
        if result.read.plan.one_scan:
            result.scanning = True
        for batch in batches:
            generations = {name: result.symbols[name].generation for name in batch}
            for name, generation in generations.items():
                result.symbols[name].reading = generation
            self._submit(result.read, generations)

    def _submit(self, read: CoordinatedRead, generations: Mapping[str, int]) -> None:
        """Hand one read to the session's read pool, or drop it once the session stopped.

        `stop`, or the pool's own stop, may come while this thread still handles the events
        queued before it; a read of those is dropped, since every result is failed or about to
        fail. A submit only queues the task and at most starts a pool thread.

        Args:
            read: The result.
            generations: The symbols to read, at their generations.
        """
        with self._lock:
            closed = self._closed
        task = functools.partial(self._read, read, dict(generations))
        if not closed and self._pool.submit(task):
            return
        logger.debug(
            "call %d follow: the coordinator stopped; the read of %s is dropped",
            read.call,
            bounded_list(sorted(generations)),
        )

    def _read(self, read: CoordinatedRead, generations: Mapping[str, int]) -> None:
        """A pool task: read some symbols of one result and queue the outcome.

        Runs in the session's execution context. An error the read raises fails the result:
        a `MassiveError` as it is (a rejected key, a session closed during the read), anything
        else as a defect of the library, logged with its traceback.

        Args:
            read: The result.
            generations: The symbols, at the generations the read is for.
        """
        names = tuple(generations)
        self.post(_Began(read, generations))
        began_at = _now_ns()
        started = time.monotonic()
        outcomes: Mapping[str, SymbolOutcome] = {}
        table_error: MassiveError | None = None
        with entered(self._exec_ctx):
            try:
                outcomes = read.plan.read(names, read.stop_token)
            except MassiveError as error:
                table_error = error
            except BaseException as error:
                table_error = MassiveError(
                    library_bug(
                        f"{read.dataset}: the read of {', '.join(names)} failed: "
                        f"{type(error).__name__}: {error}"
                    ),
                    source=ErrorSource.LIBRARY,
                )
                mark_failure_logged(table_error)  # logged here, with its trace
                logger.exception("call %d follow: a read failed. %s", read.call, LIBRARY_BUG)
        self.post(
            _Outcome(
                read,
                generations,
                outcomes,
                began_at,
                time.monotonic() - started,
                table_error,
            )
        )

    def _fail(self, result: _Result, error: MassiveError) -> None:
        """Fail a whole result once, and stop following its symbols.

        Args:
            result: The result's state.
            error: The failure.
        """
        if result.ended:
            return
        result.ended = True
        log_failure(
            logger,
            error,
            "%s call %d: the followed read failed, and its table with it: %s",
            result.read.dataset,
            result.read.call,
        )
        result.read.fail_table(error)

    def _stopped(self, result: _Result) -> None:
        """The result stopped: every symbol still following becomes `STOPPED`.

        Args:
            result: The result's state.
        """
        result.ended = True
        for name, symbol in result.symbols.items():
            if symbol.state not in _ENDED:
                symbol.state = SymbolReadState.STOPPED
                symbol.due = symbol.next_attempt_at = None
                result.dirty.add(name)
        result.queue.clear()
        logger.debug("call %d follow: stopped", result.read.call)

    def _show(self, result: _Result) -> None:
        """Write the result's changed `m.symbol_reads()` rows and its `m.streams()` progress.

        The work is in the symbols whose row changed (`_Result.dirty`), not in all of them: the
        progress is updated from those alone (`_Progress.count`), and its symbol list and
        `Error` parts are made again only when the table's symbols, or its retrying and failed
        ones, changed.

        Args:
            result: The result's state.
        """
        read = result.read
        core = read.core
        progress = result.progress
        if result.dirty:
            now = _now_ns()
            names = sorted(result.dirty)
            result.dirty.clear()
            for name in names:
                progress.count(name, result.symbols[name])
            core.write_symbol_reads([self._row(result, name, now) for name in names])
        if progress.members_changed:
            progress.members_changed = False
            first = heapq.nsmallest(SYMBOLS_LISTED, progress.members)
            progress.symbols = symbol_list_text(len(progress.members), first)
        if progress.troubled_changed:
            progress.troubled_changed = False
            progress.notes = {
                NotePart.RETRYING_SYMBOLS: self._note(result, SymbolReadState.RETRYING),
                NotePart.FAILED_SYMBOLS: self._note(result, SymbolReadState.FAILED),
            }
        count, text = progress.symbols
        loading, rows_now, notes = progress.loading, progress.rows, progress.notes
        shown = (text, count, loading, rows_now, tuple(notes.items()), result.skipped)
        if shown == result.shown:
            return
        result.shown = shown
        transport = _shown_transport(result.transports)
        core.workers.update_followed_read(
            read.call,
            StreamUpdate(
                rows_published=rows_now,
                symbol_count=count,
                symbols=text,
                notes=notes,
                skipped=result.skipped,
                clear_skipped=result.skipped is None,
                loading=loading,
                transport=transport,
            ),
            loading=bool(loading),
        )

    @staticmethod
    def _row(result: _Result, name: str, now: int) -> SymbolReadRow:
        """Return a symbol's `m.symbol_reads()` row.

        Args:
            result: The result's state.
            name: The symbol.
            now: The row's `UpdatedAt`, epoch nanoseconds.

        Returns:
            The row.
        """
        symbol = result.symbols[name]
        return SymbolReadRow(
            stream_id=result.read.call,
            symbol=name,
            dataset=result.read.dataset,
            state=symbol.state,
            transport=symbol.transport,
            added_at=symbol.added_at,
            read_at=symbol.read_at if symbol.in_table or symbol.kept else None,
            rows=symbol.rows if symbol.in_table else 0,
            attempts=symbol.attempts,
            next_attempt_at=symbol.next_attempt_at,
            error=symbol.error,
            updated_at=now,
        )

    @staticmethod
    def _note(result: _Result, state: SymbolReadState) -> str | None:
        """Return the `Error` part of the table's symbols in one state, grouped by cause.

        Each cause shows the error text of its first symbol in name order, and its first
        symbols by name.

        Args:
            result: The result's state, whose progress groups the symbols.
            state: `RETRYING` or `FAILED`.

        Returns:
            Such as `"2 symbols failed (delete and re-add a row to read one again):
            EntitlementError: HTTP 403 ...: AAPL, MSFT"`, or None when no symbol is in `state`.
        """
        groups = result.progress.troubled[state]
        if not groups:
            return None
        total = sum(len(names) for names in groups.values())
        parts = []
        for cause in sorted(groups)[:_CAUSES_SHOWN]:
            names = heapq.nsmallest(_SYMBOLS_SHOWN, groups[cause])
            shown = ", ".join(names)
            hidden = len(groups[cause]) - _SYMBOLS_SHOWN
            more = f" (+{hidden} more)" if hidden > 0 else ""
            text = result.symbols[names[0]].error or ""
            text = text if len(text) <= _CAUSE_SHOWN else text[: _CAUSE_SHOWN - 1] + "…"
            parts.append(f"{text}: {shown}{more}")
        if len(groups) > _CAUSES_SHOWN:
            parts.append(f"{len(groups) - _CAUSES_SHOWN} more causes")
        noun = "symbol" if total == 1 else "symbols"
        if state is SymbolReadState.FAILED:
            head = f"{total} {noun} failed (delete and re-add a row to read one again)"
        else:
            head = f"{total} {noun} retrying after a failure that may pass"
        return f"{head}: {'; '.join(parts)}"

    def _forget(self, result: _Result) -> None:
        """Forget a result once its blink table has shut down: no event of it matters any more.

        Args:
            result: A result this batch touched.
        """
        key = id(result.read)
        if result.ended and result.read.shut_down and key in self._results:
            del self._results[key]
            with self._lock:
                self._following.pop(key, None)

    def _exited(self) -> None:
        """The coordinator thread ends (`stop`): forget every result, so none outlives it.

        Their tables are failed or about to fail: `stop` runs at the session's close, after its
        live tables failed, or once a collected session has no result left.
        """
        logger.debug("follow: the coordinator ends with %d results", len(self._results))
        self._results.clear()
        self._retries.clear()
        with self._lock:
            self._following.clear()

    def _died(self, death: BaseException) -> None:
        """The coordinator thread died: fail every followed read it served or was handed.

        Runs on the dying thread. Its results would stop following without an error otherwise.
        A `follow` that came before the thread was cleared queued its start to the dead thread,
        so every read handed over is failed, not only those the thread had started; a `follow`
        after it starts a new thread.

        Args:
            death: What ended the thread.
        """
        error = MassiveError(
            library_bug(f"the follow coordinator died ({death!r}); this table no longer updates"),
            source=ErrorSource.LIBRARY,
        )
        for result in self._results.values():
            result.ended = True
        self._results.clear()  # before the next thread can start: never two owners
        self._retries.clear()
        with self._lock:
            reads = list(self._following.values())
            self._following.clear()
            self._worker = None
        logger.debug("follow: the coordinator died; failing %d followed reads", len(reads))
        for read in reads:
            read.fail_table(error)


def _ns_text(value: int) -> str:
    """Return an instant in epoch nanoseconds as UTC text for a log line.

    Args:
        value: The instant.

    Returns:
        Such as `"2026-11-02T14:05:12Z"`.
    """
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(value / 1e9))
