"""`poll()`: live tables that re-read a REST dataset on an interval, one poller thread each.

`FetchQuery.poll(interval, mode=...)` returns a live table that one *poller* keeps current: a daemon
thread that runs a cycle (one `fetch()` of the dataset, through the REST transport's reader),
publishes its rows, and waits for the next cycle. Every mode publishes through one blink table's
publisher (`BlinkPublisher`, whose failure is delivered exactly once), created with the result
inside its result scope on the caller thread, so a failure can fail the table
(`publish_failure`) in every mode:

- `APPEND`: the publisher's blink table made append-only, fed only the rows not seen before. A
  row's identity is its values of the poll keys and then the revision keys
  (`DatasetSpec.poll_keys`, `DatasetSpec.revision_keys`); a cycle skips every row whose
  identity the poller already appended, or that an earlier row of the same cycle carries. The
  poller keeps every identity it appended for its whole life: each is a row the append-only
  table holds, so the set never outgrows the table, and no row is ever appended twice.
- `UPSERT`: the blink table's `last_by` on the dataset's poll keys (`DatasetSpec.poll_keys`):
  keyed, and a key never leaves. Poll keys that are every vendor column
  (`reference.related_companies`) still leave `TimestampInsert` to aggregate, so each distinct
  row is kept once, with the time of the latest poll that returned it.
- `REPLACE`: the same `last_by`, plus one *tombstone* row for each key the previous cycle had
  and this one lacks, marked in a hidden boolean column that the result filters on and drops;
  so a key absent from the latest cycle leaves the table, also when the cycle is empty. A cycle
  is published in one `add`, so its rows and its removals reach the table in one update. The
  `last_by` state keeps a hidden row for every key ever seen, as the `last_by()` of a stream
  does, so a `REPLACE` table over a churning key set (an options chain, the day's movers) grows
  with the keys it has ever held.

A poll key may be a list column (`long[]` or `String[]`, such as a 13F line's `OtherManagers`),
which Deephaven cannot key an aggregation on. A keyed table whose poll keys hold one keys its
`last_by` on a hidden text column instead (`LIST_KEY_COLUMN`): the JSON text of the row's list
keys, which the poller writes beside every row and tombstone, and which the result drops. Two
rows have the same text exactly when their lists are equal, item by item and in order, as
Deephaven compares two arrays and as `APPEND` compares identities: the same items in another
order, as Massive sent them, are another key. The library never sorts a list Massive sends.

Every row's `TimestampInsert` is when its page of the cycle arrived, so in a keyed table it is
the time of the latest poll that returned the row; it is never part of a row's identity.

A cycle reads a reactive symbol table once, with `Table.snapshot()` on the poller thread (never
in a listener), and requests the dataset's fetch path for the symbols it holds; a fan-out path
makes its requests on the poller's pool of `max_concurrency` threads. Each cycle updates the
table's row of `m.streams()`: `RowsPublished`, `RequestsPerCycle`, and `SymbolCount` for a
reactive table. Its problems are rows of `m.problems()`: a run of failed cycles is a
`poll_cycle_failed` span, cycles that end after their interval a `poll_cycle_late` span, and a
symbol-table value it skips a `noncanonical_symbols` span.

**Symbols left out.** By default one symbol never fails a whole table. A symbol of a reactive
table that routes to a path lacking a parameter the poll sends (`adjusted=False` on a crypto
indicator), and a symbol Massive answers with HTTP 404, follow
`ErrorPolicy.unavailable_symbols`: under `LOG`, the default, and `IGNORE` they have no rows in
the cycle (a `REPLACE` table drops their rows), and the REST reader reports each, with the
reason, as an `unavailable_symbols` problem of the table every cycle that leaves it out, so the
table never loses rows without saying why. Under `RAISE` the REST
reader raises `UnavailableSymbolError`, a `PermanentError`, which fails the table.

**Failures.** A cycle that raises `AuthenticationError`, `EntitlementError`, or a
`PermanentError` fails the table through `publish_failure`, marks the row `FAILED`
(`SessionCore.fail_stream`), and ends the poller. The permanent errors are `RowDataError` (a
read of which the library could read no row whole, a withheld row of a cause the policy sets to
`RAISE`, or a row that, in any mode, has a null poll key, which would merge unrelated rows; one
row the library cannot read whole is withheld and reported, and the table goes on),
`SymbolRowError` (a symbol table's row that is not Massive's ticker, under
`noncanonical_symbols=RAISE`; under `LOG` and `IGNORE` the row is skipped and listed in
`m.problems()`), and `UnavailableSymbolError` (a symbol Massive has no data for, under
`unavailable_symbols=RAISE`). The failure is logged at the level its `is_bug` gives
(`log_failure`): ERROR for a defect, naming whose it is, and WARNING otherwise.
A refusal (`AuthenticationError` or `EntitlementError`) logs one WARNING per session, dataset,
and kind of refusal, and each later `poll()` table refused the same way logs at DEBUG: every
poll makes its own requests and gets its own error, so twenty polls of a dataset the plan
does not include would otherwise log twenty WARNINGs for one cause. Any
other error, also one while publishing, opens the table's `poll_cycle_failed` span of
`m.problems()`, with the error and the time of the last good cycle (or, before one, of the
poll's start); the table keeps its rows and the next cycle runs as scheduled, and ends the span
once it succeeds. The span runs from the first failed cycle's start (that cycle published
nothing, whatever pages it received) to the good cycle, the time over which rows may be
missing. A run of failing cycles fails the
table, in every mode and naming the last good cycle, once one of its failures comes
`MassiveConfig.max_outage` seconds or more after its first failed cycle last made progress:
the rows since then may be missing, and a table that kept running would show stale data as if
it were current. The run starts at its first failed cycle's start, or at that cycle's latest
page when it received one, so one failure of an hourly poll, or of a slow cycle that kept
receiving pages, never fails it. A cycle that succeeds with no new row is a good cycle, so a
quiet dataset never fails. The first failing
cycle of a run of failures logs one WARNING, or for a bug one ERROR with its
trace; later ones log at DEBUG, and the first cycle that succeeds again logs the recovery at
INFO.

**Late cycles.** A cycle that ends after its interval (its requests are paced, or held by a
rate-limit pause) leaves the table late: the first one of a run logs one WARNING that names
`m.rate_limits()`, and the first cycle that ends in time again logs at INFO. A cycle blocks its
poller's thread, so the session's poll watch thread (`Pollers`) checks the running cycles every
`WATCH_INTERVAL` seconds: once one runs past its interval, the table's `poll_cycle_late` span
says "cycle running since <start>; last good cycle at <time>" until a cycle ends in time. A
late cycle that has received
no page for more than `max_outage` seconds (from its start, or from the start of the run of
failing cycles it continues, and never less than `WATCH_INTERVAL`) fails the table the same
way; one that keeps receiving pages, however slowly, never does, and with a `max_outage` of
zero only a cycle that ends in a failure fails the table.

**Lifetime.** A poll keeps polling while its table is in use, by the result or by anything
derived from it. Releasing the result (`m.release`, or garbage collection through the reaper)
only marks its row `RELEASED`; the poller ends when the poll's blink table shuts down, which
happens once nothing uses the table any more, when the table fails, or when the session closes,
which fails the table (`Poller.fail`) after stopping the poller. Every live poller is also
registered with the session's `Pollers`, which the session's REST closer stops, and a poller can
no longer be registered once that has run, so a `poll()` that races `close()` never starts a
poller the session would not stop. A stopped poller finishes a request already on the wire,
stops at once a request still waiting for the rate governor (its stop token is the `Cancel`
the REST transport hands `open_poll`, which every cycle's requests follow), and publishes
nothing more; a failed table keeps its rows. The poller holds the
session core only through a weak reference; the poll's record in the session keeps the session
alive while the poll runs (`WorkerTables.track`).
"""

from __future__ import annotations

import functools
import json
import logging
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Protocol

import pyarrow as pa
import pyarrow.compute as pc
from deephaven import dtypes
from deephaven.stream import blink_to_append_only

from deephaven_massive._datasets.row_times import INSERT
from deephaven_massive._datasets.symbols import AllSymbols, SymbolTable
from deephaven_massive._enums import PollMode, Recency
from deephaven_massive._errors import (
    LIBRARY_BUG,
    NEW_SESSION,
    AuthenticationError,
    BrokenGuaranteeError,
    EntitlementError,
    ErrorSource,
    MassiveError,
    PermanentError,
    RowDataError,
    SessionClosedError,
    failure_text,
    is_bug_of,
    library_bug,
    required,
    source_of,
    unmatched_form,
)
from deephaven_massive._log_limits import bounded_list, log_failure
from deephaven_massive._massive import Transport
from deephaven_massive._tables.columns import col_defs, dataset_table, describe
from deephaven_massive._tables.diagnostics import (
    StreamKind,
    StreamRow,
    StreamUpdate,
    describe_symbols,
    shown_query,
    utc_text,
)
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import make_result
from deephaven_massive._tables.problem_events import ProblemCause, SpanTracker
from deephaven_massive._tables.publisher import BlinkPublisher, blink_publisher
from deephaven_massive._tables.refused_values import refused_values_of
from deephaven_massive._tables.watchlist import SkippedValues, SymbolRowRule, read_symbols

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable, Sequence
    from datetime import timedelta

    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._datasets.schema import Schema
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._massive import DriftReport
    from deephaven_massive._tables.problem_events import Problem
    from deephaven_massive._tables.publisher import Publisher
    from deephaven_massive._tables.session_core import SessionCore

__all__ = [
    "CLOCK",
    "LIST_KEY_COLUMN",
    "LIVE_COLUMN",
    "CycleReader",
    "CycleRows",
    "PollClock",
    "Poller",
    "Pollers",
    "SymbolRowError",
    "build_table",
    "open_poll",
]

logger = logging.getLogger(__name__)

_ROW_TEXT: Final = 300
"""The longest rendering of a row in the error of a null poll key."""

LIVE_COLUMN: Final = "__MassiveLive"
"""The hidden column of a `REPLACE` table's publisher: False marks a tombstone. Dataset columns
are PascalCase wire names, so none can take this name."""

LIST_KEY_COLUMN: Final = "__MassiveListKey"
"""The hidden column of a keyed (`UPSERT` or `REPLACE`) table's publisher whose poll keys hold a
list column: the JSON text of the row's list keys, in key order, on which the `last_by` keys in
their place. Hidden like `LIVE_COLUMN`, and dropped from the result."""


class SymbolRowError(PermanentError):
    """A row of a `poll()` table's reactive symbol table that is not Massive's exact ticker.

    Raised only under `noncanonical_symbols=RAISE` (under `LOG` and
    `IGNORE` the row is skipped and listed in the row's `Error`), or for a symbol table that
    has failed. The table fails with it, since every later cycle would read the same row; its
    message names `dm.canonicalize_symbols`, which converts such a table. The table is the
    caller's, so its source is `ErrorSource.USER`, and it is a bug.
    """

    default_source = ErrorSource.USER
    default_is_bug = True


_PUBLISHED_MODES: Final = frozenset({PollMode.REPLACE, PollMode.UPSERT, PollMode.APPEND})
"""The modes a cycle publishes by; `AUTO` is resolved to one of them before a poller starts."""

_FATAL: Final = (AuthenticationError, EntitlementError, PermanentError)
"""The errors that fail a `poll()` table: retrying the next cycle cannot fix them."""

_LIBRARY_BUGS: Final = (True, ErrorSource.LIBRARY)
"""The kind (`_run_of`) of a run of cycles that fail of a bug in the library."""


def _run_of(error: Exception) -> tuple[bool, ErrorSource | None]:
    """Return the kind of failure a cycle's error starts or continues a run of.

    Failures that are no bug make one kind, whatever their source, so an outage whose
    cycles alternate between Massive's HTTP 503 and a lost connection logs one WARNING. A bug
    is a kind of its own for each source, so it is never hidden behind an outage.

    Args:
        error: The cycle's error.

    Returns:
        `(True, source)` for a bug; `(False, None)` for any other failure.
    """
    return (True, source_of(error)) if is_bug_of(error) else (False, None)


class PollClock(Protocol):
    """The time source of the pollers: the system clock, or a fake one in tests."""

    def monotonic(self) -> float:
        """Return a monotonic time in seconds."""
        ...

    def time(self) -> float:
        """Return the wall-clock time in seconds since the epoch."""
        ...

    def wait(self, event: threading.Event, seconds: float) -> bool:
        """Wait up to `seconds`, or until `event` is set.

        Args:
            event: Ends the wait early when set.
            seconds: The longest wait.

        Returns:
            True if `event` was set.
        """
        ...


class _SystemClock:
    """The real clock: `time.monotonic`, `time.time`, and `threading.Event.wait`."""

    @staticmethod
    def monotonic() -> float:
        """Return `time.monotonic()`."""
        return time.monotonic()

    @staticmethod
    def time() -> float:
        """Return `time.time()`."""
        return time.time()

    @staticmethod
    def wait(event: threading.Event, seconds: float) -> bool:
        """Wait on the event for up to `seconds`.

        Args:
            event: Ends the wait early when set.
            seconds: The longest wait.

        Returns:
            True if `event` was set.
        """
        return event.wait(seconds)


CLOCK: PollClock = _SystemClock()
"""The clock every new poller waits on; tests replace it so that no cycle waits for real."""


@dataclass(frozen=True, slots=True)
class CycleRows:
    """What one `poll()` cycle read: its rows, its cost, and the symbols it left out.

    Returned by the cycle reader (`CycleReader`, the REST transport's `read_cycle`) to the
    poller's thread; immutable.

    Attributes:
        rows: The rows in the dataset's schema.
        requests: How many requests the cycle made.
        patience: The most seconds of rate-limit pauses the cycle's reads would wait out (their
            patience), for the cycle's log line.
        left_out: The symbols the cycle has no rows for, each with why, such as
            `"X:BTCUSD (/v1/indicators/sma/{cryptoTicker} does not take ['adjusted'])"` or
            `"AAPL1 (HTTP 404 from /v3/trades/{stockTicker})"`, in the order found; empty for
            none, for the cycle's log line. The REST reader reports each in `m.problems()`.
    """

    rows: pa.Table
    requests: int
    patience: float
    left_out: tuple[str, ...] = ()


class CycleReader(Protocol):
    """Reads one cycle's rows: the REST transport's `RestReader.read_cycle`, bound to a dataset.

    It holds no session core, so a poller never keeps the core alive through it.
    """

    def __call__(
        self,
        *,
        stream_id: int,
        symbols: tuple[str, ...] | AllSymbols | None,
        values: tuple[str, ...] | None,
        record: Callable[[DriftReport], None],
        on_recency: Callable[[Recency], None],
        executor: ThreadPoolExecutor,
        cancel: threading.Event,
    ) -> CycleRows:
        """Request the dataset for the symbols; return the rows, the cost, and what was left out.

        Args:
            stream_id: The table's `StreamId`, which the cycle's problems carry.
            symbols: The cycle's static symbols: a sorted tuple, `ALL`, or None.
            values: The values a value table holds now (`Request.value_table`: underlyings
                or product codes), each read as if passed alone; None without one.
            record: Receives each page's drift, as the cycle's progress.
            on_recency: Receives what a page's recency labels show (`REALTIME` or `DELAYED`),
                from any of the cycle's threads, for the table's `Recency`.
            executor: The poller's pool for fan-out requests.
            cancel: The poller's stop token: once set, the cycle's requests stop waiting for
                the rate governor.

        Returns:
            The cycle's read (`CycleRows`).
        """
        ...


WATCH_INTERVAL: Final = 1.0
"""Seconds of real time between the running-cycle checks of a session's poll watch thread."""


class Pollers:
    """The live pollers of one session, stopped together when the session closes.

    The session's REST state holds one (`RestState.pollers`) and its `close` runs as a session
    closer. Once closed it refuses new pollers, so a `poll()` that races `close()` fails
    instead of starting a poller nothing would stop. Thread-safe: one leaf lock guards the set,
    the flag, and the watch thread's handle; pollers are stopped, and the watch thread woken,
    after it is released.

    While any poller is live, one daemon *watch* thread, named after the session
    (`"massive #1 poll watch"`), runs `check_running` every `WATCH_INTERVAL` seconds: a cycle
    blocks its poller's thread (in paced requests or rate-limit pauses), so only another thread
    can mark it late or fail it once it stops making progress. The first poller starts the
    thread; the last one to leave, or `close`, ends it, and the next poller starts a new one.
    Each thread waits on an event of its own, so one that is ending never stops its successor.
    """

    __slots__ = ("_closed", "_label", "_live", "_lock", "_wake", "_watch")

    def __init__(self, label: str) -> None:
        """Start with no poller and no watch thread.

        Args:
            label: The session's label, for the closed-session error.
        """
        self._label = label
        self._lock = threading.Lock()
        self._live: set[Poller] = set()
        self._closed = False
        self._watch: threading.Thread | None = None
        self._wake = threading.Event()  # set to end the current watch thread

    @property
    def closed(self) -> bool:
        """True once `close` has run."""
        with self._lock:
            return self._closed

    def add(self, poller: Poller) -> None:
        """Register a poller before it starts, starting the watch thread if none runs.

        Args:
            poller: The new poller.

        Raises:
            SessionClosedError: If the session has closed (`"MassiveSession #1 is closed; ..."`).
        """
        with self._lock:
            if self._closed:
                raise SessionClosedError(f"{self._label} is closed; {NEW_SESSION}")
            self._live.add(poller)
            watch = None
            if self._watch is None:
                self._wake = threading.Event()
                watch = self._watch = threading.Thread(
                    target=self._run_watch,
                    args=(self._wake,),
                    name=poller.watch_thread_name,
                    daemon=True,
                )
        if watch is not None:
            logger.debug("%s: started the poll watch thread", self._label)
            watch.start()

    def discard(self, poller: Poller) -> None:
        """Forget a poller whose table was released; an unknown poller is ignored.

        The last poller to leave ends the watch thread.

        Args:
            poller: The poller.
        """
        with self._lock:
            self._live.discard(poller)
            end = not self._live and self._watch is not None
            if end:
                self._watch = None
            wake = self._wake
        if end:
            self._end_watch(wake, "no poller is live")

    def close(self) -> None:
        """Stop every live poller and the watch thread, and refuse new pollers; never waits."""
        with self._lock:
            self._closed = True
            live = tuple(self._live)
            self._live.clear()
            end = self._watch is not None
            self._watch = None
            wake = self._wake
        if end:
            self._end_watch(wake, "the session closed")
        for poller in live:
            poller.stop()

    def check_running(self) -> None:
        """Check each live poller's running cycle (`Poller.check_running`) once.

        The watch thread calls it every `WATCH_INTERVAL` seconds; a test calls it directly. A
        check that raises is logged at ERROR as a defect, and the other pollers are checked.
        """
        with self._lock:
            live = tuple(self._live)
        for poller in live:
            try:
                poller.check_running()
            except Exception:
                logger.exception(
                    "%s: checking a poll()'s running cycle failed. %s", self._label, LIBRARY_BUG
                )

    def _end_watch(self, wake: threading.Event, reason: str) -> None:
        """End a watch thread; called once per thread, without the lock.

        Args:
            wake: The thread's event, read under the lock as the thread was given up.
            reason: Why it ends, for the DEBUG log.
        """
        wake.set()
        logger.debug("%s: ending the poll watch thread: %s", self._label, reason)

    def _run_watch(self, wake: threading.Event) -> None:
        """The watch thread: check the running cycles until its own event is set.

        Args:
            wake: Set when the thread should end.
        """
        while not wake.wait(WATCH_INTERVAL):
            self.check_running()


class _RecencySeen:
    """What one `poll()` cycle's pages showed about their recency, from any of its threads.

    Thread-safe: the cycle's fan-out threads add to it under its leaf lock.
    """

    __slots__ = ("_lock", "_seen")

    def __init__(self) -> None:
        """Start with nothing seen."""
        self._lock = threading.Lock()
        self._seen: set[Recency] = set()

    def add(self, recency: Recency) -> None:
        """Record what one page showed.

        Args:
            recency: `REALTIME` or `DELAYED`.
        """
        with self._lock:
            self._seen.add(recency)

    def shown(self) -> Recency | None:
        """Return the cycle's recency for the table's row.

        Returns:
            `DELAYED` when any page was labelled delayed, else `REALTIME` when a page was
            labelled real-time, else None: no label, so the row keeps what it shows.
        """
        with self._lock:
            seen = set(self._seen)
        if Recency.DELAYED in seen:
            return Recency.DELAYED
        return Recency.REALTIME if seen else None


class Poller:
    """One `poll()` table's publisher and the daemon thread that keeps it current.

    Created by `open_poll` on the caller thread, which builds the table (`build`), registers
    the poller, and then starts its thread (`start`). The thread enters the session's execution
    context for its whole life, and leaves the session's `Pollers` when it ends. Every other
    method is safe from any thread: `stop` and `release` only set an event and update
    registries, `fail` fails the table, and `check_running` (the watch thread's) marks or
    fails a cycle that runs late. The key state of `REPLACE` and `APPEND` belongs to
    the thread alone, so it takes no lock; the publish lock, below which only `registry_lock`
    is taken (a cycle's rows are converted to a table before it), keeps a failure and an `add`
    from interleaving and makes the failure once-only.
    """

    __slots__ = (
        "__weakref__",
        "_began",
        "_began_at",
        "_clock",
        "_core",
        "_cycles",
        "_exec_ctx",
        "_failed",
        "_failing",
        "_failing_at",
        "_failing_since",
        "_failures",
        "_gap_from",
        "_good_at",
        "_interval",
        "_key_names",
        "_keys",
        "_late",
        "_late_spans",
        "_list_keys",
        "_marked_late",
        "_max_concurrency",
        "_max_outage",
        "_mode",
        "_pollers",
        "_progress_at",
        "_publish_lock",
        "_published",
        "_publisher",
        "_read",
        "_request",
        "_rule",
        "_running",
        "_schema",
        "_seen",
        "_skipped",
        "_spec",
        "_stop",
        "_stream_id",
        "_succeeded",
        "_thread",
        "_thread_name",
        "_value_rule",
        "_watch_lock",
        "_watch_name",
        "_watched",
    )

    def __init__(
        self,
        core: SessionCore,
        *,
        spec: DatasetSpec,
        request: Request,
        interval: timedelta,
        mode: PollMode,
        stream_id: int,
        read: CycleReader,
        pollers: Pollers,
        clock: PollClock,
        publisher: Publisher,
        stop: threading.Event,
    ) -> None:
        """Prepare the poller; nothing is requested, and the thread is not started yet.

        Args:
            core: The session's core, held weakly.
            spec: The dataset, with a fetch path.
            request: The normalized arguments.
            interval: The time from one cycle's start to the next.
            mode: How cycles combine.
            stream_id: The table's `StreamId` in `m.streams()`.
            read: Reads one cycle's rows.
            pollers: The session's live pollers.
            clock: The time source of the waits between cycles.
            publisher: The table's publisher (`build_table`).
            stop: Set to stop the poller; the publisher's blink table sets it when it shuts
                down. Each cycle's requests listen to it, so a stopped poller never waits for
                a rate-limited slot.

        Raises:
            BrokenGuaranteeError: If `mode` is not one a cycle publishes by (`REPLACE`,
                `UPSERT`, or `APPEND`): the caller resolves `AUTO`, and a new mode needs a rule.
        """
        if mode not in _PUBLISHED_MODES:
            raise BrokenGuaranteeError(library_bug(f"{spec.name} poll() has no rule for {mode}"))
        # Returns the core, or None once it is freed: a weak reference.
        self._core: Callable[[], SessionCore | None] = weakref.ref(core)
        self._exec_ctx = core.exec_ctx
        # The poller thread's name; its fan-out pool's threads add " rest_<n>".
        self._thread_name = f"{core.thread_prefix} poll-{stream_id}"
        self._watch_name = f"{core.thread_prefix} poll watch"
        self._max_concurrency = core.config.max_concurrency
        self._max_outage = core.config.max_outage
        self._schema = core.dataset_schema(spec)
        self._spec = spec
        self._key_names = spec.poll_keys
        self._list_keys = list_keys(self._schema, self._key_names)
        self._request = request
        self._rule = SymbolRowRule.of(spec, request.on_error.noncanonical_symbols)
        self._value_rule = (
            None
            if request.value_table is None
            else SymbolRowRule.for_values(
                spec, request.value_table, request.on_error.noncanonical_symbols
            )
        )
        self._interval = interval.total_seconds()
        self._mode = mode
        self._stream_id = stream_id
        # The table's span problems: the values it skips and its run of failed cycles (only
        # the poller thread touches them), and its late cycles (guarded by the watch lock).
        self._skipped = SkippedValues(
            self._rule,
            stream_id=stream_id,
            values=self._value_rule,
            report=functools.partial(_report_problems, self._core),
        )
        _follow_refused(request.symbols, self._skipped)
        where = f"{spec.name} poll() stream {stream_id}"
        self._failures = SpanTracker(
            ProblemCause.POLL_CYCLE_FAILED, dataset=spec.name, stream_id=stream_id, where=where
        )
        self._late_spans = SpanTracker(
            ProblemCause.POLL_CYCLE_LATE, dataset=spec.name, stream_id=stream_id, where=where
        )
        self._read = read
        self._pollers = pollers
        self._clock = clock
        self._stop = stop
        self._publisher = publisher
        self._publish_lock = threading.Lock()
        self._failed = False
        # The poller thread's episodes: what the run of failing cycles fails of (`_run_of`;
        # None outside one), and whether cycles run late.
        self._failing: tuple[bool, ErrorSource | None] | None = None
        self._late = False
        # The last good cycle's start (wall-clock, for the text), or the poll's start before
        # one; and when the current run of failing cycles began (monotonic, and wall-clock),
        # or None outside one. Only the poller thread touches them.
        self._good_at = clock.time()
        self._succeeded = False
        self._failing_since: float | None = None
        self._failing_at = 0.0
        # When the current run's rows may have started to go missing: its first failed
        # cycle's start (wall-clock), since that cycle published nothing, whatever pages it
        # received. Only the poller thread touches it.
        self._gap_from = 0.0
        # The current cycle's start (monotonic, and wall-clock); only the poller thread
        # touches them.
        self._began = 0.0
        self._began_at = 0.0
        # The watch's view of the running cycle, guarded by the watch lock, a leaf lock but for
        # `registry_lock`, which `check_running` takes under it: whether a cycle runs; the
        # monotonic time since which it has made no progress (its start, or the start of the
        # run of failing cycles it continues, or its latest page); and whether it shows that
        # it runs late.
        self._watch_lock = threading.Lock()
        self._running = False
        self._progress_at = 0.0
        self._marked_late = False
        self._watched = (0.0, 0.0, "")  # the cycle's start (monotonic, wall) and good text
        self._keys: set[tuple[Hashable, ...]] = set()
        # The identities an APPEND table holds; only the poller thread touches it.
        self._seen: set[tuple[Hashable, ...]] = set()
        self._published = 0
        self._cycles = 0  # cycles started; only the poller thread touches it
        self._thread = threading.Thread(target=self._run, name=self._thread_name, daemon=True)

    @property
    def thread(self) -> threading.Thread:
        """The poller's thread."""
        return self._thread

    @property
    def watch_thread_name(self) -> str:
        """The name of its session's poll watch thread, such as `"massive #1 poll watch"`."""
        return self._watch_name

    @property
    def stopped(self) -> bool:
        """True once the poller has been asked to stop."""
        return self._stop.is_set()

    def start(self) -> None:
        """Start the thread; its first cycle runs at once."""
        self._thread.start()

    def stop(self) -> None:
        """Ask the thread to stop after its current request; never waits."""
        self._stop.set()

    def abandon(self) -> None:
        """Stop a poller whose thread will never start, and leave the session's pollers.

        For `WorkerTables.track`: a started thread leaves the pollers itself when it ends.
        """
        self._stop.set()
        self._pollers.discard(self)

    def release(self) -> None:
        """The result was released: mark the row `RELEASED`; the poller keeps running.

        Runs on the reaper thread as the result record's `on_released` callback, so it takes
        no lock beyond the leaf locks of the registries it updates. A table derived from the
        result may still use the poll; the poller stops when the poll's blink table shuts down.
        The session is alive: the poll's record keeps it in the live set until this has run.
        """
        required(self._core(), "the session").workers.release(self._stream_id)

    def fail(self, error: MassiveError) -> bool:
        """Stop the poller and fail its table, once: the session closed, or a thread died.

        The session's hook for the poll (`WorkerTables.track`), called outside every lock
        while the poll is registered, so the session is alive. The row is updated first
        (`SessionCore.fail_stream`, which keeps a `CLOSED` row as it is), then the table fails;
        a table that already failed is left alone.

        Args:
            error: The error the table fails with.

        Returns:
            False when the table had already failed.
        """
        self._stop.set()
        return self._fail_once(required(self._core(), "the session"), error)

    def _run(self) -> None:
        """Run cycles until stopped or failed, waiting `interval` from one start to the next.

        The poller leaves the session's `Pollers` when the thread ends, however it ends, and
        logs at DEBUG why it ended (`_end_reason`) and how many cycles it started.
        """
        reason = "its thread raised"
        try:
            with (
                entered(self._exec_ctx),
                ThreadPoolExecutor(self._max_concurrency, f"{self._thread_name} rest") as pool,
            ):
                while not self._stop.is_set():
                    began = self._began = self._clock.monotonic()
                    self._began_at = self._clock.time()
                    self._cycles += 1
                    if not self._cycle(pool):
                        break
                    took = self._clock.monotonic() - began
                    self._note_pace(took)
                    self._clock.wait(self._stop, max(self._interval - took, 0.0))
            reason = self._end_reason()
        finally:
            self._pollers.discard(self)
            logger.debug(
                "%s poll() stream %d stopped after %d cycles: %s",
                self._spec.name,
                self._stream_id,
                self._cycles,
                reason,
            )

    def _end_reason(self) -> str:
        """Say why the poller's loop ended, for the log line of its end.

        Returns:
            `"its table failed"` when the table failed (a fatal cycle, the session's close, or
            a library thread's death); `"its session is gone"` when the session's core was
            collected; otherwise `"it was stopped: its table shut down"`, since only the stop
            token ends the loop then.
        """
        with self._publish_lock:
            failed = self._failed
        if failed:
            return "its table failed"
        if self._core() is None:
            return "its session is gone"
        return "it was stopped: its table shut down"

    def _note_pace(self, took: float) -> None:
        """Report the start and the end of a run of cycles that end after their interval.

        A late cycle leaves the table behind the interval the user asked for: its requests are
        paced at a Basic subscription's limit, or held by a rate-limit pause. A run of them is
        one `poll_cycle_late` span of `m.problems()`, from the first late cycle's start (or
        from the watch's mark of a cycle running past its interval) to the first cycle in time
        again. The first late cycle of a run logs one WARNING, and the first cycle in time
        again logs at INFO.

        Args:
            took: The cycle's duration, in seconds.
        """
        late = took > self._interval
        if late and not self._late:
            logger.warning(
                "%s poll() stream %d: a cycle took %.0f s, longer than the %g s interval, so the "
                "table updates late; m.rate_limits() shows whether its requests are paced or "
                "paused by Massive's rate limit, and m.problems() lists the run",
                self._spec.name,
                self._stream_id,
                took,
                self._interval,
            )
        elif self._late and not late:
            logger.info(
                "%s poll() stream %d: cycles end within the %g s interval again",
                self._spec.name,
                self._stream_id,
                self._interval,
            )
        self._late = late
        detail = f"a cycle took {took:.0f} s, longer than the {self._interval:g} s interval"
        self._show_late(detail if late else None, since=self._began_at)

    def _show_late(self, detail: str | None, *, since: float) -> None:
        """Open, update, or end the table's `poll_cycle_late` span, under the watch lock.

        The poller thread and the watch thread both call it; the lock keeps their snapshots in
        the order they were taken.

        Args:
            detail: Why the table is late, or None once a cycle ends in time.
            since: When the late run began, wall-clock seconds, for a span that opens now.
        """
        core = self._core()
        if core is None:
            return
        with self._watch_lock:
            spans = self._late_spans.update(
                {} if detail is None else {"late": (None, None, detail, 0)},
                at=int(self._clock.time() * 1e9),
                since=int(since * 1e9),
            )
            if spans is not None:
                core.problems([spans])

    def _watch_begin(self) -> None:
        """Show the watch thread that a cycle starts, with what its late mark names.

        Until a page arrives, the cycle has made no progress since its start, or, when it
        continues a run of failing cycles, since that run's first cycle started.
        """
        good = self._good_text()
        since = self._began if self._failing_since is None else self._failing_since
        with self._watch_lock:
            self._running, self._marked_late = True, False
            self._watched = (self._began, self._began_at, good)
            self._progress_at = since

    def _watch_end(self) -> float:
        """Show the watch thread that the cycle has ended; idempotent.

        Called before the cycle updates the table's row, so a late mark never lands after the
        row's own update and outlives the cycle.

        Returns:
            The monotonic time of the cycle's last progress: its latest page, or else the
            time `_watch_begin` started counting from.
        """
        with self._watch_lock:
            self._running = False
            return self._progress_at

    def _page_received(self, drift: DriftReport) -> None:
        """A page of the cycle arrived: count it as progress.

        The REST reader calls it for every page, on the poller's thread or a fan-out thread;
        the reader reports the page's drift itself.

        Args:
            drift: The page's drift, unused here.
        """
        del drift
        now = self._clock.monotonic()
        with self._watch_lock:
            self._progress_at = max(self._progress_at, now)

    def check_running(self) -> None:
        """Mark a cycle that runs past its interval, and fail one that stopped making progress.

        Called by the session's poll watch thread (`Pollers`), never by the poller's own
        thread, which the cycle blocks. A running cycle that has passed its interval opens the
        table's `poll_cycle_late` span: "cycle running since <start>; last good cycle at
        <time>", until a cycle ends in time. A cycle that has passed its interval and received no
        page for more than `MassiveConfig.max_outage` seconds, and more than `WATCH_INTERVAL`
        (counted from its start, or from the start of the run of failing cycles it continues,
        until its first page), fails the table; with a `max_outage` of zero, no running cycle
        fails here, only one that ends in a failure. Such a failure fails the
        table with a `MassiveError` (`ErrorSource.NETWORK`) and stops the poller: its rows may
        be missing while the table looks current. A cycle that keeps receiving pages, however
        slowly its requests are paced or paused, never fails here.
        """
        now = self._clock.monotonic()
        with self._watch_lock:
            if not self._running:
                return
            # The running cycle's frame holds the core (`_cycle`).
            core = required(self._core(), "the session")
            began, began_at, good = self._watched
            if now - began <= self._interval:
                return
            stalled = now - self._progress_at
            # One check's spacing is the least stall it can measure; a zero limit fails only
            # cycles that end in a failure (`_carry_on`), never a running one between pages.
            failing = self._max_outage > 0 and stalled > max(self._max_outage, WATCH_INTERVAL)
            marking = not failing and not self._marked_late
            if marking:
                self._marked_late = True
                note = f"cycle running since {utc_text(began_at)}; {good}"
                # Under the watch lock, so the cycle's own report follows it.
                spans = self._late_spans.update(
                    {"late": (None, None, note, 0)},
                    at=int(self._clock.time() * 1e9),
                    since=int(began_at * 1e9),
                )
                if spans is not None:
                    core.problems([spans])
            if failing:
                self._running = False  # fails once
                # Stopped under the watch lock: the cycle's `_watch_end` comes after, so it
                # sees the stop and publishes nothing; the cycle's waits end too.
                self._stop.set()
        if marking:
            logger.debug(
                "%s poll() stream %d: the cycle started at %s runs past the %g s interval; "
                "marked its row",
                self._spec.name,
                self._stream_id,
                utc_text(began_at),
                self._interval,
            )
        if not failing:
            return
        error = MassiveError(
            f"{self._spec.name} poll(): {good}; the cycle running since {utc_text(began_at)} "
            f"has received nothing for {stalled:.0f} s, longer than MassiveConfig.max_outage "
            f"({self._max_outage:g} s); the rows since then may be missing, so this table "
            "stopped; call the verb again for a new live table",
            source=ErrorSource.NETWORK,
        )
        logger.debug(
            "%s poll() stream %d: no page for %.0f s, max_outage is %g s; failing it",
            self._spec.name,
            self._stream_id,
            stalled,
            self._max_outage,
        )
        with entered(self._exec_ctx):
            self._fail_fatal(core, error)

    def _symbols(self) -> tuple[tuple[str, ...] | AllSymbols | None, tuple[str, ...]]:
        """Return the cycle's symbols, reading a reactive table once.

        A row of a reactive table that is not Massive's exact ticker follows the session's
        `ErrorPolicy.noncanonical_symbols` (`read_symbols`): under `RAISE` it fails the table;
        under `LOG` and `IGNORE` it is skipped, and returned so the cycle can list it.

        Returns:
            The request's symbols, or for a reactive table the symbols it holds now, sorted
            (null rows are ignored); and the values skipped this cycle, sorted (empty but for
            a reactive table under `LOG` or `IGNORE`).

        Raises:
            SymbolRowError: If a row of the reactive table is not Massive's exact ticker under
                `RAISE`, or the table has failed; the poll's table fails with it.
        """
        symbols = self._request.symbols
        if not isinstance(symbols, SymbolTable):
            return symbols, ()
        try:
            rows = read_symbols(self._rule, symbols.table, stream_id=self._stream_id)
        except ValueError as error:
            raise SymbolRowError(str(error)) from None
        return rows.symbols, rows.skipped

    def _values(self) -> tuple[tuple[str, ...] | None, tuple[str, ...]]:
        """Return the values a value table holds now, read once for the cycle.

        A row that is not a value the dataset takes (an underlying that is not a stock or index
        ticker) follows `ErrorPolicy.noncanonical_symbols`, as a symbol table's row does.

        Returns:
            None and no skipped value for a request without a value table; else the table's
            values, sorted (null rows are ignored), and the values skipped this cycle.

        Raises:
            SymbolRowError: If a row is refused under `RAISE`, or the table has failed; the
                poll's table fails with it.
        """
        values = self._request.value_table
        if values is None or self._value_rule is None:
            return None, ()
        try:
            rows = read_symbols(self._value_rule, values.table, stream_id=self._stream_id)
        except ValueError as error:
            raise SymbolRowError(str(error)) from None
        return rows.symbols, rows.skipped

    def _cycle(self, pool: ThreadPoolExecutor) -> bool:
        """Run one cycle: read, publish, and update the table's row.

        Every step is guarded: a failure while publishing is handled like one while reading,
        so it never ends the poller unseen.

        Args:
            pool: The poller's fan-out pool.

        Returns:
            False when the poller should end: the session's core is gone, the poller was
            stopped during the cycle, or the cycle failed the table.
        """
        core = self._core()
        if core is None:
            return False
        self._watch_begin()  # while it runs, this frame holds the core for `check_running`
        try:
            return self._publish_cycle(core, pool)
        except _FATAL as error:
            self._watch_end()
            if not self._stop.is_set():
                self._fail_fatal(core, error)
            return False
        except MassiveError as error:  # any other library error: the next cycle runs
            return self._carry_on(core, error)
        except Exception as error:  # a defect: logged with its trace, and the next cycle runs
            if not self._stop.is_set() and self._failing != _LIBRARY_BUGS:
                logger.exception(
                    "%s poll() stream %d: the cycle failed. %s",
                    self._spec.name,
                    self._stream_id,
                    LIBRARY_BUG,
                )
            return self._carry_on(core, error)
        finally:
            self._watch_end()

    def _publish_cycle(self, core: SessionCore, pool: ThreadPoolExecutor) -> bool:
        """Read the cycle's rows, publish them, and update the table's row.

        Args:
            core: The session's core.
            pool: The poller's fan-out pool.

        Returns:
            False if the poller was stopped while it read, so nothing was published.
        """
        symbols, skipped_symbols = self._symbols()
        values, skipped_values = self._values()
        record = self._page_received
        seen = _RecencySeen()
        read = self._read(
            stream_id=self._stream_id,
            symbols=symbols,
            values=values,
            record=record,
            on_recency=seen.add,
            executor=pool,
            cancel=self._stop,
        )
        self._watch_end()  # before the row's update, so no late mark can follow it
        if self._stop.is_set():
            return False
        published = self._publish(read.rows)
        self._published += published
        logger.debug(
            "stream %d: %s poll() cycle: %d requests, %d rows read, %d published in %.3f s, "
            "waiting out at most %.0f s of pauses; left out: %s",
            self._stream_id,
            self._spec.name,
            read.requests,
            read.rows.num_rows,
            published,
            self._clock.monotonic() - self._began,
            read.patience,
            bounded_list(read.left_out) if read.left_out else "none",
        )
        update = StreamUpdate(
            rows_published=self._published,
            requests_per_cycle=read.requests,
            symbol_count=_symbol_count(symbols),
            recency=seen.shown(),
        )
        core.update_streams({self._stream_id: update})
        # A successful cycle ends an earlier run of failed cycles; the skipped values are its.
        ended = self._failures.update({}, at=int(self._began_at * 1e9))  # where rows flow again
        if ended is not None:
            core.problems([ended])
        self._skipped.update(skipped_symbols, values=skipped_values)
        self._good_at, self._succeeded, self._failing_since = self._began_at, True, None
        if self._failing is not None:
            self._failing = None
            logger.info(
                "%s poll() stream %d: a cycle succeeded again; rows from %s to %s may be "
                "missing, recorded in m.problems()",
                self._spec.name,
                self._stream_id,
                utc_text(self._gap_from),
                utc_text(self._began_at),
            )
        return True

    def _fail_fatal(self, core: SessionCore, error: MassiveError) -> None:
        """Fail the table with a cycle's fatal error; a failure that raises is logged at ERROR.

        The poller ends either way. Without the guard, an exception from the failure itself
        (Deephaven's `publish_failure`) would end the poller's thread through
        `threading.excepthook`, past the library's log, with its row already `FAILED` and its
        table never failed.

        Args:
            core: The session's core.
            error: The cycle's fatal error.
        """
        try:
            self._fail_once(core, error)
        except Exception:
            logger.exception(
                "%s poll() stream %d: could not fail its table with %s. %s",
                self._spec.name,
                self._stream_id,
                error,
                LIBRARY_BUG,
            )

    def _carry_on(self, core: SessionCore, error: Exception) -> bool:
        """Report a cycle's error as the table's problem, or fail the table once it is too late.

        The table keeps its rows, and its `poll_cycle_failed` span names the cycle's error and
        the last good cycle's time. A failure that comes `MassiveConfig.max_outage` seconds or
        more after the
        run's start fails the table instead (`_outage_failure`), in every mode, and the poller
        ends. The run starts at its first failed cycle's last progress: the cycle's start, or
        its latest page. Counting from there, not from the last good cycle, keeps one transient
        failure from failing an hourly poll, or a slow cycle that kept receiving pages.

        The first failure of a run is logged once, by its text, which names the HTTP status,
        the path, and the request id: a `MassiveError` that is no bug at WARNING, and a
        defect at ERROR (an exception that is not a `MassiveError` has been logged with its
        trace by the caller). Every later failure of the run of the same kind logs at
        DEBUG, until a cycle succeeds again (`_publish_cycle`); a failure of another kind
        (`_run_of`) starts a new run, so a bug is never hidden behind an outage.

        Args:
            core: The session's core.
            error: The cycle's error.

        Returns:
            Whether the poller carries on: False once it has been stopped, or once the table
            failed.
        """
        progress = self._watch_end()  # before the row's update, so no late mark follows it
        if self._stop.is_set():
            return False
        if self._failing_since is None:  # a run of failing cycles begins with this cycle
            # From its last progress: its start, or its latest page.
            self._failing_since = progress
            self._failing_at = self._began_at + (progress - self._began)
            self._gap_from = self._began_at  # it published nothing from its start
        down = self._clock.monotonic() - self._failing_since
        if down >= self._max_outage:
            self._fail_fatal(core, self._outage_failure(error, down))
            return False
        run = _run_of(error)
        if self._failing == run:
            logger.debug("%s poll() stream %d: %s", self._spec.name, self._stream_id, error)
        elif isinstance(error, MassiveError):
            level = logging.ERROR if error.is_bug else logging.WARNING
            logger.log(
                level,
                "%s poll() stream %d: a cycle failed, and the next one runs as scheduled: %s",
                self._spec.name,
                self._stream_id,
                failure_text(error),
                exc_info=error if error.is_bug else None,  # a bug, with its traceback
            )
        self._failing = run
        spans = self._failures.update(
            {"failed": (None, None, f"{error}; {self._good_text()}", 0)},
            at=int(self._clock.time() * 1e9),
            since=int(self._gap_from * 1e9),
        )
        if spans is not None:
            core.problems([spans])
        return True

    def _good_text(self) -> str:
        """Name the last good cycle's time, or the poll's start before one.

        Returns:
            The text, such as `"last good cycle at 2026-10-30T14:03:05Z"`.
        """
        at = utc_text(self._good_at)
        if self._succeeded:
            return f"last good cycle at {at}"
        return f"no good cycle since the poll started at {at}"

    def _outage_failure(self, error: Exception, down: float) -> MassiveError:
        """Return the error the table fails with: its cycles have failed for `max_outage`.

        Args:
            error: The latest cycle's error, quoted in the message; its source, `is_retryable`, and
                `is_bug` are the failure's.
            down: Seconds since the run of failing cycles began.

        Returns:
            The error, naming the last good cycle, the limit, and what to do, and chained to
            the latest error, so a bug's traceback reaches the log.
        """
        logger.debug(
            "%s poll() stream %d: cycles have failed for %.0f s, max_outage is %g s; failing it",
            self._spec.name,
            self._stream_id,
            down,
            self._max_outage,
        )
        failure = MassiveError(
            f"{self._spec.name} poll(): {self._good_text()}; every cycle since "
            f"{utc_text(self._failing_at)} has failed, for {down:.0f} s, longer than "
            f"MassiveConfig.max_outage ({self._max_outage:g} s); the rows since then may be "
            "missing, so this table stopped; call the verb again for a new live table. The "
            f"latest error: {error}",
            source=source_of(error),
            is_retryable=isinstance(error, MassiveError) and error.is_retryable,
            is_bug=is_bug_of(error),
        )
        failure.__cause__ = error  # its log record carries the latest error's traceback too
        return failure

    def _publish(self, table: pa.Table) -> int:
        """Publish one cycle's rows as the mode shapes them.

        `REPLACE` adds a tombstone for each key the cycle lost; `APPEND` keeps only the rows
        not seen before (`_fresh`). `UPSERT` publishes the rows as they are.

        Args:
            table: The cycle's rows in the dataset's schema.

        Returns:
            How many of the cycle's rows were published (tombstones not counted); 0 once the
            table has failed, when nothing is published.
        """
        self._check_keys(table)
        keys = None
        identities = None
        published: int = table.num_rows
        if self._mode is PollMode.REPLACE:
            table, keys = self._replace(table)
        elif self._mode is PollMode.APPEND:
            table, identities = self._fresh(table)
            published = table.num_rows
        # UPSERT publishes every row as it came; `__init__` admits no other mode.
        if self._list_keys and self._mode is not PollMode.APPEND:
            table = table.append_column(LIST_KEY_COLUMN, _list_key_text(table, self._list_keys))
        # Converted before the publish lock, so the lock is held only for the add: converting a
        # large cycle takes a while, and a close failing the table would wait for it. The
        # conversion builds static tables and takes no update-graph lock.
        rows = dataset_table(table, self._schema.date_columns) if table.num_rows else None
        with self._publish_lock:
            if self._failed:  # the session closed after the stop check: publish nothing
                return 0
            if rows is not None:
                self._publisher.add(rows)
        # The key state changes only once the cycle is published: if it failed, the next
        # cycle still tombstones the keys this one dropped, or appends the rows it skipped.
        if keys is not None:
            self._keys = keys
        if identities is not None:
            self._seen |= identities
        return published

    def _check_keys(self, table: pa.Table) -> None:
        """Fail the cycle if any row has a null (or NaN) poll key, in every mode.

        A null key would silently merge unrelated rows: one keyed row for all of them, or
        every later row with the same null skipped by `APPEND`; a NaN never equals itself, so
        `APPEND` would append its row again every cycle. Like a missing required key, it is a
        data failure, so it fails the table. The dataset's nullable poll keys
        (`DatasetSpec.nullable_poll_keys`, such as an option's `PutCall`, empty for common
        stock) and its revision keys are not checked: a null there is a value.

        Args:
            table: The cycle's rows.

        Raises:
            RowDataError: Naming the dataset, the key column, the first such row's position in
                the cycle, and that row's non-null values but its `TimestampInsert`, truncated
                to `_ROW_TEXT` characters, so the row can be found although it was never
                published.
        """
        nullable = self._spec.nullable_poll_keys
        for name in (k for k in self._key_names if k not in nullable):
            missing = pc.is_null(table.column(name), nan_is_null=True)
            row = pc.index(missing, True).as_py()
            if row >= 0:
                (values,) = table.slice(row, 1).to_pylist()
                # The row as Massive sent it: the arrival time is the library's own.
                text = repr({k: v for k, v in values.items() if v is not None and k != INSERT})
                text = text if len(text) <= _ROW_TEXT else text[: _ROW_TEXT - 3] + "..."
                raise RowDataError(
                    f"{self._spec.name} poll(): row {row} of the cycle has a null poll key "
                    f"{name!r}: {text}"
                )

    def _fresh(self, table: pa.Table) -> tuple[pa.Table, set[tuple[Hashable, ...]]]:
        """Keep the rows of an `APPEND` cycle whose identity the poller has not appended yet.

        A row's identity is its poll keys followed by its revision keys. A row whose identity
        an earlier row of the same cycle carries is dropped too, so the first one wins. The
        poller's set is left as is; `_publish` adds to it once the cycle is published.

        Args:
            table: The cycle's rows.

        Returns:
            The rows to append, and every identity of the cycle (those already held are
            harmless to add again).
        """
        names = (*self._key_names, *self._spec.revision_keys)
        identities = zip(*(_hashable(table.column(name)) for name in names), strict=True)
        taken: set[tuple[Hashable, ...]] = set()
        keep = []
        for identity in identities:
            keep.append(identity not in self._seen and identity not in taken)
            taken.add(identity)
        return table.filter(pa.array(keep, pa.bool_())), taken

    def _replace(self, table: pa.Table) -> tuple[pa.Table, set[tuple[Hashable, ...]]]:
        """Mark a cycle's rows live and add one tombstone per key the last published cycle had.

        The poller's key set is left as is; `_publish` replaces it once the cycle is published.

        Args:
            table: The cycle's rows.

        Returns:
            The rows with the live column, then the tombstones; and the cycle's key set.
        """
        names = self._key_names
        keys = set(zip(*(_hashable(table.column(name)) for name in names), strict=True))
        gone = self._keys - keys
        rows = table.append_column(LIVE_COLUMN, pa.array([True] * table.num_rows, pa.bool_()))
        if not gone:
            return rows, keys
        ordered = sorted(gone, key=repr)
        columns = []
        for column in rows.schema:
            if column.name in names:
                position = names.index(column.name)
                columns.append(pa.array([key[position] for key in ordered], column.type))
            elif column.name == LIVE_COLUMN:
                columns.append(pa.array([False] * len(ordered), pa.bool_()))
            else:
                columns.append(pa.nulls(len(ordered), column.type))
        tombstones = pa.Table.from_arrays(columns, schema=rows.schema)
        return pa.concat_tables([rows, tombstones]), keys

    def _log_failure(self, core: SessionCore, error: MassiveError) -> None:
        """Log the failure that fails the table, at the level its `is_bug` gives (`log_failure`).

        A refusal is logged at WARNING only for the session's first `poll()` table of the
        dataset refused with that kind of error and status (`SessionCore.warnings`), and at
        DEBUG for every later one; every refusal is also logged at DEBUG with its stream.

        Args:
            core: The session's core.
            error: The failure.
        """
        name, stream_id = self._spec.name, self._stream_id
        if not isinstance(error, AuthenticationError | EntitlementError):
            log_failure(logger, error, "%s poll() stream %d failed: %s", name, stream_id)
            return
        log_failure(
            logger,
            error,
            "%s poll() stream %d failed: %s",
            name,
            stream_id,
            expected=logging.DEBUG,
        )
        core.warnings.log(
            f"poll refused\0{name}\0{type(error).__name__}\0{error.status}",
            "%s poll() stream %d failed: %s (each later poll() table of %s refused this way "
            "logs at DEBUG)",
            name,
            stream_id,
            failure_text(error),
            name,
        )

    def _fail_once(self, core: SessionCore, error: MassiveError) -> bool:
        """Mark the row `FAILED`, then fail the table, once; log it at the level its `is_bug` gives.

        The failure is recorded first, as `StreamSink.fail` does for a stream, so the row is on
        its way to `m.streams()` before anyone can see the table failed. When the session's
        close came first, the table fails with the close's error instead and nothing is logged,
        so the table and its `CLOSED` row name the same cause. The table counts as failed even
        when `publish_failure` raises (the exception then propagates): its row already reads
        `FAILED`, so it publishes no more rows, and the failure is not tried again.

        Args:
            core: The session's core.
            error: The failure.

        Returns:
            False when the table had already failed.
        """
        with self._publish_lock:
            if self._failed:
                return False
            closed = core.fail_stream(self._stream_id, error)
            if closed is None:
                self._log_failure(core, error)
            try:
                self._publisher.publish_failure(
                    error if closed in (None, str(error)) else SessionClosedError(closed)
                )
            finally:  # the row reads FAILED: the table publishes nothing more either way
                self._failed = True
            return True


def _symbol_count(symbols: tuple[str, ...] | AllSymbols | None) -> int | None:
    """Count a cycle's symbols for its stream row's `SymbolCount`.

    Args:
        symbols: The cycle's static symbols.

    Returns:
        The length of a symbol list; None for `dm.ALL` and for a dataset without instruments.

    Raises:
        BrokenGuaranteeError: For a form of symbols with no rule here.
    """
    match symbols:
        case tuple():
            return len(symbols)
        case AllSymbols() | None:
            return None
        case _:
            raise BrokenGuaranteeError(unmatched_form("a poll cycle's symbol count", symbols))


def _logged_symbols(symbols: NormalizedSymbols | None) -> str:
    """Write a poll stream's symbols for the log line of its start.

    Args:
        symbols: The request's symbols.

    Returns:
        The first symbols of a list (`bounded_list`); `ALL` or `reactive table`
        (`describe_symbols`); `none` for a dataset without instruments.

    Raises:
        BrokenGuaranteeError: For a form of symbols with no rule here.
    """
    match symbols:
        case tuple():
            return bounded_list(symbols)
        case AllSymbols() | SymbolTable():
            return describe_symbols(symbols)[1]
        case None:
            return "none"
        case _:
            raise BrokenGuaranteeError(unmatched_form("a poll stream's logged symbols", symbols))


def _report_problems(
    core_ref: Callable[[], SessionCore | None], problems: Sequence[Problem]
) -> None:
    """Hand problems of a poll's symbol table to its session, if the session lives.

    Its `SkippedValues` calls it; the session is held weakly, so the poller and its skipped
    values never keep it alive.

    Args:
        core_ref: Returns the session's core, or None once it is freed.
        problems: The problems.
    """
    core = core_ref()
    if core is not None:
        core.problems(problems)


def _follow_refused(symbols: NormalizedSymbols | None, skipped: SkippedValues) -> None:
    """Join the values `canonicalize_symbols` refuses to a poll's own skipped values.

    Args:
        symbols: The poll's symbols; only a live table `canonicalize_symbols` returned has
            refused values (`refused_values_of`).
        skipped: The poll's skipped values.
    """
    table = symbols.table if isinstance(symbols, SymbolTable) else None
    refused = None if table is None else refused_values_of(table)
    if refused is not None:
        refused.follow(skipped)


def _hashable(column: pa.ChunkedArray) -> list[object]:
    """Return a column's values in a form a set can hold: a list value as a tuple.

    A row's identity is a tuple of its key values, held in the poller's sets, so a list key
    (`OtherManagers`) becomes a tuple of its items; pyarrow builds a list column back from it.

    Args:
        column: One key column of a cycle.

    Returns:
        Its values, each list as a tuple and every other value as is. A value a set cannot
        hold raises the set's `TypeError` where the poller adds it.
    """
    values: list[object] = list(column.to_pylist())
    if not pa.types.is_list(column.type):
        return values
    return [tuple(value) if isinstance(value, list) else value for value in values]


def _list_key_text(table: pa.Table, names: Sequence[str]) -> pa.Array:
    """Return the `LIST_KEY_COLUMN` of a keyed table's rows: their list keys as JSON text.

    Args:
        table: The rows to publish, tombstones included.
        names: The list columns among the poll keys, in key order.

    Returns:
        A `string` array, one compact JSON array per row holding its list keys in order (null
        for a null list), so equal lists give equal text and unequal lists unequal text.
    """
    columns = [table.column(name).to_pylist() for name in names]
    texts = [json.dumps(row, separators=(",", ":")) for row in zip(*columns, strict=True)]
    return pa.array(texts, pa.string())


def list_keys(schema: Schema, keys: Sequence[str]) -> tuple[str, ...]:
    """Return the poll keys that are list columns, which no `last_by` can key on directly.

    Args:
        schema: The dataset's schema in the session.
        keys: The table's key columns (`DatasetSpec.poll_keys`).

    Returns:
        The keys whose type is a list (`long[]`, `String[]`), in key order; empty for most
        datasets.
    """
    lists = {c.name for c in schema.columns if pa.types.is_list(c.type.arrow_type)}
    return tuple(key for key in keys if key in lists)


def build_table(
    spec: DatasetSpec,
    schema: Schema,
    mode: PollMode,
    *,
    stream_id: int,
    on_shutdown: Callable[[], None],
    release_later: Callable[[LivenessScope], None],
) -> tuple[Table, BlinkPublisher]:
    """Create a `poll()` table's publisher and the result shaped from its blink table.

    Runs on the caller thread, inside the result's liveness scope and the session's execution
    context. The publisher is named `massive.<dataset>#<stream id>`, and its blink table's
    shutdown (nothing uses the table any more, or it failed) runs `on_shutdown`.

    Args:
        spec: The dataset, for its name and poll keys.
        schema: The dataset's schema.
        mode: How cycles combine.
        stream_id: The table's `StreamId`.
        on_shutdown: The blink table's `on_shutdown_callback`: it stops the poller and tells
            the session (`WorkerTables.shutdown_callback`).
        release_later: Releases a failed table's pin later, on the session's reaper
            (`blink_publisher`, `SessionCore.release_later`).

    Returns:
        The result (append-only for `APPEND`, the `last_by` of the poll keys for `UPSERT`,
        and for `REPLACE` that `last_by` without its tombstones and their hidden column) and
        the publisher. Every schema ends with `TimestampInsert`, which the catalog refuses as a
        poll key, so `last_by` always has a column to aggregate.
    """
    definitions = col_defs(schema)
    replace = mode is PollMode.REPLACE
    keys = list(spec.poll_keys)
    lists = list_keys(schema, keys) if mode is not PollMode.APPEND else ()
    hidden = []
    if replace:
        definitions[LIVE_COLUMN] = dtypes.bool_
        hidden.append(LIVE_COLUMN)
    if lists:
        # Deephaven keys no aggregation on an array column: the poller's text of the list keys
        # stands in for them, and each list column is aggregated as the row's last value.
        definitions[LIST_KEY_COLUMN] = dtypes.string
        keys = [key for key in keys if key not in lists] + [LIST_KEY_COLUMN]
        hidden.append(LIST_KEY_COLUMN)
        logger.debug(
            "stream %d: %s poll(mode=%s) keys on %s: the list poll keys %s are keyed by their "
            "JSON text in %s",
            stream_id,
            spec.name,
            mode.value,
            keys,
            list(lists),
            LIST_KEY_COLUMN,
        )
    blink, publisher = blink_publisher(
        f"massive.{spec.name}#{stream_id}",
        definitions,
        on_shutdown=on_shutdown,
        release_later=release_later,
    )
    if mode is PollMode.APPEND:
        return blink_to_append_only(blink), publisher
    keyed = blink.last_by(keys)
    if replace:
        keyed = keyed.where(LIVE_COLUMN)
    return (keyed.drop_columns(hidden) if hidden else keyed), publisher


def _build(
    core: SessionCore,
    spec: DatasetSpec,
    *,
    mode: PollMode,
    stream_id: int,
    stop: threading.Event,
    note: str | None,
) -> tuple[tuple[Table, BlinkPublisher], LivenessScope]:
    """Create a `poll()` table in a new result scope (`make_result`).

    Args:
        core: The session's core, for the dataset's schema.
        spec: The dataset.
        mode: How cycles combine.
        stream_id: The table's `StreamId`.
        stop: The poller's stop token, set when the blink table shuts down.
        note: The sentence the table description adds about its recency, or None.

    Returns:
        The result and its publisher (`build_table`), and the result scope. The result is the
        shape `build_table` derives, described last (`describe`), so it carries the dataset's
        column descriptions and lists its columns in schema order in every mode, also after
        the `last_by` of `UPSERT` and `REPLACE` put the poll keys first; the poller writes
        only through the publisher.
    """
    built: list[BlinkPublisher] = []
    schema = core.dataset_schema(spec)

    def shape() -> Table:
        on_shutdown = core.workers.shutdown_callback(stream_id, stop)
        table, publisher = build_table(
            spec,
            schema,
            mode,
            stream_id=stream_id,
            on_shutdown=on_shutdown,
            release_later=core.release_later,
        )
        built.append(publisher)
        return describe(table, spec, schema, note=note)

    result, scope = make_result(core.exec_ctx, shape)
    return (result, built[0]), scope


def open_poll(
    core: SessionCore,
    *,
    spec: DatasetSpec,
    request: Request,
    interval: timedelta,
    mode: PollMode,
    query: str,
    read: CycleReader,
    pollers: Pollers,
    host: str | None,
    path: str | None,
    recency: Recency | None,
    clock: PollClock,
    stop: threading.Event,
    note: str | None = None,
) -> Table:
    """Create a `poll()` table and start its poller.

    On the caller thread: the publisher and the result are created inside the result's own
    liveness scope (in the session's execution context); the table's row is registered in
    `m.streams()` with a new `StreamId`, which keeps the session alive while the poll runs; the
    poller joins the session's pollers; and the result is tracked, so that releasing it marks
    the row released. Only then does the poller's thread start, unless the session closed
    meanwhile: the result is then released and the closed session's `MassiveError` raised. If
    any step raises, even `KeyboardInterrupt`, every step already taken is undone and nothing
    keeps running. The start is logged at DEBUG, with the session's label, the stream id,
    the query, the interval, the mode, and the symbols (the count and the first twenty); the
    poller logs its own end. Asynchronous exceptions are handled on a best-effort basis, as
    `release_scope` documents for its own lock: a `KeyboardInterrupt` landing in the instant
    between the table's creation returning and the cleanup's `try` would leave the table's
    scope unreleased.

    Args:
        core: The session's core.
        spec: The dataset, with a fetch path.
        request: The normalized arguments.
        interval: The time from one cycle's start to the next.
        mode: How cycles combine.
        query: The `repr` of the query, for the row's `Query`.
        read: Reads one cycle's rows.
        pollers: The session's live pollers.
        host: The REST host, for the row's `Host`.
        path: The fetch path, for the row's `Path`.
        recency: The row's first `Recency`: `UNKNOWN` for current data, which a cycle whose
            pages Massive labels real-time or delayed turns into `REALTIME` or `DELAYED`; None
            for data no label describes (reference, economy, and partner data).
        clock: The time source of the waits between cycles.
        stop: The poller's stop token, which the caller creates and every cycle's requests
            listen to; the table's shutdown and the session's close set it.
        note: The sentence the table's description adds about its recency
            (`columns.rest_recency_note`), or None.

    Returns:
        The live table.

    Raises:
        MassiveError: If the session closed after the poll was registered but before its
            poller started (`"MassiveSession #1 is closed; ..."`). One closed before the poll could
            be registered raises the `MassiveError` of `WorkerTables.track` or
            `Pollers.add`.
    """
    stream_id = core.next_stream_id()
    with entered(core.exec_ctx):
        (result, publisher), scope = _build(
            core,
            spec,
            mode=mode,
            stream_id=stream_id,
            stop=stop,
            note=note,
        )
    symbols = request.symbols

    def make() -> tuple[Poller, StreamRow]:
        count, text = (None, "") if symbols is None else describe_symbols(symbols)
        row = StreamRow(
            stream_id=stream_id,
            kind=StreamKind.POLL,
            dataset=spec.name,
            query=shown_query(query),
            symbol_count=count,
            symbols=text,
            held_results=1,
            transport=Transport.REST,
            host=host,
            path=path,
            recency=recency,
            created_at=time.time_ns(),
        )
        poller = Poller(
            core,
            spec=spec,
            request=request,
            interval=interval,
            mode=mode,
            stream_id=stream_id,
            read=read,
            pollers=pollers,
            clock=clock,
            publisher=publisher,
            stop=stop,
        )
        pollers.add(poller)  # raises if the session closed: the poller then joined nothing
        return poller, row

    poller = core.workers.track(result, scope, make)
    logger.debug(
        "%s: poll() stream %d started: %s every %.3f s, mode %s, symbols %s",
        core.label,
        stream_id,
        shown_query(query),
        interval.total_seconds(),
        mode.name,
        _logged_symbols(symbols),
    )
    poller.start()
    return result
