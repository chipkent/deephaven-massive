"""`poll()`: live tables that re-read a REST dataset on an interval, one poller thread each.

`Query.poll(interval, mode=...)` returns a live table that one *poller* keeps current: a daemon
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
table's row of `m.streams()`: `RowsPublished`, `RequestsPerCycle`, `SymbolCount` for a reactive
table, and `SchemaDrift` from each page's drift.

**Failures.** A cycle that raises `AuthenticationError`, `EntitlementError`, or a
`PermanentError` fails the table through `publish_failure`, marks the row `FAILED`
(`SessionCore.fail_stream`), and ends the poller. The permanent errors are `RowDataError` (a
row that breaks the schema, or, in any mode, has a null poll key, which would merge unrelated
rows), `UnsupportedParameterError` (a symbol of a reactive table routed to a path that lacks a
parameter the caller set), `SymbolRowError` (a symbol table's row that is not Massive's
ticker, under `noncanonical_symbols=RAISE`; under `LOG` and `IGNORE` the row is skipped and the
cycle's `Error` lists it), and `_ingest/rest.py`'s `UnlistedPairError` (a crypto pair whose
currencies Massive's reference data does not give). The failure is logged at the level its
cause gives (`log_failure`): ERROR for a defect, naming whose it is, and WARNING otherwise. Any
other error, also one while publishing, is shown in the row's `Error`; the table keeps its rows
and the next cycle runs as scheduled, and clears the `Error` once it succeeds. The first failing
cycle of a run of failures logs one WARNING (an expected cause) or ERROR (a defect, with its
trace), later ones log at DEBUG, and the first cycle that succeeds again logs the recovery at
INFO.

**Late cycles.** A cycle that ends after its interval (its requests are paced, or held by a
rate-limit pause) leaves the table late: the first one of a run logs one WARNING that names
`m.rate_limits()`, and the first cycle that ends in time again logs at INFO.

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
alive while the poll runs (`SessionCore.register_worker`).
"""

from __future__ import annotations

import functools
import json
import logging
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Final, Protocol, cast

import pyarrow as pa
import pyarrow.compute as pc
from deephaven import dtypes
from deephaven.stream import blink_to_append_only

from deephaven_massive._datasets.column_descriptions import RAW_SYMBOL
from deephaven_massive._datasets.row_times import INSERT
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import PollMode
from deephaven_massive._errors import (
    LIBRARY_BUG,
    AuthenticationError,
    EntitlementError,
    ErrorCause,
    MassiveError,
    PermanentError,
    RowDataError,
    cause_of,
    failure_text,
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
)
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import make_result, release_scope
from deephaven_massive._tables.publisher import BlinkPublisher, blink_publisher
from deephaven_massive._tables.watchlist import SymbolRowRule, read_symbols, skipped_rows_text

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
    from deephaven_massive._tables.session_core import SessionCore

__all__ = [
    "CLOCK",
    "LIST_KEY_COLUMN",
    "LIVE_COLUMN",
    "CycleReader",
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

    Raised only under `MassiveConfig.noncanonical_symbols=OnError.RAISE` (under `LOG` and
    `IGNORE` the row is skipped and listed in the row's `Error`), or for a symbol table that
    has failed. The table fails with it, since every later cycle would read the same row; its
    message names `dm.canonicalize_symbols`, which converts such a table. The table is the
    caller's, so its cause is `ErrorCause.USER_DEFECT`.
    """

    default_cause = ErrorCause.USER_DEFECT


_FATAL: Final = (AuthenticationError, EntitlementError, PermanentError)
"""The errors that fail a `poll()` table: retrying the next cycle cannot fix them."""


class PollClock(Protocol):
    """The time source of the pollers: the system clock, or a fake one in tests."""

    def monotonic(self) -> float:
        """Return a monotonic time in seconds."""
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
    """The real clock: `time.monotonic` and `threading.Event.wait`."""

    @staticmethod
    def monotonic() -> float:
        """Return `time.monotonic()`."""
        return time.monotonic()

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


class CycleReader(Protocol):
    """Reads one cycle's rows: the REST transport's `RestReader.read_cycle`, bound to a dataset.

    It holds no session core, so a poller never keeps the core alive through it.
    """

    def __call__(
        self,
        *,
        symbols: NormalizedSymbols | None,
        record: Callable[[DriftReport], None],
        executor: ThreadPoolExecutor,
        cancel: threading.Event,
    ) -> tuple[pa.Table, int, float]:
        """Request the dataset for the symbols; return the rows, request count, and patience.

        Args:
            symbols: The cycle's static symbols: a sorted tuple, `ALL`, or None.
            record: Receives each page's drift, for the table's `SchemaDrift`.
            executor: The poller's pool for fan-out requests.
            cancel: The poller's stop token: once set, the cycle's requests stop waiting for
                the rate governor.

        Returns:
            The rows in the dataset's schema, how many requests were made, and the most
            seconds of rate-limit pauses the cycle's reads would wait out (their patience),
            for the cycle's log line.
        """
        ...


class Pollers:
    """The live pollers of one session, stopped together when the session closes.

    The session's REST state holds one (`RestState.pollers`) and its `close` runs as a session
    closer. Once closed it refuses new pollers, so a `poll()` that races `close()` fails
    instead of starting a poller nothing would stop. Thread-safe: one leaf lock guards the set
    and the flag, and pollers are stopped after it is released.
    """

    __slots__ = ("_closed", "_label", "_live", "_lock")

    def __init__(self, label: str) -> None:
        """Start with no poller.

        Args:
            label: The session's label, for the closed-session error.
        """
        self._label = label
        self._lock = threading.Lock()
        self._live: set[Poller] = set()
        self._closed = False

    @property
    def closed(self) -> bool:
        """True once `close` has run."""
        with self._lock:
            return self._closed

    def add(self, poller: Poller) -> None:
        """Register a poller before it starts.

        Args:
            poller: The new poller.

        Raises:
            MassiveError: If the session has closed (`"MassiveSession #1 is closed"`).
        """
        with self._lock:
            if self._closed:
                raise MassiveError(f"{self._label} is closed")
            self._live.add(poller)

    def discard(self, poller: Poller) -> None:
        """Forget a poller whose table was released; an unknown poller is ignored.

        Args:
            poller: The poller.
        """
        with self._lock:
            self._live.discard(poller)

    def close(self) -> None:
        """Stop every live poller and refuse new ones; idempotent, and never waits."""
        with self._lock:
            self._closed = True
            live = tuple(self._live)
            self._live.clear()
        for poller in live:
            poller.stop()


class Poller:
    """One `poll()` table's publisher and the daemon thread that keeps it current.

    Created by `open_poll` on the caller thread, which builds the table (`build`), registers
    the poller, and then starts its thread (`start`). The thread enters the session's execution
    context for its whole life, and leaves the session's `Pollers` when it ends. Every other
    method is safe from any thread: `stop` and `release` only set an event and update
    registries, and `fail` fails the table. The key state of `REPLACE` and `APPEND` belongs to
    the thread alone, so it takes no lock; the publish lock, below which only `registry_lock`
    is taken (a cycle's rows are converted to a table before it), keeps a failure and an `add`
    from interleaving and makes the failure once-only.
    """

    __slots__ = (
        "__weakref__",
        "_clock",
        "_core",
        "_cycles",
        "_exec_ctx",
        "_failed",
        "_failing",
        "_interval",
        "_key_names",
        "_keys",
        "_late",
        "_list_keys",
        "_max_concurrency",
        "_mode",
        "_pollers",
        "_publish_lock",
        "_published",
        "_publisher",
        "_read",
        "_request",
        "_rule",
        "_schema",
        "_seen",
        "_spec",
        "_stop",
        "_stream_id",
        "_thread",
        "_thread_name",
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
        publisher: BlinkPublisher,
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
        """
        self._core = weakref.ref(core)
        self._exec_ctx = core.exec_ctx
        # The poller thread's name; its fan-out pool's threads add " rest_<n>".
        self._thread_name = f"{core.thread_prefix} poll-{stream_id}"
        self._max_concurrency = core.config.max_concurrency
        self._schema = core.dataset_schema(spec)
        self._spec = spec
        self._key_names = key_columns(spec, self._schema)
        self._list_keys = list_keys(self._schema, self._key_names)
        self._request = request
        self._rule = SymbolRowRule.of(spec, core.config.noncanonical_symbols)
        self._interval = interval.total_seconds()
        self._mode = mode
        self._stream_id = stream_id
        self._read = read
        self._pollers = pollers
        self._clock = clock
        self._stop = stop
        self._publisher = publisher
        self._publish_lock = threading.Lock()
        self._failed = False
        # The poller thread's episodes: the cause of the run of failing cycles (None outside
        # one), and whether cycles run late.
        self._failing: ErrorCause | None = None
        self._late = False
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
    def stopped(self) -> bool:
        """True once the poller has been asked to stop."""
        return self._stop.is_set()

    def start(self) -> None:
        """Start the thread; its first cycle runs at once."""
        self._thread.start()

    def stop(self) -> None:
        """Ask the thread to stop after its current request; never waits."""
        self._stop.set()

    def release(self) -> None:
        """The result was released: mark the row `RELEASED`; the poller keeps running.

        Runs on the reaper thread as the result record's `on_released` callback, so it takes
        no lock beyond the leaf locks of the registries it updates. A table derived from the
        result may still use the poll; the poller stops when the poll's blink table shuts down.
        The session is alive: the poll's record keeps it in the live set until this has run.
        """
        cast("SessionCore", self._core()).release_worker(self._stream_id)

    def fail(self, error: MassiveError) -> bool:
        """Stop the poller and fail its table, once: the session closed, or a thread died.

        The session's hook for the poll (`SessionCore.register_worker`), called outside every lock
        while the poll is registered, so the session is alive. The row is updated first
        (`SessionCore.fail_stream`, which keeps a `CLOSED` row as it is), then the table fails;
        a table that already failed is left alone.

        Args:
            error: The error the table fails with.

        Returns:
            False when the table had already failed.
        """
        self._stop.set()
        return self._fail_once(cast("SessionCore", self._core()), error)

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
                    began = self._clock.monotonic()
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
        """Log the start and the end of a run of cycles that end after their interval.

        A late cycle leaves the table behind the interval the user asked for: its requests are
        paced at a Basic subscription's limit, or held by a rate-limit pause. The first late
        cycle of a run logs one WARNING, and the first cycle in time again logs at INFO.

        Args:
            took: The cycle's duration, in seconds.
        """
        late = took > self._interval
        if late and not self._late:
            logger.warning(
                "%s poll() stream %d: a cycle took %.0f s, longer than the %g s interval, so the "
                "table updates late; m.rate_limits() shows whether its requests are paced or "
                "paused by Massive's rate limit",
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

    def _symbols(self) -> tuple[NormalizedSymbols | None, tuple[str, ...]]:
        """Return the cycle's symbols, reading a reactive table once.

        A row of a reactive table that is not Massive's exact ticker follows the session's
        `MassiveConfig.noncanonical_symbols` (`read_symbols`): under `RAISE` it fails the table;
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
        try:
            return self._publish_cycle(core, pool)
        except _FATAL as error:
            if not self._stop.is_set():
                self._fail_fatal(core, error)
            return False
        except MassiveError as error:  # any other library error: the next cycle runs
            return self._carry_on(core, error)
        except Exception as error:  # a defect: logged with its trace, and the next cycle runs
            if not self._stop.is_set() and self._failing is not ErrorCause.LIBRARY_DEFECT:
                logger.exception(
                    "%s poll() stream %d: the cycle failed. %s",
                    self._spec.name,
                    self._stream_id,
                    LIBRARY_BUG,
                )
            return self._carry_on(core, error)

    def _publish_cycle(self, core: SessionCore, pool: ThreadPoolExecutor) -> bool:
        """Read the cycle's rows, publish them, and update the table's row.

        Args:
            core: The session's core.
            pool: The poller's fan-out pool.

        Returns:
            False if the poller was stopped while it read, so nothing was published.
        """
        began = self._clock.monotonic()
        symbols, skipped = self._symbols()
        record = functools.partial(core.record_stream_drift, self._stream_id, Transport.REST)
        table, requests, patience = self._read(
            symbols=symbols, record=record, executor=pool, cancel=self._stop
        )
        if self._stop.is_set():
            return False
        published = self._publish(table)
        self._published += published
        logger.debug(
            "stream %d: %s poll() cycle: %d requests, %d rows read, %d published in %.3f s, "
            "waiting out at most %.0f s of pauses",
            self._stream_id,
            self._spec.name,
            requests,
            table.num_rows,
            published,
            self._clock.monotonic() - began,
            patience,
        )
        # A successful cycle clears an earlier cycle's error; the skipped rows are kept apart.
        update = StreamUpdate(
            rows_published=self._published,
            requests_per_cycle=requests,
            symbol_count=len(symbols) if isinstance(symbols, tuple) else None,
            clear_error=True,
            skipped=skipped_rows_text(skipped) if skipped else None,
            clear_skipped=not skipped,
        )
        core.update_streams({self._stream_id: update})
        if self._failing is not None:
            self._failing = None
            logger.info(
                "%s poll() stream %d: a cycle succeeded again", self._spec.name, self._stream_id
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
        """Show a cycle's error in the table's row; the table keeps its rows.

        The first failure of a run is logged once, by its text, which names the HTTP status,
        the path, and the request id: a `MassiveError` with an expected cause at WARNING, and a
        defect at ERROR (an exception that is not a `MassiveError` has been logged with its
        trace by the caller). Every later failure of the run with the same cause logs at
        DEBUG, until a cycle succeeds again (`_publish_cycle`); a failure of another cause
        starts a new run, so a defect is never hidden behind an outage.

        Args:
            core: The session's core.
            error: The cycle's error.

        Returns:
            Whether the poller carries on: False once it has been stopped.
        """
        if self._stop.is_set():
            return False
        cause = cause_of(error)
        if self._failing is cause:
            logger.debug("%s poll() stream %d: %s", self._spec.name, self._stream_id, error)
        elif isinstance(error, MassiveError):
            level = logging.WARNING if cause is ErrorCause.EXPECTED else logging.ERROR
            logger.log(
                level,
                "%s poll() stream %d: a cycle failed, and the next one runs as scheduled: %s",
                self._spec.name,
                self._stream_id,
                failure_text(error),
            )
        self._failing = cause
        core.update_streams({self._stream_id: StreamUpdate(error=str(error))})
        return True

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
        stock) and its revision keys are not checked: a null there is a value. Nor is `Symbol`
        in a `WARN` session (`key_columns`): the reader refuses a row with no symbol, so a null
        `Symbol` there is a row Massive spelled in a way the library cannot read, keyed by its
        `RawSymbol`.

        Args:
            table: The cycle's rows.

        Raises:
            RowDataError: Naming the dataset, the key column, the first such row's position in
                the cycle, and that row's non-null values but its `TimestampInsert`, truncated
                to `_ROW_TEXT` characters, so the row can be found although it was never
                published.
        """
        nullable = self._spec.nullable_poll_keys
        # In a WARN session the key holds RawSymbol (`key_columns`): a row's Symbol is null
        # exactly when its RawSymbol holds the unreadable text, so neither is checked.
        warn = RAW_SYMBOL in self._key_names
        checked = (
            k for k in self._key_names if k not in nullable and not (warn and k in _SYMBOL_KEYS)
        )
        for name in checked:
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

    def _fail_once(self, core: SessionCore, error: MassiveError) -> bool:
        """Mark the row `FAILED`, then fail the table, once; log it at the level its cause gives.

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
            closed = core.fail_stream(self._stream_id, str(error))
            if closed is None:
                log_failure(
                    logger,
                    error,
                    "%s poll() stream %d failed: %s",
                    self._spec.name,
                    self._stream_id,
                )
            try:
                self._publisher.publish_failure(
                    error if closed in (None, str(error)) else MassiveError(closed)
                )
            finally:  # the row reads FAILED: the table publishes nothing more either way
                self._failed = True
            return True


_SYMBOL_KEYS: Final = frozenset({"Symbol", RAW_SYMBOL})
"""The key columns of a row's symbol in a `WARN` session: one of the two is always set."""


def _hashable(column: pa.ChunkedArray) -> list[Hashable]:
    """Return a column's values in a form a set can hold: a list value as a tuple.

    A row's identity is a tuple of its key values, held in the poller's sets, so a list key
    (`OtherManagers`) becomes a tuple of its items; pyarrow builds a list column back from it.

    Args:
        column: One key column of a cycle.

    Returns:
        Its values, each list as a tuple and every other value as is.
    """
    values = column.to_pylist()
    if not pa.types.is_list(column.type):
        return cast("list[Hashable]", values)
    return [None if value is None else tuple(value) for value in values]


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
        keys: The table's key columns (`key_columns`).

    Returns:
        The keys whose type is a list (`long[]`, `String[]`), in key order; empty for most
        datasets.
    """
    lists = {c.name for c in schema.columns if pa.types.is_list(c.type.arrow_type)}
    return tuple(key for key in keys if key in lists)


def key_columns(spec: DatasetSpec, schema: Schema) -> tuple[str, ...]:
    """Return the columns that identify a row of a `poll()` table: its key in every mode.

    They are the dataset's poll keys (`DatasetSpec.poll_keys`), plus `RawSymbol` when they hold
    `Symbol` and the schema has a `RawSymbol` column (a session whose `unreadable_symbols` is
    `LOG`, the default, or `IGNORE`). Such a session keeps a row whose symbol Massive spelled in
    a way the library cannot read, with a null `Symbol` and the text in `RawSymbol`; keyed on
    `Symbol` alone, every such row would merge into one, or fail the null-key check. With
    `RawSymbol` in the key, a readable row is keyed by its `Symbol` (its `RawSymbol` is null)
    and an unreadable one by its text.

    Args:
        spec: The dataset.
        schema: The dataset's schema in the session.

    Returns:
        The key columns, in order.
    """
    if "Symbol" in spec.poll_keys and RAW_SYMBOL in schema.column_names:
        return (*spec.poll_keys, RAW_SYMBOL)
    return spec.poll_keys


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
            the session (`_shut_down`).
        release_later: Releases a failed table's pin later, on the session's reaper
            (`blink_publisher`, `SessionCore.release_later`).

    Returns:
        The result (append-only for `APPEND`, the `last_by` of the poll keys for `UPSERT`,
        and for `REPLACE` that `last_by` without its tombstones and their hidden column) and
        the publisher. Every schema ends with `TimestampInsert`, which no dataset keys on, so
        `last_by` always has a column to aggregate.
    """
    definitions = col_defs(schema)
    replace = mode is PollMode.REPLACE
    keys = list(key_columns(spec, schema))
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


def _shut_down(core_ref: weakref.ref[SessionCore], stream_id: int, stop: threading.Event) -> None:
    """A `poll()` blink table's shutdown callback: stop the poller and tell the session.

    Runs on whichever thread dropped the table's last reference (the reaper, an update-graph
    thread after a failure, a gRPC thread, or Deephaven's liveness-cleanup thread), so it only
    sets a token and updates the registry (`SessionCore.worker_shut_down`). The session is alive:
    the poll's record keeps it in the live set until this has run, and before the record
    exists the opening caller holds it.

    Args:
        core_ref: The session's core, weakly.
        stream_id: The table's `StreamId`.
        stop: The poller's stop token.
    """
    stop.set()
    cast("SessionCore", core_ref()).worker_shut_down(stream_id)


def _build(
    core: SessionCore,
    spec: DatasetSpec,
    *,
    mode: PollMode,
    stream_id: int,
    stop: threading.Event,
) -> tuple[tuple[Table, BlinkPublisher], LivenessScope]:
    """Create a `poll()` table in a new result scope (`make_result`).

    Args:
        core: The session's core, for the dataset's schema.
        spec: The dataset.
        mode: How cycles combine.
        stream_id: The table's `StreamId`.
        stop: The poller's stop token, set when the blink table shuts down.

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
        on_shutdown = functools.partial(_shut_down, weakref.ref(core), stream_id, stop)
        table, publisher = build_table(
            spec,
            schema,
            mode,
            stream_id=stream_id,
            on_shutdown=on_shutdown,
            release_later=core.release_later,
        )
        built.append(publisher)
        return describe(table, spec, schema)

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
    clock: PollClock,
    stop: threading.Event,
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
        clock: The time source of the waits between cycles.
        stop: The poller's stop token, which the caller creates and every cycle's requests
            listen to; the table's shutdown and the session's close set it.

    Returns:
        The live table.

    Raises:
        MassiveError: If the session closed after the poll was registered but before its
            poller started (`"MassiveSession #1 is closed"`). One closed before the poll could
            be registered raises the `MassiveError` of `SessionCore.register_worker` or
            `Pollers.add`.
    """
    stream_id = core.next_stream_id()
    with entered(core.exec_ctx):
        (result, publisher), scope = _build(core, spec, mode=mode, stream_id=stream_id, stop=stop)
    poller: Poller | None = None
    registered = False
    try:
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
        symbols = request.symbols
        count, text = (None, "") if symbols is None else describe_symbols(symbols)
        core.register_worker(
            StreamRow(
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
                created_at=time.time_ns(),
            ),
            fail=poller.fail,
        )
        registered = True
        pollers.add(poller)
        core.results.track(result, scope, poller.release)
    except BaseException:
        try:
            if poller is not None:
                poller.stop()
                pollers.discard(poller)
            if registered:
                core.release_worker(stream_id)
        finally:
            release_scope(core.exec_ctx, scope)
        raise
    if core.closed:  # close() failed the table meanwhile; never start its poller
        core.results.release(result)
        raise MassiveError(f"{core.label} is closed")
    logger.debug(
        "%s: poll() stream %d started: %s every %.3f s, mode %s, symbols %s",
        core.label,
        stream_id,
        shown_query(query),
        interval.total_seconds(),
        mode.name,
        "none"
        if symbols is None
        else bounded_list(symbols)
        if isinstance(symbols, tuple)
        else text,
    )
    poller.start()
    return result
