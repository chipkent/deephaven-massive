"""`poll()`: live tables that re-read a REST dataset on an interval, one poller thread each.

`Query.poll(interval, mode=...)` returns a live table that one *poller* keeps current: a daemon
thread that runs a cycle (one `fetch()` of the dataset, through the REST transport's reader),
publishes its rows, and waits for the next cycle. Every mode publishes through one Deephaven
`table_publisher`, created with the result inside its result scope on the caller thread, so a
failure can fail the table (`publish_failure`) in every mode:

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

Every row's `TimestampInsert` is when its page of the cycle arrived, so in a keyed table it is
the time of the latest poll that returned the row; it is never part of a row's identity.

A cycle reads a reactive symbol table once, with `Table.snapshot()` on the poller thread (never
in a listener), and requests the dataset's fetch path for the symbols it holds; a fan-out path
makes its requests on the poller's pool of `max_concurrency` threads. Each cycle updates the
table's row of `m.streams()`: `RowsPublished`, `RequestsPerCycle`, `SymbolCount` for a reactive
table, and `SchemaDrift` from each page's drift.

**Failures.** A cycle that raises `AuthenticationError`, `EntitlementError`, `RowDataError` (a
row that breaks the schema, or, in any mode, has a null poll key, which would merge unrelated
rows), or `UnsupportedParameterError` (a symbol of a reactive table routed to a path that lacks
a parameter the caller set) fails the table through `publish_failure`, marks the row `FAILED`
(`SessionCore.fail_stream`), and ends the poller. Any other error, also one while publishing,
is logged at ERROR and shown in the row's `Error`; the table keeps its rows and the next cycle
runs as scheduled, and clears the `Error` once it succeeds.

**Lifetime.** The poller ends when its table is released (`m.release`, or garbage collection
through the reaper), when the table's blink table shuts down, when it fails, or when the
session closes: every live poller is registered with the session's `Pollers`, which the
session's REST closer stops, and a poller can no longer be registered once that has run, so a
`poll()` that races `close()` never starts a poller the session would not stop. A stopped poller
finishes the request it is waiting on and publishes nothing more; the table keeps its rows. The
poller holds the session core only through a weak reference, so a `poll()` table never keeps an
unclosed session from being collected (whose collection then stops the poller).
"""

from __future__ import annotations

import functools
import logging
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Final, Protocol

import pyarrow as pa
import pyarrow.compute as pc
from deephaven import dtypes
from deephaven.stream import blink_to_append_only
from deephaven.stream.table_publisher import table_publisher

from deephaven_massive._datasets.row_times import INSERT
from deephaven_massive._datasets.symbols import SymbolTable, symbol_canonicalizer
from deephaven_massive._enums import PollMode
from deephaven_massive._errors import AuthenticationError, EntitlementError, MassiveError
from deephaven_massive._massive import Transport
from deephaven_massive._tables.columns import col_defs, dataset_table, describe
from deephaven_massive._tables.diagnostics import StreamRow, StreamUpdate, describe_symbols
from deephaven_massive._tables.liveness import entered, make_result

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable
    from datetime import timedelta

    from deephaven.liveness_scope import LivenessScope
    from deephaven.stream.table_publisher import TablePublisher
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._datasets.schema import Schema
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._massive import DriftReport
    from deephaven_massive._tables.core import SessionCore

__all__ = [
    "CLOCK",
    "LIVE_COLUMN",
    "CycleReader",
    "PollClock",
    "Poller",
    "Pollers",
    "RowDataError",
    "UnsupportedParameterError",
    "build_table",
    "open_poll",
]

logger = logging.getLogger(__name__)

_ROW_TEXT: Final = 300
"""The longest rendering of a row in the error of a null poll key."""

LIVE_COLUMN: Final = "__MassiveLive"
"""The hidden column of a `REPLACE` table's publisher: False marks a tombstone. Dataset columns
are PascalCase wire names, so none can take this name."""


class RowDataError(MassiveError):
    """A REST row that breaks its dataset's schema.

    A row that lacks a key the dataset requires, a value that does not convert to its field's
    type, a timestamp outside 1990 to 2100, or, in a `poll()` cycle, a null or NaN value in
    one of the dataset's poll keys. `fetch()` and `history()` raise it like any `MassiveError`;
    a `poll()` table fails with it, since the next cycle would fail alike.
    """


class UnsupportedParameterError(MassiveError):
    """A parameter the caller set that the path a `poll()` cycle chose does not take.

    A reactive symbol table can route a new symbol to a path that lacks a parameter (an
    explicit `adjusted` on the crypto indicators). Retrying cannot fix it, so the table fails
    with it.
    """


_FATAL: Final = (AuthenticationError, EntitlementError, RowDataError, UnsupportedParameterError)
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
    """Reads one cycle's rows: the REST transport's `RestReader.read_symbols`, bound to a dataset.

    It holds no session core, so a poller never keeps the core alive through it.
    """

    def __call__(
        self,
        *,
        symbols: NormalizedSymbols | None,
        record: Callable[[DriftReport], None],
        executor: ThreadPoolExecutor,
    ) -> tuple[pa.Table, int]:
        """Request the dataset for the symbols and return the rows and the request count.

        Args:
            symbols: The cycle's static symbols: a sorted tuple, `ALL`, or None.
            record: Receives each page's drift, for the table's `SchemaDrift`.
            executor: The poller's pool for fan-out requests.

        Returns:
            The rows in the dataset's schema, and how many requests were made.
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
    context for its whole life. Every other method is safe from any thread: `stop` and
    `release` only set an event and update registries. The key state of `REPLACE` and `APPEND`
    belongs to the thread alone, so it takes no lock.
    """

    __slots__ = (
        "__weakref__",
        "_clock",
        "_core",
        "_exec_ctx",
        "_interval",
        "_keys",
        "_label",
        "_max_concurrency",
        "_mode",
        "_pollers",
        "_published",
        "_publisher",
        "_read",
        "_request",
        "_schema",
        "_seen",
        "_spec",
        "_stop",
        "_stream_id",
        "_thread",
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
        publisher: TablePublisher,
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
                down.
        """
        self._core = weakref.ref(core)
        self._exec_ctx = core.exec_ctx
        self._label = core.label
        self._max_concurrency = core.config.max_concurrency
        self._schema = core.catalog.dataset_schema(spec)
        self._spec = spec
        self._request = request
        self._interval = interval.total_seconds()
        self._mode = mode
        self._stream_id = stream_id
        self._read = read
        self._pollers = pollers
        self._clock = clock
        self._stop = stop
        self._publisher = publisher
        self._keys: set[tuple[Hashable, ...]] = set()
        # The identities an APPEND table holds; only the poller thread touches it.
        self._seen: set[tuple[Hashable, ...]] = set()
        self._published = 0
        self._thread = threading.Thread(
            target=self._run, name=f"{core.label} poll #{stream_id}", daemon=True
        )

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
        """The table was released: stop, leave the session's pollers, mark the row `RELEASED`.

        Runs on the reaper thread as the result record's `on_released` callback, so it takes
        no lock beyond the leaf locks of the registries it updates.
        """
        self._stop.set()
        self._pollers.discard(self)
        core = self._core()
        if core is not None:
            core.release_poll(self._stream_id)

    def _run(self) -> None:
        """Run cycles until stopped or failed, waiting `interval` from one start to the next."""
        with (
            entered(self._exec_ctx),
            ThreadPoolExecutor(
                self._max_concurrency, f"{self._label} poll #{self._stream_id}"
            ) as pool,
        ):
            while not self._stop.is_set():
                began = self._clock.monotonic()
                if not self._cycle(pool):
                    return
                remaining = self._interval - (self._clock.monotonic() - began)
                self._clock.wait(self._stop, max(remaining, 0.0))

    def _symbols(self) -> NormalizedSymbols | None:
        """Return the cycle's symbols, reading a reactive table once.

        Returns:
            The request's symbols, or for a reactive table the valid symbols it holds now,
            canonical and sorted (rows that are no valid symbol of the class are ignored and
            logged once at WARNING).
        """
        symbols = self._request.symbols
        if not isinstance(symbols, SymbolTable):
            return symbols
        canonical = symbol_canonicalizer(self._spec.asset_class)
        snapshot = symbols.table.snapshot()
        found = {canonical(row[0]) for row in snapshot.iter_tuple(cols="Symbol")}
        return tuple(sorted(symbol for symbol in found if symbol is not None))

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
                self._fail(core, error)
            return False
        except MassiveError as error:  # any other library error: the next cycle runs
            return self._carry_on(core, error)
        except Exception as error:  # a defect: logged with its trace, and the next cycle runs
            if not self._stop.is_set():
                logger.exception("%s poll(): the cycle failed", self._spec.name)
            return self._carry_on(core, error)

    def _publish_cycle(self, core: SessionCore, pool: ThreadPoolExecutor) -> bool:
        """Read the cycle's rows, publish them, and update the table's row.

        Args:
            core: The session's core.
            pool: The poller's fan-out pool.

        Returns:
            False if the poller was stopped while it read, so nothing was published.
        """
        symbols = self._symbols()
        record = functools.partial(core.record_stream_drift, self._stream_id, Transport.REST.value)
        table, requests = self._read(symbols=symbols, record=record, executor=pool)
        if self._stop.is_set():
            return False
        self._published += self._publish(table)
        update = StreamUpdate(
            rows_published=self._published,
            requests_per_cycle=requests,
            symbol_count=len(symbols) if isinstance(symbols, tuple) else None,
            clear_error=True,
        )
        core.update_streams({self._stream_id: update})
        return True

    def _carry_on(self, core: SessionCore, error: Exception) -> bool:
        """Show a cycle's error in the table's row; the table keeps its rows.

        A library error is logged at ERROR by its text, which names the HTTP status, the path,
        and the request id; a defect has been logged with its trace by the caller.

        Args:
            core: The session's core.
            error: The cycle's error.

        Returns:
            Whether the poller carries on: False once it has been stopped.
        """
        if self._stop.is_set():
            return False
        if isinstance(error, MassiveError):
            logger.error("%s poll(): %s", self._spec.name, error)
        core.update_streams({self._stream_id: StreamUpdate(error=str(error))})
        return True

    def _publish(self, table: pa.Table) -> int:
        """Publish one cycle's rows as the mode shapes them.

        `REPLACE` adds a tombstone for each key the cycle lost; `APPEND` keeps only the rows
        not seen before (`_fresh`). `UPSERT` publishes the rows as they are.

        Args:
            table: The cycle's rows in the dataset's schema.

        Returns:
            How many of the cycle's rows were published (tombstones not counted).
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
        if table.num_rows:
            self._publisher.add(dataset_table(table, self._schema.date_columns))
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
        for name in (key for key in self._spec.poll_keys if key not in nullable):
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
        names = (*self._spec.poll_keys, *self._spec.revision_keys)
        identities = zip(*(table.column(name).to_pylist() for name in names), strict=True)
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
        names = self._spec.poll_keys
        keys = set(zip(*(table.column(name).to_pylist() for name in names), strict=True))
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

    def _fail(self, core: SessionCore, error: MassiveError) -> None:
        """Fail the table with `error`, mark its row `FAILED`, and log it at ERROR.

        Args:
            core: The session's core.
            error: The failure.
        """
        logger.error("%s poll() failed: %s", self._spec.name, error)
        self._publisher.publish_failure(error)
        core.fail_stream(self._stream_id, str(error))


def build_table(
    spec: DatasetSpec, schema: Schema, mode: PollMode, *, stream_id: int, stop: threading.Event
) -> tuple[Table, TablePublisher]:
    """Create a `poll()` table's publisher and the result shaped from its blink table.

    Runs on the caller thread, inside the result's liveness scope and the session's execution
    context. The publisher is named `massive.<dataset>#<stream id>`, and its blink table's
    shutdown (the result's release, or a failure) sets `stop`.

    Args:
        spec: The dataset, for its name and poll keys.
        schema: The dataset's schema.
        mode: How cycles combine.
        stream_id: The table's `StreamId`.
        stop: The poller's stop event.

    Returns:
        The result (append-only for `APPEND`, the `last_by` of the poll keys for `UPSERT`,
        and for `REPLACE` that `last_by` without its tombstones and their hidden column) and
        the publisher. Every schema ends with `TimestampInsert`, which no dataset keys on, so
        `last_by` always has a column to aggregate.
    """
    definitions = col_defs(schema)
    replace = mode is PollMode.REPLACE
    if replace:
        definitions[LIVE_COLUMN] = dtypes.bool_
    blink, publisher = table_publisher(
        f"massive.{spec.name}#{stream_id}", definitions, on_shutdown_callback=stop.set
    )
    if mode is PollMode.APPEND:
        return blink_to_append_only(blink), publisher
    keyed = blink.last_by(list(spec.poll_keys))
    if not replace:
        return keyed, publisher
    return keyed.where(LIVE_COLUMN).drop_columns(LIVE_COLUMN), publisher


def _build(
    core: SessionCore,
    spec: DatasetSpec,
    *,
    mode: PollMode,
    stream_id: int,
    stop: threading.Event,
) -> tuple[tuple[Table, TablePublisher], LivenessScope]:
    """Create a `poll()` table in a new result scope (`make_result`).

    Args:
        core: The session's core, for the dataset's schema.
        spec: The dataset.
        mode: How cycles combine.
        stream_id: The table's `StreamId`.
        stop: The poller's stop event.

    Returns:
        The result and its publisher (`build_table`), and the result scope. The result is the
        shape `build_table` derives, described last (`describe`), so it carries the dataset's
        column descriptions and lists its columns in schema order in every mode, also after
        the `last_by` of `UPSERT` and `REPLACE` put the poll keys first; the poller writes
        only through the publisher.
    """
    built: list[TablePublisher] = []
    schema = core.catalog.dataset_schema(spec)

    def shape() -> Table:
        table, publisher = build_table(spec, schema, mode, stream_id=stream_id, stop=stop)
        built.append(publisher)
        return describe(table, spec, schema)

    result, scope = make_result(shape)
    return (result, built[0]), scope


def open_poll(
    core: SessionCore,
    *,
    spec: DatasetSpec,
    request: Request,
    interval: timedelta,
    mode: PollMode,
    read: CycleReader,
    pollers: Pollers,
    host: str | None,
    path: str | None,
    clock: PollClock,
) -> Table:
    """Create a `poll()` table and start its poller.

    On the caller thread: the publisher and the result are created inside the result's own
    liveness scope (in the session's execution context); the table's row is registered in
    `m.streams()` with a new `StreamId`; the poller joins the session's pollers; and the result
    is tracked, so that releasing it stops the poller. Only then does the poller's thread
    start. If any step raises, even `KeyboardInterrupt`, every step already taken is undone
    and nothing keeps running.

    Args:
        core: The session's core.
        spec: The dataset, with a fetch path.
        request: The normalized arguments.
        interval: The time from one cycle's start to the next.
        mode: How cycles combine.
        read: Reads one cycle's rows.
        pollers: The session's live pollers.
        host: The REST host, for the row's `Host`.
        path: The fetch path, for the row's `Path`.
        clock: The time source of the waits between cycles.

    Returns:
        The live table.

    A session closed before the poller could be registered raises the `MassiveError` of
    `SessionCore.register_poll` or `Pollers.add`.
    """
    stream_id = core.next_stream_id()
    stop = threading.Event()
    with entered(core.exec_ctx):
        (result, publisher), scope = _build(core, spec, mode=mode, stream_id=stream_id, stop=stop)
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
    registered = False
    try:
        core.register_poll(
            StreamRow(
                stream_id=stream_id,
                dataset=spec.name,
                symbol_count=count,
                symbols=text,
                held_results=1,
                transport=Transport.REST,
                host=host,
                path=path,
                created_at=time.time_ns(),
            )
        )
        registered = True
        pollers.add(poller)
        core.results.track(result, scope, poller.release)
    except BaseException:
        try:
            poller.stop()
            pollers.discard(poller)
            if registered:
                core.release_poll(stream_id)
        finally:
            with entered(core.exec_ctx):
                scope.release()
        raise
    poller.start()
    return result
