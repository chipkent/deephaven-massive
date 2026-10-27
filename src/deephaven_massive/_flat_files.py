"""Flat-file history: streaming Massive's S3 `.csv.gz` files into static Deephaven tables.

Massive publishes one gzip CSV file per dataset and day in the `flatfiles` bucket at
`https://files.massive.com`, under keys `<prefix>/<YYYY>/<MM>/<YYYY-MM-DD>.csv.gz` (for
example `us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz`). This module is the flat-file
transport. `HistoryQuery.history()` decides that flat files serve a request
(`_history_source.choose_source`) and hands it the partition dates to read. The S3 access
itself (the bucket client, the key layout, the S3 errors, and the CSV decoding) is the wire
package's: `_massive.FlatFileClient` opens each file and `_massive.FlatFile` decodes it, and this
module builds the `_massive.ReadPlan` that says what to keep.

- `FlatFileHistory` reads the files of one `history()` call in parallel, at most
  `MassiveConfig.max_concurrency` at a time, each in blocks of `BLOCK_SIZE` bytes. Each block
  is first checked (futures sessions), then filtered to the requested symbols with
  `pyarrow.compute.is_in` (symbol pushdown) and to the half-open `[start, end)` range on
  `Timestamp`; only the surviving rows are renamed to the dataset's PascalCase columns, have
  their condition and indicator lists parsed, and are converted to the column types here. A
  file's surviving rows become one static Deephaven table per `_TABLE_BLOCKS` blocks of data
  (usually one table per file), and the tables are merged at the end. No block is ever
  converted to Python objects. Arrow reads about 32 blocks ahead of the consumer on its own
  I/O threads, so a reader holds about 32 blocks of decompressed text plus at most
  `_TABLE_BLOCKS` blocks of surviving rows, whatever the file's length.

Rows come in partition-date order, then (for futures) venue order (CME, CBOT, NYMEX, COMEX),
then file order; they are not sorted by time across venues. Only an empty CSV field is null:
text such as `NA` or `NULL` is data (both are valid tickers).

A missing file is judged by its date: on a weekend (for every class but crypto, which trades
every day) it is skipped silently; on a weekday it is skipped with a WARNING, since it is most
likely a market holiday. A missing file for the last requested date may instead be late: within
a day after its publication time (`_history_source.within_publication_grace`) it raises
`MassiveError` naming both causes, and after that it is skipped with a WARNING like any other
weekday.

Massive writes `0` where a timestamp has no value (`trf_timestamp` of a trade without a TRF), so
in every `Instant` column except `Timestamp` a `0` becomes null before unit conversion.

Futures files are published per venue (`us_futures_cme/trades_v1`, ...): the dataset's prefix
holds the placeholder `{venue}`, `venue=None` reads all four venues, and HTTP 403 on any venue
raises `EntitlementError` naming the venue and the `venue=` parameter; a 403 that rejects the S3
credentials themselves raises `AuthenticationError` for every class. Futures files are assumed
to be keyed by trading session (`session_end_date`), which Massive's documentation does not
state outright, so every block of every futures file is checked against its key's date, and a
mismatch (or a missing session date) raises `MassiveError` rather than return rows of the wrong
session. Every error of the wire package becomes its public counterpart, with its status and
request id: a rejected credential or a refusal gains the remedy (the settings to check, the plan,
the `venue=` parameter), and any other wire error goes through `_errors.from_wire`. An Arrow
error while converting or gathering a file's rows (out of memory, say) is a failed read of that
file.

A failed read, a `KeyboardInterrupt`, or `m.close()` stops every other read of the call at its
next block, and reads not yet started are cancelled. After a close, no request starts once a
reader has seen it; a request already in flight is discarded, and the call raises
`MassiveError("MassiveSession #1 is closed")`.
"""

from __future__ import annotations

import contextlib
import functools
import logging
import threading
from concurrent.futures import FIRST_EXCEPTION, CancelledError, ThreadPoolExecutor, wait
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final

import pyarrow as pa
import pyarrow.compute as pc
from deephaven import merge
from deephaven.arrow import to_table

from deephaven_massive._catalog import unit_rules
from deephaven_massive._dates import TimeRange
from deephaven_massive._enums import AssetClass, FuturesVenue, Transport
from deephaven_massive._errors import (
    AuthenticationError,
    EntitlementError,
    MassiveError,
    from_wire,
)
from deephaven_massive._history_source import within_publication_grace
from deephaven_massive._liveness import entered
from deephaven_massive._massive import (
    ReadColumn,
    ReadPlan,
    WireAuthError,
    WireEntitlementError,
    WireError,
    object_key,
)
from deephaven_massive._schema import ColumnType
from deephaven_massive._timeunits import TimeField, TimeUnitTable

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from concurrent.futures import Future
    from datetime import date

    from deephaven.table import Table

    from deephaven_massive._catalog import DatasetSpec
    from deephaven_massive._dates import FuturesSessions
    from deephaven_massive._massive import FlatFileClient
    from deephaven_massive._query import Request
    from deephaven_massive._session import SessionCore
    from deephaven_massive._symbols import NormalizedSymbols

__all__ = ["BLOCK_SIZE", "FlatFileHistory"]

logger = logging.getLogger(__name__)

BLOCK_SIZE: Final = 1024 * 1024
"""Bytes of decompressed CSV per block. Arrow reads about 32 blocks ahead of the consumer, so
this sets a reader's memory: about 32 MiB of text in flight per concurrent reader."""

_TABLE_BLOCKS: Final = 16
"""A file's surviving rows are converted to a Deephaven table each time they reach this many
blocks of Arrow data, and at the end of the file. Fewer, larger tables cost far less JVM heap
than one per block (each table carries a fixed overhead), while memory stays bounded."""

_SATURDAY: Final = 5


@dataclass(frozen=True, slots=True)
class _Key:
    """One S3 object to read.

    Attributes:
        key: The object key, such as `us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz`.
        day: The file's partition date (for futures, its trading session).
        venue: The futures venue of a futures key; None otherwise.
    """

    key: str
    day: date
    venue: FuturesVenue | None


def _keys(prefix: str, venue: FuturesVenue | None, dates: Sequence[date]) -> tuple[_Key, ...]:
    """Return the S3 objects a request reads, by date and then by venue.

    Args:
        prefix: The dataset's prefix for the request; a futures prefix holds `{venue}`.
        venue: The futures venue to read, or None for every venue.
        dates: The partition dates.

    Returns:
        One `_Key` per date, or per date and venue (in `FuturesVenue` order) for a futures
        prefix.
    """
    if "{venue}" not in prefix:
        venues: tuple[FuturesVenue | None, ...] = (None,)
    else:
        venues = tuple(FuturesVenue) if venue is None else (venue,)
    keys = []
    for day in dates:
        for each in venues:
            name = None if each is None else each.value
            keys.append(_Key(object_key(prefix, day, name), day, each))
    return tuple(keys)


def _read_type(column_type: ColumnType) -> pa.DataType:
    """Return the Arrow type the CSV reader parses a column's header as.

    Args:
        column_type: The type of the column the header fills.

    Returns:
        `int64` for an `Instant` (epoch integers, converted by declared unit), `string` for an
        `int[]` list (parsed after filtering), and the column's own Arrow type otherwise.
    """
    if column_type is ColumnType.INSTANT:
        return pa.int64()
    if column_type is ColumnType.INT_ARRAY:
        return pa.string()
    return column_type.arrow_type


@dataclass(frozen=True, eq=False, slots=True)
class _Transform:
    """How one request turns flat-file batches into blocks of the dataset's schema.

    Built once per `history()` call by `build` and shared, read-only, by the reader threads.
    The wire package's reader does the filtering, renaming, and list parsing the `plan` asks
    for; `apply` then converts each surviving batch to the dataset's schema.

    Attributes:
        dataset: The dataset name, for time-unit lookups and messages.
        plan: What every file of the request reads and keeps; `plan_for` adds a futures
            file's session.
        columns: Per schema column, in order: its type, its name, and the header that fills
            it, or None for a column the files do not carry (null).
        arrow_schema: The dataset's Arrow schema.
        units: The dataset's declared timestamp units.
        session_header: For futures, the header that holds the session date; None otherwise.
    """

    dataset: str
    plan: ReadPlan
    columns: tuple[tuple[ColumnType, str, str | None], ...]
    arrow_schema: pa.Schema
    units: TimeUnitTable
    session_header: str | None

    @classmethod
    def build(
        cls,
        spec: DatasetSpec,
        symbols: NormalizedSymbols | None,
        window: TimeRange | FuturesSessions,
    ) -> _Transform:
        """Prepare the transform of a request from the dataset's flat-file key map.

        `DatasetSpec` guarantees, at import, that a dataset with flat files has a `FLAT_FILES`
        key map filling `Symbol` and `Timestamp`, and `SessionEndDate` for futures.

        Args:
            spec: The dataset; its `FLAT_FILES` key map names the headers to read.
            symbols: The canonical symbols to keep, as a tuple; anything else (`dm.ALL`, or
                None for a dataset without instruments) keeps every row.
            window: The normalized `history()` bounds. Futures sessions keep every time,
                since their files hold exactly the requested sessions.

        Returns:
            The transform.
        """
        key_map = spec.key_maps[Transport.FLAT_FILES]
        header_of = {column: header for header, column in key_map.items()}
        types = {column.name: column.type for column in spec.schema.columns}
        futures = spec.asset_class is AssetClass.FUTURES
        units = TimeUnitTable(unit_rules({spec.name: spec}))
        timestamp_header = header_of["Timestamp"]
        to_ns = functools.partial(
            units.to_epoch_ns_array, TimeField(spec.name, Transport.FLAT_FILES, timestamp_header)
        )
        plan = ReadPlan(
            columns=tuple(
                ReadColumn(
                    header,
                    column,
                    _read_type(types[column]),
                    types[column].arrow_type if types[column] is ColumnType.INT_ARRAY else None,
                )
                for header, column in key_map.items()
            ),
            symbol_header=header_of["Symbol"],
            symbols=pa.array(symbols, pa.string()) if isinstance(symbols, tuple) else None,
            time_header=timestamp_header,
            window_ns=((window.start_ns, window.end_ns) if isinstance(window, TimeRange) else None),
            to_ns=to_ns,
            session=None,
            block_size=BLOCK_SIZE,
        )
        return cls(
            dataset=spec.name,
            plan=plan,
            columns=tuple((c.type, c.name, header_of.get(c.name)) for c in spec.schema.columns),
            arrow_schema=spec.schema.arrow_schema(),
            units=units,
            session_header=header_of["SessionEndDate"] if futures else None,
        )

    def plan_for(self, key: _Key) -> ReadPlan:
        """Return the read plan of one file: a futures file must hold only its own session.

        Args:
            key: The file.

        Returns:
            The request's plan, with the session check of a futures file.
        """
        if self.session_header is None:
            return self.plan
        return replace(self.plan, session=(self.session_header, key.day))

    def apply(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Turn one batch of surviving rows into a block of the dataset's schema.

        Pure Arrow: unit conversion of every `Instant`, and nulls for the columns the files
        lack. The lists are already parsed, and the rows already filtered, by the reader.

        Args:
            batch: One block's surviving rows, with the plan's columns.

        Returns:
            The block in the dataset's schema.

        A timestamp outside the declared unit's range raises the `MassiveError` of
        `TimeUnitTable.to_epoch_ns_array`.
        """
        arrays = [self._column(batch, column) for column in self.columns]
        return pa.RecordBatch.from_arrays(arrays, schema=self.arrow_schema)

    def _column(
        self, batch: pa.RecordBatch, column: tuple[ColumnType, str, str | None]
    ) -> pa.Array:
        """Convert one column's values to its type, or make the column of nulls it lacks.

        Args:
            batch: The filtered batch.
            column: The column's type, its name, and the header that fills it, or None.

        Returns:
            An Arrow array of the column's type, one value per row of `batch`. In an `Instant`
            column other than `Timestamp`, an epoch value of `0` is null.
        """
        column_type, name, header = column
        if header is None:
            return pa.nulls(batch.num_rows, column_type.arrow_type)
        values = batch[name]
        if column_type is ColumnType.INSTANT:
            if header != self.plan.time_header:
                # Massive writes 0 where a timestamp has no value; WebSocket and REST omit it.
                values = pc.if_else(pc.equal(values, 0), pa.scalar(None, pa.int64()), values)
            return pc.cast(self._nanos(header, values), column_type.arrow_type)
        return values

    def _nanos(self, header: str, values: pa.Array) -> pa.Array:
        """Convert a timestamp header's epoch integers to nanoseconds by its declared unit.

        Args:
            header: The CSV header, such as `"sip_timestamp"`.
            values: Its `int64` values.

        Returns:
            Epoch nanoseconds, `int64`.
        """
        field_id = TimeField(self.dataset, Transport.FLAT_FILES, header)
        return self.units.to_epoch_ns_array(field_id, values)


class _CancelledError(Exception):
    """A read stopped because another read of the same call failed or was interrupted."""


class FlatFileHistory:
    """The flat-file transport of one `history()` call: reads and the table.

    `SessionCore.flat_file_transport()` creates one per call, so it may hold the session's core
    strongly; the long-lived state, the S3 client, lives in the session's `FlatFileClient`.
    `history()` runs on the caller thread and starts at most `max_concurrency` reader threads
    of its own, which enter the session's execution context and end before it returns.
    """

    __slots__ = ("_client", "_core")

    def __init__(self, core: SessionCore, client: FlatFileClient) -> None:
        """Bind the transport to its session and the session's flat-file client.

        Args:
            core: The session's core: configuration, label, and execution context.
            client: The session's flat-file client.
        """
        self._core = core
        self._client = client

    def history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: TimeRange | FuturesSessions,
        *,
        dates: Sequence[date],
    ) -> Table:
        """Return the rows of `window` from the dataset's flat files, as one static table.

        Flat files have already been chosen for the request
        (`_history_source.choose_source`), so the dataset has a file for its timespan. Files
        are read by a pool of `min(max_concurrency, files)` threads, which end before this
        method returns. If any read fails, or this thread is interrupted, every other read
        stops at its next block, reads that have not started are cancelled, and the failure of
        the earliest file is raised. Closing the session stops the reads the same way. Logs
        `source=S3 files=<n> skipped=<dates>` at INFO.

        Args:
            spec: The dataset.
            request: The normalized arguments, with the raw `start` and `end` bounds, and
                static symbols (the query reads a reactive symbol table before this call).
            window: The bounds under the class's day rules (`_dates.bounds`).
            dates: The partition dates to read (`_history_source.partition_dates`).

        Returns:
            A static table in the dataset's schema: the files' rows in date order (and, for
            futures, venue order within a date), each file's rows in file order. A range
            without rows gives an empty table with every column.

        Errors, all raised on the calling thread: `AuthenticationError` when S3 rejects the S3
        credentials; `EntitlementError` when S3 answers HTTP 403 for a file otherwise, naming
        the venue and the `venue=` parameter for futures; `MassiveError` when the file for the
        last requested date is missing on a trading day within a day after its publication time
        (`"... is not yet published (flat files land ~11:00 ET next day), or was a market
        holiday"`), when the S3 client cannot be created, the bucket is unreachable or answers
        another error, a file is not the expected gzip CSV (for example, a documented header is
        missing), a futures file holds another session, or the session is closed during the
        call. Errors that come from S3 carry its `x-amz-request-id` as `request_id`.
        """
        prefix = spec.flat_file_prefixes[request.timespan]
        transform = _Transform.build(spec, request.symbols, window)
        return self._read(spec, transform, _keys(prefix, request.venue, dates), last=dates[-1])

    def _read(
        self, spec: DatasetSpec, transform: _Transform, keys: Sequence[_Key], *, last: date
    ) -> Table:
        """Read files on a pool of reader threads and merge their rows into one static table.

        Waits until every read has finished or one has failed. On a failure, or an interrupt
        of this thread, it sets the call's cancel event, which stops the other reads at their
        next block, cancels the reads not yet started, and waits for the pool to drain; then it
        raises the failure of the earliest file (an interrupt is re-raised as is). If the
        session was closed meanwhile, it raises `"MassiveSession #1 is closed"`, whatever the
        reads returned: a request in flight at the close may have been answered with any
        outcome. The missing-file error of `_skip_missing` propagates.

        Args:
            spec: The dataset.
            transform: The request's block transform.
            keys: The files to read, in result order.
            last: The last partition date of the request.

        Returns:
            The merged table.
        """
        cancel = threading.Event()
        workers = min(self._core.config.max_concurrency, len(keys))
        with ThreadPoolExecutor(workers, f"{self._core.label} flat files") as pool:
            futures = [pool.submit(self._read_key, transform, key, cancel=cancel) for key in keys]
            try:
                wait(futures, return_when=FIRST_EXCEPTION)
            finally:
                cancel.set()
                pool.shutdown(cancel_futures=True)
        # A request already past its check when the session closed may have been answered with
        # any outcome (a row, a missing file, a refusal): report the close instead.
        self._check_open()
        parts = [_outcome(future) for future in futures]
        missing = [key for key, part in zip(keys, parts, strict=True) if part is None]
        self._skip_missing(spec, missing, last=last)
        logger.info(
            "%s history: source=S3 files=%d skipped=%s",
            spec.name,
            len(keys) - len(missing),
            ",".join(sorted({key.day.isoformat() for key in missing})) or "none",
        )
        tables = [table for part in parts if part is not None for table in part]
        with entered(self._core.exec_ctx):
            # Merging onto an empty table in the schema keeps every column when no row matched.
            return merge([to_table(transform.arrow_schema.empty_table()), *tables])

    def _check_open(self) -> None:
        """Reject further work once the session's flat-file client is closed.

        Raises:
            MassiveError: `"MassiveSession #1 is closed"` once the session has closed.
        """
        if self._client.closed:
            raise MassiveError(f"{self._core.label} is closed")

    def _check(self, cancel: threading.Event) -> None:
        """Stop a read if the session was closed or another read of the call failed.

        Called before each request and after each block. A closed session raises the
        `MassiveError` of `_check_open`.

        Args:
            cancel: The call's cancel event.

        Raises:
            _CancelledError: If the call was cancelled.
        """
        self._check_open()
        if cancel.is_set():
            raise _CancelledError

    def _read_key(
        self, transform: _Transform, key: _Key, *, cancel: threading.Event
    ) -> list[Table] | None:
        """Stream one file into static Deephaven tables of its surviving rows.

        Runs on a reader thread, inside the session's execution context for the Deephaven
        calls. Surviving rows are gathered per file and converted once they reach
        `_TABLE_BLOCKS` blocks of data and at the end of the file, so a file usually becomes
        one table; when a selective read gathers `_TABLE_BLOCKS` small batches first, they are
        merged into one, so the per-batch overhead never grows with the file. The file, and
        its batch generator, are closed however the read ends.

        Args:
            transform: The request's block transform.
            key: The file to read.
            cancel: The call's cancel event, checked before the request and after each block.

        Returns:
            The file's tables, or None if the file does not exist.

        Raises:
            MassiveError: If converting or gathering the file's rows fails in Arrow (out of
                memory, say): `"cannot read <location>: <Arrow's message>"`. A failed request
                or read (for example, a file that is not the expected gzip CSV) raises the
                public error of `_public`: `MassiveError`, `AuthenticationError`, or
                `EntitlementError`. A closed session raises the `MassiveError` of
                `_check_open`, and a cancelled call `_CancelledError`.
        """
        self._check(cancel)
        try:
            file = self._client.open(key.key)
            if file is None:
                return None
            plan = transform.plan_for(key)
            with file, contextlib.closing(file.batches(plan)) as batches:
                self._check(cancel)
                try:
                    return self._gather(
                        batches, transform, limit=_TABLE_BLOCKS * plan.block_size, cancel=cancel
                    )
                except pa.ArrowException as error:
                    raise MassiveError(f"cannot read {file.location}: {error}") from error
        except WireError as error:
            public = _public(error, key)
            raise public from error

    def _gather(
        self,
        batches: Iterator[pa.RecordBatch],
        transform: _Transform,
        *,
        limit: int,
        cancel: threading.Event,
    ) -> list[Table]:
        """Convert a file's batches to the schema and gather them into static tables.

        Args:
            batches: The file's batches, one per block.
            transform: The request's block transform.
            limit: The bytes of surviving rows that make one table.
            cancel: The call's cancel event, checked after each block.

        Returns:
            The file's tables, in file order.
        """
        tables: list[Table] = []
        pending: list[pa.RecordBatch] = []
        size = 0
        with entered(self._core.exec_ctx):
            for batch in batches:
                rows = transform.apply(batch)
                if rows.num_rows:
                    pending.append(rows)
                    size += rows.nbytes
                if size >= limit:
                    tables.append(_convert(pending, transform))
                    pending, size = [], 0
                elif len(pending) >= _TABLE_BLOCKS:
                    # Every batch carries allocation overhead that nbytes does not count;
                    # merging the small batches of a selective read keeps its memory flat
                    # however long the file is.
                    gathered = pa.Table.from_batches(pending, schema=transform.arrow_schema)
                    pending = gathered.combine_chunks().to_batches()
                self._check(cancel)
            if pending:
                tables.append(_convert(pending, transform))
        return tables

    def _skip_missing(self, spec: DatasetSpec, missing: Sequence[_Key], *, last: date) -> None:
        """Apply the missing-file rules to the files that did not exist.

        A weekend file is skipped silently (except for crypto, which trades every day); a
        weekday file is skipped with a WARNING (most likely a market holiday). The file for the
        last requested date may instead be late, so within
        `_history_source.PUBLICATION_GRACE` after its publication time it raises; after that it
        is skipped with a WARNING too.

        Args:
            spec: The dataset.
            missing: The keys that S3 reported as missing, in date order.
            last: The last partition date of the request.

        Raises:
            MassiveError: If the file for `last` is missing on a trading day within a day after
                its publication time: `"<dataset>: <key> does not exist: <date> is not yet
                published (flat files land ~11:00 ET next day), or was a market holiday"`.
        """
        cutoff = self._core.config.flat_file_publish_cutoff
        for key in missing:
            trading = spec.asset_class is AssetClass.CRYPTO or key.day.weekday() < _SATURDAY
            if not trading:
                continue
            day = key.day.isoformat()
            if key.day == last and within_publication_grace(spec, key.day, cutoff=cutoff):
                raise MassiveError(
                    f"{spec.name}: {key.key} does not exist: {day} is not yet published (flat "
                    "files land ~11:00 ET next day), or was a market holiday"
                )
            logger.warning(
                "%s: %s does not exist; skipping %s (a market holiday?)", spec.name, key.key, day
            )


def _public(error: WireError, key: _Key) -> MassiveError:
    """Return the public error for a wire error of one file, with the user's remedy.

    A rejected credential names the settings to check; a refusal names the plan and, for a
    futures file, the venue and the `venue=` parameter. Every other wire error becomes its
    public counterpart through `_errors.from_wire`. Each keeps the wire error's status and
    request id.

    Args:
        error: The wire error.
        key: The file it came from.

    Returns:
        `AuthenticationError`, `EntitlementError`, or `MassiveError`.
    """
    if isinstance(error, WireAuthError):
        return AuthenticationError(
            f"S3 rejected the flat-file credentials: {error.message}; check MassiveConfig "
            "s3_access_key_id and s3_secret_access_key",
            status=error.status,
            request_id=error.request_id,
        )
    if isinstance(error, WireEntitlementError):
        if key.venue is None:
            return EntitlementError(
                f"the plan does not include these flat files: {error.message}",
                status=error.status,
                request_id=error.request_id,
            )
        return EntitlementError(
            f"the plan does not include {key.venue.name} futures flat files: {error.message}; "
            "pass venue=dm.FuturesVenue.<VENUE> for a venue the plan includes",
            status=error.status,
            request_id=error.request_id,
        )
    return from_wire(error)


def _convert(batches: list[pa.RecordBatch], transform: _Transform) -> Table:
    """Turn a file's gathered surviving rows into one static Deephaven table.

    Runs on a reader thread inside the session's execution context.

    Args:
        batches: Blocks in the dataset's schema, each with at least one row.
        transform: The request's transform, for the schema.

    Returns:
        The static table.
    """
    return to_table(pa.Table.from_batches(batches, schema=transform.arrow_schema))


def _outcome(future: Future[list[Table] | None]) -> list[Table] | None:
    """Return a finished read's tables, or re-raise its failure.

    Called in file order after the pool has drained, so the first failure raised is the
    earliest file's. A read that was cancelled before it started, or stopped because another
    read failed, is skipped over: the read that caused it raises in its place. A failure that
    is not a `MassiveError` (a defect) propagates unchanged.

    Args:
        future: A read of the call.

    Returns:
        The read's tables, None for a missing file, or None for a cancelled or stopped read
        (only ever reached alongside a real failure, which is raised first or later in file
        order).

    Raises:
        MassiveError: The read's own failure (`EntitlementError` for a refusal).
    """
    try:
        return future.result()
    except (CancelledError, _CancelledError):
        return None
    except MassiveError:
        raise
