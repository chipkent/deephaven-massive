"""Flat-file history: streaming Massive's S3 `.csv.gz` files into static Deephaven tables.

Massive publishes one gzip CSV file per dataset and day in the `flatfiles` bucket at
`https://files.massive.com`, under keys `<prefix>/<YYYY>/<MM>/<YYYY-MM-DD>.csv.gz` (for
example `us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz`). This module is the flat-file
transport: pure S3 I/O. `HistoryQuery.history()` decides that flat files serve a request
(`_sources.choose_source`) and hands it the partition dates to read.

- `FlatFileBucket` is the session's connection to the bucket: one boto3 S3 client, created on
  first use and closed with the session.
- `FlatFileHistory` reads the files of one `history()` call in parallel, at most
  `MassiveConfig.max_concurrency` at a time, each with `pyarrow.csv.open_csv` in blocks of
  `BLOCK_SIZE` bytes. Each block is first checked (futures sessions), then filtered to the
  requested symbols with `pyarrow.compute.is_in` (symbol pushdown) and to the half-open
  `[start, end)` range on `Timestamp`; only the surviving rows are renamed to the dataset's
  PascalCase columns, converted to the column types, and have their condition and indicator
  lists parsed. A file's surviving rows become one static Deephaven table per
  `_TABLE_BLOCKS` blocks of data (usually one table per file), and the tables are merged at
  the end. No block is ever converted to Python objects. Arrow reads about 32 blocks ahead of
  the consumer on its own I/O threads, so a reader holds about 32 blocks of decompressed text
  plus at most `_TABLE_BLOCKS` blocks of surviving rows, whatever the file's length.

Rows come in partition-date order, then (for futures) venue order (CME, CBOT, NYMEX, COMEX),
then file order; they are not sorted by time across venues. Only an empty CSV field is null:
text such as `NA` or `NULL` is data (both are valid tickers).

A missing file is judged by its date: on a weekend (for every class but crypto, which trades
every day) it is skipped silently; on a weekday it is skipped with a WARNING, since it is most
likely a market holiday; for the last requested date it raises `MassiveError`, since that file
is most likely not yet published.

Futures files are published per venue (`us_futures_cme/trades_v1`, ...): the dataset's prefix
holds the placeholder `{venue}`, `venue=None` reads all four venues, and HTTP 403 on any venue
raises `EntitlementError` naming the venue and the `venue=` parameter. Futures files are assumed
to be keyed by trading session (`session_end_date`), which Massive's documentation does not
state outright, so every block of every futures file is checked against its key's date, and a
mismatch (or a missing session date) raises `MassiveError` rather than return rows of the wrong
session.

A failed read, a `KeyboardInterrupt`, or `m.close()` stops every other read of the call at its
next block, and reads not yet started are cancelled. After a close, no request starts once a
reader has seen it; a request already in flight is discarded, and the call raises
`MassiveError("MassiveSession #1 is closed")`.
"""

from __future__ import annotations

import gzip
import logging
import threading
import zlib
from concurrent.futures import FIRST_EXCEPTION, CancelledError, ThreadPoolExecutor, wait
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Final

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from deephaven import merge
from deephaven.arrow import to_table

from deephaven_massive._catalog import unit_rules
from deephaven_massive._dates import TimeRange
from deephaven_massive._enums import AssetClass, FuturesVenue, Transport
from deephaven_massive._errors import EntitlementError, MassiveError
from deephaven_massive._liveness import entered
from deephaven_massive._schema import ColumnType
from deephaven_massive._sources import NOT_PUBLISHED
from deephaven_massive._timeunits import TimeField, TimeUnitTable

if TYPE_CHECKING:
    from collections.abc import Sequence
    from concurrent.futures import Future
    from datetime import date

    from deephaven.table import Table
    from types_boto3_s3.client import S3Client

    from deephaven_massive._catalog import DatasetSpec
    from deephaven_massive._config import MassiveConfig
    from deephaven_massive._dates import FuturesSessions
    from deephaven_massive._query import Request
    from deephaven_massive._session import SessionCore
    from deephaven_massive._symbols import NormalizedSymbols

__all__ = ["BLOCK_SIZE", "FlatFileBucket", "FlatFileHistory"]

logger = logging.getLogger(__name__)

BLOCK_SIZE: Final = 1024 * 1024
"""Bytes of decompressed CSV per block. Arrow reads about 32 blocks ahead of the consumer, so
this sets a reader's memory: about 32 MiB of text in flight per concurrent reader."""

_TABLE_BLOCKS: Final = 16
"""A file's surviving rows are converted to a Deephaven table each time they reach this many
blocks of Arrow data, and at the end of the file. Fewer, larger tables cost far less JVM heap
than one per block (each table carries a fixed overhead), while memory stays bounded."""

_CREDENTIAL_ERRORS: Final = frozenset({"InvalidAccessKeyId", "SignatureDoesNotMatch"})
"""S3 error codes, answered with HTTP 403, that mean the S3 credentials themselves are wrong."""

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
            base = prefix if each is None else prefix.format(venue=each.value)
            keys.append(_Key(f"{base}/{day:%Y}/{day:%m}/{day.isoformat()}.csv.gz", day, each))
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
    """How one request turns CSV blocks into blocks of the dataset's schema.

    Built once per `history()` call by `build` and shared, read-only, by the reader threads.

    Attributes:
        dataset: The dataset name, for time-unit lookups and messages.
        read_options: Block size and threading of the CSV reader.
        convert_options: The headers to read, the type each is parsed as, and the one null
            marker, the empty field.
        columns: Per schema column, in order: its type and the header that fills it, or None
            for a column the files do not carry (null).
        arrow_schema: The dataset's Arrow schema.
        units: The dataset's declared timestamp units.
        symbol_header: The header that fills `Symbol`.
        timestamp_header: The header that fills `Timestamp`.
        symbols: The requested canonical symbols, or None for every symbol.
        window: The half-open instant range rows must fall in, or None for futures sessions,
            whose files hold exactly the requested sessions.
        session_header: For futures, the header that holds the session date; None otherwise.
    """

    dataset: str
    read_options: pacsv.ReadOptions
    convert_options: pacsv.ConvertOptions
    columns: tuple[tuple[ColumnType, str | None], ...]
    arrow_schema: pa.Schema
    units: TimeUnitTable
    symbol_header: str
    timestamp_header: str
    symbols: pa.Array | None
    window: TimeRange | None
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
            window: The normalized `history()` bounds.

        Returns:
            The transform.
        """
        key_map = spec.key_maps[Transport.FLAT_FILES]
        header_of = {column: header for header, column in key_map.items()}
        types = {column.name: column.type for column in spec.schema.columns}
        futures = spec.asset_class is AssetClass.FUTURES
        read_types = {header: _read_type(types[column]) for header, column in key_map.items()}
        return cls(
            dataset=spec.name,
            read_options=pacsv.ReadOptions(block_size=BLOCK_SIZE, use_threads=False),
            convert_options=pacsv.ConvertOptions(
                column_types=read_types,
                include_columns=list(read_types),
                # Massive writes a missing value as an empty field and nothing else. Arrow's
                # default null markers include `NA` and `NULL`, which are real tickers.
                null_values=[""],
                strings_can_be_null=True,
            ),
            columns=tuple((c.type, header_of.get(c.name)) for c in spec.schema.columns),
            arrow_schema=spec.schema.arrow_schema(),
            units=TimeUnitTable(unit_rules({spec.name: spec})),
            symbol_header=header_of["Symbol"],
            timestamp_header=header_of["Timestamp"],
            symbols=pa.array(symbols, pa.string()) if isinstance(symbols, tuple) else None,
            window=window if isinstance(window, TimeRange) else None,
            session_header=header_of["SessionEndDate"] if futures else None,
        )

    def apply(self, block: pa.RecordBatch, key: _Key) -> pa.RecordBatch:
        """Turn one CSV block into a block of the dataset's schema, keeping the requested rows.

        Pure Arrow, in this order: the futures session check on the whole block, the symbol
        filter, the `[start, end)` filter on the converted `Timestamp`, and then, on the
        surviving rows only, unit conversion of every `Instant`, list parsing of every `int[]`
        column (`"12,37"` becomes `[12, 37]`), and nulls for the columns the files lack.

        Args:
            block: One block as the CSV reader parsed it.
            key: The file it came from.

        Returns:
            The block in the dataset's schema; possibly empty.

        Raises:
            MassiveError: If a futures block holds a session date other than its key's date, or
                none. A timestamp outside the declared unit's range raises the `MassiveError`
                of `TimeUnitTable.to_epoch_ns_array`; a condition list that is not integers
                raises `pyarrow.ArrowInvalid`.
        """
        if self.session_header is not None:
            sessions = block[self.session_header]
            matches = pc.fill_null(pc.equal(sessions, pa.scalar(key.day, pa.date32())), False)
            wrong = pc.filter(sessions, pc.invert(matches))
            if len(wrong):
                found = wrong[0].as_py()
                shown = "empty" if found is None else found.isoformat()
                raise MassiveError(
                    f"{key.key} holds session_end_date {shown}, not {key.day.isoformat()}: "
                    "futures flat files are not keyed by trading session as this library "
                    "assumes, so their rows cannot be assigned to sessions"
                )
        if self.symbols is not None:
            block = block.filter(pc.is_in(block[self.symbol_header], value_set=self.symbols))
        if self.window is not None:
            nanos = self._nanos(self.timestamp_header, block[self.timestamp_header])
            inside = pc.and_(
                pc.greater_equal(nanos, self.window.start_ns), pc.less(nanos, self.window.end_ns)
            )
            block = block.filter(inside)
        arrays = [self._column(block, column_type, header) for column_type, header in self.columns]
        return pa.RecordBatch.from_arrays(arrays, schema=self.arrow_schema)

    def _column(
        self, block: pa.RecordBatch, column_type: ColumnType, header: str | None
    ) -> pa.Array:
        """Convert one header's values to its column, or make the column of nulls it lacks.

        Args:
            block: The filtered block.
            column_type: The column's type.
            header: The header that fills the column, or None.

        Returns:
            An Arrow array of the column's type, one value per row of `block`.
        """
        if header is None:
            return pa.nulls(block.num_rows, column_type.arrow_type)
        values = block[header]
        if column_type is ColumnType.INSTANT:
            return pc.cast(self._nanos(header, values), column_type.arrow_type)
        if column_type is ColumnType.INT_ARRAY:
            return pc.cast(pc.split_pattern(values, ","), column_type.arrow_type)
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


def _s3_client(config: MassiveConfig) -> S3Client:
    """Create the boto3 S3 client of the flat-file bucket from the configuration.

    A dedicated boto3 session is created, because boto3's default session is not safe to
    create from several threads. Requests are signed with Signature Version 4, as Massive's
    endpoint requires, and the connection pool holds one connection per concurrent reader.

    Args:
        config: The session's configuration, with both S3 credentials set.

    Returns:
        A client for `config.s3_endpoint_url`; it makes no request until it is used.
    """
    session = boto3.session.Session(
        aws_access_key_id=config.s3_access_key_id,
        aws_secret_access_key=config.s3_secret_access_key,
    )
    return session.client(
        "s3",
        endpoint_url=config.s3_endpoint_url,
        config=Config(signature_version="s3v4", max_pool_connections=config.max_concurrency),
    )


class FlatFileBucket:
    """A session's connection to the flat-file bucket: one S3 client, created on first use.

    Created by `SessionCore.__init__`, which registers `close` as one of the session's closers,
    so the client is closed by `m.close()` or, for a session garbage-collected without
    `close()`, on the session's reaper thread. Once closed, the bucket makes no further request:
    reads in progress stop at their next block. It holds the configuration and nothing of the
    session, so it never keeps a session alive. Thread-safe: the client is created and closed
    under the bucket's own leaf lock (no network call, no Deephaven call, and no wait happen
    while it is held), and boto3 clients are safe to share between threads.
    """

    __slots__ = ("_client", "_closed", "_config", "_label", "_lock")

    def __init__(self, config: MassiveConfig, label: str) -> None:
        """Remember the configuration; no client is created and nothing touches the network.

        Args:
            config: The session's configuration (credentials, endpoint, concurrency).
            label: The session's label, such as `"MassiveSession #1"`, for error messages.
        """
        self._config = config
        self._label = label
        self._lock = threading.Lock()
        self._client: S3Client | None = None
        self._closed = threading.Event()

    @property
    def closed(self) -> bool:
        """True once `close()` has run."""
        return self._closed.is_set()

    def client(self) -> S3Client:
        """Return the bucket's S3 client, creating it on the first call.

        Creating the client reads local AWS configuration but makes no request, so it is done
        under the bucket's lock; concurrent first calls create exactly one client.

        Returns:
            The shared boto3 S3 client.

        Raises:
            MassiveError: If the bucket was closed (the session was closed while a `history()`
                call that had already started was about to read), or the SDK cannot create the
                client, for example because `AWS_PROFILE` names a profile that does not exist.
        """
        with self._lock:
            self.check_open()
            if self._client is None:
                try:
                    self._client = _s3_client(self._config)
                except BotoCoreError as error:
                    raise MassiveError(
                        f"cannot create the S3 client for {self._config.s3_endpoint_url}: {error}"
                    ) from error
            return self._client

    def check_open(self) -> None:
        """Reject further use of a closed bucket.

        Raises:
            MassiveError: `"MassiveSession #1 is closed"` once `close()` has run.
        """
        if self._closed.is_set():
            raise MassiveError(f"{self._label} is closed")

    def close(self) -> None:
        """Close the client, if one was created, and refuse every further request.

        Idempotent. Runs as a session closer: on the thread that closes the session, or on the
        reaper thread for a session collected without `close()`. Reads in progress stop at
        their next block with `MassiveError`.
        """
        with self._lock:
            self._closed.set()
            client, self._client = self._client, None
        if client is not None:
            client.close()


class _CancelledError(Exception):
    """A read stopped because another read of the same call failed or was interrupted."""


class FlatFileHistory:
    """The flat-file transport of one `history()` call: source choice, reads, and the table.

    `SessionCore.flat_file_transport()` creates one per call, so it may hold the session's core
    strongly; the long-lived state, the S3 client, lives in the session's `FlatFileBucket`.
    `history()` runs on the caller thread and starts at most `max_concurrency` reader threads
    of its own, which enter the session's execution context and end before it returns.
    """

    __slots__ = ("_bucket", "_core")

    def __init__(self, core: SessionCore, bucket: FlatFileBucket) -> None:
        """Bind the transport to its session and the session's bucket.

        Args:
            core: The session's core: configuration, execution context, and REST transport.
            bucket: The session's flat-file bucket.
        """
        self._core = core
        self._bucket = bucket

    def history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: TimeRange | FuturesSessions,
        *,
        dates: Sequence[date],
    ) -> Table:
        """Return the rows of `window` from the dataset's flat files, as one static table.

        Flat files have already been chosen for the request (`_sources.choose_source`), so the
        dataset has a file for its timespan. Files are read by a pool of
        `min(max_concurrency, files)` threads, which end before this method returns. If any read
        fails, or this thread is interrupted, every other read stops at its next block, reads
        that have not started are cancelled, and the failure of the earliest file is raised.
        Closing the session stops the reads the same way. Logs
        `source=S3 files=<n> skipped=<dates>` at INFO.

        Args:
            spec: The dataset.
            request: The normalized arguments, with the raw `start` and `end` bounds, and
                static symbols (the query reads a reactive symbol table before this call).
            window: The bounds under the class's day rules (`_dates.bounds`).
            dates: The partition dates to read (`_sources.partition_dates`).

        Returns:
            A static table in the dataset's schema: the files' rows in date order (and, for
            futures, venue order within a date), each file's rows in file order. A range
            without rows gives an empty table with every column.

        Errors, all raised on the calling thread: `EntitlementError` when S3 answers HTTP 403
        for a file, naming the venue and the `venue=` parameter for futures, or the S3
        credentials when S3 rejects them; `MassiveError` when the file for the last requested
        date is missing on a trading day (`"... not yet published; flat files land ~11:00 ET
        next day"`), when the S3 client cannot be created, the bucket is unreachable or answers
        another error, a file is not the expected gzip CSV (for example, a documented header is
        missing), a futures file holds another session, or the session is closed during the
        call.
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
        session was closed meanwhile, it closes the client once more, now that no read is
        running, and raises the `MassiveError` of `FlatFileBucket.check_open`, whatever the
        reads returned. The missing-file error of `_skip_missing` propagates.

        Args:
            spec: The dataset.
            transform: The request's block transform.
            keys: The files to read, in result order.
            last: The last partition date of the request.

        Returns:
            The merged table.
        """
        client = self._bucket.client()
        cancel = threading.Event()
        workers = min(self._core.config.max_concurrency, len(keys))
        with ThreadPoolExecutor(workers, f"{self._core.label} flat files") as pool:
            futures = [
                pool.submit(self._read_key, client, transform, key, cancel=cancel) for key in keys
            ]
            try:
                wait(futures, return_when=FIRST_EXCEPTION)
            finally:
                cancel.set()
                pool.shutdown(cancel_futures=True)
        if self._bucket.closed:
            # A request already past its check when the session closed may have opened a new
            # connection pool, and its answer may look like any outcome (a row, a missing file,
            # a refusal). No read of this call is running now: close the client once more and
            # report the close.
            client.close()
            self._bucket.check_open()
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

    def _check(self, cancel: threading.Event) -> None:
        """Stop a read if the session was closed or another read of the call failed.

        Called before each request and after each block. A closed session raises the
        `MassiveError` of `FlatFileBucket.check_open`.

        Args:
            cancel: The call's cancel event.

        Raises:
            _CancelledError: If the call was cancelled.
        """
        self._bucket.check_open()
        if cancel.is_set():
            raise _CancelledError

    def _read_key(
        self, client: S3Client, transform: _Transform, key: _Key, *, cancel: threading.Event
    ) -> list[Table] | None:
        """Stream one file into static Deephaven tables of its surviving rows.

        Runs on a reader thread, inside the session's execution context for the Deephaven
        calls. Surviving rows are gathered per file and converted once they reach
        `_TABLE_BLOCKS` blocks of data and at the end of the file, so a file usually becomes
        one table. The response body is closed however the read ends.

        Args:
            client: The bucket's S3 client.
            transform: The request's block transform.
            key: The file to read.
            cancel: The call's cancel event, checked before the request and after each block.

        Returns:
            The file's tables, or None if the file does not exist.

        Raises:
            MassiveError: If the request or the read fails, or the file is not the expected
                gzip CSV.
        """
        config = self._core.config
        location = f"s3://{config.s3_bucket}/{key.key} at {config.s3_endpoint_url}"
        self._check(cancel)
        try:
            body = client.get_object(Bucket=config.s3_bucket, Key=key.key)["Body"]
        except ClientError as error:
            self._refused(error, key, location)
            return None
        except BotoCoreError as error:
            raise MassiveError(f"cannot read {location}: {error}") from error
        limit = _TABLE_BLOCKS * transform.read_options.block_size
        tables: list[Table] = []
        pending: list[pa.RecordBatch] = []
        size = 0
        try:
            self._check(cancel)
            # Python's gzip decompresses on the reader's own read calls; Arrow asks for one
            # block per call and reads about 32 blocks ahead of this loop.
            stream = pa.PythonFile(gzip.GzipFile(fileobj=body, mode="rb"), mode="r")
            reader = pacsv.open_csv(
                stream,
                read_options=transform.read_options,
                convert_options=transform.convert_options,
            )
            with entered(self._core.exec_ctx):
                for block in reader:
                    rows = transform.apply(block, key)
                    if rows.num_rows:
                        pending.append(rows)
                        size += rows.nbytes
                    if size >= limit:
                        tables.append(_convert(pending, transform))
                        pending, size = [], 0
                    self._check(cancel)
                if pending:
                    tables.append(_convert(pending, transform))
        except (pa.ArrowException, OSError, EOFError, zlib.error, BotoCoreError) as error:
            raise MassiveError(f"cannot read {location}: {error}") from error
        finally:
            body.close()
        return tables

    def _refused(self, error: ClientError, key: _Key, location: str) -> None:
        """Map an S3 error response: a missing file is None, anything else raises.

        Args:
            error: The SDK error, carrying the HTTP status and S3's code and message.
            key: The file that was requested.
            location: The file's location, for messages.

        Raises:
            EntitlementError: On HTTP 403. The server's code and message are quoted. When S3
                rejects the credentials themselves (`InvalidAccessKeyId`,
                `SignatureDoesNotMatch`), the message names `s3_access_key_id` and
                `s3_secret_access_key`; otherwise it says the plan lacks the files, and for a
                futures file names the venue and the `venue=` parameter.
            MassiveError: On any status other than 403 and 404.
        """
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == HTTPStatus.NOT_FOUND:
            return
        details = error.response.get("Error", {})
        code = details.get("Code", "")
        server = f"{code}: {details.get('Message', '')}"
        if status == HTTPStatus.FORBIDDEN:
            if code in _CREDENTIAL_ERRORS:
                raise EntitlementError(
                    f"S3 rejected the flat-file credentials: HTTP 403 for {location} ({server}); "
                    "check MassiveConfig s3_access_key_id and s3_secret_access_key"
                ) from error
            if key.venue is None:
                raise EntitlementError(
                    f"the plan does not include these flat files: HTTP 403 for {location} "
                    f"({server})"
                ) from error
            raise EntitlementError(
                f"the plan does not include {key.venue.name} futures flat files: HTTP 403 for "
                f"{location} ({server}); pass venue=dm.FuturesVenue.<VENUE> for a venue the "
                "plan includes"
            ) from error
        raise MassiveError(f"cannot read {location}: HTTP {status} ({server})") from error

    @staticmethod
    def _skip_missing(spec: DatasetSpec, missing: Sequence[_Key], *, last: date) -> None:
        """Apply the missing-file rules to the files that did not exist.

        A weekend file is skipped silently (except for crypto, which trades every day); a
        weekday file is skipped with a WARNING (most likely a market holiday).

        Args:
            spec: The dataset.
            missing: The keys that S3 reported as missing, in date order.
            last: The last partition date of the request.

        Raises:
            MassiveError: If the file for `last` is missing on a trading day:
                `"... not yet published; flat files land ~11:00 ET next day"`.
        """
        for key in missing:
            trading = spec.asset_class is AssetClass.CRYPTO or key.day.weekday() < _SATURDAY
            if not trading:
                continue
            if key.day == last:
                raise MassiveError(f"{spec.name}: {key.key} is {NOT_PUBLISHED}")
            logger.warning(
                "%s: %s does not exist; skipping %s (a market holiday?)",
                spec.name,
                key.key,
                key.day.isoformat(),
            )


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
