"""Flat-file history: streaming Massive's S3 `.csv.gz` files into static Deephaven tables.

Massive publishes one gzip CSV file per dataset and day in the `flatfiles` bucket at
`https://files.massive.com`, under keys `<prefix>/<YYYY>/<MM>/<YYYY-MM-DD>.csv.gz` (for
example `us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz`). This module owns everything
between those files and the table `history()` returns:

- `choose_source` decides where `history()` reads from. With `HistorySource.FLAT_FILES` it
  checks that flat files can serve the request; with `HistorySource.AUTO` it arbitrates between
  flat files and REST. Flat files can serve a request when S3 credentials are configured, the
  dataset has a file for the request (for bars only 1-minute and 1-day bars, and 1-session
  bars for futures, and only unadjusted where the dataset has an `adjusted` parameter), and
  every file the range needs is already published. REST can serve it when the dataset has a
  REST history endpoint and the symbols are explicit (or the request is daily bars with a
  grouped-daily endpoint). When both can, many symbols
  (`MassiveConfig.flat_file_symbol_threshold` or more, or `dm.ALL`) read flat files and fewer
  use REST.
- `partition_dates` turns a `history()` window into the dates of the files it needs, in the
  asset class's day zone; a futures date names a trading session.
- `FlatFileBucket` is the session's connection to the bucket: one boto3 S3 client, created on
  first use and closed with the session.
- `FlatFileHistory` is the flat-file transport the session hands to `history()`
  (`SessionCore.flat_file_transport()`). It reads the files of a request in parallel, at most
  `MassiveConfig.max_concurrency` at a time, each with `pyarrow.csv.open_csv` in blocks of
  `BLOCK_SIZE` bytes. Every block is filtered to the requested symbols with
  `pyarrow.compute.is_in` first (symbol pushdown), then to the half-open `[start, end)` range
  on `Timestamp`; only the surviving rows are renamed to the dataset's PascalCase columns,
  converted to the column types, and have their condition and indicator lists parsed. Each
  block becomes one static Deephaven table at once, so memory holds at most one block per
  reader however long the range, and the tables are merged at the end. No block is ever
  converted to Python objects.

A day's file counts as published from `MassiveConfig.flat_file_publish_cutoff` (New York time)
after the next day starts in the class's day zone (`_dates.publish_time`); Massive says files
land "by approximately 11:00 AM ET the following day". A missing file is judged by its date: on
a weekend (for every class but crypto, which trades every day) it is skipped silently; on a
weekday it is skipped with a WARNING, since it is most likely a market holiday; for the last
requested date it raises `MassiveError`, since that file is most likely not yet published.

Futures files are published per venue (`us_futures_cme/trades_v1`, ...): the dataset's prefix
holds the placeholder `{venue}`, `venue=None` reads all four venues, and HTTP 403 on any venue
raises `EntitlementError` naming the venue and the `venue=` parameter. Futures files are assumed
to be keyed by trading session (`session_end_date`), which Massive's documentation does not
state outright, so every futures block is checked against its key's date, and a mismatch raises
`MassiveError` rather than return rows of the wrong session.
"""

from __future__ import annotations

import gzip
import logging
import threading
import zlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
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
from deephaven_massive._dates import (
    CHICAGO,
    DAY_ZONES,
    NEW_YORK,
    FuturesSessions,
    TimeRange,
    flat_file_published,
)
from deephaven_massive._enums import AssetClass, FuturesVenue, HistorySource, Transport
from deephaven_massive._errors import EntitlementError, MassiveError
from deephaven_massive._liveness import entered
from deephaven_massive._query import rest_history_problem
from deephaven_massive._schema import ColumnType
from deephaven_massive._symbols import SymbolTable, symbol_canonicalizer
from deephaven_massive._timeunits import TimeField, TimeUnitTable

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import tzinfo

    from deephaven.table import Table
    from types_boto3_s3.client import S3Client

    from deephaven_massive._catalog import DatasetSpec
    from deephaven_massive._config import MassiveConfig
    from deephaven_massive._query import Request
    from deephaven_massive._session import SessionCore
    from deephaven_massive._symbols import NormalizedSymbols
    from deephaven_massive._types import _All

__all__ = [
    "BLOCK_SIZE",
    "FlatFileBucket",
    "FlatFileHistory",
    "choose_source",
    "partition_dates",
]

logger = logging.getLogger(__name__)

BLOCK_SIZE: Final = 16 * 1024 * 1024
"""Bytes of uncompressed CSV per block: the unit of memory one reader holds at a time."""

_NOT_PUBLISHED: Final = "not yet published; flat files land ~11:00 ET next day"

_PARTITION_ZONES: Final[Mapping[AssetClass | None, tzinfo]] = dict(
    [*DAY_ZONES.items(), (AssetClass.FUTURES, CHICAGO), (None, NEW_YORK)]
)
"""The zone whose calendar dates name a class's files and start its publication clock.

Futures files are named by trading session, and Massive standardizes futures data in Central
Time. A dataset without an asset class uses New York, as `_dates.bounds` does."""

_ONE_DAY: Final = timedelta(days=1)
_SATURDAY: Final = 5


def _now() -> datetime:
    """Return the current instant: the one clock of this module, which tests replace.

    Returns:
        An aware UTC `datetime`.
    """
    return datetime.now(UTC)


def partition_dates(spec: DatasetSpec, window: TimeRange | FuturesSessions) -> tuple[date, ...]:
    """Return the dates of the files a `history()` window needs, oldest first.

    For a `TimeRange` they are the dates of `start` and of `end - 1 ns` in the class's day zone
    and every date between: New York for stocks, options, and indices, UTC for forex and
    crypto. For date-bounded futures (`FuturesSessions`) they are the sessions from `first` to
    `last`. A futures range given with `datetime` bounds is read by Central Time date, plus the
    session after the last date, because a futures session opens on the evening before its
    `session_end_date`; its rows are then filtered on `Timestamp`.

    Args:
        spec: The dataset; its asset class picks the day zone.
        window: The normalized `history()` bounds (`_dates.bounds`).

    Returns:
        The partition dates; never empty.
    """
    if isinstance(window, FuturesSessions):
        first, last = window.first, window.last
    else:
        dates = window.partition_dates(_PARTITION_ZONES[spec.asset_class])
        first, last = dates[0], dates[-1]
        if spec.asset_class is AssetClass.FUTURES:
            last += _ONE_DAY
    return tuple(first + timedelta(days=n) for n in range((last - first).days + 1))


def _prefix(spec: DatasetSpec, request: Request) -> str | None:
    """Return the dataset's S3 key prefix for a request, or None when no file serves it.

    Args:
        spec: The dataset.
        request: The normalized arguments; for bars, the timespan selects the prefix, and only
            multiplier 1 has files.

    Returns:
        The prefix, such as `"us_stocks_sip/minute_aggs_v1"`; a futures prefix still holds the
        placeholder `{venue}`.
    """
    if request.multiplier != 1:
        return None
    return spec.flat_file_prefixes.get(request.timespan)


def _flat_file_problems(
    spec: DatasetSpec,
    request: Request,
    dates: Sequence[date],
    *,
    config: MassiveConfig,
    now: datetime,
) -> list[str]:
    """List every reason flat files cannot serve a request; empty when they can.

    Args:
        spec: The dataset.
        request: The normalized arguments.
        dates: The partition dates of the request (`partition_dates`).
        config: The session's configuration (credentials and publish cutoff).
        now: The current instant.

    Returns:
        One human-readable reason per violated constraint, in a fixed order: credentials, the
        dataset's files, adjustment, publication.
    """
    problems = []
    if config.s3_access_key_id is None:
        problems.append(
            "no S3 credentials are configured (MassiveConfig s3_access_key_id and "
            "s3_secret_access_key, or MASSIVE_S3_ACCESS_KEY_ID and MASSIVE_S3_SECRET_ACCESS_KEY)"
        )
    if _prefix(spec, request) is None:
        if spec.is_bars and spec.flat_file_prefixes:
            spans = ", ".join(f"1-{span}" for span in spec.flat_file_prefixes)
            problems.append(f"{spec.name} flat files hold only {spans} bars")
        else:
            problems.append(f"{spec.name} has no flat files")
    if dict(request.params).get("adjusted", False) is not False:
        problems.append("flat-file bars are unadjusted; pass adjusted=False")
    zone = _PARTITION_ZONES[spec.asset_class]
    cutoff = config.flat_file_publish_cutoff
    late = [d for d in dates if not flat_file_published(d, now=now, zone=zone, cutoff=cutoff)]
    if late:
        problems.append(
            f"the file for {late[0].isoformat()} is {_NOT_PUBLISHED} (a day's file counts as "
            f"published from {cutoff.isoformat('minutes')} New York time after the day ends)"
        )
    return problems


def choose_source(
    spec: DatasetSpec,
    request: Request,
    dates: Sequence[date],
    *,
    requested: HistorySource,
    symbol_count: int | None,
    config: MassiveConfig,
    now: datetime,
) -> HistorySource:
    """Decide whether a `history()` call reads flat files or REST.

    Flat files are possible when S3 credentials are configured, the dataset has a file for the
    request (for bars: multiplier 1 and a timespan the dataset has files for, which is `MINUTE`
    or `DAY`, or `MINUTE` or `SESSION` for futures), the request is unadjusted or the dataset
    has no `adjusted` parameter, and every partition date is published at `now`. REST is
    possible when the dataset has a REST history endpoint and the symbols are explicit, or the
    request is 1-day bars with a grouped-daily endpoint. Under `AUTO`, when both are possible,
    flat files win for `dm.ALL` (and for datasets without instruments) or for at least
    `config.flat_file_symbol_threshold` symbols, and REST wins for fewer. Pure: no I/O.

    Args:
        spec: The dataset.
        request: The normalized arguments (symbols, timespan, multiplier, `adjusted`).
        dates: The partition dates of the request (`partition_dates`).
        requested: `HistorySource.AUTO` or `HistorySource.FLAT_FILES`. An explicit `REST` is
            handled by the query and never reaches the flat-file transport.
        symbol_count: The number of explicit symbols, or None for `dm.ALL`.
        config: The session's configuration.
        now: The current instant, for the publication rule.

    Returns:
        `HistorySource.FLAT_FILES` or `HistorySource.REST`.

    Raises:
        ValueError: Under `FLAT_FILES`, if flat files cannot serve the request; under `AUTO`, if
            neither source can. The message lists every violated constraint, per source.
    """
    flat = _flat_file_problems(spec, request, dates, config=config, now=now)
    if requested is HistorySource.FLAT_FILES:
        if flat:
            raise ValueError(f"{spec.name} cannot be read from flat files: {'; '.join(flat)}")
        return HistorySource.FLAT_FILES
    rest = rest_history_problem(spec, request)
    if flat and rest is not None:
        raise ValueError(
            f"{spec.name} history has no possible source. "
            f"Flat files: {'; '.join(flat)}. REST: {rest}."
        )
    if flat:
        return HistorySource.REST
    if rest is not None:
        return HistorySource.FLAT_FILES
    if symbol_count is None or symbol_count >= config.flat_file_symbol_threshold:
        return HistorySource.FLAT_FILES
    return HistorySource.REST


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
        One `_Key` per date, or per date and venue for a futures prefix.
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
        symbols: tuple[str, ...] | _All | None,
        window: TimeRange | FuturesSessions,
    ) -> _Transform:
        """Prepare the transform of a request from the dataset's flat-file key map.

        Args:
            spec: The dataset; its `FLAT_FILES` key map names the headers to read.
            symbols: The canonical symbols to keep; `dm.ALL` (or None, for a dataset without
                instruments) keeps every row.
            window: The normalized `history()` bounds.

        Returns:
            The transform.

        Raises:
            ValueError: If a futures dataset's key map has no header for `SessionEndDate`, so
                the files' session keys could not be checked.
        """
        key_map = spec.key_maps[Transport.FLAT_FILES]
        header_of = {column: header for header, column in key_map.items()}
        types = {column.name: column.type for column in spec.schema.columns}
        session_header = None
        if spec.asset_class is AssetClass.FUTURES:
            session_header = header_of.get("SessionEndDate")
            if session_header is None:
                raise ValueError(f"{spec.name}: futures flat files need a session date header")
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
            session_header=session_header,
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
            MassiveError: If a futures block holds a session date other than its key's date. A
                timestamp outside the declared unit's range raises the `MassiveError` of
                `TimeUnitTable.to_epoch_ns_array`; a condition list that is not integers raises
                `pyarrow.ArrowInvalid`.
        """
        if self.session_header is not None:
            sessions = block[self.session_header]
            wrong = pc.filter(sessions, pc.not_equal(sessions, pa.scalar(key.day, pa.date32())))
            if len(wrong):
                raise MassiveError(
                    f"{key.key} holds session_end_date {wrong[0]}, not {key.day.isoformat()}: "
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
    `close()`, on the session's reaper thread. It holds the configuration and nothing of the
    session, so it never keeps a session alive. Thread-safe: the client is created and closed
    under the bucket's own leaf lock, and boto3 clients are safe to share between threads.
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
        self._closed = False

    def client(self) -> S3Client:
        """Return the bucket's S3 client, creating it on the first call.

        Creating the client reads no file and makes no request, so it is done under the
        bucket's lock; concurrent first calls create exactly one client.

        Returns:
            The shared boto3 S3 client.

        Raises:
            MassiveError: If the bucket was closed, which happens when the session is closed
                while a `history()` call that had already started is about to read.
        """
        with self._lock:
            if self._closed:
                raise MassiveError(f"{self._label} is closed")
            if self._client is None:
                self._client = _s3_client(self._config)
            return self._client

    def close(self) -> None:
        """Close the client, if one was created, and refuse to create another.

        Idempotent. Runs as a session closer: on the thread that closes the session, or on the
        reaper thread for a session collected without `close()`. Reads in progress may still
        finish; closing only releases the client's pooled connections.
        """
        with self._lock:
            self._closed = True
            client, self._client = self._client, None
        if client is not None:
            client.close()


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
        source: HistorySource,
    ) -> Table:
        """Return the rows of `window` from flat files, or under `AUTO` from the better source.

        A reactive symbol table is read once, now, and its valid symbols are used (rows that
        are not valid symbols of the class are ignored with a WARNING, as for streams). The
        source is then chosen by `choose_source`; a REST choice is delegated to the session's
        REST transport with the symbols read, and logs `source=REST` at INFO. A flat-file read
        logs `source=S3 files=<n> skipped=<dates>` at INFO.

        Files are read by a pool of `min(max_concurrency, files)` threads, which end before
        this method returns. If any read fails, reads that have not started are cancelled,
        reads in progress finish, and the first failure in date order is raised.

        Args:
            spec: The dataset.
            request: The normalized arguments, with the raw `start` and `end` bounds.
            window: The bounds under the class's day rules (`_dates.bounds`).
            source: `HistorySource.FLAT_FILES` or `HistorySource.AUTO`.

        Returns:
            A static table in the dataset's schema: the files' rows in date order (and, for
            futures, venue order within a date), each file's rows in file order. A range
            without rows gives an empty table with every column.

        Errors, all raised on the calling thread: `ValueError` when a reactive symbol table
        holds no valid symbol or no source can serve the request (`choose_source`);
        `EntitlementError` when S3 answers HTTP 403 for a file, naming the venue and the
        `venue=` parameter for futures; `MassiveError` when the file for the last requested
        date is missing on a trading day (`"... not yet published; flat files land ~11:00 ET
        next day"`), when the bucket is unreachable or answers another error, when a file is
        not the expected gzip CSV (for example, a documented header is missing), or when a
        futures file holds another session; and the REST transport's own errors.
        """
        symbols = self._static_symbols(spec, request.symbols)
        request = replace(request, symbols=symbols)
        dates = partition_dates(spec, window)
        chosen = choose_source(
            spec,
            request,
            dates,
            requested=source,
            symbol_count=len(symbols) if isinstance(symbols, tuple) else None,
            config=self._core.config,
            now=_now(),
        )
        if chosen is HistorySource.REST:
            logger.info("%s history: source=REST", spec.name)
            return self._core.rest_transport().history(spec, request, window)
        # Flat files were chosen, so the dataset has a file for this timespan (multiplier 1).
        prefix = spec.flat_file_prefixes[request.timespan]
        transform = _Transform.build(spec, symbols, window)
        return self._read(spec, transform, _keys(prefix, request.venue, dates), last=dates[-1])

    def _static_symbols(
        self, spec: DatasetSpec, symbols: NormalizedSymbols | None
    ) -> tuple[str, ...] | _All | None:
        """Return the request's symbols, reading a reactive symbol table once.

        The table is read with `Table.iter_tuple`, which holds the update graph's shared lock
        while it reads, so the symbols are one consistent snapshot. Runs on the caller thread
        with no library lock held.

        Args:
            spec: The dataset, whose asset class canonicalizes the table's symbols.
            symbols: The normalized symbols of the request.

        Returns:
            The sorted canonical symbols read from a table, or `symbols` unchanged.

        Raises:
            ValueError: If the table holds no valid symbol.
        """
        if not isinstance(symbols, SymbolTable):
            return symbols
        canonical_symbol = symbol_canonicalizer(spec.asset_class)
        with entered(self._core.exec_ctx):
            raw = {canonical_symbol(row[0]) for row in symbols.table.iter_tuple(cols="Symbol")}
        found = tuple(sorted(symbol for symbol in raw if symbol is not None))
        if not found:
            raise ValueError(f"{spec.name} history: the symbol table holds no valid symbols")
        return found

    def _read(
        self, spec: DatasetSpec, transform: _Transform, keys: Sequence[_Key], *, last: date
    ) -> Table:
        """Read files on a pool of reader threads and merge their rows into one static table.

        The reads' errors, and the missing-file error of `_skip_missing`, propagate.

        Args:
            spec: The dataset.
            transform: The request's block transform.
            keys: The files to read, in result order.
            last: The last partition date of the request.

        Returns:
            The merged table.
        """
        client = self._bucket.client()
        workers = min(self._core.config.max_concurrency, len(keys))
        with ThreadPoolExecutor(workers, f"{self._core.label} flat files") as pool:
            futures = [pool.submit(self._read_key, client, transform, key) for key in keys]
            try:
                parts = [future.result() for future in futures]
            except BaseException:
                pool.shutdown(cancel_futures=True)
                raise
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

    def _read_key(self, client: S3Client, transform: _Transform, key: _Key) -> list[Table] | None:
        """Stream one file into static Deephaven tables, one per block with surviving rows.

        Runs on a reader thread, inside the session's execution context for the Deephaven
        calls. Holds one block of Arrow memory at a time.

        Args:
            client: The bucket's S3 client.
            transform: The request's block transform.
            key: The file to read.

        Returns:
            The tables of the file's blocks, or None if the file does not exist.

        Raises:
            MassiveError: If the request or the read fails, or the file is not the expected
                gzip CSV.
        """
        config = self._core.config
        location = f"s3://{config.s3_bucket}/{key.key} at {config.s3_endpoint_url}"
        try:
            body = client.get_object(Bucket=config.s3_bucket, Key=key.key)["Body"]
        except ClientError as error:
            self._refused(error, key, location)
            return None
        except BotoCoreError as error:
            raise MassiveError(f"cannot read {location}: {error}") from error
        tables = []
        try:
            # Python's gzip decompresses only as much as each read asks for; Arrow's own gzip
            # stream keeps megabytes of decompressed text alive, growing with the file.
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
                        tables.append(to_table(pa.Table.from_batches([rows])))
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
            EntitlementError: On HTTP 403. The server's code and message are quoted (an
                `InvalidAccessKeyId` here means the S3 credentials themselves are wrong); for a
                futures file the message names the venue and the `venue=` parameter.
            MassiveError: On any status other than 403 and 404.
        """
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == HTTPStatus.NOT_FOUND:
            return
        details = error.response.get("Error", {})
        server = f"{details.get('Code', '')}: {details.get('Message', '')}"
        if status == HTTPStatus.FORBIDDEN:
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
                raise MassiveError(f"{spec.name}: {key.key} is {_NOT_PUBLISHED}")
            logger.warning(
                "%s: %s does not exist; skipping %s (a market holiday?)",
                spec.name,
                key.key,
                key.day.isoformat(),
            )
