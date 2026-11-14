"""Flat-file history: streaming Massive's S3 `.csv.gz` files into static Deephaven tables.

Massive publishes one gzip CSV file per dataset and day in the `flatfiles` bucket at
`https://files.massive.com`, under keys `<prefix>/<YYYY>/<MM>/<YYYY-MM-DD>.csv.gz` (for
example `us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz`). This module is the flat-file
transport. `HistoryQuery.history()` decides that flat files serve a request
(`choose_source` in `_datasets/history_source.py`) and hands it the partition dates to read.
The S3 access itself (the bucket client, the key layout, the S3 errors, and the CSV
decoding) is the wire package's: `_massive.FlatFileClient` opens each file and
`_massive.FlatFile` decodes it, and this module builds the `_massive.ReadPlan` that says what
to keep.

- `FlatFileHistory` reads the files of one `history()` call in parallel, at most
  `MassiveConfig.max_concurrency` at a time, each in blocks of `BLOCK_SIZE` bytes. Each block
  is filtered to the requested symbols with `pyarrow.compute.is_in` (symbol pushdown), cleared
  of the rows whose key cannot be read, and filtered to the window's `Timestamp` range (for
  bars, the starts of the bars whose whole window fits the range); only the surviving rows are
  renamed to the dataset's columns, have their condition and indicator lists parsed, and are
  converted to the column types here. 1-day bars, which `day_aggs_v1` stamps at the day's
  start, the 16:00 New York session close (stocks), midnight in Chicago (indices), or
  23:59:59.999 UTC (forex and crypto), are stamped at 00:00 of their day in the class day zone
  (`row_times.stamp_rule`); a stamp of another form is kept as sent and logs one WARNING per
  form of stamp per session, so every bar of a file is served. Every
  block's rows then get the library's time columns (`row_times.TimeRule`), with
  `TimestampInsert` the instant the block was decoded. The
  headers, their types, and their timestamp units are the prefix's generated fields
  (`source(Transport.FLAT_FILES, prefix)`), renamed for the dataset (`source_columns.rename`), and
  the columns are the dataset's merged schema (`Catalog.dataset_schema`), so a column the files do
  not carry is null and a column a kind widens (an integer size in a `double` column) is widened
  (`source_columns.widen`; a whole number the widening rounds is reported as drift). A file's
  surviving rows become one static Deephaven table per `_TABLE_BLOCKS` blocks of data (usually
  one table per file), and the tables are merged at the end.
  No block is ever converted to Python objects. A reader decompresses its file on its own thread
  and parses it in chunks of `CHUNK_BLOCKS` blocks on `PARSE_THREADS` threads of its own, so it
  holds about `PARSE_THREADS + 1` chunks of decompressed text and their rows, plus at most
  `_TABLE_BLOCKS` blocks of surviving rows, whatever the file's length.

Rows come in partition-date order, then (for futures) venue order (CME, CBOT, NYMEX, COMEX),
then file order: this module never sorts them by time, so a futures read's rows are not in time
order across venues. A `history()` whose missing days REST serves sorts the joined rows by time
(`_ingest/history_reads.py`), so that read comes back in time order. Only an empty CSV field is
null: text such as `NA` or `NULL` is data (both are valid tickers).

A missing file is judged by what exists, never by the clock, since Massive's publication time
varies. On a day without trading (a weekend for every class but crypto, which trades every
day) it is skipped silently. A missing trading day's file depends on whether another source
can serve the call (`infer_holidays`, which the query sets). When REST can, the read does not
guess: it returns every such day as unread (`FlatFileRead.unread`), keeping no row of it, and
the query reads each from REST, where no rows means a day without trading. When nothing else
can serve the call, Massive's order decides: it writes a day's file after the day ends, in date
order, so a missing trading day's file with a later day's file after it (in the range, or
looked for in the `ROLL_LIMIT`, 7 days, after the range's end, and before the current day) is a
day without trading, a holiday, and is skipped with a WARNING. With no later file, a missing day
within `ROLL_LIMIT` of the current day may simply not be published yet, and an older one has no
file at all; the query raises, saying which. A futures exchange holiday folds
into the next session, so in that case a futures range named by instants reads the later
session file found too: its early-opened hours may lie inside the range. A range named by
session dates never rolls forward: a date names its own session's file exactly.

A missing file is one S3 answers with HTTP 404 and the S3 error code `NoSuchKey`. Any other
404 (another S3 code, or a page that is not S3's at all, such as the HTML "404 Not Found" page
Massive's front end serves for a path it does not route) raises `MassiveError` naming the
object and the settings to check (`MassiveConfig.s3_bucket` and `s3_endpoint_url`). A range
with no file at all is no error: it returns an empty table, with a WARNING per missing
weekday, whatever the reason.

A connection that drops part-way through a file does not fail the call: a file's rows stay
with its reader until the file ends, so the reader discards them and reads the file again
from its start, at most three more times on the shared retry schedule
(`FlatFileClient.wait_to_reread`), and raises `MassiveError` naming the file if the last
attempt fails too.

Columns are matched by header name, never by position, so a file whose columns are reordered
reads identically. Every `Instant` column goes through the timestamp routine of
`_datasets/timestamps.py`: Massive writes `0` where a timestamp has no value (`trf_timestamp` of
a trade without a TRF), so in every `Instant` column except `Timestamp` a `0` becomes null; each
value is then scaled to nanoseconds by its field's unit, with an overflow check, and must lie in
1990 to 2100, so a value in another unit than documented is never served as a wrong instant.

Schema drift is reported, not raised: a header the dataset does not document is dropped, a
documented header the file lacks becomes a column of nulls, and a value that does not parse as
its type, or an `Instant` other than the time column that fails the routine, is null; each
logs one WARNING per (dataset, transport, header) through the session's `DriftRegistry`. A row
whose key (its ticker, its time, or for futures its session) is empty, does not parse, or
fails the routine cannot be placed, so it is dropped and counted (`DriftRegistry` logs one
WARNING per dataset with the running count). A malformed line, one that is not UTF-8 text or
does not split into the header's fields (too few or too many, or a quote that does not close),
follows `MassiveConfig.malformed_rows`: under `LOG`, the default, it is skipped and each file
that had one logs one WARNING with the count and the first one's row number, never its text;
under `IGNORE` that line is at DEBUG; under `RAISE` the call fails with `RowDataError`. A file
that lacks a key header fails the call with `MassiveError`, and so does a file whose every row
is malformed or whose every row of the requested symbols was dropped, the sign of a change in
the file's format rather than of one odd row.

Futures files are published per venue (`us_futures_cme/trades_v1`, ...): the dataset's prefix holds
the placeholder `{venue}`, `venues` (all four by default) picks the venues read, and HTTP 403 on any
venue raises `EntitlementError` naming the venue and the `venues=` parameter; a 403 that rejects the
S3 credentials themselves raises `AuthenticationError` for every class. Each futures row keeps its
own session (`session_end_date`) in `Date`, whichever file it came from, since Massive's
documentation does not state that a file holds only the session of its date; a call named by
dates keeps the rows of the requested sessions, and a file that holds another session's rows logs
one WARNING per dataset per session. A row is never labeled with a session it does not have. Every
error of the wire package becomes its public counterpart, with its status and request id: a rejected
credential or a refusal gains the remedy (the settings to check, the plan, the `venues=` parameter),
and any other wire error goes through `_errors.from_wire`. An Arrow error while converting or
gathering a file's rows (out of memory, say) is a failed read of that file.

A failed read, a `KeyboardInterrupt`, or `m.close()` stops every other read of the call at its
next block, and reads not yet started are cancelled. After a close, no request starts once a
reader has seen it; a request already in flight is discarded, and the call raises
`SessionClosedError("MassiveSession #1 is closed; ...")`.

The module is one subject although it runs past 1,000 lines: it is one transport, from the
read plan of a call to its table, and every step shares the plan's columns and its failure
rules.
"""

from __future__ import annotations

import contextlib
import functools
import logging
import threading
import time
import traceback
from concurrent.futures import FIRST_EXCEPTION, CancelledError, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import timedelta
from http import HTTPStatus
from typing import TYPE_CHECKING, Final

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.bar_periods import intraday
from deephaven_massive._datasets.catalog import timespan_key
from deephaven_massive._datasets.column_descriptions import OPTION_ROOT, RAW_SYMBOL
from deephaven_massive._datasets.dates import DaySpan, TimeRange, day_start, is_trading_day
from deephaven_massive._datasets.epoch import epoch_ns
from deephaven_massive._datasets.history_source import current_partition
from deephaven_massive._datasets.row_times import (
    DATE,
    BarBounds,
    TimeRule,
    time_column,
    time_rule,
    within,
)
from deephaven_massive._datasets.source_columns import RoundedFields, rename, required_fields
from deephaven_massive._datasets.symbols import option_roots
from deephaven_massive._datasets.timestamps import (
    first_failure,
    masked_instants,
    mismatch_message,
)
from deephaven_massive._datasets.wire_symbols import (
    WireSymbols,
    unreadable_symbol_message,
    wire_spellings,
)
from deephaven_massive._enums import AssetClass, FuturesVenue, OnError
from deephaven_massive._errors import (
    NEW_SESSION,
    READ_STOPPED,
    AuthenticationError,
    EntitlementError,
    ErrorSource,
    MassiveError,
    RowDataError,
    SessionClosedError,
    from_wire,
    required,
)
from deephaven_massive._ingest.history_reads import read_history, read_history_by_symbol
from deephaven_massive._ingest.stop_tokens import as_cancel
from deephaven_massive._massive import (
    Cancel,
    DriftReport,
    DriftSample,
    ReadColumn,
    ReadPlan,
    Transport,
    WireAuthError,
    WireEntitlementError,
    WireError,
    WireType,
    object_key,
)
from deephaven_massive._tables.columns import dataset_table
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.transports import FlatFileRead

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence
    from concurrent.futures import Future
    from datetime import date

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._datasets.schema import ColumnType
    from deephaven_massive._datasets.source_columns import SourceColumn
    from deephaven_massive._datasets.timestamps import TimeUnit
    from deephaven_massive._log_limits import BoundedLog
    from deephaven_massive._massive import FlatFile, FlatFileClient
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._tables.transports import SymbolOutcome

__all__ = ["BLOCK_SIZE", "ROLL_LIMIT", "FlatFileHistory"]

logger = logging.getLogger(__name__)

BLOCK_SIZE: Final = 1024 * 1024
"""Bytes of decompressed CSV per block: the unit each block of rows is filtered, converted, and
handed on in."""

CHUNK_BLOCKS: Final = 16
"""Blocks of decompressed CSV in each chunk a reader cuts its file into to parse in parallel
(`ReadPlan.chunk_size`): 16 MiB, enough that a chunk's parse outweighs handing it to a thread."""

PARSE_THREADS: Final = 3
"""Threads that parse one file's chunks beside the reader thread that decompresses it
(`ReadPlan.parse_threads`). Decompressing takes about a fifth of the time parsing and filtering
do, so three keep one reader busy; a reader then holds about four chunks (64 MiB) of text, and
the rows parsed from them, in flight."""

_ONE_DAY: Final = timedelta(days=1)

_TABLE_BLOCKS: Final = 16
"""A file's surviving rows are converted to a Deephaven table each time they reach this many
blocks of Arrow data, and at the end of the file. Fewer, larger tables cost far less JVM heap
than one per block (each table carries a fixed overhead), while memory stays bounded."""


ROLL_LIMIT: Final = timedelta(days=7)
"""How far past the first missing trading day of a range's end a later day's file is sought.

The longest run of days without trading (or, for futures, without a session) is a holiday
weekend of four days, so a week always reaches the next day with a file once it is out."""


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


def _window_ns(window: HistoryWindow) -> tuple[int, int] | None:
    """Return the `Timestamp` range a read keeps, as epoch nanoseconds, or None for every time.

    Args:
        window: The call's window.

    Returns:
        The range of `HistoryWindow.times`; None where only sessions bound the rows (futures
        trades, quotes, and session bars named by date, kept by each row's own session).
    """
    times = window.times
    return None if times is None else (times.start_ns, times.end_ns)


def _tiles(spec: DatasetSpec, request: Request, window: HistoryWindow) -> bool:
    """Tell whether the files' rows inside a day REST reads are dropped, to tile with REST.

    A range of instants keeps a file's rows by the call's range only, and a futures session
    a holiday folded into opens early, so its file holds hours that REST's read of the
    holiday's session also returns. Dropping the files' rows that lie in the instants of a
    day REST reads (`_outside_days`) makes the two sources tile exactly, and leaves every
    other row where it is: a file may hold rows of a neighbouring session, which no other
    source returns. Bars of a day or longer are not trimmed: a session bar starts before its
    session's open, and REST serves those bars by their `Date` instead.

    Args:
        spec: The dataset.
        request: The normalized arguments; for bars, the timespan.
        window: The call's window.

    Returns:
        True for a range of instants, except for bars of a day or longer.
    """
    if not isinstance(window.span, TimeRange):
        return False
    timespan = request.timespan
    return not spec.is_bars or timespan is None or intraday(timespan)


def _outside_days(
    parts: Sequence[tuple[date, Table]],
    *,
    time_name: str,
    tile: AssetClass,
    days: Sequence[date],
    exec_ctx: ExecutionContext,
) -> list[tuple[date, Table]]:
    """Drop the files' rows that lie in the instants of the days REST reads instead.

    A day's instants are `[day_start(day), day_start(day + 1))` in the class's day rules (for
    futures, its session's hours): exactly what REST reads for it. Only those rows are
    dropped, so a file's rows of a neighbouring day or session that was read stay.

    Args:
        parts: The kept files' tables, each with its file's day.
        time_name: The time column the instants are compared on (`Timestamp`, or a bar's
            `TimestampStart`).
        tile: The asset class whose day rules apply.
        days: The days REST reads.
        exec_ctx: The session's execution context, entered for the filter.

    Returns:
        The parts, each table filtered (a static `where`), in the same order.
    """
    excluded = " && ".join(
        f"(epochNanos({time_name}) < {epoch_ns(day_start(tile, day))}L"
        f" || epochNanos({time_name}) >= {epoch_ns(day_start(tile, day + _ONE_DAY))}L)"
        for day in days
    )
    logger.debug("flat-file rows inside the days REST reads are dropped: %s", excluded)
    with entered(exec_ctx):
        return [(day, table.where(excluded)) for day, table in parts]


def _ends_at_an_instant(spec: DatasetSpec, window: HistoryWindow) -> bool:
    """Tell whether a futures range ends at an instant, so a holiday may hide its last rows.

    Args:
        spec: The dataset.
        window: The call's window.

    Returns:
        True for futures named by instants (two aware datetimes).
    """
    if spec.asset_class is not AssetClass.FUTURES:
        return False
    return isinstance(window.span, TimeRange)


def _keys(
    prefix: str, venues: tuple[FuturesVenue, ...] | None, dates: Sequence[date]
) -> tuple[_Key, ...]:
    """Return the S3 objects a request reads, by date and then by venue.

    Args:
        prefix: The dataset's prefix for the request; a futures prefix holds `{venue}`.
        venues: The futures venues to read, in `FuturesVenue` order (`Request.venues`), for
            a futures prefix; None for any other dataset, whose prefix holds no `{venue}`.
        dates: The partition dates.

    Returns:
        One `_Key` per date, or per date and venue (in the order of `venues`) for a futures
        prefix.
    """
    read: tuple[FuturesVenue | None, ...] = (None,) if venues is None else venues
    keys = []
    for day in dates:
        for each in read:
            name = None if each is None else each.value
            keys.append(_Key(object_key(prefix, day, name), day, each))
    return tuple(keys)


_READ_TYPES: Final[Mapping[WireType, pa.DataType]] = {
    WireType.LONG: pa.int64(),
    WireType.DOUBLE: pa.float64(),
    WireType.BOOL: pa.bool_(),
    WireType.STRING: pa.string(),
    WireType.JSON_TEXT: pa.string(),
    WireType.DATE: pa.date32(),
    WireType.EPOCH: pa.int64(),
    WireType.ISO_INSTANT: pa.timestamp("ns", tz="UTC"),
    WireType.LONG_ARRAY: pa.string(),
}
"""The Arrow type the CSV reader parses each wire type as: an epoch as its raw integer count
(converted by its unit afterwards), a list of codes as text (parsed into `list<int64>` after
filtering), and every other type as its column's own Arrow type. A flat file has no list of
text: its only lists are the code lists of a `code-list-text` field."""

_LONG_LIST: Final = pa.list_(pa.int64())


class _OtherSessions:
    """What one futures read's files showed of rows from sessions other than their own day's.

    A futures flat file is named by session date, but Massive does not document that it holds
    only that session's rows. When a file holds a row of an earlier session, a requested
    session's rows may also sit in the file after it, which a call by dates reads only up to
    its last session; when a file holds a row of a later session, they may sit in the file
    before it. The read cannot see those files, so it says which requested sessions may be
    incomplete (`uncertain`). Shared by the read's reader threads; one leaf lock guards it.
    """

    __slots__ = ("_earlier", "_later", "_lock")

    def __init__(self) -> None:
        """Start having seen no other session."""
        self._lock = threading.Lock()
        self._earlier = False
        self._later = False

    def saw(self, others: pa.Array, day: date) -> None:
        """Record a file's rows of other sessions.

        Args:
            others: The sessions, `date32`, of the file's rows whose session is not its day.
            day: The file's day.
        """
        own = pa.scalar(day, pa.date32())
        earlier = bool(pc.any(pc.less(others, own)).as_py())
        later = bool(pc.any(pc.greater(others, own)).as_py())
        with self._lock:
            self._earlier |= earlier
            self._later |= later

    def uncertain(self, sessions: DaySpan | None) -> tuple[date, ...]:
        """Return the requested sessions whose rows may sit in a file the read did not read.

        Args:
            sessions: The sessions a call by dates requested, or None for a call by instants,
                whose files cover its whole time range.

        Returns:
            The first session when some file held a later session's rows, and the last when
            some file held an earlier session's rows, oldest first, without repeats; empty
            when no file held another session's rows.
        """
        if sessions is None:
            return ()
        with self._lock:
            found = {sessions.first} if self._later else set()
            if self._earlier:
                found.add(sessions.last)
        return tuple(sorted(found))


@dataclass(frozen=True, eq=False, slots=True)
class _Transform:
    """How one request turns flat-file batches into blocks of the dataset's schema.

    Built once per `history()` call by `build` and shared, read-only, by the reader threads.
    The wire package's reader does the filtering, renaming, and list parsing the `plan` asks
    for; `apply` then converts each surviving batch to the dataset's schema.

    Attributes:
        dataset: The dataset name, for messages.
        plan: What every file of the request reads and keeps.
        columns: Per schema column, in order: its type, its name, and the source column that
            fills it, or None for a column the files do not carry (null).
        arrow_schema: The dataset's Arrow schema.
        date_columns: The dataset's calendar-date columns, stored one shared string per date
            (`columns.dataset_table`).
        session_header: For futures, the header that holds the session date; None otherwise.
        sessions: For futures named by date, the sessions whose rows are kept
            (`HistoryWindow.sessions`); None to keep every session.
        warnings: The session's once-per-session warnings, for a futures file that holds
            rows of sessions other than its own.
        prefix: The prefix the files come from, for messages.
        symbols: How the files' tickers become canonical (`WireSymbols`), and what a spelling
            the library cannot read does.
        times: How the rows' time columns are filled (`row_times.TimeRule`).
        time_name: The dataset's time column, `Timestamp` or a bar's `TimestampStart`.
        bars: For a dataset of bars, the window its bars must lie in (`HistoryWindow.bars`);
            None for ticks.
        seen: What the read's futures files showed of other sessions' rows, for the
            sessions the read may have left incomplete (`FlatFileRead.uncertain`); its own
            per call, shared by the call's reader threads.
    """

    dataset: str
    plan: ReadPlan
    columns: tuple[tuple[ColumnType, str, SourceColumn | None], ...]
    arrow_schema: pa.Schema
    date_columns: tuple[str, ...]
    session_header: str | None
    sessions: DaySpan | None
    warnings: BoundedLog
    prefix: str
    times: TimeRule
    time_name: str
    symbols: WireSymbols
    bars: BarBounds | None = None
    seen: _OtherSessions = field(default_factory=_OtherSessions)

    @classmethod
    def build(
        cls,
        catalog: Catalog,
        spec: DatasetSpec,
        *,
        prefix: str,
        request: Request,
        window: HistoryWindow,
        unreadable_symbols: OnError,
        warnings: BoundedLog,
    ) -> _Transform:
        """Prepare the transform of a request from the prefix's generated source.

        The headers to read are the generated fields of `source(FLAT_FILES, prefix)`, renamed
        for the dataset (`source_columns.rename`), and the schema is the dataset's merged schema.
        `DatasetSpec` guarantees, when it is created, that every prefix declares its `symbol`
        and `time` keys from its own columns, and its `session` key for futures.

        Args:
            catalog: The catalog, for the generated source and the dataset's schema.
            spec: The dataset.
            prefix: The dataset's prefix for the request; a futures prefix holds `{venue}`.
            request: The normalized arguments: the canonical symbols to keep, as a tuple
                (anything else, `dm.ALL` or None for a dataset without instruments, keeps every
                row), and the bars' timespan and multiplier. The files are filtered on every
                spelling `wire_symbols.canonical_from_wire` reads as one of the symbols
                (`wire_spellings`: a forex quotes file may write `X:EUR-USD`).
            window: The call's window: rows are kept inside `HistoryWindow.times` (for bars,
                the starts of the bars that fit wholly); futures named by date keep the rows of
                `HistoryWindow.sessions`, by each row's own session.
            unreadable_symbols: The session's setting for a ticker spelling the library cannot
                read; `LOG` and `IGNORE` also add the `RawSymbol` column to the schema.
            warnings: The session's once-per-session warnings, which the time rule logs to
                (`row_times.time_rule`): that of a daily summary, and its bars' stamp rule.

        Returns:
            The transform.
        """
        schema = catalog.dataset_schema(spec, raw_symbol=unreadable_symbols is not OnError.RAISE)
        # A flat-file dataset always has a class: `DatasetSpec` refuses `ClassFromPrefix.ANY`
        # with flat files.
        file_class = spec.single_class
        source = catalog.source(Transport.FLAT_FILES, prefix)
        renamed = rename(spec, source)
        by_column = {c.column: c for c in renamed}
        # Every column of a flat-file source is a field: no key of one comes from the request.
        header = {
            c.column: required(c.wire_key, f"{prefix} column {c.column}'s field") for c in renamed
        }
        keys = spec.keys_for(Transport.FLAT_FILES, prefix)
        # The session key is renamed to `Date`; the files' own header holds it.
        session_header = None if keys.date is None else header[DATE]
        time_name = time_column(spec.times)
        time_unit = required(by_column[time_name].unit, f"{prefix} column {time_name}'s unit")
        to_ns = functools.partial(_time_nanos, f"{spec.name}.{time_name}", time_unit)
        symbols = request.symbols
        plan = ReadPlan(
            columns=tuple(
                ReadColumn(
                    header[c.column],
                    c.column,
                    _READ_TYPES[c.wire_type],
                    _LONG_LIST if c.wire_type is WireType.LONG_ARRAY else None,
                )
                for c in renamed
            ),
            symbol_header=header["Symbol"],
            symbols=(
                pa.array(
                    [w for s in symbols for w in wire_spellings(file_class, s)],
                    pa.string(),
                )
                if isinstance(symbols, tuple)
                else None
            ),
            time_header=header[time_name],
            window_ns=_window_ns(window),
            to_ns=to_ns,
            block_size=BLOCK_SIZE,
            chunk_size=CHUNK_BLOCKS * BLOCK_SIZE,
            parse_threads=PARSE_THREADS,
            required=frozenset(f.wire_key for f in required_fields(spec, source)),
        )
        return cls(
            dataset=spec.name,
            plan=plan,
            columns=tuple((c.type, c.name, by_column.get(c.name)) for c in schema.columns),
            arrow_schema=schema.arrow_schema(),
            date_columns=schema.date_columns,
            session_header=session_header,
            sessions=window.sessions,
            warnings=warnings,
            prefix=prefix,
            times=time_rule(spec, request, asset_class=file_class, warnings=warnings),
            time_name=time_name,
            symbols=WireSymbols(
                spec.name, spec.asset_class, unreadable_symbols, spec.symbols_as_written
            ),
            bars=window.bars,
        )

    def apply(
        self, batch: pa.RecordBatch, *, received: int, key: _Key
    ) -> tuple[pa.RecordBatch, DriftReport]:
        """Turn one batch of surviving rows into a block of the dataset's schema.

        Pure Arrow: the timestamp routine on every epoch `Instant`, the merge's widening of a
        column whose source type is narrower than the dataset's, nulls for the schema
        columns the prefix does not carry, and the library's time columns
        (`row_times.TimeRule.fill`). The lists are already parsed, the rows already
        filtered, and a documented header the file lacks already a column of nulls, by the
        reader, which also dropped every row whose time the routine cannot place. A bar whose
        window does not lie in the window is dropped (`row_times.within`).

        A futures row keeps its own session, the file's `session_end_date` header, in `Date`,
        whatever file it came from, and is kept only when that session was requested
        (`sessions`). A file that holds rows of a session other than its own logs one WARNING
        per dataset per session, naming the file and the session found.

        Args:
            batch: One block's surviving rows, with the plan's columns.
            received: When the block was decoded, epoch nanoseconds: every row's
                `TimestampInsert`.
            key: The file the block came from.

        Returns:
            The block in the dataset's schema; and the drift of its timestamps and widening:
            the headers of the epoch columns other than the time column that held a value the
            routine cannot place (null), and those of the integer columns whose widening to a
            `double` rounded a value (`RoundedFields`), each with its first such value.

        A 1-day bar stamped at a time of day Massive is not known to use is served as sent and
        logs the WARNING of `row_times.StampRule.stamp`.

        Raises:
            MassiveError: If a row's ticker is a spelling the library cannot read and the
                session's `unreadable_symbols` is `RAISE`
                (`wire_symbols.unreadable_symbol_message`); under `LOG` and `IGNORE` the row
                keeps a null `Symbol` and the text in `RawSymbol`.
        """
        if self.session_header is not None:
            batch = self._own_sessions(batch, key)
        arrays: dict[str, pa.Array] = {}
        raw = pa.nulls(batch.num_rows, pa.string())
        odd: dict[str, DriftSample] = {}
        rounded = RoundedFields()
        for column in self.columns:
            column_type, name, _ = column
            if name == OPTION_ROOT:
                values = option_roots(arrays["Symbol"])
            elif name == RAW_SYMBOL:
                values = raw
            elif name == "Symbol":
                read = self.symbols.read(batch["Symbol"], {})
                if read.unreadable and not self.symbols.keeps:
                    raise MassiveError(
                        unreadable_symbol_message(self.dataset, read.unreadable),
                        source=ErrorSource.LIBRARY,  # a spelling the library cannot read
                    )
                values, raw = read.symbols, read.raw
            else:
                values = self._column(batch, column, odd=odd, rounded=rounded)
            # A field's column is widened already (`_column`); this casts the others.
            arrays[name] = rounded.widen(values, column_type.arrow_type, wire_key=None)
        rows = pa.RecordBatch.from_arrays(list(arrays.values()), schema=self.arrow_schema)
        stamps = self.times.stamps
        if stamps is not None:
            rows = stamps.stamp(self.dataset, rows, source=f"the flat files {self.prefix}")
        filled = self.times.fill(rows, received=received)
        kept = filled if self.bars is None else within(filled, self.bars)
        drift = DriftReport(unconvertible=frozenset(odd), samples=odd)
        return kept, drift.merge(rounded.drift())

    def _own_sessions(self, batch: pa.RecordBatch, key: _Key) -> pa.RecordBatch:
        """Keep a futures block's rows of the requested sessions; warn of a file's other sessions.

        Args:
            batch: The block's rows, whose `Date` holds each row's own session date.
            key: The file the block came from.

        Returns:
            The rows whose session was requested; every row when the call names instants,
            whose rows the reader already kept by time.
        """
        sessions = batch[DATE]
        own = pa.scalar(key.day, pa.date32())
        others = pc.filter(sessions, pc.not_equal(sessions, own))
        if len(others):
            self.seen.saw(others, key.day)
            self.warnings.log(
                f"{self.dataset}\0other sessions",
                "%s: the flat file %s holds rows of session %s, not only of its own %s; each "
                "row keeps its own session in Date and is kept when that session was requested, "
                "but rows of a requested session may sit in a neighbouring day's file, which a "
                "call by dates does not read, and be missing",
                self.dataset,
                key.key,
                others[0].as_py().isoformat(),
                key.day.isoformat(),
            )
        if self.sessions is None:
            return batch
        first = pa.scalar(self.sessions.first, pa.date32())
        last = pa.scalar(self.sessions.last, pa.date32())
        return batch.filter(
            pc.and_(pc.greater_equal(sessions, first), pc.less_equal(sessions, last))
        )

    def _column(
        self,
        batch: pa.RecordBatch,
        column: tuple[ColumnType, str, SourceColumn | None],
        *,
        odd: dict[str, DriftSample],
        rounded: RoundedFields,
    ) -> pa.Array:
        """Convert one column's values to its type, or make the column of nulls it lacks.

        Args:
            batch: The filtered batch.
            column: The column's type, its name, and the source column that fills it, or None.
            odd: Updated with the header of an epoch column whose value the timestamp routine
                cannot place, and its first such value.
            rounded: Notes the header of an integer column whose widening to a `double`
                rounded a value.

        Returns:
            An Arrow array of the column's type, one value per row of `batch`. In an `Instant`
            column other than the dataset's time column, an epoch value of `0` is null, and so
            is a value the routine cannot place.
        """
        column_type, name, source = column
        if source is None:
            return pa.nulls(batch.num_rows, column_type.arrow_type)
        values = batch[name]
        if source.unit is not None:
            sent = values
            values, failed = masked_instants(
                source.unit,
                sent,
                time_column=name == self.time_name,
                label=f"{self.dataset}.{name}",
            )
            first = first_failure(sent, failed)
            if first is not None:
                odd[required(source.wire_key, f"column {name}'s field")] = DriftSample(
                    None, str(first)
                )
        return rounded.widen(values, column_type.arrow_type, wire_key=source.wire_key)


def _time_nanos(label: str, unit: TimeUnit, values: pa.Array) -> tuple[pa.Array, str]:
    """Convert a flat file's time header to epoch nanoseconds, for the reader's key check.

    The reader's `ReadPlan.to_ns`: the timestamp routine (`timestamps.masked_instants`) on the
    dataset's time column, whose failing values the reader drops with their rows.

    Args:
        label: The column, as `<dataset>.<column>`, for the reason.
        unit: The header's unit.
        values: The header's epoch integers.

    Returns:
        The nanoseconds, null where a value fails the routine; and the first failure's
        message (`timestamps.mismatch_message`), or `""` when no value fails.
    """
    nanos, failed = masked_instants(unit, values, time_column=True, label=label)
    first = first_failure(values, failed)
    return nanos, "" if first is None else mismatch_message(label, unit, first)


class _CancelledError(Exception):
    """A read stopped: another read of its call failed or was interrupted, or the call stopped."""


class FlatFileHistory:
    """The flat-file transport of one `history()` call: reads and the table.

    `SessionClients.flat_files()` creates one per call, so it may hold the session's core
    strongly; the long-lived state, the S3 client, lives in the session's `FlatFileClient`,
    which `SessionClients` owns.
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
        window: HistoryWindow,
        *,
        dates: Sequence[date],
        call: int,
        infer_holidays: bool,
        stop: threading.Event | None = None,
    ) -> FlatFileRead:
        """Return the rows of `window` from the dataset's flat files, and the days they lack.

        Flat files have already been chosen for the request
        (`choose_source` in `_datasets/history_source.py`), so the dataset has a file for its
        timespan. Files are read by a pool of `min(max_concurrency, files)` threads, which end
        before this method returns. If any read fails, or this thread is interrupted, every
        other read stops at its next block, reads that have not started are cancelled, and the
        failure of the earliest file is raised. Closing the session stops the reads the same
        way, and so does setting a background read's `stop` token, after which the call raises
        `MassiveError("<dataset> history: the read was stopped ...")` once the pool has drained.
        Logs `source=S3 files=<n> skipped=<dates> unread=<dates>` at INFO.

        Args:
            spec: The dataset.
            request: The normalized arguments, with the raw `start` and `end` bounds, and
                static symbols (the query reads a reactive symbol table before this call).
            window: The call's window (`history_window` in `_datasets/history_source.py`).
            dates: The partition dates to read (`partition_dates` in `_datasets/history_source.py`).
            call: The call's id (`SessionCore.next_stream_id`), which names its reader threads
                (`"massive #1 call-9 files_0"`).
            infer_holidays: True when no other source can serve the call: a missing trading
                day's file with a later day's file is skipped as a day without trading, and
                only the first day with no later file is unread. False to report every missing
                trading day as unread, for the query to read from REST.
            stop: A background read's stop token (`SessionClients.read_stop`, a `Cancel`);
                None when only the session's close should stop the reads.

        Returns:
            The read (`FlatFileRead`): each kept file's static tables with its day, in date
            order (and, for futures, venue order within a date), each file's rows in file
            order; an empty table with every column; and the unread days, whose rows (and,
            under `infer_holidays`, every later day's) the parts leave out.

        Errors, all raised on the calling thread: `AuthenticationError` when S3 rejects the S3
        credentials; `EntitlementError` when S3 answers HTTP 403 for a file otherwise, naming
        the venue and the `venues=` parameter for futures; `MassiveError` when the S3 client
        cannot be created, the bucket is unreachable, answers
        a 404 that is not S3's `NoSuchKey` (a wrong bucket or endpoint: `"cannot read
        s3://<bucket>/<key> at <endpoint>: HTTP 404 (404: Not Found); check MassiveConfig
        s3_bucket and s3_endpoint_url"`), or answers another error, a file's connection drops
        part-way on its first read and on each of its three rereads, a file is not the
        expected gzip CSV (for example, it lacks the symbol or time header), a file's every
        row of the requested symbols was dropped because its key could not be read (`"...
        timestamp unit mismatch for <dataset>.<column>: ..."`, naming the file), or the session
        is closed during the call. Errors that come from S3 carry its `x-amz-request-id` as
        `request_id`. An extra or missing documented header, a value that does not parse, and
        a row dropped for its key are not errors: they log a WARNING once per header, and per
        dataset for dropped rows, each session.
        """
        prefix = spec.flat_file_prefixes[timespan_key(request.timespan)]
        transform = _Transform.build(
            self._core.catalog,
            spec,
            prefix=prefix,
            request=request,
            window=window,
            unreadable_symbols=self._core.config.unreadable_symbols,
            warnings=self._core.warnings,
        )
        tiled = not infer_holidays and _tiles(spec, request, window)
        keys = _keys(prefix, request.venues, dates)
        threads = f"{self._core.thread_prefix} call-{call} files"
        return self._read(
            spec,
            transform,
            keys,
            prefix=prefix,
            roll=_ends_at_an_instant(spec, window),
            infer_holidays=infer_holidays,
            tile=spec.single_class if tiled else None,
            threads=threads,
            stop=as_cancel(stop),
        )

    def read_history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        dates: Sequence[date],
        call: int,
        forced: bool,
        stop: threading.Event | None = None,
    ) -> tuple[Table, str | None, str | None]:
        """Return a `history()` call's rows from the flat files, and from REST what they lack.

        The flat-file read of a call `history()` sent to the flat files
        (`history_reads.read_history`): under `AUTO`, the days and files the flat files cannot
        serve are read from REST when REST can serve the request.

        Args:
            spec: The dataset.
            request: The request with static symbols and the parsed bounds.
            window: The call's window, which holds rows.
            dates: The partition dates of the files the window needs.
            call: The call's id.
            forced: True for `source=FLAT_FILES`, which never reads REST.
            stop: A background read's stop token; None for a blocking read.

        Returns:
            The static table; the description's source sentence when REST served any of it,
            which replaces the sentence of the source choice (None when only the flat files
            did); and the sentences the description adds on what the files may lack: the days
            taken for holidays and the sessions that may be incomplete (None for neither).

        Errors are those of `history_reads.read_history`.
        """
        return read_history(
            spec,
            request,
            window,
            core=self._core,
            files=self,
            dates=dates,
            call=call,
            stop=stop,
            forced=forced,
        )

    def history_by_symbol(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        symbols: Sequence[str],
        dates: Sequence[date],
        call: int,
        forced: bool,
        stop: threading.Event | None = None,
    ) -> dict[str, SymbolOutcome]:
        """Read `history()` from the flat files for each symbol, each with its own outcome.

        See `FlatFileTransport.history_by_symbol` and `history_reads.read_history_by_symbol`:
        the files are scanned once for every symbol and split by `Symbol`, and the days they
        lack under `AUTO` are read from REST symbol by symbol.

        Args:
            spec: The dataset, with a flat-file prefix for the request.
            request: The normalized arguments, with the parsed bounds; its symbols are ignored.
            window: The call's window.
            symbols: The canonical symbols to read.
            dates: The partition dates of the files the window needs.
            call: The call's id.
            forced: True for `source=FLAT_FILES`, which never reads REST.
            stop: A read's stop token; None for a read only the session's close stops.

        Returns:
            Each symbol, in the order given, with its outcome.

        Errors are those of `history_reads.read_history_by_symbol`.
        """
        return read_history_by_symbol(
            spec,
            request,
            window,
            core=self._core,
            files=self,
            symbols=symbols,
            dates=dates,
            call=call,
            stop=stop,
            forced=forced,
        )

    def _read(
        self,
        spec: DatasetSpec,
        transform: _Transform,
        keys: Sequence[_Key],
        *,
        prefix: str,
        roll: bool = False,
        infer_holidays: bool = True,
        tile: AssetClass | None = None,
        threads: str,
        stop: Cancel | None = None,
    ) -> FlatFileRead:
        """Read files on a pool of reader threads, and say which days they could not serve.

        Waits until every read has finished or one has failed. On a failure, or an interrupt
        of this thread, it sets the call's cancel event, which stops the other reads at their
        next block, cancels the reads not yet started, and waits for the pool to drain; then it
        raises the failure of the earliest file (an interrupt is re-raised as is). If the
        session was closed meanwhile, it raises `"MassiveSession #1 is closed; ..."`, whatever the
        reads returned: a request in flight at the close may have been answered with any
        outcome. A missing file of a day without trading (`dates.is_trading_day`) is skipped
        silently. A missing trading day's file is judged by `infer_holidays`. When it is set
        (no other source can serve the call), `_after_missing` decides: the day is skipped with
        a WARNING when a later day's file exists (a day without trading, such as a holiday),
        and the first one with no later file is unread, with no row kept from it on. When it
        is not set, the library does not guess: every missing trading day is unread, and no
        row of it is kept (for futures, of any venue), since the query reads it from REST.

        Args:
            spec: The dataset.
            transform: The request's block transform.
            keys: The files to read, in result order.
            prefix: The dataset's prefix, with its `{venue}` placeholder for futures: where to
                look for the files after a range's missing last files.
            roll: True when a futures range ends at an instant: the next existing session file
                after a venue's missing last files is read too (`_after_missing`), when
                `infer_holidays` is set.
            infer_holidays: True to take a missing trading day with a later file for a day
                without trading; False to report every missing trading day as unread.
            tile: The asset class whose day rules give the instants of an unread day, which
                REST reads: the kept files' rows inside them are dropped (`_outside_days`),
                so the two sources never return a row twice. None to drop none.
            threads: The name prefix of the reader threads, such as `"massive #1 call-9 files"`.
            stop: A background read's stop token, which the call's cancel event follows; once
                it is set, the call raises `MassiveError` naming the cancellation after the
                pool has drained. None for none.

        Returns:
            The read: each kept file's tables with its day, the empty table in the schema, the
            unread days, and the days skipped as days without trading.

        Raises:
            MassiveError: If `stop` was set: the read was cancelled.
        """
        cancel = Cancel(*(() if stop is None else (stop,)))
        workers = min(self._core.config.max_concurrency, len(keys))
        with ThreadPoolExecutor(workers, threads) as pool:
            futures = [pool.submit(self._read_key, transform, key, cancel=cancel) for key in keys]
            try:
                wait(futures, return_when=FIRST_EXCEPTION)
            finally:
                cancel.set()
                pool.shutdown(cancel_futures=True)
        cancel.close()  # a parent keeps each child until then
        # A request already past its check when the session closed may have been answered with
        # any outcome (a row, a missing file, a refusal): report the close instead.
        self._check_open()
        if stop is not None and stop.is_set():  # the reads stopped early: report no result
            raise MassiveError(f"{spec.name} history: {READ_STOPPED}", source=ErrorSource.USER)
        parts = [_outcome(future) for future in futures]
        missing = [key for key, part in zip(keys, parts, strict=True) if part is None]
        _warn_if_none_exist(spec, keys, missing)
        trading = sorted({key.day for key in missing if is_trading_day(spec.asset_class, key.day)})
        later: list[tuple[date, Table]] = []
        unread: tuple[date, ...]
        absent: tuple[date, ...] = ()
        if infer_holidays:
            unpublished, absent, later = self._after_missing(
                spec,
                transform,
                keys=keys,
                missing=set(missing),
                prefix=prefix,
                roll=roll,
                stop=stop,
            )
            unread = () if unpublished is None else (unpublished,)
            holidays = [
                day
                for day in trading
                if (unpublished is None or day < unpublished) and day not in absent
            ]
        else:
            unread, holidays = tuple(trading), []
        for key in missing:
            if key.day in holidays:
                logger.warning(
                    "%s: %s does not exist, though a later day's file does; skipping %s, a day "
                    "without trading (a holiday?)",
                    spec.name,
                    key.key,
                    key.day.isoformat(),
                )
        logger.info(
            "%s history: source=S3 files=%d skipped=%s unread=%s",
            spec.name,
            len(keys) - len(missing),
            ",".join(sorted({key.day.isoformat() for key in missing})) or "none",
            ",".join(day.isoformat() for day in unread) or "none",
        )
        first = unread[0] if infer_holidays and unread else None
        kept = [
            (key.day, table)
            for key, part in zip(keys, parts, strict=True)
            if part is not None and key.day not in unread and (first is None or key.day < first)
            for table in part
        ]
        if not unread:
            # A later file read for a holiday's hours.
            kept.extend(later)
        elif tile is not None:
            kept = _outside_days(
                kept,
                time_name=transform.time_name,
                tile=tile,
                days=unread,
                exec_ctx=self._core.exec_ctx,
            )
        with entered(self._core.exec_ctx):
            # Merging onto an empty table in the schema keeps every column when no row matched.
            empty = dataset_table(transform.arrow_schema.empty_table(), transform.date_columns)
        return FlatFileRead(
            tuple(kept),
            empty,
            unread,
            transform.seen.uncertain(transform.sessions),
            absent,
            tuple(holidays),
        )

    def _after_missing(
        self,
        spec: DatasetSpec,
        transform: _Transform,
        *,
        keys: Sequence[_Key],
        missing: set[_Key],
        prefix: str,
        roll: bool,
        stop: Cancel | None,
    ) -> tuple[date | None, tuple[date, ...], list[tuple[date, Table]]]:
        """Tell the files missing at a range's end apart: no trading, not out yet, or none.

        Massive writes a day's file after the day ends, in date order, so a missing trading
        day's file with a later day's file after it is a day without trading (a holiday), and
        one with no later file may simply not be published yet. For each venue (one, outside
        futures), the run of missing files that ends its keys is examined: a run of days
        without trading only (weekends) needs nothing; otherwise the days after the range are
        looked for, trading days only, up to `ROLL_LIMIT` (7 days) past the range's end and
        never as far as `current_partition`, whose file cannot exist yet. A file found makes
        the run days without trading. With none found, a missing trading day less than
        `ROLL_LIMIT` before `current_partition` may be unpublished, and the earliest such day
        is; an older one has no file at all (a file is published the day after its day, give
        or take a weekend), whatever the length of the run. No clock time is assumed:
        Massive's publication time varies.

        When `roll` is set (futures named by instants), the file found is read, since a
        holiday's trading folds into the next session, which opens early: the session of
        Tuesday 2025-01-21 opened on Sunday 2025-01-19 at 17:00 Chicago time, so a range that
        ends on the holiday, or on the weekend after it, finds rows in the later file; the
        transform keeps only the rows inside the range. Otherwise the file is only opened and
        closed. The looks run on the caller thread, one at a time. Each checks the session
        before its request and after every block, as the pool's reads do, so a close stops it
        with `"MassiveSession #1 is closed; ..."`. The pool's cancel event is already set when this
        runs, so these looks listen to the background read's stop token instead, if there is
        one.

        Args:
            spec: The dataset.
            transform: The request's block transform.
            keys: The request's files, in date and then venue order.
            missing: The files of `keys` that do not exist.
            prefix: The dataset's prefix, with its `{venue}` placeholder for futures.
            roll: True to read the later file found (futures named by instants).
            stop: A background read's stop token, or None when only the session's close
                should stop the reads.

        Returns:
            The first unpublished day over all venues, or None; the missing trading days that
            have no file at all, oldest first; and the tables of the later files read for
            `roll`, each with its file's day, in venue order.

        Raises:
            MassiveError: `"<dataset> history: the read was stopped ..."` once `stop` is set. A
                failed request or read raises the public error `_read_key` raises
                (`MassiveError`, `AuthenticationError`, or `EntitlementError`).
        """
        cancel = Cancel() if stop is None else stop
        current = current_partition(spec)
        unpublished: date | None = None
        absent: set[date] = set()
        tables: list[tuple[date, Table]] = []
        for venue in dict.fromkeys(key.venue for key in keys):
            own = [key for key in keys if key.venue == venue]
            run: list[_Key] = []
            for key in reversed(own):
                if key not in missing:
                    break
                run.append(key)
            trading = [key for key in run if is_trading_day(spec.asset_class, key.day)]
            if not trading:
                continue
            first = trading[-1]  # the run is newest first
            later = self._later_file(
                spec,
                transform,
                first,
                after=own[-1].day,
                current=current,
                prefix=prefix,
                roll=roll,
                cancel=cancel,
            )
            if later is not None:
                tables.extend(later)
                continue
            recent = [key.day for key in trading if key.day + ROLL_LIMIT >= current]
            absent.update(key.day for key in trading if key.day + ROLL_LIMIT < current)
            if recent:
                earliest = min(recent)
                unpublished = earliest if unpublished is None else min(unpublished, earliest)
        return unpublished, tuple(sorted(absent)), tables

    def _later_file(
        self,
        spec: DatasetSpec,
        transform: _Transform,
        first: _Key,
        *,
        after: date,
        current: date,
        prefix: str,
        roll: bool,
        cancel: Cancel,
    ) -> list[tuple[date, Table]] | None:
        """Look for the first file after a range's missing last files; see `_after_missing`.

        Args:
            spec: The dataset.
            transform: The request's block transform.
            first: The run's first missing trading day's file, for its venue and the log.
            after: The range's last partition date: the look starts the day after, and goes
                on `ROLL_LIMIT` past it.
            current: `current_partition`: the look stops before it.
            prefix: The dataset's prefix, with its `{venue}` placeholder for futures.
            roll: True to read the file found; False to only open and close it.
            cancel: The event that stops a look: the background read's stop token, or a
                private one when only the session's close should stop it.

        Returns:
            The file's tables, each with the file's day, when it is read (none when it is only
            opened), or None when no later file exists in the looked-for days.

        Raises:
            MassiveError: `"<dataset> history: the read was stopped ..."` once `cancel` is set;
                a failed request or read raises the public error `_read_key` raises.
        """
        name = None if first.venue is None else first.venue.value
        day, limit = after, after + ROLL_LIMIT
        while (day := day + timedelta(days=1)) < current and day <= limit:
            if not is_trading_day(spec.asset_class, day):
                continue
            key = _Key(object_key(prefix, day, name), day, first.venue)
            try:
                found = (
                    self._read_key(transform, key, cancel=cancel)
                    if roll
                    else self._opened(key, cancel=cancel)
                )
            except _CancelledError:
                logger.debug("%s history: stopped before reading %s", spec.name, key.key)
                raise MassiveError(
                    f"{spec.name} history: {READ_STOPPED}", source=ErrorSource.USER
                ) from None
            if found is not None:
                logger.info(
                    "%s: %s is missing and %s exists, so it was a day without trading%s",
                    spec.name,
                    first.key,
                    key.key,
                    "; read for the hours a holiday moved into it" if roll else "",
                )
                return [(day, table) for table in found]
        return None

    def _opened(self, key: _Key, *, cancel: Cancel) -> list[Table] | None:
        """Tell whether a file exists by opening it and closing it unread.

        Args:
            key: The file.
            cancel: The call's cancel event, checked before the request.

        Returns:
            No tables when the file exists; None when it does not.

        Raises:
            MassiveError: The public error of `_public` when the request fails, including
                `AuthenticationError` and `EntitlementError`; `_check_open`'s when the session
                is closed. A cancelled call raises `_CancelledError`.
        """
        self._check(cancel)
        try:
            file = self._client.open(key.key, cancel=cancel)
        except WireError as error:
            public = _public(error, key)
            raise public from error
        if file is None:
            self._check(cancel)  # a wait for a connection the call's cancel ended
            return None
        with file:
            return []

    def _check_open(self) -> None:
        """Reject further work once the session's flat-file client is closed.

        Raises:
            SessionClosedError: `"MassiveSession #1 is closed; ..."` once the session has closed.
        """
        if self._client.closed:
            raise SessionClosedError(f"{self._core.label} is closed; {NEW_SESSION}")

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

    def _read_key(self, transform: _Transform, key: _Key, *, cancel: Cancel) -> list[Table] | None:
        """Stream one file into static Deephaven tables of its surviving rows.

        Runs on a reader thread, inside the session's execution context for the Deephaven
        calls. Surviving rows are gathered per file and converted once they reach
        `_TABLE_BLOCKS` blocks of data and at the end of the file, so a file usually becomes
        one table; when a selective read gathers `_TABLE_BLOCKS` small batches first, they are
        merged into one, so the per-batch overhead never grows with the file. The file, and
        its batch generator, are closed however the read ends.

        No table leaves this method before the file ends, so a connection that drops part-way
        through (`FlatFile.dropped`) costs only this file's progress: its tables are discarded,
        the body is closed, and the file is requested again and read from its start, after the
        wait of `FlatFileClient.wait_to_reread` (at most three rereads, 0.5, 1, then 2 seconds
        apart plus jitter). The wait ends early when the call is cancelled or the session
        closes, and the next check then stops the read; a cancel or close that comes once the
        rereads are spent is reported in place of the dropped connection.

        Args:
            transform: The request's block transform.
            key: The file to read.
            cancel: The call's cancel event, checked before the request and after each block.

        Errors: `MassiveError` if converting or gathering the file's rows fails in Arrow (out of
        memory, say): `"cannot read <location>: <Arrow's message>"`. A failed request or read
        (for example, a file that is not the expected gzip CSV, or a file whose every row
        of the requested symbols was dropped for a key it cannot read) raises the public error of
        `_public`: `MassiveError`, `AuthenticationError`, or `EntitlementError`; a dropped
        connection raises it once the rereads are spent. A closed session raises the
        `MassiveError` of `_check_open`, and a cancelled call `_CancelledError`.

        Returns:
            The file's tables, or None if the file does not exist.
        """
        retry = 0
        while True:
            outcome = self._read_once(transform, key, cancel=cancel)
            if not isinstance(outcome, WireError):
                return outcome
            # The body is closed by now, so no connection idles through the wait.
            if not self._client.wait_to_reread(outcome, retry, cancel):
                # A stop or a close that came during the last read is what ended the call, so
                # it is reported rather than the dropped connection.
                if cancel.is_set() or self._client.closed:
                    logger.debug(
                        "%s: rereads spent as the read was stopped; reporting the stop, not: %s",
                        key.key,
                        outcome,
                    )
                self._check(cancel)
                public = _public(outcome, key)
                raise public from outcome
            retry += 1

    def _read_once(
        self, transform: _Transform, key: _Key, *, cancel: Cancel
    ) -> list[Table] | WireError | None:
        """Request one file and read it from its start once; `_read_key` reads it again.

        Args:
            transform: The request's block transform.
            key: The file to read.
            cancel: The call's cancel event, checked before the request and after each block;
                it also ends a wait for one of the session's connections.

        Returns:
            The file's tables; None if the file does not exist; or, when the connection dropped
            part-way through the body (`FlatFile.dropped`), the read's error, with the body
            closed and the frames its traceback holds cleared, so the tables gathered so far
            are freed before the wait and the next read.

        Raises:
            MassiveError: Every failure `_read_key` describes except a dropped connection,
                including its subclasses `AuthenticationError` and `EntitlementError`. A
                cancelled call raises `_CancelledError` instead, which is not a
                `MassiveError`.
        """
        self._check(cancel)
        file = None
        try:
            file = self._client.open(key.key, cancel=cancel)
            if file is None:
                self._check(cancel)  # a wait for a connection the call's cancel ended
                return None
            with file, contextlib.closing(file.batches(transform.plan)) as batches:
                self._check(cancel)
                try:
                    return self._gather(batches, transform, key=key, cancel=cancel, file=file)
                except pa.ArrowException as error:
                    raise MassiveError(
                        f"cannot read {file.location}: {error}", source=ErrorSource.LIBRARY
                    ) from error
        except WireError as error:
            if file is not None and file.dropped:
                # None of the tables gathered so far was handed on, so reading the file again
                # from its start repeats no row. The error's traceback still holds them, in
                # the frames it passed through; clearing those frames frees them now rather
                # than after the next read.
                return _released(error)
            public = _public(error, key)
            raise public from error

    def _gather(
        self,
        batches: Iterator[tuple[pa.RecordBatch, DriftReport]],
        transform: _Transform,
        *,
        key: _Key,
        cancel: threading.Event,
        file: FlatFile,
    ) -> list[Table]:
        """Convert a file's batches to the schema and gather them into static tables.

        Once the file is read, reports its drift (the blocks' drift and their timestamps')
        to the session's `DriftRegistry`, which logs a WARNING the first time the dataset's
        flat files show each extra, missing, or unconvertible header, and counts the rows
        dropped for a key. A read that a dropped connection ends reports nothing, so a file
        read again is counted once. A file with malformed lines (not UTF-8 text, or not the
        header's fields) follows `MassiveConfig.malformed_rows` (`_log_malformed`): under `LOG`, it
        logs one WARNING with their count and the first one's row number and fault
        (`FlatFile.malformed_reason`), and under `IGNORE` the same line at DEBUG. Then logs
        one DEBUG line with the rows after each step: read, of the requested symbols, kept by
        the window (and a readable key), and kept as whole bars of the requested sessions, so
        an empty table can be traced to the step that emptied it.

        Args:
            batches: The file's batches, one per block, each with its drift.
            transform: The request's block transform.
            key: The file the batches come from.
            cancel: The call's cancel event, checked after each block.
            file: The file, for its location (`s3://<bucket>/<key> at <endpoint>`), named in
                the drift WARNING, and its row counts.

        Returns:
            The file's tables, in file order.

        Raises:
            RowDataError: Under `malformed_rows=RAISE`, at the first block handed on after the
                file's first malformed line (`_malformed_message`); every chunk hands on one,
                so a run of such lines fails the read before the file ends.
        """
        tables: list[Table] = []
        pending: list[pa.RecordBatch] = []
        size = 0
        limit = _TABLE_BLOCKS * transform.plan.block_size
        found = DriftReport()
        served = 0
        setting = self._core.config.malformed_rows
        with entered(self._core.exec_ctx):
            for batch, drift in batches:
                if file.malformed_lines and setting is OnError.RAISE:
                    raise RowDataError(_malformed_message(transform.dataset, file))
                rows, odd = transform.apply(batch, received=time.time_ns(), key=key)
                served += rows.num_rows
                found = found.merge(drift).merge(odd)
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
        self._core.drift.report(transform.dataset, Transport.FLAT_FILES, found, where=file.location)
        if file.malformed_lines:
            _log_malformed(transform.dataset, file, setting)
        logger.debug(
            "%s: %s: %d rows read, %d of the requested symbols, %d in the window, %d kept as "
            "whole bars",
            transform.dataset,
            file.location,
            file.rows_read,
            file.rows_of_symbols,
            file.rows_kept,
            served,
        )
        return tables


def _log_malformed(dataset: str, file: FlatFile, setting: OnError) -> None:
    """Log the malformed lines of a file read to its end, under `malformed_rows` `LOG` or `IGNORE`.

    The line gives the lines' count and the first one's row number and fault
    (`FlatFile.malformed_reason`), never a line's text. Under `RAISE`, the read failed at the
    first block after such a line, so no file with one is read to its end.

    Args:
        dataset: The dataset read, such as `stocks.trades`.
        file: The file, with at least one malformed line.
        setting: The session's `malformed_rows`: WARNING under `LOG`, DEBUG under `IGNORE`.
    """
    logger.log(
        logging.WARNING if setting is OnError.LOG else logging.DEBUG,
        "%s: skipped %d malformed line(s) of %s, the first: %s; the file's other rows are kept",
        dataset,
        file.malformed_lines,
        file.location,
        file.malformed_reason,
    )


def _malformed_message(dataset: str, file: FlatFile) -> str:
    """Say which file holds a malformed line, under `malformed_rows=RAISE`, and what to do.

    A `RowDataError` carries it (`ErrorSource.LIBRARY`: data from Massive the library
    can't read), and a retry of the read would meet the same line.

    Args:
        dataset: The dataset read, such as `stocks.trades`.
        file: The file, with at least one malformed line.

    Returns:
        The message: the dataset, the file, the first such line's row number and fault (never
        its text), and the setting that skips such lines.
    """
    return (
        f"{dataset}: {file.location} holds a malformed line, the first: "
        f"{file.malformed_reason}. To skip such lines and keep the file's other rows, pass "
        "MassiveConfig(malformed_rows=dm.OnError.LOG), or write 'malformed_rows: log' in the "
        "settings file"
    )


def _warn_if_none_exist(spec: DatasetSpec, keys: Sequence[_Key], missing: Sequence[_Key]) -> None:
    """Warn when not one of several trading days' files exists: the address may be wrong.

    A wrong prefix, bucket, or venue reads as a run of missing files, which the read takes for
    days not published yet: the call then reads REST or raises "not published yet", which
    would hide the real cause. Two or more trading days' files, all missing, are logged at
    WARNING with the count and where they were looked for.

    Args:
        spec: The dataset.
        keys: Every file the call read, in date order.
        missing: The files S3 reported as missing.
    """
    trading = [key for key in keys if is_trading_day(spec.asset_class, key.day)]
    absent = set(missing)
    if len(trading) > 1 and all(key in absent for key in trading):
        logger.warning(
            "%s: 0 of %d trading days' files exist (%s..%s, first looked for at %s); "
            "the bucket, prefix, or venue may be wrong",
            spec.name,
            len(trading),
            trading[0].day.isoformat(),
            trading[-1].day.isoformat(),
            trading[0].key,
        )


def _released(error: WireError) -> WireError:
    """Clear the local variables of the finished frames a dropped read's traceback holds.

    The reader raises the error in the batch generator's frame, and it passes through
    `_gather`'s frame on its way to `_read_once`, so this one traceback holds both: the
    gathered tables and pending batches in `_gather`'s locals, and the Arrow reader and its
    current block in the generator's. Clearing those frames releases the values; the error
    keeps its message and its traceback's file and line entries, so a final failure reads the
    same. Its cause, the stream's `OSError`, was raised below the generator and holds none of
    the file's rows. A frame that is still running (`_read_once`'s own) is left alone.

    Args:
        error: The error of a failed read.

    Returns:
        The same error.
    """
    traceback.clear_frames(error.__traceback__)
    return error


def _public(error: WireError, key: _Key) -> MassiveError:
    """Return the public error for a wire error of one file, with the user's remedy.

    A rejected credential names the settings to check; a refusal names the plan and, for a
    futures file, the venue and the `venues=` parameter; a 404 (a missing bucket, since a
    missing file is no error) names the bucket and endpoint settings. Every other wire error
    becomes its public counterpart through `_errors.from_wire`. Each keeps the wire error's
    status and request id.

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
            "pass venues={dm.FuturesVenue.<VENUE>, ...} with only the venues the plan includes",
            status=error.status,
            request_id=error.request_id,
        )
    if error.status == HTTPStatus.NOT_FOUND:
        # S3 answers a missing file with `NoSuchKey`, which the client reports as no file; any
        # other 404 means the bucket or endpoint is not the one that holds the files.
        return MassiveError(
            f"{error.message}; check MassiveConfig s3_bucket and s3_endpoint_url",
            status=error.status,
            request_id=error.request_id,
        )
    return from_wire(error)


def _convert(batches: list[pa.RecordBatch], transform: _Transform) -> Table:
    """Turn a file's gathered surviving rows into one static Deephaven table.

    Runs on a reader thread inside the session's execution context; every calendar-date
    column stores one shared string per distinct date (`columns.dataset_table`).

    Args:
        batches: Blocks in the dataset's schema, each with at least one row.
        transform: The request's transform, for the schema.

    Returns:
        The static table.
    """
    return dataset_table(
        pa.Table.from_batches(batches, schema=transform.arrow_schema), transform.date_columns
    )


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
