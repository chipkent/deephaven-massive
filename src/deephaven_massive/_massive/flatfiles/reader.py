"""Decoding one flat file: a gzip CSV object streamed into Arrow batches, one per block.

`FlatFile` wraps the body of one S3 object, as a binary stream that `FlatFileClient.open`
provides, and decodes it with `pyarrow.csv.open_csv` in blocks of `ReadPlan.block_size`
bytes of decompressed text. Python's `gzip` decompresses on the reader's own read calls; Arrow
asks for one block per call and reads about 32 blocks ahead of the consumer on its own I/O
thread, so a reader holds about 32 blocks of text whatever the file's length.

Columns are matched by name, never by position. The reader first reads the header line itself
and parses it with the `csv` module, then hands Arrow those names and asks for the documented
headers (`ReadPlan.columns`) by name: a reordered file reads identically, a header the plan
does not document is dropped and reported as extra, a documented header the file lacks becomes
a null column of its type and is reported as missing, and a file that lacks one of the plan's
key headers (`ReadPlan.required`) fails before its first block. Every block's batch travels
with a `DriftReport` of those headers and of its own odd values; what to log and show is the
library's decision.

Arrow reads every header as text, so one value that does not parse never fails a block. Each
block then goes through pure Arrow operations, in this order:

1. the symbol filter (`pyarrow.compute.is_in` on the symbol header);
2. the key check on the surviving rows: a row whose key header (`ReadPlan.required`) is empty
   or does not parse as its type, or whose time header the plan's converter cannot place
   (`ReadPlan.to_ns`), is dropped and counted; if every surviving row is dropped, the block
   raises, since that is the sign of a change in the file's format rather than of one odd row;
3. the half-open `[start, end)` filter on the time header, in epoch nanoseconds;
4. on the rows left, the conversion of every other header to its type, and the parsing of list
   headers (`"12,37"` becomes `[12, 37]`): a value that does not parse is null, and its header
   is reported in `DriftReport.unconvertible` with the first such text; then the rename from
   CSV headers to the library's column names.

Instants stay in the file's own unit: the library converts them. Only an empty CSV field is
null; text such as `NA` or `NULL` is data (both are valid tickers). A block is converted to
Python objects only where a whole column fails to parse, to find the values that do not.

`ReadPlan` and `ReadColumn` describe what to read; the library builds them from a dataset, so
this module knows no dataset. It imports only the standard library, `pyarrow`, and `net`: the
client hands it a stream whose read errors are `OSError`s, so it needs no botocore. A failed
read of that stream (a connection dropped part-way through the body) sets `FlatFile.dropped`,
so a caller can tell a failed transfer, worth reading again, from a broken file. The library
module that uses it is `_ingest/flatfiles.py`.
"""

from __future__ import annotations

import csv
import functools
import gzip
import threading
import zlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, BinaryIO, Final, Protocol, Self, cast

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv

from deephaven_massive._massive.net.errors import WireError
from deephaven_massive._massive.spec.types import DriftReport, DriftSample

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from types import TracebackType

__all__ = ["FlatFile", "ReadColumn", "ReadPlan"]

_SAMPLE_LIMIT: Final = 200
"""How many characters of a value's text a drift sample or a dropped row's reason quotes."""


def _empty(plan: ReadPlan) -> pa.RecordBatch:
    """Return a batch with the plan's columns and no rows.

    Args:
        plan: The plan whose columns the batch has.

    Returns:
        The empty batch, each column of its `list_type` or `read_type`.
    """
    arrays = [pa.array([], column.list_type or column.read_type) for column in plan.columns]
    return pa.RecordBatch.from_arrays(arrays, names=[column.column for column in plan.columns])


class _Stream(Protocol):
    """The binary stream a flat file is read from: an object's body, or any file of bytes."""

    def read(self, size: int = -1, /) -> bytes:
        """Read up to `size` bytes; raise `OSError` if the read fails."""
        ...

    def close(self) -> None:
        """Release the stream; idempotent."""
        ...


class _Watched:
    """A stream that records, on its file's event, that one of its reads failed.

    The decoders above it (gzip, Arrow) turn a failed read into errors of their own, which a
    broken file raises too; the event keeps the difference. Arrow may read on its own I/O
    thread, so the record is an event rather than a plain attribute.
    """

    __slots__ = ("_body", "_failed")

    def __init__(self, body: _Stream, failed: threading.Event) -> None:
        """Watch a stream.

        Args:
            body: The stream.
            failed: Set when a read of `body` raises.
        """
        self._body = body
        self._failed = failed

    def read(self, size: int = -1, /) -> bytes:
        """Read up to `size` bytes from the stream.

        Args:
            size: The most bytes to return; negative for the rest of the stream.

        Returns:
            The bytes read; empty at the end of the stream.

        Raises:
            OSError: The stream's own failure, after the event is set.
        """
        try:
            return self._body.read(size)
        except OSError:
            self._failed.set()
            raise


@dataclass(frozen=True, slots=True)
class ReadColumn:
    """One CSV header to read, and the column it becomes.

    Attributes:
        header: The CSV header, such as `"sip_timestamp"`.
        column: The name of the column in the batches `FlatFile.batches` yields, such as
            `"Timestamp"`.
        read_type: The Arrow type the header's text is converted to: `int64` for an epoch
            timestamp (in the file's unit), `string` for a list, otherwise the column's type.
        list_type: For a list header, the Arrow list type its comma-separated text is parsed
            into (such as `list<int64>`); None otherwise.
    """

    header: str
    column: str
    read_type: pa.DataType
    list_type: pa.DataType | None = None


@dataclass(frozen=True, eq=False, slots=True)
class ReadPlan:
    """What to read from one flat file, and which rows to keep.

    The library builds one per request (`_ingest/flatfiles.py`), from the dataset's flat-file
    headers and the request; it is read-only and may be shared between threads.

    Attributes:
        columns: The headers to read, in the order of the batches' columns.
        symbol_header: The header that holds each row's symbol.
        symbols: The symbols to keep, as a `string` array, or None for every symbol.
        time_header: The header that holds each row's timestamp.
        window_ns: The half-open `(start, end)` range of epoch nanoseconds rows must fall in,
            or None to keep every time (futures named by date, whose rows the library keeps
            by their session).
        to_ns: Converts the time header's epoch integers to epoch nanoseconds by its declared
            unit. It returns the nanoseconds, null where a value cannot be placed (it
            overflows, or lies outside the range the unit allows), and why the first such
            value cannot, or `""` when every value can. Such a row is dropped.
        block_size: Bytes of decompressed CSV per block, and the longest header line read.
        required: The key headers every file must have, such as the symbol and time headers;
            a file that lacks one fails before its first block, and a row whose value of one
            is empty or does not parse is dropped.
    """

    columns: tuple[ReadColumn, ...]
    symbol_header: str
    symbols: pa.Array | None
    time_header: str
    window_ns: tuple[int, int] | None
    to_ns: Callable[[pa.Array], tuple[pa.Array, str]]
    block_size: int
    required: frozenset[str]


class FlatFile:
    """One flat file being read: the open body of an S3 object and how to decode it.

    `FlatFileClient.open` creates one per existing key, around the object's body. A single
    reader thread iterates `batches()`; `close()` releases the body and may be called from any
    thread, any number of times. Use it as a context manager so the body is closed however the
    read ends.

    Example:
        ::

            with client.open("us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz") as file:
                for batch, drift in file.batches(plan):
                    ...
    """

    __slots__ = ("_body", "_dropped", "_host", "_key", "_location")

    def __init__(self, body: _Stream, key: str, *, location: str, host: str | None) -> None:
        """Wrap an object's body; nothing is read yet.

        Args:
            body: The gzip bytes of the object, as a stream whose failed reads raise `OSError`.
            key: The object key, for messages.
            location: Where the object is (`s3://<bucket>/<key> at <endpoint>`), for messages.
            host: The endpoint's host, for the errors' `host`.
        """
        self._body = body
        self._key = key
        self._location = location
        self._host = host
        self._dropped = threading.Event()

    @property
    def dropped(self) -> bool:
        """True once a read of the body has failed: the connection dropped or stalled.

        A `WireError` from `batches()` with this set means the transfer failed, not the file:
        the same object, requested again and read from its start, may well succeed. False when
        the read failed because the file itself is broken (not gzip, a value that does not
        parse), which a second read would only repeat. Safe to read from any thread.
        """
        return self._dropped.is_set()

    @property
    def key(self) -> str:
        """The object key, such as `"us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz"`."""
        return self._key

    @property
    def location(self) -> str:
        """Where the object is, for messages: `"s3://<bucket>/<key> at <endpoint>"`."""
        return self._location

    def __enter__(self) -> Self:
        """Return the file itself.

        Returns:
            This `FlatFile`.
        """
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the body, whatever ended the block.

        Args:
            kind: The type of the exception that ended the block, if any.
            error: The exception, if any.
            traceback: Its traceback, if any.
        """
        self.close()

    def close(self) -> None:
        """Close the body, which returns or drops its connection.

        Idempotent and safe from any thread. The library closes a file on its reader thread,
        once the read has ended.
        """
        self._body.close()

    def batches(self, plan: ReadPlan) -> Generator[tuple[pa.RecordBatch, DriftReport], None, None]:
        """Decode the file block by block, keeping the rows the plan asks for.

        Reads the header line first and matches columns by name. Yields one batch per block,
        possibly empty, so the consumer can check for cancellation after every block; a file
        with a header and no rows yields one empty batch, so its drift is still reported.
        Each batch has one column per `plan.columns`, named by `ReadColumn.column`, typed by
        `read_type` (or `list_type`); a documented header the file lacks is a column of nulls.
        Blocks are read on the calling thread as it iterates; Arrow reads ahead on its own I/O
        thread.

        Args:
            plan: What to read and which rows to keep.

        Yields:
            The kept rows of each block, and the block's `DriftReport`: `extra` holds the
            headers the plan does not document (dropped) and `missing` the documented headers
            the file lacks (null), the same for every block of a file; `unconvertible` the
            headers that held, in some kept row, text that does not parse as their type
            (null), and `dropped_rows` the rows of the requested symbols dropped for a key
            (`ReadPlan.required`, `ReadPlan.to_ns`), with the first one's reason.

        Raises:
            WireError: If the body is not gzip, is truncated or corrupt, or the connection
                drops (then `dropped` is True); if the file is empty (no header line); if the
                header line is longer than `plan.block_size`, not UTF-8, or not valid CSV; if
                the header names a column twice or lacks a key header of `plan.required`
                (before the first block, naming the headers); or if every row of a block that
                holds the requested symbols is dropped for a key, naming the first one's
                reason. Every message names the file.
        """
        read_types = {column.header: column.read_type for column in plan.columns}
        try:
            # gzip only reads its file object; its stub also asks for `seek`, which a response
            # body has no use for.
            body = _Watched(self._body, self._dropped)
            text = gzip.GzipFile(fileobj=cast("BinaryIO", body), mode="rb")
            names = self._header(text, plan)
            headers = DriftReport(
                extra=frozenset(names).difference(read_types),
                missing=frozenset(read_types).difference(names),
            )
            if not text.peek(1):
                # A header and no rows. Arrow refuses an empty body, and the drift still has
                # to reach the library, so the file is one empty batch.
                yield _empty(plan), headers
                return
            reader = pacsv.open_csv(
                pa.PythonFile(text, mode="r"),
                read_options=pacsv.ReadOptions(
                    # The header line is already read: every line Arrow sees is a row.
                    column_names=names,
                    block_size=plan.block_size,
                    use_threads=False,
                ),
                convert_options=pacsv.ConvertOptions(
                    # Every header is read as text and converted afterwards (`_apply`), so a
                    # value that does not parse costs only itself, never its block.
                    column_types=dict.fromkeys(read_types, pa.string()),
                    include_columns=list(read_types),
                    include_missing_columns=True,
                    # Massive writes a missing value as an empty field and nothing else. Arrow's
                    # default null markers include `NA` and `NULL`, which are real tickers.
                    null_values=[""],
                    strings_can_be_null=True,
                ),
            )
            for block in reader:
                rows, drift = self._apply(block, plan)
                yield rows, headers.merge(drift)
        except (
            pa.ArrowException,
            OSError,
            EOFError,
            zlib.error,
            UnicodeDecodeError,
            csv.Error,
        ) as error:
            raise WireError(
                f"cannot read {self._location}: {error}",
                host=self._host,
                path=self._key,
                # A body that broke off is a transport failure; any other is the file's content.
                unreadable=not self._dropped.is_set(),
            ) from error

    def _header(self, text: gzip.GzipFile, plan: ReadPlan) -> list[str]:
        """Read and parse the file's header line, and require the plan's key headers.

        Args:
            text: The decompressed file, positioned at its start; left at the first row.
            plan: The plan, for its key headers and the longest line read.

        Returns:
            The header names, in file order.

        Raises the `WireError` of `_error` if the file is empty, the line is longer than
        `plan.block_size`, the header names a column twice, or the header lacks a key header
        of `plan.required`; the message names the headers.
        """
        line = text.readline(plan.block_size)
        if not line:
            # Massive publishes no empty partitions: an empty object is a broken upload.
            raise self._error("the file is empty: no header line")
        if len(line) == plan.block_size and not line.endswith(b"\n"):
            raise self._error(f"the header line is longer than {plan.block_size} bytes")
        # The csv module parses quoted names and drops the line end, `\r\n` included;
        # `utf-8-sig` drops a byte-order mark, as Arrow's own header parsing does.
        names = next(csv.reader([line.decode("utf-8-sig")]), [])
        repeated = sorted({name for name in names if names.count(name) > 1})
        if repeated:
            # Matching by name cannot tell which of two equal names holds the column.
            raise self._error(f"the header names {', '.join(repeated)} more than once")
        lacking = sorted(plan.required - set(names))
        if lacking:
            raise self._error(f"the header lacks the key column {', '.join(lacking)}")
        return names

    def _error(self, reason: str) -> WireError:
        """Return the error of a file that cannot be read, naming the file.

        Args:
            reason: What is wrong with the file.

        Returns:
            `WireError("cannot read <location>: <reason>")`, marked `unreadable`, for the
            caller to raise.
        """
        return WireError(
            f"cannot read {self._location}: {reason}",
            host=self._host,
            path=self._key,
            unreadable=True,
        )

    def _apply(self, block: pa.RecordBatch, plan: ReadPlan) -> tuple[pa.RecordBatch, DriftReport]:
        """Filter one block, drop the rows whose key cannot be read, and convert the rest.

        Args:
            block: One block as the CSV reader read it, every header as text.
            plan: What to keep.

        Returns:
            The kept rows, possibly none, in the plan's columns; and the block's own drift:
            the rows dropped for a key (`_keyed`), and the headers that held text that does
            not parse (`unconvertible`, with the first such text as the sample).

        Raises:
            WireError: If the block holds rows of the requested symbols and every one of them
                is dropped for a key (`_keyed`).
        """
        if plan.symbols is not None:
            block = block.filter(pc.is_in(block[plan.symbol_header], value_set=plan.symbols))
        block, drift, nanos = self._keyed(block, plan)
        if plan.window_ns is not None:
            start, end = plan.window_ns
            if nanos is None:  # the time header is no key of this plan
                times = pc.cast(block[plan.time_header], _read_type(plan, plan.time_header))
                nanos, _ = plan.to_ns(times)
            block = block.filter(pc.and_(pc.greater_equal(nanos, start), pc.less(nanos, end)))
        arrays = []
        odd: dict[str, DriftSample] = {}
        for column in plan.columns:
            sent = block[column.header]
            if column.list_type is None:
                convert = functools.partial(pc.cast, target_type=column.read_type)
            else:
                convert = functools.partial(_split_list, list_type=column.list_type)
            values, failed = _parsed(sent, convert)
            if failed is not None:
                first = pc.filter(sent, failed)[0].as_py()
                odd[column.header] = DriftSample(None, first[:_SAMPLE_LIMIT])
            arrays.append(values)
        rows = pa.RecordBatch.from_arrays(arrays, names=[c.column for c in plan.columns])
        return rows, drift.merge(DriftReport(unconvertible=frozenset(odd), samples=odd))

    def _keyed(
        self, block: pa.RecordBatch, plan: ReadPlan
    ) -> tuple[pa.RecordBatch, DriftReport, pa.Array | None]:
        """Drop the rows of a block whose key header is empty, does not parse, or cannot be placed.

        A key header (`ReadPlan.required`) is empty or does not parse as its type, or, when the
        time header is a key, its value is one the plan's converter cannot place
        (`ReadPlan.to_ns`). The time header is then converted here once, for the window too.

        Args:
            block: The block's rows of the requested symbols, every header as text.
            plan: The plan, for its key headers and time converter.

        Returns:
            The rows kept, still as text; the drift of the rows dropped (`dropped_rows`, and a
            reason naming the header, and the value, of the first header that dropped a row;
            the library names the file); and the kept rows' times in epoch nanoseconds, or None
            when the time header is no key.

        Raises:
            WireError: If the block had rows and every one of them is dropped, the sign of a
                change in the file's format rather than of one odd row; the message names the
                row count and the reason.
        """
        keep = pa.repeat(pa.scalar(value=True), block.num_rows)
        nanos: pa.Array | None = None
        reason = ""
        for header in sorted(plan.required):
            sent = block[header]
            read_type = _read_type(plan, header)
            values, failed = _parsed(sent, functools.partial(pc.cast, target_type=read_type))
            bad = pc.is_null(sent) if failed is None else pc.or_(pc.is_null(sent), failed)
            if not reason and pc.any(bad).as_py():
                reason = _unreadable_key(header, sent, bad=bad, read_type=read_type)
            if header == plan.time_header:
                nanos, problem = plan.to_ns(values)
                bad = pc.or_(bad, pc.and_(pc.is_valid(values), pc.is_null(nanos)))
                reason = reason or problem
            keep = pc.and_(keep, pc.invert(bad))
        if pc.all(keep, min_count=0).as_py():  # an empty block keeps every row it has
            return block, DriftReport(), nanos
        rows = block.num_rows
        block = block.filter(keep)
        if not block.num_rows:
            raise self._error(
                f"every one of {rows} rows of a block was dropped, so the file's format may "
                f"have changed; the first: {reason}"
            )
        dropped = DriftReport(dropped_rows=rows - block.num_rows, dropped_reason=reason)
        return block, dropped, None if nanos is None else nanos.filter(keep)


def _read_type(plan: ReadPlan, header: str) -> pa.DataType:
    """Return the type a header of the plan is read as.

    Args:
        plan: The plan.
        header: One of its headers.

    Returns:
        The header's `ReadColumn.read_type`.
    """
    return next(c.read_type for c in plan.columns if c.header == header)


def _unreadable_key(header: str, sent: pa.Array, *, bad: pa.Array, read_type: pa.DataType) -> str:
    """Return why the first row whose key header is empty or does not parse was dropped.

    Args:
        header: The key header.
        sent: Its text in each row.
        bad: Where its text is empty or does not parse; true somewhere.
        read_type: The type it is read as.

    Returns:
        `"a row has no <header>"` when the first such row's field is empty, or `"<header>
        holds '<text>', which is not <type>"`.
    """
    text = sent[pc.index(bad, True).as_py()].as_py()
    if text is None:
        return f"a row has no {header}"
    return f"{header} holds {text[:_SAMPLE_LIMIT]!r}, which is not {read_type}"


def _split_list(text: pa.Array, *, list_type: pa.DataType) -> pa.Array:
    """Parse a list header's comma-separated text (`"12,37"`) into its list type.

    Args:
        text: The header's text in each row.
        list_type: The Arrow list type, such as `list<int64>`.

    Returns:
        The lists; a null text stays null.
    """
    return pc.cast(pc.split_pattern(text, ","), list_type)


def _parsed(
    text: pa.Array, convert: Callable[[pa.Array], pa.Array]
) -> tuple[pa.Array, pa.Array | None]:
    """Convert a header's text to its type, the values that do not parse becoming null.

    The whole column is converted in one Arrow call. Only when that call refuses the column
    is each value tried on its own, to find the ones that do not parse; that is rare, so the
    usual block makes no Python call per value.

    Args:
        text: The header's text in each row; an empty field is null.
        convert: Converts a text array to the header's type, raising `ArrowInvalid` when one
            of its values does not parse.

    Returns:
        The converted values, null where the text was empty or does not parse; and a mask of
        the values that did not parse, or None when every value did.
    """
    try:
        return convert(text), None
    except pa.ArrowInvalid:
        pass
    failed = pa.array(
        [value is not None and not _parses(value, convert) for value in text.to_pylist()]
    )
    cleaned = pc.if_else(failed, pa.scalar(None, pa.string()), text)
    return convert(cleaned), failed


def _parses(value: str, convert: Callable[[pa.Array], pa.Array]) -> bool:
    """Tell whether one text value parses as its header's type.

    Args:
        value: The text.
        convert: The header's conversion (`_parsed`).

    Returns:
        True when `convert` accepts it.
    """
    try:
        convert(pa.array([value], pa.string()))
    except pa.ArrowInvalid:
        return False
    return True
