"""Decoding one flat file: a gzip CSV object streamed into Arrow batches, one per block.

`FlatFile` wraps the body of one S3 object, as a binary stream that `FlatFileClient.open`
provides. The reader's own thread decompresses it with Python's `gzip` and cuts the text after
a line end into chunks of `ReadPlan.chunk_size` bytes; a small pool of `ReadPlan.parse_threads`
threads parses each chunk with `pyarrow.csv.read_csv`, in blocks of `ReadPlan.block_size`
bytes, and filters and converts its blocks, while the reader decompresses the next. Blocks
come out in file order, and at most one chunk more than the pool has threads is read ahead
of the consumer, so a reader holds a few chunks of text whatever the file's length.

Columns are matched by name, never by position. The reader first reads the header line itself
and parses it with the `csv` module, then hands Arrow those names and asks for the documented
headers (`ReadPlan.columns`) by name: a reordered file reads identically, a header the plan
does not document is dropped and reported as extra, a documented header the file lacks becomes
a null column of its type and is reported as missing, and a file that lacks one of the plan's
key headers (`ReadPlan.required`) fails before its first block. Every block's batch travels
with a `DriftReport` of those headers and of its own odd values; what to log and show is the
library's decision.

A malformed line is skipped and counted (`FlatFile.malformed_lines`, with the first one's row
number and what is wrong with it in `FlatFile.malformed_reason`), never its text, and the rest
of its block is read: a line that is not UTF-8 text, found before Arrow reads its chunk, since
Arrow would refuse the whole chunk for it, and a line Arrow cannot split into the header's
fields (too few or too many, or a quote that does not close). Whether a skipped line fails the
read is the library's decision. A file whose every row is malformed raises after its last
block.

Arrow reads every header as text, so one value that does not parse never fails a block. Each
block then goes through pure Arrow operations, in this order:

1. the symbol filter (`pyarrow.compute.is_in` on the symbol header);
2. the key check on the surviving rows: a row whose key header (`ReadPlan.required`) is empty
   or does not parse as its type, or whose time header the plan's converter cannot place
   (`ReadPlan.to_ns`), cannot be placed; if every row of the requested symbols in the whole
   file has such a key, the file raises after its last block, since that is the sign of a
   change in the file's format rather than of one odd row (one block can hold a single row of
   an illiquid symbol, so the rule is never applied to a block alone);
3. the half-open `[start, end)` filter on the time header, in epoch nanoseconds, which keeps a
   row whose time cannot be read, since no window can place it;
4. on the rows left, the conversion of every header to its type, and the parsing of list
   headers (`"12,37"` becomes `[12, 37]`): a value that does not parse is null, and its header
   is reported in `DriftReport.unconvertible` with the first such text; then the rename from
   CSV headers to the library's column names.

A row left that the reader could not read whole is marked in the block's `RowMarks`:
`RowFault.UNREADABLE_KEYS` for a key from step 2, else `RowFault.UNCONVERTIBLE_VALUES` for a
value from step 4. A key-marked row's other values add no `unconvertible` drift. What happens
to a marked row is the library's decision.

Instants stay in the file's own unit: the library converts them. Only an empty CSV field is
null; text such as `NA` or `NULL` is data (both are valid tickers). A typed value's surrounding
spaces are trimmed, as Arrow's typed parsing does. No block is converted to Python objects:
the values of a column that do not parse are found by vectorized pattern and range tests.

`ReadPlan` and `ReadColumn` describe what to read; the library builds them from a dataset, so
this module knows no dataset. It imports only the standard library, `pyarrow`, and `net`: the
client hands it a stream whose read errors are `OSError`s, so it needs no botocore. A failed
read of that stream (a connection dropped part-way through the body) sets `FlatFile.dropped`,
so a caller can tell a failed transfer, worth reading again, from a broken file. The library
module that uses it is `_ingest/flatfiles.py`.
"""

from __future__ import annotations

import contextlib
import csv
import dataclasses
import functools
import gzip
import io
import threading
import zlib
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, Protocol, Self, TypeAlias, TypeVar

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv

from deephaven_massive._massive.net.errors import WireError, WireFailure
from deephaven_massive._massive.spec.types import (
    DriftReport,
    DriftSample,
    RowFault,
    RowMarker,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Generator, Iterator, Mapping
    from concurrent.futures import Executor, Future
    from types import TracebackType

    from deephaven_massive._massive.spec.types import RowMarks

__all__ = ["FlatFile", "ReadColumn", "ReadPlan"]

_SAMPLE_LIMIT: Final = 200
"""How many characters of a value's text a drift sample or a marked row's reason quotes."""

_READ_FAILURES: Final = (OSError, EOFError, zlib.error)
"""The failures of reading or decompressing a file, raised after the rows read before them."""

_T = TypeVar("_T")
_R = TypeVar("_R")


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

    def seek(self, offset: int, /) -> int:
        """Refuse to seek: a response body reads forward only.

        gzip seeks its file object only to rewind, which reading a file once never does.

        Args:
            offset: The position asked for.

        Raises:
            io.UnsupportedOperation: Always, naming the position.
        """
        raise io.UnsupportedOperation(f"a response body cannot seek (to {offset})")


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
            unit: null where a value cannot be placed (it overflows, or lies outside the range
            the unit allows). Such a row's key cannot be read.
        unplaced: Why `to_ns` cannot place one epoch integer, for its row's mark.
        block_size: Bytes of decompressed CSV per block, and the longest header line read.
        chunk_size: Bytes of decompressed CSV the reader cuts the file into, after a line
            end, to parse each piece on its own: a multiple of `block_size`.
        parse_threads: Threads that parse one file's chunks at once, beside the reader thread
            that decompresses it. At most one chunk more than this is read ahead of the
            consumer, so a reader holds about `(parse_threads + 1) * chunk_size` bytes of
            text, and the rows parsed from it, whatever the file's length.
        required: The key headers every file must have, such as the symbol and time headers;
            a file that lacks one fails before its first block, and a row whose value of one
            is empty or does not parse is marked `RowFault.UNREADABLE_KEYS`.
    """

    columns: tuple[ReadColumn, ...]
    symbol_header: str
    symbols: pa.Array | None
    time_header: str
    window_ns: tuple[int, int] | None
    to_ns: Callable[[pa.Array], pa.Array]
    unplaced: Callable[[int], str]
    block_size: int
    chunk_size: int
    parse_threads: int
    required: frozenset[str]


class FlatFile:
    """One flat file being read: the open body of an S3 object and how to decode it.

    `FlatFileClient.open` creates one per existing key, around the object's body. Not
    thread-safe: a single reader thread iterates `batches()` and calls `close()`, which
    releases the body and may be called any number of times. Use it as a context manager so
    the body is closed however the read ends.

    Example:
        ::

            with client.open("us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz") as file:
                for batch, drift, marks in file.batches(plan):
                    ...

    Attributes:
        rows_read: The rows `batches()` has read from the file so far, every symbol's.
        rows_of_symbols: Of those, the rows of the plan's symbols (all of them when the plan
            names none).
        rows_kept: Of those, the rows `batches()` yielded: inside the plan's window, or with a
            time it cannot read. The three tell a caller which step emptied a file.
        malformed_lines: The lines `batches()` skipped as malformed: a line that is not UTF-8
            text, or one Arrow could not split into the header's fields: too few or too many,
            or a quote that does not close (which takes the lines after it, up to the next
            quote or the end of its block, into the same bad row; those lines are counted
            too). Not part of `rows_read`.
        malformed_reason: Where the first skipped line is and what is wrong with it, such as
            `"row 1234 after the header has 5 fields where the header has 9"` or
            `"row 1234 after the header is not UTF-8 text"`; rows are
            counted from the line after the header, blank lines left out. Never quotes the
            line, which a caller could log. `""` when no line was skipped.

        Every count is written by the reader thread as it iterates, and is final once
        `batches()` ends.
    """

    __slots__ = (
        "_body",
        "_dropped",
        "_host",
        "_key",
        "_location",
        "malformed_lines",
        "malformed_reason",
        "rows_kept",
        "rows_of_symbols",
        "rows_read",
    )

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
        self.rows_read = 0
        self.rows_of_symbols = 0
        self.rows_kept = 0
        self.malformed_lines = 0
        self.malformed_reason = ""

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

        Idempotent. Call it on the reader thread, once the read has ended, as the library does.
        """
        self._body.close()

    def batches(
        self, plan: ReadPlan
    ) -> Generator[tuple[pa.RecordBatch, DriftReport, RowMarks], None, None]:
        """Decode the file block by block, keeping the rows the plan asks for.

        Reads the header line first and matches columns by name. A line that is not UTF-8
        text, or that Arrow cannot split into the header's fields, is skipped and counted
        (`malformed_lines`, `malformed_reason`). Yields one batch per block, possibly empty, so
        the consumer can check for cancellation after every block; a file with a header and no
        rows yields one empty batch, so its drift is still reported.
        Each batch has one column per `plan.columns`, named by `ReadColumn.column`, typed by
        `read_type` (or `list_type`); a documented header the file lacks is a column of nulls.
        The calling thread decompresses the file as it iterates, and a pool of
        `plan.parse_threads` threads parses it, at most `plan.parse_threads + 1` chunks ahead
        (`_blocks`); Arrow starts no thread of its own.

        Args:
            plan: What to read and which rows to keep.

        Yields:
            The kept rows of each block; the block's `DriftReport`: `extra` holds the headers
            the plan does not document (dropped) and `missing` the documented headers the file
            lacks (null), the same for every block of a file, and `unconvertible` the headers
            that held, in some kept row whose keys could be read, text that does not parse as
            their type (null); and the block's `RowMarks`, by index into its rows: a key
            (`ReadPlan.required`, `ReadPlan.to_ns`) empty, unparseable, or not placeable
            (`RowFault.UNREADABLE_KEYS`, the reason naming the header and the value), or
            another header's text that does not parse (`RowFault.UNCONVERTIBLE_VALUES`). Each
            mark's symbol is the row's symbol header as written.

        Raises:
            WireError: If the body is not gzip, is truncated or corrupt, or the connection
                drops (then `dropped` is True); if the file is empty (no header line); if the
                header line is longer than `plan.block_size`, not UTF-8, or not valid CSV; if
                the header names a column twice or lacks a key header of `plan.required`
                (before the first block, naming the headers); or, after its last block, if
                every row of the file was malformed, or the file held rows of the requested
                symbols and every one of them had a key that cannot be read, naming the first
                one's reason. Every message names the file.
        """
        read_types = {column.header: column.read_type for column in plan.columns}
        try:
            body = _Watched(self._body, self._dropped)
            text = gzip.GzipFile(fileobj=body, mode="rb")
            names = self._header(text, plan)
            headers = DriftReport(
                extra=frozenset(names).difference(read_types),
                missing=frozenset(read_types).difference(names),
            )
            if not text.peek(1):
                # A header and no rows. Arrow refuses an empty body, and the drift still has
                # to reach the library, so the file is one empty batch.
                yield _empty(plan), headers, _NO_MARKS
                return
            options = (
                pacsv.ReadOptions(
                    # The header line is already read: every line Arrow sees is a row.
                    column_names=names,
                    block_size=plan.block_size,
                    use_threads=False,
                ),
                pacsv.ConvertOptions(
                    # Every header is read as text and converted afterwards (`_apply`), so a
                    # value that does not parse costs only itself, never its block.
                    column_types=dict.fromkeys(read_types, pa.string()),
                    include_columns=list(read_types),
                    include_missing_columns=True,
                    # Massive writes a missing value as an empty field and nothing else. Arrow's
                    # default null markers include `NA` and `NULL`, which are real tickers.
                    null_values=[""],
                    strings_can_be_null=True,
                    # Each chunk is checked as UTF-8 whole before Arrow reads it (`_is_text`).
                    check_utf8=False,
                ),
            )
            # Rows of the requested symbols the file held, and how many had a key unread.
            held = unread = 0
            reason = ""
            for block in self._blocks(text, plan, options):
                self.rows_read += block.read
                self.rows_of_symbols += block.considered
                self.rows_kept += block.rows.num_rows
                held += block.considered
                unread += block.unread
                reason = reason or block.unread_reason
                yield block.rows, headers.merge(block.drift), block.marks
            if self.malformed_lines and not self.rows_read:
                raise self._error(
                    f"every one of its {self.malformed_lines} lines is malformed, so the file's "
                    f"format may have changed; the first: {self.malformed_reason}"
                )
            if held and unread == held:
                raise self._error(
                    f"every one of the {held} rows of the requested symbols was dropped, so the "
                    f"file's format may have changed; the first: {reason}"
                )
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
                kind=(WireFailure.NO_ANSWER if self._dropped.is_set() else WireFailure.UNREADABLE),
            ) from error

    def _blocks(
        self,
        text: gzip.GzipFile,
        plan: ReadPlan,
        options: tuple[pacsv.ReadOptions, pacsv.ConvertOptions],
    ) -> Generator[_Block, None, None]:
        """Parse and filter the file's rows chunk by chunk on a small pool, in file order.

        The calling thread decompresses the file and cuts the text after a line end into
        chunks of about `plan.chunk_size` bytes (`_chunk`); `plan.parse_threads` threads of
        the pool each parse a chunk (`_parse`), skipping its malformed lines, which are added
        to `malformed_lines` and `malformed_reason` as each chunk is handed on. At most
        `plan.parse_threads + 1` chunks are read ahead of the consumer, so a reader holds about
        that many chunks of text and their rows whatever the file's length. The pool's threads
        are named after the calling thread, and end before this generator does, however it
        ends.

        Args:
            text: The decompressed file, positioned at its first row.
            plan: What to read and which rows to keep.
            options: How Arrow reads the file's rows: its read and convert options.

        Yields:
            What `_apply` returns for each block, in file order.

        Raises:
            The errors of decompressing and parsing (`pyarrow.ArrowException`, `OSError`,
            `EOFError`, `zlib.error`), which `batches` turns into `WireError`. A failure to
            read or decompress is raised once every block read before it has been handed on,
            as a reader that reached it block by block would.
        """
        parse = functools.partial(self._parse, plan=plan, options=options)
        rows_before = 0  # rows read in the chunks handed on, for the first skipped row's number

        def handed(parsed: _Parsed) -> list[_Block]:
            nonlocal rows_before
            blocks, rows, malformed = parsed
            if malformed.rows:
                self.malformed_lines += malformed.lines
                self.malformed_reason = self.malformed_reason or malformed.reason(rows_before)
            rows_before += rows  # read only up to the chunk of the first skipped row
            return blocks

        chunks = iter(
            functools.partial(_chunk, text, size=plan.chunk_size, line_limit=plan.block_size), b""
        )
        name = f"{threading.current_thread().name} parse"
        # Closing the read-ahead first cancels the chunks not yet parsed, so the pool's
        # shutdown waits only for the ones running when this generator ends early.
        with (
            ThreadPoolExecutor(plan.parse_threads, name) as pool,
            contextlib.closing(
                _in_order(pool, chunks, parse, ahead=plan.parse_threads, deferred=_READ_FAILURES)
            ) as parsed_chunks,
        ):
            for parsed in parsed_chunks:
                yield from handed(parsed)

    def _parse(
        self,
        chunk: bytes,
        *,
        plan: ReadPlan,
        options: tuple[pacsv.ReadOptions, pacsv.ConvertOptions],
    ) -> _Parsed:
        """Parse one chunk into blocks, skipping its malformed lines, and filter each block.

        A chunk that is not UTF-8 text first loses each line that is not (`_text_lines`), so
        one such line costs only itself: Arrow would refuse the whole chunk. Arrow then reads
        the rest in blocks of `plan.block_size`, skipping each row it cannot split into the
        header's fields (`_Malformed.skip`), and `_apply` filters and converts each block.
        Runs on a thread of the reader's pool.

        Args:
            chunk: The chunk's text, cut after a line end.
            plan: What to read and which rows to keep.
            options: How Arrow reads the file's rows: its read and convert options.

        Returns:
            What `_apply` returns for each block, or one empty block when the chunk has no row
            left, so the consumer still checks for cancellation and sees the chunk's skipped
            lines; the rows Arrow read; and the lines skipped.
        """
        read_options, convert_options = options
        malformed = _Malformed()
        if not _is_text(chunk):
            chunk = _text_lines(chunk, malformed)
        blocks = []
        rows = 0
        if chunk:  # Arrow refuses an empty body: every line was removed
            table = pacsv.read_csv(
                pa.py_buffer(chunk),
                read_options=read_options,
                parse_options=pacsv.ParseOptions(invalid_row_handler=malformed.skip),
                convert_options=convert_options,
            )
            blocks = [self._apply(b, plan) for b in table.to_batches()]
            rows = table.num_rows
        return blocks or [_Block(_empty(plan))], rows, malformed

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
            `WireError("cannot read <location>: <reason>")`, `WireFailure.UNREADABLE`, for
            the caller to raise.
        """
        return WireError(
            f"cannot read {self._location}: {reason}",
            host=self._host,
            path=self._key,
            kind=WireFailure.UNREADABLE,
        )

    def _apply(self, block: pa.RecordBatch, plan: ReadPlan) -> _Block:
        """Filter one block, mark the rows it cannot read whole, and convert it.

        Args:
            block: One block as the CSV reader read it, every header as text.
            plan: What to keep.

        Returns:
            The block's kept rows, possibly none, in the plan's columns, with their drift and
            marks, and its counts for the whole-file rule of `batches`.
        """
        read = block.num_rows
        if plan.symbols is not None:
            block = block.filter(pc.is_in(block[plan.symbol_header], value_set=plan.symbols))
        considered = block.num_rows
        keys = self._keyed(block, plan)
        first = keys.reasons[min(keys.reasons)][1] if keys.reasons else ""
        block, keyed = _windowed(block, keys, plan)
        rows, drift, marks = self._converted(block, keyed, plan)
        return _Block(rows, drift, marks, considered, len(keys.reasons), first, read)

    def _converted(
        self, block: pa.RecordBatch, keyed: Mapping[int, tuple[str, str]], plan: ReadPlan
    ) -> tuple[pa.RecordBatch, DriftReport, RowMarks]:
        """Convert a filtered block's headers to their types, and mark the rows not read whole.

        Args:
            block: The block's kept rows, every header as text.
            keyed: Each kept row whose key cannot be read, with its key's column and why.
            plan: The plan, for its columns.

        Returns:
            The rows in the plan's columns; the drift of the headers whose text does not parse
            in a row whose keys could be read; and the marks.
        """
        marker = RowMarker()
        symbols = block[plan.symbol_header]
        for row in sorted(keyed):
            key_column, reason = keyed[row]
            symbol = symbols[row].as_py()
            marker.mark(
                RowFault.UNREADABLE_KEYS, row, field=key_column, reason=reason, symbol=symbol
            )
        arrays = []
        odd: dict[str, DriftSample] = {}
        for column in plan.columns:
            sent = block[column.header]
            values, failed = _parsed(sent, column.read_type, column.list_type)
            if failed is not None:
                self._mark_unfit(
                    sent, failed, column, keyed=keyed, symbols=symbols, marker=marker, odd=odd
                )
            arrays.append(values)
        rows = pa.RecordBatch.from_arrays(arrays, names=[c.column for c in plan.columns])
        return rows, DriftReport(unconvertible=frozenset(odd), samples=odd), marker.marks()

    @staticmethod
    def _mark_unfit(
        sent: pa.Array,
        failed: pa.Array,
        column: ReadColumn,
        *,
        keyed: Collection[int],
        symbols: pa.Array,
        marker: RowMarker,
        odd: dict[str, DriftSample],
    ) -> None:
        """Mark the rows whose text of one header does not parse, and record the header's drift.

        A row whose key cannot be read keeps that fault and adds no drift; a row an earlier
        header marked keeps its first such header.

        Args:
            sent: The header's text in each kept row.
            failed: Where it does not parse.
            column: The header's column.
            keyed: The rows whose key cannot be read.
            symbols: Each kept row's symbol text.
            marker: Receives the marks.
            odd: Receives the header and its first such text.
        """
        for row in pc.indices_nonzero(failed).to_pylist():
            if row in keyed:
                continue
            text = sent[row].as_py()[:_SAMPLE_LIMIT]
            marker.mark(
                RowFault.UNCONVERTIBLE_VALUES,
                row,
                field=column.column,
                reason=f"{column.header} holds {text!r}, which is not {column.read_type}",
                symbol=symbols[row].as_py(),
            )
            odd.setdefault(column.header, DriftSample(None, text))

    def _keyed(self, block: pa.RecordBatch, plan: ReadPlan) -> _Keys:
        """Find the rows of a block whose key header is empty, does not parse, or cannot be placed.

        A key header (`ReadPlan.required`) is empty or does not parse as its type, or, when the
        time header is a key, its value is one the plan's converter cannot place
        (`ReadPlan.to_ns`). The time header is then converted here once, for the window too. A
        row with several such keys is marked for the first header, in name order.

        Args:
            block: The block's rows of the requested symbols, every header as text.
            plan: The plan, for its key headers and time converter.

        Returns:
            Each such row's index with its first header's column and why (naming the header
            and quoting the value), and the rows' times in epoch nanoseconds, or None when the
            time header is no key.
        """
        reasons: dict[int, tuple[str, str]] = {}
        nanos: pa.Array | None = None
        columns = {c.header: c.column for c in plan.columns}
        for header in sorted(plan.required):
            sent = block[header]
            read_type = _read_type(plan, header)
            values, failed = _parsed(sent, read_type)
            bad = pc.is_null(sent) if failed is None else pc.or_(pc.is_null(sent), failed)
            unplaced = None
            if header == plan.time_header:
                nanos = plan.to_ns(values)
                unplaced = pc.and_(pc.is_valid(values), pc.is_null(nanos))
                bad = pc.or_(bad, unplaced)
            for row in pc.indices_nonzero(bad).to_pylist():
                if row in reasons:
                    continue
                text = sent[row].as_py()
                if text is None:
                    why = f"a row has no {header}"
                elif unplaced is not None and unplaced[row].as_py():
                    why = plan.unplaced(values[row].as_py())
                else:
                    why = f"{header} holds {text[:_SAMPLE_LIMIT]!r}, which is not {read_type}"
                reasons[row] = (columns[header], why)
        return _Keys(reasons, nanos)


def _windowed(
    block: pa.RecordBatch, keys: _Keys, plan: ReadPlan
) -> tuple[pa.RecordBatch, Mapping[int, tuple[str, str]]]:
    """Keep a block's rows inside the plan's window, and those whose time cannot be read.

    Args:
        block: The block's rows of the requested symbols, every header as text.
        keys: The rows whose key cannot be read, and the rows' times (`FlatFile._keyed`).
        plan: The plan, for its window and time header.

    Returns:
        The kept rows, and each of them whose key cannot be read, by its index among them.
    """
    if plan.window_ns is None:
        return block, keys.reasons
    start, end = plan.window_ns
    nanos = keys.nanos
    if nanos is None:  # the time header is no key of this plan
        times, _ = _parsed(block[plan.time_header], _read_type(plan, plan.time_header))
        nanos = plan.to_ns(times)
        inside = pc.and_(pc.greater_equal(nanos, start), pc.less(nanos, end))
    else:  # a time that cannot be read stays for its mark: no window can place it
        within = pc.and_(pc.greater_equal(nanos, start), pc.less(nanos, end))
        inside = pc.or_(pc.fill_null(within, fill_value=False), pc.is_null(nanos))
    kept = pc.indices_nonzero(pc.fill_null(inside, fill_value=False)).to_pylist()
    at = {row: index for index, row in enumerate(kept)}
    keyed = {at[row]: why for row, why in keys.reasons.items() if row in at}
    return block.take(pa.array(kept, pa.int64())), keyed


@dataclass(frozen=True, slots=True)
class _Keys:
    """The rows of a block whose key cannot be read, and the block's times.

    Attributes:
        reasons: Each such row's index, with its first such header's column and why.
        nanos: Every row's time in epoch nanoseconds, null where it cannot be read; None when
            the time header is no key of the plan.
    """

    reasons: Mapping[int, tuple[str, str]]
    nanos: pa.Array | None


@dataclass(frozen=True, slots=True)
class _Block:
    """One block's rows as the reader hands them on, and its counts for the whole-file rule.

    Attributes:
        rows: The kept rows, in the plan's columns.
        drift: The block's own drift: the headers that held text that does not parse, in a row
            whose keys could be read.
        marks: The kept rows it could not read whole, by index into `rows`.
        considered: The block's rows of the requested symbols, before the window.
        unread: How many of those had a key that cannot be read.
        unread_reason: Why the first of them could not be read; `""` when none.
        read: The block's rows, every symbol's.
    """

    rows: pa.RecordBatch
    drift: DriftReport = dataclasses.field(default_factory=DriftReport)
    marks: RowMarks = dataclasses.field(default_factory=lambda: _NO_MARKS)
    considered: int = 0
    unread: int = 0
    unread_reason: str = ""
    read: int = 0


_NO_MARKS: Final[RowMarks] = MappingProxyType({})
"""The marks of a block whose every row was read whole."""


class _Malformed:
    """The malformed lines of one chunk, skipped: not UTF-8 text, or not the header's fields.

    `FlatFile._parse` makes one per chunk. It records each line that is not UTF-8 text
    (`not_text`) as it removes the line, before Arrow reads the chunk, and passes `skip` to
    Arrow as the chunk's `invalid_row_handler`. Both run on the parse thread reading that
    chunk; the reader thread reads the counts only once that parse has ended, so no lock is
    needed.

    Attributes:
        rows: The rows skipped, as Arrow counts rows (a quote that does not close makes one row
            of several lines); a line that is not UTF-8 text is one row.
        lines: The lines those rows spanned.
    """

    __slots__ = ("_first", "lines", "rows")

    def __init__(self) -> None:
        """Start with nothing skipped."""
        self.rows = 0
        self.lines = 0
        self._first: tuple[int, str] = (0, "")

    def not_text(self, number: int) -> None:
        """Count a line that is not UTF-8 text, removed before Arrow reads the chunk.

        Args:
            number: The line's row number in the chunk (from 1, blank lines left out).
        """
        self._note(number, "is not UTF-8 text")

    def skip(self, row: pacsv.InvalidRow) -> str:
        """Count a row Arrow cannot split into the header's fields, and tell Arrow to skip it.

        Args:
            row: Arrow's description of the row: its number in the chunk Arrow was given
                (from 1, blank lines left out), its field count, the header's, and its text,
                which is only counted. Without the lines `not_text` removed, the number is low
                by the removed lines before the row, but such a row is never the chunk's first:
                the first removed line comes before it, and its number is no higher.

        Returns:
            `"skip"`, Arrow's word for leaving the row out and reading on.
        """
        problem = f"has {row.actual_columns} fields where the header has {row.expected_columns}"
        self._note(row.number, problem, lines=row.text.count("\n") + 1)
        return "skip"

    def _note(self, number: int, problem: str, *, lines: int = 1) -> None:
        """Count a skipped row, keeping the chunk's first one.

        Args:
            number: The row's number in the chunk.
            problem: What is wrong with it, such as `"is not UTF-8 text"`.
            lines: The lines it spans.
        """
        if not self.rows or number < self._first[0]:
            self._first = (number, problem)
        self.rows += 1
        self.lines += lines

    def reason(self, rows_before: int) -> str:
        """Return where the chunk's first skipped row is in the file, and what is wrong with it.

        Args:
            rows_before: The rows Arrow read in the file's earlier chunks, which skipped none:
                only the file's first skipped row is reported.

        Returns:
            Such as `"row 1234 after the header has 5 fields where the header has 9"` or
            `"row 1234 after the header is not UTF-8 text"`.
        """
        number, problem = self._first
        return f"row {rows_before + number} after the header {problem}"


def _is_text(chunk: bytes) -> bool:
    """Tell whether a chunk is UTF-8 text, in one Arrow validation that copies nothing.

    Args:
        chunk: The chunk's bytes.

    Returns:
        True when every byte belongs to a valid UTF-8 sequence.
    """
    offsets = pa.array([0, len(chunk)], pa.int64()).buffers()[1]
    text = pa.Array.from_buffers(pa.large_string(), 1, [None, offsets, pa.py_buffer(chunk)])
    try:
        text.validate(full=True)
    except pa.ArrowInvalid:
        return False
    return True


def _text_lines(chunk: bytes, malformed: _Malformed) -> bytes:
    """Remove the lines of a chunk that are not UTF-8 text, counting each in `malformed`.

    Arrow refuses a whole chunk for one value that is not UTF-8 text, and cannot hand such a
    line to `_Malformed.skip` either, so the lines are found here, one by one, only in a chunk
    that `_is_text` refused. Massive writes no line end inside a value, so a line is a row.

    Args:
        chunk: A chunk that is not UTF-8 text.
        malformed: The chunk's count of skipped lines.

    Returns:
        The chunk without those lines; empty when none was text.
    """
    kept = []
    number = 0
    for line in chunk.splitlines(keepends=True):
        if line.rstrip(b"\r\n"):  # Arrow numbers rows with the blank lines left out
            number += 1
        try:
            line.decode("utf-8")
        except UnicodeDecodeError:
            malformed.not_text(number)
        else:
            kept.append(line)
    return b"".join(kept)


_Parsed: TypeAlias = tuple[list[_Block], int, _Malformed]
"""One parsed chunk: what `_apply` returns for each block, the chunk's rows Arrow read, and the
rows it skipped."""


def _in_order(
    pool: Executor,
    items: Iterator[_T],
    work: Callable[[_T], _R],
    *,
    ahead: int,
    deferred: tuple[type[Exception], ...],
) -> Generator[_R, None, None]:
    """Run `work` on each item on a pool, and yield the results in the items' order.

    At most `ahead + 1` items are taken before their results are handed on, so a slow consumer
    holds a bounded number of items and results however many there are. A failure of `work`
    is raised when its result's turn comes. When the generator ends early, the work not yet
    started is cancelled; the caller's pool waits for the work running.

    Args:
        pool: The pool that runs the work.
        items: The items, read on the calling thread.
        work: What to run on each item.
        ahead: How many items to take beyond the one whose result is awaited next.
        deferred: The failures of reading `items` that wait for the results of every item
            read before them, as a consumer that reached them in order would; any other
            failure of reading `items` is raised at once.

    Yields:
        Each item's result, in the items' order.

    Raises:
        Exception: The `deferred` failure of reading `items`, once every result before it has
            been yielded.
    """
    pending: deque[Future[_R]] = deque()
    failed: Exception | None = None
    try:
        while True:
            try:
                item = next(items)
            except StopIteration:
                break
            except deferred as error:
                failed = error
                break
            pending.append(pool.submit(work, item))
            if len(pending) > ahead:
                yield pending.popleft().result()
        while pending:
            yield pending.popleft().result()
    finally:
        for future in pending:  # ended early: drop the work not yet started
            future.cancel()
    if failed is not None:
        raise failed


def _chunk(text: gzip.GzipFile, *, size: int, line_limit: int) -> bytes:
    """Read the next chunk of the decompressed file: about `size` bytes, cut after a line end.

    Massive writes no line end inside a value, so cutting after one never splits a row of a
    good file. A quote that does not close is a malformed row however the text is cut: Arrow
    skips it, with the lines it takes in up to the next quote or the end of its block.

    Args:
        text: The decompressed file.
        size: The bytes to read before the cut.
        line_limit: The most bytes read to finish the last line: a row longer than a block,
            which Arrow refuses anyway ("straddling object"), is not read whole into memory.

    Returns:
        The chunk; empty at the end of the file.
    """
    chunk = text.read(size)
    if chunk.endswith(b"\n") or not chunk:
        return chunk
    return chunk + text.readline(line_limit)


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


_TEXT_OF: Final[Mapping[pa.DataType, str]] = MappingProxyType(
    {
        pa.int64(): r"^-?[0-9]{1,38}$",
        pa.float64(): (
            r"^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?$|^(?i:[+-]?(nan|inf|infinity))$"
        ),
        pa.bool_(): r"^(?i:true|false|1|0)$",
        pa.date32(): r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
        pa.timestamp("ns", tz="UTC"): (
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?"
            r"(Z|[+-][0-9]{2}:[0-9]{2})$"
        ),
    }
)
"""The pattern a typed header's value must match, its surrounding spaces trimmed, for Arrow's
cast to read it: every text a pattern admits is one the cast reads, so one vectorized match
finds the values that do not parse without a Python call per value. An `int64` is then checked
against its range, and a date or an instant against the calendar, which a pattern cannot see."""

_CALENDAR: Final[Mapping[pa.DataType, tuple[int, str]]] = MappingProxyType(
    {pa.date32(): (10, "%Y-%m-%d"), pa.timestamp("ns", tz="UTC"): (19, "%Y-%m-%dT%H:%M:%S")}
)
"""For a date or an instant, how many leading characters name its calendar day and time of
day, and their format: a value is a real date only when that text reads and prints back the
same (`2025-02-30` reads as March 2, so it fails)."""

_INT64_LOW: Final = pa.scalar(-(2**63), pa.decimal128(38, 0))
_INT64_HIGH: Final = pa.scalar(2**63 - 1, pa.decimal128(38, 0))
"""The signed 64-bit range, as decimals, which hold every integer the `int64` pattern admits."""

_NO_TEXT: Final = pa.scalar(None, pa.string())


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
    text: pa.Array, read_type: pa.DataType, list_type: pa.DataType | None = None
) -> tuple[pa.Array, pa.Array | None]:
    """Convert a header's text to its type, the values that do not parse becoming null.

    A value's surrounding spaces are trimmed first, as Arrow's typed CSV parsing does (a list's
    text is not, as before). The whole column is converted in one Arrow call; only when that
    call refuses the column are the values that do not parse found, by vectorized tests
    (`_parseable`, `_unparseable_lists`), so a column with odd values costs about what a clean
    one does, never a Python call per value.

    Args:
        text: The header's text in each row; an empty field is null.
        read_type: The type of the header's values: `string` (a list's text, too) or one of
            `_TEXT_OF`'s.
        list_type: For a list header, its Arrow list type; None otherwise.

    Returns:
        The converted values, null where the text was empty or does not parse; and a mask of
        the values that did not parse, or None when every value did.
    """
    if list_type is not None:
        convert = functools.partial(_split_list, list_type=list_type)
    elif read_type == pa.string():
        return text, None
    else:
        text = pc.utf8_trim_whitespace(text)
        convert = functools.partial(pc.cast, target_type=read_type)
    try:
        return convert(text), None
    except pa.ArrowInvalid:
        pass
    if list_type is None:
        failed = pc.invert(pc.fill_null(_parseable(text, read_type), fill_value=True))
    else:
        failed = _unparseable_lists(text, list_type.value_type)
    return convert(pc.if_else(failed, _NO_TEXT, text)), failed


def _parseable(text: pa.Array, read_type: pa.DataType) -> pa.Array:
    """Tell which trimmed texts Arrow's cast reads as their type, in vectorized calls.

    Args:
        text: Trimmed text values.
        read_type: Their type; one of `_TEXT_OF`'s.

    Returns:
        A boolean array, true where the value parses; null where the text is null.
    """
    ok = pc.match_substring_regex(text, _TEXT_OF[read_type])
    if read_type == pa.int64():
        numbers = pc.cast(pc.if_else(ok, text, _NO_TEXT), pa.decimal128(38, 0))
        inside = pc.and_(pc.greater_equal(numbers, _INT64_LOW), pc.less_equal(numbers, _INT64_HIGH))
        return pc.and_(ok, pc.fill_null(inside, fill_value=False))
    if read_type in _CALENDAR:
        width, form = _CALENDAR[read_type]
        day = pc.utf8_slice_codeunits(pc.if_else(ok, text, _NO_TEXT), 0, width)
        read = pc.strptime(day, format=form, unit="s", error_is_null=True)
        same = pc.equal(pc.strftime(read, format=form), day)
        return pc.and_(ok, pc.fill_null(same, fill_value=False))
    return ok


def _unparseable_lists(text: pa.Array, element_type: pa.DataType) -> pa.Array:
    """Find the list texts with an element that does not parse, in vectorized calls.

    Args:
        text: The list header's comma-separated text in each row.
        element_type: The type of its elements, such as `int64`.

    Returns:
        A boolean array, true for a row whose text is not null and holds an element that
        does not parse (an empty element, as in `"12,,3"`, included).
    """
    parts = pc.split_pattern(text, ",")
    bad_items = pc.invert(_parseable(pc.list_flatten(parts), element_type))
    bad_rows = pc.filter(pc.list_parent_indices(parts), bad_items)
    rows = pc.subtract(pc.cumulative_sum(pa.repeat(pa.scalar(1, pa.int64()), len(text))), 1)
    return pc.is_in(rows, value_set=pc.cast(bad_rows, pa.int64()))
