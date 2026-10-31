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

Arrow reads every header as text, so one value that does not parse never fails a block. Each
block then goes through pure Arrow operations, in this order:

1. the symbol filter (`pyarrow.compute.is_in` on the symbol header);
2. the key check on the surviving rows: a row whose key header (`ReadPlan.required`) is empty
   or does not parse as its type, or whose time header the plan's converter cannot place
   (`ReadPlan.to_ns`), is dropped and counted; if every row of the requested symbols in the
   whole file is dropped, the file raises after its last block, since that is the sign of a
   change in the file's format rather than of one odd row (one block can hold a single row of
   an illiquid symbol, so the rule is never applied to a block alone);
3. the half-open `[start, end)` filter on the time header, in epoch nanoseconds;
4. on the rows left, the conversion of every other header to its type, and the parsing of list
   headers (`"12,37"` becomes `[12, 37]`): a value that does not parse is null, and its header
   is reported in `DriftReport.unconvertible` with the first such text; then the rename from
   CSV headers to the library's column names.

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

import csv
import functools
import gzip
import threading
import zlib
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, BinaryIO, Final, Protocol, Self, cast

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv

from deephaven_massive._massive.net.errors import WireError
from deephaven_massive._massive.spec.types import DriftReport, DriftSample

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Mapping
    from concurrent.futures import Future
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
        chunk_size: Bytes of decompressed CSV the reader cuts the file into, after a line
            end, to parse each piece on its own: a multiple of `block_size`.
        parse_threads: Threads that parse one file's chunks at once, beside the reader thread
            that decompresses it. At most one chunk more than this is read ahead of the
            consumer, so a reader holds about `(parse_threads + 1) * chunk_size` bytes of
            text, and the rows parsed from it, whatever the file's length.
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
    chunk_size: int
    parse_threads: int
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

    Attributes:
        rows_read: The rows `batches()` has read from the file so far, every symbol's.
        rows_of_symbols: Of those, the rows of the plan's symbols (all of them when the plan
            names none).
        rows_kept: Of those, the rows `batches()` yielded: a readable key, inside the plan's
            window. The three tell a caller which step emptied a file; they are written by
            the reader thread as it iterates and are final once `batches()` ends.
    """

    __slots__ = (
        "_body",
        "_dropped",
        "_host",
        "_key",
        "_location",
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
                (before the first block, naming the headers); or, after its last block, if
                the file held rows of the requested symbols and every one of them was dropped
                for a key, naming the first one's reason. Every message names the file.
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
                ),
            )
            # Rows of the requested symbols the file held, and how many of them were dropped.
            held = dropped = 0
            reason = ""
            for rows, drift, considered, read in self._blocks(text, plan, options):
                self.rows_read += read
                self.rows_of_symbols += considered
                self.rows_kept += rows.num_rows
                held += considered
                dropped += drift.dropped_rows
                reason = reason or drift.dropped_reason
                yield rows, headers.merge(drift)
            if held and dropped == held:
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
                unreadable=not self._dropped.is_set(),
            ) from error

    def _blocks(
        self,
        text: gzip.GzipFile,
        plan: ReadPlan,
        options: tuple[pacsv.ReadOptions, pacsv.ConvertOptions],
    ) -> Generator[tuple[pa.RecordBatch, DriftReport, int, int], None, None]:
        """Parse and filter the file's rows chunk by chunk on a small pool, in file order.

        The calling thread decompresses the file and cuts the text after a line end into
        chunks of about `plan.chunk_size` bytes (`_chunk`); `plan.parse_threads` threads of
        the pool each parse a chunk (`pyarrow.csv.read_csv`, in blocks of `plan.block_size`)
        and run `_apply` on its blocks. At most `plan.parse_threads + 1` chunks are read
        ahead of the consumer, so a reader holds about that many chunks of text and their
        rows whatever the file's length. The pool's threads are named after the calling
        thread, and end before this generator does, however it ends.

        Args:
            text: The decompressed file, positioned at its first row.
            plan: What to read and which rows to keep.
            options: How Arrow reads the file's rows: its read and convert options.

        Yields:
            What `_apply` returns for each block, then the block's row count (every symbol's),
            in file order.

        Raises:
            The errors of decompressing and parsing (`pyarrow.ArrowException`, `OSError`,
            `EOFError`, `zlib.error`), which `batches` turns into `WireError`. A failure to
            read or decompress is raised once every block read before it has been handed on,
            as a reader that reached it block by block would.
        """
        read_options, convert_options = options

        def parse(chunk: bytes) -> list[tuple[pa.RecordBatch, DriftReport, int, int]]:
            table = pacsv.read_csv(
                pa.py_buffer(chunk), read_options=read_options, convert_options=convert_options
            )
            return [(*self._apply(block, plan), block.num_rows) for block in table.to_batches()]

        name = f"{threading.current_thread().name} parse"
        pending: deque[Future[list[tuple[pa.RecordBatch, DriftReport, int, int]]]] = deque()
        failed: Exception | None = None
        with ThreadPoolExecutor(plan.parse_threads, name) as pool:
            try:
                while True:
                    try:
                        chunk = _chunk(text, size=plan.chunk_size, line_limit=plan.block_size)
                    except (OSError, EOFError, zlib.error) as error:
                        # The rows read before the failure are handed on first, in order.
                        failed, chunk = error, b""
                    if not chunk:
                        break
                    pending.append(pool.submit(parse, chunk))
                    if len(pending) > plan.parse_threads:
                        yield from pending.popleft().result()
                while pending:
                    yield from pending.popleft().result()
            finally:
                for future in pending:  # ended early: drop the chunks not yet parsed
                    future.cancel()
        if failed is not None:
            raise failed

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

    def _apply(
        self, block: pa.RecordBatch, plan: ReadPlan
    ) -> tuple[pa.RecordBatch, DriftReport, int]:
        """Filter one block, drop the rows whose key cannot be read, and convert the rest.

        Args:
            block: One block as the CSV reader read it, every header as text.
            plan: What to keep.

        Returns:
            The kept rows, possibly none, in the plan's columns; the block's own drift: the
            rows dropped for a key (`_keyed`), and the headers that held text that does not
            parse (`unconvertible`, with the first such text as the sample); and how many rows
            of the requested symbols the block held, for the whole-file rule of `batches`.
        """
        if plan.symbols is not None:
            block = block.filter(pc.is_in(block[plan.symbol_header], value_set=plan.symbols))
        considered = block.num_rows
        block, drift, nanos = self._keyed(block, plan)
        if plan.window_ns is not None:
            start, end = plan.window_ns
            if nanos is None:  # the time header is no key of this plan
                times, _ = _parsed(block[plan.time_header], _read_type(plan, plan.time_header))
                nanos, _ = plan.to_ns(times)
            block = block.filter(pc.and_(pc.greater_equal(nanos, start), pc.less(nanos, end)))
        arrays = []
        odd: dict[str, DriftSample] = {}
        for column in plan.columns:
            sent = block[column.header]
            values, failed = _parsed(sent, column.read_type, column.list_type)
            if failed is not None:
                first = pc.filter(sent, failed)[0].as_py()
                odd[column.header] = DriftSample(None, first[:_SAMPLE_LIMIT])
            arrays.append(values)
        rows = pa.RecordBatch.from_arrays(arrays, names=[c.column for c in plan.columns])
        return rows, drift.merge(DriftReport(unconvertible=frozenset(odd), samples=odd)), considered

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
        """
        keep = pa.repeat(pa.scalar(value=True), block.num_rows)
        nanos: pa.Array | None = None
        reason = ""
        for header in sorted(plan.required):
            sent = block[header]
            read_type = _read_type(plan, header)
            values, failed = _parsed(sent, read_type)
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
        dropped = DriftReport(dropped_rows=rows - block.num_rows, dropped_reason=reason)
        return block, dropped, None if nanos is None else nanos.filter(keep)


def _chunk(text: gzip.GzipFile, *, size: int, line_limit: int) -> bytes:
    """Read the next chunk of the decompressed file: about `size` bytes, cut after a line end.

    Arrow reads the files with `newlines_in_values` off, so it ends a row at every line end,
    even inside quotes: cutting after one never splits a row it would read whole.

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
