"""Decoding one flat file: a gzip CSV object streamed into Arrow batches, one per block.

`FlatFile` wraps the body of one S3 object, as a binary stream that `FlatFileClient.open`
provides, and decodes it with `pyarrow.csv.open_csv` in blocks of `ReadPlan.block_size`
bytes of decompressed text. Python's `gzip` decompresses on the reader's own read calls; Arrow
asks for one block per call and reads about 32 blocks ahead of the consumer on its own I/O
thread, so a reader holds about 32 blocks of text whatever the file's length.

Each block goes through pure Arrow operations, in this order:

1. the futures session check on the whole block (every row's session date must be the file's);
2. the symbol filter (`pyarrow.compute.is_in` on the symbol header);
3. the half-open `[start, end)` filter on the time header, through the plan's converter to
   epoch nanoseconds;
4. on the surviving rows only, the rename from CSV headers to the library's column names and
   the parsing of list headers (`"12,37"` becomes `[12, 37]`).

Instants stay in the file's own unit: the library converts them. Only an empty CSV field is
null; text such as `NA` or `NULL` is data (both are valid tickers). No block is converted to
Python objects.

`ReadPlan` and `ReadColumn` describe what to read; the library builds them from a dataset, so
this module knows no dataset. It imports only the standard library, `pyarrow`, and `net`: the
client hands it a stream whose read errors are `OSError`s, so it needs no botocore. The library
module that uses it is `_ingest/flatfiles.py`.
"""

from __future__ import annotations

import gzip
import zlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, BinaryIO, Protocol, Self, cast

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv

from deephaven_massive._massive.net.errors import WireError

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from datetime import date
    from types import TracebackType

__all__ = ["FlatFile", "ReadColumn", "ReadPlan"]


class _Stream(Protocol):
    """The binary stream a flat file is read from: an object's body, or any file of bytes."""

    def read(self, size: int = -1, /) -> bytes:
        """Read up to `size` bytes; raise `OSError` if the read fails."""
        ...

    def close(self) -> None:
        """Release the stream; idempotent."""
        ...


@dataclass(frozen=True, slots=True)
class ReadColumn:
    """One CSV header to read, and the column it becomes.

    Attributes:
        header: The CSV header, such as `"sip_timestamp"`.
        column: The name of the column in the batches `FlatFile.batches` yields, such as
            `"Timestamp"`.
        read_type: The Arrow type the CSV reader parses the header as: `int64` for an epoch
            timestamp (in the file's unit), `string` for a list, otherwise the column's type.
        list_type: For a list header, the Arrow list type its comma-separated text is parsed
            into (such as `list<int32>`); None otherwise.
    """

    header: str
    column: str
    read_type: pa.DataType
    list_type: pa.DataType | None = None


@dataclass(frozen=True, eq=False, slots=True)
class ReadPlan:
    """What to read from one flat file, and which rows to keep.

    The library builds one per file (`_ingest/flatfiles.py`), from the dataset's flat-file headers
    and the request; it is read-only and may be shared between threads.

    Attributes:
        columns: The headers to read, in the order of the batches' columns.
        symbol_header: The header that holds each row's symbol.
        symbols: The symbols to keep, as a `string` array, or None for every symbol.
        time_header: The header that holds each row's timestamp.
        window_ns: The half-open `(start, end)` range of epoch nanoseconds rows must fall in,
            or None to keep every time (date-bounded futures, whose files hold exactly the
            requested sessions).
        to_ns: Converts the time header's epoch integers to epoch nanoseconds by its declared
            unit; it may raise for a value outside that unit's range.
        session: For a futures file, the header that holds each row's trading session date
            and the session every row must belong to (the file's date); None otherwise.
        block_size: Bytes of decompressed CSV per block.
    """

    columns: tuple[ReadColumn, ...]
    symbol_header: str
    symbols: pa.Array | None
    time_header: str
    window_ns: tuple[int, int] | None
    to_ns: Callable[[pa.Array], pa.Array]
    session: tuple[str, date] | None
    block_size: int


class FlatFile:
    """One flat file being read: the open body of an S3 object and how to decode it.

    `FlatFileClient.open` creates one per existing key, around the object's body. A single
    reader thread iterates `batches()`; `close()` releases the body and may be called from any
    thread, any number of times. Use it as a context manager so the body is closed however the
    read ends.

    Example:
        ::

            with client.open("us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz") as file:
                for batch in file.batches(plan):
                    ...
    """

    __slots__ = ("_body", "_host", "_key", "_location")

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

    def batches(self, plan: ReadPlan) -> Generator[pa.RecordBatch, None, None]:
        """Decode the file block by block, keeping the rows the plan asks for.

        Yields one batch per block, possibly empty, so the consumer can check for cancellation
        after every block. Each batch has one column per `plan.columns`, named by
        `ReadColumn.column`, typed by `read_type` (or `list_type`). Blocks are read on the
        calling thread as it iterates; Arrow reads ahead on its own I/O thread.

        Args:
            plan: What to read and which rows to keep.

        Yields:
            The surviving rows of each block.

        Raises:
            WireError: If the body is not gzip, is truncated or corrupt, or the connection
                drops; if a documented header is missing or a value does not parse as its
                type (the message quotes Arrow's); or if a futures block holds a session
                date other than the plan's session, or none. Every message names the file. An
                error that `plan.to_ns` raises propagates unchanged, unless it is an Arrow
                error, which becomes `WireError` like any other.
        """
        read_types = {column.header: column.read_type for column in plan.columns}
        try:
            # gzip only reads its file object; its stub also asks for `seek`, which a response
            # body has no use for.
            body = cast("BinaryIO", self._body)
            stream = pa.PythonFile(gzip.GzipFile(fileobj=body, mode="rb"), mode="r")
            reader = pacsv.open_csv(
                stream,
                read_options=pacsv.ReadOptions(block_size=plan.block_size, use_threads=False),
                convert_options=pacsv.ConvertOptions(
                    column_types=read_types,
                    include_columns=list(read_types),
                    # Massive writes a missing value as an empty field and nothing else. Arrow's
                    # default null markers include `NA` and `NULL`, which are real tickers.
                    null_values=[""],
                    strings_can_be_null=True,
                ),
            )
            for block in reader:
                yield self._apply(block, plan)
        except (pa.ArrowException, OSError, EOFError, zlib.error) as error:
            raise WireError(
                f"cannot read {self._location}: {error}", host=self._host, path=self._key
            ) from error

    def _apply(self, block: pa.RecordBatch, plan: ReadPlan) -> pa.RecordBatch:
        """Filter one block and turn its surviving rows into the plan's columns.

        Args:
            block: One block as the CSV reader parsed it.
            plan: What to keep.

        Returns:
            The surviving rows, possibly none.

        Raises:
            WireError: If a futures block holds a session date other than the plan's session,
                or none; the message names the first such row's date.
        """
        if plan.session is not None:
            header, day = plan.session
            sessions = block[header]
            expected = pa.scalar(day, pa.date32())
            matches = pc.fill_null(pc.equal(sessions, expected), fill_value=False)
            wrong = pc.filter(sessions, pc.invert(matches))
            if len(wrong):
                found = wrong[0].as_py()
                shown = "empty" if found is None else found.isoformat()
                raise WireError(
                    f"{self._key} holds session_end_date {shown}, not {day.isoformat()}: "
                    "futures flat files are not keyed by trading "
                    "session as this library assumes, so their rows cannot be assigned to "
                    "sessions",
                    host=self._host,
                    path=self._key,
                )
        if plan.symbols is not None:
            block = block.filter(pc.is_in(block[plan.symbol_header], value_set=plan.symbols))
        if plan.window_ns is not None:
            start, end = plan.window_ns
            nanos = plan.to_ns(block[plan.time_header])
            block = block.filter(pc.and_(pc.greater_equal(nanos, start), pc.less(nanos, end)))
        arrays = []
        for column in plan.columns:
            values = block[column.header]
            if column.list_type is not None:
                values = pc.cast(pc.split_pattern(values, ","), column.list_type)
            arrays.append(values)
        return pa.RecordBatch.from_arrays(arrays, names=[c.column for c in plan.columns])
