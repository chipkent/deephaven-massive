"""Live streams on the Deephaven side: publishers, the flush thread, and the shared-batch fan-out.

A WebSocket *stream* is one `(dataset, channel, symbols)` with its own publisher and blink table.
The WebSocket transport (`_ingest/ws.py`) owns the connections and decides which topics are
subscribed; this module owns everything that touches Deephaven for a stream:

- `ChannelPlan`, how one dataset reads one channel: the channel's generated columns, renamed
  for the dataset (`source_columns.rename`), their wire types, the dataset's merged schema, and the
  required keys and symbol column the wire client checks (`set_required`). Pure; built once
  per (dataset, channel).
- `open_stream_tables`, which creates a stream's liveness scope and, inside it, the publisher
  and its blink table (`blink_publisher`, whose failure is delivered exactly once) and, for a
  reactive symbol table, the `SymbolWatch` that follows it. It runs on the caller thread,
  inside the session's execution context.
- `SharedBatch`, one flush's converted rows of a channel for one dataset, as a static table
  that every stream of the dataset on the channel selects its rows from.
- `StreamSink`, a stream as the flush thread sees it: its session, the publisher, the symbols it
  receives (`None` for `dm.ALL`), and its count of published rows.
- `Flusher`, one per connection: a daemon thread that every `flush_interval` drains the
  connection's row buffers (one `queue.SimpleQueue` per channel, fed by the connection's loop
  thread with each frame's rows and the instant it arrived) and its control queue (failures and
  drift), and publishes. A connection may serve the streams of several sessions that share it,
  so the flusher holds no session: each sink reports to its own.

**Fan-out.** Per channel and flush, the rows (tuples of field values, symbols as the wire spells
them) are converted once: one Arrow table built from the rows, raw symbols mapped to canonical ones
per distinct symbol, every epoch run through the timestamp routine as a vectorized mask, columns
widened to the dataset schema, and the library's time columns filled (`row_times.TimeRule`): `Date`,
a bar's end, and `TimestampInsert`, the instant each row's frame arrived. Futures messages carry no
session date, so a futures row's `Date` is the 17:00 Chicago rule's, corrected by the connection's
`FuturesSessionDates` (the WebSocket transport's schedule calendar), which never waits; a row its
schedules cannot place gets a null `Date` (the calendar logs it), since the rule's date can be wrong
there, and the stream keeps running with its `Error` untouched. A row whose time column fails the
bound cannot be placed, so it is dropped from the batch and counted, and any other timestamp that
fails it is null and reported as schema drift; neither fails a stream. The batch becomes one static
Deephaven table (`deephaven.arrow.to_table`), shared by the dataset's streams on the channel
(`SharedBatch`). Each `dm.ALL` stream, and each symbol stream that holds every symbol of the batch,
gets the whole batch; a symbol stream that holds none of them gets nothing. A symbol stream that
holds exactly one of them gets that symbol's rows from a partition of the batch by `Symbol`, built
once per flush the first time a stream needs it (`Table.partition_by`), when the batch holds at
least `SPLIT_MIN_ROWS` rows and at least `SPLIT_MIN_STREAMS` streams take one symbol each: many
single-symbol streams of a large batch then cost one partition and a lookup each rather than one
scan of the batch each, while a lone single-symbol stream beside a busy channel filters, since the
partition's cost grows with the batch's distinct symbols. Every other stream that holds some but not
all of the batch's symbols gets `batch.where_in(filter, "Symbol")`, which keeps the rows in arrival
order across its symbols, where `filter` is a static one-column table cached per stream and rebuilt
only when the stream's symbols change. So the Python work is O(rows) per channel and flush plus a
set intersection per stream; the Java work is one partition at most, which grows with the batch's
distinct symbols, plus one lookup or filter and one `add` per stream. Each flush ends with one
`SessionCore.update_streams` call per session, carrying the published-row total of each of its
streams.

**Failures.** The connection's loop thread never calls Java, so it hands every stream failure
(a refused topic, a rejected key) to the flush thread's control queue; the flush thread fails
the stream's table with `publish_failure` and records it with `SessionCore.fail_stream`, once
per stream. Drift goes through the same queue: the flush thread logs it once per (dataset,
transport, field) through each session's `DriftRegistry`, counts the rows dropped because a
key could not be read, and adds it to each receiving stream's `SchemaDrift`
(`SessionCore.record_stream_drift`). One odd row never fails a stream; but once
`DROPPED_RUN_LIMIT` rows of a dataset on a channel in a row have been dropped, with none
published between them, the dataset's streams on the channel fail, since Massive's format has
most likely changed and the streams would otherwise stay empty without an error. A reactive
stream's watchlist that fails queues the stream's failure on its session's reaper
(`StreamSink.fail_later`), since the listener reports it on an update-graph thread.

**Threads and locks.** The flush thread creates only static tables, enters the execution
context of the session that created its connection through its own wrapper (sessions share a
connection only when they share an update graph), and never holds `registry_lock` while it calls
Deephaven. Its one lock, the flush lock, serializes flushes (the thread's own and any a test
calls) and is never taken under another library lock; the routing table has its own leaf lock.
The flusher holds no session, and a routed sink holds its own only weakly. A flush thread that
dies on something it cannot carry on after fails every stream it routes before it ends.

The module is one subject although it runs past 1,000 lines: it is one stream's path from its
publisher to the fan-out, and the flush thread, the routing, and the failures all act on the
same sinks under the same flush lock.
"""

from __future__ import annotations

import dataclasses
import functools
import logging
import queue
import threading
import time
import weakref
from collections import Counter
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final, Protocol, TypeVar, cast

import pyarrow as pa
import pyarrow.compute as pc
from deephaven import new_table
from deephaven.column import string_col

from deephaven_massive._datasets.column_descriptions import OPTION_ROOT, RAW_SYMBOL
from deephaven_massive._datasets.row_times import TimeRule, time_column
from deephaven_massive._datasets.source_columns import (
    RoundedFields,
    key_column,
    rename,
    required_fields,
)
from deephaven_massive._datasets.symbols import SymbolTable, option_roots
from deephaven_massive._datasets.timestamps import (
    first_failure,
    masked_instants,
    mismatch_message,
)
from deephaven_massive._datasets.wire_symbols import WireSymbols, unreadable_symbol_message
from deephaven_massive._enums import NotBars, OnError
from deephaven_massive._errors import LIBRARY_BUG, ErrorCause, MassiveError, library_bug
from deephaven_massive._log_limits import bounded_list, log_failure, mark_failure_logged
from deephaven_massive._massive import DriftReport, DriftSample, Transport, WireType
from deephaven_massive._tables.columns import col_defs, dataset_table
from deephaven_massive._tables.diagnostics import NotePart, StreamUpdate
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import new_scope, release_scope
from deephaven_massive._tables.publisher import BlinkPublisher, blink_publisher
from deephaven_massive._tables.transports import StreamHandle
from deephaven_massive._tables.watchlist import SymbolRowRule, SymbolWatch, skipped_rows_text
from deephaven_massive._types import AllSymbols

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence
    from collections.abc import Set as AbstractSet

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import PartitionedTable, Table

    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.schema import ColumnType, Schema
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._datasets.timestamps import TimeUnit
    from deephaven_massive._enums import AssetClass, Recency
    from deephaven_massive._tables.session_core import SessionCore

__all__ = [
    "BACKLOG_WARNING_ROWS",
    "DROPPED_RUN_LIMIT",
    "SPLIT_MIN_ROWS",
    "SPLIT_MIN_STREAMS",
    "SYMBOL_CACHE_LIMIT",
    "ChannelPlan",
    "Flusher",
    "FuturesSessionDates",
    "OpenedStream",
    "SharedBatch",
    "StreamSink",
    "open_stream_tables",
]

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

BACKLOG_WARNING_ROWS: Final = 1_000_000
"""Rows a connection may hold waiting for its flush thread before a WARNING is logged.

Rows wait between two flushes (`MassiveConfig.flush_interval`), so a connection normally holds
only a few flush intervals' worth. A backlog this large means the flushes are falling behind the
wire: the update graph is stalled or too slow for the stream (a `dm.ALL` burst at the open, for
example). Nothing is dropped, so memory grows with the backlog; the WARNING says so, once each
time the backlog crosses this level, and again only after it has fallen below half of it.
"""

DROPPED_RUN_LIMIT: Final = 100
"""Rows of a dataset on a channel, dropped in a row with none published, that fail its streams.

A row whose symbol or time cannot be read is dropped and counted, and fails no stream: one odd
row from Massive should never cost a stream the rows after it. But when every row is dropped,
Massive has most likely changed its format (a renamed key, a new timestamp unit), and a stream
would stay empty with only a log line to say why. So once this many rows in a row have been
dropped for one dataset on one channel of a connection, across flushes and symbols, with no row
of it published between them, every stream of the dataset on the channel fails, naming the
first dropped row's reason. A published row starts the count again. Each symbol has the same
run of its own, which other symbols' rows do not end: once this many of one symbol's rows in a
row have been dropped, with none of its rows published, the streams that name that symbol
fail, so a symbol Massive stopped sending readably never freezes them without an error. A
`dm.ALL` stream is never failed for one symbol; its `Error` names the symbol as possibly stale,
without failing the stream, until a row of the symbol is published again.
"""

SYMBOL_CACHE_LIMIT: Final = 100_000
"""Wire spellings a flush thread remembers for one dataset on one channel before it starts over.

A flush thread reads each distinct wire symbol once and remembers its canonical symbol, so a
stream's steady traffic costs no symbol reading. A `dm.ALL` stream of options, crypto, or forex
meets new spellings every day (new contracts are listed, old ones expire), so the memory would
grow for as long as the connection lives. Past this many spellings the memory is emptied after
the flush and refilled from the next flushes' rows. A full memory of options spellings measures
about 10 to 12 megabytes, and starting over costs one extra reading of each spelling still
trading.
"""

SPLIT_MIN_ROWS: Final = 5_000
"""Rows a batch must hold before streams that take one symbol each may share a split of it.

A stream that holds exactly one of a flush's symbols takes that symbol's rows either from a
split of the batch by `Symbol` (`Table.partition_by`), shared by every such stream of the flush,
or from a filter of its own (`Table.where_in`). A filter scans the whole batch, about 0.25 ms
at 2,000 rows and 1.5 ms at 20,000; the split costs work for each distinct symbol of the batch
and then about 0.3 ms per stream. Below this many rows a filter costs no more than a stream's
share of the split, however many streams there are, so the batch is never split.
`test_benchmark_symbol_split` in the stream tests records both paths.
"""

SPLIT_MIN_STREAMS: Final = 8
"""Streams that must each take one symbol of a batch before the batch is split by symbol.

With fewer such streams the split's own cost, which grows with the batch's distinct symbols,
outweighs the filters it saves, so each stream filters (`SPLIT_MIN_ROWS` says what each path
costs). This keeps a lone `m.stocks.trades("AAPL")` beside a busy `dm.ALL` channel on one
filter per flush.
"""

_SYMBOL: Final = "Symbol"

_WIRE_ARROW: Final[Mapping[WireType, pa.DataType]] = {
    WireType.LONG: pa.int64(),
    WireType.EPOCH: pa.int64(),
    WireType.DOUBLE: pa.float64(),
    WireType.BOOL: pa.bool_(),
    WireType.STRING: pa.string(),
    WireType.JSON_TEXT: pa.string(),
    WireType.DATE: pa.date32(),
    WireType.ISO_INSTANT: pa.int64(),
    WireType.LONG_ARRAY: pa.list_(pa.int64()),
    WireType.STRING_ARRAY: pa.list_(pa.string()),
}
"""The Arrow type of each wire type as the WebSocket client delivers its values: an epoch as its
raw integer count (converted by its unit afterwards), an ISO-8601 instant as integer
nanoseconds, a date as a `datetime.date`, and a list as a list of integers or of text."""


class FuturesSessionDates(Protocol):
    """The schedule calendar that corrects streamed futures rows' session dates.

    The WebSocket transport gives a futures connection's flusher one
    (`_ingest/futures_calendar.py`); `ChannelPlan.convert` hands it the rule's dates of every
    futures row. It never waits on the network.
    """

    def correct(self, symbols: pa.Array, instants: pa.Array, dates: pa.Array) -> pa.Array:
        """Return the rows' session dates, corrected by the schedules.

        Args:
            symbols: The rows' canonical symbols, `string`.
            instants: The rows' instants (a tick's time, a bar's start), epoch nanoseconds,
                `int64`.
            dates: The 17:00 Chicago rule's session dates, ISO text.

        Returns:
            The dates to keep, ISO text, one per row; null for a row the schedules cannot
            place, whose rule date may be wrong.
        """
        ...


@dataclass(frozen=True, slots=True)
class Converted:
    """One channel's rows of one flush, converted for one dataset by `ChannelPlan.convert`.

    Attributes:
        batch: The rows in the dataset's schema, without the rows dropped.
        drift: The drift of the rows' timestamps: the rows dropped because their time column
            fails the timestamp routine (`dropped_rows`, with the first one's reason), and
            the wire keys of the other epoch fields that held a value it fails, read as null
            (`unconvertible`, with the first such value).
        unreadable: Under the session's `unreadable_symbols=RAISE`, the failure of the rows
            whose symbol is a spelling the library cannot read, which were dropped: it fails
            every stream of the dataset on the channel, since none can tell whose row it was;
            `""` when there was none.
        symbols: The batch's distinct canonical symbols, so a stream that receives none of
            them, or all of them, needs no filter.
        null_symbols: True when some row of the batch has no valid symbol (a null `Symbol`).
        dropped_symbols: How many rows of each canonical symbol were dropped because their
            time column fails the timestamp routine; a row with no valid symbol is not
            counted here.
    """

    batch: pa.Table
    drift: DriftReport
    unreadable: str = ""
    symbols: frozenset[str] = frozenset()
    null_symbols: bool = False
    dropped_symbols: Mapping[str, int] = dataclasses.field(default_factory=dict)


STALE_SYMBOLS_SHOWN: Final = 3
"""How many stale symbols a `dm.ALL` stream's `Error` names before it only counts the rest."""


def _stale_text(stale: Mapping[str, str]) -> str:
    """Word a `dm.ALL` stream's warning about the symbols whose rows are being dropped.

    Args:
        stale: Each such symbol, in the order found, with the first dropped row's reason.

    Returns:
        `"rows for ESH5 are being dropped (<reason>); its last values may be stale"`, or for
        several the first `STALE_SYMBOLS_SHOWN` and how many more; `""` for none.
    """
    if not stale:
        return ""
    symbols = list(stale)
    shown = ", ".join(symbols[:STALE_SYMBOLS_SHOWN])
    more = len(symbols) - STALE_SYMBOLS_SHOWN
    if more > 0:
        shown += f" and {more} more"
    whose = "its" if len(symbols) == 1 else "their"
    return (
        f"rows for {shown} are being dropped ({stale[symbols[0]]}); {whose} last values may "
        "be stale"
    )


_K = TypeVar("_K")
"""The key of a run of dropped rows: a channel plan, or a symbol."""


def _extend_run(
    runs: dict[_K, tuple[int, str]], key: _K, *, dropped: int, reason: str
) -> tuple[int, str] | None:
    """Add dropped rows to a run of rows dropped in a row; end the run at `DROPPED_RUN_LIMIT`.

    A dataset's run on a channel and each symbol's run are counted the same way: the rows
    dropped with none published, and the first one's reason, which the failure names.

    Args:
        runs: The runs, by key; the caller removes a run when a row of its key is published.
        key: The run's key.
        dropped: The rows dropped now.
        reason: Why the first of them was dropped.

    Returns:
        The run's length and first reason once it reaches `DROPPED_RUN_LIMIT`, when it is
        removed so that the next one starts over; None while it is shorter.
    """
    count, first = runs.get(key, (0, ""))
    count += dropped
    first = first or reason
    if count < DROPPED_RUN_LIMIT:
        runs[key] = (count, first)
        return None
    runs.pop(key, None)
    return count, first


def _counted_symbols(symbols: pa.Array, rows: pa.Array | None) -> dict[str, int]:
    """Count the rows of each symbol a mask selects.

    Args:
        symbols: The rows' canonical symbols, null where a row has none that is valid.
        rows: The mask of the rows to count, or None for none.

    Returns:
        Each symbol's count of selected rows; a null symbol is left out. Only the selected
        rows' distinct symbols reach Python.
    """
    if rows is None:
        return {}
    counts = pc.value_counts(pc.drop_null(pc.filter(symbols, rows)))
    return dict(
        zip(counts.field("values").to_pylist(), counts.field("counts").to_pylist(), strict=True)
    )


class SharedBatch:
    """One flush's converted rows of a channel for one dataset, shared by its streams.

    The flush thread creates one per dataset and channel in each flush, after converting the
    rows, and hands it to every stream of the dataset on the channel (`StreamSink.publish`);
    it is dropped when the flush ends. Each stream takes the whole table, nothing, the rows of
    one symbol (`rows_of`), or a filter of the table. When the flush counted enough streams
    that take one symbol each (`splits`), their rows come from a partition of the table by
    `Symbol`, built the first time a stream asks for them and reused by every later stream of
    the flush, so its cost is paid once per flush however many streams read it; otherwise such
    a stream filters. Used by the one flush thread that created it, so it needs no lock.

    Attributes:
        table: The static table of the batch's rows, in the dataset's schema.
        converted: The batch's conversion, for its distinct symbols.
        splits: Whether a stream that takes one symbol reads it from the shared partition
            (`rows_of`) rather than filtering the table.
    """

    __slots__ = ("_partition", "converted", "splits", "table")

    def __init__(self, table: Table, converted: Converted, *, splits: bool) -> None:
        """Wrap a flush's batch; nothing is computed until a stream asks for one symbol's rows.

        Args:
            table: The static table of the batch's rows (`columns.dataset_table`).
            converted: The batch's conversion (`ChannelPlan.convert`).
            splits: Whether streams that take one symbol share a partition of the table: the
                flush passes True when the batch holds at least `SPLIT_MIN_ROWS` rows and at
                least `SPLIT_MIN_STREAMS` of its streams take one symbol each.
        """
        self.table: Final = table
        self.converted: Final = converted
        self.splits: Final = splits
        self._partition: PartitionedTable | None = None

    def rows_of(self, symbol: str) -> Table:
        """Return the batch's rows of one symbol, in arrival order.

        The first call partitions the table by `Symbol` (a static partition, which takes no
        update-graph lock); every call then looks the symbol up in it.

        Args:
            symbol: One of the batch's canonical symbols (`Converted.symbols`).

        Returns:
            A static table of the rows whose `Symbol` is `symbol`, with every column.
        """
        if self._partition is None:
            self._partition = self.table.partition_by(_SYMBOL)
        return cast("Table", self._partition.get_constituent(symbol))


@dataclass(frozen=True, eq=False, slots=True)
class ChannelPlan:
    """How one dataset reads the rows of one WebSocket channel.

    Built once per (dataset, channel) by `build` from the committed generated files, immutable,
    and shared by every stream of the pair on every thread. `convert` runs on the flush thread.

    Attributes:
        dataset: The dataset name, such as `"stocks.trades"`.
        asset_class: The dataset's class.
        symbols: How the wire's symbols become canonical (`WireSymbols`), and what a spelling
            the library cannot read does.
        channel: The channel, such as `"T"`.
        wire_schema: The Arrow schema of the decoded rows: one field per column of the
            channel's source that the dataset keeps, named by its long-form column.
        wire_positions: For each field of `wire_schema`, the position of its value in a decoded
            row: its index in the source's fields.
        columns: Per dataset column, in order: its type, its name, the long-form column of the
            rows that fills it (None for a column the channel does not carry, which is null), and
            the unit of an epoch column (None for any other).
        arrow_schema: The dataset's Arrow schema.
        date_columns: The dataset's calendar-date columns, stored one shared string per date
            (`columns.dataset_table`).
        symbol_field: The long-form column of the rows that holds the raw symbol; None for a
            stream whose rows name no symbol (`DatasetSpec.names_symbols` False), whose
            batches have no `Symbol` and go whole to its streams.
        required: The long-form columns a row must carry: the dataset's keys for the channel's
            source, excluding keys filled from a request (`set_required`).
        times: How the rows' time columns are filled: the dataset's row times, its class,
            and for bars the channel's timespan (`row_times.TimeRule`).
        time_name: The dataset's time column (`Timestamp`, or a bar's `TimestampStart`), whose
            epoch `0` is kept, and so fails the timestamp bound, rather than read as absent.
        wire_keys: The wire key of each column the channel carries, by dataset column, for the
            drift of a value the timestamp routine fails or the widening rounds.
    """

    dataset: str
    asset_class: AssetClass
    symbols: WireSymbols
    channel: str
    wire_schema: pa.Schema
    wire_positions: tuple[int, ...]
    columns: tuple[tuple[ColumnType, str, str | None, TimeUnit | None], ...]
    arrow_schema: pa.Schema
    date_columns: tuple[str, ...]
    symbol_field: str | None
    required: frozenset[str]
    times: TimeRule
    time_name: str
    wire_keys: Mapping[str, str]

    @classmethod
    def build(
        cls,
        catalog: Catalog,
        spec: DatasetSpec,
        channel: str,
        *,
        unreadable_symbols: OnError,
    ) -> ChannelPlan:
        """Prepare the reading of a channel's rows for a dataset.

        Args:
            catalog: The catalog, for the generated source and the dataset's merged schema.
            spec: A WebSocket dataset (with channels, so with an asset class).
            channel: One of its channels.
            unreadable_symbols: The session's setting for a symbol spelling the library cannot
                read; `LOG` and `IGNORE` also add the `RawSymbol` column to the schema.

        Returns:
            The plan.

        Raises:
            ValueError: If a dataset whose rows name their symbol declares no symbol key for
                the channel's source: a stream routes such rows by symbol, so it cannot read
                the channel.

        An unknown source raises the `KeyError` of `Catalog.source`, and a bad key declaration
        the `ValueError` of `source_columns.rename`.
        """
        asset_class = cast("AssetClass", spec.asset_class)
        key = f"{asset_class.value}/{channel}"
        source = catalog.source(Transport.WS, key)
        renamed = rename(spec, source)
        symbol = spec.keys_for(Transport.WS, key).symbol
        if spec.names_symbols and (symbol is None or symbol.from_request):
            raise ValueError(f"{spec.name}: the WebSocket source {key} declares no symbol key")
        # A WebSocket source has no key filled from a request: every column has its field.
        fields = {c.column: (c, f) for c in renamed if (f := c.field) is not None}
        # A decoded row holds its values in the order of the source's fields.
        position = {f.wire_key: i for i, f in enumerate(source.fields)}
        schema = catalog.dataset_schema(spec, raw_symbol=unreadable_symbols is not OnError.RAISE)
        symbol_field = None if symbol is None else fields[key_column(spec, "symbol", symbol)][1]
        # A bars channel serves one timespan, always of one unit; any other channel has none.
        span = next(span for span, name in spec.channels.items() if name == channel)
        timespan = None if span is NotBars.ANY else span
        return cls(
            dataset=spec.name,
            asset_class=asset_class,
            symbols=WireSymbols(
                spec.name, asset_class, unreadable_symbols, spec.symbols_as_written
            ),
            channel=channel,
            wire_schema=pa.schema(
                [pa.field(f.column, _WIRE_ARROW[c.wire_type]) for c, f in fields.values()]
            ),
            wire_positions=tuple(position[f.wire_key] for _, f in fields.values()),
            columns=tuple(
                (c.type, c.name, None, None)
                if c.name not in fields
                else (c.type, c.name, fields[c.name][1].column, fields[c.name][0].unit)
                for c in schema.columns
            ),
            arrow_schema=schema.arrow_schema(),
            date_columns=schema.date_columns,
            symbol_field=None if symbol_field is None else symbol_field.column,
            required=frozenset(f.column for f in required_fields(spec, source)),
            times=TimeRule(spec.name, spec.times, asset_class, timespan),
            time_name=time_column(spec.times),
            wire_keys={name: field.wire_key for name, (_, field) in fields.items()},
        )

    def convert(
        self,
        rows: Sequence[Sequence[object]],
        symbols: dict[str, str | None],
        *,
        arrived: Sequence[int],
        sessions: FuturesSessionDates | None = None,
    ) -> Converted:
        """Turn one flush's rows of the channel into a batch of the dataset's schema.

        Pure Arrow apart from the symbol map: the rows become one Arrow table at once (the
        rows turned into columns in one call, each column converted to Arrow in one call), each
        distinct raw symbol is read once (`WireSymbols.read`, remembered in `symbols`), every
        epoch column runs through the masked timestamp routine, every column is widened to its
        dataset type (a whole number the widening rounds is reported as drift,
        `RoundedFields`), the derived columns (`OptionRoot`, `RawSymbol`) are computed, and the
        library's time columns are filled (`row_times.TimeRule.fill`). A row whose time column
        overflows or lies outside 1990 to 2100 is dropped and counted; any other timestamp that
        does is null and reported as drift. A row whose symbol is a spelling the library cannot
        read is dropped and reported too, failing every stream of the dataset on the channel
        (it cannot be routed to one), when the session's `unreadable_symbols` is `RAISE`; under
        `LOG` and `IGNORE` it is kept with a null `Symbol` and the text in `RawSymbol`.

        Args:
            rows: The decoded rows, each a tuple of the values of the source's fields, in the
                order of `Source.fields` (`FrameDecoder` in the wire package).
            symbols: The caller's map from raw to canonical symbol (None for an unreadable
                spelling), read and extended here; one per flush thread and plan, so it needs
                no lock.
            arrived: When each row's frame arrived, epoch nanoseconds, one per row: the rows'
                `TimestampInsert`.
            sessions: For a futures channel, the schedule calendar that corrects the rows'
                session dates; None keeps the 17:00 Chicago rule's.

        Returns:
            The batch without the dropped rows, the drift of its timestamps, the failure of
            its unreadable symbols, and the batch's distinct symbols.
        """
        values = list(zip(*rows, strict=True)) if rows else None  # one tuple per source field
        wire = pa.Table.from_arrays(
            [
                pa.array(() if values is None else values[position], type=arrow_type)
                for position, arrow_type in zip(
                    self.wire_positions, self.wire_schema.types, strict=True
                )
            ],
            schema=self.wire_schema,
        )
        count = wire.num_rows
        dropped: pa.Array | None = None
        late: pa.Array | None = None  # the rows whose time column fails the routine
        unreadable = ""
        found: list[DriftReport] = []
        arrays: dict[str, pa.Array] = {}
        raw = pa.nulls(count, pa.string())
        rounded = RoundedFields()
        for column_type, name, field, unit in self.columns:
            bad = None
            if name == OPTION_ROOT:
                values = option_roots(arrays[_SYMBOL])
            elif name == RAW_SYMBOL:
                values = raw
            elif field is None:
                values = pa.nulls(count, column_type.arrow_type)
            elif name == _SYMBOL:
                values, raw, bad, unreadable = self._read_symbols(wire.column(field), symbols)
            elif unit is not None:
                values, bad = self._instants(name, unit, wire.column(field), found=found)
                late = bad if bad is not None else late
            else:
                values = wire.column(field).combine_chunks()
            if bad is not None:
                dropped = bad if dropped is None else pc.call_function("or", [dropped, bad])
            arrays[name] = rounded.widen(
                values, column_type.arrow_type, wire_key=self.wire_keys.get(name)
            )
        found.append(rounded.drift())
        batch = pa.Table.from_arrays(list(arrays.values()), schema=self.arrow_schema)
        batch = self.times.fill(
            batch,
            received=pa.array(arrived, pa.int64()),
            session_dates=None if sessions is None else sessions.correct,
        )
        if dropped is not None:
            batch = batch.filter(pc.call_function("invert", [dropped]))
        if self.symbol_field is None:  # rows that name no symbol go whole to every stream
            drift = functools.reduce(DriftReport.merge, found, DriftReport())
            return Converted(batch, drift, unreadable)
        distinct = pc.call_function("unique", [batch.column(_SYMBOL)]).to_pylist()
        return Converted(
            batch,
            functools.reduce(DriftReport.merge, found, DriftReport()),
            unreadable,
            symbols=frozenset(s for s in distinct if s is not None),
            null_symbols=None in distinct,
            dropped_symbols=_counted_symbols(arrays[_SYMBOL], late),
        )

    def _read_symbols(
        self, column: pa.ChunkedArray, symbols: dict[str, str | None]
    ) -> tuple[pa.Array, pa.Array, pa.Array | None, str]:
        """Read the rows' wire symbols; under `RAISE`, mark the rows of unreadable ones.

        Args:
            column: The rows' symbols as sent.
            symbols: The flush thread's raw-to-canonical map (`convert`).

        Returns:
            The canonical symbols, the `RawSymbol` values, the mask of the rows to drop (None
            when no row is dropped), and the failure of those rows, which fails every stream
            of the dataset on the channel (`""` when none is dropped). Rows are dropped only
            when the session's `unreadable_symbols` is `RAISE`.
        """
        read = self.symbols.read(column.combine_chunks(), symbols)
        if not read.unreadable or self.symbols.keeps:
            return read.symbols, read.raw, None, ""
        bad = pc.call_function("is_valid", [read.raw])
        message = unreadable_symbol_message(self.dataset, read.unreadable)
        return read.symbols, read.raw, bad, message

    def _instants(
        self, name: str, unit: TimeUnit, column: pa.ChunkedArray, *, found: list[DriftReport]
    ) -> tuple[pa.Array, pa.Array | None]:
        """Convert an epoch column, marking the rows whose time column fails the routine.

        Args:
            name: The column.
            unit: The epoch's unit.
            column: The rows' raw values.
            found: Extended with the drift of a value that fails the routine: in the time
                column, the rows dropped and the first failure's message
                (`timestamps.mismatch_message`); in any other, the column's wire key and its
                first failing value, which reads as null.

        Returns:
            The values as nanoseconds, null where a value fails; and for the time column the
            mask of the rows to drop, or None when no row is dropped.
        """
        sent = column.combine_chunks()
        values, failed = masked_instants(
            unit, sent, time_column=name == self.time_name, label=f"{self.dataset}.{name}"
        )
        first = first_failure(sent, failed)
        if first is None:
            return values, None
        if name == self.time_name:
            reason = mismatch_message(f"{self.dataset}.{name}", unit, first)
            count = pc.call_function("sum", [failed]).as_py()
            found.append(DriftReport(dropped_rows=count, dropped_reason=reason))
            return values, failed
        key = self.wire_keys[name]
        found.append(
            DriftReport(
                unconvertible=frozenset({key}), samples={key: DriftSample(None, str(first))}
            )
        )
        return values, None


class StreamSink:
    """One stream as its connection's flush thread publishes to it.

    Created by `open_stream_tables` on the caller thread and added to the connection's routing
    table by the transport; removed when the stream's blink table has shut down, or when its
    session closes. It refers to its session's core weakly (`core()`): a flush thread shared by
    several sessions needs to know which one each stream reports to, but a sink can outlive its
    stream's record in a container that is cleaned up later (the routes of a connection, until
    its flush thread ends), and a strong reference there
    would keep a finished session alive. While the stream is registered its record keeps the
    session in the live set, so the core is there whenever the stream still matters; once it is
    gone, the sink skips what it would have reported to it. Two locks keep it consistent
    whichever thread reaches it:

    - its leaf lock guards the symbol set, which the connection's loop replaces (`set_symbols`)
      when a reactive symbol table changes;
    - its publish lock serializes `publish`, `publish_held`, and `fail`, which call Deephaven.
      Usually only the flush thread takes it, but a stream is failed from the caller thread
      when its connection failed while it was opening, from the closing thread at
      `m.close()`, and from the reaper when its watchlist failed; and the verb that opened a
      stream adds its held rows from its own thread. The order is the flush lock (on a flush
      thread), then the publish lock, then `registry_lock` (`SessionCore.fail_stream`, which
      `fail` calls); the publish lock is never taken under `registry_lock`. A caller that holds
      the update-graph lock (a console command) takes it to fail the stream or to add its held
      rows, which is safe because nothing done under it takes the update-graph lock or waits
      for an update cycle:
      `partition_by` and `where_in` of static tables, `publisher.add` of a static table,
      `publish_failure`, and `SessionCore.fail_stream`.

    A failed stream publishes nothing more, and is failed once, so a failure queued behind the
    session's close never replaces the close.

    A new sink holds the rows routed to it until its stream's first result is built
    (`publish_held`): the stream subscribes before the verb builds that result, and a row the
    blink table delivered in between would never reach an aggregation such as `last_by`, which
    only sees rows that arrive after it exists. The held tables are static and few (only those
    of the open), and `publish_held` adds them in order before any later row.

    Attributes:
        stream_id: The stream's id.
        plan: How the stream's dataset reads its channel.
        publisher: The stream's publisher.
        rows_published: The rows published to it so far, held ones included.
        rows_generation: The generation of the socket its newest rows arrived on
            (`Flusher.rows`), 0 before its first row. Only the flush thread writes it; the
            connection's loop reads it to tell rows of a reconnected socket from rows of the
            dropped one that a slow flush published late.
    """

    __slots__ = (
        "__weakref__",
        "_core",
        "_filter",
        "_filter_version",
        "_held",
        "_lock",
        "_publish_lock",
        "_symbols",
        "_version",
        "failed",
        "plan",
        "publisher",
        "rows_generation",
        "rows_published",
        "stream_id",
    )

    def __init__(
        self,
        stream_id: int,
        *,
        core: SessionCore,
        plan: ChannelPlan,
        publisher: BlinkPublisher,
        symbols: frozenset[str] | None,
    ) -> None:
        """Create the sink.

        Args:
            stream_id: The stream's id.
            core: The stream's session, which records its rows and its failure.
            plan: How the stream's dataset reads its channel.
            publisher: The stream's publisher.
            symbols: The canonical symbols the stream receives; None for `dm.ALL`, which
                receives every row of the channel.
        """
        self.stream_id: Final = stream_id
        self._core: Final = weakref.ref(core)
        self.plan: Final = plan
        self.publisher: Final = publisher
        self._lock = threading.Lock()
        self._symbols = symbols
        self._version = 0
        # Publish-lock state.
        self._publish_lock = threading.Lock()
        self._filter: Table | None = None
        self._filter_version = -1
        # The rows routed before the first result was built; None once `publish_held` ran.
        self._held: list[Table] | None = []
        self.rows_published = 0
        # Written only by the flush thread (`Flusher._publish_batch`).
        self.rows_generation = 0
        self.failed = False

    def core(self) -> SessionCore | None:
        """Return the stream's session core, or None once it has been freed.

        Returns:
            The core.
        """
        return self._core()

    def set_symbols(self, symbols: frozenset[str]) -> None:
        """Replace the symbols a reactive stream receives. Any thread; never blocks.

        Args:
            symbols: The canonical symbols the stream's watchlist now holds.
        """
        with self._lock:
            self._symbols = symbols
            self._version += 1

    def publish(self, batch: SharedBatch) -> int:
        """Add the part of a channel's batch this stream receives.

        No Python work per row: a `dm.ALL` stream gets the whole batch; a symbol stream gets
        nothing when it holds none of the batch's symbols, the whole batch when it holds all of
        them (the common single-stream case), the rows of the one batch symbol it holds from the
        batch's shared partition (`SharedBatch.rows_of`) when the batch is split, and otherwise
        a `where_in` against its cached filter table, rebuilt only when its symbols changed.

        Runs under the publish lock; the class docstring says why that is safe for a caller
        that holds the update graph's lock. Until the stream's first result is built, the rows
        are held rather than added (`publish_held`).

        Args:
            batch: The channel's shared batch for the stream's dataset.

        Returns:
            The rows added or held; 0 once the stream has failed.
        """
        with self._publish_lock:
            if self.failed:
                return 0
            selected = self._select(batch)
            added = 0 if selected is None else selected.size
            if selected is not None and added:
                if self._held is None:
                    self.publisher.add(selected)
                else:
                    self._held.append(selected)
                self.rows_published += added
            return added

    def publish_held(self) -> None:
        """Add the rows held since the stream opened, and every later row as it comes.

        The verb that opened the stream calls it on its own thread right after it built the
        stream's first result, so every row the stream received reaches the blink table in an
        update cycle after that result exists. Idempotent; a failed stream drops what it held.
        Takes the publish lock and calls Deephaven (`publisher.add` of static tables), so never
        call it under `registry_lock` or on an update-graph thread; a caller that holds the
        update graph's lock may (the class docstring says why). The thread needs an execution
        context.
        """
        with self._publish_lock:
            held = self._held
            if held and not self.failed:
                logger.debug(
                    "%s stream %d: its first result is built; adding the %d rows it received "
                    "while it opened",
                    self.plan.dataset,
                    self.stream_id,
                    sum(table.size for table in held),
                )
                # One table at a time: an interrupt leaves the rest held for the next call.
                while held:
                    self.publisher.add(held.pop(0))
            self._held = None

    @property
    def receives_all(self) -> bool:
        """True for a `dm.ALL` stream, which receives every symbol of its channel."""
        with self._lock:
            return self._symbols is None

    def holds(self, symbol: str) -> bool:
        """Tell whether the stream names `symbol` among its symbols. Any thread; never blocks.

        Args:
            symbol: A canonical symbol.

        Returns:
            True for a symbol stream that holds it; False for `dm.ALL`, which names none.
        """
        with self._lock:
            symbols = self._symbols
        return symbols is not None and symbol in symbols

    def takes_one_symbol(self, converted: Converted) -> bool:
        """Tell whether the stream would take one symbol's rows of a batch, and no other rows.

        The flush counts these streams to choose between a shared partition of the batch and a
        filter per stream (`SPLIT_MIN_STREAMS`). Any thread; never blocks.

        Args:
            converted: The batch's conversion, for its distinct symbols.

        Returns:
            True for a symbol stream that holds exactly one of the batch's symbols, unless that
            symbol is the batch's only one and every row has it, which takes the whole batch.
        """
        with self._lock:
            symbols = self._symbols
        if symbols is None or len(symbols & converted.symbols) != 1:
            return False
        return len(converted.symbols) > 1 or converted.null_symbols

    def _select(self, batch: SharedBatch) -> Table | None:
        """Choose the rows of a batch the stream receives; the publish lock is held.

        Args:
            batch: The channel's shared batch.

        Returns:
            The rows, or None when the stream receives none.
        """
        with self._lock:
            symbols, version = self._symbols, self._version
        if symbols is None:
            return batch.table
        converted = batch.converted
        held = symbols & converted.symbols
        if not held:
            return None
        if len(held) == len(converted.symbols) and not converted.null_symbols:
            return batch.table
        if len(held) == 1 and batch.splits:
            (symbol,) = held
            return batch.rows_of(symbol)
        if version != self._filter_version:
            self._filter = new_table([string_col(_SYMBOL, sorted(symbols))])
            self._filter_version = version
        return batch.table.where_in(cast("Table", self._filter), _SYMBOL)

    def fail(self, error: MassiveError) -> bool:
        """Fail the stream's table and record the failure, once.

        The failure is recorded first (`SessionCore.fail_stream`) and the table failed after
        (`publish_failure`), so a verb that finds the table failed always finds the failure
        recorded too, and names it. When the session's close came first, the table fails with
        the close's error instead, so the table and its `CLOSED` row name the same cause, and
        nothing is logged (the close logs its count). Any other failure is logged once per
        failure, however many streams it fails, at the level its cause gives (`log_failure`):
        ERROR for a defect, naming whose it is, and WARNING otherwise.
        The stream counts as failed even when `publish_failure` raises (the exception then
        propagates): its row already reads `FAILED`, so it publishes no more rows, and the
        failure is not tried again. Never call it on an update-graph thread or under
        `registry_lock`; the thread needs an execution context.

        Args:
            error: The error the table fails with.

        Returns:
            False when the stream had already failed, so nothing was done.
        """
        with self._publish_lock:
            if self.failed:
                return False
            core = self._core()
            closed = None if core is None else core.fail_stream(self.stream_id, str(error))
            if closed is None:
                log_failure(
                    logger, error, "%s stream %d failed: %s", self.plan.dataset, self.stream_id
                )
            try:
                self.publisher.publish_failure(
                    error if closed in (None, str(error)) else MassiveError(closed)
                )
            finally:  # the row reads FAILED: the stream publishes nothing more either way
                self.failed = True
            return True

    def fail_later(self, message: str) -> None:
        """Queue the stream's failure on its session's reaper: its watchlist cannot be followed.

        The watchlist's listener reports a row that is not canonical, or the failure of the
        watchlist itself, on an update-graph thread, where failing a table (a Deephaven call
        that takes the publish lock) is not allowed, so the failure is only queued here; it
        never blocks. The reaper, unlike a connection's flush thread, runs as long as the
        session.

        Args:
            message: Why the stream cannot follow its watchlist, naming the fix.
        """
        failure = MassiveError(message, cause=ErrorCause.USER_DEFECT)  # the symbol table's

        def fail() -> None:
            self.fail(failure)

        core = self._core()
        if core is not None:  # a freed session has no stream left to fail
            core.defer(fail)


_Frame = tuple[int, int, list[tuple[object, ...]]]
"""One frame's rows of one channel, with the instant it arrived (epoch nanoseconds) and the
generation of the socket it arrived on."""


class _Routes:
    """The streams of one connection by channel, and the row buffer of each channel.

    Shared by the caller thread (a stream is added), the connection's loop thread (a stream is
    removed; rows are buffered), and the flush thread (it reads). Guarded by one leaf lock, held
    only to copy or replace a reference: the routes are an immutable mapping replaced on every
    change, so a reader works on a snapshot without the lock.
    """

    __slots__ = ("_buffers", "_lock", "_routes")

    def __init__(self) -> None:
        """Start with no stream and no buffer."""
        self._lock = threading.Lock()
        self._routes: Mapping[str, tuple[StreamSink, ...]] = {}
        self._buffers: Mapping[str, queue.SimpleQueue[_Frame]] = {}

    def add(self, channel: str, sink: StreamSink) -> None:
        """Route a channel's rows to a stream, creating the channel's buffer if needed.

        Args:
            channel: The stream's channel.
            sink: The stream.
        """
        with self._lock:
            routes = dict(self._routes)
            routes[channel] = (*routes.get(channel, ()), sink)
            self._routes = routes
            if channel not in self._buffers:
                self._buffers = {**self._buffers, channel: queue.SimpleQueue()}

    def remove(self, stream_id: int) -> StreamSink | None:
        """Stop routing rows to a stream.

        Args:
            stream_id: The stream.

        Returns:
            The removed stream, or None if it was not routed.
        """
        with self._lock:
            found = None
            routes: dict[str, tuple[StreamSink, ...]] = {}
            for channel, sinks in self._routes.items():
                kept = tuple(s for s in sinks if s.stream_id != stream_id)
                found = found or next((s for s in sinks if s.stream_id == stream_id), None)
                if kept:
                    routes[channel] = kept
            self._routes = routes
        return found

    def clear(self) -> None:
        """Stop routing rows to any stream: the flush thread has ended and reads no more."""
        with self._lock:
            self._routes = {}

    def sinks(self) -> tuple[StreamSink, ...]:
        """Return every routed stream.

        Returns:
            The streams, by channel and then in the order they were added.
        """
        with self._lock:
            routes = self._routes
        return tuple(sink for sinks in routes.values() for sink in sinks)

    def current(self) -> Mapping[str, tuple[StreamSink, ...]]:
        """Return the current routes, to read without the lock.

        Returns:
            The streams by channel.
        """
        with self._lock:
            return self._routes

    def buffers(self) -> tuple[tuple[str, queue.SimpleQueue[_Frame]], ...]:
        """Return the row buffer of each channel that has ever been routed.

        Returns:
            `(channel, buffer)` pairs.
        """
        with self._lock:
            return tuple(self._buffers.items())

    def buffer(self, channel: str) -> queue.SimpleQueue[_Frame] | None:
        """Return the row buffer of a channel.

        Args:
            channel: The channel.

        Returns:
            The buffer, or None when no stream was ever routed to the channel.
        """
        with self._lock:
            return self._buffers.get(channel)


@dataclass(frozen=True, slots=True)
class _Fail:
    """A request to fail streams, from the connection's loop thread.

    Attributes:
        stream_ids: The streams to fail.
        error: The error their tables fail with.
    """

    stream_ids: tuple[int, ...]
    error: MassiveError


@dataclass(frozen=True, slots=True)
class _Batch:
    """One channel's rows of one flush, and the rows the client dropped from its frames.

    Attributes:
        rows: The decoded rows, in arrival order, each a tuple of the source's fields.
        arrived: When each row's frame arrived, epoch nanoseconds, one per row.
        lost: The rows the client dropped from the channel's frames since the last flush,
            because a key could not be read, as the frames' drift merged (`dropped_rows`).
        lost_symbols: How many of those rows each canonical symbol had.
        generation: The generation of the socket every row arrived on (`Flusher.rows`); a
            flush whose frames span two sockets publishes one batch per socket. 0 for a batch
            of no rows.
    """

    rows: list[tuple[object, ...]]
    arrived: list[int]
    lost: DriftReport
    lost_symbols: Mapping[str, int]
    generation: int


@dataclass(frozen=True, slots=True)
class _Drift:
    """A channel's drift in one frame, from the connection's loop thread.

    Attributes:
        channel: The channel.
        drift: The frame's drift for the channel.
        where: The connection's URL and the channel, for the drift WARNING.
        dropped: How many of the frame's dropped rows each canonical symbol had, for the
            symbol's run of dropped rows (`DROPPED_RUN_LIMIT`).
    """

    channel: str
    drift: DriftReport
    where: str
    dropped: Mapping[str, int]


def _do_nothing() -> None:
    """Do nothing: the default `on_new_generation` of a `Flusher`."""


class Flusher:
    """The flush thread of one connection: drains its row buffers and publishes to its streams.

    Created by the connection; `start()` starts the daemon thread, which flushes every
    `interval` seconds until `stop()`. `flush()` may also be called directly (the tests do):
    flushes are serialized by the flush lock. The producers, the connection's loop thread and
    the caller thread, only put plain data on queues (`rows`, `fail`, `drift`) or change the
    routes, and never wait.

    The flusher holds no session core: the sinks it routes carry their own, and it reports each
    stream to that one. Each flush enters the execution context it was created with through a
    fresh wrapper. An exception in a flush is logged at ERROR and the thread keeps flushing;
    anything the thread cannot carry on after (a `KeyboardInterrupt` or `SystemExit`) first
    fails every routed stream, so no table is left frozen without an error.

    It counts the rows buffered and not taken by a flush yet (`backlog`), under a leaf lock of its
    own, and logs a WARNING when the count passes `BACKLOG_WARNING_ROWS`. The buffers are
    unbounded on purpose: a slow flush delays rows but never drops them.

    It remembers the canonical symbol of each wire spelling it has read, per dataset and channel,
    so steady traffic costs no symbol reading. That memory is bounded: a flush forgets the
    datasets no longer routed, and empties a dataset's memory once it passes
    `SYMBOL_CACHE_LIMIT` spellings. A spelling the library cannot read is still warned about
    once, whatever is forgotten.

    Attributes:
        routes: The connection's streams by channel, and its row buffers.
    """

    __slots__ = (
        "_backlog",
        "_backlog_lock",
        "_backlog_warned",
        "_control",
        "_drift",
        "_exec_ctx",
        "_final",
        "_flush_lock",
        "_interval",
        "_new_generation",
        "_noted",
        "_on_new_generation",
        "_runs",
        "_sessions",
        "_stale",
        "_stop",
        "_symbol_runs",
        "_symbols",
        "_thread",
        "routes",
    )

    def __init__(
        self,
        exec_ctx: ExecutionContext,
        name: str,
        *,
        interval: float,
        sessions: FuturesSessionDates | None = None,
        on_new_generation: Callable[[], object] = _do_nothing,
    ) -> None:
        """Create the flusher; nothing runs until `start()`.

        Args:
            exec_ctx: The execution context every flush enters: that of the session which
                created the connection.
            name: The thread's name, for thread dumps.
            interval: Seconds between flushes (`MassiveConfig.flush_interval`).
            sessions: For a futures connection, the schedule calendar that corrects its rows'
                session dates; None otherwise.
            on_new_generation: Called after a flush in which a stream got its first rows of a
                newer socket generation (`StreamSink.rows_generation`), on the flush thread
                with no lock held; it must not block. The connection posts its recovery check
                to its loop with it, so a stream an outage held down recovers as soon as rows
                of the new socket reach it. Does nothing by default.
        """
        self._exec_ctx = exec_ctx
        self._on_new_generation = on_new_generation
        self._interval = interval
        self._sessions = sessions
        self.routes: Final = _Routes()
        self._control: queue.SimpleQueue[_Fail | _Drift] = queue.SimpleQueue()
        self._flush_lock = threading.Lock()
        self._stop = threading.Event()
        self._final = threading.Event()
        # Flush-lock state: the raw-to-canonical symbol map of each routed plan, at most
        # `SYMBOL_CACHE_LIMIT` spellings each after a flush.
        self._symbols: dict[ChannelPlan, dict[str, str | None]] = {}
        # Flush-lock state: per `dm.ALL` stream, the symbols whose rows are being dropped, with
        # the first reason, which its `Error` names; and the stale-symbol part last written.
        self._stale: dict[int, dict[str, str]] = {}
        self._noted: dict[int, str] = {}
        # Flush-lock state: per routed plan, the rows dropped in a row with none published,
        # and the first one's reason (`DROPPED_RUN_LIMIT`).
        self._runs: dict[ChannelPlan, tuple[int, str]] = {}
        # Flush-lock state: per routed plan and symbol, the same run (`_counted_symbols`).
        self._symbol_runs: dict[ChannelPlan, dict[str, tuple[int, str]]] = {}
        # Flush-lock state: the drift a flush found, merged per (streams, where), which the
        # flush reports once it has released the flush lock (`_report_found`).
        self._drift: dict[tuple[tuple[StreamSink, ...], str], DriftReport] = {}
        # Flush-lock state: whether this flush gave a stream rows of a newer socket generation.
        self._new_generation = False
        # Guarded by the backlog lock, a leaf lock.
        self._backlog_lock = threading.Lock()
        self._backlog = 0
        self._backlog_warned = False
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)

    @property
    def backlog(self) -> int:
        """Rows buffered and not taken by a flush yet: received, and waiting for the next one."""
        with self._backlog_lock:
            return self._backlog

    def start(self) -> None:
        """Start the flush thread. Call it once."""
        self._thread.start()

    def stop(self, *, final: bool = False) -> None:
        """Ask the flush thread to end after its current flush. Never waits; idempotent.

        Args:
            final: True to have the thread flush once more before it ends, so the failures
                queued last (a connection's fatal failure fails every stream) are applied.
                False when the session closes: what is still buffered is not published, and
                no stream is failed here, since the close has already failed its tables.
        """
        if final:
            self._final.set()
        self._stop.set()

    def join(self, timeout: float) -> bool:
        """Wait for the flush thread to end, if it was started.

        Args:
            timeout: The longest wait in seconds.

        Returns:
            Whether the thread has ended (true for a thread never started).
        """
        if self._thread.ident is not None:
            self._thread.join(timeout)
        return not self._thread.is_alive()

    def rows(self, channel: str, rows: list[tuple[object, ...]], *, generation: int) -> None:
        """Buffer one frame's rows of a channel, with the instant they arrived. Never blocks.

        The instant, `time.time_ns()` now, on the connection's loop thread as the frame is
        handed over, becomes every row's `TimestampInsert`. Rows of a channel no stream was
        ever routed to are dropped. The rows count in the backlog until a flush takes them; a
        backlog that passes `BACKLOG_WARNING_ROWS` is logged at WARNING (see that constant for
        when it is logged again).

        Args:
            channel: The channel.
            rows: The decoded rows, each a tuple of the source's fields; the flusher keeps the
                list.
            generation: The generation of the socket the frame arrived on, which each stream
                the rows reach records (`StreamSink.rows_generation`).
        """
        buffer = self.routes.buffer(channel)
        if buffer is None:
            return
        with self._backlog_lock:  # counted before the put, so a flush never counts below zero
            self._backlog += len(rows)
            backlog = self._backlog
            warn = backlog > BACKLOG_WARNING_ROWS and not self._backlog_warned
            self._backlog_warned |= warn
        buffer.put((time.time_ns(), generation, rows))
        if warn:
            logger.warning(
                "%s: %d rows are waiting to be published, more than %d; the flushes are "
                "falling behind (a stalled or slow update graph), so memory grows until they "
                "catch up",
                self._thread.name,
                backlog,
                BACKLOG_WARNING_ROWS,
            )

    def fail(self, stream_ids: Iterable[int], error: MassiveError) -> None:
        """Ask the flush thread to fail streams. Never blocks.

        Args:
            stream_ids: The streams; one already failed is skipped.
            error: The error their tables fail with, also logged (`StreamSink.fail`) and shown
                in `m.streams()`.
        """
        self._control.put(_Fail(tuple(stream_ids), error))

    def drift(
        self,
        channel: str,
        drift: DriftReport,
        *,
        where: str,
        dropped: Mapping[str, int] | None = None,
    ) -> None:
        """Ask the flush thread to report a channel's drift for the streams on it. Never blocks.

        Args:
            channel: The channel.
            drift: The frame's drift for the channel.
            where: Where the frame came from, such as `"wss://delayed.massive.com/stocks
                channel T"`, named in the drift WARNING.
            dropped: How many of the frame's dropped rows (`DriftReport.dropped_rows`) each
                canonical symbol had; None when no dropped row had a valid symbol.
        """
        self._control.put(_Drift(channel, drift, where, dict(dropped or {})))

    def _run(self) -> None:
        """Flush every interval until `stop()`; a failing flush is logged and the next one runs.

        Anything else that escapes a flush ends the thread: it is logged at ERROR as a defect
        of the library, every routed stream is failed with a `MassiveError` naming the thread
        (`ErrorCause.LIBRARY_DEFECT`, logged once), and it is re-raised.
        However the thread ends, it clears the routes last: nothing reads them any more, and a
        stream left there would be held by a connection that is gone.
        """
        try:
            while not self._stop.wait(self._interval):
                self._flush_logged()
            if self._final.is_set():
                self._flush_logged()
        except BaseException as death:
            error = MassiveError(
                library_bug(
                    f"the flush thread {self._thread.name} died ({death!r}); this table no "
                    "longer updates"
                ),
                cause=ErrorCause.LIBRARY_DEFECT,
            )
            log_failure(logger, error, "%s", exc_info=death)
            with entered(self._exec_ctx):
                self._fail_each(self.routes.sinks(), error)
            raise
        finally:
            self.routes.clear()

    def _flush_logged(self) -> None:
        """Flush once, logging a failure at ERROR instead of raising it."""
        try:
            self.flush()
        except Exception:
            logger.exception("%s: a flush failed; continuing. %s", self._thread.name, LIBRARY_BUG)

    def flush(self) -> None:
        """Apply the queued failures, publish every buffered row, then report the queued drift.

        Failures come first, so a stream that failed gets no row published after its failure
        was reported. Each step is isolated: a stream that cannot be failed, or a drift report
        that raises, is logged at ERROR, and the flush still publishes every other stream's
        rows, applies the other failures, and reports the row counts. Runs under the flush
        lock, inside the flusher's execution context, with no other library lock held: it
        calls Deephaven (`to_table`, `partition_by`, `where_in`, `add`, `publish_failure`) and
        then each session with a stream that published once (`update_streams`). The drift the
        flush found, its frames' and its rows' timestamps', is merged per channel and dataset
        (`_found`) and reported once the flush lock is released (`_report_found`), so the
        session's `DriftRegistry` logs once per flush, not once per frame. A channel's frames
        from two sockets publish as one batch per socket (`_batches`), and a flush that gave a
        stream its first rows of a newer socket calls `on_new_generation` last.
        """
        with self._flush_lock, entered(self._exec_ctx):
            controls = _drain(self._control)
            drained = {channel: _drain(buffer) for channel, buffer in self.routes.buffers()}
            # Each frame is `(arrived, generation, rows)`: the backlog counts the rows.
            self._drained(sum(len(rows) for frames in drained.values() for *_, rows in frames))
            # Routes after the rows: a stream routed meanwhile gets the rows that reached it.
            routes = self.routes.current()
            lost: dict[str, DriftReport] = {}  # the rows the client dropped, by channel
            lost_symbols: dict[str, Counter[str]] = {}  # and by symbol
            for item in controls:
                if isinstance(item, _Fail):
                    failing = [s for s in self.routes.sinks() if s.stream_id in item.stream_ids]
                    self._fail_each(failing, item.error)
                elif item.drift.dropped_rows:
                    lost[item.channel] = lost.get(item.channel, DriftReport()).merge(item.drift)
                    lost_symbols.setdefault(item.channel, Counter()).update(item.dropped)
            updates: dict[StreamSink, StreamUpdate] = {}
            for channel, frames in drained.items():
                sinks = routes.get(channel, ())
                if sinks and (frames or channel in lost):
                    for batch in _batches(frames, lost, lost_symbols, channel=channel):
                        self._publish(batch, sinks, updates=updates)
            for item in controls:
                if isinstance(item, _Drift):
                    self._found(routes.get(item.channel, ()), item.drift, where=item.where)
            _report_updates(updates)
            self._forget([sink for sinks in routes.values() for sink in sinks])
            found, self._drift = self._drift, {}
            new_generation, self._new_generation = self._new_generation, False
        self._report_found(found)
        if new_generation:
            self._on_new_generation()

    def _found(self, sinks: tuple[StreamSink, ...], drift: DriftReport, *, where: str) -> None:
        """Add drift a flush found to what it reports once the flush lock is released.

        Merged per streams and origin, so a flush of a thousand frames that each dropped a row
        reports, and logs, once rather than a thousand times.

        Args:
            sinks: The streams of the channel or dataset the drift is about.
            drift: The drift.
            where: The connection's URL and the channel, or the flush thread and the channel.
        """
        key = (sinks, where)
        self._drift[key] = self._drift.get(key, DriftReport()).merge(drift)

    def _report_found(
        self, found: Mapping[tuple[tuple[StreamSink, ...], str], DriftReport]
    ) -> None:
        """Report a flush's drift, with no lock held: its logs never wait under the flush lock.

        A report that raises is logged at ERROR, and the others are still reported.

        Args:
            found: The flush's drift, merged per streams and origin (`_found`).
        """
        for (sinks, where), drift in found.items():
            try:
                self._report_drift(sinks, drift, where=where)
            except Exception:
                logger.exception(
                    "%s: could not report the drift of %s. %s",
                    self._thread.name,
                    where,
                    LIBRARY_BUG,
                )

    def _forget(self, routed: list[StreamSink]) -> None:
        """Forget the flush-lock state of streams and datasets no longer routed.

        Args:
            routed: The streams the flush routed.
        """
        live = {sink.stream_id for sink in routed}
        if self._stale:  # the stale symbols and warnings of streams no longer routed
            self._stale = {i: m for i, m in self._stale.items() if i in live}
        if self._noted:
            self._noted = {i: m for i, m in self._noted.items() if i in live}
        plans = {sink.plan for sink in routed}
        if self._symbols:  # the spellings of datasets no longer routed
            self._symbols = {p: m for p, m in self._symbols.items() if p in plans}
        if self._runs:  # and their runs of dropped rows
            self._runs = {p: run for p, run in self._runs.items() if p in plans}
        if self._symbol_runs:
            self._symbol_runs = {p: r for p, r in self._symbol_runs.items() if p in plans}

    def _fail_each(self, sinks: Iterable[StreamSink], error: MassiveError) -> None:
        """Fail streams one by one; a stream whose failure raises costs the others nothing.

        A failure that raises is logged at ERROR with the stream's id, and the next stream is
        failed.

        Args:
            sinks: The streams to fail; one already failed is skipped by its own `fail`.
            error: The error their tables fail with.
        """
        for sink in sinks:
            try:
                sink.fail(error)
            except Exception:
                logger.exception(
                    "%s: could not fail stream %d (%s). %s",
                    self._thread.name,
                    sink.stream_id,
                    error,
                    LIBRARY_BUG,
                )

    def _drained(self, count: int) -> None:
        """Take rows a flush drained off the backlog; re-arm the WARNING below half the level.

        Args:
            count: How many rows the flush took from the buffers.
        """
        with self._backlog_lock:
            self._backlog -= count
            if self._backlog < BACKLOG_WARNING_ROWS // 2:
                self._backlog_warned = False

    def _publish(
        self,
        batch: _Batch,
        sinks: tuple[StreamSink, ...],
        *,
        updates: dict[StreamSink, StreamUpdate],
    ) -> None:
        """Convert a channel's rows once per dataset and add them to each of its streams.

        Args:
            batch: The channel's rows of this flush, and the rows the client dropped.
            sinks: The channel's streams.
            updates: Receives each stream's new published-row total and `Error` notes.
        """
        by_plan: dict[ChannelPlan, list[StreamSink]] = {}
        for sink in sinks:
            if not sink.failed:
                by_plan.setdefault(sink.plan, []).append(sink)
        for plan, readers in by_plan.items():
            try:
                self._publish_plan(plan, batch, readers=readers, updates=updates)
            except Exception as error:  # one dataset's failure never costs the others their rows
                failure = MassiveError(
                    library_bug(
                        f"{plan.dataset}: could not publish a {plan.channel} batch: {error}"
                    ),
                    cause=ErrorCause.LIBRARY_DEFECT,
                )
                # Logged here, with the trace; each stream's failure then logs at DEBUG only.
                mark_failure_logged(failure)
                logger.exception("%s", failure)
                self._fail_each(readers, failure)

    def _publish_plan(
        self,
        plan: ChannelPlan,
        batch: _Batch,
        *,
        readers: list[StreamSink],
        updates: dict[StreamSink, StreamUpdate],
    ) -> None:
        """Convert a channel's rows for one dataset once and add them to its streams.

        A futures row the schedules cannot place has a null `Date` and leaves every stream's
        `Error` alone (the calendar logs it). The drift of the rows' timestamps is
        reported like a frame's (`_found`), and the rows dropped, here or by the client,
        count toward the plan's run (`_counted`).

        Args:
            plan: The dataset's plan for the channel.
            batch: The channel's rows of this flush, and the rows the client dropped.
            readers: The dataset's streams on the channel.
            updates: Receives each stream's new published-row total and `Error` notes.
        """
        converted = self._convert(plan, batch.rows, arrived=batch.arrived)
        if converted.unreadable:  # a spelling the library cannot read is its own defect
            failure = MassiveError(converted.unreadable, cause=ErrorCause.LIBRARY_DEFECT)
            self._fail_each(readers, failure)
        drift = converted.drift
        if drift.fields_drifted or drift.dropped_rows:
            where = f"{self._thread.name} channel {plan.channel}"
            self._found(tuple(readers), drift, where=where)
        # The dataset's run first: when it fails every stream, no symbol is named stale.
        if self._counted(plan, converted, lost=batch.lost, readers=readers):
            return
        self._counted_symbols(plan, converted, batch=batch, readers=readers)
        if converted.batch.num_rows:
            self._publish_batch(
                plan, converted, readers=readers, updates=updates, generation=batch.generation
            )
        self._write_notes(readers, updates)

    def _publish_batch(
        self,
        plan: ChannelPlan,
        converted: Converted,
        *,
        readers: list[StreamSink],
        updates: dict[StreamSink, StreamUpdate],
        generation: int,
    ) -> None:
        """Add a converted batch to the dataset's streams, and record their row totals.

        A stream that receives rows records the socket generation they arrived on
        (`StreamSink.rows_generation`); the first rows of a newer generation make the flush
        call `on_new_generation` once it ends.

        Args:
            plan: The dataset's plan for the channel.
            converted: The flush's conversion, with rows.
            readers: The dataset's streams on the channel.
            updates: Receives each stream's new published-row total.
            generation: The socket generation of the batch's rows.
        """
        splits = (
            converted.batch.num_rows >= SPLIT_MIN_ROWS
            and sum(sink.takes_one_symbol(converted) for sink in readers) >= SPLIT_MIN_STREAMS
        )
        shared = SharedBatch(
            dataset_table(converted.batch, plan.date_columns), converted, splits=splits
        )
        for sink in readers:
            if sink.publish(shared):
                found = updates.get(sink, StreamUpdate())
                updates[sink] = replace(found, rows_published=sink.rows_published)
                if generation > sink.rows_generation:
                    sink.rows_generation = generation
                    self._new_generation = True

    def _write_notes(
        self, readers: list[StreamSink], updates: dict[StreamSink, StreamUpdate]
    ) -> None:
        """Write each `dm.ALL` stream's stale-symbol part of its `Error` when it changed.

        The part (`NotePart.STALE_SYMBOLS`) names the symbols whose rows are being dropped
        (`_stale_text`); it does not fail the stream, and the other sources' parts (the
        connection's outage or options-cap notes) stay as they are.

        Args:
            readers: The dataset's streams on the channel.
            updates: Receives each stream's changed parts.
        """
        for sink in readers:
            key = sink.stream_id
            text = _stale_text(self._stale.get(key, {})) or None
            if text == self._noted.get(key):
                continue
            if text is None:
                del self._noted[key]
            else:
                self._noted[key] = text
            found = updates.get(sink, StreamUpdate())
            updates[sink] = replace(found, notes={NotePart.STALE_SYMBOLS: text})

    def _counted(
        self,
        plan: ChannelPlan,
        converted: Converted,
        *,
        lost: DriftReport,
        readers: list[StreamSink],
    ) -> bool:
        """Count a flush's dropped rows toward the plan's run; fail its streams at the limit.

        A flush that publishes a row of the plan ends its run. Otherwise its dropped rows, the
        client's and the conversion's, extend the run, and once it reaches `DROPPED_RUN_LIMIT`
        every stream of the dataset on the channel fails with a `MassiveError` naming the run's
        length and its first row's reason, a defect of the library to fix
        (`ErrorCause.LIBRARY_DEFECT`), and the run starts over.

        Args:
            plan: The dataset's plan for the channel.
            converted: The flush's conversion of the channel's rows for the plan.
            lost: The channel's rows the client dropped in this flush, as drift.
            readers: The dataset's streams on the channel.

        Returns:
            True when the streams were failed, so nothing is published.
        """
        if converted.batch.num_rows:
            self._runs.pop(plan, None)
            return False
        dropped = converted.drift.dropped_rows + lost.dropped_rows
        if not dropped:
            return False
        reason = lost.dropped_reason or converted.drift.dropped_reason
        ended = _extend_run(self._runs, plan, dropped=dropped, reason=reason)
        if ended is None:
            return False
        count, reason = ended
        failure = MassiveError(
            library_bug(
                f"{plan.dataset}: the last {count} rows of channel {plan.channel} were dropped "
                "because a key could not be read, and none was published, so Massive's format "
                f"has most likely changed; the first: {reason}"
            ),
            cause=ErrorCause.LIBRARY_DEFECT,
        )
        self._fail_each(readers, failure)
        return True

    def _counted_symbols(
        self,
        plan: ChannelPlan,
        converted: Converted,
        *,
        batch: _Batch,
        readers: list[StreamSink],
    ) -> None:
        """Count a flush's dropped rows toward each symbol's run; fail its streams at the limit.

        The dataset's run (`_counted`) never ends while other symbols publish, so one symbol
        whose every row is dropped would leave its streams frozen without an error. Each
        symbol therefore has a run of its own: a published row of the symbol ends it, and once
        `DROPPED_RUN_LIMIT` of its rows in a row have been dropped, every stream that holds the
        symbol by name fails with a `MassiveError` naming it, the run's length, and its first
        row's reason (`ErrorCause.LIBRARY_DEFECT`), and the run starts over. A `dm.ALL` stream
        is never failed for one symbol, since it serves every other symbol; instead its
        `Error` names the symbol as possibly stale (`_write_notes`) until a row of the symbol
        is published again. A symbol becoming stale logs a WARNING (`_mark_stale`), and one
        published again an INFO (`_clear_stale`).

        Args:
            plan: The dataset's plan for the channel.
            converted: The flush's conversion of the channel's rows for the plan.
            batch: The channel's rows of this flush, with the rows the client dropped.
            readers: The dataset's streams on the channel.
        """
        runs = self._symbol_runs.setdefault(plan, {})
        for symbol in converted.symbols:
            runs.pop(symbol, None)
        # A stream failed this flush (a spelling the library cannot read) publishes nothing.
        everyone = [s for s in readers if s.receives_all and not s.failed]
        self._clear_stale(plan, everyone, published=converted.symbols)
        became: dict[str, str] = {}  # the symbols that become stale, with their first reason
        dropped = Counter(converted.dropped_symbols)
        dropped.update(batch.lost_symbols)
        reason = batch.lost.dropped_reason or converted.drift.dropped_reason
        for symbol, rows in dropped.items():
            if symbol in converted.symbols:
                continue
            ended = _extend_run(runs, symbol, dropped=rows, reason=reason)
            if ended is None:
                continue
            count, first = ended
            failure = MassiveError(
                library_bug(
                    f"{plan.dataset}: the last {count} rows of {symbol} on channel "
                    f"{plan.channel} were dropped because a key could not be read, and none "
                    f"was published; the first: {first}"
                ),
                cause=ErrorCause.LIBRARY_DEFECT,
            )
            self._fail_each([s for s in readers if s.holds(symbol)], failure)
            became[symbol] = first
        self._mark_stale(plan, everyone, became)

    def _mark_stale(
        self, plan: ChannelPlan, everyone: list[StreamSink], became: Mapping[str, str]
    ) -> None:
        """Name symbols as possibly stale in each `dm.ALL` stream; warn of the new ones once.

        One WARNING per flush and dataset lists the symbols not stale before in some stream,
        with the first one's reason and the streams, through `bounded_list`; a symbol stale
        already is not warned about again.

        Args:
            plan: The dataset's plan for the channel.
            everyone: The dataset's `dm.ALL` streams on the channel.
            became: Each symbol whose run reached the limit, with its first row's reason.
        """
        new: set[str] = set()
        for sink in everyone:
            stale = self._stale.setdefault(sink.stream_id, {})
            new.update(symbol for symbol in became if symbol not in stale)
            stale.update(became)
        if new:
            first = min(new)
            logger.warning(
                "%s: every recent row of %s on channel %s was dropped because a key could not "
                "be read (the first: %s), so the dm.ALL streams %s may be stale for them; they "
                "keep running and name the symbols in their Error until a row is published",
                plan.dataset,
                bounded_list(sorted(new)),
                plan.channel,
                became[first],
                bounded_list(sink.stream_id for sink in everyone),
            )

    def _clear_stale(
        self, plan: ChannelPlan, everyone: list[StreamSink], *, published: AbstractSet[str]
    ) -> None:
        """Clear the stale mark of symbols published again; log the recovery at INFO.

        Args:
            plan: The dataset's plan for the channel.
            everyone: The dataset's `dm.ALL` streams on the channel.
            published: The symbols this flush published.
        """
        recovered: set[str] = set()
        for sink in everyone:
            stale = self._stale.get(sink.stream_id)
            if stale is not None:
                back = published & stale.keys()
                for symbol in back:
                    del stale[symbol]
                recovered |= back
        if recovered:
            logger.info(
                "%s: rows of %s on channel %s are published again; the dm.ALL streams no "
                "longer name them as stale",
                plan.dataset,
                bounded_list(sorted(recovered)),
                plan.channel,
            )

    def _convert(
        self, plan: ChannelPlan, rows: list[tuple[object, ...]], *, arrived: list[int]
    ) -> Converted:
        """Convert a channel's rows for one dataset, with the plan's remembered symbols.

        The plan's memory of wire spellings is emptied after the conversion once it holds more
        than `SYMBOL_CACHE_LIMIT` of them, logged at DEBUG; the next flushes refill it.

        Args:
            plan: The dataset's plan for the channel.
            rows: The channel's rows of this flush.
            arrived: When each row's frame arrived, epoch nanoseconds, one per row.

        Returns:
            The plan's conversion of the rows (`ChannelPlan.convert`).
        """
        known = self._symbols.setdefault(plan, {})
        converted = plan.convert(rows, known, arrived=arrived, sessions=self._sessions)
        if len(known) > SYMBOL_CACHE_LIMIT:
            logger.debug(
                "%s: the %s symbol cache holds %d spellings, past its limit of %d; starts over",
                plan.dataset,
                plan.channel,
                len(known),
                SYMBOL_CACHE_LIMIT,
            )
            known.clear()
        return converted

    @staticmethod
    def _report_drift(sinks: tuple[StreamSink, ...], drift: DriftReport, *, where: str) -> None:
        """Log a channel's drift once per session, dataset, and field; add it to each stream's row.

        Args:
            sinks: The channel's streams.
            drift: The drift.
            where: The connection's URL and the channel.
        """
        live = [(core, sink) for sink in sinks if (core := sink.core()) is not None]
        for core, dataset in dict.fromkeys((core, sink.plan.dataset) for core, sink in live):
            core.drift.report(dataset, Transport.WS, drift, where=where)
        for core, sink in live:
            core.record_stream_drift(sink.stream_id, Transport.WS, drift)


def _report_updates(updates: Mapping[StreamSink, StreamUpdate]) -> None:
    """Hand each session the row changes of its streams, in one `update_streams` call.

    A stream whose session has been freed shows no row, so its change is dropped.

    Args:
        updates: The change of each stream.
    """
    by_core: dict[SessionCore, dict[int, StreamUpdate]] = {}
    for sink, update in updates.items():
        core = sink.core()
        if core is not None:
            by_core.setdefault(core, {})[sink.stream_id] = update
    for core, changes in by_core.items():
        core.update_streams(changes)


def _fail_later(sink_ref: weakref.ref[StreamSink], message: str) -> None:
    """A watch's `on_invalid`: queue its stream's failure, if the stream still exists.

    The listener's callbacks are held from Java until the JVM collects the listener, so they
    hold the sink, and through it the session, only weakly: a strong reference there would
    keep a finished session alive until the next JVM collection.

    Args:
        sink_ref: The stream's sink, weakly.
        message: Why the stream cannot follow its watchlist.
    """
    sink = sink_ref()
    if sink is not None:
        sink.fail_later(message)


def _show_skipped(sink_ref: weakref.ref[StreamSink], values: tuple[str, ...]) -> None:
    """A watch's `on_skipped`: show the symbol-table values its stream skips in `m.streams()`.

    The text is kept apart from the transport's own warning (`StreamRow.skipped`), so neither
    replaces or clears the other in the row's `Error`.

    Runs in the watch's listener, under the update-graph lock: `SessionCore.update_streams`
    takes only the leaf `registry_lock` and makes no Deephaven call, so it never blocks there.
    The sink and its session are held weakly, as in `_fail_later`.

    Args:
        sink_ref: The stream's sink, weakly.
        values: The values skipped now, sorted; empty once every bad row is fixed or deleted,
            which clears the row's `Error`.
    """
    sink = sink_ref()
    core = None if sink is None else sink.core()
    if sink is None or core is None:
        return
    if values:
        update = StreamUpdate(skipped=skipped_rows_text(values))
    else:
        update = StreamUpdate(clear_skipped=True)
    core.update_streams({sink.stream_id: update})


def _batches(
    frames: list[_Frame],
    lost: Mapping[str, DriftReport],
    lost_symbols: Mapping[str, Counter[str]],
    *,
    channel: str,
) -> list[_Batch]:
    """Return a channel's batches of one flush: one per socket generation of its frames.

    Frames arrive in order, so each generation's frames are consecutive. Almost every flush
    holds one generation; one that spans a reconnect gets a batch per socket, so each stream
    records the generation of the rows it actually received. The rows the client dropped go
    with the last batch, and a flush of dropped rows only gets one batch of no rows.

    Args:
        frames: The channel's frames this flush drained, in arrival order.
        lost: The rows the client dropped, by channel.
        lost_symbols: How many of them each canonical symbol had, by channel.
        channel: The channel.

    Returns:
        The batches, in arrival order.
    """
    runs: list[list[_Frame]] = []
    for frame in frames:
        if runs and runs[-1][0][1] == frame[1]:
            runs[-1].append(frame)
        else:
            runs.append([frame])
    batches = [
        _Batch(
            [row for *_, rows in run for row in rows],
            [at for at, _, rows in run for _ in rows],
            DriftReport(),
            Counter(),
            run[0][1],
        )
        for run in runs
    ] or [_Batch([], [], DriftReport(), Counter(), 0)]
    batches[-1] = replace(
        batches[-1],
        lost=lost.get(channel, DriftReport()),
        lost_symbols=lost_symbols.get(channel, Counter()),
    )
    return batches


def _drain(source: queue.SimpleQueue[_T]) -> list[_T]:
    """Take everything queued so far, without waiting.

    Args:
        source: The queue.

    Returns:
        The items, in queue order.
    """
    items = []
    while True:
        try:
            items.append(source.get_nowait())
        except queue.Empty:
            return items


@dataclass(frozen=True, slots=True)
class OpenedStream:
    """What `open_stream_tables` created for one stream.

    Attributes:
        handle: The stream as the session holds it: its id, blink table, scope, host, path,
            and its fail and watch-stop hooks.
        sink: The stream as its connection's flush thread publishes to it.
        watch: The watch of a reactive symbol table, not yet started; None otherwise. The
            handle's `stop_watch` holds it, so the session's record keeps it reachable.
    """

    handle: StreamHandle
    sink: StreamSink
    watch: SymbolWatch | None


def open_stream_tables(
    plan: ChannelPlan,
    schema: Schema,
    symbols: NormalizedSymbols,
    *,
    core: SessionCore,
    stream_id: int,
    host: str,
    path: str,
    evidence: str,
    recency: Recency,
    on_shutdown: Callable[[], None],
    on_symbols: Callable[[frozenset[str], frozenset[str]], None],
) -> OpenedStream:
    """Create a stream's scope and, inside it, its publisher, blink table, and symbol watch.

    Runs on the caller thread, inside the session's execution context, with no library lock
    held. The publisher is named `massive.{dataset}#{stream_id}`. For a reactive symbol table,
    the `SymbolWatch` is created in the stream scope and tied to the blink table's liveness
    (`SymbolWatch.keep_alive_with`), so it follows the watchlist for as long as anything keeps
    the stream alive, and the handle holds it until the session's record of the stream goes;
    the caller starts it once the stream's topics can receive its changes. A watchlist row that
    is not canonical, or a failure of the watchlist itself, queues the stream's failure
    (`StreamSink.fail_later`). If anything raises, even
    `KeyboardInterrupt`, the scope is released (which shuts the blink table down and runs
    `on_shutdown`) and the exception propagates.

    Args:
        plan: How the dataset reads the channel.
        schema: The dataset's merged schema, the blink table's columns.
        symbols: The stream's symbols: a sorted tuple of canonical symbols, `dm.ALL`, or a
            reactive table's `SymbolTable` box.
        core: The stream's session, which its sink reports to.
        stream_id: The id the session minted for the stream.
        host: The WebSocket host serving the stream.
        path: The cluster path, such as `/stocks`.
        evidence: Why the host was chosen (`HostDecision.evidence`), carried by the handle.
        recency: Whether the host serves real-time or delayed data, carried by the handle.
        on_shutdown: The blink table's `on_shutdown_callback`.
        on_symbols: Receives the watch's changes of a reactive symbol table.

    Returns:
        The stream's handle, sink, and watch.
    """
    scope = new_scope()
    try:
        with scope.open():
            blink, publisher = blink_publisher(
                f"massive.{plan.dataset}#{stream_id}",
                col_defs(schema),
                on_shutdown=on_shutdown,
                release_later=core.release_later,
            )
            watch = None
            received: frozenset[str] | None
            if isinstance(symbols, AllSymbols):
                received = None
            else:
                reactive = isinstance(symbols, SymbolTable)
                received = frozenset() if reactive else frozenset(cast("tuple[str, ...]", symbols))
            sink = StreamSink(
                stream_id, core=core, plan=plan, publisher=publisher, symbols=received
            )
            if isinstance(symbols, SymbolTable):
                rule = SymbolRowRule(
                    plan.dataset,
                    plan.asset_class,
                    plan.symbols.as_written,
                    core.config.noncanonical_symbols,
                )
                watch = SymbolWatch(
                    rule,
                    symbols.table,
                    on_symbols,
                    stream_id=stream_id,
                    on_invalid=functools.partial(_fail_later, weakref.ref(sink)),
                    on_skipped=functools.partial(_show_skipped, weakref.ref(sink)),
                )
                watch.keep_alive_with(blink)
        handle = StreamHandle(
            stream_id,
            blink,
            scope,
            host,
            path,
            evidence,
            sink.fail,
            sink.publish_held,
            stop_watch=None if watch is None else watch.stop,
            recency=recency,
        )
        return OpenedStream(handle, sink, watch)
    except BaseException:  # also KeyboardInterrupt: the scope must not leak
        release_scope(core.exec_ctx, scope)
        raise
