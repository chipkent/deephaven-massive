"""The Deephaven side of a dataset schema: column types, column descriptions, and `columns()`.

`_datasets/schema.py` describes columns without Deephaven: a `ColumnType` names a type and
knows its Arrow form, and a `Schema` is an ordered set of typed columns, each with its
description and the transports that fill it. This module is the Deephaven half of that
description:

- `dh_type` maps each `ColumnType` to its Deephaven `DType`, and `col_defs` turns a `Schema`
  into the `{name: DType}` column definitions that `blink_publisher` takes.
- `describe` returns a copy of a result table in the dataset's column order that carries the
  dataset's column descriptions (the Deephaven IDE's column-header tooltips) and its table
  description, `"<dataset>: <summary>"`. Every verb calls it on the exact table it returns,
  so every verb returns the columns in the order `columns()` lists them.
- `column_table` builds the static table a query's `columns()` returns: one row per column,
  with its type, description, and transports.
- `dataset_table` turns a dataset's Arrow rows into a Deephaven table, storing one shared
  string per distinct calendar date (`Schema.date_columns`).

Keeping this here, beside the other code that creates tables, leaves `_datasets` free of
Deephaven calls. `dh_type` and `col_defs` are pure; `describe`, `column_table`, and
`dataset_table` create a table, so they run where the caller wants the table managed (inside
a result's liveness scope for `describe`), on a thread inside the session's execution context.
All five are thread-safe.
"""

from __future__ import annotations

import importlib
import logging
import threading
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, cast

import pyarrow as pa
import pyarrow.compute as pc
from deephaven import arrow as dh_arrow
from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.jcompat import j_hashmap
from deephaven.table import Table

from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._enums import Recency

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from collections.abc import Set as AbstractSet

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.schema import Schema
    from deephaven_massive._enums import Subscription

__all__ = [
    "COLUMN_TABLE_DESCRIPTIONS",
    "col_defs",
    "column_table",
    "dataset_table",
    "describe",
    "dh_type",
    "recency_note",
    "rest_recency_note",
]

logger = logging.getLogger(__name__)

# jpy, the Python-Java bridge Deephaven runs on, ships no type information, so it is loaded
# as a plain module object.
jpy = importlib.import_module("jpy")
_JString = jpy.get_type("java.lang.String")
_System = jpy.get_type("java.lang.System")
_LinkedHashMap = jpy.get_type("java.util.LinkedHashMap")
_QueryTable = jpy.get_type("io.deephaven.engine.table.impl.QueryTable")
_ArrayBackedColumnSource = jpy.get_type(
    "io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource"
)

# `dtypes.Instant` is annotated as `type` by deephaven-core although it is a `DType`
# instance, so the table is typed by value and narrowed on lookup.
_DH_TYPES: Final[Mapping[ColumnType, object]] = {
    ColumnType.INSTANT: dtypes.Instant,
    ColumnType.DOUBLE: dtypes.double,
    ColumnType.LONG: dtypes.long,
    ColumnType.INT: dtypes.int32,
    ColumnType.STRING: dtypes.string,
    ColumnType.BOOLEAN: dtypes.bool_,
    ColumnType.LONG_ARRAY: dtypes.long_array,
    ColumnType.STRING_ARRAY: dtypes.string_array,
}


def dh_type(column_type: ColumnType) -> dtypes.DType:
    """Return the Deephaven type of a column type.

    The Deephaven type is the one `deephaven.arrow.to_table` produces from the column type's
    Arrow form (`ColumnType.arrow_type`), so a table built from `Schema.to_arrow` and a table
    built from these definitions agree column for column.

    Args:
        column_type: The column type, such as `ColumnType.INSTANT`.

    Returns:
        The Deephaven `DType`, such as `dtypes.Instant`.
    """
    return cast("dtypes.DType", _DH_TYPES[column_type])


def col_defs(schema: Schema) -> dict[str, dtypes.DType]:
    """Return the Deephaven column definitions of a schema.

    Example:
        ::

            from deephaven_massive._datasets.schema import Column, ColumnType, Schema
            from deephaven_massive._tables.columns import col_defs

            schema = Schema(
                "example",
                (Column("Symbol", ColumnType.STRING), Column("Price", ColumnType.DOUBLE)),
            )
            col_defs(schema)  # {"Symbol": dtypes.string, "Price": dtypes.double}

    Args:
        schema: The schema.

    Returns:
        A `{name: DType}` mapping in the schema's column order, as `blink_publisher` takes
        it.
    """
    return {column.name: dh_type(column.type) for column in schema.columns}


def dataset_table(rows: pa.Table, dates: Sequence[str]) -> Table:
    """Convert rows of a dataset's schema to a static Deephaven table, one string per date.

    `deephaven.arrow.to_table` makes one Java `String` per row of a text column, so a
    column of a million rows of one date would hold a million copies of it. Every
    calendar-date column (`Schema.date_columns`: the library's `Date` and every vendor date,
    ISO `yyyy-MM-dd` text) is therefore built here instead, from one shared Java `String` per
    distinct date for the whole process (`_DatePool`), so a stored date costs a reference per
    row. The other columns convert through `to_table`, and the date columns join them as
    in-memory column sources of the same rows, in schema order, with no Deephaven `update()`,
    whose fixed cost of a few milliseconds a call would fall on every stream flush. Each run
    of rows of one date is filled by copying its shared `String` and then doubling the filled
    stretch, so a column costs a few Java calls per run. A column of more than `_MAX_RUNS`
    runs, which only interleaved dates make, is interned by a formula instead (`_interned`),
    whose JVM-interned `String`s are shared within that table too. A list-of-strings column
    that holds no string in some record batch is converted alone and joined the same way,
    since converted among other columns it would corrupt the columns after it
    (`_string_lists_without_values`).
    Every transport converts its rows through here, and nothing else may build a dataset
    table: the date columns must be ISO dates, whose set is small, never other text, which
    would grow the pool without bound.

    Its fixed cost per call is under a millisecond, against several through `update()`: on 50
    rows about 1.4 ms, against 0.6 ms for `to_table` alone; on 2,000 rows the two match; on
    large batches it is faster than `to_table`, since the date strings are never made per row
    (about 29 against 46 ms on 200,000 rows), all on one loaded machine.

    Runs on any thread inside the session's execution context. The table is static, and
    Deephaven's liveness scopes manage only refreshing tables, so no scope holds it: it lives
    as long as something refers to it, such as its Python wrapper or a table built from it,
    whatever scope the calling thread has open. Rows with neither
    a date column nor such a list column are converted as is. `rows` must hold at least one
    column of another kind (every dataset has `TimestampInsert`).

    Args:
        rows: The rows, an Arrow table in the dataset's schema.
        dates: The dataset's calendar-date columns (`Schema.date_columns`).

    Returns:
        The static table, with every column of `rows`, in order. Its row keys are the
        positions 0 to n - 1 (`isFlat()`), as in `to_table`'s own result.
    """
    apart = _string_lists_without_values(rows)
    if not dates and not apart:
        return dh_arrow.to_table(rows)
    if apart:
        logger.debug(
            "converting the list columns %s of %d rows alone: a record batch holds no string",
            apart,
            rows.num_rows,
        )
    runs = {name: _DateRuns.of(rows.column(name)) for name in dates}
    interned = any(len(found.starts) > _MAX_RUNS for found in runs.values())
    if interned:
        logger.debug(
            "interning the date columns of %d rows by a formula: runs per column %s, over %d",
            rows.num_rows,
            {name: len(found.starts) for name, found in runs.items()},
            _MAX_RUNS,
        )
        runs = {}
    others = dh_arrow.to_table(rows.drop_columns([*runs, *apart])).j_table
    sources = _LinkedHashMap()
    for name in rows.column_names:
        if name in runs:
            values = runs[name].strings()
            column = _ArrayBackedColumnSource.getMemoryColumnSource(values, _JString.jclass, None)
            sources.put(name, column)
        elif name in apart:
            alone = dh_arrow.to_table(rows.select([name])).j_table
            sources.put(name, alone.getColumnSource(name))
        else:
            sources.put(name, others.getColumnSource(name))
    joined = _QueryTable(others.getRowSet(), sources)
    # The rows are those of `others`, a static table whose rows are 0 to n - 1, so the joined
    # table is flat as `to_table`'s own result is, and row positions serve as row keys.
    joined.setFlat()
    table = Table(joined)
    return _interned(table, dates) if interned else table


def _string_lists_without_values(rows: pa.Table) -> list[str]:
    """Return the list-of-strings columns that some record batch holds no string in.

    `deephaven.arrow.to_table` sends each record batch to Deephaven's Arrow reader. When a
    list-of-strings column of a batch holds no string at all, because every row is null or an
    empty list, that reader returns its empty values without consuming their buffers, so every
    later column of the batch reads the wrong buffers: a later list column fails (`asserted
    length >= 0`), and a later column of another type silently takes shifted values. As the
    last column of a batch the column converts correctly, so `dataset_table` converts each
    such column alone and joins it to the others, and every other column converts as before,
    with no copy. A list of numbers is not affected.

    The batches checked are those of the whole table, whose boundaries include those of any
    subset of its columns, so a column that holds a string in every batch with rows here holds
    one in every batch with rows of the table it is converted in. A batch without rows is
    harmless (`_holds_no_string`).

    Args:
        rows: The rows.

    Returns:
        The names of the columns to convert alone, in the table's order; empty when none is.
    """
    lists = [
        field.name
        for field in rows.schema
        if pa.types.is_list(field.type) and pa.types.is_string(field.type.value_type)
    ]
    if not lists:
        return lists
    batches = rows.to_batches()
    return [name for name in lists if any(_holds_no_string(batch, name) for batch in batches)]


def _holds_no_string(batch: pa.RecordBatch, name: str) -> bool:
    """Say whether a list column of a record batch has rows but holds no element.

    A batch with no row is harmless: Deephaven converts it correctly, whatever its list columns
    hold. Counting it would convert a column alone just because a table's chunks include an
    empty one, as they do when an empty page is joined to a real one.

    Args:
        batch: The record batch.
        name: The list column.

    Returns:
        True when the batch has rows and every row of the column is null or an empty list,
        its first and last offsets equal.
    """
    offsets = cast("pa.ListArray", batch.column(name)).offsets
    return batch.num_rows > 0 and bool(offsets[0].as_py() == offsets[-1].as_py())


_MAX_RUNS: Final = 4096
"""The most runs of one date a column is built from by copying; more fall back to a formula."""


class _DatePool:
    """One Java `String` per distinct date text, for the life of the process.

    Each date's `String` lives in a one-element Java array, so it never crosses into Python,
    where jpy would turn it into a Python `str` and back into a new `String`. Thread-safe: the
    map sits behind a leaf lock, held only to look up or add an entry; the Java array is
    created under it, with no Deephaven call.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._arrays: dict[str, object] = {}

    def array(self, text: str) -> object:
        """Return the one-element `String[]` that holds a date's shared `String`.

        Args:
            text: An ISO `yyyy-MM-dd` date.

        Returns:
            The Java array; the same one for every call with the same text.
        """
        with self._lock:
            found = self._arrays.get(text)
            if found is None:
                found = self._arrays[text] = cast("object", jpy.array("java.lang.String", [text]))
            return found


_POOL: Final = _DatePool()


@dataclass(frozen=True, slots=True)
class _DateRuns:
    """A date column as runs of rows of one date: where each run starts, and its date.

    Attributes:
        count: The number of rows.
        starts: The first row of each run, ascending; the first is 0 when there are rows.
        texts: Each run's date, ISO text, or None for a run of nulls.
    """

    count: int
    starts: tuple[int, ...]
    texts: tuple[str | None, ...]

    @classmethod
    def of(cls, column: pa.ChunkedArray) -> _DateRuns:
        """Split a column into runs of one date each, using Arrow compute alone.

        Args:
            column: An Arrow string column of ISO dates and nulls.

        Returns:
            Its runs.
        """
        count = len(column)
        if count == 0:
            return cls(0, (), ())
        distinct = pc.unique(column)
        # `distinct` holds null too where the column does, so every row has a code and a run of
        # nulls is a run like any other, whose text is None.
        codes = pc.index_in(column, value_set=distinct).combine_chunks()
        changes = pc.indices_nonzero(pc.not_equal(codes[1:], codes[:-1]))
        starts = [0, *pc.add(changes, 1).to_pylist()]
        texts = distinct.to_pylist()
        firsts = codes.take(pa.array(starts, pa.int64())).to_pylist()
        return cls(count, tuple(starts), tuple(texts[code] for code in firsts))

    def strings(self) -> object:
        """Build the Java `String[]` of the column, every row of a date sharing one `String`.

        Each run copies its date's pooled `String` once and then doubles the filled stretch,
        so it costs about one Java call per doubling, not one per row.

        Returns:
            The array, nulls kept.
        """
        out: object = jpy.array("java.lang.String", self.count)
        ends = (*self.starts[1:], self.count) if self.starts else ()
        for first, end, text in zip(self.starts, ends, self.texts, strict=True):
            if text is None:
                continue
            _System.arraycopy(_POOL.array(text), 0, out, first, 1)
            filled = 1
            while filled < end - first:
                step = min(filled, end - first - filled)
                _System.arraycopy(out, first, out, first + filled, step)
                filled += step
        return out


def _interned(table: Table, dates: Sequence[str]) -> Table:
    """Intern the date columns of a converted table with interleaved dates, by a formula.

    Args:
        table: The table, its date columns converted as plain strings.
        dates: The date columns.

    Returns:
        The static table, every date `String` the JVM's interned one.
    """
    return table.update([f"{name} = {name} == null ? null : {name}.intern()" for name in dates])


def describe(table: Table, spec: DatasetSpec, schema: Schema, *, note: str | None = None) -> Table:
    """Return a copy of a result table in the dataset's column order, with its descriptions.

    The copy lists the schema's columns first, in schema order, which is the order `columns()`
    promises for every verb. A `last_by` puts its key columns first, so the table is first
    reordered by a view (`moveColumnsUp`), which copies no data and keeps the table's
    attributes. The copy then gets two table attributes through
    Deephaven's `GridAttributes` methods, which `io.deephaven.engine.table.Table` extends:
    `withColumnDescriptions` with every column of the schema whose description is not empty
    (`ColumnDescriptions`, which Barrage sends to clients and the Deephaven IDE shows as the
    column-header tooltip), then `withDescription` with `"<dataset>: <summary>"`
    (`TableDescription`), such as `"stocks.trades: Trades of US-listed stocks, tick by tick."`,
    followed by `note` when one is given (a `history()` table's source).
    The reordering checks that every schema column exists, and `withColumnDescriptions` that
    every described one does, so a schema that disagrees with the table fails here, at once;
    `Table.with_attributes`, which skips that check, is not used.

    Call it last, on the exact table a verb returns, after its shape is derived: an
    append-only or ring table built from a described blink table would lose the attributes,
    because neither copies them. The copy is a new table that depends on `table`, so `table`
    (for a stream, the blink table that owns the `on_shutdown_callback`) lives as long as the
    copy does, and the copy ticks with it. Every other attribute of `table` is kept, the blink
    attribute included, so the copy of a blink table is a blink table.

    The copy is created in the liveness scope at the top of the calling thread's stack, so
    call it on the caller thread inside the result's scope and the session's execution
    context, never in a table listener. Copying a table can briefly take the update-graph
    shared lock (its initial snapshot falls back to the lock after repeated concurrent
    attempts), like any table operation, so it never runs under `registry_lock`.

    Example:
        ::

            from deephaven import new_table
            from deephaven.column import double_col

            from deephaven_massive._datasets.catalog import DatasetSpec
            from deephaven_massive._datasets.schema import Column, ColumnType, Schema
            from deephaven_massive._enums import AssetClass, NotBars
            from deephaven_massive._tables.columns import describe

            spec = DatasetSpec(
                name="stocks.x",
                summary="Example prices.",
                asset_class=AssetClass.STOCKS,
                channels={NotBars.ANY: "T"},
            )
            schema = Schema("stocks.x", (Column("Price", ColumnType.DOUBLE, "The price."),))
            result = describe(new_table([double_col("Price", [1.5])]), spec, schema)
            result.attributes()["TableDescription"]  # "stocks.x: Example prices."

    Args:
        table: The table the verb would otherwise return, with every column of `schema`.
        spec: The dataset, for its name and summary.
        schema: The dataset's schema (`Catalog.dataset_schema(spec)`), for its descriptions.
        note: A sentence to add to the table description after the summary, such as
            `"Read from REST: AUTO chose it (2 symbols < flat_file_symbol_threshold 50)."`;
            None adds nothing.

    Returns:
        A new table with the same rows, columns, and other attributes as `table`, the
        schema's columns first and in schema order, plus `TableDescription` and, when the
        schema describes any column, `ColumnDescriptions`.

    Raises:
        RuntimeError: If a column of `schema` is missing from `table` (Deephaven's
            `IllegalArgumentException`, "Column(s) not found: ..."), or if `table` has already
            failed, as a failed stream's blink table has (copying a table listens to it). The
            Java methods are called directly, so the error is jpy's `RuntimeError`, not
            Deephaven's `DHError`; its message names the dataset and the Java exception, which
            is chained as its cause.
    """
    descriptions = {c.name: c.description for c in schema.columns if c.description}
    try:
        # A key aggregation (`last_by`) puts its keys first; every verb lists schema order.
        described = table.j_table.moveColumnsUp(*schema.column_names)
        if descriptions:  # an empty attribute would claim descriptions the table lacks
            described = described.withColumnDescriptions(j_hashmap(descriptions))
        text = (
            f"{spec.name}: {spec.summary}"
            if note is None
            else f"{spec.name}: {spec.summary} {note}"
        )
        described = described.withDescription(text)
    except RuntimeError as error:  # jpy's form of a Java exception
        raise RuntimeError(f"describing a {spec.name} result failed: {error}") from error
    return Table(j_table=described)


def recency_note(recency: Recency, *, host: str) -> str:
    """Return the sentence a stream's tables add to their description about its feed.

    Args:
        recency: The stream's host's recency (`HostDecision.recency`).
        host: The stream's WebSocket host.

    Returns:
        For example `"Live data from Massive's delayed feed (delayed.massive.com), 15 minutes
        behind (10 for futures)."`; `UNKNOWN` never claims real-time.
    """
    if recency is Recency.REALTIME:
        return f"Live data from Massive's real-time feed ({host})."
    if recency is Recency.DELAYED:
        return (
            f"Live data from Massive's delayed feed ({host}), 15 minutes behind (10 for futures)."
        )
    return f"Live data from {host}, a pinned host whose recency the library does not know."


def rest_recency_note(
    spec: DatasetSpec, delayed_subscriptions: AbstractSet[Subscription]
) -> str | None:
    """Return the sentence a REST table's description adds about its recency.

    `fetch()` and `poll()` tables of one dataset carry the same sentence.

    Args:
        spec: The dataset.
        delayed_subscriptions: `MassiveConfig.delayed_subscriptions`.

    Returns:
        For the current data of one subscription (`DatasetSpec.current_subscription`): when
        it is listed, that Massive's labels are not checked; otherwise, that a response
        labelled `DELAYED` fails the table. None for any other dataset. Neither sentence
        claims real-time data: REST says so only through its labels.
    """
    subscription = spec.current_subscription
    if subscription is None:
        return None
    if subscription in delayed_subscriptions:
        return (
            "Data as the plan serves it, real-time or delayed: "
            "MassiveConfig.delayed_subscriptions lists its subscription, so Massive's DELAYED "
            "labels are not checked."
        )
    return (
        "Checked for delayed data: a response Massive labels DELAYED fails the table unless "
        "MassiveConfig.delayed_subscriptions lists its subscription."
    )


COLUMN_TABLE_DESCRIPTIONS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "Name": "The column name.",
        "Type": (
            "The column's Deephaven type: long, int, double, Boolean, String, Instant, long[], or "
            "String[]; a calendar date is ISO yyyy-MM-dd text, a String."
        ),
        "Description": "What the column holds; empty when Massive gives no description.",
        "SuppliedBy": (
            "The transports whose sources fill the column for this dataset, spelled REST, flat "
            'files, and WebSocket, in that order, joined by ", "; a column the transport '
            "serving a call does not fill is null in its result."
        ),
    }
)
"""The descriptions of the four columns of a `columns()` table, in column order; read-only."""


def column_table(schema: Schema) -> Table:
    """Build the static table a query's `columns()` returns: what each dataset column means.

    Pure apart from creating the table: no request, no stream, no session state. The table is
    created in the liveness scope at the top of the calling thread's stack, as any static
    table the caller makes is, and its own four columns carry descriptions
    (`COLUMN_TABLE_DESCRIPTIONS`).

    Args:
        schema: The dataset's schema (`Catalog.dataset_schema`).

    Returns:
        A static table with one row per schema column, in result column order: `Name`
        (String), `Type` (String, the `ColumnType` value), `Description` (String, possibly
        empty), and `SuppliedBy` (String, the column's transports as `TRANSPORT_LABELS` spells
        them, joined by ", ", possibly empty).
    """
    columns = schema.columns
    table = new_table(
        [
            string_col("Name", [c.name for c in columns]),
            string_col("Type", [c.type.value for c in columns]),
            string_col("Description", [c.description for c in columns]),
            string_col(
                "SuppliedBy",
                [", ".join(TRANSPORT_LABELS[t] for t in c.supplied_by) for c in columns],
            ),
        ]
    )
    described = table.j_table.withColumnDescriptions(j_hashmap(dict(COLUMN_TABLE_DESCRIPTIONS)))
    return Table(j_table=described)
