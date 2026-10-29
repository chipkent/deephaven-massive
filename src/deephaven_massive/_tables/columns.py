"""The Deephaven side of a dataset schema: column types, column descriptions, and `columns()`.

`_datasets/schema.py` describes columns without Deephaven: a `ColumnType` names a type and
knows its Arrow form, and a `Schema` is an ordered set of typed columns, each with its
description and the transports that fill it. This module is the Deephaven half of that
description:

- `dh_type` maps each `ColumnType` to its Deephaven `DType`, and `col_defs` turns a `Schema`
  into the `{name: DType}` column definitions that `table_publisher` and `input_table` take.
- `describe` returns a copy of a result table in the dataset's column order that carries the
  dataset's column descriptions (the web UI's column-header tooltips) and its table
  description, `"<dataset>: <summary>"`. Every verb calls it on the exact table it returns,
  so every verb returns the columns in the order `columns()` lists them.
- `column_table` builds the static table a query's `columns()` returns: one row per column,
  with its type, description, and transports.

Keeping this here, beside the other code that creates tables, leaves `_datasets` free of
Deephaven calls. `dh_type` and `col_defs` are pure; `describe` and `column_table` create a
table, so they run where the caller wants the table managed (inside a result's liveness scope
for `describe`), on a thread inside the session's execution context. All four are
thread-safe.
"""

from __future__ import annotations

import logging
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, cast

from deephaven import arrow as dh_arrow
from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.jcompat import j_hashmap
from deephaven.table import Table

from deephaven_massive._datasets.schema import TRANSPORT_LABELS, ColumnType

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    import pyarrow as pa

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.schema import Schema

__all__ = [
    "COLUMN_TABLE_DESCRIPTIONS",
    "col_defs",
    "column_table",
    "dataset_table",
    "describe",
    "dh_type",
]

logger = logging.getLogger(__name__)

# `dtypes.Instant` is annotated as `type` by deephaven-core although it is a `DType`
# instance, so the table is typed by value and narrowed on lookup.
_DH_TYPES: Final[Mapping[ColumnType, object]] = {
    ColumnType.INSTANT: dtypes.Instant,
    ColumnType.DOUBLE: dtypes.double,
    ColumnType.LONG: dtypes.long,
    ColumnType.INT: dtypes.int32,
    ColumnType.STRING: dtypes.string,
    ColumnType.BOOLEAN: dtypes.bool_,
    ColumnType.INT_ARRAY: dtypes.int32_array,
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
        A `{name: DType}` mapping in the schema's column order, as
        `table_publisher(col_defs=...)` and `input_table(col_defs=...)` take it.
    """
    return {column.name: dh_type(column.type) for column in schema.columns}


def dataset_table(rows: pa.Table, dates: Sequence[str]) -> Table:
    """Convert rows of a dataset's schema to a static Deephaven table, one string per date.

    `deephaven.arrow.to_table` makes one Java `String` per row of a text column, so a
    column of a million rows of one date would hold a million copies of it. Every
    calendar-date column (`Schema.date_columns`: the library's `Date` and every vendor date,
    ISO `yyyy-MM-dd` text) is therefore stored again through `String.intern()`, so every row
    of one date refers to one shared `String`: a stored date costs a reference per row, not a
    string. On a million rows of five dates the column holds five `String`s instead of a
    million, for about 0.15 seconds more per million rows once the formula is compiled. Every
    transport converts its rows through here.

    Runs on any thread inside the session's execution context; the table is static and is
    created in the liveness scope at the top of the calling thread's stack. A schema without
    a date column is converted as is.

    Args:
        rows: The rows, an Arrow table in the dataset's schema.
        dates: The dataset's calendar-date columns (`Schema.date_columns`).

    Returns:
        The static table, with every column of `rows`.
    """
    table = dh_arrow.to_table(rows)
    if not dates:
        return table
    return table.update([f"{name} = {name} == null ? null : {name}.intern()" for name in dates])


def describe(table: Table, spec: DatasetSpec, schema: Schema) -> Table:
    """Return a copy of a result table in the dataset's column order, with its descriptions.

    The copy lists the schema's columns first, in schema order, which is the order `columns()`
    promises for every verb. A `last_by` puts its key columns first, so the table is first
    reordered by a view (`moveColumnsUp`), which copies no data and keeps the table's
    attributes. The copy then gets two table attributes through
    Deephaven's `GridAttributes` methods, which `io.deephaven.engine.table.Table` extends:
    `withColumnDescriptions` with every column of the schema whose description is not empty
    (`ColumnDescriptions`, which Barrage sends to clients and the web UI shows as the
    column-header tooltip), then `withDescription` with `"<dataset>: <summary>"`
    (`TableDescription`), such as `"stocks.trades: Trades of US-listed stocks, tick by tick."`.
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
            from deephaven_massive._enums import AssetClass
            from deephaven_massive._tables.columns import describe

            spec = DatasetSpec(
                name="stocks.x",
                summary="Example prices.",
                asset_class=AssetClass.STOCKS,
                channels={None: "T"},
            )
            schema = Schema("stocks.x", (Column("Price", ColumnType.DOUBLE, "The price."),))
            result = describe(new_table([double_col("Price", [1.5])]), spec, schema)
            result.attributes()["TableDescription"]  # "stocks.x: Example prices."

    Args:
        table: The table the verb would otherwise return, with every column of `schema`.
        spec: The dataset, for its name and summary.
        schema: The dataset's schema (`Catalog.dataset_schema(spec)`), for its descriptions.

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
        described = described.withDescription(f"{spec.name}: {spec.summary}")
    except RuntimeError as error:  # jpy's form of a Java exception
        raise RuntimeError(f"describing a {spec.name} result failed: {error}") from error
    return Table(j_table=described)


COLUMN_TABLE_DESCRIPTIONS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "Name": "The column name.",
        "Type": (
            "The column's Deephaven type: long, int, double, boolean, String, Instant, or int[]; "
            "a calendar date is ISO yyyy-MM-dd text, a String."
        ),
        "Description": "What the column holds; empty when Massive gives no description.",
        "SuppliedBy": (
            "The transports whose sources fill the column for this dataset, spelled REST, flat "
            'files, and WebSocket, in that order, joined by ", "; empty for a column that only '
            "other datasets of the same kind supply, which this dataset always leaves null."
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
