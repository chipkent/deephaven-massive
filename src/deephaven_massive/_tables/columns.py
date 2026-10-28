"""The Deephaven side of a dataset schema: column types, column descriptions, and `columns()`.

`_datasets/schema.py` describes columns without Deephaven: a `ColumnType` names a type and
knows its Arrow form, and a `Schema` is an ordered set of typed columns, each with its
description and the transports that fill it. This module is the Deephaven half of that
description:

- `dh_type` maps each `ColumnType` to its Deephaven `DType`, and `col_defs` turns a `Schema`
  into the `{name: DType}` column definitions that `table_publisher` and `input_table` take.
- `describe` returns a copy of a result table that carries the dataset's column descriptions
  (the web UI's column-header tooltips) and its table description, `"<dataset>: <summary>"`.
  Every verb calls it on the exact table it returns.
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

from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.jcompat import j_hashmap
from deephaven.table import Table

from deephaven_massive._datasets.schema import TRANSPORT_LABELS, ColumnType

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.schema import Schema

__all__ = ["COLUMN_TABLE_DESCRIPTIONS", "col_defs", "column_table", "describe", "dh_type"]

logger = logging.getLogger(__name__)

# `dtypes.Instant` and `dtypes.LocalDate` are annotated as `type` by deephaven-core although
# they are `DType` instances, so the table is typed by value and narrowed on lookup.
_DH_TYPES: Final[Mapping[ColumnType, object]] = {
    ColumnType.INSTANT: dtypes.Instant,
    ColumnType.LOCAL_DATE: dtypes.LocalDate,
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


def describe(table: Table, spec: DatasetSpec, schema: Schema) -> Table:
    """Return a copy of a result table that carries the dataset's descriptions.

    The copy gets two table attributes through Deephaven's `GridAttributes` methods, which
    `io.deephaven.engine.table.Table` extends: `withColumnDescriptions` with every column of
    the schema whose description is not empty (`ColumnDescriptions`, which Barrage sends to
    clients and the web UI shows as the column-header tooltip), then `withDescription` with
    `"<dataset>: <summary>"` (`TableDescription`), such as
    `"stocks.trades: Trades of US-listed stocks, tick by tick."`. `withColumnDescriptions`
    checks that every described column exists, so a schema that disagrees with the table fails
    here, at once; `Table.with_attributes`, which skips that check, is not used.

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
        table: The table the verb would otherwise return. It must have every column of
            `schema` that has a description.
        spec: The dataset, for its name and summary.
        schema: The dataset's schema (`Catalog.dataset_schema(spec)`), for its descriptions.

    Returns:
        A new table with the same rows, columns, and other attributes as `table`, plus
        `TableDescription` and, when the schema describes any column, `ColumnDescriptions`.

    Raises:
        RuntimeError: If a described column is missing from `table` (Deephaven's
            `NoSuchColumnException`, "Unknown column names [...]"), or if `table` has already
            failed, as a failed stream's blink table has (copying a table listens to it). The
            Java methods are called directly, so the error is jpy's `RuntimeError`, not
            Deephaven's `DHError`; its message names the dataset and the Java exception, which
            is chained as its cause.
    """
    descriptions = {c.name: c.description for c in schema.columns if c.description}
    described = table.j_table
    try:
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
            "The column's Deephaven type: long, int, double, boolean, String, Instant, LocalDate, "
            "or int[]."
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
