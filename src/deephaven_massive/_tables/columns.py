"""The Deephaven column types of a dataset schema.

`_datasets/schema.py` describes columns without Deephaven: a `ColumnType` names a type and
knows its Arrow form, and a `Schema` is an ordered set of typed columns. This module is the
Deephaven half of that description: `dh_type` maps each `ColumnType` to its Deephaven `DType`,
and `col_defs` turns a `Schema` into the `{name: DType}` column definitions that
`table_publisher` and `input_table` take. Keeping the mapping here, beside the other code that
creates tables, leaves `_datasets` free of Deephaven calls.

Every function here is pure and thread-safe; none creates a table.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final, cast

from deephaven import dtypes

from deephaven_massive._datasets.schema import ColumnType

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._datasets.schema import Schema

__all__ = ["col_defs", "dh_type"]

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
