"""Plain Arrow columns for the tests: decode what Deephaven encodes, and cast instants to ints.

Deephaven 43 sends `deephaven.arrow.to_arrow` a column whose values repeat in runs as
`run_end_encoded`, and one whose few distinct values repeat out of order as `dictionary`, while
Deephaven 42 sent every column plain. `plain_column` turns any of them into the plain column
with the same values, and an instant into `int` nanoseconds since the epoch, so a test reads
the same rows whichever encoding it gets. `tests/conftest.py` builds the `table_rows` fixture
on `snapshot_rows`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.compute as pc
from deephaven import arrow as dh_arrow

if TYPE_CHECKING:
    from deephaven.table import Table


def plain_column(column: pa.ChunkedArray) -> pa.ChunkedArray:
    """Return a column decoded to its plain values, with instants as epoch nanoseconds.

    Args:
        column: A column of `deephaven.arrow.to_arrow`, plain or encoded.

    Returns:
        The column with the same values, never run-end encoded or dictionary encoded, and a
        timestamp column cast to `int64`.
    """
    if pa.types.is_run_end_encoded(column.type):
        column = pc.run_end_decode(column)
    if pa.types.is_dictionary(column.type):
        column = column.cast(column.type.value_type)
    return column.cast(pa.int64()) if pa.types.is_timestamp(column.type) else column


def snapshot_rows(table: Table) -> list[dict[str, Any]]:
    """Return a table's rows as dictionaries, every instant as epoch nanoseconds.

    Args:
        table: The Deephaven table to read.

    Returns:
        One `{column: value}` dictionary per row, with plain values.
    """
    data = dh_arrow.to_arrow(table)
    columns = [plain_column(column) for column in data.columns]
    rows: list[dict[str, Any]] = pa.table(columns, names=data.column_names).to_pylist()
    return rows
