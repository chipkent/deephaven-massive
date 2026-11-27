"""Tests of the plain Arrow rows of the live tests (`tests/_live/arrow_rows.py`)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa
from deephaven import arrow as dh_arrow
from deephaven import empty_table

from tests._live.arrow_rows import plain_column, snapshot_rows

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.table import Table

# Deephaven 43 encodes a column of repeated values only from about 16 rows, so these two sizes
# sit well past the threshold.
RUN_ROWS = 32
CYCLE_ROWS = 1_024


def runs() -> Table:
    # Timestamps that repeat in runs of ten.
    return empty_table(RUN_ROWS).update(
        ["Ts = epochNanosToInstant(1000L + (long)(i / 10))", "S = `AAPL`"]
    )


def cycle() -> Table:
    # Timestamps and text that cycle, so their values repeat out of order.
    return empty_table(CYCLE_ROWS).update(
        ["Ts = epochNanosToInstant(1000L + (long)(i % 3))", "S = i % 2 == 0 ? `AAPL` : `MSFT`"]
    )


def test_the_tables_come_back_encoded_from_deephaven() -> None:
    # Guards the tests below: if Deephaven stops encoding these, they would test nothing.
    assert pa.types.is_run_end_encoded(dh_arrow.to_arrow(runs()).column("Ts").type)
    assert pa.types.is_dictionary(dh_arrow.to_arrow(cycle()).column("Ts").type)


def test_encoded_timestamps_come_back_as_ints(
    table_rows: Callable[[Table], list[dict[str, Any]]],
) -> None:
    assert [r["Ts"] for r in table_rows(runs())] == [1000 + i // 10 for i in range(RUN_ROWS)]
    assert [r["Ts"] for r in table_rows(cycle())] == [1000 + i % 3 for i in range(CYCLE_ROWS)]


def test_encoded_text_comes_back_plain() -> None:
    rows = snapshot_rows(cycle())
    assert [r["S"] for r in rows] == ["AAPL" if i % 2 == 0 else "MSFT" for i in range(CYCLE_ROWS)]
    assert pa.types.is_string(plain_column(dh_arrow.to_arrow(cycle()).column("S")).type)


def test_a_plain_column_is_only_cast() -> None:
    small = empty_table(3).update(["Ts = epochNanosToInstant(1000L + i)", "N = i"])
    assert snapshot_rows(small) == [{"Ts": 1000 + i, "N": i} for i in range(3)]
