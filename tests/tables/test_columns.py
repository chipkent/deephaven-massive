"""Tests for the Deephaven column types of a dataset schema."""

from deephaven import dtypes

from deephaven_massive._datasets.schema import Column, ColumnType, Schema
from deephaven_massive._tables.columns import col_defs, dh_type


def test_every_column_type_has_a_deephaven_type() -> None:
    assert {t: dh_type(t) for t in ColumnType} == {
        ColumnType.INSTANT: dtypes.Instant,
        ColumnType.LOCAL_DATE: dtypes.LocalDate,
        ColumnType.DOUBLE: dtypes.double,
        ColumnType.LONG: dtypes.long,
        ColumnType.INT: dtypes.int32,
        ColumnType.STRING: dtypes.string,
        ColumnType.BOOLEAN: dtypes.bool_,
        ColumnType.INT_ARRAY: dtypes.int32_array,
    }


def test_col_defs_keep_the_schema_order() -> None:
    schema = Schema(
        "example",
        (
            Column("Symbol", ColumnType.STRING),
            Column("Timestamp", ColumnType.INSTANT),
            Column("Price", ColumnType.DOUBLE),
        ),
    )
    defs = col_defs(schema)
    assert list(defs) == ["Symbol", "Timestamp", "Price"]
    assert defs == {
        "Symbol": dtypes.string,
        "Timestamp": dtypes.Instant,
        "Price": dtypes.double,
    }
