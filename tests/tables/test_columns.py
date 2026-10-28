"""Tests for the Deephaven side of a dataset schema: types, descriptions, and `columns()`.

The description tests build every result shape directly in the embedded JVM (a static table,
a publisher's blink table, and its append-only, ring, and `last_by` tables) and check that
`describe` gives each one the schema's descriptions and column order without changing what it
is: the copy ticks with its parent, keeps the blink attribute, and keeps its parent, and so the
stream's `on_shutdown_callback`, alive exactly as long as it lives.
"""

import threading
from collections.abc import Callable
from typing import Any, cast

import pytest
from deephaven import dtypes, new_table
from deephaven.column import double_col, string_col
from deephaven.execution_context import ExecutionContext
from deephaven.stream import blink_to_append_only
from deephaven.stream.table_publisher import table_publisher
from deephaven.table import Table
from deephaven.table_factory import ring_table

from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.schema import Column, ColumnType, Schema
from deephaven_massive._enums import AssetClass
from deephaven_massive._massive import Transport
from deephaven_massive._tables.columns import (
    COLUMN_TABLE_DESCRIPTIONS,
    col_defs,
    column_table,
    describe,
    dh_type,
)
from deephaven_massive._tables.liveness import new_scope

TIMEOUT = 10.0

SPEC = DatasetSpec(
    name="stocks.trades",
    summary="Trades of US-listed stocks, tick by tick.",
    asset_class=AssetClass.STOCKS,
    channels={None: "T"},
)
SCHEMA = Schema(
    "stocks.trades",
    (
        Column("Symbol", ColumnType.STRING, "The instrument.", (Transport.WS,)),
        Column(
            "Price",
            ColumnType.DOUBLE,
            "The price.",
            (Transport.REST, Transport.FLAT_FILES, Transport.WS),
        ),
        Column("Note", ColumnType.STRING),  # no description: left out of the attribute
    ),
)
DESCRIPTIONS = {"Symbol": "The instrument.", "Price": "The price."}
TABLE_DESCRIPTION = "stocks.trades: Trades of US-listed stocks, tick by tick."


def test_every_column_type_has_a_deephaven_type() -> None:
    assert {t: dh_type(t) for t in ColumnType} == {
        ColumnType.INSTANT: dtypes.Instant,
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


# --- describe --------------------------------------------------------------------------------


def column_descriptions(table: Table) -> dict[str, str]:
    # The attribute is a Java map; read its entries.
    found = table.attributes()["ColumnDescriptions"]
    return {str(entry.getKey()): str(entry.getValue()) for entry in found.entrySet().toArray()}


def assert_described(table: Table) -> None:
    assert column_descriptions(table) == DESCRIPTIONS
    assert table.attributes()["TableDescription"] == TABLE_DESCRIPTION


def rows(*symbols: str) -> Table:
    return new_table(
        [
            string_col("Symbol", list(symbols)),
            double_col("Price", [1.5] * len(symbols)),
            string_col("Note", [""] * len(symbols)),
        ]
    )


def wait_for(condition: Callable[[], bool]) -> bool:
    # Bounded wait for an update-graph cycle to deliver a change.
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(200):
        if condition():
            return True
        pause.wait(0.05)
    return condition()


def test_a_static_table_is_described(dh_exec_ctx: ExecutionContext) -> None:
    parent = rows("AAPL", "MSFT").move_columns_up(["Note"])
    result = describe(parent, SPEC, SCHEMA)
    assert_described(result)
    assert result.column_names == list(SCHEMA.column_names)  # back in schema order
    assert result.j_table is not parent.j_table
    assert "TableDescription" not in parent.attributes()  # the parent is unchanged
    assert not result.is_refreshing
    assert result.size == 2


class Stream:
    """A publisher-backed blink table in its own stream scope, as a real stream has."""

    def __init__(self) -> None:
        self.shutdown = threading.Event()
        self.scope = new_scope()
        with self.scope.open():
            self.blink, self.publisher = table_publisher(
                "massive.test", col_defs(SCHEMA), on_shutdown_callback=self.shutdown.set
            )


# Each verb's shape of the stream's blink table, before `describe`.
SHAPES: dict[str, Callable[[Table], Table]] = {
    "blink": lambda blink: blink,
    "append_only": blink_to_append_only,
    "ring": lambda blink: ring_table(blink, 10),
    "last_by": lambda blink: blink.last_by("Symbol"),
    "last_by_price": lambda blink: blink.last_by("Price"),  # puts Price first
    "last_by_where_in": lambda blink: blink.last_by("Symbol").where_in(rows("AAPL"), "Symbol"),
}


@pytest.mark.parametrize("shape", SHAPES)
def test_every_live_shape_is_described_and_ticks(
    dh_exec_ctx: ExecutionContext,
    rows_when: Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]],
    shape: str,
) -> None:
    stream = Stream()
    scope = new_scope()
    with scope.open():
        result = describe(SHAPES[shape](stream.blink), SPEC, SCHEMA)
        # A blink table empties every cycle; an append-only view of it shows what arrived.
        seen = blink_to_append_only(result) if shape == "blink" else result
    assert_described(result)
    assert result.column_names == list(SCHEMA.column_names)
    assert result.is_refreshing
    assert result.is_blink == (shape == "blink")
    stream.publisher.add(rows("AAPL", "MSFT"))
    # Every row has the same price, so the `last_by` on it keeps only the last row, MSFT.
    only = {"last_by_where_in": ["AAPL"], "last_by_price": ["MSFT"]}
    expected = only.get(shape, ["AAPL", "MSFT"])
    rows_when(seen, lambda arrived: sorted(row["Symbol"] for row in arrived) == expected)
    scope.release()
    stream.scope.release()
    assert stream.shutdown.wait(TIMEOUT)


def test_each_description_of_one_blink_table_is_a_new_table(
    dh_exec_ctx: ExecutionContext,
) -> None:
    # The stream's own blink table is never described, so every call copies it.
    stream = Stream()
    scope = new_scope()
    with scope.open():
        first = describe(stream.blink, SPEC, SCHEMA)
        second = describe(stream.blink, SPEC, SCHEMA)
    assert first is not second
    assert first != second  # two Java tables
    assert first != stream.blink
    assert "TableDescription" not in stream.blink.attributes()
    scope.release()
    stream.scope.release()
    assert stream.shutdown.wait(TIMEOUT)


@pytest.mark.parametrize("shape", ["blink", "append_only"])
def test_releasing_a_described_result_shuts_the_stream_down(
    dh_exec_ctx: ExecutionContext, shape: str
) -> None:
    stream = Stream()
    scope = new_scope()
    with scope.open():
        result = describe(SHAPES[shape](stream.blink), SPEC, SCHEMA)
    stream.scope.release()  # the stream's last hold goes; the described result keeps it
    assert not stream.shutdown.is_set()
    assert result.is_refreshing
    scope.release()
    assert stream.shutdown.wait(TIMEOUT)


@pytest.mark.parametrize("shape", ["blink", "append_only"])
def test_a_table_derived_from_a_described_result_keeps_the_stream_alive(
    dh_exec_ctx: ExecutionContext, shape: str
) -> None:
    stream = Stream()
    scope = new_scope()
    with scope.open():
        result = describe(SHAPES[shape](stream.blink), SPEC, SCHEMA)
    user_scope = new_scope()
    with user_scope.open():
        derived = result.where("Price > 0")
    scope.release()
    stream.scope.release()
    assert not stream.shutdown.is_set()
    assert derived.is_refreshing
    assert column_descriptions(derived) == DESCRIPTIONS  # a filter keeps the descriptions
    user_scope.release()
    assert stream.shutdown.wait(TIMEOUT)


def test_a_failed_blink_table_cannot_be_described(dh_exec_ctx: ExecutionContext) -> None:
    # Copying a table listens to it, so a stream that failed fails every shape, blink too.
    stream = Stream()
    stream.publisher.publish_failure(RuntimeError("the stream failed"))
    assert wait_for(lambda: bool(stream.blink.j_table.isFailed()))
    scope = new_scope()
    with scope.open(), pytest.raises(RuntimeError):
        describe(stream.blink, SPEC, SCHEMA)
    scope.release()
    stream.scope.release()
    assert stream.shutdown.wait(TIMEOUT)


def test_a_description_of_a_missing_column_fails(dh_exec_ctx: ExecutionContext) -> None:
    lacking = new_table([string_col("Symbol", ["AAPL"])])  # no Price column
    with pytest.raises(
        RuntimeError,
        match=r"^describing a stocks\.trades result failed: .*Column\(s\) not found: Price, Note",
    ) as raised:
        describe(lacking, SPEC, SCHEMA)
    assert type(raised.value.__cause__) is RuntimeError  # jpy's form of the Java exception


def test_a_schema_without_descriptions_sets_only_the_table_description(
    dh_exec_ctx: ExecutionContext,
) -> None:
    bare = Schema("stocks.trades", tuple(Column(c.name, c.type) for c in SCHEMA.columns))
    result = describe(rows("AAPL"), SPEC, bare)
    assert "ColumnDescriptions" not in result.attributes()
    assert result.attributes()["TableDescription"] == TABLE_DESCRIPTION


# --- column_table ----------------------------------------------------------------------------


def test_the_column_table_lists_every_column(
    dh_exec_ctx: ExecutionContext,
    dh_to_pylist: Callable[[Table], list[dict[str, object]]],
) -> None:
    table = column_table(SCHEMA)
    assert not table.is_refreshing
    assert [(c.name, c.data_type) for c in table.columns] == [
        ("Name", dtypes.string),
        ("Type", dtypes.string),
        ("Description", dtypes.string),
        ("SuppliedBy", dtypes.string),
    ]
    assert dh_to_pylist(table) == [
        {
            "Name": "Symbol",
            "Type": "String",
            "Description": "The instrument.",
            "SuppliedBy": "WebSocket",
        },
        {
            "Name": "Price",
            "Type": "double",
            "Description": "The price.",
            "SuppliedBy": "REST, flat files, WebSocket",
        },
        {"Name": "Note", "Type": "String", "Description": "", "SuppliedBy": ""},
    ]
    assert column_descriptions(table) == dict(COLUMN_TABLE_DESCRIPTIONS)
    assert list(COLUMN_TABLE_DESCRIPTIONS) == ["Name", "Type", "Description", "SuppliedBy"]
    with pytest.raises(TypeError):  # read-only: no caller can change the header text
        cast("dict[str, str]", COLUMN_TABLE_DESCRIPTIONS)["Name"] = "changed"
