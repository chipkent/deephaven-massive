"""Tests for the Deephaven side of a dataset schema: types, descriptions, and `columns()`.

The description tests build every result shape directly in the embedded JVM (a static table,
a publisher's blink table, and its append-only, ring, and `last_by` tables) and check that
`describe` gives each one the schema's descriptions and column order without changing what it
is: the copy ticks with its parent, keeps the blink attribute, and keeps its parent, and so the
stream's `on_shutdown_callback`, alive exactly as long as it lives.
"""

import logging
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, merge, new_table
from deephaven.column import double_col, string_col
from deephaven.execution_context import ExecutionContext
from deephaven.stream import blink_to_append_only
from deephaven.table import Table
from deephaven.table_factory import ring_table

from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.schema import Column, ColumnType, Schema
from deephaven_massive._enums import AssetClass, NotBars
from deephaven_massive._massive import Transport
from deephaven_massive._tables import columns
from deephaven_massive._tables.columns import (
    COLUMN_TABLE_DESCRIPTIONS,
    col_defs,
    column_table,
    dataset_table,
    describe,
    dh_type,
)
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive._tables.publisher import blink_publisher

if TYPE_CHECKING:
    from deephaven.liveness_scope import LivenessScope

TIMEOUT = 10.0

SPEC = DatasetSpec(
    name="stocks.trades",
    summary="Trades of US-listed stocks, tick by tick.",
    asset_class=AssetClass.STOCKS,
    channels={NotBars.ANY: "T"},
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
        ColumnType.LONG_ARRAY: dtypes.long_array,
        ColumnType.STRING_ARRAY: dtypes.string_array,
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
        self.deferred: list[LivenessScope] = []  # a failure's pin, released on the reaper
        self.scope = new_scope()
        with self.scope.open():
            self.blink, self.publisher = blink_publisher(
                "massive.test",
                col_defs(SCHEMA),
                on_shutdown=self.shutdown.set,
                release_later=self.deferred.append,
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
    assert stream.shutdown.wait(TIMEOUT)  # the failure's delivery shut it down
    for pin in stream.deferred:
        pin.release()


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


def _string_objects(table: Table, column: str) -> int:
    # Identity is read inside Deephaven: a string crossing to Python is copied.
    ids = table.update(f"Id = java.lang.System.identityHashCode({column})")
    return ids.where(f"!isNull({column})").select_distinct("Id").size


def test_a_dataset_table_shares_one_string_per_date_across_calls(dh_exec_ctx: object) -> None:
    # Rows in runs of one date, as every transport sends them, nulls among them.
    days = [None, *["2025-03-03"] * 1000, *[None] * 3, *["2025-03-04"] * 777, "2025-03-03"]
    rows = pa.table(
        {"Note": pa.array(days), "Date": pa.array(days), "Price": pa.array([1.0] * len(days))}
    )
    first, second = dataset_table(rows, ("Date",)), dataset_table(rows, ("Date",))
    # The column holds one string per distinct date, the same one in every table the process
    # builds; any other text column is as made. The columns keep their order and values.
    both = merge([first, second])
    assert _string_objects(both, "Date") == 2
    assert _string_objects(first, "Note") > 2
    assert first.column_names == ["Note", "Date", "Price"]
    assert dh_arrow.to_arrow(first).column("Date").to_pylist() == days
    assert dh_arrow.to_arrow(first).column("Price").to_pylist() == [1.0] * len(days)


def test_dates_in_runs_never_take_the_formula(
    dh_exec_ctx: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Rows in runs, as every transport sends them, are built from the pool: the formula, whose
    # fixed cost would fall on every flush, is only for interleaved dates.
    def refused(*_: object) -> Table:
        raise AssertionError("the formula path was taken")

    monkeypatch.setattr(columns, "_interned", refused)
    days = [*["2025-03-03"] * 500, None, *["2025-03-04"] * 500]
    table = dataset_table(
        pa.table({"Date": pa.array(days), "Price": pa.array([1.0] * 1001)}), ("Date",)
    )
    assert _string_objects(table, "Date") == 2


def test_interleaved_dates_are_shared_through_a_formula(dh_exec_ctx: object) -> None:
    # More runs than are worth copying one by one: the column is interned by a formula.
    days = [None if n % 7 == 0 else f"2025-03-0{1 + n % 3}" for n in range(30_000)]
    rows = pa.table({"Date": pa.array(days), "Note": pa.array(days)})
    table = dataset_table(rows, ("Date",))
    assert _string_objects(table, "Date") == 3
    assert _string_objects(table, "Note") > 3
    assert dh_arrow.to_arrow(table).column("Date").to_pylist() == days


def test_an_empty_dataset_table_keeps_its_columns(dh_exec_ctx: object) -> None:
    rows = pa.table({"Date": pa.array([], pa.string()), "Price": pa.array([], pa.float64())})
    table = dataset_table(rows, ("Date",))
    assert (table.size, table.column_names) == (0, ["Date", "Price"])


def test_a_dataset_table_without_dates_is_converted_as_is(dh_exec_ctx: object) -> None:
    table = dataset_table(pa.table({"Price": pa.array([1.5])}), ())
    assert table.size == 1


_STRINGS = pa.list_(pa.string())


def test_list_columns_without_strings_keep_every_later_column(
    dh_exec_ctx: object, caplog: pytest.LogCaptureFixture
) -> None:
    # A list-of-strings column with no string in it, ahead of other columns, would make
    # Deephaven's reader fail on a later list and shift the values of any other later column.
    rows = pa.table(
        {
            "Keywords": pa.array([None, None, None], _STRINGS),
            "Empty": pa.array([[], None, []], _STRINGS),
            "Tickers": pa.array([["AAPL"], None, ["MSFT", "UBS"]], _STRINGS),
            "Ids": pa.array([None, None, None], pa.list_(pa.int64())),
            "Date": pa.array(["2025-03-03", "2025-03-03", None]),
            "Size": pa.array([1, 2, 3]),
        }
    )
    with caplog.at_level(logging.DEBUG, logger=columns.__name__):
        table = dataset_table(rows, ("Date",))
    assert table.column_names == rows.column_names
    assert dh_arrow.to_arrow(table).to_pylist() == rows.to_pylist()
    assert "converting the list columns ['Keywords', 'Empty'] of 3 rows alone" in caplog.text


def test_a_list_without_strings_in_one_record_batch_is_converted_alone(
    dh_exec_ctx: object,
) -> None:
    # The record batches split where any column's chunks do, so the first batch of `Keywords`
    # holds no string although the column does, and no date column is needed to take this path.
    rows = pa.table(
        {
            "Keywords": pa.chunked_array([pa.array([None, ["rates"]], _STRINGS)]),
            "Tickers": pa.chunked_array([pa.array([["AAPL"]], _STRINGS), [["UBS"]]]),
            "Size": pa.chunked_array([[1], [2]]),
        }
    )
    table = dataset_table(rows, ())
    assert dh_arrow.to_arrow(table).to_pylist() == rows.to_pylist()


def test_interleaved_dates_beside_a_list_without_strings_keep_their_values(
    dh_exec_ctx: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The formula interns the dates; they are never also copied run by run.
    def refused(*_: object) -> object:
        raise AssertionError("the dates were copied run by run")

    monkeypatch.setattr(columns._DateRuns, "strings", refused)
    days = [None if n % 7 == 0 else f"2025-03-0{1 + n % 3}" for n in range(30_000)]
    rows = pa.table(
        {
            "Keywords": pa.array([None] * len(days), _STRINGS),
            "Date": pa.array(days),
            "Size": pa.array(range(len(days))),
        }
    )
    table = dataset_table(rows, ("Date",))
    assert _string_objects(table, "Date") == 3
    assert dh_arrow.to_arrow(table).to_pylist() == rows.to_pylist()


def test_lists_that_hold_strings_convert_with_the_other_columns(
    dh_exec_ctx: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Only a list of strings with none in a batch is converted apart; other columns cost
    # one conversion.
    converted: list[list[str]] = []
    convert = dh_arrow.to_table

    def counted(rows: pa.Table) -> Table:
        converted.append(rows.column_names)
        return convert(rows)

    monkeypatch.setattr(dh_arrow, "to_table", counted)
    rows = pa.table(
        {
            "Tickers": pa.array([["AAPL"], None], _STRINGS),
            "Ids": pa.array([None, None], pa.list_(pa.int64())),
        }
    )
    table = dataset_table(rows, ())
    assert converted == [["Tickers", "Ids"]]
    assert dh_arrow.to_arrow(table).to_pylist() == rows.to_pylist()
