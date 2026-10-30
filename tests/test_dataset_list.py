"""Tests for `dm.datasets()`, the static table of every dataset."""

from collections.abc import Callable
from typing import Any

from deephaven import dtypes
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._dataset_list import DATASET_LIST_DESCRIPTIONS
from deephaven_massive._datasets.column_reference import PAGES_URL
from deephaven_massive._session import CATALOG

ToPylist = Callable[[Table], list[dict[str, Any]]]

_PAGES = PAGES_URL


def _by_dataset(table: Table, to_pylist: ToPylist) -> dict[str, dict[str, Any]]:
    return {row["Dataset"]: row for row in to_pylist(table)}


def test_one_row_per_dataset_in_catalog_order(dh_to_pylist: ToPylist) -> None:
    table = dm.datasets()
    assert not table.is_refreshing
    assert [row["Dataset"] for row in dh_to_pylist(table)] == list(CATALOG.by_name)
    assert {column.name: column.data_type for column in table.columns} == {
        "Dataset": dtypes.string,
        "Call": dtypes.string,
        "Summary": dtypes.string,
        "Verbs": dtypes.string_array,
        "Transports": dtypes.string_array,
        "Documentation": dtypes.string,
    }


def test_each_row_names_its_verbs_transports_and_documentation(dh_to_pylist: ToPylist) -> None:
    rows = _by_dataset(dm.datasets(), dh_to_pylist)
    stream_and_history = ["blink", "append_only", "ring", "last_by", "history"]
    assert rows["stocks.trades"] == {
        "Dataset": "stocks.trades",
        "Call": "m.stocks.trades",
        "Summary": CATALOG.by_name["stocks.trades"].summary,
        "Verbs": stream_and_history,
        "Transports": ["REST", "flat files", "WebSocket"],
        "Documentation": f"{_PAGES}stocks.md#trades",
    }
    assert rows["stocks.last_trade"]["Verbs"] == ["fetch", "poll"]
    assert rows["stocks.last_trade"]["Transports"] == ["REST"]
    # Bars stream at some timespans only; the dataset lists every verb one of its calls has.
    assert rows["stocks.bars"]["Verbs"] == stream_and_history
    ratios = rows["stocks.financials.ratios"]
    assert ratios["Documentation"] == f"{_PAGES}stocks.md#financials-ratios"
    assert rows["reference.snapshots"]["Call"] == "m.snapshots"


def test_the_list_can_be_filtered_by_verb(dh_to_pylist: ToPylist) -> None:
    streams = dm.datasets().where("Arrays.asList(Verbs).contains(`blink`)")
    names = {row["Dataset"] for row in dh_to_pylist(streams)}
    assert "stocks.trades" in names
    assert "stocks.last_trade" not in names


def test_every_column_is_described() -> None:
    table = dm.datasets()
    found = table.attributes()["ColumnDescriptions"]
    descriptions = {
        str(entry.getKey()): str(entry.getValue()) for entry in found.entrySet().toArray()
    }
    assert descriptions == dict(DATASET_LIST_DESCRIPTIONS)
    assert set(descriptions) == {column.name for column in table.columns}
