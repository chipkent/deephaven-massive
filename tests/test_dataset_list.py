"""Tests for `dm.datasets()`, the static table of every dataset."""

from collections.abc import Callable, Iterator
from typing import Any

import pytest
from deephaven import dtypes
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive import _dataset_list
from deephaven_massive._dataset_list import DATASET_LIST_DESCRIPTIONS, dataset_calls
from deephaven_massive._datasets.column_reference import PAGES_URL, DatasetCall
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
        "Documentation": f"{_PAGES}stocks.trades.md",
    }
    assert rows["stocks.last_trade"]["Verbs"] == ["fetch", "poll"]
    assert rows["stocks.last_trade"]["Transports"] == ["REST"]
    # Bars stream at some timespans only; the dataset lists every verb one of its calls has.
    assert rows["stocks.bars"]["Verbs"] == stream_and_history
    ratios = rows["stocks.financials.ratios"]
    assert ratios["Documentation"] == f"{_PAGES}stocks.financials.ratios.md"
    assert rows["snapshots"]["Call"] == "m.snapshots"
    assert rows["snapshots"]["Documentation"] == f"{_PAGES}snapshots.md"


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


# --- dataset_calls -----------------------------------------------------------------------------


def test_every_dataset_has_its_call() -> None:
    calls = dataset_calls()
    assert list(calls) == list(CATALOG.by_name)
    assert calls["stocks.bars"] == DatasetCall(
        signature="m.stocks.bars(symbols, timespan, *, multiplier=1, adjusted=True)",
        verbs=("blink", "append_only", "ring", "last_by", "history"),
        arguments=("symbols", "timespan", "multiplier", "adjusted"),
        typed=("symbols", "timespan", "multiplier", "adjusted"),
    )
    # The session's own dataset method, and defaults written as a user types them.
    assert calls["snapshots"].signature.startswith("m.snapshots(symbols, *, sort=None")
    assert "timespan=dm.Timespan.DAY" in calls["indicators.sma"].signature
    assert "venues=frozenset(dm.FuturesVenue)" in calls["futures.trades"].signature


def test_arguments_named_apart_from_their_wire_parameter() -> None:
    tickers = dataset_calls()["reference.tickers"]
    # `ticker_type` sets `type` and `as_of` sets `date`; `cik` is its own wire name.
    assert {"type", "date", "cik"} <= set(tickers.typed)
    assert "ticker_type" not in tickers.typed
    assert "id" in dataset_calls()["reference.conditions"].typed
    # An argument that sets no wire parameter keeps its name.
    assert "include_otc" in dataset_calls()["stocks.snapshots"].typed


def test_a_default_set_of_some_members_is_written_as_its_repr() -> None:
    some = frozenset({dm.FuturesVenue.CME})
    assert _dataset_list._default(some) == repr(some)
    assert _dataset_list._default(frozenset()) == "frozenset()"


def test_a_dataset_without_a_method_or_a_method_without_a_dataset_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real = _dataset_list._dataset_methods

    def extra() -> Iterator[tuple[str, Callable[..., object]]]:
        yield from ((name, method) for name, method in real() if name != "stocks.trades")
        yield "stocks.nothing", print

    monkeypatch.setattr(_dataset_list, "_dataset_methods", extra)
    with pytest.raises(
        LookupError,
        match=r"without a method: \['stocks.trades'\]; .*"
        r"without a dataset: \['stocks.nothing'\]",
    ):
        dataset_calls()
