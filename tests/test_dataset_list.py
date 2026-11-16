"""Tests for `dm.datasets()`, the static table of every dataset."""

from collections.abc import Callable, Iterator
from typing import Any

import pytest
from deephaven import dtypes
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive import _dataset_list
from deephaven_massive._dataset_list import DATASET_LIST_DESCRIPTIONS, dataset_calls
from deephaven_massive._datasets.column_reference import PAGES_URL, DatasetCall, QueryMethod
from deephaven_massive._datasets.filters import filter_keys
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
        "Methods": dtypes.string_array,
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
        "Methods": stream_and_history,
        "Transports": ["REST", "flat files", "WebSocket"],
        "Documentation": f"{_PAGES}stocks.trades.md",
    }
    assert rows["stocks.last_trade"]["Methods"] == ["fetch", "poll"]
    assert rows["stocks.last_trade"]["Transports"] == ["REST"]
    # Bars stream at some timespans only: each stream method names the bars that have it.
    bars = "(1-second and 1-minute bars at multiplier=1)"
    assert rows["stocks.bars"]["Methods"] == [
        "history",
        f"blink {bars}",
        f"append_only {bars}",
        f"ring {bars}",
        f"last_by {bars}",
    ]
    ratios = rows["stocks.financials.ratios"]
    assert ratios["Documentation"] == f"{_PAGES}stocks.financials.ratios.md"
    assert rows["snapshots"]["Call"] == "m.snapshots"
    assert rows["snapshots"]["Documentation"] == f"{_PAGES}snapshots.md"


def test_the_list_can_be_filtered_by_verb(dh_to_pylist: ToPylist) -> None:
    streams = dm.datasets().where("Arrays.asList(Methods).contains(`blink`)")
    names = {row["Dataset"] for row in dh_to_pylist(streams)}
    assert "stocks.trades" in names
    assert "stocks.last_trade" not in names
    # Not every bars query streams, so the bars datasets are not listed as plain streams.
    assert "stocks.bars" not in names


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
        signature="m.stocks.bars(symbols, timespan, *, multiplier=1, adjusted=True, on_error=None)",
        verbs=("history",),
        arguments=("symbols", "timespan", "multiplier", "adjusted", "on_error"),
        typed=("symbols", "timespan", "multiplier", "adjusted", "on_error"),
        # Each argument's entry of the docstring's `Args`, its lines joined.
        descriptions=(
            "Stock tickers, or `dm.ALL`.",
            "The bar's unit. Massive refuses `dm.Timespan.SESSION`, a futures unit.",
            "Units per bar.",
            "Adjusts volume as well as prices.",
            "What a bad symbol or row does in this call (`dm.ErrorPolicy`).",
        ),
        # Each verb's call, with its positional arguments, and its docstring's summary.
        methods=calls["stocks.bars"].methods,
        # The stream methods that only 1-second and 1-minute bars have.
        stream_verbs=("blink", "append_only", "ring", "last_by"),
        stream_condition="1-second and 1-minute bars at multiplier=1",
    )
    methods = {m.name: m for m in calls["stocks.bars"].methods}
    assert list(methods) == ["history", "blink", "append_only", "ring", "last_by"]
    assert methods["ring"] == QueryMethod(
        "ring", "ring(capacity)", "The latest `capacity` rows the stream sends."
    )
    assert methods["history"].call == "history(start, end=None)"
    assert methods["last_by"].call == "last_by(by='Symbol')"
    poll = calls["stocks.dividends"].methods[1]
    assert poll == QueryMethod(
        "poll", "poll(interval)", "A live table that fetches the query again every `interval`."
    )
    # An entry that runs over several lines is joined into one.
    assert calls["stocks.previous_day_bar"].descriptions[0] == (
        "Stock tickers. `dm.ALL` is refused; for every ticker's bar of one day, use "
        "`m.stocks.bars(dm.ALL, dm.Timespan.DAY).history(day, day)`."
    )
    # The session's own dataset method, and defaults written as a user types them.
    assert calls["snapshots"].signature.startswith("m.snapshots(symbols, *, sort=None")
    assert "timespan=dm.Timespan.DAY" in calls["indicators.sma"].signature
    assert "venues=None, on_error=None" in calls["futures.trades"].signature
    # Every dataset method takes the call's error policy, last.
    assert all(call.signature.endswith("on_error=None)") for call in calls.values())


def test_arguments_named_apart_from_their_wire_parameter() -> None:
    tickers = dataset_calls()["reference.tickers"]
    # `ticker_type` sets `type` and `as_of` sets `date`; `cik` is its own wire name.
    assert {"type", "date", "cik"} <= set(tickers.typed)
    assert "ticker_type" not in tickers.typed
    assert "id" in dataset_calls()["reference.conditions"].typed
    # `underlying` sets the one wire parameter that starts with it, `underlying_ticker`.
    assert "underlying_ticker" in dataset_calls()["options.contracts"].typed
    # An argument that sets no wire parameter keeps its name.
    assert "include_otc" in dataset_calls()["stocks.snapshots"].typed


def test_no_filters_key_the_reference_lists_is_refused_as_an_argument(
    massive_session: dm.MassiveSession,
) -> None:
    # Every key a dataset page lists must reach the filter check, never the refusal of a key
    # that has its own argument: a wrong wire name for an argument would list such a key.
    refused = []
    for name, call in dataset_calls().items():
        if "filters" not in call.arguments:
            continue
        method: Any = massive_session
        for part in name.split("."):
            method = getattr(method, part)
        required = [
            "AAPL" if argument == "symbols" else dm.Timespan.DAY
            for argument in call.arguments
            if f" {argument}," in f" {call.signature.split('(', 1)[1]}".split("*")[0]
        ]
        for key in filter_keys(CATALOG, CATALOG.by_name[name], typed=call.typed):
            try:
                method(*required, filters={key: "1"})
            except (TypeError, ValueError) as error:
                if "its own argument" in str(error) or "not a filter" in str(error):
                    refused.append(f"{name}: {key}")
    assert refused == []


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


def test_every_argument_of_every_dataset_is_described() -> None:
    undescribed = [
        f"{name}: {argument}"
        for name, call in dataset_calls().items()
        for argument, text in zip(call.arguments, call.descriptions, strict=True)
        if not text
    ]
    assert undescribed == []


def test_an_args_section_that_ends_the_docstring_keeps_its_last_entry() -> None:
    def documented(symbols: object, limit: object) -> None:
        """Read rows.

        Args:
            symbols: The symbols.
            limit: The most rows.
        """

    assert _dataset_list._argument_lines(documented) == {
        "symbols": "The symbols.",
        "limit": "The most rows.",
    }


def test_a_method_without_an_args_section_has_no_argument_lines() -> None:
    def undocumented(self: object, symbols: object) -> None:
        """Read nothing."""

    assert _dataset_list._argument_lines(undocumented) == {}
