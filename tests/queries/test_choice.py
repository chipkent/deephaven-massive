"""Tests for `queries/_choice.py`: the query class each dataset and `bars()` call gets."""

import dataclasses
from typing import Any

import pytest

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._enums import (
    AssetClass,
    NotBars,
    Timespan,
)
from deephaven_massive._session import CATALOG
from deephaven_massive.queries import (
    FetchQuery,
    HistoryQuery,
    Query,
    StreamHistoryQuery,
    SymbolStreamQuery,
)
from deephaven_massive.queries._choice import (
    bar_query_class,
    bar_stream_condition,
    bar_stream_verbs,
    query_class,
    verbs,
)
from tests._fakes.queries import (
    FF,
)

SESSION_FILES = "us_futures_{venue}/session_aggs_v1"


INDEX_FILES = "us_indices/minute_aggs_v1"


STOCK_BARS = DatasetSpec(
    name="stocks.bars",
    summary="A dataset for the tests.",
    is_bars=True,
    asset_class=AssetClass.STOCKS,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    history_path="/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}",
)


FUTURES_BARS = DatasetSpec(
    name="futures.bars",
    summary="A dataset for the tests.",
    is_bars=True,
    asset_class=AssetClass.FUTURES,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    history_path="/futures/v1/aggs/{ticker}",
    flat_file_prefixes={Timespan.SESSION: SESSION_FILES},
    keys={
        (FF, SESSION_FILES): SourceKeys(
            KeySpec("Ticker"), KeySpec("WindowStart"), date=KeySpec("SessionEndDate")
        )
    },
)


INDEX_BARS = DatasetSpec(
    name="indices.bars",
    summary="A dataset for the tests.",
    is_bars=True,
    asset_class=AssetClass.INDICES,
    flat_file_prefixes={Timespan.MINUTE: INDEX_FILES},
    keys={(FF, INDEX_FILES): SourceKeys(KeySpec("Ticker"), KeySpec("WindowStart"))},
)


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ((STOCK_BARS, Timespan.SECOND, 1), StreamHistoryQuery),
        ((STOCK_BARS, Timespan.MINUTE, 1), StreamHistoryQuery),
        ((STOCK_BARS, Timespan.DAY, 1), HistoryQuery),
        ((STOCK_BARS, Timespan.MINUTE, 5), HistoryQuery),
        ((STOCK_BARS, Timespan.DAY, 2), HistoryQuery),
        ((STOCK_BARS, Timespan.HOUR, 1), HistoryQuery),
        ((FUTURES_BARS, Timespan.MINUTE, 1), StreamHistoryQuery),
        ((FUTURES_BARS, Timespan.SESSION, 1), HistoryQuery),
        ((INDEX_BARS, Timespan.DAY, 1), HistoryQuery),
    ],
)
def test_bar_query_class_selection(case: tuple[DatasetSpec, Timespan, int], expected: type) -> None:
    assert bar_query_class(*case) is expected


def test_bar_timespan_rules() -> None:
    with pytest.raises(ValueError, match="futures bars have no DAY timespan"):
        bar_query_class(FUTURES_BARS, Timespan.DAY, 1)
    with pytest.raises(ValueError, match="SESSION exists only for futures"):
        bar_query_class(STOCK_BARS, Timespan.SESSION, 1)
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan, e\.g\. dm\.Timespan\.DAY"):
        bar_query_class(STOCK_BARS, "day", 1)
    with pytest.raises(ValueError, match="multiplier must be positive"):
        bar_query_class(STOCK_BARS, Timespan.DAY, 0)


@pytest.mark.parametrize("multiplier", [True, 1.0])
def test_bar_multiplier_must_be_an_int(multiplier: object) -> None:
    with pytest.raises(TypeError, match="multiplier must be an int"):
        bar_query_class(STOCK_BARS, Timespan.DAY, multiplier)


_FETCH = {"fetch_path": "/v2/last/trade/{stocksTicker}"}


_HISTORY = {"history_path": "/v3/trades/{stockTicker}"}


_CHANNELS = {"channels": {NotBars.ANY: "T"}}


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({**_FETCH}, FetchQuery),
        ({**_CHANNELS}, SymbolStreamQuery),
        ({**_HISTORY}, HistoryQuery),
        ({"grouped_daily_path": "/v2/aggs/grouped/locale/us/market/stocks/{date}"}, HistoryQuery),
        ({**_CHANNELS, **_HISTORY}, StreamHistoryQuery),
    ],
)
def test_the_query_class_follows_the_transports(
    changes: dict[str, Any], expected: type[Query]
) -> None:
    spec = DatasetSpec(
        name="stocks.trades",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        **changes,
    )
    assert query_class(spec) is expected


def test_flat_files_are_a_history_source() -> None:
    assert query_class(INDEX_BARS) is HistoryQuery
    values = DatasetSpec(
        name="indices.values",
        summary="A dataset for the tests.",
        asset_class=AssetClass.INDICES,
        channels={NotBars.ANY: "V"},
        flat_file_prefixes={NotBars.ANY: "us_indices/values_v1"},
        keys={(FF, "us_indices/values_v1"): SourceKeys(KeySpec("Ticker"), KeySpec("Timestamp"))},
    )
    assert query_class(values) is StreamHistoryQuery


def test_a_bars_dataset_has_the_class_of_its_general_overload() -> None:
    # Every bars spec, whatever its transports; each call picks its own class.
    assert {query_class(STOCK_BARS), query_class(FUTURES_BARS)} == {HistoryQuery}


def test_verbs_are_the_methods_every_query_of_a_dataset_has() -> None:
    by_name = CATALOG.by_name
    assert verbs(by_name["stocks.trades"]) == ("blink", "append_only", "ring", "last_by", "history")
    assert verbs(by_name["stocks.last_trade"]) == ("fetch", "poll")
    # A stream whose rows name no symbol has no `last_by()`.
    assert verbs(by_name["options.admin_messages"]) == ("blink", "append_only", "ring")
    # Only some bars stream, so no stream method is one every bars query has.
    assert verbs(STOCK_BARS) == ("history",)


def test_the_stream_methods_of_bars_name_the_bars_that_stream() -> None:
    assert bar_stream_verbs(STOCK_BARS) == ("blink", "append_only", "ring", "last_by")
    assert bar_stream_condition(STOCK_BARS) == "1-second and 1-minute bars"
    minute_only = dataclasses.replace(STOCK_BARS, channels={Timespan.MINUTE: "AM"})
    assert bar_stream_condition(minute_only) == "1-minute bars"
    # Any other dataset has no stream method that only some of its queries have.
    assert bar_stream_verbs(CATALOG.by_name["stocks.trades"]) == ()
    assert bar_stream_verbs(CATALOG.by_name["stocks.last_trade"]) == ()
