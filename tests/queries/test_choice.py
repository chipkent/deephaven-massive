"""Tests for `queries/_choice.py`: the query class each dataset and `bars()` call gets."""

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


def test_every_bars_dataset_gives_one_streaming_class() -> None:
    # Every call of `bars()` returns this class; its stream methods refuse bars that don't stream.
    assert query_class(STOCK_BARS) is StreamHistoryQuery
    assert query_class(FUTURES_BARS) is StreamHistoryQuery


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
    # Any other dataset has no stream method that only some of its queries have.
    assert bar_stream_verbs(CATALOG.by_name["stocks.trades"]) == ()
    assert bar_stream_verbs(CATALOG.by_name["stocks.last_trade"]) == ()
