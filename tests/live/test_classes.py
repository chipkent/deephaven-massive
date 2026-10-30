"""Tests of the asset class a raw request reads (`tests/_live/classes.py`)."""

from __future__ import annotations

import pytest

from tests._live import classes


@pytest.mark.parametrize(
    ("template", "params", "bucket"),
    [
        ("/futures/v1/aggs/{ticker}", {"ticker": "ESZ6"}, "futures"),
        ("/v2/aggs/ticker/{stocksTicker}/prev", {"stocksTicker": "AAPL"}, "stocks"),
        ("/v2/aggs/ticker/{cryptoTicker}/prev", {"cryptoTicker": "X:BTCUSD"}, "currencies"),
        ("/v2/aggs/ticker/{indicesTicker}/prev", {"indicesTicker": "I:NDX"}, "indices"),
        ("/v3/trades/{optionsTicker}", {"optionsTicker": "O:SPY261218C00600000"}, "options"),
        ("/v3/snapshot/options/{underlyingAsset}", {"underlyingAsset": "SPY"}, "options"),
        ("/v2/snapshot/locale/global/markets/forex/tickers", {}, "currencies"),
        ("/v1/marketstatus/now", {}, "other"),
    ],
)
def test_the_bucket_of_a_request(template: str, params: dict[str, object], bucket: str) -> None:
    assert classes.bucket_of(template, params) == bucket


@pytest.mark.parametrize(
    ("path", "name"),
    [
        ("/futures/v1/aggs/ESZ3", "futures"),
        ("/v2/aggs/ticker/O:NVDA240621C00120000/range/1/day/2023-06-05/2023-06-12", "options"),
        ("/v2/aggs/ticker/C:EURUSD/prev", "currencies"),
        ("/v3/snapshot/indices", "indices"),
        ("/v2/aggs/ticker/AAPL/range/1/day/2023-01-02/2023-01-09", "stocks"),
        ("/v3/quotes/AAPL", "stocks"),
        ("/v3/reference/tickers/types", None),
    ],
)
def test_the_class_of_a_sent_path(path: str, name: str | None) -> None:
    assert classes.class_of_path(path) == name


_PAID = {"stocks": "starter", "options": "starter", "indices": "starter", "currencies": "starter"}


@pytest.mark.parametrize(
    ("basic", "expected"),
    [
        ("indices", classes.MarketLookup("indices", ("indices",), "fx")),
        ("currencies", classes.MarketLookup("currencies", ("fx", "crypto"), "stocks")),
        # The last class takes its control market from the first.
        ("stocks", classes.MarketLookup("stocks", ("stocks",), "indices")),
    ],
)
def test_a_market_lookup_spends_a_class_on_basic_and_controls_with_another(
    basic: str, expected: classes.MarketLookup
) -> None:
    assert classes.market_lookup({**_PAID, basic: "basic", "futures": "basic"}) == expected


def test_no_market_lookup_without_a_class_whose_tickers_a_market_lists_on_basic() -> None:
    assert classes.market_lookup({**_PAID, "options": "basic", "futures": "basic"}) is None
