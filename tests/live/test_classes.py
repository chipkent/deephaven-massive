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
