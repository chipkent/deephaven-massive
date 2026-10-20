"""Tests for SymbolNormalizer across all 6 market domains."""

from __future__ import annotations

import pytest

from deephaven_massive.engine.symbols import SymbolNormalizer


def test_symbol_normalizer_domains() -> None:
    with pytest.raises(ValueError, match="Unsupported domain"):
        SymbolNormalizer("commodities")

    stocks = SymbolNormalizer("stocks")
    assert stocks.canonical(" aapl ") == "AAPL"
    assert stocks.canonical("*") == "*"
    assert stocks.rest_ticker("aapl") == "AAPL"
    assert stocks.rest_ticker("*") == "*"
    assert stocks.ws_topic("T", "aapl") == "T.AAPL"
    assert stocks.ws_topic("T", "*") == "T.*"

    futures = SymbolNormalizer("futures")
    assert futures.canonical("esz5") == "ESZ5"
    assert futures.rest_ticker("esz5") == "ESZ5"
    assert futures.ws_topic("T", "esz5") == "T.ESZ5"

    options = SymbolNormalizer("options")
    assert options.canonical("AAPL251219C00150000") == "O:AAPL251219C00150000"
    assert options.canonical("O:AAPL251219C00150000") == "O:AAPL251219C00150000"
    assert options.canonical("O:*") == "*"
    assert options.ws_topic("T", "AAPL251219C00150000") == "T.O:AAPL251219C00150000"
    assert options.ws_topic("T", "*") == "T.O:*"

    indices = SymbolNormalizer("indices")
    assert indices.canonical("spx") == "I:SPX"
    assert indices.canonical("I:SPX") == "I:SPX"
    assert indices.canonical("I:*") == "*"
    assert indices.ws_topic("V", "spx") == "V.I:SPX"
    assert indices.ws_topic("V", "*") == "V.I:*"

    forex = SymbolNormalizer("forex")
    assert forex.canonical("eur/usd") == "EUR/USD"
    assert forex.canonical("C:EUR-USD") == "EUR/USD"
    assert forex.canonical("C:EURUSD") == "EUR/USD"
    assert forex.canonical("C:*") == "*"
    assert forex.rest_ticker("EUR/USD") == "C:EURUSD"
    assert forex.ws_topic("C", "C:EURUSD") == "C.EUR/USD"
    assert forex.ws_topic("C", "*") == "C.*"

    crypto = SymbolNormalizer("crypto")
    assert crypto.canonical("btc/usd") == "BTC-USD"
    assert crypto.canonical("X:BTC-USD") == "BTC-USD"
    assert crypto.canonical("X:BTCUSD") == "BTC-USD"
    assert crypto.canonical("X:SHIBUSDT") == "SHIB-USDT"
    assert crypto.canonical("X:*") == "*"
    assert crypto.rest_ticker("BTC-USD") == "X:BTCUSD"
    assert crypto.ws_topic("XT", "X:BTCUSD") == "XT.BTC-USD"
    assert crypto.ws_topic("XT", "*") == "XT.*"


def test_symbol_normalizer_errors_and_lists() -> None:
    stocks = SymbolNormalizer("stocks")
    with pytest.raises(ValueError, match="Symbol cannot be empty"):
        stocks.canonical("   ")

    with pytest.raises(ValueError, match="WebSocket channel cannot be empty"):
        stocks.ws_topic("  ", "AAPL")

    forex = SymbolNormalizer("forex")
    with pytest.raises(ValueError, match="Cannot parse currency/crypto pair"):
        forex.canonical("INVALIDPAIRXYZ")
    with pytest.raises(ValueError, match="Cannot parse currency/crypto pair"):
        forex.canonical("A/B/C")

    assert stocks.canonical_list("aapl") == ["AAPL"]
    assert stocks.canonical_list(["aapl", "msft", "AAPL"]) == ["AAPL", "MSFT"]
    with pytest.raises(ValueError, match="At least one symbol"):
        stocks.canonical_list([])
