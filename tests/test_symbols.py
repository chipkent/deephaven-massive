"""Unit tests for SymbolNormalizer across all 6 market domains."""

from __future__ import annotations

import pytest

from deephaven_massive.engine.symbols import SymbolNormalizer


def test_symbol_normalizer_rejects_unsupported_domain() -> None:
    """Verify SymbolNormalizer raises ValueError when initialized with an unknown domain."""
    with pytest.raises(ValueError, match="Unsupported domain"):
        SymbolNormalizer("commodities")


@pytest.mark.parametrize(
    ("domain", "raw", "expected"),
    [
        ("stocks", " aapl ", "AAPL"),
        ("stocks", "*", "*"),
        ("futures", "esz5", "ESZ5"),
        ("options", "AAPL251219C00150000", "O:AAPL251219C00150000"),
        ("options", "O:AAPL251219C00150000", "O:AAPL251219C00150000"),
        ("options", "O:*", "*"),
        ("indices", "spx", "I:SPX"),
        ("indices", "I:SPX", "I:SPX"),
        ("indices", "I:*", "*"),
        ("forex", "eur/usd", "EUR/USD"),
        ("forex", "C:EUR-USD", "EUR/USD"),
        ("forex", "C:EURUSD", "EUR/USD"),
        ("forex", "C:USDMXN", "USD/MXN"),
        ("forex", "C:USDTHB", "USD/THB"),
        ("forex", "C:*", "*"),
        ("crypto", "btc/usd", "BTC-USD"),
        ("crypto", "X:BTC-USD", "BTC-USD"),
        ("crypto", "X:BTCUSD", "BTC-USD"),
        ("crypto", "X:OPUSDT", "OP-USDT"),
        ("crypto", "X:SHIBUSDT", "SHIB-USDT"),
        ("crypto", "X:*", "*"),
    ],
)
def test_symbol_normalizer_canonical_formatting(domain: str, raw: str, expected: str) -> None:
    """Verify canonical symbol formatting across all 6 market domains and wildcard inputs."""
    normalizer = SymbolNormalizer(domain)
    assert normalizer.canonical(raw) == expected


@pytest.mark.parametrize(
    ("domain", "raw", "expected"),
    [
        ("stocks", "aapl", "AAPL"),
        ("stocks", "*", "*"),
        ("futures", "esz5", "ESZ5"),
        ("options", "AAPL251219C00150000", "O:AAPL251219C00150000"),
        ("indices", "spx", "I:SPX"),
        ("forex", "EUR/USD", "C:EURUSD"),
        ("forex", "C:*", "*"),
        ("crypto", "BTC-USD", "X:BTCUSD"),
        ("crypto", "X:*", "*"),
    ],
)
def test_symbol_normalizer_rest_ticker_formatting(domain: str, raw: str, expected: str) -> None:
    """Verify REST path ticker formatting strips separators and applies domain prefixes."""
    normalizer = SymbolNormalizer(domain)
    assert normalizer.rest_ticker(raw) == expected


@pytest.mark.parametrize(
    ("domain", "channel", "raw", "expected"),
    [
        ("stocks", "T", "aapl", "T.AAPL"),
        ("stocks", "T", "*", "T.*"),
        ("futures", "T", "esz5", "T.ESZ5"),
        ("options", "T", "AAPL251219C00150000", "T.O:AAPL251219C00150000"),
        ("options", "T", "*", "T.O:*"),
        ("indices", "V", "spx", "V.I:SPX"),
        ("indices", "V", "*", "V.I:*"),
        ("forex", "C", "C:EURUSD", "C.EUR/USD"),
        ("forex", "C", "*", "C.*"),
        ("crypto", "XT", "X:BTCUSD", "XT.BTC-USD"),
        ("crypto", "XT", "*", "XT.*"),
    ],
)
def test_symbol_normalizer_ws_topic_formatting(
    domain: str, channel: str, raw: str, expected: str
) -> None:
    """Verify WebSocket topic formatting across all domains including domain-specific wildcards."""
    normalizer = SymbolNormalizer(domain)
    assert normalizer.ws_topic(channel, raw) == expected


def test_symbol_normalizer_canonical_list_deduplicates_preserving_order() -> None:
    """Verify canonical_list accepts strings or iterables and deduplicates in insertion order."""
    stocks = SymbolNormalizer("stocks")
    assert stocks.canonical_list("aapl") == ["AAPL"]
    assert stocks.canonical_list(["aapl", "msft", "AAPL", "nvda", "MSFT"]) == [
        "AAPL",
        "MSFT",
        "NVDA",
    ]


def test_symbol_normalizer_validation_errors() -> None:
    """Verify SymbolNormalizer rejects empty symbols, empty channels, invalid pairs, and empty lists."""
    stocks = SymbolNormalizer("stocks")
    with pytest.raises(ValueError, match="Symbol cannot be empty"):
        stocks.canonical("   ")

    with pytest.raises(ValueError, match="WebSocket channel cannot be empty"):
        stocks.ws_topic("  ", "AAPL")

    with pytest.raises(ValueError, match="At least one symbol"):
        stocks.canonical_list([])

    forex = SymbolNormalizer("forex")
    with pytest.raises(ValueError, match="Cannot parse currency/crypto pair"):
        forex.canonical("INVALIDPAIRXYZ")
    with pytest.raises(ValueError, match="Cannot parse currency/crypto pair"):
        forex.canonical("A/B/C")
