"""Tests of what the live check makes of Massive's crypto list (`tests/_live/ticker_lists.py`).

The payload under `tests/data/live/ticker_lists/` holds crypto rows in Massive's shape whose
currencies join to their ticker or do not.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast

from tests._live import ticker_lists

if TYPE_CHECKING:
    from collections.abc import Callable

    import pytest

_OWNER = "live/ticker_lists"


def _json(load_fixture: Callable[[str, str], bytes], name: str) -> dict[str, list[dict[str, Any]]]:
    """Read one payload fixture of this module that holds lists of rows by name.

    Returns:
        The decoded JSON.
    """
    return cast("dict[str, list[dict[str, Any]]]", json.loads(load_fixture(_OWNER, name)))


def test_the_crypto_report_writes_every_row_the_library_cannot_split(
    load_fixture: Callable[[str, str], bytes],
) -> None:
    tickers = _json(load_fixture, "crypto_tickers.json")
    lines = ticker_lists.crypto_report(
        tickers["listed"], tickers["delisted"], listed_limit=5, delisted_limit=None
    )
    assert lines == [
        "listed crypto tickers: 5 rows (may be cut short at the read's limit)",
        "delisted crypto tickers: 3 rows",
        "tickers in both reads (the library keeps the listed row): 1, e.g. ['X:DOTUSD']",
        "tickers without the X: prefix: ['BTC-EUR']",
        "rows whose currencies do not join to the ticker: 3",
        "    X:1INCHUSD (listed): base '1INCH', quote None",
        "    X:BTCPYUSD (listed): base 'BTCP', quote 'USD'",
        "    BTC-EUR (delisted): base 'BTC', quote 'EUR'",
    ]


def test_the_crypto_report_shows_at_most_the_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ticker_lists, "SHARED_SHOWN", 1)
    monkeypatch.setattr(ticker_lists, "UNJOINED_SHOWN", 1)
    rows = [{"ticker": "X:AUSD"}, {"ticker": "X:BUSD"}]
    lines = ticker_lists.crypto_report(rows, rows, listed_limit=None, delisted_limit=None)
    assert (
        lines[2] == "tickers in both reads (the library keeps the listed row): 2, e.g. ['X:AUSD']"
    )
    assert lines[4:] == [
        "rows whose currencies do not join to the ticker: 4",
        "    X:AUSD (listed): base None, quote None",
    ]
