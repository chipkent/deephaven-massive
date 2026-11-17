"""Tests of what the live checks make of Massive's ticker lists (`tests/_live/ticker_lists.py`).

The payloads under `tests/data/live/ticker_lists/` hold rows in Massive's shapes: the futures
spread forms of the vendored samples (`GCQ6-GCZ6`, `BOB 02-01 U6`, `CL:BF Q6-U6-V6`,
`6A:XF:AUDUSD:Q6`) beside an undocumented one, and crypto rows whose currencies join to their
ticker or do not.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm
from tests._live import ticker_lists

if TYPE_CHECKING:
    from collections.abc import Callable

_OWNER = "live/ticker_lists"


def _json(load_fixture: Callable[[str, str], bytes], name: str) -> dict[str, list[dict[str, Any]]]:
    """Read one payload fixture of this module that holds lists of rows by name.

    Returns:
        The decoded JSON.
    """
    payload = json.loads(load_fixture(_OWNER, name))
    assert isinstance(payload, dict)
    return payload


def _products(load_fixture: Callable[[str, str], bytes]) -> list[dict[str, Any]]:
    """Read the combo products of the payload fixture.

    Returns:
        The rows.
    """
    rows = json.loads(load_fixture(_OWNER, "combo_products.json"))
    assert isinstance(rows, list)
    return rows


@pytest.mark.parametrize(
    ("ticker", "form"),
    [
        ("GCQ6-GCZ6", "A9-A9"),
        ("BOB 02-01 U6", "A 9-9 A9"),
        ("CL:BF Q6-U6-V6", "A:A A9-A9-A9"),
        ("6A:XF:AUDUSD:Q6", "9A:A:A:A9"),
        ("ESZ5", "A9"),
    ],
)
def test_a_ticker_form_writes_letter_and_digit_runs_as_one_character(
    ticker: str, form: str
) -> None:
    assert ticker_lists.ticker_form(ticker) == form


def test_accepts_tells_whether_the_check_raised_value_error() -> None:
    assert ticker_lists.accepts(dm.canonical_symbol_future_spread, "GCQ6-GCZ6")
    assert not ticker_lists.accepts(dm.canonical_symbol_future_spread, "ESZ5")


def test_the_legs_of_a_spread_are_its_parts_with_a_contract_shape() -> None:
    assert ticker_lists.ticker_legs("GCQ6-GCZ6") == ["GCQ6", "GCZ6"]
    assert ticker_lists.ticker_legs("10YN6-10YQ6") == ["10YN6", "10YQ6"]
    assert ticker_lists.ticker_legs("CL:BF Q6-U6-V6") == []
    assert ticker_lists.ticker_legs("6A:XF:AUDUSD:Q6") == []


def test_each_venue_read_names_its_anchors_then_its_products_by_strategy_type(
    load_fixture: Callable[[str, str], bytes],
) -> None:
    products = _products(load_fixture)
    reads = ticker_lists.spread_reads(products)
    assert [(read.venue, read.product_codes) for read in reads] == [
        ("XCME", ("ES", "6A", "6A:XF")),
        ("XCBT", ("ZN", "ZC", "BOB")),
        # One butterfly, one calendar, then the next calendar: each type in turn.
        ("XNYM", ("CL", "NG", "CL:BF", "CL:BZ", "NG:BZ")),
        # A combo product with an anchor's code is named once.
        ("XCEC", ("GC", "SI")),
        # Other venues follow, the one with the most products first.
        ("XMGE", ("MW:IC", "MW:BZ")),
        ("XKBT", ("KE:IC",)),
    ]


def test_a_venue_read_names_at_most_the_products_per_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ticker_lists, "PRODUCTS_PER_READ", 2)
    products = [
        {"product_code": code, "strategy_type": kind, "trading_venue": "XNYM"}
        for code, kind in (("A", "x"), ("B", "x"), ("", "y"), ("C", "y"))
    ]
    reads = ticker_lists.spread_reads(products)
    assert reads[2].product_codes == ("CL", "NG", "A", "C")


def test_the_spread_report_writes_every_form_with_examples_and_the_refused_tickers(
    load_fixture: Callable[[str, str], bytes],
) -> None:
    lines = ticker_lists.spread_report(
        _products(load_fixture),
        _json(load_fixture, "combo_contracts.json"),
        spread=dm.canonical_symbol_future_spread,
    )
    assert lines[0] == (
        "combo products: 10, by venue {'XCBT': 1, 'XCEC': 1, 'XCME': 1, 'XKBT': 1, 'XMGE': 2, "
        "'XNYM': 4}"
    )
    assert lines[1] == (
        "    XCBT: strategy types {'inter_commodity': 1}; e.g. [\"BOB 'Soybean Oil Board Crush'\"]"
    )
    # A venue's forms come most tickers first, then by form; the row without a ticker is skipped.
    nymex = lines.index("XNYM combo contracts: 5 tickers in 4 forms")
    assert lines[nymex + 1 : nymex + 6] == [
        (
            "    form 'A:A A9-A9-A9': 2 tickers, the library accepts 2; e.g. "
            "[\"CL:BF Q6-U6-V6 ('CL Butterfly Q6-U6-V6', product CL:BF, legs [])\", "
            "\"CL:BF U6-V6-X6 ('CL Butterfly U6-V6-X6', product CL:BF, legs [])\"]"
        ),
        # An undocumented form is counted, and its tickers are listed as refused.
        (
            "    form 'A A9/A9 A': 1 tickers, the library accepts 0; e.g. "
            "[\"ES Z6/H7 SPR ('An undocumented spread', product ES, legs [])\"]"
        ),
        (
            "    form 'A9-A9': 1 tickers, the library accepts 1; e.g. "
            "[\"CLQ6-CLU6 ('CL Calendar Spread Q6-U6', product CL, legs ['CLQ6', 'CLU6'])\"]"
        ),
        (
            "    form 'A:A A9-A9': 1 tickers, the library accepts 1; e.g. "
            "[\"CL:BZ U6-V6 ('CL Calendar U6-V6', product CL:BZ, legs [])\"]"
        ),
        "    refused by the library: ['ES Z6/H7 SPR']",
    ]
    assert "XCEC combo contracts: 0 tickers in 0 forms" in lines
    assert lines[-1] == "    refused by the library: []"


def test_the_spread_report_shows_at_most_the_refused_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ticker_lists, "REFUSED_SHOWN", 1)
    contracts = {"XNYM": [{"ticker": "ZZ!1"}, {"ticker": "ZZ!2"}]}
    lines = ticker_lists.spread_report([], contracts, spread=dm.canonical_symbol_future_spread)
    assert lines[0] == "combo products: 0, by venue {}"
    assert lines[-1] == "    refused by the library: ['ZZ!1']"


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
