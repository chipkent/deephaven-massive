"""Live checks of the symbol rule against Massive's own tickers, and of how Massive reads case.

The library sends every ticker as written, in any case. It refuses only text no request can
carry (empty, whitespace at either end, a control character, `,`, or `*`), a symbol without its
class's prefix (`O:`, `I:`, `C:`, `X:`), and a forex or crypto pair written with `-` or `/`.
These tests page Massive's own list of each class and check that the rule accepts every ticker
on it, and that each converts to and from each transport unchanged. They also record what
Massive answers to a ticker in another case, and to a forex pair written with a dash. Each
test records what Massive sent (`evidence`). How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

import pytest

import deephaven_massive as dm
from deephaven_massive._datasets.symbols import check_symbol, class_spelling_reason
from deephaven_massive._datasets.wire_symbols import canonical_from_wire, wire_spellings, ws_symbol
from deephaven_massive._enums import AssetClass
from deephaven_massive._massive import RestClient, Transport, source
from tests._live import bad_values

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from typing import TypeAlias

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"

_TICKERS: Final = "/v3/reference/tickers"
_OPTION_CONTRACTS: Final = "/v3/reference/options/contracts"
_FUTURES_CONTRACTS: Final = "/futures/v1/contracts"
_PAGE: Final = 1000
"""Rows of one page: the largest each list serves, read once, so each list costs one request."""

_SHOWN: Final = 20
"""The most refused or changed tickers the evidence names, per list."""


def _first_page(
    client: RestClient, path: str, query: Mapping[str, object], *, bucket: str
) -> list[dict[str, Any]]:
    """Read one page of a list.

    Args:
        client: The wire REST client.
        path: The list's path.
        query: Its query parameters, the page size among them.
        bucket: The rate bucket of its class.

    Returns:
        The page's rows, as Massive sent them.
    """
    pages = client.pages(source(Transport.REST, path), {}, query, bucket=bucket, limit=_PAGE)
    return [row for page in pages for row in page.rows]


def _crypto_pairs(rows: Sequence[Mapping[str, Any]]) -> dict[str, tuple[str, str]]:
    """Return each crypto ticker's currencies whose join is its ticker, as the library reads them.

    Args:
        rows: Rows of the crypto ticker list.

    Returns:
        The base and quote of each ticker that splits.
    """
    pairs = {}
    for row in rows:
        ticker, base, quote = (
            row.get("ticker"),
            row.get("base_currency_symbol"),
            row.get("currency_symbol"),
        )
        if isinstance(base, str) and isinstance(quote, str) and f"X:{base}{quote}" == ticker:
            pairs[ticker] = (base, quote)
    return pairs


def _problems(
    asset_class: AssetClass, tickers: Sequence[str], pairs: Mapping[str, tuple[str, str]]
) -> list[str]:
    """Return how the library would fail each ticker: refuse it, change it, or not match it.

    Args:
        asset_class: The list's class.
        tickers: Its tickers.
        pairs: The currencies of each crypto ticker that splits; empty for another class.

    Returns:
        One description per ticker the rule refuses or a conversion changes.
    """
    found = []
    for ticker in tickers:
        try:
            check_symbol(ticker)
        except ValueError as error:
            found.append(f"{ticker!r}: refused: {error}")
            continue
        reason = class_spelling_reason(asset_class, ticker)
        if reason is not None:
            found.append(f"{ticker!r}: refused: {reason}")
        if dm.canonical_symbol(ticker, asset_class) != ticker:
            found.append(
                f"{ticker!r}: canonical_symbol gives {dm.canonical_symbol(ticker, asset_class)!r}"
            )
        if not wire_spellings(asset_class, ticker):
            found.append(f"{ticker!r}: no flat-file spelling reads back as it")
        if asset_class is AssetClass.CRYPTO and ticker not in pairs:
            continue  # its currencies do not join; item 49 records such pairs
        topic = ws_symbol(asset_class, ticker, pairs)
        if canonical_from_wire(asset_class, topic) != ticker:
            found.append(f"{ticker!r}: stream topic {topic!r} reads back as another ticker")
    return found


@pytest.mark.verifies("119")
@pytest.mark.requests(1)
@pytest.mark.timeout(120)
@pytest.mark.parametrize(
    ("asset_class", "path", "query", "bucket"),
    [
        pytest.param(
            AssetClass.STOCKS,
            _TICKERS,
            {"market": "stocks", "limit": _PAGE},
            "stocks",
            marks=pytest.mark.needs("stocks:basic"),
            id="stocks",
        ),
        pytest.param(
            AssetClass.INDICES,
            _TICKERS,
            {"market": "indices", "limit": _PAGE},
            "indices",
            marks=pytest.mark.needs("indices:basic"),
            id="indices",
        ),
        pytest.param(
            AssetClass.FOREX,
            _TICKERS,
            {"market": "fx", "limit": _PAGE},
            "currencies",
            marks=pytest.mark.needs("currencies:basic"),
            id="forex",
        ),
        pytest.param(
            AssetClass.CRYPTO,
            _TICKERS,
            {"market": "crypto", "limit": _PAGE},
            "currencies",
            marks=pytest.mark.needs("currencies:basic"),
            id="crypto",
        ),
        pytest.param(
            AssetClass.OPTIONS,
            _OPTION_CONTRACTS,
            {"limit": _PAGE},
            "options",
            marks=pytest.mark.needs("options:basic"),
            id="options",
        ),
        pytest.param(
            AssetClass.FUTURES,
            _FUTURES_CONTRACTS,
            {"type": "single", "limit": _PAGE},
            "futures",
            marks=pytest.mark.needs("futures:basic"),
            id="futures-single",
        ),
        pytest.param(
            AssetClass.FUTURES,
            _FUTURES_CONTRACTS,
            {"type": "combo", "limit": _PAGE},
            "futures",
            marks=pytest.mark.needs("futures:basic"),
            id="futures-combo",
        ),
    ],
)
def test_the_symbol_rule_accepts_every_ticker_massive_lists(
    live_rest: RestClient,
    *,
    asset_class: AssetClass,
    path: str,
    query: Mapping[str, object],
    bucket: str,
    evidence: Evidence,
) -> None:
    """Whether the rule accepts each ticker of one page of Massive's own list of a class.

    Reads one page of 1,000 rows: the ticker list of the market for stocks, indices, forex,
    and crypto, the options contracts list, and the futures contracts list of single contracts
    and of combos. For every ticker, asserts that the library sends it as written (no refusal,
    its class's prefix present, a forex or crypto pair joined), that `canonical_symbol` keeps
    it, that a flat-file filter holds a spelling that reads back as it, and that its stream
    topic reads back as it. Records the page's size and the characters its tickers use, so a
    lower-case letter, a space, or a separator Massive writes shows up. Reading it: no problem
    line confirms the rule for that class; each problem line is a ticker Massive lists that
    the library would refuse or misread.
    """
    rows = _first_page(live_rest, path, query, bucket=bucket)
    tickers = sorted({row["ticker"] for row in rows if isinstance(row.get("ticker"), str)})
    pairs = _crypto_pairs(rows) if asset_class is AssetClass.CRYPTO else {}
    evidence(f"{len(rows)} rows, {len(tickers)} tickers, e.g. {tickers[:5]} ... {tickers[-5:]}")
    evidence(f"characters used: {''.join(sorted(set(''.join(tickers))))!r}")
    if asset_class is AssetClass.CRYPTO:
        evidence(f"{len(tickers) - len(pairs)} tickers whose currencies do not join, not streamed")
    found = _problems(asset_class, tickers, pairs)
    for line in found[:_SHOWN]:
        evidence(line)
    assert tickers, f"Massive listed no {asset_class.value} ticker"
    assert not found, f"{len(found)} tickers the library refuses or misreads: {found[:_SHOWN]}"


@pytest.mark.needs("stocks:starter")
@pytest.mark.settles("120")
@pytest.mark.requests(2)
@pytest.mark.timeout(60)
def test_massive_answers_tickers_in_another_case(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What Massive answers to tickers in another case than its own.

    Sends the unified snapshot of `AAPL`, `aapl`, `KIMpL` (a Kimco preferred share), `kimpl`,
    and `BRK.B` in one request, and the previous day bar of `aapl`. Reading it: three snapshot
    rows, one per ticker as Massive writes it, and an empty or refused `aapl` bar mean Massive
    reads case, so `aapl` names no ticker and gives no rows, as the library assumes; five
    rows, or a bar for `aapl`, mean Massive folds case, and `aapl` reads AAPL's data.
    """
    assert bad_values.ask_all(live_rest, bad_values.TICKER_CASE, evidence)


@pytest.mark.needs("currencies:starter")
@pytest.mark.settles("120")
@pytest.mark.requests(1)
@pytest.mark.timeout(60)
def test_massive_answers_a_forex_pair_written_with_a_dash(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What Massive's REST quotes answer for `C:EUR-USD`, which the library refuses.

    The library refuses it, naming `C:EURUSD`, because a flat-file filter and a stream topic
    match only the joined ticker. Reading it: an error or no rows means REST agrees; quotes
    mean REST reads the dash as EUR/USD, and a REST-only read of it would have worked.
    """
    assert bad_values.ask_all(live_rest, bad_values.PAIR_SEPARATOR, evidence)
