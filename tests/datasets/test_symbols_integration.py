"""Live checks of the symbol grammar against Massive's own tickers, and of how Massive reads case.

The library sends a ticker in Massive's canonical form for its class as written, and refuses any
other form, naming the form to write. These tests page Massive's own list of each class and
check that the grammar accepts every ticker on it, and that each converts to and from each
transport unchanged: a listed form the grammar refuses is a library defect to fix. They also
record what Massive answers to a ticker in another case, and to a forex pair written with a
dash. Each test records what Massive sent (`evidence`). A test the key's plan does not cover
skips on Massive's refusal, quoting it. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Final
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm
from deephaven_massive._datasets.symbols import check_symbol
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
"""Rows of one page: the largest each list serves."""

_SHOWN: Final = 20
"""The most refused or changed tickers the evidence names, per list."""

_OPTION_UNDERLYINGS: Final = (
    *("AAPL", "SPY", "QQQ", "BRK.B", "BF.B", "GOOGL", "GOOG"),
    *("BAC", "KIM", "WFC"),
    *("SPX", "NDX", "VIX", "RUT"),
)
"""The underlyings whose every options contract is read: share classes (`BRK.B`, `BF.B`,
`GOOGL`, `GOOG`), the common stocks behind preferreds the stock lists hold (`BAC`, `KIM`,
`WFC`), and index options, which the contracts list takes by their bare ticker (`SPX`)."""

_TODAY: Final = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
"""Today in New York, the day whose tradeable futures contracts are read: without a day, the
futures contracts list holds each contract once for every day of its history."""


def _every_page(
    client: RestClient, path: str, query: Mapping[str, object], *, bucket: str
) -> tuple[list[dict[str, Any]], int]:
    """Read every page of a list, following its `next_url`.

    Args:
        client: The wire REST client.
        path: The list's path.
        query: Its query parameters, the page size among them.
        bucket: The rate bucket of its class.

    Returns:
        The rows, as Massive sent them, and how many requests the pages took.
    """
    rows: list[dict[str, Any]] = []
    requests = 0
    for page in client.pages(source(Transport.REST, path), {}, query, bucket=bucket):
        rows.extend(page.rows)
        requests += 1
    return rows, requests


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
            check_symbol(asset_class, ticker)
        except ValueError as error:
            found.append(f"{ticker!r}: refused: {error}")
            continue
        if dm.canonical_symbol(ticker, asset_class) != ticker:
            found.append(
                f"{ticker!r}: canonical_symbol gives {dm.canonical_symbol(ticker, asset_class)!r}"
            )
        if not wire_spellings(asset_class, ticker):
            found.append(f"{ticker!r}: no flat-file spelling reads back as it")
        if asset_class is AssetClass.CRYPTO and ticker not in pairs:
            continue  # its currencies do not join, so no stream topic can be built for it
        topic = ws_symbol(asset_class, ticker, pairs)
        if canonical_from_wire(asset_class, topic) != ticker:
            found.append(f"{ticker!r}: stream topic {topic!r} reads back as another ticker")
    return found


@pytest.mark.verifies("119")
@pytest.mark.timeout(1800)
@pytest.mark.parametrize(
    ("asset_class", "path", "queries", "bucket"),
    [
        pytest.param(
            AssetClass.STOCKS,
            _TICKERS,
            ({"market": "stocks", "limit": _PAGE},),
            "stocks",
            id="stocks",
        ),
        pytest.param(
            AssetClass.INDICES,
            _TICKERS,
            ({"market": "indices", "limit": _PAGE},),
            "indices",
            id="indices",
        ),
        pytest.param(
            AssetClass.FOREX,
            _TICKERS,
            ({"market": "fx", "limit": _PAGE},),
            "currencies",
            id="forex",
        ),
        pytest.param(
            AssetClass.CRYPTO,
            _TICKERS,
            ({"market": "crypto", "limit": _PAGE},),
            "currencies",
            id="crypto",
        ),
        pytest.param(
            AssetClass.OPTIONS,
            _OPTION_CONTRACTS,
            tuple({"underlying_ticker": u, "limit": _PAGE} for u in _OPTION_UNDERLYINGS),
            "options",
            id="options",
        ),
        pytest.param(
            AssetClass.FUTURES,
            _FUTURES_CONTRACTS,
            (
                {"type": "single", "date": _TODAY, "active": True, "limit": _PAGE},
                {"type": "combo", "date": _TODAY, "active": True, "limit": _PAGE},
            ),
            "futures",
            id="futures",
        ),
    ],
)
def test_the_symbol_rule_accepts_every_ticker_massive_lists(
    live_rest: RestClient,
    *,
    asset_class: AssetClass,
    path: str,
    queries: Sequence[Mapping[str, object]],
    bucket: str,
    evidence: Evidence,
) -> None:
    """Whether the grammar accepts every ticker of Massive's own list of a class.

    Reads every page, following `next_url`: the ticker list of the market for stocks,
    indices, forex, and crypto; every futures contract, single and combo, tradeable today;
    and, since the options contracts list holds millions of contracts, a sample: every current
    contract of 14 underlyings, share classes (`BRK.B`, `BF.B`, `GOOGL`), the common stocks
    behind listed preferreds (`BAC`, `KIM`, `WFC`), and index options (`SPX`, `NDX`, `VIX`,
    `RUT`). For every ticker, asserts that the grammar accepts it as written, that
    `canonical_symbol` keeps it, that a flat-file filter holds a spelling that reads back as
    it, and that its stream topic reads back as it. Records each list's size, the requests it
    took, and the characters its tickers use, so a lower-case letter, a space, or a separator
    Massive writes shows up.

    Cost, estimated, not yet measured: about 30 requests for stocks, 15 for indices, 2 each
    for forex and crypto, 60 for the options sample, and 30 for futures, about 140 in all, so
    about 5 minutes unpaced; a class the run paces stays within the timeout. Each case records
    the count it took.

    Reading it: no problem line confirms the grammar for that class; each problem line is a
    ticker Massive lists that the library would refuse or misread, a defect to fix in the
    grammar.
    """
    rows: list[dict[str, Any]] = []
    requests = 0
    for query in queries:
        read, sent = _every_page(live_rest, path, query, bucket=bucket)
        rows.extend(read)
        requests += sent
    tickers = sorted({row["ticker"] for row in rows if isinstance(row.get("ticker"), str)})
    pairs = _crypto_pairs(rows) if asset_class is AssetClass.CRYPTO else {}
    evidence(f"{requests} requests, {len(rows)} rows, {len(tickers)} tickers")
    evidence(f"e.g. {tickers[:5]} ... {tickers[-5:]}")
    evidence(f"characters used: {''.join(sorted(set(''.join(tickers))))!r}")
    if asset_class is AssetClass.CRYPTO:
        evidence(f"{len(tickers) - len(pairs)} tickers whose currencies do not join, not streamed")
    found = _problems(asset_class, tickers, pairs)
    for line in found[:_SHOWN]:
        evidence(line)
    assert tickers, f"Massive listed no {asset_class.value} ticker"
    assert not found, f"{len(found)} tickers the library refuses or misreads: {found[:_SHOWN]}"


@pytest.mark.settles("120")
@pytest.mark.timeout(300)
def test_massive_answers_tickers_in_another_case(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What Massive answers to tickers in another case than its own.

    Sends the unified snapshot of `AAPL`, `aapl`, `KIMpL` (a Kimco preferred share), `kimpl`,
    and `BRK.B` in one request, and the previous day bar of `aapl`. The snapshot answers a
    ticker it does not hold with a row carrying an `error` field, so the verdict counts only
    rows without one. Reading it: three such rows, and an empty or refused `aapl` bar, mean
    Massive reads case, so `aapl` names no ticker, and the grammar's refusal of it, naming
    `AAPL`, spares the user a silent empty answer; five such rows, or a bar for `aapl`, mean
    Massive folds case, and the refusal could be relaxed.
    """
    assert bad_values.ask_all(live_rest, bad_values.TICKER_CASE, evidence)


@pytest.mark.settles("120")
@pytest.mark.timeout(300)
def test_massive_answers_a_forex_pair_written_with_a_dash(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What Massive's REST quotes answer for `C:EUR-USD`, which the library refuses.

    The library refuses it, naming `C:EURUSD`, because a flat-file filter and a stream topic
    match only the joined ticker. Reading it: an error or no rows means REST agrees; quotes
    mean REST reads the dash as EUR/USD, and the refusal still stands on the flat-file and
    stream silence.
    """
    assert bad_values.ask_all(live_rest, bad_values.PAIR_SEPARATOR, evidence)
