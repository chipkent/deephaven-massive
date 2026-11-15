"""Live checks of how Massive spells share-class and preferred stock tickers in its rows.

The library publishes every `Symbol` in Massive's canonical form (`BRK.B`, `KIMpL`), and reads a
row's ticker back into it (`canonical_from_wire`); a spelling it cannot read leaves a null
`Symbol`, keeps the text in `RawSymbol`, and logs one WARNING naming the text, so the owner can
report it to Massive. These tests read rows of such stocks from each transport that serves
stocks and record the raw spelling each one writes, so a transport that writes a share class or
a preferred otherwise than its ticker list (`BRK/B`, `BRK-B`, `KIM-L`) shows up with its exact
text. Each test records what Massive sent (`evidence`); one the key's plan does not cover skips
on Massive's refusal, quoting it. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

import pytest

import deephaven_massive as dm
from deephaven_massive._datasets.symbols import is_canonical
from deephaven_massive._enums import AssetClass
from deephaven_massive._massive import RestClient, Transport, WireEntitlementError, source

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Mapping, Sequence
    from datetime import date
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
FlatFileTickers: TypeAlias = (
    "Callable[[str, date, Collection[str] | None], "
    "tuple[list[dict[str, str | None]], frozenset[str]] | None]"
)

_SHARE_CLASSES: Final = ("BRK.B", "BF.B")
"""Two share classes of Massive's ticker list: Berkshire Hathaway B and Brown-Forman B."""
_PREFERREDS: Final = ("KIMpL", "BACpL")
"""Two preferred shares of Massive's ticker list: Kimco series L and Bank of America series L."""
_TICKERS: Final = (*_SHARE_CLASSES, *_PREFERREDS)
_ROOTS: Final = ("BRK", "BF", "KIM", "BAC")
"""The roots whose every spelling a transport's rows are searched for."""

_TICKER_LIST: Final = "/v3/reference/tickers"
_DAY_FILE: Final = "us_stocks_sip/day_aggs_v1"
_CONSENSUS: Final = "/benzinga/v1/consensus-ratings/{ticker}"


def _spelled(rows: Sequence[Mapping[str, Any]], key: str) -> list[str]:
    """Return the distinct raw spellings, under one key, of the rows that name one of the roots.

    Args:
        rows: Raw rows, as Massive sent them.
        key: The key that holds the ticker.

    Returns:
        The sorted distinct values whose letters start with one of `_ROOTS`.
    """
    found = {
        str(value)
        for row in rows
        if (value := row.get(key)) is not None
        and any(str(value).upper().startswith(root) for root in _ROOTS)
    }
    return sorted(found)


def _symbols(rows: Sequence[Mapping[str, Any]]) -> list[tuple[str | None, str | None]]:
    """Return each row's `Symbol` and `RawSymbol`, the latter None where the table has none.

    Args:
        rows: Rows of a dataset table.

    Returns:
        One `(Symbol, RawSymbol)` per row, sorted with nulls last, since a row whose ticker the
        library cannot read has a null `Symbol` beside rows that have one.
    """
    pairs = [(row.get("Symbol"), row.get("RawSymbol")) for row in rows]
    return sorted(pairs, key=lambda pair: [(text is None, text or "") for text in pair])


def _check(pairs: Sequence[tuple[str | None, str | None]], evidence: Evidence) -> list[str]:
    """Record each published symbol and return those that are not canonical.

    Args:
        pairs: Each row's `Symbol` and `RawSymbol`.
        evidence: Records a line.

    Returns:
        One line per row whose `Symbol` is null or not a canonical stock ticker, naming the
        raw text Massive sent.
    """
    evidence(f"published (Symbol, RawSymbol): {pairs}")
    return [
        f"Symbol {symbol!r} from Massive's text {raw!r}"
        for symbol, raw in pairs
        if symbol is None or not is_canonical(AssetClass.STOCKS, symbol)
    ]


@pytest.mark.verifies("121")
@pytest.mark.timeout(300)
def test_rest_rows_spell_share_classes_and_preferreds_canonically(
    live_session: dm.MassiveSession,
    live_rest: RestClient,
    *,
    recent_monday: date,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Which spelling REST rows write for a share class and a preferred, and what is published.

    Reads Massive's ticker list for the four tickers (one request, reference data), so the
    evidence shows the list's own spelling of each; then each ticker's previous day bar through
    the library (four requests, `/v2/aggs/ticker/{ticker}/prev`), and the grouped daily bars of
    the most recent Monday (one request), kept to rows whose ticker starts with one of the four
    roots, so a row Massive writes as `BRK/B` or `KIM-L` is listed beside `BRK.B`. Records the
    raw `ticker` or `T` spellings and every published `Symbol` and `RawSymbol`.

    Reading it: every published `Symbol` canonical and every `RawSymbol` null means REST writes
    the ticker list's spelling, which the library reads back; a null `Symbol` names the raw
    text to report to Massive, and a spelling in the raw lines beside a canonical one is a
    second form Massive writes for the same stock.
    """
    listed = live_rest.get(
        source(Transport.REST, _TICKER_LIST),
        {},
        {"ticker.any_of": ",".join(_TICKERS), "market": "stocks"},
        bucket="other",
    ).rows
    evidence(f"ticker list spellings: {_spelled(listed, 'ticker')}")
    problems: list[str] = []
    for ticker in _TICKERS:
        bar = live_session.stocks.previous_day_bar(ticker).fetch(wait=True)
        rows = table_rows(bar)
        evidence(f"previous day bar of {ticker}: {len(rows)} rows")
        problems.extend(_check(_symbols(rows), evidence))
    grouped = live_session.stocks.bars(dm.ALL, dm.Timespan.DAY).history(
        recent_monday, recent_monday, source=dm.HistorySource.REST, wait=True
    )
    rows = [
        row
        for row in table_rows(grouped)
        if str(row.get("Symbol") or row.get("RawSymbol")).startswith(_ROOTS)
    ]
    evidence(f"grouped daily bars of {recent_monday}: {len(rows)} rows of the four roots")
    problems.extend(_check(_symbols(rows), evidence))
    assert listed, "Massive's ticker list holds none of the four tickers"
    assert not problems, f"REST rows the library cannot read back: {problems}"


@pytest.mark.needs("s3")
@pytest.mark.verifies("121")
@pytest.mark.timeout(600)
def test_flat_file_rows_spell_share_classes_and_preferreds_canonically(
    live_session: dm.MassiveSession,
    *,
    recent_monday: date,
    flat_file_tickers: FlatFileTickers,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Which spelling the day-aggregates file writes for a share class and a preferred.

    Reads the whole `us_stocks_sip/day_aggs_v1` file of the most recent Monday raw, no REST
    request, and records every `ticker` text that starts with one of the four roots; then the
    library's `history(source=FLAT_FILES)` of the four tickers for that day, and every
    published `Symbol` and `RawSymbol`.

    Reading it: the four tickers among the raw spellings, and four canonical published rows,
    mean the file writes the ticker list's spelling; a raw spelling such as `BRK/B` beside no
    `BRK.B` means the file writes another form, which the library's filter would miss, so it
    is reported to Massive and the grammar's read-back is extended with the evidence.
    """
    raw = flat_file_tickers(_DAY_FILE, recent_monday, None)
    assert raw is not None, f"no {_DAY_FILE} file for {recent_monday}"
    spellings = _spelled(raw[0], "ticker")
    evidence(f"{_DAY_FILE} {recent_monday}: raw ticker spellings of the four roots: {spellings}")
    bars = live_session.stocks.bars(list(_TICKERS), dm.Timespan.DAY).history(
        recent_monday, recent_monday, source=dm.HistorySource.FLAT_FILES, wait=True
    )
    rows = table_rows(bars)
    problems = _check(_symbols(rows), evidence)
    missing = sorted(set(_TICKERS) - {row.get("Symbol") for row in rows})
    evidence(f"tickers with no published row: {missing}")
    assert not problems, f"file rows the library cannot read back: {problems}"
    assert not missing, f"the file writes no row the filter reads as: {missing}; raw {spellings}"


@pytest.mark.verifies("121")
@pytest.mark.timeout(300)
def test_partner_rows_spell_share_classes_canonically(
    live_session: dm.MassiveSession,
    live_rest: RestClient,
    *,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Which spelling Benzinga's consensus ratings write for a share class.

    Reads the consensus ratings of the two share classes raw (two requests,
    `/benzinga/v1/consensus-ratings/{ticker}`, charged to the Stocks bucket as the library
    charges them) and records each row's `ticker`; then the same through the library, and
    every published `Symbol` and `RawSymbol`. Preferreds have no analyst consensus, so they
    are left out. Without the Benzinga add-on the test skips, quoting Massive's refusal.

    Reading it: canonical published symbols mean the partner writes Massive's spelling; a null
    `Symbol` names the raw text (`BRK/B`, say) to report to Massive, and until then such a row
    keeps its text in `RawSymbol`.
    """
    rest_path = source(Transport.REST, _CONSENSUS)
    for ticker in _SHARE_CLASSES:
        try:
            rows = live_rest.get(rest_path, {"ticker": ticker}, {}, bucket="stocks").rows
        except WireEntitlementError as error:
            pytest.skip(f"not entitled to Benzinga's consensus ratings ({error})")
        evidence(f"raw consensus of {ticker}: {len(rows)} rows, tickers {_spelled(rows, 'ticker')}")
    table = live_session.partners.benzinga.consensus_ratings(list(_SHARE_CLASSES)).fetch(wait=True)
    problems = _check(_symbols(table_rows(table)), evidence)
    assert not problems, f"partner rows the library cannot read back: {problems}"
