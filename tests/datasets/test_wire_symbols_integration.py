"""Live checks of how Massive spells share-class and preferred stock tickers in its rows.

The library publishes every `Symbol` in Massive's canonical form (`BRK.B`, `KIMpL`), and reads a
row's ticker back into it (`canonical_from_wire`); a row whose spelling it cannot read is
withheld, and one WARNING names the text, so the owner can report it. These tests read rows
of such stocks from each transport that serves stocks and record the raw spelling each one
writes, so a transport that writes a share class or a preferred otherwise than its ticker
list (`BRK/B`, `BRK-B`, `KIM-L`) shows up with its exact text. Each test records what
Massive sent (`evidence`); one the key's plan does not cover skips on Massive's refusal,
quoting it. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

import pytest

import deephaven_massive as dm
from deephaven_massive._datasets.symbols import is_canonical
from deephaven_massive._enums import AssetClass
from deephaven_massive._massive import (
    RestClient,
    RowFault,
    Transport,
    WireEntitlementError,
    source,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Mapping, Sequence
    from datetime import date
    from typing import TypeAlias

    from deephaven.table import Table

    from deephaven_massive._tables.problem_events import ProblemEvent

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
_GROUPED: Final = "/v2/aggs/grouped/locale/us/market/stocks/{date}"


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


def _withheld(session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Collect the spelling of every row the session withholds for an unreadable symbol.

    Args:
        session: The live session.
        monkeypatch: Replaces the session's sink of problem events for the test.

    Returns:
        A list the session extends with each such spelling as Massive sent it.
    """
    spellings: list[str] = []

    def problems(events: Sequence[ProblemEvent]) -> None:
        spellings.extend(str(e.symbol) for e in events if e.cause is RowFault.UNREADABLE_SYMBOLS)

    monkeypatch.setattr(session._core, "problems", problems)
    return spellings


def _check(
    rows: Sequence[Mapping[str, Any]], withheld: Sequence[str], evidence: Evidence
) -> list[str]:
    """Record each published symbol and each withheld spelling; return the ones to report.

    Args:
        rows: Rows of a dataset table.
        withheld: The spellings of the rows withheld so far for an unreadable symbol.
        evidence: Records a line.

    Returns:
        One line per published `Symbol` that is not a canonical stock ticker, and one per
        withheld spelling, naming the text Massive sent.
    """
    published = sorted(str(row.get("Symbol")) for row in rows)
    evidence(f"published Symbol: {published}; withheld spellings: {sorted(withheld)}")
    return [
        *(f"Symbol {s!r}" for s in published if not is_canonical(AssetClass.STOCKS, s)),
        *(f"withheld Massive's text {text!r}" for text in sorted(set(withheld))),
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
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Which spelling REST rows write for a share class and a preferred, and what is published.

    Reads Massive's ticker list for each of the four tickers (four requests, reference data,
    since the list takes one `ticker` a request), so the evidence shows the list's own spelling
    of each; then each ticker's previous day bar through
    the library (four requests, `/v2/aggs/ticker/{ticker}/prev`), and the grouped daily bars of
    the most recent Monday through the library and raw (two requests), kept to rows whose
    ticker starts with one of the four roots, so a row Massive writes as `BRK/B` or `KIM-L` is
    listed beside `BRK.B`. Records the raw `ticker` or `T` spellings, every published `Symbol`,
    the text of every row withheld for a symbol the library cannot read, and the tickers with
    no published grouped row.

    Reading it: every published `Symbol` canonical and no row withheld means REST writes the
    ticker list's spelling, which the library reads back; a withheld row names the raw text to
    report to Massive, and a spelling in the raw lines beside a canonical one is a second form
    Massive writes for the same stock.
    """
    ticker_list = source(Transport.REST, _TICKER_LIST)
    listed = [
        row
        for ticker in _TICKERS
        for row in live_rest.get(
            ticker_list, {}, {"ticker": ticker, "market": "stocks"}, bucket="other"
        ).rows
    ]
    evidence(f"ticker list spellings: {_spelled(listed, 'ticker')}")
    withheld = _withheld(live_session, monkeypatch)
    problems: list[str] = []
    for ticker in _TICKERS:
        bar = live_session.stocks.previous_day_bar(ticker).fetch(wait=True)
        rows = table_rows(bar)
        evidence(f"previous day bar of {ticker}: {len(rows)} rows")
        problems.extend(_check(rows, withheld, evidence))
    grouped = live_session.stocks.bars(dm.ALL, dm.Timespan.DAY).history(
        recent_monday, recent_monday, source=dm.HistorySource.REST, wait=True
    )
    rows = [row for row in table_rows(grouped) if str(row.get("Symbol")).startswith(_ROOTS)]
    evidence(f"grouped daily bars of {recent_monday}: {len(rows)} rows of the four roots")
    problems.extend(_check(rows, withheld, evidence))
    raw = live_rest.get(
        source(Transport.REST, _GROUPED), {"date": recent_monday.isoformat()}, {}, bucket="stocks"
    ).rows
    evidence(f"raw grouped daily spellings of the four roots: {_spelled(raw, 'T')}")
    missing = sorted(set(_TICKERS) - {row.get("Symbol") for row in rows})
    evidence(f"tickers with no published grouped row: {missing}")
    problems.extend(f"no published grouped row for {ticker}" for ticker in missing)
    assert listed, "Massive's ticker list holds none of the four tickers"
    assert not problems, f"REST rows the library cannot read back: {problems}"


@pytest.mark.verifies("121")
@pytest.mark.timeout(600)
def test_flat_file_rows_spell_share_classes_and_preferreds_canonically(
    live_session: dm.MassiveSession,
    *,
    recent_monday: date,
    flat_file_tickers: FlatFileTickers,
    table_rows: TableRows,
    evidence: Evidence,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Which spelling the day-aggregates file writes for a share class and a preferred.

    Reads the whole `us_stocks_sip/day_aggs_v1` file of the most recent Monday raw, no REST
    request, and records every `ticker` text that starts with one of the four roots; then the
    library's as-traded (`adjusted=False`) `history(source=FLAT_FILES)` of the four tickers
    for that day, since flat-file bars are unadjusted, every published `Symbol`, and the text
    of every row withheld for a symbol the library cannot read.

    Reading it: the four tickers among the raw spellings, and four canonical published rows,
    mean the file writes the ticker list's spelling; a raw spelling such as `BRK/B` beside no
    `BRK.B` means the file writes another form, which the library's filter would miss, so it
    is reported to Massive and the grammar's read-back is extended with the evidence.
    """
    raw = flat_file_tickers(_DAY_FILE, recent_monday, None)
    assert raw is not None, f"no {_DAY_FILE} file for {recent_monday}"
    spellings = _spelled(raw[0], "ticker")
    evidence(f"{_DAY_FILE} {recent_monday}: raw ticker spellings of the four roots: {spellings}")
    withheld = _withheld(live_session, monkeypatch)
    bars = live_session.stocks.bars(list(_TICKERS), dm.Timespan.DAY, adjusted=False).history(
        recent_monday, recent_monday, source=dm.HistorySource.FLAT_FILES, wait=True
    )
    rows = table_rows(bars)
    problems = _check(rows, withheld, evidence)
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
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Which spelling Benzinga's consensus ratings write for a share class.

    Reads the consensus ratings of the two share classes raw (two requests,
    `/benzinga/v1/consensus-ratings/{ticker}`, charged to the Stocks bucket as the library
    charges them) and records each row's `ticker`; then the same through the library, and
    every published `Symbol` and withheld spelling. Preferreds have no analyst consensus, so
    they are left out. Without the Benzinga add-on the test skips, quoting Massive's refusal.

    Reading it: canonical published symbols mean the partner writes Massive's spelling; a
    withheld row names the raw text (`BRK/B`, say) to report to Massive.
    """
    rest_path = source(Transport.REST, _CONSENSUS)
    for ticker in _SHARE_CLASSES:
        try:
            rows = live_rest.get(rest_path, {"ticker": ticker}, {}, bucket="stocks").rows
        except WireEntitlementError as error:
            pytest.skip(f"not entitled to Benzinga's consensus ratings ({error})")
        evidence(f"raw consensus of {ticker}: {len(rows)} rows, tickers {_spelled(rows, 'ticker')}")
    withheld = _withheld(live_session, monkeypatch)
    table = live_session.partners.benzinga.consensus_ratings(list(_SHARE_CLASSES)).fetch(wait=True)
    problems = _check(table_rows(table), withheld, evidence)
    assert not problems, f"partner rows the library cannot read back: {problems}"
