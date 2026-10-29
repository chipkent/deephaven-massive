"""Live checks of options data against Massive: underlyings, deliverables, and bar stamps.

Each test records what Massive sent (`evidence`) and asserts only that the requests worked:
the evidence, not a pass, answers the question. Raw rows come from the wire REST client,
because "a key is missing" and "a key holds an empty list" look the same once the library has
read them; the library's own answer is recorded beside them. How to run these tests is in
`docs/live-tests.md`.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import (
    RestClient,
    Transport,
    WireEntitlementError,
    WireError,
    source,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Mapping
    from typing import TypeAlias

    from deephaven.table import Table

    from deephaven_massive._query import Query

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Evidence: TypeAlias = "Callable[[str], None]"
HalfDayReport: TypeAlias = "Callable[[str, str], list[str]]"
RestRows: TypeAlias = (
    "Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]]"
)
FlatFileTickers: TypeAlias = (
    "Callable[[str, date, Collection[str] | None], "
    "tuple[list[dict[str, str | None]], frozenset[str]] | None]"
)
StampedBar: TypeAlias = "tuple[int, tuple[float | None, ...]]"
DayBarReport: TypeAlias = (
    "Callable[[str, str, Mapping[str, Mapping[date, list[StampedBar]]]], tuple[list[str], int]]"
)
PrevBarReport: TypeAlias = "Callable[[str, str], tuple[list[str], int]]"
PeriodBarReport: TypeAlias = (
    "Callable[[str, str, Mapping[str, tuple[date, date]]], tuple[list[str], int]]"
)
LibraryStamps: TypeAlias = "Callable[[Callable[[], Table]], str]"

_CONTRACTS = "/v3/reference/options/contracts"
_CHAIN = "/v3/snapshot/options/{underlyingAsset}"
_SNAPSHOT = "/v3/snapshot"

_UNDERLYINGS = ("SPX", "I:SPX", "SPXW", "NDX", "NDXP", "VIX", "VIXW", "BRK.B", "BRKB")
"""Underlyings whose spelling the options endpoints may not agree on."""

_SNAPSHOT_PLAN = "Options Starter ($29/month)"
"""The plan of the chain and unified snapshots of options."""

_CONTRACT_PAGE = 1000
_CHAIN_PAGE = 250
_MAX_PAGES = 15
"""The most pages read while looking for one root's contracts in a listing sorted by ticker."""

_EXAMPLES = 3
"""How many contracts of a root are enough: the deliverable check shows three."""

_DELIVERABLE_FIELDS = ("additional_underlyings", "cfi", "correction", "shares_per_contract")
_DELIVERABLE_COLUMNS = ("AdditionalUnderlyings", "Cfi", "Correction", "SharesPerContract")

_ADJUSTED: tuple[tuple[str, str, date], ...] = (
    ("DELL1", "DELL", date(2021, 11, 15)),
    ("FF1", "FF", date(2024, 4, 15)),
    ("AAPL7", "AAPL", date(2014, 6, 10)),
    ("AAPL", "AAPL", date(2026, 6, 1)),
    ("SPX", "SPX", date(2026, 6, 1)),
)
"""(contract root, underlying, a day the contracts were listed): three adjusted roots after a
spin-off, a special dividend, and a split, then a standard equity root and an index root."""

_OCC_TAIL = 15
"""Characters of an OCC symbol after its root: the expiry (6), the type (1), the strike (8)."""


def _root(ticker: str) -> str:
    """Return an OCC option symbol's root, such as `"DELL1"` for `"O:DELL1211119C00050000"`.

    Returns:
        The root.
    """
    return ticker.removeprefix("O:")[:-_OCC_TAIL]


def _raw(
    client: RestClient, path: str, *, params: Mapping[str, object], evidence: Evidence
) -> list[dict[str, Any]]:
    """Read the raw rows of a request's first page; record a refusal as evidence.

    A rejection (an unknown underlying) is itself evidence, so it is recorded and the check
    goes on; so is a snapshot refused for want of Options Starter, labelled "not entitled".

    Returns:
        The rows; empty after a rejection or a refused snapshot.

    Raises:
        WireEntitlementError: When the contracts reference itself is refused.
    """
    path_params = {"underlyingAsset": params["underlyingAsset"]} if "{" in path else {}
    query = {key: value for key, value in params.items() if key != "underlyingAsset"}
    try:
        return client.get(source(Transport.REST, path), path_params, query).rows
    except WireEntitlementError as error:
        if path == _CONTRACTS:
            raise
        evidence(f"{path} {dict(params)}: not entitled: needs {_SNAPSHOT_PLAN} ({error})")
        return []
    except WireError as error:
        evidence(f"{path} {dict(params)}: {type(error).__name__}: {error}")
        return []


def _library(build: Callable[[], Query], table_rows: TableRows) -> list[dict[str, Any]] | str:
    """Build one library query, fetch it, and describe the outcome.

    Returns:
        The result's rows, or the text of the error building or fetching raised.
    """
    try:
        return table_rows(build().fetch())
    except dm.EntitlementError as error:
        return f"not entitled: needs {_SNAPSHOT_PLAN} ({error})"
    except (dm.MassiveError, ValueError) as error:
        return f"{type(error).__name__}: {error}"


def _root_contracts(
    client: RestClient, root: str, *, query: Mapping[str, object], evidence: Evidence
) -> list[dict[str, Any]]:
    """Page through a contract listing sorted by ticker until one root's contracts are passed.

    A root such as `AAPL7` sorts after every `AAPL` contract of its time, beyond the first
    page; this follows the pages (at most 15) until three of the root's contracts are found or
    a ticker sorts past the root, and records how far it read, so a listing cut short shows in
    the evidence.

    Returns:
        The root's contracts; empty after a rejection, which is recorded.

    Raises:
        WireEntitlementError: When the listing is refused for want of a plan.
    """
    prefix = f"O:{root}"
    found: list[dict[str, Any]] = []
    read = pages = 0
    last = ""
    query = {**query, "sort": "ticker", "order": "asc", "limit": _CONTRACT_PAGE}
    try:
        for page in client.pages(source(Transport.REST, _CONTRACTS), {}, query):
            pages += 1
            read += len(page.rows)
            found.extend(r for r in page.rows if _root(str(r.get("ticker", ""))) == root)
            last = str(page.rows[-1].get("ticker", "")) if page.rows else last
            passed = last[: len(prefix)] > prefix
            if passed or len(found) >= _EXAMPLES or pages >= _MAX_PAGES:
                break
    except WireEntitlementError:
        raise
    except WireError as error:
        evidence(f"{_CONTRACTS} {dict(query)}: {type(error).__name__}: {error}")
    evidence(
        f"    {dict(query)}: {pages} pages, {read} rows, last ticker {last!r}, "
        f"{len(found)} of root {root}{' (page cap reached)' if pages >= _MAX_PAGES else ''}"
    )
    return found


@pytest.mark.needs("options:basic")
@pytest.mark.settles("index underlying spelling", "BRK.B underlying")
@pytest.mark.requests(45)
@pytest.mark.timeout(100)
def test_underlying_spelling_on_the_options_endpoints(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """How each options endpoint spells and accepts an underlying, for indices and `BRK.B`.

    For `SPX`, `I:SPX`, `SPXW`, `NDX`, `NDXP`, `VIX`, `VIXW`, `BRK.B`, and `BRKB`, records one
    full page of the contracts reference (the roots it lists and the `underlying_ticker` values),
    one page of the chain snapshot (`underlying_asset.ticker`), the unified snapshot's
    `underlying_asset.ticker` of one contract, and the library's `chain()` and `contracts()`.
    Reading it: the spelling that answers is the one the library must send for `underlying=`;
    a root such as `SPXW` listed under `SPX` means roots and underlyings differ. The chain path
    sends `I:SPX` percent-encoded, so an `I:SPX` chain result counts only if the encoded-colon
    check of the indices tests passes.
    """
    answered = 0
    options = live_session.options
    for underlying in _UNDERLYINGS:
        contracts = _raw(
            live_rest,
            _CONTRACTS,
            params={"underlying_ticker": underlying, "limit": _CONTRACT_PAGE},
            evidence=evidence,
        )
        chain = _raw(
            live_rest,
            _CHAIN,
            params={"underlyingAsset": underlying, "limit": _CHAIN_PAGE},
            evidence=evidence,
        )
        answered += bool(contracts) + bool(chain)
        roots = sorted({_root(str(r.get("ticker", ""))) for r in contracts})
        spelled = sorted({str(r.get("underlying_ticker")) for r in contracts})
        evidence(
            f"{underlying}: contracts {len(contracts)} rows; roots {roots}; "
            f"underlying_ticker {spelled}"
        )
        chained = sorted({str(r.get("underlying_asset", {}).get("ticker")) for r in chain})
        evidence(f"{underlying}: chain {len(chain)} rows; underlying_asset.ticker {chained}")
        if contracts:
            unified = _raw(
                live_rest,
                _SNAPSHOT,
                params={"ticker.any_of": [contracts[0]["ticker"]]},
                evidence=evidence,
            )
            named = [r.get("underlying_asset", {}).get("ticker") for r in unified]
            evidence(
                f"{underlying}: unified snapshot of {contracts[0]['ticker']}: "
                f"underlying_asset.ticker {named}"
            )
        library_chain = _library(partial(options.chain, underlying, limit=3), table_rows)
        library_contracts = _library(
            partial(options.contracts, underlying=underlying, limit=3), table_rows
        )
        for name, outcome in (("chain", library_chain), ("contracts", library_contracts)):
            shown = (
                outcome
                if isinstance(outcome, str)
                else [
                    {k: r.get(k) for k in ("Symbol", "Underlying", "OptionRoot")} for r in outcome
                ]
            )
            evidence(f"{underlying}: library {name}: {shown}")
    assert answered


@pytest.mark.needs("options:starter")
@pytest.mark.settles("adjusted deliverables")
@pytest.mark.timeout(400)
def test_deliverable_fields_of_adjusted_and_standard_contracts(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """How the contracts reference describes adjusted and standard deliverables.

    Lists the contracts of the adjusted roots `DELL1` (as of 2021-11-15, after a spin-off),
    `FF1` (2024-04-15, a special dividend), and `AAPL7` (2014-06-10, the 7:1 split), and of
    standard `AAPL` and `SPX` contracts (2026-06-01), looked up by underlying (following pages
    until the root is passed) and by root, and records `additional_underlyings` (or
    `<missing>`), `cfi`, `correction`, and `shares_per_contract` of up to three of each, and
    the library's columns for the first. The 2014 and 2021 days need a plan with that much
    history. Reading it: standard contracts without `additional_underlyings` (or with `[]`)
    and adjusted ones with a non-empty list mean null and `[]` both mark a standard
    deliverable; an adjusted contract with neither means only `Cfi` or the root's trailing
    digit can tell. A root found by neither lookup, with the page counts showing the listing
    was read past it, is not kept by Massive for that day.
    """
    found = 0
    for root, underlying, listed in _ADJUSTED:
        evidence(f"{root} (underlying {underlying}, as of {listed}):")
        matching = [
            row
            for expired in (False, True)
            for lookup in dict.fromkeys((underlying, root))
            for row in _root_contracts(
                live_rest,
                root,
                query={"underlying_ticker": lookup, "as_of": listed, "expired": expired},
                evidence=evidence,
            )
        ]
        rows = list({str(row["ticker"]): row for row in matching}.values())
        found += bool(rows)
        evidence(f"    {len(rows)} contracts of root {root}")
        for row in rows[:3]:
            present = {key: row.get(key, "<missing>") for key in _DELIVERABLE_FIELDS}
            evidence(f"    {row.get('ticker')} underlying={row.get('underlying_ticker')} {present}")
        if rows:
            ticker = str(rows[0]["ticker"])
            outcome = _library(
                partial(live_session.options.contracts, symbols=ticker, as_of=listed), table_rows
            )
            shown = (
                outcome
                if isinstance(outcome, str)
                else [{k: r.get(k) for k in _DELIVERABLE_COLUMNS} for r in outcome]
            )
            evidence(f"    library contracts({ticker}): {shown}")
    assert found


@pytest.mark.needs("options:starter")
@pytest.mark.settles("half-day stamps")
@pytest.mark.timeout(100)
def test_half_day_option_bars_and_their_stamps(
    live_rest: RestClient, *, half_day_report: HalfDayReport, evidence: Evidence
) -> None:
    """How REST stamps an option contract's bars on a half trading day (13:00 New York close).

    Picks one SPY contract listed on 2025-11-28 and expiring after 2025-12-24, and records its
    range day bar and first and last minute bars on both half days, in UTC and New York time.
    Reading it: options trade until 13:00 (or 13:15 for index options) on a half day; the
    stamps show whether the library's day label holds for them.
    """
    contracts = live_rest.get(
        source(Transport.REST, _CONTRACTS),
        {},
        {
            "underlying_ticker": "SPY",
            "as_of": date(2025, 11, 28),
            "expiration_date.gte": date(2026, 1, 16),
            "limit": 1,
        },
    ).rows
    assert contracts, "no SPY contract listed on 2025-11-28"
    ticker = str(contracts[0]["ticker"])
    lines = half_day_report("optionsTicker", ticker)
    for line in lines:
        evidence(line)
    assert lines


_STAMP_DAYS = (date(2024, 11, 27), date(2024, 11, 29))
"""A full trading day and the half day after it (Black Friday 2024, closing at 13:00)."""
_STAMP_WEEKS = (date(2024, 11, 18), date(2024, 12, 1))
"""Two calendar weeks, the second holding Thanksgiving and the half day."""
_STAMP_CONTRACTS = ("O:SPY241220C00600000", "O:SPXW241220C06000000")
"""A SPY and an SPXW call expiring 2024-12-20, each struck near its underlying's 2024-11-27
close (SPY about 599, SPX about 5,999), so both traded on every stamp day."""
_DAY_FILE = "us_options_opra/day_aggs_v1"
_MS = 1_000_000


def _third_friday(day: date) -> date:
    """Return the third Friday of the month after `day`: a standard monthly expiration.

    Returns:
        The date.
    """
    first = (day.replace(day=1) + timedelta(days=32)).replace(day=1)
    return first + timedelta(days=(4 - first.weekday()) % 7 + 14)


def _current_contracts(rest_rows: RestRows) -> tuple[list[str], list[str]]:
    """Return candidate SPY and SPXW calls near the money that are listed today.

    `/prev` serves only the most recent completed trading day, so its stamp is checked on a
    contract listed now: calls expiring on next month's third Friday, struck nearest SPY's
    `/prev` close (strikes every $5) and ten times it for SPXW (every 25 points), then the
    neighbouring strikes, in case the nearest has not traded.

    Returns:
        The SPY candidates and the SPXW candidates, nearest first.
    """
    (spy,) = rest_rows("/v2/aggs/ticker/{stocksTicker}/prev", {"stocksTicker": "SPY"}, {})
    expiry = f"{_third_friday(datetime.now(UTC).date()):%y%m%d}"

    def candidates(root: str, level: float, step: int) -> list[str]:
        strike = round(level / step) * step
        return [f"O:{root}{expiry}C{s * 1000:08d}" for s in (strike, strike + step, strike - step)]

    close = float(spy["c"])
    return candidates("SPY", close, 5), candidates("SPXW", close * 10, 25)


def _file_bar(row: Mapping[str, str | None]) -> StampedBar:
    """Return a day-file row's `window_start` (as epoch ms) and its values.

    Returns:
        The stamped bar.
    """
    keys = ("open", "high", "low", "close", "volume")
    values = tuple(None if row.get(k) is None else float(str(row[k])) for k in keys)
    return int(str(row["window_start"])) // _MS, values


def _stamp_contract_lines(
    session: dm.MassiveSession,
    ticker: str,
    *,
    reports: tuple[DayBarReport, PeriodBarReport],
    library_stamps: LibraryStamps,
) -> tuple[list[str], bool]:
    """Read one stamp contract's 1-day bars and week bars, raw and through the library.

    Returns:
        The evidence lines, and whether both stamp days and the weeks had bars.
    """
    day_bar_report, period_bar_report = reports
    day_lines, days = day_bar_report("optionsTicker", ticker, {})
    week_lines, weeks = period_bar_report("optionsTicker", ticker, {"week": _STAMP_WEEKS})
    rest = dm.HistorySource.REST
    daily = session.options.bars(ticker, dm.Timespan.DAY)
    weekly = session.options.bars(ticker, dm.Timespan.WEEK)
    library = [
        f"library {ticker} history(REST): "
        + library_stamps(lambda: daily.history(*_STAMP_DAYS, source=rest)),
        f"library {ticker} week history: "
        + library_stamps(lambda: weekly.history(*_STAMP_WEEKS, source=rest)),
    ]
    return [*day_lines, *week_lines, *library], days == len(_STAMP_DAYS) and weeks > 0


@pytest.mark.needs("options:basic", "stocks:basic")
@pytest.mark.settles("51")
@pytest.mark.requests(26)
@pytest.mark.timeout(100)
def test_option_day_and_week_bar_stamps_from_rest(
    live_session: dm.MassiveSession,
    *,
    rest_rows: RestRows,
    day_bar_report: DayBarReport,
    prev_bar_report: PrevBarReport,
    period_bar_report: PeriodBarReport,
    library_stamps: LibraryStamps,
    evidence: Evidence,
) -> None:
    """How REST stamps a SPY and an SPXW contract's 1-day and week bars.

    For `O:SPY241220C00600000` and `O:SPXW241220C06000000` it records, each stamp in UTC and
    New York time, the range endpoint's 1-day bar of 2024-11-27 (a full day) and 2024-11-29 (a
    half day, closing at 13:00, 13:15 for SPXW) and the week bars of 2024-11-18 to
    2024-12-01. `/prev` serves only the most recent completed trading day, so it is read for a
    SPY and an SPXW call listed now, beside the range bar of that day, with whether their
    values agree. The library's windows for the same requests follow. Reading it: range
    stamps at 00:00 New York time and a `/prev` stamp at 16:00 (16:15 for SPXW; 13:00 or
    13:15 on a short day) confirm the library's day label; any other stamp is refused with a
    `MassiveError`, shown in the library lines.
    """
    complete = []
    for ticker in _STAMP_CONTRACTS:
        lines, whole = _stamp_contract_lines(
            live_session,
            ticker,
            reports=(day_bar_report, period_bar_report),
            library_stamps=library_stamps,
        )
        for line in lines:
            evidence(line)
        complete.append(whole)
    answered = []
    for candidates in _current_contracts(rest_rows):
        for ticker in candidates:
            lines, found = prev_bar_report("optionsTicker", ticker)
            for line in lines:
                evidence(line)
            if found:
                bars = live_session.options.bars(ticker, dm.Timespan.DAY)
                evidence(f"library {ticker} fetch() (/prev): {library_stamps(bars.fetch)}")
                answered.append(ticker)
                break
    assert all(complete), "a stamp contract lacks a 1-day or a week bar"
    assert len(answered) == len(_STAMP_CONTRACTS), f"/prev answered only for {answered}"


@pytest.mark.needs("options:starter", "s3")
@pytest.mark.settles("51")
@pytest.mark.timeout(100)
def test_option_day_bar_stamps_of_the_day_file(
    live_session: dm.MassiveSession,
    *,
    flat_file_tickers: FlatFileTickers,
    day_bar_report: DayBarReport,
    library_stamps: LibraryStamps,
    evidence: Evidence,
) -> None:
    """How `us_options_opra/day_aggs_v1` stamps the SPY and SPXW contracts' 1-day bars.

    Records each contract's file row of 2024-11-27 (a full day) and 2024-11-29 (a half day),
    its `window_start` in UTC and New York time, beside the range endpoint's bar of the same
    day and whether their open, high, low, close, and volume agree, and the library's
    `history(source=FLAT_FILES)` windows. Reading it: a stamp at 00:00 New York time and
    agreeing values confirm one day label for the files and REST.
    """
    files: dict[str, dict[date, list[StampedBar]]] = {t: {} for t in _STAMP_CONTRACTS}
    for day in _STAMP_DAYS:
        result = flat_file_tickers(_DAY_FILE, day, _STAMP_CONTRACTS)
        for row in [] if result is None else result[0]:
            files[str(row["ticker"])].setdefault(day, []).append(_file_bar(row))
    for ticker, by_day in files.items():
        lines, _ = day_bar_report("optionsTicker", ticker, {_DAY_FILE: by_day})
        for line in lines:
            evidence(line)
        as_traded = live_session.options.bars(ticker, dm.Timespan.DAY, adjusted=False)
        outcome = library_stamps(
            partial(as_traded.history, *_STAMP_DAYS, source=dm.HistorySource.FLAT_FILES)
        )
        evidence(f"library {ticker} history(FLAT_FILES): {outcome}")
    missing = [(t, d) for t in _STAMP_CONTRACTS for d in _STAMP_DAYS if not files[t].get(d)]
    assert not missing, f"the day files lack {missing}"


_NVDA_CONTRACT = "O:NVDA240621C00120000"
"""An NVDA call expiring after its 10-for-1 split of 2024-06-10, under its post-split strike."""


@pytest.mark.needs("options:basic")
@pytest.mark.settles("57")
@pytest.mark.requests(2)
@pytest.mark.timeout(100)
def test_option_bars_across_the_underlyings_split(
    live_session: dm.MassiveSession, *, table_rows: TableRows, evidence: Evidence
) -> None:
    """Whether option bars are split-adjusted for their underlying's split.

    Records an NVDA call's day bars from 2024-06-05 to 2024-06-12 with `adjusted=true` and
    `adjusted=false`. Reading it: prices before 2024-06-10 that differ by the split's ratio mean
    options are adjusted in full like stocks; equal prices, or no bars before the split under
    the post-split ticker, mean the contract was renamed instead, and the docstrings must say
    so.
    """
    # A request that fails raises; an empty result is evidence in itself (a renamed contract).
    for adjusted in (True, False):
        rows = table_rows(
            live_session.options.bars(_NVDA_CONTRACT, dm.Timespan.DAY, adjusted=adjusted).history(
                date(2024, 6, 5), date(2024, 6, 12), source=dm.HistorySource.REST
            )
        )
        closes = [(r["Date"], r["Close"], r["Volume"]) for r in rows]
        evidence(f"{_NVDA_CONTRACT} adjusted={adjusted}: (Date, Close, Volume) {closes}")
