"""Live checks of options data against Massive: underlyings, deliverables, and bar stamps.

Each test records what Massive sent (`evidence`). A test marked `settles` asserts only that the
requests worked, and its evidence, not a pass, answers the question; a test marked `verifies` also
asserts the answer the library assumes, and fails when Massive no longer gives it. Raw rows come
from the wire REST client, because "a key is missing" and "a key holds an empty list" look the same
once the library has read them; the library's own answer is recorded beside them. The underlying
and contract-symbol tests send values the library refuses or used to (`tests/_live/bad_values.py`)
and record whether Massive refuses them too. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, date, datetime, timedelta
from datetime import time as dt_time
from functools import partial
from typing import TYPE_CHECKING, Any, Literal, assert_never
from zoneinfo import ZoneInfo

import pytest
from deephaven import dtypes, new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._datasets.wire_symbols import is_index_option
from deephaven_massive._ingest.rate_limits import OTHER
from deephaven_massive._massive import (
    RestClient,
    Transport,
    WireEntitlementError,
    WireError,
    source,
)
from tests._live import bad_values, calendar, greeks, option_requests, refusals
from tests._live.feeds import with_delayed

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Mapping
    from typing import TypeAlias

    from deephaven.table import Table
    from deephaven.table_factory import InputTable

    from deephaven_massive.queries import FetchQuery

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Evidence: TypeAlias = "Callable[[str], None]"
MinuteSpanReport: TypeAlias = "Callable[[str, date], int]"
RestRows: TypeAlias = (
    "Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]]"
)
FlatFileTickers: TypeAlias = (
    "Callable[[str, date, Collection[str] | None], "
    "tuple[list[dict[str, str | None]], frozenset[str]] | None]"
)
StampedBar: TypeAlias = "tuple[int, tuple[float | None, ...]]"
StampDays: TypeAlias = "dict[str, tuple[date, date]]"
RangeBars: TypeAlias = "Callable[[str, str, tuple[date, date]], list[dict[str, Any]]]"
ToStampedBar: TypeAlias = "Callable[[Mapping[str, Any]], StampedBar]"
DayBars: TypeAlias = "dict[date, list[StampedBar]]"
DayBarReport: TypeAlias = (
    "Callable[[str, tuple[date, ...], Mapping[str, Mapping[date, list[StampedBar]]]], DayBars]"
)
PrevBarReport: TypeAlias = "Callable[[str], list[StampedBar]]"
PeriodBarReport: TypeAlias = (
    "Callable[[str, Mapping[str, tuple[date, date]], Mapping[str, tuple[int, int]]], bool]"
)
LibraryStamps: TypeAlias = "Callable[[Callable[[], Table]], str]"
StreamReport: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
WaitAcked: TypeAlias = "Callable[[dm.MassiveSession, int, float], int]"
SkipIfNotEntitled: TypeAlias = "Callable[[dm.MassiveSession], None]"

_CONTRACTS = option_requests.CONTRACTS
_NEW_YORK = ZoneInfo("America/New_York")
_CHAIN = "/v3/snapshot/options/{underlyingAsset}"
_SNAPSHOT = "/v3/snapshot"

_UNDERLYINGS = ("SPX", "I:SPX", "SPXW", "NDX", "NDXP", "VIX", "VIXW", "BRK.B", "BRKB")
"""Underlyings whose spelling the options endpoints may not agree on."""

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
)
"""(contract root, underlying, a day the contracts were listed): three adjusted roots, after a
spin-off, a special dividend, and a split. Adjusted roots are rare and named by hand, so the
older ones lie beyond a short history; a root whose day Massive refuses the key is recorded with
Massive's refusal and passed over."""


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
    goes on; so is a snapshot Massive refuses, labelled "not entitled" with Massive's refusal.

    Returns:
        The rows; empty after a rejection or a refused snapshot.

    Raises:
        WireEntitlementError: When the contracts reference itself is refused.
    """
    path_params = {"underlyingAsset": params["underlyingAsset"]} if "{" in path else {}
    query = {key: value for key, value in params.items() if key != "underlyingAsset"}
    try:
        return client.get(source(Transport.REST, path), path_params, query, bucket="options").rows
    except WireEntitlementError as error:
        if path == _CONTRACTS:
            raise
        evidence(f"{path} {dict(params)}: not entitled: {refusals.refusal_line(error)}")
        return []
    except WireError as error:
        evidence(f"{path} {dict(params)}: {type(error).__name__}: {error}")
        return []


def _library(build: Callable[[], FetchQuery], table_rows: TableRows) -> list[dict[str, Any]] | str:
    """Build one library query, fetch it, and describe the outcome.

    Returns:
        The result's rows, or the text of the error building or fetching raised.
    """
    try:
        return table_rows(build().fetch(wait=True))
    except dm.EntitlementError as error:
        return f"not entitled: {refusals.refusal_line(error)}"
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
        WireEntitlementError: When Massive refuses the key the listing.
    """
    prefix = f"O:{root}"
    found: list[dict[str, Any]] = []
    read = pages = 0
    last = ""
    query = {**query, "sort": "ticker", "order": "asc", "limit": _CONTRACT_PAGE}
    try:
        for page in client.pages(source(Transport.REST, _CONTRACTS), {}, query, bucket="options"):
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


@pytest.mark.settles("45")
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
    `underlying_asset.ticker` of one contract, and the library's `chain()` and `contracts()`:
    up to 45 requests. A snapshot Massive refuses is recorded with Massive's refusal and the
    rest still run. Reading it: the spelling that
    answers is the one the library must send for `underlying=`; a root such as `SPXW` listed
    under `SPX` means roots and underlyings differ. The chain path sends `I:SPX`
    percent-encoded, so an `I:SPX` chain result counts only if the encoded-colon check of the
    indices tests passes.
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
        roots = sorted({_root(str(r.get("ticker", ""))) for r in contracts})
        spelled = sorted({str(r.get("underlying_ticker")) for r in contracts})
        evidence(
            f"{underlying}: contracts {len(contracts)} rows; roots {roots}; "
            f"underlying_ticker {spelled}"
        )
        chain = _raw(
            live_rest,
            _CHAIN,
            params={"underlyingAsset": underlying, "limit": _CHAIN_PAGE},
            evidence=evidence,
        )
        answered += bool(contracts) + bool(chain)
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


@pytest.mark.verifies("46")
@pytest.mark.requests(13)
@pytest.mark.timeout(400)
def test_deliverable_fields_of_adjusted_and_standard_contracts(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    recent_monday: date,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """How the contracts reference describes adjusted and standard deliverables.

    Lists the contracts of the adjusted roots `DELL1` (as of 2021-11-15, after a spin-off), `FF1`
    (2024-04-15, a special dividend), and `AAPL7` (2014-06-10, the 7:1 split), and of standard
    `AAPL` and `SPX` contracts (the most recent published Monday), looked up by underlying
    (following pages until the root is passed) and by root, and records `additional_underlyings` (or
    `<missing>`), `cfi`, `correction`, and `shares_per_contract` of up to three of each, and the
    library's columns for the first. Every root is asked for; a root whose day Massive refuses the
    key (its refusal of the data timeframe) is recorded, "outside the plan's history" with Massive's
    refusal, and the rest still run. Passes when some root was read and no standard contract carries
    a non-empty `additional_underlyings`, so null and `[]` both mark a standard deliverable, as the
    chain's documented recipe assumes. Reading the rest: an adjusted contract with no list (a
    split's, which changes only the share count) means only `Cfi` or the root's trailing digit can
    tell it from a standard one. A root found by neither lookup, with the page counts showing the
    listing was read past it, is not kept by Massive for that day.

    Raises:
        WireEntitlementError: When Massive refuses a listing for another reason than its day;
            the live tests' hook skips the test, quoting Massive.
    """
    found = 0
    standard = (("AAPL", "AAPL", recent_monday), ("SPX", "SPX", recent_monday))
    roots = (*_ADJUSTED, *standard)
    listed_extra: list[str] = []
    for root, underlying, listed in roots:
        evidence(f"{root} (underlying {underlying}, as of {listed}):")
        try:
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
        except WireEntitlementError as error:
            if not refusals.is_timeframe_refusal(error):
                raise
            evidence(f"    outside the plan's history (Massive: {refusals.refusal_line(error)})")
            continue
        rows = list({str(row["ticker"]): row for row in matching}.values())
        found += bool(rows)
        evidence(f"    {len(rows)} contracts of root {root}")
        if (root, underlying, listed) in standard:
            listed_extra += [str(r["ticker"]) for r in rows if r.get("additional_underlyings")]
        for row in rows[:3]:
            present = {key: row.get(key, "<missing>") for key in _DELIVERABLE_FIELDS}
            evidence(f"    {row.get('ticker')} underlying={row.get('underlying_ticker')} {present}")
        if rows:
            ticker = str(rows[0]["ticker"])
            outcome = _library(
                partial(live_session.options.contract_details, ticker, as_of=listed), table_rows
            )
            shown = (
                outcome
                if isinstance(outcome, str)
                else [{k: r.get(k) for k in _DELIVERABLE_COLUMNS} for r in outcome]
            )
            evidence(f"    library contract_details({ticker}): {shown}")
    assert found
    assert not listed_extra, f"standard contracts with further deliverables: {listed_extra[:5]}"


_DAY_FILE = "us_options_opra/day_aggs_v1"
_SPY_PREV = "/v2/aggs/ticker/{stocksTicker}/prev"
_SPX_SPELLINGS = ("SPX", "I:SPX", "SPXW")
"""The `underlying_ticker` spellings tried for SPXW contracts, which the reference may file
under any of them."""

_PREV_TRIES = 5
"""How many listed calls of a root `/prev` is asked for, in turn, until one has a bar: a
listed contract that did not trade on the previous day has none."""


_DAY_STAMPS = frozenset({(0, 0), (13, 0), (13, 15), (16, 0), (16, 15)})
"""The New York times of day (hour, minute) at which the library expects an option's 1-day bar
stamp: midnight, and the closes of a half day and a full day, for equity and index options."""


def _day_stamp(millis: int, day: date) -> bool:
    """Tell whether an option 1-day bar's stamp is one the library expects for its day.

    Args:
        millis: The stamp, in epoch milliseconds.
        day: The bar's trading day.

    Returns:
        True for a `_DAY_STAMPS` time, to the second, of `day` in New York.
    """
    local = datetime.fromtimestamp(millis / 1000, UTC).astimezone(_NEW_YORK)
    return local.date() == day and local.second == 0 and (local.hour, local.minute) in _DAY_STAMPS


def _new_york_day(millis: int) -> date:
    """Return the New York calendar day of an epoch-millisecond stamp.

    `/prev` serves the most recent completed trading day, which the test does not know in
    advance, so its bar's day is the day its stamp falls on.

    Returns:
        The day.
    """
    return datetime.fromtimestamp(millis / 1000, UTC).astimezone(_NEW_YORK).date()


def _monthly_expiry_after(day: date) -> date:
    """Return the standard monthly expiration of the month after a day's: its third Friday.

    Monthly contracts of SPY and SPXW are listed months ahead, so on a half day and the day
    before it the next month's are listed and trading, with weeks left to expiry. When the third
    Friday is an exchange holiday, as Juneteenth is on June 18, 2027, the contracts expire on the
    trading day before it.

    Returns:
        The date, between the 14th and the 21st of the next month.
    """
    first = (day.replace(day=1) + timedelta(days=32)).replace(day=1)
    friday = first + timedelta(days=(4 - first.weekday()) % 7 + 14)
    return friday if calendar.is_trading_day(friday) else calendar.trading_day_before(friday)


def _stamp_candidates(range_bars: RangeBars, days: tuple[date, date]) -> dict[str, list[str]]:
    """Return SPY and SPXW calls that were listed on the stamp days, nearest strike first.

    The calls expire on the third Friday of the month after the half day's. SPY strikes are
    taken every $5 around SPY's close of the full day, and SPXW strikes every 25 points around
    ten times it (SPX trades near ten times SPY); each list ends with the two neighbouring
    strikes, in case the nearest did not trade.

    Returns:
        The candidates, by root.
    """
    full_day, half_day = days
    bars = range_bars("SPY", "day", (full_day, full_day))
    assert len(bars) == 1, f"SPY range 1-day bars of {full_day}: {bars}"
    close = float(bars[0]["c"])
    code = f"{_monthly_expiry_after(half_day):%y%m%d}"

    def candidates(root: str, level: float, step: int) -> list[str]:
        strike = round(level / step) * step
        return [f"O:{root}{code}C{s * 1000:08d}" for s in (strike, strike + step, strike - step)]

    return {"SPY": candidates("SPY", close, 5), "SPXW": candidates("SPXW", close * 10, 25)}


def _listed_calls(
    live_rest: RestClient, rest_rows: RestRows, evidence: Evidence
) -> dict[str, list[str]]:
    """Return SPY and SPXW calls listed today, from the money upward, for `/prev`.

    `/prev` serves only the most recent completed trading day, so its stamp is checked on
    contracts listed now. Each root's candidates are its `_PREV_TRIES` lowest-strike calls at
    or above the money (SPY's `/prev` close, ten times it for SPXW) expiring on the third
    Friday of next month, the most traded expiry, from one page of the contracts reference.
    SPXW is looked up under each spelling of its underlying in turn, and the spelling that
    answered is recorded.

    Returns:
        The tickers by root, lowest strike first; empty for a root with no such contract.
    """
    prevs = rest_rows(_SPY_PREV, {"stocksTicker": "SPY"}, {})
    assert len(prevs) == 1, f"SPY /prev returned {prevs}"
    close = float(prevs[0]["c"])
    query = {
        "contract_type": "call",
        "expiration_date": _monthly_expiry_after(datetime.now(UTC).date()),
        "sort": "strike_price",
        "order": "asc",
    }
    contracts = source(Transport.REST, _CONTRACTS)
    spy = live_rest.get(
        contracts,
        {},
        query | {"underlying_ticker": "SPY", "strike_price.gte": close, "limit": _PREV_TRIES},
        bucket="options",
    ).rows
    found: dict[str, list[str]] = {"SPY": [str(r["ticker"]) for r in spy], "SPXW": []}
    for spelling in _SPX_SPELLINGS:
        rows = live_rest.get(
            contracts,
            {},
            query
            | {
                "underlying_ticker": spelling,
                "strike_price.gte": close * 10,
                "limit": _CONTRACT_PAGE,
            },
            bucket="options",
        ).rows
        spxw = [str(r["ticker"]) for r in rows if str(r["ticker"]).startswith("O:SPXW")]
        evidence(
            f"contracts reference underlying_ticker={spelling}: {len(rows)} calls, "
            f"SPXW {spxw[:_PREV_TRIES]}"
        )
        if spxw:
            found["SPXW"] = spxw[:_PREV_TRIES]
            break
    return found


def _stamp_contract_weeks(
    session: dm.MassiveSession,
    ticker: str,
    *,
    stamp_days: StampDays,
    report: PeriodBarReport,
    library_stamps: LibraryStamps,
    evidence: Evidence,
) -> bool:
    """Record one stamp contract's week bars, raw and through the library, and its library days.

    Returns:
        Whether the range returned one week bar for each of the two weeks.
    """
    complete = report(ticker, {"week": stamp_days["week"]}, {"week": (2, 2)})
    rest = dm.HistorySource.REST
    daily = session.options.bars(ticker, dm.Timespan.DAY)
    evidence(
        f"library {ticker} history(REST): "
        + library_stamps(partial(daily.history, *stamp_days["day"], source=rest, wait=True))
    )
    weekly = session.options.bars(ticker, dm.Timespan.WEEK)
    evidence(
        f"library {ticker} week history: "
        + library_stamps(partial(weekly.history, *stamp_days["week"], source=rest, wait=True))
    )
    return complete


def _prev_bars(
    session: dm.MassiveSession,
    ticker: str,
    *,
    report: PrevBarReport,
    library_stamps: LibraryStamps,
    evidence: Evidence,
) -> list[StampedBar]:
    """Record one listed contract's `/prev` bar, raw and through the library.

    A refusal propagates; any other error of the raw request is recorded instead.

    Returns:
        The raw `/prev` bars, stamped; empty when `/prev` returned none or failed.

    Raises:
        WireEntitlementError: When the key's plan refuses the request.
    """
    try:
        found = report(ticker)
    except WireEntitlementError:
        raise
    except WireError as error:
        evidence(f"{ticker} /prev: {type(error).__name__}: {error}")
        return []
    previous = session.options.previous_day_bar(ticker)
    prev_stamps = library_stamps(partial(previous.fetch, wait=True))
    evidence(f"library {ticker} previous_day_bar fetch() (/prev): {prev_stamps}")
    return found


@pytest.mark.verifies("51")
@pytest.mark.requests(2)
@pytest.mark.timeout(130)
def test_option_day_and_week_bar_stamps_from_rest(
    live_session: dm.MassiveSession,
    live_rest: RestClient,
    *,
    stamp_days: StampDays,
    rest_rows: RestRows,
    range_bars: RangeBars,
    day_bar_report: DayBarReport,
    prev_bar_report: PrevBarReport,
    period_bar_report: PeriodBarReport,
    minute_span_report: MinuteSpanReport,
    library_stamps: LibraryStamps,
    evidence: Evidence,
) -> None:
    """How REST stamps a SPY and an SPXW contract's 1-day, minute, and week bars.

    The days are the most recent half day whose week has ended (13:00 close; 13:15 for SPXW)
    and the full trading day before it. For a SPY and an SPXW call expiring on the next month's
    third Friday, struck near SPY's close of the full day (the first
    strike with a range bar on both days), it records each stamp in UTC and New York time:
    the range endpoint's 1-day bars around both days, the first and last minute bar of each
    day, and the week bars of the half day's week and the week before. `/prev` serves only the
    most recent completed trading day, so it is read for a SPY and an SPXW call listed now,
    from the contracts reference: per root, the first of up to five strikes from the money
    upward that has a `/prev` bar, as a contract that did not trade that day has none. Each is
    read beside the range bars of that day, with whether it is a half day and which day's bar
    it matches. The library's windows for the same requests
    follow. Reading it: range stamps at 00:00 New York time, a last minute bar at 12:59 (13:14
    for SPXW) on the half day, and a `/prev` stamp at 16:00 (16:15 for SPXW; 13:00 or 13:15
    after a half day) confirm the library's day label; a bar at any other stamp is served,
    labelled by the New York day its stamp falls on, with a WARNING in the log. Fails when a
    range 1-day bar or a `/prev` bar is stamped at a time other than one the library expects
    for its day: 00:00, 13:00, 13:15, 16:00, or 16:15 New York time, and when no listed call of
    a root has a `/prev` bar.
    """
    days = stamp_days["day"]
    candidates = _stamp_candidates(range_bars, days)
    complete: dict[str, bool] = {}
    odd: list[tuple[str, str, date, int]] = []
    for root, tickers in candidates.items():
        for ticker in tickers:
            ranged = day_bar_report(ticker, days, {})
            if not all(ranged.values()):
                continue
            odd += [
                ("range", ticker, day, bar[0])
                for day, bars in ranged.items()
                for bar in bars
                if not _day_stamp(bar[0], day)
            ]
            for day in days:
                minute_span_report(ticker, day)
            complete[root] = _stamp_contract_weeks(
                live_session,
                ticker,
                stamp_days=stamp_days,
                report=period_bar_report,
                library_stamps=library_stamps,
                evidence=evidence,
            )
            break
    listed = _listed_calls(live_rest, rest_rows, evidence)
    prevs: dict[str, list[StampedBar]] = {}
    for root, tickers in listed.items():
        missed: list[str] = []
        for ticker in tickers:
            bars = _prev_bars(
                live_session,
                ticker,
                report=prev_bar_report,
                library_stamps=library_stamps,
                evidence=evidence,
            )
            if bars:
                prevs[ticker] = bars
                evidence(f"{root} /prev answered for {ticker}; no bar before it for {missed}")
                break
            missed.append(ticker)
        else:
            evidence(f"{root} /prev: no listed call has a bar; tried {missed}")
    answered = [_root(ticker) for ticker in prevs]
    odd += [
        ("/prev", ticker, _new_york_day(bar[0]), bar[0])
        for ticker, bars in prevs.items()
        for bar in bars
        if not _day_stamp(bar[0], _new_york_day(bar[0]))
    ]
    unpriced = [root for root in candidates if root not in complete]
    assert not unpriced, f"no {unpriced} candidate has a range bar on both days: {candidates}"
    assert all(complete.values()), f"a stamp contract lacks a week bar of one week: {complete}"
    assert sorted(answered) == sorted(listed), (
        f"/prev answered for {list(prevs)}; listed calls by root, all tried: {listed}"
    )
    assert not odd, f"(source, ticker, day, stamp ms) the library does not expect: {odd}"


@pytest.mark.verifies("51")
@pytest.mark.requests(1)
@pytest.mark.timeout(100)
def test_option_day_bar_stamps_of_the_day_file(
    live_session: dm.MassiveSession,
    *,
    stamp_days: StampDays,
    range_bars: RangeBars,
    flat_file_tickers: FlatFileTickers,
    stamped_bar: ToStampedBar,
    day_bar_report: DayBarReport,
    library_stamps: LibraryStamps,
    evidence: Evidence,
) -> None:
    """How `us_options_opra/day_aggs_v1` stamps a SPY and an SPXW contract's 1-day bars.

    Uses the candidates of the REST stamp test (next-month calls struck near SPY's close of the
    full day) and, for each root, the first strike the file holds on both the most recent half
    day whose week has ended and the full trading day before it. Records its file rows, each
    `window_start` in UTC and New York time, beside the range endpoint's bars around those
    days and which day's bar each matches with the largest relative error, and the library's
    `history(source=FLAT_FILES)` windows. Passes when the file holds a candidate of each root
    on both days, stamped where the library expects an option's 1-day bar: at 00:00, 13:00,
    13:15, 16:00, or 16:15 New York time of its own day. Reading the rest: a match with the
    range bar of the same day confirms one day label for the files and REST.
    """
    days = stamp_days["day"]
    roots = _stamp_candidates(range_bars, days)
    files: dict[str, dict[date, list[StampedBar]]] = {t: {} for c in roots.values() for t in c}
    for day in days:
        result = flat_file_tickers(_DAY_FILE, day, list(files))
        for row in [] if result is None else result[0]:
            files[str(row["ticker"])].setdefault(day, []).append(stamped_bar(row))
    chosen = {
        root: next((t for t in tickers if len(files[t]) == len(days)), None)
        for root, tickers in roots.items()
    }
    for ticker in filter(None, chosen.values()):
        day_bar_report(ticker, days, {_DAY_FILE: files[ticker]})
        as_traded = live_session.options.bars(ticker, dm.Timespan.DAY, adjusted=False)
        outcome = library_stamps(
            partial(as_traded.history, *days, source=dm.HistorySource.FLAT_FILES, wait=True)
        )
        evidence(f"library {ticker} history(FLAT_FILES): {outcome}")
    assert all(chosen.values()), f"the day files hold no candidate on both days: {roots}"
    odd = [
        (ticker, day, bar[0])
        for ticker in filter(None, chosen.values())
        for day, bars in files[ticker].items()
        for bar in bars
        if not _day_stamp(bar[0], day)
    ]
    assert not odd, f"day-file stamps the library does not expect: {odd}"


_SPLIT_DAY = date(2024, 6, 10)
_SPLIT_RATIO = 10
_SPLIT_CONTRACTS = ("O:NVDA240621C01200000", "O:NVDA240621C00120000")
"""A liquid call across NVDA's 10-for-1 split of 2024-06-10, by its ticker before the split and
after: the June 21 call struck at $1,200 before and at $120 after, near the money on both sides.
The split changed the contract's ticker with its strike, and the live run of 2026-11-03 found the
later ticker's bars only from the split day, under both bases, so a read of either ticker alone
cannot cross the split. Recent forward splits are mostly thin over-the-counter tickers without
listed options, and the lowest-strike call a contracts listing returns first may not trade at
all."""


@pytest.mark.settles("57")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_option_bars_across_the_underlyings_split(
    live_session: dm.MassiveSession,
    *,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether option bars are split-adjusted for their underlying's split.

    Reads the day bars of one expired NVDA call across NVDA's 10-for-1 split of 2024-06-10,
    under its ticker before the split, `O:NVDA240621C01200000`, and after it,
    `O:NVDA240621C00120000`, from three days before the split to two days after, with
    `adjusted=true` and `adjusted=false`. The split is more than two years old, so Massive may
    refuse a key its days; the test then skips, quoting Massive and the earliest day asked.
    Fails unless the `adjusted=true` reads of the two
    tickers together have bars on both sides of the split, without which nothing can be read from
    them. Reading it: prices before the split that differ by its ratio between the two reads mean
    options are adjusted in full like stocks; equal prices mean the contract's history is kept as
    traded, and the docstrings must say so. Bars of a ticker on the other side of the split mean
    Massive carries the history across the ticker change.
    """
    evidence(f"{_SPLIT_CONTRACTS}: one call on NVDA, split {_SPLIT_RATIO}:1 on {_SPLIT_DAY}")
    adjusted_days: list[str] = []
    for contract in _SPLIT_CONTRACTS:
        for adjusted in (True, False):
            rows = table_rows(
                live_session.options.bars(contract, dm.Timespan.DAY, adjusted=adjusted).history(
                    _SPLIT_DAY - timedelta(days=3),
                    _SPLIT_DAY + timedelta(days=2),
                    source=dm.HistorySource.REST,
                    wait=True,
                )
            )
            if adjusted:
                adjusted_days += [str(r["Date"]) for r in rows]
            closes = [(r["Date"], r["Close"], r["Volume"]) for r in rows]
            evidence(f"{contract} adjusted={adjusted}: (Date, Close, Volume) {closes}")
    split = _SPLIT_DAY.isoformat()
    days = sorted(adjusted_days)
    assert any(day < split for day in days), f"no adjusted bar before {split}: {days}"
    assert any(day >= split for day in days), f"no adjusted bar from {split}: {days}"


_FOLLOW_WAIT = 90.0
"""Seconds a followed read's step may take."""


def _underlyings_until(
    table_rows: TableRows, result: Table, done: Callable[[set[str]], bool]
) -> set[str]:
    """Read a result's underlyings until `done` holds, failing after `_FOLLOW_WAIT` seconds.

    Returns:
        The distinct `Underlying` values that satisfied it.
    """
    pause = threading.Event()  # never set; only paces the loop
    deadline = time.monotonic() + _FOLLOW_WAIT
    while True:
        seen = {str(row["Underlying"]) for row in table_rows(result)}
        if done(seen):
            return seen
        assert time.monotonic() < deadline, f"the followed result never got there: {seen}"
        pause.wait(0.25)


@pytest.mark.timeout(300)
def test_the_chain_follows_a_live_table_of_underlyings(
    live_session: dm.MassiveSession,
    *,
    table_rows: TableRows,
    evidence: Evidence,
    dh_input_table: Callable[..., InputTable],
) -> None:
    """Whether `fetch()` of the option chain follows a live table of underlyings.

    Follows an input table holding `SPY` with `m.options.chain(underlyings,
    contract_type=dm.ContractType.CALL, limit=20).fetch()`, adds `QQQ` (one request), and
    deletes it. Passes when the result holds only SPY's contracts at the call, gains QQQ's once
    QQQ is read, and loses them once QQQ's row is deleted, while SPY's stay. Each step waits at
    most 90 s.
    """
    m = live_session
    underlyings = dh_input_table({"Symbol": dtypes.string}, key_cols="Symbol")
    underlyings.add(new_table([string_col("Symbol", ["SPY"])]))
    query = m.options.chain(underlyings, contract_type=dm.ContractType.CALL, limit=20)
    result = query.fetch(wait=True)
    try:
        first = _underlyings_until(table_rows, result, lambda seen: seen == {"SPY"})
        evidence(f"at the call: {sorted(first)}, {result.size} rows")
        underlyings.add(new_table([string_col("Symbol", ["QQQ"])]))
        added = _underlyings_until(table_rows, result, lambda seen: seen == {"SPY", "QQQ"})
        evidence(f"after adding QQQ: {sorted(added)}, {result.size} rows")
        underlyings.delete(new_table([string_col("Symbol", ["QQQ"])]))
        left = _underlyings_until(table_rows, result, lambda seen: seen == {"SPY"})
        evidence(f"after deleting QQQ: {sorted(left)}, {result.size} rows")
    finally:
        m.release(result)


_ADMIN_WAIT = 60.0
"""Seconds the admin messages test waits for Massive's acknowledgement, and then for a row."""


@pytest.mark.verifies("82")
@pytest.mark.timeout(150)
def test_admin_messages_stream_every_exchange_on_the_real_time_feed(
    live_session: dm.MassiveSession,
    *,
    wait_acked: WaitAcked,
    table_rows: TableRows,
    stream_report: StreamReport,
    skip_if_not_entitled: SkipIfNotEntitled,
    evidence: Evidence,
) -> None:
    """Whether Massive acknowledges `AD.*` on the real-time options feed, and what rows carry.

    Streams `m.options.admin_messages()`, which subscribes every exchange (`AD.*`) on
    `socket.massive.com`, and records the connection, the stream, and up to five rows: each
    one's exchange, sequence number, SIP timestamp, and text as OPRA sent it. Passes when
    Massive acknowledged the topic, which it does whether or not the market is open, the
    stream still runs, and every row that arrived has its `Timestamp` and `Message`. Messages
    come only with FLEX activity or an exchange's notice, so no row is required. Massive's
    refusal skips the test, quoting Massive; the refusal test checks what it says.
    """
    table = live_session.options.admin_messages().append_only()
    acked = wait_acked(live_session, 1, time.monotonic() + _ADMIN_WAIT)
    deadline = time.monotonic() + _ADMIN_WAIT
    pause = threading.Event()  # never set; only paces the wait for a row
    while acked and not table_rows(table) and time.monotonic() < deadline:
        pause.wait(1.0)
    rows = table_rows(table)
    for line in stream_report(live_session):
        evidence(line)
    states = [
        r["State"]
        for r in table_rows(live_session.streams())
        if r["Dataset"] == "options.admin_messages"
    ]
    skip_if_not_entitled(live_session)
    live_session.release(table)
    evidence(f"AD.* acknowledged topics {acked}; stream {states}; rows received: {len(rows)}")
    for row in rows[:5]:
        evidence(
            f"    exchange {row['Exchange']} sequence {row['SequenceNumber']} at "
            f"{row['Timestamp']}: {row['Message']!r}"
        )
    assert acked, "Massive acknowledged no AD.* topic on the real-time options feed"
    assert states == ["RUNNING"], f"the admin messages stream is {states}"
    assert all(row["Timestamp"] is not None and row["Message"] is not None for row in rows)


_ADMIN_PAGE = "https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.admin_messages.md"
"""The dataset page of the admin messages, whose table of the plans that include them every
refusal of them links."""


@pytest.mark.verifies("82")
@pytest.mark.timeout(150)
def test_a_refusal_of_the_admin_messages_links_the_datasets_plans(
    live_config: dm.MassiveConfig,
    *,
    table_rows: TableRows,
    stream_report: StreamReport,
    evidence: Evidence,
) -> None:
    """What a refusal of the admin messages says, and whether it links the dataset's page.

    Streams `m.options.admin_messages()` from a session whose `delayed_subscriptions` leaves
    options out (and any delayed host pin of options). When Massive labels the key's options
    data delayed, the call raises the delayed-data `dm.EntitlementError` every options stream
    gets, naming `delayed_subscriptions`; when Massive refuses `AD.*`, the stream's row turns
    `FAILED` with Massive's refusal. Passes when one of the two happens and Massive's refusal
    ends with the link to the dataset's page of plans. Skips when Massive serves the admin
    messages, which the real-time test checks.
    """
    listed = live_config.delayed_subscriptions - {dm.Subscription.OPTIONS}
    session = dm.MassiveSession(with_delayed(live_config, listed))
    try:
        try:
            table = session.options.admin_messages().append_only()
        except dm.EntitlementError as error:
            outcome, message = "refused at the call", str(error)
        else:
            deadline = time.monotonic() + _ADMIN_WAIT
            pause = threading.Event()  # never set; only paces the wait for the refusal
            rows: list[dict[str, Any]] = []
            while time.monotonic() < deadline:
                rows = [
                    r
                    for r in table_rows(session.streams())
                    if r["Dataset"] == "options.admin_messages"
                ]
                if any(r["State"] == "FAILED" for r in rows):
                    break
                pause.wait(0.5)
            for line in stream_report(session):
                evidence(line)
            outcome = str(rows[0]["State"]) if rows else "no stream row"
            message = str(rows[0]["Error"]) if rows else ""
            session.release(table)
    finally:
        session.close()
    evidence(f"options admin messages: {outcome}: {message}")
    if outcome == "RUNNING":
        pytest.skip("Massive served the admin messages; the real-time test covers them")
    assert outcome in {"refused at the call", "FAILED"}, f"the admin messages stream is {outcome}"
    if outcome == "refused at the call":
        assert "would be delayed" in message, "the call's refusal is not the delayed-data error"
        assert "delayed_subscriptions" in message
    else:
        assert message.endswith(_ADMIN_PAGE), "the refusal does not link the dataset's plans"


_GAMMA_CONTRACTS = 5
"""How many calls near the money the gamma check reads."""

_GAMMA_DAYS = (20, 50)
"""The days to expiry of the calls the gamma check reads: far enough that gamma is smooth."""


@pytest.mark.settles("83")
@pytest.mark.timeout(120)
def test_snapshot_gamma_against_black_scholes_per_dollar_and_per_cent(
    live_session: dm.MassiveSession,
    *,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether `GreeksGamma` of the options snapshot is per $0.01 of the underlying, or per $1.

    Massive's description, which the dataset page quotes, says per $0.01. Reads SPY's price
    from its option chain (or its previous close, when the chain carries none), up to five
    calls near the money that expire in 20 to 50 days, their snapshots, and the latest
    3-month Treasury yield as the risk-free rate, with no dividend yield. For each call it
    records `GreeksGamma`, the Black-Scholes gamma per $1 from the call's strike, its time to
    expiry, and its `ImpliedVolatility` (`tests/_live/greeks.py`), their ratio, and the unit
    the ratio matches. Passes when the snapshots returned their calls, whatever the unit: the
    evidence, read by a person, settles it.
    """
    today = calendar.today()
    rate, rate_text = _risk_free_rate(live_session, table_rows, today)
    spot, symbols = _calls_near_the_money(live_session, table_rows, today)
    evidence(f"SPY {spot}, {rate_text}")
    if not symbols:
        pytest.skip(f"no call near SPY's {spot} carries greeks now")
    now = datetime.now(UTC)
    units = []
    for row in table_rows(live_session.options.snapshots(symbols).fetch(wait=True)):
        expiry = datetime.combine(
            date.fromisoformat(row["ExpirationDate"]), dt_time(16, 0), _NEW_YORK
        )
        textbook = greeks.black_scholes_gamma(
            spot=spot,
            strike=row["StrikePrice"],
            years=(expiry - now).total_seconds() / (365 * 24 * 3600),
            volatility=row["ImpliedVolatility"],
            rate=rate,
        )
        unit = greeks.gamma_unit(row["GreeksGamma"], textbook)
        units.append(unit)
        evidence(
            f"{row['Symbol']}: GreeksGamma {row['GreeksGamma']}, Black-Scholes per $1 "
            f"{textbook:.6g}, ratio {row['GreeksGamma'] / textbook:.4g}: {unit}"
        )
    assert units, "the snapshots returned no row"
    evidence(f"units: {units}; Massive's description says per $0.01")


def _risk_free_rate(
    live_session: dm.MassiveSession, table_rows: TableRows, today: date
) -> tuple[float, str]:
    """Return the newest 3-month Treasury yield as a fraction, and a line that names it."""
    start = (today - timedelta(days=14)).isoformat()
    yields = table_rows(live_session.economy.treasury_yields(start=start).fetch(wait=True))
    dated = [row for row in yields if row["Yield3Month"] is not None]
    assert dated, f"no 3-month Treasury yield since {start}"
    newest = max(dated, key=lambda r: r["Date"])
    named = f"3-month yield {newest['Yield3Month']}% of {newest['Date']}"
    return float(newest["Yield3Month"]) / 100, named


def _calls_near_the_money(
    live_session: dm.MassiveSession, table_rows: TableRows, today: date
) -> tuple[float, list[str]]:
    """Return SPY's price and up to `_GAMMA_CONTRACTS` calls nearest it that carry greeks.

    The price is the chain's `UnderlyingAssetPrice`, or SPY's previous close when the chain
    carries none, as on a plan without Stocks data.
    """
    window = {
        "expiration_date_gte": (today + timedelta(days=_GAMMA_DAYS[0])).isoformat(),
        "expiration_date_lte": (today + timedelta(days=_GAMMA_DAYS[1])).isoformat(),
    }
    calls = partial(live_session.options.chain, "SPY", contract_type=dm.ContractType.CALL)
    first = table_rows(calls(limit=1, filters=window).fetch(wait=True))
    spot = first[0]["UnderlyingAssetPrice"] if first else None
    if spot is None:
        bars = table_rows(live_session.stocks.previous_day_bar("SPY").fetch(wait=True))
        assert len(bars) == 1, f"SPY has {len(bars)} previous-day bars, not one"
        spot = bars[0]["Close"]
        assert spot is not None, "SPY's previous-day bar has no close"
    near = {"strike_price_gte": spot * 0.98, "strike_price_lte": spot * 1.02}
    chain = table_rows(calls(filters={**window, **near}).fetch(wait=True))
    priced = [r for r in chain if r["GreeksGamma"] and r["ImpliedVolatility"]]
    priced.sort(key=lambda r: abs(r["StrikePrice"] - spot))
    return float(spot), [r["Symbol"] for r in priced[:_GAMMA_CONTRACTS]]


@pytest.mark.settles("103")
@pytest.mark.requests(2)
def test_massive_answers_a_chain_of_a_currency_or_crypto_pair(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What the option chain answers for `C:EURUSD` and `X:BTCUSD` as the underlying.

    The library sends any canonical ticker as the chain's `underlying`. DATA counts the
    contracts on the pair sent.
    """
    assert bad_values.ask_all(live_rest, bad_values.UNDERLYING_CHAIN, evidence)


@pytest.mark.settles("108")
@pytest.mark.requests(1)
def test_massive_answers_a_chain_of_an_option_contract(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What the option chain answers for `O:AAPL250117C00150000`, a contract, as the underlying.

    The library sends any canonical ticker as the chain's `underlying`. ERROR means Massive
    judges it; EMPTY, or DATA, means it answers with no error, and the library should refuse
    it. DATA counts the contracts on the contract sent.
    """
    assert bad_values.ask_all(live_rest, bad_values.UNDERLYING_CHAIN_CONTRACT, evidence)


@pytest.mark.settles("103")
@pytest.mark.requests(2)
def test_massive_answers_contracts_of_a_currency_or_crypto_pair(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What the contracts list answers to `underlying_ticker` `C:EURUSD` and `X:BTCUSD`.

    The library takes only a stock or index ticker as `underlying`. DATA counts the
    contracts on the pair sent.
    """
    assert bad_values.ask_all(live_rest, bad_values.UNDERLYING_CONTRACTS, evidence)


@pytest.mark.settles("109")
@pytest.mark.requests(1)
def test_massive_answers_contracts_of_an_option_contract(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What the contracts list answers to `underlying_ticker` `O:AAPL250117C00150000`.

    The library sends any canonical ticker but a pair as `underlying`. ERROR means Massive
    judges it; EMPTY, or DATA, means it answers with no error, and the library should refuse
    it. DATA counts the contracts on the contract sent.
    """
    assert bad_values.ask_all(live_rest, bad_values.UNDERLYING_CONTRACTS_CONTRACT, evidence)


@pytest.mark.settles("104")
@pytest.mark.requests(1)
def test_massive_answers_contract_details_of_a_stock_ticker(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What the contract overview answers for `AAPL`, a stock rather than a contract.

    `m.options.contract_details` refuses an underlying passed as `symbols` and names
    `m.options.contracts`. DATA counts the rows describing a contract named `AAPL`.
    """
    assert bad_values.ask_all(live_rest, bad_values.CONTRACT_SYMBOL, evidence)


_CONTRACT = "/v3/reference/options/contracts/{options_ticker}"
_SAMPLE_ROOTS = ("SPY", "SPXW", "AMCR1")
"""A standard, a weekly, and an adjusted root: Massive's own examples of roots that differ from
their underlying."""


def _page(client: RestClient, request: option_requests.Request) -> list[dict[str, Any]]:
    """Read the raw rows of the first page of an `option_requests` request.

    Returns:
        The rows.

    Raises:
        WireEntitlementError: When Massive refuses the key access to the path; the live-test
            hook skips, quoting it.
    """
    path, query = request
    return client.get(source(Transport.REST, path), {}, query, bucket="options").rows


_STANDING_ROOTS = ("SPY", "SPXW")
"""The sample roots whose contracts always have open interest; an adjusted root's may have
none on the day read."""


def _root_samples(client: RestClient, evidence: Evidence) -> dict[str, list[dict[str, Any]]]:
    """Read one page of the latest open interest of each sample root, each root on its own.

    A shared page could hold one root's rows alone, so each root gets its own request. Records
    each root's row count; an adjusted root with no rows is recorded, not failed.

    Returns:
        Each sample root's rows.

    Raises:
        WireEntitlementError: When Massive refuses the key access to the open interest; the
            live-test hook skips, quoting it.
    """
    samples = {
        root: _page(client, option_requests.open_interest(root=root, limit=100))
        for root in _SAMPLE_ROOTS
    }
    evidence(f"open interest rows per root: { {k: len(v) for k, v in samples.items()} }")
    return samples


@pytest.mark.verifies("140")
@pytest.mark.requests(5)
def test_root_is_the_ticker_s_occ_root(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Massive's `root` of the open interest and unusual trades is each ticker's OCC root.

    Reads one page of open interest of each of the roots `SPY`, `SPXW`, and `AMCR1`, one page of
    the latest unusual trades of any root, and the library's open interest of `SPXW`, and
    records the roots and counts. Fails when `SPY`, `SPXW`, or the library's read has no row,
    since the check would then rest on nothing, and on any row whose `root` is not its ticker's
    root: the two datasets then can't give Massive's `root` the name `OptionRoot`. A refusal
    skips the test, quoting Massive.
    """
    samples = _root_samples(live_rest, evidence)
    trades = _page(live_rest, option_requests.unusual_trades(limit=300))
    rows = [row for found in samples.values() for row in found] + trades
    differ = [(r.get("ticker"), r.get("root")) for r in rows if r.get("root") != _root(r["ticker"])]
    roots = sorted({str(r.get("root")) for r in trades})
    evidence(f"{len(trades)} unusual trades of roots {roots[:30]}")
    evidence(f"rows whose root is unlike the ticker's: {differ[:10]}")
    library = table_rows(live_session.options.open_interest(root="SPXW", limit=5).fetch(wait=True))
    evidence(f"library SPXW: {[(r['Symbol'], r['OptionRoot']) for r in library]}")
    assert all(samples[root] for root in _STANDING_ROOTS)
    assert library
    assert not differ
    assert all(r["OptionRoot"] == _root(r["Symbol"]) for r in library)


_UNDERLYING_ROOTS = ("SPX", "SPXW", "NDX", "VIX", "RUT", "DJT", "AMCR1", "AAPL")
"""Roots whose underlying the two new datasets must name as the contracts reference does: index
roots and their weeklies, the stock `DJT`, an adjusted root, and a plain stock. `I:DJT` is not
on Massive's index list, so `DJT`'s rows are judged by the index-list pre-filter alone. An option
on a stock or fund whose ticker is also on the list is looked for and compared by
`test_underlying_of_a_ticker_on_the_index_list_matches_the_contracts_reference`."""

_PER_ROOT = 2
"""Contracts compared per root and dataset."""


def _contracts_underlying(
    session: dm.MassiveSession, ticker: str, *, day: str, table_rows: TableRows
) -> str | None:
    """Read one contract's canonical `Underlying` from `m.options.contract_details()` as of a day.

    Returns:
        The contract's `Underlying`, or None when the reference has no row for it.
    """
    found = table_rows(session.options.contract_details(ticker, as_of=day).fetch(wait=True))
    return str(found[0]["Underlying"]) if found else None


def _row_day(row: Mapping[str, Any]) -> str:
    """Return the trading day of a raw open interest or unusual trades row, as ISO text.

    An open interest row has its `date`; an unusual trade has only its SIP timestamp, read in
    New York time.

    Returns:
        The day.
    """
    return (
        str(row.get("date") or "")
        or datetime.fromtimestamp(int(row["sip_timestamp"]) / 1e9, _NEW_YORK).date().isoformat()
    )


RowSource: TypeAlias = Literal["open interest", "unusual trades"]
"""The dataset a raw row came from."""

Sample: TypeAlias = "tuple[RowSource, dict[str, Any]]"
"""A raw row to compare, with the dataset it came from."""


def _published_underlyings(
    session: dm.MassiveSession,
    source_name: RowSource,
    ticker: str,
    *,
    day: str,
    table_rows: TableRows,
) -> list[str]:
    """Read the library's `Underlying` of one contract on one day from the dataset named.

    Returns:
        The `Underlying` of each row, from `m.options.open_interest()` for "open interest" and
        `m.options.unusual_trades()` for "unusual trades".
    """
    match source_name:
        case "open interest":
            library = session.options.open_interest(ticker, start=day, end=day)
        case "unusual trades":
            library = session.options.unusual_trades(ticker, start=day, end=day)
        case _:
            assert_never(source_name)
    return [str(r["Underlying"]) for r in table_rows(library.fetch(wait=True))]


@pytest.mark.verifies("141")
@pytest.mark.requests(95)
@pytest.mark.timeout(600)
def test_underlying_matches_the_contracts_reference(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Both new datasets name each contract's underlying as `m.options.contract_details()` does.

    For the roots `SPX`, `SPXW`, `NDX`, `VIX`, `RUT`, `DJT`, `AMCR1`, and `AAPL`, reads up to
    two contracts each from one page of open interest per root and from one page of the latest
    unusual trades, and two expired `SPXW` contracts from a year earlier. Then it reads each
    contract's `Underlying` from `m.options.contract_details()` as of the row's day. It checks two
    things for every contract read:

    - Massive's own `underlying_ticker` equals the reference's `Underlying` apart from an
      index's `I:` prefix, so Massive folds weekly and adjusted roots as the reference does;
    - the library's `Underlying` on `m.options.open_interest()` and `m.options.unusual_trades()`
      of those contracts equals the reference's exactly, `I:SPX` included.

    Records every comparison and the roots no row was found for. Fails on any mismatch, or
    when `SPX` and `AAPL` give no open-interest row, since an index and a stock must both be
    compared. A refusal skips the test, quoting Massive.
    """
    rows: list[Sample] = []
    for root in _UNDERLYING_ROOTS:
        found = _page(live_rest, option_requests.open_interest(root=root, limit=50))
        rows += [("open interest", r) for r in found[:_PER_ROOT]]
    # Expired contracts, a year back, which the reference must find as of their day.
    year_ago = datetime.now(_NEW_YORK).date() - timedelta(days=365)
    expired = _page(
        live_rest, option_requests.open_interest(root="SPXW", through=year_ago, limit=2)
    )
    rows += [("open interest", r) for r in expired]
    trades = _page(live_rest, option_requests.unusual_trades(limit=1000))
    for root in _UNDERLYING_ROOTS:
        rows += [("unusual trades", r) for r in trades if r.get("root") == root][:_PER_ROOT]
    seen = {str(r.get("root")) for _, r in rows}
    evidence(f"roots with rows: {sorted(seen)}; without: {sorted(set(_UNDERLYING_ROOTS) - seen)}")
    folded: list[str] = []
    wrong: list[str] = []
    for source_name, row in rows:
        ticker = str(row["ticker"])
        day = _row_day(row)
        reference = _contracts_underlying(live_session, ticker, day=day, table_rows=table_rows)
        sent = row.get("underlying_ticker")
        published = _published_underlyings(
            live_session, source_name, ticker, day=day, table_rows=table_rows
        )
        evidence(
            f"{source_name} {ticker} {day}: Massive {sent!r}, contracts {reference!r}, "
            f"library {sorted(set(published))}"
        )
        if reference is None:
            continue
        if sent != reference.removeprefix("I:"):
            folded.append(f"{ticker}: {sent!r} vs {reference!r}")
        if any(value != reference for value in published):
            wrong.append(f"{source_name} {ticker}: {published[:3]} vs {reference!r}")
    assert {"SPX", "AAPL"} <= {str(r.get("root")) for s, r in rows if s == "open interest"}
    assert not folded, folded
    assert not wrong, wrong


@pytest.mark.verifies("142")
@pytest.mark.requests(1)
def test_unusual_trade_prices_are_per_share(live_rest: RestClient, *, evidence: Evidence) -> None:
    """An unusual trade's `price` is per share of the underlying, not per contract.

    Reads one page of unusual trades and checks Massive's own rule for `block_premium`, a
    notional premium of price x size x 100 of $100,000 or more, on the trades of standard roots
    that carry a price and a size: every flagged trade meets it and every other trade falls
    short. Fails when either set breaks the rule, which would mean
    the price is not per share and the column's corrected description is wrong. A refusal
    skips the test, quoting Massive.
    """
    rows = _page(live_rest, option_requests.unusual_trades(limit=1000))
    # A trade without a price or size can't be checked; an adjusted root (`AAPL1`) may have
    # another multiplier than the 100 shares Massive's rule assumes, so only standard roots
    # are judged.
    judged = [
        r
        for r in rows
        if r.get("price") is not None
        and r.get("size") is not None
        and not any(c.isdigit() for c in _root(str(r["ticker"])))
    ]
    premium = {id(r): float(r["price"]) * float(r["size"]) * 100 for r in judged}
    flagged = [r for r in judged if "block_premium" in (r.get("flags") or [])]
    others = [r for r in judged if "block_premium" not in (r.get("flags") or [])]
    low = [(r["ticker"], r["price"], r["size"]) for r in flagged if premium[id(r)] < 100_000]
    high = [(r["ticker"], r["price"], r["size"]) for r in others if premium[id(r)] >= 100_000]
    evidence(
        f"{len(rows)} trades, {len(judged)} judged: {len(flagged)} block_premium, "
        f"{len(others)} other; flagged under $100,000 {low[:5]}; unflagged at $100,000 or "
        f"more {high[:5]}"
    )
    assert flagged
    assert not low
    assert not high


@pytest.mark.requests(1)
def test_admin_message_history_reads_a_day_of_every_exchange(
    live_session: dm.MassiveSession,
    *,
    previous_trading_day: date,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """`history()` of the admin messages reads a past day's archive with the stream's columns.

    Reads the last trading day before the run and records how many messages each exchange sent,
    with one message's text. Fails when the call fails or its columns are not the stream's; a
    day with no message passes, since OPRA sends admin messages only with FLEX activity or an
    exchange's notice. A refusal skips the test, quoting Massive.
    """
    query = live_session.options.admin_messages()
    table = query.history(previous_trading_day, previous_trading_day, wait=True)
    found = table_rows(table)
    exchanges = sorted({int(r["Exchange"]) for r in found})
    first = repr(found[0]["Message"]) if found else "none"
    evidence(
        f"{previous_trading_day}: {len(found)} messages from exchanges {exchanges}; first {first}"
    )
    assert list(table.column_names) == [str(r["Name"]) for r in table_rows(query.columns())]
    assert all(r["Date"] == previous_trading_day.isoformat() for r in found)


def _tickers(client: RestClient, request: option_requests.Request) -> tuple[set[str], int]:
    """Read every page of a ticker list.

    Returns:
        The tickers, each without an index's `I:`, and the pages read.

    Raises:
        WireEntitlementError: When Massive refuses the key the list; the live-test hook skips,
            quoting it.
    """
    path, query = request
    tickers: set[str] = set()
    pages = 0
    for found in client.pages(source(Transport.REST, path), {}, query, bucket=OTHER):
        pages += 1
        tickers |= {str(r["ticker"]).removeprefix("I:") for r in found.rows if r.get("ticker")}
    return tickers, pages


def _index_list(client: RestClient, evidence: Evidence) -> set[str]:
    """Read Massive's whole index list, active and retired, as the library's pre-filter does.

    Records its size and pages.

    Returns:
        Each listed index's bare ticker, such as `SPX`.

    Raises:
        WireEntitlementError: When Massive refuses the key the list.
    """
    active, active_pages = _tickers(client, option_requests.index_list(active=True))
    retired, retired_pages = _tickers(client, option_requests.index_list(active=False))
    listed = active | retired
    evidence(f"index list: {len(listed)} indices in {active_pages + retired_pages} pages")
    return listed


_INDEX_UNDERLYINGS = ("I:SPX", "I:NDX", "I:VIX", "I:RUT", "I:XSP", "I:DJX", "I:OEX")
"""Indices with listed options, whose contracts' underlyings must be on Massive's index list."""


@pytest.mark.verifies("143")
@pytest.mark.requests(60)
@pytest.mark.timeout(600)
def test_index_option_underlyings_are_on_the_index_list(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Every sampled option on an index names an index on Massive's index list.

    Reads Massive's whole index list, active and retired, then up to 20 contracts each of
    `I:SPX`, `I:NDX`, `I:VIX`, `I:RUT`, `I:XSP`, `I:DJX`, and `I:OEX` from
    `m.options.contracts()`. For every contract whose CFI code marks an option on an index:

    - its bare underlying must be on the list;
    - the bare underlying one whole-market page of open interest gives its root, the value the
      open interest's and unusual trades' pre-filter reads, must be on the list too.

    An underlying Massive lists no contract for is recorded, not failed. Also records the list's
    pages and how many distinct roots of that page name a listed index: the Contract Overview
    requests a first whole-market read sends. The `requests` marker assumes the list fits in
    50 pages of 1,000. A refusal skips the test, quoting Massive.
    """
    listed = _index_list(live_rest, evidence)
    market = _page(live_rest, option_requests.open_interest(limit=50000))
    roots = {str(r.get("root")): str(r.get("underlying_ticker")) for r in market}
    off_list: list[str] = []
    compared = 0
    for underlying in _INDEX_UNDERLYINGS:
        rows = table_rows(
            live_session.options.contracts(underlying=underlying, limit=20).fetch(wait=True)
        )
        on_index = [r for r in rows if is_index_option(str(r.get("Cfi") or ""))]
        compared += len(on_index)
        named = sorted({str(r["Underlying"]) for r in on_index})
        evidence(f"{underlying}: {len(rows)} contracts, {len(on_index)} on an index: {named}")
        off_list += [n for n in named if n.removeprefix("I:") not in listed]
        for root in sorted({str(r["OptionRoot"]) for r in on_index} & roots.keys()):
            evidence(f"open interest of root {root} names {roots[root]!r}")
            if roots[root] not in listed:
                off_list.append(f"open interest root {root}: {roots[root]!r}")
    index_named = sorted(root for root, under in roots.items() if under in listed)
    evidence(
        f"one whole-market page of open interest: {len(market)} rows, {len(roots)} roots, "
        f"{len(index_named)} naming a listed index {index_named[:40]}"
    )
    assert compared
    assert not off_list, off_list


_CANDIDATES = 25
"""Tickers on both the index list and the active stock list whose contracts are read in one run.
The overlap is too long to read whole, so each day reads another window of it."""

_PER_SEARCH = 8
"""The most collision roots compared from each search: the whole-market pages, and the stock
list."""


def _market_collisions(
    client: RestClient, listed: Collection[str], evidence: Evidence
) -> dict[str, Sample]:
    """Find the collision roots of one whole-market page of open interest and of unusual trades.

    A collision root is one whose bare underlying is on the index list. Records each page's
    size and the collision roots with the underlying Massive wrote.

    Returns:
        One row of each collision root, by root.

    Raises:
        WireEntitlementError: When Massive refuses the key either dataset.
    """
    found: dict[str, Sample] = {}
    reads: tuple[tuple[RowSource, option_requests.Request], ...] = (
        ("open interest", option_requests.open_interest(limit=50000)),
        ("unusual trades", option_requests.unusual_trades(limit=1000)),
    )
    for source_name, request in reads:
        rows = _page(client, request)
        roots = {_root(str(r["ticker"])) for r in rows}
        for row in rows:
            if str(row.get("underlying_ticker")) in listed:
                found.setdefault(_root(str(row["ticker"])), (source_name, row))
        evidence(f"one whole-market page of {source_name}: {len(rows)} rows, {len(roots)} roots")
    named = {root: row.get("underlying_ticker") for root, (_, row) in sorted(found.items())}
    evidence(f"roots of those pages whose bare underlying is on the index list: {named}")
    return found


def _stock_side_roots(
    client: RestClient, listed: Collection[str], evidence: Evidence
) -> dict[str, str]:
    """Search for options on a stock or fund whose ticker is also on the index list.

    Reads Massive's active stock list, takes the tickers also on the index list, and reads one
    page of today's contracts of `_CANDIDATES` of them, a window that moves each day. The
    contracts list takes an index bare, so one ticker's page may hold an index's options and a
    stock's: each root is sorted by its contracts' CFI codes. Records the overlap, the window,
    and every root found with its kinds.

    Returns:
        A contract of each root whose CFI code marks an option on something other than an
        index, by root.

    Raises:
        WireEntitlementError: When Massive refuses the key the stock list or the contracts.
    """
    stocks, pages = _tickers(client, option_requests.stock_list())
    shared = sorted(set(stocks) & set(listed))
    evidence(
        f"active stock list: {len(stocks)} tickers in {pages} pages; {len(shared)} also on the "
        f"index list: {shared[:100]}"
    )
    if not shared:
        return {}
    count = min(_CANDIDATES, len(shared))
    start = datetime.now(_NEW_YORK).date().toordinal() * _CANDIDATES % len(shared)
    window = (shared + shared)[start : start + count]
    evidence(f"contracts read for {count} of them, from position {start}: {window}")
    other: dict[str, str] = {}
    for ticker in window:
        path, query = option_requests.contracts(underlying=ticker, limit=_CONTRACT_PAGE)
        rows = _raw(client, path, params=query, evidence=evidence)
        kinds: dict[str, set[str]] = {}
        for row in rows:
            cfi = row.get("cfi")
            # A contract without a CFI code says nothing about its underlying's kind.
            kind = "no CFI" if not cfi else "index" if is_index_option(str(cfi)) else "other"
            root = _root(str(row["ticker"]))
            kinds.setdefault(root, set()).add(kind)
            if kind == "other":
                other.setdefault(root, str(row["ticker"]))
        shown = {root: sorted(found) for root, found in sorted(kinds.items())}
        evidence(f"    {ticker}: {len(rows)} contracts; roots by CFI kind {shown}")
    return other


def _compare_collision(
    session: dm.MassiveSession,
    sample: Sample,
    *,
    table_rows: TableRows,
    evidence: Evidence,
) -> tuple[str, list[str]]:
    """Compare one collision row's underlying: Massive's, the reference's, and the library's.

    Reads the contract's `Underlying` and `Cfi` from `m.options.contract_details()` as of the
    row's day, and the library's `Underlying` of the same contract and day from the row's
    dataset, and records all of them.

    Returns:
        The side the reference puts the contract on ("index", "other", "no CFI", or "not
        compared" when the reference or the library has no row), and each mismatch found.
    """
    source_name, row = sample
    ticker = str(row["ticker"])
    day = _row_day(row)
    found = table_rows(session.options.contract_details(ticker, as_of=day).fetch(wait=True))
    sent = row.get("underlying_ticker")
    published = _published_underlyings(session, source_name, ticker, day=day, table_rows=table_rows)
    reference = str(found[0]["Underlying"]) if found else None
    cfi = found[0]["Cfi"] if found else None
    evidence(
        f"{source_name} {ticker} {day}: Massive {sent!r}, contracts {reference!r} (CFI {cfi!r}), "
        f"library {sorted(set(published))}"
    )
    if reference is None or not published:
        return "not compared", []
    # A contract the reference gives no CFI code keeps Massive's bare ticker in the library.
    side = "no CFI" if not cfi else "index" if is_index_option(str(cfi)) else "other"
    problems = [
        f"{source_name} {ticker}: library {value!r} vs {reference!r}"
        for value in sorted(set(published))
        if value != reference
    ]
    if sent != reference.removeprefix("I:"):
        problems.append(f"{ticker}: Massive {sent!r} vs {reference!r}")
    return side, problems


@pytest.mark.verifies("141")
@pytest.mark.requests(150)
@pytest.mark.timeout(900)
def test_underlying_of_a_ticker_on_the_index_list_matches_the_contracts_reference(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Where a root's bare underlying is on the index list, both datasets name it as the reference.

    Such a root is the one case the library's index-list pre-filter can't settle: its rows get
    their `Underlying` from the Contract Overview of the root's first contract. It may be an
    option on that index (expected `I:X`) or on a stock or fund that shares the index's ticker
    (expected bare `X`). The test finds both from Massive's data rather than naming them:

    - it reads Massive's whole index list, active and retired, as the library does;
    - it takes the collision roots of one whole-market page each of open interest and unusual
      trades: roots whose bare underlying is on the list;
    - it reads the active stock list and one page of today's contracts of 25 of the tickers
      on both lists (another window each day), and takes the roots whose CFI code marks an
      option on something other than an index, each with one page of its open interest.

    For up to eight roots of each search it compares one row: Massive's `underlying_ticker`
    must equal `m.options.contract_details()`'s `Underlying` apart from `I:`, and the library's
    `Underlying` of the same contract and day must equal it exactly. Records every collision
    found, every comparison and the side its CFI code puts it on, and says plainly when no
    option on a stock or fund sharing an index's ticker was found, so the stock side went
    unchecked. Fails on any mismatch, or when no option on an index was compared.

    Up to about 150 requests: the index list twice (about 16 pages each, once by the test and once
    by the library's pre-filter), the stock list (the marker assumes up to 35 pages), two
    whole-market pages, 25 contracts pages, up to eight root pages of open interest, and up to
    three per compared root (the reference, the library's read, and its root lookup). A
    refusal skips the test, quoting Massive.
    """
    listed = _index_list(live_rest, evidence)
    market = _market_collisions(live_rest, listed, evidence)
    stock_roots = _stock_side_roots(live_rest, listed, evidence)
    samples = [market[root] for root in sorted(market)[:_PER_SEARCH]]
    for root in sorted(stock_roots.keys() - market.keys())[:_PER_SEARCH]:
        rows = _page(live_rest, option_requests.open_interest(root=root, limit=5))
        evidence(f"open interest of root {root} ({stock_roots[root]}): {len(rows)} rows")
        samples += [("open interest", row) for row in rows[:1]]
    sides: dict[str, list[str]] = {}
    problems: list[str] = []
    for sample in samples:
        side, found = _compare_collision(
            live_session, sample, table_rows=table_rows, evidence=evidence
        )
        sides.setdefault(side, []).append(str(sample[1]["ticker"]))
        problems += found
    evidence(f"contracts compared by the side their CFI code puts them on: {sides}")
    if not sides.get("other"):
        evidence(
            "STOCK SIDE NOT CHECKED: no option on a stock or fund whose ticker is on the index "
            "list was found and compared in the pages read, so this run checked options on an "
            "index only"
        )
    assert sides.get("index"), "no option on an index was compared"
    assert not problems, problems
