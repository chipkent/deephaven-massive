"""Live checks of options data against Massive: underlyings, deliverables, and bar stamps.

Each test records what Massive sent (`evidence`). A test marked `settles` asserts only that the
requests worked, and its evidence, not a pass, answers the question; a test marked `verifies` also
asserts the answer the library assumes, and fails when Massive no longer gives it. Raw rows come
from the wire REST client, because "a key is missing" and "a key holds an empty list" look the same
once the library has read them; the library's own answer is recorded beside them. How to run these
tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, date, datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pytest
from deephaven import dtypes, new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._massive import (
    RestClient,
    Transport,
    WireEntitlementError,
    WireError,
    source,
)
from tests._live import calendar

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Mapping
    from typing import TypeAlias

    from deephaven.table import Table
    from deephaven.table_factory import InputTable

    from deephaven_massive._query import Query

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

_CONTRACTS = "/v3/reference/options/contracts"
_NEW_YORK = ZoneInfo("America/New_York")
_CHAIN = "/v3/snapshot/options/{underlyingAsset}"
_SNAPSHOT = "/v3/snapshot"

_UNDERLYINGS = ("SPX", "I:SPX", "SPXW", "NDX", "NDXP", "VIX", "VIXW", "BRK.B", "BRKB")
"""Underlyings whose spelling the options endpoints may not agree on."""

_BASIC_UNDERLYINGS = ("SPX", "I:SPX", "BRK.B", "BRKB")
"""The underlyings read with Options on Basic, one contracts request each: an index with and
without its prefix, and `BRK.B` with and without its dot."""

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
)
"""(contract root, underlying, a day the contracts were listed): three adjusted roots, after a
spin-off, a special dividend, and a split. Adjusted roots are rare and named by hand, so the
older ones lie beyond a short history; the test reads only those the plan keeps."""


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
        return client.get(source(Transport.REST, path), path_params, query, bucket="options").rows
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


@pytest.mark.needs("options:basic")
@pytest.mark.settles("45")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_underlying_spelling_on_the_options_endpoints(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    classes_on_basic: frozenset[str],
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """How each options endpoint spells and accepts an underlying, for indices and `BRK.B`.

    For `SPX`, `I:SPX`, `SPXW`, `NDX`, `NDXP`, `VIX`, `VIXW`, `BRK.B`, and `BRKB`, records one
    full page of the contracts reference (the roots it lists and the `underlying_ticker` values),
    one page of the chain snapshot (`underlying_asset.ticker`), the unified snapshot's
    `underlying_asset.ticker` of one contract, and the library's `chain()` and `contracts()`.
    With Options on Basic, which has no snapshots and allows 5 requests a minute, it records
    only the contracts page of `SPX`, `I:SPX`, `BRK.B`, and `BRKB`, which still shows which
    spelling of an index and of `BRK.B` the reference answers. Reading it: the spelling that
    answers is the one the library must send for `underlying=`; a root such as `SPXW` listed
    under `SPX` means roots and underlyings differ. The chain path sends `I:SPX`
    percent-encoded, so an `I:SPX` chain result counts only if the encoded-colon check of the
    indices tests passes.
    """
    answered = 0
    options = live_session.options
    basic = "options" in classes_on_basic
    if basic:
        evidence(
            "Options on Basic: the contracts reference of "
            f"{', '.join(_BASIC_UNDERLYINGS)} only; the snapshots need {_SNAPSHOT_PLAN}"
        )
    for underlying in _BASIC_UNDERLYINGS if basic else _UNDERLYINGS:
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
        if basic:
            answered += bool(contracts)
            continue
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


@pytest.mark.needs("options:starter")
@pytest.mark.verifies("46")
@pytest.mark.timeout(400)
def test_deliverable_fields_of_adjusted_and_standard_contracts(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    plan_history_start: Callable[[str], date | None],
    recent_monday: date,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """How the contracts reference describes adjusted and standard deliverables.

    Lists the contracts of the adjusted roots `DELL1` (as of 2021-11-15, after a spin-off),
    `FF1` (2024-04-15, a special dividend), and `AAPL7` (2014-06-10, the 7:1 split), and of
    standard `AAPL` and `SPX` contracts (the most recent published Monday), looked up by
    underlying (following pages until the root is passed) and by root, and records
    `additional_underlyings` (or `<missing>`), `cfi`, `correction`, and `shares_per_contract`
    of up to three of each, and the library's columns for the first. A root listed before the
    first day the key's Options plan keeps, by the documented history windows, is not read; its
    line says "outside the plan's history". Passes when some root was read and no standard
    contract carries a non-empty `additional_underlyings`, so null and `[]` both mark a standard
    deliverable, as the chain's documented recipe assumes. Reading the rest: an adjusted
    contract with no list (a split's, which changes only the share count) means only `Cfi` or
    the root's trailing digit can tell it from a standard one. A root found by neither lookup,
    with the page counts showing the listing was read past it, is not kept by Massive for that
    day.
    """
    first = plan_history_start("options")
    found = 0
    standard = (("AAPL", "AAPL", recent_monday), ("SPX", "SPX", recent_monday))
    roots = (*_ADJUSTED, *standard)
    listed_extra: list[str] = []
    for root, underlying, listed in roots:
        evidence(f"{root} (underlying {underlying}, as of {listed}):")
        if first is not None and listed < first:
            evidence(f"    outside the plan's history: {listed} is before {first}")
            continue
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
        if (root, underlying, listed) in standard:
            listed_extra += [str(r["ticker"]) for r in rows if r.get("additional_underlyings")]
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
        + library_stamps(partial(daily.history, *stamp_days["day"], source=rest))
    )
    weekly = session.options.bars(ticker, dm.Timespan.WEEK)
    evidence(
        f"library {ticker} week history: "
        + library_stamps(partial(weekly.history, *stamp_days["week"], source=rest))
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
    evidence(f"library {ticker} previous_day_bar fetch() (/prev): {library_stamps(previous.fetch)}")
    return found


@pytest.mark.needs("options:starter", "stocks:basic")
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


@pytest.mark.needs("options:starter", "s3", "stocks:basic")
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
            partial(as_traded.history, *days, source=dm.HistorySource.FLAT_FILES)
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


@pytest.mark.needs("options:developer")
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
    `adjusted=true` and `adjusted=false`. It needs Options Developer: Basic and Starter keep two
    years of history, and the split is older. Fails unless the `adjusted=true` reads of the two
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


@pytest.mark.needs("options:starter")
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
    result = query.fetch()
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


@pytest.mark.needs("options:advanced")
@pytest.mark.verifies("82")
@pytest.mark.timeout(150)
def test_admin_messages_stream_every_exchange_on_the_real_time_feed(
    live_session: dm.MassiveSession,
    *,
    wait_acked: WaitAcked,
    table_rows: TableRows,
    stream_report: StreamReport,
    evidence: Evidence,
) -> None:
    """Whether Massive acknowledges `AD.*` on the real-time options feed, and what rows carry.

    With Options Advanced held, streams `m.options.admin_messages()`, which subscribes every
    exchange (`AD.*`) on `socket.massive.com`, and records the connection, the stream, and up
    to five rows: each one's exchange, sequence number, SIP timestamp, and text as OPRA sent it.
    Passes when Massive acknowledged the topic, which it does whether or not the market is open,
    the stream still runs, and every row that arrived has its `Timestamp` and `Message`.
    Messages come only with FLEX activity or an exchange's notice, so no row is required; a
    refusal fails the test, since Options Advanced includes the channel.
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
