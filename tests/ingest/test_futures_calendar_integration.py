"""Live checks of what the futures session calendar reads from Massive: products and schedules.

Streamed futures rows carry no session date, so the library dates them from Massive's futures
schedules, after finding each contract's product. These tests record what that design depends
on and Massive's docs leave open: whether combos have a product and a schedule, and whether
the contracts list holds them; how many products, contracts, and schedule events there are;
what the contract and snapshot lookups cost, with a `date` and nested `fields` and without;
whether a combo's snapshot product places its trades in Massive's own session; whether the
trades path reads a combo whose ticker holds `:` or a space, and spelled how; how often
contracts are listed, and which tickers the snapshot or the contracts list lacks; how far
ahead each product's schedule reaches; whether any trade falls outside every session; and
Massive's whole answers where those checks found data missing, to quote to Massive. Each
test sends REST requests only and records what Massive sent (`evidence`), except the
streamed-tickers test, which streams every futures trade on the key's WebSocket for 10
minutes: it is marked `integration_all_symbols`, so `-m integration` leaves it out and
`-m integration_all_symbols` runs it. Every test is marked `settles`: its evidence, not a
pass, answers its question. A read cut short by its page or request cap says its counts are
lower bounds. A request Massive refuses for want of entitlement skips the test with Massive's
refusal. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

import json
import re
import statistics
import threading
import time
from collections import Counter
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from typing import TYPE_CHECKING, Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm
from deephaven_massive._datasets.epoch import epoch_ns
from deephaven_massive._datasets.futures_sessions import futures_session
from deephaven_massive._datasets.symbols import is_futures_contract
from deephaven_massive._ingest.futures_calendar import TICKERS_PER_REQUEST, contracts_query
from deephaven_massive._massive import (
    RestClient,
    RestPath,
    Transport,
    WireAuthError,
    WireEntitlementError,
    WireError,
    source,
)
from tests._live.ticker_lists import ticker_form

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"

_SNAPSHOT = "/futures/v1/snapshot"
_CONTRACTS = "/futures/v1/contracts"
_PRODUCTS = "/futures/v1/products"
_SCHEDULES = "/futures/v1/schedules"
_TRADES = "/futures/v1/trades/{ticker}"
_MARKET_STATUS = "/futures/v1/market-status"

_CHICAGO = ZoneInfo("America/Chicago")

_LIST_PAGE = 1000
"""The largest page of the futures contracts and schedules lists; a page this full may be cut
short."""

_PRODUCTS_PAGE = 50_000
"""The largest page of the futures products list and snapshot."""

_DAYS_BACK = timedelta(days=7)
_DAYS_AHEAD = timedelta(days=10)
"""The window of sessions the futures calendar loads around today."""


def _page(client: RestClient, path: str, *, query: Mapping[str, object]) -> list[dict[str, Any]]:
    """Read the raw rows of the first page of a futures request.

    The path values are the query's `ticker` for a path with a `{ticker}` placeholder.

    Returns:
        The rows as Massive sent them. A refusal raises the wire error.
    """
    path_params = {"ticker": query["ticker"]} if "{ticker}" in path else {}
    rest = {key: value for key, value in query.items() if not path_params or key != "ticker"}
    return client.get(source(Transport.REST, path), path_params, rest, bucket="futures").rows


def _read_pages(
    client: RestClient, path: str | RestPath, query: Mapping[str, object], *, most: int
) -> tuple[list[dict[str, Any]], int, bool]:
    """Read up to `most` pages of a futures list, following each page's `next_url`.

    The path is a key of the library's spec, or a spec path itself (`_with_fields`).

    Returns:
        The rows, the pages read, and whether the list ended within them (the last page read
        has no `next_url`).
    """
    rows: list[dict[str, Any]] = []
    read = 0
    rest_path = source(Transport.REST, path) if isinstance(path, str) else path
    for page in client.pages(rest_path, {}, query, bucket="futures"):
        rows.extend(page.rows)
        read += 1
        if not (isinstance(page.body, dict) and page.body.get("next_url")):
            return rows, read, True
        if read == most:
            break
    return rows, read, False


def _chicago(nanos: int) -> str:
    """Write epoch nanoseconds as Chicago wall time with its weekday.

    Returns:
        Such as `"Mon 2026-03-02 16:45:00"`.
    """
    return f"{datetime.fromtimestamp(nanos / 1e9, UTC).astimezone(_CHICAGO):%a %Y-%m-%d %H:%M:%S}"


def _instant(nanos: int) -> datetime:
    """Return epoch nanoseconds as an aware `datetime`, to the microsecond below.

    Returns:
        The instant in UTC.
    """
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=nanos // 1000)


def _schedule_spans(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, tuple[int, int]]]:
    """Group schedule events into sessions, as the futures calendar does.

    Returns:
        By product code, then `session_end_date`, the session's first and last event in epoch
        nanoseconds.
    """
    stamps: dict[str, dict[str, list[int]]] = {}
    for row in rows:
        code, day, stamp = (row.get(k) for k in ("product_code", "session_end_date", "timestamp"))
        if isinstance(code, str) and isinstance(day, str) and isinstance(stamp, str):
            at = epoch_ns(datetime.fromisoformat(stamp))
            stamps.setdefault(code, {}).setdefault(day, []).append(at)
    return {
        code: {day: (min(found), max(found)) for day, found in days.items()}
        for code, days in stamps.items()
    }


def _session_holding(spans: Mapping[str, tuple[int, int]], nanos: int) -> str:
    """Name the session whose span, first event to last, holds an instant.

    Returns:
        Its `session_end_date`, or `"none"` with the sessions read.
    """
    held = [day for day, (first, last) in sorted(spans.items()) if first <= nanos <= last]
    return held[0] if held else f"none of {sorted(spans)}"


_COMBO_ROOTS = ("CL", "GC")
"""The ticker prefixes whose combos the combo check reads: crude oil and gold, whose calendar
spreads (`GCQ6-GCJ7`) and butterflies (`CL:BF Q6-U6-V6`) trade every day."""

_COMBOS_PER_ROOT = 2
"""The combos read per prefix: those of the most recent trades, one per ticker form."""

_RECENT_TRADE = timedelta(days=7)
"""How recent a combo's last trade must be for its session to fall in the schedules read."""

_LEG_SEPARATOR = re.compile(r"[-: ]")
"""What ends a combo ticker's first leg: `GCQ6` of `GCQ6-GCJ7`, `CL` of `CL:BF Q6-U6-V6`."""


@dataclass(frozen=True)
class _Combo:
    """A combo read from the snapshot: its ticker, product code, and last trade."""

    ticker: str
    product_code: str
    traded: int
    """The last trade's `last_updated`, in epoch nanoseconds."""


def _snapshot_combos(
    rows: Sequence[Mapping[str, Any]], since: int
) -> tuple[dict[str, str], list[_Combo]]:
    """Read the snapshot's product codes, and the combos traded since an instant.

    A combo is a ticker the futures calendar does not take for a single contract
    (`is_futures_contract`).

    Returns:
        Each ticker's `details.product_code`, and the combos with a `last_trade` at or after
        `since` (epoch nanoseconds), most recent first.
    """
    products: dict[str, str] = {}
    recent: list[_Combo] = []
    for row in rows:
        details = row.get("details") or {}
        ticker, code = details.get("ticker"), details.get("product_code")
        if not isinstance(ticker, str):
            continue
        products[ticker] = str(code)
        traded = (row.get("last_trade") or {}).get("last_updated")
        if not is_futures_contract(ticker) and isinstance(traded, int) and traded >= since:
            recent.append(_Combo(ticker, str(code), traded))
    return products, sorted(recent, key=lambda combo: combo.traded, reverse=True)


def _one_per_form(combos: Sequence[_Combo], count: int) -> list[_Combo]:
    """Take the first combos of distinct ticker forms (`ticker_form`), at most `count`.

    Returns:
        The combos, in their order.
    """
    taken: dict[str, _Combo] = {}
    for combo in combos:
        if len(taken) < count:
            taken.setdefault(ticker_form(combo.ticker), combo)
    return list(taken.values())


def _read_combos(
    client: RestClient, evidence: Evidence, *, per_root: int
) -> tuple[list[_Combo], list[_Combo], dict[str, str]]:
    """Read the snapshot of each of `_COMBO_ROOTS` and take up to `per_root` recent combos of each.

    Returns:
        The combos taken, each prefix's most recent first; each prefix's most recent combo;
        and every ticker's snapshot product code.
    """
    since = int((datetime.now(UTC) - _RECENT_TRADE).timestamp()) * 1_000_000_000
    products_of: dict[str, str] = {}
    combos: list[_Combo] = []
    latest: list[_Combo] = []
    for root in _COMBO_ROOTS:
        end = root[:-1] + chr(ord(root[-1]) + 1)
        query = {"ticker.gte": root, "ticker.lt": end, "limit": _PRODUCTS_PAGE}
        rows = _page(client, _SNAPSHOT, query=query)
        products, recent = _snapshot_combos(rows, since)
        products_of.update(products)
        picked = _one_per_form(recent, per_root)
        full = " (a full page)" if len(rows) == _PRODUCTS_PAGE else ""
        evidence(
            f"snapshot {root} to {end}: {len(rows)} rows{full}, "
            f"{sum(not is_futures_contract(t) for t in products)} combos, "
            f"{len(recent)} traded within 7 days, read "
            f"{[(c.ticker, c.product_code, _chicago(c.traded)) for c in picked]}"
        )
        if rows:
            first = rows[0]
            evidence(
                f"snapshot row keys {sorted(first)}, details {first.get('details')}, "
                f"last_trade {first.get('last_trade')}"
            )
        combos.extend(picked)
        latest.extend(picked[:1])
    return combos, latest, products_of


def _record_products(
    client: RestClient,
    combos: Sequence[_Combo],
    candidates: Mapping[str, Sequence[str]],
    *,
    evidence: Evidence,
) -> None:
    """Record whether the products list holds each combo's candidate products (Q1)."""
    codes = sorted({code for found in candidates.values() for code in found})
    if not codes:
        evidence("Q1: no combo has a candidate product code; the products list is not read")
        return
    rows = _page(client, _PRODUCTS, query={"product_code.any_of": codes, "limit": _PRODUCTS_PAGE})
    full = " (a full page: a product may be left out)" if len(rows) == _PRODUCTS_PAGE else ""
    evidence(f"Q1 products list for {codes}: {len(rows)} rows{full}")
    listed: dict[str, set[tuple[str, str]]] = {}
    for row in rows:
        listed.setdefault(str(row.get("product_code")), set()).add(
            (str(row.get("type")), str(row.get("strategy_type")))
        )
    for combo in combos:
        for code in candidates[combo.ticker]:
            evidence(
                f"Q1 {combo.ticker!r}: candidate {code!r} (the snapshot's product code: "
                f"{code == combo.product_code}), ticker starts with it: "
                f"{combo.ticker.startswith(code)}, products list (type, strategy_type): "
                f"{sorted(listed.get(code, set())) or 'not listed'}"
            )


def _record_schedules(
    client: RestClient,
    combos: Sequence[_Combo],
    candidates: Mapping[str, Sequence[str]],
    *,
    evidence: Evidence,
) -> dict[str, dict[str, tuple[int, int]]]:
    """Record the candidates' sessions, and which of them holds each combo's last trade (Q2).

    Reads the window the futures calendar reads: 7 days back to 10 ahead.

    Returns:
        The sessions read, as `_schedule_spans` gives them.
    """
    codes = sorted({code for found in candidates.values() for code in found})
    if not codes:
        evidence("Q2: no combo has a candidate product code; the schedules are not read")
        return {}
    today = futures_session(datetime.now(UTC))  # the calendar's today
    query = {
        "product_code.any_of": codes,
        "session_end_date.gte": today - _DAYS_BACK,
        "session_end_date.lte": today + _DAYS_AHEAD,
        "limit": _LIST_PAGE,
    }
    schedules = _page(client, _SCHEDULES, query=query)
    spans = _schedule_spans(schedules)
    full = " (a full page)" if len(schedules) == _LIST_PAGE else ""
    evidence(f"Q2 schedules: {len(schedules)} events{full} for {codes}")
    for code in codes:
        evidence(f"Q2 {code!r}: sessions {sorted(spans.get(code, {}))}")
    for combo in combos:
        for code in candidates[combo.ticker]:
            evidence(
                f"Q2 {combo.ticker!r}: last trade {_chicago(combo.traded)} Chicago, in a "
                f"session of {code!r}: {_session_holding(spans.get(code, {}), combo.traded)}"
            )
    return spans


def _record_trades(
    client: RestClient,
    combos: Sequence[_Combo],
    *,
    spans: Mapping[str, Mapping[str, tuple[int, int]]],
    candidates: Mapping[str, Sequence[str]],
    evidence: Evidence,
) -> None:
    """Record the session Massive gives each combo's latest trade, beside the schedules' (Q2).

    The trade read is the latest when the request runs, which may not be the snapshot's, so
    its own instant is placed in the candidates' sessions.
    """
    for combo in combos:
        query = {"ticker": combo.ticker, "sort": "timestamp.desc", "limit": 1}
        trades = _page(client, _TRADES, query=query)
        for trade in trades:
            stamp = trade.get("timestamp")
            if not isinstance(stamp, int):
                evidence(f"Q2 {combo.ticker!r}: latest trade without an integer timestamp {trade}")
                continue
            placed = {
                code: _session_holding(spans.get(code, {}), stamp)
                for code in candidates[combo.ticker]
            }
            evidence(
                f"Q2 {combo.ticker!r}: latest trade at {_chicago(stamp)} Chicago, Massive's "
                f"session_end_date {trade.get('session_end_date')!r}, the schedules' session "
                f"by candidate {placed}"
            )
        if not trades:
            evidence(f"Q2 {combo.ticker!r}: the trades list returned no trade")


def _record_contracts(client: RestClient, tickers: Sequence[str], evidence: Evidence) -> None:
    """Record what the contracts list answers for the combos, asked as the calendar asks (Q3)."""
    query = {"ticker.any_of": list(tickers), "limit": _LIST_PAGE}
    contracts = _page(client, _CONTRACTS, query=query)
    full = " (a full page: a combo may be left out)" if len(contracts) == _LIST_PAGE else ""
    evidence(f"Q3 contracts list for {list(tickers)}: {len(contracts)} rows{full}")
    for ticker in tickers:
        rows = [row for row in contracts if row.get("ticker") == ticker]
        kinds = sorted({(str(r.get("type")), str(r.get("product_code"))) for r in rows})
        days = sorted(str(r.get("date")) for r in rows)
        evidence(
            f"Q3 {ticker!r}: {len(rows)} rows, (type, product_code) {kinds or 'not listed'}"
            + (f", dates {days[0]} to {days[-1]}" if days else "")
        )


@pytest.mark.settles("122")
@pytest.mark.requests(7)
@pytest.mark.timeout(200)
def test_products_schedules_and_contracts_of_futures_combos(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """Whether a futures combo's product and schedule could date its streamed rows.

    The calendar that dates streamed futures rows looks up a single contract's product in the
    contracts list and loads that product's schedule; it never looks up a combo (a spread
    traded as one instrument, `GCQ6-GCJ7`, `CL:BF Q6-U6-V6`), so a combo's streamed rows get
    a null `Date`. This records whether Massive's data could place them.

    Reads the snapshot of every ticker from `CL` up to `CM`, then from `GC` up to `GD`, and
    takes of each the combos (tickers the calendar does not take for a single contract) of
    the most recent trades within 7 days, one per ticker form, two at most. Each combo has up
    to two candidate products: its snapshot row's `details.product_code`, and that of its
    first leg's own snapshot row (`GCQ6` of `GCQ6-GCJ7`). Then reads the products list and
    the schedules of the candidates, the schedules from 7 days back to 10 ahead (the
    calendar's window); the contracts list for the combos, by `ticker.any_of` at the largest
    page and nothing else (the calendar's lookup); and the latest trade of the most recent
    combo of each prefix, whose `session_end_date` is Massive's own date for it. Records:

    - Q1: each combo's candidates, whether its ticker starts with each, and whether the
      products list holds each (with its `type` and `strategy_type`).
    - Q2: the sessions each candidate's schedule holds, the session whose span (first event
      to last) holds each combo's last trade, and each latest trade's own `session_end_date`
      beside the session its instant falls in.
    - Q3: the rows the contracts list returned for each combo (their `type` and
      `product_code`), and whether its page was full, which could leave a combo out.

    Passes when every request is answered and a combo traded within 7 days; the answers are
    evidence, never asserted. Sends 7 requests: two snapshot pages, the products, the
    schedules, the contracts, and two trades pages.
    """
    combos, latest, products_of = _read_combos(live_rest, evidence, per_root=_COMBOS_PER_ROOT)
    assert combos, "no CL or GC combo traded within 7 days: nothing to judge"
    candidates = {
        combo.ticker: sorted(
            {combo.product_code, products_of.get(_LEG_SEPARATOR.split(combo.ticker, 1)[0], "")}
            - {"", "None"}
        )
        for combo in combos
    }
    _record_products(live_rest, combos, candidates, evidence=evidence)
    spans = _record_schedules(live_rest, combos, candidates, evidence=evidence)
    _record_trades(live_rest, latest, spans=spans, candidates=candidates, evidence=evidence)
    _record_contracts(live_rest, sorted(candidates), evidence)


_CONTRACT_PAGES = 15
"""The most pages of active contracts the counting check reads, 1,000 contracts each."""

_SCHEDULE_PAGES = 5
"""The most pages of one day's schedule events, of every product, the counting check reads."""

_MONTH_AND_YEAR = re.compile(r"[FGHJKMNQUVXZ]\d{1,2}$")
"""A single contract ticker's end: a month letter and a one- or two-digit year."""


def _record_roots(singles: Sequence[Mapping[str, Any]], evidence: Evidence) -> None:
    """Record whether each single contract's ticker, less its month and year, is its product."""
    pairs = sorted({(str(r.get("ticker")), str(r.get("product_code"))) for r in singles})
    unlike = [(t, code) for t, code in pairs if _MONTH_AND_YEAR.sub("", t) != code]
    shapeless = [t for t, _ in pairs if not _MONTH_AND_YEAR.search(t)]
    evidence(
        f"Q5 single contracts: {len(pairs)} tickers, {len(pairs) - len(unlike)} whose root "
        f"(the ticker less its month letter and year) is their product code"
    )
    evidence(f"Q5 roots that differ from the product code (ticker, product_code): {unlike[:40]}")
    evidence(f"Q5 single tickers without a month letter and year: {shapeless[:40]}")


def _weekdays(first: date, last: date) -> int:
    """Count the weekdays from one date to another, both included.

    Returns:
        The count.
    """
    return sum((first + timedelta(days=n)).weekday() < 5 for n in range((last - first).days + 1))


@pytest.mark.settles("123")
@pytest.mark.requests(21)
@pytest.mark.timeout(400)
def test_count_of_futures_products_contracts_and_schedule_pages(
    live_rest: RestClient, *, recent_monday: date, evidence: Evidence
) -> None:
    """How many futures products and active contracts there are, and what their schedules cost.

    Sizes loading every product's schedule up front, and records whether a single contract's
    ticker root is its product code. Reads, for the most recent published Monday: the products
    list (one page of up to 50,000); the active contracts, single and combo, up to 15 pages of
    1,000; and that day's schedule events of every product, up to 5 pages of 1,000. Records:

    - Q4: the products by `type`; the active contracts by `type`, and whether the list ended
      within the pages read (if not, the counts are lower bounds); the day's schedule events,
      the products they cover, and the requests that loading every product's schedule over the
      futures calendar's window (7 days back to 10 ahead) would take at 1,000 events a page,
      taking each weekday in it to have as many events as that Monday.
    - Q5: of the single contracts read, how many have a ticker that, less its month letter and
      one- or two-digit year, equals their `product_code`, and every one that differs.

    Passes when every request is answered and lists something; the answers are evidence,
    never asserted. Sends at most 21 requests.
    """
    day = recent_monday
    products = _page(live_rest, _PRODUCTS, query={"date": day, "limit": _PRODUCTS_PAGE})
    kinds = Counter(str(row.get("type")) for row in products)
    codes = {str(row.get("product_code")) for row in products}
    full = " (a full page: more may exist)" if len(products) == _PRODUCTS_PAGE else ""
    evidence(
        f"Q4 products on {day}: {len(products)} rows{full}, {len(codes)} product codes, "
        f"by type {dict(kinds)}"
    )
    query = {"date": day, "active": True, "limit": _LIST_PAGE}
    contracts, pages, ended = _read_pages(live_rest, _CONTRACTS, query, most=_CONTRACT_PAGES)
    tickers = {(str(r.get("ticker")), str(r.get("type"))) for r in contracts}
    evidence(
        f"Q4 active contracts on {day}: {len(tickers)} tickers in {pages} pages, "
        f"{'the whole list' if ended else 'the list goes on (a lower bound)'}, by type "
        f"{dict(Counter(kind for _, kind in tickers))}, "
        f"{len({str(r.get('product_code')) for r in contracts})} product codes"
    )
    query = {"session_end_date": day, "limit": _LIST_PAGE}
    events, pages, ended = _read_pages(live_rest, _SCHEDULES, query, most=_SCHEDULE_PAGES)
    scheduled = {str(row.get("product_code")) for row in events}
    today = futures_session(datetime.now(UTC))  # the calendar's today
    sessions = _weekdays(today - _DAYS_BACK, today + _DAYS_AHEAD)
    evidence(
        f"Q4 schedule events for the session of {day}: {len(events)} in {pages} pages"
        f"{'' if ended else ' (more follow)'}, for {len(scheduled)} product codes, "
        f"{len(scheduled - codes)} of them not in the products list, "
        f"{len(codes - scheduled)} listed products with none"
    )
    evidence(
        f"Q4 every product's schedule over {sessions} weekday sessions: about "
        f"{-(-len(events) * sessions // _LIST_PAGE)} requests of {_LIST_PAGE} events"
        f"{'' if ended else ' at least'}"
    )
    _record_roots([r for r in contracts if r.get("type") == "single"], evidence)
    assert products, f"Massive listed no futures product on {day}"
    assert contracts, f"Massive listed no active futures contract on {day}"
    assert events, f"Massive's schedules hold no event for the session of {day}"


_WINDOW_PAGES = 30
"""The most pages of every product's schedule over the calendar's window the size check reads."""


@pytest.mark.settles("124")
@pytest.mark.requests(31)
@pytest.mark.timeout(600)
def test_schedule_window_size_and_horizon(live_rest: RestClient, *, evidence: Evidence) -> None:
    """How large every product's schedule over the calendar's window is, and how far ahead it goes.

    Sizes loading every schedule up front. Reads the schedule events of every product for the
    sessions from 7 days back to 10 ahead (the calendar's window), up to 30 pages of 1,000,
    and then the one event with the latest `session_end_date` Massive holds. Records:

    - the events, pages, and JSON bytes of the window, and whether it ended within the pages
      read (if not, the counts are lower bounds);
    - the products and sessions it covers, the events per session, and the event types;
    - the latest session in the window, and the products whose schedule stops before its end;
    - the latest session Massive holds for any product: how far ahead schedules go.

    Passes when every request is answered and the window holds an event; the answers are
    evidence. Sends at most 31 requests.

    Raises:
        WireEntitlementError: When Massive refuses a request for want of entitlement, which
            skips the test with Massive's refusal.
        WireError: When the horizon request fails other than by Massive refusing its sort
            (HTTP 400), which is recorded instead.
    """
    today = futures_session(datetime.now(UTC))  # the calendar's today
    last_day = today + _DAYS_AHEAD
    query = {
        "session_end_date.gte": today - _DAYS_BACK,
        "session_end_date.lte": last_day,
        "limit": _LIST_PAGE,
    }
    events, pages, ended = _read_pages(live_rest, _SCHEDULES, query, most=_WINDOW_PAGES)
    spans = _schedule_spans(events)
    per_session = Counter(
        (str(r.get("product_code")), str(r.get("session_end_date"))) for r in events
    )
    evidence(
        f"window {today - _DAYS_BACK} to {last_day}: {len(events)} events in {pages} pages "
        f"({'the whole window' if ended else 'more follow: lower bounds'}), "
        f"{len(json.dumps(events))} bytes of JSON rows, {len(spans)} products, "
        f"{len(per_session)} sessions"
    )
    evidence(
        f"events per session (count: sessions): {sorted(Counter(per_session.values()).items())}"
    )
    evidence(f"event types: {dict(Counter(str(r.get('event')) for r in events))}")
    latest = {code: max(days) for code, days in spans.items()}
    short = sorted(code for code, day in latest.items() if day < last_day.isoformat())
    evidence(
        f"latest session in the window: {max(latest.values(), default=None)}; "
        f"{len(short)} products end before {last_day}, e.g. "
        f"{[(code, latest[code]) for code in short[:15]]}"
    )
    try:  # Massive documents no `session_end_date` sort column; its refusal is evidence
        furthest = _page(live_rest, _SCHEDULES, query={"sort": "session_end_date.desc", "limit": 1})
    except WireEntitlementError:
        raise
    except WireError as error:
        if error.status != 400:  # not Massive refusing the sort: a failure of the request
            raise
        evidence(f"latest session Massive holds: the sort was refused: {error}")
        furthest = []
    evidence(
        "latest session Massive holds: "
        + (
            f"{furthest[0].get('session_end_date')} ({furthest[0].get('product_code')})"
            if furthest
            else "none"
        )
    )
    assert events, "Massive's schedules hold no event in the calendar's window"


_LATENCY_RUNS = 5
"""How many times each lookup is timed."""

_LATENCY_PAGES = 4
"""The most pages of one lookup the latency check reads, each a request."""

_SERVER_ERROR = 500
"""The lowest HTTP status of a server's failure, which the latency check records as evidence."""


def _timed(
    client: RestClient, path: str, query: Mapping[str, object]
) -> tuple[list[float], int, int, bool]:
    """Time one whole lookup, every page as the calendar reads it, `_LATENCY_RUNS` times.

    Returns:
        The seconds of each run, the last run's rows, the most pages a run read, and whether
        every run read the whole lookup within `_LATENCY_PAGES` pages.
    """
    seconds: list[float] = []
    rows: list[dict[str, Any]] = []
    most, every = 0, True
    for _ in range(_LATENCY_RUNS):
        start = time.perf_counter()
        rows, pages, ended = _read_pages(client, path, query, most=_LATENCY_PAGES)
        seconds.append(time.perf_counter() - start)
        most, every = max(most, pages), every and ended
    return seconds, len(rows), most, every


@pytest.mark.settles("125")
@pytest.mark.requests(102)
@pytest.mark.timeout(900)
def test_contract_and_schedule_lookup_latency(
    live_rest: RestClient, *, recent_monday: date, evidence: Evidence
) -> None:
    """How long the calendar's two lookups take: a contract's product, then its schedule.

    Reads one page of the single contracts active on the most recent published Monday for
    their tickers, and one page of the single products listed that day for their codes. Then
    times, five times each, the calendar's whole contract lookup (`ticker.any_of`, the
    largest page, every page as the calendar reads them) for 1 and for 100 tickers, and its
    whole schedule lookup (`product_code.any_of` over its window) for 1, 20, and 100
    products, each up to 4 pages. Records each lookup's seconds, median, and slowest, with
    its rows and pages, and whether it ended within 4 pages (if not, its times are of its
    first 4 pages). A time includes any wait for the key's rate limit. A lookup Massive's
    servers fail with an HTTP 5xx after the client's retries (the contract lookup of 100
    tickers, which sends no `date`, answered HTTP 504 on 2026-11-16) is recorded with its
    error and not timed again.

    Passes when some lookup is answered and every other failed with a 5xx; the times and
    failures are evidence. Sends at most 102 requests: 2, then 5 lookups timed 5 times at up
    to 4 pages each, a failed request counting with its retries.

    Raises:
        WireError: When a lookup fails other than with an HTTP 5xx: a refused key or plan, a
            refused request, or an answer the client cannot read.
    """
    query = {"date": recent_monday, "active": True, "type": "single", "limit": _LIST_PAGE}
    singles = _page(live_rest, _CONTRACTS, query=query)
    tickers = sorted({str(r.get("ticker")) for r in singles})[:100]
    query = {"date": recent_monday, "type": "single", "limit": _LIST_PAGE}
    products = _page(live_rest, _PRODUCTS, query=query)
    codes = sorted({str(r.get("product_code")) for r in products})[:100]
    evidence(
        f"{len(singles)} active single contracts read: {len(tickers)} tickers; "
        f"{len(products)} single products read: {len(codes)} codes"
    )
    assert tickers, f"Massive listed no active single contract on {recent_monday}"
    assert codes, f"Massive listed no single product on {recent_monday}"
    today = futures_session(datetime.now(UTC))  # the calendar's today
    lookups: Sequence[tuple[str, str, Mapping[str, object]]] = [
        (
            f"contracts of {len(tickers[:n])} tickers",
            _CONTRACTS,
            {"ticker.any_of": tickers[:n], "limit": _LIST_PAGE},
        )
        for n in (1, 100)
    ] + [
        (
            f"schedules of {len(codes[:n])} products",
            _SCHEDULES,
            {
                "product_code.any_of": codes[:n],
                "session_end_date.gte": today - _DAYS_BACK,
                "session_end_date.lte": today + _DAYS_AHEAD,
                "limit": _LIST_PAGE,
            },
        )
        for n in (1, 20, 100)
    ]
    answered = 0
    for label, path, lookup in lookups:
        try:
            seconds, rows, pages, ended = _timed(live_rest, path, lookup)
        except WireError as error:
            if error.status is None or error.status < _SERVER_ERROR:
                raise
            evidence(f"{label}: failed: {error}")
            continue
        answered += 1
        evidence(
            f"{label}: {rows} rows in up to {pages} pages"
            f"{'' if ended else ' (a run stopped at the page cap: its time is of the first pages)'}"
            ", seconds "
            f"{[round(s, 3) for s in seconds]}, median {statistics.median(seconds):.3f}, "
            f"slowest {max(seconds):.3f}"
        )
    assert answered, "no lookup was answered"


_LISTING_DAYS = 90
"""How many days of new listings the listing check counts."""

_LISTING_PAGES = 15
"""The most pages of new listings the listing check reads."""


@pytest.mark.settles("126")
@pytest.mark.requests(15)
@pytest.mark.timeout(400)
def test_new_futures_listings_per_day(
    live_rest: RestClient, *, recent_monday: date, evidence: Evidence
) -> None:
    """How many futures contracts Massive lists each day, which sizes contracts new to the list.

    The contracts list is updated daily, so a contract first traded today may be missing
    from it until the next update. Massive's list records no day a contract was added, so
    the first trade date stands in for it. Reads, as of the most recent published Monday, the
    contracts whose `first_trade_date` is within the 90 days before, up to 15 pages of 1,000,
    and counts the distinct tickers by `type`, per first trade date and per weekday. Records
    the days with listings, the mean and largest count per day, each of the last 14 days'
    counts, and whether the list ended within the pages read (if not, the counts are lower
    bounds).

    Passes when every request is answered and lists a contract; the counts are evidence.
    Sends at most 15 requests.
    """
    since = recent_monday - timedelta(days=_LISTING_DAYS)
    query = {
        "date": recent_monday,
        "first_trade_date.gte": since,
        "first_trade_date.lte": recent_monday,
        "limit": _LIST_PAGE,
    }
    rows, pages, ended = _read_pages(live_rest, _CONTRACTS, query, most=_LISTING_PAGES)
    listed = {
        (str(r.get("ticker")), str(r.get("type")), r["first_trade_date"])
        for r in rows
        if isinstance(r.get("first_trade_date"), str)
    }
    evidence(
        f"contracts first traded {since} to {recent_monday}: {len(listed)} tickers in {pages} "
        f"pages ({'the whole list' if ended else 'more follow: lower bounds'}), by type "
        f"{dict(Counter(kind for _, kind, _ in listed))}"
    )
    for kind in sorted({kind for _, kind, _ in listed}):
        per_day = Counter(day for _, k, day in listed if k == kind)
        per_weekday = Counter(date.fromisoformat(day).strftime("%a") for day in per_day.elements())
        recent = sorted(per_day.items())[-14:]
        evidence(
            f"type {kind}: {len(per_day)} days with listings, mean "
            f"{statistics.mean(per_day.values()):.1f} and most {max(per_day.values())} a day; "
            f"by weekday {dict(per_weekday)}; last 14 days {recent}"
        )
    assert listed, f"Massive listed no contract first traded since {since}"


_GAP_PRODUCTS = (
    "ES",
    "NQ",
    "YM",
    "RTY",
    "CL",
    "NG",
    "RB",
    "HO",
    "GC",
    "SI",
    "HG",
    "ZN",
    "ZB",
    "ZF",
    "ZC",
    "ZS",
    "ZW",
    "6E",
    "6J",
    "LE",
)
"""Products of all four venues whose front contracts the gap check reads."""


def _front_contracts(client: RestClient, day: date, evidence: Evidence) -> dict[str, str]:
    """Find each of `_GAP_PRODUCTS`' front contract: the active one that expires first.

    Returns:
        Each product's front contract ticker, for the products Massive listed one for.
    """
    query = {
        "product_code.any_of": list(_GAP_PRODUCTS),
        "date": day,
        "active": True,
        "type": "single",
        "limit": _LIST_PAGE,
    }
    rows = _page(client, _CONTRACTS, query=query)
    fronts: dict[str, tuple[str, str]] = {}
    for row in rows:
        code, ticker, last = (row.get(k) for k in ("product_code", "ticker", "last_trade_date"))
        if not (isinstance(code, str) and isinstance(ticker, str) and isinstance(last, str)):
            continue
        if (
            code in _GAP_PRODUCTS
            and last >= datetime.now(UTC).date().isoformat()
            and (code not in fronts or last < fronts[code][1])
        ):
            fronts[code] = (ticker, last)
    evidence(f"front contracts on {day} ({len(rows)} rows read): {fronts}")
    return {code: ticker for code, (ticker, _) in fronts.items()}


def _latest_gap(spans: Mapping[str, tuple[int, int]], now: int) -> tuple[int, int, str] | None:
    """Find the latest gap between two sessions of a product whose second has begun by `now`.

    Returns:
        The gap's start (the last event of one session) and end (the first event of the
        next), in epoch nanoseconds, and the next session's `session_end_date`; None when no
        such pair of sessions was read. The start may not be before the end, when the next
        session's first event comes before the earlier one's last.
    """
    ordered = sorted(spans.items(), key=lambda item: item[1])
    gaps = [
        (earlier[1][1], later[1][0], later[0])
        for earlier, later in pairwise(ordered)
        if later[1][0] <= now
    ]
    return gaps[-1] if gaps else None


@pytest.mark.settles("128")
@pytest.mark.requests(22)
@pytest.mark.timeout(400)
def test_trades_outside_every_futures_session(
    live_rest: RestClient, *, recent_monday: date, evidence: Evidence
) -> None:
    """Whether Massive sends futures trades between sessions, and which session it gives them.

    The calendar dates a streamed trade by the session whose span (first schedule event to
    last) holds it, and a trade between one session's last event and the next's first by the
    next session. Finds the front contract (the active one that expires first) of 20 products
    of all four venues, reads their schedules from 10 days back to 3 ahead, and for each product
    reads the trades of its front contract strictly between the latest two sessions of which
    the second has begun: from the last event of one to the first event of the next (the
    daily close to the next pre-open, or the weekend). Only that latest pair is read: when
    it touches, the product is recorded without a gap, even if an earlier pair has one.
    Records, per product, the gap, the trades in it (a page of 1,000 at most, marked when
    full), Massive's `session_end_date` of those trades beside the next session's, which the
    calendar would give them, and the first few; then the total.

    Passes when every request is answered and some product has a gap; the counts are
    evidence. Sends at most 22 requests.
    """
    fronts = _front_contracts(live_rest, recent_monday, evidence)
    assert fronts, f"Massive listed no active front contract of {list(_GAP_PRODUCTS)}"
    today = datetime.now(UTC).date()
    query = {
        "product_code.any_of": sorted(fronts),
        "session_end_date.gte": today - timedelta(days=10),
        "session_end_date.lte": today + timedelta(days=3),
        "limit": _LIST_PAGE,
    }
    spans = _schedule_spans(_page(live_rest, _SCHEDULES, query=query))
    now = epoch_ns(datetime.now(UTC))
    total = gaps = 0
    for code, ticker in sorted(fronts.items()):
        gap = _latest_gap(spans.get(code, {}), now)
        if gap is None:
            evidence(f"{code}: no gap between two begun sessions in {sorted(spans.get(code, {}))}")
            continue
        start, end, following = gap
        if start >= end:
            evidence(
                f"{code}: no gap: the session of {following} begins at {_chicago(end)}, before "
                f"the earlier one's last event at {_chicago(start)} Chicago"
            )
            continue
        gaps += 1
        trades = _page(
            live_rest,
            _TRADES,
            query={
                "ticker": ticker,
                "timestamp.gt": _instant(start),
                "timestamp.lt": _instant(end),
                "sort": "timestamp.asc",
                "limit": _LIST_PAGE,
            },
        )
        total += len(trades)
        first = [
            (
                _chicago(t["timestamp"])
                if isinstance(t.get("timestamp"), int)
                else t.get("timestamp"),
                t.get("session_end_date"),
            )
            for t in trades[:3]
        ]
        dates = Counter(str(t.get("session_end_date")) for t in trades)
        evidence(
            f"{code} {ticker}: gap {_chicago(start)} to {_chicago(end)} Chicago, before the "
            f"session of {following}: {len(trades)} trades"
            f"{' (a full page)' if len(trades) == _LIST_PAGE else ''}, Massive's "
            f"session_end_date {dict(dates)}, first {first}"
        )
    evidence(f"total: {total} trades in the gaps of {gaps} products")
    assert gaps, "no product had a gap between two begun sessions"


_FIELDS_TEMPLATE = "product_code.any_of"
"""A list parameter every futures list path has, with no default, maximum, or values, whose
copy under another name `_with_fields` adds."""


def _with_fields(path: str) -> RestPath:
    """Return a futures path's spec with Massive's `fields` query parameter added.

    Massive's docs list `fields` (the fields to return for each result, nested ones by dot)
    on the futures snapshot, contracts, and schedules paths, but the OpenAPI spec the
    library's spec is generated from does not, so the REST client would refuse it. These
    tests add it, as a comma-separated list like `product_code.any_of`, to ask Massive what
    it does with it.

    Returns:
        The path, with one more parameter.
    """
    rest_path = source(Transport.REST, path)
    template = next(param for param in rest_path.params if param.name == _FIELDS_TEMPLATE)
    return replace(rest_path, params=(*rest_path.params, replace(template, name="fields")))


_PROXY_AUTH = 407
"""The HTTP status of a proxy refusing its credentials: the network's answer, not Massive's."""


def _refused(error: WireError) -> bool:
    """Tell whether a wire error is Massive refusing a request, other than for the key.

    Returns:
        True for an HTTP 4xx answer that is neither an authentication nor an entitlement
        refusal, nor a proxy's refusal of its credentials (407): one these tests record as
        evidence instead of failing.
    """
    keyed = isinstance(error, (WireAuthError, WireEntitlementError))
    status = error.status
    return not keyed and status is not None and 400 <= status < 500 and status != _PROXY_AUTH


def _spread(items: Sequence[str], count: int) -> list[str]:
    """Take `count` items spread evenly over a sorted list: the middle of each equal share.

    Returns:
        The items, in their order; all of them when there are no more than `count`.
    """
    if len(items) <= count:
        return list(items)
    return [items[(2 * n + 1) * len(items) // (2 * count)] for n in range(count)]


_DAY_TRIES = 4
"""The most days, from the calendar's today back, `_listed_day` asks the contracts list for."""


def _listed_day(client: RestClient, evidence: Evidence) -> date:
    """Find the latest day, from the calendar's today back, the contracts list holds.

    The contracts list holds trading days only, and is updated daily, so the calendar's today
    (a Saturday, or a Monday before the update) may hold no contract; a lookup with that
    `date` would answer nothing, for want of the day rather than of the contract. Asks for
    one contract of each day, today first, at most `_DAY_TRIES` days.

    Returns:
        The first day asked about that holds a contract, or the last asked when none does
        (every lookup on it then answers nothing, which its evidence shows).
    """
    today = futures_session(datetime.now(UTC))  # the calendar's today
    day = today
    for back in range(_DAY_TRIES):
        day = today - timedelta(days=back)
        if _page(client, _CONTRACTS, query={"date": day, "limit": 1}):
            break
    evidence(f"contracts list day: {day:%a %Y-%m-%d} (the calendar's today is {today})")
    return day


_SNAPSHOT_PAGES = 3
"""The most pages of the whole snapshot, 50,000 rows each, a check reads."""

_SNAPSHOT_FIELDS = ["details.ticker", "details.product_code", "last_trade.last_updated"]
"""The snapshot fields the whole-snapshot read asks for, to keep its pages small."""


@dataclass(frozen=True)
class _Listed:
    """A snapshot row's facts: its ticker's product code and last trade."""

    product_code: str
    traded: int | None
    """The last trade's `last_updated`, in epoch nanoseconds; None when it has none."""


def _read_snapshot(client: RestClient, evidence: Evidence) -> dict[str, _Listed]:
    """Read the whole futures snapshot, up to `_SNAPSHOT_PAGES` pages, with few fields.

    Asks for `_SNAPSHOT_FIELDS` only; when Massive refuses `fields`, reads whole rows instead,
    and says so. Sends at most 6 requests: up to 3 pages, then up to 3 again after a refusal.

    Returns:
        Each ticker's product code and last trade.

    Raises:
        WireError: When a read fails other than by Massive refusing `fields`.
    """
    query: dict[str, object] = {"fields": _SNAPSHOT_FIELDS, "limit": _PRODUCTS_PAGE}
    how = f"fields {_SNAPSHOT_FIELDS}"
    try:
        rows, pages, ended = _read_pages(
            client, _with_fields(_SNAPSHOT), query, most=_SNAPSHOT_PAGES
        )
    except WireError as error:
        if not _refused(error):
            raise
        evidence(f"snapshot with fields {_SNAPSHOT_FIELDS}: refused: {error}; whole rows read")
        query, how = {"limit": _PRODUCTS_PAGE}, "whole rows"
        rows, pages, ended = _read_pages(client, _SNAPSHOT, query, most=_SNAPSHOT_PAGES)
    listed: dict[str, _Listed] = {}
    for row in rows:
        details = row.get("details") or {}
        ticker, traded = details.get("ticker"), (row.get("last_trade") or {}).get("last_updated")
        if isinstance(ticker, str):
            stamp = traded if isinstance(traded, int) else None
            listed[ticker] = _Listed(str(details.get("product_code")), stamp)
    evidence(
        f"snapshot ({how}): {len(rows)} rows in "
        f"{pages} pages ({'the whole snapshot' if ended else 'more follow: a lower bound'}), "
        f"{len(listed)} tickers, {sum(t.traded is not None for t in listed.values())} with a "
        f"last trade, first row {rows[0] if rows else None}"
    )
    return listed


def _look_up(
    client: RestClient,
    path: str,
    tickers: Sequence[str],
    *,
    query: Mapping[str, object],
    most: int,
) -> tuple[set[str], list[str]]:
    """Ask a futures list for tickers by `ticker.any_of`, 100 a request, at most `most` requests.

    When `most` requests cannot ask about every ticker, asks about as many as they can,
    spread evenly over the sorted tickers (`_spread`).

    Returns:
        The tickers the list returned a row for, and the tickers asked about.
    """
    asked = _spread(sorted(tickers), _LOOKUP_CHUNK * most)
    found: set[str] = set()
    for start in range(0, len(asked), _LOOKUP_CHUNK):
        chunk = asked[start : start + _LOOKUP_CHUNK]
        for row in _page(client, path, query={**query, "ticker.any_of": chunk}):
            ticker = (
                (row.get("details") or {}).get("ticker") if path == _SNAPSHOT else row.get("ticker")
            )
            if isinstance(ticker, str):
                found.add(ticker)
    return found, asked


def _share(asked: Sequence[str], of: Sequence[str]) -> str:
    """Say how many of a set of tickers were asked about.

    Returns:
        Such as `"2000 of 5120 (spread evenly; a lower bound)"`, or `"all 80"`.
    """
    if len(asked) == len(of):
        return f"all {len(of)}"
    return f"{len(asked)} of {len(of)} (spread evenly; a lower bound)"


_LAST_RUN_COMBOS = ("CLZ6-CLF7", "CL:BZ N7-Q7", "GCZ6-GCG7")
"""The combos an earlier run asked the contracts list for together, on one full page that held
none of `GCZ6-GCG7`'s rows."""

_RECENT_COMBOS_PER_ROOT = 3
"""The recently traded combos of each of `_COMBO_ROOTS` the one-at-a-time check asks about."""


def _record_contract_rows(
    label: str, rows: Sequence[Mapping[str, Any]], evidence: Evidence
) -> None:
    """Record what one contracts read returned: its rows' type, product code, and dates."""
    kinds = sorted({(str(r.get("type")), str(r.get("product_code"))) for r in rows})
    days = sorted(str(r.get("date")) for r in rows)
    full = " (a full page: a lower bound)" if len(rows) == _LIST_PAGE else ""
    evidence(
        f"{label}: {len(rows)} rows{full}, (type, product_code) {kinds or 'none'}"
        + (f", dates {days[0]} to {days[-1]}" if days else "")
    )


@pytest.mark.settles("129")
@pytest.mark.requests(34)
@pytest.mark.timeout(300)
def test_combo_tickers_in_contracts_list_one_at_a_time(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """Whether the futures contracts list holds a combo, asked for that combo alone.

    An earlier run asked the contracts list for three combos at once and got one full page of
    1,000 rows, sorted by product code, none of them `GCZ6-GCG7`'s: that combo may have been
    cut off by the page rather than missing. Reads the snapshot from `CL` up to `CM` and from
    `GC` up to `GD`, and takes of each up to 3 combos traded within 7 days, one per ticker
    form, beside `CLZ6-CLF7`, `CL:BZ N7-Q7`, and `GCZ6-GCG7`. Finds the latest day the
    contracts list holds (`_listed_day`). Asks the contracts list for each combo alone three
    ways: by `ticker` (every date), by `ticker` and that `date`, and by those and
    `type=combo`; then for them all at once by `ticker.any_of` and that `date`. Records each
    read's rows, their `type`, `product_code`, and dates, and whether its page was full (a
    lower bound: a full every-date page may lack the latest dates).

    A combo the one-ticker reads return no row for is missing from the contracts list; one
    they return rows for and the `ticker.any_of` read does not was cut off by the page; a
    combo with rows on other dates and none on the day asked was not listed that day.
    Passes when every request is answered; the answers are evidence. Sends at most 34
    requests.
    """
    combos, _, _ = _read_combos(live_rest, evidence, per_root=_RECENT_COMBOS_PER_ROOT)
    tickers = sorted({*_LAST_RUN_COMBOS, *(combo.ticker for combo in combos)})
    day = _listed_day(live_rest, evidence)
    for ticker in tickers:
        for label, query in (
            ("every date", {"ticker": ticker, "limit": _LIST_PAGE}),
            (f"date {day}", {"ticker": ticker, "date": day, "limit": _LIST_PAGE}),
            (
                f"date {day}, type combo",
                {"ticker": ticker, "date": day, "type": "combo", "limit": _LIST_PAGE},
            ),
        ):
            rows = _page(live_rest, _CONTRACTS, query=query)
            _record_contract_rows(f"{ticker!r} alone, {label}", rows, evidence)
    query = {"ticker.any_of": tickers, "date": day, "limit": _LIST_PAGE}
    together = _page(live_rest, _CONTRACTS, query=query)
    full = " (a full page: a lower bound)" if len(together) == _LIST_PAGE else ""
    evidence(f"all {len(tickers)} by ticker.any_of, date {day}: {len(together)} rows{full}")
    for ticker in tickers:
        rows = [row for row in together if row.get("ticker") == ticker]
        _record_contract_rows(f"{ticker!r} among all, date {day}", rows, evidence)


_LOOKUP_SIZES = (1, 100, 500)
"""The tickers per lookup the cost checks time."""

_COST_RUNS = 5
"""How many times the cost checks time each lookup."""

_COST_PAGES = 5
"""The most pages of one contracts lookup the cost check reads, each a request."""


@dataclass(frozen=True)
class _Timing:
    """One timed read of a lookup: its rows, and the seconds of each page."""

    rows: int
    seconds: tuple[float, ...]
    """Each page's seconds, from asking for it to its rows, in order."""
    ended: bool
    """Whether the lookup ended within the pages read."""


def _timed_read(
    client: RestClient, path: str, query: Mapping[str, object], *, most: int
) -> _Timing:
    """Read up to `most` pages of a futures lookup, timing each page.

    Returns:
        The rows read, each page's seconds, and whether the lookup ended within them.
    """
    rows = 0
    seconds: list[float] = []
    start = time.perf_counter()
    for page in client.pages(source(Transport.REST, path), {}, query, bucket="futures"):
        seconds.append(time.perf_counter() - start)
        rows += len(page.rows)
        if not (isinstance(page.body, dict) and page.body.get("next_url")):
            return _Timing(rows, tuple(seconds), ended=True)
        if len(seconds) == most:
            break
        start = time.perf_counter()
    return _Timing(rows, tuple(seconds), ended=False)


def _cost_sample(client: RestClient, evidence: Evidence) -> list[str]:
    """Take the tickers the cost checks look up: 500 spread evenly over the whole snapshot.

    Returns:
        The tickers, sorted; of them, `_spread` takes 100 and 1 again.
    """
    sample = _spread(sorted(_read_snapshot(client, evidence)), _LOOKUP_SIZES[-1])
    assert sample, "the futures snapshot lists no ticker"
    evidence(f"tickers looked up: {len(sample)}, from {sample[0]!r} to {sample[-1]!r}")
    return sample


def _lookups(sample: Sequence[str], day: date) -> dict[str, dict[str, object]]:
    """Build the cost check's contracts lookups, those with `date` first.

    Each of `_LOOKUP_SIZES` is looked up with `date`; each but the largest also without one,
    which an earlier run saw fail with HTTP 502 for 500 tickers.

    Returns:
        Each lookup's query by its label, in the order the runs send them.
    """
    queries = {
        size: {"ticker.any_of": _spread(sample, size), "limit": _LIST_PAGE}
        for size in _LOOKUP_SIZES
    }
    lookups = {f"{size} tickers, date {day}": {**q, "date": day} for size, q in queries.items()}
    lookups.update(
        (f"{size} tickers, no date", q) for size, q in queries.items() if size != _LOOKUP_SIZES[-1]
    )
    return lookups


def _cost_run(
    client: RestClient, label: str, query: Mapping[str, object], *, evidence: Evidence
) -> _Timing | WireError:
    """Time one run of a contracts lookup and record it at once, or record its failure.

    Returns:
        The run's timing, or the `WireError` it failed with after the client's retries.

    Raises:
        WireAuthError: When Massive refuses the key.
        WireEntitlementError: When the key's plan lacks futures data.
    """
    try:
        run = _timed_read(client, _CONTRACTS, query, most=_COST_PAGES)
    except (WireAuthError, WireEntitlementError):
        raise
    except WireError as error:
        evidence(f"contracts, {label}: failed: {error}")
        return error
    evidence(
        f"contracts, {label}: {run.rows} rows, {len(run.seconds)} pages"
        f"{'' if run.ended else ' (cut: a lower bound)'}, seconds per page "
        f"{[round(s, 3) for s in run.seconds]}"
    )
    return run


def _record_cost(label: str, runs: Sequence[_Timing | WireError], evidence: Evidence) -> None:
    """Record a lookup's runs: the seconds of those answered, and the status of each failure."""
    timed = [run for run in runs if isinstance(run, _Timing)]
    failed = [run.status for run in runs if isinstance(run, WireError)]
    totals = [sum(run.seconds) for run in timed]
    capped = (
        ""
        if all(run.ended for run in timed)
        else f" (a run stopped at {_COST_PAGES} pages: its rows and seconds are lower bounds)"
    )
    summary = (
        f"median {statistics.median(totals):.3f}, slowest {max(totals):.3f}"
        if totals
        else "no run answered"
    )
    evidence(
        f"contracts, {label}: {len(timed)} of {len(runs)} runs answered{capped}, seconds per "
        f"run {[round(t, 3) for t in totals]}, {summary}; failures by HTTP status {failed}"
    )


@pytest.mark.settles("130")
@pytest.mark.requests(95)
@pytest.mark.timeout(1200)
def test_contracts_lookup_cost_with_and_without_date(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What a contracts lookup by `ticker.any_of` costs with a `date` and without one.

    A lookup by `ticker.any_of` and no `date` pages through every date of every contract; an
    earlier run timed it at 14 s for 100 tickers in four of five runs and 0.45 s in the fifth,
    unexplained. The calendar now always sends a `date` range
    (`test_calendar_contracts_lookup_as_sent`). Reads the whole snapshot, takes 500 tickers
    spread evenly over it (singles and combos), and of them 100 and then 1, spread evenly
    again; finds the latest day the contracts list holds (`_listed_day`). Then times five
    lookups, each at the largest page and up to 5 pages: 1, 100, and 500 tickers with that
    `date`, then 1 and 100 without one (500 without one failed with HTTP 502 in an earlier
    run, and is not sent). Each runs 5 times, the five taking turns, those with `date` first
    in each turn, so a slow spell of Massive's falls on all five alike (a cache of Massive's
    would still favor the later runs). Records each run as it completes: its rows, pages,
    and seconds per page; and a run that fails (`WireError`, after the client's retries), as
    evidence, without stopping the others. Then records each lookup's runs answered, seconds
    per run, median, and slowest. A run cut at 5 pages (as 1 and 100 tickers without `date`
    may be) says its rows and seconds are lower bounds, of its first pages only.

    A lookup with `date` that returns fewer rows than tickers asked found some tickers not
    listed that day. Seconds per page tell the causes apart: pages of like time mean the cost
    is the rows `date` leaves out; one slow page among fast ones means Massive's variance.
    Times include any wait for the key's rate limit. Passes when some run is answered; the
    times and failures are evidence. Sends at most 95 requests: up to 6 for the snapshot
    (`_read_snapshot`), up to 4 days asked, then 5 runs of up to 17 pages.

    Raises:
        WireAuthError: When Massive refuses the key.
        WireEntitlementError: When the key's plan lacks futures data.
    """
    sample = _cost_sample(live_rest, evidence)
    day = _listed_day(live_rest, evidence)
    lookups = _lookups(sample, day)
    runs: dict[str, list[_Timing | WireError]] = {label: [] for label in lookups}
    for turn in range(1, _COST_RUNS + 1):
        for label, query in lookups.items():
            runs[label].append(
                _cost_run(live_rest, f"{label}, run {turn}", query, evidence=evidence)
            )
    for label, done in runs.items():
        _record_cost(label, done, evidence)
    assert any(isinstance(run, _Timing) for done in runs.values() for run in done), (
        "no contracts lookup was answered"
    )


@pytest.mark.settles("139")
@pytest.mark.requests(20)
@pytest.mark.timeout(600)
def test_calendar_contracts_lookup_as_sent(live_rest: RestClient, *, evidence: Evidence) -> None:
    """What the calendar's own contracts lookup of 100 tickers costs, and whether it fails.

    Massive answered a 100-ticker `ticker.any_of` lookup with no `date` with HTTP 504 twice in
    one day. The calendar sends no such lookup: it asks the week up to today's session
    (`futures_calendar.contracts_query`), at most `TICKERS_PER_REQUEST` tickers a request.
    Reads the whole snapshot, takes 100 tickers spread evenly over it, and sends the
    calendar's query for them 5 times, timing each page. Records each run's rows, pages, and
    seconds, and any failure's status and text, as evidence. Passes when every run is
    answered; a failure is recorded, then fails the test, so the evidence names it. Sends at
    most 20 requests: up to 6 for the snapshot, then 5 runs of up to 2 pages.
    """
    tickers = tuple(_spread(sorted(_read_snapshot(live_rest, evidence)), TICKERS_PER_REQUEST))
    today = futures_session(datetime.now(UTC))  # the calendar's today
    query = contracts_query(tickers, today, page=_LIST_PAGE)
    evidence(
        f"the calendar's query: {len(tickers)} tickers, date.gte {query['date.gte']}, "
        f"date.lte {query['date.lte']}, limit {query['limit']}"
    )
    failures: list[str] = []
    for run in range(1, _COST_RUNS + 1):
        try:
            timing = _timed_read(live_rest, _CONTRACTS, query, most=2)
        except dm.MassiveError as error:
            failures.append(f"run {run}: HTTP {error.status}: {error}")
            evidence(f"run {run}: failed with HTTP {error.status}: {error}")
            continue
        evidence(
            f"run {run}: {timing.rows} rows, {len(timing.seconds)} pages"
            f"{'' if timing.ended else ' (more follow)'}, seconds per page "
            f"{[round(s, 3) for s in timing.seconds]}, total {sum(timing.seconds):.3f}"
        )
    assert failures == []


_PRODUCT_FIELDS = ["details.ticker", "details.product_code"]
"""The nested snapshot fields a product lookup needs."""


@pytest.mark.settles("131")
@pytest.mark.requests(36)
@pytest.mark.timeout(600)
def test_snapshot_lookup_by_ticker_list(live_rest: RestClient, *, evidence: Evidence) -> None:
    """What a snapshot lookup by `ticker.any_of` costs, and whether nested `fields` works.

    The snapshot names each ticker's product in its row, so a calendar could find the
    products of a batch of tickers in one request. Reads the whole snapshot and takes 500
    tickers spread evenly over it, and of them 100 and then 1, spread evenly again. Then
    times six lookups, each one page of up to 50,000 rows: 1, 100, and 500 tickers, without
    `fields` and with `fields=details.ticker,details.product_code`. Each runs 5 times, the
    six taking turns. Records each lookup's rows, how many of its tickers came back, its JSON
    bytes, its first row, and its seconds per run, median, and slowest; and per size, whether
    `fields` was honored: every row holds `details` with `ticker` and `product_code` only, the
    same tickers come back as without it, in fewer bytes. A lookup Massive refuses with an HTTP 4xx
    other than for the key (a URL too long, a `fields` it does not take) is recorded with
    Massive's answer and not sent again.

    Times include any wait for the key's rate limit. Passes when every request is answered
    or refused as above; the times are evidence. Sends at most 36 requests: up to 6 for the
    snapshot (`_read_snapshot`), then 30.

    Raises:
        WireError: When a lookup fails other than by such a refusal.
    """
    sample = _cost_sample(live_rest, evidence)
    snapshot = _with_fields(_SNAPSHOT)
    lookups: dict[tuple[int, bool], dict[str, object]] = {}
    for size in _LOOKUP_SIZES:
        query: dict[str, object] = {"ticker.any_of": _spread(sample, size), "limit": _PRODUCTS_PAGE}
        lookups[size, False] = query
        lookups[size, True] = {**query, "fields": _PRODUCT_FIELDS}
    seconds: dict[tuple[int, bool], list[float]] = {key: [] for key in lookups}
    first: dict[tuple[int, bool], list[dict[str, Any]]] = {}
    refused: set[tuple[int, bool]] = set()
    for _ in range(_COST_RUNS):
        for key, query in lookups.items():
            if key in refused:
                continue
            start = time.perf_counter()
            try:
                page = live_rest.get(snapshot, {}, query, bucket="futures")
            except WireError as error:
                if not _refused(error):
                    raise
                refused.add(key)
                evidence(f"snapshot, {key[0]} tickers, fields {key[1]}: refused: {error}")
                continue
            seconds[key].append(time.perf_counter() - start)
            if key not in first:
                first[key] = page.rows
                asked = set(_spread(sample, key[0]))
                returned = {
                    details.get("ticker")
                    for row in page.rows
                    if isinstance(details := row.get("details"), dict)
                }
                more = isinstance(page.body, dict) and page.body.get("next_url")
                evidence(
                    f"snapshot, {key[0]} tickers, fields {key[1]}: {len(page.rows)} rows"
                    f"{' (more follow: a lower bound)' if more else ''}, "
                    f"{len(returned & asked)} of {len(asked)} tickers returned, "
                    f"{len(json.dumps(page.rows))} bytes, first row "
                    f"{page.rows[0] if page.rows else None}"
                )
    for (size, fields), times in seconds.items():
        if times:
            evidence(
                f"snapshot, {size} tickers, fields {fields}: seconds "
                f"{[round(s, 3) for s in times]}, median {statistics.median(times):.3f}, "
                f"slowest {max(times):.3f}"
            )
    for size in _LOOKUP_SIZES:
        whole, chosen = first.get((size, False)), first.get((size, True))
        if whole is None or chosen is None:
            continue
        evidence(f"fields at {size} tickers: {_fields_verdict(chosen, whole)}")


def _tickers_of(rows: Sequence[Mapping[str, Any]]) -> set[object]:
    """Collect the `details.ticker` of snapshot rows.

    Returns:
        The tickers, with None for a row without one.
    """
    return {
        details.get("ticker") if isinstance(details := row.get("details"), dict) else None
        for row in rows
    }


def _fields_verdict(chosen: Sequence[Mapping[str, Any]], whole: Sequence[Mapping[str, Any]]) -> str:
    """Judge whether Massive honored `fields=details.ticker,details.product_code`.

    It did when every row holds `details` alone, with `ticker` and `product_code` alone, the
    same tickers come back as without `fields`, and the rows take fewer bytes.

    Returns:
        The verdict, with what it rests on.
    """
    shapes = Counter(
        (tuple(sorted(row)), tuple(sorted(row.get("details") or {}))) for row in chosen
    )
    wanted = (("details",), ("product_code", "ticker"))
    same = _tickers_of(chosen) == _tickers_of(whole)
    smaller = len(json.dumps(chosen)) < len(json.dumps(whole))
    honored = bool(chosen) and set(shapes) == {wanted} and same and smaller
    return (
        f"honored {honored}: (row keys, details keys) by rows {dict(shapes)}, the same tickers "
        f"as without fields {same}, bytes {len(json.dumps(chosen))} with it and "
        f"{len(json.dumps(whole))} without"
    )


_COMBO_POOL = 100
"""The most combos the combo-dates check reads the schedules of, before picking."""

_DATED_COMBOS = 30
"""The most combos the combo-dates check reads trades of, a request each."""

_COMBO_TRADES = 200
"""The latest trades of each combo the combo-dates check places."""

_COMBO_DAYS = timedelta(days=3)
"""How far back the combo-dates check takes combos' trades."""

_COMBO_SCHEDULE_PAGES = 10
"""The most pages of the candidates' schedules the combo-dates check reads."""

_PRODUCT_PAIR = re.compile(r"^([A-Z0-9]+):([A-Z0-9]+) ")
"""The two product codes an inter-commodity combo may name before its months: `CL` and `BZ` of
`CL:BZ N7-Q7` (`BF` of `CL:BF F7-G7-H7` names a butterfly, not a product)."""


def _dash_legs(ticker: str) -> list[str]:
    """Split a combo ticker into its `-` legs, leaving out a trailing ratio.

    Returns:
        Such as `["CLG7", "BZG7"]` of `CLG7-BZG7`, `["NKDZ6", "NIYZ6"]` of `NKDZ6-NIYZ6 2:3`,
        or `["CL:BZ"]` of `CL:BZ H7-M7`, whose legs are months, not tickers.
    """
    return ticker.split(" ", 1)[0].split("-")


def _pool_combos(listed: Mapping[str, _Listed], since: int) -> list[str]:
    """Order the combos traded since an instant, those most likely to tell products apart first.

    A combo here is a ticker with a leg separator (`-`, `:`, or a space). First come those
    whose ticker does not start with their product code, then inter-commodity spreads (a `:`,
    or `-` legs of different products), then one per product code; each group most recent
    trade first.

    Returns:
        Up to `_COMBO_POOL` tickers, in that order.
    """
    recent = sorted(
        (
            (ticker, found)
            for ticker, found in listed.items()
            if _LEG_SEPARATOR.search(ticker) and found.traded is not None and found.traded >= since
        ),
        key=lambda item: item[1].traded or 0,
        reverse=True,
    )

    def mixed(ticker: str) -> bool:
        legs = {listed[leg].product_code for leg in _dash_legs(ticker) if leg in listed}
        return ":" in ticker or len(legs) > 1

    unprefixed = [ticker for ticker, found in recent if not ticker.startswith(found.product_code)]
    inter = [ticker for ticker, _ in recent if mixed(ticker)]
    one_per_code: dict[str, str] = {}
    for ticker, found in recent:
        one_per_code.setdefault(found.product_code, ticker)
    return list(dict.fromkeys([*unprefixed, *inter, *one_per_code.values()]))[:_COMBO_POOL]


def _candidates(ticker: str, listed: Mapping[str, _Listed], singles: set[str]) -> list[str]:
    """Name the products whose schedule could date a combo's trades.

    They are its snapshot product code, every `-` leg's (`CL` and `BZ` of `CLG7-BZG7`), and
    the two product codes an inter-commodity combo names (`_PRODUCT_PAIR`), each only when
    `singles` (the single products listed) holds it.

    Returns:
        The codes, sorted.
    """
    codes = {listed[ticker].product_code}
    codes |= {listed[leg].product_code for leg in _dash_legs(ticker) if leg in listed}
    pair = _PRODUCT_PAIR.match(ticker)
    if pair:
        codes |= set(pair.groups())
    return sorted(code for code in codes if code == listed[ticker].product_code or code in singles)


def _spans_differ(codes: Sequence[str], spans: Mapping[str, Mapping[str, tuple[int, int]]]) -> bool:
    """Tell whether two candidates' sessions differ, so that a trade could tell them apart.

    Returns:
        True when some candidate's sessions, by `session_end_date` with their first and last
        event, are not those of the first candidate.
    """
    return any(spans.get(code, {}) != spans.get(codes[0], {}) for code in codes[1:])


def _candidate_sessions(
    client: RestClient,
    candidates: Mapping[str, Sequence[str]],
    *,
    today: date,
    evidence: Evidence,
) -> dict[str, dict[str, tuple[int, int]]]:
    """Read the candidates' sessions from 4 days back to 1 ahead, to the end or 10 pages.

    Returns:
        The sessions, as `_schedule_spans` gives them.
    """
    query = {
        "product_code.any_of": sorted({code for codes in candidates.values() for code in codes}),
        "session_end_date.gte": today - timedelta(days=4),
        "session_end_date.lte": today + timedelta(days=1),
        "limit": _LIST_PAGE,
    }
    events, pages, ended = _read_pages(client, _SCHEDULES, query, most=_COMBO_SCHEDULE_PAGES)
    spans = _schedule_spans(events)
    evidence(
        f"schedules of the candidates: {len(events)} events in {pages} pages "
        f"({'all of them' if ended else 'more follow: a lower bound'}), {len(spans)} products"
    )
    return spans


_PLACED_APART = "placed apart"
"""The tally of the trades two of a combo's candidates place in different sessions: the only
trades that tell the snapshot's product from the others. A trade one candidate places and
another places in no session is tallied apart, since a schedule read short can leave it out."""


@dataclass(frozen=True)
class _Candidates:
    """A combo's candidate products: the snapshot's, every one, and whether they discriminate."""

    product_code: str
    codes: Sequence[str]
    discriminating: bool
    """Whether the candidates' sessions differ, so that a trade can tell them apart."""


def _place_combo_trades(
    client: RestClient,
    ticker: str,
    candidates: _Candidates,
    *,
    spans: Mapping[str, Mapping[str, tuple[int, int]]],
    since: datetime,
    evidence: Evidence,
) -> Counter[str]:
    """Read a combo's latest trades and place each in each candidate's session.

    A trades read Massive refuses (`_refused`) is recorded, and places nothing.

    Returns:
        The trades by candidate (`product` for the snapshot's, `other`) and verdict (`agree`
        with the trade's `session_end_date`, `disagree`, or `none` when no session holds it),
        under `_PLACED_APART` the trades two candidates place in different sessions, and the
        trades some candidates place and others place in none.

    Raises:
        WireError: When the read fails other than by such a refusal.
    """
    query = {
        "ticker": ticker,
        "timestamp.gte": since,
        "sort": "timestamp.desc",
        "limit": _COMBO_TRADES,
    }
    try:
        trades = _page(client, _TRADES, query=query)
    except WireError as error:
        if not _refused(error):
            raise
        evidence(f"{ticker!r}: trades read refused: {error}")
        return Counter()
    tally: Counter[str] = Counter()
    parts: Counter[str] = Counter()
    unlike: list[tuple[str, str, str, str]] = []
    for trade in trades:
        stamp, day = trade.get("timestamp"), trade.get("session_end_date")
        if not isinstance(stamp, int):
            tally["no integer timestamp"] += 1
            continue
        local = _instant(stamp).astimezone(_CHICAGO)
        parts[f"{local:%a} {'evening' if local.hour >= 17 else 'day'}"] += 1
        placements = {
            code: _session_holding(spans.get(code, {}), stamp) for code in candidates.codes
        }
        dated = {placed for placed in placements.values() if not placed.startswith("none")}
        if len(dated) > 1:
            tally[_PLACED_APART] += 1
        elif dated and any(placed.startswith("none") for placed in placements.values()):
            # some candidates place it and others place it in no session: a short read may do that
            tally["placed by some candidates only"] += 1
        for code, placed in placements.items():
            verdict = (
                "agree" if placed == day else "none" if placed.startswith("none") else ("disagree")
            )
            tally[f"{'product' if code == candidates.product_code else 'other'} {verdict}"] += 1
            if verdict == "disagree":
                unlike.append((code, _chicago(stamp), str(day), placed))
    evidence(
        f"{ticker!r} (product {candidates.product_code!r}, candidates {list(candidates.codes)}, "
        f"sessions differ {candidates.discriminating}): {len(trades)} trades, {dict(tally)}, by "
        f"part {dict(parts)}, disagreements (candidate, trade in Chicago, Massive's session, the "
        f"schedule's) {unlike[:5]}"
    )
    return tally


@pytest.mark.settles("132")
@pytest.mark.requests(51)
@pytest.mark.timeout(600)
def test_combo_trade_sessions_by_snapshot_product(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """Whether a combo's snapshot product code puts its trades in Massive's own session.

    The calendar would date a combo's streamed rows by the schedule of the product its snapshot
    row names. An earlier run checked three combos whose tickers start with their product code,
    on a Sunday evening, when every product's trades fall in Monday's session: that could not
    tell the product Massive names from any other. Reads the whole snapshot and the single
    products listed on the latest day the contracts list holds (`_listed_day`), and takes up to
    100 combos traded within 3 days: first those whose ticker does not start with their product
    code, then inter-commodity spreads (`CL:BZ N7-Q7`, or legs of different products), then one
    per product code. A combo's candidates are its snapshot product code, every `-` leg's (`CL`
    and `BZ` of `CLG7-BZG7`), and the two codes an inter-commodity ticker names (`CL`, `BZ` of
    `CL:BZ N7-Q7`), when they are listed single products. Reads the candidates' schedules from 4
    days back to 1 ahead, to the end (up to 10 pages, else a lower bound). Picks 30 of the
    combos, first those whose candidates' sessions differ (only their trades can tell one
    candidate from another), and reads each one's latest 200 trades within 3 days. Places each
    trade in the session of each candidate whose span (first event to last) holds it, beside the
    trade's own `session_end_date`. Records per combo and in total: trades that agree, disagree
    (each listed), and that no session holds, by candidate (the snapshot's product or another),
    and the trades by Chicago weekday and part of the session (from 17:00, the evening that
    belongs to the next day); and how many combos have a trade their candidates place in
    different sessions, the only trades that discriminate. A trades read Massive refuses (a
    ticker it cannot take in the path) is recorded, not failed.

    The snapshot's product dates combos when its trades agree; that settles it against the
    other candidates only by the trades they place apart, so with none the evidence says the
    question stays open. Run on a weekday, so trades from both parts of a session are read.
    Passes when every request is answered or refused as above and a combo's trade is read;
    the counts are evidence. Sends at most 51 requests.

    Raises:
        WireError: When a trades read fails other than by a refusal.
    """
    listed = _read_snapshot(live_rest, evidence)
    now = datetime.now(UTC)
    today = futures_session(now)  # the calendar's today
    query = {"date": _listed_day(live_rest, evidence), "type": "single", "limit": _PRODUCTS_PAGE}
    singles = {str(row.get("product_code")) for row in _page(live_rest, _PRODUCTS, query=query)}
    pool = _pool_combos(listed, epoch_ns(now - _COMBO_DAYS))
    assert pool, "no combo traded within 3 days: nothing to place"
    candidates = {ticker: _candidates(ticker, listed, singles) for ticker in pool}
    spans = _candidate_sessions(live_rest, candidates, today=today, evidence=evidence)
    deciding = {ticker for ticker in pool if _spans_differ(candidates[ticker], spans)}
    combos = list(dict.fromkeys([*sorted(deciding, key=pool.index), *pool]))[:_DATED_COMBOS]
    evidence(
        f"{len(singles)} single products; {len(pool)} combos in the pool, {len(deciding)} whose "
        "candidates' sessions differ"
    )
    totals: Counter[str] = Counter()
    telling = 0
    for ticker in combos:
        tally = _place_combo_trades(
            live_rest,
            ticker,
            _Candidates(listed[ticker].product_code, candidates[ticker], ticker in deciding),
            spans=spans,
            since=now - _COMBO_DAYS,
            evidence=evidence,
        )
        totals += tally
        telling += tally[_PLACED_APART] > 0
    evidence(
        f"total over {len(combos)} combos, {len(deciding & set(combos))} whose candidates' "
        f"sessions differ, {telling} with a trade they place apart"
        f"{'' if telling else ' (none: the snapshot product against the others stays open)'}: "
        f"{dict(totals)}"
    )
    assert totals, "no combo trade was read"


_SPACED_WINDOW = timedelta(hours=1)
"""How long before a combo's last snapshot trade the path spelling check reads its trades."""


def _spelled_path(path: str, text: str) -> RestPath:
    """Return a futures path's spec with its `{ticker}` placeholder replaced by text as written.

    The REST client encodes a path value itself, percent-encoding every character but
    letters, digits, and `-._~` (`CL%3ABZ%20H7-M7`); this spec sends another spelling, such
    as `CL:BZ%20H7-M7`, to tell Massive's reading of the path from the client's encoding.

    Returns:
        The path with no path parameter, the text in its place.
    """
    rest_path = source(Transport.REST, path)
    params = tuple(param for param in rest_path.params if not param.in_path)
    return replace(rest_path, key=rest_path.key.replace("{ticker}", text), params=params)


def _record_trades_read(
    client: RestClient,
    label: str,
    rest_path: RestPath,
    *,
    path_params: Mapping[str, object],
    query: Mapping[str, object],
    evidence: Evidence,
) -> None:
    """Read one page of a combo's trades and record its count and latest trade, or the refusal.

    Raises:
        WireError: When the read fails other than by Massive refusing it (`_refused`).
    """
    try:
        trades = client.get(rest_path, path_params, query, bucket="futures").rows
    except WireError as error:
        if not _refused(error):
            raise
        evidence(f"{label}: refused: {error}")
        return
    stamps = [stamp for trade in trades if isinstance(stamp := trade.get("timestamp"), int)]
    latest = _chicago(max(stamps)) if stamps else None
    evidence(f"{label}: {len(trades)} trades, latest {latest}")


def _spaced_and_plain_combos(client: RestClient, evidence: Evidence) -> tuple[_Combo, _Combo]:
    """Pick the CL combos the path spelling check reads, from the snapshot from `CL` up to `CM`.

    Returns:
        The combo traded most recently within 7 days whose ticker holds `:` and a space, and
        the one whose ticker holds neither, the control.
    """
    since = epoch_ns(datetime.now(UTC) - _RECENT_TRADE)
    query = {"ticker.gte": "CL", "ticker.lt": "CM", "limit": _PRODUCTS_PAGE}
    _, recent = _snapshot_combos(_page(client, _SNAPSHOT, query=query), since)
    spaced = [combo for combo in recent if ":" in combo.ticker and " " in combo.ticker]
    plain = [combo for combo in recent if ":" not in combo.ticker and " " not in combo.ticker]
    assert spaced, "no CL combo whose ticker holds ':' and a space traded within 7 days"
    assert plain, "no CL combo whose ticker holds neither ':' nor a space traded within 7 days"
    combo, control = spaced[0], plain[0]
    evidence(
        f"combo {combo.ticker!r}, last snapshot trade {_chicago(combo.traded)}; control "
        f"{control.ticker!r}, last snapshot trade {_chicago(control.traded)}"
    )
    return combo, control


@pytest.mark.settles("136")
@pytest.mark.requests(9)
@pytest.mark.timeout(300)
def test_spaced_combo_trades_by_path_spelling(
    live_session: dm.MassiveSession,
    live_rest: RestClient,
    *,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether Massive's trades path reads a combo whose ticker holds `:` or a space.

    An earlier run read no trade from `/futures/v1/trades/{ticker}` for any of 21 such combos
    (`CL:BZ H7-M7`), though the snapshot showed each trading minutes before, while combos
    without them (`CLG7-BZG7`) read 200 each. The library sends such a ticker percent-encoded,
    `CL%3ABZ%20H7-M7`. Massive's docs say nothing of how a path ticker is encoded; a URL path
    may hold `:` as it is and a space as `%20` (RFC 3986), and some servers read `+` as a space.
    Reads the snapshot from `CL` up to `CM`, and takes the combo traded most recently within 7
    days whose ticker holds `:` and a space (so that each spelling below differs) and, as the
    control, the one traded most recently whose ticker holds neither. Reads up to 10 of the
    combo's trades, latest first, from an hour before its last snapshot trade, its ticker
    spelled four ways: as the library sends it, with `:` as it is (`CL:BZ%20H7-M7`), and with
    `+` for the space, `:` as it is (`CL:BZ+H7-M7`) and encoded (`CL%3ABZ+H7-M7`). Reads the
    control's trades the same way, from an hour before its own last trade, as the library sends
    it and with its `-` escaped (`CLG7%2DBZG7`), which shows whether Massive decodes an escape
    in the path at all; then the combo's trades in its hour as a user reads them,
    `m.futures.trades(combo).history(start, end)`. Records each read's trades and latest trade
    in Chicago time, or Massive's refusal.

    When some spelling reads trades the library's does not, the library's encoding is the cause,
    and that spelling is the one to send; so is it when the control reads trades as the library
    sends it and none with its `-` escaped, since Massive then reads no escape. When no spelling
    reads a trade and the control reads trades both ways, Massive's trades path lacks such
    combos: a report for Massive, and `history()` of one gives no rows. When the control reads
    none either, the run settles nothing. Passes when every request is answered or refused; the
    counts are evidence. Sends at most 9 requests: the snapshot, six trades reads, and up to 2
    pages of `history()`.

    Raises:
        WireError: When a trades read fails other than by a refusal.
    """
    combo, control = _spaced_and_plain_combos(live_rest, evidence)
    start = _instant(combo.traded) - _SPACED_WINDOW
    trades = {"timestamp.gte": start, "sort": "timestamp.desc", "limit": 10}
    since_control = _instant(control.traded) - _SPACED_WINDOW
    colon = quote(combo.ticker, safe=":")
    for text in (
        colon,
        colon.replace("%20", "+"),
        quote(combo.ticker, safe="").replace("%20", "+"),
    ):
        rest_path = _spelled_path(_TRADES, text)
        _record_trades_read(
            live_rest, rest_path.key, rest_path, path_params={}, query=trades, evidence=evidence
        )
    escaped = quote(control.ticker, safe="").replace("-", "%2D")
    control_path = _spelled_path(_TRADES, escaped)
    control_trades = {**trades, "timestamp.gte": since_control}
    _record_trades_read(
        live_rest,
        f"control {control_path.key}",
        control_path,
        path_params={},
        query=control_trades,
        evidence=evidence,
    )
    for ticker, query in ((combo.ticker, trades), (control.ticker, control_trades)):
        _record_trades_read(
            live_rest,
            f"{ticker!r} as the library sends it ({quote(ticker, safe='')})",
            source(Transport.REST, _TRADES),
            path_params={"ticker": ticker},
            query=query,
            evidence=evidence,
        )
    end = _instant(combo.traded) + timedelta(seconds=1)
    rows = table_rows(live_session.futures.trades(combo.ticker).history(start, end, wait=True))
    evidence(
        f"history() of {combo.ticker!r}, {start:%H:%M:%S} to {end:%H:%M:%S} UTC: {len(rows)} rows"
    )


_LISTING_CONTRACT_PAGES = 40
"""The most pages of the day's active contracts the listing-day check reads."""

_CROSS_LOOKUPS = 20
"""The most lookups, 100 tickers each, the listing-day check sends to the contracts list."""

_LATE_LOOKUPS = 5
"""The most lookups, 100 tickers each, the listing-day check sends for a week's listings."""

_SNAPSHOT_LOOKUPS = 10
"""The most lookups, 100 tickers each, the listing-day check sends to the snapshot."""

_NEW_LISTING_DAYS = timedelta(days=3)
"""How recent a contract's first trade date must be to count as a new listing."""


def _read_active_contracts(
    client: RestClient, day: date, evidence: Evidence
) -> dict[str, dict[str, Any]]:
    """Read the contracts active on a day, up to 40 pages, and count its new listings.

    Returns:
        Each contract's row, by ticker.
    """
    query = {"date": day, "active": True, "limit": _LIST_PAGE}
    rows, pages, ended = _read_pages(client, _CONTRACTS, query, most=_LISTING_CONTRACT_PAGES)
    contracts = {str(row.get("ticker")): row for row in rows}
    firsts = Counter(str(row.get("first_trade_date")) for row in contracts.values())
    recent = (day - _NEW_LISTING_DAYS).isoformat()
    evidence(
        f"contracts active on {day}: {len(contracts)} tickers in {pages} pages "
        f"({'the whole list' if ended else 'more follow: a lower bound'}), by type "
        f"{dict(Counter(str(row.get('type')) for row in contracts.values()))}; first traded on "
        f"{day}: {firsts[day.isoformat()]}, since {recent}: "
        f"{sum(n for first, n in firsts.items() if first >= recent)}"
    )
    return contracts


def _record_traded_but_unlisted(
    client: RestClient,
    listed: Mapping[str, _Listed],
    contracts: Mapping[str, object],
    *,
    day: date,
    evidence: Evidence,
) -> None:
    """Record the snapshot's tickers traded in today's session that the contracts list lacks.

    Asks the contracts list for those its pages read lacked, by `ticker.any_of` and `day`,
    and for those still lacking by `ticker.any_of` and the week up to `day`.
    """
    today = futures_session(datetime.now(UTC))  # the calendar's today
    traded = sorted(
        ticker
        for ticker, found in listed.items()
        if found.traded is not None and futures_session(_instant(found.traded)) == today
    )
    unread = sorted(set(traded) - set(contracts))
    query = {"date": day, "limit": _LIST_PAGE}
    found, asked = _look_up(client, _CONTRACTS, unread, query=query, most=_CROSS_LOOKUPS)
    lacking = sorted(set(asked) - found)
    query = {"date.gte": day - timedelta(days=7), "date.lte": day, "limit": _LIST_PAGE}
    later, again = _look_up(client, _CONTRACTS, lacking, query=query, most=_LATE_LOOKUPS)
    evidence(
        f"snapshot tickers traded in the session of {today}: {len(traded)}; not in the "
        f"contracts pages read: {len(unread)}; asked about {_share(asked, unread)}: "
        f"{len(found)} listed on {day}, {len(lacking)} not; of those, asked about "
        f"{_share(again, lacking)} within the week before: {len(later)} listed, "
        f"{len(set(again) - later)} not, e.g. {sorted(set(again) - later)[:40]}"
    )


@pytest.mark.settles("133")
@pytest.mark.requests(85)
@pytest.mark.timeout(900)
def test_snapshot_and_contracts_list_differences_on_a_listing_day(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """How many futures tickers the snapshot holds and the contracts list lacks, and the reverse.

    The calendar would find a ticker's product in the snapshot first and the contracts list
    second. New contracts are listed in bursts (3,035 combos and 1,631 singles first traded on
    Monday 2026-11-02, the month's first Monday; one on the Monday after), so the check means
    most on such a day, and records how many contracts were first traded on its day to say
    whether it was one. Finds the latest day the contracts list holds (`_listed_day`), reads
    the whole snapshot and the contracts active that day (`date`, `active=true`, up to 40
    pages, else a lower bound). Takes the snapshot's tickers whose last trade is in the
    calendar's today's session; asks the contracts list, by `ticker.any_of` and that `date`,
    for those its pages lacked (up to 20 requests of 100), and for those still lacking, by
    `ticker.any_of` and a `date` within the week before (up to 5). Asks the snapshot, by
    `ticker.any_of`, for the contracts list's tickers the snapshot lacked (up to 10). Where
    the requests cannot ask about every ticker, they ask about some spread evenly, and the
    evidence says what share (a lower bound). Records the counts and the tickers each list
    lacks.

    Passes when every request is answered and both lists hold a ticker; the counts are
    evidence. Sends at most 85 requests.
    """
    day = _listed_day(live_rest, evidence)
    listed = _read_snapshot(live_rest, evidence)
    contracts = _read_active_contracts(live_rest, day, evidence)
    assert listed, "the futures snapshot lists no ticker"
    assert contracts, f"the contracts list holds no contract active on {day}"
    _record_traded_but_unlisted(live_rest, listed, contracts, day=day, evidence=evidence)
    unsnapped = sorted(set(contracts) - set(listed))
    found, asked = _look_up(
        live_rest, _SNAPSHOT, unsnapped, query={"limit": _PRODUCTS_PAGE}, most=_SNAPSHOT_LOOKUPS
    )
    missing = sorted(set(asked) - found)
    recent = (day - _NEW_LISTING_DAYS).isoformat()
    new = {t for t in missing if str(contracts[t].get("first_trade_date")) >= recent}
    evidence(
        f"contracts active on {day} not in the snapshot read: {len(unsnapped)}; asked the "
        f"snapshot about {_share(asked, unsnapped)}: {len(found)} in it, {len(missing)} not, "
        f"{len(new)} of them first traded since {recent}, e.g. {missing[:40]}"
    )


_HORIZON_PRODUCTS = 250
"""The most products the horizon check asks the latest session of, a request each."""

_HORIZON_TRADED = timedelta(days=7)
"""How recent a ticker's last trade must be for its product to count as traded."""


_DAY_RANGES = ((-1, "past"), (3, "0 to 3"), (9, "4 to 9"), (30, "10 to 30"), (365, "31 to 365"))
"""The ranges of days ahead the horizon check counts products in, by their last day."""


def _days_range(days: int | None) -> str:
    """Name the range of `_DAY_RANGES` a count of days ahead falls in.

    Returns:
        Its name, `"over 365"` past them, or `"none"` for a product without a session.
    """
    if days is None:
        return "none"
    return next((name for last, name in _DAY_RANGES if days <= last), "over 365")


@pytest.mark.settles("135")
@pytest.mark.requests(256)
@pytest.mark.timeout(1200)
def test_schedule_horizon_per_traded_product(live_rest: RestClient, *, evidence: Evidence) -> None:
    """How many days ahead each traded futures product's schedule reaches.

    The calendar dates a row only by a session Massive has published, and loads sessions up
    to 10 days ahead. An earlier run found the latest session Massive holds for any product
    (in 2028), and read a window cut by its page cap, so how far each product's schedule
    reaches is unknown. Reads the whole snapshot, takes the product codes of the tickers
    traded within 7 days (those whose rows the calendar dates), and asks each product's
    latest session (`product_code`, `sort=session_end_date.desc`, `limit=1`): one request a
    product, every session it holds considered, up to 250 products spread evenly (beyond
    them the counts are a lower bound, and it says so). Records how many days ahead of the
    calendar's today each product's latest session is, counted in ranges, and every product
    whose latest session is fewer than 10 days ahead, or that has none. The sort names a
    column Massive does not document; an earlier run's same sort was answered, and a
    refusal fails the test with Massive's answer.

    Passes when every request is answered and a product was asked about; the days are
    evidence. Sends at most 256 requests.
    """
    listed = _read_snapshot(live_rest, evidence)
    since = epoch_ns(datetime.now(UTC) - _HORIZON_TRADED)
    traded = sorted(
        {found.product_code for found in listed.values() if (found.traded or 0) >= since}
    )
    asked = _spread(traded, _HORIZON_PRODUCTS)
    assert asked, "no futures ticker traded within 7 days"
    today = futures_session(datetime.now(UTC))  # the calendar's today
    ahead: dict[str, int | None] = {}
    for code in asked:
        query = {"product_code": code, "sort": "session_end_date.desc", "limit": 1}
        rows = _page(live_rest, _SCHEDULES, query=query)
        last = rows[0].get("session_end_date") if rows else None
        ahead[code] = (date.fromisoformat(last) - today).days if isinstance(last, str) else None
    counted = Counter(_days_range(days) for days in ahead.values())
    evidence(
        f"products traded within 7 days: {len(traded)}; asked about {_share(asked, traded)}; "
        f"days ahead of {today} of their latest session, by range: {dict(counted)}"
    )
    short = sorted(
        (code, days) for code, days in ahead.items() if days is None or days < _DAYS_AHEAD.days
    )
    evidence(f"products whose latest session is under 10 days ahead, or none: {short}")


_STREAM_SECONDS = 600.0
"""How long the streamed-tickers check streams every futures trade."""

_LOOKUP_CHUNK = 100
"""Tickers per contracts lookup, as the futures calendar asks."""

_LOOKUP_CHUNKS = 30
"""The most contracts lookups the streamed-tickers check sends."""

_LOOKUP_PAGES = 3
"""The most pages of one streamed-tickers contracts lookup: 100 tickers listed on each of a
week's trading days fill under one page of 1,000, so a third page means the cap cut it."""

_LOOKUP_DAYS = timedelta(days=7)
"""How far back the streamed-tickers check asks the contracts list for each contract's dates."""

_STREAM_SNAPSHOT_LOOKUPS = 30
"""The most snapshot lookups, 100 tickers each, the streamed-tickers check sends."""

_STREAM_CONTRACT_LOOKUPS = 10
"""The most contracts lookups by date, 100 tickers each, the streamed-tickers check sends."""


def _record_streamed_in_neither(
    client: RestClient, seen: Sequence[str], evidence: Evidence
) -> None:
    """Record the streamed tickers that neither the snapshot nor the contracts list holds.

    Asks the snapshot for every ticker seen, singles and combos alike, by `ticker.any_of`;
    then the contracts list, by `ticker.any_of` and the latest day it holds (`_listed_day`),
    for those the snapshot lacks.
    """
    query: dict[str, object] = {"limit": _PRODUCTS_PAGE}
    in_snapshot, asked = _look_up(
        client, _SNAPSHOT, seen, query=query, most=_STREAM_SNAPSHOT_LOOKUPS
    )
    unsnapped = sorted(set(asked) - in_snapshot)
    day = _listed_day(client, evidence)
    query = {"date": day, "limit": _LIST_PAGE}
    in_contracts, checked = _look_up(
        client, _CONTRACTS, unsnapped, query=query, most=_STREAM_CONTRACT_LOOKUPS
    )
    neither = sorted(set(checked) - in_contracts)
    evidence(
        f"asked the snapshot about {_share(asked, seen)} tickers seen: {len(in_snapshot)} in "
        f"it; asked the contracts list about {_share(checked, unsnapped)} it lacks: "
        f"{len(in_contracts)} listed on {day}, {len(neither)} in neither"
    )
    for start in range(0, len(neither), _LOOKUP_CHUNK):
        evidence(f"in neither: {neither[start : start + _LOOKUP_CHUNK]}")


def _record_unlisted_singles(
    client: RestClient, singles: Sequence[str], evidence: Evidence
) -> None:
    """Record the single contracts streamed that the contracts list lacks within a week.

    Looks them up 100 a lookup by `ticker.any_of`, the first `_LOOKUP_CHUNKS` lookups in
    sorted order, with the dates from `_LOOKUP_DAYS` before the calendar's today to it, each
    lookup to its end or `_LOOKUP_PAGES` pages.
    """
    chunks = [singles[i : i + _LOOKUP_CHUNK] for i in range(0, len(singles), _LOOKUP_CHUNK)]
    today = futures_session(datetime.now(UTC))  # the calendar's today
    dates = {"date.gte": today - _LOOKUP_DAYS, "date.lte": today, "limit": _LIST_PAGE}
    found: set[str] = set()
    cut = 0
    for chunk in chunks[:_LOOKUP_CHUNKS]:
        query = {**dates, "ticker.any_of": chunk}
        rows, _, ended = _read_pages(client, _CONTRACTS, query, most=_LOOKUP_PAGES)
        found |= {str(r.get("ticker")) for r in rows}
        cut += not ended
    looked_up = [t for chunk in chunks[:_LOOKUP_CHUNKS] for t in chunk]
    missing = sorted(set(looked_up) - found)
    capped = (
        f" ({cut} lookups cut at {_LOOKUP_PAGES} pages: some it lacks may be on pages not read)"
        if cut
        else ""
    )
    evidence(
        f"looked up {len(looked_up)} of {len(singles)} single contracts, dates "
        f"{dates['date.gte']} to {today}; the contracts list holds "
        f"{len(looked_up) - len(missing)}, lacks {len(missing)}{capped}"
    )
    for start in range(0, len(missing), _LOOKUP_CHUNK):
        evidence(f"not in the contracts list: {missing[start : start + _LOOKUP_CHUNK]}")


@pytest.mark.integration_all_symbols
@pytest.mark.settles("127", "134")
@pytest.mark.requests(135)
@pytest.mark.timeout(1500)
def test_streamed_futures_tickers_are_listed(
    live_session: dm.MassiveSession,
    live_rest: RestClient,
    *,
    table_rows: TableRows,
    skip_if_not_entitled: Callable[[dm.MassiveSession, str], None],
    evidence: Evidence,
) -> None:
    """Whether the contracts list or the snapshot holds every futures ticker that trades.

    Streams every futures trade (`m.futures.trades(dm.ALL).last_by()`) on the key's WebSocket
    for 10 minutes, which takes over the key's own futures connection the whole time. Then
    looks up the single contract tickers seen in the contracts list, 100 a lookup by
    `ticker.any_of`, the largest page, and the dates from 7 days before the calendar's today to
    it (`date.gte`, `date.lte`): the contracts list holds one row per contract and trading day,
    and a lookup without a date reads every date of every contract, whose first page leaves
    most contracts out. Each lookup reads every page, up to 3 (a lookup cut there says some
    tickers it lacks may be on pages not read), at most 30 lookups: the first 3,000 in sorted
    order. Records the tickers seen, singles and combos, how many were looked up, and every
    one looked up that the list does not hold within the week. Then asks the snapshot for every
    ticker seen, singles and combos alike (`ticker.any_of`, 100 a request, up to 30), and the
    contracts list, by `ticker.any_of` and the latest day it holds, for those the snapshot
    lacks (up to 10, after up to 4 requests to find that day); where they cannot ask about
    every ticker, they ask about some spread evenly, and the evidence says what share (a
    lower bound). Records every ticker in neither: the ones the calendar could not date. Means
    most on a Monday that lists many contracts. Skips at once when Massive's futures market
    status for ES is not `open`, and with Massive's refusal when the stream is refused.

    Passes when trades arrived, the stream never failed, and every lookup is answered; the
    missing tickers are evidence. Sends at most 135 requests of its own: the market status
    and the lookups. The stream's own futures calendar also looks up the contracts it sees,
    100 a request, and loads their products' schedules, on the same futures rate limit;
    their number depends on how many contracts trade, and the timeout leaves 10 minutes for
    them and the lookups.
    """
    market = _page(live_rest, _MARKET_STATUS, query={"product_code": "ES"})
    status = str(market[0].get("market_event")) if market else "unknown"
    evidence(f"futures market status (ES): {status}")
    if status != "open":
        pytest.skip(f"the futures market is not open: Massive says {status!r}")
    board = live_session.futures.trades(dm.ALL).last_by()
    try:
        pause = threading.Event()  # never set; only paces the polling
        deadline = time.monotonic() + _STREAM_SECONDS
        while time.monotonic() < deadline:
            if any(r["State"] == "FAILED" for r in table_rows(live_session.streams())):
                break
            pause.wait(5.0)
        seen = sorted({str(r["Symbol"]) for r in table_rows(board)})
        for row in table_rows(live_session.streams()):
            evidence(f"stream {row['Dataset']}: {row['State']}, error {row['Error']!r}")
        skip_if_not_entitled(live_session, "the futures trades stream")
        failed = [
            row["Error"] for row in table_rows(live_session.streams()) if row["State"] == "FAILED"
        ]
        assert not failed, f"the stream failed before its {_STREAM_SECONDS:.0f} s: {failed}"
    finally:
        live_session.release(board)
    singles = [t for t in seen if is_futures_contract(t)]
    evidence(
        f"tickers seen in {_STREAM_SECONDS:.0f} s: {len(seen)}, {len(singles)} single contracts"
    )
    _record_unlisted_singles(live_rest, singles, evidence)
    _record_streamed_in_neither(live_rest, seen, evidence)
    assert seen, f"no futures trade arrived in {_STREAM_SECONDS:.0f} s, though trades were due"


_TAS_UNDERLYINGS = {
    "BZT": "BZ",
    "CLT": "CL",
    "GFT": "GF",
    "HET": "HE",
    "HG0": "HG",
    "HGT": "HG",
    "HOT": "HO",
    "KET": "KE",
    "LET": "LE",
    "NGT": "NG",
    "PAT": "PA",
    "PLT": "PL",
    "RBT": "RB",
    "SBT": "ZS",
    "SIT": "SI",
    "ZCT": "ZC",
    "ZLT": "ZL",
    "ZMT": "ZM",
    "ZWT": "ZW",
}
"""The products two runs found traded within 7 days with no schedule, each with the product of
the futures contract it settles to. CME's contract specifications name `CLT`, `HGT`, and `HG0`
as Trade at Settlement (TAS) products; the others are taken to be TAS products from CME notices
and filings."""

_UNLISTED_STREAMED = ("6E:XF:EURUSD:Z6", "SR1H7-ZQH7", "0BTZ9", "0NANHZ601")
"""Tickers a run streamed that neither the snapshot nor the contracts list held: a CME FX Link
spread, a 1-Month SOFR against 30-Day Fed Funds spread, and two single contracts."""

_SPACED_COMBO = "CL:BZ F7-G7"
"""A combo whose ticker holds `:` and a space, for which a run read no trades."""

_DASHED_COMBO = "CLZ7-CLM8"
"""A combo whose ticker holds neither, traded the same second, for which that run read 10."""


def _record_body(
    client: RestClient,
    label: str,
    path: str,
    *,
    query: Mapping[str, object],
    evidence: Evidence,
) -> dict[str, Any] | None:
    """Read one page of a futures path and record its whole body, or Massive's refusal.

    The query's `ticker` fills a path's `{ticker}`, which the REST client percent-encodes.

    Returns:
        The body, or None when Massive refused the request (`_refused`).

    Raises:
        WireError: When the read fails other than by a refusal.
    """
    path_params = {"ticker": query["ticker"]} if "{ticker}" in path else {}
    rest = {key: value for key, value in query.items() if key not in path_params}
    try:
        page = client.get(source(Transport.REST, path), path_params, rest, bucket="futures")
    except WireError as error:
        if not _refused(error):
            raise
        evidence(f"{label}: refused: {error}")
        return None
    body = page.body if isinstance(page.body, dict) else {"body": page.body}
    evidence(f"{label}: {json.dumps(body, default=str)}")
    return body


def _record_first_singles(
    client: RestClient, codes: Sequence[str], *, day: date, evidence: Evidence
) -> None:
    """Record the first single contract a day's contracts list holds for each product.

    Asks for the products' single contracts on `day`, up to 2 pages, and records how many
    rows each product had and its first row whole.
    """
    query = {"product_code.any_of": list(codes), "date": day, "type": "single", "limit": _LIST_PAGE}
    rows, pages, ended = _read_pages(client, _CONTRACTS, query, most=2)
    firsts: dict[str, dict[str, Any]] = {}
    for row in rows:
        firsts.setdefault(str(row.get("product_code")), row)
    evidence(
        f"single contracts of {list(codes)} on {day}: {len(rows)} rows in {pages} pages "
        f"({'the whole list' if ended else 'more follow: a lower bound'}), by product "
        f"{dict(Counter(str(row.get('product_code')) for row in rows))}"
    )
    for code in codes:
        evidence(f"first single contract of {code}: {json.dumps(firsts.get(code), default=str)}")


@pytest.mark.settles("134", "135", "136", "137")
@pytest.mark.requests(15)
@pytest.mark.timeout(300)
def test_response_bodies_of_futures_gaps(live_rest: RestClient, *, evidence: Evidence) -> None:
    """Record Massive's whole answers where earlier runs found futures data missing.

    Earlier runs recorded counts only: products with no schedule, taken to be CME Trade at
    Settlement (TAS) products; streamed tickers that neither the snapshot nor the contracts
    list held; and a combo whose ticker holds `:` and a space, whose trades path read nothing.
    This records the bodies themselves, to quote to Massive. Asks the schedules for the latest
    session of `CLT` and of its underlying `CL` (`sort=session_end_date.desc`, `limit=1`); the
    products records of the 19 TAS products and their 18 underlyings, in one request; the
    contracts active on the latest day the contracts list holds for the TAS products, and the
    first such single contract of each underlying (up to 2 pages); the snapshot and that day's
    contracts for two of the streamed tickers in neither, an FX Link spread and a SOFR against
    Fed Funds spread, with the two single contracts beside them; and the snapshot and the
    latest 10 trades of `CL:BZ F7-G7` and of `CLZ7-CLM8`, each ticker as the library sends it.
    Records each body whole, or Massive's refusal; for the underlyings' contracts, the first
    row of each product and the count of rows.

    Passes when every request is answered or refused; the bodies are evidence. Sends at most
    15 requests: up to 4 to find the contracts list's latest day, and 11 more.

    Raises:
        WireError: When a read fails other than by a refusal.
    """
    for code in ("CLT", "CL"):
        schedules = {"product_code": code, "sort": "session_end_date.desc", "limit": 1}
        _record_body(
            live_rest, f"schedules of {code}", _SCHEDULES, query=schedules, evidence=evidence
        )
    tas = sorted(_TAS_UNDERLYINGS)
    underlyings = sorted(set(_TAS_UNDERLYINGS.values()))
    products: dict[str, object] = {"product_code.any_of": tas + underlyings, "limit": 100}
    _record_body(live_rest, "products", _PRODUCTS, query=products, evidence=evidence)
    day = _listed_day(live_rest, evidence)
    contracts: dict[str, object] = {"product_code.any_of": tas, "date": day, "limit": _LIST_PAGE}
    _record_body(
        live_rest, "contracts of the TAS products", _CONTRACTS, query=contracts, evidence=evidence
    )
    _record_first_singles(live_rest, underlyings, day=day, evidence=evidence)
    unlisted: dict[str, object] = {"ticker.any_of": list(_UNLISTED_STREAMED), "limit": _LIST_PAGE}
    label = f"of {list(_UNLISTED_STREAMED)}"
    _record_body(live_rest, f"snapshot {label}", _SNAPSHOT, query=unlisted, evidence=evidence)
    listed = {**unlisted, "date": day}
    _record_body(
        live_rest, f"contracts {label} on {day}", _CONTRACTS, query=listed, evidence=evidence
    )
    combos = [_SPACED_COMBO, _DASHED_COMBO]
    snapshot: dict[str, object] = {"ticker.any_of": combos}
    _record_body(live_rest, f"snapshot of {combos}", _SNAPSHOT, query=snapshot, evidence=evidence)
    for ticker in combos:
        trades = {"ticker": ticker, "sort": "timestamp.desc", "limit": 10}
        label = f"trades of {ticker!r}, path {quote(ticker, safe='')}"
        _record_body(live_rest, label, _TRADES, query=trades, evidence=evidence)
