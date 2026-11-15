"""Live checks of what the futures session calendar reads from Massive: products and schedules.

Streamed futures rows carry no session date, so the library dates them from Massive's futures
schedules, after finding each contract's product in the contracts list. These tests record what
that design depends on and Massive's docs leave open: whether combos have a product and a
schedule, how many products, contracts, and schedule events there are, how long the lookups
take, how often contracts are listed, whether every streamed ticker is listed, and whether any
trade falls outside every session. Each test sends REST requests only and records what Massive
sent (`evidence`), except the streamed-tickers test, which streams every futures trade on the
key's WebSocket for 10 minutes: it is marked `integration_all_symbols`, so `-m integration`
leaves it out and `-m integration_all_symbols` runs it. Every test is marked `settles`: its
evidence, not a pass, answers its question. A request Massive refuses for want of entitlement
skips the test with Massive's refusal. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

import json
import re
import statistics
import threading
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm
from deephaven_massive._datasets.epoch import epoch_ns
from deephaven_massive._datasets.futures_sessions import futures_session
from deephaven_massive._datasets.symbols import is_futures_contract
from deephaven_massive._massive import (
    RestClient,
    Transport,
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
    client: RestClient, path: str, query: Mapping[str, object], *, most: int
) -> tuple[list[dict[str, Any]], int, bool]:
    """Read up to `most` pages of a futures list, following each page's `next_url`.

    Returns:
        The rows, the pages read, and whether the list ended within them (the last page read
        has no `next_url`).
    """
    rows: list[dict[str, Any]] = []
    read = 0
    for page in client.pages(source(Transport.REST, path), {}, query, bucket="futures"):
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
    client: RestClient, evidence: Evidence
) -> tuple[list[_Combo], list[_Combo], dict[str, str]]:
    """Read the snapshot of each of `_COMBO_ROOTS` and take its recently traded combos.

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
        picked = _one_per_form(recent, _COMBOS_PER_ROOT)
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
    combos, latest, products_of = _read_combos(live_rest, evidence)
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


def _timed(client: RestClient, path: str, query: Mapping[str, object]) -> tuple[list[float], int]:
    """Time one request `_LATENCY_RUNS` times.

    Returns:
        The seconds of each run, and the rows of the last.
    """
    seconds: list[float] = []
    rows: list[dict[str, Any]] = []
    for _ in range(_LATENCY_RUNS):
        start = time.perf_counter()
        rows = _page(client, path, query=query)
        seconds.append(time.perf_counter() - start)
    return seconds, len(rows)


@pytest.mark.settles("125")
@pytest.mark.requests(27)
@pytest.mark.timeout(400)
def test_contract_and_schedule_lookup_latency(
    live_rest: RestClient, *, recent_monday: date, evidence: Evidence
) -> None:
    """How long the calendar's two lookups take: a contract's product, then its schedule.

    Reads one page of the single contracts active on the most recent published Monday for
    their tickers, and one page of the single products listed that day for their codes. Then
    times, five times each, the calendar's contract
    lookup (`ticker.any_of`, the largest page) for 1 and for 100 tickers, and its schedule
    lookup (`product_code.any_of` over its window, the largest page) for 1, 20, and 100
    products. Records each lookup's seconds, median, and slowest, with its rows. A time
    includes any wait for the key's rate limit, which the evidence's wall times show.

    Passes when every request is answered; the times are evidence. Sends 27 requests.
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
    for label, path, lookup in lookups:
        seconds, rows = _timed(live_rest, path, lookup)
        evidence(
            f"{label}: {rows} rows, seconds {[round(s, 3) for s in seconds]}, median "
            f"{statistics.median(seconds):.3f}, slowest {max(seconds):.3f}"
        )


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
    from it until the next update. Reads, as of the most recent published Monday, the
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
    daily close to the next pre-open, or the weekend). Records, per product, the gap, the
    trades in it (a page of 1,000 at most, marked when full), Massive's `session_end_date` of
    those trades beside the next session's, which the calendar would give them, and the first
    few; then the total. A pair of sessions that touch has no gap and is recorded as such.

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


_STREAM_SECONDS = 600.0
"""How long the streamed-tickers check streams every futures trade."""

_LOOKUP_CHUNK = 100
"""Tickers per contracts lookup, as the futures calendar asks."""

_LOOKUP_CHUNKS = 30
"""The most contracts lookups the streamed-tickers check sends."""


@pytest.mark.integration_all_symbols
@pytest.mark.settles("127")
@pytest.mark.requests(31)
@pytest.mark.timeout(1200)
def test_streamed_futures_tickers_are_listed(
    live_session: dm.MassiveSession,
    live_rest: RestClient,
    *,
    table_rows: TableRows,
    rows_due: Callable[..., tuple[str, bool]],
    skip_if_not_entitled: Callable[[dm.MassiveSession, str], None],
    evidence: Evidence,
) -> None:
    """Whether the contracts list holds every single futures contract that trades.

    Streams every futures trade (`m.futures.trades(dm.ALL).last_by()`) on the key's WebSocket
    for 10 minutes, which takes over the key's own futures connection the whole time. Then
    looks up every single contract ticker seen in the contracts list, 100 a request as the
    calendar asks (`ticker.any_of`, the largest page), at most 30 requests. Records the
    tickers seen, singles and combos, and every single contract the list does not hold.
    Skips at once when no futures trade is due (the market is closed), and with Massive's
    refusal when the stream is refused.

    Passes when trades arrived and every lookup is answered; the missing tickers are
    evidence. Sends at most 31 requests of its own: the market status and the lookups. The
    stream's own futures calendar also looks up the contracts it sees, 100 a request, and
    loads their products' schedules, on the same futures rate limit; their number depends on
    how many contracts trade, and the timeout leaves 10 minutes for them and the lookups.
    """
    status, due = rows_due("futures", "trades")
    if not due:
        pytest.skip(f"no futures trade is due: the market is {status}")
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
    finally:
        live_session.release(board)
    singles = [t for t in seen if is_futures_contract(t)]
    evidence(
        f"tickers seen in {_STREAM_SECONDS:.0f} s: {len(seen)}, {len(singles)} single contracts"
    )
    chunks = [singles[i : i + _LOOKUP_CHUNK] for i in range(0, len(singles), _LOOKUP_CHUNK)]
    found: set[str] = set()
    for chunk in chunks[:_LOOKUP_CHUNKS]:
        query = {"ticker.any_of": chunk, "limit": _LIST_PAGE}
        found |= {str(r.get("ticker")) for r in _page(live_rest, _CONTRACTS, query=query)}
    looked_up = [t for chunk in chunks[:_LOOKUP_CHUNKS] for t in chunk]
    missing = sorted(set(looked_up) - found)
    evidence(
        f"looked up {len(looked_up)} of {len(singles)} single contracts; the contracts list "
        f"holds {len(looked_up) - len(missing)}, lacks {len(missing)}: {missing[:50]}"
    )
    assert seen, f"no futures trade arrived in {_STREAM_SECONDS:.0f} s, though trades were due"
