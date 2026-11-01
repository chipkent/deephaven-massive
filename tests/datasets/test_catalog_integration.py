"""The live smoke tier: every dataset of the catalog, through every verb it offers, once.

Each test builds one dataset's query through the public `dm` API with one symbol of its class,
runs one verb, and asserts that the call worked and that the result has exactly the dataset's
columns. No human reads anything:

- `fetch()`: one call, with `limit` 5 where the dataset takes a limit; an empty result passes.
- `history()`: the last trading day before today, holidays skipped; whole days for bars, a
  five-minute window (15:00 to 15:05 New York time) for ticks; an empty result passes.
- `poll()`: one case only, on `reference.market_status`, which belongs to no subscription and so
  is never paced: it passes once two cycles have published rows, within 30 s. Every other
  dataset served by REST alone is already called by its `fetch()` case, the same request.
- `append_only()`: it must reach Massive's acknowledgement (up to 15 s), then rows, whenever
  rows must flow: while the stream's market is open for its kind of rows (stock trades and
  quotes in the extended hours too) and has been open longer than the plan's feed delay
  (`streams`). A minute bar stream waits until just past the next minute boundary. When rows
  need not flow, the case skips after the acknowledgement, "not exercised: market closed
  (acknowledged)" or the like, so a pass always means rows reached the table.

A dataset the key's plan does not include is a skip, "not entitled: needs <plan>", never a
failure. Each case also notes every WARNING the library logged and every `SchemaDrift` the
session's `streams()` and `connections()` show; a pass with any of them is counted "passed with
drift" in the coverage that ends the terminal summary (passed, not entitled, otherwise skipped,
failed), which lists each one and groups the skips by the plan or the reason.

The cases run round-robin across the classes, so a class on Basic waits for its pace while the
next class's case runs. The tier is marked `integration_smoke` (and `integration`) and runs only
with `uv run pytest -m integration_smoke`. On a free key it stays within Massive's rate limits:
every REST request to a class on Basic waits for a place in that class's 5 a minute, each case's
time limit grows with its request count, and the streams of a Basic class (Basic has no
WebSocket access) skip before any call, naming the Starter plan they need. How to run it is in
`docs/live-tests.md`.
"""

from __future__ import annotations

import inspect
import itertools
import logging
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, Protocol, cast
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm
from deephaven_massive._enums import ClassFromPrefix
from deephaven_massive._massive import RestClient, Transport, source
from deephaven_massive._session import CATALOG
from tests._live import streams

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from datetime import date
    from typing import TypeAlias

    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._query import QueryBase

pytestmark = [pytest.mark.integration, pytest.mark.integration_smoke]

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Evidence: TypeAlias = "Callable[[str], None]"
SkipIfNotEntitled: TypeAlias = "Callable[[dm.MassiveSession, str], None]"
RowsDue: TypeAlias = "Callable[[str, str], tuple[str, bool]]"

_NEW_YORK = ZoneInfo("America/New_York")
_POLL_DATASET: Final = "reference.market_status"
"""The one dataset the tier polls: it belongs to no subscription, so no plan paces it."""

_POLL_INTERVAL = 5.0
"""Seconds between the poll case's cycles."""

_POLL_WAIT = 30.0
"""Seconds the poll case waits for its two cycles."""

_STREAM_WAIT = 15.0
"""Seconds a stream case waits for Massive's acknowledgement."""

_TICK_WAIT = 45.0
"""Seconds a stream case of ticks or values waits for its first row once rows must flow."""

_STREAM_GAP = 3.0
"""Seconds between one stream test's session closing and the next one connecting."""

_LIMIT = 5

_SUBSCRIPTIONS = {
    dm.AssetClass.STOCKS: "stocks",
    dm.AssetClass.OPTIONS: "options",
    dm.AssetClass.INDICES: "indices",
    dm.AssetClass.FOREX: "currencies",
    dm.AssetClass.CRYPTO: "currencies",
    dm.AssetClass.FUTURES: "futures",
}
_MARKETS = {
    dm.AssetClass.STOCKS: "stocks",
    dm.AssetClass.OPTIONS: "options",
    dm.AssetClass.INDICES: "indices",
    dm.AssetClass.FOREX: "fx",
    dm.AssetClass.CRYPTO: "crypto",
    dm.AssetClass.FUTURES: "futures",
}
"""The market whose status says whether a class's stream must deliver rows."""

_PLAN_NAMES = {
    "stocks": "a Stocks plan",
    "options": "an Options plan",
    "indices": "an Indices plan",
    "currencies": "a Currencies plan",
    "futures": "a Futures plan",
}
_SYMBOLS: dict[dm.AssetClass | ClassFromPrefix, str] = {
    dm.AssetClass.STOCKS: "AAPL",
    dm.AssetClass.FOREX: "C:EURUSD",
    dm.AssetClass.CRYPTO: "X:BTCUSD",
}
_ETF = "SPY"
"""The symbol of the ETF Global datasets, which describe funds."""

_EVERY_SYMBOL: Final = frozenset({"stocks.luld"})
"""The streams read for every symbol (`dm.ALL`): one stock's LULD bands change too rarely for
a row to arrive in a test's time, while some band of the market's changes every few seconds."""

_RARE: Final = {
    "stocks.imbalances": "imbalances arrive only around the opening and closing auctions",
}
"""The streams whose rows no test can wait for, with why: they pass only by skipping after
the acknowledgement."""

_FIXED_ARGUMENTS: dict[str, object] = {
    "direction": dm.Direction.GAINERS,
    "window": 10,
    "underlying": "AAPL",
    "from_currency": "EUR",
    "to_currency": "USD",
    "short_window": 12,
    "long_window": 26,
    "signal_window": 9,
}
"""Values of the required arguments other than symbols, timespans, and dates."""

_STREAM_CLOSED = [float("-inf")]
"""When the last stream test's session closed."""

_FOUND: dict[str, str] = {}
"""Symbols looked up once per run: an options contract."""

_REQUESTS: Final = {"fetch": 2, "poll": 1, "history": 4, "stream": 2}
"""The REST requests one case makes to one class at most, by verb: an option contract's lookup
and the call, the pages of one day's history, and a stream's host decision and market status."""


def _class_key(name: str) -> str:
    """Return the class a dataset's requests count against, for the round-robin order.

    Returns:
        Its subscription, such as `"stocks"`, or `"other"` for data of no single class.
    """
    return _subscription(CATALOG.by_name[name]) or "other"


def _cases() -> list[tuple[str, str]]:
    """Return every (dataset, verb) pair the tier runs, round-robin across the classes.

    Every dataset runs `fetch()`, `history()`, and its stream where it offers them; only
    `_POLL_DATASET` runs `poll()`. The pairs of each class keep the catalog's order, and the
    classes take turns, so a class on Basic, paced at 5 requests a minute, never runs two
    cases in a row while another class has cases left.

    Returns:
        The pairs.
    """
    by_class: dict[str, list[tuple[str, str]]] = {}
    for name, spec in CATALOG.by_name.items():
        verbs = [
            verb
            for verb, offered in (
                ("fetch", spec.has_fetch),
                ("poll", name == _POLL_DATASET),
                ("history", spec.has_history),
                ("stream", spec.has_channels),
            )
            if offered
        ]
        by_class.setdefault(_class_key(name), []).extend((name, verb) for verb in verbs)
    turns = itertools.zip_longest(*by_class.values())
    return [case for turn in turns for case in turn if case is not None]


def _params() -> list[Any]:
    """Return every (dataset, verb) pair as a test case with its plan and request count.

    A dataset of one class needs that class's Basic plan for REST and its Starter plan for a
    stream (Basic has no WebSocket access), and its Business plan for a fair-market-value (FMV)
    stream, which only Business serves; the `needs` marker skips it before any call when
    the key lacks the plan, and lets the timeout grow with the `requests` marker on a class
    left on Basic, whose five requests a minute pace it. A stream case gets 160 s: its
    acknowledgement, the wait for its rows (up to 75 s for a minute bar), and the gap between
    two stream cases.

    Returns:
        One `pytest.param` per pair, with its id `<dataset>-<verb>`.
    """
    params = []
    for name, verb in _cases():
        spec = CATALOG.by_name[name]
        subscription = _subscription(spec)
        marks = [pytest.mark.requests(_REQUESTS[verb])]
        if verb == "stream":
            marks.append(pytest.mark.timeout(160))
        if subscription is not None:
            plan = "starter" if verb == "stream" else "basic"
            if name.endswith(".fmv") and verb == "stream":
                plan = "business"
            marks.append(pytest.mark.needs(f"{subscription}:{plan}"))
        params.append(pytest.param(name, verb, marks=marks, id=f"{name}-{verb}"))
    return params


def _subscription(spec: DatasetSpec) -> str | None:
    """Return the subscription a dataset belongs to, or None for data of no single class.

    Returns:
        `"stocks"`, `"options"`, `"indices"`, `"currencies"`, `"futures"`, or None.
    """
    return None if spec.asset_class is ClassFromPrefix.ANY else _SUBSCRIPTIONS[spec.asset_class]


def _plan(name: str, spec: DatasetSpec, verb: str, *, flat_files: bool = False) -> str:
    """Name the plan a dataset's verb needs, for the "not entitled" skip and its group.

    Args:
        name: The dataset.
        spec: Its catalog entry.
        verb: The verb.
        flat_files: Whether the refusal came from the S3 flat files rather than REST.

    Returns:
        Such as `"a Stocks plan with WebSocket access"`, or `"a Stocks plan with flat-file
        access"` for a `history()` the flat files refused.
    """
    if name.startswith("partners."):
        return f"the {name.split('.')[1]} partner add-on"
    if name.endswith(".fmv"):
        return "a Business plan (FMV)"
    subscription = _subscription(spec)
    plan = "a plan" if subscription is None else _PLAN_NAMES[subscription]
    if verb == "stream":
        return f"{plan} with WebSocket access"
    return f"{plan} with flat-file access" if flat_files else f"{plan} with REST access"


def _option(client: RestClient) -> str:
    """Return the AAPL call nearest the money a month or more out, looked up once per run.

    Reads AAPL's previous close (its `previous_day_bar`), then the listed calls expiring 30
    days or more from now with a strike within 10% of that close, and takes, at the nearest of
    those expirations, the strike nearest the close: the contract most likely to trade while
    a stream case listens.

    Returns:
        Its OCC symbol, such as `"O:AAPL261218C00200000"`.
    """
    if "options" not in _FOUND:
        bars = client.get(
            source(Transport.REST, "/v2/aggs/ticker/{stocksTicker}/prev"),
            {"stocksTicker": "AAPL"},
            {},
            bucket="stocks",
        ).rows
        assert bars, "no previous AAPL bar"
        close = float(str(bars[0]["c"]))
        later = datetime.now(UTC).date() + timedelta(days=30)
        rows = client.get(
            source(Transport.REST, "/v3/reference/options/contracts"),
            {},
            {
                "underlying_ticker": "AAPL",
                "contract_type": "call",
                "expiration_date.gte": later,
                "strike_price.gte": round(close * 0.9, 2),
                "strike_price.lte": round(close * 1.1, 2),
                "sort": "expiration_date",
                "order": "asc",
                "limit": 250,
            },
            bucket="options",
        ).rows
        assert rows, f"no listed AAPL call near {close}"
        expiry = min(str(r["expiration_date"]) for r in rows)
        nearest = min(
            (r for r in rows if str(r["expiration_date"]) == expiry),
            key=lambda r: abs(float(str(r["strike_price"])) - close),
        )
        _FOUND["options"] = str(nearest["ticker"])
    return _FOUND["options"]


def _symbol(name: str, spec: DatasetSpec, client: RestClient, *, symbols: Mapping[Any, str]) -> str:
    """Return the one symbol a dataset is called with.

    Returns:
        The symbol of its class (an ETF for the ETF Global datasets, AAPL for data of no class).
    """
    if name.startswith("partners.etf_global."):
        return _ETF
    if spec.asset_class is dm.AssetClass.OPTIONS:
        return _option(client)
    return symbols.get(spec.asset_class, "AAPL")


def _daily_unit(spec: DatasetSpec) -> dm.Timespan:
    """Return the timespan of one bar per trading day for a bars dataset.

    Futures bars have no `Timespan.DAY`: Massive's futures aggregates endpoint has a
    trading-session unit instead, and `bars()` refuses `DAY` for futures and `SESSION` for every
    other class. The catalog records no timespans, so the choice follows that asset-class rule.

    Returns:
        `Timespan.SESSION` for futures bars, `Timespan.DAY` for every other class.
    """
    return dm.Timespan.SESSION if spec.asset_class is dm.AssetClass.FUTURES else dm.Timespan.DAY


def _query(
    session: dm.MassiveSession,
    name: str,
    verb: str,
    *,
    client: RestClient,
    symbols: Mapping[Any, str],
    day: date,
) -> QueryBase:
    """Build a dataset's query for one verb, with one symbol and the fewest rows.

    Every required argument gets a value (`_FIXED_ARGUMENTS`, `day` for a `date`); `symbols`
    gets the class's symbol even where it is optional (every symbol, `dm.ALL`, for the streams
    of `_EVERY_SYMBOL`), and `limit` 5 where the dataset takes one. A bars dataset, whose
    `timespan` is required, uses `Timespan.MINUTE` for a stream (the one-minute channel every
    bars dataset offers) and its daily unit for every other verb (the previous day's bar for
    `fetch()`, one daily bar for `history()`): see `_daily_unit`.

    Returns:
        The query.

    Raises:
        AssertionError: If the method needs an argument this module has no value for.
    """
    spec = CATALOG.by_name[name]
    target: Any = session
    for part in name.split("."):  # every dataset method is at its name's path
        target = getattr(target, part)
    method: Callable[..., QueryBase] = target
    arguments: dict[str, object] = {}
    for parameter in inspect.signature(method).parameters.values():
        if parameter.name == "symbols" and verb == "stream" and name in _EVERY_SYMBOL:
            arguments["symbols"] = dm.ALL
        elif parameter.name == "symbols":
            arguments["symbols"] = _symbol(name, spec, client, symbols=symbols)
        elif parameter.name == "timespan":
            arguments["timespan"] = dm.Timespan.MINUTE if verb == "stream" else _daily_unit(spec)
        elif parameter.name == "limit" and verb in {"fetch", "poll"}:
            arguments["limit"] = _LIMIT
        elif parameter.default is not inspect.Parameter.empty:
            continue
        elif parameter.name == "date":
            arguments["date"] = day
        elif parameter.name in _FIXED_ARGUMENTS:
            arguments[parameter.name] = _FIXED_ARGUMENTS[parameter.name]
        else:
            msg = f"{name}: no value for its required argument {parameter.name!r}"
            raise AssertionError(msg)
    return method(**arguments)


class _Fetches(Protocol):
    """A query with `fetch()`."""

    def fetch(self) -> Table:
        """Return the dataset's current rows."""
        ...


class _Histories(Protocol):
    """A query with `history()`."""

    def history(self, start: date | datetime, end: date | datetime) -> Table:
        """Return the dataset's rows over a range."""
        ...


def _run(query: QueryBase, verb: str, *, bars: bool, day: date) -> Table:
    """Run one fetch or history verb on a query that offers it.

    Returns:
        The static result: for `history()`, the whole day for bars and 15:00 to 15:05 New York
        time for ticks.
    """
    if verb == "fetch":
        return cast("_Fetches", query).fetch()
    histories = cast("_Histories", query)
    if bars:
        return histories.history(day, day)
    start = datetime(day.year, day.month, day.day, 15, tzinfo=_NEW_YORK)
    return histories.history(start, start + timedelta(minutes=5))


@dataclass(frozen=True)
class _Case:
    """One smoke case's dataset and the fixtures its verb's run needs.

    Attributes:
        session: The live session.
        name: The dataset.
        spec: Its catalog entry.
        plan: The plan it needs (`_plan`), named when the plan refuses it.
        table_rows: Reads a table's rows.
        caplog: The captured log records, for the library's warnings.
        skip_if_not_entitled: Skips the case when a stream of the session was refused.
        evidence: Records a line of evidence.
    """

    session: dm.MassiveSession
    name: str
    spec: DatasetSpec
    plan: str
    table_rows: TableRows
    caplog: pytest.LogCaptureFixture
    skip_if_not_entitled: SkipIfNotEntitled
    evidence: Evidence

    def drift(self) -> list[str]:
        """Return what the case saw drift: the library's warnings and any `SchemaDrift`.

        Returns:
            One line per WARNING or worse a `deephaven_massive` logger emitted during the
            case, and per non-empty `SchemaDrift` of the session's streams and connections.
        """
        found = [
            f"{record.levelname} {record.name}: {record.getMessage()}"
            for record in self.caplog.records
            if record.levelno >= logging.WARNING and record.name.startswith("deephaven_massive")
        ]
        for view in (self.session.streams(), self.session.connections()):
            found.extend(
                f"SchemaDrift {r['SchemaDrift']}" for r in self.table_rows(view) if r["SchemaDrift"]
            )
        return found

    def failure(self) -> str | None:
        """Return the error of the session's first failed stream, or None.

        Returns:
            The `Error` of the first `FAILED` row of `streams()`.
        """
        failed = [r for r in self.table_rows(self.session.streams()) if r["State"] == "FAILED"]
        return str(failed[0]["Error"]) if failed else None


def _call(case: _Case, query: QueryBase, verb: str, *, day: date) -> tuple[list[str], list[str]]:
    """Run a `fetch()` or `history()` case; a refusal or missing S3 credentials skip it.

    Returns:
        The result's columns and what the case saw drift.

    Raises:
        ValueError: When the verb refuses its arguments for any reason but missing S3
            credentials.
    """
    try:
        table = _run(query, verb, bars=case.spec.is_bars, day=day)
    except dm.EntitlementError as error:
        plan = _plan(case.name, case.spec, verb, flat_files="flat files" in str(error))
        _PLAN_OF[case.name, verb] = plan
        pytest.skip(f"not entitled: needs {plan} ({case.name}: {error})")
    except ValueError as error:
        if "s3" not in str(error).lower():
            raise
        pytest.skip(f"needs S3 credentials for flat files ({error})")
    columns = list(table.column_names)
    case.evidence(f"{case.name} {verb}: {table.size} rows; columns {columns}")
    return columns, case.drift()


def _poll_case(case: _Case, query: QueryBase) -> tuple[list[str], list[str]]:
    """Run the poll case: it passes once two cycles have published rows.

    A cycle shows as a new `TimestampInsert`: every row of a cycle carries that cycle's.

    Returns:
        The table's columns and what the case saw drift.
    """
    table = cast("Any", query).poll(_POLL_INTERVAL)
    cycles: set[object] = set()
    pause = threading.Event()  # never set; only paces the polling
    deadline = time.monotonic() + _POLL_WAIT
    while len(cycles) < 2 and time.monotonic() < deadline and case.failure() is None:
        cycles.update(row["TimestampInsert"] for row in case.table_rows(table))
        pause.wait(0.5)
    failure, drift, columns = case.failure(), case.drift(), list(table.column_names)
    case.session.release(table)
    case.evidence(f"{case.name} poll: {len(cycles)} cycles of rows; columns {columns}")
    case.skip_if_not_entitled(case.session, case.plan)
    assert failure is None, f"{case.name} poll failed: {failure}"
    assert len(cycles) >= 2, f"{case.name}: {len(cycles)} poll cycles in {_POLL_WAIT:.0f} s"
    return columns, drift


def _stream_case(
    case: _Case,
    query: QueryBase,
    *,
    wait_acked: Callable[[dm.MassiveSession, int, float], int],
    wait_rows: WaitRows,
    rows_due: RowsDue,
) -> tuple[list[str], list[str]]:
    """Run a stream case: Massive must acknowledge it, and rows must arrive when they are due.

    Rows are due while the stream's market is open for its kind of rows and has been open
    longer than the plan's feed delay (`rows_due`), unless the stream is one of `_RARE`. A
    minute bar stream waits until just past the next minute boundary, any other for
    `_TICK_WAIT`. When no rows are due, the case skips after the acknowledgement.

    Returns:
        The table's columns and what the case saw drift.
    """
    table = cast("Any", query).append_only()
    acked = wait_acked(case.session, 1, time.monotonic() + _STREAM_WAIT)
    family = case.name.rpartition(".")[2]
    status, due = rows_due(_MARKETS[cast("dm.AssetClass", case.spec.asset_class)], family)
    due = due and case.name not in _RARE
    wait = _TICK_WAIT
    if family == "bars":
        now = datetime.now(UTC)
        wait = (streams.minute_bar_deadline(now) - now).total_seconds()
    rows = wait_rows(case.session, table, time.monotonic() + wait) if due else []
    failure, drift, columns = case.failure(), case.drift(), list(table.column_names)
    case.session.release(table)
    _STREAM_CLOSED[0] = time.monotonic()
    case.evidence(
        f"{case.name} stream: acknowledged {acked}; {status}; due {due}; {len(rows)} rows"
    )
    case.skip_if_not_entitled(case.session, case.plan)
    assert failure is None, f"{case.name} stream failed: {failure}"
    assert acked >= 1, f"{case.name}: Massive acknowledged no topic"
    if not due:
        why = _RARE.get(case.name, f"market {status}, or within the feed delay")
        pytest.skip(f"not exercised: {why} (acknowledged)")
    assert rows, f"{case.name}: no row arrived in {wait:.0f} s, though rows were due"
    return columns, drift


_PLAN_OF: dict[tuple[str, str], str] = {}
"""The plan a refused `fetch()` or `history()` case needs, when it differs from its verb's
usual one (a refusal of the flat files); the test records it as the case's `plan`."""


@pytest.mark.timeout(100)
@pytest.mark.parametrize(("name", "verb"), _params())
def test_every_dataset_and_verb_returns_its_columns(
    live_session: dm.MassiveSession,
    name: str,
    verb: str,
    *,
    live_rest: RestClient,
    index_symbol: str,
    previous_trading_day: date,
    front_contract: Callable[[str, date], str],
    table_rows: TableRows,
    wait_acked: Callable[[dm.MassiveSession, int, float], int],
    wait_rows: WaitRows,
    rows_due: RowsDue,
    skip_if_not_entitled: SkipIfNotEntitled,
    record_property: Callable[[str, object], None],
    caplog: pytest.LogCaptureFixture,
    evidence: Evidence,
) -> None:
    """One dataset's verb, called once, works and returns exactly the dataset's columns.

    Reading it needs nobody. A `fetch()` or `history()` passes when the call succeeded with
    exactly the dataset's columns, in the order `columns()` lists them, an empty result
    included. The poll passes when two cycles published rows. A stream passes only when
    Massive acknowledged its topic and rows reached its table; it skips after the
    acknowledgement when rows need not flow (a closed market, the feed delay of a delayed plan,
    a stream of rare events). "Not entitled: needs <plan>" is a skip, and so is a history the
    request cannot be served without S3 credentials; anything else fails. Every library
    WARNING and every `SchemaDrift` the case saw is recorded, and counted in the coverage.
    """
    spec = CATALOG.by_name[name]
    plan = _plan(name, spec, verb)
    if verb == "stream":
        since = time.monotonic() - _STREAM_CLOSED[0]
        if since < _STREAM_GAP:
            threading.Event().wait(_STREAM_GAP - since)
    symbols = {
        **_SYMBOLS,
        dm.AssetClass.INDICES: index_symbol,
        dm.AssetClass.FUTURES: front_contract("ES", datetime.now(UTC).date()),
    }
    query = _query(
        live_session, name, verb, client=live_rest, symbols=symbols, day=previous_trading_day
    )
    expected = [row["Name"] for row in table_rows(query.columns())]
    case = _Case(live_session, name, spec, plan, table_rows, caplog, skip_if_not_entitled, evidence)
    try:
        if verb in {"fetch", "history"}:
            columns, drift = _call(case, query, verb, day=previous_trading_day)
        elif verb == "poll":
            columns, drift = _poll_case(case, query)
        else:
            columns, drift = _stream_case(
                case, query, wait_acked=wait_acked, wait_rows=wait_rows, rows_due=rows_due
            )
    finally:
        record_property("plan", _PLAN_OF.pop((name, verb), plan))
    if drift:
        record_property("drift", "; ".join(drift))
        for line in drift:
            evidence(f"drift: {line}")
    assert columns == expected
