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
  quotes in the extended hours too), by the market's clock (`streams`). A minute bar stream
  waits until just past the next minute boundary. When rows need not flow, the case skips
  after the acknowledgement, "not exercised: market closed (acknowledged)" or the like, so a
  pass always means rows reached the table. A stock's trades or quotes that send no row in
  the extended hours skip too: one stock can go a minute without a tick then. A stream of a
  subscription the owner's settings list in `delayed_subscriptions` that sends no row skips,
  since the delayed feed lags by an amount the tests do not know. Otherwise, in the regular
  session, no row fails the case.

A dataset Massive refuses the key is a skip that quotes Massive's refusal, never a failure.
Each case also notes every WARNING the library logged and every `SchemaDrift` the session's
`streams()` and `connections()` show; a pass with any of them is counted "passed with drift"
in the coverage that ends the terminal summary (passed, not entitled, otherwise skipped,
failed), which lists each one and groups the refusals by the subscription and verb refused,
and the other skips by their reason.

The cases run round-robin across the classes, so a class Massive limits waits for its pace
while the next class's case runs. The tier is marked `integration_smoke` (and `integration`)
and runs only with `uv run pytest -m integration_smoke`. Every REST request of a subscription
Massive limits waits for its pace, and each case's time limit grows with its request count.
How to run it is in `docs/live-tests.md`.
"""

from __future__ import annotations

import inspect
import itertools
import logging
import re
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm
from deephaven_massive._enums import ClassFromPrefix
from deephaven_massive._massive import RestClient, Transport, WireEntitlementError, source
from deephaven_massive._session import CATALOG
from deephaven_massive.queries import FetchQuery, HistoryQuery, NoSymbolStreamQuery, StreamQuery
from tests._live import streams
from tests._live.feeds import with_delayed

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from contextlib import AbstractContextManager
    from datetime import date
    from typing import TypeAlias

    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive.queries import Query

pytestmark = [pytest.mark.integration, pytest.mark.integration_smoke]

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Evidence: TypeAlias = "Callable[[str], None]"
SkipIfNotEntitled: TypeAlias = "Callable[[dm.MassiveSession], None]"
SkipIfRealTimeOnlyRefused: TypeAlias = "Callable[[], AbstractContextManager[None]]"
RowsDue: TypeAlias = "Callable[[str, str], tuple[str, bool]]"

_NEW_YORK = ZoneInfo("America/New_York")
_POLL_DATASET: Final = "reference.market_status"
"""The one dataset the tier polls: it belongs to no subscription, so no subscription's limit
paces it."""

_POLL_INTERVAL = 5.0
"""Seconds between the poll case's cycles."""

_POLL_WAIT = 30.0
"""Seconds the poll case waits for its two cycles."""

_STREAM_WAIT = 15.0
"""Seconds a stream case waits for Massive's acknowledgement."""

_TICK_WAIT = 60.0
"""Seconds a stream case of ticks or values waits for its first row once rows must flow."""

_STREAM_GAP = 3.0
"""Seconds between one stream test's session closing and the next one connecting."""

_MINUTE_BAR_WAIT = 60.0 + streams.MINUTE_BAR_LAG.total_seconds()
"""The most seconds a minute bar stream case waits for its first row: just past the next minute."""

_CASE_TIMEOUT = 100.0
"""Seconds a fetch or poll case may take, before any growth for paced requests."""

_HISTORY_TIMEOUT = 200.0
"""Seconds a history case may take, before any growth for paced requests. Every history case
but `indices.values` reads one symbol from REST, a few requests at most."""

_DAY_FILE_TIMEOUT = 900.0
"""Seconds the `indices.values` history case may take, before any growth for paced requests.

Massive has no REST history of index values, so the case reads the flat files, and a flat-file
read downloads the whole day's file, however short its window: for 2026-11-09,
`us_indices/values_v1/2026/11/2026-11-09.csv.gz` was 2,938,190,238 bytes. One run read it in
about 94 s; another, on 2026-11-10, was still downloading at 252 s. The time is the network's,
so the limit assumes a modest 30 Mbit/s (3.75 MB/s): 2.94 GB / 3.75 MB/s = 784 s for the
download, and about 100 s more for the read that follows, 900 s in all. It guards against a
hang, not a slow connection."""

_STREAM_SETUP = 60.0
"""Seconds a stream case may spend outside its designed waits: building its query (a REST
lookup for some symbols), opening the stream, and releasing it."""

_STREAM_TIMEOUT = _STREAM_GAP + _STREAM_WAIT + 2 * max(_TICK_WAIT, _MINUTE_BAR_WAIT) + _STREAM_SETUP
"""Seconds a stream case may take: the gap after the previous stream, the acknowledgement, the
wait for rows and its one retry when another client took the connection's slot, and its setup.
A slot lost during the case then ends in the slot message or a pass, never in a timeout."""

_TIMEOUTS = {"history": _HISTORY_TIMEOUT, "stream": _STREAM_TIMEOUT}
"""The verbs whose cases may take longer than `_CASE_TIMEOUT`, and their limits."""

_CASE_TIMEOUTS = {("indices.values", "history"): _DAY_FILE_TIMEOUT}
"""The cases whose limit is not their verb's, and their limits."""

_SOURCE_CHOICE = re.compile(r" history: AUTO read .+? \(.*\); the read leaves .* null")
"""The library's once-per-session notice that an `AUTO` history read chose the source that
leaves some columns null, such as "futures.trades history: AUTO read REST (1 symbols <
flat_file_symbol_threshold 50); the read leaves Correction, Exchange null, ..."."""

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

_SYMBOLS: dict[dm.AssetClass | ClassFromPrefix, str] = {
    dm.AssetClass.STOCKS: "AAPL",
    dm.AssetClass.FOREX: "C:EURUSD",
    dm.AssetClass.CRYPTO: "X:BTCUSD",
    dm.AssetClass.INDICES: "I:NDX",
}
"""The symbol of each class with a fixed one; any index serves, so indices read the
Nasdaq-100."""
_ETF = "SPY"
"""The symbol of the ETF Global datasets, which describe funds."""

_EVERY_SYMBOL: Final = frozenset({"stocks.luld"})
"""The streams read for every symbol (`dm.ALL`): one stock's LULD bands change too rarely for
a row to arrive in a test's time, while some band of the market's changes every few seconds."""

_RARE: Final = {
    "stocks.imbalances": "imbalances arrive only around the opening and closing auctions",
    "options.admin_messages": (
        "OPRA sends admin messages only with FLEX activity or an exchange's notice, "
        "not on a schedule"
    ),
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
    classes take turns, so a class Massive limits to a few requests a minute never runs two
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
    """Return every (dataset, verb) pair as a test case with its request count and timeout.

    Every case sends its request whatever the key holds; Massive's refusal skips it. The
    timeout grows with the `requests` marker, since a subscription Massive limits paces each
    request. A fetch or poll case gets `_CASE_TIMEOUT` before that growth, a history case
    `_HISTORY_TIMEOUT` (`_DAY_FILE_TIMEOUT`
    for `indices.values`, whose read downloads a whole day's flat file), and a stream case
    `_STREAM_TIMEOUT`, which covers its designed waits. The timeout is set here alone: a
    `timeout` mark on the test function would win over these, since pytest finds the
    function's marks before a parameter's.

    Returns:
        One `pytest.param` per pair, with its id `<dataset>-<verb>`.
    """
    params = []
    for name, verb in _cases():
        timeout = _CASE_TIMEOUTS.get((name, verb), _TIMEOUTS.get(verb, _CASE_TIMEOUT))
        marks = [pytest.mark.requests(_REQUESTS[verb]), pytest.mark.timeout(timeout)]
        params.append(pytest.param(name, verb, marks=marks, id=f"{name}-{verb}"))
    return params


def _subscription(spec: DatasetSpec) -> str | None:
    """Return the subscription a dataset belongs to, or None for data of no single class.

    Returns:
        `"stocks"`, `"options"`, `"indices"`, `"currencies"`, `"futures"`, or None.
    """
    return None if spec.asset_class is ClassFromPrefix.ANY else _SUBSCRIPTIONS[spec.asset_class]


def _refused_group(spec: DatasetSpec, verb: str, *, flat_files: bool = False) -> str:
    """Name what a refusal of a case refused, for the coverage's group of refusals.

    Args:
        spec: The dataset's catalog entry.
        verb: The verb.
        flat_files: Whether the refusal came from the S3 flat files rather than REST.

    Returns:
        The subscription and the verb, such as `"stocks stream"`, `"other fetch"` for data of
        no single class, or `"stocks history (flat files)"`.
    """
    group = f"{_subscription(spec) or 'other'} {verb}"
    return f"{group} (flat files)" if flat_files else group


def _option(client: RestClient) -> str:
    """Return the busiest AAPL option near the money at the front expiration, looked up once.

    Reads AAPL's previous close (its `previous_day_bar`), then the chain snapshot of the
    contracts expiring after today with a strike within 5% of that close, and takes, at the
    nearest of those expirations, the contract with the most volume today: the one most
    likely to trade while a stream case listens. A key Massive refuses the chain snapshot
    gets the listed call at that expiration with the strike nearest the close instead.

    Returns:
        Its OCC symbol, such as `"O:AAPL261106C00270000"`.
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
        window = {
            "expiration_date.gt": datetime.now(UTC).date(),
            "strike_price.gte": round(close * 0.95, 2),
            "strike_price.lte": round(close * 1.05, 2),
            "sort": "expiration_date",
            "order": "asc",
            "limit": 250,
        }
        try:
            chain = client.get(
                source(Transport.REST, "/v3/snapshot/options/{underlyingAsset}"),
                {"underlyingAsset": "AAPL"},
                window,
                bucket="options",
            ).rows
        except WireEntitlementError:
            listed = client.get(
                source(Transport.REST, "/v3/reference/options/contracts"),
                {},
                {**window, "underlying_ticker": "AAPL", "contract_type": "call"},
                bucket="options",
            ).rows
            assert listed, f"no listed AAPL call near {close}"
            expiry = min(str(r["expiration_date"]) for r in listed)
            nearest = min(
                (r for r in listed if str(r["expiration_date"]) == expiry),
                key=lambda r: abs(float(str(r["strike_price"])) - close),
            )
            _FOUND["options"] = str(nearest["ticker"])
        else:
            assert chain, f"no AAPL option near {close} in the chain snapshot"
            contracts = [
                (_json_object(r["details"]), _json_object(r.get("day") or {})) for r in chain
            ]
            expiry = min(str(details["expiration_date"]) for details, _ in contracts)
            busiest, _ = max(
                (c for c in contracts if str(c[0]["expiration_date"]) == expiry),
                key=lambda c: float(str(c[1].get("volume") or 0)),
            )
            _FOUND["options"] = str(busiest["ticker"])
    return _FOUND["options"]


def _json_object(value: object) -> Mapping[str, object]:
    """Return a JSON object of Massive's answer, checked to be one."""
    assert isinstance(value, dict), value
    return value


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
) -> Query:
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
    method: Callable[..., Query] = target
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


def _run(query: Query, verb: str, *, bars: bool, day: date) -> Table:
    """Run one fetch or history verb on a query that offers it.

    Returns:
        The static result: for `history()`, the whole day for bars and 15:00 to 15:05 New York
        time for ticks.
    """
    if verb == "fetch":
        assert isinstance(query, FetchQuery)
        return query.fetch(wait=True)
    histories = query
    assert isinstance(histories, HistoryQuery)
    if bars:
        return histories.history(day, day, wait=True)
    start = datetime(day.year, day.month, day.day, 15, tzinfo=_NEW_YORK)
    return histories.history(start, start + timedelta(minutes=5), wait=True)


@dataclass(frozen=True)
class _Case:
    """One smoke case's dataset and the fixtures its verb's run needs.

    Attributes:
        session: The live session.
        name: The dataset.
        spec: Its catalog entry.
        table_rows: Reads a table's rows.
        caplog: The captured log records, for the library's warnings.
        skip_if_not_entitled: Skips the case when a stream of the session was refused.
        evidence: Records a line of evidence.
        record_property: Records a property of the case's report, for the coverage: what a
            refusal refused (`refused`) and its `drift`.
    """

    session: dm.MassiveSession
    name: str
    spec: DatasetSpec
    table_rows: TableRows
    caplog: pytest.LogCaptureFixture
    skip_if_not_entitled: SkipIfNotEntitled
    evidence: Evidence
    record_property: Callable[[str, object], None]

    def drift(self) -> list[str]:
        """Return what the case saw drift: the library's warnings and any `SchemaDrift`.

        The notice that an `AUTO` history read chose a source leaving some columns null
        (`_SOURCE_CHOICE`) is the documented behavior of a small read, not drift: it is
        recorded as evidence and left out.

        Returns:
            One line per WARNING or worse a `deephaven_massive` logger emitted during the
            case, the source-choice notice excepted, and per non-empty `SchemaDrift` of the
            session's streams and connections.
        """
        found = []
        for record in self.caplog.records:
            if record.levelno < logging.WARNING or not record.name.startswith("deephaven_massive"):
                continue
            message = record.getMessage()
            if _SOURCE_CHOICE.search(message):
                self.evidence(f"source choice: {message}")
                continue
            found.append(f"{record.levelname} {record.name}: {message}")
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


def _call(case: _Case, query: Query, verb: str, *, day: date) -> tuple[list[str], list[str]]:
    """Run a `fetch()` or `history()` case; a refusal or missing S3 credentials skip it.

    Returns:
        The result's columns and what the case saw drift.

    Raises:
        EntitlementError: When the call is refused; the live tests' hook skips the case,
            quoting the refusal, after this records what was refused.
        ValueError: When the verb refuses its arguments for any reason but missing S3
            credentials.
    """
    started = time.monotonic()
    try:
        table = _run(query, verb, bars=case.spec.is_bars, day=day)
    except dm.EntitlementError as error:
        flat_files = "flat files" in str(error)
        case.record_property("refused", _refused_group(case.spec, verb, flat_files=flat_files))
        raise
    except ValueError as error:
        if "s3" not in str(error).lower():
            raise
        pytest.skip(f"needs S3 credentials for flat files ({error})")
    columns = list(table.column_names)
    took = time.monotonic() - started
    case.evidence(f"{case.name} {verb}: {table.size} rows in {took:.0f} s; columns {columns}")
    return columns, case.drift()


def _poll_case(case: _Case, query: Query) -> tuple[list[str], list[str]]:
    """Run the poll case: it passes once two cycles have published rows.

    A cycle shows as a new `TimestampInsert`: every row of a cycle carries that cycle's.

    Returns:
        The table's columns and what the case saw drift.
    """
    assert isinstance(query, FetchQuery)
    table = query.poll(_POLL_INTERVAL)
    cycles: set[object] = set()
    pause = threading.Event()  # never set; only paces the polling
    deadline = time.monotonic() + _POLL_WAIT
    while len(cycles) < 2 and time.monotonic() < deadline and case.failure() is None:
        cycles.update(row["TimestampInsert"] for row in case.table_rows(table))
        pause.wait(0.5)
    failure, drift, columns = case.failure(), case.drift(), list(table.column_names)
    case.session.release(table)
    case.evidence(f"{case.name} poll: {len(cycles)} cycles of rows; columns {columns}")
    case.skip_if_not_entitled(case.session)
    assert failure is None, f"{case.name} poll failed: {failure}"
    assert len(cycles) >= 2, f"{case.name}: {len(cycles)} poll cycles in {_POLL_WAIT:.0f} s"
    return columns, drift


_SLOT_TAKEN = "another client on this key took the connection (max_connections)"
"""Why a stream case fails when its connection lost its slot and no row came after the retry."""


def _socket_rows(case: _Case) -> list[dict[str, Any]]:
    """Return the case class's WebSocket rows of `m.connections()`.

    Returns:
        The rows whose `Transport` is `WebSocket` and whose `AssetClass` is the case's class.
    """
    asset_class = str(case.spec.asset_class)
    return [
        r
        for r in case.table_rows(case.session.connections())
        if r["Transport"] == "WebSocket" and r["AssetClass"] == asset_class
    ]


def _reconnects(rows: list[dict[str, Any]]) -> int:
    """Return how often the connections of some `m.connections()` rows have reconnected.

    Returns:
        The sum of their `Reconnects`.
    """
    return sum(int(r["Reconnects"] or 0) for r in rows)


def _row_wait(family: str) -> float:
    """Return how many seconds a stream case waits for its first row from now.

    Returns:
        Until just past the next minute boundary for a minute bar stream
        (`streams.minute_bar_deadline`), `_TICK_WAIT` for any other.
    """
    if family != "bars":
        return _TICK_WAIT
    now = datetime.now(UTC)
    return (streams.minute_bar_deadline(now) - now).total_seconds()


def _slot_taken(case: _Case, before: int) -> bool:
    """Tell whether the case class's connection lost its slot since `before` was read.

    Records the class's WebSocket rows of `m.connections()` when it did.

    Args:
        case: The stream case.
        before: The rows' `Reconnects` total before the stream opened (`_reconnects`).

    Returns:
        True when a row is `WAITING_SLOT` or the connections reconnected since.
    """
    rows = _socket_rows(case)
    taken = any(r["State"] == "WAITING_SLOT" for r in rows) or _reconnects(rows) > before
    if taken:
        case.evidence(f"{case.name} stream: the connection lost its slot: {rows}")
    return taken


def _record_bar_lag(case: _Case, row: dict[str, Any]) -> None:
    """Record when a stream's first minute bar ended and when it reached the table.

    Args:
        case: The minute bar stream case.
        row: The first row of its table.
    """
    end, inserted = row.get("TimestampEnd"), row.get("TimestampInsert")
    lag = None if end is None or inserted is None else (int(inserted) - int(end)) / 1e9
    case.evidence(
        f"{case.name} first bar: TimestampEnd {end}, TimestampInsert {inserted}, "
        f"arrived {lag} s after its minute ended (allowance {streams.MINUTE_BAR_LAG})"
    )


def _may_go_quiet(asset_class: dm.AssetClass, family: str) -> bool:
    """Tell whether a stream of one symbol may send no row in its wait, though rows are due.

    Returns:
        True for a stock's trades or quotes while the market is in the extended hours
        (`streams.may_go_quiet`).
    """
    return streams.may_go_quiet(_MARKETS[asset_class], family, now=datetime.now(UTC))


def _stream_case(
    case: _Case,
    query: Query,
    *,
    wait_acked: Callable[[dm.MassiveSession, int, float], int],
    wait_rows: WaitRows,
    rows_due: RowsDue,
    delayed: bool,
) -> tuple[list[str], list[str]]:
    """Run a stream case: Massive must acknowledge it, and rows must arrive when they are due.

    Rows are due while the stream's market is open for its kind of rows, by the market's clock,
    and before the regular session's close for a stream that needs it (`rows_due`), unless
    the stream is one of `_RARE`. A minute bar stream waits until just past the next minute
    boundary, any other for `_TICK_WAIT`. When no rows are due, the case skips after the
    acknowledgement. A stock trade or quote stream that gets no row while the market is in
    the extended hours skips too, since one stock can go that long without a tick then
    (`streams.may_go_quiet`), when the market is in the extended hours both as the wait
    starts and as it ends; a wait that reaches the regular session fails. A stream of a
    subscription listed in `delayed_subscriptions` (`delayed`) that gets no row skips: the
    delayed feed lags by an amount the tests do not know.

    Another client on the key can take the class's connection slot during the wait: the
    connection then waits for the slot (`WAITING_SLOT`) or reconnects. When no row came and
    the class's WebSocket row of `m.connections()` shows either, the case waits once more,
    as long again from then (past the next minute boundary for minute bars), and fails with
    `_SLOT_TAKEN` rather than "no row arrived" if still nothing comes. For a minute bar
    stream, the first row's `TimestampEnd` and `TimestampInsert` are recorded, so the
    evidence shows how long after its minute Massive's bar arrived (`streams.MINUTE_BAR_LAG`
    is the allowance).

    Returns:
        The table's columns and what the case saw drift.
    """
    assert isinstance(query, (StreamQuery, NoSymbolStreamQuery))
    table = query.append_only()
    before = _reconnects(_socket_rows(case))
    acked = wait_acked(case.session, 1, time.monotonic() + _STREAM_WAIT)
    family = case.name.rpartition(".")[2]
    asset_class = case.spec.single_class
    status, due = rows_due(_MARKETS[asset_class], family)
    due = due and case.name not in _RARE
    quiet = due and _may_go_quiet(asset_class, family)
    wait = _row_wait(family)
    rows = wait_rows(case.session, table, time.monotonic() + wait) if due else []
    taken = due and not rows and _slot_taken(case, before)
    if taken:
        wait = _row_wait(family)
        rows = wait_rows(case.session, table, time.monotonic() + wait)
    quiet = quiet and _may_go_quiet(asset_class, family)  # and still, at the end
    failure, drift, columns = case.failure(), case.drift(), list(table.column_names)
    case.session.release(table)
    _STREAM_CLOSED[0] = time.monotonic()
    case.evidence(
        f"{case.name} stream: acknowledged {acked}; {status}; due {due}; "
        f"may go quiet {quiet}; {len(rows)} rows"
    )
    if family == "bars" and rows:
        _record_bar_lag(case, rows[0])
    case.skip_if_not_entitled(case.session)
    assert failure is None, f"{case.name} stream failed: {failure}"
    assert acked >= 1, f"{case.name}: Massive acknowledged no topic"
    if not due:
        why = _RARE.get(case.name, f"market {status}, or past the close")
        pytest.skip(f"not exercised: {why} (acknowledged)")
    assert rows or not taken, f"{case.name}: {_SLOT_TAKEN}"
    if not rows and delayed:
        pytest.skip(
            f"no row on the delayed feed in {wait:.0f} s; the market is {status} (acknowledged)"
        )
    if not rows and quiet:
        pytest.skip(
            f"not exercised: no row in {wait:.0f} s of the extended-hours session (acknowledged)"
        )
    assert rows, f"{case.name}: no row arrived in {wait:.0f} s, though rows were due"
    return columns, drift


@pytest.mark.parametrize(("name", "verb"), _params())
def test_every_dataset_and_verb_returns_its_columns(
    live_session: dm.MassiveSession,
    name: str,
    verb: str,
    *,
    live_config: dm.MassiveConfig,
    live_rest: RestClient,
    previous_trading_day: date,
    front_contract: Callable[[str, date], str],
    table_rows: TableRows,
    wait_acked: Callable[[dm.MassiveSession, int, float], int],
    wait_rows: WaitRows,
    rows_due: RowsDue,
    skip_if_not_entitled: SkipIfNotEntitled,
    skip_if_real_time_only_refused: SkipIfRealTimeOnlyRefused,
    record_property: Callable[[str, object], None],
    caplog: pytest.LogCaptureFixture,
    evidence: Evidence,
) -> None:
    """One dataset's verb, called once, works and returns exactly the dataset's columns.

    Reading it needs nobody. A `fetch()` or `history()` passes when the call succeeded with
    exactly the dataset's columns, in the order `columns()` lists them, an empty result
    included. The poll passes when two cycles published rows. A stream passes only when
    Massive acknowledged its topic and rows reached its table; it skips after the
    acknowledgement when rows need not flow (a closed market, a stock, option, or index stream
    after the regular session's close, a stream of rare events), when a stock's trades or
    quotes sent no row in the extended hours, and when a stream of a subscription listed in
    `delayed_subscriptions` sent no row. Massive's refusal is a skip that quotes it, and so is
    a history the request cannot be served without S3 credentials; anything else fails.
    Every library WARNING and every `SchemaDrift` the case saw is recorded, and counted in the
    coverage. A stream Massive serves only in real time opens a session of its own with its
    subscription left out of `delayed_subscriptions` (and any delayed host pin of its class),
    as a user streaming it would; the library's refusal of it at the call, when Massive labels
    the key's data of its subscription delayed, skips, quoting the library.
    """
    spec = CATALOG.by_name[name]
    record_property("refused", _refused_group(spec, verb))  # read only when Massive refuses
    subscription = _subscription(spec)
    listed = live_config.delayed_subscriptions
    own: dm.MassiveSession | None = None
    if verb == "stream" and spec.real_time_only and subscription is not None:
        listed = listed - {dm.Subscription(subscription)}
        own = dm.MassiveSession(with_delayed(live_config, listed))
    try:
        with skip_if_real_time_only_refused():
            _run_case(
                own or live_session,
                name,
                verb,
                delayed=subscription is not None and dm.Subscription(subscription) in listed,
                live_rest=live_rest,
                previous_trading_day=previous_trading_day,
                front_contract=front_contract,
                table_rows=table_rows,
                wait_acked=wait_acked,
                wait_rows=wait_rows,
                rows_due=rows_due,
                skip_if_not_entitled=skip_if_not_entitled,
                record_property=record_property,
                caplog=caplog,
                evidence=evidence,
            )
    finally:
        if own is not None:
            own.close()


def _run_case(
    session: dm.MassiveSession,
    name: str,
    verb: str,
    *,
    delayed: bool,
    live_rest: RestClient,
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
    """Run one smoke case in a session, and assert it returns exactly the dataset's columns.

    Args:
        session: The session the case runs in.
        name: The dataset.
        verb: The verb.
        delayed: Whether the session lists the dataset's subscription as delayed.
        live_rest: Looks up an option contract.
        previous_trading_day: The day a history reads.
        front_contract: Names the front ES contract.
        table_rows: Reads a table's rows.
        wait_acked: Waits for a stream's acknowledgement.
        wait_rows: Waits for a stream's rows.
        rows_due: Tells whether a stream's rows are due.
        skip_if_not_entitled: Skips the case when a stream of the session was refused.
        record_property: Records a property of the case's report, for the coverage.
        caplog: The captured log records, for the library's warnings.
        evidence: Records a line of evidence.
    """
    spec = CATALOG.by_name[name]
    if verb == "stream":
        since = time.monotonic() - _STREAM_CLOSED[0]
        if since < _STREAM_GAP:
            threading.Event().wait(_STREAM_GAP - since)
    symbols = {**_SYMBOLS, dm.AssetClass.FUTURES: front_contract("ES", datetime.now(UTC).date())}
    query = _query(session, name, verb, client=live_rest, symbols=symbols, day=previous_trading_day)
    expected = [row["Name"] for row in table_rows(query.columns())]
    case = _Case(
        session,
        name,
        spec,
        table_rows,
        caplog,
        skip_if_not_entitled,
        evidence,
        record_property,
    )
    if verb in {"fetch", "history"}:
        columns, drift = _call(case, query, verb, day=previous_trading_day)
    elif verb == "poll":
        columns, drift = _poll_case(case, query)
    else:
        columns, drift = _stream_case(
            case,
            query,
            wait_acked=wait_acked,
            wait_rows=wait_rows,
            rows_due=rows_due,
            delayed=delayed,
        )
    if drift:
        record_property("drift", "; ".join(drift))
        for line in drift:
            evidence(f"drift: {line}")
    assert columns == expected
