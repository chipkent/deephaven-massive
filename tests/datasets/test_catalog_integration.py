"""The live smoke tier: every dataset of the catalog, through every verb it offers, once.

Each test builds one dataset's query through the public `dm` API with one symbol of its class,
runs one verb, and asserts only that the call worked and that the result has exactly the
dataset's columns (an empty result included). No human reads anything:

- `fetch()`: one call, with `limit` 5 where the dataset takes a limit.
- `history()`: one recent weekday; whole days for bars, a five-minute window (15:00 to 15:05
  New York time) for ticks.
- `poll()` (for the datasets served by REST alone): the first cycle, up to 20 s, then the
  table is released.
- `append_only()`: up to 15 s of the stream, then the table is released (a closed market
  sends nothing, which passes).

A dataset the key's plan does not include is a skip, "not entitled: needs <plan>", never a
failure. The terminal summary ends with a coverage count: passed, not entitled, failed.

The tier is marked `integration_smoke` (and `integration`) and runs only with
`uv run pytest -m integration_smoke`. On a free key it stays within Massive's rate limits:
every REST request to a class declared Basic (`MASSIVE_LIVE_PLANS`) waits for a place in that
class's 5 a minute, and the streams of a Basic class (Basic has no WebSocket access) skip
before any call, naming the Starter plan they need. How to run it is in `docs/live-tests.md`.
"""

from __future__ import annotations

import inspect
import threading
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol, cast
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import RestClient, Transport, source
from deephaven_massive._session import CATALOG

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import date
    from typing import TypeAlias

    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._query import QueryBase

pytestmark = [pytest.mark.integration, pytest.mark.integration_smoke]

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Evidence: TypeAlias = "Callable[[str], None]"
SkipIfNotEntitled: TypeAlias = "Callable[[dm.MassiveSession, str], None]"

_NEW_YORK = ZoneInfo("America/New_York")
_POLL_WAIT = 20.0
"""Seconds a poll test waits for its first cycle's rows."""

_STREAM_WAIT = 15.0
"""Seconds a stream test listens."""

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
_PLAN_NAMES = {
    "stocks": "a Stocks plan",
    "options": "an Options plan",
    "indices": "an Indices plan",
    "currencies": "a Currencies plan",
    "futures": "a Futures plan",
}
_SYMBOLS = {
    dm.AssetClass.STOCKS: "AAPL",
    dm.AssetClass.INDICES: "I:SPX",
    dm.AssetClass.FOREX: "C:EURUSD",
    dm.AssetClass.CRYPTO: "X:BTCUSD",
}
_ETF = "SPY"
"""The symbol of the ETF Global datasets, which describe funds."""

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

_ATTRIBUTE_PATHS = {"reference.snapshots": ("snapshots",)}
"""Datasets whose method is not at their catalog name's path."""

_STREAM_CLOSED = [float("-inf")]
"""When the last stream test's session closed."""

_FOUND: dict[str, str] = {}
"""Symbols looked up once per run: an options contract and a futures contract."""


def _cases() -> list[tuple[str, str]]:
    """Return every (dataset, verb) pair of the catalog.

    Returns:
        The pairs, in catalog order.
    """
    cases: list[tuple[str, str]] = []
    for name, spec in CATALOG.by_name.items():
        if spec.has_fetch:
            cases.append((name, "fetch"))
        if spec.has_fetch and not (spec.has_history or spec.has_channels):
            cases.append((name, "poll"))
        if spec.has_history:
            cases.append((name, "history"))
        if spec.has_channels:
            cases.append((name, "stream"))
    return cases


def _recent_weekday() -> date:
    """Return the weekday before today in New York.

    Returns:
        The day; a holiday's results are empty, which passes.
    """
    day = datetime.now(_NEW_YORK).date() - timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def _subscription(spec: DatasetSpec) -> str | None:
    """Return the subscription a dataset belongs to, or None for data of no single class.

    Returns:
        `"stocks"`, `"options"`, `"indices"`, `"currencies"`, `"futures"`, or None.
    """
    return None if spec.asset_class is None else _SUBSCRIPTIONS[spec.asset_class]


def _plan(name: str, spec: DatasetSpec, verb: str) -> str:
    """Name the plan a dataset's verb needs, for the "not entitled" skip.

    Returns:
        Such as `"a Stocks plan with WebSocket access to stocks.trades"`.
    """
    if name.startswith("partners."):
        return f"the {name.split('.')[1]} partner add-on ({name})"
    if name.endswith(".fmv"):
        return f"a Business plan ({name})"
    subscription = _subscription(spec)
    plan = "a plan" if subscription is None else _PLAN_NAMES[subscription]
    access = "WebSocket access to" if verb == "stream" else "REST access to"
    return f"{plan} with {access} {name}"


def _front_future() -> str:
    """Return the E-mini S&P 500 contract of the next quarterly month.

    Returns:
        Such as `"ESZ6"` in October 2026.
    """
    today = datetime.now(UTC).date()
    month = next((m for m in (3, 6, 9, 12) if m > today.month), 3)
    year = today.year + (month == 3 and today.month >= 12)
    code = {3: "H", 6: "M", 9: "U", 12: "Z"}[month]
    return f"ES{code}{year % 10}"


def _option(client: RestClient) -> str:
    """Return one listed AAPL option contract expiring a month or more out, looked up once.

    Returns:
        Its OCC symbol, such as `"O:AAPL261218C00200000"`.
    """
    if "options" not in _FOUND:
        later = datetime.now(UTC).date() + timedelta(days=30)
        rows = client.get(
            source(Transport.REST, "/v3/reference/options/contracts"),
            {},
            {"underlying_ticker": "AAPL", "expiration_date.gte": later, "limit": 1},
        ).rows
        assert rows, "no listed AAPL option contract"
        _FOUND["options"] = str(rows[0]["ticker"])
    return _FOUND["options"]


def _symbol(name: str, spec: DatasetSpec, client: RestClient) -> str:
    """Return the one symbol a dataset is called with.

    Returns:
        The symbol of its class (an ETF for the ETF Global datasets, AAPL for data of no class).
    """
    if name.startswith("partners.etf_global."):
        return _ETF
    if spec.asset_class is dm.AssetClass.OPTIONS:
        return _option(client)
    if spec.asset_class is dm.AssetClass.FUTURES:
        return _FOUND.setdefault("futures", _front_future())
    return _SYMBOLS.get(spec.asset_class, "AAPL") if spec.asset_class else "AAPL"


def _query(session: dm.MassiveSession, name: str, verb: str, *, client: RestClient) -> QueryBase:
    """Build a dataset's query for one verb, with one symbol and the fewest rows.

    Every required argument gets a value (`_FIXED_ARGUMENTS`, the recent weekday for a
    `date`); `symbols` gets the class's symbol even where it is optional, and `limit` 5 where
    the dataset takes one. A bars dataset uses `Timespan.DAY` for `fetch()` and `poll()` (the
    previous day's bar) and its default timespan otherwise.

    Returns:
        The query.

    Raises:
        AssertionError: If the method needs an argument this module has no value for.
    """
    spec = CATALOG.by_name[name]
    target: Any = session
    for part in _ATTRIBUTE_PATHS.get(name, tuple(name.split("."))):
        target = getattr(target, part)
    method: Callable[..., QueryBase] = target
    arguments: dict[str, object] = {}
    for parameter in inspect.signature(method).parameters.values():
        if parameter.name == "symbols":
            arguments["symbols"] = _symbol(name, spec, client)
        elif parameter.name == "timespan" and verb in {"fetch", "poll"}:
            arguments["timespan"] = dm.Timespan.DAY
        elif parameter.name == "limit" and verb in {"fetch", "poll"}:
            arguments["limit"] = _LIMIT
        elif parameter.default is not inspect.Parameter.empty:
            continue
        elif parameter.name == "date":
            arguments["date"] = _recent_weekday()
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


def _run(query: QueryBase, verb: str, *, bars: bool) -> Table:
    """Run one fetch or history verb on a query that offers it.

    Returns:
        The static result.
    """
    if verb == "fetch":
        return cast("_Fetches", query).fetch()
    day = _recent_weekday()
    histories = cast("_Histories", query)
    if bars:
        return histories.history(day, day)
    start = datetime(day.year, day.month, day.day, 15, tzinfo=_NEW_YORK)
    return histories.history(start, start + timedelta(minutes=5))


@pytest.mark.timeout(100)
@pytest.mark.parametrize(("name", "verb"), _cases(), ids=[f"{n}-{v}" for n, v in _cases()])
def test_every_dataset_and_verb_returns_its_columns(
    live_session: dm.MassiveSession,
    name: str,
    verb: str,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    skip_if_not_entitled: SkipIfNotEntitled,
    require_plans: Callable[[tuple[str, ...]], None],
    evidence: Evidence,
) -> None:
    """One dataset's verb, called once, works and returns exactly the dataset's columns.

    Reading it needs nobody: a pass means the call succeeded (an empty result included) with
    exactly the dataset's columns, in order (a keyed `poll()` table that puts its keys first
    is an expected failure until the library's fix lands); "not entitled: needs <plan>" is a
    skip; anything else fails. A history the request cannot be served without S3 credentials
    skips, naming them.

    Raises:
        ValueError: When a verb refuses its arguments for any reason but missing S3
            credentials: a defect of this module or of the library.
    """
    spec = CATALOG.by_name[name]
    plan = _plan(name, spec, verb)
    subscription = _subscription(spec)
    if verb == "stream":
        since = time.monotonic() - _STREAM_CLOSED[0]
        if since < _STREAM_GAP:
            threading.Event().wait(_STREAM_GAP - since)
    if verb == "stream" and subscription is not None:
        require_plans((f"{subscription}:starter",))
    query = _query(live_session, name, verb, client=live_rest)
    expected = [row["Name"] for row in table_rows(query.columns())]
    if verb in {"fetch", "history"}:
        try:
            table = _run(query, verb, bars=spec.is_bars)
        except dm.EntitlementError as error:
            pytest.skip(f"not entitled: needs {plan} ({error})")
        except ValueError as error:
            if "s3" not in str(error).lower():
                raise
            pytest.skip(f"needs S3 credentials for flat files ({error})")
    else:
        live: Any = query
        table = live.poll(60) if verb == "poll" else live.append_only()
        pause = threading.Event()  # never set; only paces the polling
        deadline = time.monotonic() + (_POLL_WAIT if verb == "poll" else _STREAM_WAIT)
        while time.monotonic() < deadline and not (verb == "poll" and table_rows(table)):
            if any(r["State"] == "FAILED" for r in table_rows(live_session.streams())):
                break
            pause.wait(0.5)
        failed = [r for r in table_rows(live_session.streams()) if r["State"] == "FAILED"]
        live_session.release(table)
        if verb == "stream":
            _STREAM_CLOSED[0] = time.monotonic()
        skip_if_not_entitled(live_session, plan)
        assert not failed, f"{name} {verb} failed: {failed[0]['Error']}"
    columns = list(table.column_names)
    evidence(f"{name} {verb}: {table.size} rows; columns {columns}")
    if verb == "poll" and columns != expected and sorted(columns) == sorted(expected):
        pytest.xfail(
            "a keyed poll() table puts its poll keys first, against columns()'s order; "
            "the library fix that keeps the dataset's order is pending"
        )
    assert columns == expected
