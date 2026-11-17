"""Tests for the REST transport: requests, pages, conversion, errors, history, and hosts."""

import dataclasses
import functools
import gc
import logging
import math
import re
import threading
import weakref
from collections.abc import Callable, Iterator, Mapping, Sequence
from collections.abc import Set as AbstractSet
from concurrent.futures import ALL_COMPLETED, FIRST_EXCEPTION, Future, ThreadPoolExecutor
from concurrent.futures import wait as futures_wait
from datetime import UTC, date, datetime, timedelta
from types import MethodType
from typing import Any, NoReturn, ParamSpec, TypedDict
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import new_table
from deephaven.column import string_col
from deephaven.table import Table
from deephaven.update_graph import shared_lock
from typing_extensions import Unpack, override

import deephaven_massive as dm
from deephaven_massive import _log_limits, _massive, _session
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.futures_sessions import CHICAGO
from deephaven_massive._datasets.history_source import history_window
from deephaven_massive._datasets.request import Request, ValueTable
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import (
    AssetClass,
    ChannelFamily,
    Direction,
    HistorySource,
    Market,
    Order,
    PollMode,
    Recency,
    Subscription,
    Timespan,
)
from deephaven_massive._errors import (
    LIBRARY_BUG,
    NEW_SESSION,
    READ_STOPPED,
    CutShortError,
    RowDataError,
    UnavailableSymbolError,
)
from deephaven_massive._ingest import (
    crypto_tickers,
    host_decision,
    rate_limits,
    rest,
    rest_requests,
    shared_lookup,
)
from deephaven_massive._ingest.clients import SessionClients
from deephaven_massive._ingest.crypto_tickers import LIST_KEY, NOT_LISTED, CryptoTickers
from deephaven_massive._ingest.shared_lookup import SharedLookup
from deephaven_massive._log_limits import LoggedKeys
from deephaven_massive._massive import Cancel, RestPath, Transport
from deephaven_massive._tables import poll, problem_reporter
from deephaven_massive._tables.background_read import BackgroundRead
from deephaven_massive._tables.diagnostics import Diagnostics
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.problem_events import ProblemCause, ProblemEvent
from deephaven_massive._tables.read_tally import ReadTally
from deephaven_massive._tables.session_core import SessionCore
from deephaven_massive._tables.transports import (
    HostDecision,
    SymbolFailure,
    SymbolOutcome,
    SymbolRows,
)
from deephaven_massive.namespaces import _futures as futures_ns
from deephaven_massive.namespaces import _options as options_ns
from deephaven_massive.namespaces import _reference as reference
from deephaven_massive.namespaces import _stocks as stocks_ns
from deephaven_massive.namespaces._query_building import build_query
from deephaven_massive.queries import FetchQuery, HistoryQuery, Query
from tests._fakes.clock import FakeClock
from tests._fakes.rest import FakeRestServer, Received, Reply, envelope

# The reply and envelope builders are plain functions of the fake, which no fixture can hand
# over; the server itself comes from the `fake_rest` fixture.

R = Transport.REST
LOGGER = "deephaven_massive._ingest.rest"
SHARED_LOGGER = "deephaven_massive._ingest.shared_lookup"
ToPylist = Callable[[Table], list[dict[str, Any]]]
RowsWhen = Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]]
NS = 10**9
TIMEOUT = 10.0
"""Seconds of real time a test waits for an event before it fails."""


def alarms(caplog: pytest.LogCaptureFixture) -> list[str]:
    # The problem reporter's WARNINGs: the first problem of each cause on each table or read.
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == problem_reporter.__name__ and r.levelno == logging.WARNING
    ]


def record_problems(m: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch) -> list[ProblemEvent]:
    # The problems the session's reads report, in order; they still reach its reporter, which
    # logs the alarm, and m.problems().
    events: list[ProblemEvent] = []
    report = m._core.problems

    def recorded(found: Sequence[ProblemEvent]) -> None:
        events.extend(found)
        report(found)

    monkeypatch.setattr(m._core, "problems", recorded)
    return events


def no_data_alarm(dataset: str, read: int, symbol: str, *, why: str) -> str:
    # The WARNING of a read's first symbol Massive has no data for.
    return (
        f"{dataset} table {read}: unavailable_symbols: {symbol}: Massive has no data for it "
        f"{why} (at {dataset}); m.problems() lists it and every later unavailable_symbols "
        "problem of this table, which the log shows at DEBUG only"
    )


DIVIDENDS_PATH = "/stocks/v1/dividends"
DIVIDENDS = DatasetSpec(
    summary="A dataset for the tests.",
    name="stocks.dividends",
    asset_class=AssetClass.STOCKS,
    fetch_path=DIVIDENDS_PATH,
    symbol_param="ticker.any_of",
    time_axis=("ex_dividend_date.gte", "ex_dividend_date.lte"),
    keys={(R, DIVIDENDS_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
    page_size=1000,
    symbols_per_request=2,
)
CRYPTO_LAST_PATH = "/v1/last/crypto/{from}/{to}"
CRYPTO_LAST = DatasetSpec(
    summary="A dataset for the tests.",
    name="crypto.last",
    asset_class=AssetClass.CRYPTO,
    fetch_path=CRYPTO_LAST_PATH,
    serves_current_data=True,
    refused_alternative="crypto.snapshots",
    keys={(R, CRYPTO_LAST_PATH): SourceKeys(symbol=KeySpec("from", from_request=True))},
)
MOVERS_PATH = "/v2/snapshot/locale/us/markets/stocks/{direction}"
MOVERS = DatasetSpec(
    summary="A dataset for the tests.",
    name="stocks.movers",
    asset_class=AssetClass.STOCKS,
    fetch_path=MOVERS_PATH,
    serves_current_data=True,
    keys={(R, MOVERS_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
)
CONTRACTS_PATH = "/v3/reference/options/contracts"
CONTRACT_PATH = "/v3/reference/options/contracts/{options_ticker}"
CONTRACTS = DatasetSpec(
    summary="A dataset for the tests.",
    name="options.contracts",
    asset_class=AssetClass.OPTIONS,
    fetch_path=CONTRACTS_PATH,
    detail_path=CONTRACT_PATH,
    keys={
        (R, CONTRACTS_PATH): SourceKeys(symbol=KeySpec("Ticker")),
        (R, CONTRACT_PATH): SourceKeys(symbol=KeySpec("Ticker")),
    },
)
SMA_STOCK = "/v1/indicators/sma/{stockTicker}"
SMA_CRYPTO = "/v1/indicators/sma/{cryptoTicker}"
SMA = DatasetSpec(
    summary="A dataset for the tests.",
    name="indicators.sma",
    row_times=RowTimes.BAR,
    fetch_path=SMA_STOCK,
    fetch_paths_by_class={AssetClass.CRYPTO: SMA_CRYPTO},
    time_axis=("timestamp.gte", "timestamp.lt"),
    keys={
        (R, SMA_STOCK): SourceKeys(
            symbol=KeySpec("stockTicker", from_request=True), time=KeySpec("Timestamp")
        ),
        (R, SMA_CRYPTO): SourceKeys(
            symbol=KeySpec("cryptoTicker", from_request=True), time=KeySpec("Timestamp")
        ),
    },
    latest_is_current=True,
)
CONVERSION_PATH = "/v1/conversion/{from}/{to}"
CONVERSION = DatasetSpec(
    summary="A dataset for the tests.",
    name="forex.conversion",
    asset_class=AssetClass.FOREX,
    fetch_path=CONVERSION_PATH,
    serves_current_data=True,
    keys={(R, CONVERSION_PATH): SourceKeys(symbol=KeySpec("Symbol"))},
)
INDEX_SNAPSHOT_PATH = "/v3/snapshot/indices"
INDEX_SNAPSHOTS = DatasetSpec(
    summary="A dataset for the tests.",
    name="indices.snapshots",
    asset_class=AssetClass.INDICES,
    fetch_path=INDEX_SNAPSHOT_PATH,
    serves_current_data=True,
    symbol_param="ticker.any_of",
    keys={(R, INDEX_SNAPSHOT_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
)
# A tick dataset over the indices snapshot: each index's level, timed by its `last_updated`.
INDEX_LEVELS = DatasetSpec(
    summary="A dataset for the tests.",
    name="indices.levels",
    asset_class=AssetClass.INDICES,
    row_times=RowTimes.TICK,
    fetch_path=INDEX_SNAPSHOT_PATH,
    serves_current_data=True,
    symbol_param="ticker.any_of",
    keys={(R, INDEX_SNAPSHOT_PATH): SourceKeys(KeySpec("Ticker"), KeySpec("LastUpdated"))},
)
TRADES_PATH = "/v3/trades/{stockTicker}"
TRADES = DatasetSpec(
    summary="A dataset for the tests.",
    name="stocks.trades",
    asset_class=AssetClass.STOCKS,
    history_path=TRADES_PATH,
    time_axis=("timestamp.gte", "timestamp.lt"),
    keys={
        (R, TRADES_PATH): SourceKeys(
            KeySpec("stockTicker", from_request=True), KeySpec("SipTimestamp")
        )
    },
)
RANGE_PATH = "/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}"
GROUPED_PATH = "/v2/aggs/grouped/locale/us/market/stocks/{date}"
BARS = DatasetSpec(
    summary="A dataset for the tests.",
    name="stocks.bars",
    asset_class=AssetClass.STOCKS,
    is_bars=True,
    history_path=RANGE_PATH,
    grouped_daily_path=GROUPED_PATH,
    keys={
        (R, RANGE_PATH): SourceKeys(
            KeySpec("stocksTicker", from_request=True), KeySpec("WindowStart")
        ),
        (R, GROUPED_PATH): SourceKeys(KeySpec("Ticker"), KeySpec("WindowStart")),
    },
)
FUTURES_TRADES_PATH = "/futures/v1/trades/{ticker}"
FUTURES_TRADES = DatasetSpec(
    summary="A dataset for the tests.",
    name="futures.trades",
    asset_class=AssetClass.FUTURES,
    row_times=RowTimes.TICK,
    history_path=FUTURES_TRADES_PATH,
    time_axis=("timestamp.gte", "timestamp.lt"),
    keys={
        (R, FUTURES_TRADES_PATH): SourceKeys(
            KeySpec("ticker", from_request=True),
            KeySpec("Timestamp"),
            date=KeySpec("SessionEndDate"),
        )
    },
)
FUTURES_AGGS_PATH = "/futures/v1/aggs/{ticker}"
FUTURES_BARS = DatasetSpec(
    summary="A dataset for the tests.",
    name="futures.bars",
    asset_class=AssetClass.FUTURES,
    is_bars=True,
    history_path=FUTURES_AGGS_PATH,
    time_axis=("window_start.gte", "window_start.lt"),
    keys={
        (R, FUTURES_AGGS_PATH): SourceKeys(
            KeySpec("ticker", from_request=True),
            KeySpec("WindowStart"),
            date=KeySpec("SessionEndDate"),
        )
    },
)
FOREX_RANGE_PATH = "/v2/aggs/ticker/{forexTicker}/range/{multiplier}/{timespan}/{from}/{to}"
FOREX_GROUPED_PATH = "/v2/aggs/grouped/locale/global/market/fx/{date}"
FOREX_BARS = DatasetSpec(
    summary="A dataset for the tests.",
    name="forex.bars",
    asset_class=AssetClass.FOREX,
    is_bars=True,
    history_path=FOREX_RANGE_PATH,
    grouped_daily_path=FOREX_GROUPED_PATH,
    keys={
        (R, FOREX_RANGE_PATH): SourceKeys(
            KeySpec("forexTicker", from_request=True), KeySpec("WindowStart")
        ),
        (R, FOREX_GROUPED_PATH): SourceKeys(KeySpec("Ticker"), KeySpec("WindowStart")),
    },
)
CRYPTO_RANGE_PATH = "/v2/aggs/ticker/{cryptoTicker}/range/{multiplier}/{timespan}/{from}/{to}"
CRYPTO_BARS = DatasetSpec(
    summary="A dataset for the tests.",
    name="crypto.bars",
    asset_class=AssetClass.CRYPTO,
    is_bars=True,
    history_path=CRYPTO_RANGE_PATH,
    keys={
        (R, CRYPTO_RANGE_PATH): SourceKeys(
            KeySpec("cryptoTicker", from_request=True), KeySpec("WindowStart")
        ),
    },
)
CONSTITUENTS_PATH = "/etf-global/v1/constituents"
CONSTITUENTS = DatasetSpec(
    summary="A dataset for the tests.",
    name="partners.etf_constituents",
    fetch_path=CONSTITUENTS_PATH,
    symbol_param="composite_ticker",
    keys={(R, CONSTITUENTS_PATH): SourceKeys(symbol=KeySpec("CompositeTicker"))},
)
NEWS_PATH = "/v2/reference/news"
NEWS = DatasetSpec(
    summary="A dataset for the tests.",
    name="reference.news",
    fetch_path=NEWS_PATH,
    time_axis=("published_utc.gte", "published_utc.lt"),
    keys={(R, NEWS_PATH): SourceKeys()},
)
LISTED_PATH = "/benzinga/v2/news"
# Rows that list their tickers: the library fills Symbol from the named tickers they list.
LISTED = DatasetSpec(
    summary="A dataset for the tests.",
    name="partners.benzinga.news",
    fetch_path=LISTED_PATH,
    symbol_param="tickers.any_of",
    ticker_list="Tickers",
    poll_keys=("BenzingaId", "Symbol"),
    nullable_poll_keys={"Symbol": "no ticker named"},
)
RATINGS_PATH = "/benzinga/v1/ratings"
RATINGS = DatasetSpec(
    summary="A dataset for the tests.",
    name="partners.ratings",
    fetch_path=RATINGS_PATH,
    keys={(R, RATINGS_PATH): SourceKeys()},
)
NEWS_TICKERS = DatasetSpec(
    summary="A dataset for the tests.",
    name="reference.news_tickers",
    fetch_path=NEWS_PATH,
    symbol_param="ticker",
    time_axis=("published_utc.gte", "published_utc.lt"),
    keys={(R, NEWS_PATH): SourceKeys()},
)
# A path per symbol class makes each request follow its symbols' class, as the indicators do.
NEWS_BY_CLASS = dataclasses.replace(
    NEWS_TICKERS,
    name="reference.news_by_class",
    fetch_paths_by_class={AssetClass.FOREX: NEWS_PATH},
)
CRYPTO_OPEN_CLOSE_PATH = "/v1/open-close/crypto/{from}/{to}/{date}"
CRYPTO_OPEN_CLOSE = DatasetSpec(
    summary="A dataset for the tests.",
    name="crypto.daily_open_close",
    asset_class=AssetClass.CRYPTO,
    fetch_path=CRYPTO_OPEN_CLOSE_PATH,
    per_symbol=True,
    keys={(R, CRYPTO_OPEN_CLOSE_PATH): SourceKeys(symbol=KeySpec("Symbol"))},
)
MIXED = DatasetSpec(
    summary="A dataset for the tests.",
    name="stocks.mixed",
    asset_class=AssetClass.STOCKS,
    fetch_path=DIVIDENDS_PATH,
    extra_paths=(CONTRACT_PATH,),
    keys={
        (R, DIVIDENDS_PATH): SourceKeys(symbol=KeySpec("Ticker")),
        (R, CONTRACT_PATH): SourceKeys(symbol=KeySpec("Ticker")),
    },
)
UNIFIED_PATH = "/v3/snapshot"
# The unified snapshot: tickers of every class, charged to no one subscription.
UNIFIED = DatasetSpec(
    summary="A dataset for the tests.",
    name="snapshots",
    fetch_path=UNIFIED_PATH,
    serves_current_data=True,
    symbol_param="ticker.any_of",
    keys={(R, UNIFIED_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
)
SPECS = (
    UNIFIED,
    LISTED,
    DIVIDENDS,
    MIXED,
    CRYPTO_LAST,
    MOVERS,
    CONTRACTS,
    SMA,
    CONVERSION,
    INDEX_SNAPSHOTS,
    INDEX_LEVELS,
    TRADES,
    BARS,
    FUTURES_BARS,
    FOREX_BARS,
    CRYPTO_BARS,
    CONSTITUENTS,
    NEWS,
    RATINGS,
    NEWS_TICKERS,
    NEWS_BY_CLASS,
    CRYPTO_OPEN_CLOSE,
)

AAPL_DIVIDEND = {
    "ticker": "AAPL",
    "cash_amount": 0.25,
    "distribution_type": "recurring",
    "ex_dividend_date": "2025-02-10",
    "frequency": 4,
    "id": "E1",
}


@pytest.fixture
def session(
    fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> Iterator[dm.MassiveSession]:
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    m = dm.MassiveSession(
        dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url, max_concurrency=4)
    )
    yield m
    m.close()


def _endpoints(m: dm.MassiveSession) -> rest.RestEndpoints:
    transport = m._core.rest_transport()
    assert isinstance(transport, rest.RestEndpoints)
    return transport


_P = ParamSpec("_P")


class ReadArguments(TypedDict, total=False):
    """The keywords `build_query` takes besides the dataset, the query type, and the bounds."""

    symbols: dm.SymbolsLike | None
    symbols_optional: bool
    all_alternative: str | None
    timespan: Timespan | None
    multiplier: int
    venues: AbstractSet[dm.FuturesVenue] | None
    adjusted: bool | None
    params: Mapping[str, object] | None
    arguments: Mapping[str, str] | None
    filters: Mapping[str, dm.FilterValue] | None
    limit: int | None
    values: ValueTable | None
    on_error: dm.ErrorPolicy | None


class QueryArguments(ReadArguments, total=False):
    """Every keyword `build_query` takes besides the session, the dataset, and the query type."""

    start: dm.DateLike | None
    end: dm.DateLike | None


def query(m: dm.MassiveSession, spec: DatasetSpec, **kwargs: Unpack[QueryArguments]) -> Query:
    # An indicator is computed over bars of a timespan, which its factory always sets.
    if spec is SMA:
        kwargs.setdefault("timespan", Timespan.MINUTE)
    # A list dataset's symbols only filter it, as its factory says.
    kwargs.setdefault("symbols_optional", spec.symbol_param is not None)
    return build_query(m._core, spec, query_type=Query, **kwargs)


def fetch(
    m: dm.MassiveSession, spec: DatasetSpec, **kwargs: Unpack[QueryArguments]
) -> list[dict[str, Any]]:
    q = query(m, spec, **kwargs)
    assert isinstance(q, FetchQuery)
    return rows(q.fetch(wait=True))


def rows(table: Table, *, arrival: bool = False) -> list[dict[str, Any]]:
    # Instants become epoch nanoseconds, so assertions keep every digit. Every row carries the
    # instant its page arrived, last; it is checked here and, unless asked for, left out, so
    # an assertion compares only what Massive sent.
    data = dh_arrow.to_arrow(table)
    assert data.column_names[-1] == "TimestampInsert"
    assert data.column("TimestampInsert").null_count == 0
    columns = [c.cast(pa.int64()) if pa.types.is_timestamp(c.type) else c for c in data.columns]
    out: list[dict[str, Any]] = pa.Table.from_arrays(columns, names=data.column_names).to_pylist()
    if not arrival:
        for row in out:
            del row["TimestampInsert"]
    return out


def epoch_ns(moment: datetime) -> int:
    return int(moment.timestamp()) * NS


# --- Requests ----------------------------------------------------------------------------------


def test_fetch_sends_the_typed_parameters_axis_filters_and_page_size(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([AAPL_DIVIDEND])))
    out = fetch(
        session,
        DIVIDENDS,
        symbols=["MSFT", "AAPL", "IBM"],
        start="2025-01-01",
        end=date(2025, 3, 31),
        params={"sort": "ex_dividend_date", "order": Order.DESC},
        filters={"frequency_gte": 4},
    )
    requests = fake_rest.get_queries()
    # Two symbols a request (symbols_per_request), sorted canonical order.
    assert sorted(q["ticker.any_of"][0] for _, q in requests) == ["AAPL,IBM", "MSFT"]
    first = requests[0][1]
    assert first["ex_dividend_date.gte"] == ["2025-01-01"]
    assert first["ex_dividend_date.lte"] == ["2025-03-31"]  # date axes are whole days
    assert first["sort"] == ["ex_dividend_date.desc"]  # no `order` parameter: folded
    assert "order" not in first
    assert first["frequency.gte"] == ["4"]
    assert first["limit"] == ["1000"]  # the page size, never the row cap
    assert [(r["Symbol"], r["CashAmount"], r["ExDividendDate"]) for r in out] == [
        ("AAPL", 0.25, "2025-02-10"),
        ("AAPL", 0.25, "2025-02-10"),
    ]


def test_iso_text_in_a_date_filter_is_sent_as_its_date(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(CONSTITUENTS_PATH, Reply(envelope([])))
    q = query(session, CONSTITUENTS, symbols="SPY", filters={"effective_date": "2025-09-18"})
    assert q._request.filters == (("effective_date", date(2025, 9, 18)),)
    assert isinstance(q, FetchQuery)
    q.fetch(wait=True)
    ((_, sent_query),) = fake_rest.get_queries()
    assert sent_query["effective_date"] == ["2025-09-18"]
    assert sent_query["composite_ticker"] == ["SPY"]


def test_every_filter_kind_reaches_the_wire_as_the_check_parsed_it(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(CONTRACTS_PATH, Reply(envelope([])))
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([])))
    fetch(
        session,
        CONTRACTS,
        filters={
            "underlying_ticker": "NVDA",
            "contract_type": "put",
            "expiration_date_lte": "2026-01-16",
            "as_of": "2025-09-18T09:30:00-04:00",
            "strike_price_gte": 100,
            "strike_price_lt": 150.5,
            "expired": False,
        },
    )
    fetch(session, DIVIDENDS, filters={"frequency": 4, "distribution_type_any_of": "special"})
    (_, contracts), (_, dividends) = fake_rest.get_queries()
    assert {key: values for key, values in contracts.items() if key != "limit"} == {
        "underlying_ticker": ["NVDA"],
        "contract_type": ["put"],
        "expiration_date.lte": ["2026-01-16"],
        "as_of": ["2025-09-18T13:30:00Z"],  # an instant goes out in UTC
        "strike_price.gte": ["100"],
        "strike_price.lt": ["150.5"],
        "expired": ["false"],
    }
    assert dividends["frequency"] == ["4"]
    assert dividends["distribution_type.any_of"] == ["special"]  # bare text: a list of one


def test_a_date_on_an_instant_filter_is_sent_as_its_days_bound(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(RATINGS_PATH, Reply(envelope([])))
    fetch(
        session,
        RATINGS,
        filters={
            "last_updated_gte": "2025-06-02",
            "last_updated_lt": date(2025, 6, 4),
            "date_gte": "2025-06-01",
            "ticker_any_of": "AAPL,MSFT",
        },
    )
    ((_, sent_query),) = fake_rest.get_queries()
    # `last_updated` is an instant field behind a date-typed filter: its days start in New
    # York. `date` is a calendar date and keeps the day.
    assert sent_query["last_updated.gte"] == ["2025-06-02T04:00:00Z"]
    assert sent_query["last_updated.lt"] == ["2025-06-04T04:00:00Z"]
    assert sent_query["date.gte"] == ["2025-06-01"]
    assert sent_query["ticker.any_of"] == ["AAPL,MSFT"]
    with pytest.raises(ValueError, match=r"last_updated\.lte has no exact unit to end a day"):
        query(session, RATINGS, filters={"last_updated_lte": "2025-06-02"})


def test_a_crypto_indicator_takes_utc_days_and_a_stock_new_york_days(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # The indicators have no asset class: each request follows its symbol's class.
    fake_rest.reply("/v1/indicators/sma/X%3ABTCUSD", Reply(envelope({"values": []})))
    fake_rest.reply("/v1/indicators/sma/AAPL", Reply(envelope({"values": []})))
    fetch(session, SMA, symbols=["X:BTCUSD", "AAPL"], start="2025-03-03", end="2025-03-03")
    by_path = dict(fake_rest.get_queries())
    utc_day = epoch_ns(datetime(2025, 3, 3, tzinfo=UTC)) // 10**6
    new_york_day = epoch_ns(datetime(2025, 3, 3, 5, tzinfo=UTC)) // 10**6
    one_day = 86_400_000
    assert by_path["/v1/indicators/sma/X:BTCUSD"]["timestamp.gte"] == [str(utc_day)]
    assert by_path["/v1/indicators/sma/X:BTCUSD"]["timestamp.lt"] == [str(utc_day + one_day)]
    assert by_path["/v1/indicators/sma/AAPL"]["timestamp.gte"] == [str(new_york_day)]


class _Patiences:
    """Record each read's patience and the patience every claim is opened with."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.taken: list[_massive.Patience] = []
        self.claims: list[tuple[str, _massive.Patience]] = []
        admission = _massive.RequestGovernor.admission
        claims = self.claims

        def recording(
            governor: _massive.RequestGovernor,
            bucket: str,
            *,
            lane: object,
            patience: _massive.Patience,
        ) -> object:
            claims.append((bucket, patience))
            return admission(governor, bucket, lane=lane, patience=patience)

        taken = self.taken

        def patience(_reader: rest.RestReader) -> _massive.Patience:
            taken.append(_massive.Patience(600.0))
            return taken[-1]

        monkeypatch.setattr(rest.RestReader, "patience", patience)
        monkeypatch.setattr(_massive.RequestGovernor, "admission", recording)

    def shared(self) -> bool:
        """Tell whether one patience was taken and every claim was opened with it.

        Returns:
            True when so.
        """
        return len(self.taken) == 1 and all(p is self.taken[0] for _, p in self.claims)


def test_a_read_of_several_paths_shares_one_patience(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The stock and crypto indicator paths are one fetch: its patience is taken once, so the
    # second path never gets a fresh ten minutes after the first spent its own.
    fake_rest.reply("/v1/indicators/sma/X%3ABTCUSD", Reply(envelope({"values": []})))
    fake_rest.reply("/v1/indicators/sma/AAPL", Reply(envelope({"values": []})))
    patiences = _Patiences(monkeypatch)
    fetch(session, SMA, symbols=["X:BTCUSD", "AAPL"], start="2025-03-03", end="2025-03-03")
    assert len(patiences.claims) == 2
    assert patiences.shared()


def test_a_history_read_takes_one_patience_for_its_pages(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    start = epoch_ns(datetime(2025, 3, 3, 15, tzinfo=UTC))
    target = "/v3/trades/AAPL?cursor=next"
    fake_rest.reply(target, Reply(envelope([{"sip_timestamp": start + 1, "price": 2.0}])))
    fake_rest.reply(
        TRADES_PATH.format(stockTicker="AAPL"),
        Reply(
            envelope([{"sip_timestamp": start, "price": 1.0}], next_url=f"{fake_rest.url}{target}")
        ),
    )
    patiences = _Patiences(monkeypatch)
    history(session, TRADES, start="2025-03-03", end="2025-03-03", symbols=["AAPL"])
    assert [bucket for bucket, _ in patiences.claims] == ["stocks", "stocks"]  # both pages
    assert patiences.shared()


def test_a_host_decision_takes_a_patience_of_its_own(
    *, session: dm.MassiveSession, snapshots: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    patiences = _Patiences(monkeypatch)
    session._core.rest_transport().resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    assert [bucket for bucket, _ in patiences.claims] == ["stocks"]
    assert patiences.shared()


def test_the_futures_calendars_lookups_take_a_patience_each(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_rest.reply("/futures/v1/schedules", Reply(envelope([{"product_code": "ES"}])))
    patiences = _Patiences(monkeypatch)
    transport = _endpoints(session)
    rows = transport.fetch_rows(
        "/futures/v1/schedules", {"product_code": "ES"}, bucket="futures", cancel=Cancel()
    )
    assert rows == [{"product_code": "ES"}]
    assert [bucket for bucket, _ in patiences.claims] == ["futures"]
    assert patiences.shared()


def test_the_crypto_ticker_list_takes_one_patience_for_its_pages(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_rest.reply(
        "/v3/reference/tickers?market=crypto&active=true&limit=1000",
        Reply(envelope([_ticker("X:DOTUSD", "DOT", "USD")])),
    )
    fake_rest.reply(
        "/v3/reference/tickers?market=crypto&active=false&limit=1000", Reply(envelope([]))
    )
    patiences = _Patiences(monkeypatch)
    reader = _endpoints(session)._reader("crypto pairs", session._core.policy)
    assert reader.crypto_pair("X:DOTUSD") == ("DOT", "USD")
    assert reader.crypto_pair("X:DOTUSD") == ("DOT", "USD")  # the kept list takes no patience
    assert [bucket for bucket, _ in patiences.claims] == ["other", "other"]
    assert patiences.shared()


def test_an_aware_datetime_on_an_inclusive_instant_filter_is_sent_as_given(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(RATINGS_PATH, Reply(envelope([])))
    noon = datetime(2025, 6, 2, 12, 30, tzinfo=ZoneInfo("America/New_York"))
    fetch(session, RATINGS, filters={"last_updated_lte": noon, "last_updated_gt": noon})
    ((_, sent_query),) = fake_rest.get_queries()
    assert sent_query["last_updated.lte"] == ["2025-06-02T16:30:00Z"]
    assert sent_query["last_updated.gt"] == ["2025-06-02T16:30:00Z"]


def test_a_list_of_dates_takes_date_items(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(RATINGS_PATH, Reply(envelope([])))
    fetch(session, RATINGS, filters={"date_any_of": [date(2025, 6, 2), "2025-06-03"]})
    ((_, sent_query),) = fake_rest.get_queries()
    assert sent_query["date.any_of"] == ["2025-06-02,2025-06-03"]
    with pytest.raises(ValueError, match=r"filter 'ticker_any_of' takes no dates"):
        query(session, RATINGS, filters={"ticker_any_of": [date(2025, 6, 2)]})


def test_each_ticker_takes_its_own_class_days_on_a_dataset_with_a_path_per_class(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # 2025-03-09 is the day New York moves its clocks forward: its day is 23 hours long.
    fake_rest.reply(NEWS_PATH, Reply(envelope([])))
    fetch(
        session, NEWS_BY_CLASS, symbols=["X:BTCUSD", "AAPL"], start="2025-03-09", end="2025-03-09"
    )
    bounds = sorted(
        (q["ticker"][0], q["published_utc.gte"][0], q["published_utc.lt"][0])
        for _, q in fake_rest.get_queries()
    )
    assert bounds == [
        ("AAPL", "2025-03-09T05:00:00Z", "2025-03-10T04:00:00Z"),
        ("X:BTCUSD", "2025-03-09T00:00:00Z", "2025-03-10T00:00:00Z"),
    ]


def test_any_other_dataset_without_a_class_keeps_new_york_days(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(NEWS_PATH, Reply(envelope([])))
    fetch(session, NEWS_TICKERS, symbols=["X:BTCUSD", "AAPL"], start="2025-03-09", end="2025-03-09")
    assert {q["published_utc.gte"][0] for _, q in fake_rest.get_queries()} == {
        "2025-03-09T05:00:00Z"
    }


def test_a_currency_pair_path_fills_from_and_to_from_each_symbol(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # The rows carry their own symbol, so no key fills the pair from the request.
    body = {
        "symbol": "BTC-USD",
        "isUTC": True,
        "day": "2025-03-03T00:00:00Z",
        "open": 90000.5,
        "close": 91000.25,
        "openTrades": [],
        "closingTrades": [],
    }
    fake_rest.reply("/v1/open-close/crypto/BTC/USD/2025-03-03", Reply(body))
    serve_pairs(fake_rest)
    out = fetch(session, CRYPTO_OPEN_CLOSE, symbols=["X:BTCUSD"], params={"date": date(2025, 3, 3)})
    assert [path for path, _ in fake_rest.get_queries()] == [
        TICKERS_PATH,  # the pair's split comes from Massive's listed crypto tickers,
        TICKERS_PATH,  # and its delisted ones
        "/v1/open-close/crypto/BTC/USD/2025-03-03",
    ]
    assert [(r["Symbol"], r["Open"]) for r in out] == [("X:BTCUSD", 90000.5)]


def _ticker(symbol: str, base: str, quote: str) -> dict[str, object]:
    # One row of /v3/reference/tickers for a crypto pair.
    return {
        "ticker": symbol,
        "market": "crypto",
        "base_currency_symbol": base,
        "currency_symbol": quote,
    }


TICKERS_PATH = "/v3/reference/tickers"
PAIRS = {
    "X:BTCUSD": ("BTC", "USD"),
    "X:ETHUSD": ("ETH", "USD"),
    "X:SOLUSD": ("SOL", "USD"),
    "X:XRPUSD": ("XRP", "USD"),
}
"""The crypto pairs these tests use, as Massive's crypto ticker list splits them."""


def serve_pairs(server: FakeRestServer) -> None:
    # Massive's crypto ticker list, which a `{from}/{to}` path reads once per session.
    # It is read as two lists: the listed pairs, then the delisted ones (none here).
    rows = [_ticker(symbol, base, quote) for symbol, (base, quote) in PAIRS.items()]
    server.reply(TICKERS_PATH, Reply(envelope(rows)), Reply(envelope([])))


def sent_data(server: FakeRestServer) -> list[tuple[str, dict[str, list[str]]]]:
    # Every GET but the read of the crypto ticker list.
    return [(path, query) for path, query in server.get_queries() if path != TICKERS_PATH]


def split_pair(symbol: str) -> tuple[str, str]:
    # The split of a crypto pair, as `RestReader.crypto_pair` reads it from the list.
    return PAIRS[symbol]


LIST_QUERY = {"market": ["crypto"], "active": ["true"], "limit": ["1000"]}
"""The query of the first page of Massive's list of listed crypto tickers."""
DELISTED_QUERY = {"market": ["crypto"], "active": ["false"], "limit": ["1000"]}
"""The query of the first page of Massive's list of delisted crypto tickers."""


def test_an_ambiguous_crypto_pair_takes_its_split_from_the_ticker_list_read_once(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # X:DOTUSD ends in USD and in TUSD: Massive's list says where it splits, never a guess.
    # The whole list is read once, every page, and serves every later pair of the session.
    second = f"{fake_rest.url}/v3/reference/tickers?cursor=two"
    first_page = {**envelope([_ticker("X:DOTUSD", "DOT", "USD")]), "next_url": second}
    fake_rest.reply("/v3/reference/tickers?market=crypto&active=true&limit=1000", Reply(first_page))
    fake_rest.reply(
        "/v3/reference/tickers?market=crypto&active=false&limit=1000", Reply(envelope([]))
    )
    fake_rest.reply(
        "/v3/reference/tickers?cursor=two",
        Reply(envelope([_ticker("X:SHIBUSD", "SHIB", "USD"), _ticker("X:ETHWBTC", "ETH", "WBTC")])),
    )
    body = {"symbol": "DOT-USD", "isUTC": True, "day": "2025-03-03T00:00:00Z", "open": 4.5}
    fake_rest.reply("/v1/open-close/crypto/DOT/USD/2025-03-03", Reply(body), Reply(body))
    for _ in range(2):
        out = fetch(
            session, CRYPTO_OPEN_CLOSE, symbols=["X:DOTUSD"], params={"date": date(2025, 3, 3)}
        )
        assert [(r["Symbol"], r["Open"]) for r in out] == [("X:DOTUSD", 4.5)]
    reader = _endpoints(session)._reader("call-1", session._core.policy)
    assert reader.crypto_pair("X:SHIBUSD") == ("SHIB", "USD")  # from the second page
    # It ends in BTC, which is a quote too; only the list says its quote is WBTC.
    assert reader.crypto_pair("X:ETHWBTC") == ("ETH", "WBTC")
    assert fake_rest.get_queries() == [
        ("/v3/reference/tickers", LIST_QUERY),
        ("/v3/reference/tickers", {"cursor": ["two"]}),
        ("/v3/reference/tickers", DELISTED_QUERY),
        ("/v1/open-close/crypto/DOT/USD/2025-03-03", {}),
        ("/v1/open-close/crypto/DOT/USD/2025-03-03", {}),  # the list is kept
    ]


def test_a_host_decision_that_breaks_is_a_library_defect_logged_at_error(
    *, session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def broken(*_: object, **__: object) -> NoReturn:
        raise RuntimeError("the rule broke")

    monkeypatch.setattr(host_decision, "decide", broken)
    with (
        caplog.at_level(logging.WARNING, LOGGER),
        pytest.raises(RuntimeError, match="the rule broke"),
    ):
        session._core.rest_transport().resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    [record] = caplog.records
    assert (record.levelno, record.getMessage()) == (
        logging.ERROR,
        f"deciding the stocks WebSocket host failed: RuntimeError('the rule broke'). {LIBRARY_BUG}",
    )


class _WatchedFuture(Future[CryptoTickers]):
    # A future that says when a caller starts waiting on it.
    def __init__(self) -> None:
        super().__init__()
        self.waiting = threading.Event()

    @override
    def add_done_callback(self, fn: Callable[[Future[CryptoTickers]], object]) -> None:
        # A caller that joins the list asks to hear when it completes, then waits.
        super().add_done_callback(fn)
        self.waiting.set()


def _wait_for_pair(
    reader: rest.RestReader, future: _WatchedFuture
) -> tuple[list[object], threading.Thread]:
    # Call crypto_pair on another thread while `future` is the list being read: the thread
    # waits on it, and puts what the call returned or raised in the list once it completes.
    reader.state.crypto._lookup._futures[LIST_KEY] = future  # another thread claimed it
    reader.state.crypto._lookup._claimed[LIST_KEY] = 0.0
    results: list[object] = []

    def wait() -> None:
        try:
            results.append(reader.crypto_pair("X:DOTUSD"))
        except dm.MassiveError as error:
            results.append(error)

    waiter = threading.Thread(target=wait)
    waiter.start()
    assert future.waiting.wait(30)
    return results, waiter


@pytest.mark.parametrize("outcome", ["fails", "succeeds"])
def test_a_caller_waiting_on_the_crypto_ticker_list_takes_its_outcome(
    *, session: dm.MassiveSession, outcome: str
) -> None:
    reader = _endpoints(session)._reader("call-1", session._core.policy)
    future = _WatchedFuture()
    results, waiter = _wait_for_pair(reader, future)
    refused = dm.EntitlementError("refused", status=403)
    if outcome == "fails":
        future.set_exception(refused)
    else:
        future.set_result({"X:DOTUSD": ("DOT", "USD")})
    waiter.join(30)
    (result,) = results
    if outcome == "fails":
        assert isinstance(result, dm.EntitlementError)
        assert str(result).endswith("reading Massive's crypto tickers failed: refused")
        # Each waiter raises a copy, so no exception object is raised on two threads.
        assert isinstance(result.__cause__, dm.EntitlementError)
        assert result.__cause__ is not refused
        assert result.__cause__.status == 403
    else:
        assert result == ("DOT", "USD")


class _Caller:
    # A thread that makes one call, holding the update graph's shared lock as a console
    # command holds its lock when `console` is set, and keeps what the call returned or raised.
    def __init__(self, m: dm.MassiveSession, call: Callable[[], object], *, console: bool) -> None:
        self.result: object = None
        self._m = m
        self._call = call
        self._console = console
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def _run(self) -> None:
        if self._console:
            # Deephaven's lock wraps an error raised inside it, so the call keeps its own.
            with shared_lock(self._m._core.exec_ctx.update_graph):
                self._make()
        else:
            self._make()

    def _make(self) -> None:
        try:
            self.result = self._call()
        except (dm.MassiveError, KeyboardInterrupt) as error:
            self.result = error

    def join(self) -> object:
        self._thread.join(TIMEOUT)
        assert not self._thread.is_alive()
        return self.result


class _Listeners:
    """Record the listeners subscribed to one governor bucket that are still subscribed."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.buckets: list[str] = []
        self.live: list[Callable[[], None]] = []
        subscribe = _massive.RequestGovernor.subscribe_bucket
        unsubscribe = _massive.RequestGovernor.unsubscribe

        def subscribing(
            governor: _massive.RequestGovernor, bucket: str, listener: Callable[[], None]
        ) -> None:
            self.buckets.append(bucket)
            self.live.append(listener)
            subscribe(governor, bucket, listener)

        def unsubscribing(governor: _massive.RequestGovernor, listener: Callable[[], None]) -> None:
            if listener in self.live:
                self.live.remove(listener)
            unsubscribe(governor, listener)

        monkeypatch.setattr(_massive.RequestGovernor, "subscribe_bucket", subscribing)
        monkeypatch.setattr(_massive.RequestGovernor, "unsubscribe", unsubscribing)


def _manual_session(
    fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> tuple[dm.MassiveSession, FakeClock]:
    # A session whose governor runs on a manual clock: each pause lasts until the test moves
    # time past it.
    clock = FakeClock(manual=True)
    create = functools.partial(
        _massive.RequestGovernor,
        rate_limits.BUCKETS,
        learn_only=rate_limits.LEARN_ONLY,
        clock=clock,
        labels=rate_limits.LABELS,
    )
    monkeypatch.setattr(rate_limits, "GOVERNORS", rate_limits.GovernorRegistry(create))
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    return m, clock


PAUSE = Reply(
    {"status": "ERROR", "message": "slow down"}, status=429, headers={"Retry-After": "60"}
)
"""Massive's refusal of a request, asking for a pause of 60 s."""
LIST_TARGET = "/v3/reference/tickers?market=crypto&active=true&limit=1000"
"""The first page of Massive's list of listed crypto tickers."""
DELISTED_TARGET = "/v3/reference/tickers?market=crypto&active=false&limit=1000"
"""The first page of Massive's list of delisted crypto tickers."""


def _split_dot(m: dm.MassiveSession) -> Callable[[], object]:
    # A call that splits X:DOTUSD from the session's crypto ticker list.
    reader = _endpoints(m)._reader("crypto pairs", m._core.policy)
    return functools.partial(reader.crypto_pair, "X:DOTUSD")


def _gave_up_after_two_pauses(result: object) -> None:
    # The caller holding the update graph's lock waits out 90 s of pauses: the second 60 s
    # pause takes it past that, after it spent 60 s.
    assert isinstance(result, dm.MassiveError)
    assert result.status == 429
    assert re.search(
        r"rate limited: Massive is refusing .*this read waits out at most 90 s of pauses and "
        r"has spent 60 s",
        str(result),
    )


@pytest.mark.parametrize("console", ["joiner", "maker", "neither"])
def test_a_caller_joining_the_crypto_ticker_list_waits_with_its_own_patience(
    *,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    console: str,
) -> None:
    # Two callers need the list at once; `console` names the one inside a console command
    # (90 s of pauses), the other waits out 600 s. Massive pauses the list twice for 60 s.
    m, clock = _manual_session(fake_rest, monkeypatch)
    listeners = _Listeners(monkeypatch)
    fake_rest.reply(LIST_TARGET, PAUSE, PAUSE, Reply(envelope([_ticker("X:DOTUSD", "DOT", "USD")])))
    fake_rest.reply(DELISTED_TARGET, Reply(envelope([])))
    caplog.set_level(logging.DEBUG, LOGGER)
    caplog.set_level(logging.DEBUG, SHARED_LOGGER)
    try:
        maker = _Caller(m, _split_dot(m), console=console == "maker")
        clock.wait_for_deadlines([60.0])  # the maker waits out Massive's first pause
        joiner = _Caller(m, _split_dot(m), console=console == "joiner")
        clock.wait_for_deadlines([60.0, math.inf])  # the joiner shares it, charged 60 s
        clock.advance(60.0)  # the list's next request is refused: 60 s more, 120 s in all
        if console == "neither":
            clock.wait_for_deadlines([120.0, math.inf])
        else:
            _gave_up_after_two_pauses((joiner if console == "joiner" else maker).join())
            # The other caller reads on: the maker its own lookup, or the joiner a new one
            # of its own, since the maker's giving up says nothing about the list.
            clock.wait_for_deadlines([120.0])
        clock.advance(60.0)
        if console != "maker":
            assert maker.join() == ("DOT", "USD")
        if console != "joiner":
            assert joiner.join() == ("DOT", "USD")
    finally:
        m.close()
    assert len(fake_rest.get_queries()) == 4  # the list's three attempts, then the delisted page
    assert (listeners.buckets, listeners.live) == (["other"], [])  # heard, then removed
    patience = 90 if console == "joiner" else 600
    assert (
        f"{m._core.label}: waits for the crypto ticker list another caller is making, through "
        f"at most {patience} s of its pauses"
    ) in caplog.messages
    stopped = [r for r in caplog.records if "stops waiting for the crypto" in r.getMessage()]
    # The caller raises the error, so giving up on the shared wait is only DEBUG detail.
    assert [r.levelno for r in stopped] == ([logging.DEBUG] if console == "joiner" else [])
    retried = [r for r in caplog.records if "this caller tries again" in r.getMessage()]
    assert [r.levelno for r in retried] == ([logging.INFO] if console == "maker" else [])


def test_a_caller_waiting_on_an_interrupted_lookup_makes_it_itself(
    *, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    m, clock = _manual_session(fake_rest, monkeypatch)
    fake_rest.reply(LIST_TARGET, Reply(envelope([_ticker("X:DOTUSD", "DOT", "USD")])))
    fake_rest.reply(DELISTED_TARGET, Reply(envelope([])))
    client = _endpoints(m)._state.client
    claimed = threading.Event()
    interrupt = threading.Event()

    def interrupting(
        pages: Callable[_P, Iterator[_massive.Page]],
    ) -> Callable[_P, Iterator[_massive.Page]]:
        def interrupted(*args: _P.args, **kwargs: _P.kwargs) -> Iterator[_massive.Page]:
            if not claimed.is_set():  # the maker, interrupted (as by Ctrl-C) once a caller joined
                claimed.set()
                assert interrupt.wait(TIMEOUT)
                raise KeyboardInterrupt
            return pages(*args, **kwargs)

        return interrupted

    monkeypatch.setattr(client, "pages", interrupting(client.pages))
    caplog.set_level(logging.INFO, LOGGER)
    caplog.set_level(logging.INFO, SHARED_LOGGER)
    try:
        maker = _Caller(m, _split_dot(m), console=False)
        assert claimed.wait(TIMEOUT)
        joiner = _Caller(m, _split_dot(m), console=True)
        clock.wait_for_deadlines([math.inf])  # the joiner waits on the maker's lookup
        interrupt.set()
        assert isinstance(maker.join(), KeyboardInterrupt)
        assert joiner.join() == ("DOT", "USD")  # it read the list itself
    finally:
        m.close()
    assert len(fake_rest.get_queries()) == 2
    assert any(
        "the caller making the crypto ticker list stopped (KeyboardInterrupt())" in message
        for message in caplog.messages
    )


def test_a_stopped_read_stops_its_crypto_ticker_list_and_a_waiter_reads_it_itself(
    *, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # The maker is a background read released while Massive pauses the list: it stops at
    # once, and the caller waiting on it reads the list itself instead of failing with the
    # maker's cancellation.
    m, clock = _manual_session(fake_rest, monkeypatch)
    fake_rest.reply(LIST_TARGET, PAUSE, Reply(envelope([_ticker("X:DOTUSD", "DOT", "USD")])))
    fake_rest.reply(DELISTED_TARGET, Reply(envelope([])))
    stop = Cancel()
    reader = _endpoints(m)._reader("crypto pairs", m._core.policy)
    caplog.set_level(logging.INFO, LOGGER)
    caplog.set_level(logging.INFO, SHARED_LOGGER)
    try:
        maker = _Caller(
            m, functools.partial(reader.crypto_pair, "X:DOTUSD", cancel=stop), console=False
        )
        clock.wait_for_deadlines([60.0])  # the maker waits out Massive's pause
        joiner = _Caller(m, _split_dot(m), console=False)
        clock.wait_for_deadlines([60.0, math.inf])
        stop.set()  # the maker's table is released
        stopped = maker.join()
        assert isinstance(stopped, dm.MassiveError)
        assert "cancelled" in str(stopped)
        clock.wait_for_deadlines([60.0])  # the joiner reads the list, after the same pause
        clock.advance(60.0)
        assert joiner.join() == ("DOT", "USD")
    finally:
        m.close()
    assert len(fake_rest.get_queries()) == 3  # the maker's paused page, then the joiner's two lists
    assert any(
        "the caller making the crypto ticker list stopped (its read was stopped:" in message
        for message in caplog.messages
    )


def test_a_refusal_as_the_maker_stops_is_copied_to_its_waiters(
    *, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Massive refuses the maker's request just as its background read is released: the
    # refusal is Massive's answer, not the stop's cancellation, so the waiter raises a copy of
    # it rather than asking again.
    m, clock = _manual_session(fake_rest, monkeypatch)
    client = _endpoints(m)._state.client
    claimed = threading.Event()
    refuse = threading.Event()
    stop = Cancel()
    requests: list[object] = []

    def refused(*args: object, **kwargs: object) -> Iterator[_massive.Page]:
        requests.append(args)
        claimed.set()
        assert refuse.wait(TIMEOUT)
        stop.set()  # the maker's table is released as the refusal arrives
        raise _massive.WireError(
            "HTTP 403 for GET /v3/reference/tickers", status=403, kind=_massive.WireFailure.REFUSED
        )

    monkeypatch.setattr(client, "pages", refused)
    reader = _endpoints(m)._reader("crypto pairs", m._core.policy)
    try:
        maker = _Caller(
            m, functools.partial(reader.crypto_pair, "X:DOTUSD", cancel=stop), console=False
        )
        assert claimed.wait(TIMEOUT)
        joiner = _Caller(m, _split_dot(m), console=False)
        clock.wait_for_deadlines([math.inf])  # the joiner waits on the maker's lookup
        refuse.set()
        made, copied = maker.join(), joiner.join()
    finally:
        m.close()
    assert isinstance(made, dm.MassiveError)
    assert isinstance(copied, dm.MassiveError)
    assert copied is not made
    assert (type(copied), str(copied), copied.status) == (type(made), str(made), 403)
    assert len(requests) == 1  # the waiter copied the refusal and asked nothing itself


def test_a_stopped_read_stops_waiting_on_the_crypto_ticker_list(
    *, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The waiter is the background read released: it stops waiting at once, and the maker
    # reads on.
    m, clock = _manual_session(fake_rest, monkeypatch)
    listeners = _Listeners(monkeypatch)
    fake_rest.reply(LIST_TARGET, PAUSE, Reply(envelope([_ticker("X:DOTUSD", "DOT", "USD")])))
    fake_rest.reply(DELISTED_TARGET, Reply(envelope([])))
    stop = Cancel()
    reader = _endpoints(m)._reader("crypto pairs", m._core.policy)
    try:
        maker = _Caller(m, _split_dot(m), console=False)
        clock.wait_for_deadlines([60.0])
        joiner = _Caller(
            m, functools.partial(reader.crypto_pair, "X:DOTUSD", cancel=stop), console=False
        )
        clock.wait_for_deadlines([60.0, math.inf])
        stop.set()
        stopped = joiner.join()
        assert isinstance(stopped, dm.MassiveError)
        assert str(stopped).endswith(
            f"waiting for the crypto ticker list: cancelled; {READ_STOPPED}"
        )
        clock.advance(60.0)
        assert maker.join() == ("DOT", "USD")
    finally:
        m.close()
    assert len(fake_rest.get_queries()) == 3
    assert listeners.live == []  # the waiter's listeners are gone


def test_a_stopped_fetch_reads_no_crypto_ticker_list(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # Planning a crypto pair's requests listens to the read's stop token.
    rest = session._core.rest_transport()
    pair = query(session, CRYPTO_OPEN_CLOSE, symbols=["X:DOTUSD"], params={"date": "2025-03-03"})
    stop = Cancel()
    stop.set()
    with pytest.raises(dm.MassiveError, match="cancelled"):
        rest.fetch(CRYPTO_OPEN_CLOSE, pair._request, call=1, stop=stop)
    assert fake_rest.requests == []


def test_a_stopped_history_splits_its_pairs_with_its_stop_token(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A REST history's planning splits its crypto pairs with a lookup that listens to the
    # read's stop token, so a released read reads no ticker list.
    transport = session._core.rest_transport()
    splits: list[Callable[[str], tuple[str, str]]] = []

    def planned(*_: object, split: Callable[[str], tuple[str, str]], **__: object) -> None:
        splits.append(split)
        raise KeyboardInterrupt  # the plan is all this test needs

    monkeypatch.setattr(rest, "_history_plan", planned)
    q = query(session, CRYPTO_BARS, symbols=["X:DOTUSD"], timespan=Timespan.DAY)
    day = date(2025, 3, 3)
    request = dataclasses.replace(q._request, start=day, end=day)
    window = history_window(CRYPTO_BARS, request, start=day, end=day, classes=())
    stop = Cancel()
    stop.set()
    with pytest.raises(KeyboardInterrupt):
        transport.history(CRYPTO_BARS, request, window, call=1, tally=ReadTally(), stop=stop)
    (split,) = splits
    with pytest.raises(dm.MassiveError, match="cancelled"):
        split("X:DOTUSD")
    assert fake_rest.requests == []


@pytest.mark.parametrize("console", ["joiner", "maker"])
def test_a_caller_waiting_on_a_host_decision_waits_with_its_own_patience(
    *,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    load_fixture: Callable[[str, str], bytes],
    console: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    m, clock = _manual_session(fake_rest, monkeypatch)
    listeners = _Listeners(monkeypatch)
    snapshot = Reply(load_fixture("ingest/host_decision", "stocks_real_time.json"))
    fake_rest.reply("/v3/snapshot", PAUSE, PAUSE, snapshot)

    def decide() -> object:
        return m._core.rest_transport().resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)

    try:
        maker = _Caller(m, decide, console=console == "maker")
        clock.wait_for_deadlines([60.0])
        joiner = _Caller(m, decide, console=console == "joiner")
        clock.wait_for_deadlines([60.0, math.inf])
        clock.advance(60.0)
        _gave_up_after_two_pauses((joiner if console == "joiner" else maker).join())
        clock.wait_for_deadlines([120.0])  # the other caller decides on
        clock.advance(60.0)
        decision = (maker if console == "joiner" else joiner).join()
    finally:
        m.close()
    assert isinstance(decision, HostDecision)
    assert decision.host == "socket.massive.com"
    assert len(fake_rest.get_queries()) == 3
    assert (listeners.buckets, listeners.live) == (["stocks"], [])
    # The caller that gave up raises; its expected failure logs nothing above DEBUG.
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING and r.name == LOGGER]


def _lookup(m: dm.MassiveSession, kind: str) -> Callable[[], object]:
    # A call that needs one of the session's shared lookups: the crypto ticker list or the
    # stocks WebSocket host decision.
    if kind == "crypto":
        return _split_dot(m)
    transport = m._core.rest_transport()
    return functools.partial(transport.resolve_host, AssetClass.STOCKS, ChannelFamily.TICK)


def _claim(m: dm.MassiveSession, kind: str) -> tuple[Future[Any], bool]:
    # Claim the shared lookup of `kind`, as a caller does before making or joining it.
    state = _endpoints(m)._state
    if kind == "crypto":
        return state.crypto._lookup._claim(LIST_KEY)
    return state.hosts._claim((AssetClass.STOCKS, ChannelFamily.TICK))


@pytest.mark.parametrize("kind", ["crypto", "host"])
@pytest.mark.parametrize(
    "failure",
    [
        shared_lookup._AbandonedError("rate limited: the maker gave up"),
        dm.MassiveError("the read failed: boom", status=500),
    ],
    ids=["abandoned", "failed"],
)
def test_a_failed_lookup_is_never_returned_as_known(
    *, session: dm.MassiveSession, kind: str, failure: Exception
) -> None:
    # A caller can read a lookup's future just before its maker forgets it and fails it. The
    # quick check must then say "not known", so the caller claims or waits as usual: it must
    # neither raise the maker's abandonment, which is never raised, nor the maker's own error
    # object, which every other caller gets a copy of.
    future, created = _claim(session, kind)
    assert created
    future.set_exception(failure)  # failed, and not yet forgotten: what the late reader sees
    state = _endpoints(session)._state
    if kind == "crypto":
        assert state.crypto._lookup.known(LIST_KEY) is None
    else:
        assert state.hosts.known((AssetClass.STOCKS, ChannelFamily.TICK)) is None


@pytest.mark.parametrize("kind", ["crypto", "host"])
@pytest.mark.parametrize(
    "failure",
    [
        shared_lookup._AbandonedError("rate limited: the maker gave up"),
        dm.MassiveError("the read failed: boom", status=500),
    ],
    ids=["abandoned", "failed"],
)
def test_a_lookup_that_fails_as_a_late_caller_looks_is_made_again(
    *,
    session: dm.MassiveSession,
    snapshots: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    failure: Exception,
) -> None:
    # The late caller reads the claimed lookup under the lock; before it looks at it, the
    # maker forgets the lookup and fails it. The late caller must make the lookup itself.
    snapshots.reply(LIST_TARGET, Reply(envelope([_ticker("X:DOTUSD", "DOT", "USD")])))
    snapshots.reply(DELISTED_TARGET, Reply(envelope([])))
    state = _endpoints(session)._state
    key = (AssetClass.STOCKS, ChannelFamily.TICK)
    claimed, created = _claim(session, kind)
    assert created
    succeeded = shared_lookup._succeeded_result
    lookup = state.crypto._lookup if kind == "crypto" else state.hosts
    claimed_key: Any = LIST_KEY if kind == "crypto" else key

    def maker_fails_meanwhile(future: Future[Any] | None) -> object:
        if future is claimed and not claimed.done():
            with lookup._lock:  # forgotten, then failed, as the maker's `fail` does
                del lookup._futures[claimed_key]
                del lookup._claimed[claimed_key]
            claimed.set_exception(failure)
        return succeeded(future)

    monkeypatch.setattr(shared_lookup, "_succeeded_result", maker_fails_meanwhile)
    result = _lookup(session, kind)()
    assert claimed.done()  # the race really ran
    if kind == "crypto":
        assert result == ("DOT", "USD")
    else:
        assert isinstance(result, HostDecision)
        assert result.host == "socket.massive.com"


@pytest.mark.parametrize("kind", ["crypto", "host"])
def test_an_interrupt_before_a_lookup_leaves_nothing_claimed(
    *, session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    def interrupted(_: object) -> NoReturn:  # Ctrl-C while asking Deephaven for the patience
        raise KeyboardInterrupt

    monkeypatch.setattr(rest, "read_patience", interrupted)
    with pytest.raises(KeyboardInterrupt):
        _lookup(session, kind)()
    assert _claim(session, kind)[1]  # nobody holds a lookup that nobody would complete


@pytest.mark.parametrize("kind", ["crypto", "host"])
def test_a_caller_whose_lookup_is_abandoned_before_it_looks_makes_it_itself(
    *,
    session: dm.MassiveSession,
    snapshots: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    # The caller claims a lookup another caller is making, which that caller abandons before
    # this one first looks at it: it must try again, not raise the abandonment.
    snapshots.reply(LIST_TARGET, Reply(envelope([_ticker("X:DOTUSD", "DOT", "USD")])))
    snapshots.reply(DELISTED_TARGET, Reply(envelope([])))
    claim = SharedLookup._claim
    abandoned: Future[Any] = Future()
    abandoned.set_exception(shared_lookup._AbandonedError("rate limited: the maker gave up"))
    claims = iter([(abandoned, False)])

    def claiming(state: SharedLookup[Any, Any], key: object) -> tuple[Future[Any], bool]:
        return next(claims, None) or claim(state, key)

    monkeypatch.setattr(SharedLookup, "_claim", claiming)
    result = _lookup(session, kind)()
    if kind == "crypto":
        assert result == ("DOT", "USD")
    else:
        assert isinstance(result, HostDecision)
        assert result.host == "socket.massive.com"


def test_an_interrupt_as_the_crypto_read_starts_leaves_nothing_claimed(
    *, session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    def interrupted(*_: object) -> NoReturn:  # Ctrl-C as the claimed read begins
        raise KeyboardInterrupt

    monkeypatch.setattr(crypto_tickers.CryptoTickerList, "_read", interrupted)
    with pytest.raises(KeyboardInterrupt):
        _split_dot(session)()
    assert _claim(session, "crypto")[1]  # the claim was given up, so a new one is made


@pytest.mark.parametrize("kind", ["crypto", "host"])
def test_a_failed_lookup_is_forgotten_before_its_waiters_hear_of_it(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    # A waiter that tries again must claim a fresh lookup, never find the failed one, so the
    # claim is removed before its future completes. A refused host check is not a failure
    # (the class streams real-time data then), so the host lookup fails on a rejected key.
    refused = Reply(
        {"status": "NOT_AUTHORIZED", "message": "upgrade"}, status=403 if kind == "crypto" else 401
    )
    fake_rest.reply(LIST_TARGET if kind == "crypto" else "/v3/snapshot", refused)
    claim = SharedLookup._claim
    fresh_when_failed: list[bool] = []

    def claiming(state: SharedLookup[Any, Any], key: object) -> tuple[Future[Any], bool]:
        future, created = claim(state, key)
        if created and not fresh_when_failed:
            # When the failure reaches the future, a new claim must already be fresh.
            future.add_done_callback(lambda _: fresh_when_failed.append(claim(state, key)[1]))
        return future, created

    monkeypatch.setattr(SharedLookup, "_claim", claiming)
    with pytest.raises(dm.MassiveError):
        _lookup(session, kind)()
    assert fresh_when_failed == [True]


def _failed_soon(table: Table) -> bool:
    # Waits up to 30 s for a live table to fail.
    pause = threading.Event()
    for _ in range(3000):
        if table.j_table.isFailed():
            return True
        pause.wait(0.01)
    return False


def test_a_poll_plans_a_pair_massive_does_not_list_as_a_call_with_no_data(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # A pair Massive's list does not hold has no data, as a 404 has: the cycle plans it as a
    # call it never sends, whether the pair came in code or in a table. Under RAISE such a
    # pair fails the table at its first cycle, in code or in a table, never at the call.
    serve_pairs(fake_rest)
    params = {"date": date(2025, 3, 3)}
    reader = _endpoints(session)._reader("poll", session._core.policy)
    watch = SymbolTable(new_table([string_col("Symbol", ["X:BTCUSD", "X:DOTUSD"])]))
    request = Request(symbols=watch, params=(("date", date(2025, 3, 3)),))
    planned, left_out = reader._plan_cycle(
        CRYPTO_OPEN_CLOSE, request, ("X:BTCUSD", "X:DOTUSD"), cancel=Cancel()
    )
    calls = [call for _, calls in planned for call in calls]
    assert [(call.symbol, call.unsplit) for call in calls] == [
        ("X:BTCUSD", None),
        ("X:DOTUSD", NOT_LISTED),
    ]
    assert left_out == []
    config = dm.MassiveConfig(
        api_key="test-key",
        rest_base_url=fake_rest.url,
        on_error=dm.ErrorPolicy(unavailable_symbols=dm.OnError.RAISE),
    )
    with dm.MassiveSession(config) as strict:
        serve_pairs(fake_rest)
        in_code = query(strict, CRYPTO_OPEN_CLOSE, symbols=["X:DOTUSD"], params=params)
        assert isinstance(in_code, FetchQuery)
        assert _failed_soon(in_code.poll(60))
        table = query(strict, CRYPTO_OPEN_CLOSE, symbols=watch.table, params=params)
        assert isinstance(table, FetchQuery)
        assert _failed_soon(table.poll(60))


def test_a_fetch_reports_a_pair_massive_does_not_list_as_a_symbol_with_no_data(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    last = {
        "conditions": [1],
        "exchange": 1,
        "price": 97000.5,
        "size": 0.1,
        "timestamp": 1704310200000,
    }
    btc = Reply({"status": "success", "request_id": "r1", "symbol": "BTC-USD", "last": last})
    fake_rest.reply("/v1/last/crypto/BTC/USD", btc, btc, btc)
    serve_pairs(fake_rest)
    table = new_table([string_col("Symbol", ["X:BTCUSD", "X:DOTUSD"])])
    with caplog.at_level(logging.WARNING):
        out = fetch(session, CRYPTO_LAST, symbols=table)  # LOG, the default
    assert [r["Symbol"] for r in out] == ["X:BTCUSD"]
    assert [path for path, _ in sent_data(fake_rest)] == ["/v1/last/crypto/BTC/USD"]
    assert caplog.messages == [
        no_data_alarm(
            "crypto.last", 1, "X:DOTUSD", why="(Massive's crypto ticker list does not hold it)"
        )
    ]
    # A pair Massive has no data for gives no rows, passed in code or in a table, alone too.
    assert fetch(session, CRYPTO_LAST, symbols=["X:DOTUSD"]) == []
    unlisted = new_table([string_col("Symbol", ["X:DOTUSD"])])
    assert fetch(session, CRYPTO_LAST, symbols=unlisted) == []
    for setting in (dm.OnError.IGNORE, dm.OnError.RAISE):
        config = dm.MassiveConfig(
            api_key="test-key",
            rest_base_url=fake_rest.url,
            on_error=dm.ErrorPolicy(unavailable_symbols=setting),
        )
        with dm.MassiveSession(config) as other:
            serve_pairs(fake_rest)
            caplog.clear()
            with caplog.at_level(logging.WARNING):
                if setting is dm.OnError.RAISE:
                    with pytest.raises(UnavailableSymbolError, match="X:DOTUSD"):
                        fetch(other, CRYPTO_LAST, symbols=table)
                else:
                    assert len(fetch(other, CRYPTO_LAST, symbols=table)) == 1
            assert caplog.records == []  # IGNORE logs only at DEBUG, and RAISE fails


def test_date_bounds_on_an_instant_axis_are_sent_as_the_days_instants(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(NEWS_PATH, Reply(envelope([])))
    fetch(session, NEWS, start="2025-06-02", end=date(2025, 6, 2))
    # `published_utc` is an instant field, so its date-typed filter gets the half-open
    # instants of the whole day in New York (the zone of a dataset without a class).
    ((_, sent_query),) = fake_rest.get_queries()
    assert sent_query["published_utc.gte"] == ["2025-06-02T04:00:00Z"]
    assert sent_query["published_utc.lt"] == ["2025-06-03T04:00:00Z"]


def test_a_list_symbol_parameter_without_a_limit_takes_every_symbol_at_once(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope([])))
    fetch(session, INDEX_SNAPSHOTS, symbols=["I:SPX", "I:NDX", "I:DJI"])
    assert [q["ticker.any_of"] for _, q in fake_rest.get_queries()] == [["I:DJI,I:NDX,I:SPX"]]


def test_a_list_symbol_parameter_under_a_limit_still_takes_every_symbol_at_once(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # `limit` is Massive's cap on the request's total rows, so the named symbols share one
    # request and one cap instead of a request each.
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope([])))
    fetch(session, INDEX_SNAPSHOTS, symbols=["I:SPX", "I:NDX", "I:DJI"], limit=2)
    assert [q["ticker.any_of"] for _, q in fake_rest.get_queries()] == [["I:DJI,I:NDX,I:SPX"]]


def test_limit_caps_rows_across_requests(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([AAPL_DIVIDEND, AAPL_DIVIDEND])))
    out = fetch(session, DIVIDENDS, symbols=["A", "B", "C"], limit=3)
    assert len(out) == 3
    # A cap below the page size is the page size: no request asks for rows it cannot keep.
    assert [q["limit"] for _, q in fake_rest.get_queries()] == [["3"], ["3"]]


def test_limit_caps_each_symbol_s_rows_where_a_request_names_one_symbol(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # An indicator sends one symbol per request, so the cap is per symbol: a symbol with many
    # rows never crowds out the next.
    values = [{"timestamp": 1741010400000 + i * 86_400_000, "value": float(i)} for i in range(3)]
    fake_rest.reply("/v1/indicators/sma/AAPL", Reply(envelope({"values": values})))
    fake_rest.reply("/v1/indicators/sma/MSFT", Reply(envelope({"values": values})))
    out = fetch(session, SMA, symbols=["AAPL", "MSFT"], limit=2)
    assert [r["Symbol"] for r in out] == ["AAPL", "AAPL", "MSFT", "MSFT"]


def test_a_limit_above_the_page_size_keeps_the_page_size(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([AAPL_DIVIDEND])))
    fetch(session, DIVIDENDS, symbols=["AAPL"], limit=5000)
    assert [q["limit"] for _, q in fake_rest.get_queries()] == [["1000"]]


def test_order_is_sent_as_is_where_the_path_has_it(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v1/indicators/sma/AAPL", Reply(envelope({"values": []})))
    fetch(session, SMA, symbols=["AAPL"], params={"order": Order.ASC})
    assert fake_rest.get_queries()[0][1]["order"] == ["asc"]


def test_order_alone_goes_as_is_on_a_list_path_with_its_own_order(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(NEWS_PATH, Reply(envelope([])))
    fetch(session, NEWS, params={"order": Order.DESC})
    ((_, sent_query),) = fake_rest.get_queries()
    assert sent_query["order"] == ["desc"]
    assert "sort" not in sent_query


def test_order_without_sort_fails_where_no_path_has_order(session: dm.MassiveSession) -> None:
    # The movers path has neither `order` nor `sort`, and dividends only `sort`: the
    # direction could only travel as a suffix of a sort field.
    with pytest.raises(ValueError, match=r"^stocks\.movers: order needs sort"):
        query(session, MOVERS, params={"direction": Direction.GAINERS, "order": Order.ASC})
    with pytest.raises(ValueError, match=r"^stocks\.dividends: order needs sort"):
        query(session, DIVIDENDS, params={"order": Order.ASC})


def test_order_without_sort_fails_on_a_chosen_path_without_order(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # The contracts list path has `order`; the per-contract detail path has neither.
    q = query(session, CONTRACTS, symbols=["O:SPY251219C00600000"], params={"order": Order.ASC})
    assert isinstance(q, FetchQuery)
    with pytest.raises(ValueError, match=r"does not take \['order'\]"):
        q.fetch(wait=True)
    assert fake_rest.requests == []


def test_path_parameters_come_from_the_typed_parameters(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    gainers = "/v2/snapshot/locale/us/markets/stocks/gainers"
    fake_rest.reply(gainers, Reply(envelope(status="OK", tickers=[{"ticker": "NVDA"}])))
    out = fetch(session, MOVERS, params={"direction": Direction.GAINERS})
    assert fake_rest.get_queries() == [(gainers, {})]
    assert [r["Symbol"] for r in out] == ["NVDA"]


def test_a_currency_pair_fills_from_and_to_and_the_symbol_from_the_request(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    last = {
        "conditions": [1],
        "exchange": 1,
        "price": 97000.5,
        "size": 0.1,
        "timestamp": 1704310200000,
    }
    fake_rest.reply(
        "/v1/last/crypto/BTC/USD",
        Reply({"status": "success", "request_id": "r1", "symbol": "BTC-USD", "last": last}),
    )
    serve_pairs(fake_rest)
    with caplog.at_level(logging.WARNING):
        out = fetch(session, CRYPTO_LAST, symbols=["X:BTCUSD", "X:ETHUSD"])
    assert sorted(path for path, _ in sent_data(fake_rest)) == [
        "/v1/last/crypto/BTC/USD",
        "/v1/last/crypto/ETH/USD",
    ]
    # ETH answered 404 (unscripted): it is dropped with one WARNING.
    assert [(r["Symbol"], r["Price"], r["Timestamp"]) for r in out] == [
        ("X:BTCUSD", 97000.5, 1704310200000 * 10**6)
    ]
    assert caplog.messages == [
        no_data_alarm(
            "crypto.last", 1, "X:ETHUSD", why="(HTTP 404 from /v1/last/crypto/{from}/{to})"
        )
    ]


def test_fan_out_threads_are_named_after_the_session_and_the_call(
    session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    last = {"conditions": [1], "exchange": 1, "price": 1.5, "size": 0.1, "timestamp": 1704310200000}
    for pair in ("BTC/USD", "ETH/USD"):
        body = {"status": "success", "request_id": "r", "symbol": pair, "last": last}
        fake_rest.reply(f"/v1/last/crypto/{pair}", Reply(body))
    serve_pairs(fake_rest)
    with caplog.at_level(logging.DEBUG, "deephaven_massive"):
        fetch(session, CRYPTO_LAST, symbols=["X:BTCUSD", "X:ETHUSD"])
    workers = {str(r.threadName) for r in caplog.records if r.threadName != "MainThread"}

    # The call's id is the first of the test, and each worker's number follows the prefix.
    prefix = f"{session._core.thread_prefix} call-1 rest_"
    assert workers
    assert all(re.fullmatch(rf"{re.escape(prefix)}\d", name) for name in workers), workers


def test_symbols_route_to_their_class_path_and_send_only_its_parameters(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(
        "/v1/indicators/sma/AAPL",
        # 2024-01-03 00:00 New York time: a stock's day value carries its day's start.
        Reply(envelope({"values": [{"timestamp": 1704258000000, "value": 185.0}]})),
    )
    fake_rest.reply(
        "/v1/indicators/sma/X%3ABTCUSD",
        # 2024-01-03 00:00 UTC: a crypto pair's day is the UTC day.
        Reply(envelope({"values": [{"timestamp": 1704240000000, "value": 97000.0}]})),
    )
    out = fetch(
        session,
        SMA,
        symbols=["AAPL", "X:BTCUSD"],
        timespan=Timespan.DAY,
        params={"window": 50},
        start=datetime(2024, 1, 1, tzinfo=UTC),
    )
    requests = dict(fake_rest.get_queries())
    assert requests["/v1/indicators/sma/AAPL"] == {
        "limit": ["5000"],
        "timespan": ["day"],
        "window": ["50"],
        "timestamp.gte": ["1704067200000"],  # the indicators count milliseconds
    }
    assert requests["/v1/indicators/sma/X:BTCUSD"]["timespan"] == ["day"]
    assert [(r["Symbol"], r["Value"]) for r in out] == [("AAPL", 185.0), ("X:BTCUSD", 97000.0)]


def test_an_explicit_adjusted_fails_on_a_path_without_it(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    q = query(session, SMA, symbols=["X:BTCUSD"], adjusted=False)
    assert isinstance(q, FetchQuery)
    with pytest.raises(ValueError, match=r"cryptoTicker\} does not take \['adjusted'\]"):
        q.fetch(wait=True)
    assert fake_rest.requests == []
    fake_rest.reply("/v1/indicators/sma/AAPL", Reply(envelope({"values": []})))
    fetch(session, SMA, symbols=["AAPL"], adjusted=False)
    assert fake_rest.get_queries()[0][1]["adjusted"] == ["false"]


def test_a_per_symbol_detail_path_serves_named_symbols(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    contract = {"ticker": "O:SPY250117C00600000", "strike_price": 600.0}
    fake_rest.reply(CONTRACTS_PATH, Reply(envelope([contract])))
    fake_rest.reply(
        "/v3/reference/options/contracts/O%3ASPY250117C00600000", Reply(envelope(contract))
    )
    listed = fetch(session, CONTRACTS, filters={"strike_price_gte": 500})
    named = fetch(session, CONTRACTS, symbols=["O:SPY250117C00600000"])
    assert [path for path, _ in fake_rest.get_queries()] == [
        CONTRACTS_PATH,
        "/v3/reference/options/contracts/O:SPY250117C00600000",
    ]
    assert listed == named
    # A filter of the list path cannot reach the detail path: it fails, never drops.
    q = query(session, CONTRACTS, symbols=["O:SPY250117C00600000"], filters={"strike_price_gte": 1})
    assert isinstance(q, FetchQuery)
    with pytest.raises(ValueError, match=r"does not take \['strike_price_gte'\]"):
        q.fetch(wait=True)


def test_row_symbols_are_canonical(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    body = {
        "status": "success",
        "request_id": "r1",
        "from": "AUD",
        "to": "USD",
        "initialAmount": 100,
        "converted": 65.5,
        "symbol": "AUD/USD",
    }
    fake_rest.reply("/v1/conversion/AUD/USD", Reply(body))
    fake_rest.reply("/v1/conversion/AUD/XXX", Reply({**body, "symbol": "not a pair"}))
    out = fetch(session, CONVERSION, params={"from": "AUD", "to": "USD", "amount": 100})
    assert [(r["Symbol"], r["Converted"]) for r in out] == [("C:AUDUSD", 65.5)]
    assert "RawSymbol" not in out[0]
    assert fake_rest.get_queries()[0] == ("/v1/conversion/AUD/USD", {"amount": ["100"]})
    # Under RAISE, a spelling the library cannot read fails the request, naming it.
    config = dm.MassiveConfig(
        api_key="test-key",
        rest_base_url=fake_rest.url,
        on_error=dm.ErrorPolicy(unreadable_symbols=dm.OnError.RAISE),
    )
    with dm.MassiveSession(config) as strict:
        assert (
            fetch(strict, CONVERSION, params={"from": "AUD", "to": "USD"})[0]["Converted"] == 65.5
        )
        # Under RAISE the row's cause fails the read first.
        with pytest.raises(
            RowDataError,
            match=r"^forex\.conversion: Massive sent rows the library cannot read whole, which it "
            r"withheld \(1 under unreadable_symbols\); the first: Massive sent the symbol "
            r"'not a pair', a spelling the library cannot read\.",
        ):
            fetch(strict, CONVERSION, params={"from": "AUD", "to": "XXX"})


def dividend(ticker: str, row_id: str) -> dict[str, object]:
    # One dividend row of a ticker as Massive spells it.
    return {**AAPL_DIVIDEND, "ticker": ticker, "id": row_id}


def test_an_unreadable_row_symbol_is_withheld_by_default(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    events = record_problems(session, monkeypatch)
    rows = [dividend("a b", "E9"), AAPL_DIVIDEND]
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope(rows)))
    with caplog.at_level(logging.WARNING):
        out = fetch(session, DIVIDENDS, symbols=["AAPL"])
    assert [(r["Id"], r["Symbol"]) for r in out] == [("E1", "AAPL")]
    assert "RawSymbol" not in out[0]
    assert [(e.cause, e.symbol, e.field, e.rows, e.stream_id) for e in events] == [
        (ProblemCause.UNREADABLE_SYMBOLS, "a b", "Symbol", 1, 1)  # of the read, call 1
    ]
    assert events[0].detail.startswith("Massive sent the symbol 'a b'")
    (alarm,) = alarms(caplog)
    assert alarm.startswith("stocks.dividends table 1: unreadable_symbols: 1 rows of a b, ")


@pytest.mark.parametrize(
    ("cause", "page"),
    [
        ("unreadable_symbols", [dividend("a b", "E9"), AAPL_DIVIDEND]),
        ("unconvertible_values", [{**AAPL_DIVIDEND, "frequency": 4.5}, AAPL_DIVIDEND]),
        (
            "unreadable_keys",
            [{k: v for k, v in AAPL_DIVIDEND.items() if k != "ticker"}, AAPL_DIVIDEND],
        ),
    ],
)
def test_a_withheld_rows_cause_under_raise_fails_the_read(
    *, fake_rest: FakeRestServer, cause: str, page: list[dict[str, object]]
) -> None:
    # The good row beside it does not save the read: the cause's setting fails it.
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope(page)))
    config = dm.MassiveConfig(
        api_key="test-key",
        rest_base_url=fake_rest.url,
        on_error=dm.ErrorPolicy(**{cause: dm.OnError.RAISE}),
    )
    with dm.MassiveSession(config) as m, pytest.raises(RowDataError) as info:
        fetch(m, DIVIDENDS, symbols=["AAPL"])
    assert str(info.value).startswith(
        "stocks.dividends: Massive sent rows the library cannot read whole, which it withheld "
        f"(1 under {cause}); the first: "
    )


def test_a_reactive_symbol_table_is_read_once_for_fetch(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope([])))
    watch = new_table([string_col("Symbol", ["I:SPX", "I:NDX", None])])
    fetch(session, INDEX_SNAPSHOTS, symbols=watch)
    assert fake_rest.get_queries()[0][1]["ticker.any_of"] == ["I:NDX,I:SPX"]
    # A row that is not Massive's ticker is skipped, so the request names only the others.
    fetch(session, INDEX_SNAPSHOTS, symbols=new_table([string_col("Symbol", ["SPX", "I:VIX"])]))
    assert fake_rest.get_queries()[1][1]["ticker.any_of"] == ["I:VIX"]
    # A table whose rows are all bad raises rather than read nothing.
    with pytest.raises(
        ValueError,
        match=r"^m\.indices\.snapshots\(\): no row of the symbol table is a ticker this dataset "
        r"takes, so the result would follow nothing: skipping 1 symbol-table value this dataset "
        r"does not take: 'SPX'; fix or delete that row, or report it at https://",
    ):
        fetch(session, INDEX_SNAPSHOTS, symbols=new_table([string_col("Symbol", ["SPX"])]))
    # Under RAISE, a bad row raises at once, naming the fix.
    config = dm.MassiveConfig(
        api_key="test-key",
        rest_base_url=fake_rest.url,
        on_error=dm.ErrorPolicy(noncanonical_symbols=dm.OnError.RAISE),
    )
    with dm.MassiveSession(config) as strict:
        bad = new_table([string_col("Symbol", ["SPX", "I:VIX"])])
        with pytest.raises(
            ValueError, match=r"^indices\.snapshots: a symbol table holds 'SPX' .*use 'I:SPX'"
        ):
            fetch(strict, INDEX_SNAPSHOTS, symbols=bad)


# --- Rows ----------------------------------------------------------------------------------------


def test_error_rows_of_a_snapshot_are_kept_and_logged_once(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    refused = {"ticker": "I:VIX", "error": "NOT_ENTITLED", "message": "Not entitled"}
    other = {**refused, "ticker": "I:RUT"}
    spx = {"ticker": "I:SPX", "value": 5900.0, "last_updated": 1704310200000000000}
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope([refused, spx, other])))
    with caplog.at_level(logging.WARNING, LOGGER):
        out = fetch(session, INDEX_SNAPSHOTS, symbols=["I:SPX", "I:VIX"])
        fetch(session, INDEX_SNAPSHOTS, symbols=["I:SPX", "I:VIX"])
    assert [(r["Symbol"], r["Error"], r["Value"]) for r in out] == [
        ("I:SPX", None, 5900.0),
        ("I:VIX", "NOT_ENTITLED", None),
        ("I:RUT", "NOT_ENTITLED", None),
    ]
    # One WARNING per page, naming each ticker once per session.
    assert [r.message for r in caplog.records if "error rows" in r.message] == [
        (
            f"indices.snapshots: Massive answered error rows from {INDEX_SNAPSHOT_PATH} for 2 "
            "[I:VIX (NOT_ENTITLED: Not entitled), I:RUT (NOT_ENTITLED: Not entitled)]; the rows "
            "are kept with their Error and Message columns"
        )
    ]


def test_the_first_part_of_a_page_gives_a_drift_sample(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A snapshot page is read in two parts, its good rows and then its error rows; a field both
    # parts carry is sampled from the good rows, read first, whatever the rows' order.
    refused = {"ticker": "I:VIX", "error": "NOT_ENTITLED", "message": "x", "new": "from error"}
    spx = {"ticker": "I:SPX", "value": 1.0, "last_updated": 1704310200000000000, "new": "good"}
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope([refused, spx])))
    events = record_problems(session, monkeypatch)
    fetch(session, INDEX_SNAPSHOTS, symbols=["I:SPX", "I:VIX"])
    [drift] = [e for e in events if e.cause is ProblemCause.SCHEMA_DRIFT]
    assert drift.field == "new"
    assert drift.detail.endswith('; first value "good"')


def test_drift_is_a_problem_of_each_read_and_its_status_is_logged_once(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_rest.reply(
        DIVIDENDS_PATH,
        Reply(envelope([{**AAPL_DIVIDEND, "brand_new": 1}], status="PARTIAL")),
    )
    events = record_problems(session, monkeypatch)
    with caplog.at_level(logging.WARNING):
        fetch(session, DIVIDENDS, symbols=["AAPL"])
        fetch(session, DIVIDENDS, symbols=["AAPL"])
    assert caplog.text.count("undocumented response status 'PARTIAL'") == 1
    # Each read's drift is its own problem, with the field and the first value it held; each
    # read's first warns once.
    assert [(e.cause, e.field, e.stream_id) for e in events] == [
        (ProblemCause.SCHEMA_DRIFT, "brand_new", 1),
        (ProblemCause.SCHEMA_DRIFT, "brand_new", 2),
    ]
    assert all(e.detail.endswith("; first value 1") for e in events)
    assert [a.split(":")[0] for a in alarms(caplog)] == [
        "stocks.dividends table 1",
        "stocks.dividends table 2",
    ]


def test_drift_of_a_per_symbol_request_names_the_symbol(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    last = {"conditions": [1], "exchange": 1, "price": 1.5, "size": 0.1, "timestamp": 1704310200000}
    body = {
        "status": "success",
        "request_id": "r",
        "symbol": "BTC-USD",
        "last": {**last, "new": "x"},
    }
    fake_rest.reply("/v1/last/crypto/BTC/USD", Reply(body))
    serve_pairs(fake_rest)
    events = record_problems(session, monkeypatch)
    fetch(session, CRYPTO_LAST, symbols=["X:BTCUSD"])
    (drift,) = events
    assert (drift.cause, drift.field, drift.where) == (
        ProblemCause.SCHEMA_DRIFT,
        "new",
        f"GET {CRYPTO_LAST_PATH}",
    )
    assert drift.detail.endswith('; first sent for X:BTCUSD; first value "x"')


def test_a_row_without_its_symbol_is_a_data_failure(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    row = {key: value for key, value in AAPL_DIVIDEND.items() if key != "ticker"}
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([row])))
    with (
        caplog.at_level(logging.ERROR, LOGGER),
        pytest.raises(RowDataError, match=r"^stocks.dividends: "),
    ):
        fetch(session, DIVIDENDS, symbols=["AAPL"])
    # Logged at ERROR with the same text, after the id of the call that failed.
    assert re.search(r"call 1 failed after \d+\.\d{3} s: stocks\.dividends: ", caplog.text)


def test_one_odd_row_costs_the_call_only_itself(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A row without its symbol and one with a value that does not fit are withheld and
    # counted; the good row beside them lands.
    nameless = {key: value for key, value in AAPL_DIVIDEND.items() if key != "ticker"}
    odd = {**AAPL_DIVIDEND, "id": "E2", "frequency": 4.5}
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([nameless, odd, AAPL_DIVIDEND])))
    events = record_problems(session, monkeypatch)
    with caplog.at_level(logging.WARNING):
        out = fetch(session, DIVIDENDS, symbols=["AAPL"])
    assert [(r["Id"], r["Frequency"]) for r in out] == [("E1", 4)]
    assert sorted((e.cause, e.symbol, e.field, e.rows) for e in events) == [
        (ProblemCause.UNCONVERTIBLE_VALUES, "AAPL", "Frequency", 1),
        (ProblemCause.UNREADABLE_KEYS, None, "Ticker", 1),
    ]
    # The first of each cause warns, pointing at m.problems().
    assert sorted(alarms(caplog)) == [
        (
            "stocks.dividends table 1: unconvertible_values: 1 rows of AAPL, "
            + next(e.detail for e in events if e.cause is ProblemCause.UNCONVERTIBLE_VALUES)
            + f" (at REST GET {DIVIDENDS_PATH}); m.problems() lists it and every later "
            "unconvertible_values problem of this table, which the log shows at DEBUG only"
        ),
        (
            f"stocks.dividends table 1: unreadable_keys: 1 rows, {DIVIDENDS_PATH}: row 0: no "
            f"Ticker (wire key 'ticker'), which the dataset needs (at REST GET {DIVIDENDS_PATH}); "
            "m.problems() lists it and every later unreadable_keys problem of this table, which "
            "the log shows at DEBUG only"
        ),
    ]


def test_a_row_without_an_optional_symbol_keeps_a_null_symbol(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    optional = DatasetSpec(
        summary="A dataset for the tests.",
        name="stocks.filings",
        asset_class=AssetClass.STOCKS,
        fetch_path=DIVIDENDS_PATH,
        keys={(R, DIVIDENDS_PATH): SourceKeys(symbol=KeySpec("Ticker", required=False))},
        poll_keys=("Id",),
    )
    row = {key: value for key, value in AAPL_DIVIDEND.items() if key != "ticker"}
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([row, AAPL_DIVIDEND])))
    with caplog.at_level(logging.WARNING, LOGGER):
        out = fetch(session, optional)
    assert [r["Symbol"] for r in out] == [None, "AAPL"]
    assert caplog.text == ""  # a missing optional symbol is no invalid symbol


def test_a_timestamp_out_of_bounds_is_a_data_failure(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    nanos = 1704310200000000000  # nanoseconds where the indicators send milliseconds
    fake_rest.reply("/v1/indicators/sma/AAPL", Reply(envelope({"values": [{"timestamp": nanos}]})))
    mismatch = r"timestamp unit mismatch for indicators\.sma\.Timestamp"
    with pytest.raises(RowDataError, match=mismatch):
        fetch(session, SMA, symbols=["AAPL"])


# --- Errors ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("spec", "path", "symbols", "refusal"),
    [
        (
            DIVIDENDS,
            DIVIDENDS_PATH,
            ["AAPL"],
            (
                "stocks.dividends: the Stocks subscription of your plan does not include this "
                "data; Massive answered HTTP 403 from GET /stocks/v1/dividends?"
            ),
        ),
        (
            CONTRACTS,
            CONTRACTS_PATH,
            None,
            (
                "options.contracts: your account does not include this data; Massive answered "
                "HTTP 403 from GET /v3/reference/options/contracts?"
            ),
        ),
    ],
)
def test_a_refusal_names_the_dataset_and_its_subscription(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    spec: DatasetSpec,
    path: str,
    symbols: list[str] | None,
    refusal: str,
) -> None:
    fake_rest.reply(path, Reply({"status": "NOT_AUTHORIZED", "message": "upgrade"}, status=403))
    with pytest.raises(dm.EntitlementError) as info:
        fetch(session, spec, symbols=symbols)
    assert str(info.value).startswith(refusal)
    assert str(info.value).endswith("upgrade")
    assert (info.value.status, info.value.source) == (403, dm.ErrorSource.MASSIVE)


def test_a_refusal_names_the_alternative_where_one_exists(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(
        CRYPTO_LAST_PATH.format(**{"from": "BTC", "to": "USD"}), Reply(b"{}", status=403)
    )
    serve_pairs(fake_rest)
    with pytest.raises(dm.EntitlementError) as info:
        fetch(session, CRYPTO_LAST, symbols=["X:BTCUSD"])
    assert str(info.value).startswith(
        "crypto.last: the Currencies subscription of your plan does not include this data; "
        "Massive answered HTTP 403 from GET /v1/last/crypto/BTC/USD"
    )
    assert str(info.value).endswith("; try m.crypto.snapshots(...), which carries the same values")


@pytest.mark.parametrize(
    ("status", "error"), [(401, dm.AuthenticationError), (403, dm.EntitlementError)]
)
def test_refusals_raise_their_public_errors(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    status: int,
    error: type[dm.MassiveError],
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake_rest.reply(
        DIVIDENDS_PATH,
        Reply({"status": "ERROR", "request_id": "r9", "message": "no"}, status=status),
    )
    with caplog.at_level(logging.DEBUG), pytest.raises(error) as info:
        fetch(session, DIVIDENDS, symbols=["AAPL"])
    assert (info.value.status, info.value.request_id) == (status, "r9")
    assert info.value.source is dm.ErrorSource.MASSIVE
    # A refusal the caller receives is expected: the call logs it at DEBUG, never higher.
    failed = [r for r in caplog.records if r.getMessage().startswith("call ")]
    assert [r.levelno for r in failed if "failed" in r.getMessage()] == [logging.DEBUG]
    assert f"HTTP {status}" in caplog.text
    assert "test-key" not in caplog.text


def test_a_lasting_rate_limit_names_itself_and_warns_once(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Massive asks for a pause beyond what a read waits: the read gives up at once. A pause
    # lasts at most ten minutes, which a read's full patience always covers, so this read has
    # five minutes of it.
    monkeypatch.setattr(rate_limits, "READ_PATIENCE", 300.0)
    refusal = {"status": "ERROR", "request_id": "r7", "message": "slow down"}
    fake_rest.reply(DIVIDENDS_PATH, Reply(refusal, status=429, headers={"Retry-After": "700"}))
    given_up = (
        r"^rate limited: Massive is refusing stocks requests with HTTP 429 \(latest refusal: "
        r".*r7.*\)\. Its next request, a probe, goes at 2027-01-15T08:10:00\+00:00 "
        r"\(.* local time\), but this read waits out at most 300 s of pauses and has spent "
        r"0 s of its patience\. The usual cause is another program or process using the same "
        r"Massive account, whose limit they share\. "
        r"To go on: retry after that time; stop the other program; or, if this subscription is "
        r"on the Basic tier, declare it Basic \(rest_basic_subscriptions\) so its requests are "
        r"paced, or upgrade it\. m\.rate_limits\(\) shows each subscription's state$"
    )
    with caplog.at_level(logging.INFO):
        for _ in range(2):
            with pytest.raises(dm.MassiveError, match=given_up):
                fetch(session, DIVIDENDS, symbols=["AAPL"])
    # The second call failed without a request: the bucket's pause still stands.
    assert len(fake_rest.get_queries()) == 1
    # The first 429 is a notice at INFO, once; the calls raise, and log nothing above it.
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    warnings = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.INFO and "with HTTP 429 (" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert warnings[0].startswith(
        f"{session._core.label}: Massive refused a stocks request with HTTP 429 (HTTP 429 from GET "
    )
    assert warnings[0].endswith("send the request id to Massive's support")
    assert "test-key" not in caplog.text


def test_a_failed_symbol_fails_the_whole_fan_out(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v1/last/crypto/BTC/USD", Reply(b"oops", status=500))
    serve_pairs(fake_rest)
    with pytest.raises(dm.MassiveError, match="HTTP 500"):
        fetch(session, CRYPTO_LAST, symbols=["X:BTCUSD", "X:ETHUSD", "X:SOLUSD"])


def test_a_closed_session_reports_itself(session: dm.MassiveSession) -> None:
    transport = session._core.rest_transport()
    session.close()
    with pytest.raises(
        dm.MassiveError, match=r"^MassiveSession #\d+ is closed; " + re.escape(NEW_SESSION) + "$"
    ):
        transport.fetch(DIVIDENDS, Request(symbols=("AAPL",)), call=1)


# --- History -----------------------------------------------------------------------------------


def history(
    m: dm.MassiveSession,
    spec: DatasetSpec,
    *,
    start: dm.DateLike,
    end: dm.DateLike | None,
    **kwargs: Unpack[ReadArguments],
) -> list[dict[str, Any]]:
    q = query(m, spec, **kwargs)
    assert isinstance(q, HistoryQuery)
    return rows(q.history(start, end, source=HistorySource.REST, wait=True))


def test_history_bounds_a_timestamp_axis_exactly_and_pages(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    start = epoch_ns(datetime(2025, 3, 3, 5, tzinfo=UTC))  # midnight in New York
    first = {"sip_timestamp": start + 1, "price": 1.0, "size": 10}
    early = {"sip_timestamp": start - 1, "price": 2.0, "size": 10}  # outside: dropped
    page2 = "/v3/trades/AAPL?cursor=abc"
    fake_rest.reply(page2, Reply(envelope([early])))
    fake_rest.reply(
        TRADES_PATH.format(stockTicker="AAPL"),
        Reply(envelope([first], next_url=f"{fake_rest.url}{page2}")),
    )
    with caplog.at_level(logging.INFO, LOGGER):
        out = history(session, TRADES, start="2025-03-03", end="2025-03-03", symbols=["AAPL"])
    assert [(r["Symbol"], r["Timestamp"]) for r in out] == [("AAPL", start + 1)]
    query_sent = fake_rest.get_queries()[0][1]
    assert query_sent["timestamp.gte"] == [str(start)]
    assert query_sent["timestamp.lt"] == [str(start + 86_400 * NS)]
    assert (
        "stocks.trades history: source=REST requests=2, waiting out at most 600 s of pauses"
        in caplog.text
    )


MIDNIGHT = epoch_ns(datetime(2025, 3, 3, 5, tzinfo=UTC))
"""The start of the 2025-03-03 window: midnight in New York."""


def window_trade(offset: int, price: object) -> dict[str, object]:
    # A trade `offset` nanoseconds after the window's start; a negative offset lies outside it.
    return {"sip_timestamp": MIDNIGHT + offset, "price": price, "size": 10}


@pytest.mark.parametrize(
    "page",
    [
        pytest.param([window_trade(1, "x"), window_trade(2, "y")], id="every-row-inside"),
        # The good rows outside the window count neither way.
        pytest.param(
            [window_trade(-2, 1.0), window_trade(1, "x"), window_trade(-1, 2.0)], id="good-outside"
        ),
    ],
)
def test_a_read_whose_rows_in_the_window_are_all_unreadable_fails(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, page: list[dict[str, object]]
) -> None:
    fake_rest.reply(TRADES_PATH.format(stockTicker="AAPL"), Reply(envelope(page)))
    with pytest.raises(
        RowDataError,
        match=r"^stocks\.trades: the library could read none of the \d rows of the read's symbols "
        r"and window from GET /v3/trades/\{stockTicker\} whole",
    ):
        history(session, TRADES, start="2025-03-03", end="2025-03-03", symbols=["AAPL"])


def serve_pages(fake_rest: FakeRestServer, pages: Sequence[list[dict[str, object]]]) -> None:
    # Serve AAPL's trades as the given pages, each pointing to the next.
    first = TRADES_PATH.format(stockTicker="AAPL")
    for index, rows in enumerate(pages):
        target = first if index == 0 else f"{first}?cursor={index}"
        last = index == len(pages) - 1
        following = None if last else f"{fake_rest.url}{first}?cursor={index + 1}"
        fake_rest.reply(target, Reply(envelope(rows, next_url=following)))


@pytest.mark.parametrize("bad", [0, 10], ids=["first-page", "last-page"])
def test_a_page_whose_rows_are_all_unreadable_beside_pages_read_whole_fails_nothing(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    bad: int,
) -> None:
    # The read is judged across its pages: ten pages read whole and one of an unreadable row
    # give the ten rows, and the one row is withheld and reported.
    events: list[ProblemEvent] = []
    monkeypatch.setattr(session._core, "problems", events.extend)
    pages = [[window_trade(index + 1, float(index))] for index in range(11)]
    pages[bad] = [window_trade(bad + 1, "x")]
    serve_pages(fake_rest, pages)
    out = history(session, TRADES, start="2025-03-03", end="2025-03-03", symbols=["AAPL"])
    assert [r["Price"] for r in out] == [float(i) for i in range(11) if i != bad]
    assert [(e.cause, e.symbol, e.field, e.rows) for e in events] == [
        (ProblemCause.UNCONVERTIBLE_VALUES, "AAPL", "Price", 1)
    ]


def test_a_read_of_many_pages_whose_rows_are_all_unreadable_fails(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    serve_pages(fake_rest, [[window_trade(index + 1, "x")] for index in range(11)])
    with pytest.raises(RowDataError, match=r"could read none of the 11 rows of the read's"):
        history(session, TRADES, start="2025-03-03", end="2025-03-03", symbols=["AAPL"])


def read_aapl_and_msft_by_symbol(
    session: dm.MassiveSession, fake_rest: FakeRestServer, *, msft_price: object
) -> Mapping[str, SymbolOutcome]:
    # Read AAPL, whose only row is withheld, and MSFT, by symbol, as a followed table does;
    # a `msft_price` of None has Massive answer HTTP 404 for MSFT: it has no data.
    aapl = TRADES_PATH.format(stockTicker="AAPL")
    fake_rest.reply(aapl, Reply(envelope([window_trade(1, "x")])))
    msft = TRADES_PATH.format(stockTicker="MSFT")
    rows = None if msft_price is None else [window_trade(1, msft_price)]
    fake_rest.reply(msft, Reply(status=404) if rows is None else Reply(envelope(rows)))
    day = date(2025, 3, 3)
    asked = Request(start=day, end=day, on_error=session._core.policy)
    window = history_window(TRADES, asked, start=day, end=day, classes=())
    transport = session._core.rest_transport()
    tally = ReadTally()
    out = transport.history_by_symbol(
        TRADES, asked, window, symbols=["AAPL", "MSFT"], call=1, tally=tally
    )
    return tally.judge(TRADES.name, out)


def test_a_symbol_whose_rows_are_all_unreadable_beside_one_read_whole_gets_no_rows(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    out = read_aapl_and_msft_by_symbol(session, fake_rest, msft_price=1.0)
    aapl, msft = out["AAPL"], out["MSFT"]
    assert isinstance(aapl, SymbolRows)
    assert isinstance(msft, SymbolRows)
    assert aapl.table.size == 0
    assert msft.table.size == 1


def test_a_read_by_symbol_whose_rows_are_all_unreadable_fails_each_symbol_alone(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # Each symbol fails, as a failed request fails its own, so a followed table goes on.
    out = read_aapl_and_msft_by_symbol(session, fake_rest, msft_price="y")
    for symbol in ("AAPL", "MSFT"):
        failed = out[symbol]
        assert isinstance(failed, SymbolFailure)
        assert isinstance(failed.error, RowDataError)
        assert "could read none of the 2 rows of the read's symbols" in str(failed.error)


def test_a_symbol_massive_has_no_data_for_keeps_its_rows_when_the_read_fails(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # MSFT had no rows to read: its empty rows stand, and only AAPL fails.
    out = read_aapl_and_msft_by_symbol(session, fake_rest, msft_price=None)
    aapl, msft = out["AAPL"], out["MSFT"]
    assert isinstance(aapl, SymbolFailure)
    assert isinstance(aapl.error, RowDataError)
    assert isinstance(msft, SymbolRows)
    assert (msft.table.size, msft.no_data) == (0, True)


def test_unreadable_rows_outside_the_window_are_neither_published_nor_reported(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[ProblemEvent] = []
    monkeypatch.setattr(session._core, "problems", events.extend)
    page = [
        window_trade(-2, "x"),
        window_trade(1, 1.0),
        window_trade(-1, "y"),
        window_trade(2, 2.0),
    ]
    fake_rest.reply(TRADES_PATH.format(stockTicker="AAPL"), Reply(envelope(page)))
    out = history(session, TRADES, start="2025-03-03", end="2025-03-03", symbols=["AAPL"])
    assert [r["Price"] for r in out] == [1.0, 2.0]
    assert events == []


def test_bars_fill_the_range_segments_in_milliseconds(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    midnight = int(datetime(2025, 3, 3, 5, tzinfo=UTC).timestamp()) * 1000
    bar = {"t": midnight, "o": 1.0, "c": 2.0, "v": 100}
    late = {"t": midnight + 86_400_000, "o": 1.0}  # the day after: dropped
    path = f"/v2/aggs/ticker/AAPL/range/1/day/{midnight}/{midnight + 86_399_999}"
    fake_rest.reply(path, Reply(envelope([bar, late])))
    out = history(
        session,
        BARS,
        start=date(2025, 3, 3),
        end=date(2025, 3, 3),
        symbols=["AAPL"],
        timespan=Timespan.DAY,
        adjusted=False,
    )
    assert fake_rest.get_queries() == [(path, {"adjusted": ["false"], "sort": ["asc"]})]
    # The bar is March 3 in New York: from its midnight to the next.
    assert [
        (r["Symbol"], r["Date"], r["TimestampStart"], r["TimestampEnd"], r["Close"]) for r in out
    ] == [("AAPL", "2025-03-03", midnight * 10**6, (midnight + 86_400_000) * 10**6, 2.0)]


def test_all_daily_bars_read_one_grouped_day_at_a_time(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    day = int(datetime(2025, 3, 3, 5, tzinfo=UTC).timestamp()) * 1000
    fake_rest.reply(
        GROUPED_PATH.format(date="2025-03-03"), Reply(envelope([{"T": "AAPL", "t": day, "c": 2.0}]))
    )
    fake_rest.reply(
        GROUPED_PATH.format(date="2025-03-04"),
        Reply(envelope([{"T": "MSFT", "t": day + 86_400_000, "c": 3.0}])),
    )
    out = history(
        session,
        BARS,
        start="2025-03-03",
        end="2025-03-04",
        symbols=dm.ALL,
        timespan=Timespan.DAY,
        adjusted=True,
    )
    assert sorted(path for path, _ in fake_rest.get_queries()) == [
        GROUPED_PATH.format(date="2025-03-03"),
        GROUPED_PATH.format(date="2025-03-04"),
    ]
    assert [(r["Symbol"], r["Close"]) for r in out] == [("AAPL", 2.0), ("MSFT", 3.0)]


def test_all_daily_bars_request_only_the_days_asked_for(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # The days the flat files lack are read one grouped day each, not the whole range.
    day = int(datetime(2025, 3, 4, 5, tzinfo=UTC).timestamp()) * 1000
    fake_rest.reply(
        GROUPED_PATH.format(date="2025-03-04"), Reply(envelope([{"T": "MSFT", "t": day, "c": 3.0}]))
    )
    q = query(session, BARS, symbols=dm.ALL, timespan=Timespan.DAY, adjusted=False)
    first, last = date(2025, 3, 3), date(2025, 3, 7)
    request = dataclasses.replace(q._request, start=first, end=last)
    window = history_window(BARS, request, start=first, end=last, classes=())
    table = session._core.rest_transport().history(
        BARS, request, window, call=1, tally=ReadTally(), days=[date(2025, 3, 4)]
    )
    assert [(r["Symbol"], r["Close"]) for r in rows(table)] == [("MSFT", 3.0)]
    assert [path for path, _ in fake_rest.get_queries()] == [GROUPED_PATH.format(date="2025-03-04")]


def test_futures_bars_by_date_request_whole_sessions(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    inside = {
        "ticker": "ESZ5",
        "session_end_date": "2025-03-04",
        "window_start": 1741039200000000000,
    }
    outside = {**inside, "session_end_date": "2025-03-03"}
    fake_rest.reply("/futures/v1/aggs/ESZ5", Reply(envelope([outside, inside])))
    out = history(
        session,
        FUTURES_BARS,
        start="2025-03-04",
        end="2025-03-04",
        symbols=["ESZ5"],
        timespan=Timespan.MINUTE,
        multiplier=5,
    )
    q = fake_rest.get_queries()[0][1]
    # From the session's regular open, Monday 17:00 Chicago, less the four-day holiday
    # margin, to 18:00 Chicago on the session's date; the rows are kept by session.
    opens = datetime(2025, 2, 27, 17, tzinfo=CHICAGO)
    assert q["window_start.gte"] == [str(epoch_ns(opens))]
    assert q["window_start.lt"] == [str(epoch_ns(datetime(2025, 3, 4, 18, tzinfo=CHICAGO)))]
    assert q["resolution"] == ["5min"]
    assert q["sort"] == ["window_start.asc"]  # oldest first, as flat files return rows
    # The row keeps its session as `Date`, and ends five minutes after its start.
    assert [(r["Date"], r["TimestampEnd"] - r["TimestampStart"]) for r in out] == [
        ("2025-03-04", 5 * 60 * NS)
    ]


def futures_trade(moment: datetime, session: str, ticker: str = "ESH5") -> dict[str, object]:
    return {
        "ticker": ticker,
        "timestamp": epoch_ns(moment),
        "sequence_number": 1,
        "report_sequence": 1,
        "channel": 1,
        "price": 1.0,
        "session_end_date": session,
    }


def session_target(ticker: str, day: str) -> str:
    return f"/futures/v1/trades/{ticker}?limit=50000&sort=timestamp.asc&session_end_date={day}"


def test_futures_trades_by_date_read_each_session_by_its_date(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # CME traded Martin Luther King Day (Monday 2025-01-20) from Sunday 17:00 Chicago time
    # under the trade date 2025-01-21, a day before that session's regular open: asking for
    # the session by its date reaches it without any time window.
    sunday = futures_trade(datetime(2025, 1, 19, 18, tzinfo=CHICAGO), "2025-01-21")
    tuesday = futures_trade(datetime(2025, 1, 21, 10, tzinfo=CHICAGO), "2025-01-21")
    friday = futures_trade(datetime(2025, 1, 17, 10, tzinfo=CHICAGO), "2025-01-17")
    fake_rest.reply(
        session_target("ESH5", "2025-01-21"), Reply(envelope([friday, sunday, tuesday]))
    )
    out = history(session, FUTURES_TRADES, start="2025-01-21", end="2025-01-21", symbols=["ESH5"])
    path = FUTURES_TRADES_PATH.format(ticker="ESH5")
    # `sort` lists no values: the time axis's field, ascending.
    assert fake_rest.get_queries() == [
        (path, {"limit": ["50000"], "sort": ["timestamp.asc"], "session_end_date": ["2025-01-21"]})
    ]
    # A row of another session is dropped.
    assert [r["Timestamp"] for r in out] == [sunday["timestamp"], tuesday["timestamp"]]


def test_a_futures_session_without_data_is_left_out_as_its_own_problem(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Each session is its own request series, so a 404 on a later session after rows of an
    # earlier one is that session's: it has no data, and the call goes on.
    trade = futures_trade(datetime(2025, 1, 21, 10, tzinfo=CHICAGO), "2025-01-21")
    fake_rest.reply(session_target("ESH5", "2025-01-21"), Reply(envelope([trade])))
    fake_rest.reply(session_target("ESH5", "2025-01-22"), Reply(status=404))
    fake_rest.reply(session_target("ESH5", "2025-01-23"), Reply(status=404))
    events = record_problems(session, monkeypatch)
    with caplog.at_level(logging.WARNING):
        out = history(
            session, FUTURES_TRADES, start="2025-01-21", end="2025-01-23", symbols=["ESH5"]
        )
    assert [r["Timestamp"] for r in out] == [trade["timestamp"]]
    # A problem for each session, and one WARNING for the read, naming the first.
    why = "(HTTP 404 from /futures/v1/trades/{ticker})"
    assert [(e.cause, e.symbol, e.detail) for e in events] == [
        (
            ProblemCause.UNAVAILABLE_SYMBOLS,
            "ESH5",
            f"Massive has no data for it for the session {day} {why}",
        )
        for day in ("2025-01-22", "2025-01-23")
    ]
    assert caplog.messages == [
        no_data_alarm("futures.trades", 1, "ESH5", why=f"for the session 2025-01-22 {why}")
    ]


def test_futures_sessions_skip_weekends_and_come_in_date_order(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    # Good Friday 2024: no session ended on Friday 03-29; Monday 04-01's opened on Thursday.
    days = {"2024-03-28": 10, "2024-03-29": None, "2024-04-01": 9}
    for ticker in ("ESH5", "NQH5"):
        for day, hour in days.items():
            trades = (
                []
                if hour is None
                else [futures_trade(datetime(2024, 3, 28, hour, tzinfo=CHICAGO), day, ticker)]
            )
            fake_rest.reply(session_target(ticker, day), Reply(envelope(trades)))
    with caplog.at_level(logging.INFO):
        out = history(
            session,
            FUTURES_TRADES,
            start="2024-03-28",
            end="2024-04-01",
            symbols=["NQH5", "ESH5"],
        )
    asked = sorted(q["session_end_date"][0] for _, q in fake_rest.get_queries())
    assert asked == ["2024-03-28"] * 2 + ["2024-03-29"] * 2 + ["2024-04-01"] * 2
    # Sessions in date order, and the symbols of one session in symbol order.
    assert [(r["Symbol"], r["Date"]) for r in out] == [
        ("ESH5", "2024-03-28"),
        ("NQH5", "2024-03-28"),
        ("ESH5", "2024-04-01"),
        ("NQH5", "2024-04-01"),
    ]
    assert (
        "futures.trades history: 6 requests, one per session and symbol, more where a session "
        "spans several pages; if the futures subscription is on the Basic tier (5 requests a "
        "minute), they take at least 1 min"
    ) in caplog.messages


def test_a_short_session_download_is_not_announced(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    # A weekend holds no session, and five requests are not announced.
    for day in ("2025-03-03", "2025-03-04", "2025-03-05", "2025-03-06", "2025-03-07"):
        fake_rest.reply(session_target("ESH5", day), Reply(envelope([])))
    with caplog.at_level(logging.INFO):
        history(session, FUTURES_TRADES, start="2025-03-01", end="2025-03-09", symbols=["ESH5"])
    assert len(fake_rest.get_queries()) == 5
    assert "requests a minute" not in caplog.text


def test_a_session_end_date_filter_needs_datetimes(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    q = query(session, FUTURES_TRADES, symbols=["ESH5"], filters={"session_end_date": "2025-01-21"})
    assert isinstance(q, HistoryQuery)
    with pytest.raises(ValueError, match=r"history\(\) dates set session_end_date themselves"):
        q.history("2025-01-21", "2025-01-21", source=HistorySource.REST, wait=True)
    assert fake_rest.get_queries() == []
    # With datetimes, the filter and the time range travel together.
    fake_rest.reply(FUTURES_TRADES_PATH.format(ticker="ESH5"), Reply(envelope([])))
    start = datetime(2025, 1, 21, 8, tzinfo=CHICAGO)
    q.history(start, start + timedelta(hours=1), source=HistorySource.REST, wait=True)
    sent_query = fake_rest.get_queries()[0][1]
    assert sent_query["session_end_date"] == ["2025-01-21"]
    assert sent_query["timestamp.gte"] == [str(epoch_ns(start))]


def test_futures_trades_by_instants_take_the_exact_timestamp_range(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    evening = futures_trade(datetime(2024, 12, 16, 18, tzinfo=CHICAGO), "2024-12-17")
    morning = futures_trade(datetime(2024, 12, 17, 9, tzinfo=CHICAGO), "2024-12-17")
    after = futures_trade(datetime(2024, 12, 17, 16, 30, tzinfo=CHICAGO), "2024-12-17")
    before = futures_trade(datetime(2024, 12, 16, 10, tzinfo=CHICAGO), "2024-12-16")
    fake_rest.reply(
        FUTURES_TRADES_PATH.format(ticker="ESH5"),
        Reply(envelope([before, evening, morning, after])),
    )
    start = datetime(2024, 12, 16, 17, tzinfo=CHICAGO)
    end = datetime(2024, 12, 17, 16, tzinfo=CHICAGO)
    out = history(session, FUTURES_TRADES, start=start, end=end, symbols=["ESH5"])
    q = fake_rest.get_queries()[0][1]
    assert q["timestamp.gte"] == [str(epoch_ns(start))]
    assert q["timestamp.lt"] == [str(epoch_ns(end))]
    assert "session_end_date" not in q
    assert [r["Timestamp"] for r in out] == [evening["timestamp"], morning["timestamp"]]


# --- The time axis -------------------------------------------------------------------------------


# --- Host decisions ----------------------------------------------------------------------------


@pytest.fixture
def snapshots(
    fake_rest: FakeRestServer, load_fixture: Callable[[str, str], bytes]
) -> FakeRestServer:
    fake_rest.reply(
        "/v3/snapshot", Reply(load_fixture("ingest/host_decision", "stocks_real_time.json"))
    )
    return fake_rest


def connection_rows(m: dm.MassiveSession, dh_to_pylist: ToPylist) -> list[dict[str, Any]]:
    assert m._core.diagnostics.wait_until_shown(30)
    return list(dh_to_pylist(m.connections()))


def test_a_decision_is_cached_and_recorded(
    *, session: dm.MassiveSession, snapshots: FakeRestServer, dh_to_pylist: ToPylist
) -> None:
    transport = session._core.rest_transport()
    decision = transport.resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    again = session._core.rest_transport().resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    assert decision is again
    assert (decision.host, decision.recency) == ("socket.massive.com", Recency.REALTIME)
    assert len(snapshots.requests) == 1
    (row,) = connection_rows(session, dh_to_pylist)
    assert (row["Transport"], row["AssetClass"], row["Family"], row["Host"], row["Path"]) == (
        "REST",
        "stocks",
        "TICK",
        "socket.massive.com",
        "/stocks",
    )
    assert (row["Recency"], row["State"], row["Evidence"]) == (
        "REALTIME",
        "CONNECTED",
        "last_trade.timeframe=REAL-TIME",
    )


def test_a_delayed_plan_refuses_the_verb_and_a_later_upgrade_is_seen(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    # The refusal is not cached: the next verb asks again, and sees a plan upgraded meanwhile.
    fake_rest.reply(
        "/v3/snapshot", Reply(load_fixture("ingest/host_decision", "stocks_delayed.json"))
    )
    transport = session._core.rest_transport()
    with pytest.raises(dm.EntitlementError, match=r"^stocks streams would be delayed"):
        transport.resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    fake_rest.reply_first(
        "/v3/snapshot", Reply(load_fixture("ingest/host_decision", "stocks_real_time.json"))
    )
    assert transport.resolve_host(AssetClass.STOCKS, ChannelFamily.TICK).recency is Recency.REALTIME
    assert len(fake_rest.requests) == 2


def test_a_listed_subscription_decides_the_delayed_feed(
    *,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    m = _delayed_session(fake_rest, Subscription.STOCKS)
    try:
        fake_rest.reply(
            "/v3/snapshot", Reply(load_fixture("ingest/host_decision", "stocks_delayed.json"))
        )
        decision = m._core.rest_transport().resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
        assert (decision.host, decision.recency) == ("delayed.massive.com", Recency.DELAYED)
    finally:
        m.close()


def test_a_failed_decision_is_not_cached(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
    dh_to_pylist: ToPylist,
) -> None:
    fake_rest.reply("/v3/snapshot", Reply(b'{"status":"ERROR","request_id":"x1"}', status=401))
    transport = session._core.rest_transport()
    with pytest.raises(dm.AuthenticationError):
        transport.resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    (row,) = connection_rows(session, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert "HTTP 401" in row["LastError"]
    assert "x1" in row["LastError"]
    fake_rest.reply_first(
        "/v3/snapshot", Reply(load_fixture("ingest/host_decision", "stocks_real_time.json"))
    )
    assert transport.resolve_host(AssetClass.STOCKS, ChannelFamily.TICK).recency is Recency.REALTIME


def test_a_defect_while_deciding_caches_nothing(
    *, session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*args: object, **kwargs: object) -> object:
        raise KeyError("bug")

    monkeypatch.setattr(host_decision, "decide", broken)
    transport = _endpoints(session)
    with pytest.raises(KeyError):
        transport.resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    _, owner = transport._state.hosts._claim((AssetClass.STOCKS, ChannelFamily.TICK))
    assert owner  # nothing was cached


def test_sixteen_threads_share_one_decision(
    *, session: dm.MassiveSession, snapshots: FakeRestServer
) -> None:
    arrived = threading.Barrier(17)
    release = threading.Event()

    def hold(_: object) -> None:  # the one request waits until every thread has asked
        release.wait(30)

    snapshots.on_request(hold)
    results: list[object] = []

    def ask() -> None:
        arrived.wait(30)
        results.append(
            session._core.rest_transport().resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
        )

    threads = [threading.Thread(target=ask) for _ in range(16)]
    for thread in threads:
        thread.start()
    arrived.wait(30)
    release.set()
    for thread in threads:
        thread.join(30)
    assert len(results) == 16
    assert all(result is results[0] for result in results)
    assert len(snapshots.requests) == 1


def test_a_caller_that_arrives_while_another_decides_waits_for_its_decision(
    *, session: dm.MassiveSession, snapshots: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The second caller must reach the wait while the first holds the claim: the request is
    # held until the second caller has started waiting on the first one's decision.
    requested = threading.Event()
    release = threading.Event()

    def hold(_: object) -> None:
        requested.set()
        release.wait(30)

    snapshots.on_request(hold)
    waiting = threading.Event()

    def debug(message: str, *_args: object) -> None:  # a caller logs that it starts waiting
        if "another caller is making" in message:
            waiting.set()

    monkeypatch.setattr(shared_lookup.logger, "debug", debug)
    transport = session._core.rest_transport()
    decide = functools.partial(transport.resolve_host, AssetClass.STOCKS, ChannelFamily.TICK)
    with ThreadPoolExecutor(2) as pool:
        maker = pool.submit(decide)
        assert requested.wait(30)
        joiner = pool.submit(decide)
        assert waiting.wait(30)
        release.set()
        decision = maker.result(30)
        assert joiner.result(30) is decision
    assert decision.host == "socket.massive.com"
    assert len(snapshots.requests) == 1


def test_the_transport_is_bound_to_its_core(session: dm.MassiveSession) -> None:
    transport = session._core.rest_transport()
    assert isinstance(transport, rest.RestEndpoints)
    assert type(transport._core) is SessionCore


def test_columns_a_path_does_not_carry_are_null(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([AAPL_DIVIDEND])))
    (row,) = fetch(session, MIXED)
    assert (row["Symbol"], row["CashAmount"], row["StrikePrice"]) == ("AAPL", 0.25, None)


def test_a_missing_symbol_warns_once_per_read_and_an_error_row_once_per_session(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    refused = {"ticker": "I:VIX", "error": "NOT_ENTITLED", "message": "Not entitled"}
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope([refused])))
    serve_pairs(fake_rest)
    with caplog.at_level(logging.WARNING):
        for _ in range(2):
            assert fetch(session, CRYPTO_LAST, symbols=["X:ETHUSD"]) == []
            (row,) = fetch(session, INDEX_SNAPSHOTS, symbols=["I:VIX"])
            assert (row["Symbol"], row["Error"]) == ("I:VIX", "NOT_ENTITLED")
    # Each read is its own table in m.problems(), so each warns of its first missing symbol.
    why = "(HTTP 404 from /v1/last/crypto/{from}/{to})"
    assert alarms(caplog) == [
        no_data_alarm("crypto.last", read, "X:ETHUSD", why=why) for read in (1, 3)
    ]
    assert caplog.text.count("for 1 [I:VIX (NOT_ENTITLED: Not entitled)]") == 1


def test_a_read_fans_out_on_the_callers_pool(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    last = {"price": 1.0, "timestamp": 1704310200000}
    for pair in ("BTC/USD", "ETH/USD"):
        fake_rest.reply(
            f"/v1/last/crypto/{pair}",
            Reply({"status": "success", "request_id": "r", "symbol": pair, "last": last}),
        )
    reader = _endpoints(session)._reader("call-1", session._core.policy)
    path = reader.rest_path(CRYPTO_LAST_PATH)
    request = Request(symbols=("X:BTCUSD", "X:ETHUSD"))
    calls = rest_requests.plan_calls(
        CRYPTO_LAST, path, request, symbols=request.static_symbols, split=split_pair
    )
    with ThreadPoolExecutor(2) as pool:
        table, requests = reader.read(
            CRYPTO_LAST,
            path,
            calls,
            request=request,
            limit=None,
            executor=pool,
            patience=_massive.Patience(math.inf),
            tally=ReadTally(),
        )
        assert not pool._shutdown  # the caller's pool stays open
    assert (table.num_rows, requests) == (2, 2)


class _RefusalError(ValueError):
    """A path's refusal of a parameter, which a test can hold weakly to see it freed."""


def test_a_parameter_one_path_refuses_leaves_no_cycle(
    *, session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The crypto path refuses `adjusted` and the stock path takes it, so the check passes. The
    # crypto path's error must be freed when the check returns, not left in a reference cycle
    # with the check's frame: that cycle holds the reader, and through it the session, until
    # the cyclic collector runs. The collector stays off, so only reference counting frees it.
    refusals: list[weakref.ref[_RefusalError]] = []
    real = rest_requests.shared_params

    def base(
        spec: DatasetSpec, rest_path: RestPath, request: Request, *, fixed: Mapping[str, object]
    ) -> tuple[dict[str, object], dict[str, object]]:
        try:
            return real(spec, rest_path, request, fixed=fixed)
        except ValueError as error:
            refusal = _RefusalError(str(error))
            refusals.append(weakref.ref(refusal))
            try:
                raise refusal from None
            finally:
                del refusal  # this frame must not hold its own error

    monkeypatch.setattr(rest, "shared_params", base)
    reader = _endpoints(session)._reader("poll", session._core.policy)
    enabled = gc.isenabled()
    gc.disable()
    try:
        reader.check_parameters(SMA, Request(adjusted=False))
        assert len(refusals) == 1
        assert refusals[0]() is None
    finally:
        if enabled:
            gc.enable()


def test_a_failed_history_is_logged_and_raised(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    fake_rest.reply(TRADES_PATH.format(stockTicker="AAPL"), Reply(b"{}", status=403))
    with caplog.at_level(logging.DEBUG), pytest.raises(dm.EntitlementError):
        history(session, TRADES, start="2025-03-03", end="2025-03-03", symbols=["AAPL"])
    assert "HTTP 403" in caplog.text
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_a_404_without_a_symbol_fails_the_call(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # Only a per-symbol request drops on 404; a list path that is missing is an error.
    with pytest.raises(dm.MassiveError, match="HTTP 404"):
        fetch(session, DIVIDENDS, symbols=["AAPL"])


def test_a_404_once_the_session_is_closed_reports_the_close_not_a_symbol_without_data(
    *, session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    reader = _endpoints(session)._reader("call-1", session._core.policy)
    path = reader.rest_path(CRYPTO_LAST_PATH)
    request = Request(symbols=("X:BTCUSD",))
    calls = rest_requests.plan_calls(
        CRYPTO_LAST, path, request, symbols=request.static_symbols, split=split_pair
    )

    def pages(*_: object, **__: object) -> NoReturn:
        # The session closes while the symbol's request is on the wire, which then ends in 404.
        reader.state.close()
        raise _massive.WireError(
            "HTTP 404 from GET /v1/last/crypto/BTC/USD",
            status=404,
            kind=_massive.WireFailure.REFUSED,
        )

    monkeypatch.setattr(reader.state.client, "pages", pages)
    with (
        caplog.at_level(logging.DEBUG, LOGGER),
        ThreadPoolExecutor(1) as pool,
        pytest.raises(
            dm.MassiveError,
            match=r"^MassiveSession #\d+ is closed; " + re.escape(NEW_SESSION) + "$",
        ),
    ):
        reader.read(
            CRYPTO_LAST,
            path,
            calls,
            request=request,
            limit=None,
            executor=pool,
            patience=_massive.Patience(math.inf),
            tally=ReadTally(),
        )
    assert "has no data" not in caplog.text  # the symbol is not dropped as missing
    assert (
        f"crypto.last: HTTP 404 for X:BTCUSD from {CRYPTO_LAST_PATH} arrived after "
        f"{session._core.label} closed; the call reports the close"
    ) in caplog.messages


def test_an_error_row_needs_only_its_symbol(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # The time key is required of every data row, but an error row carries no time.
    timed = DatasetSpec(
        summary="A dataset for the tests.",
        name="indices.timed",
        asset_class=AssetClass.INDICES,
        fetch_path=INDEX_SNAPSHOT_PATH,
        symbol_param="ticker.any_of",
        keys={(R, INDEX_SNAPSHOT_PATH): SourceKeys(KeySpec("Ticker"), KeySpec("LastUpdated"))},
    )
    refused = {"ticker": "I:VIX", "error": "NOT_ENTITLED", "message": "Not entitled"}
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope([refused])))
    reader = _endpoints(session)._reader("call-1", session._core.policy)
    table, _ = reader.read_fetch(
        timed, Request(symbols=("I:VIX",)), symbols=("I:VIX",), tally=ReadTally()
    )
    assert table.column("Symbol").to_pylist() == ["I:VIX"]
    assert table.column("Timestamp").to_pylist() == [None]


def test_a_failed_call_cancels_the_calls_not_yet_started(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_rest.reply("/v1/last/crypto/BTC/USD", Reply(b"{}", status=401))
    reader = _endpoints(session)._reader("call-1", session._core.policy)
    path = reader.rest_path(CRYPTO_LAST_PATH)
    symbols = ("X:BTCUSD", "X:ETHUSD", "X:SOLUSD", "X:XRPUSD")
    calls = rest_requests.plan_calls(
        CRYPTO_LAST, path, Request(symbols=symbols), symbols=symbols, split=split_pair
    )
    # The other symbols are answered at once with HTTP 404 (no rows), so the one worker could
    # run through every queued call before the read wakes up to cancel them. Each call after
    # the failed one is held at the server until the last two calls are done, which only the
    # read's cancellation can make happen while the worker is held.
    queued: list[Future[object]] = []  # complete before any callback reads it
    cancelled = threading.Event()

    def settled(_: Future[object]) -> None:
        if all(future.done() for future in queued[2:]):
            cancelled.set()

    def hold(received: Received) -> None:
        if "BTC" not in received.target and not cancelled.wait(10):
            cancelled.set()  # nothing cancelled the last calls: let them run, so the count fails

    fake_rest.on_request(hold)
    pool = ThreadPoolExecutor(1)
    submit = pool.submit

    def recorded(fn: Callable[[object], object], call: object, /) -> Future[object]:
        future = submit(fn, call)
        queued.append(future)
        if len(queued) == len(calls):
            for each in queued[2:]:
                each.add_done_callback(settled)
        return future

    monkeypatch.setattr(pool, "submit", recorded)
    request = Request(symbols=symbols)
    with pool, pytest.raises(dm.AuthenticationError):
        reader.read(
            CRYPTO_LAST,
            path,
            calls,
            request=request,
            limit=None,
            executor=pool,
            patience=_massive.Patience(math.inf),
            tally=ReadTally(),
        )
    # The one worker may take the next call before the read cancels it; the rest never start.
    assert len(fake_rest.get_queries()) <= 2


def test_a_decision_is_shared_even_if_recording_it_fails(
    *, session: dm.MassiveSession, snapshots: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(self: object, row: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(Diagnostics, "write", broken)
    transport = session._core.rest_transport()
    with pytest.raises(KeyboardInterrupt):
        transport.resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    monkeypatch.undo()
    # The decision was completed before the row: a later caller gets it, never blocks.
    later = session._core.rest_transport().resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    assert later.recency is Recency.REALTIME
    assert len(snapshots.requests) == 1


def test_a_parameter_no_fetch_path_takes_fails_a_reactive_poll_at_once(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    watch = new_table([string_col("Symbol", ["X:BTCUSD"])])
    q = query(session, SMA, symbols=watch, params={"bogus": 1})
    assert isinstance(q, FetchQuery)
    with pytest.raises(ValueError, match=r"does not take \['bogus'\]"):
        q.poll(60)
    assert fake_rest.requests == []


def test_an_empty_symbol_tuple_reads_nothing(*, session: dm.MassiveSession) -> None:
    reader = _endpoints(session)._reader("call-1", session._core.policy)
    request = Request(symbols=(), adjusted=False)  # no path would take `adjusted` here
    assert reader.plan_fetch(DIVIDENDS, request, ()) == []
    table, requests = reader.read_fetch(DIVIDENDS, request, symbols=(), tally=ReadTally())
    assert (table.num_rows, requests) == (0, 0)
    assert table.schema.names[0] == "Symbol"


def test_an_error_row_of_a_request_symbol_names_that_symbol(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    refused = {"error": "NOT_ENTITLED", "message": "Not entitled"}
    fake_rest.reply(
        "/v1/last/crypto/BTC/USD",
        Reply({"status": "success", "request_id": "r", "last": refused}),
    )
    serve_pairs(fake_rest)
    with caplog.at_level(logging.WARNING, LOGGER):
        (row,) = fetch(session, CRYPTO_LAST, symbols=["X:BTCUSD"])
    assert row["Symbol"] == "X:BTCUSD"
    assert (
        "error rows from /v1/last/crypto/{from}/{to} for 1 [X:BTCUSD (NOT_ENTITLED: Not entitled)]"
        in caplog.text
    )


def test_a_nanosecond_inclusive_axis_fails_a_reactive_poll_at_once(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    path = "/futures/v1/trades/{ticker}"
    spec = DatasetSpec(
        summary="A dataset for the tests.",
        name="futures.inclusive",
        asset_class=AssetClass.FUTURES,
        fetch_path=path,
        time_axis=("timestamp.gte", "timestamp.lte"),
        keys={(R, path): SourceKeys(symbol=KeySpec("ticker", from_request=True))},
    )
    transport = _endpoints(session)
    watch = SymbolTable(new_table([string_col("Symbol", ["ESZ5"])]))
    request = Request(symbols=watch, end=date(2025, 3, 4))
    with pytest.raises(ValueError, match=r"timestamp\.lte takes nanoseconds"):
        transport.poll(
            spec,
            request,
            interval=timedelta(seconds=60),
            mode=PollMode.REPLACE,
            query="<Query test>",
        )
    assert fake_rest.requests == []


# --- Whole bar windows, open and empty ranges, and 1-day stamps --------------------------------


def ny_ms(day: date) -> int:
    return (
        int(datetime(day.year, day.month, day.day, tzinfo=ZoneInfo("America/New_York")).timestamp())
        * 1000
    )


def utc_ms(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp()) * 1000


def test_week_bars_request_the_first_whole_week_and_keep_whole_weeks(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # Wednesday 03-05 to Friday 03-21 holds the weeks of 03-10 and 03-17 whole. The request
    # starts on the Sunday Massive starts the first of them on, so its bar is returned
    # whether Massive selects bars by stamp or builds them from `{from}`.
    start, end = ny_ms(date(2025, 3, 9)), ny_ms(date(2025, 3, 22)) - 1
    path = f"/v2/aggs/ticker/AAPL/range/1/week/{start}/{end}"
    stamps = [ny_ms(date(2025, 3, d)) for d in (2, 9, 16, 23)]  # Sundays, where Massive stamps
    fake_rest.reply(path, Reply(envelope([{"t": t, "c": 1.0} for t in stamps])))
    out = history(
        session,
        BARS,
        start="2025-03-05",
        end="2025-03-21",
        symbols=["AAPL"],
        timespan=Timespan.WEEK,
        adjusted=False,
    )
    assert [p for p, _ in fake_rest.get_queries()] == [path]
    # Each week, stamped on its Sunday, runs from its Monday's midnight to the Saturday's
    # after its Friday, and spans several dates, so it has no `Date`.

    assert [(r["TimestampStart"], r["TimestampEnd"], r["Date"]) for r in out] == [
        (ny_ms(date(2025, 3, 10)) * 10**6, ny_ms(date(2025, 3, 15)) * 10**6, None),
        (ny_ms(date(2025, 3, 17)) * 10**6, ny_ms(date(2025, 3, 22)) * 10**6, None),
    ]


def test_an_open_end_leaves_out_the_unfinished_month(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(history_source, "_now", lambda: datetime(2025, 6, 18, 15, tzinfo=UTC))
    start, end = ny_ms(date(2025, 4, 1)), ny_ms(date(2025, 6, 19)) - 1
    path = f"/v2/aggs/ticker/AAPL/range/1/month/{start}/{end}"
    stamps = [ny_ms(date(2025, m, 1)) for m in (4, 5, 6)]
    fake_rest.reply(path, Reply(envelope([{"t": t, "c": 1.0} for t in stamps])))
    out = history(
        session,
        BARS,
        start="2025-04-01",
        end=None,
        symbols=["AAPL"],
        timespan=Timespan.MONTH,
        adjusted=False,
    )
    assert [p for p, _ in fake_rest.get_queries()] == [path]
    # Each month ends at midnight after its last weekday: Wednesday 04-30 and Friday 05-30.
    assert [(r["TimestampStart"], r["TimestampEnd"]) for r in out] == [
        (stamps[0] * 10**6, ny_ms(date(2025, 5, 1)) * 10**6),
        (stamps[1] * 10**6, ny_ms(date(2025, 5, 31)) * 10**6),
    ]


@pytest.mark.parametrize("source", [HistorySource.REST, HistorySource.AUTO])
def test_an_empty_range_returns_every_column_without_a_request(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, source: HistorySource
) -> None:
    moment = datetime(2025, 3, 3, 15, tzinfo=UTC)
    q = query(session, TRADES, symbols=["AAPL"])
    assert isinstance(q, HistoryQuery)
    table = q.history(moment, moment, source=source, wait=True)
    assert table.size == 0
    assert "Timestamp" in table.column_names
    # So is a range too short to hold one whole bar.
    bars = query(session, BARS, symbols=["AAPL"], timespan=Timespan.MONTH, adjusted=False)
    assert isinstance(bars, HistoryQuery)
    assert bars.history("2025-03-05", "2025-03-20", source=source, wait=True).size == 0
    assert fake_rest.get_queries() == []


def test_a_long_grouped_daily_download_is_announced(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    for day in range(3, 18):
        fake_rest.reply(GROUPED_PATH.format(date=f"2025-03-{day:02d}"), Reply(envelope([])))
    with caplog.at_level(logging.INFO):
        history(
            session,
            BARS,
            start="2025-03-03",
            end="2025-03-17",
            symbols=dm.ALL,
            timespan=Timespan.DAY,
            adjusted=True,
        )
    # Stocks have no grouped day on a Saturday or Sunday: 11 weekdays, not 15 days.
    days = sorted(path.rsplit("/", 1)[1] for path, _ in fake_rest.get_queries())
    assert len(days) == 11
    assert all(date.fromisoformat(day).weekday() < 5 for day in days)
    assert (
        "stocks.bars history: 11 requests, one per day; if the stocks subscription is on the "
        "Basic tier (5 requests a minute), they take at least 2 min"
    ) in caplog.text


def unexpected_stamps(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return the WARNINGs of bars stamped where Massive is not known to stamp one."""
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and "is not a stamp Massive is known to use" in r.message
    ]


def test_forex_day_bars_are_stamped_at_the_utc_days_start(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The range's 00:00 UTC stays and the day's last millisecond moves to 00:00 UTC.
    march_3, march_4 = utc_ms(date(2025, 3, 3)), utc_ms(date(2025, 3, 4))
    path = f"/v2/aggs/ticker/C:EURUSD/range/1/day/{march_3}/{utc_ms(date(2025, 3, 5)) - 1}"
    bars = [{"t": march_3, "c": 1.0}, {"t": march_4 + 86_400_000 - 1, "c": 2.0}]
    fake_rest.reply(path.replace(":", "%3A"), Reply(envelope(bars)))
    out = history(
        session,
        FOREX_BARS,
        start="2025-03-03",
        end="2025-03-04",
        symbols=["C:EURUSD"],
        timespan=Timespan.DAY,
        adjusted=True,
    )
    assert [(r["Date"], r["TimestampStart"], r["TimestampEnd"], r["Close"]) for r in out] == [
        ("2025-03-03", march_3 * 10**6, march_4 * 10**6, 1.0),
        ("2025-03-04", march_4 * 10**6, (march_4 + 86_400_000) * 10**6, 2.0),
    ]
    # The grouped day of every pair, stamped at its last millisecond.
    grouped = FOREX_GROUPED_PATH.format(date="2025-03-03")
    last_ms, new_york = march_4 - 1, ny_ms(date(2025, 3, 3))
    fake_rest.reply(
        grouped,
        Reply(envelope([{"T": "C:EURUSD", "t": last_ms, "c": 3.0}])),
        Reply(
            envelope(
                [
                    {"T": "C:EURUSD", "t": new_york, "c": 3.0},
                    {"T": "C:GBPUSD", "t": last_ms, "c": 4.0},
                ]
            )
        ),
    )
    every = history(
        session,
        FOREX_BARS,
        start="2025-03-03",
        end="2025-03-03",
        symbols=dm.ALL,
        timespan=Timespan.DAY,
        adjusted=True,
    )
    assert [(r["Symbol"], r["TimestampStart"]) for r in every] == [("C:EURUSD", march_3 * 10**6)]
    assert not unexpected_stamps(caplog)
    # A stamp at midnight New York time may be another day cut: which day the bar covers
    # would be a guess, so it is withheld and reported, beside the pair stamped as Massive
    # does; one WARNING names the symbol and the stamp.
    events: list[ProblemEvent] = []
    monkeypatch.setattr(session._core, "problems", events.extend)
    with caplog.at_level(logging.WARNING):
        odd = history(
            session,
            FOREX_BARS,
            start="2025-03-03",
            end="2025-03-03",
            symbols=dm.ALL,
            timespan=Timespan.DAY,
            adjusted=True,
        )
    assert [(r["Symbol"], r["Close"]) for r in odd] == [("C:GBPUSD", 4.0)]
    assert [(e.cause, e.symbol, e.field) for e in events] == [
        (ProblemCause.UNCONVERTIBLE_VALUES, "C:EURUSD", "TimestampStart")
    ]
    assert events[0].detail == (
        "stamped at 2025-03-03T05:00:00+00:00, which Massive is not known to use for a 1-day bar"
    )
    [warning] = unexpected_stamps(caplog)
    assert warning.startswith(
        "forex.bars: a 1-day bar of C:EURUSD from /v2/aggs/grouped/locale/global/market/fx/{date} "
        "is stamped 2025-03-03T05:00:00+00:00, which is not a stamp Massive is known to use for "
        "such a bar (00:00:00.000, 23:59:59.999 in UTC). The library cannot tell which day it "
        "covers, so the row is withheld"
    )


def test_a_history_by_instants_is_not_announced_however_many_symbols(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    # Only one-request-per-day downloads are announced; six symbols of one day are not.
    tickers = ["A", "B", "C", "D", "E", "F"]
    for ticker in tickers:
        fake_rest.reply(TRADES_PATH.format(stockTicker=ticker), Reply(envelope([])))
    with caplog.at_level(logging.INFO):
        history(session, TRADES, start="2025-03-03", end="2025-03-03", symbols=tickers)
    assert len(fake_rest.get_queries()) == 6
    assert "requests a minute" not in caplog.text


def crypto_bars(
    m: dm.MassiveSession, timespan: Timespan, *, multiplier: int, start: str, end: str
) -> list[dict[str, Any]]:
    return history(
        m,
        CRYPTO_BARS,
        start=start,
        end=end,
        symbols=["X:BTCUSD"],
        timespan=timespan,
        multiplier=multiplier,
    )


def test_longer_crypto_bars_are_expected_at_utc_midnight(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)
    # A two-day bar stamped at midnight New York time may hold hours of the next day: it is
    # withheld and logs one WARNING; a page of no other bar fails the read.
    start, end = utc_ms(date(2025, 3, 3)), utc_ms(date(2025, 3, 7)) - 1
    two_days = f"/v2/aggs/ticker/X:BTCUSD/range/2/day/{start}/{end}".replace(":", "%3A")
    new_york = ny_ms(date(2025, 3, 3))
    fake_rest.reply(two_days, Reply(envelope([{"t": new_york, "c": 1.0}])))
    with pytest.raises(RowDataError, match="could read none of the 1 rows"):
        crypto_bars(session, Timespan.DAY, multiplier=2, start="2025-03-03", end="2025-03-06")
    [warning] = unexpected_stamps(caplog)
    assert warning.startswith(
        "crypto.bars: a bar of X:BTCUSD from /v2/aggs/ticker/{cryptoTicker}/range/{multiplier}/"
        "{timespan}/{from}/{to} is stamped 2025-03-03T05:00:00+00:00, which is not a stamp "
        "Massive is known to use for such a bar (00:00:00.000 in UTC). The library cannot tell "
        "which day it covers, so the row is withheld rather than published at a guessed day "
        "(bars of 2 days)"
    )
    caplog.clear()
    # Massive stamps a crypto week at 00:00 UTC on its Sunday (2026-10-11 in the live check),
    # and the week runs to the next Sunday: it is kept as stamped.
    sunday, after = utc_ms(date(2026, 10, 11)), utc_ms(date(2026, 10, 18))
    week = f"/v2/aggs/ticker/X:BTCUSD/range/1/week/{sunday}/{after - 1}".replace(":", "%3A")
    monday = sunday + 86_400_000
    fake_rest.reply(
        week,
        Reply(envelope([{"t": sunday, "c": 2.0}])),
        Reply(envelope([{"t": monday, "c": 2.0}])),
    )
    out = crypto_bars(session, Timespan.WEEK, multiplier=1, start="2026-10-11", end="2026-10-17")
    assert [(r["TimestampStart"], r["TimestampEnd"], r["Close"]) for r in out] == [
        (sunday * 10**6, after * 10**6, 2.0)
    ]
    assert not unexpected_stamps(caplog)
    # A week bar stamped on another day may cover another week: it is withheld, and its form
    # warns.
    with pytest.raises(RowDataError, match="could read none of the 1 rows"):
        crypto_bars(session, Timespan.WEEK, multiplier=1, start="2026-10-11", end="2026-10-17")
    [warning] = unexpected_stamps(caplog)
    assert (
        "is stamped 2026-10-12T00:00:00+00:00, which is not a stamp Massive is known to use for "
        "such a bar (00:00:00.000 in UTC, on a Sunday)"
    ) in warning
    caplog.clear()

    # A month bar must be stamped on its 1st.
    first, april = utc_ms(date(2025, 3, 1)), utc_ms(date(2025, 4, 1))
    month = f"/v2/aggs/ticker/X:BTCUSD/range/1/month/{first}/{april - 1}".replace(":", "%3A")
    second = first + 86_400_000
    fake_rest.reply(
        month,
        Reply(envelope([{"t": first, "c": 3.0}])),
        Reply(envelope([{"t": second, "c": 3.0}])),
    )
    out = crypto_bars(session, Timespan.MONTH, multiplier=1, start="2025-03-01", end="2025-03-31")
    # A crypto month is the whole calendar month, weekends included.
    assert [(r["TimestampStart"], r["TimestampEnd"], r["Close"]) for r in out] == [
        (first * 10**6, april * 10**6, 3.0)
    ]
    assert not unexpected_stamps(caplog)
    # A month bar stamped on the 2nd is withheld.
    with pytest.raises(RowDataError, match="could read none of the 1 rows"):
        crypto_bars(session, Timespan.MONTH, multiplier=1, start="2025-03-01", end="2025-03-31")
    [warning] = unexpected_stamps(caplog)
    assert "is stamped 2025-03-02T00:00:00+00:00" in warning
    assert "on the 1st of a month)" in warning


def test_forex_grouped_daily_keeps_its_sunday_but_not_its_saturday(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # Forex trades from Sunday evening to Friday evening UTC: its Sunday is a grouped day, its
    # Saturday holds no trading and is not requested.
    for day in ("2025-03-07", "2025-03-09"):
        fake_rest.reply(FOREX_GROUPED_PATH.format(date=day), Reply(envelope([])))
    history(
        session,
        FOREX_BARS,
        start="2025-03-07",
        end="2025-03-09",
        symbols=dm.ALL,
        timespan=Timespan.DAY,
        adjusted=True,
    )
    assert sorted(path.rsplit("/", 1)[1] for path, _ in fake_rest.get_queries()) == [
        "2025-03-07",
        "2025-03-09",
    ]


# --- Rate limits ---------------------------------------------------------------------------------


def governor_of(m: dm.MassiveSession) -> _massive.RequestGovernor:
    return _endpoints(m)._state.client.governor


def requests_by_bucket(m: dm.MassiveSession) -> dict[str, int]:
    return {state.bucket: state.attempts for state in governor_of(m).snapshot() if state.attempts}


def test_each_request_is_charged_to_its_subscription() -> None:
    aapl = rest_requests.Call({}, {"ticker.any_of": ("AAPL",)})
    assert rest.call_bucket(DIVIDENDS, DIVIDENDS_PATH, aapl) == "stocks"
    btc = rest_requests.Call({}, {}, "X:BTCUSD")
    assert rest.call_bucket(CRYPTO_LAST, CRYPTO_LAST_PATH, btc) == "currencies"
    eur = rest_requests.Call({}, {}, "C:EURUSD")
    assert rest.call_bucket(CONVERSION, CONVERSION_PATH, eur) == "currencies"
    # A dataset with a path per symbol class follows the class its request's symbols share.
    assert rest.call_bucket(SMA, SMA_CRYPTO, btc) == "currencies"
    assert rest.call_bucket(SMA, SMA_STOCK, rest_requests.Call({}, {}, "AAPL")) == "stocks"


@pytest.mark.parametrize("market", list(Market))
def test_a_reference_request_is_charged_to_other_whatever_market_it_filters(
    market: Market,
) -> None:
    # Massive counts reference requests in a bucket of their own, not under the asset class
    # they filter by; the session's own crypto ticker list goes there too.
    call = rest_requests.Call({}, {"market": market, "active": True})
    tickers = "/v3/reference/tickers"
    assert rest.call_bucket(reference.TICKERS, tickers, call) == "other"
    assert (
        rest.call_bucket(reference.TICKERS, tickers, rest_requests.Call({}, {"active": True}))
        == "other"
    )


@pytest.mark.parametrize(
    ("spec", "template"),
    [
        (options_ns.CONTRACTS, "/v3/reference/options/contracts"),
        (stocks_ns.IPOS, "/vX/reference/ipos"),
        (NEWS, NEWS_PATH),
    ],
    ids=["options contracts", "IPOs", "news"],
)
def test_every_reference_endpoint_is_charged_to_other_whatever_its_dataset_class(
    spec: DatasetSpec, template: str
) -> None:
    # A dataset of one class (options contracts, IPOs) still reads a reference endpoint,
    # which Massive counts in the reference bucket.
    assert rest.call_bucket(spec, template, rest_requests.Call({}, {}, "AAPL")) == "other"


def test_each_path_logs_its_bucket_and_why_once(
    *,
    session: dm.MassiveSession,
    snapshots: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_log_limits, "_logged", LoggedKeys())  # as in a fresh process
    snapshots.reply(DIVIDENDS_PATH, Reply(envelope([AAPL_DIVIDEND])))
    snapshots.reply(NEWS_PATH, Reply(envelope([])))
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive._ingest.rest"):
        fetch(session, DIVIDENDS, symbols=["AAPL"])
        fetch(session, DIVIDENDS, symbols=["MSFT"])  # the same path and bucket: not logged again
        fetch(session, NEWS)
    charged = [message for message in caplog.messages if " is charged to the " in message]
    later = "; later requests of this path charged to it are not logged"
    stocks = "is charged to the stocks bucket: its data's class"
    other = "is charged to the other bucket: a reference endpoint"
    assert charged == [
        f"{DIVIDENDS.name} {DIVIDENDS_PATH} (symbols: AAPL) {stocks}{later}",
        f"{NEWS.name} {NEWS_PATH} (symbols: none) {other}{later}",
    ]
    # Each fetch names its patience once, in its own line, not once per request.
    reads = [m for m in caplog.messages if " fetch: " in m]
    assert reads == [
        f"{DIVIDENDS.name} fetch: 1 requests on 1 paths; the read waits out 600 s of pauses",
        f"{DIVIDENDS.name} fetch: 1 requests on 1 paths; the read waits out 600 s of pauses",
        f"{NEWS.name} fetch: 1 requests on 1 paths; the read waits out 600 s of pauses",
    ]


def test_a_session_charges_reads_and_host_decisions_to_their_buckets(
    *, session: dm.MassiveSession, snapshots: FakeRestServer
) -> None:
    snapshots.reply(DIVIDENDS_PATH, Reply(envelope([AAPL_DIVIDEND])))
    snapshots.reply(NEWS_PATH, Reply(envelope([])))
    fetch(session, DIVIDENDS, symbols=["AAPL"])
    fetch(session, NEWS)
    # The stocks host is decided from the unified snapshot, but for the stocks subscription.
    session._core.rest_transport().resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    assert requests_by_bucket(session) == {"stocks": 2, "other": 1}


def test_a_learned_basic_subscription_is_paced_and_shown(
    *,
    fake_rest: FakeRestServer,
    rest_clock: FakeClock,
    dh_to_pylist: ToPylist,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    basic = Reply({"message": "You've exceeded the maximum requests per minute"}, status=429)
    fake_rest.reply(DIVIDENDS_PATH, basic, Reply(envelope([AAPL_DIVIDEND])))
    try:
        with caplog.at_level(logging.INFO):
            assert [r["Symbol"] for r in fetch(m, DIVIDENDS, symbols=["AAPL"])] == ["AAPL"]
        assert rest_clock.waits == [61.0]
        assert "that is the Basic tier's limit of 5 requests per minute" in caplog.text
        assert m._core.diagnostics.wait_until_shown(30)
        stocks = next(r for r in dh_to_pylist(m.rate_limits()) if r["Bucket"] == "stocks")
        assert (stocks["RequestsPerMinute"], stocks["LimitSource"]) == (5, "LEARNED")
        assert (stocks["Attempts"], stocks["Refusals"]) == (2, 1)
    finally:
        m.close()


def test_a_paced_fetch_announces_how_long_it_takes(
    *, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    config = dm.MassiveConfig(
        api_key="test-key",
        rest_base_url=fake_rest.url,
        rest_basic_subscriptions={dm.Subscription.CURRENCIES},
    )
    m = dm.MassiveSession(config)
    pairs = ["C:EURUSD", "C:GBPUSD", "C:AUDUSD", "C:USDJPY", "C:USDCHF", "C:NZDUSD"]
    for pair in pairs:
        base, quote = pair[2:5], pair[5:]
        body = {"status": "success", "from": base, "to": quote, "symbol": f"{base}/{quote}"}
        fake_rest.reply(f"/v1/conversion/{base}/{quote}", Reply(body))
    try:
        with caplog.at_level(logging.INFO):
            fetch(m, CONVERSION, symbols=pairs)
    finally:
        m.close()
    assert (
        "forex.conversion fetch: 6 requests; paced at 5 requests a minute (currencies); at that "
        "pace they take at least 1 min"
    ) in caplog.messages


def test_a_failed_call_stops_the_calls_waiting_for_a_slot_and_is_the_one_raised(
    *, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A manual clock: a call waiting for a slot can only stop by being cancelled.
    clock = FakeClock(manual=True)
    create = functools.partial(
        _massive.RequestGovernor,
        rate_limits.BUCKETS,
        learn_only=rate_limits.LEARN_ONLY,
        clock=clock,
    )
    monkeypatch.setattr(rate_limits, "GOVERNORS", rate_limits.GovernorRegistry(create))
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    config = dm.MassiveConfig(
        api_key="test-key",
        rest_base_url=fake_rest.url,
        max_concurrency=3,
        rest_basic_subscriptions={dm.Subscription.CURRENCIES},
    )
    m = dm.MassiveSession(config)
    governor = governor_of(m)
    for _ in range(4):  # one slot is left
        governor.done(
            governor.admission("currencies", lane=object()).acquire(_massive.Cancel()),
            answered=True,
        )
    refused = Reply({"status": "NOT_AUTHORIZED", "message": "upgrade"}, status=403)
    for pair in ("EUR/USD", "GBP/USD", "AUD/USD"):
        fake_rest.reply(f"/v1/conversion/{pair}", refused)
    errors: list[BaseException] = []

    def read() -> None:
        try:
            fetch(m, CONVERSION, symbols=["C:EURUSD", "C:GBPUSD", "C:AUDUSD"])
        except dm.EntitlementError as error:
            errors.append(error)

    try:
        # On its own thread, so a call left waiting for a slot fails the test, not hangs it.
        reader = threading.Thread(target=read, daemon=True)
        reader.start()
        reader.join(TIMEOUT)
        assert not reader.is_alive(), "the calls waiting for a slot were not stopped"
        assert len(errors) == 1
        assert "upgrade" in str(errors[0])
        assert len(fake_rest.requests) == 1  # the others gave up their places unsent
        assert next(s for s in governor.snapshot() if s.bucket == "currencies").waiting == 0
    finally:
        m.close()


def test_a_poll_whose_cycles_outlast_their_interval_warns(
    *,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    monkeypatch.setattr(poll, "CLOCK", FakeClock(manual=True))
    config = dm.MassiveConfig(
        api_key="test-key",
        rest_base_url=fake_rest.url,
        rest_basic_subscriptions={dm.Subscription.STOCKS},
    )
    m = dm.MassiveSession(config)
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([])))
    q = query(m, DIVIDENDS, symbols=["A", "B", "C", "D", "E", "F"])
    assert isinstance(q, FetchQuery)
    warned = threading.Event()

    class _Late(logging.Handler):
        @override
        def emit(self, record: logging.LogRecord) -> None:
            if "cycles run late" in record.getMessage():
                warned.set()

    late = _Late()
    logging.getLogger(rate_limits.__name__).addHandler(late)
    try:
        with caplog.at_level(logging.WARNING):
            table = q.poll(30)
            assert warned.wait(TIMEOUT)  # logged by the poller's first cycle
        m.release(table)
    finally:
        logging.getLogger(rate_limits.__name__).removeHandler(late)
        m.close()
    assert (
        "stocks.dividends poll(): each cycle makes 3 stocks requests, but they are paced at 5 "
        "per minute, so a cycle takes at least 37 s, longer than the 30 s interval; cycles run "
        "late"
    ) in caplog.messages


def test_a_fan_out_raises_the_real_failure_not_a_call_it_cancelled(
    session: dm.MassiveSession,
) -> None:
    # The first call only fails because the second one's failure cancelled its wait.
    reader = _endpoints(session)._reader("call-1", session._core.policy)
    stop = _massive.Cancel()

    def run(call: rest_requests.Call) -> NoReturn:
        if call.symbol == "A":
            assert stop.wait(10)
            raise dm.MassiveError("cancelled")
        raise dm.EntitlementError("not entitled")

    calls = [rest_requests.Call({}, {}, "A"), rest_requests.Call({}, {}, "B")]
    with ThreadPoolExecutor(2) as pool, pytest.raises(dm.EntitlementError):
        reader._fan_out(run, calls, executor=pool, stop=stop)
    assert stop.is_set()


def test_a_poll_nothing_uses_stops_waiting_for_a_slot(
    *, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = FakeClock(manual=True)  # a waiting cycle can only stop through its poller's token
    create = functools.partial(
        _massive.RequestGovernor,
        rate_limits.BUCKETS,
        learn_only=rate_limits.LEARN_ONLY,
        clock=clock,
    )
    monkeypatch.setattr(rate_limits, "GOVERNORS", rate_limits.GovernorRegistry(create))
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    monkeypatch.setattr(poll, "CLOCK", FakeClock(manual=True))
    config = dm.MassiveConfig(
        api_key="test-key",
        rest_base_url=fake_rest.url,
        rest_basic_subscriptions={dm.Subscription.STOCKS},
    )
    m = dm.MassiveSession(config)
    governor = governor_of(m)
    for _ in range(5):
        governor.done(
            governor.admission("stocks", lane=object()).acquire(_massive.Cancel()), answered=True
        )
    q = query(m, DIVIDENDS, symbols=["AAPL"])
    assert isinstance(q, FetchQuery)
    left = threading.Event()

    def on_change() -> None:  # the governor's notice once the cycle gives up its place
        if not next(s for s in governor.snapshot() if s.bucket == "stocks").waiting:
            left.set()

    try:
        table = q.poll(60)
        assert clock.wait_for_sleepers(1) == [61.0]  # the first cycle waits for a slot
        governor.subscribe(on_change)
        m.release(table)
        del table  # nothing uses the poll any more: its blink table shuts down
        gc.collect()
        assert left.wait(TIMEOUT)
        assert fake_rest.requests == []
        # The cancelled wait also reached the session's rows.
        assert m._core.diagnostics.wait_until_shown(TIMEOUT)
        limits = dh_arrow.to_arrow(m.rate_limits()).to_pylist()
        stocks = next(r for r in limits if r["Bucket"] == "stocks")
        assert stocks["Waiting"] == 0
    finally:
        governor.unsubscribe(on_change)
        m.close()


class _HeldFanOut:
    """A fan-out of AAPL and MSFT trades whose first MSFT page waits for the read's token.

    AAPL is answered only once MSFT's first page is on the wire, and that page only once the
    token is set. The read's own `wait` is wrapped: its second call, the one that waits for
    every call after a failure or an interrupt, comes after the token is set, and only then is
    MSFT's first page let go. So whether MSFT asks for a second page depends on the token
    alone, never on timing.
    """

    def __init__(
        self, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch, *, interrupt: bool
    ) -> None:
        msft = TRADES_PATH.format(stockTicker="MSFT")
        start = epoch_ns(datetime(2025, 3, 3, 5, tzinfo=UTC))
        row = {"sip_timestamp": start + 1, "price": 1.0, "size": 1}
        for index in range(15):  # fifteen pages, if nothing stopped them
            target = msft if index == 0 else f"{msft}?cursor={index}"
            following = None if index == 14 else f"{fake_rest.url}{msft}?cursor={index + 1}"
            fake_rest.reply(target, Reply(envelope([row], next_url=following)))
        self.paging = threading.Event()  # MSFT's first page was asked for
        self.stopped = threading.Event()  # the read's token is set
        fake_rest.on_request(self._hold)
        real = futures_wait

        def wait(futures: Sequence[Future[object]], return_when: str = ALL_COMPLETED) -> object:
            if return_when == FIRST_EXCEPTION and interrupt:
                assert self.paging.wait(TIMEOUT)
                raise KeyboardInterrupt
            if return_when == ALL_COMPLETED:  # called in the read's `finally`, token set
                self.stopped.set()
            return real(futures, return_when=return_when)

        monkeypatch.setattr(rest, "wait", wait)

    def _hold(self, received: Received) -> None:
        if "AAPL" in received.target:
            assert self.paging.wait(TIMEOUT)
        else:
            self.paging.set()
            assert self.stopped.wait(TIMEOUT)


def test_a_failed_symbol_stops_the_others_paging_after_the_page_on_the_wire(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _HeldFanOut(fake_rest, monkeypatch, interrupt=False)
    fake_rest.reply(TRADES_PATH.format(stockTicker="AAPL"), Reply(b"{}", status=401))
    with pytest.raises(dm.AuthenticationError):
        history(session, TRADES, start="2025-03-03", end="2025-03-03", symbols=["AAPL", "MSFT"])
    # The read's token is checked before every page: MSFT finishes the page on the wire and
    # asks for no other.
    assert sum(1 for path, _ in fake_rest.get_queries() if "MSFT" in path) == 1


def test_an_interrupted_fan_out_stops_its_running_calls(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _HeldFanOut(fake_rest, monkeypatch, interrupt=True)
    fake_rest.reply(TRADES_PATH.format(stockTicker="AAPL"), Reply(envelope([])))
    with pytest.raises(KeyboardInterrupt):
        history(session, TRADES, start="2025-03-03", end="2025-03-03", symbols=["AAPL", "MSFT"])
    assert sum(1 for path, _ in fake_rest.get_queries() if "MSFT" in path) == 1


# --- Time columns, adjustment, and missing data ------------------------------------------------


def test_every_row_of_a_page_shares_the_instant_the_page_arrived(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    start = epoch_ns(datetime(2025, 3, 3, 15, tzinfo=UTC))
    page1 = [{"sip_timestamp": start + n, "price": 1.0} for n in range(2)]
    page2 = [{"sip_timestamp": start + n, "price": 2.0} for n in range(2, 5)]
    target = "/v3/trades/AAPL?cursor=next"
    fake_rest.reply(target, Reply(envelope(page2)))
    fake_rest.reply(
        TRADES_PATH.format(stockTicker="AAPL"),
        Reply(envelope(page1, next_url=f"{fake_rest.url}{target}")),
    )
    q = query(session, TRADES, symbols=["AAPL"])
    assert isinstance(q, HistoryQuery)
    out = rows(
        q.history("2025-03-03", "2025-03-03", source=HistorySource.REST, wait=True), arrival=True
    )
    first = {row["TimestampInsert"] for row in out if row["Price"] == 1.0}
    second = {row["TimestampInsert"] for row in out if row["Price"] == 2.0}
    assert len(first) == len(second) == 1
    assert first.pop() < second.pop()  # the second page arrived later


def test_a_tick_dataset_dates_each_row_in_its_day_zone(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    string_objects: Callable[[Table, str], int],
) -> None:
    # 03:00 UTC on March 4 is still March 3 in New York.
    late = epoch_ns(datetime(2025, 3, 4, 3, tzinfo=UTC))
    snapshot = {"ticker": "I:SPX", "value": 1.0, "last_updated": late}
    other = {"ticker": "I:NDX", "value": 2.0, "last_updated": late}
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope([snapshot, other])))
    q = query(session, INDEX_LEVELS, symbols=["I:NDX", "I:SPX"])
    assert isinstance(q, FetchQuery)
    table = q.fetch(wait=True)
    row = rows(table)[0]
    assert (row["Symbol"], row["Date"], row["Timestamp"]) == ("I:SPX", "2025-03-03", late)
    assert string_objects(table, "Date") == 1  # the two rows share one stored string


def test_a_symbol_with_no_data_returns_no_rows(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    # Massive answers HTTP 404 for a symbol it has nothing for: no rows, one WARNING.
    fake_rest.reply(TRADES_PATH.format(stockTicker="MSFT"), Reply(envelope([])))
    with caplog.at_level(logging.WARNING):
        out = history(
            session, TRADES, start="2025-03-03", end="2025-03-03", symbols=["MSFT", "ZZZZ"]
        )
    assert out == []
    assert caplog.messages == [
        no_data_alarm("stocks.trades", 1, "ZZZZ", why="(HTTP 404 from /v3/trades/{stockTicker})")
    ]


def test_a_404_after_the_first_page_means_the_data_was_cut_short(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    start = epoch_ns(datetime(2025, 3, 3, 15, tzinfo=UTC))
    gone = "/v3/trades/AAPL?cursor=gone"
    fake_rest.reply(gone, Reply(b'{"status":"NOT_FOUND"}', status=404))
    fake_rest.reply(
        TRADES_PATH.format(stockTicker="AAPL"),
        Reply(envelope([{"sip_timestamp": start}], next_url=f"{fake_rest.url}{gone}")),
    )
    with pytest.raises(dm.MassiveError, match="AAPL's data was cut short") as info:
        history(session, TRADES, start="2025-03-03", end="2025-03-03", symbols=["AAPL"])
    assert str(info.value).startswith(
        "stocks.trades: AAPL's data was cut short: Massive answered HTTP 404 for page 2 of "
        "/v3/trades/{stockTicker} after 1 page had arrived; retry the call"
    )
    assert info.value.status == 404
    # Unlike any other 404: a new call starts over with a new cursor.
    assert info.value.is_retryable


def daily_bars(m: dm.MassiveSession, *, adjusted: bool) -> list[dict[str, Any]]:
    return history(
        m,
        BARS,
        start=date(2025, 3, 3),
        end=date(2025, 3, 4),
        symbols=["AAPL"],
        timespan=Timespan.DAY,
        adjusted=adjusted,
    )


def daily_path() -> str:
    midnight = int(datetime(2025, 3, 3, 5, tzinfo=UTC).timestamp()) * 1000
    return f"/v2/aggs/ticker/AAPL/range/1/day/{midnight}/{midnight + 2 * 86_400_000 - 1}"


def test_every_page_of_bars_must_echo_the_requested_adjustment(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    midnight = int(datetime(2025, 3, 3, 5, tzinfo=UTC).timestamp()) * 1000
    path = daily_path()
    second = f"{path}?cursor=two"
    # The first call's first page echoes the basis asked for and its next_url page another:
    # the call fails. The second call's pages both echo the basis it asked for.
    first = [{"t": midnight, "c": 1.0}]
    later = [{"t": midnight + 86_400_000, "c": 2.0}]
    fake_rest.reply(
        path,
        Reply(envelope(first, adjusted=True, next_url=f"{fake_rest.url}{second}")),
        Reply(envelope(first, adjusted=False, next_url=f"{fake_rest.url}{second}")),
    )
    fake_rest.reply(second, Reply(envelope(later, adjusted=False)))
    with pytest.raises(
        dm.MassiveError,
        match=r"^stocks\.bars: Massive answered /v2/aggs/ticker/\{stocksTicker\}/range/.* with "
        r"adjusted=False although adjusted=True was requested, so the call would mix "
        r"split-adjusted and as-traded prices; retry it$",
    ) as mixed:
        daily_bars(session, adjusted=True)
    assert mixed.value.is_retryable  # the next answer may agree with the request
    assert [r["Close"] for r in daily_bars(session, adjusted=False)] == [1.0, 2.0]


def test_a_page_without_its_adjusted_echo_warns_once_per_path(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    midnight = int(datetime(2025, 3, 3, 5, tzinfo=UTC).timestamp()) * 1000
    fake_rest.reply(daily_path(), Reply(envelope([{"t": midnight, "c": 1.0}])))
    with caplog.at_level(logging.WARNING, LOGGER):
        for _ in range(2):
            assert [r["Close"] for r in daily_bars(session, adjusted=True)] == [1.0]
    assert caplog.text.count("answered without its adjusted echo") == 1
    assert "(adjusted=True requested) cannot be checked" in caplog.text


def test_only_bars_with_an_explicit_adjustment_are_checked(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    # An indicator echoes nothing, and forex bars send no `adjusted`: neither is checked.
    fake_rest.reply(
        "/v1/indicators/sma/AAPL",
        Reply(envelope({"values": [{"timestamp": 1741010400000, "value": 1.0}]}, adjusted=False)),
    )
    midnight = int(datetime(2025, 3, 3, tzinfo=UTC).timestamp()) * 1000
    fake_rest.reply(
        f"/v2/aggs/ticker/C%3AEURUSD/range/1/day/{midnight}/{midnight + 86_400_000 - 1}",
        Reply(envelope([{"t": midnight, "c": 1.0}], adjusted=True)),
    )
    with caplog.at_level(logging.WARNING, LOGGER):
        assert len(fetch(session, SMA, symbols=["AAPL"], adjusted=True)) == 1
        forex = history(
            session,
            FOREX_BARS,
            start="2025-03-03",
            end="2025-03-03",
            symbols=["C:EURUSD"],
            timespan=Timespan.DAY,
        )
    assert [r["Close"] for r in forex] == [1.0]
    assert "adjusted echo" not in caplog.text


def test_an_indicator_takes_the_day_zone_of_its_symbols_class(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # 02:00 UTC on March 4 is March 3 in New York and March 4 in UTC.
    stamp = int(datetime(2025, 3, 4, 2, tzinfo=UTC).timestamp()) * 1000
    value = envelope({"values": [{"timestamp": stamp, "value": 1.0}]})
    fake_rest.reply("/v1/indicators/sma/AAPL", Reply(value))
    fake_rest.reply("/v1/indicators/sma/X%3ABTCUSD", Reply(value))
    out = fetch(session, SMA, symbols=["AAPL", "X:BTCUSD"], timespan=Timespan.HOUR)
    assert sorted(
        (r["Symbol"], r["Date"], r["TimestampEnd"] - r["TimestampStart"]) for r in out
    ) == [
        ("AAPL", "2025-03-03", 3600 * NS),
        ("X:BTCUSD", "2025-03-04", 3600 * NS),
    ]


def test_futures_bars_of_two_sessions_named_by_dates_keep_the_buckets_inside_the_range(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # Monday 03-25 to Friday 03-29, 2024. Massive's 2-session bars are buckets of two UTC
    # days from 00:00 UTC, each labelled with its last session. Good Friday folded into the
    # session of Monday 04-01, which opened on Thursday 03-28, so the bucket of 03-27 and
    # 03-28 is labelled 04-01, after the range, and is left out.
    def bar(day: int, label: str) -> dict[str, object]:
        start = datetime(2024, 3, day, tzinfo=UTC)
        return {"ticker": "ESM4", "session_end_date": label, "window_start": epoch_ns(start)}

    bars = [bar(23, "2024-03-25"), bar(25, "2024-03-27"), bar(27, "2024-04-01")]
    fake_rest.reply("/futures/v1/aggs/ESM4", Reply(envelope(bars)))
    out = history(
        session,
        FUTURES_BARS,
        start="2024-03-25",
        end="2024-03-29",
        symbols=["ESM4"],
        timespan=Timespan.SESSION,
        multiplier=2,
    )
    query = fake_rest.get_queries()[0][1]
    assert query["resolution"] == ["2session"]
    # Requested from 00:00 UTC on the Saturday before the first Monday.
    assert query["window_start.gte"] == [str(epoch_ns(datetime(2024, 3, 23, tzinfo=UTC)))]
    # A bar of two sessions has no `Date`; it ends at 17:00 Chicago on its label.
    assert [(r["Date"], r["TimestampEnd"]) for r in out] == [
        (None, epoch_ns(datetime(2024, 3, 25, 17, tzinfo=CHICAGO))),
        (None, epoch_ns(datetime(2024, 3, 27, 17, tzinfo=CHICAGO))),
    ]


def test_a_futures_session_bar_ends_when_its_session_is_over(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    opens = datetime(2024, 3, 27, 17, tzinfo=CHICAGO)
    row = {"ticker": "ESM4", "session_end_date": "2024-03-28", "window_start": epoch_ns(opens)}
    fake_rest.reply("/futures/v1/aggs/ESM4", Reply(envelope([row])))
    (bar,) = history(
        session,
        FUTURES_BARS,
        start="2024-03-28",
        end="2024-03-28",
        symbols=["ESM4"],
        timespan=Timespan.SESSION,
    )
    assert (bar["Date"], bar["TimestampStart"], bar["TimestampEnd"]) == (
        "2024-03-28",
        epoch_ns(opens),
        epoch_ns(datetime(2024, 3, 28, 17, tzinfo=CHICAGO)),
    )


@pytest.mark.parametrize(
    "on_error",
    [None, dm.ErrorPolicy(unconvertible_values=dm.OnError.RAISE)],
    ids=["default", "raise"],
)
def test_a_bar_outside_the_whole_bar_window_is_neither_withheld_nor_counted(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    on_error: dm.ErrorPolicy | None,
) -> None:
    # At noon on 03-28 the two-session bucket of 03-27 and 03-28 is still open, so it is not
    # the read's, though its label lies in the range. Its close can't be read, yet the read
    # gives no rows, reports nothing, and never fails.
    noon = datetime(2024, 3, 28, 12, tzinfo=CHICAGO)
    monkeypatch.setattr(history_source, "_now", lambda: noon)
    events: list[ProblemEvent] = []
    monkeypatch.setattr(session._core, "problems", events.extend)
    row = {
        "ticker": "ESM4",
        "session_end_date": "2024-03-28",
        "window_start": epoch_ns(datetime(2024, 3, 27, tzinfo=UTC)),
        "close": "x",
    }
    fake_rest.reply("/futures/v1/aggs/ESM4", Reply(envelope([row])))
    out = history(
        session,
        FUTURES_BARS,
        start="2024-03-25",
        end=None,
        symbols=["ESM4"],
        timespan=Timespan.SESSION,
        multiplier=2,
        on_error=on_error,
    )
    assert fake_rest.get_queries()
    assert out == []
    assert events == []


def test_fetch_rows_reads_every_page_raw(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    target = "/futures/v1/schedules?cursor=two"
    fake_rest.reply(target, Reply(envelope([{"product_code": "ES", "extra": 2}])))
    fake_rest.reply(
        "/futures/v1/schedules",
        Reply(envelope([{"product_code": "GC"}], next_url=f"{fake_rest.url}{target}")),
    )
    transport = _endpoints(session)
    query = {"product_code.any_of": ("ES", "GC")}
    out = transport.fetch_rows("/futures/v1/schedules", query, bucket="futures", cancel=Cancel())
    assert out == [{"product_code": "GC"}, {"product_code": "ES", "extra": 2}]
    assert fake_rest.get_queries()[0][1]["product_code.any_of"] == ["ES,GC"]
    assert requests_by_bucket(session) == {"futures": 2}  # both pages, charged as asked
    # A failed request raises its public error.
    fake_rest.reply("/futures/v1/schedules?product_code=ZZ", Reply(b"{}", status=403))
    with pytest.raises(dm.EntitlementError):
        transport.fetch_rows(
            "/futures/v1/schedules", {"product_code": "ZZ"}, bucket="futures", cancel=Cancel()
        )
    # A set token ends the read before it is sent: the session's own lookups stop at once.
    stopped = Cancel()
    stopped.set()
    with pytest.raises(dm.MassiveError, match="cancelled"):
        transport.fetch_rows("/futures/v1/schedules", query, bucket="futures", cancel=stopped)
    assert len(fake_rest.requests) == 3


# --- Background reads ------------------------------------------------------------------------


def _join_reads(m: dm.MassiveSession) -> None:
    # Waits for every background read of the session to end.
    for state in list(m._core.workers._workers.values()):
        assert isinstance(state.fail, MethodType)  # a read's own `fail`
        read = state.fail.__self__
        assert isinstance(read, BackgroundRead)
        read.join(30.0)


def test_a_background_fetch_checks_its_parameters_with_no_request(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    rest = session._core.rest_transport()
    q = query(session, SMA, symbols=["X:BTCUSD"], adjusted=False)
    assert isinstance(q, FetchQuery)
    with pytest.raises(ValueError, match=r"cryptoTicker\} does not take \['adjusted'\]"):
        q.fetch(wait=False)
    # The check splits no crypto pair, which would read Massive's list.
    pair = query(session, CRYPTO_OPEN_CLOSE, symbols=["X:DOTUSD"], params={"date": "2025-03-03"})
    rest.check_fetch(CRYPTO_OPEN_CLOSE, pair._request)
    assert fake_rest.requests == []
    assert session._core.workers._workers == {}


def test_a_pair_massive_does_not_list_fails_a_fetch_under_raise_never_at_the_call(
    *, fake_rest: FakeRestServer
) -> None:
    # As a 404 under RAISE does: the read fails, and a call that waits raises; nothing is
    # decided at the call, and only the crypto ticker list is read.
    serve_pairs(fake_rest)
    params = {"date": "2025-03-03"}
    config = dm.MassiveConfig(
        api_key="test-key",
        rest_base_url=fake_rest.url,
        on_error=dm.ErrorPolicy(unavailable_symbols=dm.OnError.RAISE),
    )
    with dm.MassiveSession(config) as session:
        pair = query(session, CRYPTO_OPEN_CLOSE, symbols=["X:DOTUSD"], params=params)
        assert isinstance(pair, FetchQuery)
        background = pair.fetch()
        assert background.is_refreshing
        session.release(background)
        with pytest.raises(UnavailableSymbolError, match=r"no data for 1 \[X:DOTUSD \(Massive"):
            pair.fetch(wait=True)
        assert sent_data(fake_rest) == []


def test_a_fetch_of_a_pair_massive_does_not_list_gives_no_rows(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # Massive has no data for a pair its crypto ticker list does not hold: under the default
    # LOG it gives no rows, and only the crypto ticker list is read.
    serve_pairs(fake_rest)
    params = {"date": "2025-03-03"}
    pair = query(session, CRYPTO_OPEN_CLOSE, symbols=["X:DOTUSD"], params=params)
    assert isinstance(pair, FetchQuery)
    assert pair.fetch(wait=True).size == 0
    assert sent_data(fake_rest) == []


def test_a_background_fetch_lands_the_rows_the_read_requested(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, rows_when: RowsWhen
) -> None:
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([AAPL_DIVIDEND])))
    q = query(session, DIVIDENDS, symbols=["AAPL"])
    assert isinstance(q, FetchQuery)
    table = q.fetch(wait=False)
    _join_reads(session)
    # The read's own row says how it ended; a failure shows its error here. It shows DONE
    # only after the update cycle that added the rows, so the table holds them from then on.
    (row,) = rows_when(session.streams(), lambda rows: bool(rows) and rows[0]["State"] != "LOADING")
    assert (row["State"], row["Error"], row["RowsPublished"]) == ("DONE", None, 1)
    assert [(r["Symbol"], r["CashAmount"]) for r in rows(table)] == [("AAPL", 0.25)]
    session.release(table)


def test_a_background_history_checks_its_plan_with_no_request(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # Grouped daily bars of every symbol, a symbol's bars, and a crypto pair, which the check
    # does not split: none of them makes a request.
    rest = session._core.rest_transport()
    first, last = date(2025, 3, 3), date(2025, 3, 4)
    for spec, tickers in ((BARS, dm.ALL), (BARS, ["AAPL"]), (CRYPTO_BARS, ["X:BTCUSD"])):
        q = query(session, spec, symbols=tickers, timespan=Timespan.DAY)
        request = dataclasses.replace(q._request, start=first, end=last)
        window = history_window(spec, request, start=first, end=last, classes=())
        rest.check_history(spec, request, window)
    assert fake_rest.requests == []


def test_a_set_stop_token_ends_a_read_before_its_first_request(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    rest = session._core.rest_transport()
    stop = Cancel()
    stop.set()
    q = query(session, DIVIDENDS, symbols=["AAPL"])
    with pytest.raises(dm.MassiveError):
        rest.fetch(DIVIDENDS, q._request, call=1, stop=stop)
    assert fake_rest.requests == []


# --- Delayed data on a real-time subscription -------------------------------------------------


def _delayed_session(fake_rest: FakeRestServer, *listed: Subscription) -> dm.MassiveSession:
    return dm.MassiveSession(
        dm.MassiveConfig(
            api_key="test-key", rest_base_url=fake_rest.url, delayed_subscriptions=set(listed)
        )
    )


def test_a_delayed_row_fails_a_real_time_fetch(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(
        INDEX_SNAPSHOT_PATH, Reply(envelope([{"ticker": "I:SPX", "timeframe": "DELAYED"}]))
    )
    with pytest.raises(dm.EntitlementError) as info:
        fetch(session, INDEX_SNAPSHOTS, symbols=["I:SPX"])
    assert str(info.value) == (
        "indices.snapshots data for I:SPX would be delayed (Massive reports timeframe=DELAYED), "
        "and "
        "dm.Subscription.INDICES is not in MassiveConfig.delayed_subscriptions, so the library "
        "serves only real-time indices data. To accept delayed indices data (its streams then "
        "use Massive's delayed feed too): "
        "dm.MassiveConfig(delayed_subscriptions={dm.Subscription.INDICES})"
    )


def test_a_delayed_envelope_fails_a_real_time_fetch(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    gainers = "/v2/snapshot/locale/us/markets/stocks/gainers"
    fake_rest.reply(gainers, Reply(envelope(status="DELAYED", tickers=[{"ticker": "NVDA"}])))
    with pytest.raises(dm.EntitlementError, match=r"Massive reports status=DELAYED"):
        fetch(session, MOVERS, params={"direction": Direction.GAINERS})


def test_a_listed_subscription_serves_delayed_rest_data_and_says_so(
    *, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    m = _delayed_session(fake_rest, Subscription.INDICES)
    try:
        fake_rest.reply(
            INDEX_SNAPSHOT_PATH,
            Reply(envelope([{"ticker": "I:SPX", "timeframe": "DELAYED"}], status="DELAYED")),
        )
        q = query(m, INDEX_SNAPSHOTS, symbols=["I:SPX"])
        assert isinstance(q, FetchQuery)
        table = q.fetch(wait=True)
        assert [r["Symbol"] for r in rows(table)] == ["I:SPX"]
        description = table.attributes()["TableDescription"]
        assert description.endswith(
            "Data as the plan serves it, real-time or delayed: MassiveConfig.delayed_subscriptions "
            "lists its subscription, so Massive's DELAYED labels are not checked."
        )
    finally:
        m.close()


def test_an_unlisted_fetch_says_it_was_checked_like_its_poll(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(
        INDEX_SNAPSHOT_PATH, Reply(envelope([{"ticker": "I:SPX", "timeframe": "REAL-TIME"}]))
    )
    q = query(session, INDEX_SNAPSHOTS, symbols=["I:SPX"])
    assert isinstance(q, FetchQuery)
    table = q.fetch(wait=True)
    assert table.attributes()["TableDescription"].endswith(
        "Checked for delayed data: a response Massive labels DELAYED fails the table unless "
        "MassiveConfig.delayed_subscriptions lists its subscription."
    )


_SMA_VALUE = {"timestamp": 1741010400000, "value": 1.0}
"""One indicator value, of 2025-03-03."""


@pytest.mark.parametrize(
    ("bounds", "checked"),
    [
        ({}, True),  # the latest values
        ({"start": "2025-03-03"}, True),  # through now
        ({"end": date(2999, 1, 1)}, True),
        ({"end": datetime(2999, 1, 1, tzinfo=UTC)}, True),
        ({"start": "2025-03-03", "end": "2025-03-04"}, False),  # a past range: final values
        ({"end": datetime(2025, 3, 4, tzinfo=UTC)}, False),
    ],
)
def test_an_indicator_fetch_is_checked_when_its_window_reaches_now(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, bounds: dict[str, Any], checked: bool
) -> None:
    fake_rest.reply(
        "/v1/indicators/sma/AAPL", Reply(envelope({"values": [_SMA_VALUE]}, status="DELAYED"))
    )
    if checked:
        with pytest.raises(dm.EntitlementError, match=r"Massive reports status=DELAYED"):
            fetch(session, SMA, symbols=["AAPL"], **bounds)
    else:
        assert [r["Symbol"] for r in fetch(session, SMA, symbols=["AAPL"], **bounds)] == ["AAPL"]


def test_a_crypto_indicator_reaching_now_is_never_refused(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # Massive has no delayed currencies feed, so its label names no remedy.
    fake_rest.reply(
        "/v1/indicators/sma/X%3ABTCUSD", Reply(envelope({"values": [_SMA_VALUE]}, status="DELAYED"))
    )
    assert [r["Symbol"] for r in fetch(session, SMA, symbols=["X:BTCUSD"])] == ["X:BTCUSD"]


def test_a_delayed_label_fails_an_indicator_poll(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, rows_when: RowsWhen
) -> None:
    fake_rest.reply(
        "/v1/indicators/sma/AAPL", Reply(envelope({"values": [_SMA_VALUE]}, status="DELAYED"))
    )
    q = query(session, SMA, symbols=["AAPL"], start="2025-03-03", end="2025-03-04")
    assert isinstance(q, FetchQuery)
    table = q.poll(60)  # polled, so current whatever its window
    (row,) = rows_when(session.streams(), lambda rows: bool(rows) and rows[0]["Error"])
    assert row["State"] == "FAILED"
    assert "dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS})" in row["Error"]
    session.release(table)


def test_a_listed_subscriptions_indicator_is_served_delayed(
    *, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    m = _delayed_session(fake_rest, Subscription.STOCKS)
    try:
        fake_rest.reply(
            "/v1/indicators/sma/AAPL", Reply(envelope({"values": [_SMA_VALUE]}, status="DELAYED"))
        )
        assert [r["Symbol"] for r in fetch(m, SMA, symbols=["AAPL"])] == ["AAPL"]
    finally:
        m.close()


def test_reference_data_is_never_checked(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    contract = {"ticker": "O:SPY251219C00600000"}
    fake_rest.reply(CONTRACTS_PATH, Reply(envelope([contract], status="DELAYED")))
    assert [r["Symbol"] for r in fetch(session, CONTRACTS)] == ["O:SPY251219C00600000"]


def test_history_is_never_checked(*, session: dm.MassiveSession, fake_rest: FakeRestServer) -> None:
    # History is past data: a DELAYED label says nothing about it, and nothing is refused.
    start = epoch_ns(datetime(2025, 3, 3, 5, tzinfo=UTC))
    trade = {"sip_timestamp": start + 1, "price": 1.0, "size": 10, "timeframe": "DELAYED"}
    fake_rest.reply(
        TRADES_PATH.format(stockTicker="AAPL"), Reply(envelope([trade], status="DELAYED"))
    )
    out = history(session, TRADES, start="2025-03-03", end="2025-03-03", symbols=["AAPL"])
    assert [r["Symbol"] for r in out] == ["AAPL"]


def test_past_data_is_never_checked(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # Massive labels past data DELAYED on a plan whose current data is delayed, though its rows
    # are final: only a dataset of current data is checked, so the dividends are served.
    labelled = {**AAPL_DIVIDEND, "timeframe": "DELAYED"}
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([labelled], status="DELAYED")))
    assert [r["Symbol"] for r in fetch(session, DIVIDENDS, symbols=["AAPL"])] == ["AAPL"]


def test_a_delayed_refusal_names_five_refused_tickers_and_counts_the_rest(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    names = ["I:SPX", "I:NDX", "I:DJI", "I:VIX", "I:RUT", "I:OEX", "I:XAU"]
    delayed = [{"ticker": t, "last_quote": {"timeframe": "DELAYED"}} for t in names]
    real_time = {"ticker": "I:COMP", "timeframe": "REAL-TIME"}
    # A repeated ticker is named once, and a row with no ticker is refused but names none.
    unnamed = {"timeframe": "DELAYED"}
    rows = [real_time, *delayed, delayed[0], unnamed]
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope(rows)))
    with pytest.raises(dm.EntitlementError) as info:
        fetch(session, INDEX_SNAPSHOTS, symbols=[*names, "I:COMP"])
    assert str(info.value).startswith(
        "indices.snapshots data for I:SPX, I:NDX, I:DJI, I:VIX, I:RUT and 2 more would be delayed "
        "(Massive reports last_quote.timeframe=DELAYED), and dm.Subscription.INDICES is not in "
        "MassiveConfig.delayed_subscriptions"
    )


def test_a_delayed_envelope_names_the_requested_tickers(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope([{"ticker": "I:SPX"}], status="DELAYED")))
    with pytest.raises(dm.EntitlementError) as info:
        fetch(session, INDEX_SNAPSHOTS, symbols=["I:SPX"])
    assert str(info.value).startswith(
        "indices.snapshots data for I:SPX would be delayed (Massive reports status=DELAYED)"
    )


def test_a_delayed_label_fails_a_real_time_poll_table(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, rows_when: RowsWhen
) -> None:
    fake_rest.reply(
        INDEX_SNAPSHOT_PATH, Reply(envelope([{"ticker": "I:SPX", "timeframe": "DELAYED"}]))
    )
    q = query(session, INDEX_SNAPSHOTS, symbols=["I:SPX"])
    assert isinstance(q, FetchQuery)
    table = q.poll(60)
    assert table.attributes()["TableDescription"].endswith(
        "Checked for delayed data: a response Massive labels DELAYED fails the table unless "
        "MassiveConfig.delayed_subscriptions lists its subscription."
    )
    (row,) = rows_when(session.streams(), lambda rows: bool(rows) and rows[0]["Error"])
    assert (row["State"], row["Recency"]) == ("FAILED", "UNKNOWN")  # never real-time unproven
    assert "dm.MassiveConfig(delayed_subscriptions={dm.Subscription.INDICES})" in row["Error"]
    session.release(table)


@pytest.mark.parametrize(
    ("listed", "row", "shown"),
    [
        ((), {"ticker": "I:SPX"}, "UNKNOWN"),  # no label: nothing proves real-time
        ((), {"ticker": "I:SPX", "timeframe": "REAL-TIME"}, "REALTIME"),
        ((Subscription.INDICES,), {"ticker": "I:SPX", "timeframe": "DELAYED"}, "DELAYED"),
        ((Subscription.INDICES,), {"ticker": "I:SPX"}, "UNKNOWN"),
    ],
)
def test_a_poll_tables_recency_follows_the_labels_it_was_sent(
    *,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    rows_when: RowsWhen,
    listed: tuple[Subscription, ...],
    row: dict[str, object],
    shown: str,
) -> None:
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    m = _delayed_session(fake_rest, *listed)
    try:
        fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope([row])))
        q = query(m, INDEX_SNAPSHOTS, symbols=["I:SPX"])
        assert isinstance(q, FetchQuery)
        table = q.poll(60)
        (seen,) = rows_when(m.streams(), lambda rows: bool(rows) and rows[0]["RowsPublished"])
        assert seen["Recency"] == shown
        m.release(table)
    finally:
        m.close()


@pytest.mark.parametrize(("spec", "recency"), [(CONTRACTS, None), (CONSTITUENTS, None)])
def test_a_poll_of_data_no_label_describes_shows_no_recency(
    *,
    session: dm.MassiveSession,
    rows_when: RowsWhen,
    spec: DatasetSpec,
    recency: str | None,
) -> None:
    q = query(session, spec)
    assert isinstance(q, FetchQuery)
    table = q.poll(60)
    assert "Checked for delayed data" not in table.attributes()["TableDescription"]
    (row,) = rows_when(session.streams(), bool)
    assert row["Recency"] == recency
    session.release(table)


# --- The unified snapshot: each row is its own class's data -----------------------------------


def _unified(m: dm.MassiveSession, *tickers: str) -> list[dict[str, Any]]:
    return fetch(m, UNIFIED, symbols=list(tickers))


def test_a_delayed_stock_row_fails_a_unified_snapshot(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(
        UNIFIED_PATH,
        Reply(envelope([{"ticker": "AAPL", "last_trade": {"timeframe": "DELAYED"}}])),
    )
    with pytest.raises(dm.EntitlementError) as info:
        _unified(session, "AAPL")
    assert str(info.value).startswith(
        "snapshots data for AAPL would be delayed (Massive reports last_trade.timeframe=DELAYED), "
        "and dm.Subscription.STOCKS is not in MassiveConfig.delayed_subscriptions"
    )


def test_each_unified_row_is_held_to_its_own_subscription(
    *, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    m = _delayed_session(fake_rest, Subscription.STOCKS)
    contract = "O:SPY251219C00600000"
    try:
        stock = {"ticker": "AAPL", "last_trade": {"timeframe": "DELAYED"}}
        option = {"ticker": contract, "last_trade": {"timeframe": "REAL-TIME"}}
        fake_rest.reply(UNIFIED_PATH, Reply(envelope([stock, option])))
        # Stocks are listed: its delayed row passes beside a real-time option.
        assert [r["Symbol"] for r in _unified(m, "AAPL", contract)] == ["AAPL", contract]
        delayed_option = {"ticker": contract, "last_trade": {"timeframe": "DELAYED"}}
        fake_rest.reply_first(UNIFIED_PATH, Reply(envelope([stock, delayed_option])))
        with pytest.raises(dm.EntitlementError, match=r"dm\.Subscription\.OPTIONS is not in"):
            _unified(m, "AAPL", contract)
    finally:
        m.close()


@pytest.mark.parametrize(
    ("listed", "refused"),
    [
        ((Subscription.STOCKS,), "OPTIONS"),
        ((Subscription.STOCKS, Subscription.OPTIONS), None),
    ],
)
def test_a_delayed_envelope_needs_every_class_of_the_request_listed(
    *,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    listed: tuple[Subscription, ...],
    refused: str | None,
) -> None:
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    m = _delayed_session(fake_rest, *listed)
    contract = "O:SPY251219C00600000"
    try:
        fake_rest.reply(UNIFIED_PATH, Reply(envelope([{"ticker": "AAPL"}], status="DELAYED")))
        if refused is None:
            assert [r["Symbol"] for r in _unified(m, "AAPL", contract)] == ["AAPL"]
        else:
            with pytest.raises(dm.EntitlementError, match=rf"dm\.Subscription\.{refused} is"):
                _unified(m, "AAPL", contract)
    finally:
        m.close()


def test_a_delay_label_is_never_held_against_currencies(
    *, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Massive has no delayed currencies feed and the configuration cannot list one, so a
    # refusal could only name a remedy the configuration refuses.
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    m = _delayed_session(fake_rest, Subscription.STOCKS)
    try:
        rows = [{"ticker": "AAPL"}, {"ticker": "X:BTCUSD"}]
        fake_rest.reply(UNIFIED_PATH, Reply(envelope(rows, status="DELAYED")))
        assert [r["Symbol"] for r in _unified(m, "AAPL", "X:BTCUSD")] == ["AAPL", "X:BTCUSD"]
        crypto = {"ticker": "X:BTCUSD", "last_trade": {"timeframe": "DELAYED"}}
        fake_rest.reply_first(UNIFIED_PATH, Reply(envelope([crypto])))
        assert [r["Symbol"] for r in _unified(m, "X:BTCUSD")] == ["X:BTCUSD"]
    finally:
        m.close()


def test_a_unified_row_without_a_ticker_or_a_request_without_one_refuses_nothing(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # Neither names a subscription a label could be held against.
    fake_rest.reply(
        UNIFIED_PATH,
        Reply(
            envelope(
                [{"ticker": "AAPL"}, {"last_trade": {"timeframe": "DELAYED"}}], status="DELAYED"
            )
        ),
    )
    q = query(session, UNIFIED)
    assert isinstance(q, FetchQuery)
    assert q.fetch(wait=True).size == 1  # the row without a ticker is dropped as unreadable


# --- Bounded and explaining log records ---------------------------------------------------------


def test_the_missing_symbols_of_a_read_warn_once_and_each_is_a_problem(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_rest.reply(TRADES_PATH.format(stockTicker="MSFT"), Reply(envelope([])))
    events = record_problems(session, monkeypatch)
    with caplog.at_level(logging.DEBUG, LOGGER):
        for _ in range(2):  # each read is its own table: each warns of its first
            history(
                session,
                TRADES,
                start="2025-03-03",
                end="2025-03-03",
                symbols=["MSFT", "YYYY", "ZZZZ"],
            )
    why = "(HTTP 404 from /v3/trades/{stockTicker})"
    assert alarms(caplog) == [
        no_data_alarm("stocks.trades", read, "YYYY", why=why) for read in (1, 2)
    ]
    # Each symbol of each read is a problem, logged at DEBUG, and the read names them
    # together at DEBUG.
    assert [(e.cause, e.stream_id, e.symbol, e.detail) for e in events] == [
        (ProblemCause.UNAVAILABLE_SYMBOLS, read, symbol, f"Massive has no data for it {why}")
        for read in (1, 2)
        for symbol in ("YYYY", "ZZZZ")
    ]
    debug = [r.getMessage() for r in caplog.records if r.name == LOGGER]
    each = [m for m in debug if " has no data (" in m]
    assert len(each) == 4  # each symbol of each read
    assert all(m.startswith("stocks.trades: ") and "HTTP 404 from" in m for m in each)
    together = [m for m in debug if "Massive has no data for" in m]
    left_out = f"stocks.trades: Massive has no data for 2 [YYYY {why}, ZZZZ {why}], so they are"
    assert together == [f"{left_out} left out"] * 2


def test_unavailable_symbols_decides_what_a_read_does_with_a_symbol_without_data(
    *, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    fake_rest.reply(TRADES_PATH.format(stockTicker="MSFT"), Reply(envelope([])))
    day = date(2025, 3, 3)
    no_data = (
        r"^stocks\.trades: Massive has no data for 1 \[ZZZZ \(HTTP 404 from "
        r"/v3/trades/\{stockTicker\}\)\]\. Remove the symbol; to leave it out and read the "
        r"others, pass on_error=dm\.ErrorPolicy\(unavailable_symbols=dm\.OnError\.LOG\)"
    )

    def session_with(setting: dm.OnError) -> dm.MassiveSession:
        return dm.MassiveSession(
            dm.MassiveConfig(
                api_key="test-key",
                rest_base_url=fake_rest.url,
                on_error=dm.ErrorPolicy(unavailable_symbols=setting),
            )
        )

    # IGNORE leaves the symbol out, as LOG does, and names it only at DEBUG.
    with session_with(dm.OnError.IGNORE) as m, caplog.at_level(logging.DEBUG, LOGGER):
        history(m, TRADES, start=day, end=day, symbols=["MSFT", "ZZZZ"])
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("no data for 1 [ZZZZ (" in r.getMessage() for r in caplog.records)
    with session_with(dm.OnError.RAISE) as m:
        # A static read fails, naming the symbol and the setting that would leave it out.
        with pytest.raises(UnavailableSymbolError, match=no_data):
            history(m, TRADES, start=day, end=day, symbols=["MSFT", "ZZZZ"])
        # A read by symbol fails that symbol alone; the others keep their rows.
        asked = Request(start=day, end=day, on_error=m._core.policy)
        window = history_window(TRADES, asked, start=day, end=day, classes=())
        transport = m._core.rest_transport()
        out = transport.history_by_symbol(
            TRADES, asked, window, symbols=["MSFT", "ZZZZ"], call=1, tally=ReadTally()
        )
    assert isinstance(out["MSFT"], SymbolRows)
    failed = out["ZZZZ"]
    assert isinstance(failed, SymbolFailure)
    assert isinstance(failed.error, UnavailableSymbolError)
    assert re.match(no_data, str(failed.error))


@pytest.mark.parametrize("setting", [dm.OnError.RAISE, dm.OnError.LOG])
def test_each_rest_verb_follows_the_call_s_unavailable_symbols_over_the_session_s(
    setting: dm.OnError, *, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The session and the call disagree: the call's setting decides whether a symbol without
    # data fails history() and fetch(), and, under RAISE, poll().
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    other = dm.OnError.LOG if setting is dm.OnError.RAISE else dm.OnError.RAISE
    config = dm.MassiveConfig(
        api_key="test-key",
        rest_base_url=fake_rest.url,
        on_error=dm.ErrorPolicy(unavailable_symbols=other),
    )
    call = dm.ErrorPolicy(unavailable_symbols=setting)
    fake_rest.reply(TRADES_PATH.format(stockTicker="MSFT"), Reply(envelope([])))
    serve_pairs(fake_rest)
    day = date(2025, 3, 3)
    with dm.MassiveSession(config) as m:
        pair = query(
            m, CRYPTO_OPEN_CLOSE, symbols=["X:DOTUSD"], params={"date": day}, on_error=call
        )
        assert isinstance(pair, FetchQuery)
        if setting is dm.OnError.LOG:
            history(m, TRADES, start=day, end=day, symbols=["MSFT", "ZZZZ"], on_error=call)
            assert pair.fetch(wait=True).size == 0
            return
        with pytest.raises(UnavailableSymbolError, match=r"no data for 1 \[ZZZZ \("):
            history(m, TRADES, start=day, end=day, symbols=["MSFT", "ZZZZ"], on_error=call)
        with pytest.raises(UnavailableSymbolError, match=r"no data for 1 \[X:DOTUSD \("):
            pair.fetch(wait=True)
        assert _failed_soon(pair.poll(60))


def test_a_rest_fetch_follows_the_call_s_unreadable_symbols_over_the_session_s(
    *, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A call under RAISE fails on the row's cause; under the session's LOG the row is withheld,
    # and a page whose only row is withheld fails as one the library could read none of.
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    body = {"status": "success", "request_id": "r1", "from": "AUD", "to": "XXX"}
    fake_rest.reply("/v1/conversion/AUD/XXX", Reply({**body, "symbol": "not a pair"}))
    config = dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url)
    strict = dm.ErrorPolicy(unreadable_symbols=dm.OnError.RAISE)
    params = {"from": "AUD", "to": "XXX"}
    with dm.MassiveSession(config) as m:
        with pytest.raises(dm.MassiveError, match=r"\(1 under unreadable_symbols\)"):
            fetch(m, CONVERSION, params=params, on_error=strict)
        with pytest.raises(dm.MassiveError, match=r"could read none of the 1 rows"):
            fetch(m, CONVERSION, params=params)


def test_the_error_rows_of_a_page_warn_in_one_bounded_line_and_each_at_debug(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    tickers = [f"I:X{n:02d}" for n in range(25)]
    refused = [{"ticker": t, "error": "NOT_ENTITLED", "message": "Not entitled"} for t in tickers]
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope(refused)))
    with caplog.at_level(logging.DEBUG, LOGGER):
        fetch(session, INDEX_SNAPSHOTS, symbols=["I:SPX"])
    (warning,) = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert warning.startswith(
        f"indices.snapshots: Massive answered error rows from {INDEX_SNAPSHOT_PATH} for 25 "
        "[I:X00 (NOT_ENTITLED: Not entitled), "
    )
    assert warning.endswith(
        "I:X19 (NOT_ENTITLED: Not entitled), +5 more]; the rows are kept with their Error and "
        "Message columns"
    )
    debug = [r.getMessage() for r in caplog.records if "an error row" in r.getMessage()]
    assert debug[-1] == (
        f"indices.snapshots: Massive answered an error row from {INDEX_SNAPSHOT_PATH} for "
        "I:X24 (NOT_ENTITLED: Not entitled)"
    )
    assert len(debug) == 25


def test_an_error_row_answer_is_cut_in_the_warning_and_whole_at_debug(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    message = "x" * 500
    refused = {"ticker": "I:VIX", "error": "NOT_ENTITLED", "message": message}
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope([refused])))
    with caplog.at_level(logging.DEBUG, LOGGER):
        fetch(session, INDEX_SNAPSHOTS, symbols=["I:VIX"])
    (warning,) = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    shown = "I:VIX (NOT_ENTITLED: " + message
    assert f"for 1 [{shown[: rest._ANSWER_SHOWN - 1]}…]; the rows are kept" in warning
    assert any(f"for {shown})" in r.getMessage() for r in caplog.records)


def test_the_facts_a_session_warned_about_are_bounded(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(rest, "_FACTS_LIMIT", 2)
    governor = _massive.RequestGovernor(("stocks",), clock=FakeClock())
    client = _massive.RestClient(  # sends no request
        "k", base_url="http://127.0.0.1:9", max_concurrency=1, governor=governor
    )
    state = rest.RestState(client, "MassiveSession #9")
    with caplog.at_level(logging.DEBUG, LOGGER):
        assert [state.first("x", "a"), state.first("x", "b"), state.first("x", "a")] == [
            True,
            True,
            False,
        ]
        # A third fact empties the memory first, so the first counts as new again.
        assert [state.first("x", "c"), state.first("x", "a")] == [True, True]
    assert caplog.messages == [
        "MassiveSession #9: forgot the 2 facts already warned about, past the limit of 2"
    ]
    state.close()


def test_each_page_logs_its_rows_after_each_step(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    # Four weekly bars: the window keeps the two whole weeks inside it.
    start, end = ny_ms(date(2025, 3, 9)), ny_ms(date(2025, 3, 22)) - 1
    path = f"/v2/aggs/ticker/AAPL/range/1/week/{start}/{end}"
    stamps = [ny_ms(date(2025, 3, d)) for d in (2, 9, 16, 23)]
    fake_rest.reply(path, Reply(envelope([{"t": t, "c": 1.0} for t in stamps])))
    with caplog.at_level(logging.DEBUG, LOGGER):
        history(
            session,
            BARS,
            start="2025-03-05",
            end="2025-03-21",
            symbols=["AAPL"],
            timespan=Timespan.WEEK,
            adjusted=False,
        )
    pages = [r.getMessage() for r in caplog.records if "rows read" in r.getMessage()]
    assert pages == [
        (
            "stocks.bars: page 1 of /v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/"
            "{from}/{to} for AAPL: 4 rows read, 4 converted, 2 in the window, 0 withheld, 2 kept "
            "as whole bars"
        )
    ]


def test_an_unknown_recency_label_warns_once_per_subscription_and_value(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    rows = [
        {"ticker": "I:SPX", "value": 1.0, "timeframe": "SOMETIMES"},
        {"ticker": "I:NDX", "value": 2.0, "timeframe": "SOMETIMES"},
    ]
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope(rows)))
    with caplog.at_level(logging.WARNING, LOGGER):
        for _ in range(2):
            out = fetch(session, INDEX_SNAPSHOTS, symbols=["I:SPX", "I:NDX"])
            assert [r["Symbol"] for r in out] == ["I:SPX", "I:NDX"]  # served
    assert caplog.messages == [
        (
            f"indices.snapshots: {INDEX_SNAPSHOT_PATH} answered timeframe='SOMETIMES', which is "
            "neither DELAYED nor REAL-TIME, for indices data; the library cannot tell whether "
            "the row is delayed and serves it"
        )
    ]


# --- Rows that list their tickers ------------------------------------------------------------


def _listed(*rows: tuple[int, list[str]]) -> Reply:
    # A page of articles, each with its id and the tickers it lists.
    return Reply(envelope([{"benzinga_id": i, "tickers": t} for i, t in rows]))


def test_a_row_is_listed_under_each_named_ticker_it_lists(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # One request names both tickers; a row listing both is each one's, in its list's order,
    # and a row listing neither is left out, as a row outside the window is: no event, and one
    # WARNING a session.
    page = _listed((1, ["MSFT", "AAPL"]), (2, ["AAPL"]), (3, ["TSLA"]))
    fake_rest.reply(LISTED_PATH, page, page)
    events: list[ProblemEvent] = []
    monkeypatch.setattr(session._core, "problems", events.extend)
    with caplog.at_level(logging.WARNING, LOGGER):
        for _ in range(2):
            out = fetch(session, LISTED, symbols=["AAPL", "MSFT"])
            assert [(r["BenzingaId"], r["Symbol"]) for r in out] == [
                (1, "MSFT"),
                (1, "AAPL"),
                (2, "AAPL"),
            ]
    assert [q["tickers.any_of"] for _, q in fake_rest.get_queries()] == [["AAPL,MSFT"]] * 2
    assert [m for m in caplog.messages if "list none" in m] == [
        (
            "partners.benzinga.news: 1 rows Massive returned from /benzinga/v2/news list none "
            "of the request's tickers (2 [AAPL, MSFT]) in Tickers, so they are left out"
        )
    ]
    assert events == []


def test_one_named_ticker_is_every_row_s_and_none_leaves_symbol_null(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    page = _listed((4, ["TSLA"]))
    fake_rest.reply(LISTED_PATH, page, page)
    assert [r["Symbol"] for r in fetch(session, LISTED, symbols=["AAPL"])] == ["AAPL"]
    assert [r["Symbol"] for r in fetch(session, LISTED)] == [None]


def test_listing_a_row_under_each_ticker_can_pass_the_limit(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # `limit` caps the rows Massive returns; each is then listed under its named tickers.
    fake_rest.reply(LISTED_PATH, _listed((1, ["AAPL", "MSFT"])))
    out = fetch(session, LISTED, symbols=["AAPL", "MSFT"], limit=1)
    assert [r["Symbol"] for r in out] == ["AAPL", "MSFT"]
    assert fake_rest.get_queries()[0][1]["limit"] == ["1"]


# --- poll() cycles of a table of values -----------------------------------------------------------


def _value_cycle(m: dm.MassiveSession, values: tuple[str, ...]) -> poll.CycleRows:
    # One poll() cycle of futures products over a table of product codes, read as it holds them.
    with entered(m._core.exec_ctx):
        table = new_table([string_col("ProductCode", list(values))])
    boxed = ValueTable("product_code", "product_code", "ProductCode", underlying=False, table=table)
    reader = _endpoints(m)._reader("poll", m._core.policy)
    with ThreadPoolExecutor(1) as pool:
        return reader.read_cycle(
            futures_ns.PRODUCTS,
            Request(value_table=boxed),
            symbols=None,
            values=values,
            record=lambda _: None,
            on_recency=lambda _: None,
            executor=pool,
            cancel=Cancel(),
            pace=rate_limits.PaceCheck("futures.products poll()", 60.0),
        )


def test_a_poll_cycle_reads_each_value_and_leaves_out_one_massive_has_no_data_for(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    # One value after another, in the order given: ES, then NQ, which Massive does not know.
    es = Reply(envelope([{"product_code": "ES", "name": "E-mini S&P 500"}]))
    fake_rest.reply("/futures/v1/products", es, Reply({"status": "NOT_FOUND"}, status=404))
    with caplog.at_level(logging.WARNING):
        read = _value_cycle(rest_session, ("ES", "NQ"))
    assert read.rows.column("ProductCode").to_pylist() == ["ES"]
    assert read.requests == 1
    assert read.left_out == ("NQ (HTTP 404 from /futures/v1/products)",)
    assert [query["product_code"] for _, query in fake_rest.get_queries()] == [["ES"], ["NQ"]]
    assert any("NQ" in message and "no data" in message for message in caplog.messages)


def test_a_poll_cycle_raises_a_value_s_failure_other_than_no_data(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    refusal = Reply({"status": "NOT_AUTHORIZED", "message": "upgrade"}, status=403)
    fake_rest.reply("/futures/v1/products", refusal)
    with pytest.raises(dm.EntitlementError):
        _value_cycle(rest_session, ("ES",))


def test_a_poll_cycle_judges_its_values_as_one_read(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # NQ's only row is unreadable: beside ES's, it is withheld; alone, the cycle fails.
    es = Reply(envelope([{"product_code": "ES", "name": "E-mini S&P 500"}]))
    nq = Reply(envelope([{"product_code": "NQ", "last_updated": "no time"}]))
    fake_rest.reply("/futures/v1/products", es, nq, nq, nq)
    read = _value_cycle(rest_session, ("ES", "NQ"))
    assert read.rows.column("ProductCode").to_pylist() == ["ES"]
    with pytest.raises(RowDataError, match="could read none of the 2 rows"):
        _value_cycle(rest_session, ("NQ", "YM"))


def test_a_poll_cycle_of_an_empty_table_of_values_reads_nothing(
    rest_session: dm.MassiveSession,
) -> None:
    read = _value_cycle(rest_session, ())
    assert (read.rows.num_rows, read.requests, read.left_out) == (0, 0, ())


def _not_found(
    m: dm.MassiveSession, error: _massive.WireError, *, symbol: str | None, requests: int
) -> object:
    reader = _endpoints(m)._reader("call-1", m._core.policy)
    return reader._not_found(
        error,
        spec=TRADES,
        call=rest_requests.Call({"stockTicker": symbol}, {}, symbol),
        template=TRADES_PATH,
        bucket="stocks",
        window=None,
        kept=[],
        requests=requests,
    )


def test_a_404_on_a_symbols_first_request_leaves_the_symbol_out(
    session: dm.MassiveSession,
) -> None:
    error = _massive.WireError(
        "not found", status=404, request_id="r1", kind=_massive.WireFailure.REFUSED
    )
    outcome = _not_found(session, error, symbol="ZZZZ", requests=0)
    missing = rest._Missing("ZZZZ", "(HTTP 404 from /v3/trades/{stockTicker})")
    assert outcome == rest._Outcome((), 1, missing=missing)
    assert str(missing) == "ZZZZ (HTTP 404 from /v3/trades/{stockTicker})"


def test_a_404_on_a_symbols_later_page_cuts_its_data_short(session: dm.MassiveSession) -> None:
    error = _massive.WireError(
        "not found", status=404, request_id="r1", kind=_massive.WireFailure.REFUSED
    )
    with pytest.raises(CutShortError, match="after 2 pages had arrived") as info:
        _not_found(session, error, symbol="AAPL", requests=2)
    assert info.value.request_id == "r1"


@pytest.mark.parametrize(
    ("status", "symbol"), [(404, None), (500, "AAPL")], ids=["no-symbol", "not-404"]
)
def test_only_a_per_symbol_404_is_about_the_symbol(
    *, session: dm.MassiveSession, status: int, symbol: str | None
) -> None:
    error = _massive.WireError("failed", status=status, kind=_massive.WireFailure.REFUSED)
    with pytest.raises(dm.MassiveError) as info:
        _not_found(session, error, symbol=symbol, requests=0)
    assert not isinstance(info.value, CutShortError)
    assert info.value.status == status


def _recency(
    rows: Sequence[Mapping[str, object]],
    *,
    fixed: Subscription | None = None,
    listed: frozenset[Subscription] = frozenset(),
) -> rest._PageRecency:
    return rest._page_recency(rows, fixed=fixed, listed=listed, symbol_key="ticker", symbol=None)


def test_page_recency_refuses_each_unlisted_subscriptions_delayed_rows() -> None:
    found = _recency(
        [
            {"ticker": "AAPL", "timeframe": "DELAYED"},
            {"ticker": "MSFT", "last_trade": {"timeframe": "DELAYED"}},
            {"ticker": "AAPL", "timeframe": "DELAYED"},
            {"ticker": "X:BTCUSD", "timeframe": "DELAYED"},
            {"ticker": "I:SPX", "timeframe": "REAL-TIME"},
        ]
    )
    assert found.delayed
    assert found.real_time
    # The first evidence is kept; a currency's delay is shown but never refused.
    assert found.refused == {Subscription.STOCKS: (("AAPL", "MSFT"), "timeframe=DELAYED")}
    assert found.unknown == ()


def test_page_recency_passes_a_listed_subscription_and_collects_unknown_labels() -> None:
    found = _recency(
        [{"ticker": "AAPL", "timeframe": "DELAYED"}, {"timeframe": "SOON"}],
        fixed=Subscription.STOCKS,
        listed=frozenset({Subscription.STOCKS}),
    )
    assert found.delayed
    assert not found.real_time
    assert found.refused == {}
    assert found.unknown == ((Subscription.STOCKS, ("timeframe", "SOON")),)


def test_page_recency_holds_no_row_whose_subscription_is_unknown() -> None:
    found = _recency([{"timeframe": "DELAYED"}, {"ticker": "AAPL"}])
    assert found == rest._PageRecency(delayed=False, real_time=False, refused={}, unknown=())


def test_a_call_with_no_data_is_never_counted_as_a_request(session: dm.MassiveSession) -> None:
    # A pair Massive's list does not hold is never sent, so pacing and announcements skip it.
    path = _endpoints(session)._reader("call-1", session._core.policy).rest_path(CRYPTO_LAST_PATH)
    sent = rest_requests.Call({"from": "BTC", "to": "USD"}, {}, "X:BTCUSD")
    unsent = rest_requests.Call({}, {}, "X:DOTUSD", NOT_LISTED)
    assert len(rest._buckets(CRYPTO_LAST, [(path, [sent, unsent])])) == 1


def test_a_stream_splits_a_pair_through_the_sessions_rest_transport(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # The WebSocket transport's lookup reaches the session's one crypto ticker list.
    serve_pairs(fake_rest)
    ports = session._core._ports
    assert isinstance(ports, SessionClients)
    assert ports._crypto_pair("X:BTCUSD", session._core.policy) == ("BTC", "USD")
    assert ports._crypto_pair("X:NOPEUSD", session._core.policy) == NOT_LISTED


def test_a_pair_whose_currencies_do_not_join_is_left_out_and_named_in_a_poll(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A poll leaves it out of the cycle, and a read lists it as an unreadable_symbols problem
    # of its table, never as a symbol with no data.
    unjoined = _ticker("X:DOTUSD", "DOT", "USDT")
    fake_rest.reply(TICKERS_PATH, Reply(envelope([unjoined])), Reply(envelope([])))
    events: list[ProblemEvent] = []
    monkeypatch.setattr(session._core, "problems", events.extend)
    reader = _endpoints(session)._reader("poll", session._core.policy)
    request = Request(symbols=("X:DOTUSD",), params=(("date", date(2025, 3, 3)),))
    _, left_out = reader._plan_cycle(CRYPTO_OPEN_CLOSE, request, ("X:DOTUSD",), cancel=Cancel())
    assert left_out == [
        (
            "X:DOTUSD (Massive's reference data gives the currencies 'DOT' and 'USDT', which do "
            "not join to it)"
        )
    ]
    with caplog.at_level(logging.WARNING, LOGGER):
        assert fetch(session, CRYPTO_LAST, symbols=["X:DOTUSD"]) == []
    assert [r for r in caplog.records if r.name == LOGGER] == []  # not as no data
    assert sent_data(fake_rest) == []
    # The read, not the plan, lists it: once, for the fetch that left it out.
    [event] = events
    assert (event.cause, event.symbol, event.field, event.policy) == (
        ProblemCause.UNREADABLE_SYMBOLS,
        "X:DOTUSD",
        "Symbol",
        dm.OnError.LOG,
    )
    assert event.detail.endswith("which do not join to it; the pair is never requested")


def test_ignored_withheld_rows_log_no_warning_but_still_reach_the_session(
    *,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Under IGNORE a withheld row logs nothing above DEBUG, and its event still reaches the
    # session; under RAISE the event reaches the session before the read fails.
    odd = {**AAPL_DIVIDEND, "id": "E2", "frequency": 4.5}
    page = Reply(envelope([odd, AAPL_DIVIDEND]))
    fake_rest.reply(DIVIDENDS_PATH, page, page)
    config = dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url)
    with dm.MassiveSession(config) as m:
        events: list[ProblemEvent] = []
        monkeypatch.setattr(m._core, "problems", events.extend)
        quiet = dm.ErrorPolicy(unconvertible_values=dm.OnError.IGNORE)
        with caplog.at_level(logging.DEBUG, LOGGER):
            out = fetch(m, DIVIDENDS, symbols=["AAPL"], on_error=quiet)
        assert [r["Id"] for r in out] == ["E1"]
        withheld = [r for r in caplog.records if "withheld 1 REST row" in r.getMessage()]
        assert withheld == []
        strict = dm.ErrorPolicy(unconvertible_values=dm.OnError.RAISE)
        with pytest.raises(RowDataError, match=r"\(1 under unconvertible_values\)"):
            fetch(m, DIVIDENDS, symbols=["AAPL"], on_error=strict)
    assert [(e.cause, e.symbol, e.field, e.rows) for e in events] == [
        (ProblemCause.UNCONVERTIBLE_VALUES, "AAPL", "Frequency", 1)
    ] * 2


def test_a_row_whose_ticker_list_cannot_be_read_is_withheld_not_left_out(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Its list may hold a named ticker, so it is no row outside the request: it is withheld and
    # reported, beside the row that lists MSFT.
    page = Reply(
        envelope([{"benzinga_id": 1, "tickers": ["MSFT"]}, {"benzinga_id": 2, "tickers": "MSFT"}])
    )
    fake_rest.reply(LISTED_PATH, page)
    events: list[ProblemEvent] = []
    monkeypatch.setattr(session._core, "problems", events.extend)
    out = fetch(session, LISTED, symbols=["AAPL", "MSFT"])
    assert [(r["BenzingaId"], r["Symbol"]) for r in out] == [(1, "MSFT")]
    assert [(e.cause, e.field, e.rows) for e in events] == [
        (ProblemCause.UNCONVERTIBLE_VALUES, "Tickers", 1)
    ]


def test_a_marked_row_whose_readable_list_names_none_of_the_tickers_is_left_out(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Its list was read and names none of the request's tickers, so whatever else is wrong with
    # it, it is no row of the request: left out, with no event and no failure.
    page = Reply(
        envelope(
            [{"benzinga_id": 1, "tickers": ["MSFT"]}, {"benzinga_id": "x", "tickers": ["TSLA"]}]
        )
    )
    fake_rest.reply(LISTED_PATH, page)
    events: list[ProblemEvent] = []
    monkeypatch.setattr(session._core, "problems", events.extend)
    out = fetch(session, LISTED, symbols=["AAPL", "MSFT"])
    assert [(r["BenzingaId"], r["Symbol"]) for r in out] == [(1, "MSFT")]
    assert events == []
