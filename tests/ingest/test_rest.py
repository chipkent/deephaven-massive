"""Tests for the REST transport: requests, pages, conversion, errors, history, and hosts."""

import dataclasses
import functools
import logging
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast
from urllib.parse import parse_qs, unquote, urlsplit
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import new_table
from deephaven.column import string_col
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive import _massive, _session
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.dates import CHICAGO, DaySpan, TimeRange
from deephaven_massive._datasets.history_source import HistoryWindow
from deephaven_massive._datasets.request import HostDecision, Request
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import (
    AssetClass,
    ChannelFamily,
    Direction,
    HistorySource,
    Order,
    PollMode,
    Recency,
    Timespan,
)
from deephaven_massive._ingest import hosts, rest
from deephaven_massive._ingest.poll import RowDataError
from deephaven_massive._massive import RestPath, Transport
from deephaven_massive._namespaces.base import build_query
from deephaven_massive._query import HistoryQuery, Query, QueryBase
from deephaven_massive._tables.core import SessionCore
from deephaven_massive._tables.diagnostics import Diagnostics
from tests._fakes.clock import FakeClock

# The reply and envelope builders are plain functions of the fake, which no fixture can hand
# over; the server itself comes from the `fake_rest` fixture.
from tests._fakes.rest import FakeRestServer, Reply, envelope

R = Transport.REST
LOGGER = "deephaven_massive._ingest.rest"
ToPylist = Callable[[Table], list[dict[str, Any]]]
NS = 10**9

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
    keys={(R, CRYPTO_LAST_PATH): SourceKeys(symbol=KeySpec("from", from_request=True))},
)
MOVERS_PATH = "/v2/snapshot/locale/us/markets/stocks/{direction}"
MOVERS = DatasetSpec(
    summary="A dataset for the tests.",
    name="stocks.movers",
    asset_class=AssetClass.STOCKS,
    fetch_path=MOVERS_PATH,
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
)
CONVERSION_PATH = "/v1/conversion/{from}/{to}"
CONVERSION = DatasetSpec(
    summary="A dataset for the tests.",
    name="forex.conversion",
    asset_class=AssetClass.FOREX,
    fetch_path=CONVERSION_PATH,
    keys={(R, CONVERSION_PATH): SourceKeys(symbol=KeySpec("Symbol"))},
)
INDEX_SNAPSHOT_PATH = "/v3/snapshot/indices"
INDEX_SNAPSHOTS = DatasetSpec(
    summary="A dataset for the tests.",
    name="indices.snapshots",
    asset_class=AssetClass.INDICES,
    fetch_path=INDEX_SNAPSHOT_PATH,
    symbol_param="ticker.any_of",
    keys={(R, INDEX_SNAPSHOT_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
)
INDEX_VALUES = DatasetSpec(
    summary="A dataset for the tests.",
    name="indices.values",
    asset_class=AssetClass.INDICES,
    kind="values",
    fetch_path=INDEX_SNAPSHOT_PATH,
    symbol_param="ticker.any_of",
    keys={(R, INDEX_SNAPSHOT_PATH): SourceKeys(KeySpec("Ticker"), KeySpec("Timestamp"))},
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
    history_path=FUTURES_TRADES_PATH,
    time_axis=("timestamp.gte", "timestamp.lt"),
    keys={
        (R, FUTURES_TRADES_PATH): SourceKeys(
            KeySpec("ticker", from_request=True),
            KeySpec("Timestamp"),
            session=KeySpec("SessionEndDate"),
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
            session=KeySpec("SessionEndDate"),
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
SPECS = (
    DIVIDENDS,
    MIXED,
    CRYPTO_LAST,
    MOVERS,
    CONTRACTS,
    SMA,
    CONVERSION,
    INDEX_SNAPSHOTS,
    INDEX_VALUES,
    TRADES,
    BARS,
    FUTURES_BARS,
    FOREX_BARS,
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


@pytest.fixture(autouse=True)
def retry_clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    """Give every session's REST client a fake clock, so retry waits never sleep.

    The session reads `_massive.RestClient` when it creates its client, so the patch reaches
    it. `setitem` keeps the lazily resolved name out of the module's attributes at teardown.

    Returns:
        The clock.
    """
    clock = FakeClock()
    monkeypatch.setitem(
        vars(_massive), "RestClient", functools.partial(_massive.RestClient, clock=clock)
    )
    return clock


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


def query(m: dm.MassiveSession, spec: DatasetSpec, **kwargs: object) -> QueryBase:
    return build_query(m._core, spec, **cast("dict[str, Any]", kwargs))


def fetch(m: dm.MassiveSession, spec: DatasetSpec, **kwargs: object) -> list[dict[str, Any]]:
    q = query(m, spec, **kwargs)
    assert isinstance(q, Query)
    return rows(q.fetch())


def rows(table: Table) -> list[dict[str, Any]]:
    # Instants become epoch nanoseconds, so assertions keep every digit.
    data = dh_arrow.to_arrow(table)
    columns = [c.cast(pa.int64()) if pa.types.is_timestamp(c.type) else c for c in data.columns]
    out: list[dict[str, Any]] = pa.Table.from_arrays(columns, names=data.column_names).to_pylist()
    return out


def sent(server: FakeRestServer) -> list[tuple[str, dict[str, list[str]]]]:
    # Every GET the server received: its path and decoded query.
    return [
        (unquote(urlsplit(r.target).path), parse_qs(urlsplit(r.target).query))
        for r in server.requests
        if r.method == "GET"
    ]


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
    requests = sent(fake_rest)
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
        ("AAPL", 0.25, date(2025, 2, 10)),
        ("AAPL", 0.25, date(2025, 2, 10)),
    ]


def test_iso_text_in_a_date_filter_is_sent_as_its_date(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(CONSTITUENTS_PATH, Reply(envelope([])))
    q = query(session, CONSTITUENTS, symbols="SPY", filters={"effective_date": "2025-09-18"})
    assert q._request.filters == (("effective_date", date(2025, 9, 18)),)
    assert isinstance(q, Query)
    q.fetch()
    ((_, sent_query),) = sent(fake_rest)
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
    (_, contracts), (_, dividends) = sent(fake_rest)
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
            "ticker_any_of": "AAPL, MSFT",
        },
    )
    ((_, sent_query),) = sent(fake_rest)
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
    fake_rest.reply("/v1/indicators/sma/X:BTCUSD", Reply(envelope({"values": []})))
    fake_rest.reply("/v1/indicators/sma/AAPL", Reply(envelope({"values": []})))
    fetch(session, SMA, symbols=["X:BTCUSD", "AAPL"], start="2025-03-03", end="2025-03-03")
    by_path = dict(sent(fake_rest))
    utc_day = epoch_ns(datetime(2025, 3, 3, tzinfo=UTC)) // 10**6
    new_york_day = epoch_ns(datetime(2025, 3, 3, 5, tzinfo=UTC)) // 10**6
    one_day = 86_400_000
    assert by_path["/v1/indicators/sma/X:BTCUSD"]["timestamp.gte"] == [str(utc_day)]
    assert by_path["/v1/indicators/sma/X:BTCUSD"]["timestamp.lt"] == [str(utc_day + one_day)]
    assert by_path["/v1/indicators/sma/AAPL"]["timestamp.gte"] == [str(new_york_day)]


def test_date_filters_on_instant_parameters_keep_their_calendar_meaning() -> None:
    path = cast("RestPath", _massive.source(R, SMA_CRYPTO))
    day = date(2025, 3, 3)
    instant = datetime(2025, 3, 3, 12, tzinfo=UTC)
    query = {
        "timestamp.gte": day,  # from the day's start
        "timestamp.lt": day,  # before the day's start
        "timestamp.gt": day,  # after the day's end
        "timestamp.lte": day,  # up to the day's end
        "timestamp": day,  # no range bound: kept
        "window": 50,
        "unknown.gte": day,
    }
    assert rest._filter_days(path, {**query, "series_type": instant}, AssetClass.CRYPTO) == {
        **query,
        "series_type": instant,
        "timestamp.gte": datetime(2025, 3, 3, tzinfo=UTC),
        "timestamp.lt": datetime(2025, 3, 3, tzinfo=UTC),
        "timestamp.gt": datetime(2025, 3, 3, 23, 59, 59, 999000, tzinfo=UTC),
        "timestamp.lte": datetime(2025, 3, 3, 23, 59, 59, 999000, tzinfo=UTC),
    }
    # An instant the caller gave is exact already; a calendar date parameter keeps its day.
    # An instant given by the caller is sent as given, even on an inclusive or exclusive
    # bound that a date would move by one unit.
    given = {"timestamp.gte": instant, "timestamp.lte": instant, "timestamp.gt": instant}
    assert rest._filter_days(path, given, None) == given
    dividends = cast("RestPath", _massive.source(R, DIVIDENDS_PATH))
    assert rest._filter_days(dividends, {"ex_dividend_date.gte": day}, None) == {
        "ex_dividend_date.gte": day
    }


def test_an_aware_datetime_on_an_inclusive_instant_filter_is_sent_as_given(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(RATINGS_PATH, Reply(envelope([])))
    noon = datetime(2025, 6, 2, 12, 30, tzinfo=ZoneInfo("America/New_York"))
    fetch(session, RATINGS, filters={"last_updated_lte": noon, "last_updated_gt": noon})
    ((_, sent_query),) = sent(fake_rest)
    assert sent_query["last_updated.lte"] == ["2025-06-02T16:30:00Z"]
    assert sent_query["last_updated.gt"] == ["2025-06-02T16:30:00Z"]


def test_a_list_of_dates_takes_date_items(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(RATINGS_PATH, Reply(envelope([])))
    fetch(session, RATINGS, filters={"date_any_of": [date(2025, 6, 2), "2025-06-03"]})
    ((_, sent_query),) = sent(fake_rest)
    assert sent_query["date.any_of"] == ["2025-06-02,2025-06-03"]
    with pytest.raises(ValueError, match=r"filter 'ticker_any_of' takes text items"):
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
        for _, q in sent(fake_rest)
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
    assert {q["published_utc.gte"][0] for _, q in sent(fake_rest)} == {"2025-03-09T05:00:00Z"}


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
    out = fetch(session, CRYPTO_OPEN_CLOSE, symbols=["X:BTCUSD"], params={"date": date(2025, 3, 3)})
    assert [path for path, _ in sent(fake_rest)] == ["/v1/open-close/crypto/BTC/USD/2025-03-03"]
    assert [(r["Symbol"], r["Open"]) for r in out] == [("X:BTCUSD", 90000.5)]


def test_date_bounds_on_an_instant_axis_are_sent_as_the_days_instants(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(NEWS_PATH, Reply(envelope([])))
    fetch(session, NEWS, start="2025-06-02", end=date(2025, 6, 2))
    # `published_utc` is an instant field, so its date-typed filter gets the half-open
    # instants of the whole day in New York (the zone of a dataset without a class).
    ((_, sent_query),) = sent(fake_rest)
    assert sent_query["published_utc.gte"] == ["2025-06-02T04:00:00Z"]
    assert sent_query["published_utc.lt"] == ["2025-06-03T04:00:00Z"]


def test_a_list_symbol_parameter_without_a_limit_takes_every_symbol_at_once(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope([])))
    fetch(session, INDEX_SNAPSHOTS, symbols=["I:SPX", "I:NDX", "I:DJI"])
    assert [q["ticker.any_of"] for _, q in sent(fake_rest)] == [["I:DJI,I:NDX,I:SPX"]]


def test_limit_caps_rows_across_requests(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([AAPL_DIVIDEND, AAPL_DIVIDEND])))
    out = fetch(session, DIVIDENDS, symbols=["A", "B", "C"], limit=3)
    assert len(out) == 3
    # A cap below the page size is the page size: no request asks for rows it cannot keep.
    assert [q["limit"] for _, q in sent(fake_rest)] == [["3"], ["3"]]


def test_a_limit_above_the_page_size_keeps_the_page_size(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([AAPL_DIVIDEND])))
    fetch(session, DIVIDENDS, symbols=["AAPL"], limit=5000)
    assert [q["limit"] for _, q in sent(fake_rest)] == [["1000"]]


def test_order_is_sent_as_is_where_the_path_has_it(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v1/indicators/sma/AAPL", Reply(envelope({"values": []})))
    fetch(session, SMA, symbols=["AAPL"], params={"order": Order.ASC})
    assert sent(fake_rest)[0][1]["order"] == ["asc"]


def test_order_alone_goes_as_is_on_a_list_path_with_its_own_order(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(NEWS_PATH, Reply(envelope([])))
    fetch(session, NEWS, params={"order": Order.DESC})
    ((_, sent_query),) = sent(fake_rest)
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
    assert isinstance(q, Query)
    with pytest.raises(ValueError, match=r"does not take \['order'\]"):
        q.fetch()
    assert fake_rest.requests == []


def test_path_parameters_come_from_the_typed_parameters(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    gainers = "/v2/snapshot/locale/us/markets/stocks/gainers"
    fake_rest.reply(gainers, Reply(envelope(status="OK", tickers=[{"ticker": "NVDA"}])))
    out = fetch(session, MOVERS, params={"direction": Direction.GAINERS})
    assert sent(fake_rest) == [(gainers, {})]
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
    with caplog.at_level(logging.WARNING, LOGGER):
        out = fetch(session, CRYPTO_LAST, symbols=["BTC-USD", "ETH-USD"])
    assert sorted(path for path, _ in sent(fake_rest)) == [
        "/v1/last/crypto/BTC/USD",
        "/v1/last/crypto/ETH/USD",
    ]
    # ETH answered 404 (unscripted): it is dropped with one WARNING.
    assert [(r["Symbol"], r["Price"], r["Timestamp"]) for r in out] == [
        ("X:BTCUSD", 97000.5, 1704310200000 * 10**6)
    ]
    assert "X:ETHUSD has no data (HTTP 404" in caplog.text


def test_a_dataset_without_pairs_refuses_a_pair_path() -> None:
    spec = DatasetSpec(
        summary="A dataset for the tests.",
        name="stocks.pair",
        asset_class=AssetClass.STOCKS,
        fetch_path=CRYPTO_LAST_PATH,
        keys={(R, CRYPTO_LAST_PATH): SourceKeys(symbol=KeySpec("from", from_request=True))},
    )
    path = cast("RestPath", _massive.source(R, CRYPTO_LAST_PATH))
    with pytest.raises(ValueError, match="only forex and crypto symbols are currency pairs"):
        rest.plan_calls(spec, path, Request(symbols=("AAPL",)), symbols=("AAPL",))


def test_a_per_symbol_path_needs_explicit_symbols() -> None:
    path = cast("RestPath", _massive.source(R, CRYPTO_LAST_PATH))
    with pytest.raises(ValueError, match="needs explicit symbols"):
        rest.plan_calls(CRYPTO_LAST, path, Request(symbols=dm.ALL), symbols=dm.ALL)


def test_a_symbol_query_parameter_of_one_value_fans_out() -> None:
    news = DatasetSpec(
        summary="A dataset for the tests.",
        name="reference.news",
        fetch_path="/v2/reference/news",
        symbol_param="ticker",
        keys={(R, "/v2/reference/news"): SourceKeys()},
    )
    path = cast("RestPath", _massive.source(R, "/v2/reference/news"))
    calls = rest.plan_calls(news, path, Request(), symbols=("AAPL", "MSFT"))
    assert [(c.symbol, dict(c.query)) for c in calls] == [
        ("AAPL", {"ticker": "AAPL"}),
        ("MSFT", {"ticker": "MSFT"}),
    ]
    assert rest.plan_calls(news, path, Request(), symbols=()) == []
    # A path that takes no symbols ignores them: one request.
    movers = cast("RestPath", _massive.source(R, MOVERS_PATH))
    request = Request(params=(("direction", Direction.LOSERS),))
    (call,) = rest.plan_calls(MOVERS, movers, request, symbols=("AAPL", "MSFT"))
    assert (call.symbol, dict(call.path_params), dict(call.query)) == (
        None,
        {"direction": Direction.LOSERS},
        {},
    )
    assert [c.symbol for c in rest.plan_calls(news, path, Request(), symbols=None)] == [None]


def test_a_per_symbol_path_with_a_list_parameter_sends_one_symbol_each() -> None:
    spec = DatasetSpec(
        summary="A dataset for the tests.",
        name="stocks.each",
        asset_class=AssetClass.STOCKS,
        fetch_path=DIVIDENDS_PATH,
        symbol_param="ticker.any_of",
        per_symbol=True,
        keys={(R, DIVIDENDS_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
    )
    path = cast("RestPath", _massive.source(R, DIVIDENDS_PATH))
    calls = rest.plan_calls(spec, path, Request(), symbols=("A", "B"))
    assert [dict(c.query) for c in calls] == [{"ticker.any_of": ("A",)}, {"ticker.any_of": ("B",)}]


def test_a_symbol_filled_into_a_query_parameter() -> None:
    spec = DatasetSpec(
        summary="A dataset for the tests.",
        name="stocks.by_query",
        asset_class=AssetClass.STOCKS,
        fetch_path=DIVIDENDS_PATH,
        keys={(R, DIVIDENDS_PATH): SourceKeys(symbol=KeySpec("ticker", from_request=True))},
    )
    path = cast("RestPath", _massive.source(R, DIVIDENDS_PATH))
    calls = rest.plan_calls(spec, path, Request(), symbols=("A",))
    assert [(c.symbol, dict(c.path_params), dict(c.query)) for c in calls] == [
        ("A", {}, {"ticker": "A"})
    ]


def test_symbols_route_to_their_class_path_and_send_only_its_parameters(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(
        "/v1/indicators/sma/AAPL",
        Reply(envelope({"values": [{"timestamp": 1704310200000, "value": 185.0}]})),
    )
    fake_rest.reply(
        "/v1/indicators/sma/X%3ABTCUSD",
        Reply(envelope({"values": [{"timestamp": 1704310200000, "value": 97000.0}]})),
    )
    out = fetch(
        session,
        SMA,
        symbols=["AAPL", "X:BTCUSD"],
        timespan=Timespan.DAY,
        params={"window": 50},
        start=datetime(2024, 1, 1, tzinfo=UTC),
    )
    requests = dict(sent(fake_rest))
    assert requests["/v1/indicators/sma/AAPL"] == {
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
    assert isinstance(q, Query)
    with pytest.raises(ValueError, match=r"cryptoTicker\} does not take \['adjusted'\]"):
        q.fetch()
    assert fake_rest.requests == []
    fake_rest.reply("/v1/indicators/sma/AAPL", Reply(envelope({"values": []})))
    fetch(session, SMA, symbols=["AAPL"], adjusted=False)
    assert sent(fake_rest)[0][1]["adjusted"] == ["false"]


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
    assert [path for path, _ in sent(fake_rest)] == [
        CONTRACTS_PATH,
        "/v3/reference/options/contracts/O:SPY250117C00600000",
    ]
    assert listed == named
    # A filter of the list path cannot reach the detail path: it fails, never drops.
    q = query(session, CONTRACTS, symbols=["O:SPY250117C00600000"], filters={"strike_price_gte": 1})
    assert isinstance(q, Query)
    with pytest.raises(ValueError, match=r"does not take \['strike_price_gte'\]"):
        q.fetch()


def test_row_symbols_are_canonical(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
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
    assert sent(fake_rest)[0] == ("/v1/conversion/AUD/USD", {"amount": ["100"]})
    with caplog.at_level(logging.WARNING, LOGGER):
        odd = fetch(session, CONVERSION, params={"from": "AUD", "to": "XXX"})
    assert odd[0]["Symbol"] == "not a pair"
    assert "not a valid symbol of its class" in caplog.text


def test_a_reactive_symbol_table_is_read_once_for_fetch(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope([])))
    watch = new_table([string_col("Symbol", ["SPX", "I:NDX", "not a symbol!"])])
    fetch(session, INDEX_SNAPSHOTS, symbols=watch)
    assert sent(fake_rest)[0][1]["ticker.any_of"] == ["I:NDX,I:SPX"]


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
    # Once per ticker and session.
    kept = "the row is kept with its Error and Message columns"
    assert caplog.text.count(f"NOT_ENTITLED for I:VIX: Not entitled; {kept}") == 1
    assert caplog.text.count(f"NOT_ENTITLED for I:RUT: Not entitled; {kept}") == 1


def test_error_rows_of_a_projected_path_are_dropped_with_a_warning(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    refused = {"ticker": "I:VIX", "error": "NOT_ENTITLED", "message": "Not entitled"}
    spx = {"ticker": "I:SPX", "value": 5900.0, "last_updated": 1704310200000000000}
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope([refused, spx])))
    with caplog.at_level(logging.WARNING, LOGGER):
        out = fetch(session, INDEX_VALUES, symbols=["I:SPX", "I:VIX"])
    assert [(r["Symbol"], r["Timestamp"], r["Value"]) for r in out] == [
        ("I:SPX", 1704310200000000000, 5900.0)
    ]
    assert (
        "Massive answered NOT_ENTITLED for I:VIX: Not entitled; the row is dropped" in caplog.text
    )


def test_drift_is_reported_once_per_field_and_status(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    fake_rest.reply(
        DIVIDENDS_PATH,
        Reply(envelope([{**AAPL_DIVIDEND, "brand_new": 1}], status="PARTIAL")),
    )
    with caplog.at_level(logging.WARNING):
        fetch(session, DIVIDENDS, symbols=["AAPL"])
        fetch(session, DIVIDENDS, symbols=["AAPL"])
    assert caplog.text.count("'brand_new' is not in the schema") == 1
    assert caplog.text.count("undocumented response status 'PARTIAL'") == 1


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
    assert "stocks.dividends: " in caplog.text  # logged at ERROR with the same text


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
    with caplog.at_level(logging.ERROR, LOGGER), pytest.raises(error) as info:
        fetch(session, DIVIDENDS, symbols=["AAPL"])
    assert (info.value.status, info.value.request_id) == (status, "r9")
    assert f"HTTP {status}" in caplog.text
    assert "test-key" not in caplog.text


def test_a_lasting_rate_limit_names_itself_and_warns_once(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    fake_rest.reply(DIVIDENDS_PATH, Reply(b"{}", status=429, headers={"Retry-After": "1"}))
    with caplog.at_level(logging.WARNING, LOGGER):
        for _ in range(2):
            with pytest.raises(dm.MassiveError, match=r"^rate limited: ") as info:
                fetch(session, DIVIDENDS, symbols=["AAPL"])
            assert info.value.status == 429
    assert caplog.text.count("Basic keys allow 5 requests per minute") == 1


def test_a_failed_symbol_fails_the_whole_fan_out(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v1/last/crypto/BTC/USD", Reply(b"oops", status=500))
    with pytest.raises(dm.MassiveError, match="HTTP 500"):
        fetch(session, CRYPTO_LAST, symbols=["BTC-USD", "ETH-USD", "SOL-USD"])


def test_a_closed_session_reports_itself(session: dm.MassiveSession) -> None:
    transport = session._core.rest_transport()
    session.close()
    with pytest.raises(dm.MassiveError, match=r"^MassiveSession #\d+ is closed$"):
        transport.fetch(DIVIDENDS, Request(symbols=("AAPL",)))


# --- History -----------------------------------------------------------------------------------


def history(
    m: dm.MassiveSession, spec: DatasetSpec, *, start: object, end: object, **kwargs: object
) -> list[dict[str, Any]]:
    q = query(m, spec, **kwargs)
    assert isinstance(q, HistoryQuery)
    return rows(q.history(cast("Any", start), cast("Any", end), source=HistorySource.REST))


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
    query_sent = sent(fake_rest)[0][1]
    assert query_sent["timestamp.gte"] == [str(start)]
    assert query_sent["timestamp.lt"] == [str(start + 86_400 * NS)]
    assert "stocks.trades history: source=REST requests=2" in caplog.text


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
    assert sent(fake_rest) == [(path, {"adjusted": ["false"], "sort": ["asc"]})]
    assert [(r["Symbol"], r["Timestamp"], r["Close"]) for r in out] == [
        ("AAPL", midnight * 10**6, 2.0)
    ]


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
    assert sorted(path for path, _ in sent(fake_rest)) == [
        GROUPED_PATH.format(date="2025-03-03"),
        GROUPED_PATH.format(date="2025-03-04"),
    ]
    assert [(r["Symbol"], r["Close"]) for r in out] == [("AAPL", 2.0), ("MSFT", 3.0)]


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
    q = sent(fake_rest)[0][1]
    # From the session's regular open, Monday 17:00 Chicago, less the four-day holiday
    # margin, to 18:00 Chicago on the session's date; the rows are kept by session.
    opens = datetime(2025, 2, 27, 17, tzinfo=CHICAGO)
    assert q["window_start.gte"] == [str(epoch_ns(opens))]
    assert q["window_start.lt"] == [str(epoch_ns(datetime(2025, 3, 4, 18, tzinfo=CHICAGO)))]
    assert q["resolution"] == ["5min"]
    assert q["sort"] == ["window_start.asc"]  # oldest first, as flat files return rows
    assert [r["SessionEndDate"] for r in out] == [date(2025, 3, 4)]


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
    return f"/futures/v1/trades/{ticker}?sort=timestamp.asc&session_end_date={day}"


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
    assert sent(fake_rest) == [
        (path, {"sort": ["timestamp.asc"], "session_end_date": ["2025-01-21"]})
    ]
    # A row of another session is dropped.
    assert [r["Timestamp"] for r in out] == [sunday["timestamp"], tuesday["timestamp"]]


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
    with caplog.at_level(logging.INFO, LOGGER):
        out = history(
            session,
            FUTURES_TRADES,
            start="2024-03-28",
            end="2024-04-01",
            symbols=["NQH5", "ESH5"],
        )
    asked = sorted(q["session_end_date"][0] for _, q in sent(fake_rest))
    assert asked == ["2024-03-28"] * 2 + ["2024-03-29"] * 2 + ["2024-04-01"] * 2
    # Sessions in date order, and the symbols of one session in symbol order.
    assert [(r["Symbol"], r["SessionEndDate"]) for r in out] == [
        ("ESH5", date(2024, 3, 28)),
        ("NQH5", date(2024, 3, 28)),
        ("ESH5", date(2024, 4, 1)),
        ("NQH5", date(2024, 4, 1)),
    ]
    assert (
        "futures.trades history: 6 requests, one per day or session; at a Basic key's 5 "
        "requests a minute that takes about 2 minutes"
    ) in caplog.text


def test_a_short_session_download_is_not_announced(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    # A weekend holds no session, and five requests are not announced.
    for day in ("2025-03-03", "2025-03-04", "2025-03-05", "2025-03-06", "2025-03-07"):
        fake_rest.reply(session_target("ESH5", day), Reply(envelope([])))
    with caplog.at_level(logging.INFO, LOGGER):
        history(session, FUTURES_TRADES, start="2025-03-01", end="2025-03-09", symbols=["ESH5"])
    assert len(sent(fake_rest)) == 5
    assert "one per day or session" not in caplog.text


def test_a_session_end_date_filter_needs_datetimes(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    q = query(session, FUTURES_TRADES, symbols=["ESH5"], filters={"session_end_date": "2025-01-21"})
    assert isinstance(q, HistoryQuery)
    with pytest.raises(ValueError, match=r"history\(\) dates set session_end_date themselves"):
        q.history("2025-01-21", "2025-01-21", source=HistorySource.REST)
    assert sent(fake_rest) == []
    # With datetimes, the filter and the time range travel together.
    fake_rest.reply(FUTURES_TRADES_PATH.format(ticker="ESH5"), Reply(envelope([])))
    start = datetime(2025, 1, 21, 8, tzinfo=CHICAGO)
    q.history(start, start + timedelta(hours=1), source=HistorySource.REST)
    sent_query = sent(fake_rest)[0][1]
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
    q = sent(fake_rest)[0][1]
    assert q["timestamp.gte"] == [str(epoch_ns(start))]
    assert q["timestamp.lt"] == [str(epoch_ns(end))]
    assert "session_end_date" not in q
    assert [r["Timestamp"] for r in out] == [evening["timestamp"], morning["timestamp"]]


def window_of(*, times: TimeRange | None = None, sessions: DaySpan | None = None) -> HistoryWindow:
    span: TimeRange | DaySpan = times or sessions or DaySpan(date(2025, 1, 1), date(2025, 1, 1))
    return HistoryWindow(span, times, sessions, times)


def test_keep_bounds_times_and_sessions_where_the_rows_carry_them() -> None:
    late = epoch_ns(datetime(2025, 3, 4, 20, tzinfo=CHICAGO))
    times = pa.RecordBatch.from_pydict({"Timestamp": pa.array([late], pa.timestamp("ns", "UTC"))})
    days = pa.RecordBatch.from_pydict({"SessionEndDate": pa.array([date(2025, 3, 3)], pa.date32())})
    other = pa.RecordBatch.from_pydict({"Other": pa.array([1])})
    sessions = window_of(sessions=DaySpan(date(2025, 3, 4), date(2025, 3, 4)))
    before_late = window_of(
        times=TimeRange(datetime(2025, 3, 4, tzinfo=UTC), datetime(2025, 3, 5, 1, tzinfo=UTC))
    )
    assert rest._keep(times, sessions).num_rows == 1  # sessions bound only SessionEndDate
    assert rest._keep(times, before_late).num_rows == 0  # 02:00 UTC on 03-05 is past the end
    assert rest._keep(days, sessions).num_rows == 0
    assert rest._keep(days, before_late).num_rows == 1  # no Timestamp to bound
    assert rest._keep(other, sessions).num_rows == 1
    assert rest._keep(times, None).num_rows == 1
    upper = window_of(sessions=DaySpan(date(2025, 3, 1), date(2025, 3, 2)))
    assert rest._keep(days, upper).num_rows == 0  # after the last session
    lower = window_of(
        times=TimeRange(datetime(2025, 3, 5, 3, tzinfo=UTC), datetime(2025, 3, 6, tzinfo=UTC))
    )
    assert rest._keep(times, lower).num_rows == 0  # before the first instant


@pytest.mark.parametrize(
    ("path", "time_axis", "expected"),
    [
        (TRADES_PATH, ("timestamp.gte", "timestamp.lt"), {"order": "asc", "sort": "timestamp"}),
        ("/v1/indicators/sma/{stockTicker}", ("timestamp.gte", "timestamp.lt"), {"order": "asc"}),
        (RANGE_PATH, None, {"sort": "asc"}),
        (FUTURES_AGGS_PATH, ("window_start.gte", "window_start.lt"), {"sort": "window_start.asc"}),
        (FUTURES_TRADES_PATH, ("timestamp.gte", "timestamp.lt"), {"sort": "timestamp.asc"}),
        (FUTURES_TRADES_PATH, None, {}),
        # Several sort fields: the axis field when listed, else only the direction.
        (
            "/vX/reference/ipos",
            ("announced_date.gte", "announced_date.lte"),
            {"order": "asc", "sort": "announced_date"},
        ),
        ("/vX/reference/ipos", ("other.gte", "other.lt"), {"order": "asc"}),
        ("/v1/open-close/{stocksTicker}/{date}", None, {}),
    ],
)
def test_history_asks_for_ascending_order(
    path: str, time_axis: tuple[str, str] | None, expected: dict[str, object]
) -> None:
    spec = DatasetSpec(
        summary="A dataset for the tests.",
        name="test.ascending",
        history_path=path,
        time_axis=time_axis,
    )
    rest_path = cast("RestPath", _massive.source(R, path))
    assert rest._ascending(spec, rest_path, Request()) == expected
    # A direction the caller set is kept.
    assert rest._ascending(spec, rest_path, Request(params=(("order", "desc"),))) == {}


def test_a_caller_direction_folded_into_sort_is_not_overridden() -> None:
    # The futures aggregates take no `order`: a caller's `order` folds into `sort`, and the
    # ascending default stays out of the way.
    rest_path = cast("RestPath", _massive.source(R, FUTURES_AGGS_PATH))
    mine = Request(
        symbols=("ESZ5",),
        timespan=Timespan.MINUTE,
        params=(("order", Order.DESC), ("sort", "window_start")),
    )
    order = rest._ascending(FUTURES_BARS, rest_path, mine)
    (call,) = rest.plan_calls(FUTURES_BARS, rest_path, mine, symbols=("ESZ5",), extra=order)
    assert call.query["sort"] == "window_start.desc"
    assert "order" not in call.query
    # Without a caller direction, the ascending one is sent.
    plain = Request(symbols=("ESZ5",), timespan=Timespan.MINUTE)
    order = rest._ascending(FUTURES_BARS, rest_path, plain)
    (call,) = rest.plan_calls(FUTURES_BARS, rest_path, plain, symbols=("ESZ5",), extra=order)
    assert call.query["sort"] == "window_start.asc"


def test_ascending_prefers_the_axis_field_among_several_ascending_sorts() -> None:
    spec = DatasetSpec(
        summary="A dataset for the tests.",
        name="test.ascending",
        history_path=FUTURES_AGGS_PATH,
        time_axis=("window_start.gte", "window_start.lt"),
    )
    aggs = cast("RestPath", _massive.source(R, FUTURES_AGGS_PATH))
    values = ("ticker.asc", "window_start.asc", "window_start.desc")
    params = tuple(
        dataclasses.replace(p, values=values) if p.name == "sort" else p for p in aggs.params
    )
    several = dataclasses.replace(aggs, params=params)
    assert rest._ascending(spec, several, Request()) == {"sort": "window_start.asc"}
    other = dataclasses.replace(spec, time_axis=("other.gte", "other.lt"))
    assert rest._ascending(other, several, Request()) == {"sort": "ticker.asc"}


# --- The time axis -------------------------------------------------------------------------------


def axis_spec(lower: str, upper: str, path: str = TRADES_PATH) -> DatasetSpec:
    return DatasetSpec(
        summary="A dataset for the tests.",
        name="stocks.axis",
        asset_class=AssetClass.STOCKS,
        history_path=path,
        time_axis=(lower, upper),
        keys={
            (R, path): SourceKeys(
                KeySpec("stockTicker", from_request=True), KeySpec("SipTimestamp")
            )
        }
        if path == TRADES_PATH
        else {},
    )


def axis(spec: DatasetSpec, start: object, end: object) -> dict[str, object]:
    path = cast("RestPath", _massive.source(R, cast("str", spec.history_path)))
    return rest._axis_query(spec, path, dates=(cast("Any", start), cast("Any", end)))


def test_date_axes_are_whole_days_and_shift_for_exclusive_ends() -> None:
    spec = axis_spec("ex_dividend_date.gt", "ex_dividend_date.lt", DIVIDENDS_PATH)
    assert axis(spec, date(2025, 1, 10), date(2025, 1, 20)) == {
        "ex_dividend_date.gt": date(2025, 1, 9),
        "ex_dividend_date.lt": date(2025, 1, 21),
    }
    inclusive = axis_spec("ex_dividend_date.gte", "ex_dividend_date.lte", DIVIDENDS_PATH)
    assert axis(inclusive, date(2025, 1, 10), None) == {"ex_dividend_date.gte": date(2025, 1, 10)}


def test_timestamp_axes_take_midnights_in_the_day_zone() -> None:
    spec = axis_spec("timestamp.gte", "timestamp.lt")
    out = axis(spec, date(2025, 3, 3), date(2025, 3, 3))
    assert out["timestamp.gte"] == datetime(2025, 3, 3, 5, tzinfo=UTC)
    # A dataset without an asset class bounds its days in New York.
    path = cast("RestPath", _massive.source(R, SMA_STOCK))
    unclassed = rest._axis_query(SMA, path, dates=(date(2025, 7, 1), None))
    assert unclassed == {"timestamp.gte": datetime(2025, 7, 1, 4, tzinfo=UTC)}
    assert out["timestamp.lt"] == datetime(2025, 3, 4, 5, tzinfo=UTC)


def test_inclusive_millisecond_bounds_move_back_one_unit() -> None:
    spec = axis_spec("timestamp.gt", "timestamp.lte", SMA_STOCK)
    out = axis(spec, date(2025, 3, 3), date(2025, 3, 3))
    assert out["timestamp.gt"] == datetime(2025, 3, 3, 4, 59, 59, 999000, tzinfo=UTC)
    assert out["timestamp.lte"] == datetime(2025, 3, 4, 4, 59, 59, 999000, tzinfo=UTC)


def test_an_instant_field_behind_a_date_parameter_makes_an_instant_axis() -> None:
    spec = axis_spec("published_utc.gte", "published_utc.lt", NEWS_PATH)
    out = axis(spec, date(2025, 3, 3), date(2025, 3, 3))
    assert out == {
        "published_utc.gte": datetime(2025, 3, 3, 5, tzinfo=UTC),
        "published_utc.lt": datetime(2025, 3, 4, 5, tzinfo=UTC),
    }
    assert all(type(value) is datetime for value in out.values())
    # An instant bound is already exact.
    instant = datetime(2025, 3, 3, 14, 30, tzinfo=UTC)
    assert axis(spec, instant, None) == {"published_utc.gte": instant}
    # Massive does not document the precision of the ISO-8601 text, so no inclusive bound.
    inclusive = axis_spec("published_utc.gte", "published_utc.lte", NEWS_PATH)
    with pytest.raises(ValueError, match=r"published_utc\.lte takes ISO-8601 instants"):
        axis(inclusive, None, date(2025, 3, 3))


def test_a_date_parameter_without_a_field_of_its_name_stays_whole_days() -> None:
    # The options chain snapshot nests `expiration_date` under `details`.
    path = "/v3/snapshot/options/{underlyingAsset}"
    spec = axis_spec("expiration_date.gte", "expiration_date.lte", path)
    assert axis(spec, date(2025, 3, 3), date(2025, 3, 4)) == {
        "expiration_date.gte": date(2025, 3, 3),
        "expiration_date.lte": date(2025, 3, 4),
    }


def test_a_nanosecond_inclusive_bound_is_refused() -> None:
    spec = axis_spec("timestamp.gte", "timestamp.lte")
    with pytest.raises(ValueError, match=r"timestamp\.lte takes nanoseconds"):
        axis(spec, None, date(2025, 3, 3))


def test_an_axis_the_path_lacks_or_of_another_kind() -> None:
    spec = axis_spec("ticker.gte", "nothing.lt", DIVIDENDS_PATH)
    assert axis(spec, date(2025, 3, 3), date(2025, 3, 4)) == {"ticker.gte": "2025-03-03"}
    assert (
        rest._axis_query(
            MOVERS, cast("RestPath", _massive.source(R, MOVERS_PATH)), dates=(None, None)
        )
        == {}
    )


# --- Host decisions ----------------------------------------------------------------------------


@pytest.fixture
def snapshots(
    fake_rest: FakeRestServer, load_fixture: Callable[[str, str], bytes]
) -> FakeRestServer:
    fake_rest.reply("/v3/snapshot", Reply(load_fixture("ingest/hosts", "stocks_delayed.json")))
    return fake_rest


def connection_rows(m: dm.MassiveSession, dh_to_pylist: ToPylist) -> list[dict[str, Any]]:
    assert m._core.diagnostics.barrier().wait(30)
    return list(dh_to_pylist(m.connections()))


def test_a_decision_is_cached_and_recorded(
    *, session: dm.MassiveSession, snapshots: FakeRestServer, dh_to_pylist: ToPylist
) -> None:
    transport = session._core.rest_transport()
    decision = transport.resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    again = session._core.rest_transport().resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    assert decision is again
    assert (decision.host, decision.recency) == ("delayed.massive.com", Recency.DELAYED)
    assert len(snapshots.requests) == 1
    (row,) = connection_rows(session, dh_to_pylist)
    assert (row["Transport"], row["AssetClass"], row["Family"], row["Host"], row["Path"]) == (
        "REST",
        "stocks",
        "TICK",
        "delayed.massive.com",
        "/stocks",
    )
    assert (row["Recency"], row["State"], row["Evidence"]) == (
        "DELAYED",
        "CONNECTED",
        "last_trade.timeframe=DELAYED",
    )


def test_a_failed_decision_is_not_cached(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
    dh_to_pylist: ToPylist,
) -> None:
    fake_rest.reply("/v3/snapshot", Reply(b'{"status":"ERROR","request_id":"x1"}', status=403))
    transport = session._core.rest_transport()
    with pytest.raises(dm.EntitlementError, match="stocks plan has no WebSocket access"):
        transport.resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    (row,) = connection_rows(session, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert "HTTP 403" in row["LastError"]
    assert "x1" in row["LastError"]
    fake_rest.reply_first(
        "/v3/snapshot", Reply(load_fixture("ingest/hosts", "stocks_real_time.json"))
    )
    assert transport.resolve_host(AssetClass.STOCKS, ChannelFamily.TICK).recency is Recency.REALTIME


def test_a_defect_while_deciding_caches_nothing(
    *, session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*args: object, **kwargs: object) -> object:
        raise KeyError("bug")

    monkeypatch.setattr(hosts, "decide", broken)
    transport = cast("rest.RestEndpoints", session._core.rest_transport())
    with pytest.raises(KeyError):
        transport.resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    _, owner = transport._state.host_future((AssetClass.STOCKS, ChannelFamily.TICK))
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


def test_waiters_get_a_copy_of_the_deciders_error(session: dm.MassiveSession) -> None:
    future: Future[Any] = Future()
    error = dm.EntitlementError("no", status=403, request_id="r1")
    future.set_exception(error)
    with pytest.raises(dm.EntitlementError, match=r"^no$") as info:
        rest._waited(future)
    assert info.value is not error
    assert (info.value.status, info.value.request_id) == (403, "r1")


def test_forgetting_keeps_a_newer_decision(session: dm.MassiveSession) -> None:
    state = cast("rest.RestEndpoints", session._core.rest_transport())._state
    key = (AssetClass.STOCKS, ChannelFamily.TICK)
    old, _ = state.host_future(key)
    state.forget_host(key, old)
    new, created = state.host_future(key)
    assert created
    state.forget_host(key, old)  # not the current one: kept
    assert state.host_future(key) == (new, False)


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


def test_a_missing_symbol_and_a_dropped_row_warn_once_per_session(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    refused = {"ticker": "I:VIX", "error": "NOT_ENTITLED", "message": "Not entitled"}
    fake_rest.reply(INDEX_SNAPSHOT_PATH, Reply(envelope([refused])))
    with caplog.at_level(logging.WARNING, LOGGER):
        for _ in range(2):
            assert fetch(session, CRYPTO_LAST, symbols=["ETH-USD"]) == []
            assert fetch(session, INDEX_VALUES, symbols=["I:VIX"]) == []
    assert caplog.text.count("X:ETHUSD has no data") == 1
    assert caplog.text.count("for I:VIX: Not entitled; the row is dropped") == 1


def test_a_read_fans_out_on_the_callers_pool(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    last = {"price": 1.0, "timestamp": 1704310200000}
    for pair in ("BTC/USD", "ETH/USD"):
        fake_rest.reply(
            f"/v1/last/crypto/{pair}",
            Reply({"status": "success", "request_id": "r", "symbol": pair, "last": last}),
        )
    reader = cast("rest.RestEndpoints", session._core.rest_transport())._reader()
    path = reader.rest_path(CRYPTO_LAST_PATH)
    request = Request(symbols=("X:BTCUSD", "X:ETHUSD"))
    calls = rest.plan_calls(CRYPTO_LAST, path, request, symbols=request.symbols)
    with ThreadPoolExecutor(2) as pool:
        table, requests = reader.read(CRYPTO_LAST, path, calls, limit=None, executor=pool)
        assert not pool._shutdown  # the caller's pool stays open
    assert (table.num_rows, requests) == (2, 2)


def test_a_failed_history_is_logged_and_raised(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    fake_rest.reply(TRADES_PATH.format(stockTicker="AAPL"), Reply(b"{}", status=403))
    with caplog.at_level(logging.ERROR, LOGGER), pytest.raises(dm.EntitlementError):
        history(session, TRADES, start="2025-03-03", end="2025-03-03", symbols=["AAPL"])
    assert "HTTP 403" in caplog.text


def test_a_404_without_a_symbol_fails_the_call(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # Only a per-symbol request drops on 404; a list path that is missing is an error.
    with pytest.raises(dm.MassiveError, match="HTTP 404"):
        fetch(session, DIVIDENDS, symbols=["AAPL"])


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
    reader = cast("rest.RestEndpoints", session._core.rest_transport())._reader()
    table, _ = reader.read_fetch(timed, Request(symbols=("I:VIX",)), symbols=("I:VIX",))
    assert table.column("Symbol").to_pylist() == ["I:VIX"]
    assert table.column("Timestamp").to_pylist() == [None]


def test_a_failed_call_cancels_the_calls_not_yet_started(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v1/last/crypto/BTC/USD", Reply(b"{}", status=401))
    reader = cast("rest.RestEndpoints", session._core.rest_transport())._reader()
    path = reader.rest_path(CRYPTO_LAST_PATH)
    symbols = ("X:BTCUSD", "X:ETHUSD", "X:SOLUSD", "X:XRPUSD")
    calls = rest.plan_calls(CRYPTO_LAST, path, Request(symbols=symbols), symbols=symbols)
    with ThreadPoolExecutor(1) as pool, pytest.raises(dm.AuthenticationError):
        reader.read(CRYPTO_LAST, path, calls, limit=None, executor=pool)
    # The one worker may take the next call before the read cancels it; the rest never start.
    assert len(sent(fake_rest)) <= 2


def test_an_empty_symbol_tuple_plans_no_request() -> None:
    path = cast("RestPath", _massive.source(R, INDEX_SNAPSHOT_PATH))
    assert rest.plan_calls(INDEX_SNAPSHOTS, path, Request(), symbols=()) == []
    assert rest.fetch_paths(SMA, ()) == [(SMA_STOCK, ())]


def test_futures_date_bounds_are_whole_sessions() -> None:
    path = "/futures/v1/trades/{ticker}"
    spec = DatasetSpec(
        summary="A dataset for the tests.",
        name="futures.recent",
        asset_class=AssetClass.FUTURES,
        fetch_path=path,
        time_axis=("timestamp.gte", "timestamp.lt"),
        keys={(R, path): SourceKeys(symbol=KeySpec("ticker", from_request=True))},
    )
    rest_path = cast("RestPath", _massive.source(R, path))
    out = rest._axis_query(spec, rest_path, dates=(date(2025, 3, 4), date(2025, 3, 4)))
    # The session ending 2025-03-04 opens at 17:00 Chicago time on the 3rd.
    assert out == {
        "timestamp.gte": datetime(2025, 3, 3, 17, tzinfo=CHICAGO),
        "timestamp.lt": datetime(2025, 3, 4, 17, tzinfo=CHICAGO),
    }


def test_range_segments_floor_to_whole_milliseconds() -> None:
    start = datetime(2025, 3, 3, 14, 30, 0, 123456, tzinfo=UTC)
    end = datetime(2025, 3, 3, 15, 0, 0, 500, tzinfo=UTC)
    out = rest._range_segments(TimeRange(start, end))
    assert out == {
        "from": datetime(2025, 3, 3, 14, 30, 0, 123000, tzinfo=UTC),
        "to": datetime(2025, 3, 3, 15, 0, 0, 0, tzinfo=UTC),
    }


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
    assert later.recency is Recency.DELAYED
    assert len(snapshots.requests) == 1


def test_a_parameter_no_fetch_path_takes_fails_a_reactive_poll_at_once(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    watch = new_table([string_col("Symbol", ["X:BTCUSD"])])
    q = query(session, SMA, symbols=watch, params={"bogus": 1})
    assert isinstance(q, Query)
    with pytest.raises(ValueError, match=r"does not take \['bogus'\]"):
        q.poll(60)
    assert fake_rest.requests == []


def test_an_empty_symbol_tuple_reads_nothing(*, session: dm.MassiveSession) -> None:
    reader = cast("rest.RestEndpoints", session._core.rest_transport())._reader()
    request = Request(symbols=(), adjusted=False)  # no path would take `adjusted` here
    assert reader.plan_fetch(DIVIDENDS, request, ()) == []
    table, requests = reader.read_fetch(DIVIDENDS, request, symbols=())
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
    with caplog.at_level(logging.WARNING, LOGGER):
        (row,) = fetch(session, CRYPTO_LAST, symbols=["BTC-USD"])
    assert row["Symbol"] == "X:BTCUSD"
    assert "Massive answered NOT_ENTITLED for X:BTCUSD: Not entitled" in caplog.text


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
    transport = cast("rest.RestEndpoints", session._core.rest_transport())
    watch = SymbolTable(new_table([string_col("Symbol", ["ESZ5"])]))
    request = Request(symbols=watch, end=date(2025, 3, 4))
    with pytest.raises(ValueError, match=r"timestamp\.lte takes nanoseconds"):
        transport.poll(spec, request, interval=timedelta(seconds=60), mode=PollMode.REPLACE)
    assert fake_rest.requests == []


def test_a_server_verdict_replaces_the_cached_decision(
    *, session: dm.MassiveSession, snapshots: FakeRestServer, dh_to_pylist: ToPylist
) -> None:
    transport = session._core.rest_transport()
    first = transport.resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    verdict = HostDecision("delayed.massive.com", Recency.DELAYED, "server verdict: real-time")
    transport.record_verdict(AssetClass.STOCKS, ChannelFamily.TICK, verdict)
    later = session._core.rest_transport().resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    assert later is verdict
    assert later is not first
    assert len(snapshots.requests) == 1  # the verdict needed no request
    (row,) = connection_rows(session, dh_to_pylist)
    assert (row["Host"], row["Recency"], row["Evidence"], row["State"]) == (
        "delayed.massive.com",
        "DELAYED",
        "server verdict: real-time",
        "CONNECTED",
    )
    # A pair never decided before takes the verdict too, with no request.
    fmv = HostDecision("business.massive.com", Recency.DELAYED, "server verdict: x")
    transport.record_verdict(AssetClass.OPTIONS, ChannelFamily.FMV, fmv)
    assert transport.resolve_host(AssetClass.OPTIONS, ChannelFamily.FMV) is fmv


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
    # Wednesday 03-05 to Friday 03-21 holds the weeks of 03-10 and 03-17 whole.
    start, end = ny_ms(date(2025, 3, 10)), ny_ms(date(2025, 3, 22)) - 1
    path = f"/v2/aggs/ticker/AAPL/range/1/week/{start}/{end}"
    stamps = [ny_ms(date(2025, 3, d)) for d in (2, 9, 16, 23)]  # Sundays, as a week may be
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
    assert [p for p, _ in sent(fake_rest)] == [path]
    assert [r["Timestamp"] for r in out] == [stamps[1] * 10**6, stamps[2] * 10**6]


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
    assert [p for p, _ in sent(fake_rest)] == [path]
    assert [r["Timestamp"] for r in out] == [stamps[0] * 10**6, stamps[1] * 10**6]


@pytest.mark.parametrize("source", [HistorySource.REST, HistorySource.AUTO])
def test_an_empty_range_returns_every_column_without_a_request(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, source: HistorySource
) -> None:
    moment = datetime(2025, 3, 3, 15, tzinfo=UTC)
    q = query(session, TRADES, symbols=["AAPL"])
    assert isinstance(q, HistoryQuery)
    table = q.history(moment, moment, source=source)
    assert table.size == 0
    assert "Timestamp" in table.column_names
    # So is a range too short to hold one whole bar.
    bars = query(session, BARS, symbols=["AAPL"], timespan=Timespan.MONTH, adjusted=False)
    assert isinstance(bars, HistoryQuery)
    assert bars.history("2025-03-05", "2025-03-20", source=source).size == 0
    assert sent(fake_rest) == []


def test_a_long_grouped_daily_download_is_announced(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    for day in range(3, 11):
        fake_rest.reply(GROUPED_PATH.format(date=f"2025-03-{day:02d}"), Reply(envelope([])))
    with caplog.at_level(logging.INFO, LOGGER):
        history(
            session,
            BARS,
            start="2025-03-03",
            end="2025-03-10",
            symbols=dm.ALL,
            timespan=Timespan.DAY,
            adjusted=True,
        )
    assert len(sent(fake_rest)) == 8
    assert "stocks.bars history: 8 requests, one per day or session" in caplog.text
    assert "takes about 2 minutes" in caplog.text


def test_forex_day_bars_are_stamped_at_the_utc_days_start(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # Any stamp of a forex 1-day bar moves to 00:00 UTC of its day, unchecked.
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
    assert [(r["Timestamp"], r["Close"]) for r in out] == [
        (march_3 * 10**6, 1.0),
        (march_4 * 10**6, 2.0),
    ]
    # The grouped day of every pair, stamped anywhere in its day.
    grouped = FOREX_GROUPED_PATH.format(date="2025-03-03")
    new_york = ny_ms(date(2025, 3, 3))
    fake_rest.reply(grouped, Reply(envelope([{"T": "C:EURUSD", "t": new_york, "c": 3.0}])))
    every = history(
        session,
        FOREX_BARS,
        start="2025-03-03",
        end="2025-03-03",
        symbols=dm.ALL,
        timespan=Timespan.DAY,
        adjusted=True,
    )
    assert [(r["Symbol"], r["Timestamp"]) for r in every] == [("C:EURUSD", march_3 * 10**6)]


def test_a_history_by_instants_is_not_announced_however_many_symbols(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    # Only one-request-per-day downloads are announced; six symbols of one day are not.
    tickers = ["A", "B", "C", "D", "E", "F"]
    for ticker in tickers:
        fake_rest.reply(TRADES_PATH.format(stockTicker=ticker), Reply(envelope([])))
    with caplog.at_level(logging.INFO, LOGGER):
        history(session, TRADES, start="2025-03-03", end="2025-03-03", symbols=tickers)
    assert len(sent(fake_rest)) == 6
    assert "one per day or session" not in caplog.text
