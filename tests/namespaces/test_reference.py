"""Tests for the `m.reference` namespace and the record of the unified snapshot."""

import json
import logging
import re
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast
from urllib.parse import parse_qs, unquote, urlencode, urlsplit

import pyarrow as pa
import pytest
from deephaven import dtypes, input_table, new_table
from deephaven.column import string_col
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.schema import ColumnType, required_fields
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import ClassFromPrefix
from deephaven_massive._ingest import poll
from deephaven_massive._ingest.poll import Pollers, open_poll
from deephaven_massive._massive import RestPath, Transport, source, to_batch
from deephaven_massive._namespaces import reference
from deephaven_massive._namespaces.base import Namespace
from deephaven_massive._query import Query
from deephaven_massive._session import CATALOG
from tests._fakes.clock import FakeClock
from tests._fakes.rest import FakeRestServer, Reply, envelope

OWNER = "namespaces/reference"
R = Transport.REST
NAMES = [
    "tickers",
    "ticker_details",
    "ticker_types",
    "ticker_events",
    "related_companies",
    "news",
    "exchanges",
    "conditions",
    "market_status",
    "market_holidays",
    "snapshots",
]


def _types(spec: DatasetSpec) -> dict[str, ColumnType]:
    return {c.name: c.type for c in CATALOG.dataset_schema(spec).columns}


def test_namespace_is_reached_from_the_session(massive_session: dm.MassiveSession) -> None:
    namespace = massive_session.reference
    assert type(namespace) is reference._Reference
    assert isinstance(namespace, Namespace)


def test_datasets_are_declared_by_the_namespace_package() -> None:
    assert [spec.name for spec in reference.DATASETS] == [f"reference.{n}" for n in NAMES]
    assert all(CATALOG.by_name[spec.name] is spec for spec in reference.DATASETS)
    assert all(
        spec.asset_class is ClassFromPrefix.ANY and spec.kind is None for spec in reference.DATASETS
    )
    assert all(spec.sources == ((R, spec.fetch_path),) for spec in reference.DATASETS)


# --- Declarations and schemas ----------------------------------------------------------------


def test_tickers_are_instruments_filtered_one_ticker_a_request() -> None:
    spec = reference.TICKERS
    assert spec.fetch_path == "/v3/reference/tickers"
    assert spec.symbol_param == "ticker"
    assert not spec.per_symbol  # no symbols lists every ticker
    assert CATALOG.takes_symbols(spec)
    assert spec.page_size == 1000
    assert CATALOG.dataset_schema(spec).column_names[0] == "Symbol"
    types = _types(spec)
    assert "Ticker" not in types
    assert types["LastUpdatedUtc"] is ColumnType.INSTANT


def test_ticker_details_fan_out_per_symbol() -> None:
    spec = reference.TICKER_DETAILS
    assert spec.per_symbol
    assert spec.keys_for(R, "/v3/reference/tickers/{ticker}") == SourceKeys(KeySpec("Ticker"))
    types = _types(spec)
    assert CATALOG.dataset_schema(spec).column_names[0] == "Symbol"
    assert types["ListDate"] is ColumnType.STRING
    assert types["AddressCity"] is ColumnType.STRING


def test_ticker_events_take_the_symbol_from_the_request() -> None:
    spec = reference.TICKER_EVENTS
    assert spec.keys_for(R, "/vX/reference/tickers/{id}/events").symbol == KeySpec(
        "id", from_request=True
    )
    # The event's `Date` is the vendor's date, not a trading day: reference rows have none.
    assert CATALOG.dataset_schema(spec).column_names == (
        "Symbol",
        "Date",
        "Type",
        "TickerChangeTicker",
        "TimestampInsert",
    )
    assert spec.poll_keys == ("Symbol", "Date", "Type")


def test_related_companies_name_the_peer_and_the_company_asked_about() -> None:
    spec = reference.RELATED_COMPANIES
    keys = spec.keys_for(R, "/v1/related-companies/{ticker}")
    assert keys == SourceKeys(symbol=KeySpec("Ticker"), fanout=KeySpec("ticker", from_request=True))
    assert CATALOG.dataset_schema(spec).column_names == ("Symbol", "Underlying", "TimestampInsert")
    assert spec.poll_keys == ("Underlying", "Symbol")


def test_news_keeps_its_tickers_and_appends() -> None:
    spec = reference.NEWS
    assert spec.keys == {}
    assert spec.symbol_param == "ticker"
    assert not spec.per_symbol
    assert not CATALOG.takes_symbols(spec)
    assert spec.time_axis == ("published_utc.gte", "published_utc.lt")
    assert spec.poll_mode is dm.PollMode.APPEND
    types = _types(spec)
    assert "Symbol" not in types
    assert types["Tickers"] is ColumnType.STRING
    assert types["PublishedUtc"] is ColumnType.INSTANT


@pytest.mark.parametrize(
    ("spec", "poll_keys"),
    [
        (reference.TICKER_TYPES, ("AssetClass", "Locale", "Code")),
        (reference.EXCHANGES, ("AssetClass", "Id")),
        (reference.CONDITIONS, ("AssetClass", "Type", "Id")),
        (reference.MARKET_STATUS, ()),
        (reference.MARKET_HOLIDAYS, ("Exchange", "Date")),
    ],
)
def test_lookup_tables_describe_no_instrument(
    spec: DatasetSpec, poll_keys: tuple[str, ...]
) -> None:
    assert spec.keys == {}
    assert spec.symbol_param is None
    assert spec.poll_keys == poll_keys
    types = _types(spec)
    assert "Symbol" not in types
    assert set(poll_keys) <= set(types)


def test_market_status_flattens_its_maps() -> None:
    types = _types(reference.MARKET_STATUS)
    assert types["ServerTime"] is ColumnType.INSTANT
    assert types["ExchangesNyse"] is ColumnType.STRING
    assert types["CurrenciesFx"] is ColumnType.STRING
    assert types["IndicesGroupsSAndP"] is ColumnType.STRING


def test_market_holidays_hours_are_instants() -> None:
    types = _types(reference.MARKET_HOLIDAYS)
    assert (types["Open"], types["Close"]) == (ColumnType.INSTANT, ColumnType.INSTANT)
    assert types["Date"] is ColumnType.STRING


def test_the_unified_snapshot_record() -> None:
    spec = reference.SNAPSHOTS
    assert CATALOG.by_name["reference.snapshots"] is spec
    assert spec.fetch_path == "/v3/snapshot"
    assert spec.symbol_param == "ticker.any_of"
    assert spec.page_size == 250
    assert not spec.per_symbol
    schema = CATALOG.dataset_schema(spec)
    assert schema.column_names[0] == "Symbol"
    types = _types(spec)
    # An option's underlying stays in its own column: the other classes' rows have none.
    assert "Underlying" not in types
    assert types["UnderlyingAssetTicker"] is ColumnType.STRING
    assert types["LastTradeConditions"] is ColumnType.INT_ARRAY
    assert types["LastUpdated"] is ColumnType.INSTANT
    assert {"Error", "Message"} <= set(types)


@pytest.mark.parametrize("name", NAMES)
def test_the_summary_opens_the_docstring(name: str) -> None:
    owner = dm.MassiveSession if name == "snapshots" else reference._Reference
    doc = getattr(owner, name).__doc__
    assert doc.splitlines()[0] == CATALOG.by_name[f"reference.{name}"].summary


# --- Factories -------------------------------------------------------------------------------


def test_tickers_query(massive_session: dm.MassiveSession) -> None:
    ref: Any = massive_session.reference
    query = ref.tickers()
    assert type(query) is Query
    assert query._spec is reference.TICKERS
    assert query._request == Request(params=(("active", True),))
    full = ref.tickers(
        symbols=["MSFT", "AAPL"],
        market=dm.Market.STOCKS,
        ticker_type=dm.TickerType.CS,
        exchange="XNAS",
        cusip="037833100",
        cik="0000320193",
        date="2025-01-02",
        active=False,
        search="apple",
        sort="ticker",
        order=dm.Order.DESC,
        limit=5,
        filters={"ticker_gte": "A"},
    )
    assert full._request == Request(
        symbols=("AAPL", "MSFT"),
        params=(
            ("active", False),
            ("cik", "0000320193"),
            ("cusip", "037833100"),
            ("date", date(2025, 1, 2)),
            ("exchange", "XNAS"),
            ("market", dm.Market.STOCKS),
            ("order", dm.Order.DESC),
            ("search", "apple"),
            ("sort", "ticker"),
            ("type", dm.TickerType.CS),
        ),
        filters=(("ticker_gte", "A"),),
        limit=5,
    )


def test_ticker_details_and_events_queries(massive_session: dm.MassiveSession) -> None:
    ref: Any = massive_session.reference
    details = ref.ticker_details("AAPL", date=date(2024, 1, 2))
    assert details._spec is reference.TICKER_DETAILS
    assert details._request == Request(symbols=("AAPL",), params=(("date", date(2024, 1, 2)),))
    # The day is a calendar date, so an aware datetime is refused.
    early = datetime(2025, 1, 3, 2, tzinfo=UTC)
    with pytest.raises(ValueError, match="date is a calendar date; pass a date"):
        ref.ticker_details("AAPL", date=early)
    with pytest.raises(ValueError, match="date is a calendar date; pass a date"):
        ref.tickers(date=early)
    assert ref.ticker_details(["O:SPY251219C00600000"])._request == Request(
        symbols=("O:SPY251219C00600000",)
    )
    events = ref.ticker_events(["META", "AAPL"], types=["ticker_change", "other"])
    assert events._spec is reference.TICKER_EVENTS
    assert events._request == Request(
        symbols=("AAPL", "META"), params=(("types", "ticker_change,other"),)
    )
    assert ref.ticker_events("META", types="ticker_change")._request.params == (
        ("types", "ticker_change"),
    )
    assert ref.ticker_events("META")._request == Request(symbols=("META",))


def test_related_companies_query(massive_session: dm.MassiveSession) -> None:
    ref: Any = massive_session.reference
    query = ref.related_companies("AAPL")
    assert query._spec is reference.RELATED_COMPANIES
    assert query._request == Request(symbols=("AAPL",))


def test_a_symbol_table_is_boxed(massive_session: dm.MassiveSession) -> None:
    ref: Any = massive_session.reference
    table = new_table([string_col("Symbol", ["AAPL"])])
    query = ref.related_companies(table)
    assert query._request.symbols == SymbolTable(table)


def test_news_query(massive_session: dm.MassiveSession) -> None:
    ref: Any = massive_session.reference
    query = ref.news(
        symbols="AAPL",
        start=datetime(2025, 6, 1, tzinfo=UTC),
        end=datetime(2025, 6, 2, 12, tzinfo=UTC),
        sort="published_utc",
        order=dm.Order.ASC,
        limit=50,
    )
    assert query._spec is reference.NEWS
    assert query._request == Request(
        symbols=("AAPL",),
        start=datetime(2025, 6, 1, tzinfo=UTC),
        end=datetime(2025, 6, 2, 12, tzinfo=UTC),
        params=(("order", dm.Order.ASC), ("sort", "published_utc")),
        limit=50,
    )
    assert ref.news()._request == Request()
    assert ref.news(symbols=["AAPL"])._request == Request(symbols=("AAPL",))


def test_news_takes_a_watchlist(massive_session: dm.MassiveSession) -> None:
    # The endpoint's `ticker` parameter is single-valued and it has no `ticker.any_of`, so a
    # watchlist is one request per ticker.
    news = cast("RestPath", source(R, "/v2/reference/news"))
    assert "ticker.any_of" not in {p.name for p in news.params}
    assert reference.NEWS.symbols_overlap
    ref: Any = massive_session.reference
    assert ref.news(symbols=["MSFT", "AAPL"])._request == Request(symbols=("AAPL", "MSFT"))
    table = new_table([string_col("Symbol", ["AAPL"])])
    assert ref.news(symbols=table)._request == Request(symbols=SymbolTable(table))
    with pytest.raises(ValueError, match="no symbols"):
        ref.news(symbols=[])


def test_lookup_queries(massive_session: dm.MassiveSession) -> None:
    ref: Any = massive_session.reference
    types = ref.ticker_types(asset_class=dm.AssetClass.FOREX, locale=dm.Locale.GLOBAL)
    assert types._spec is reference.TICKER_TYPES
    assert types._request == Request(params=(("asset_class", "fx"), ("locale", dm.Locale.GLOBAL)))
    exchanges = ref.exchanges(asset_class=dm.AssetClass.FUTURES)
    assert exchanges._spec is reference.EXCHANGES
    assert exchanges._request == Request(params=(("asset_class", "futures"),))
    conditions = ref.conditions(
        asset_class=dm.AssetClass.OPTIONS,
        data_type=dm.ConditionDataType.TRADE,
        condition_id=2,
        sip=dm.Sip.OPRA,
        sort="id",
        order=dm.Order.ASC,
        limit=10,
    )
    assert conditions._spec is reference.CONDITIONS
    assert conditions._request == Request(
        params=(
            ("asset_class", "options"),
            ("data_type", dm.ConditionDataType.TRADE),
            ("id", 2),
            ("order", dm.Order.ASC),
            ("sip", dm.Sip.OPRA),
            ("sort", "id"),
        ),
        limit=10,
    )
    for query, spec in (
        (ref.ticker_types(), reference.TICKER_TYPES),
        (ref.exchanges(), reference.EXCHANGES),
        (ref.conditions(), reference.CONDITIONS),
        (ref.market_status(), reference.MARKET_STATUS),
        (ref.market_holidays(), reference.MARKET_HOLIDAYS),
    ):
        assert type(query) is Query
        assert query._spec is spec
        assert query._request == Request()
    assert repr(ref.market_status()) == "<Query reference.market_status>"


def test_each_endpoint_accepts_its_own_asset_classes(
    massive_session: dm.MassiveSession,
) -> None:
    ref = massive_session.reference
    rejected: list[tuple[Callable[..., Query], str, dm.AssetClass, list[str]]] = [
        (ref.ticker_types, "ticker_types", dm.AssetClass.FUTURES, ["STOCKS", "OPTIONS", "INDICES"]),
        (ref.exchanges, "exchanges", dm.AssetClass.INDICES, ["STOCKS", "OPTIONS", "FUTURES"]),
        (ref.conditions, "conditions", dm.AssetClass.FUTURES, ["STOCKS", "OPTIONS"]),
        (ref.conditions, "conditions", dm.AssetClass.INDICES, ["STOCKS", "OPTIONS"]),
    ]
    for factory, name, asset_class, accepted in rejected:
        message = (
            f"reference.{name} does not accept AssetClass.{asset_class.name}; "
            f"it accepts {[*accepted, 'FOREX', 'CRYPTO']}"
        )
        with pytest.raises(ValueError, match=re.escape(message)):
            factory(asset_class=asset_class)


def test_dm_all_names_the_alternative(massive_session: dm.MassiveSession) -> None:
    ref: Any = massive_session.reference
    alternatives = {
        "tickers": "symbols=None for every ticker",
        "news": "symbols=None for every article",
    }
    for method, alternative in alternatives.items():
        with pytest.raises(
            ValueError, match=re.escape(f"does not accept dm.ALL; use {alternative}")
        ):
            getattr(ref, method)(symbols=dm.ALL)
    with pytest.raises(ValueError, match=r"use m\.reference\.tickers\(\) for every ticker"):
        ref.ticker_details(dm.ALL)
    for method in ("ticker_events", "related_companies"):
        with pytest.raises(ValueError, match=r"does not accept dm\.ALL; use a list of symbols"):
            getattr(ref, method)(dm.ALL)


def test_arguments_are_validated_at_construction(massive_session: dm.MassiveSession) -> None:
    ref: Any = massive_session.reference
    for name in ("exchange", "cusip", "cik", "search"):
        with pytest.raises(TypeError, match=f"{name} must be a str, not int"):
            ref.tickers(**{name: 1})
    with pytest.raises(TypeError, match="active must be a bool, not NoneType"):
        ref.tickers(active=None)
    with pytest.raises(ValueError, match="needs a timezone"):
        ref.tickers(date="2025-01-02T09:30")
    with pytest.raises(ValueError, match="is not an ISO-8601 date"):
        ref.ticker_details("AAPL", date="soon")
    with pytest.raises(ValueError, match="types is empty"):
        ref.ticker_events("AAPL", types=[])
    with pytest.raises(TypeError, match="types must hold only str"):
        ref.ticker_events("AAPL", types=["ticker_change", 1])
    with pytest.raises(TypeError, match="types must be a sequence of str, not int"):
        ref.ticker_events("AAPL", types=1)
    for bad in (True, "2"):
        with pytest.raises(TypeError, match="condition_id must be an int"):
            ref.conditions(condition_id=bad)
    # A reserved wire name names the factory's argument, which may be spelled differently.
    with pytest.raises(ValueError, match=r"^type is set through its own argument, ticker_type="):
        ref.tickers(filters={"type": "CS"})
    with pytest.raises(ValueError, match=r"^id_gte is set through its own argument, condition_id="):
        ref.conditions(filters={"id_gte": 1})
    with pytest.raises(ValueError, match="published_utc_gte is set through start= and end="):
        ref.news(filters={"published_utc_gte": "2025-01-01"})
    with pytest.raises(ValueError, match="end"):
        ref.news(start="2025-02-01", end="2025-01-01")
    with pytest.raises(ValueError, match=r"reference\.conditions has no filter 'nope'"):
        ref.conditions(filters={"nope": 1})


def test_enums_reject_text(massive_session: dm.MassiveSession) -> None:
    ref: Any = massive_session.reference
    calls: list[tuple[Callable[..., Query], str, str]] = [
        (ref.tickers, "market", "Market"),
        (ref.tickers, "ticker_type", "TickerType"),
        (ref.tickers, "order", "Order"),
        (ref.ticker_types, "asset_class", "AssetClass"),
        (ref.ticker_types, "locale", "Locale"),
        (ref.news, "order", "Order"),
        (ref.exchanges, "asset_class", "AssetClass"),
        (ref.exchanges, "locale", "Locale"),
        (ref.conditions, "asset_class", "AssetClass"),
        (ref.conditions, "data_type", "ConditionDataType"),
        (ref.conditions, "sip", "Sip"),
        (ref.conditions, "order", "Order"),
    ]
    for factory, parameter, enum in calls:
        with pytest.raises(TypeError, match=rf"{parameter} must be dm\.{enum}"):
            factory(**{parameter: "stocks"})


# --- Vendor samples --------------------------------------------------------------------------


def _rows(payload: object, result_key: str | None) -> list[dict[str, object]]:
    # The rows of a response body, found as the REST client finds them.
    found = payload
    for part in (result_key or "").split(".") if result_key else ():
        found = cast("dict[str, object]", found)[part]
    return cast("list[dict[str, object]]", found if isinstance(found, list) else [found])


@dataclass(frozen=True)
class Sample:
    # A vendor sample: the dataset, its fixture, values of its first row, and the drift found.
    spec: DatasetSpec
    fixture: str
    expected: dict[str, object]
    extra: tuple[str, ...] = ()
    missing: tuple[str, ...] = ()


SAMPLES = [
    Sample(reference.TICKERS, "tickers", {"Ticker": "A", "Type": "CS"}),
    Sample(
        reference.TICKER_DETAILS,
        "ticker_details",
        {"Ticker": "AAPL", "AddressCity": "Cupertino", "ListDate": date(1980, 12, 12)},
    ),
    Sample(reference.TICKER_TYPES, "ticker_types", {"Code": "CS", "AssetClass": "stocks"}),
    Sample(
        reference.TICKER_EVENTS,
        "ticker_events",
        {"Date": date(2022, 6, 9), "TickerChangeTicker": "META", "Type": "ticker_change"},
    ),
    Sample(reference.RELATED_COMPANIES, "related_companies", {"Ticker": "MSFT"}),
    Sample(reference.NEWS, "news", {"Tickers": '["UBS"]', "PublisherName": "Investing.com"}),
    Sample(reference.EXCHANGES, "exchanges", {"Id": 1, "Mic": "XASE"}),
    Sample(
        reference.CONDITIONS,
        "conditions",
        {"Id": 2, "SipMappingCta": "B", "UpdateRulesConsolidatedUpdatesVolume": True},
    ),
    Sample(
        reference.MARKET_STATUS,
        "market_status",
        {"Market": "extended-hours", "ExchangesNyse": "extended-hours", "AfterHours": True},
    ),
    Sample(
        reference.MARKET_HOLIDAYS,
        "market_holidays",
        {"Exchange": "NYSE", "Date": date(2020, 11, 26), "Status": "closed"},
    ),
    # The vendor's sample sends `details.underlying_ticker`, which its schema lacks.
    Sample(
        reference.SNAPSHOTS,
        "snapshots",
        {
            "Ticker": "O:NCLH221014C00005000",
            "UnderlyingAssetTicker": "AAPL",
            "GreeksDelta": 0.5520187372272933,
            "LastTradeConditions": [209],
        },
        extra=("details.underlying_ticker",),
    ),
]


@pytest.mark.parametrize("sample", SAMPLES, ids=[s.fixture for s in SAMPLES])
def test_the_vendor_samples_decode(
    load_fixture: Callable[[str, str], bytes], sample: Sample
) -> None:
    spec = sample.spec
    rest_path = cast("RestPath", source(R, cast("str", spec.fetch_path)))
    rows = _rows(json.loads(load_fixture(OWNER, f"{sample.fixture}.json")), rest_path.result_key)
    required = [f.column for f in required_fields(spec, rest_path)]
    batch, report = to_batch(rows, rest_path, required=required)
    assert tuple(sorted(report.extra)) == sample.extra
    assert tuple(sorted(report.missing)) == sample.missing
    first = batch.to_pylist()[0]
    assert {column: first[column] for column in sample.expected} == sample.expected


# --- End to end through the fake REST server -------------------------------------------------


@pytest.fixture
def rest_session(fake_rest: FakeRestServer) -> Iterator[dm.MassiveSession]:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    yield m
    m.close()


Fixture = Callable[[str, str], bytes]
ToRows = Callable[[Table], list[dict[str, Any]]]


def sent(server: FakeRestServer) -> list[tuple[str, dict[str, list[str]]]]:
    # Every GET the server received: its decoded path and query.
    return [
        (unquote(urlsplit(r.target).path), parse_qs(urlsplit(r.target).query))
        for r in server.requests
    ]


def sample(load_fixture: Fixture, name: str) -> Reply:
    # The vendor sample as the reply body, without its `next_url`, which names Massive's host.
    body = json.loads(load_fixture(OWNER, f"{name}.json"))
    if isinstance(body, dict):
        body.pop("next_url", None)
    return Reply(body)


def test_tickers_fetch(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    dh_to_pylist: ToRows,
) -> None:
    fake_rest.reply("/v3/reference/tickers", sample(load_fixture, "tickers"))
    out = dh_to_pylist(
        rest_session.reference.tickers(
            symbols="A", market=dm.Market.STOCKS, ticker_type=dm.TickerType.CS, date="2021-04-25"
        ).fetch()
    )
    assert sent(fake_rest) == [
        (
            "/v3/reference/tickers",
            {
                "ticker": ["A"],
                "market": ["stocks"],
                "type": ["CS"],
                "date": ["2021-04-25"],
                "active": ["true"],
                "limit": ["1000"],
            },
        )
    ]
    assert len(out) == 1
    row = out[0]
    assert row["Symbol"] == "A"
    assert row["Name"] == "Agilent Technologies Inc."
    assert row["LastUpdatedUtc"] == datetime(2021, 4, 25, tzinfo=UTC)
    assert row["DelistedUtc"] is None


def test_tickers_without_symbols_list_every_ticker_and_symbols_fan_out(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v3/reference/tickers", Reply(envelope([])))
    rest_session.reference.tickers().fetch()
    assert sent(fake_rest) == [("/v3/reference/tickers", {"active": ["true"], "limit": ["1000"]})]
    rest_session.reference.tickers(symbols=["MSFT", "AAPL"]).fetch()
    # The per-symbol requests run concurrently, so they arrive in any order.
    fanned = sorted(params["ticker"] for _, params in sent(fake_rest)[1:])
    assert fanned == [["AAPL"], ["MSFT"]]
    assert all(params["limit"] == ["1000"] for _, params in sent(fake_rest))


def test_ticker_details_fetch_one_request_per_symbol(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    dh_to_pylist: ToRows,
) -> None:
    fake_rest.reply("/v3/reference/tickers/AAPL", sample(load_fixture, "ticker_details"))
    out = dh_to_pylist(rest_session.reference.ticker_details("AAPL", date="2024-01-02").fetch())
    assert sent(fake_rest) == [("/v3/reference/tickers/AAPL", {"date": ["2024-01-02"]})]
    assert [(r["Symbol"], r["AddressCity"], r["ListDate"]) for r in out] == [
        ("AAPL", "Cupertino", "1980-12-12")
    ]


def test_ticker_events_fill_the_symbol_from_the_request(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    dh_to_pylist: ToRows,
) -> None:
    fake_rest.reply("/vX/reference/tickers/META/events", sample(load_fixture, "ticker_events"))
    out = dh_to_pylist(rest_session.reference.ticker_events("META", types="ticker_change").fetch())
    assert sent(fake_rest) == [("/vX/reference/tickers/META/events", {"types": ["ticker_change"]})]
    assert [(r["Symbol"], r["Date"], r["Type"], r["TickerChangeTicker"]) for r in out] == [
        ("META", "2022-06-09", "ticker_change", "META"),
        ("META", "2012-05-18", "ticker_change", "FB"),
    ]


def test_related_companies_name_the_peer_and_the_ticker_asked_about(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    dh_to_pylist: ToRows,
) -> None:
    fake_rest.reply("/v1/related-companies/AAPL", sample(load_fixture, "related_companies"))
    out = dh_to_pylist(rest_session.reference.related_companies("AAPL").fetch())
    assert sent(fake_rest) == [("/v1/related-companies/AAPL", {})]
    assert len(out) == 10
    assert out[0]["TimestampInsert"] is not None
    assert {k: v for k, v in out[0].items() if k != "TimestampInsert"} == {
        "Symbol": "MSFT",
        "Underlying": "AAPL",
    }


def test_news_fetch(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    dh_to_pylist: ToRows,
) -> None:
    fake_rest.reply("/v2/reference/news", sample(load_fixture, "news"))
    query = rest_session.reference.news(
        symbols="UBS", start=date(2024, 6, 24), end=date(2024, 6, 24), order=dm.Order.DESC
    )
    out = dh_to_pylist(query.fetch())
    ((path, params),) = sent(fake_rest)
    assert path == "/v2/reference/news"
    assert params["ticker"] == ["UBS"]
    assert params["order"] == ["desc"]
    assert params["limit"] == ["1000"]
    # `published_utc` is an instant: a date bound is its New York midnight, end exclusive.
    assert params["published_utc.gte"] == ["2024-06-24T04:00:00Z"]
    assert params["published_utc.lt"] == ["2024-06-25T04:00:00Z"]
    assert "Symbol" not in out[0]
    assert out[0]["Tickers"] == '["UBS"]'
    assert out[0]["PublishedUtc"] == datetime(2024, 6, 24, 18, 33, 53, tzinfo=UTC)


def _article(article_id: str, published: str, *tickers: str) -> dict[str, object]:
    return {
        "id": article_id,
        "published_utc": published,
        "tickers": list(tickers),
        "title": f"Article {article_id}",
        "article_url": f"https://example.com/{article_id}",
        "publisher": {"name": "Example"},
    }


_NEWS = "/v2/reference/news"


def _news_target(ticker: str, **params: object) -> str:
    # The path and query the client sends: its parameters sorted by name, percent-encoded.
    query = {
        "limit": 1000,
        "ticker": ticker,
        **{k.replace("_gte", ".gte"): v for k, v in params.items()},
    }
    return f"{_NEWS}?{urlencode(sorted(query.items()))}"


def test_news_of_a_watchlist_reads_each_ticker_and_keeps_an_article_once(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: ToRows,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="deephaven_massive._ingest.rest")
    shared = _article("both", "2024-06-24T18:00:00Z", "AAPL", "MSFT")
    # An article without an id cannot be told from another, so neither copy is dropped.
    unnamed = {
        k: v for k, v in _article("", "2024-06-24T15:00:00Z", "AAPL", "MSFT").items() if k != "id"
    }
    fake_rest.reply(
        _news_target("AAPL", limit=3),
        Reply(envelope([shared, _article("a1", "2024-06-24T17:00:00Z", "AAPL"), unnamed])),
    )
    fake_rest.reply(
        _news_target("MSFT", limit=3),
        Reply(envelope([shared, _article("m1", "2024-06-24T16:00:00Z", "MSFT"), unnamed])),
    )
    out = dh_to_pylist(rest_session.reference.news(symbols=["MSFT", "AAPL"], limit=3).fetch())
    assert sorted(r.target for r in fake_rest.requests) == [
        _news_target("AAPL", limit=3),
        _news_target("MSFT", limit=3),
    ]
    # Ticker by ticker, in ticker order; the shared article once, where AAPL's request had it,
    # and `limit` caps each ticker's articles, not the table.
    assert [row["Id"] for row in out] == ["both", "a1", None, "m1", None]
    assert (
        "reference.news: kept 5 of 6 rows from 2 requests, each row once by Id; limit=3 per "
        "request" in caplog.messages
    )


def test_news_polls_follow_a_live_watchlist(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: ToRows,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock(manual=True)
    monkeypatch.setattr(poll, "CLOCK", clock)
    shared = _article("both", "2024-06-24T18:00:00Z", "AAPL", "MSFT")
    since = "2024-06-24T04:00:00Z"
    fake_rest.reply(_news_target("AAPL", published_utc_gte=since), Reply(envelope([shared])))
    fake_rest.reply(
        _news_target("MSFT", published_utc_gte=since),
        Reply(envelope([shared, _article("m1", "2024-06-24T16:00:00Z", "MSFT")])),
    )
    watch = input_table({"Symbol": dtypes.string}, key_cols="Symbol")
    watch.add(new_table([string_col("Symbol", ["AAPL"])]))
    table = rest_session.reference.news(symbols=watch, start="2024-06-24").poll(60)
    _until(lambda: [r["Id"] for r in dh_to_pylist(table)] == ["both"])
    clock.wait_for_sleepers(1)
    watch.add(new_table([string_col("Symbol", ["MSFT"])]))
    clock.advance(60)
    # The next cycle reads the added ticker too; the article both tickers return stays one row.
    _until(lambda: sorted(r["Id"] for r in dh_to_pylist(table)) == ["both", "m1"])
    rest_session.release(table)


def test_lookup_fetches(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    dh_to_pylist: ToRows,
) -> None:
    ref = rest_session.reference
    fake_rest.reply("/v3/reference/tickers/types", sample(load_fixture, "ticker_types"))
    fake_rest.reply("/v3/reference/exchanges", sample(load_fixture, "exchanges"))
    fake_rest.reply("/v3/reference/conditions", sample(load_fixture, "conditions"))
    fake_rest.reply("/v1/marketstatus/now", sample(load_fixture, "market_status"))
    fake_rest.reply("/v1/marketstatus/upcoming", sample(load_fixture, "market_holidays"))
    types = dh_to_pylist(ref.ticker_types(asset_class=dm.AssetClass.STOCKS).fetch())
    exchanges = dh_to_pylist(ref.exchanges(locale=dm.Locale.US).fetch())
    conditions = dh_to_pylist(ref.conditions(condition_id=2, sip=dm.Sip.CTA).fetch())
    status = dh_to_pylist(ref.market_status().fetch())
    holidays = dh_to_pylist(ref.market_holidays().fetch())
    assert sent(fake_rest) == [
        ("/v3/reference/tickers/types", {"asset_class": ["stocks"]}),
        ("/v3/reference/exchanges", {"locale": ["us"]}),
        ("/v3/reference/conditions", {"id": ["2"], "sip": ["CTA"], "limit": ["1000"]}),
        ("/v1/marketstatus/now", {}),
        ("/v1/marketstatus/upcoming", {}),
    ]
    assert [r["Code"] for r in types] == ["CS", "ETF"]
    assert [(r["Id"], r["Acronym"]) for r in exchanges] == [(1, "AMEX"), (62, None)]
    assert conditions[0]["DataTypes"] == '["trade"]'
    assert conditions[0]["UpdateRulesMarketCenterUpdatesVolume"] is True
    assert len(status) == 1
    assert status[0]["ServerTime"] == datetime(2020, 11, 10, 22, 37, 37, tzinfo=UTC)
    assert status[0]["ExchangesOtc"] == "closed"
    assert len(holidays) == 5
    assert holidays[3]["Status"] == "early-close"
    assert holidays[3]["Open"] == datetime(2020, 11, 27, 14, 30, tzinfo=UTC)
    assert holidays[3]["Close"] == datetime(2020, 11, 27, 18, tzinfo=UTC)
    assert holidays[0]["Open"] is None  # a full holiday has no hours


def test_unified_snapshots_fetch(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    dh_to_pylist: ToRows,
) -> None:
    fake_rest.reply("/v3/snapshot", sample(load_fixture, "snapshots"))
    symbols = ["O:NCLH221014C00005000", "AAPL", "TSLAAPL"]
    out = dh_to_pylist(rest_session.snapshots(symbols).fetch())
    assert sent(fake_rest) == [
        (
            "/v3/snapshot",
            {"ticker.any_of": ["AAPL,O:NCLH221014C00005000,TSLAAPL"], "limit": ["250"]},
        )
    ]
    by_symbol = {r["Symbol"]: r for r in out}
    assert set(by_symbol) == set(symbols)
    option = by_symbol["O:NCLH221014C00005000"]
    assert option["UnderlyingAssetTicker"] == "AAPL"
    assert option["LastTradeConditions"] == [209]
    assert option["LastTradeSipTimestamp"].value == 1675280958783136800  # every nanosecond
    # A ticker Massive does not know keeps its row, with the error.
    assert by_symbol["TSLAAPL"]["Error"] == "NOT_FOUND"


def test_unified_snapshots_send_250_tickers_a_request(
    rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v3/snapshot", Reply(envelope([])))
    symbols = [f"T{i:03d}" for i in range(251)]
    rest_session.snapshots(symbols).fetch()
    batches = [q["ticker.any_of"][0].split(",") for _, q in sent(fake_rest)]
    assert sorted(len(batch) for batch in batches) == [1, 250]
    assert sorted(s for batch in batches for s in batch) == symbols


# --- Polling the market status ------------------------------------------------------------


class _Statuses:
    # A cycle reader that answers each cycle with the next scripted market status row.
    def __init__(self, *markets: str) -> None:
        schema = CATALOG.dataset_schema(reference.MARKET_STATUS).arrow_schema()
        self.tables = [
            pa.Table.from_pylist(
                [{"Market": market, "ServerTime": datetime(2025, 1, 2, 15, i, tzinfo=UTC)}],
                schema=schema,
            )
            for i, market in enumerate(markets)
        ]

    def __call__(
        self,
        *,
        symbols: object,
        record: Callable[[object], None],
        executor: ThreadPoolExecutor,
        cancel: object,
    ) -> tuple[pa.Table, int]:
        return self.tables.pop(0) if len(self.tables) > 1 else self.tables[0], 1


def _until(condition: Callable[[], bool]) -> None:
    # Waits for the update graph to apply a published cycle; fails after 30 s.
    event = threading.Event()
    for _ in range(3000):
        if condition():
            return
        event.wait(0.01)
    assert condition()


@pytest.mark.parametrize("mode", [dm.PollMode.REPLACE, dm.PollMode.UPSERT])
def test_market_status_polls_one_row_in_every_keyed_mode(
    massive_session: dm.MassiveSession, dh_to_pylist: ToRows, mode: dm.PollMode
) -> None:
    # No poll key: the table is the latest status, never one row per `ServerTime`.
    assert reference.MARKET_STATUS.poll_keys == ()
    # An explicit APPEND poll appends one row per new server time instead.
    assert reference.MARKET_STATUS.revision_keys == ("ServerTime",)
    clock = FakeClock(manual=True)
    pollers = Pollers(massive_session._core.label)
    table = open_poll(
        massive_session._core,
        spec=reference.MARKET_STATUS,
        request=Request(),
        interval=timedelta(seconds=60),
        mode=mode,
        read=cast("Any", _Statuses("open", "closed")),
        pollers=pollers,
        host="api.massive.com",
        path="/v1/marketstatus/now",
        clock=clock,
    )
    _until(lambda: [r["Market"] for r in dh_to_pylist(table)] == ["open"])
    clock.wait_for_sleepers(1)
    clock.advance(60)
    _until(lambda: [r["Market"] for r in dh_to_pylist(table)] == ["closed"])
    massive_session.release(table)


def test_the_current_state_datasets_revise_on_their_update_times() -> None:
    # An explicit APPEND poll appends each new state of a ticker, or each new market status.
    revised = {spec.name: spec.revision_keys for spec in reference.DATASETS if spec.revision_keys}
    assert revised == {
        "reference.market_status": ("ServerTime",),
        "reference.snapshots": ("LastUpdated", "LastQuoteLastUpdated", "LastTradeLastUpdated"),
    }
    for name, keys in revised.items():
        assert set(keys) <= set(CATALOG.dataset_schema(CATALOG.by_name[name]).column_names)
