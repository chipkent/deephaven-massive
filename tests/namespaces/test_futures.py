"""Tests for the `m.futures` namespace: its dataset specs, factory methods, and schemas."""

import csv
import inspect
import io
import json
import re
from collections.abc import Callable, Iterator
from datetime import date
from typing import Any, cast

import pytest

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.request import Request
from deephaven_massive._massive import Transport, source
from deephaven_massive._namespaces import futures
from deephaven_massive._namespaces.base import Namespace, query_class
from deephaven_massive._query import HistoryQuery, Query, QueryBase, StreamHistoryQuery
from deephaven_massive._session import CATALOG

OWNER = "namespaces/futures"
DOCS = "https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.md"

Loader = Callable[[str, str], bytes]


def request(query: QueryBase) -> Request:
    return query._request


def test_namespace_is_reached_from_the_session(massive_session: dm.MassiveSession) -> None:
    namespace = massive_session.futures
    assert type(namespace) is futures._Futures
    assert isinstance(namespace, Namespace)


def test_every_dataset_is_declared_in_method_order() -> None:
    assert [spec.name for spec in futures.DATASETS] == [
        "futures.trades",
        "futures.quotes",
        "futures.bars",
        "futures.snapshots",
        "futures.contracts",
        "futures.products",
        "futures.exchanges",
        "futures.market_status",
        "futures.schedules",
    ]
    for spec in futures.DATASETS:
        assert CATALOG.by_name[spec.name] is spec
        assert spec.asset_class is dm.AssetClass.FUTURES


def test_the_futures_datasets_pass_every_catalog_gate() -> None:
    names = {spec.name for spec in futures.DATASETS}
    assert [p for p in CATALOG.problems() if any(f": {n}:" in p for n in names)] == []


def test_streams_use_the_futures_cluster_and_venue_templated_files() -> None:
    sources = {key for spec in futures.DATASETS for _, key in spec.sources}
    assert {"futures/T", "futures/Q", "futures/A", "futures/AM"} <= sources
    prefixes = [p for spec in futures.DATASETS for p in spec.flat_file_prefixes.values()]
    assert prefixes
    assert all(p.startswith("us_futures_{venue}/") for p in prefixes)


# Factories and the query each one builds.


def test_trades_and_quotes_stream_and_have_history(massive_session: dm.MassiveSession) -> None:
    for factory in (massive_session.futures.trades, massive_session.futures.quotes):
        q = factory(["NQZ5", "ESZ5", "ESZ5"])
        assert type(q) is StreamHistoryQuery
        assert request(q).symbols == ("ESZ5", "NQZ5")
        assert request(q).venue is None
        cme = factory("ESZ5", venue=dm.FuturesVenue.CME)
        assert request(cme).venue is dm.FuturesVenue.CME
        assert request(factory(dm.ALL)).symbols is dm.ALL


_PLAIN = cast("Any", "text")


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (lambda f: f.trades("ESZ5", venue=_PLAIN), "venue must be dm.FuturesVenue"),
        (lambda f: f.quotes("ESZ5", venue=_PLAIN), "venue must be dm.FuturesVenue"),
        (lambda f: f.bars("ESZ5", venue=_PLAIN), "venue must be dm.FuturesVenue"),
        (lambda f: f.bars("ESZ5", _PLAIN), "timespan must be dm.Timespan"),
        (lambda f: f.snapshots("ESZ5", order=_PLAIN), "order must be dm.Order"),
        (lambda f: f.contracts(order=_PLAIN), "order must be dm.Order"),
        (lambda f: f.products(order=_PLAIN), "order must be dm.Order"),
        (lambda f: f.schedules(order=_PLAIN), "order must be dm.Order"),
    ],
    ids=[
        "trades.venue",
        "quotes.venue",
        "bars.venue",
        "bars.timespan",
        "snapshots.order",
        "contracts.order",
        "products.order",
        "schedules.order",
    ],
)
def test_enum_parameters_reject_plain_strings(
    massive_session: dm.MassiveSession, call: Callable[[Any], object], message: str
) -> None:
    with pytest.raises(TypeError, match=re.escape(message)):
        call(massive_session.futures)


def test_an_exhausted_generator_is_no_symbols(massive_session: dm.MassiveSession) -> None:
    symbols: Iterator[str] = iter(["ESZ5"])
    list(symbols)
    with pytest.raises(ValueError, match="no symbols"):
        massive_session.futures.quotes(cast("Any", symbols))


@pytest.mark.parametrize(
    ("bar", "expected"),
    [
        ((dm.Timespan.SECOND, 1), StreamHistoryQuery),
        ((dm.Timespan.MINUTE, 1), StreamHistoryQuery),
        ((dm.Timespan.MINUTE, 5), HistoryQuery),
        ((dm.Timespan.HOUR, 1), HistoryQuery),
        ((dm.Timespan.SESSION, 1), HistoryQuery),
        ((dm.Timespan.WEEK, 2), HistoryQuery),
    ],
)
def test_bars_pick_their_class_from_the_timespan(
    massive_session: dm.MassiveSession, bar: tuple[Any, int], expected: type
) -> None:
    timespan, multiplier = bar
    q = massive_session.futures.bars("ESZ5", timespan, multiplier=multiplier)
    assert type(q) is expected
    assert request(q).timespan is timespan
    assert request(q).multiplier == multiplier


def test_bars_default_to_streamed_minutes(massive_session: dm.MassiveSession) -> None:
    q = massive_session.futures.bars(dm.ALL, venue=dm.FuturesVenue.NYMEX)
    assert type(q) is StreamHistoryQuery
    assert request(q).timespan is dm.Timespan.MINUTE
    assert request(q).venue is dm.FuturesVenue.NYMEX


def test_bars_reject_the_day_timespan(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=r"no DAY timespan; use dm\.Timespan\.SESSION"):
        massive_session.futures.bars("ESZ5", cast("Any", dm.Timespan.DAY))
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan"):
        massive_session.futures.bars("ESZ5", cast("Any", "minute"))
    with pytest.raises(ValueError, match="multiplier must be positive"):
        massive_session.futures.bars("ESZ5", dm.Timespan.HOUR, multiplier=0)


def test_snapshots_take_symbols_or_all(massive_session: dm.MassiveSession) -> None:
    q = massive_session.futures.snapshots(
        ["ESZ5"], sort="ticker", order=dm.Order.DESC, limit=10, filters={"ticker_gte": "E"}
    )
    assert type(q) is Query
    assert request(q).symbols == ("ESZ5",)
    assert request(q).params == (("order", dm.Order.DESC), ("sort", "ticker"))
    assert request(q).filters == (("ticker_gte", "E"),)
    assert request(q).limit == 10
    every_es = massive_session.futures.snapshots(dm.ALL, product_code="ES")
    assert request(every_es).symbols is dm.ALL
    assert request(every_es).params == (("product_code", "ES"),)


def test_snapshot_symbols_are_set_only_through_symbols(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match="has no filter 'ticker_any_of'"):
        massive_session.futures.snapshots("ESZ5", filters={"ticker_any_of": ["NQZ5"]})


def test_contracts(massive_session: dm.MassiveSession) -> None:
    q = massive_session.futures.contracts(
        product_code="ES", filters={"active": True, "last_trade_date_gte": date(2026, 1, 1)}
    )
    assert type(q) is Query
    assert request(q).symbols is None
    assert request(q).params == (("product_code", "ES"),)
    assert dict(request(q).filters) == {"active": True, "last_trade_date_gte": date(2026, 1, 1)}
    with pytest.raises(ValueError, match="filter 'active' takes a boolean value"):
        massive_session.futures.contracts(filters={"active": "yes"})
    with pytest.raises(TypeError, match="sort must be a str"):
        massive_session.futures.contracts(sort=cast("str", 1))


def test_products(massive_session: dm.MassiveSession) -> None:
    q = massive_session.futures.products(
        product_code="GC", sort="name", limit=5, filters={"sector": "precious"}
    )
    assert type(q) is Query
    assert request(q).params == (("product_code", "GC"), ("sort", "name"))
    assert request(q).limit == 5
    with pytest.raises(ValueError, match="filter 'sector' takes"):
        massive_session.futures.products(filters={"sector": "nonsense"})
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        massive_session.futures.products(order=cast("dm.Order", "asc"))


def test_exchanges(massive_session: dm.MassiveSession) -> None:
    q = massive_session.futures.exchanges()
    assert type(q) is Query
    assert request(q) == type(request(q))()
    assert repr(q) == "<Query futures.exchanges>"


@pytest.mark.parametrize("name", ["exchanges", "market_status"])
def test_a_path_without_sort_offers_no_sort_or_order(name: str) -> None:
    parameters = inspect.signature(getattr(futures._Futures, name)).parameters
    assert not {"sort", "order"} & set(parameters)


def test_market_status(massive_session: dm.MassiveSession) -> None:
    q = massive_session.futures.market_status(
        product_code="ES", limit=3, filters={"product_code_gte": "C"}
    )
    assert type(q) is Query
    assert request(q).params == (("product_code", "ES"),)
    assert request(q).filters == (("product_code_gte", "C"),)
    assert request(q).limit == 3
    with pytest.raises(ValueError, match="has no filter 'ticker'"):
        massive_session.futures.market_status(filters={"ticker": "ESZ5"})


def test_schedules_select_sessions(massive_session: dm.MassiveSession) -> None:
    q = massive_session.futures.schedules(
        "2025-12-22", date(2025, 12, 26), product_code="ES", filters={"trading_venue": "XCME"}
    )
    assert type(q) is Query
    assert request(q).params == (("product_code", "ES"),)
    assert request(q).start == date(2025, 12, 22)
    assert request(q).end == date(2025, 12, 26)
    assert request(massive_session.futures.schedules()).start is None
    with pytest.raises(ValueError, match="must be before end"):
        massive_session.futures.schedules("2025-12-26", "2025-12-22")
    for key in ("session_end_date", "session_end_date_gt"):
        with pytest.raises(ValueError, match=rf"^{key} is set through start= and end="):
            massive_session.futures.schedules(filters={key: "2025-12-22"})


# Specs: the transports, keys, and schemas they declare.


def _spec(name: str) -> DatasetSpec:
    return CATALOG.by_name[f"futures.{name}"]


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("trades", StreamHistoryQuery),
        ("quotes", StreamHistoryQuery),
        ("bars", HistoryQuery),
        ("snapshots", Query),
        ("contracts", Query),
        ("products", Query),
        ("exchanges", Query),
        ("market_status", Query),
        ("schedules", Query),
    ],
)
def test_query_classes(name: str, expected: type) -> None:
    assert query_class(_spec(name)) is expected


def test_no_futures_dataset_has_a_last_endpoint() -> None:
    streamed = [spec for spec in futures.DATASETS if spec.has_channels]
    assert streamed
    assert not any(spec.has_fetch for spec in streamed)


@pytest.mark.parametrize("name", ["trades", "quotes", "bars"])
def test_market_data_starts_with_symbol_and_timestamp(name: str) -> None:
    schema = CATALOG.dataset_schema(_spec(name))
    assert schema.column_names[:2] == ("Symbol", "Timestamp")
    types = {column.name: column.type.value for column in schema.columns}
    assert types["Symbol"] == "String"
    assert types["Timestamp"] == "Instant"
    assert types["SessionEndDate"] == "LocalDate"
    assert "Ticker" not in types
    assert "Underlying" not in types


def test_bar_times_come_from_the_window_start() -> None:
    names = CATALOG.dataset_schema(_spec("bars")).column_names
    assert "WindowStart" not in names
    assert "SettlementPrice" in names


def test_snapshots_name_the_contract_symbol_and_have_no_single_time() -> None:
    schema = CATALOG.dataset_schema(_spec("snapshots"))
    assert schema.column_names[0] == "Symbol"
    assert "Timestamp" not in schema.column_names
    assert "DetailsTicker" not in schema.column_names
    assert {"LastTradePrice", "LastQuoteBid", "SessionSettlementPrice"} <= set(schema.column_names)


@pytest.mark.parametrize(
    "name", ["contracts", "products", "exchanges", "market_status", "schedules"]
)
def test_reference_datasets_take_no_symbols(name: str) -> None:
    spec = _spec(name)
    assert not CATALOG.takes_symbols(spec)
    assert spec.keys == {}
    schema = CATALOG.dataset_schema(spec)
    assert "Symbol" not in schema.column_names
    assert set(spec.poll_keys) <= set(schema.column_names)


def test_schedule_and_status_times_are_instants_and_sessions_dates() -> None:
    for name in ("market_status", "schedules"):
        types = {c.name: c.type.value for c in CATALOG.dataset_schema(_spec(name)).columns}
        assert types["Timestamp"] == "Instant"
        assert types["SessionEndDate"] == "LocalDate"


# Docstrings: each factory starts with its summary and links its column reference section.


@pytest.mark.parametrize("spec", futures.DATASETS, ids=lambda spec: spec.name)
def test_docstrings_link_the_column_reference(spec: DatasetSpec) -> None:
    dataset = spec.name.removeprefix("futures.")
    doc = getattr(futures._Futures, dataset).__doc__
    assert doc is not None
    assert f"{DOCS}#{dataset.replace('.', '-')}" in doc
    assert doc.splitlines()[0] == spec.summary
    assert "columns()" in doc


# Fixtures: every payload is shaped by the generated source it stands for.


def _flatten(row: dict[str, Any], prefix: str = "") -> set[str]:
    keys: set[str] = set()
    for key, value in row.items():
        dotted = f"{prefix}{key}"
        if isinstance(value, dict):
            keys |= _flatten(value, f"{dotted}.")
        else:
            keys.add(dotted)
    return keys


_REST_FIXTURES = {
    "trades.json": "/futures/v1/trades/{ticker}",
    "quotes.json": "/futures/v1/quotes/{ticker}",
    "aggs.json": "/futures/v1/aggs/{ticker}",
    "snapshot.json": "/futures/v1/snapshot",
    "contracts.json": "/futures/v1/contracts",
    "products.json": "/futures/v1/products",
    "exchanges.json": "/futures/v1/exchanges",
    "market_status.json": "/futures/v1/market-status",
    "schedules.json": "/futures/v1/schedules",
}


@pytest.mark.parametrize(("name", "path"), _REST_FIXTURES.items())
def test_rest_fixtures_are_shaped_by_their_path(load_fixture: Loader, name: str, path: str) -> None:
    generated = source(Transport.REST, path)
    wire = {f.wire_key for f in generated.fields}
    required = {f.wire_key for f in generated.fields if f.required}
    rows = json.loads(load_fixture(OWNER, name))["results"]
    assert rows
    for row in rows:
        keys = _flatten(row)
        assert keys <= wire
        assert required <= keys


def test_ws_frames_are_shaped_by_their_channels(load_fixture: Loader) -> None:
    frames = json.loads(load_fixture(OWNER, "ws_frames.json"))
    assert [frame["ev"] for frame in frames] == ["T", "Q", "A", "AM"]
    for frame in frames:
        wire = {f.wire_key for f in source(Transport.WS, f"futures/{frame['ev']}").fields}
        assert set(frame) - {"ev"} == wire


@pytest.mark.parametrize("name", ["trades_v1", "quotes_v1", "minute_aggs_v1", "session_aggs_v1"])
def test_flat_file_fixtures_are_shaped_by_their_prefix(load_fixture: Loader, name: str) -> None:
    text = load_fixture(OWNER, f"{name}.csv").decode()
    header = next(csv.reader(io.StringIO(text)))
    generated = source(Transport.FLAT_FILES, f"us_futures_{{venue}}/{name}")
    assert header == [f.wire_key for f in generated.fields]
