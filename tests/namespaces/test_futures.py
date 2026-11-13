"""Tests for the `m.futures` namespace: its dataset specs, factory methods, and schemas.

The end-to-end tests at the end run each dataset through the transports: its exact request
through the fake REST server, a frame of each channel through the fake WebSocket server into
`blink()`, and one block of each flat-file prefix through the fake S3 bucket.
"""

from __future__ import annotations

import csv
import inspect
import io
import json
import logging
import re
import threading
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import new_table
from deephaven.column import string_col
from deephaven.stream import blink_to_append_only

import deephaven_massive as dm
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.catalog import KeySpec
from deephaven_massive._datasets.futures_sessions import CHICAGO
from deephaven_massive._massive import Transport, object_key, source
from deephaven_massive._session import CATALOG
from deephaven_massive.namespaces import _futures as futures
from deephaven_massive.namespaces._namespace import Namespace
from deephaven_massive.namespaces._query_building import (
    build_query,
)
from deephaven_massive.queries import FetchQuery, HistoryQuery, Query, StreamHistoryQuery
from deephaven_massive.queries._choice import query_class

# The reply builder is a plain class of the fake, which no fixture can hand over; the servers
# and the streaming session come from the `fake_rest`, `fake_s3`, and `ws_session` fixtures.
from tests._fakes.rest import FakeRestServer, Reply

if TYPE_CHECKING:
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from tests._fakes.s3 import FakeS3
    from tests._fakes.ws_session import WsSession

NOT_STREAMED = r"\.blink\(\): only 1-second and 1-minute bars at multiplier=1 stream"
"""The refusal of a stream of bars Massive doesn't stream."""

OWNER = "namespaces/futures"

Loader = Callable[[str, str], bytes]


def test_namespace_is_reached_from_the_session(massive_session: dm.MassiveSession) -> None:
    namespace = massive_session.futures
    assert type(namespace) is futures.FuturesNamespace
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
        assert q._request.symbols == ("ESZ5", "NQZ5")
        # Every venue by default, in FuturesVenue order.
        assert q._request.venues == tuple(dm.FuturesVenue)
        cme = factory("ESZ5", venues={dm.FuturesVenue.CME})
        assert cme._request.venues == (dm.FuturesVenue.CME,)
        two = factory("ESZ5", venues={dm.FuturesVenue.COMEX, dm.FuturesVenue.CBOT})
        assert two._request.venues == (dm.FuturesVenue.CBOT, dm.FuturesVenue.COMEX)
        assert factory(dm.ALL)._request.symbols is dm.ALL
        assert "venues={dm.FuturesVenue.CME}" in repr(cme)
        # Left out, venues reads every venue the plan allows, skipping a refused one; named,
        # a refused venue follows refused_venues, the session's setting when left out.
        assert q._request.refused_venues is dm.OnError.LOG
        assert "venues" not in repr(q)
        every = factory("ESZ5", venues=frozenset(dm.FuturesVenue))
        assert every._request.venues == tuple(dm.FuturesVenue)
        assert every._request.refused_venues is dm.OnError.RAISE
        assert "venues=" in repr(every)
        skips = factory("ESZ5", venues={dm.FuturesVenue.CME}, refused_venues=dm.OnError.LOG)
        assert skips._request.refused_venues is dm.OnError.LOG
        assert "refused_venues=dm.OnError.LOG" in repr(skips)
        # Without named venues, refused_venues has nothing to decide.
        ignored = factory("ESZ5", refused_venues=dm.OnError.RAISE)
        assert ignored._request.refused_venues is dm.OnError.LOG


def test_refused_venues_left_out_follows_the_session() -> None:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="k", refused_venues=dm.OnError.IGNORE))
    try:
        cme = {dm.FuturesVenue.CME}
        for q in (
            m.futures.trades("ESZ5", venues=cme),
            m.futures.quotes("ESZ5", venues=cme),
            m.futures.bars("ESZ5", dm.Timespan.MINUTE, venues=cme),
        ):
            assert q._request.refused_venues is dm.OnError.IGNORE
        bars = m.futures.bars(
            "ESZ5", dm.Timespan.MINUTE, venues=cme, refused_venues=dm.OnError.RAISE
        )
        assert bars._request.refused_venues is dm.OnError.RAISE
        assert m.stocks.trades("AAPL")._request.refused_venues is None
    finally:
        m.close()


_PLAIN = cast("Any", "text")


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (lambda f: f.trades("ESZ5", venues=_PLAIN), "venues must be a set of dm.FuturesVenue"),
        (lambda f: f.quotes("ESZ5", venues={_PLAIN}), "venues must hold only dm.FuturesVenue"),
        (
            lambda f: f.bars("ESZ5", dm.Timespan.MINUTE, venues=dm.FuturesVenue.CME),
            "venues must be a set of dm.FuturesVenue, such as",
        ),
        (lambda f: f.bars("ESZ5", _PLAIN), "timespan must be dm.Timespan"),
        (lambda f: f.trades("ESZ5", refused_venues=_PLAIN), "refused_venues must be dm.OnError"),
        (lambda f: f.quotes("ESZ5", refused_venues=_PLAIN), "refused_venues must be dm.OnError"),
        (
            lambda f: f.bars("ESZ5", dm.Timespan.MINUTE, refused_venues=_PLAIN),
            "refused_venues must be dm.OnError",
        ),
        (lambda f: f.snapshots("ESZ5", order=_PLAIN), "order must be dm.Order"),
        (lambda f: f.contracts(order=_PLAIN), "order must be dm.Order"),
        (lambda f: f.products(order=_PLAIN), "order must be dm.Order"),
        (lambda f: f.schedules(order=_PLAIN), "order must be dm.Order"),
    ],
    ids=[
        "trades.venues",
        "quotes.venues",
        "bars.venues",
        "bars.timespan",
        "trades.refused_venues",
        "quotes.refused_venues",
        "bars.refused_venues",
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
    ("bar", "streams"),
    [
        ((dm.Timespan.SECOND, 1), True),
        ((dm.Timespan.MINUTE, 1), True),
        ((dm.Timespan.MINUTE, 5), False),
        ((dm.Timespan.HOUR, 1), False),
        ((dm.Timespan.SESSION, 1), False),
        ((dm.Timespan.WEEK, 2), False),
    ],
)
def test_every_bars_call_is_one_class_and_only_some_stream(
    massive_session: dm.MassiveSession, bar: tuple[Any, int], streams: bool
) -> None:
    timespan, multiplier = bar
    q = massive_session.futures.bars("ESZ5", timespan, multiplier=multiplier)
    assert type(q) is StreamHistoryQuery  # every bars call, streaming or not
    if not streams:
        with pytest.raises(ValueError, match=NOT_STREAMED):
            q.blink()
    assert q._request.timespan is timespan
    assert q._request.multiplier == multiplier


def test_minute_bars_stream_and_keep_their_venues(massive_session: dm.MassiveSession) -> None:
    q = massive_session.futures.bars(dm.ALL, dm.Timespan.MINUTE, venues={dm.FuturesVenue.NYMEX})
    assert type(q) is StreamHistoryQuery
    assert q._request.timespan is dm.Timespan.MINUTE
    assert q._request.venues == (dm.FuturesVenue.NYMEX,)
    with pytest.raises(ValueError, match="venues is empty; leave it out to read every venue"):
        massive_session.futures.bars(dm.ALL, dm.Timespan.MINUTE, venues=frozenset())


def test_bars_take_the_day_timespan(
    massive_session: dm.MassiveSession,
) -> None:
    # Massive serves `1day` futures bars, and answers HTTP 400 to a size it does not serve.
    assert massive_session.futures.bars("ESZ5", dm.Timespan.DAY)._request.timespan is (
        dm.Timespan.DAY
    )
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan"):
        massive_session.futures.bars("ESZ5", cast("Any", "minute"))
    hours = massive_session.futures.bars("ESZ5", dm.Timespan.HOUR, multiplier=0)
    assert hours._request.multiplier == 0


def test_every_contract_s_day_bars_point_to_session_bars(
    massive_session: dm.MassiveSession,
) -> None:
    # Day bars are read from REST only, which needs explicit contracts; the session files
    # hold every contract's session bars.
    query = massive_session.futures.bars(dm.ALL, dm.Timespan.DAY)
    with pytest.raises(ValueError, match=r"use dm\.Timespan\.SESSION for every contract's daily"):
        query.history("2025-02-04", "2025-02-05", wait=True)


def test_snapshots_take_symbols_or_all(massive_session: dm.MassiveSession) -> None:
    q = massive_session.futures.snapshots(
        ["ESZ5"], sort="ticker", order=dm.Order.DESC, limit=10, filters={"ticker_gte": "E"}
    )
    assert type(q) is FetchQuery
    assert q._request.symbols == ("ESZ5",)
    assert q._request.params == (("order", dm.Order.DESC), ("sort", "ticker"))
    assert q._request.filters == (("ticker_gte", "E"),)
    assert q._request.limit == 10
    every_es = massive_session.futures.snapshots(dm.ALL, product_code="ES")
    assert every_es._request.symbols is dm.ALL
    assert every_es._request.params == (("product_code", "ES"),)


def test_snapshot_symbols_are_set_only_through_symbols(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match="ticker_any_of is set through symbols=, not filters"):
        massive_session.futures.snapshots("ESZ5", filters={"ticker_any_of": ["NQZ5"]})


def test_contracts(massive_session: dm.MassiveSession) -> None:
    q = massive_session.futures.contracts(
        product_code="ES", filters={"active": True, "last_trade_date_gte": date(2026, 1, 1)}
    )
    assert type(q) is FetchQuery
    assert q._request.symbols is None
    assert q._request.params == (("product_code", "ES"),)
    assert dict(q._request.filters) == {"active": True, "last_trade_date_gte": date(2026, 1, 1)}
    with pytest.raises(ValueError, match="filter 'active' takes a bool, not 'yes'"):
        massive_session.futures.contracts(filters={"active": "yes"})
    with pytest.raises(TypeError, match="sort must be a str"):
        massive_session.futures.contracts(sort=cast("str", 1))


def test_contracts_take_symbols_and_a_day(massive_session: dm.MassiveSession) -> None:
    contracts = massive_session.futures.contracts
    q = contracts(["ESZ5", "NQZ5"], as_of="2025-06-02")
    assert q._request.symbols == ("ESZ5", "NQZ5")
    assert q._request.params == (("date", date(2025, 6, 2)),)
    with pytest.raises(ValueError, match=r"does not accept dm\.ALL; leave symbols out"):
        contracts(dm.ALL)
    with pytest.raises(ValueError, match="ticker_any_of is set through symbols=, not filters"):
        contracts(filters={"ticker_any_of": ["ESZ5"]})
    with pytest.raises(ValueError, match="date is set through its own argument, as_of="):
        contracts(filters={"date": "2025-06-02"})
    with pytest.raises(ValueError, match="as_of"):
        contracts(as_of=datetime(2025, 6, 2, 12, tzinfo=UTC))


def test_products_take_a_day(massive_session: dm.MassiveSession) -> None:
    q = massive_session.futures.products(as_of=date(2025, 6, 2))
    assert q._request.params == (("date", date(2025, 6, 2)),)
    with pytest.raises(ValueError, match="date is set through its own argument, as_of="):
        massive_session.futures.products(filters={"date": "2025-06-02"})


def test_products(massive_session: dm.MassiveSession) -> None:
    q = massive_session.futures.products(
        product_code="GC", sort="name", limit=5, filters={"sector": "precious"}
    )
    assert type(q) is FetchQuery
    assert q._request.params == (("product_code", "GC"), ("sort", "name"))
    assert q._request.limit == 5
    with pytest.raises(ValueError, match="filter 'sector' takes"):
        massive_session.futures.products(filters={"sector": "nonsense"})
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        massive_session.futures.products(order=cast("dm.Order", "asc"))


def test_exchanges(massive_session: dm.MassiveSession) -> None:
    q = massive_session.futures.exchanges()
    assert type(q) is FetchQuery
    assert q._request == type(q._request)()
    assert repr(q) == "m.futures.exchanges()"
    assert massive_session.futures.exchanges(limit=2)._request.limit == 2


def rest_params(name: str) -> set[str]:
    path = _spec(name).fetch_path
    assert path is not None
    return {p.name for p in source(Transport.REST, path).params}


@pytest.mark.parametrize("name", ["exchanges", "market_status"])
def test_a_path_without_filters_or_sort_offers_none(name: str) -> None:
    # Every parameter of either path is `limit` or a form of the typed `product_code`.
    parameters = inspect.signature(getattr(futures.FuturesNamespace, name)).parameters
    assert not {"filters", "sort", "order"} & set(parameters)
    assert {p.split(".")[0] for p in rest_params(name)} <= {"limit", "product_code"}


def test_market_status(massive_session: dm.MassiveSession) -> None:
    q = massive_session.futures.market_status(product_code="ES", limit=3)
    assert type(q) is FetchQuery
    assert q._request.params == (("product_code", "ES"),)
    assert q._request.limit == 3


@pytest.mark.parametrize(
    "call",
    [
        lambda f: f.snapshots(dm.ALL, filters={"product_code_any_of": "ES"}),
        lambda f: f.contracts(filters={"product_code_gte": "C"}),
        lambda f: f.products(filters={"product_code": "ES"}),
        lambda f: f.schedules(filters={"product_code_lt": "Z"}),
    ],
    ids=["snapshots", "contracts", "products", "schedules"],
)
def test_product_code_forms_are_set_only_through_product_code(
    massive_session: dm.MassiveSession, call: Callable[[Any], object]
) -> None:
    with pytest.raises(
        ValueError, match="is set through its own argument, product_code=, not filters"
    ):
        call(massive_session.futures)


_PRODUCT_DATASETS = ["snapshots", "contracts", "products", "market_status", "schedules"]


def _with_product_code(namespace: object, name: str, product_code: object) -> Query:
    factory = getattr(namespace, name)
    if name == "snapshots":
        return cast("Query", factory(dm.ALL, product_code=product_code))
    return cast("Query", factory(product_code=product_code))


@pytest.mark.parametrize("name", _PRODUCT_DATASETS)
def test_every_product_code_path_takes_a_list_of_codes(name: str) -> None:
    # One request serves several codes, so a sequence never has to split into requests.
    assert "product_code.any_of" in rest_params(name)


@pytest.mark.parametrize("name", _PRODUCT_DATASETS)
def test_product_code_is_one_code_or_several(massive_session: dm.MassiveSession, name: str) -> None:
    f = massive_session.futures
    one = _with_product_code(f, name, "ES")._request.params
    several = _with_product_code(f, name, ["ES", "NQ"])._request.params
    assert ("product_code", "ES") in one
    assert not any(key == "product_code.any_of" for key, _ in one)
    assert ("product_code.any_of", ("ES", "NQ")) in several
    assert not any(key == "product_code" for key, _ in several)
    assert not any(
        key.startswith("product_code")
        for key, _ in _with_product_code(f, name, None)._request.params
    )
    with pytest.raises(ValueError, match="product_code is empty; pass None for every product"):
        _with_product_code(f, name, [])
    with pytest.raises(TypeError, match="product_code must be a str, a sequence of str, or None"):
        _with_product_code(f, name, 5)
    with pytest.raises(TypeError, match="element 1 of the sequence is int, not str"):
        _with_product_code(f, name, ["ES", 5])


@pytest.mark.parametrize("name", _PRODUCT_DATASETS)
def test_product_code_takes_a_table_of_codes(massive_session: dm.MassiveSession, name: str) -> None:
    f = massive_session.futures
    codes = new_table([string_col("ProductCode", ["ES", "NQ"])])
    query = _with_product_code(f, name, codes)
    values = query._request.value_table
    assert values is not None
    assert (values.parameter, values.column, values.table) == ("product_code", "ProductCode", codes)
    assert not any(key.startswith("product_code") for key, _ in query._request.params)
    with pytest.raises(
        ValueError, match=r"^a table passed as product_code must have a ProductCode column"
    ):
        _with_product_code(f, name, new_table([string_col("Symbol", ["ES"])]))


def test_schedules_select_sessions(massive_session: dm.MassiveSession) -> None:
    q = massive_session.futures.schedules(
        product_code="ES",
        start="2025-12-22",
        end=date(2025, 12, 26),
        filters={"trading_venue": "XCME"},
    )
    assert type(q) is FetchQuery
    assert q._request.params == (("product_code", "ES"),)
    assert q._request.start == date(2025, 12, 22)
    assert q._request.end == date(2025, 12, 26)
    assert massive_session.futures.schedules()._request.start is None
    with pytest.raises(ValueError, match="must not be after end"):
        massive_session.futures.schedules(start="2025-12-26", end="2025-12-22")
    # The axis is a calendar date of sessions, so a datetime bound is refused.
    evening = datetime(2025, 12, 22, 18, tzinfo=CHICAGO)
    for bounds in ({"start": evening}, {"end": evening}):
        with pytest.raises(
            ValueError,
            match=r"^futures\.schedules: session_end_date is a calendar date; pass dates$",
        ):
            massive_session.futures.schedules(**cast("dict[str, Any]", bounds))
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
        ("bars", StreamHistoryQuery),
        ("snapshots", FetchQuery),
        ("contracts", FetchQuery),
        ("products", FetchQuery),
        ("exchanges", FetchQuery),
        ("market_status", FetchQuery),
        ("schedules", FetchQuery),
    ],
)
def test_query_classes(name: str, expected: type) -> None:
    assert query_class(_spec(name)) is expected


def test_no_futures_dataset_has_a_last_endpoint() -> None:
    streamed = [spec for spec in futures.DATASETS if spec.has_channels]
    assert streamed
    assert not any(spec.has_fetch for spec in streamed)


@pytest.mark.parametrize("name", ["trades", "quotes", "bars"])
def test_market_data_starts_with_symbol_session_date_and_time(name: str) -> None:
    schema = CATALOG.dataset_schema(_spec(name))
    times = ("TimestampStart", "TimestampEnd") if name == "bars" else ("Timestamp",)
    assert schema.column_names[: 2 + len(times)] == ("Symbol", "Date", *times)
    assert schema.column_names[-1] == "TimestampInsert"
    types = {column.name: column.type.value for column in schema.columns}
    assert types["Symbol"] == "String"
    assert all(types[time] == "Instant" for time in times)
    # The session, `session_end_date`, is the row's `Date`, ISO text.
    assert types["Date"] == "String"
    assert {"Ticker", "Underlying", "SessionEndDate"}.isdisjoint(types)


def test_bar_times_come_from_the_window_start() -> None:
    names = CATALOG.dataset_schema(_spec("bars")).column_names
    assert {"WindowStart", "EndTimestamp", "Timestamp"}.isdisjoint(names)
    assert "SettlementPrice" in names


def test_snapshots_name_the_contract_symbol_and_have_no_single_time() -> None:
    schema = CATALOG.dataset_schema(_spec("snapshots"))
    assert schema.column_names[0] == "Symbol"
    assert "Timestamp" not in schema.column_names
    assert "Ticker" not in schema.column_names
    assert {"LastTradePrice", "LastQuoteBidPrice", "SessionSettlementPrice"} <= set(
        schema.column_names
    )


def test_contracts_name_the_contract_symbol_from_the_row() -> None:
    spec = _spec("contracts")
    assert CATALOG.takes_symbols(spec)
    assert spec.keys[(Transport.REST, "/futures/v1/contracts")].declared() == (
        ("symbol", KeySpec("Ticker")),
    )
    assert (spec.symbol_param, spec.symbols_per_request) == ("ticker.any_of", 250)
    schema = CATALOG.dataset_schema(spec)
    assert schema.column_names[:2] == ("Symbol", "RawSymbol")
    assert "Ticker" not in schema.column_names
    assert spec.poll_keys == ("Symbol", "Date")


@pytest.mark.parametrize("name", ["products", "exchanges", "market_status", "schedules"])
def test_reference_datasets_take_no_symbols(name: str) -> None:
    spec = _spec(name)
    assert not CATALOG.takes_symbols(spec)
    # Only a row time is declared, where the rows have one: `Timestamp` is the library's name.
    timed = name in {"market_status", "schedules"}
    assert all(keys.declared() == (("time", KeySpec("Timestamp")),) for keys in spec.keys.values())
    assert bool(spec.keys) == timed
    schema = CATALOG.dataset_schema(spec)
    assert "Symbol" not in schema.column_names
    assert set(spec.poll_keys) <= set(schema.column_names)


def test_schedule_and_status_times_are_instants_and_sessions_dates() -> None:
    for name in ("market_status", "schedules"):
        types = {c.name: c.type.value for c in CATALOG.dataset_schema(_spec(name)).columns}
        assert types["Timestamp"] == "Instant"
        assert types["SessionEndDate"] == "String"  # every date is ISO text
        assert "Date" not in types  # reference rows have no trading-day column of the library


# Docstrings: each factory starts with its summary and names its dataset page.


@pytest.mark.parametrize("spec", futures.DATASETS, ids=lambda spec: spec.name)
def test_docstrings_name_their_dataset_page(spec: DatasetSpec) -> None:
    dataset = spec.name.removeprefix("futures.")
    doc = getattr(futures.FuturesNamespace, dataset).__doc__
    assert doc is not None
    assert f"docs/datasets/{spec.name}.md" in doc
    assert doc.splitlines()[0] == spec.summary


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
    "poll_keys_contracts.json": "/futures/v1/contracts",
    "poll_keys_products.json": "/futures/v1/products",
    "poll_keys_exchanges.json": "/futures/v1/exchanges",
    "poll_keys_market_status.json": "/futures/v1/market-status",
    "poll_keys_schedules.json": "/futures/v1/schedules",
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
    # The reader matches headers by name; the generated order puts the prices first.
    assert sorted(header) == sorted(f.wire_key for f in generated.fields)


# End to end: each dataset through the fake REST server, WebSocket server, and S3 bucket.

NS = 10**9


def rows(table: Table) -> list[dict[str, Any]]:
    # Instants become epoch nanoseconds, so assertions keep every digit. Every dataset row
    # carries when it arrived, last, which the rows here leave out once it is checked.
    data = dh_arrow.to_arrow(table)
    columns = [c.cast(pa.int64()) if pa.types.is_timestamp(c.type) else c for c in data.columns]
    out: list[dict[str, Any]] = pa.Table.from_arrays(columns, names=data.column_names).to_pylist()
    if "TimestampInsert" in data.column_names:
        assert data.column_names[-1] == "TimestampInsert"
        assert all(row.pop("TimestampInsert") is not None for row in out)
    return out


def chicago_open(day: date) -> int:
    # 17:00 America/Chicago on `day`: the open of the session that ends the next trading day.
    return int(datetime.combine(day, time(17), CHICAGO).timestamp()) * NS


@pytest.fixture
def serve(fake_rest: FakeRestServer, load_fixture: Loader) -> Callable[[str, str], None]:
    def reply(path: str, fixture: str) -> None:
        fake_rest.reply(path, Reply(json.loads(load_fixture(OWNER, fixture))))

    return reply


@pytest.mark.parametrize(
    ("dataset", "path", "fixture"),
    [
        ("trades", "/futures/v1/trades/ESZ4", "trades.json"),
        ("quotes", "/futures/v1/quotes/GCJ5", "quotes.json"),
    ],
)
def test_tick_history_over_rest_reads_each_session_by_its_date(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    serve: Callable[[str, str], None],
    load_fixture: Loader,
    dataset: str,
    path: str,
    fixture: str,
) -> None:
    serve(path, fixture)
    symbol = path.rsplit("/", 1)[1]
    factory = getattr(rest_session.futures, dataset)
    out = rows(
        factory(symbol).history("2024-12-17", "2024-12-17", source=dm.HistorySource.REST, wait=True)
    )
    # A date names its session, which is requested by that date with no time window, so the
    # hours a holiday adds to a session are covered. Rows are kept by SessionEndDate.
    assert fake_rest.get_queries() == [
        (
            path,
            {
                "limit": ["50000"],
                "sort": ["timestamp.asc"],
                "session_end_date": ["2024-12-17"],
            },
        )
    ]
    payload = json.loads(load_fixture(OWNER, fixture))["results"]
    # The fixture's last row belongs to the next session; its session, `Date`, drops it.
    assert payload[-1]["session_end_date"] == "2024-12-18"
    assert [(r["Symbol"], r["Timestamp"], r["SequenceNumber"]) for r in out] == [
        (symbol, p["timestamp"], p["sequence_number"])
        for p in payload
        if p["session_end_date"] == "2024-12-17"
    ]
    assert {r["Date"] for r in out} == {"2024-12-17"}
    assert all(r["Exchange"] is None for r in out)  # only the flat files carry it


def test_trades_rest_rows(
    *, rest_session: dm.MassiveSession, serve: Callable[[str, str], None]
) -> None:
    serve("/futures/v1/trades/ESZ4", "trades.json")
    q = rest_session.futures.trades("ESZ4")
    first = rows(q.history("2024-12-17", "2024-12-17", source=dm.HistorySource.REST, wait=True))[0]
    supplied = {
        "Symbol": "ESZ4",
        "Timestamp": 1734472799000509200,
        "SequenceNumber": 27317882,
        "ReportSequence": 12033289,
        "Price": 6052.0,
        "Size": 3,
        "Date": "2024-12-17",  # Massive's session_end_date
        "Channel": 360,
    }
    assert {name: first[name] for name in supplied} == supplied
    # Every other column of the trades kind comes from the flat files or another asset class.
    assert all(value is None for name, value in first.items() if name not in supplied)


def test_quotes_rest_rows(
    *, rest_session: dm.MassiveSession, serve: Callable[[str, str], None]
) -> None:
    serve("/futures/v1/quotes/GCJ5", "quotes.json")
    q = rest_session.futures.quotes("GCJ5")
    first = rows(q.history("2024-12-17", "2024-12-17", source=dm.HistorySource.REST, wait=True))[0]
    assert first["AskTimestamp"] == first["BidTimestamp"] == 1734472800076125400
    assert (first["AskPrice"], first["AskSize"], first["BidPrice"], first["BidSize"]) == (
        None,
        0.0,
        2660.0,
        1.0,
    )


@pytest.mark.parametrize(
    ("timespan", "resolution"),
    # Massive serves `1day` futures bars as the same one-session buckets as `1session`.
    [(dm.Timespan.SESSION, "1session"), (dm.Timespan.DAY, "1day")],
)
def test_session_bars_over_rest(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    serve: Callable[[str, str], None],
    timespan: dm.Timespan,
    resolution: str,
) -> None:
    serve("/futures/v1/aggs/GCJ5", "aggs.json")
    q = rest_session.futures.bars("GCJ5", timespan)
    out = rows(q.history("2025-02-04", "2025-02-05", source=dm.HistorySource.REST, wait=True))
    assert fake_rest.get_queries() == [
        (
            "/futures/v1/aggs/GCJ5",
            {
                "resolution": [resolution],
                "limit": ["50000"],
                "window_start.gte": [str(chicago_open(date(2025, 1, 30)))],
                # 18:00 Chicago on the last session date.
                "window_start.lt": [str(chicago_open(date(2025, 2, 5)) + 3600 * NS)],
                "sort": ["window_start.asc"],
            },
        )
    ]
    # A session bar starts at Massive's window start and ends at 17:00 Chicago time on its
    # session date, which is its `Date`.
    assert [
        (r["Symbol"], r["TimestampStart"], r["TimestampEnd"], r["Date"], r["Close"]) for r in out
    ] == [
        ("GCJ5", 1738540800000000000, chicago_open(date(2025, 2, 4)), "2025-02-04", 2874.2),
        ("GCJ5", 1738627200000000000, chicago_open(date(2025, 2, 5)), "2025-02-05", 2884.8),
    ]
    assert out[0]["SettlementPrice"] == 2875.8
    assert out[0]["Volume"] == 133127.0


def test_five_minute_bars_send_their_resolution(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    serve: Callable[[str, str], None],
) -> None:
    serve("/futures/v1/aggs/GCJ5", "aggs.json")
    q = rest_session.futures.bars("GCJ5", dm.Timespan.MINUTE, multiplier=5)
    q.history("2025-02-04", "2025-02-05", wait=True)
    assert [query["resolution"] for _, query in fake_rest.get_queries()] == [["5min"]]


def test_snapshots_over_rest(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    serve: Callable[[str, str], None],
) -> None:
    serve("/futures/v1/snapshot", "snapshot.json")
    q = rest_session.futures.snapshots(
        ["ESZ5", "CBN5"], product_code="CB", sort="ticker", order=dm.Order.DESC
    )
    out = rows(q.fetch(wait=True))
    assert fake_rest.get_queries() == [
        (
            "/futures/v1/snapshot",
            {
                "product_code": ["CB"],
                "sort": ["ticker.desc"],
                "limit": ["50000"],
                "ticker.any_of": ["CBN5,ESZ5"],
            },
        )
    ]
    assert len(out) == 1
    row = out[0]
    assert row["Symbol"] == "CBN5"
    assert row["ProductCode"] == "CB"
    assert row["SettlementDate"] == "2025-07-30"  # midnight in Chicago, as a date
    assert row["LastMinuteLastUpdated"] == 1746045300000 * 10**6  # sent in milliseconds
    assert row["LastTradeTimestamp"] == 1746045242858242600  # sent in nanoseconds
    assert (row["LastTradePrice"], row["LastTradeSize"]) == (240.00000000000003, 5)
    assert row["SessionPreviousSettlement"] == 219.39


def test_snapshot_minute_time_sent_in_nanoseconds(
    *,
    rest_session: dm.MassiveSession,
    serve: Callable[[str, str], None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # As Massive sent the snapshot on 2026-11-02: the minute bar's `last_updated` in
    # nanoseconds, where the documented sample has milliseconds.
    serve("/futures/v1/snapshot", "snapshot_nanosecond_minute.json")
    with caplog.at_level(logging.WARNING):
        out = rows(rest_session.futures.snapshots("CBN5").fetch(wait=True))
    minute = datetime(2026, 11, 2, 17, 23, tzinfo=UTC)
    assert out[0]["LastMinuteLastUpdated"] == int(minute.timestamp()) * 10**9
    assert "schema drift" not in caplog.text


def test_every_contract_of_a_product(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    serve: Callable[[str, str], None],
) -> None:
    serve("/futures/v1/snapshot", "snapshot.json")
    rest_session.futures.snapshots(dm.ALL, product_code="CB").fetch(wait=True)
    assert fake_rest.get_queries() == [
        ("/futures/v1/snapshot", {"product_code": ["CB"], "limit": ["50000"]})
    ]


def test_contracts_over_rest(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    serve: Callable[[str, str], None],
) -> None:
    serve("/futures/v1/contracts", "contracts.json")
    q = rest_session.futures.contracts(product_code="ES", filters={"active": True}, limit=5)
    out = rows(q.fetch(wait=True))
    assert fake_rest.get_queries() == [
        (
            "/futures/v1/contracts",
            {"product_code": ["ES"], "active": ["true"], "limit": ["5"]},
        )
    ]
    assert [(r["Symbol"], r["ProductCode"], r["Active"], r["LastTradeDate"]) for r in out] == [
        ("ESZ6", "ES", True, "2026-12-18")
    ]
    assert out[0]["TradeTickSize"] == 0.25
    assert out[0]["RawSymbol"] is None
    assert "Ticker" not in out[0]


def test_contract_symbols_and_day_go_on_the_wire(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    serve: Callable[[str, str], None],
) -> None:
    serve("/futures/v1/contracts", "contracts.json")
    rest_session.futures.contracts(["NQZ6", "ESZ6"], as_of="2026-10-07").fetch(wait=True)
    assert fake_rest.get_queries() == [
        (
            "/futures/v1/contracts",
            {"ticker.any_of": ["ESZ6,NQZ6"], "date": ["2026-10-07"], "limit": ["1000"]},
        )
    ]


def test_a_long_contract_list_is_split_into_requests(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/futures/v1/contracts", Reply({"status": "OK", "results": []}))
    # 360 contracts: three products, twelve months, ten years.
    tickers = [f"{p}{m}{y}" for p in ("ES", "NQ", "GC") for y in range(10) for m in "FGHJKMNQUVXZ"]
    rest_session.futures.contracts(tickers).fetch(wait=True)
    sizes = sorted(
        len(query["ticker.any_of"][0].split(",")) for _, query in fake_rest.get_queries()
    )
    assert sizes == [110, 250]


def test_several_product_codes_go_in_one_request(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    serve: Callable[[str, str], None],
) -> None:
    serve("/futures/v1/contracts", "contracts.json")
    rest_session.futures.contracts(product_code=["NQ", "ES"]).fetch(wait=True)
    assert fake_rest.get_queries() == [
        ("/futures/v1/contracts", {"product_code.any_of": ["NQ,ES"], "limit": ["1000"]})
    ]


def test_exchanges_over_rest(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    serve: Callable[[str, str], None],
) -> None:
    serve("/futures/v1/exchanges", "exchanges.json")
    out = rows(rest_session.futures.exchanges().fetch(wait=True))
    assert fake_rest.get_queries() == [("/futures/v1/exchanges", {"limit": ["1000"]})]
    assert out == [
        {
            "Acronym": "CME",
            "Id": "4",
            "Locale": "US",
            "Mic": "XCME",
            "Name": "Chicago Mercantile Exchange",
            "OperatingMic": "XCME",
            "Type": "exchange",
            "Url": "https://cmegroup.com",
        }
    ]


def test_products_over_rest(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    serve: Callable[[str, str], None],
) -> None:
    serve("/futures/v1/products", "products.json")
    out = rows(rest_session.futures.products(product_code="EBE").fetch(wait=True))
    assert fake_rest.get_queries() == [
        ("/futures/v1/products", {"product_code": ["EBE"], "limit": ["50000"]})
    ]
    assert [(r["ProductCode"], r["TradingVenue"], r["Date"], r["LastUpdated"]) for r in out] == [
        # Massive sends a time of day with an offset: 2017-01-01T17:11:39-06:00 is 23:11:39 UTC.
        ("EBE", "XNYM", "2025-07-07", 1483312299000000000)
    ]
    assert out[0]["UnitOfMeasureQty"] == 1000.0


def test_market_status_over_rest(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    serve: Callable[[str, str], None],
) -> None:
    serve("/futures/v1/market-status", "market_status.json")
    out = rows(rest_session.futures.market_status(product_code="ERL").fetch(wait=True))
    assert fake_rest.get_queries() == [
        ("/futures/v1/market-status", {"product_code": ["ERL"], "limit": ["100"]})
    ]
    assert out == [
        {
            "MarketEvent": "open",
            "Name": "ERCOT North 345 kV Hub Day-Ahead 5 MW Off-Peak Futures",
            "ProductCode": "ERL",
            "SessionEndDate": "2025-12-05",
            "Timestamp": 1764889200 * NS,  # 2025-12-04T23:00:00+00:00
            "TradingVenue": "XNYM",
        }
    ]


def test_schedules_over_rest(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    serve: Callable[[str, str], None],
) -> None:
    serve("/futures/v1/schedules", "schedules.json")
    q = rest_session.futures.schedules(
        product_code="ERL", start=date(2024, 6, 10), end=date(2024, 6, 10)
    )
    out = rows(q.fetch(wait=True))
    assert fake_rest.get_queries() == [
        (
            "/futures/v1/schedules",
            {
                "product_code": ["ERL"],
                "limit": ["1000"],
                "session_end_date.gte": ["2024-06-10"],
                "session_end_date.lte": ["2024-06-10"],
            },
        )
    ]
    assert [(r["Event"], r["Timestamp"], r["SessionEndDate"]) for r in out] == [
        ("pre_open", 1717966800 * NS, "2024-06-10"),
        ("open", 1717970400 * NS, "2024-06-10"),
        ("close", 1718053200 * NS, "2024-06-10"),
    ]


@pytest.mark.parametrize(
    ("dataset", "path"),
    [
        ("contracts", "/futures/v1/contracts"),
        ("products", "/futures/v1/products"),
        ("exchanges", "/futures/v1/exchanges"),
        ("market_status", "/futures/v1/market-status"),
        ("schedules", "/futures/v1/schedules"),
    ],
)
def test_poll_keys_tell_the_rows_apart(
    *,
    rest_session: dm.MassiveSession,
    serve: Callable[[str, str], None],
    dataset: str,
    path: str,
) -> None:
    # The keys of a REPLACE poll name every row once, or the keyed table would merge rows.
    # The fixture holds, for each key column, a row that differs from the first only there,
    # so every key column is needed: without any one of them, two rows would collide.
    serve(path, f"poll_keys_{dataset}.json")
    out = rows(getattr(rest_session.futures, dataset)().fetch(wait=True))
    keys = _spec(dataset).poll_keys
    assert len(out) == len(keys) + 1
    assert len({tuple(r[k] for k in keys) for r in out}) == len(out)
    assert all(r[k] is not None for r in out for k in keys)
    for dropped in keys:
        rest = [k for k in keys if k != dropped]
        assert len({tuple(r[k] for k in rest) for r in out}) < len(out), dropped


# Streams: the futures connection reaches the fake WebSocket server.


PRODUCTS = {"ESZ4": "ES", "6CH5": "6C"}
"""The product of each contract the streams below read, as `/futures/v1/contracts` lists it."""

SCHEDULES = {
    # A made-up fold: Friday 2024-12-13's trading belongs to the session of Monday 12-16, so a
    # trade the 17:00 Chicago rule dates 12-13 is moved to 12-16 by the schedule.
    "ES": [
        ("2024-12-12", "2024-12-11T23:00:00+00:00", "2024-12-12T22:00:00+00:00"),
        ("2024-12-16", "2024-12-12T23:00:00+00:00", "2024-12-16T22:00:00+00:00"),
    ],
    # Regular sessions, which agree with the rule.
    "6C": [
        ("2024-12-20", "2024-12-19T23:00:00+00:00", "2024-12-20T22:00:00+00:00"),
        ("2025-07-08", "2025-07-07T22:00:00+00:00", "2025-07-08T21:00:00+00:00"),
    ],
}
"""Each product's sessions: the session date, its pre-open, and its close."""


SCHEDULE_ROWS: dict[str, list[dict[str, object]]] = {
    "/futures/v1/contracts": [
        {"ticker": ticker, "product_code": product} for ticker, product in PRODUCTS.items()
    ],
    "/futures/v1/schedules": [
        {"product_code": product, "session_end_date": day, "event": event, "timestamp": stamp}
        for product, sessions in SCHEDULES.items()
        for day, opens, closes in sessions
        for event, stamp in (("pre_open", opens), ("close", closes))
    ],
}
"""The contracts and schedules the streams' session calendar looks up, as Massive serves them:
one row per contract, and a pre-open and a close per session."""


def at_ms(millis: int) -> datetime:
    # The instant `millis` milliseconds after the epoch, as a snapshot's row holds it.
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=millis)


@pytest.mark.parametrize(
    ("build", "topic", "index"),
    [
        (lambda f: f.trades("ESZ4"), "T.ESZ4", 0),
        (lambda f: f.quotes("ESZ4"), "Q.ESZ4", 1),
        (lambda f: f.bars("6CH5", dm.Timespan.SECOND), "A.6CH5", 2),
        (lambda f: f.bars("6CH5", dm.Timespan.MINUTE), "AM.6CH5", 3),
    ],
    ids=["trades", "quotes", "second-bars", "minute-bars"],
)
def test_one_frame_lands_in_blink(
    *,
    ws_session: WsSession,
    load_fixture: Loader,
    dh_exec_ctx: object,
    rows_when: Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]],
    build: Callable[[Any], StreamHistoryQuery],
    topic: str,
    index: int,
) -> None:
    frame = json.loads(load_fixture(OWNER, "ws_frames.json"))[index]
    ws_session.rows.update(SCHEDULE_ROWS)
    live = build(ws_session.session.futures).blink()
    seen = blink_to_append_only(live)
    # The stream's contract is looked up as it opens; its schedule places its rows.
    assert ws_session.connections.futures_calendar().wait_loaded(
        [frame["sym"]], 15.0, stop=threading.Event()
    )
    ws_session.subscribed(topic)
    assert ws_session.urls == ["wss://socket.test/futures"]  # every venue streams on /futures
    ws_session.publish(frame)
    # Wait on a consistent snapshot of the rows and assert on that same snapshot: a size read
    # can show a row that the update cycle in progress is still adding.
    (row,) = rows_when(seen, lambda arrived: len(arrived) == 1)
    assert row["Symbol"] == frame["sym"]
    time_key = "t" if frame["ev"] in {"T", "Q"} else "s"
    time = "Timestamp" if frame["ev"] in {"T", "Q"} else "TimestampStart"
    assert row[time] == at_ms(frame[time_key])  # sent in milliseconds
    assert row["TimestampInsert"] is not None
    # The stream carries no session: the rule dates the trade and quote 2024-12-13, and the
    # schedule moves them to the session it places them in; the bars' agree with the rule.
    expected = {"T": "2024-12-16", "Q": "2024-12-16", "A": "2024-12-20", "AM": "2025-07-08"}
    assert row["Date"] == expected[frame["ev"]]
    if frame["ev"] == "T":
        assert (row["Price"], row["Size"], row["SequenceNumber"]) == (6064.5, 100, 32599300)
    elif frame["ev"] == "Q":
        assert (row["BidPrice"], row["AskPrice"], row["AskTimestamp"]) == (
            6064.25,
            6064.5,
            at_ms(frame["at"]),
        )
    else:
        assert (row["Open"], row["Close"], row["Volume"]) == (6994.5, 6995.0, 91.0)
        assert row["DollarVolume"] == 1353.1
        # The end is the start plus the bar's length; for a one-second bar, the later of that
        # and Massive's `e`, which agree here.
        length = 1_000 if frame["ev"] == "A" else 60_000
        assert row["TimestampEnd"] == at_ms(frame["s"] + length)
    ws_session.session.release(live)


def test_a_contracts_table_feeds_a_stream(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    ws_session: WsSession,
    load_fixture: Loader,
    dh_exec_ctx: object,
    rows_when: Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]],
) -> None:
    # A contracts table's `Symbol` is the contract, so a part of it is a stream's symbol table.
    listed = [{"ticker": "ESZ4", "product_code": "ES"}, {"ticker": "ESH5", "product_code": "ES"}]
    fake_rest.reply("/futures/v1/contracts", Reply({"status": "OK", "results": listed}))
    contracts = rest_session.futures.contracts(product_code="ES").fetch(wait=True)
    ws_session.rows.update(SCHEDULE_ROWS)
    live = ws_session.session.futures.trades(contracts.head(1)).last_by()
    assert ws_session.connections.futures_calendar().wait_loaded(
        ["ESZ4"], 15.0, stop=threading.Event()
    )
    ws_session.subscribed("T.ESZ4")
    ws_session.publish(json.loads(load_fixture(OWNER, "ws_frames.json"))[0])
    (row,) = rows_when(live, lambda arrived: len(arrived) == 1)
    assert (row["Symbol"], row["Price"]) == ("ESZ4", 6064.5)
    ws_session.session.release(live)


# Flat files: one block of one venue's file per prefix, read back through history().

NOW = datetime(2025, 3, 10, 16, tzinfo=UTC)


@pytest.fixture
def s3_session(fake_s3: FakeS3, monkeypatch: pytest.MonkeyPatch) -> Iterator[dm.MassiveSession]:
    monkeypatch.setattr(history_source, "_now", lambda: NOW)
    credentials = {"s3_access_key_id": "AKIDFLATFILES", "s3_secret_access_key": "s3-test-key"}
    m = dm.MassiveSession(
        dm.MassiveConfig(api_key="test-key", **cast("dict[str, Any]", credentials))
    )
    yield m
    m.close()


CME = dm.FuturesVenue.CME


@pytest.mark.parametrize(
    ("build", "prefix", "day"),
    [
        (lambda f: f.trades("ESZ4", venues={CME}), "trades_v1", date(2024, 12, 17)),
        (lambda f: f.quotes("GCJ5", venues={CME}), "quotes_v1", date(2024, 12, 17)),
        (
            lambda f: f.bars("GCJ5", dm.Timespan.MINUTE, venues={CME}),
            "minute_aggs_v1",
            date(2025, 2, 4),
        ),
        (
            lambda f: f.bars("GCJ5", dm.Timespan.SESSION, venues={CME}),
            "session_aggs_v1",
            date(2025, 2, 4),
        ),
    ],
    ids=["trades", "quotes", "minute-bars", "session-bars"],
)
def test_one_block_round_trips_through_the_flat_files(
    *,
    s3_session: dm.MassiveSession,
    fake_s3: FakeS3,
    load_fixture: Loader,
    make_csv_gz: Callable[[str], bytes],
    build: Callable[[Any], HistoryQuery],
    prefix: str,
    day: date,
) -> None:
    text = load_fixture(OWNER, f"{prefix}.csv").decode()
    key = object_key(f"us_futures_{{venue}}/{prefix}", day, "cme")
    fake_s3.put(key, make_csv_gz(text))
    query = build(s3_session.futures)
    out = rows(query.history(day, day, source=dm.HistorySource.FLAT_FILES, wait=True))
    assert fake_s3.keys() == [key]
    wire = next(csv.DictReader(io.StringIO(text)))
    time_key = "window_start" if "window_start" in wire else "timestamp"
    assert len(out) == 1
    row = out[0]
    assert row["Symbol"] == wire["ticker"]
    time = "TimestampStart" if time_key == "window_start" else "Timestamp"
    assert row[time] == int(wire[time_key])
    assert row["Date"] == day.isoformat()  # the file's session_end_date
    assert row["Exchange"] == (int(wire["exchange"]) if "exchange" in wire else None)
    if "close" in wire:
        assert (row["Close"], row["Volume"]) == (float(wire["close"]), float(wire["volume"]))
    else:
        assert row["SequenceNumber"] == int(wire["sequence_number"])


SPREAD_ROWS = (
    "GCJ5,2025-02-04,1738540800000000000,1,1,1,1,1,1,1,1",
    "10YN6-10YQ6,2025-02-04,1738540800000000000,2,2,2,2,2,2,2,2",
    "BOB 02-01 U6,2025-02-04,1738540800000000000,3,3,3,3,3,3,3,3",
    "CL:BF Q6-U6-V6,2025-02-04,1738540800000000000,4,4,4,4,4,4,4,4",
    "6A:XF:AUDUSD:Q6,2025-02-04,1738540800000000000,5,5,5,5,5,5,5,5",
)


def test_a_file_of_spreads_reads_every_ticker_as_written(
    *,
    s3_session: dm.MassiveSession,
    fake_s3: FakeS3,
    load_fixture: Loader,
    make_csv_gz: Callable[[str], bytes],
) -> None:
    # Massive's own CBOT, NYMEX, and CME samples hold spreads beside single contracts; under
    # the default, an unreadable ticker would fail the read, so every one must be read.
    header = load_fixture(OWNER, "session_aggs_v1.csv").decode().splitlines()[0]
    day = date(2025, 2, 4)
    key = object_key("us_futures_{venue}/session_aggs_v1", day, "cbot")
    fake_s3.put(key, make_csv_gz("\n".join([header, *SPREAD_ROWS]) + "\n"))
    every = s3_session.futures.bars(dm.ALL, dm.Timespan.SESSION, venues={dm.FuturesVenue.CBOT})
    out = rows(every.history(day, day, source=dm.HistorySource.FLAT_FILES, wait=True))
    assert [r["Symbol"] for r in out] == [row.split(",")[0] for row in SPREAD_ROWS]
    # A spread is a symbol a dataset takes, and filters the same file.
    one = s3_session.futures.bars(
        "BOB 02-01 U6", dm.Timespan.SESSION, venues={dm.FuturesVenue.CBOT}
    )
    picked = rows(one.history(day, day, source=dm.HistorySource.FLAT_FILES, wait=True))
    assert [(r["Symbol"], r["Close"]) for r in picked] == [("BOB 02-01 U6", 3.0)]


def test_futures_datasets_take_contracts_and_spreads(massive_session: dm.MassiveSession) -> None:
    # A contract or a combo in Massive's shape, a form never seen before included; garbage is
    # refused, as dm.canonical_symbol refuses it.
    q = massive_session.futures.trades(["ESZ5", "GCQ6-GCZ6", "CL:BZ U6-V6", "ZZ:NEW COMBO Z6"])
    assert q._request.symbols == ("CL:BZ U6-V6", "ESZ5", "GCQ6-GCZ6", "ZZ:NEW COMBO Z6")
    for bad in (" ESZ5", "X:ESZ5", "", "esz5", "ESZ"):
        with pytest.raises(ValueError, match="ticker"):
            massive_session.futures.trades(bad)


def utc_ns(day: str) -> int:
    # 00:00 UTC on an ISO date, where Massive starts a futures bucket.
    return int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp()) * NS


def serve_buckets(fake_rest: FakeRestServer, buckets: list[tuple[str, str]]) -> None:
    # ESZ6 bars of a session or longer as Massive sent them on 2026-11-02: each bucket's
    # first UTC day and its label, the bucket's last session.
    results = [
        {"ticker": "ESZ6", "session_end_date": label, "window_start": utc_ns(day), "close": 1.0}
        for day, label in buckets
    ]
    fake_rest.reply("/futures/v1/aggs/ESZ6", Reply({"status": "OK", "results": results}))


AFTER_THE_LIVE_RUN = datetime(2026, 11, 2, 12, tzinfo=CHICAGO)
"""Monday 2026-11-02 at noon Chicago, when the live run read the bars these tests replay."""


def test_a_month_bar_by_date_needs_the_session_that_opens_on_its_last_day(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A session belongs to the day it opens, the evening before its date, so September 2026's
    # bar holds the sessions of 09-02 to 10-01 and is labelled 10-01.
    monkeypatch.setattr(history_source, "_now", lambda: AFTER_THE_LIVE_RUN)
    months = [
        ("2026-08-01", "2026-09-01"),
        ("2026-09-01", "2026-10-01"),
        ("2026-10-01", "2026-10-30"),
    ]
    serve_buckets(fake_rest, months)
    q = rest_session.futures.bars("ESZ6", dm.Timespan.MONTH)
    out = rows(q.history("2026-09-01", "2026-10-01", source=dm.HistorySource.REST, wait=True))
    assert [(r["TimestampStart"], r["TimestampEnd"], r["Date"]) for r in out] == [
        (utc_ns("2026-09-01"), chicago_open(date(2026, 10, 1)), None)
    ]
    [(_, query)] = fake_rest.get_queries()
    assert query["window_start.gte"] == [str(utc_ns("2026-09-01"))]
    assert query["window_start.lt"] == [str(chicago_open(date(2026, 10, 1)) + 3600 * NS)]
    # Ending on 09-30 leaves out September's last session, so no month bar fits.
    serve_buckets(fake_rest, months)
    assert (
        rows(q.history("2026-09-01", "2026-09-30", source=dm.HistorySource.REST, wait=True)) == []
    )


# The ESZ6 2-session bars Massive sent from 2026-08-01: each covers two calendar days, so the
# one of Thursday 08-13 holds Friday's session alone.
TWO_SESSIONS = [
    ("2026-08-01", "2026-08-03"),
    ("2026-08-03", "2026-08-05"),
    ("2026-08-05", "2026-08-07"),
    ("2026-08-09", "2026-08-11"),
    ("2026-08-11", "2026-08-13"),
    ("2026-08-13", "2026-08-14"),
    ("2026-08-15", "2026-08-17"),
]


def test_bars_of_several_sessions_by_date_keep_every_bucket_inside_the_range(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Sessions 2026-08-03 to 08-14: the bucket of Saturday 08-01 holds Monday's session alone,
    # so it is the first; the bucket of 08-15 holds Monday 08-17's, after the range.
    monkeypatch.setattr(history_source, "_now", lambda: AFTER_THE_LIVE_RUN)
    serve_buckets(fake_rest, TWO_SESSIONS)
    q = rest_session.futures.bars("ESZ6", dm.Timespan.SESSION, multiplier=2)
    out = rows(q.history("2026-08-03", "2026-08-14", source=dm.HistorySource.REST, wait=True))
    assert [(r["TimestampStart"], r["TimestampEnd"]) for r in out] == [
        (utc_ns(day), chicago_open(date.fromisoformat(label))) for day, label in TWO_SESSIONS[:6]
    ]
    assert {r["Date"] for r in out} == {None}
    # Requested from 00:00 UTC on the Saturday before the first Monday.
    [(_, query)] = fake_rest.get_queries()
    assert query["window_start.gte"] == [str(utc_ns("2026-08-01"))]


def test_bars_of_several_sessions_by_instants_keep_the_buckets_at_the_range_s_edges(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # From 00:00 UTC on 08-01 to 00:00 UTC on 08-15: the first bucket starts exactly at the
    # range's start, before its session's 17:00 Chicago open, and the last ends at 17:00
    # Chicago on 08-14, before the range's end; both are kept.
    monkeypatch.setattr(history_source, "_now", lambda: AFTER_THE_LIVE_RUN)
    serve_buckets(fake_rest, TWO_SESSIONS)
    q = rest_session.futures.bars("ESZ6", dm.Timespan.SESSION, multiplier=2)
    start, end = datetime(2026, 8, 1, tzinfo=UTC), datetime(2026, 8, 15, tzinfo=UTC)
    out = rows(q.history(start, end, source=dm.HistorySource.REST, wait=True))
    assert [r["TimestampStart"] for r in out] == [utc_ns(day) for day, _ in TWO_SESSIONS[:6]]


def test_a_range_of_instants_from_a_session_s_open_leaves_out_its_session_bar(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The session of 09-14 opens on Sunday 09-13 at 17:00 Chicago, but its 1-session bar starts
    # at 00:00 UTC that Sunday: a range from the open leaves it out, one from midnight keeps it.
    monkeypatch.setattr(history_source, "_now", lambda: AFTER_THE_LIVE_RUN)
    serve_buckets(fake_rest, [("2026-09-13", "2026-09-14"), ("2026-09-14", "2026-09-15")])
    q = rest_session.futures.bars("ESZ6", dm.Timespan.SESSION)
    end = datetime(2026, 9, 16, tzinfo=UTC)
    session_open = datetime(2026, 9, 13, 17, tzinfo=CHICAGO)
    from_open = rows(q.history(session_open, end, source=dm.HistorySource.REST, wait=True))
    assert [r["TimestampStart"] for r in from_open] == [utc_ns("2026-09-14")]
    midnight = datetime(2026, 9, 13, tzinfo=UTC)
    from_midnight = rows(q.history(midnight, end, source=dm.HistorySource.REST, wait=True))
    assert [r["TimestampStart"] for r in from_midnight] == [
        utc_ns("2026-09-13"),
        utc_ns("2026-09-14"),
    ]


def test_a_bar_whose_bucket_is_still_open_is_left_out(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The 2-month bars of ESZ6 as read on 2026-11-02: November and December's bucket holds only
    # the session of 11-02. A day later that session has closed, so the bar's own end has
    # passed and only the completion judgment can leave it out: later sessions may still join.
    monkeypatch.setattr(history_source, "_now", lambda: datetime(2026, 11, 3, 12, tzinfo=CHICAGO))
    buckets = [
        ("2026-07-01", "2026-09-01"),
        ("2026-09-01", "2026-10-30"),
        ("2026-11-01", "2026-11-02"),
    ]
    serve_buckets(fake_rest, buckets)
    q = rest_session.futures.bars("ESZ6", dm.Timespan.MONTH, multiplier=2)
    out = rows(q.history("2026-07-01", None, source=dm.HistorySource.REST, wait=True))
    assert [r["TimestampEnd"] for r in out] == [
        chicago_open(date(2026, 9, 1)),
        chicago_open(date(2026, 10, 30)),
    ]


def test_history_with_dates_refuses_a_session_end_date_filter(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # The factories take no filters; a query built with one (as the shared layers allow) is
    # refused, since the dates set session_end_date themselves.
    q = build_query(
        rest_session._core,
        futures.TRADES,
        query_type=HistoryQuery,
        symbols=["ESZ4"],
        filters={"session_end_date": "2024-12-17"},
    )
    with pytest.raises(ValueError, match=r"history\(\) dates set session_end_date themselves"):
        q.history("2024-12-17", "2024-12-17", source=dm.HistorySource.REST, wait=True)
    assert fake_rest.get_queries() == []


def test_the_current_state_datasets_revise_on_their_update_times() -> None:
    # An explicit APPEND poll appends each new state of a contract, or each status change.
    revised = {spec.name: spec.revision_keys for spec in futures.DATASETS if spec.revision_keys}
    assert revised == {
        "futures.snapshots": ("LastQuoteTimestamp", "LastTradeTimestamp"),
        "futures.market_status": ("Timestamp",),
    }
    for name, keys in revised.items():
        assert set(keys) <= set(CATALOG.dataset_schema(CATALOG.by_name[name]).column_names)


def test_no_futures_poll_key_is_nullable() -> None:
    # No vendor description says a key column of these datasets is left empty, and no fixture
    # leaves one empty: a null there fails the table.
    assert all(not spec.nullable_poll_keys for spec in futures.DATASETS)
