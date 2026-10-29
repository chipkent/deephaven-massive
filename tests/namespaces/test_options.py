"""Tests for the `m.options` namespace: its dataset specs, schemas, and factory methods.

The specs are checked against the generated sources through the catalog gates, their merged
schemas for the key columns, the payload fixtures (shaped from Massive's sample responses)
against the generated REST fields, and each factory for the request it builds and the errors
it raises at construction. Each dataset then runs end to end on every transport it has: the
exact REST requests through the fake REST server, one frame of each channel through the fake
WebSocket server into `blink()`, and one flat file of each prefix through the fake S3.
"""

from __future__ import annotations

import functools
import inspect
import json
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs, unquote, urlsplit

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import new_table
from deephaven.column import string_col
from deephaven.stream import blink_to_append_only

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import ChannelFamily, HistorySource
from deephaven_massive._massive import RestPath, Transport, source, to_batch
from deephaven_massive._namespaces import options
from deephaven_massive._namespaces.base import Namespace, query_class
from deephaven_massive._query import (
    HistoryLatestQuery,
    HistoryQuery,
    Query,
    QueryBase,
    StreamHistoryLatestQuery,
    StreamHistoryQuery,
    StreamQuery,
)
from deephaven_massive._session import CATALOG
from tests._fakes.clock import FakeClock

# The reply and envelope builders are plain functions of the fake, which no fixture can hand
# over; the server itself comes from the `fake_rest` fixture.
from tests._fakes.rest import Reply

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

    from deephaven.table import Table

    from tests._fakes.rest import FakeRestServer
    from tests._fakes.s3 import FakeS3
    from tests._fakes.ws_session import WsSession

R = Transport.REST
OWNER = "namespaces/options"
CONTRACT = "O:AAPL250117C00150000"
NAMES = (
    "trades",
    "quotes",
    "bars",
    "fmv",
    "chain",
    "snapshots",
    "contracts",
    "daily_open_close",
)


def request_of(query: QueryBase) -> dict[str, Any]:
    r = query._request
    return {"symbols": r.symbols, "params": dict(r.params), "filters": dict(r.filters)}


# --- Specs and the catalog -------------------------------------------------------------------


def test_namespace_is_reached_from_the_session(massive_session: dm.MassiveSession) -> None:
    namespace = massive_session.options
    assert type(namespace) is options._Options
    assert isinstance(namespace, Namespace)
    assert repr(namespace).endswith(" options>")


def test_every_dataset_is_declared_in_method_order() -> None:
    assert tuple(spec.name for spec in options.DATASETS) == tuple(f"options.{n}" for n in NAMES)
    assert all(CATALOG.by_name[spec.name] is spec for spec in options.DATASETS)
    assert all(spec.asset_class is dm.AssetClass.OPTIONS for spec in options.DATASETS)


def test_the_options_datasets_pass_every_catalog_gate() -> None:
    assert Catalog(options.DATASETS).problems() == ()


def test_each_dataset_reads_its_sources() -> None:
    assert {spec.name: spec.kind for spec in options.DATASETS} == {
        "options.trades": "trades",
        "options.quotes": "quotes",
        "options.bars": "bars",
        "options.fmv": "fmv",
        "options.chain": None,
        "options.snapshots": None,
        "options.contracts": None,
        "options.daily_open_close": None,
    }
    # Every dataset but the chain, which takes its underlying instead, takes contracts.
    takes = {spec.name for spec in options.DATASETS if CATALOG.takes_symbols(spec)}
    assert takes == {spec.name for spec in options.DATASETS} - {"options.chain"}
    assert options.TRADES.sources == (
        (R, "/v2/last/trade/{optionsTicker}"),
        (R, "/v3/trades/{optionsTicker}"),
        (Transport.WS, "options/T"),
        (Transport.FLAT_FILES, "us_options_opra/trades_v1"),
    )
    assert options.QUOTES.sources == (
        (R, "/v3/quotes/{optionsTicker}"),
        (Transport.WS, "options/Q"),
        (Transport.FLAT_FILES, "us_options_opra/quotes_v1"),
    )
    assert options.BARS.sources == (
        (R, "/v2/aggs/ticker/{optionsTicker}/prev"),
        (R, "/v2/aggs/ticker/{optionsTicker}/range/{multiplier}/{timespan}/{from}/{to}"),
        (Transport.WS, "options/A"),
        (Transport.WS, "options/AM"),
        (Transport.FLAT_FILES, "us_options_opra/minute_aggs_v1"),
        (Transport.FLAT_FILES, "us_options_opra/day_aggs_v1"),
    )
    assert options.FMV.sources == ((Transport.WS, "options/FMV"),)
    assert options.FMV.family is dm._enums.ChannelFamily.FMV
    assert options.CHAIN.sources == ((R, "/v3/snapshot/options/{underlyingAsset}"),)
    assert options.SNAPSHOTS.sources == ((R, "/v3/snapshot"),)
    assert options.CONTRACTS.sources == (
        (R, "/v3/reference/options/contracts"),
        (R, "/v3/reference/options/contracts/{options_ticker}"),
    )
    assert options.DAILY_OPEN_CLOSE.sources == ((R, "/v1/open-close/{optionsTicker}/{date}"),)


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (options.TRADES, StreamHistoryLatestQuery),
        (options.QUOTES, StreamHistoryQuery),
        (options.BARS, HistoryQuery),
        (options.FMV, StreamQuery),
        (options.CHAIN, Query),
        (options.SNAPSHOTS, Query),
        (options.CONTRACTS, Query),
        (options.DAILY_OPEN_CLOSE, Query),
    ],
)
def test_query_class_of_each_dataset(spec: DatasetSpec, expected: type) -> None:
    assert query_class(spec) is expected


def test_page_sizes_are_the_largest_each_endpoint_returns() -> None:
    sizes = {spec.name: spec.page_size for spec in options.DATASETS}
    assert sizes == {
        "options.trades": 50_000,
        "options.quotes": 50_000,
        "options.bars": 50_000,
        "options.fmv": None,
        "options.chain": 250,
        "options.snapshots": 250,
        "options.contracts": 1_000,
        "options.daily_open_close": None,
    }


# --- Schemas ---------------------------------------------------------------------------------


@pytest.mark.parametrize("spec", [options.TRADES, options.QUOTES, options.BARS, options.FMV])
def test_kind_datasets_lead_with_symbol_and_an_instant_timestamp(spec: DatasetSpec) -> None:
    schema = CATALOG.dataset_schema(spec)
    assert schema.column_names[:2] == ("Symbol", "Timestamp")
    types = {c.name: c.type for c in schema.columns}
    assert types["Symbol"] is ColumnType.STRING
    assert types["Timestamp"] is ColumnType.INSTANT
    assert "Underlying" not in types
    assert "Ticker" not in types


def test_trades_merge_the_rest_stream_and_flat_file_columns() -> None:
    names = CATALOG.dataset_schema(options.TRADES).column_names
    # SipTimestamp became Timestamp; the participant and TRF times stay their own columns.
    assert "SipTimestamp" not in names
    assert {"Price", "Size", "Exchange", "Conditions", "ParticipantTimestamp"} <= set(names)
    types = {c.name: c.type for c in CATALOG.dataset_schema(options.TRADES).columns}
    assert types["Conditions"] is ColumnType.INT_ARRAY
    assert types["Size"] is ColumnType.DOUBLE  # the WebSocket's integer joins the REST number


def test_bars_key_on_the_window_start() -> None:
    names = CATALOG.dataset_schema(options.BARS).column_names
    assert "WindowStart" not in names
    assert {"Open", "High", "Low", "Close", "Volume", "Vwap"} <= set(names)


@pytest.mark.parametrize(
    ("spec", "gone"),
    [
        (options.CHAIN, {"DetailsTicker", "UnderlyingAssetTicker"}),
        (options.SNAPSHOTS, {"Ticker", "UnderlyingAssetTicker"}),
        (options.CONTRACTS, {"Ticker", "UnderlyingTicker"}),
    ],
)
def test_contract_rows_carry_symbol_and_underlying(spec: DatasetSpec, gone: set[str]) -> None:
    schema = CATALOG.dataset_schema(spec)
    names = schema.column_names
    assert names[0] == "Symbol"
    assert "Underlying" in names
    assert "Timestamp" not in names
    assert not gone & set(names)
    types = {c.name: c.type for c in schema.columns}
    assert types["Underlying"] is ColumnType.STRING


def test_snapshots_carry_the_greeks_and_the_per_ticker_error() -> None:
    names = set(CATALOG.dataset_schema(options.SNAPSHOTS).column_names)
    assert {"GreeksDelta", "GreeksGamma", "GreeksTheta", "GreeksVega"} <= names
    assert {"ImpliedVolatility", "OpenInterest", "Error", "Message"} <= names


def test_daily_open_close_has_symbol_and_its_day() -> None:
    schema = CATALOG.dataset_schema(options.DAILY_OPEN_CLOSE)
    types = {c.name: c.type for c in schema.columns}
    assert schema.column_names[0] == "Symbol"
    assert types["From"] is ColumnType.LOCAL_DATE
    assert "Timestamp" not in types


# --- Fixtures --------------------------------------------------------------------------------


def required_keys(path: str) -> tuple[str, ...]:
    # The symbol and time key columns the options datasets read from this path's own rows.
    found = {
        key.column
        for spec in options.DATASETS
        for role, key in spec.keys_for(R, path).declared()
        if not key.from_request and role in {"symbol", "time"}
    }
    return tuple(sorted(found))


def test_required_keys_are_the_row_keys_of_each_path() -> None:
    assert required_keys("/v3/trades/{optionsTicker}") == ("SipTimestamp",)
    assert required_keys("/v2/last/trade/{optionsTicker}") == ("SipTimestamp", "Ticker")
    assert required_keys("/v3/snapshot/options/{underlyingAsset}") == ("DetailsTicker",)


@pytest.mark.parametrize(
    ("name", "path"),
    [
        ("last_trade.json", "/v2/last/trade/{optionsTicker}"),
        ("trades.json", "/v3/trades/{optionsTicker}"),
        ("quotes.json", "/v3/quotes/{optionsTicker}"),
        ("prev.json", "/v2/aggs/ticker/{optionsTicker}/prev"),
        ("range.json", "/v2/aggs/ticker/{optionsTicker}/range/{multiplier}/{timespan}/{from}/{to}"),
        ("chain.json", "/v3/snapshot/options/{underlyingAsset}"),
        ("snapshot.json", "/v3/snapshot"),
        ("contracts.json", "/v3/reference/options/contracts"),
        ("contract.json", "/v3/reference/options/contracts/{options_ticker}"),
        ("open_close.json", "/v1/open-close/{optionsTicker}/{date}"),
    ],
)
def test_fixtures_are_shaped_like_the_generated_fields(
    load_fixture: Callable[[str, str], bytes], name: str, path: str
) -> None:
    required = required_keys(path)
    body = json.loads(load_fixture(OWNER, name))
    rest = cast("RestPath", source(R, path))
    if rest.result_key is None:  # the body is the row; its envelope keys are not fields
        rows = [{k: v for k, v in body.items() if k not in {"status", "request_id"}}]
    else:
        found = body[rest.result_key]
        rows = found if isinstance(found, list) else [found]
    batch, drift = to_batch(rows, rest, required=required)
    assert batch.num_rows == len(rows)
    assert drift.extra == frozenset()


def test_the_snapshot_fixture_has_a_per_ticker_error_row(
    load_fixture: Callable[[str, str], bytes],
) -> None:
    rows = json.loads(load_fixture(OWNER, "snapshot.json"))["results"]
    rest = cast("RestPath", source(R, "/v3/snapshot"))
    batch, _ = to_batch(rows, rest, required=("Ticker",))
    table = batch.to_pydict()
    assert table["Ticker"] == ["O:NCLH221014C00005000", "O:NCLH221014C00999000"]
    assert table["UnderlyingAssetTicker"] == ["NCLH", None]
    assert table["Error"] == [None, "NOT_FOUND"]


# --- Factories -------------------------------------------------------------------------------


def test_trades_quotes_and_fmv_normalize_occ_symbols(massive_session: dm.MassiveSession) -> None:
    m = massive_session
    for factory, cls in (
        (m.options.trades, StreamHistoryLatestQuery),
        (m.options.quotes, StreamHistoryQuery),
        (m.options.fmv, StreamQuery),
    ):
        q = factory(["AAPL250117C00150000", "o:aapl250117p00150000"])
        assert type(q) is cls
        assert q._request.symbols == ("O:AAPL250117C00150000", "O:AAPL250117P00150000")
        assert factory(dm.ALL)._request.symbols is dm.ALL
        with pytest.raises(ValueError, match="is not an OCC option symbol"):
            factory("AAPL")
        with pytest.raises(ValueError, match="carries the prefix of another asset class"):
            factory("X:BTCUSD")


def test_a_symbol_table_is_followed(massive_session: dm.MassiveSession) -> None:
    watch = new_table([string_col("Symbol", [CONTRACT])])
    q = massive_session.options.trades(watch)
    assert isinstance(q._request.symbols, SymbolTable)
    with pytest.raises(ValueError, match="must have a Symbol column"):
        massive_session.options.trades(new_table([string_col("Ticker", [CONTRACT])]))


def test_bars_pick_their_class_from_the_timespan(massive_session: dm.MassiveSession) -> None:
    bars = massive_session.options.bars
    assert type(bars(CONTRACT)) is StreamHistoryQuery
    assert type(bars(CONTRACT, dm.Timespan.SECOND)) is StreamHistoryQuery
    assert type(bars(CONTRACT, dm.Timespan.DAY)) is HistoryLatestQuery
    assert type(bars(CONTRACT, dm.Timespan.MINUTE, multiplier=5)) is HistoryQuery
    assert type(bars(CONTRACT, dm.Timespan.HOUR)) is HistoryQuery
    q = bars(CONTRACT, dm.Timespan.DAY, adjusted=False)
    assert (q._request.timespan, q._request.multiplier) == (dm.Timespan.DAY, 1)
    assert q._request.adjusted is False
    assert bars(CONTRACT)._request.adjusted is True
    with pytest.raises(ValueError, match="SESSION exists only for futures"):
        bars(CONTRACT, dm.Timespan.SESSION)
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan"):
        bars(CONTRACT, cast("dm.Timespan", "minute"))
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        bars(CONTRACT, adjusted=cast("bool", 1))


def test_chain_sends_its_underlying_and_contract_type(massive_session: dm.MassiveSession) -> None:
    q = massive_session.options.chain(
        " aapl ",
        contract_type=dm.ContractType.PUT,
        sort="strike_price",
        order=dm.Order.DESC,
        limit=10,
        filters={"strike_price_gte": 150, "expiration_date_lte": "2025-01-17"},
    )
    assert type(q) is Query
    assert request_of(q) == {
        "symbols": None,
        "params": {
            "contract_type": dm.ContractType.PUT,
            "order": dm.Order.DESC,
            "sort": "strike_price",
            "underlyingAsset": "AAPL",
        },
        "filters": {"expiration_date_lte": date(2025, 1, 17), "strike_price_gte": 150},
    }
    assert q._request.limit == 10
    assert request_of(massive_session.options.chain("SPY"))["params"] == {"underlyingAsset": "SPY"}


def test_chain_rejects_bad_arguments(massive_session: dm.MassiveSession) -> None:
    chain = massive_session.options.chain
    with pytest.raises(TypeError, match="underlying must be a str, not _All"):
        chain(cast("str", dm.ALL))
    with pytest.raises(ValueError, match="underlying must not be empty"):
        chain("  ")
    with pytest.raises(TypeError, match=r"contract_type must be dm\.ContractType"):
        chain("AAPL", contract_type=cast("dm.ContractType", "call"))
    with pytest.raises(ValueError, match=r"^contract_type is set through its own argument"):
        chain("AAPL", filters={"contract_type": "call"})
    with pytest.raises(ValueError, match=r"options\.chain has no filter 'ticker'"):
        chain("AAPL", filters={"ticker": "x"})


def test_snapshots_restrict_the_unified_snapshot_to_options(
    massive_session: dm.MassiveSession,
) -> None:
    snapshots = massive_session.options.snapshots
    q = snapshots([CONTRACT], filters={"ticker_gte": "O:A"}, limit=500)
    assert type(q) is Query
    assert request_of(q) == {
        "symbols": (CONTRACT,),
        "params": {"type": "options"},
        "filters": {"ticker_gte": "O:A"},
    }
    assert snapshots(dm.ALL)._request.symbols is dm.ALL
    with pytest.raises(ValueError, match=r"^type is set through its own argument"):
        snapshots(dm.ALL, filters={"type": "stocks"})
    with pytest.raises(ValueError, match=r"^ticker_any_of is set through symbols=, not"):
        snapshots(dm.ALL, filters={"ticker_any_of": [CONTRACT]})


def test_contracts_list_or_describe(massive_session: dm.MassiveSession) -> None:
    contracts = massive_session.options.contracts
    listed = contracts(
        underlying="nvda",
        contract_type=dm.ContractType.CALL,
        as_of="2025-01-02",
        expired=False,
        filters={"strike_price_gte": 100, "expiration_date_lte": date(2026, 1, 16)},
    )
    assert type(listed) is Query
    assert request_of(listed) == {
        "symbols": None,
        "params": {
            "as_of": date(2025, 1, 2),
            "contract_type": dm.ContractType.CALL,
            "expired": False,
            "underlying_ticker": "NVDA",
        },
        "filters": {"expiration_date_lte": date(2026, 1, 16), "strike_price_gte": 100},
    }
    described = contracts(symbols=[CONTRACT])
    assert request_of(described) == {"symbols": (CONTRACT,), "params": {}, "filters": {}}
    assert request_of(contracts()) == {"symbols": None, "params": {}, "filters": {}}


def test_contracts_reject_bad_arguments(massive_session: dm.MassiveSession) -> None:
    contracts = massive_session.options.contracts
    with pytest.raises(ValueError, match=r"use m\.options\.contracts\(\) without symbols"):
        contracts(symbols=dm.ALL)
    with pytest.raises(TypeError, match="expired must be a bool, not int"):
        contracts(expired=cast("bool", 1))
    with pytest.raises(ValueError, match="needs a timezone"):
        contracts(as_of=datetime(2025, 1, 2, 9, 30, tzinfo=UTC).replace(tzinfo=None))
    for key in (
        "underlying_ticker",
        "contract_type",
        "as_of_gte",
        "expired",
    ):
        with pytest.raises(ValueError, match=f"^{key} is set through its own argument"):
            contracts(filters={key: "x"})
    with pytest.raises(ValueError, match=r"^ticker is set through symbols=, not filters"):
        contracts(filters={"ticker": CONTRACT})


def test_as_of_and_date_refuse_a_time(massive_session: dm.MassiveSession) -> None:
    # Both are calendar dates: an instant would name a day only through a zone.
    late = datetime(2025, 1, 3, 2, 0, tzinfo=UTC)
    with pytest.raises(ValueError, match=r"^options\.contracts: as_of is a calendar date"):
        massive_session.options.contracts(as_of=late)
    with pytest.raises(ValueError, match=r"^options\.daily_open_close: date is a calendar date"):
        massive_session.options.daily_open_close(CONTRACT, late)
    q = massive_session.options.contracts(as_of="2025-01-02")
    assert dict(q._request.params) == {"as_of": date(2025, 1, 2)}


def test_daily_open_close_takes_explicit_contracts(massive_session: dm.MassiveSession) -> None:
    daily = massive_session.options.daily_open_close
    q = daily("AAPL250117C00150000", date(2025, 1, 2))
    assert type(q) is Query
    assert request_of(q) == {
        "symbols": (CONTRACT,),
        "params": {"date": date(2025, 1, 2)},
        "filters": {},
    }
    assert q._request.adjusted is True
    assert daily(CONTRACT, "2025-01-02", adjusted=False)._request.adjusted is False
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        daily(CONTRACT, "2025-01-02", adjusted=cast("bool", 0))
    with pytest.raises(ValueError, match=r"use m\.options\.snapshots\(dm\.ALL\)"):
        daily(dm.ALL, "2025-01-02")
    with pytest.raises(TypeError):
        daily(CONTRACT, cast("str", 20250102))


def test_enum_arguments_reject_plain_strings(massive_session: dm.MassiveSession) -> None:
    m = massive_session
    call, desc = cast("dm.ContractType", "call"), cast("dm.Order", "desc")
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan"):
        m.options.bars(CONTRACT, cast("dm.Timespan", "day"))
    with pytest.raises(TypeError, match=r"contract_type must be dm\.ContractType"):
        m.options.chain("AAPL", contract_type=call)
    with pytest.raises(TypeError, match=r"contract_type must be dm\.ContractType"):
        m.options.contracts(contract_type=call)
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        m.options.chain("AAPL", order=desc)
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        m.options.snapshots(CONTRACT, order=desc)
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        m.options.contracts(order=desc)


# --- Docstrings ------------------------------------------------------------------------------


def _factory(name: str) -> Callable[..., object]:
    return cast("Callable[..., object]", getattr(options._Options, name))


@pytest.mark.parametrize("name", NAMES)
def test_docstrings_link_their_column_reference_section(name: str) -> None:
    doc = inspect.getdoc(_factory(name)) or ""
    link = (
        f"https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.md#{name}"
    )
    assert link in doc
    assert "columns()" in doc
    assert "dm.ALL" in doc
    assert "limit" in doc


def test_every_factory_has_a_dataset() -> None:
    public = {
        name
        for name, member in vars(options._Options).items()
        if callable(member) and not name.startswith("_")
    }
    assert public == set(NAMES)


def test_typed_filters_are_left_alone_when_no_filters_are_given(
    massive_session: dm.MassiveSession,
) -> None:
    # An empty mapping sets nothing, like None.
    empty: Mapping[str, Any] = {}
    assert request_of(massive_session.options.chain("AAPL", filters=empty))["filters"] == {}
    # A filters value that is no mapping gets the factory's TypeError, not a key complaint.
    with pytest.raises(TypeError, match="filters must be a mapping, not list"):
        massive_session.options.chain("AAPL", filters=cast("Mapping[str, Any]", ["contract_type"]))


# --- End to end: REST ------------------------------------------------------------------------

TSLA = "O:TSLA210903C00700000"
NS = 10**9


@pytest.fixture
def rest_session(
    fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> Iterator[dm.MassiveSession]:
    # A session on the fake REST server; its retry waits are on the `rest_clock` fake clock.
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    yield m
    m.close()


def wire(path: str) -> str:
    # A path as the client sends it: the colon of an `O:` symbol is percent-encoded.
    return path.replace(":", "%3A")


def body(load_fixture: Callable[[str, str], bytes], name: str) -> object:
    return json.loads(load_fixture(OWNER, name))


def sent(server: FakeRestServer) -> list[tuple[str, dict[str, list[str]]]]:
    # Every GET the server received: its decoded path and query.
    return [
        (unquote(urlsplit(r.target).path), parse_qs(urlsplit(r.target).query))
        for r in server.requests
        if r.method == "GET"
    ]


def rows(table: Table) -> list[dict[str, Any]]:
    # Instants become epoch nanoseconds, so assertions keep every digit.
    data = dh_arrow.to_arrow(table)
    columns = [c.cast(pa.int64()) if pa.types.is_timestamp(c.type) else c for c in data.columns]
    out: list[dict[str, Any]] = pa.Table.from_arrays(columns, names=data.column_names).to_pylist()
    return out


def test_trades_fetch_reads_the_last_trade(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply(wire(f"/v2/last/trade/{TSLA}"), Reply(body(load_fixture, "last_trade.json")))
    out = rows(rest_session.options.trades("TSLA210903C00700000").fetch())
    assert sent(fake_rest) == [(f"/v2/last/trade/{TSLA}", {})]
    assert len(out) == 1
    row = out[0]
    assert (row["Symbol"], row["Timestamp"], row["Price"]) == (TSLA, 1617901342969834000, 115.55)
    assert (row["Size"], row["Exchange"], row["Conditions"]) == (25.0, 312, [227])
    assert row["ParticipantTimestamp"] == 1617901342969834000
    with pytest.raises(ValueError, match=r"use m\.options\.snapshots\(dm\.ALL\)\.fetch\(\)"):
        rest_session.options.trades(dm.ALL).fetch()
    assert len(sent(fake_rest)) == 1


def test_trades_and_quotes_history_read_a_day_over_rest(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    contract = "O:AZO140621P00530000"
    fake_rest.reply(wire(f"/v3/trades/{contract}"), Reply(body(load_fixture, "trades.json")))
    day = date(2014, 6, 2)
    midnight = int(datetime(2014, 6, 2, 4, tzinfo=UTC).timestamp()) * NS  # New York midnight
    trades = rows(
        rest_session.options.trades(contract).history(day, day, source=HistorySource.REST)
    )
    assert sent(fake_rest) == [
        (
            f"/v3/trades/{contract}",
            {
                "timestamp.gte": [str(midnight)],
                "timestamp.lt": [str(midnight + 86_400 * NS)],
                "limit": ["50000"],
                "order": ["asc"],  # history() asks for the oldest rows first
                "sort": ["timestamp"],
            },
        )
    ]
    assert [(r["Symbol"], r["Timestamp"], r["Price"], r["Conditions"]) for r in trades] == [
        (contract, 1401715883806000000, 6.91, None),
        (contract, 1401716547786000000, 7.2, [209]),
    ]

    quote_contract = "O:SPY241220P00720000"
    fake_rest.reply(wire(f"/v3/quotes/{quote_contract}"), Reply(body(load_fixture, "quotes.json")))
    day = date(2022, 2, 17)
    quotes = rows(
        rest_session.options.quotes(quote_contract).history(day, day, source=HistorySource.REST)
    )
    path, query = sent(fake_rest)[-1]
    assert path == f"/v3/quotes/{quote_contract}"
    assert query["limit"] == ["50000"]
    # The rows are newest first, as the vendor sample gives them; history keeps server order.
    assert [(r["Symbol"], r["Timestamp"], r["BidPrice"], r["AskSize"]) for r in quotes] == [
        (quote_contract, 1645119125346243600, 277.5, 10.0),
        (quote_contract, 1645119118474271000, 277.5, 1.0),
    ]


def test_bars_fetch_the_previous_session_and_read_a_range(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply(wire(f"/v2/aggs/ticker/{TSLA}/prev"), Reply(body(load_fixture, "prev.json")))
    prev = rows(rest_session.options.bars(TSLA, dm.Timespan.DAY).fetch())
    assert sent(fake_rest) == [(f"/v2/aggs/ticker/{TSLA}/prev", {"adjusted": ["true"]})]
    assert [(r["Symbol"], r["Timestamp"], r["Close"], r["Volume"]) for r in prev] == [
        (TSLA, 1605042000000 * 10**6, 115.97, 131704427.0)
    ]

    contract = "O:RDFN211119C00025000"
    start = int(datetime(2021, 9, 23, 4, tzinfo=UTC).timestamp()) * 1000  # New York midnight
    end = int(datetime(2021, 9, 25, 4, tzinfo=UTC).timestamp()) * 1000 - 1
    path = f"/v2/aggs/ticker/{contract}/range/1/day/{start}/{end}"
    fake_rest.reply(wire(path), Reply(body(load_fixture, "range.json")))
    bars = rows(
        rest_session.options.bars(contract, dm.Timespan.DAY, adjusted=False).history(
            date(2021, 9, 23), date(2021, 9, 24), source=HistorySource.REST
        )
    )
    assert sent(fake_rest)[-1] == (
        path,
        {"adjusted": ["false"], "limit": ["50000"], "sort": ["asc"]},
    )
    assert [(r["Symbol"], r["Timestamp"], r["Close"]) for r in bars] == [
        (contract, 1632369600000 * 10**6, 26.2),
        (contract, 1632456000000 * 10**6, 28.3),
    ]


def test_chain_reads_one_underlying(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply("/v3/snapshot/options/AAPL", Reply(body(load_fixture, "chain.json")))
    out = rows(
        rest_session.options.chain(
            "aapl",
            contract_type=dm.ContractType.CALL,
            sort="strike_price",
            order=dm.Order.ASC,
            filters={"expiration_date_lte": date(2022, 1, 21), "strike_price_gte": 150},
        ).fetch()
    )
    assert sent(fake_rest) == [
        (
            "/v3/snapshot/options/AAPL",
            {
                "contract_type": ["call"],
                "sort": ["strike_price"],
                "order": ["asc"],
                "expiration_date.lte": ["2022-01-21"],
                "strike_price.gte": ["150"],
                "limit": ["250"],
            },
        )
    ]
    (row,) = out
    assert (row["Symbol"], row["Underlying"]) == ("O:AAPL220121C00150000", "AAPL")
    assert (row["DetailsStrikePrice"], row["GreeksTheta"], row["OpenInterest"]) == (
        150.0,
        0.00229,
        1543.0,
    )
    assert row["DetailsExpirationDate"] == date(2022, 1, 21)
    assert row["DayLastUpdated"] == 1605195918507251700


def test_snapshots_ask_for_options_250_contracts_a_request(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply("/v3/snapshot", Reply(body(load_fixture, "snapshot.json")))
    contracts = [f"O:NCLH221014C{strike:08d}" for strike in range(1000, 1000 + 251)]
    rest_session.options.snapshots(contracts).fetch()
    requests = sent(fake_rest)
    # The two requests run concurrently, so they arrive in either order.
    chunks = sorted((q["ticker.any_of"][0].split(",") for _, q in requests), key=len)
    assert chunks == [contracts[-1:], contracts[:250]]
    assert all(q["type"] == ["options"] and q["limit"] == ["250"] for _, q in requests)


def test_snapshots_keep_the_error_row_of_a_contract(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply("/v3/snapshot", Reply(body(load_fixture, "snapshot.json")))
    out = rows(rest_session.options.snapshots(dm.ALL).fetch())
    assert sent(fake_rest) == [("/v3/snapshot", {"type": ["options"], "limit": ["250"]})]
    found, missing = out
    assert (found["Symbol"], found["Underlying"], found["Error"]) == (
        "O:NCLH221014C00005000",
        "NCLH",
        None,
    )
    assert found["GreeksDelta"] == 0.5520187372272933
    assert found["LastTradeSipTimestamp"] == 1675280958783136800
    assert (missing["Symbol"], missing["Underlying"]) == ("O:NCLH221014C00999000", None)
    assert (missing["Error"], missing["Message"]) == ("NOT_FOUND", "Ticker not found.")


def test_contracts_list_or_read_each_contract(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply("/v3/reference/options/contracts", Reply(body(load_fixture, "contracts.json")))
    listed = rows(
        rest_session.options.contracts(
            underlying="aapl",
            contract_type=dm.ContractType.CALL,
            as_of=date(2021, 11, 1),
            expired=False,
            sort="strike_price",
            order=dm.Order.DESC,
            filters={"strike_price_lte": 90},
        ).fetch()
    )
    assert sent(fake_rest) == [
        (
            "/v3/reference/options/contracts",
            {
                "underlying_ticker": ["AAPL"],
                "contract_type": ["call"],
                "as_of": ["2021-11-01"],
                "expired": ["false"],
                "sort": ["strike_price"],
                "order": ["desc"],
                "strike_price.lte": ["90"],
                "limit": ["1000"],
            },
        )
    ]
    assert [(r["Symbol"], r["Underlying"], r["StrikePrice"]) for r in listed] == [
        ("O:AAPL211119C00085000", "AAPL", 85.0),
        ("O:AAPL211119C00090000", "AAPL", 90.0),
    ]
    assert listed[0]["AdditionalUnderlyings"] is None
    assert json.loads(listed[1]["AdditionalUnderlyings"])[0]["underlying"] == "VMW"

    one = "O:AAPL211119C00085000"
    fake_rest.reply(
        wire(f"/v3/reference/options/contracts/{one}"), Reply(body(load_fixture, "contract.json"))
    )
    described = rows(rest_session.options.contracts(symbols=one, as_of="2021-11-01").fetch())
    assert sent(fake_rest)[-1] == (
        f"/v3/reference/options/contracts/{one}",
        {"as_of": ["2021-11-01"]},
    )
    assert [(r["Symbol"], r["Underlying"], r["ExpirationDate"]) for r in described] == [
        (one, "AAPL", date(2021, 11, 19))
    ]


def test_a_list_argument_with_named_contracts_fails_before_any_request(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # The per-contract path takes only `as_of`; a list-only argument cannot be honored there.
    q = rest_session.options.contracts(symbols=[CONTRACT], underlying="AAPL")
    with pytest.raises(ValueError, match=r"does not take \['underlying_ticker'\]"):
        q.fetch()
    assert sent(fake_rest) == []


def test_daily_open_close_reads_one_day_per_contract(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    path = f"/v1/open-close/{TSLA}/2023-01-09"
    fake_rest.reply(wire(path), Reply(body(load_fixture, "open_close.json")))
    out = rows(
        rest_session.options.daily_open_close(TSLA, date(2023, 1, 9), adjusted=False).fetch()
    )
    assert sent(fake_rest) == [(path, {"adjusted": ["false"]})]
    assert [(r["Symbol"], r["From"], r["Open"], r["Close"], r["AfterHours"]) for r in out] == [
        (TSLA, date(2023, 1, 9), 25.0, 26.35, 26.35)
    ]


# --- End to end: WebSocket -------------------------------------------------------------------

SPY = "O:SPY251219C00650000"
TRADE_FRAME = {
    "ev": "T",
    "sym": SPY,
    "x": 65,
    "p": 1.54,
    "s": 1,
    "c": [233],
    "t": 1629820676333,
    "q": 651921857,
}
QUOTE_FRAME = {
    "ev": "Q",
    "sym": SPY,
    "bx": 302,
    "ax": 302,
    "bp": 9.71,
    "ap": 9.81,
    "bs": 17,
    "as": 24,
    "t": 1644506128351,
    "q": 844090872,
}


def bar_frame(event: str) -> dict[str, object]:
    return {
        "ev": event,
        "sym": SPY,
        "v": 2,
        "av": 8,
        "op": 2.2,
        "vw": 2.05,
        "o": 2.05,
        "c": 2.05,
        "h": 2.05,
        "l": 2.05,
        "a": 2.1312,
        "z": 2,
        "s": 1632419640000,
        "e": 1632419700000,
    }


@pytest.mark.parametrize(
    ("dataset", "topic", "frame", "expected"),
    [
        ("trades", "T", TRADE_FRAME, {"Timestamp": 1629820676333 * 10**6, "Price": 1.54}),
        ("quotes", "Q", QUOTE_FRAME, {"Timestamp": 1644506128351 * 10**6, "BidPrice": 9.71}),
        ("bars-second", "A", bar_frame("A"), {"Timestamp": 1632419640000 * 10**6, "Close": 2.05}),
        ("bars-minute", "AM", bar_frame("AM"), {"Timestamp": 1632419640000 * 10**6, "Vwap": 2.05}),
        (
            "fmv",
            "FMV",
            {"ev": "FMV", "fmv": 7.2, "sym": SPY, "t": 1401715883806000000},
            {"Timestamp": 1401715883806000000, "Fmv": 7.2},
        ),
    ],
)
def test_a_frame_of_each_channel_lands_in_blink(
    *,
    ws_session: WsSession,
    rows_when: Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]],
    dataset: str,
    topic: str,
    frame: dict[str, object],
    expected: dict[str, Any],
) -> None:
    m = ws_session.session
    queries: dict[str, Callable[[], Any]] = {
        "trades": lambda: m.options.trades(SPY),
        "quotes": lambda: m.options.quotes(SPY),
        "bars-second": lambda: m.options.bars(SPY, dm.Timespan.SECOND),
        "bars-minute": lambda: m.options.bars(SPY),
        "fmv": lambda: m.options.fmv(SPY),
    }
    blink = queries[dataset]().blink()
    seen = blink_to_append_only(blink)
    ws_session.subscribed(f"{topic}.{SPY}")
    ws_session.publish(frame)
    # Wait on a consistent snapshot, never on `size`: a size read in the middle of an update
    # cycle can count a row that a snapshot taken right after does not hold yet. The view is
    # append-only, so once a snapshot holds the row, every later one does too.
    rows_when(seen, lambda arrived: len(arrived) == 1)
    (row,) = rows(seen)
    assert row["Symbol"] == SPY
    assert {name: row[name] for name in expected} == expected
    family = ChannelFamily.FMV if topic == "FMV" else ChannelFamily.TICK
    assert ws_session.decisions == [(dm.AssetClass.OPTIONS, family)]
    m.release(blink)


# --- End to end: flat files ------------------------------------------------------------------


@pytest.fixture
def s3_session(fake_s3: FakeS3, monkeypatch: pytest.MonkeyPatch) -> Iterator[dm.MassiveSession]:
    # A session with S3 credentials, on a Monday after the March 13, 2023 files are published,
    # whose flat-file retries never sleep.
    monkeypatch.setattr(history_source, "_now", lambda: datetime(2023, 3, 14, 16, tzinfo=UTC))
    monkeypatch.setitem(
        vars(_massive),
        "FlatFileClient",
        functools.partial(_massive.FlatFileClient, clock=FakeClock()),
    )
    fields: dict[str, object] = {
        "api_key": "test-key",
        "s3_access_key_id": "AKIDFLATFILES",
        "s3_secret_access_key": "flat-file-secret",
    }
    m = dm.MassiveSession(dm.MassiveConfig(**cast("dict[str, Any]", fields)))
    yield m
    m.close()


DAY = date(2023, 3, 13)
PUT = "O:SPY230327P00390000"
CALL = "O:IWM230327C00137000"


@pytest.mark.parametrize(
    ("prefix", "header", "lines", "build", "expected"),
    [
        (
            "us_options_opra/trades_v1",
            "ticker,conditions,correction,exchange,price,sip_timestamp,size",
            [
                f"{PUT},232,,312,11.82,1678715620948000000,1",
                f"{CALL},209,,325,11.07,1678716040304000000,15",
            ],
            lambda m: m.options.trades(PUT),
            {"Timestamp": 1678715620948000000, "Price": 11.82, "Conditions": [232]},
        ),
        (
            "us_options_opra/quotes_v1",
            (
                "ticker,ask_exchange,ask_price,ask_size,bid_exchange,bid_price,bid_size,"
                "sequence_number,sip_timestamp"
            ),
            [
                f"{PUT},309,326.77,1,309,321.77,1,81779,1678715620948000000",
                f"{CALL},303,328.28,1,303,318.28,2,152697,1678716040304000000",
            ],
            lambda m: m.options.quotes(PUT),
            {"Timestamp": 1678715620948000000, "AskPrice": 326.77, "SequenceNumber": 81779},
        ),
        (
            "us_options_opra/minute_aggs_v1",
            "ticker,volume,open,close,high,low,window_start,transactions",
            [
                f"{PUT},1,11.82,11.82,11.82,11.82,1678715580000000000,1",
                f"{CALL},15,11.07,11.07,11.07,11.07,1678716000000000000,1",
            ],
            lambda m: m.options.bars(PUT, adjusted=False),
            {"Timestamp": 1678715580000000000, "Close": 11.82, "Transactions": 1},
        ),
        (
            "us_options_opra/day_aggs_v1",
            "ticker,volume,open,close,high,low,window_start,transactions",
            [
                f"{PUT},200,11.82,9.83,11.82,8.09,1678680000000000000,62",
                f"{CALL},1,35.77,35.77,35.77,35.77,1678680000000000000,1",
            ],
            lambda m: m.options.bars(PUT, dm.Timespan.DAY, adjusted=False),
            {"Timestamp": 1678680000000000000, "Close": 9.83, "Volume": 200.0},
        ),
    ],
)
def test_a_flat_file_of_each_prefix_round_trips(
    *,
    s3_session: dm.MassiveSession,
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
    prefix: str,
    header: str,
    lines: list[str],
    build: Callable[[dm.MassiveSession], Any],
    expected: dict[str, Any],
) -> None:
    key = f"{prefix}/2023/03/2023-03-13.csv.gz"
    fake_s3.put(key, make_csv_gz("\n".join([header, *lines]) + "\n"))
    out = rows(build(s3_session).history(DAY, DAY, source=HistorySource.FLAT_FILES))
    assert fake_s3.requests == [("flatfiles", key)]
    (row,) = out  # the other contract's row is filtered out
    assert row["Symbol"] == PUT
    assert {name: row[name] for name in expected} == expected


def test_the_snapshots_revise_on_a_new_quote_or_trade() -> None:
    # An explicit APPEND poll of a snapshot appends each new state of a contract.
    revision = ("LastQuoteLastUpdated", "LastTradeSipTimestamp")
    revised = {spec.name: spec.revision_keys for spec in options.DATASETS if spec.revision_keys}
    assert revised == {"options.chain": revision, "options.snapshots": revision}
    for spec in (options.CHAIN, options.SNAPSHOTS):
        assert set(revision) <= set(CATALOG.dataset_schema(spec).column_names)


def test_the_options_snapshots_and_chain_describe_a_trade_alike() -> None:
    by_name = CATALOG.by_name
    snapshots = {c.name: c for c in CATALOG.dataset_schema(by_name["options.snapshots"]).columns}
    chain = {c.name: c for c in CATALOG.dataset_schema(by_name["options.chain"]).columns}
    for name in ("LastTradePrice", "LastTradeExchange"):
        assert snapshots[name].description == chain[name].description
    assert "AAPL" not in snapshots["LastTradeId"].description
