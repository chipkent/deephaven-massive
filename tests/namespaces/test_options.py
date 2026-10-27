"""Tests for the `m.options` namespace: its dataset specs, schemas, and factory methods.

The specs are checked against the generated sources through the catalog gates, their merged
schemas for the key columns, the payload fixtures (shaped from Massive's sample responses)
against the generated REST fields, and each factory for the request it builds and the errors
it raises at construction.
"""

import inspect
import json
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime
from typing import Any, cast

import pytest
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._datasets.symbols import SymbolTable
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
        "filters": {"expiration_date_lte": "2025-01-17", "strike_price_gte": 150},
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
    with pytest.raises(ValueError, match="contract_type is set through contract_type="):
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
    with pytest.raises(ValueError, match="type is set through the dataset"):
        snapshots(dm.ALL, filters={"type": "stocks"})
    with pytest.raises(ValueError, match="no filter 'ticker_any_of'"):
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
    for key, argument in (
        ("underlying_ticker", "underlying="),
        ("contract_type", "contract_type="),
        ("as_of", "as_of="),
        ("expired", "expired="),
    ):
        with pytest.raises(ValueError, match=f"{key} is set through {argument}"):
            contracts(filters={key: "x"})
    with pytest.raises(ValueError, match=r"options\.contracts has no filter 'ticker'"):
        contracts(filters={"ticker": CONTRACT})


def test_as_of_and_date_take_the_new_york_day(massive_session: dm.MassiveSession) -> None:
    late = datetime(2025, 1, 3, 2, 0, tzinfo=UTC)  # still January 2 in New York
    q = massive_session.options.contracts(as_of=late)
    assert dict(q._request.params) == {"as_of": date(2025, 1, 2)}
    q = massive_session.options.daily_open_close(CONTRACT, late)
    assert dict(q._request.params) == {"date": date(2025, 1, 2)}


def test_daily_open_close_takes_explicit_contracts(massive_session: dm.MassiveSession) -> None:
    daily = massive_session.options.daily_open_close
    q = daily("AAPL250117C00150000", date(2025, 1, 2))
    assert type(q) is Query
    assert request_of(q) == {
        "symbols": (CONTRACT,),
        "params": {"date": date(2025, 1, 2)},
        "filters": {},
    }
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
