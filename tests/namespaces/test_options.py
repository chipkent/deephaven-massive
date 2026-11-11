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
import logging
from datetime import UTC, date, datetime, time
from typing import TYPE_CHECKING, Any, cast
from zoneinfo import ZoneInfo

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
from deephaven_massive._enums import ChannelFamily, HistorySource, Recency
from deephaven_massive._ingest.rest_requests import page_size
from deephaven_massive._massive import RestPath, Transport, source, to_batch
from deephaven_massive._session import CATALOG
from deephaven_massive._tables.transports import HostDecision
from deephaven_massive.namespaces import _options as options
from deephaven_massive.namespaces._query_building import Namespace
from deephaven_massive.queries import (
    FetchQuery,
    HistoryQuery,
    Query,
    StreamHistoryQuery,
    StreamQuery,
    SymbolStreamQuery,
)
from deephaven_massive.queries._choice import query_class
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
    "last_trade",
    "quotes",
    "bars",
    "previous_day_bar",
    "fmv",
    "admin_messages",
    "chain",
    "snapshots",
    "contracts",
    "daily_open_close",
)


def ny_midnight_ns(day: date) -> int:
    return int(datetime.combine(day, time(), ZoneInfo("America/New_York")).timestamp()) * 10**9


def request_of(query: Query) -> dict[str, Any]:
    r = query._request
    return {"symbols": r.symbols, "params": dict(r.params), "filters": dict(r.filters)}


# --- Specs and the catalog -------------------------------------------------------------------


def test_namespace_is_reached_from_the_session(massive_session: dm.MassiveSession) -> None:
    namespace = massive_session.options
    assert type(namespace) is options.OptionsNamespace
    assert isinstance(namespace, Namespace)
    assert repr(namespace).endswith(" options>")


def test_every_dataset_is_declared_in_method_order() -> None:
    assert tuple(spec.name for spec in options.DATASETS) == tuple(f"options.{n}" for n in NAMES)
    assert all(CATALOG.by_name[spec.name] is spec for spec in options.DATASETS)
    assert all(spec.asset_class is dm.AssetClass.OPTIONS for spec in options.DATASETS)


def test_the_options_datasets_pass_every_catalog_gate() -> None:
    assert Catalog(options.DATASETS).problems() == ()


def test_every_options_dataset_whose_rows_name_a_contract_has_an_option_root() -> None:
    for spec in options.DATASETS:
        if not spec.names_symbols:
            assert "OptionRoot" not in CATALOG.dataset_schema(spec).column_names
            continue
        schema = CATALOG.dataset_schema(spec)
        names = schema.column_names
        # It follows the leading key and time columns, wherever the dataset has them.
        keys = {"Symbol", "Date", "Timestamp", "TimestampStart", "TimestampEnd"}
        leading = next(i for i, name in enumerate(names) if name not in keys)
        assert names.index("OptionRoot") == leading, spec.name


def test_contracts_carry_the_raw_deliverable_fields() -> None:
    columns = {c.name: c for c in CATALOG.dataset_schema(options.CONTRACTS).columns}
    assert columns["SharesPerContract"].description == (
        "The number of shares per contract for this contract."
    )
    assert columns["AdditionalUnderlyings"].type is ColumnType.STRING  # the list as JSON text
    assert columns["AdditionalUnderlyings"].description.startswith(
        "If an option contract has additional underlyings or deliverables"
    )
    assert columns["Cfi"].description.startswith("The 6 letter CFI code of the contract")
    assert columns["Correction"].description == "The correction number for this option contract."
    assert "StandardDeliverable" not in columns


def test_each_dataset_reads_its_sources() -> None:
    assert {spec.name: spec.kind for spec in options.DATASETS} == {
        "options.trades": "trades",
        "options.last_trade": "trades",
        "options.quotes": "quotes",
        "options.bars": "bars",
        "options.previous_day_bar": "bars",
        "options.fmv": "fmv",
        "options.admin_messages": "admin_messages",
        "options.chain": None,
        "options.snapshots": None,
        "options.contracts": None,
        "options.daily_open_close": None,
    }
    # Every dataset but the chain, which takes its underlying instead, and the admin messages,
    # whose rows name no contract, takes contracts.
    takes = {spec.name for spec in options.DATASETS if CATALOG.takes_symbols(spec)}
    no_contracts = {"options.chain", "options.admin_messages"}
    assert takes == {spec.name for spec in options.DATASETS} - no_contracts
    assert options.LAST_TRADE.sources == ((R, "/v2/last/trade/{optionsTicker}"),)
    assert options.PREVIOUS_DAY_BAR.sources == ((R, "/v2/aggs/ticker/{optionsTicker}/prev"),)
    assert options.TRADES.sources == (
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
        (R, "/v2/aggs/ticker/{optionsTicker}/range/{multiplier}/{timespan}/{from}/{to}"),
        (Transport.WS, "options/A"),
        (Transport.WS, "options/AM"),
        (Transport.FLAT_FILES, "us_options_opra/minute_aggs_v1"),
        (Transport.FLAT_FILES, "us_options_opra/day_aggs_v1"),
    )
    assert options.FMV.sources == ((Transport.WS, "options/FMV"),)
    assert options.FMV.channel_family is dm._enums.ChannelFamily.FMV
    assert options.ADMIN_MESSAGES.sources == ((Transport.WS, "options/AD"),)
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
        (options.TRADES, StreamHistoryQuery),
        (options.LAST_TRADE, FetchQuery),
        (options.QUOTES, StreamHistoryQuery),
        (options.BARS, HistoryQuery),
        (options.PREVIOUS_DAY_BAR, FetchQuery),
        (options.FMV, SymbolStreamQuery),
        (options.ADMIN_MESSAGES, StreamQuery),
        (options.CHAIN, FetchQuery),
        (options.SNAPSHOTS, FetchQuery),
        (options.CONTRACTS, FetchQuery),
        (options.DAILY_OPEN_CLOSE, FetchQuery),
    ],
)
def test_query_class_of_each_dataset(spec: DatasetSpec, expected: type) -> None:
    assert query_class(spec) is expected


def test_page_sizes_are_the_largest_each_endpoint_returns() -> None:
    # A stream-only dataset reads no REST path, so it has no page size either.
    sizes = {
        spec.name: page_size(spec, CATALOG.rest_path(spec.rest_paths[0]))
        if spec.rest_paths
        else None
        for spec in options.DATASETS
    }
    assert sizes == {
        "options.trades": 50_000,
        "options.last_trade": None,
        "options.quotes": 50_000,
        "options.bars": 50_000,
        "options.previous_day_bar": None,
        "options.fmv": None,
        "options.admin_messages": None,
        "options.chain": 250,
        "options.snapshots": 250,
        "options.contracts": 1_000,
        "options.daily_open_close": None,
    }


# --- Schemas ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spec",
    [
        options.TRADES,
        options.LAST_TRADE,
        options.QUOTES,
        options.BARS,
        options.PREVIOUS_DAY_BAR,
        options.FMV,
    ],
)
def test_kind_datasets_lead_with_symbol_date_and_their_instants(spec: DatasetSpec) -> None:
    schema = CATALOG.dataset_schema(spec)
    times = ("TimestampStart", "TimestampEnd") if spec.holds_bars else ("Timestamp",)
    assert schema.column_names[: 2 + len(times)] == ("Symbol", "Date", *times)
    assert schema.column_names[-1] == "TimestampInsert"
    types = {c.name: c.type for c in schema.columns}
    assert types["Symbol"] is ColumnType.STRING
    assert types["Date"] is ColumnType.STRING
    assert all(types[name] is ColumnType.INSTANT for name in (*times, "TimestampInsert"))
    assert "Underlying" not in types
    assert "Ticker" not in types


def test_trades_merge_the_rest_stream_and_flat_file_columns() -> None:
    names = CATALOG.dataset_schema(options.TRADES).column_names
    # SipTimestamp became Timestamp; the participant and TRF times stay their own columns.
    assert "SipTimestamp" not in names
    assert {"Price", "Size", "Exchange", "Conditions", "ParticipantTimestamp"} <= set(names)
    types = {c.name: c.type for c in CATALOG.dataset_schema(options.TRADES).columns}
    assert types["Conditions"] is ColumnType.LONG_ARRAY
    assert types["Size"] is ColumnType.DOUBLE  # the WebSocket's integer joins the REST number


def test_bars_key_on_the_window_start() -> None:
    names = CATALOG.dataset_schema(options.BARS).column_names
    assert {"WindowStart", "EndTimestamp", "Timestamp"}.isdisjoint(names)
    assert {"Open", "High", "Low", "Close", "Volume", "Vwap"} <= set(names)


def test_bar_prices_say_how_they_are_adjusted() -> None:
    columns = {c.name: c.description for c in CATALOG.dataset_schema(options.BARS).columns}
    for name in ("Open", "High", "Low", "Close", "Vwap"):
        assert "Split-adjusted when the query's adjusted is True" in columns[name], name
        assert "never adjusts for cash dividends" in columns[name], name
    assert "multiplied by its ratio" in columns["Volume"]
    opened = {
        c.name: c.description for c in CATALOG.dataset_schema(options.DAILY_OPEN_CLOSE).columns
    }
    assert "Split-adjusted" in opened["PreMarket"]
    assert "Split-adjusted" in opened["Volume"]


@pytest.mark.parametrize(
    ("spec", "gone"),
    [
        (options.CHAIN, {"Ticker", "UnderlyingAssetTicker"}),
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
    # The day, `from`, is the row's `Date`, from which the library sets the day's window.
    assert schema.column_names[:4] == ("Symbol", "Date", "TimestampStart", "TimestampEnd")
    assert types["Date"] is ColumnType.STRING
    assert {"From", "Timestamp"}.isdisjoint(types)


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
    assert required_keys("/v3/snapshot/options/{underlyingAsset}") == ("Ticker",)


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


def test_trades_quotes_and_fmv_take_massive_option_tickers(
    massive_session: dm.MassiveSession,
) -> None:
    m = massive_session
    for factory, cls in (
        (m.options.trades, StreamHistoryQuery),
        (m.options.quotes, StreamHistoryQuery),
        (m.options.fmv, SymbolStreamQuery),
    ):
        q = factory(["O:AAPL250117P00150000", "O:AAPL250117C00150000"])
        assert type(q) is cls
        assert q._request.symbols == ("O:AAPL250117C00150000", "O:AAPL250117P00150000")
        assert factory(dm.ALL)._request.symbols is dm.ALL
        with pytest.raises(
            ValueError, match=r"use 'O:AAPL250117C00150000' .*canonical_symbol_option"
        ):
            factory("AAPL250117C00150000")
        with pytest.raises(ValueError, match="is an underlying, not an OCC option symbol"):
            factory("AAPL")
        with pytest.raises(ValueError, match=r"is a crypto ticker; this dataset takes option"):
            factory("X:BTCUSD")
        with pytest.raises(ValueError, match="not an OCC option symbol with a real expiry"):
            factory("O:BRK.B250117C00400000")  # an OCC root has no dot


def test_a_symbol_table_is_followed(massive_session: dm.MassiveSession) -> None:
    watch = new_table([string_col("Symbol", [CONTRACT])])
    q = massive_session.options.trades(watch)
    assert isinstance(q._request.symbols, SymbolTable)
    with pytest.raises(ValueError, match="must have a Symbol column"):
        massive_session.options.trades(new_table([string_col("Ticker", [CONTRACT])]))


def test_bars_pick_their_class_from_the_timespan(massive_session: dm.MassiveSession) -> None:
    bars = massive_session.options.bars
    assert type(bars(CONTRACT, dm.Timespan.MINUTE)) is StreamHistoryQuery
    assert type(bars(CONTRACT, dm.Timespan.SECOND)) is StreamHistoryQuery
    assert type(bars(CONTRACT, dm.Timespan.DAY)) is HistoryQuery
    assert type(bars(CONTRACT, dm.Timespan.MINUTE, multiplier=5)) is HistoryQuery
    assert type(bars(CONTRACT, dm.Timespan.HOUR)) is HistoryQuery
    q = bars(CONTRACT, dm.Timespan.DAY, adjusted=False)
    assert (q._request.timespan, q._request.multiplier) == (dm.Timespan.DAY, 1)
    assert q._request.adjusted is False
    assert bars(CONTRACT, dm.Timespan.MINUTE)._request.adjusted is True
    with pytest.raises(ValueError, match="SESSION exists only for futures"):
        bars(CONTRACT, dm.Timespan.SESSION)
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan"):
        bars(CONTRACT, cast("dm.Timespan", "minute"))
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        bars(CONTRACT, dm.Timespan.MINUTE, adjusted=cast("bool", 1))
    # None would read as a path without `adjusted`: REST would send Massive's default,
    # adjusted, while the flat files, as traded, could serve the same call.
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        bars(CONTRACT, dm.Timespan.MINUTE, adjusted=cast("bool", None))


def test_chain_sends_its_underlying_and_contract_type(massive_session: dm.MassiveSession) -> None:
    q = massive_session.options.chain(
        "AAPL",
        contract_type=dm.ContractType.PUT,
        sort="strike_price",
        order=dm.Order.DESC,
        limit=10,
        filters={"strike_price_gte": 150, "expiration_date_lte": "2025-01-17"},
    )
    assert type(q) is FetchQuery
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


def test_chain_and_contracts_take_a_table_of_underlyings(
    massive_session: dm.MassiveSession,
) -> None:
    underlyings = new_table([string_col("Symbol", ["AAPL", "I:SPX"])])
    chain = massive_session.options.chain(underlyings, contract_type=dm.ContractType.PUT)
    contracts = massive_session.options.contracts(underlying=underlyings)
    for query, parameter in ((chain, "underlyingAsset"), (contracts, "underlying_ticker")):
        values = query._request.value_table
        assert values is not None
        assert (values.parameter, values.column, values.table) == (
            parameter,
            "Symbol",
            underlyings,
        )
        assert parameter not in request_of(query)["params"]  # each verb sends one at a time
    assert request_of(chain)["params"] == {"contract_type": dm.ContractType.PUT}
    tickers = new_table([string_col("Ticker", ["AAPL"])])
    with pytest.raises(
        ValueError,
        match=r"^a table passed as underlying must have a Symbol column; this one has: Ticker\. ",
    ):
        massive_session.options.chain(tickers)
    with pytest.raises(ValueError, match="must have a Symbol column"):
        massive_session.options.contracts(underlying=tickers)


def test_chain_rejects_bad_arguments(massive_session: dm.MassiveSession) -> None:
    chain = massive_session.options.chain
    with pytest.raises(
        ValueError,
        match=(
            r"^options\.chain reads one underlying's chain and does not take dm\.ALL; "
            r"m\.options\.snapshots\(dm\.ALL\) lists every contract$"
        ),
    ):
        chain(cast("str", dm.ALL))
    with pytest.raises(TypeError, match=r"^underlying must be one ticker, a str, not int$"):
        chain(cast("str", 1))
    with pytest.raises(ValueError, match=r"^'  ' is not a Massive stock ticker such as"):
        chain("  ")
    with pytest.raises(ValueError, match=r"tickers are case-sensitive"):
        chain("aapl")
    with pytest.raises(ValueError, match=r"'i:SPX' is an index ticker \(prefix I:\)"):
        chain("i:SPX")
    with pytest.raises(ValueError, match=r"^underlying must be a stock or index ticker"):
        chain("C:EURUSD")
    with pytest.raises(ValueError, match=r"^underlying must be a stock or index ticker"):
        chain("O:AAPL250117C00150000")
    assert request_of(chain("KIMpL"))["params"]["underlyingAsset"] == "KIMpL"  # case is kept
    assert request_of(chain("I:SPX"))["params"]["underlyingAsset"] == "I:SPX"
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
    assert type(q) is FetchQuery
    assert request_of(q) == {
        "symbols": (CONTRACT,),
        "params": {},
        "filters": {"ticker_gte": "O:A"},
    }
    assert snapshots(dm.ALL)._request.symbols is dm.ALL
    assert dict(snapshots(dm.ALL)._request.params) == {"type": "options"}
    with pytest.raises(ValueError, match=r"^type is set through its own argument"):
        snapshots(dm.ALL, filters={"type": "stocks"})
    with pytest.raises(ValueError, match=r"^type is set through its own argument"):
        snapshots([CONTRACT], filters={"type": "options"})
    with pytest.raises(ValueError, match=r"^ticker_any_of is set through symbols=, not"):
        snapshots(dm.ALL, filters={"ticker_any_of": [CONTRACT]})


def test_contracts_list_or_describe(massive_session: dm.MassiveSession) -> None:
    contracts = massive_session.options.contracts
    listed = contracts(
        underlying="NVDA",
        contract_type=dm.ContractType.CALL,
        as_of="2025-01-02",
        expired=True,
        filters={"strike_price_gte": 100, "expiration_date_lte": date(2026, 1, 16)},
    )
    assert type(listed) is FetchQuery
    assert request_of(listed) == {
        "symbols": None,
        "params": {
            "as_of": date(2025, 1, 2),
            "contract_type": dm.ContractType.CALL,
            "expired": True,
            "underlying_ticker": "NVDA",
        },
        "filters": {"expiration_date_lte": date(2026, 1, 16), "strike_price_gte": 100},
    }
    described = contracts([CONTRACT])
    assert request_of(described) == {"symbols": (CONTRACT,), "params": {}, "filters": {}}
    assert request_of(contracts(symbols=[CONTRACT])) == request_of(described)
    assert request_of(contracts()) == {"symbols": None, "params": {}, "filters": {}}
    # False, the default, is Massive's own, so it is not sent, also beside named contracts.
    assert request_of(contracts(expired=False)) == request_of(contracts())
    # The request keeps the canonical index; only the wire gets the bare one.
    assert request_of(contracts(underlying="I:SPX"))["params"] == {"underlying_ticker": "I:SPX"}
    assert request_of(contracts([CONTRACT], expired=False)) == request_of(described)


def test_contracts_reject_bad_arguments(massive_session: dm.MassiveSession) -> None:
    contracts = massive_session.options.contracts
    with pytest.raises(ValueError, match=r"use m\.options\.contracts\(\) without symbols"):
        contracts(symbols=dm.ALL)
    with pytest.raises(
        ValueError,
        match=(
            r"^'AAPL' is an underlying, not an options contract: list its contracts with "
            r"m\.options\.contracts\(underlying='AAPL'\)$"
        ),
    ):
        contracts("AAPL")
    with pytest.raises(ValueError, match=r"'I:SPX' is an underlying"):
        contracts("I:SPX")
    with pytest.raises(ValueError, match=r"is not an OCC option symbol"):
        contracts("aapl")  # not canonical: the symbol check explains it
    with pytest.raises(TypeError, match=r"^underlying must be one ticker, a str, not dm\.ALL$"):
        contracts(underlying=cast("str", dm.ALL))
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
    with pytest.raises(ValueError, match=r"^options\.contracts: as_of=.* is a calendar date"):
        massive_session.options.contracts(as_of=late)
    with pytest.raises(ValueError, match=r"^options\.daily_open_close: date=.* is a calendar date"):
        massive_session.options.daily_open_close(CONTRACT, late)
    q = massive_session.options.contracts(as_of="2025-01-02")
    assert dict(q._request.params) == {"as_of": date(2025, 1, 2)}


def test_daily_open_close_takes_explicit_contracts(massive_session: dm.MassiveSession) -> None:
    daily = massive_session.options.daily_open_close
    q = daily("O:AAPL250117C00150000", date(2025, 1, 2))
    assert type(q) is FetchQuery
    assert request_of(q) == {
        "symbols": (CONTRACT,),
        "params": {"date": date(2025, 1, 2)},
        "filters": {},
    }
    assert q._request.adjusted is True
    assert daily(CONTRACT, "2025-01-02", adjusted=False)._request.adjusted is False
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        daily(CONTRACT, "2025-01-02", adjusted=cast("bool", 0))
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        daily(CONTRACT, "2025-01-02", adjusted=cast("bool", None))
    # Massive has no grouped daily endpoint for options: the day's bars serve every contract.
    with pytest.raises(
        ValueError,
        match=r"; use m\.options\.bars\(dm\.ALL, dm\.Timespan\.DAY, adjusted=False\)"
        r"\.history\(day, day\), every contract's bar of that day$",
    ):
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
    return cast("Callable[..., object]", getattr(options.OptionsNamespace, name))


@pytest.mark.parametrize("name", NAMES)
def test_docstrings_name_their_dataset_page(name: str) -> None:
    doc = inspect.getdoc(_factory(name)) or ""
    assert f"docs/datasets/options.{name}.md" in doc


def test_every_factory_has_a_dataset() -> None:
    public = {
        name
        for name, member in vars(options.OptionsNamespace).items()
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


def wire(path: str) -> str:
    # A path as the client sends it: the colon of an `O:` symbol is percent-encoded.
    return path.replace(":", "%3A")


def body(load_fixture: Callable[[str, str], bytes], name: str) -> object:
    return json.loads(load_fixture(OWNER, name))


def rows(table: Table) -> list[dict[str, Any]]:
    # Instants become epoch nanoseconds, so assertions keep every digit.
    data = dh_arrow.to_arrow(table)
    columns = [c.cast(pa.int64()) if pa.types.is_timestamp(c.type) else c for c in data.columns]
    out: list[dict[str, Any]] = pa.Table.from_arrays(columns, names=data.column_names).to_pylist()
    return out


def test_the_one_row_datasets_share_the_columns_of_their_kind(
    massive_session: dm.MassiveSession,
) -> None:
    # Each dataset has the kind's columns its own endpoints fill; the columns two of them share
    # have one type and one order, so the tables merge on them.
    for one, kind in (
        (options.LAST_TRADE, options.TRADES),
        (options.PREVIOUS_DAY_BAR, options.BARS),
    ):
        mine = [(c.name, c.type) for c in CATALOG.dataset_schema(one).columns]
        theirs = [(c.name, c.type) for c in CATALOG.dataset_schema(kind).columns]
        shared = [c for c in mine if c in theirs]
        assert shared == [c for c in theirs if c in mine]
        assert len(shared) > 5
    assert not hasattr(massive_session.options.trades(CONTRACT), "fetch")
    assert not hasattr(massive_session.options.bars(CONTRACT, dm.Timespan.DAY), "fetch")
    assert type(massive_session.options.last_trade(CONTRACT)) is FetchQuery


def test_previous_day_bar_is_one_adjusted_day_and_refuses_all(
    massive_session: dm.MassiveSession,
) -> None:
    prev = massive_session.options.previous_day_bar
    q = prev(CONTRACT)
    assert type(q) is FetchQuery
    assert (q._request.timespan, q._request.multiplier, q._request.adjusted) == (
        dm.Timespan.DAY,
        1,
        True,
    )
    assert prev(CONTRACT, adjusted=False)._request.adjusted is False
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        prev(CONTRACT, adjusted=cast("bool", 1))
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        prev(CONTRACT, adjusted=cast("bool", None))
    with pytest.raises(
        ValueError,
        match=r"use m\.options\.bars\(dm\.ALL, dm\.Timespan\.DAY, adjusted=False\)"
        r"\.history\(day, day\)",
    ):
        prev(dm.ALL)


def test_last_trade_fetch_reads_the_last_trade(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply(wire(f"/v2/last/trade/{TSLA}"), Reply(body(load_fixture, "last_trade.json")))
    out = rows(rest_session.options.last_trade("O:TSLA210903C00700000").fetch(wait=True))
    assert fake_rest.get_queries() == [(f"/v2/last/trade/{TSLA}", {})]
    assert len(out) == 1
    row = out[0]
    assert (row["Symbol"], row["Timestamp"], row["Price"]) == (TSLA, 1617901342969834000, 115.55)
    assert row["OptionRoot"] == "TSLA"
    assert (row["Size"], row["Exchange"], row["Conditions"]) == (25.0, 312, [227])
    assert row["ParticipantTimestamp"] == 1617901342969834000
    with pytest.raises(ValueError, match=r"use m\.options\.chain\(underlying\)\.fetch\(\)"):
        rest_session.options.last_trade(dm.ALL)
    assert len(fake_rest.get_queries()) == 1


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
        rest_session.options.trades(contract).history(
            day, day, source=HistorySource.REST, wait=True
        )
    )
    assert fake_rest.get_queries() == [
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
        rest_session.options.quotes(quote_contract).history(
            day, day, source=HistorySource.REST, wait=True
        )
    )
    path, query = fake_rest.get_queries()[-1]
    assert path == f"/v3/quotes/{quote_contract}"
    assert query["limit"] == ["50000"]
    # The rows are newest first, as the vendor sample gives them; history keeps server order.
    assert [(r["Symbol"], r["Timestamp"], r["BidPrice"], r["AskSize"]) for r in quotes] == [
        (quote_contract, 1645119125346243600, 277.5, 10.0),
        (quote_contract, 1645119118474271000, 277.5, 1.0),
    ]


def test_previous_day_bar_fetches_the_previous_session_and_bars_read_a_range(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply(wire(f"/v2/aggs/ticker/{TSLA}/prev"), Reply(body(load_fixture, "prev.json")))
    prev = rows(rest_session.options.previous_day_bar(TSLA).fetch(wait=True))
    assert fake_rest.get_queries() == [(f"/v2/aggs/ticker/{TSLA}/prev", {"adjusted": ["true"]})]
    # `/prev` stamps the bar at the 16:00 New York close (1605042000000); its window is its
    # whole New York day, as the range stamps it.
    assert [
        (r["Symbol"], r["Date"], r["TimestampStart"], r["TimestampEnd"], r["Close"]) for r in prev
    ] == [
        (
            TSLA,
            "2020-11-10",
            ny_midnight_ns(date(2020, 11, 10)),
            ny_midnight_ns(date(2020, 11, 11)),
            115.97,
        )
    ]

    contract = "O:RDFN211119C00025000"
    start = int(datetime(2021, 9, 23, 4, tzinfo=UTC).timestamp()) * 1000  # New York midnight
    end = int(datetime(2021, 9, 25, 4, tzinfo=UTC).timestamp()) * 1000 - 1
    path = f"/v2/aggs/ticker/{contract}/range/1/day/{start}/{end}"
    # The sample echoes `adjusted: true`; unadjusted bars come back saying so.
    ranged = cast("dict[str, object]", body(load_fixture, "range.json"))
    fake_rest.reply(wire(path), Reply({**ranged, "adjusted": False}))
    bars = rows(
        rest_session.options.bars(contract, dm.Timespan.DAY, adjusted=False).history(
            date(2021, 9, 23), date(2021, 9, 24), source=HistorySource.REST, wait=True
        )
    )
    assert fake_rest.get_queries()[-1] == (
        path,
        {"adjusted": ["false"], "limit": ["50000"], "sort": ["asc"]},
    )
    assert [(r["Symbol"], r["Date"], r["TimestampStart"], r["Close"]) for r in bars] == [
        (contract, "2021-09-23", 1632369600000 * 10**6, 26.2),
        (contract, "2021-09-24", 1632456000000 * 10**6, 28.3),
    ]


def stamp_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return the WARNINGs of bars stamped where Massive is not known to stamp one."""
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and "is not a stamp Massive is known to use" in r.message
    ]


def test_a_daily_bar_at_an_early_close_is_moved_and_off_a_known_time_warns(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)
    # The 13:00 close of a short day means its day; 14:30 is neither a start nor a close.
    early = int(datetime(2024, 11, 29, 18, tzinfo=UTC).timestamp()) * 1000
    afternoon = early + 90 * 60_000
    fake_rest.reply(
        wire(f"/v2/aggs/ticker/{TSLA}/prev"),
        Reply({"status": "OK", "results": [{"T": TSLA, "t": early, "c": 1.0}]}),
        Reply({"status": "OK", "results": [{"T": TSLA, "t": afternoon, "c": 1.0}]}),
    )
    prev = rows(rest_session.options.previous_day_bar(TSLA).fetch(wait=True))
    assert [(r["Date"], r["TimestampStart"]) for r in prev] == [
        ("2024-11-29", ny_midnight_ns(date(2024, 11, 29)))
    ]
    assert not stamp_warnings(caplog)
    # 14:30 is served too, as sent: its stamp, a day later its end, no Date, one WARNING.
    odd = rows(rest_session.options.previous_day_bar(TSLA).fetch(wait=True))
    assert [(r["Date"], r["TimestampStart"], r["TimestampEnd"]) for r in odd] == [
        (None, afternoon * 10**6, (afternoon + 86_400_000) * 10**6)
    ]
    [warning] = stamp_warnings(caplog)
    assert warning.startswith(
        f"options.previous_day_bar: a 1-day bar of {TSLA} from "
        "/v2/aggs/ticker/{optionsTicker}/prev is "
        "stamped 2024-11-29T19:30:00+00:00 (2024-11-29T14:30:00-05:00 in America/New_York), "
        "which is not a stamp Massive is known to use"
    )


def test_chain_reads_one_underlying(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply("/v3/snapshot/options/AAPL", Reply(body(load_fixture, "chain.json")))
    out = rows(
        rest_session.options.chain(
            "AAPL",
            contract_type=dm.ContractType.CALL,
            sort="strike_price",
            order=dm.Order.ASC,
            filters={"expiration_date_lte": date(2022, 1, 21), "strike_price_gte": 150},
        ).fetch(wait=True)
    )
    assert fake_rest.get_queries() == [
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
    assert row["OptionRoot"] == "AAPL"
    assert (row["StrikePrice"], row["GreeksTheta"], row["OpenInterest"]) == (
        150.0,
        0.00229,
        1543.0,
    )
    assert row["ExpirationDate"] == "2022-01-21"  # every date is ISO text
    assert row["DayLastUpdated"] == 1605195918507251700


def test_snapshots_ask_for_options_250_contracts_a_request(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply("/v3/snapshot", Reply(body(load_fixture, "snapshot.json")))
    contracts = [f"O:NCLH221014C{strike:08d}" for strike in range(1000, 1000 + 251)]
    rest_session.options.snapshots(contracts).fetch(wait=True)
    requests = fake_rest.get_queries()
    # The two requests run concurrently, so they arrive in either order.
    chunks = sorted((q["ticker.any_of"][0].split(",") for _, q in requests), key=len)
    assert chunks == [contracts[-1:], contracts[:250]]
    assert all("type" not in q and q["limit"] == ["250"] for _, q in requests)


def test_snapshots_send_no_type_beside_tickers(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    # Massive answers `type` beside `ticker.any_of` with HTTP 400 "Cannot specify tickers and
    # type.", so only `dm.ALL` sends it; this is the exact query sent for one contract.
    fake_rest.reply("/v3/snapshot", Reply(body(load_fixture, "snapshot.json")))
    rest_session.options.snapshots(["O:NCLH221014C00005000"]).fetch(wait=True)
    assert [r.target for r in fake_rest.requests if r.method == "GET"] == [
        "/v3/snapshot?limit=250&ticker.any_of=O%3ANCLH221014C00005000"
    ]


def test_snapshots_keep_the_error_row_of_a_contract(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply("/v3/snapshot", Reply(body(load_fixture, "snapshot.json")))
    out = rows(rest_session.options.snapshots(dm.ALL).fetch(wait=True))
    assert fake_rest.get_queries() == [("/v3/snapshot", {"type": ["options"], "limit": ["250"]})]
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
            underlying="AAPL",
            contract_type=dm.ContractType.CALL,
            as_of=date(2021, 11, 1),
            expired=True,
            sort="strike_price",
            order=dm.Order.DESC,
            filters={"strike_price_lte": 90},
        ).fetch(wait=True)
    )
    assert fake_rest.get_queries() == [
        (
            "/v3/reference/options/contracts",
            {
                "underlying_ticker": ["AAPL"],
                "contract_type": ["call"],
                "as_of": ["2021-11-01"],
                "expired": ["true"],
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
    described = rows(rest_session.options.contracts(one, as_of="2021-11-01").fetch(wait=True))
    assert fake_rest.get_queries()[-1] == (
        f"/v3/reference/options/contracts/{one}",
        {"as_of": ["2021-11-01"]},
    )
    assert [(r["Symbol"], r["Underlying"], r["ExpirationDate"]) for r in described] == [
        (one, "AAPL", "2021-11-19")
    ]


def test_an_index_underlyings_contracts_are_listed_by_its_bare_ticker(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    # Massive's contracts list takes and writes `SPX`, while the chain writes `I:SPX`: the
    # library sends the bare ticker and shows the canonical one, so the two tables join.
    caplog.set_level(logging.DEBUG, logger="deephaven_massive")
    reply = Reply(body(load_fixture, "contracts_index.json"))
    fake_rest.reply("/v3/reference/options/contracts", reply)
    listed = rows(rest_session.options.contracts(underlying="I:SPX").fetch(wait=True))
    assert fake_rest.get_queries() == [
        ("/v3/reference/options/contracts", {"underlying_ticker": ["SPX"], "limit": ["1000"]})
    ]
    assert [(r["Symbol"], r["Underlying"], r["OptionRoot"]) for r in listed] == [
        ("O:SPX261120C00400000", "I:SPX", "SPX"),
        ("O:SPX261120C00600000", "I:SPX", "SPX"),
    ]
    messages = [r.getMessage() for r in caplog.records]
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert (
        "options.contracts: 2 of 2 Underlying values from /v3/reference/options/contracts are "
        "indices by their CFI code and get the I: prefix, the first 'SPX'"
    ) in messages


def test_a_table_of_index_underlyings_lists_each_one_s_contracts_by_its_bare_ticker(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    # Each value of the table is read as if passed alone, so an index is sent bare and its
    # rows get their I: back, as for `underlying="I:SPX"`.
    reply = Reply(body(load_fixture, "contracts_index.json"))
    fake_rest.reply("/v3/reference/options/contracts", reply)
    underlyings = new_table([string_col("Symbol", ["I:SPX"])])
    listed = rows(rest_session.options.contracts(underlying=underlyings).fetch(wait=True))
    assert fake_rest.get_queries() == [
        ("/v3/reference/options/contracts", {"underlying_ticker": ["SPX"], "limit": ["1000"]})
    ]
    assert {r["Underlying"] for r in listed} == {"I:SPX"}


def test_a_table_of_underlyings_reads_each_one_s_chain(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    for underlying in ("AAPL", "MSFT"):
        reply = Reply(body(load_fixture, "chain.json"))
        fake_rest.reply(f"/v3/snapshot/options/{underlying}", reply)
    underlyings = new_table([string_col("Symbol", ["MSFT", "AAPL"])])
    out = rows(rest_session.options.chain(underlyings).fetch(wait=True))
    assert [path for path, _ in fake_rest.get_queries()] == [
        "/v3/snapshot/options/AAPL",
        "/v3/snapshot/options/MSFT",
    ]
    assert len(out) == 2  # each chain's one contract


def contract_row(ticker: str, underlying: str, cfi: str | None) -> dict[str, object]:
    row: dict[str, object] = {"ticker": ticker, "underlying_ticker": underlying}
    return row if cfi is None else row | {"cfi": cfi}


def warnings_of(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


def test_rows_of_another_instrument_keep_their_underlying_and_warn_once(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # `I:DJT` is sent as `DJT`, which is a stock: its options are no index's, so they keep the
    # underlying Massive wrote rather than read as the index's.
    caplog.set_level(logging.WARNING, logger="deephaven_massive")
    stock = contract_row("O:DJT261120C00020000", "DJT", "OCASPS")
    reply = Reply(body={"status": "OK", "request_id": "r", "results": [stock]})
    fake_rest.reply("/v3/reference/options/contracts", reply)
    for _ in range(2):
        listed = rows(rest_session.options.contracts(underlying="I:DJT").fetch(wait=True))
        assert [(r["Symbol"], r["Underlying"]) for r in listed] == [("O:DJT261120C00020000", "DJT")]
    assert fake_rest.get_queries()[0][1]["underlying_ticker"] == ["DJT"]
    assert warnings_of(caplog) == [
        (
            "options.contracts: asked /v3/reference/options/contracts for the contracts of I:DJT "
            "by its bare ticker, underlying_ticker=DJT, and 1 rows are not options on an index by "
            "their CFI code, so Massive answered about another instrument; they keep the "
            "underlying Massive wrote. The first (Symbol, Underlying, Cfi): "
            "[('O:DJT261120C00020000', 'DJT', 'OCASPS')]"
        )
    ]


def test_an_index_contract_without_a_cfi_code_keeps_the_bare_ticker_and_warns_once(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="deephaven_massive")
    path = "/v3/reference/options/contracts"
    # An empty answer first, then one coded row and one without a code, repeated.
    coded = contract_row("O:SPX261120C00400000", "SPX", "OCEICS")
    bare = contract_row("O:SPX261120C00600000", "SPX", None)
    empty = Reply(body={"status": "OK", "request_id": "r", "results": []})
    both = Reply(body={"status": "OK", "request_id": "r", "results": [coded, bare]})
    fake_rest.reply(path, empty, both)
    assert rows(rest_session.options.contracts(underlying="I:SPX").fetch(wait=True)) == []
    for _ in range(2):
        listed = rows(rest_session.options.contracts(underlying="I:SPX").fetch(wait=True))
        assert [(r["Symbol"], r["Underlying"]) for r in listed] == [
            ("O:SPX261120C00400000", "I:SPX"),
            ("O:SPX261120C00600000", "SPX"),
        ]
    assert warnings_of(caplog) == [
        (
            "options.contracts: 1 rows of the contracts of I:SPX from "
            "/v3/reference/options/contracts have no CFI code, so nothing says their underlying is "
            "an index and they keep Massive's bare 'SPX'; the first: O:SPX261120C00600000"
        )
    ]


def test_a_list_argument_with_named_contracts_fails_before_any_request(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # The per-contract path takes only `as_of`; a list-only argument cannot be honored there.
    q = rest_session.options.contracts(symbols=[CONTRACT], underlying="AAPL")
    with pytest.raises(ValueError, match=r"does not take \['underlying_ticker'\]"):
        q.fetch(wait=True)
    assert fake_rest.get_queries() == []


def test_daily_open_close_reads_one_day_per_contract(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    path = f"/v1/open-close/{TSLA}/2023-01-09"
    fake_rest.reply(wire(path), Reply(body(load_fixture, "open_close.json")))
    out = rows(
        rest_session.options.daily_open_close(TSLA, date(2023, 1, 9), adjusted=False).fetch(
            wait=True
        )
    )
    assert fake_rest.get_queries() == [(path, {"adjusted": ["false"]})]
    assert [(r["Symbol"], r["Date"], r["Open"], r["Close"], r["AfterHours"]) for r in out] == [
        (TSLA, "2023-01-09", 25.0, 26.35, 26.35)
    ]
    # Its window is the New York day of its date.
    assert [(r["TimestampStart"], r["TimestampEnd"]) for r in out] == [
        (ny_midnight_ns(date(2023, 1, 9)), ny_midnight_ns(date(2023, 1, 10)))
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
        (
            "bars-second",
            "A",
            bar_frame("A"),
            # The end is the start plus the bar's length, not the frame's `e`.
            {
                "TimestampStart": 1632419640000 * 10**6,
                "TimestampEnd": 1632419641000 * 10**6,
                "Date": "2021-09-23",
                "Close": 2.05,
            },
        ),
        (
            "bars-minute",
            "AM",
            bar_frame("AM"),
            {
                "TimestampStart": 1632419640000 * 10**6,
                "TimestampEnd": 1632419700000 * 10**6,
                "Date": "2021-09-23",
                "Vwap": 2.05,
            },
        ),
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
        "bars-minute": lambda: m.options.bars(SPY, dm.Timespan.MINUTE),
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
    assert row["OptionRoot"] == "SPY"
    assert {name: row[name] for name in expected} == expected
    family = ChannelFamily.FMV if topic == "FMV" else ChannelFamily.TICK
    assert ws_session.decisions == [(dm.AssetClass.OPTIONS, family)]
    m.release(blink)


# --- End to end: the admin messages, a stream whose rows name no symbol -----------------------


def admin_message(exchange: int, sequence: int, text: str) -> dict[str, object]:
    # One `AD` message as Massive's documentation shows it; the id is the time and the sequence.
    sent = 1_793_911_442_930_568_200 + sequence
    return {
        "ev": "AD",
        "i": f"{sent}-{sequence}",
        "t": sent,
        "q": sequence,
        "x": exchange,
        "m": text,
    }


FLEX = "TEXT CFLEX SPY LST PM EUR 4 LEGS 1 @ 733.36"


def shown_streams(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    # The session's `m.streams()` once every queued row write has run and been shown.
    with ws_session.connections.group.lock:
        connections = list(ws_session.connections.group.connections.values())
    for connection in connections:
        ws_session.on_loop(connection, lambda: None)
    assert ws_session.session._core.diagnostics.wait_until_shown(15.0)
    return dh_to_pylist(ws_session.session.streams())


def test_admin_messages_take_no_symbols_and_have_no_last_by(
    massive_session: dm.MassiveSession,
) -> None:
    query = massive_session.options.admin_messages()
    assert type(query) is StreamQuery
    assert query._request.symbols is None
    assert repr(query) == "m.options.admin_messages()"
    with pytest.raises(TypeError):
        massive_session.options.admin_messages(dm.ALL)  # type: ignore[call-arg]  # the refusal
    assert not hasattr(query, "last_by")
    assert not hasattr(query, "fetch")


def test_admin_messages_of_every_exchange_arrive_in_each_shape(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]],
) -> None:
    m = ws_session.session
    query = m.options.admin_messages()
    blink = query.blink()
    seen = blink_to_append_only(blink)
    kept = query.append_only()
    latest = query.ring(2)
    ws_session.subscribed("AD.*")
    ws_session.publish(
        admin_message(302, 1, FLEX), admin_message(323, 2, "ALERT"), admin_message(302, 3, "X")
    )
    for table in (seen, kept):
        rows_when(table, lambda arrived: len(arrived) == 3)
    rows_when(latest, lambda arrived: [r["SequenceNumber"] for r in arrived] == [2, 3])
    first, *_ = got = rows(kept)
    assert [(r["Exchange"], r["Message"]) for r in got] == [(302, FLEX), (323, "ALERT"), (302, "X")]
    assert first["Timestamp"] == 1_793_911_442_930_568_201
    assert first["MessageId"] == "1793911442930568201-1"
    assert first["Date"] == "2026-11-05"
    assert "Symbol" not in first
    assert "OptionRoot" not in first
    # One stream serves the three shapes; its row names no symbol.
    (row,) = shown_streams(ws_session, dh_to_pylist)
    assert (row["Dataset"], row["State"], row["Symbols"], row["SymbolCount"]) == (
        "options.admin_messages",
        "RUNNING",
        "",
        None,
    )
    for table in (blink, kept, latest):
        m.release(table)


def test_releasing_the_admin_messages_unsubscribes_every_exchange(ws_session: WsSession) -> None:
    query = ws_session.session.options.admin_messages()
    kept = query.append_only()
    latest = query.ring(5)
    ws_session.subscribed("AD.*")
    ws_session.session.release(kept)
    ws_session.session.release(latest)
    ws_session.unsubscribed("AD.*")
    assert ws_session.connection("wss://socket.test/options").leases == 0


def test_admin_messages_resubscribe_after_a_reconnect(
    ws_session: WsSession,
    *,
    rows_when: Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]],
) -> None:
    kept = ws_session.session.options.admin_messages().append_only()
    ws_session.subscribed("AD.*")
    ws_session.publish(admin_message(302, 1, FLEX))
    rows_when(kept, lambda arrived: len(arrived) == 1)
    ws_session.server.close(1011)
    ws_session.server.wait_for(lambda s: len(s.connections) == 2)
    ws_session.subscribed("AD.*")  # on the new socket
    ws_session.publish(admin_message(323, 2, "ALERT"))
    assert [r["SequenceNumber"] for r in rows_when(kept, lambda arrived: len(arrived) == 2)] == [
        1,
        2,
    ]
    ws_session.session.release(kept)


def test_one_unreadable_admin_message_never_fails_the_table(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]],
) -> None:
    kept = ws_session.session.options.admin_messages().append_only()
    ws_session.subscribed("AD.*")
    unreadable = {**admin_message(302, 1, FLEX), "t": "soon"}  # its time cannot be read
    odd = {**admin_message(323, 2, "ALERT"), "x": "PHLX"}  # its exchange cannot be read
    ws_session.publish(unreadable, odd, admin_message(302, 3, "X"))
    got = rows_when(kept, lambda arrived: len(arrived) == 2)
    assert [(r["SequenceNumber"], r["Exchange"]) for r in got] == [(2, None), (3, 302)]
    (row,) = shown_streams(ws_session, dh_to_pylist)
    assert row["State"] == "RUNNING"
    ws_session.session.release(kept)


def test_a_refused_admin_subscription_fails_its_table_and_names_its_plans(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    ws_session.server.refuse["AD.*"] = "not authorized to access AD.*"
    notices = ws_session.session.options.admin_messages().blink()
    trades = ws_session.session.options.trades(dm.ALL).blink()
    ws_session.wait(
        lambda s: any(status.kind is _massive.StatusKind.REFUSED for status in s.statuses)
    )
    ws_session.flush()
    rows_by = {r["Dataset"]: r for r in shown_streams(ws_session, dh_to_pylist)}
    refused, running = rows_by["options.admin_messages"], rows_by["options.trades"]
    assert refused["State"] == "FAILED"
    assert "refused the subscription to AD.*; Massive said: not authorized" in refused["Error"]
    assert refused["Error"].endswith(
        "options.admin_messages streams only in real time, on the plans listed at "
        "https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/"
        "options.admin_messages.md"
    )
    assert running["State"] == "RUNNING"
    ws_session.session.release(notices)
    ws_session.session.release(trades)


def test_admin_messages_refuse_the_delayed_feed_at_the_call(
    make_ws_session: Callable[..., WsSession],
) -> None:
    delayed = HostDecision("delayed.test", Recency.DELAYED, "MassiveConfig.delayed_subscriptions")
    session = make_ws_session(
        decide=lambda *_: delayed, delayed_subscriptions={dm.Subscription.OPTIONS}
    )
    with pytest.raises(ValueError, match=r"lists dm\.Subscription\.OPTIONS"):
        session.session.options.admin_messages().append_only()
    assert session.urls == []  # no connection was opened


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
            lambda m: m.options.bars(PUT, dm.Timespan.MINUTE, adjusted=False),
            {
                "Date": "2023-03-13",
                "TimestampStart": 1678715580000000000,
                "TimestampEnd": 1678715640000000000,
                "Close": 11.82,
                "Transactions": 1,
            },
        ),
        (
            "us_options_opra/day_aggs_v1",
            "ticker,volume,open,close,high,low,window_start,transactions",
            [
                f"{PUT},200,11.82,9.83,11.82,8.09,1678680000000000000,62",
                f"{CALL},1,35.77,35.77,35.77,35.77,1678680000000000000,1",
            ],
            lambda m: m.options.bars(PUT, dm.Timespan.DAY, adjusted=False),
            {
                "Date": "2023-03-13",
                "TimestampStart": 1678680000000000000,
                "TimestampEnd": 1678766400000000000,
                "Close": 9.83,
                "Volume": 200.0,
            },
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
    out = rows(build(s3_session).history(DAY, DAY, source=HistorySource.FLAT_FILES, wait=True))
    assert fake_s3.requests == [("flatfiles", key)]
    (row,) = out  # the other contract's row is filtered out
    assert row["Symbol"] == PUT
    assert row["OptionRoot"] == PUT[2:-15]
    assert {name: row[name] for name in expected} == expected


def test_the_snapshots_revise_on_a_new_quote_or_trade() -> None:
    # An explicit APPEND poll of a snapshot appends each new state of a contract.
    revision = ("LastQuoteLastUpdated", "LastTradeSipTimestamp")
    revised = {spec.name: spec.revision_keys for spec in options.DATASETS if spec.revision_keys}
    assert revised == {
        "options.last_trade": ("Timestamp",),
        "options.previous_day_bar": ("TimestampStart",),
        "options.chain": revision,
        "options.snapshots": revision,
    }
    for spec in (options.CHAIN, options.SNAPSHOTS):
        assert set(revision) <= set(CATALOG.dataset_schema(spec).column_names)


def test_the_options_snapshots_and_chain_describe_a_trade_alike() -> None:
    by_name = CATALOG.by_name
    snapshots = {c.name: c for c in CATALOG.dataset_schema(by_name["options.snapshots"]).columns}
    chain = {c.name: c for c in CATALOG.dataset_schema(by_name["options.chain"]).columns}
    for name in ("LastTradePrice", "LastTradeExchange"):
        assert snapshots[name].description == chain[name].description
    assert "AAPL" not in snapshots["LastTradeId"].description
