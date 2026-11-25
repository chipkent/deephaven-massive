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
import re
import threading
from datetime import UTC, date, datetime, time
from typing import TYPE_CHECKING, Any, NamedTuple
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.stream import blink_to_append_only
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive import _log_limits, _massive
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
from deephaven_massive._datasets.filters import filter_keys
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import (
    ChannelFamily,
    ClassFromPrefix,
    HistorySource,
    IndexPrefix,
    Recency,
)
from deephaven_massive._errors import RowDataError
from deephaven_massive._ingest.recency_labels import delayed_refusal
from deephaven_massive._ingest.rest_requests import page_size
from deephaven_massive._log_limits import LoggedKeys
from deephaven_massive._massive import RowFault, Transport, to_batch
from deephaven_massive._session import CATALOG
from deephaven_massive._tables.problem_events import ProblemAction, ProblemCause
from deephaven_massive._tables.transports import HostDecision
from deephaven_massive.namespaces import _options as options
from deephaven_massive.namespaces._namespace import Namespace
from deephaven_massive.queries import (
    FetchQuery,
    HistoryQuery,
    NoSymbolStreamHistoryQuery,
    Query,
    StreamHistoryQuery,
    StreamQuery,
)
from deephaven_massive.queries._choice import query_class
from tests._fakes.clock import FakeClock

# The reply and envelope builders are plain functions of the fake, which no fixture can hand
# over; the server itself comes from the `fake_rest` fixture.
from tests._fakes.rest import Reply

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

    from deephaven.table_factory import InputTable

    from deephaven_massive._tables.problem_events import ProblemEvent
    from tests._fakes.rest import FakeRestServer
    from tests._fakes.s3 import FakeS3
    from tests._fakes.ws_session import WsSession


def whole(text: str) -> str:
    # A pattern that matches exactly `text`, so a test pins a message's whole text.
    return f"^{re.escape(text)}$"


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
    "contract_details",
    "daily_open_close",
    "open_interest",
    "open_interest_by_underlying",
    "greeks",
    "unusual_trades",
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
    # Every dataset is options data, charged to the options subscription. The symbols of the
    # open interest by underlying are the underlyings themselves, stocks and indices.
    assert {spec.asset_class for spec in options.DATASETS} == {dm.AssetClass.OPTIONS}
    grammars = {spec.name: spec.symbols_class for spec in options.DATASETS}
    assert grammars.pop("options.open_interest_by_underlying") is ClassFromPrefix.ANY
    assert set(grammars.values()) == {dm.AssetClass.OPTIONS}


def test_the_options_datasets_pass_every_catalog_gate() -> None:
    assert Catalog(options.DATASETS).problems() == ()


def test_every_options_dataset_whose_rows_name_a_contract_has_an_option_root() -> None:
    for spec in options.DATASETS:
        if not spec.names_symbols or spec.symbols_are_underlyings:
            # The admin messages name no contract, and the open interest by underlying none.
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
        "options.contract_details": None,
        "options.daily_open_close": None,
        "options.open_interest": None,
        "options.open_interest_by_underlying": None,
        "options.greeks": None,
        "options.unusual_trades": None,
    }
    # Every dataset but the chain and the contracts list, which take an underlying instead, and
    # the admin messages, whose rows name no contract, takes symbols: contracts, or the
    # underlyings of the open interest by underlying.
    takes = {spec.name for spec in options.DATASETS if CATALOG.takes_symbols(spec)}
    no_contracts = {"options.chain", "options.contracts", "options.admin_messages"}
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
    assert options.ADMIN_MESSAGES.sources == (
        (R, "/options/v1/admin-messages"),
        (Transport.WS, "options/AD"),
    )
    assert options.CHAIN.sources == ((R, "/v3/snapshot/options/{underlyingAsset}"),)
    assert options.SNAPSHOTS.sources == ((R, "/v3/snapshot"),)
    assert options.CONTRACTS.sources == ((R, "/v3/reference/options/contracts"),)
    assert options.CONTRACT_DETAILS.sources == (
        (R, "/v3/reference/options/contracts/{options_ticker}"),
    )
    assert options.DAILY_OPEN_CLOSE.sources == ((R, "/v1/open-close/{optionsTicker}/{date}"),)
    assert options.OPEN_INTEREST.sources == ((R, "/options/v1/open-interest"),)
    assert options.OPEN_INTEREST_BY_UNDERLYING.sources == (
        (R, "/options/v1/open-interest/underlying"),
    )
    assert options.GREEKS.sources == ((R, "/options/v1/analytics"),)
    assert options.UNUSUAL_TRADES.sources == ((R, "/options/v1/unusual-trades"),)


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (options.TRADES, StreamHistoryQuery),
        (options.LAST_TRADE, FetchQuery),
        (options.QUOTES, StreamHistoryQuery),
        (options.BARS, HistoryQuery),
        (options.PREVIOUS_DAY_BAR, FetchQuery),
        (options.FMV, StreamQuery),
        (options.ADMIN_MESSAGES, NoSymbolStreamHistoryQuery),
        (options.CHAIN, FetchQuery),
        (options.SNAPSHOTS, FetchQuery),
        (options.CONTRACTS, FetchQuery),
        (options.CONTRACT_DETAILS, FetchQuery),
        (options.DAILY_OPEN_CLOSE, FetchQuery),
        (options.OPEN_INTEREST, FetchQuery),
        (options.OPEN_INTEREST_BY_UNDERLYING, FetchQuery),
        (options.GREEKS, FetchQuery),
        (options.UNUSUAL_TRADES, FetchQuery),
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
        "options.admin_messages": 50_000,
        "options.chain": 250,
        "options.snapshots": 250,
        "options.contracts": 1_000,
        "options.contract_details": None,
        "options.daily_open_close": None,
        "options.open_interest": 50_000,
        "options.open_interest_by_underlying": 50_000,
        "options.greeks": 50_000,
        "options.unusual_trades": 50_000,
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
        ("open_interest.json", "/options/v1/open-interest"),
        ("open_interest_by_underlying.json", "/options/v1/open-interest/underlying"),
        ("greeks.json", "/options/v1/analytics"),
        ("unusual_trades.json", "/options/v1/unusual-trades"),
    ],
)
def test_fixtures_are_shaped_like_the_generated_fields(
    load_fixture: Callable[[str, str], bytes], name: str, path: str
) -> None:
    required = required_keys(path)
    body = json.loads(load_fixture(OWNER, name))
    rest = CATALOG.rest_path(path)
    if rest.result_key is None:  # the body is the row; its envelope keys are not fields
        rows = [{k: v for k, v in body.items() if k not in {"status", "request_id"}}]
    else:
        found = body[rest.result_key]
        rows = found if isinstance(found, list) else [found]
    batch, drift, marks = to_batch(rows, rest, required=required)
    assert marks == {}  # every row read whole
    assert batch.num_rows == len(rows)
    assert drift.extra == frozenset()


def test_the_snapshot_fixture_has_a_per_ticker_error_row(
    load_fixture: Callable[[str, str], bytes],
) -> None:
    rows = json.loads(load_fixture(OWNER, "snapshot.json"))["results"]
    rest = CATALOG.rest_path("/v3/snapshot")
    batch, _, marks = to_batch(rows, rest, required=("Ticker",))
    assert marks == {}  # every row read whole
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
        (m.options.fmv, StreamQuery),
    ):
        q = factory(["O:AAPL250117P00150000", "O:AAPL250117C00150000"])
        assert type(q) is cls
        assert q._request.symbols == ("O:AAPL250117C00150000", "O:AAPL250117P00150000")
        assert factory(dm.ALL)._request.symbols is dm.ALL
        with pytest.raises(
            ValueError, match=r"Pass 'O:AAPL250117C00150000', which dm\.canonical_symbol_option\("
        ):
            factory("AAPL250117C00150000")
        with pytest.raises(ValueError, match="is an underlying, not an OCC option symbol"):
            factory("AAPL")
        with pytest.raises(ValueError, match=r"is a crypto ticker, and this dataset takes option"):
            factory("X:BTCUSD")
        with pytest.raises(
            ValueError,
            match=r"^'O:BRK\.B250117C00400000' is not an OCC option symbol, which looks like ",
        ):
            factory("O:BRK.B250117C00400000")  # an OCC root has no dot


def test_a_symbol_table_is_followed(massive_session: dm.MassiveSession) -> None:
    watch = new_table([string_col("Symbol", [CONTRACT])])
    q = massive_session.options.trades(watch)
    assert isinstance(q._request.symbols, SymbolTable)
    no_symbol = (
        "The symbol table has no Symbol column. It has Ticker. Name the column of tickers Symbol, "
        'as in t.rename_columns("Symbol = Ticker").'
    )
    with pytest.raises(ValueError, match=whole(no_symbol)):
        massive_session.options.trades(new_table([string_col("Ticker", [CONTRACT])]))


def test_only_bars_that_stream_get_the_stream_methods(
    massive_session: dm.MassiveSession,
) -> None:
    bars = massive_session.options.bars
    for timespan, multiplier, expected in [
        (dm.Timespan.MINUTE, 1, StreamHistoryQuery),
        (dm.Timespan.SECOND, 1, StreamHistoryQuery),
        (dm.Timespan.DAY, 1, HistoryQuery),
        (dm.Timespan.MINUTE, 5, HistoryQuery),
        (dm.Timespan.HOUR, 1, HistoryQuery),
    ]:
        assert type(bars(CONTRACT, timespan, multiplier=multiplier)) is expected
    q = bars(CONTRACT, dm.Timespan.DAY, adjusted=False)
    assert (q._request.timespan, q._request.multiplier) == (dm.Timespan.DAY, 1)
    assert q._request.adjusted is False
    assert bars(CONTRACT, dm.Timespan.MINUTE)._request.adjusted is True
    # Massive answers HTTP 400 to session bars outside futures, so they are sent.
    assert bars(CONTRACT, dm.Timespan.SESSION)._request.timespan is dm.Timespan.SESSION
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan"):
        bars(CONTRACT, "minute")  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        bars(CONTRACT, dm.Timespan.MINUTE, adjusted=1)  # type: ignore[arg-type]  # the point: refused at run time
    # None would read as a path without `adjusted`: REST would send Massive's default,
    # adjusted, while the flat files, as traded, could serve the same call.
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        bars(CONTRACT, dm.Timespan.MINUTE, adjusted=None)  # type: ignore[arg-type]  # the point: refused at run time


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
    contracts = massive_session.options.contracts(underlyings)
    assert contracts._request == massive_session.options.contracts(underlying=underlyings)._request
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
    refusal = (
        "The table passed as underlying has no Symbol column. It has Ticker. Name the column of "
        'values Symbol, as in t.rename_columns("Symbol = Ticker").'
    )
    with pytest.raises(ValueError, match=whole(refusal)):
        massive_session.options.chain(tickers)
    with pytest.raises(ValueError, match=whole(refusal)):
        massive_session.options.contracts(underlying=tickers)
    renamed = tickers.rename_columns("Symbol = Ticker")  # the advice
    assert massive_session.options.chain(renamed)._request.value_table is not None


def test_a_chain_of_a_pair_fails_with_massive_s_answer(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    refusal = {"status": "ERROR", "request_id": "r", "error": "Invalid ticker."}
    fake_rest.reply_default(Reply(refusal, status=400))
    with pytest.raises(dm.MassiveError, match=r"HTTP 400 .*Invalid ticker\."):
        rest_session.options.chain("C:EURUSD").fetch(wait=True)
    assert [path for path, _ in fake_rest.get_queries()] == ["/v3/snapshot/options/C:EURUSD"]


def test_chain_rejects_bad_arguments(massive_session: dm.MassiveSession) -> None:
    chain = massive_session.options.chain
    with pytest.raises(
        ValueError,
        match=whole(
            "options.chain reads one underlying's chain, so it does not take dm.ALL. For every "
            "contract, call m.options.snapshots(dm.ALL).fetch()."
        ),
    ):
        chain(dm.ALL)  # type: ignore[arg-type]  # the point: refused at run time
    assert massive_session.options.snapshots(dm.ALL)._request.symbols is dm.ALL  # the advice
    with pytest.raises(TypeError, match=whole("underlying must be one ticker as a str, not int")):
        chain(1)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(
        ValueError, match=r"^'  ' is not a Massive stock ticker, which looks like 'AAPL'"
    ):
        chain("  ")
    with pytest.raises(ValueError, match=r"tickers are case-sensitive"):
        chain("aapl")
    with pytest.raises(
        ValueError,
        match=r"^'i:SPX' is an index ticker, not a stock one\. "
        r"Call dm\.canonical_symbol_index\('i:SPX'\)\.$",
    ):
        chain("i:SPX")
    # Massive answers HTTP 400 to the chain of a pair, so a pair is sent.
    assert request_of(chain("C:EURUSD"))["params"]["underlyingAsset"] == "C:EURUSD"
    assert request_of(chain("X:BTCUSD"))["params"]["underlyingAsset"] == "X:BTCUSD"
    # What Massive answers to a contract as the underlying is for it to judge.
    contract = "O:AAPL250117C00150000"
    assert request_of(chain(contract))["params"]["underlyingAsset"] == contract
    assert request_of(chain("KIMpL"))["params"]["underlyingAsset"] == "KIMpL"  # case is kept
    assert request_of(chain("I:SPX"))["params"]["underlyingAsset"] == "I:SPX"
    with pytest.raises(TypeError, match=r"contract_type must be dm\.ContractType"):
        chain("AAPL", contract_type="call")  # type: ignore[arg-type]  # the point: refused at run time
    own = "The filter 'contract_type' has its own argument. Pass contract_type= instead."
    with pytest.raises(ValueError, match=whole(own)):
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
    # The library sets `type`, for dm.ALL only, so the page lists no `type` filter key.
    assert "type" not in filter_keys(CATALOG, options.SNAPSHOTS)
    library = whole(
        "The library sets 'type' itself, so it cannot be a filter. Remove it from filters."
    )
    with pytest.raises(ValueError, match=library):
        snapshots(dm.ALL, filters={"type": "stocks"})
    with pytest.raises(ValueError, match=library):
        snapshots([CONTRACT], filters={"type": "options"})
    symbols = (
        "The filter 'ticker_any_of' sets the symbols of options.snapshots. Pass symbols= instead."
    )
    with pytest.raises(ValueError, match=whole(symbols)):
        snapshots(dm.ALL, filters={"ticker_any_of": [CONTRACT]})


def test_contracts_list_contracts_by_underlying(massive_session: dm.MassiveSession) -> None:
    contracts = massive_session.options.contracts
    listed = contracts(
        "NVDA",
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
    assert request_of(contracts(underlying="NVDA")) == request_of(contracts("NVDA"))
    assert request_of(contracts()) == {"symbols": None, "params": {}, "filters": {}}
    # False, the default, is Massive's own, so it is not sent.
    assert request_of(contracts(expired=False)) == request_of(contracts())
    # The request keeps the canonical index; only the wire gets the bare one.
    assert request_of(contracts("I:SPX"))["params"] == {"underlying_ticker": "I:SPX"}


def test_contract_details_read_named_contracts(massive_session: dm.MassiveSession) -> None:
    details = massive_session.options.contract_details
    described = details([CONTRACT])
    assert type(described) is FetchQuery
    assert request_of(described) == {"symbols": (CONTRACT,), "params": {}, "filters": {}}
    assert request_of(details(symbols=CONTRACT)) == request_of(described)
    assert request_of(details(CONTRACT, as_of="2025-01-02")) == {
        "symbols": (CONTRACT,),
        "params": {"as_of": date(2025, 1, 2)},
        "filters": {},
    }
    watch = new_table([string_col("Symbol", [CONTRACT])])
    symbols = details(watch)._request.symbols
    assert isinstance(symbols, SymbolTable)
    assert symbols.table is watch


def test_contracts_reject_bad_arguments(massive_session: dm.MassiveSession) -> None:
    contracts = massive_session.options.contracts
    # Massive lists no contracts of a pair and answers with no rows, so a pair is refused.
    for pair in ("C:EURUSD", "X:BTCUSD"):
        refusal = (
            f"underlying='{pair}' is a currency or crypto pair, which has no listed options. "
            "Pass a stock or index ticker, such as 'AAPL' or 'I:SPX'."
        )
        with pytest.raises(ValueError, match=whole(refusal)):
            contracts(pair)
    # What Massive answers to a contract as the underlying is for it to judge.
    contract = "O:AAPL250117C00150000"
    assert request_of(contracts(contract))["params"]["underlying_ticker"] == contract
    with pytest.raises(
        TypeError, match=whole("underlying must be one ticker as a str, not dm.ALL")
    ):
        contracts(dm.ALL)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(ValueError, match=r"tickers are case-sensitive"):
        contracts("aapl")
    with pytest.raises(TypeError, match="expired must be a bool, not int"):
        contracts(expired=1)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(ValueError, match="has no timezone"):
        contracts(as_of=datetime(2025, 1, 2, 9, 30, tzinfo=UTC).replace(tzinfo=None))
    for key in (
        "underlying_ticker",
        "contract_type",
        "as_of_gte",
        "expired",
    ):
        with pytest.raises(ValueError, match=rf"^The filter '{key}' has its own argument\. Pass "):
            contracts(filters={key: "x"})
    # Massive's list takes `ticker` too, but a contract is named through contract_details(),
    # where it gets the canonical check.
    for key in ("ticker", "ticker_any_of"):
        with pytest.raises(
            ValueError,
            match=whole(
                f"options.contracts has no filter {key!r}. To read named contracts, call "
                "m.options.contract_details(contracts).fetch()."
            ),
        ):
            contracts(filters={key: CONTRACT})


def test_contract_details_reject_bad_arguments(massive_session: dm.MassiveSession) -> None:
    details = massive_session.options.contract_details
    with pytest.raises(
        ValueError,
        match=whole(
            "'AAPL' is an underlying, not an options contract. To list its contracts, call "
            "m.options.contracts('AAPL').fetch()."
        ),
    ):
        details("AAPL")
    with pytest.raises(ValueError, match=r"^'I:SPX' is an underlying, .*contracts\('I:SPX'\)"):
        details("I:SPX")
    assert massive_session.options.contracts("I:SPX")._request is not None  # the advice
    every = (
        "options.contract_details does not take dm.ALL. To list contracts, call "
        "m.options.contracts().fetch()."
    )
    with pytest.raises(ValueError, match=whole(every)):
        details(dm.ALL)
    with pytest.raises(ValueError, match=r"is not an OCC option symbol"):
        details("aapl")  # not canonical: the symbol check explains it
    with pytest.raises(ValueError, match=r"^symbols holds no tickers\. Pass at least one ticker"):
        details([])
    needs = (
        "options.contract_details needs symbols, and symbols is None. Pass a ticker, a list of "
        "tickers, or a table with a Symbol column."
    )
    with pytest.raises(ValueError, match=whole(needs)):
        details(None)  # type: ignore[arg-type]  # the point: refused at run time
    # In a list, an underlying meets the symbol check, which names it an underlying too.
    with pytest.raises(ValueError, match=r"^'AAPL' is an underlying, not an OCC option symbol"):
        details(["AAPL"])
    with pytest.raises(
        ValueError,
        match=r"^options\.contract_details: as_of=.* is a time, and as_of takes a calendar date",
    ):
        details(CONTRACT, as_of=datetime(2025, 1, 3, 2, 0, tzinfo=UTC))
    with pytest.raises(TypeError):
        details(CONTRACT, underlying="AAPL")  # type: ignore[call-arg]  # the point: refused at run time


def test_as_of_and_date_refuse_a_time(massive_session: dm.MassiveSession) -> None:
    # Both are calendar dates: an instant would name a day only through a zone.
    late = datetime(2025, 1, 3, 2, 0, tzinfo=UTC)
    with pytest.raises(
        ValueError,
        match=r"^options\.contracts: as_of=.* is a time, and as_of takes a calendar date",
    ):
        massive_session.options.contracts(as_of=late)
    with pytest.raises(
        ValueError,
        match=r"^options\.daily_open_close: date=.* is a time, and date takes a calendar date",
    ):
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
        daily(CONTRACT, "2025-01-02", adjusted=0)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        daily(CONTRACT, "2025-01-02", adjusted=None)  # type: ignore[arg-type]  # the point: refused at run time
    # Massive has no grouped daily endpoint for options: the day's bars serve every contract.
    with pytest.raises(
        ValueError,
        match=whole(
            "options.daily_open_close does not take dm.ALL. For every contract's bar of one day, "
            "call m.options.bars(dm.ALL, dm.Timespan.DAY, adjusted=False).history('2025-01-02', "
            "'2025-01-02')."
        ),
    ):
        daily(dm.ALL, "2025-01-02")
    with pytest.raises(TypeError):
        daily(CONTRACT, 20250102)  # type: ignore[arg-type]  # the point: refused at run time


def test_enum_arguments_reject_plain_strings(massive_session: dm.MassiveSession) -> None:
    m = massive_session
    call, desc = "call", "desc"
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan"):
        m.options.bars(CONTRACT, "day")  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=r"contract_type must be dm\.ContractType"):
        m.options.chain("AAPL", contract_type=call)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=r"contract_type must be dm\.ContractType"):
        m.options.contracts(contract_type=call)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        m.options.chain("AAPL", order=desc)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        m.options.snapshots(CONTRACT, order=desc)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        m.options.contracts(order=desc)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=r"contract_type must be dm\.ContractType"):
        m.options.open_interest(contract_type=call)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        m.options.open_interest(order=desc)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=r"contract_type must be dm\.ContractType"):
        m.options.greeks(contract_type=call)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        m.options.greeks(order=desc)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        m.options.unusual_trades(order=desc)  # type: ignore[arg-type]  # the point: refused at run time


# --- Docstrings ------------------------------------------------------------------------------


def _factory(name: str) -> object:
    method: object = getattr(options.OptionsNamespace, name)
    return method


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
        massive_session.options.chain("AAPL", filters=["contract_type"])  # type: ignore[arg-type]  # the point: refused at run time


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


def where(table: Table, clause: str) -> list[dict[str, Any]]:
    # A `where` as a message tells the user to type it, run through Java: the Python wrapper
    # would copy this frame's locals into the query scope, where the JVM holds them, keeping the
    # table and its session alive past the test.
    return rows(Table(j_table=table.j_table.where(clause)))


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
        prev(CONTRACT, adjusted=1)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        prev(CONTRACT, adjusted=None)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(
        ValueError,
        match=whole(
            "options.previous_day_bar does not take dm.ALL. For every contract's bar of one day, "
            "call m.options.bars(dm.ALL, dm.Timespan.DAY, adjusted=False).history('2025-01-02', "
            "'2025-01-02')."
        ),
    ):
        prev(dm.ALL)
    every = massive_session.options.bars(dm.ALL, dm.Timespan.DAY, adjusted=False)  # the advice
    assert every._request.symbols is dm.ALL


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
    every = (
        "options.last_trade does not take dm.ALL. For every contract of one underlying, call "
        "m.options.chain('AAPL').fetch()."
    )
    with pytest.raises(ValueError, match=whole(every)):
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
    ranged = body(load_fixture, "range.json")
    assert isinstance(ranged, dict)
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
        if r.levelno == logging.WARNING and "cannot tell which day this one covers" in r.message
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
    # 14:30 is withheld, so a page of no other bar fails the read, with one WARNING.
    with pytest.raises(dm.MassiveError, match="could not read the 1 row"):
        rest_session.options.previous_day_bar(TSLA).fetch(wait=True)
    [warning] = stamp_warnings(caplog)
    assert warning.startswith(
        f"options.previous_day_bar: Massive sent a 1-day bar of {TSLA} stamped "
        "2024-11-29T19:30:00+00:00 (2024-11-29T14:30:00-05:00 in America/New_York). Massive is "
        "known to stamp bars of 1 day at "
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


def test_contracts_list_and_contract_details_read_each_contract(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply("/v3/reference/options/contracts", Reply(body(load_fixture, "contracts.json")))
    listed = rows(
        rest_session.options.contracts(
            "AAPL",
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
    described = rows(
        rest_session.options.contract_details(one, as_of="2021-11-01").fetch(wait=True)
    )
    assert fake_rest.get_queries()[-1] == (
        f"/v3/reference/options/contracts/{one}",
        {"as_of": ["2021-11-01"]},
    )
    assert [(r["Symbol"], r["Underlying"], r["ExpirationDate"]) for r in described] == [
        (one, "AAPL", "2021-11-19")
    ]


def test_a_poll_of_a_table_of_contracts_reads_each_one_s_details(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
    rows_when: Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]],
) -> None:
    # A table of contracts is read on the per-contract path, which takes `as_of`, so the poll
    # opens and each cycle asks for each contract the table holds.
    one = "O:AAPL211119C00085000"
    fake_rest.reply(
        wire(f"/v3/reference/options/contracts/{one}"), Reply(body(load_fixture, "contract.json"))
    )
    watch = new_table([string_col("Symbol", [one])])
    table = rest_session.options.contract_details(watch, as_of="2021-11-01").poll(60)
    (row,) = rows_when(table, bool)
    assert (row["Symbol"], row["Underlying"]) == (one, "AAPL")
    assert fake_rest.get_queries()[0] == (
        f"/v3/reference/options/contracts/{one}",
        {"as_of": ["2021-11-01"]},
    )


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


DJT_STOCK_CONTRACT = contract_row("O:DJT261120C00020000", "DJT", "OCASPS")
"""A contract on the stock DJT, by its CFI code (`S`, a stock)."""
DJT_INDEX_CONTRACT = contract_row("O:DJTW261120C00020000", "DJT", "OCEICS")
"""A contract on the index I:DJT, by its CFI code (`I`, an index), listed under the bare `DJT`."""


def serve_djt_contracts(server: FakeRestServer) -> None:
    # Massive's contracts list for `DJT`, which names the stock and the index: one of each.
    results = [DJT_STOCK_CONTRACT, DJT_INDEX_CONTRACT]
    server.reply(
        "/v3/reference/options/contracts",
        Reply(body={"status": "OK", "request_id": "r", "results": results}),
    )


def test_the_stock_s_contracts_beside_an_index_s_keep_their_underlying_and_warn_once(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # `I:DJT` is sent as `DJT`, which is a stock too: the stock's options are no index's, so
    # they keep the underlying Massive wrote rather than read as the index's.
    caplog.set_level(logging.WARNING, logger="deephaven_massive")
    serve_djt_contracts(fake_rest)
    listed = rest_session.options.contracts(underlying="I:DJT").fetch(wait=True)
    expected = [("O:DJT261120C00020000", "DJT"), ("O:DJTW261120C00020000", "I:DJT")]
    assert [(r["Symbol"], r["Underlying"]) for r in rows(listed)] == expected
    # A second read gets the same rows and warns no more.
    again = rest_session.options.contracts(underlying="I:DJT").fetch(wait=True)
    assert [(r["Symbol"], r["Underlying"]) for r in rows(again)] == expected
    assert fake_rest.get_queries()[0][1]["underlying_ticker"] == ["DJT"]
    assert warnings_of(caplog) == [
        (
            "options.contracts: the underlying 'I:DJT' is sent to Massive as "
            "underlying_ticker=DJT, which also names the stock DJT, and the CFI code of 1 of the "
            "rows marks an option on something other than an index, so Massive answered about the "
            "stock DJT too. They keep the underlying Massive wrote. The first is "
            "O:DJT261120C00020000, with Underlying DJT and Cfi OCASPS. Drop the rows with "
            '.where("Underlying != `DJT`").'
        )
    ]
    # The advice: the where clause keeps the index's rows alone.
    kept = where(listed, "Underlying != `DJT`")
    assert [(r["Symbol"], r["Underlying"]) for r in kept] == [("O:DJTW261120C00020000", "I:DJT")]


def test_the_index_s_contracts_beside_a_stock_s_keep_their_underlying_and_warn_once(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The stock `DJT` is sent as is, and the bare ticker names the index `I:DJT` too: the
    # index's options get their `I:` by their CFI code, which also says they were not asked for.
    caplog.set_level(logging.WARNING, logger="deephaven_massive")
    serve_djt_contracts(fake_rest)
    listed = rest_session.options.contracts(underlying="DJT").fetch(wait=True)
    expected = [("O:DJT261120C00020000", "DJT"), ("O:DJTW261120C00020000", "I:DJT")]
    assert [(r["Symbol"], r["Underlying"]) for r in rows(listed)] == expected
    # A second read gets the same rows and warns no more.
    again = rest_session.options.contracts(underlying="DJT").fetch(wait=True)
    assert [(r["Symbol"], r["Underlying"]) for r in rows(again)] == expected
    assert fake_rest.get_queries()[0][1]["underlying_ticker"] == ["DJT"]
    assert warnings_of(caplog) == [
        (
            "options.contracts: the underlying 'DJT' is sent to Massive as underlying_ticker=DJT, "
            "which also names the index I:DJT, and the CFI code of 1 of the rows marks an option "
            "on an index, so Massive answered about the index I:DJT too. They keep the underlying "
            "Massive wrote. The first is O:DJTW261120C00020000, with Underlying I:DJT and Cfi "
            'OCEICS. Drop the rows with .where("Underlying != `I:DJT`").'
        )
    ]
    kept = where(listed, "Underlying != `I:DJT`")
    assert [(r["Symbol"], r["Underlying"]) for r in kept] == [("O:DJT261120C00020000", "DJT")]


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
            "options.contracts: the contracts of I:SPX include 1 with no CFI code, so nothing "
            "marks their underlying as the index, and they keep Massive's bare 'SPX'. The first is "
            "O:SPX261120C00600000."
        )
    ]


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


def test_open_interest_sends_its_filters_and_reads_each_contract_s_day(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    path = "/options/v1/open-interest"
    fake_rest.reply(path, Reply(body(load_fixture, "open_interest.json")))
    serve_indices(fake_rest, "SPX")
    out = rows(
        rest_session.options.open_interest(
            ["O:SPY260123P00687000", "O:SPY260123C00687000"],
            root=["SPY", "SPXW"],
            contract_type=dm.ContractType.PUT,
            start=date(2026, 1, 20),
            end="2026-01-22",
            sort="open_interest",
            order=dm.Order.DESC,
            filters={"expiration_date_lte": "2026-01-23"},
        ).fetch(wait=True)
    )
    assert fake_rest.get_queries(one_value=True) == [
        (
            path,
            {
                "ticker.any_of": "O:SPY260123C00687000,O:SPY260123P00687000",
                "root.any_of": "SPY,SPXW",
                "contract_type": "put",
                "date.gte": "2026-01-20",
                "date.lte": "2026-01-22",
                "expiration_date.lte": "2026-01-23",
                "sort": "open_interest.desc",
                "limit": "50000",
            },
        ),
        # SPY is no listed index, so its root is a stock's, with no further request.
        (INDEX_LIST, {"market": "indices", "active": "true", "limit": "1000"}),
        (INDEX_LIST, {"market": "indices", "active": "false", "limit": "1000"}),
    ]
    picked = ("Symbol", "OptionRoot", "Date", "ContractType", "OpenInterest")
    assert [tuple(r[c] for c in picked) for r in out] == [
        ("O:SPY260123P00687000", "SPY", "2026-01-22", "put", 9871),
        ("O:SPY260123C00687000", "SPY", "2026-01-22", "call", 15234),
    ]
    assert (out[0]["ExpirationDate"], out[0]["StrikePrice"], out[0]["Underlying"]) == (
        "2026-01-23",
        687.0,
        "SPY",
    )


INDEX_LIST = "/v3/reference/tickers"


def serve_indices(server: FakeRestServer, *names: str) -> None:
    # Massive's index ticker list, one page, which the library reads once per session.
    rows = [{"ticker": f"I:{name}", "market": "indices"} for name in names]
    server.reply(INDEX_LIST, Reply({"status": "OK", "results": rows}))


def contract_reference(cfi: str) -> Reply:
    # Massive's contracts reference of one contract, by the CFI code that says its underlying.
    return Reply({"status": "OK", "results": {"cfi": cfi}})


def test_open_interest_sends_one_root_as_root(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/options/v1/open-interest", Reply({"status": "OK", "results": []}))
    rest_session.options.open_interest(root="SPXW").fetch(wait=True)
    assert fake_rest.get_queries(one_value=True) == [
        ("/options/v1/open-interest", {"root": "SPXW", "limit": "50000"})
    ]


BY_UNDERLYING = "/options/v1/open-interest/underlying"
CONTRACTS_LIST = "/v3/reference/options/contracts"


def by_underlying_row(underlying: str, day: str = "2026-01-23") -> dict[str, object]:
    # One row of the open interest by underlying: the totals of every option on the ticker.
    return {
        "date": day,
        "open_interest": 300,
        "open_interest_calls": 100,
        "open_interest_puts": 200,
        "underlying_ticker": underlying,
    }


def first_contract(cfi: str | None) -> Reply:
    # The contracts list's first contract of an underlying, with its CFI code, or no contract.
    if cfi is None:
        return Reply({"status": "OK", "results": []})
    return Reply({"status": "OK", "results": [{"ticker": "O:SPX261218C06000000", "cfi": cfi}]})


def test_open_interest_by_underlying_sends_each_index_bare_and_names_it_canonical(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    sample = body(load_fixture, "open_interest_by_underlying.json")
    assert isinstance(sample, dict)
    results = [*sample["results"], by_underlying_row("SPX")]
    fake_rest.reply(BY_UNDERLYING, Reply({**sample, "results": results, "next_url": None}))
    serve_indices(fake_rest, "SPX")
    fake_rest.reply(CONTRACTS_LIST, first_contract("OCEICS"))
    out = rows(
        rest_session.options.open_interest_by_underlying(
            ["SPY", "I:SPX", "QQQ"],
            start=date(2026, 1, 22),
            end="2026-01-23",
            sort="open_interest",
            order=dm.Order.DESC,
            filters={"underlying_ticker_gte": "A"},
        ).fetch(wait=True)
    )
    assert fake_rest.get_queries(one_value=True) == [
        (
            BY_UNDERLYING,
            {
                # The index goes bare, as the contracts list takes it; the request keeps `I:SPX`.
                "underlying_ticker.any_of": "SPX,QQQ,SPY",
                "underlying_ticker.gte": "A",
                "date.gte": "2026-01-22",
                "date.lte": "2026-01-23",
                "sort": "open_interest.desc",
                "limit": "50000",
            },
        ),
        (INDEX_LIST, {"market": "indices", "active": "true", "limit": "1000"}),
        (INDEX_LIST, {"market": "indices", "active": "false", "limit": "1000"}),
        # Only SPX is on the index list: its first contract, as of the row's day, says.
        (CONTRACTS_LIST, {"underlying_ticker": "SPX", "limit": "1", "as_of": "2026-01-23"}),
    ]
    picked = ("Symbol", "Date", "OpenInterest", "OpenInterestCalls", "OpenInterestPuts")
    assert [tuple(r[c] for c in picked) for r in out] == [
        ("SPY", "2026-01-23", 20987654, 8765432, 12222222),
        ("QQQ", "2026-01-23", 9876543, 4321098, 5555445),
        ("I:SPX", "2026-01-23", 300, 100, 200),
    ]
    assert "UnderlyingTicker" not in out[0]
    assert "Underlying" not in out[0]
    assert "OptionRoot" not in out[0]


def test_open_interest_by_underlying_asks_the_contracts_list_once_per_ticker(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="deephaven_massive")
    page = [
        by_underlying_row("SPX", "2026-01-22"),
        by_underlying_row("SPX", "2026-01-23"),
        by_underlying_row("DJT", "2026-01-23"),
        by_underlying_row("AAPL", "2026-01-23"),
    ]
    fake_rest.reply(BY_UNDERLYING, Reply({"status": "OK", "results": page}))
    serve_indices(fake_rest, "SPX", "DJT")
    # The first contract listed for DJT is a stock's (the Dow Jones Transportation Average
    # shares the ticker), so its row keeps the bare ticker: the documented limit.
    fake_rest.reply(CONTRACTS_LIST, first_contract("OCEICS"), first_contract("OCASPS"))
    for _ in range(2):
        out = rows(rest_session.options.open_interest_by_underlying().fetch(wait=True))
        assert [(r["Symbol"], r["Date"]) for r in out] == [
            ("I:SPX", "2026-01-22"),
            ("I:SPX", "2026-01-23"),
            ("DJT", "2026-01-23"),
            ("AAPL", "2026-01-23"),
        ]
    # Each ticker is asked about as of its latest day on the page, not its first row's.
    lookups = [q for q in fake_rest.get_queries(one_value=True) if q[0] == CONTRACTS_LIST]
    assert lookups == [
        (CONTRACTS_LIST, {"underlying_ticker": "SPX", "limit": "1", "as_of": "2026-01-23"}),
        (CONTRACTS_LIST, {"underlying_ticker": "DJT", "limit": "1", "as_of": "2026-01-23"}),
    ]
    # The ticker that names a stock too is warned about once, when it is classified.
    assert warnings_of(caplog) == [
        (
            "DJT is on Massive's index list, and the first contract Massive lists for it is on "
            "a stock, so its rows keep the bare ticker DJT. Massive's row may also count the "
            "options on I:DJT, and the library can't separate them. Nothing to do: the row is "
            "Massive's total for the ticker."
        )
    ]


def test_open_interest_by_underlying_polls_with_the_index_prefix_from_its_first_cycle(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(BY_UNDERLYING, Reply({"status": "OK", "results": [by_underlying_row("SPX")]}))
    serve_indices(fake_rest, "SPX")
    fake_rest.reply(CONTRACTS_LIST, first_contract("OCEICS"))
    watchlist = new_table([string_col("Symbol", ["I:SPX"])])
    table = rest_session.options.open_interest_by_underlying(watchlist).poll(60)
    pace = threading.Event()  # never set; only paces the bounded wait for the first cycle
    found: list[dict[str, Any]] = []
    for _ in range(500):
        found = rows(table)
        if found:
            break
        pace.wait(0.02)
    rest_session.release(table)
    # The keyed row carries the watchlist's spelling, so a join on it works.
    assert [(r["Symbol"], r["Date"], r["OpenInterest"]) for r in found] == [
        ("I:SPX", "2026-01-23", 300)
    ]
    assert [q for q in fake_rest.get_queries(one_value=True) if q[0] == CONTRACTS_LIST] == [
        (CONTRACTS_LIST, {"underlying_ticker": "SPX", "limit": "1", "as_of": "2026-01-23"})
    ]


def test_open_interest_by_underlying_withholds_a_ticker_the_contracts_list_cannot_classify(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    spx_rows = [by_underlying_row("SPX"), by_underlying_row("SPX", "2026-01-22")]
    page = [*spx_rows, by_underlying_row("SPY")]
    with_spy = Reply({"status": "OK", "results": page})
    fake_rest.reply(BY_UNDERLYING, with_spy, with_spy, Reply({"status": "OK", "results": spx_rows}))
    serve_indices(fake_rest, "SPX")
    fake_rest.reply(CONTRACTS_LIST, first_contract(None))
    events: list[ProblemEvent] = []
    monkeypatch.setattr(rest_session._core, "problems", events.extend)
    out = rows(rest_session.options.open_interest_by_underlying().fetch(wait=True))
    # SPX may be an index's ticker without its I:, so its rows are left out and listed.
    assert [r["Symbol"] for r in out] == ["SPY"]
    [event] = events
    assert (event.cause, event.action, event.symbol, event.field, event.rows) == (
        ProblemCause.UNREADABLE_SYMBOLS,
        ProblemAction.WITHHELD,
        "SPX",
        "Symbol",
        2,
    )
    assert event.policy is dm.OnError.LOG
    assert event.detail.startswith(
        "Massive's contracts list has no contract with a CFI code for SPX as of 2026-01-23, so "
        "the library can't tell whether SPX is an index (I:SPX) or not"
    )
    strict = dm.ErrorPolicy(unreadable_symbols=dm.OnError.RAISE)
    with pytest.raises(RowDataError, match=r"2 under unreadable_symbols.*code for SPX"):
        rest_session.options.open_interest_by_underlying(on_error=strict).fetch(wait=True)
    # Every row the read would return is withheld, so it fails under the default LOG too.
    with pytest.raises(RowDataError, match=r"none of the 2 rows.*code for SPX"):
        rest_session.options.open_interest_by_underlying("I:SPX").fetch(wait=True)


def test_open_interest_by_underlying_names_the_ticker_when_its_lookup_fails(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(BY_UNDERLYING, Reply({"status": "OK", "results": [by_underlying_row("SPX")]}))
    serve_indices(fake_rest, "SPX")
    fake_rest.reply(
        CONTRACTS_LIST, Reply({"status": "NOT_AUTHORIZED", "message": "no"}, status=403)
    )
    with pytest.raises(
        dm.EntitlementError,
        match=r"^The library cannot tell whether the underlying 'SPX' is an index, since reading "
        r"Massive's contracts list for SPX failed\. Massive answered ",
    ):
        rest_session.options.open_interest_by_underlying().fetch(wait=True)


def test_an_index_prefix_no_case_handles_fails_the_read(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/options/v1/open-interest", Reply({"status": "OK", "results": [LIST_ROW]}))
    spec = options.OPEN_INTEREST
    object.__setattr__(spec, "index_prefix", "other")  # past the frozen spec's checks
    try:
        # The verb reports the defect as the library's, with the refused value.
        with pytest.raises(
            dm.MassiveError,
            match=r"unexpected error: AssertionError\(.*got: 'other'.\)\. This is a bug",
        ):
            rest_session.options.open_interest().fetch(wait=True)
    finally:
        object.__setattr__(spec, "index_prefix", IndexPrefix.BY_ROOT)
    assert spec.index_prefix is IndexPrefix.BY_ROOT


def test_greeks_of_an_index_asked_bare_whose_every_root_is_unclassified_fail(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    page = [
        {"ticker": t, "root": "SPXW", "date": "2026-11-13", "underlying_ticker": "SPX"}
        for t in UNCLASSIFIED
    ]
    fake_rest.reply("/options/v1/analytics", Reply({"status": "OK", "results": page}))
    serve_indices(fake_rest, "SPX")
    fake_rest.reply(
        wire(f"/v3/reference/options/contracts/{UNCLASSIFIED[0]}"),
        Reply({"status": "OK", "results": {}}),
    )
    # No row is kept, so there is nothing to check against the index asked for; the read fails
    # because every row it would return is withheld.
    with pytest.raises(RowDataError, match=r"none of the 2 rows.*option root SPXW"):
        rest_session.options.greeks(underlying="I:SPX").fetch(wait=True)


def test_open_interest_by_underlying_takes_only_stock_and_index_tickers(
    massive_session: dm.MassiveSession,
) -> None:
    factory = massive_session.options.open_interest_by_underlying
    assert factory()._request.symbols is None
    with pytest.raises(
        ValueError,
        match=whole(
            "options.open_interest_by_underlying does not take dm.ALL. Leave symbols out to read "
            "every underlying."
        ),
    ):
        factory(dm.ALL)
    with pytest.raises(
        ValueError,
        match=whole(
            "'C:EURUSD' is a forex ticker, and options are listed on stocks and indices only. "
            "Pass a stock or index ticker, such as 'AAPL' or 'I:SPX'."
        ),
    ):
        factory(["SPY", "C:EURUSD"])
    with pytest.raises(
        ValueError,
        match=whole(
            f"{CONTRACT!r} is an option ticker, and options are listed on stocks and indices "
            "only. Pass a stock or index ticker, such as 'AAPL' or 'I:SPX'."
        ),
    ):
        factory(CONTRACT)
    with pytest.raises(ValueError, match="'spx' is not a Massive stock ticker"):
        factory("spx")
    with pytest.raises(ValueError, match="symbols holds no tickers"):
        factory([])
    with pytest.raises(ValueError, match=r"^The filter 'underlying_ticker' "):
        factory(filters={"underlying_ticker": "SPY"})
    with pytest.raises(ValueError, match=r"^The filter 'underlying_ticker_any_of' "):
        factory(filters={"underlying_ticker_any_of": "SPY"})


def test_open_interest_by_underlying_skips_a_watchlist_row_that_is_no_underlying(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_rest.reply(BY_UNDERLYING, Reply({"status": "OK", "results": [by_underlying_row("SPY")]}))
    serve_indices(fake_rest)
    watchlist = new_table([string_col("Symbol", ["SPY", "C:EURUSD", "I:NDX"])])
    events: list[ProblemEvent] = []
    monkeypatch.setattr(rest_session._core, "problems", events.extend)
    out = rows(rest_session.options.open_interest_by_underlying(watchlist).fetch(wait=True))
    assert [r["Symbol"] for r in out] == ["SPY"]
    # The pair is skipped as a row that is no underlying; the two tickers are sent, bare.
    assert fake_rest.get_queries(one_value=True)[0] == (
        BY_UNDERLYING,
        {"underlying_ticker.any_of": "NDX,SPY", "limit": "50000"},
    )
    assert [(e.cause, e.symbol) for e in events] == [
        (ProblemCause.NONCANONICAL_SYMBOLS, "C:EURUSD")
    ]
    assert events[0].detail == (
        "'C:EURUSD' is not a stock or index ticker such as 'AAPL' or 'I:SPX'."
    )


def test_open_interest_by_underlying_follows_a_watchlist_that_names_an_index(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_input_table: Callable[..., InputTable],
) -> None:
    # A refreshing watchlist is read symbol by symbol, and each request's rows are given to
    # the symbols it names: the index as the watchlist spells it, I:SPX, although the wire
    # got SPX.
    fake_rest.reply(
        BY_UNDERLYING,
        Reply({"status": "OK", "results": [by_underlying_row("SPX"), by_underlying_row("SPY")]}),
    )
    serve_indices(fake_rest, "SPX")
    fake_rest.reply(CONTRACTS_LIST, first_contract("OCEICS"))
    watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols="Symbol")
    watchlist.add(new_table([string_col("Symbol", ["I:SPX", "SPY"])]))
    table = rest_session.options.open_interest_by_underlying(watchlist).fetch(wait=True)
    try:
        assert sorted((r["Symbol"], r["Date"]) for r in rows(table)) == [
            ("I:SPX", "2026-01-23"),
            ("SPY", "2026-01-23"),
        ]
    finally:
        rest_session.release(table)
    assert fake_rest.get_queries(one_value=True)[0] == (
        BY_UNDERLYING,
        {"underlying_ticker.any_of": "SPX,SPY", "limit": "50000"},
    )
    assert rows(rest_session.problems()) == []


def test_open_interest_by_underlying_is_charged_to_the_options_subscription(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The rows are options data, so `rest_requests_per_minute={dm.Subscription.OPTIONS: n}`
    # paces these requests with the other options datasets', although the symbols are stock
    # and index tickers.
    monkeypatch.setattr(_log_limits, "_logged", LoggedKeys())  # as in a fresh process
    fake_rest.reply(BY_UNDERLYING, Reply({"status": "OK", "results": [by_underlying_row("SPY")]}))
    serve_indices(fake_rest)
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive._ingest.rest"):
        rest_session.options.open_interest_by_underlying(["SPY", "I:SPX"]).fetch(wait=True)
    charged = [m for m in caplog.messages if " is charged to the " in m]
    assert charged == [
        (
            f"options.open_interest_by_underlying {BY_UNDERLYING}, symbols I:SPX, SPY, is "
            "charged to the options bucket as its data's class. Later requests of this path "
            "charged to it are not logged."
        )
    ]


def test_open_interest_by_underlying_sends_250_underlyings_a_request(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(BY_UNDERLYING, Reply({"status": "OK", "results": []}))
    tickers = [f"T{n:03d}" for n in range(251)]
    rest_session.options.open_interest_by_underlying(tickers).fetch(wait=True)
    # The two requests fan out, so their order is not fixed.
    sent = sorted(
        (
            q[1]["underlying_ticker.any_of"].split(",")
            for q in fake_rest.get_queries(one_value=True)
        ),
        key=len,
    )
    assert [len(chunk) for chunk in sent] == [1, 250]
    assert (sent[0], sent[1][:2]) == (["T250"], ["T000", "T001"])


def test_greeks_send_their_filters_and_read_each_contract_s_day(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    path = "/options/v1/analytics"
    fake_rest.reply(path, Reply(body(load_fixture, "greeks.json")))
    serve_indices(fake_rest, "SPX")
    out = rows(
        rest_session.options.greeks(
            ["O:AMD261121C00220000", "O:AMD261121P00220000"],
            underlying=["AMD", "I:SPX"],
            contract_type=dm.ContractType.CALL,
            start=date(2026, 10, 27),
            end="2026-10-29",
            sort="implied_volatility",
            order=dm.Order.DESC,
            filters={"expiration_date": "2026-11-21"},
        ).fetch(wait=True)
    )
    assert fake_rest.get_queries(one_value=True) == [
        (
            path,
            {
                "ticker.any_of": "O:AMD261121C00220000,O:AMD261121P00220000",
                # The index goes bare, as the contracts list takes it.
                "underlying_ticker.any_of": "AMD,SPX",
                "contract_type": "call",
                "date.gte": "2026-10-27",
                "date.lte": "2026-10-29",
                "expiration_date": "2026-11-21",
                "sort": "implied_volatility.desc",
                "limit": "50000",
            },
        ),
        # AMD is no listed index, so its root is a stock's, with no further request.
        (INDEX_LIST, {"market": "indices", "active": "true", "limit": "1000"}),
        (INDEX_LIST, {"market": "indices", "active": "false", "limit": "1000"}),
    ]
    [row] = out
    picked = ("Symbol", "OptionRoot", "Underlying", "Date", "ContractType", "ExpirationDate")
    assert tuple(row[c] for c in picked) == (
        "O:AMD261121C00220000",
        "AMD",
        "AMD",
        "2026-10-29",
        "call",
        "2026-11-21",
    )
    greeks = ("Delta", "Gamma", "Theta", "Vega", "Rho", "ImpliedVolatility")
    assert tuple(row[c] for c in greeks) == (0.5312, 0.0218, -0.3861, 0.2204, 0.0689, 0.5137)
    inputs = ("BidPrice", "AskPrice", "OptionPrice", "UnderlyingPrice", "DividendYield")
    assert tuple(row[c] for c in inputs) == (9.35, 9.5, 9.425, 221.35, 0.0)
    assert (row["RiskFreeRate"], "Bid" in row, "Ask" in row) == (0.0431, False, False)
    assert (row["Model"], row["ExerciseStyle"], row["StrikePrice"]) == (
        "binomial",
        "american",
        220.0,
    )
    assert (row["OpenInterest"], row["Volume"]) == (14823, 6210)
    # Nanosecond instants, every digit kept.
    assert (row["QuoteTimestamp"], row["UnderlyingTimestamp"]) == (
        1793303099841234000,
        1793303099812004400,
    )
    assert "Root" not in row
    assert "UnderlyingTicker" not in row


# --- A day without its date -----------------------------------------------------------------


class DatedList(NamedTuple):
    # A dataset that dates its rows by Massive's `date`: the call, Massive's path, its sample
    # page, and the symbol and day of the sample's second row, which has its date.
    call: Callable[[dm.MassiveSession], FetchQuery]
    path: str
    fixture: str
    whole: tuple[str, str]


DATED_LISTS = (
    pytest.param(
        DatedList(
            lambda m: m.options.open_interest(),
            "/options/v1/open-interest",
            "open_interest.json",
            ("O:SPY260123C00687000", "2026-01-22"),
        ),
        id="open_interest",
    ),
    pytest.param(
        DatedList(
            lambda m: m.options.open_interest_by_underlying(),
            BY_UNDERLYING,
            "open_interest_by_underlying.json",
            ("QQQ", "2026-01-23"),
        ),
        id="open_interest_by_underlying",
    ),
    pytest.param(
        DatedList(
            lambda m: m.options.greeks(),
            "/options/v1/analytics",
            "greeks.json",
            ("O:AMD261121P00220000", "2026-10-29"),
        ),
        id="greeks",
    ),
)


def undated_page(load_fixture: Callable[[str, str], bytes], fixture: str) -> Reply:
    # Massive's first sample row without its date, beside a whole row: the sample's second,
    # or, where the sample has one row, the put of the same strike.
    sample = body(load_fixture, fixture)
    assert isinstance(sample, dict)
    first, *rest = sample["results"]
    whole = rest[0] if rest else {**first, "ticker": "O:AMD261121P00220000", "contract_type": "put"}
    page = [{k: v for k, v in first.items() if k != "date"}, whole]
    return Reply({"status": "OK", "results": page})


@pytest.mark.parametrize("dated", DATED_LISTS)
def test_a_row_without_its_date_is_withheld(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
    monkeypatch: pytest.MonkeyPatch,
    dated: DatedList,
) -> None:
    events: list[ProblemEvent] = []
    monkeypatch.setattr(rest_session._core, "problems", events.extend)
    fake_rest.reply(dated.path, undated_page(load_fixture, dated.fixture))
    serve_indices(fake_rest)
    out = rows(dated.call(rest_session).fetch(wait=True))
    # No trace of the undated row, not even a null `Date`; the whole one beside it lands.
    assert [(r["Symbol"], r["Date"]) for r in out] == [dated.whole]
    assert [(e.cause, e.rows, e.stream_id is not None) for e in events] == [
        (ProblemCause.UNREADABLE_KEYS, 1, True)
    ]


@pytest.mark.parametrize("dated", DATED_LISTS)
def test_a_row_without_its_date_under_raise_fails_the_table(
    *, fake_rest: FakeRestServer, load_fixture: Callable[[str, str], bytes], dated: DatedList
) -> None:
    fake_rest.reply(dated.path, undated_page(load_fixture, dated.fixture))
    serve_indices(fake_rest)
    config = dm.MassiveConfig(
        api_key="test-key",
        rest_base_url=fake_rest.url,
        on_error=dm.ErrorPolicy(unreadable_keys=dm.OnError.RAISE),
    )
    with (
        dm.MassiveSession(config) as m,
        pytest.raises(RowDataError, match="1 under unreadable_keys"),
    ):
        dated.call(m).fetch(wait=True)


def test_greeks_send_one_underlying_bare_and_keep_the_canonical_one(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/options/v1/analytics", Reply({"status": "OK", "results": []}))
    query = rest_session.options.greeks(underlying="I:SPX")
    # The request keeps the canonical index; only the wire gets the bare one.
    assert request_of(query)["params"] == {"underlying_ticker": "I:SPX"}
    query.fetch(wait=True)
    assert fake_rest.get_queries(one_value=True) == [
        ("/options/v1/analytics", {"underlying_ticker": "SPX", "limit": "50000"})
    ]


DJT_STOCK_GREEKS = "O:DJT261218C00030000"
"""A contract of the root DJT, which is the stock's."""
DJT_INDEX_GREEKS = "O:DJTW261218C00030000"
"""A contract of the root DJTW, which is the index's."""


def serve_djt_greeks(server: FakeRestServer) -> None:
    # One day's greeks of `DJT`, which names the stock and the index: one contract of each
    # root, both written with the bare underlying, and each root's reference says whose it is.
    page = [
        {"ticker": t, "root": root, "date": "2026-11-13", "underlying_ticker": "DJT"}
        for t, root in ((DJT_STOCK_GREEKS, "DJT"), (DJT_INDEX_GREEKS, "DJTW"))
    ]
    server.reply("/options/v1/analytics", Reply({"status": "OK", "results": page}))
    serve_indices(server, "DJT")
    server.reply(
        wire(f"/v3/reference/options/contracts/{DJT_STOCK_GREEKS}"), contract_reference("OCASPS")
    )
    server.reply(
        wire(f"/v3/reference/options/contracts/{DJT_INDEX_GREEKS}"), contract_reference("OCEICS")
    )


def test_greeks_send_two_spellings_of_one_underlying_once_and_warn_about_neither(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The stock DJT and the index I:DJT are both `DJT` to Massive: one value on the wire, and
    # the request keeps both. Each root's rows keep its own underlying, and neither instrument
    # is unasked for, so nothing warns.
    caplog.set_level(logging.WARNING, logger="deephaven_massive")
    serve_djt_greeks(fake_rest)
    query = rest_session.options.greeks(underlying=["DJT", "I:DJT"])
    assert request_of(query)["params"] == {"underlying_ticker.any_of": ("DJT", "I:DJT")}
    out = rows(query.fetch(wait=True))
    assert fake_rest.get_queries(one_value=True)[0] == (
        "/options/v1/analytics",
        {"underlying_ticker.any_of": "DJT", "limit": "50000"},
    )
    assert [(r["Symbol"], r["OptionRoot"], r["Underlying"]) for r in out] == [
        (DJT_STOCK_GREEKS, "DJT", "DJT"),
        (DJT_INDEX_GREEKS, "DJTW", "I:DJT"),
    ]
    assert warnings_of(caplog) == []


def test_greeks_name_an_index_underlying_by_root(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    contract = "O:SPXW261218C06000000"
    page = [{"ticker": contract, "root": "SPXW", "date": "2026-11-13", "underlying_ticker": "SPX"}]
    fake_rest.reply("/options/v1/analytics", Reply({"status": "OK", "results": page}))
    serve_indices(fake_rest, "SPX")
    reference = f"/v3/reference/options/contracts/{contract}"
    fake_rest.reply(wire(reference), contract_reference("OCEICS"))
    [out] = rows(rest_session.options.greeks(underlying="I:SPX").fetch(wait=True))
    assert (out["Symbol"], out["OptionRoot"], out["Underlying"]) == (contract, "SPXW", "I:SPX")
    # The root was asked about once, as of the row's day; the index underlying asked for is
    # not held against the rows, whose roots say which are the index's.
    assert [q for q in fake_rest.get_queries(one_value=True) if q[0] == reference] == [
        (reference, {"as_of": "2026-11-13"})
    ]


def test_greeks_of_the_stock_beside_an_index_s_keep_their_underlying_and_warn_once(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # `I:DJT` is sent as `DJT`, which is a stock too: the stock root's contracts are no index's,
    # so they keep the underlying Massive wrote, while the index's weekly root gets its `I:`.
    caplog.set_level(logging.WARNING, logger="deephaven_massive")
    serve_djt_greeks(fake_rest)
    out = rest_session.options.greeks(underlying="I:DJT").fetch(wait=True)
    expected = [(DJT_STOCK_GREEKS, "DJT"), (DJT_INDEX_GREEKS, "I:DJT")]
    assert [(r["Symbol"], r["Underlying"]) for r in rows(out)] == expected
    # A second read gets the same rows and warns no more.
    again = rest_session.options.greeks(underlying="I:DJT").fetch(wait=True)
    assert [(r["Symbol"], r["Underlying"]) for r in rows(again)] == expected
    assert fake_rest.get_queries()[0][1]["underlying_ticker"] == ["DJT"]
    assert warnings_of(caplog) == [
        (
            "options.greeks: the underlying 'I:DJT' is sent to Massive as underlying_ticker=DJT, "
            "which also names the stock DJT, and the option root of 1 of the rows is the stock's, "
            "so Massive answered about the stock DJT too. They keep the underlying Massive wrote. "
            "The first is O:DJT261218C00030000, with Underlying DJT. Pass underlying=['DJT', "
            "'I:DJT'] to ask for both, or drop the rows with .where(\"Underlying != `DJT`\")."
        )
    ]
    # The advice: the where clause keeps the index's rows alone, and asking for both reads both.
    kept = where(out, "Underlying != `DJT`")
    assert [(r["Symbol"], r["Underlying"]) for r in kept] == [(DJT_INDEX_GREEKS, "I:DJT")]
    both = rows(rest_session.options.greeks(underlying=["DJT", "I:DJT"]).fetch(wait=True))
    assert [(r["Symbol"], r["Underlying"]) for r in both] == [
        (DJT_STOCK_GREEKS, "DJT"),
        (DJT_INDEX_GREEKS, "I:DJT"),
    ]


def test_greeks_of_the_index_beside_a_stock_s_keep_their_underlying_and_warn_once(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The stock `DJT` is sent as is, and the bare ticker names the index `I:DJT` too: the index
    # root's contracts get their `I:`, which also says they were not asked for.
    caplog.set_level(logging.WARNING, logger="deephaven_massive")
    serve_djt_greeks(fake_rest)
    out = rest_session.options.greeks(underlying="DJT").fetch(wait=True)
    expected = [(DJT_STOCK_GREEKS, "DJT"), (DJT_INDEX_GREEKS, "I:DJT")]
    assert [(r["Symbol"], r["Underlying"]) for r in rows(out)] == expected
    # A second read gets the same rows and warns no more.
    again = rest_session.options.greeks(underlying="DJT").fetch(wait=True)
    assert [(r["Symbol"], r["Underlying"]) for r in rows(again)] == expected
    assert fake_rest.get_queries()[0][1]["underlying_ticker"] == ["DJT"]
    assert warnings_of(caplog) == [
        (
            "options.greeks: the underlying 'DJT' is sent to Massive as underlying_ticker=DJT, "
            "which also names the index I:DJT, and the option root of 1 of the rows is the "
            "index's, so Massive answered about the index I:DJT too. They keep the underlying "
            "Massive wrote. The first is O:DJTW261218C00030000, with Underlying I:DJT. Pass "
            "underlying=['DJT', 'I:DJT'] to ask for both, or drop the rows with "
            '.where("Underlying != `I:DJT`").'
        )
    ]
    kept = where(out, "Underlying != `I:DJT`")
    assert [(r["Symbol"], r["Underlying"]) for r in kept] == [(DJT_STOCK_GREEKS, "DJT")]


def test_greeks_of_several_underlyings_warn_about_an_unasked_instrument_s_contracts(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The list form sends `DJT` bare too, so the stock's contracts are warned about the same.
    caplog.set_level(logging.WARNING, logger="deephaven_massive")
    serve_djt_greeks(fake_rest)
    out = rows(rest_session.options.greeks(underlying=["AMD", "I:DJT"]).fetch(wait=True))
    assert [(r["Symbol"], r["Underlying"]) for r in out] == [
        (DJT_STOCK_GREEKS, "DJT"),
        (DJT_INDEX_GREEKS, "I:DJT"),
    ]
    assert fake_rest.get_queries()[0][1]["underlying_ticker.any_of"] == ["AMD,DJT"]
    assert warnings_of(caplog) == [
        (
            "options.greeks: the underlying 'I:DJT' is sent to Massive as "
            "underlying_ticker.any_of=DJT, which also names the stock DJT, and the option root of "
            "1 of the rows is the stock's, so Massive answered about the stock DJT too. They keep "
            "the underlying Massive wrote. The first is O:DJT261218C00030000, with Underlying DJT. "
            "Pass underlying=['DJT', 'I:DJT'] to ask for both, or drop the rows with "
            '.where("Underlying != `DJT`").'
        )
    ]


def test_greeks_refuse_an_underlying_that_is_not_a_stock_or_index(
    massive_session: dm.MassiveSession,
) -> None:
    greeks = massive_session.options.greeks
    with pytest.raises(
        ValueError,
        match=whole(
            "underlying=[] is empty, so it would select nothing. Leave underlying out for every "
            "underlying."
        ),
    ):
        greeks(underlying=[])
    with pytest.raises(TypeError, match="underlying must be a str, a sequence of str, or None"):
        greeks(underlying=5)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match="underlying must be one ticker as a str, not int"):
        greeks(underlying=["AAPL", 5])  # type: ignore[list-item]  # the point: refused at run time
    pair = "underlying='C:EURUSD' is a currency or crypto pair, which has no listed options."
    with pytest.raises(ValueError, match=re.escape(pair)):
        greeks(underlying="C:EURUSD")
    with pytest.raises(ValueError, match=re.escape(pair)):
        greeks(underlying=["AAPL", "C:EURUSD"])
    with pytest.raises(ValueError, match="'spx' is not a Massive stock ticker"):
        greeks(underlying="spx")
    with pytest.raises(
        ValueError,
        match=whole(
            "The filter 'underlying_ticker' has its own argument. Pass underlying= instead."
        ),
    ):
        greeks(filters={"underlying_ticker": "AAPL"})
    with pytest.raises(
        ValueError,
        match=whole(
            "The filter 'underlying_ticker_any_of' has its own argument. Pass underlying= instead."
        ),
    ):
        greeks(filters={"underlying_ticker_any_of": "AAPL"})


LIST_SPECS = [options.OPEN_INTEREST, options.GREEKS, options.UNUSUAL_TRADES]
"""The three options list datasets, whose rows carry Massive's `root` beside the contract."""


@pytest.mark.parametrize(
    ("spec", "described"),
    [
        (options.OPEN_INTEREST, "The OCC root symbol of the contract (e.g., SPXW)."),
        (options.GREEKS, "Option root symbol"),
        (options.UNUSUAL_TRADES, "The OCC root symbol of the contract (e.g., SPXW)."),
    ],
)
def test_option_root_is_massive_s_root_where_the_rows_carry_one(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, spec: DatasetSpec, described: str
) -> None:
    # A root unlike the symbol's shows the value is Massive's, not read from `Symbol`.
    row = {
        "ticker": "O:SPY260123C00687000",
        "root": "XYZ",
        "date": "2026-01-22",
        "sip_timestamp": 1761751432123456800,
        "sequence_number": 1,
    }
    path = spec.rest_paths[0]
    fake_rest.reply(path, Reply({"status": "OK", "results": [row]}))
    [out] = rows(list_query(rest_session, spec).fetch(wait=True))
    assert (out["Symbol"], out["OptionRoot"]) == ("O:SPY260123C00687000", "XYZ")
    assert "Root" not in out
    [column] = [c for c in CATALOG.dataset_schema(spec).columns if c.name == "OptionRoot"]
    assert column.description.startswith(described)


def test_unusual_trades_read_each_trade_at_its_sip_time(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    path = "/options/v1/unusual-trades"
    fake_rest.reply(path, Reply(body(load_fixture, "unusual_trades.json")))
    spxw = "/v3/reference/options/contracts/O:SPXW261218C06000000"
    serve_indices(fake_rest, "SPX")
    fake_rest.reply(wire(spxw), contract_reference("OCEICS"))
    start = datetime(2025, 10, 29, 15, tzinfo=UTC)
    out = rows(
        rest_session.options.unusual_trades(
            "O:SPY260123C00687000", start=start, end=datetime(2025, 10, 29, 16, tzinfo=UTC)
        ).fetch(wait=True)
    )
    [(sent_path, sent), *references] = fake_rest.get_queries(one_value=True)
    assert sent_path == path
    # The index list once; then only SPXW, whose SPX is a listed index, is asked about, as of
    # its trade's New York trading day.
    assert references == [
        (INDEX_LIST, {"market": "indices", "active": "true", "limit": "1000"}),
        (INDEX_LIST, {"market": "indices", "active": "false", "limit": "1000"}),
        (spxw, {"as_of": "2025-10-29"}),
    ]
    assert sent["ticker.any_of"] == "O:SPY260123C00687000"
    assert {"sip_timestamp.gte", "sip_timestamp.lt", "limit"} <= sent.keys()
    picked = ("Symbol", "Date", "Timestamp", "OptionRoot", "Price", "Size", "Flags")
    assert [tuple(r[c] for c in picked) for r in out] == [
        (
            "O:SPY260123C00687000",
            "2025-10-29",
            1761751432123456800,
            "SPY",
            15.25,
            120,
            ["block_premium"],
        ),
        (
            "O:SPXW261218C06000000",
            "2025-10-29",
            1761751431987654400,
            "SPXW",
            42.1,
            25,
            ["block_premium", "iso"],
        ),
    ]
    # SPXW's contracts are options on an index, so their underlying is `I:SPX`.
    assert [(r["Underlying"], r["Conditions"], r["Exchange"]) for r in out] == [
        ("SPY", [227], 316),
        ("I:SPX", [219], 304),
    ]


def test_one_bare_underlying_is_a_stock_or_an_index_by_each_root(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A stock and an index can share a bare ticker; each root's own contracts say which.
    stock, index = "O:DJT261218C00030000", "O:DJTW261218C00030000"
    page = [
        {"ticker": stock, "root": "DJT", "date": "2026-11-13", "underlying_ticker": "DJT"},
        {"ticker": index, "root": "DJTW", "date": "2026-11-13", "underlying_ticker": "DJT"},
    ]
    fake_rest.reply("/options/v1/open-interest", Reply({"status": "OK", "results": page}))
    serve_indices(fake_rest, "DJT")
    fake_rest.reply(wire(f"/v3/reference/options/contracts/{stock}"), contract_reference("OCASPS"))
    fake_rest.reply(wire(f"/v3/reference/options/contracts/{index}"), contract_reference("OCEICS"))
    with caplog.at_level(logging.WARNING, "deephaven_massive._ingest.option_roots"):
        out = rows(rest_session.options.open_interest().fetch(wait=True))
    assert [(r["OptionRoot"], r["Underlying"]) for r in out] == [("DJT", "DJT"), ("DJTW", "I:DJT")]
    assert caplog.records == []  # each root is classified, so none warns


UNCLASSIFIED = ["O:SPXW261218C06000000", "O:SPXW261218P06000000"]
"""Two contracts of a root whose underlying, SPX, is a listed index."""
AAPL_CALL = "O:AAPL261218C00200000"
"""A stock's contract, whose root needs no lookup."""


def serve_unclassified(server: FakeRestServer) -> None:
    # A page of SPXW's two contracts and a stock's, whose reference gives SPXW no CFI code.
    page = [
        {"ticker": t, "root": "SPXW", "date": "2026-11-13", "underlying_ticker": "SPX"}
        for t in UNCLASSIFIED
    ]
    page.append(
        {"ticker": AAPL_CALL, "root": "AAPL", "date": "2026-11-13", "underlying_ticker": "AAPL"}
    )
    server.reply("/options/v1/open-interest", Reply({"status": "OK", "results": page}))
    serve_indices(server, "SPX")
    server.reply(
        wire(f"/v3/reference/options/contracts/{UNCLASSIFIED[0]}"),
        Reply({"status": "OK", "results": {}}),
    )


def test_the_rows_of_a_root_the_reference_cannot_classify_are_withheld(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    serve_unclassified(fake_rest)
    events: list[ProblemEvent] = []
    monkeypatch.setattr(rest_session._core, "problems", events.extend)
    out = rows(rest_session.options.open_interest().fetch(wait=True))
    # SPX may be an index's ticker without its I:, so SPXW's rows are left out and listed.
    assert [(r["Symbol"], r["Underlying"]) for r in out] == [(AAPL_CALL, "AAPL")]
    [event] = events
    assert (event.cause, event.action, event.symbol, event.field, event.rows) == (
        ProblemCause.UNREADABLE_SYMBOLS,
        ProblemAction.WITHHELD,
        "SPX",
        "Underlying",
        2,
    )
    assert event.stream_id is not None
    assert event.policy is dm.OnError.LOG
    assert event.detail.startswith(
        "Massive's Contract Overview gives no CFI code for option root SPXW (asked about "
        f"{UNCLASSIFIED[0]} as of 2026-11-13)"
    )


def test_a_read_of_an_unclassified_root_alone_fails_whatever_the_setting(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    page = [
        {"ticker": t, "root": "SPXW", "date": "2026-11-13", "underlying_ticker": "SPX"}
        for t in UNCLASSIFIED
    ]
    fake_rest.reply("/options/v1/open-interest", Reply({"status": "OK", "results": page}))
    serve_indices(fake_rest, "SPX")
    fake_rest.reply(
        wire(f"/v3/reference/options/contracts/{UNCLASSIFIED[0]}"),
        Reply({"status": "OK", "results": {}}),
    )
    # Every row the read would return is withheld, so it fails under the default LOG too.
    with pytest.raises(RowDataError, match=r"none of the 2 rows.*option root SPXW"):
        rest_session.options.open_interest(root="SPXW").fetch(wait=True)


def test_the_rows_of_an_unclassified_root_fail_the_read_under_raise(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    serve_unclassified(fake_rest)
    strict = dm.ErrorPolicy(unreadable_symbols=dm.OnError.RAISE)
    with pytest.raises(RowDataError, match=r"2 under unreadable_symbols.*option root SPXW"):
        rest_session.options.open_interest(on_error=strict).fetch(wait=True)


def test_a_poll_of_unusual_trades_names_an_index_underlying_from_its_first_cycle(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    trade = {**LIST_ROW, "ticker": "O:SPXW261218C06000000", "root": "SPXW"}
    fake_rest.reply(
        "/options/v1/unusual-trades",
        Reply({"status": "OK", "results": [{**trade, "underlying_ticker": "SPX"}]}),
    )
    reference = "/v3/reference/options/contracts/O:SPXW261218C06000000"
    serve_indices(fake_rest, "SPX")
    fake_rest.reply(wire(reference), contract_reference("OCEICS"))
    table = rest_session.options.unusual_trades(limit=10).poll(60)
    pace = threading.Event()  # never set; only paces the bounded wait for the first cycle
    found: list[dict[str, Any]] = []
    for _ in range(500):
        found = rows(table)
        if found:
            break
        pace.wait(0.02)
    rest_session.release(table)
    assert [(r["Symbol"], r["Underlying"]) for r in found] == [("O:SPXW261218C06000000", "I:SPX")]
    # The poll asked about the root once, as of the trade's New York day.
    assert [q for q in fake_rest.get_queries(one_value=True) if q[0] == reference] == [
        (reference, {"as_of": "2025-10-29"})
    ]


@pytest.mark.parametrize("refused", ["index list", "contract"])
def test_a_refused_root_lookup_fails_the_read_with_massive_s_refusal(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, refused: str
) -> None:
    fake_rest.reply(
        "/options/v1/open-interest",
        Reply({"status": "OK", "results": [{**LIST_ROW, "underlying_ticker": "SPY"}]}),
    )
    refusal = Reply(
        {"status": "NOT_AUTHORIZED", "message": "You are not entitled to this data."}, status=403
    )
    if refused == "index list":
        fake_rest.reply(INDEX_LIST, refusal)
    else:
        serve_indices(fake_rest, "SPY")  # as if SPY named an index, so its root is asked about
        fake_rest.reply(wire(f"/v3/reference/options/contracts/{LIST_ROW['ticker']}"), refusal)
    with pytest.raises(
        dm.EntitlementError,
        match=r"^The library cannot tell whether the option root 'SPY' is an index, since reading ",
    ):
        rest_session.options.open_interest().fetch(wait=True)


def list_query(m: dm.MassiveSession, spec: DatasetSpec) -> FetchQuery:
    # The query of one of the three options list datasets, with every argument left out.
    if spec is options.OPEN_INTEREST:
        return m.options.open_interest()
    if spec is options.GREEKS:
        return m.options.greeks()
    return m.options.unusual_trades()


LIST_ROW = {
    "ticker": "O:SPY260123C00687000",
    "root": "SPY",
    "date": "2026-01-22",
    "sip_timestamp": 1761751432123456800,
    "sequence_number": 1,
}
"""A row each of the three list datasets reads whole: every one's keys, and the trades' time."""

BAD_LIST_ROWS = [
    (RowFault.UNREADABLE_SYMBOLS, {**LIST_ROW, "ticker": "SPY 260123C00687000"}),
    (RowFault.UNCONVERTIBLE_VALUES, {**LIST_ROW, "strike_price": "x", "price": "x"}),
]
"""A row of each kind every list dataset withholds, with its cause."""


@pytest.mark.parametrize("spec", LIST_SPECS)
@pytest.mark.parametrize(("cause", "bad"), BAD_LIST_ROWS)
@pytest.mark.parametrize("policy", [dm.OnError.LOG, dm.OnError.IGNORE])
def test_a_list_dataset_withholds_a_row_it_cannot_read_whole(
    *,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    spec: DatasetSpec,
    cause: RowFault,
    bad: dict[str, object],
    policy: dm.OnError,
) -> None:
    fake_rest.reply(spec.rest_paths[0], Reply({"status": "OK", "results": [bad, LIST_ROW]}))
    config = dm.MassiveConfig(
        api_key="test-key",
        rest_base_url=fake_rest.url,
        on_error=dm.ErrorPolicy(**{cause.value: policy}),
    )
    with dm.MassiveSession(config) as m:
        events: list[ProblemEvent] = []
        monkeypatch.setattr(m._core, "problems", events.extend)
        out = rows(list_query(m, spec).fetch(wait=True))
    # No trace of the bad row; the whole one beside it lands, and the call has its problem.
    assert [(r["Symbol"], r["OptionRoot"]) for r in out] == [("O:SPY260123C00687000", "SPY")]
    # The rows carry both datasets' keys, so each dataset also lists the other's as drift.
    withheld = [e for e in events if e.cause is not ProblemCause.SCHEMA_DRIFT]
    assert [(e.cause, e.rows, e.stream_id is not None) for e in withheld] == [
        (ProblemCause(cause.value), 1, True)
    ]


@pytest.mark.parametrize("spec", LIST_SPECS)
@pytest.mark.parametrize(("cause", "bad"), BAD_LIST_ROWS)
def test_a_list_dataset_fails_on_a_row_it_cannot_read_whole_under_raise(
    *, fake_rest: FakeRestServer, spec: DatasetSpec, cause: RowFault, bad: dict[str, object]
) -> None:
    fake_rest.reply(spec.rest_paths[0], Reply({"status": "OK", "results": [bad, LIST_ROW]}))
    config = dm.MassiveConfig(
        api_key="test-key",
        rest_base_url=fake_rest.url,
        on_error=dm.ErrorPolicy(**{cause.value: dm.OnError.RAISE}),
    )
    with dm.MassiveSession(config) as m, pytest.raises(RowDataError, match=f"1 under {cause}"):
        list_query(m, spec).fetch(wait=True)


def test_an_unusual_trade_without_its_sip_time_is_withheld(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    timeless = {k: v for k, v in LIST_ROW.items() if k != "sip_timestamp"}
    fake_rest.reply(
        "/options/v1/unusual-trades", Reply({"status": "OK", "results": [timeless, LIST_ROW]})
    )
    out = rows(rest_session.options.unusual_trades().fetch(wait=True))
    assert [r["Timestamp"] for r in out] == [1761751432123456800]


@pytest.mark.parametrize("spec", LIST_SPECS)
def test_a_row_without_massive_s_root_has_a_null_option_root(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, spec: DatasetSpec
) -> None:
    # Massive's `root` is optional: its absence is null, never the root read from `Symbol`.
    rootless = {k: v for k, v in LIST_ROW.items() if k != "root"}
    fake_rest.reply(spec.rest_paths[0], Reply({"status": "OK", "results": [rootless]}))
    [out] = rows(list_query(rest_session, spec).fetch(wait=True))
    assert (out["Symbol"], out["OptionRoot"]) == ("O:SPY260123C00687000", None)


def test_unusual_trades_poll_as_a_feed(rest_session: dm.MassiveSession) -> None:
    # Without a start or a limit, each poll would read Massive's whole archive.
    with pytest.raises(ValueError, match=r"poll\(\) needs a start or a limit"):
        rest_session.options.unusual_trades().poll(60)
    assert options.UNUSUAL_TRADES.is_feed
    assert options.UNUSUAL_TRADES.poll_keys == ("Symbol", "Timestamp", "SequenceNumber")


def test_the_list_datasets_take_only_option_contracts_and_refuse_all(
    massive_session: dm.MassiveSession,
) -> None:
    m = massive_session
    for factory in (m.options.open_interest, m.options.greeks, m.options.unusual_trades):
        assert factory()._request.symbols is None
        every = (
            f"{factory()._spec.name} does not take dm.ALL. Leave symbols out to read every "
            "contract."
        )
        with pytest.raises(ValueError, match=whole(every)):
            factory(dm.ALL)
        with pytest.raises(ValueError, match="is an underlying, not an OCC option symbol"):
            factory("AAPL")
    with pytest.raises(
        ValueError,
        match=whole("root=[] is empty, so it would select nothing. Leave root out for every root."),
    ):
        m.options.open_interest(root=[])
    with pytest.raises(TypeError, match="root must be a str, a sequence of str, or None"):
        m.options.open_interest(root=5)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(
        ValueError,
        match=whole(
            "options.open_interest: date is a calendar date, so start and end take dates, not "
            "times. Pass dates, such as start='2025-01-02'."
        ),
    ):
        m.options.open_interest(start=datetime(2026, 1, 20, tzinfo=UTC))
    with pytest.raises(
        ValueError,
        match=whole("The filter 'root_any_of' has its own argument. Pass root= instead."),
    ):
        m.options.open_interest(filters={"root_any_of": "SPY"})


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
            # The end is the frame's `e` as sent, though it is not one second on.
            {
                "TimestampStart": 1632419640000 * 10**6,
                "TimestampEnd": 1632419700000 * 10**6,
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
    assert type(query) is NoSymbolStreamHistoryQuery
    assert query._request.symbols is None
    assert repr(query) == "m.options.admin_messages()"
    with pytest.raises(TypeError):
        massive_session.options.admin_messages(dm.ALL)  # type: ignore[call-arg, arg-type]  # the refusal
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
    # Both odd rows are withheld; the good one lands.
    got = rows_when(kept, lambda arrived: len(arrived) == 1)
    assert [(r["SequenceNumber"], r["Exchange"]) for r in got] == [(3, 302)]
    (row,) = shown_streams(ws_session, dh_to_pylist)
    assert row["State"] == "RUNNING"
    ws_session.session.release(kept)


def test_a_refused_admin_subscription_fails_its_table_and_links_its_page(
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
    assert refused["Error"] == (
        "options.admin_messages: Massive answered the subscription to AD.* on "
        'wss://socket.test/options: "not authorized '
        'to access AD.*". options.admin_messages streams only in real time. Its plans are listed '
        "at https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/"
        "options.admin_messages.md. Massive documents the options AD channel only on "
        "socket.massive.com. To stream from another host, pass "
        'websocket_hosts={dm.AssetClass.OPTIONS: "<host>"} to dm.MassiveConfig.'
    )
    assert running["State"] == "RUNNING"
    ws_session.session.release(notices)
    ws_session.session.release(trades)


def test_admin_messages_on_the_delayed_feed_fail_on_massives_refusal(
    make_ws_session: Callable[..., WsSession],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Massive documents admin messages only on the real-time feed, but whether it serves them
    # on the delayed one is its answer: the stream subscribes there, and a refusal fails it.
    delayed = HostDecision("delayed.test", Recency.DELAYED, "MassiveConfig.delayed_subscriptions")
    session = make_ws_session(
        decide=lambda *_: delayed, delayed_subscriptions={dm.Subscription.OPTIONS}
    )
    session.server.refuse["AD.*"] = "not authorized to access AD.*"
    with caplog.at_level(logging.INFO, logger="deephaven_massive._ingest.ws"):
        notices = session.session.options.admin_messages().blink()
    assert any(
        "options.admin_messages stream" in r.getMessage()
        and "Massive documents it only on socket.massive.com; subscribing on delayed.test"
        in r.getMessage()
        for r in caplog.records
    )
    session.wait(lambda s: any(status.kind is _massive.StatusKind.REFUSED for status in s.statuses))
    session.flush()
    assert session.urls == ["wss://delayed.test/options"]
    (row,) = shown_streams(session, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert row["Error"] == (
        "options.admin_messages: Massive answered the subscription to AD.* on "
        'wss://delayed.test/options: "not '
        'authorized to access AD.*". options.admin_messages streams only in real time. Its '
        "plans are listed at https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/"
        "options.admin_messages.md. Massive documents the options AD channel only on "
        "socket.massive.com. delayed_subscriptions lists dm.Subscription.OPTIONS, so this table "
        "streams from Massive's delayed feed. To stream from the real-time feed, remove "
        "dm.Subscription.OPTIONS from delayed_subscriptions."
    )
    session.session.release(notices)


def test_admin_messages_on_delayed_options_data_raise_the_delayed_data_error(
    make_ws_session: Callable[..., WsSession],
) -> None:
    # The ordinary error of delayed data, as every options stream gets: the library reads no
    # plan into Massive's label.
    def labelled_delayed(*_: object) -> HostDecision:
        raise delayed_refusal(
            dm.Subscription.OPTIONS,
            "options streams",
            "last_trade.timeframe=DELAYED",
            listed=set(),
            streams=True,
        )

    session = make_ws_session(decide=labelled_delayed)
    with pytest.raises(dm.EntitlementError) as info:
        session.session.options.admin_messages().append_only()
    assert str(info.value).startswith(
        "options streams would be delayed, since Massive reports last_trade.timeframe=DELAYED,"
    )
    assert "plan" not in str(info.value)
    assert session.urls == []


ADMIN_ARCHIVE = "/options/v1/admin-messages"
ADMIN_DAY = date(2026, 11, 5)  # the day of Massive's sample messages


def test_admin_message_history_reads_every_exchange_s_archive_by_time(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply(ADMIN_ARCHIVE, Reply(body(load_fixture, "admin_messages.json")))
    query = rest_session.options.admin_messages()
    out = rows(query.history(ADMIN_DAY, ADMIN_DAY, wait=True))
    # One request, by time alone: no ticker and no exchange, ascending, at the largest page.
    assert fake_rest.get_queries(one_value=True) == [
        (
            ADMIN_ARCHIVE,
            {
                "timestamp.gte": str(ny_midnight_ns(ADMIN_DAY)),
                "timestamp.lt": str(ny_midnight_ns(date(2026, 11, 6))),
                "sort": "timestamp.asc",
                "limit": "50000",
            },
        )
    ]
    picked = ("Date", "Timestamp", "MessageId", "Exchange", "Message", "Retransmission")
    assert [tuple(r[c] for c in picked) for r in out] == [
        (
            "2026-11-05",
            1793911442930568200,
            "1793911442930568079-4043826282",
            302,
            "FLEX AAPL 20270115 C 00200000 PRINT 150 @ 12.35",
            None,  # Massive omits the flag on an original message
        ),
        (
            "2026-11-05",
            1793911460115302100,
            "1793911460115302244-4043826283",
            300,
            "OPRA SYSTEM NORMAL",
            True,
        ),
    ]
    # The archive and the stream give one table shape.
    live_columns = CATALOG.dataset_schema(options.ADMIN_MESSAGES).column_names
    assert tuple(
        dh_arrow.to_arrow(query.history(ADMIN_DAY, ADMIN_DAY, wait=True)).column_names
    ) == (live_columns)


def test_an_empty_admin_message_archive_is_no_rows(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(ADMIN_ARCHIVE, Reply({"status": "OK", "results": []}))
    out = rest_session.options.admin_messages().history(ADMIN_DAY, ADMIN_DAY, wait=True)
    assert out.size == 0


def test_admin_message_history_has_no_flat_files(rest_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match="flat files"):
        rest_session.options.admin_messages().history(
            ADMIN_DAY, ADMIN_DAY, source=HistorySource.FLAT_FILES
        )


def unreadable_admin_messages(load_fixture: Callable[[str, str], bytes]) -> Reply:
    # Massive's first sample message without its required SIP time, beside the second, whole.
    sample = json.loads(load_fixture(OWNER, "admin_messages.json"))["results"]
    page = [{k: v for k, v in sample[0].items() if k != "timestamp"}, sample[1]]
    return Reply({"status": "OK", "results": page})


def test_an_unreadable_admin_message_in_the_archive_is_withheld(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[ProblemEvent] = []
    monkeypatch.setattr(rest_session._core, "problems", events.extend)
    fake_rest.reply(ADMIN_ARCHIVE, unreadable_admin_messages(load_fixture))
    out = rows(rest_session.options.admin_messages().history(ADMIN_DAY, ADMIN_DAY, wait=True))
    # No trace of the unreadable message; the whole one beside it lands.
    assert [r["MessageId"] for r in out] == ["1793911460115302244-4043826283"]
    assert [(e.cause, e.rows, e.stream_id is not None) for e in events] == [
        (ProblemCause.UNREADABLE_KEYS, 1, True)
    ]


def test_an_unreadable_admin_message_under_raise_fails_the_history(
    fake_rest: FakeRestServer, load_fixture: Callable[[str, str], bytes]
) -> None:
    fake_rest.reply(ADMIN_ARCHIVE, unreadable_admin_messages(load_fixture))
    config = dm.MassiveConfig(
        api_key="test-key",
        rest_base_url=fake_rest.url,
        on_error=dm.ErrorPolicy(unreadable_keys=dm.OnError.RAISE),
    )
    with (
        dm.MassiveSession(config) as m,
        pytest.raises(RowDataError, match="1 under unreadable_keys"),
    ):
        m.options.admin_messages().history(ADMIN_DAY, ADMIN_DAY, wait=True)


def test_an_archive_read_with_no_readable_message_fails_as_a_format_change(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # Under the default LOG, a read with rows but none whole fails rather than read empty.
    page = [{"id": "1-1", "exchange": 300, "message": "OPRA SYSTEM NORMAL", "sequence_number": 1}]
    fake_rest.reply(ADMIN_ARCHIVE, Reply({"status": "OK", "results": page}))
    with pytest.raises(RowDataError, match="could not read the 1 row"):
        rest_session.options.admin_messages().history(ADMIN_DAY, ADMIN_DAY, wait=True)


def test_a_refused_admin_message_archive_raises_massive_s_refusal(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    refusal = {"status": "NOT_AUTHORIZED", "message": "You are not entitled to this data."}
    fake_rest.reply(ADMIN_ARCHIVE, Reply(refusal, status=403))
    with pytest.raises(dm.EntitlementError, match="You are not entitled to this data"):
        rest_session.options.admin_messages().history(ADMIN_DAY, ADMIN_DAY, wait=True)


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
    config = dm.MassiveConfig(
        api_key="test-key",
        s3_access_key_id="AKIDFLATFILES",
        s3_secret_access_key="flat-file-secret",  # noqa: S106  # fake credential
    )
    m = dm.MassiveSession(config)
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
