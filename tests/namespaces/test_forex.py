"""Tests for the `m.forex` namespace: its dataset specs, schemas, factories, and fixtures.

The REST fixtures under `tests/data/namespaces/forex/` are the vendor's documented sample
responses; each is decoded against its generated path to prove it has the generated shape, and
served by the fake REST server to check each dataset's exact request and rows. Every streamed
channel lands one frame from the fake WebSocket server in `blink()`, and every flat-file prefix
round-trips one file through the fake S3.
"""

from __future__ import annotations

import itertools
import json
import string
import threading
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs, unquote, urlsplit

import numpy as np
import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import new_table
from deephaven.column import string_col
from deephaven.stream import blink_to_append_only
from deephaven.table import Table
from deephaven.time import to_j_instant

import deephaven_massive as dm
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import AssetClass, ChannelFamily
from deephaven_massive._massive import RestPath, Transport, source, to_batch
from deephaven_massive._namespaces import forex
from deephaven_massive._namespaces.query_building import Namespace
from deephaven_massive._query import (
    HistoryQuery,
    Query,
    QueryBase,
    StreamHistoryQuery,
    StreamQuery,
    query_class,
)
from deephaven_massive._session import CATALOG
from deephaven_massive._tables import poll
from tests._fakes.clock import FakeClock
from tests._fakes.rest import FakeRestServer, Reply

if TYPE_CHECKING:
    from tests._fakes.s3 import FakeS3
    from tests._fakes.ws_session import WsSession

R, W, F = Transport.REST, Transport.WS, Transport.FLAT_FILES
NS = 10**9
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MS = 10**6
DOCS = "https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/forex.md#"

RANGE = "/v2/aggs/ticker/{forexTicker}/range/{multiplier}/{timespan}/{from}/{to}"

# Every source the namespace reads, as the generated files list them.
FOREX_SOURCES = {
    (R, "/v1/conversion/{from}/{to}"),
    (R, "/v1/last_quote/currencies/{from}/{to}"),
    (R, "/v2/aggs/grouped/locale/global/market/fx/{date}"),
    (R, "/v2/aggs/ticker/{forexTicker}/prev"),
    (R, RANGE),
    (R, "/v2/snapshot/locale/global/markets/forex/tickers"),
    (R, "/v2/snapshot/locale/global/markets/forex/{direction}"),
    (R, "/v3/quotes/{fxTicker}"),
    (W, "forex/C"),
    (W, "forex/CA"),
    (W, "forex/CAS"),
    (W, "forex/FMV"),
    (F, "global_forex/day_aggs_v1"),
    (F, "global_forex/minute_aggs_v1"),
    (F, "global_forex/quotes_v1"),
}


def request(query: QueryBase) -> Request:
    return query._request


def test_namespace_is_reached_from_the_session(massive_session: dm.MassiveSession) -> None:
    namespace = massive_session.forex
    assert type(namespace) is forex._Forex
    assert isinstance(namespace, Namespace)


def test_the_namespace_declares_its_eight_datasets() -> None:
    names = [spec.name for spec in forex.DATASETS]
    assert names == [
        "forex.quotes",
        "forex.last_quote",
        "forex.bars",
        "forex.previous_day_bar",
        "forex.fmv",
        "forex.snapshots",
        "forex.movers",
        "forex.conversion",
    ]
    assert all(CATALOG.by_name[spec.name] is spec for spec in forex.DATASETS)


def test_the_datasets_read_every_forex_source() -> None:
    read = {s for spec in forex.DATASETS for s in spec.sources}
    assert read == FOREX_SOURCES


def test_the_namespace_passes_every_catalog_gate() -> None:
    assert Catalog(forex.DATASETS).problems() == ()
    assert [p for p in CATALOG.problems() if "forex." in p] == []


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (forex.QUOTES, StreamHistoryQuery),
        (forex.LAST_QUOTE, Query),
        (forex.BARS, HistoryQuery),
        (forex.PREVIOUS_DAY_BAR, Query),
        (forex.FMV, StreamQuery),
        (forex.SNAPSHOTS, Query),
        (forex.MOVERS, Query),
        (forex.CONVERSION, Query),
    ],
)
def test_each_dataset_has_its_query_class(spec: DatasetSpec, expected: type) -> None:
    assert query_class(spec) is expected


def _schema(spec: DatasetSpec) -> dict[str, ColumnType]:
    """Return a dataset's columns and types as merged over the forex datasets alone.

    The process catalog also merges the members other namespaces declare into each kind, so
    exact column lists are checked against the namespace's own catalog.
    """
    return {
        c.name: c.type
        for c in Catalog(forex.DATASETS).dataset_schema(spec, raw_symbol=False).columns
    }


def test_quotes_join_the_last_quote_through_its_projection() -> None:
    columns = _schema(forex.QUOTES)
    # The last quote is a dataset of its own in the same kind: the kind's columns its endpoint
    # fills, in the same order, which leaves out the stream's exchange ids.
    last = _schema(forex.LAST_QUOTE)
    assert {name: columns[name] for name in columns if name in last} == last
    assert list(last) == [name for name in columns if name in last]
    assert set(columns) - set(last) == {"AskExchange", "BidExchange"}
    assert list(columns) == [
        "Symbol",
        "Date",
        "Timestamp",
        "AskExchange",
        "AskPrice",
        "BidExchange",
        "BidPrice",
        "Exchange",
        "TimestampInsert",
    ]
    assert columns["Timestamp"] is ColumnType.INSTANT
    assert columns["Date"] is ColumnType.STRING
    assert columns["AskPrice"] is ColumnType.DOUBLE
    # The last quote's `ask` and `bid` are the stream's prices, not columns of their own.
    assert {"Ask", "Bid", "Ticker", "ParticipantTimestamp"}.isdisjoint(columns)


def test_bars_rename_the_pair_and_window_start() -> None:
    columns = _schema(forex.BARS)
    assert _schema(forex.PREVIOUS_DAY_BAR) == columns
    assert list(columns)[:4] == ["Symbol", "Date", "TimestampStart", "TimestampEnd"]
    assert list(columns)[-1] == "TimestampInsert"
    assert {"Open", "High", "Low", "Close", "Volume", "Vwap", "Transactions"} <= set(columns)
    # The stream's window end `e` becomes `TimestampEnd`, which the library recomputes.
    assert {"Pair", "Ticker", "WindowStart", "EndTimestamp", "Timestamp"}.isdisjoint(columns)
    assert columns["Volume"] is ColumnType.DOUBLE  # the stream's long joins REST's double


def test_fmv_holds_the_fair_value_per_pair() -> None:
    assert _schema(forex.FMV) == {
        "Symbol": ColumnType.STRING,
        "Date": ColumnType.STRING,
        "Timestamp": ColumnType.INSTANT,
        "Fmv": ColumnType.DOUBLE,
        "TimestampInsert": ColumnType.INSTANT,
    }


@pytest.mark.parametrize("spec", [forex.SNAPSHOTS, forex.MOVERS])
def test_snapshot_rows_lead_with_their_symbol(spec: DatasetSpec) -> None:
    columns = _schema(spec)
    assert next(iter(columns)) == "Symbol"
    assert "Ticker" not in columns
    assert columns["LastQuoteAskPrice"] is ColumnType.DOUBLE
    assert columns["Updated"] is ColumnType.INSTANT
    assert "Timestamp" not in columns  # a snapshot row keeps its own time columns


def test_conversion_keeps_the_vendor_symbol() -> None:
    columns = _schema(forex.CONVERSION)
    assert next(iter(columns)) == "Symbol"
    assert {"Converted", "InitialAmount", "LastAsk", "LastBid", "LastTimestamp"} <= set(columns)
    assert columns["LastTimestamp"] is ColumnType.INSTANT


# --- factories ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "symbols", ["EUR/USD", "eur-usd", "EURUSD", "eurusd", "c:eurusd", ["EUR/USD", "C:EURUSD"]]
)
def test_only_the_canonical_ticker_is_taken(
    massive_session: dm.MassiveSession, symbols: str | list[str]
) -> None:
    factories = (
        massive_session.forex.quotes,
        massive_session.forex.fmv,
        massive_session.forex.bars,
        massive_session.forex.snapshots,
    )
    for factory in factories:
        assert request(factory("C:EURUSD")).symbols == ("C:EURUSD",)
        with pytest.raises(ValueError, match=r"use 'C:EURUSD' .*dm\.canonical_symbol_forex"):
            factory(symbols)


def test_quotes(massive_session: dm.MassiveSession) -> None:
    query = massive_session.forex.quotes(["C:GBPUSD", "C:EURUSD"])
    assert type(query) is StreamHistoryQuery
    assert request(query) == Request(symbols=("C:EURUSD", "C:GBPUSD"))
    assert request(massive_session.forex.quotes(dm.ALL)).symbols is dm.ALL


def test_the_one_row_datasets(massive_session: dm.MassiveSession) -> None:
    last = massive_session.forex.last_quote(["C:GBPUSD", "C:EURUSD"])
    assert type(last) is Query
    assert request(last) == Request(symbols=("C:EURUSD", "C:GBPUSD"))
    prev = massive_session.forex.previous_day_bar("C:EURUSD")
    assert type(prev) is Query
    # The bar's request carries its timespan, so its rows get the 1-day day rules.
    assert request(prev) == Request(symbols=("C:EURUSD",), timespan=dm.Timespan.DAY)


def test_a_symbol_table_is_a_watchlist(massive_session: dm.MassiveSession) -> None:
    table = new_table([string_col("Symbol", ["C:EURUSD"])])
    symbols = request(massive_session.forex.quotes(table)).symbols
    assert isinstance(symbols, SymbolTable)
    assert symbols.table is table


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ((dm.Timespan.SECOND, 1), StreamHistoryQuery),
        ((dm.Timespan.MINUTE, 1), StreamHistoryQuery),
        ((dm.Timespan.DAY, 1), HistoryQuery),
        ((dm.Timespan.MINUTE, 5), HistoryQuery),
        ((dm.Timespan.HOUR, 1), HistoryQuery),
        ((dm.Timespan.DAY, 2), HistoryQuery),
    ],
)
def test_bars_pick_their_class_from_the_timespan(
    massive_session: dm.MassiveSession, case: tuple[dm.Timespan, int], expected: type
) -> None:
    timespan, multiplier = case
    query = massive_session.forex.bars("C:EURUSD", timespan, multiplier=multiplier)
    assert type(query) is expected
    assert request(query) == Request(
        symbols=("C:EURUSD",), timespan=timespan, multiplier=multiplier
    )


def test_bars_default_to_one_minute_and_take_no_adjusted(
    massive_session: dm.MassiveSession,
) -> None:
    query = massive_session.forex.bars(dm.ALL)
    assert type(query) is StreamHistoryQuery
    # Currency pairs have no corporate actions: no `adjusted` is stored, so none is ever sent.
    assert request(query) == Request(symbols=dm.ALL, timespan=dm.Timespan.MINUTE)
    with pytest.raises(TypeError, match="adjusted"):
        massive_session.forex.bars(dm.ALL, adjusted=False)  # type: ignore[call-overload]  # no such argument


def test_bars_reject_bad_arguments(massive_session: dm.MassiveSession) -> None:
    bars = massive_session.forex.bars
    with pytest.raises(ValueError, match="SESSION exists only for futures"):
        bars("C:EURUSD", dm.Timespan.SESSION)
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan"):
        bars("C:EURUSD", "minute")  # type: ignore[call-overload]  # a plain string is refused
    with pytest.raises(ValueError, match="multiplier"):
        bars("C:EURUSD", dm.Timespan.HOUR, multiplier=0)


def test_fmv(massive_session: dm.MassiveSession) -> None:
    query = massive_session.forex.fmv(dm.ALL)
    assert type(query) is StreamQuery
    assert request(query) == Request(symbols=dm.ALL)


def test_snapshots(massive_session: dm.MassiveSession) -> None:
    query = massive_session.forex.snapshots(dm.ALL)
    assert type(query) is Query
    assert request(query) == Request(symbols=dm.ALL)


def test_movers(massive_session: dm.MassiveSession) -> None:
    query = massive_session.forex.movers(dm.Direction.LOSERS)
    assert type(query) is Query
    assert request(query) == Request(params=(("direction", dm.Direction.LOSERS),))
    with pytest.raises(TypeError, match=r"direction must be dm\.Direction"):
        massive_session.forex.movers("gainers")  # type: ignore[arg-type]  # a str is refused


def test_conversion(massive_session: dm.MassiveSession) -> None:
    query = massive_session.forex.conversion(" aud", "Usd ", amount=100, precision=4)
    assert type(query) is Query
    assert request(query) == Request(
        params=(("amount", 100), ("from", "AUD"), ("precision", "4"), ("to", "USD"))
    )
    default = request(massive_session.forex.conversion("EUR", "USD"))
    assert dict(default.params) == {"amount": 1.0, "from": "EUR", "precision": "2", "to": "USD"}


def test_a_large_integer_amount_keeps_every_digit(massive_session: dm.MassiveSession) -> None:
    amount = 10**17 + 1
    query = massive_session.forex.conversion("EUR", "USD", amount=amount)
    assert dict(request(query).params)["amount"] == amount


@pytest.mark.parametrize(
    ("kwargs", "failure"),
    [
        ({"from_currency": "EURO"}, (ValueError, "from_currency='EURO' is not a three-letter")),
        ({"from_currency": "EU1"}, (ValueError, "from_currency='EU1' is not a three-letter")),
        ({"to_currency": "U$D"}, (ValueError, r"to_currency='U\$D' is not a three-letter")),
        ({"from_currency": 840}, (TypeError, "from_currency must be a str, not int")),
        ({"amount": 0}, (ValueError, "amount must be positive and finite, got 0")),
        ({"amount": -1.5}, (ValueError, "amount must be positive and finite, got -1.5")),
        ({"amount": float("inf")}, (ValueError, "amount must be positive and finite, got inf")),
        ({"amount": float("nan")}, (ValueError, "amount must be positive and finite, got nan")),
        ({"amount": "1"}, (TypeError, "amount must be an int or float, not str")),
        ({"amount": True}, (TypeError, "amount must be an int or float, not bool")),
        ({"precision": 5}, (ValueError, r"precision must be one of \[0, 1, 2, 3, 4\], got 5")),
        ({"precision": -1}, (ValueError, "precision must be one of")),
        ({"precision": 2.0}, (TypeError, "precision must be an int, not float")),
        ({"precision": True}, (TypeError, "precision must be an int, not bool")),
    ],
)
def test_conversion_rejects_bad_arguments(
    massive_session: dm.MassiveSession,
    kwargs: dict[str, object],
    failure: tuple[type[Exception], str],
) -> None:
    error, message = failure
    arguments: dict[str, object] = {"from_currency": "EUR", "to_currency": "USD", **kwargs}
    with pytest.raises(error, match=message):
        massive_session.forex.conversion(**arguments)  # type: ignore[arg-type]  # bad on purpose


@pytest.mark.parametrize(
    ("symbols", "message"),
    [
        ("EUR/US", "cannot read 'EUR/US' as a forex pair"),
        ("EUR/USD/JPY", "cannot read 'EUR/USD/JPY' as a forex pair"),
        ("X:BTCUSD", r"'X:BTCUSD' is a crypto ticker; this dataset takes forex tickers"),
        ("EU1/USD", "cannot read 'EU1/USD' as a forex pair"),
        ([], "no symbols"),
    ],
)
def test_invalid_pairs_fail_at_construction(
    massive_session: dm.MassiveSession, symbols: str | list[str], message: str
) -> None:
    forex_ns = massive_session.forex
    factories: list[Callable[[str | list[str]], QueryBase]] = [
        forex_ns.quotes,
        forex_ns.bars,
        forex_ns.fmv,
        forex_ns.snapshots,
    ]
    for factory in factories:
        with pytest.raises(ValueError, match=message):
            factory(symbols)


def test_an_exhausted_generator_has_no_symbols(massive_session: dm.MassiveSession) -> None:
    pairs = (pair for pair in ["C:EURUSD"])
    list(pairs)
    with pytest.raises(ValueError, match="no symbols"):
        massive_session.forex.quotes(cast("list[str]", pairs))  # any iterable is read


def test_a_symbol_table_needs_a_symbol_column(massive_session: dm.MassiveSession) -> None:
    table = new_table([string_col("Pair", ["C:EURUSD"])])
    with pytest.raises(ValueError, match="must have a Symbol column"):
        massive_session.forex.snapshots(table)


def test_the_one_row_datasets_refuse_every_pair(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=r"use m\.forex\.snapshots\(dm\.ALL\)\.fetch\(\)"):
        massive_session.forex.last_quote(dm.ALL)
    every_bar = r"use m\.forex\.bars\(dm\.ALL, dm\.Timespan\.DAY\)\.history\(day, day\)"
    with pytest.raises(ValueError, match=every_bar):
        massive_session.forex.previous_day_bar(dm.ALL)


def test_a_missing_verb_names_the_dataset_that_has_it(massive_session: dm.MassiveSession) -> None:
    quotes = massive_session.forex.quotes("C:EURUSD")
    with pytest.raises(AttributeError, match=r"use m\.forex\.last_quote\(\.\.\.\)\.fetch\(\)"):
        _ = quotes.fetch
    day = massive_session.forex.bars("C:EURUSD", dm.Timespan.DAY)
    with pytest.raises(AttributeError, match=r"m\.forex\.previous_day_bar\(\.\.\.\)\.fetch"):
        _ = day.fetch


def test_rest_history_of_every_pair_needs_daily_bars(massive_session: dm.MassiveSession) -> None:
    rest = dm.HistorySource.REST
    with pytest.raises(ValueError, match=r"forex\.quotes history over REST needs explicit symbols"):
        massive_session.forex.quotes(dm.ALL).history("2026-10-20", "2026-10-20", source=rest)
    with pytest.raises(ValueError, match=r"forex\.bars history over REST needs explicit symbols"):
        massive_session.forex.bars(dm.ALL, dm.Timespan.HOUR).history(
            date(2026, 10, 20), date(2026, 10, 20), source=rest
        )


def test_every_docstring_links_its_section_and_states_limit_and_all() -> None:
    methods = {
        "quotes": forex._Forex.quotes,
        "last_quote": forex._Forex.last_quote,
        "bars": forex._Forex.bars,
        "previous_day_bar": forex._Forex.previous_day_bar,
        "fmv": forex._Forex.fmv,
        "snapshots": forex._Forex.snapshots,
        "movers": forex._Forex.movers,
        "conversion": forex._Forex.conversion,
    }
    assert {f"forex.{name}" for name in methods} == {spec.name for spec in forex.DATASETS}
    for name, method in methods.items():
        doc = method.__doc__ or ""
        assert f"{DOCS}{name}" in " ".join(doc.split()), name
        assert "columns()" in doc
        assert "limit" in doc
        assert "dm.ALL" in doc


# --- fixtures -------------------------------------------------------------------------------

_ENVELOPE = ("status", "request_id", "next_url", "count")


@pytest.mark.parametrize(
    ("name", "case"),
    [
        ("last_quote.json", (forex.LAST_QUOTE, "/v1/last_quote/currencies/{from}/{to}")),
        ("quotes.json", (forex.QUOTES, "/v3/quotes/{fxTicker}")),
        ("prev.json", (forex.PREVIOUS_DAY_BAR, "/v2/aggs/ticker/{forexTicker}/prev")),
        ("range.json", (forex.BARS, RANGE)),
        ("grouped_daily.json", (forex.BARS, "/v2/aggs/grouped/locale/global/market/fx/{date}")),
        ("snapshots.json", (forex.SNAPSHOTS, "/v2/snapshot/locale/global/markets/forex/tickers")),
        ("movers.json", (forex.MOVERS, "/v2/snapshot/locale/global/markets/forex/{direction}")),
        ("conversion.json", (forex.CONVERSION, "/v1/conversion/{from}/{to}")),
    ],
)
def test_each_fixture_has_the_generated_shape(
    load_fixture: Callable[[str, str], bytes], name: str, case: tuple[DatasetSpec, str]
) -> None:
    spec, path = case
    rest_path = source(R, path)
    assert isinstance(rest_path, RestPath)
    body = json.loads(load_fixture("namespaces/forex", name))
    if rest_path.result_key is None:
        rows = [{k: v for k, v in body.items() if k not in _ENVELOPE}]
    else:
        found = body[rest_path.result_key]
        rows = found if isinstance(found, list) else [found]
    declared = spec.keys_for(R, path).declared()
    required = {key.column for _, key in declared if not key.from_request}
    batch, drift = to_batch(rows, rest_path, required=required)
    assert batch.num_rows == len(rows) > 0
    assert (drift.extra, drift.missing) == (frozenset(), frozenset())


# --- REST, end to end -------------------------------------------------------------------------


@pytest.fixture
def rest_session(fake_rest: FakeRestServer) -> Iterator[dm.MassiveSession]:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    yield m
    m.close()


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


def fixture_body(load_fixture: Callable[[str, str], bytes], name: str) -> dict[str, Any]:
    body: dict[str, Any] = json.loads(load_fixture("namespaces/forex", name))
    return body


def pick(table: Table, *names: str) -> list[tuple[Any, ...]]:
    return [tuple(row[name] for name in names) for row in rows(table)]


def test_last_quote_reads_the_last_quote_of_each_pair(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    body = fixture_body(load_fixture, "last_quote.json")
    fake_rest.reply("/v1/last_quote/currencies/AUD/USD", Reply(body))
    table = rest_session.forex.last_quote("C:AUDUSD").fetch()
    # The pair goes out as two path segments, and the whole pair comes back as `Symbol`.
    assert sent(fake_rest) == [("/v1/last_quote/currencies/AUD/USD", {})]
    columns = ("Symbol", "Date", "Timestamp", "AskPrice", "BidPrice", "Exchange")
    assert pick(table, *columns) == [
        ("C:AUDUSD", "2020-11-16", 1_605_557_756_000 * MS, 0.73124, 0.73122, 48)
    ]


def test_quotes_history_reads_the_quote_history(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    body = fixture_body(load_fixture, "quotes.json")
    del body["next_url"]  # the sample's next page is on Massive's host
    fake_rest.reply("/v3/quotes/C%3AEURUSD", Reply(body))
    query = rest_session.forex.quotes("C:EURUSD")
    table = query.history(date(2021, 7, 1), date(2021, 7, 1), source=dm.HistorySource.REST)
    # A forex day is a UTC day, sent as nanosecond bounds of the half-open window.
    day = epoch_ns(datetime(2021, 7, 1, tzinfo=UTC))
    assert sent(fake_rest) == [
        (
            "/v3/quotes/C:EURUSD",
            {
                "limit": ["50000"],
                "order": ["asc"],
                "sort": ["timestamp"],
                "timestamp.gte": [str(day)],
                "timestamp.lt": [str(day + 86_400 * NS)],
            },
        )
    ]
    columns = ("Symbol", "Timestamp", "AskPrice", "BidPrice", "AskExchange", "BidExchange")
    assert pick(table, *columns) == [
        ("C:EURUSD", 1_625_097_600_000_000_000, 1.18565, 1.18558, 48, 48),
        ("C:EURUSD", 1_625_097_600_000_000_000, 1.18565, 1.18559, 48, 48),
    ]


def test_quotes_history_takes_deephaven_instants_rounded_outward(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    # The Java Instants a console holds, with nanoseconds a datetime cannot keep: the start
    # is rounded down and the end up, so the half-open range loses no row.
    body = fixture_body(load_fixture, "quotes.json")
    del body["next_url"]  # the sample's next page is on Massive's host
    fake_rest.reply("/v3/quotes/C%3AEURUSD", Reply(body))
    start = to_j_instant("2021-07-01T00:00:00.000000500Z")
    end = to_j_instant("2021-07-01T01:00:00.000000500Z")
    assert start is not None  # Deephaven's converters are typed Optional
    assert end is not None
    rest_session.forex.quotes("C:EURUSD").history(start, end, source=dm.HistorySource.REST)
    hour = epoch_ns(datetime(2021, 7, 1, tzinfo=UTC))
    ((_, params),) = sent(fake_rest)
    assert params["timestamp.gte"] == [str(hour)]
    assert params["timestamp.lt"] == [str(hour + 3_600 * NS + 1_000)]


def test_quotes_history_takes_numpy_times_and_deephaven_text(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    # Deephaven's own text for the start, and a numpy time in UTC, with nanoseconds, for the
    # end, which rounds up so the half-open range loses no row.
    body = fixture_body(load_fixture, "quotes.json")
    del body["next_url"]  # the sample's next page is on Massive's host
    fake_rest.reply("/v3/quotes/C%3AEURUSD", Reply(body))
    end = np.datetime64("2021-07-01T01:00:00.000000500", "ns")
    query = rest_session.forex.quotes("C:EURUSD")
    query.history("2021-06-30T20:00 America/New_York", end, source=dm.HistorySource.REST)
    hour = epoch_ns(datetime(2021, 7, 1, tzinfo=UTC))
    ((_, params),) = sent(fake_rest)
    assert params["timestamp.gte"] == [str(hour)]
    assert params["timestamp.lt"] == [str(hour + 3_600 * NS + 1_000)]


BAR_COLUMNS = (
    "Symbol",
    "Date",
    "TimestampStart",
    "TimestampEnd",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Vwap",
)
DAY_NS = 86_400 * NS


def test_previous_day_bar_reads_the_previous_session(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply(
        "/v2/aggs/ticker/C%3AEURUSD/prev", Reply(fixture_body(load_fixture, "prev.json"))
    )
    table = rest_session.forex.previous_day_bar("C:EURUSD").fetch()
    assert sent(fake_rest) == [("/v2/aggs/ticker/C:EURUSD/prev", {})]
    # Stamped 23:59:59.999 UTC by `/prev`, its window here is its whole UTC day.
    start = 1_651_622_400_000 * MS
    assert pick(table, *BAR_COLUMNS) == [
        (
            "C:EURUSD",
            "2022-05-04",
            start,
            start + DAY_NS,
            1.05252,
            1.0631,
            1.0505,
            1.06206,
            180300.0,
            1.055,
        )
    ]


def test_bars_history_reads_the_aggregate_range(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply(
        "/v2/aggs/ticker/C%3AEURUSD/range/1/hour/1626912000000/1626998399999",
        Reply(fixture_body(load_fixture, "range.json")),
    )
    query = rest_session.forex.bars("C:EURUSD", dm.Timespan.HOUR)
    table = query.history("2021-07-22", "2021-07-22", source=dm.HistorySource.REST)
    # The UTC day goes out as millisecond path segments, the page size as the query.
    assert sent(fake_rest) == [
        (
            "/v2/aggs/ticker/C:EURUSD/range/1/hour/1626912000000/1626998399999",
            {"limit": ["50000"], "sort": ["asc"]},
        )
    ]
    start = 1_626_912_000_000 * MS
    assert pick(table, *BAR_COLUMNS) == [
        (
            "C:EURUSD",
            "2021-07-22",
            start,
            start + 3_600 * NS,  # an hour bar ends an hour after it starts
            1.17921,
            1.18305,
            1.1756,
            1.17721,
            125329.0,
            1.1789,
        )
    ]


def test_daily_bars_of_every_pair_read_the_grouped_day(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply(
        "/v2/aggs/grouped/locale/global/market/fx/2020-10-14",
        Reply(fixture_body(load_fixture, "grouped_daily.json")),
    )
    query = rest_session.forex.bars(dm.ALL, dm.Timespan.DAY)
    table = query.history("2020-10-14", "2020-10-14", source=dm.HistorySource.REST)
    assert sent(fake_rest) == [("/v2/aggs/grouped/locale/global/market/fx/2020-10-14", {})]
    assert [row[0] for row in pick(table, "Symbol")] == ["C:ILSCHF", "C:GBPCAD", "C:DKKAUD"]


SNAPSHOTS_PATH = "/v2/snapshot/locale/global/markets/forex/tickers"


def test_snapshots_send_the_pairs_as_tickers(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply(SNAPSHOTS_PATH, Reply(fixture_body(load_fixture, "snapshots.json")))
    table = rest_session.forex.snapshots(["C:HKDCHF", "C:EURUSD"]).fetch()
    assert sent(fake_rest) == [(SNAPSHOTS_PATH, {"tickers": ["C:EURUSD,C:HKDCHF"]})]
    columns = ("Symbol", "DayClose", "LastQuoteAskPrice", "PrevDayClose", "Updated")
    assert pick(table, *columns) == [
        ("C:HKDCHF", 0.11778221, 0.11780678, 0.11797258, 1_605_280_919_000 * MS)
    ]


def test_snapshots_of_every_pair_send_no_tickers(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply(SNAPSHOTS_PATH, Reply(fixture_body(load_fixture, "snapshots.json")))
    assert rest_session.forex.snapshots(dm.ALL).fetch().size == 1
    assert sent(fake_rest) == [(SNAPSHOTS_PATH, {})]


def test_movers_put_the_direction_in_the_path(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    path = "/v2/snapshot/locale/global/markets/forex/losers"
    fake_rest.reply(path, Reply(fixture_body(load_fixture, "movers.json")))
    table = rest_session.forex.movers(dm.Direction.LOSERS).fetch()
    assert sent(fake_rest) == [(path, {})]
    assert pick(table, "Symbol", "TodaysChangePerc") == [("C:PLNILS", 5.13770674)]


def test_conversion_sends_the_pair_amount_and_precision(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    body = fixture_body(load_fixture, "conversion.json")
    fake_rest.reply("/v1/conversion/AUD/USD", Reply(body))
    table = rest_session.forex.conversion("aud", "usd", amount=100, precision=4).fetch()
    assert sent(fake_rest) == [("/v1/conversion/AUD/USD", {"amount": ["100"], "precision": ["4"]})]
    columns = ("Symbol", "Converted", "InitialAmount", "LastAsk", "LastBid", "LastTimestamp")
    # The vendor's `AUD/USD` becomes the canonical pair.
    assert pick(table, *columns) == [
        ("C:AUDUSD", 73.14, 100.0, 1.3673344, 1.3672596, 1_605_555_313_000 * MS)
    ]


# --- WebSocket, end to end --------------------------------------------------------------------

WAIT = 15.0
RowsWhen = Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]]


def kept(blink: Table) -> Table:
    # The blink table holds a row for one update cycle only; an append-only view keeps it.
    return blink_to_append_only(blink)


def at_ms(millis: int) -> datetime:
    # The instant `millis` milliseconds after the epoch, as a snapshot's row holds it.
    return EPOCH + timedelta(milliseconds=millis)


def test_a_quote_frame_lands_in_blink(ws_session: WsSession, rows_when: RowsWhen) -> None:
    table = kept(ws_session.session.forex.quotes("C:EURUSD").blink())
    ws_session.subscribed("C.EUR-USD")
    assert ws_session.decisions == [(AssetClass.FOREX, ChannelFamily.TICK)]
    ws_session.publish(
        {"ev": "C", "p": "EUR-USD", "x": 44, "a": 1.0832, "b": 1.0831, "t": 1_700_000_000_123}
    )
    columns = ("Symbol", "Timestamp", "AskPrice", "BidPrice", "Exchange")
    (row,) = rows_when(table, bool)
    assert tuple(row[c] for c in columns) == (
        "C:EURUSD",
        at_ms(1_700_000_000_123),
        1.0832,
        1.0831,
        44,
    )


@pytest.mark.parametrize("case", [(dm.Timespan.MINUTE, "CA"), (dm.Timespan.SECOND, "CAS")])
def test_a_bar_frame_lands_in_blink(
    ws_session: WsSession, rows_when: RowsWhen, case: tuple[dm.Timespan, str]
) -> None:
    timespan, channel = case
    query = ws_session.session.forex.bars("C:EURUSD", timespan)
    assert isinstance(query, StreamHistoryQuery)
    table = kept(query.blink())
    ws_session.subscribed(f"{channel}.EUR-USD")
    frame = {
        "ev": channel,
        "pair": "EUR-USD",
        "o": 1.08,
        "c": 1.09,
        "h": 1.1,
        "l": 1.07,
        "v": 20,
        "s": 1_700_000_040_000,
        "e": 1_700_000_100_000,
    }
    ws_session.publish(frame)
    columns = ("Symbol", "Date", "TimestampStart", "Open", "Close", "Volume", "TimestampEnd")
    (row,) = rows_when(table, bool)
    # The end is the start plus the bar's length, whatever `e` says (60 s here for both).
    length = 60_000 if timespan is dm.Timespan.MINUTE else 1_000
    assert tuple(row[c] for c in columns) == (
        "C:EURUSD",
        "2023-11-14",  # the UTC day of the start
        at_ms(1_700_000_040_000),
        1.08,
        1.09,
        20.0,
        at_ms(1_700_000_040_000 + length),
    )
    assert row["TimestampInsert"] is not None


def test_a_fair_value_frame_lands_in_blink(ws_session: WsSession, rows_when: RowsWhen) -> None:
    table = kept(ws_session.session.forex.fmv("C:EURUSD").blink())
    ws_session.subscribed("FMV.EUR-USD")
    # Fair value is served by the FMV family's host, not the tick host.
    assert ws_session.decisions == [(AssetClass.FOREX, ChannelFamily.FMV)]
    ws_session.publish({"ev": "FMV", "fmv": 1.0631, "sym": "C:EURUSD", "t": 1_678_220_098_130})
    (row,) = rows_when(table, bool)
    assert (row["Symbol"], row["Timestamp"], row["Fmv"]) == (
        "C:EURUSD",
        at_ms(1_678_220_098_130),
        1.0631,
    )


# --- Flat files, end to end -------------------------------------------------------------------

FILE_DAY = "2025-03-03"
AFTER_PUBLICATION = datetime(2025, 3, 10, 16, tzinfo=UTC)


@pytest.fixture
def files_session(fake_s3: FakeS3, monkeypatch: pytest.MonkeyPatch) -> Iterator[dm.MassiveSession]:
    monkeypatch.setattr(history_source, "_now", lambda: AFTER_PUBLICATION)
    credentials = {"s3_access_key_id": "AKIDFLATFILES", "s3_secret_access_key": "flat-file-secret"}
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", **cast("Any", credentials)))
    yield m
    m.close()


def file_key(prefix: str) -> str:
    return f"{prefix}/2025/03/{FILE_DAY}.csv.gz"


def test_quote_files_round_trip(*, files_session: dm.MassiveSession, fake_s3: FakeS3) -> None:
    moment = epoch_ns(datetime(2025, 3, 3, 12, tzinfo=UTC))
    header = (
        "ticker",
        "ask_exchange",
        "ask_price",
        "bid_exchange",
        "bid_price",
        "participant_timestamp",
    )
    fake_s3.put_csv(
        file_key("global_forex/quotes_v1"),
        header,
        [
            dict(zip(header, ("C:EURUSD", 48, 1.08066, 48, 1.08063, moment), strict=True)),
            dict(zip(header, ("C:GBPUSD", 48, 1.2701, 48, 1.27, moment), strict=True)),
        ],
    )
    query = files_session.forex.quotes("C:EURUSD")
    table = query.history(FILE_DAY, FILE_DAY, source=dm.HistorySource.FLAT_FILES)
    assert fake_s3.requests == [("flatfiles", file_key("global_forex/quotes_v1"))]
    columns = ("Symbol", "Timestamp", "AskPrice", "BidPrice", "AskExchange", "Exchange")
    assert pick(table, *columns) == [("C:EURUSD", moment, 1.08066, 1.08063, 48, None)]


def test_quote_files_in_the_sample_spelling_are_read(
    *, files_session: dm.MassiveSession, fake_s3: FakeS3
) -> None:
    # The vendor's sample writes the forex quotes file's ticker as X:EUR-USD; the filter
    # looks for every spelling of the pair, and the rows hold the canonical ticker.
    moment = epoch_ns(datetime(2025, 3, 3, 12, tzinfo=UTC))
    header = (
        "ticker",
        "ask_exchange",
        "ask_price",
        "bid_exchange",
        "bid_price",
        "participant_timestamp",
    )
    fake_s3.put_csv(
        file_key("global_forex/quotes_v1"),
        header,
        [
            dict(zip(header, ("X:EUR-USD", 48, 1.08066, 48, 1.08063, moment), strict=True)),
            dict(zip(header, ("X:GBP-USD", 48, 1.2701, 48, 1.27, moment), strict=True)),
        ],
    )
    flat = dm.HistorySource.FLAT_FILES
    one = files_session.forex.quotes("C:EURUSD").history(FILE_DAY, FILE_DAY, source=flat)
    assert pick(one, "Symbol", "AskPrice") == [("C:EURUSD", 1.08066)]
    every = files_session.forex.quotes(dm.ALL).history(FILE_DAY, FILE_DAY, source=flat)
    assert pick(every, "Symbol") == [("C:EURUSD",), ("C:GBPUSD",)]


@pytest.mark.parametrize("spelling", ["C:EUR-USD", "X:EUR-USD", "C:EURUSD"])
def test_a_quote_file_in_any_massive_spelling_is_read(
    *, files_session: dm.MassiveSession, fake_s3: FakeS3, spelling: str
) -> None:
    # Massive's knowledge base says the forex files write `C:EUR-USD`; the sample shows
    # `X:EUR-USD`. A request for `C:EURUSD` finds the rows in every one.
    moment = epoch_ns(datetime(2025, 3, 3, 12, tzinfo=UTC))
    header = (
        "ticker",
        "ask_exchange",
        "ask_price",
        "bid_exchange",
        "bid_price",
        "participant_timestamp",
    )
    row = dict(zip(header, (spelling, 48, 1.08066, 48, 1.08063, moment), strict=True))
    fake_s3.put_csv(file_key("global_forex/quotes_v1"), header, [row])
    table = files_session.forex.quotes("C:EURUSD").history(
        FILE_DAY, FILE_DAY, source=dm.HistorySource.FLAT_FILES
    )
    assert pick(table, "Symbol", "AskPrice") == [("C:EURUSD", 1.08066)]


BAR_HEADER = ("ticker", "volume", "open", "close", "high", "low", "window_start", "transactions")


@pytest.mark.parametrize(
    ("timespan", "prefix", "offset"),
    [
        (dm.Timespan.MINUTE, "global_forex/minute_aggs_v1", 0),
        # The day files stamp a day at its last millisecond; it is stamped at its start here.
        (dm.Timespan.DAY, "global_forex/day_aggs_v1", 86_400 * 10**9 - 10**6),
    ],
)
def test_bar_files_round_trip(
    *,
    files_session: dm.MassiveSession,
    fake_s3: FakeS3,
    timespan: dm.Timespan,
    prefix: str,
    offset: int,
) -> None:
    start = epoch_ns(datetime(2025, 3, 3, tzinfo=UTC))
    values = ("C:EURUSD", 120, 1.08063, 1.08033, 1.0807, 1.0803, start + offset, 120)
    fake_s3.put_csv(file_key(prefix), BAR_HEADER, [dict(zip(BAR_HEADER, values, strict=True))])
    # With no `adjusted` to rule them out, AUTO reads every pair's bars from the files.
    query = files_session.forex.bars(dm.ALL, timespan)
    table = query.history(FILE_DAY, FILE_DAY)
    assert fake_s3.requests == [("flatfiles", file_key(prefix))]
    length = 60 * NS if timespan is dm.Timespan.MINUTE else DAY_NS
    assert pick(table, *BAR_COLUMNS) == [
        (
            "C:EURUSD",
            "2025-03-03",
            start,
            start + length,
            1.08063,
            1.0807,
            1.0803,
            1.08033,
            120.0,
            None,
        )
    ]


# --- Batched snapshots and poll(), end to end -------------------------------------------------


def test_a_long_snapshot_list_is_split_into_requests_of_250(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply(SNAPSHOTS_PATH, Reply(fixture_body(load_fixture, "snapshots.json")))
    codes = ["".join(letters) for letters in itertools.product(string.ascii_uppercase, repeat=3)]
    pairs = [f"C:{code}USD" for code in codes[:251]]
    rest_session.forex.snapshots(pairs).fetch()
    sizes = sorted(len(query["tickers"][0].split(",")) for _, query in sent(fake_rest))
    assert sizes == [1, 250]


@pytest.fixture
def poll_clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    # A manual clock: each poll cycle waits until the test advances it.
    clock = FakeClock(manual=True)
    monkeypatch.setattr(poll, "CLOCK", clock)
    return clock


def next_cycle(clock: FakeClock) -> None:
    # Ends the wait after a cycle and returns once the next cycle has run and waits again.
    clock.wait_for_sleepers(1)
    waits = len(clock.waits)
    clock.advance(60)
    until(lambda: len(clock.waits) > waits)


def until(condition: Callable[[], bool]) -> None:
    # Waits for the update graph to apply a published cycle; fails after WAIT seconds.
    pause = threading.Event()  # never set; only paces the wait
    for _ in range(int(WAIT / 0.01)):
        if condition():
            return
        pause.wait(0.01)
    assert condition()


def changes(
    expected: set[tuple[str, float]], column: str
) -> Callable[[list[dict[str, Any]]], bool]:
    # True for a snapshot whose rows are exactly `expected`, as (Symbol, column) pairs.
    def match(found: list[dict[str, Any]]) -> bool:
        pairs = [(row["Symbol"], row[column]) for row in found]
        return len(pairs) == len(expected) and set(pairs) == expected

    return match


def snapshot_row(ticker: str, change: float) -> dict[str, object]:
    return {"ticker": ticker, "todaysChangePerc": change, "updated": 1_605_280_919_000}


@pytest.mark.parametrize(
    ("make", "path"),
    [
        (lambda m: m.forex.snapshots(["C:EURUSD", "C:GBPUSD"]), SNAPSHOTS_PATH),
        (
            lambda m: m.forex.movers(dm.Direction.GAINERS),
            "/v2/snapshot/locale/global/markets/forex/gainers",
        ),
    ],
)
def test_snapshot_polls_keep_one_row_per_symbol(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    poll_clock: FakeClock,
    rows_when: RowsWhen,
    make: Callable[[dm.MassiveSession], Query],
    path: str,
) -> None:
    first = {
        "status": "OK",
        "tickers": [snapshot_row("C:EURUSD", 1.0), snapshot_row("C:GBPUSD", 2.0)],
    }
    second = {"status": "OK", "tickers": [snapshot_row("C:EURUSD", 3.0)]}
    fake_rest.reply(path, Reply(first), Reply(second))
    table = make(rest_session).poll(60)
    rows_when(table, changes({("C:EURUSD", 1.0), ("C:GBPUSD", 2.0)}, "TodaysChangePerc"))
    # The next cycle replaces the rows by Symbol: EUR/USD is updated, GBP/USD leaves.
    next_cycle(poll_clock)
    rows_when(table, changes({("C:EURUSD", 3.0)}, "TodaysChangePerc"))
    assert [p for p, _ in sent(fake_rest)] == [path, path]


def test_a_conversion_poll_keeps_one_row(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    poll_clock: FakeClock,
    rows_when: RowsWhen,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    body = fixture_body(load_fixture, "conversion.json")
    fake_rest.reply("/v1/conversion/AUD/USD", Reply(body), Reply({**body, "converted": 74.0}))
    table = rest_session.forex.conversion("AUD", "USD", amount=100).poll(60)
    rows_when(table, changes({("C:AUDUSD", 73.14)}, "Converted"))
    next_cycle(poll_clock)
    rows_when(table, changes({("C:AUDUSD", 74.0)}, "Converted"))


def test_a_last_quote_poll_keeps_one_row_per_pair(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    poll_clock: FakeClock,
    rows_when: RowsWhen,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    body = fixture_body(load_fixture, "last_quote.json")
    later = {**body, "last": {**body["last"], "bid": 0.7313, "timestamp": 1605557757000}}
    fake_rest.reply("/v1/last_quote/currencies/AUD/USD", Reply(body), Reply(later))
    table = rest_session.forex.last_quote("C:AUDUSD").poll(60)
    rows_when(table, changes({("C:AUDUSD", 0.73122)}, "BidPrice"))
    # The next cycle replaces the pair's row with its newer quote.
    next_cycle(poll_clock)
    rows_when(table, changes({("C:AUDUSD", 0.7313)}, "BidPrice"))


def test_the_current_state_datasets_revise_on_their_update_times() -> None:
    # An explicit APPEND poll appends each new state of a pair, or each new conversion.
    revised = {spec.name: spec.revision_keys for spec in forex.DATASETS if spec.revision_keys}
    assert revised == {
        "forex.snapshots": ("Updated",),
        "forex.movers": ("Updated",),
        "forex.conversion": ("LastTimestamp",),
        "forex.last_quote": ("Timestamp",),
        "forex.previous_day_bar": ("TimestampStart",),
    }
    for name, keys in revised.items():
        assert set(keys) <= set(CATALOG.dataset_schema(CATALOG.by_name[name]).column_names)
