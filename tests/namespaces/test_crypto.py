"""Tests for the `m.crypto` namespace: its dataset specs, schemas, factories, and fixtures.

The specs are checked against the generated sources through a catalog of the crypto datasets
alone, so the expected schemas do not depend on which other namespaces have declared members
of the shared kinds. The fixtures under `tests/data/namespaces/crypto/` are shaped from the
vendor's documented samples; they are checked against the generated sources they stand for,
then served end to end: through the fake REST server (the exact request and the decoded rows),
the fake WebSocket server (one documented frame lands in `blink()` per channel), and the fake
S3 server (one block of each flat-file prefix round-trips through `history()`).
"""

import csv
import io
import json
import threading
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, date, datetime, timedelta, timezone
from typing import Any, cast
from urllib.parse import parse_qs, unquote, urlsplit

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import new_table
from deephaven.column import string_col
from deephaven.table import Table
from deephaven.table_listener import TableUpdate, listen

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
from deephaven_massive._datasets.request import HostDecision, Request
from deephaven_massive._datasets.schema import required_fields
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import ChannelFamily, Recency
from deephaven_massive._ingest import ws
from deephaven_massive._massive import (
    DriftReport,
    Reject,
    RestPath,
    Source,
    Status,
    Transport,
    WireError,
    WsState,
    source,
    to_batch,
)
from deephaven_massive._namespaces import crypto
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
from deephaven_massive._tables.liveness import new_scope

# The reply builder is a plain class of the fake, which no fixture can hand over; the servers
# themselves come from the `fake_rest` and `fake_ws_server` fixtures.
from tests._fakes.rest import FakeRestServer, Reply
from tests._fakes.s3 import FakeS3
from tests._fakes.ws import FakeMassiveServer

Fixture = Callable[[str, str], bytes]

R, W, F = Transport.REST, Transport.WS, Transport.FLAT_FILES
OWNER = "namespaces/crypto"
CRYPTO = Catalog(crypto.DATASETS)


def request_of(query: QueryBase) -> Request:
    return query._request


# --- The namespace and its specs -------------------------------------------------------------


def test_namespace_is_reached_from_the_session(massive_session: dm.MassiveSession) -> None:
    namespace = massive_session.crypto
    assert type(namespace) is crypto._Crypto
    assert isinstance(namespace, Namespace)


def test_the_namespace_declares_every_crypto_dataset() -> None:
    assert [spec.name for spec in crypto.DATASETS] == [
        "crypto.trades",
        "crypto.quotes",
        "crypto.bars",
        "crypto.fmv",
        "crypto.snapshots",
        "crypto.movers",
        "crypto.daily_open_close",
    ]
    assert all(spec.asset_class is dm.AssetClass.CRYPTO for spec in crypto.DATASETS)


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (crypto.TRADES, StreamHistoryLatestQuery),
        (crypto.QUOTES, StreamQuery),
        (crypto.BARS, HistoryQuery),
        (crypto.FMV, StreamQuery),
        (crypto.SNAPSHOTS, Query),
        (crypto.MOVERS, Query),
        (crypto.DAILY_OPEN_CLOSE, Query),
    ],
)
def test_each_dataset_has_the_query_class_of_its_transports(
    spec: DatasetSpec, expected: type
) -> None:
    assert query_class(spec) is expected


def test_each_factory_docstring_starts_with_its_summary() -> None:
    for spec in crypto.DATASETS:
        factory = getattr(crypto._Crypto, spec.name.partition(".")[2])
        assert (factory.__doc__ or "").startswith(spec.summary + "\n"), spec.name


def test_the_crypto_datasets_pass_every_catalog_gate() -> None:
    assert CRYPTO.problems() == ()


def test_the_crypto_datasets_read_every_crypto_source() -> None:
    used = {source for spec in crypto.DATASETS for source in spec.sources}
    assert used == {
        (R, "/v1/last/crypto/{from}/{to}"),
        (R, "/v1/open-close/crypto/{from}/{to}/{date}"),
        (R, "/v2/aggs/grouped/locale/global/market/crypto/{date}"),
        (R, "/v2/aggs/ticker/{cryptoTicker}/prev"),
        (R, "/v2/aggs/ticker/{cryptoTicker}/range/{multiplier}/{timespan}/{from}/{to}"),
        (R, "/v2/snapshot/locale/global/markets/crypto/tickers"),
        (R, "/v2/snapshot/locale/global/markets/crypto/{direction}"),
        (R, "/v3/trades/{cryptoTicker}"),
        (W, "crypto/XT"),
        (W, "crypto/XQ"),
        (W, "crypto/XA"),
        (W, "crypto/XAS"),
        (W, "crypto/FMV"),
        (F, "global_crypto/day_aggs_v1"),
        (F, "global_crypto/minute_aggs_v1"),
        (F, "global_crypto/trades_v1"),
    }


def test_the_wire_details_of_the_specs() -> None:
    assert crypto.TRADES.time_axis == ("timestamp.gte", "timestamp.lt")
    assert crypto.BARS.channels == {dm.Timespan.SECOND: "XAS", dm.Timespan.MINUTE: "XA"}
    assert set(crypto.BARS.flat_file_prefixes) == {dm.Timespan.MINUTE, dm.Timespan.DAY}
    assert (crypto.TRADES.page_size, crypto.BARS.page_size) == (50_000, 50_000)
    assert crypto.FMV.family is ChannelFamily.FMV
    assert crypto.QUOTES.family is ChannelFamily.TICK
    assert crypto.SNAPSHOTS.symbol_param == "tickers"
    assert crypto.DAILY_OPEN_CLOSE.per_symbol
    assert not crypto.SNAPSHOTS.per_symbol
    assert all(spec.poll_mode is dm.PollMode.REPLACE for spec in crypto.DATASETS)


# --- Schemas ---------------------------------------------------------------------------------


def columns(spec: DatasetSpec) -> list[tuple[str, str]]:
    return [(c.name, c.type.value) for c in CRYPTO.dataset_schema(spec).columns]


def test_trades_schema() -> None:
    # The participant time is the row's time on every source but the last trade, whose own
    # `timestamp` is; the pair from the stream's `pair`, the files' `ticker`, and the request
    # is `Symbol`.
    assert columns(crypto.TRADES) == [
        ("Symbol", "String"),
        ("Timestamp", "Instant"),
        ("Conditions", "int[]"),
        ("Exchange", "long"),
        ("Id", "String"),
        ("Price", "double"),
        ("Size", "double"),
        ("ReceivedTimestamp", "Instant"),
    ]


def test_quotes_schema() -> None:
    # The stream's `bp` and `ap` take the quotes kind's names through its projection.
    assert columns(crypto.QUOTES) == [
        ("Symbol", "String"),
        ("Timestamp", "Instant"),
        ("BidPrice", "double"),
        ("BidSize", "double"),
        ("AskPrice", "double"),
        ("AskSize", "double"),
        ("Exchange", "long"),
        ("ReceivedTimestamp", "Instant"),
    ]


def test_bars_schema() -> None:
    assert columns(crypto.BARS) == [
        ("Symbol", "String"),
        ("Timestamp", "Instant"),
        ("Volume", "double"),
        ("Open", "double"),
        ("Close", "double"),
        ("High", "double"),
        ("Low", "double"),
        ("Transactions", "long"),
        ("Vwap", "double"),
        ("EndTimestamp", "Instant"),
        ("AverageTradeSize", "long"),
    ]


def test_fmv_schema() -> None:
    assert columns(crypto.FMV) == [
        ("Symbol", "String"),
        ("Timestamp", "Instant"),
        ("Fmv", "double"),
    ]


@pytest.mark.parametrize("spec", [crypto.SNAPSHOTS, crypto.MOVERS])
def test_snapshot_schemas(spec: DatasetSpec) -> None:
    found = columns(spec)
    # A snapshot row is keyed by its pair only; its update time keeps its own column.
    assert found[0] == ("Symbol", "String")
    assert ("Updated", "Instant") in found
    assert "Timestamp" not in dict(found)
    assert ("LastTradeId", "String") in found
    assert ("LastTradeParticipantTimestamp", "Instant") in found
    assert ("LastMinuteWindowStart", "Instant") in found
    assert len(found) == 31


def test_daily_open_close_schema() -> None:
    # The body's own `symbol` is the key; its day is a calendar date, not a time key.
    assert columns(crypto.DAILY_OPEN_CLOSE) == [
        ("Symbol", "String"),
        ("Close", "double"),
        ("ClosingTrades", "String"),
        ("Day", "LocalDate"),
        ("IsUtc", "boolean"),
        ("Open", "double"),
        ("OpenTrades", "String"),
    ]


def test_no_dataset_keeps_a_stand_in_for_its_keys() -> None:
    for spec in crypto.DATASETS:
        names = set(CRYPTO.dataset_schema(spec).column_names)
        assert "Symbol" in names
        assert {"Pair", "Ticker", "Underlying"}.isdisjoint(names), spec.name


# --- Factories -------------------------------------------------------------------------------


def test_trades_normalizes_every_pair_form(massive_session: dm.MassiveSession) -> None:
    q = massive_session.crypto.trades(["eth-usd", "BTC/USD", "X:BTCUSD"])
    assert type(q) is StreamHistoryLatestQuery
    assert request_of(q).symbols == ("X:BTCUSD", "X:ETHUSD")
    assert massive_session.crypto.trades("BTC-USD") == massive_session.crypto.trades("X:BTCUSD")


@pytest.mark.parametrize("method", ["trades", "quotes", "fmv", "snapshots"])
def test_the_streams_and_snapshots_accept_all(
    massive_session: dm.MassiveSession, method: str
) -> None:
    q = getattr(massive_session.crypto, method)(dm.ALL)
    assert request_of(q).symbols is dm.ALL


@pytest.mark.parametrize(
    ("method", "expected"), [("quotes", StreamQuery), ("fmv", StreamQuery), ("snapshots", Query)]
)
def test_the_single_class_factories(
    massive_session: dm.MassiveSession, method: str, expected: type
) -> None:
    q: QueryBase = getattr(massive_session.crypto, method)("SOL-USD")
    assert type(q) is expected
    assert request_of(q).symbols == ("X:SOLUSD",)


def test_a_symbol_table_is_followed(massive_session: dm.MassiveSession) -> None:
    watch = new_table([string_col("Symbol", ["BTC-USD"])])
    q = massive_session.crypto.quotes(watch)
    assert isinstance(request_of(q).symbols, SymbolTable)


@pytest.mark.parametrize(
    "case",
    [
        (dm.Timespan.SECOND, 1, StreamHistoryQuery),
        (dm.Timespan.MINUTE, 1, StreamHistoryQuery),
        (dm.Timespan.DAY, 1, HistoryLatestQuery),
        (dm.Timespan.MINUTE, 5, HistoryQuery),
        (dm.Timespan.DAY, 2, HistoryQuery),
        (dm.Timespan.HOUR, 1, HistoryQuery),
        (dm.Timespan.YEAR, 1, HistoryQuery),
    ],
)
def test_bars_pick_their_class(
    massive_session: dm.MassiveSession, case: tuple[dm.Timespan, int, type]
) -> None:
    timespan, multiplier, expected = case
    q = massive_session.crypto.bars("BTC-USD", timespan, multiplier=multiplier)
    assert type(q) is expected
    request = request_of(q)
    assert (request.timespan, request.multiplier, request.adjusted) == (timespan, multiplier, None)


def test_bars_default_to_streamed_minutes(massive_session: dm.MassiveSession) -> None:
    q = massive_session.crypto.bars(dm.ALL)
    assert type(q) is StreamHistoryQuery
    request = request_of(q)
    # Crypto has no corporate actions: bars carry no adjustment choice.
    assert (request.symbols, request.timespan, request.adjusted) == (
        dm.ALL,
        dm.Timespan.MINUTE,
        None,
    )


def test_bar_arguments_are_checked(massive_session: dm.MassiveSession) -> None:
    bars = massive_session.crypto.bars
    with pytest.raises(ValueError, match=r"Timespan\.SESSION exists only for futures"):
        bars("BTC-USD", dm.Timespan.SESSION)
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan"):
        bars("BTC-USD", "minute")  # type: ignore[call-overload]  # a plain string is refused
    with pytest.raises(ValueError, match="multiplier"):
        bars("BTC-USD", dm.Timespan.HOUR, multiplier=0)
    with pytest.raises(TypeError, match="adjusted"):
        bars("BTC-USD", adjusted=True)  # type: ignore[call-overload]  # crypto bars have none


def test_movers(massive_session: dm.MassiveSession) -> None:
    q = massive_session.crypto.movers(dm.Direction.LOSERS)
    assert type(q) is Query
    request = request_of(q)
    assert request.symbols is None
    assert request.params == (("direction", dm.Direction.LOSERS),)
    with pytest.raises(TypeError, match=r"direction must be dm\.Direction"):
        massive_session.crypto.movers("gainers")  # type: ignore[arg-type]  # a plain string


def test_daily_open_close(massive_session: dm.MassiveSession) -> None:
    q = massive_session.crypto.daily_open_close(["BTC-USD", "ETH-USD"], "2026-01-02")
    assert type(q) is Query
    request = request_of(q)
    assert request.symbols == ("X:BTCUSD", "X:ETHUSD")
    assert request.params == (("date", date(2026, 1, 2)),)
    # Crypto has no corporate actions: no adjustment choice is stored or sent.
    assert request.adjusted is None
    same = massive_session.crypto.daily_open_close("BTC-USD", date(2026, 1, 2))
    assert request_of(same).params == (("date", date(2026, 1, 2)),)
    with pytest.raises(TypeError, match="adjusted"):
        massive_session.crypto.daily_open_close(
            "BTC-USD",
            "2026-01-02",
            adjusted=False,  # type: ignore[call-arg]  # crypto open/close has none
        )


@pytest.mark.parametrize(
    "moment",
    [
        datetime(2026, 1, 2, 23, 30, tzinfo=timezone(timedelta(hours=-5))),  # 04:30 UTC, Jan 3
        "2026-01-02T23:30:00-05:00",
    ],
)
def test_daily_open_close_reads_a_time_as_its_utc_day(
    massive_session: dm.MassiveSession, moment: datetime | str
) -> None:
    q = massive_session.crypto.daily_open_close("BTC-USD", moment)
    assert request_of(q).params == (("date", date(2026, 1, 3)),)


def test_daily_open_close_arguments_are_checked(massive_session: dm.MassiveSession) -> None:
    daily = massive_session.crypto.daily_open_close
    with pytest.raises(
        ValueError,
        match=(
            r"crypto\.daily_open_close does not accept dm\.ALL; "
            r"use m\.crypto\.bars\(dm\.ALL, dm\.Timespan\.DAY\)\.history\(day, day\)"
        ),
    ):
        daily(dm.ALL, "2026-01-02")
    with pytest.raises(ValueError, match="date"):
        daily("BTC-USD", datetime(2026, 1, 2, 12))  # noqa: DTZ001  # a naive time is refused
    with pytest.raises(ValueError, match="date"):
        daily("BTC-USD", "January 2")
    with pytest.raises(TypeError):
        daily("BTC-USD", 20260102)  # type: ignore[arg-type]  # not a date


def exhausted() -> Iterator[str]:
    yield from ()


@pytest.mark.parametrize(
    "call",
    [
        lambda c: c.trades(exhausted()),
        lambda c: c.quotes([]),
        lambda c: c.bars(exhausted()),
        lambda c: c.fmv(exhausted()),
        lambda c: c.snapshots(exhausted()),
        lambda c: c.daily_open_close(exhausted(), "2026-01-02"),
    ],
    ids=["trades", "quotes", "bars", "fmv", "snapshots", "daily_open_close"],
)
def test_no_symbols_is_refused(
    massive_session: dm.MassiveSession, call: Callable[[Any], object]
) -> None:
    with pytest.raises(ValueError, match="no symbols"):
        call(massive_session.crypto)


@pytest.mark.parametrize("bad", ["BTC-XYZ", "C:EURUSD", "O:AAPL250117C00150000"])
def test_a_symbol_that_is_no_crypto_pair_is_refused(
    massive_session: dm.MassiveSession, bad: str
) -> None:
    with pytest.raises(ValueError, match=bad):
        massive_session.crypto.trades(bad)


def test_a_symbol_table_needs_a_symbol_column(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match="Symbol column"):
        massive_session.crypto.fmv(new_table([string_col("Pair", ["BTC-USD"])]))


# --- Verb-time errors that need no transport -------------------------------------------------


def test_fetch_needs_explicit_symbols(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=r"use m\.crypto\.snapshots\(dm\.ALL\)\.fetch\(\)"):
        massive_session.crypto.trades(dm.ALL).fetch()
    with pytest.raises(ValueError, match=r"crypto\.bars\.fetch\(\) needs explicit symbols"):
        massive_session.crypto.bars(dm.ALL, dm.Timespan.DAY).fetch()


def test_rest_history_needs_explicit_symbols(massive_session: dm.MassiveSession) -> None:
    rest = dm.HistorySource.REST
    with pytest.raises(ValueError, match=r"crypto\.trades history over REST needs explicit"):
        massive_session.crypto.trades(dm.ALL).history("2026-01-02", "2026-01-02", source=rest)
    with pytest.raises(ValueError, match=r"crypto\.bars history over REST needs explicit"):
        massive_session.crypto.bars(dm.ALL).history("2026-01-02", "2026-01-02", source=rest)


def test_flat_file_history_states_its_constraints(massive_session: dm.MassiveSession) -> None:
    q = massive_session.crypto.bars("BTC-USD", dm.Timespan.HOUR)
    with pytest.raises(
        ValueError,
        match=r"crypto\.bars cannot be read from flat files: .*hold only 1-minute, 1-day bars",
    ):
        q.history("2026-01-02", "2026-01-02", source=dm.HistorySource.FLAT_FILES)


# --- Fixtures --------------------------------------------------------------------------------


def rest_rows(body: dict[str, object], path: RestPath) -> list[dict[str, object]]:
    rows = body if path.result_key is None else body[path.result_key]
    return [rows] if isinstance(rows, dict) else cast("list[dict[str, object]]", rows)


@pytest.mark.parametrize(
    "case",
    [
        (crypto.TRADES, "/v1/last/crypto/{from}/{to}", "last_trade.json"),
        (crypto.TRADES, "/v3/trades/{cryptoTicker}", "trades.json"),
        (crypto.BARS, "/v2/aggs/ticker/{cryptoTicker}/prev", "prev.json"),
        (
            crypto.BARS,
            "/v2/aggs/ticker/{cryptoTicker}/range/{multiplier}/{timespan}/{from}/{to}",
            "range.json",
        ),
        (crypto.BARS, "/v2/aggs/grouped/locale/global/market/crypto/{date}", "grouped.json"),
        (crypto.SNAPSHOTS, "/v2/snapshot/locale/global/markets/crypto/tickers", "snapshots.json"),
        (crypto.MOVERS, "/v2/snapshot/locale/global/markets/crypto/{direction}", "movers.json"),
        (crypto.DAILY_OPEN_CLOSE, "/v1/open-close/crypto/{from}/{to}/{date}", "open_close.json"),
    ],
)
def test_rest_fixtures_decode_without_drift(
    load_fixture: Callable[[str, str], bytes], case: tuple[DatasetSpec, str, str]
) -> None:
    spec, template, name = case
    path = source(R, template)
    assert isinstance(path, RestPath)
    required = [field.column for field in required_fields(spec, path)]
    rows = rest_rows(json.loads(load_fixture(OWNER, name)), path)
    batch, drift = to_batch(rows, path, required=required)
    assert batch.num_rows == len(rows) > 0
    assert (drift.extra, drift.missing) == (frozenset(), frozenset())


@pytest.mark.parametrize("channel", ["XT", "XQ", "XA", "XAS", "FMV"])
def test_ws_frames_carry_exactly_the_generated_fields(
    load_fixture: Callable[[str, str], bytes], channel: str
) -> None:
    frames = json.loads(load_fixture(OWNER, "ws_frames.json"))[channel]
    keys = {f.wire_key for f in source(W, f"crypto/{channel}").fields}
    for frame in frames:
        assert frame["ev"] == channel
        assert set(frame) - {"ev"} == keys


@pytest.mark.parametrize(
    ("prefix", "name"),
    [
        ("global_crypto/trades_v1", "global_crypto_trades_v1_2023-03-28.csv"),
        ("global_crypto/minute_aggs_v1", "global_crypto_minute_aggs_v1_2023-03-28.csv"),
        ("global_crypto/day_aggs_v1", "global_crypto_day_aggs_v1_2023-03-28.csv"),
    ],
)
def test_flat_file_fixtures_have_the_generated_header(
    load_fixture: Callable[[str, str], bytes], prefix: str, name: str
) -> None:
    reader = csv.reader(io.StringIO(load_fixture(OWNER, name).decode()))
    header = next(reader)
    assert header == [f.wire_key for f in source(F, prefix).fields]
    assert all(len(row) == len(header) for row in reader)


# --- End to end: REST ----------------------------------------------------------------------------

NS = 10**9
MS = 10**6


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


def serve(server: FakeRestServer, target: str, fixture: bytes) -> None:
    # The client percent-encodes the `:` of a crypto ticker in the path.
    server.reply(target.replace(":", "%3A"), Reply(json.loads(fixture)))


def test_trades_fetch_reads_the_last_trade_of_each_pair(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    serve(fake_rest, "/v1/last/crypto/BTC/USD", load_fixture(OWNER, "last_trade.json"))
    out = rows(rest_session.crypto.trades("BTC-USD").fetch())
    assert sent(fake_rest) == [("/v1/last/crypto/BTC/USD", {})]
    [row] = out
    supplied = {
        "Symbol": "X:BTCUSD",
        "Timestamp": 1605560885027 * MS,
        "Conditions": [1],
        "Exchange": 4,
        "Price": 16835.42,
        "Size": 0.006909,
    }
    assert {name: row[name] for name in supplied} == supplied
    # The trades kind's other columns (the trade ID, the received time, and the columns other
    # asset classes supply) are null: the last-trade endpoint does not carry them.
    assert {"Id", "ReceivedTimestamp"} <= set(row)
    assert all(row[name] is None for name in set(row) - set(supplied))


def test_trades_history_over_rest_bounds_the_utc_day(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    serve(fake_rest, "/v3/trades/X:BTCUSD", load_fixture(OWNER, "trades.json"))
    q = rest_session.crypto.trades("BTC-USD")
    out = rows(q.history("2021-07-01", "2021-07-01", source=dm.HistorySource.REST))
    midnight = int(datetime(2021, 7, 1, tzinfo=UTC).timestamp()) * NS
    assert sent(fake_rest) == [
        (
            "/v3/trades/X:BTCUSD",
            {
                "timestamp.gte": [str(midnight)],
                "timestamp.lt": [str(midnight + 86_400 * NS)],
                "limit": ["50000"],
            },
        )
    ]
    assert [
        (r["Symbol"], r["Timestamp"], r["Id"], r["Price"], r["ReceivedTimestamp"]) for r in out
    ] == [
        ("X:BTCUSD", 1625097600103000000, "191450340", 35060.0, 1625097600125000000),
        ("X:BTCUSD", 1625097600368000000, "191450341", 35059.99, 1625097600390000000),
    ]


def test_daily_bars_fetch_reads_the_previous_day(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    serve(fake_rest, "/v2/aggs/ticker/X:BTCUSD/prev", load_fixture(OWNER, "prev.json"))
    out = rows(rest_session.crypto.bars("BTC-USD", dm.Timespan.DAY).fetch())
    assert sent(fake_rest) == [("/v2/aggs/ticker/X:BTCUSD/prev", {})]
    assert [(r["Symbol"], r["Timestamp"], r["Close"], r["Vwap"]) for r in out] == [
        ("X:BTCUSD", 1605416400000 * MS, 16035.9, 15954.2111)
    ]


def test_bars_history_over_rest_fills_the_range_in_utc_milliseconds(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    start = int(datetime(2020, 6, 1, tzinfo=UTC).timestamp()) * 1000
    end = start + 2 * 86_400_000 - 1
    path = f"/v2/aggs/ticker/X:BTCUSD/range/1/day/{start}/{end}"
    serve(fake_rest, path, load_fixture(OWNER, "range.json"))
    q = rest_session.crypto.bars("BTC-USD", dm.Timespan.DAY)
    out = rows(q.history("2020-06-01", "2020-06-02", source=dm.HistorySource.REST))
    assert sent(fake_rest) == [(path, {"limit": ["50000"]})]
    assert [(r["Symbol"], r["Timestamp"], r["Open"], r["Transactions"]) for r in out] == [
        ("X:BTCUSD", 1590984000000 * MS, 9557.9, 1),
        ("X:BTCUSD", 1591070400000 * MS, 10096.87, 1),
    ]


def test_every_pairs_daily_bars_read_the_grouped_day(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    path = "/v2/aggs/grouped/locale/global/market/crypto/2020-02-02"
    serve(fake_rest, path, load_fixture(OWNER, "grouped.json"))
    q = rest_session.crypto.bars(dm.ALL, dm.Timespan.DAY)
    out = rows(q.history("2020-02-02", "2020-02-02", source=dm.HistorySource.REST))
    assert sent(fake_rest) == [(path, {})]
    assert [(r["Symbol"], r["Timestamp"], r["Transactions"]) for r in out] == [
        ("X:ARDRUSD", 1580676480000 * MS, 18388),
        ("X:NGCUSD", 1580674080000 * MS, 18),
    ]


def test_snapshots_send_the_pairs_or_nothing_for_all(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    path = "/v2/snapshot/locale/global/markets/crypto/tickers"
    serve(fake_rest, path, load_fixture(OWNER, "snapshots.json"))
    out = rows(rest_session.crypto.snapshots(["FSN-USD", "BTC-USD"]).fetch())
    rows(rest_session.crypto.snapshots(dm.ALL).fetch())
    assert sent(fake_rest) == [(path, {"tickers": ["X:BTCUSD,X:FSNUSD"]}), (path, {})]
    [row] = out
    assert (row["Symbol"], row["LastTradeId"], row["Updated"]) == (
        "X:FSNUSD",
        "413131",
        1605330008999 * MS,
    )
    assert (row["LastMinuteWindowStart"], row["PrevDayVwap"]) == (1684427880000 * MS, 0.4076)


def test_movers_read_their_direction(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    path = "/v2/snapshot/locale/global/markets/crypto/losers"
    serve(fake_rest, path, load_fixture(OWNER, "movers.json"))
    out = rows(rest_session.crypto.movers(dm.Direction.LOSERS).fetch())
    assert sent(fake_rest) == [(path, {})]
    assert [(r["Symbol"], r["LastTradeId"], r["TodaysChangePerc"]) for r in out] == [
        ("X:DRNUSD", "517478762", 221.858864)
    ]


def test_daily_open_close_reads_one_pair_and_day_a_request(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    path = "/v1/open-close/crypto/BTC/USD/2020-10-09"
    serve(fake_rest, path, load_fixture(OWNER, "open_close.json"))
    q = rest_session.crypto.daily_open_close("BTC-USD", "2020-10-09")
    [row] = rows(q.fetch())
    assert sent(fake_rest) == [(path, {})]
    # The body's `BTC-USD` is canonical in `Symbol`; the trades stay JSON text.
    assert (row["Symbol"], row["Day"], row["Open"], row["Close"], row["IsUtc"]) == (
        "X:BTCUSD",
        date(2020, 10, 9),
        10932.44,
        11050.64,
        True,
    )
    assert json.loads(row["OpenTrades"])[0]["i"] == "511235746"


# --- End to end: WebSocket -----------------------------------------------------------------------

TIMEOUT = 15.0


class WsSession:
    """A session whose WebSocket connections reach the fake Massive server."""

    def __init__(self, server: FakeMassiveServer) -> None:
        self.server = server
        self.session = dm.MassiveSession(dm.MassiveConfig(api_key="test-key"))
        core = self.session._core
        connections = ws.WsConnections(
            core,
            resolve_host=lambda *_: HostDecision("socket.test", Recency.REALTIME, "a test"),
            record_verdict=lambda *_: None,
            client_factory=self.client,
        )
        cast("Any", core._ports)._streams = connections
        core.on_close(connections.close)

    def client(
        self,
        url: str,
        *,
        sources: Sequence[Source],
        on_open: Callable[[int], None],
        on_status: Callable[[int, Status], None],
        on_data: Callable[[int, str, list[dict[str, object]], list[Reject], DriftReport], None],
        on_state: Callable[[WsState, float | WireError | None], None],
    ) -> _massive.WsClient:
        return _massive.WsClient(
            self.server.url("/" + url.split("/", 3)[3]),
            api_key="test-key",
            sources=sources,
            on_open=on_open,
            on_status=on_status,
            on_data=on_data,
            on_state=on_state,
            reconnect_backoff=0.05,
            max_reconnect_backoff=0.1,
        )

    def subscribed(self, topic: str) -> None:
        def named(server: FakeMassiveServer) -> bool:
            if not server.connections or server.connections[-1].auth is None:
                return False
            frames = [json.loads(f) for f in server.connections[-1].frames]
            return any(
                f["action"] == "subscribe" and topic in f["params"].split(",") for f in frames
            )

        self.server.wait_for(named, TIMEOUT)


@pytest.fixture
def ws_session(fake_ws_server: FakeMassiveServer) -> Iterator[WsSession]:
    harness = WsSession(fake_ws_server)
    yield harness
    harness.session.close()


def landed(table: Table) -> tuple[Callable[[], list[dict[str, Any]]], Callable[[], None]]:
    # Collect the rows each update-graph cycle adds to a blink table, before it clears them;
    # the listener appends on the update-graph thread, the test reads a copy under the lock.
    got: list[dict[str, Any]] = []
    lock = threading.Lock()

    def on_update(update: TableUpdate, is_replay: bool) -> None:
        added = update.added()
        names = list(added)
        with lock:
            got.extend(
                dict(zip(names, values, strict=True))
                for values in zip(*(added[n] for n in names), strict=True)
            )

    scope = new_scope()
    with scope.open():
        handle = listen(table, on_update)

    def seen() -> list[dict[str, Any]]:
        with lock:
            return list(got)

    def stop() -> None:
        handle.stop()
        scope.release()

    return seen, stop


@pytest.mark.parametrize(
    ("dataset", "topic", "expected"),
    [
        ("trades", "XT.BTC-USD", {"Symbol": "X:BTCUSD", "Price": 33021.9, "Id": "14272084"}),
        ("quotes", "XQ.BTC-USD", {"Symbol": "X:BTCUSD", "BidPrice": 33052.79, "AskSize": 0.601}),
        ("bars", "XA.BCD-USD", {"Symbol": "X:BCDUSD", "Vwap": 0.7756, "AverageTradeSize": 73}),
        ("bars_second", "XAS.BCD-USD", {"Symbol": "X:BCDUSD", "Close": 0.784}),
        ("fmv", "FMV.BTC-USD", {"Symbol": "X:BTCUSD", "Fmv": 33021.9}),
    ],
    ids=["XT", "XQ", "XA", "XAS", "FMV"],
)
def test_a_documented_frame_lands_in_blink(
    *,
    ws_session: WsSession,
    load_fixture: Fixture,
    dataset: str,
    topic: str,
    expected: dict[str, object],
) -> None:
    channel = topic.split(".", 1)[0]
    symbol = topic.split(".", 1)[1]
    namespace = ws_session.session.crypto
    if dataset == "bars_second":
        query: StreamQuery = namespace.bars(symbol, dm.Timespan.SECOND)
    else:
        query = getattr(namespace, dataset)(symbol)
    blink = query.blink()
    seen, stop = landed(blink)
    ws_session.subscribed(topic)
    frames = json.loads(load_fixture(OWNER, "ws_frames.json"))[channel]
    ws_session.server.send(json.dumps(frames))
    wait_for(lambda: len(seen()) == 1)
    stop()
    [row] = seen()
    assert {name: row[name] for name in expected} == expected
    assert row["Timestamp"] is not None
    ws_session.session.release(blink)


def wait_for(condition: Callable[[], bool]) -> None:
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(int(TIMEOUT / 0.02)):
        if condition():
            return
        pause.wait(0.02)
    assert condition()


# --- End to end: flat files ----------------------------------------------------------------------


@pytest.fixture
def s3_session(fake_s3: FakeS3) -> Iterator[dm.MassiveSession]:
    # Test credentials for the fake S3 server, which accepts any.
    settings: dict[str, Any] = {
        "api_key": "test-key",
        "s3_access_key_id": "AKIDFLATFILES",
        "s3_secret_access_key": "flat-file-secret",
    }
    m = dm.MassiveSession(dm.MassiveConfig(**settings))
    yield m
    m.close()


@pytest.mark.parametrize(
    ("make", "prefix", "expected"),
    [
        (
            lambda c: c.trades(["BTC-USD", "ETH-USD"]),
            "global_crypto/trades_v1",
            [
                ("X:BTCUSD", 1679961600212544000, 27142.85),
                ("X:BTCUSD", 1679961600591766000, 27140.12),
                ("X:ETHUSD", 1679961600651891712, 1772.6),
            ],
        ),
        (
            lambda c: c.bars("BTC-USD"),
            "global_crypto/minute_aggs_v1",
            [
                ("X:BTCUSD", 1679961600000000000, 27139.64),
                ("X:BTCUSD", 1679961660000000000, 27169.39),
            ],
        ),
        (
            lambda c: c.bars(dm.ALL, dm.Timespan.DAY),
            "global_crypto/day_aggs_v1",
            [
                ("X:LINKUSD", 1680047999999000000, 6.947),
                ("X:ADAUSD", 1680047999999000000, 0.36816),
            ],
        ),
    ],
    ids=["trades", "minute_bars", "day_bars"],
)
def test_one_flat_file_block_round_trips(
    *,
    s3_session: dm.MassiveSession,
    fake_s3: FakeS3,
    load_fixture: Fixture,
    make_csv_gz: Callable[[str], bytes],
    make: Callable[[Any], HistoryQuery],
    prefix: str,
    expected: list[tuple[str, int, float]],
) -> None:
    name = prefix.replace("/", "_") + "_2023-03-28.csv"
    fake_s3.put(
        f"{prefix}/2023/03/2023-03-28.csv.gz", make_csv_gz(load_fixture(OWNER, name).decode())
    )
    q = make(s3_session.crypto)
    table = q.history("2023-03-28", "2023-03-28", source=dm.HistorySource.FLAT_FILES)
    price = "Price" if "trades" in prefix else "Close"
    assert [(r["Symbol"], r["Timestamp"], r[price]) for r in rows(table)] == expected
