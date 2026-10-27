"""Tests for the `m.crypto` namespace: its dataset specs, schemas, factories, and fixtures.

The specs are checked against the generated sources through a catalog of the crypto datasets
alone, so the expected schemas do not depend on which other namespaces have declared members
of the shared kinds. The fixtures under `tests/data/namespaces/crypto/` are shaped from the
vendor's documented samples; here they are checked against the generated sources they stand
for.
"""

import csv
import io
import json
from collections.abc import Callable, Iterator
from datetime import date, datetime, timedelta, timezone
from typing import Any, cast

import pytest
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.schema import required_fields
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import ChannelFamily
from deephaven_massive._massive import RestPath, Transport, source, to_batch
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
    assert (request.timespan, request.multiplier, request.adjusted) == (timespan, multiplier, True)


def test_bars_default_to_streamed_minutes(massive_session: dm.MassiveSession) -> None:
    q = massive_session.crypto.bars(dm.ALL, adjusted=False)
    assert type(q) is StreamHistoryQuery
    request = request_of(q)
    assert (request.symbols, request.timespan, request.adjusted) == (
        dm.ALL,
        dm.Timespan.MINUTE,
        False,
    )


def test_bar_arguments_are_checked(massive_session: dm.MassiveSession) -> None:
    bars = massive_session.crypto.bars
    with pytest.raises(ValueError, match=r"Timespan\.SESSION exists only for futures"):
        bars("BTC-USD", dm.Timespan.SESSION)
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan"):
        bars("BTC-USD", "minute")  # type: ignore[call-overload]  # a plain string is refused
    with pytest.raises(ValueError, match="multiplier"):
        bars("BTC-USD", dm.Timespan.HOUR, multiplier=0)
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        bars("BTC-USD", adjusted="yes")  # type: ignore[call-overload]  # not a bool


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
    assert request.adjusted is True
    same = massive_session.crypto.daily_open_close("BTC-USD", date(2026, 1, 2), adjusted=False)
    assert request_of(same).params == (("date", date(2026, 1, 2)),)
    assert request_of(same).adjusted is False
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        massive_session.crypto.daily_open_close(
            "BTC-USD",
            "2026-01-02",
            adjusted=1,  # type: ignore[arg-type]  # not a bool
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


@pytest.mark.parametrize(
    ("make", "reason"),
    [
        (
            lambda c: c.bars("BTC-USD", dm.Timespan.HOUR, adjusted=False),
            r"crypto\.bars flat files hold only 1-minute, 1-day bars",
        ),
        (lambda c: c.bars("BTC-USD"), "flat-file bars are unadjusted; pass adjusted=False"),
    ],
    ids=["hourly", "adjusted"],
)
def test_flat_file_history_states_its_constraints(
    massive_session: dm.MassiveSession, make: Callable[[Any], HistoryQuery], reason: str
) -> None:
    q = make(massive_session.crypto)
    with pytest.raises(
        ValueError, match=r"crypto\.bars cannot be read from flat files: .*" + reason
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
