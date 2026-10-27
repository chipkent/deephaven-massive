"""Tests for flat-file history: source choice, partition dates, and streaming S3 reads."""

import gc
import gzip
import logging
import re
import threading
import tracemalloc
from array import array
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, ClassVar, Protocol, cast
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import new_table
from deephaven.column import string_col
from deephaven.execution_context import get_exec_ctx
from deephaven.table import Table
from deephaven.update_graph import shared_lock

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.dates import CHICAGO, FuturesSessions, TimeRange, bounds
from deephaven_massive._datasets.schema import (
    BARS,
    INDEX_VALUES,
    QUOTES,
    TRADES,
    Column,
    ColumnType,
    Schema,
)
from deephaven_massive._datasets.timestamps import TimeUnit
from deephaven_massive._enums import AssetClass, FuturesVenue, HistorySource, Timespan, Transport
from deephaven_massive._ingest import flatfiles
from deephaven_massive._namespaces.base import build_query
from deephaven_massive._query import HistoryQuery, Request
from deephaven_massive._types import override

TIMEOUT = 30.0
NEW_YORK = ZoneInfo("America/New_York")
FF = Transport.FLAT_FILES
NOW = datetime(2025, 3, 10, 16, tzinfo=UTC)
"""A Monday afternoon, when every file of the first week of March 2025 is published."""
LOGGER = "deephaven_massive"
"""The package logger: the AUTO line comes from the query, the S3 line from the transport."""


class FakeS3(Protocol):
    # What the tests use of the `fake_s3` fixture (tests/conftest.py, tests/_fakes/s3.py).
    requests: list[tuple[str, str]]
    clients: list[dict[str, object]]
    closes: int
    closed_bodies: list[str]
    request_id: str
    before_answer: Callable[[str], object] | None

    def put(self, key: str, data: bytes) -> None: ...
    def put_dropped(self, key: str, data: bytes) -> None: ...
    def refuse(self, prefix: str, *, status: int, code: str, message: str) -> None: ...
    def fail(self, key: str, failure: str) -> None: ...
    def keys(self) -> list[str]: ...


def _ns(*fields: str) -> dict[tuple[Transport, str], TimeUnit]:
    return {(FF, name): TimeUnit.NANOSECONDS for name in fields}


STOCK_TRADES = DatasetSpec(
    name="stocks.trades",
    query_class=HistoryQuery,
    asset_class=AssetClass.STOCKS,
    schema=TRADES.schema,
    key_maps={FF: TRADES.key_maps[FF]},
    history_method="list_trades",
    flat_file_prefixes={None: "us_stocks_sip/trades_v1"},
    time_units=_ns("sip_timestamp", "participant_timestamp", "trf_timestamp"),
)
STOCK_QUOTES = DatasetSpec(
    name="stocks.quotes",
    query_class=HistoryQuery,
    asset_class=AssetClass.STOCKS,
    schema=QUOTES.schema,
    key_maps={FF: QUOTES.key_maps[FF]},
    history_method="list_quotes",
    flat_file_prefixes={None: "us_stocks_sip/quotes_v1"},
    time_units=_ns("sip_timestamp", "participant_timestamp", "trf_timestamp"),
)
STOCK_BARS = DatasetSpec(
    name="stocks.bars",
    query_class=HistoryQuery,
    asset_class=AssetClass.STOCKS,
    schema=BARS.schema,
    is_bars=True,
    key_maps={FF: BARS.key_maps[FF]},
    history_method="list_aggs",
    grouped_daily_method="get_grouped_daily_aggs",
    flat_file_prefixes={
        Timespan.MINUTE: "us_stocks_sip/minute_aggs_v1",
        Timespan.DAY: "us_stocks_sip/day_aggs_v1",
    },
    time_units=_ns("window_start"),
)
INDEX_VALUES_SPEC = DatasetSpec(
    name="indices.values",
    query_class=HistoryQuery,
    asset_class=AssetClass.INDICES,
    schema=INDEX_VALUES.schema,
    key_maps={FF: INDEX_VALUES.key_maps[FF]},
    flat_file_prefixes={None: "us_indices/values_v1"},
    time_units=_ns("timestamp"),
)
FUTURES_TRADES = DatasetSpec(
    name="futures.trades",
    query_class=HistoryQuery,
    asset_class=AssetClass.FUTURES,
    schema=TRADES.schema,
    key_maps={
        FF: {
            "ticker": "Symbol",
            "timestamp": "Timestamp",
            "price": "Price",
            "size": "Size",
            "exchange": "Exchange",
            "correction": "Correction",
            "sequence_number": "SequenceNumber",
            "report_sequence": "ReportSequence",
            "session_end_date": "SessionEndDate",
        }
    },
    history_method="list_futures_trades",
    flat_file_prefixes={None: "us_futures_{venue}/trades_v1"},
    time_units=_ns("timestamp"),
)
CRYPTO_TRADES = DatasetSpec(
    name="crypto.trades",
    query_class=HistoryQuery,
    asset_class=AssetClass.CRYPTO,
    schema=TRADES.schema,
    key_maps={
        FF: {
            "ticker": "Symbol",
            "participant_timestamp": "Timestamp",
            "price": "Price",
            "size": "Size",
            "exchange": "Exchange",
            "id": "Id",
            "conditions": "Conditions",
        }
    },
    history_method="list_trades",
    flat_file_prefixes={None: "global_crypto/trades_v1"},
    time_units=_ns("participant_timestamp"),
)
YIELDS = DatasetSpec(
    name="economy.yields",
    query_class=HistoryQuery,
    schema=Schema("economy.yields", (Column("Date", ColumnType.LOCAL_DATE),)),
    poll_keys=("Date",),
    history_method="list_treasury_yields",
)

TRADES_KEY = "us_stocks_sip/trades_v1/2025/03/2025-03-{:02d}.csv.gz"
TRADES_0303 = TRADES_KEY.format(3)
TRADES_0304 = TRADES_KEY.format(4)
TRADES_0305 = TRADES_KEY.format(5)
FUTURES_KEY = "us_futures_{}/trades_v1/{:%Y/%m/%Y-%m-%d}.csv.gz"
FUTURES_0304 = "us_futures_{}/trades_v1/2025/03/2025-03-04.csv.gz"
VENUES = ("cme", "cbot", "nymex", "comex")

TRADE_HEADER = (
    "conditions,correction,exchange,id,participant_timestamp,price,sequence_number,"
    "sip_timestamp,size,tape,ticker,trf_id,trf_timestamp\n"
)
FUTURES_HEADER = (
    "correction,price,size,exchange,sequence_number,report_sequence,session_end_date,"
    "ticker,timestamp\n"
)


def trade(n: int, nanos: int, ticker: str = "AAPL", *, participant: int | None = None) -> str:
    # One stocks trades row with Massive's documented columns.
    seen = nanos if participant is None else participant
    return f"12,,4,{n},{seen},1.5,{n},{nanos},10,1,{ticker},,\n"


def epoch_ns(moment: datetime) -> int:
    return int(moment.timestamp()) * 10**9


def config(**overrides: object) -> dm.MassiveConfig:
    fields: dict[str, object] = {
        "api_key": "test-key",
        "s3_access_key_id": "AKIDFLATFILES",
        "s3_secret_access_key": "flat-file-secret",
    }
    fields.update(overrides)
    return dm.MassiveConfig(**cast("dict[str, Any]", fields))


NO_S3 = config(s3_access_key_id=None, s3_secret_access_key=None)


def request(symbols: object = ("AAPL",), **fields: object) -> Request:
    return Request(symbols=cast("Any", symbols), **cast("dict[str, Any]", fields))


def rows(table: Table) -> list[dict[str, Any]]:
    # Instants become epoch nanoseconds, so assertions keep every digit.
    data = dh_arrow.to_arrow(table)
    columns = [c.cast(pa.int64()) if pa.types.is_timestamp(c.type) else c for c in data.columns]
    out: list[dict[str, Any]] = pa.Table.from_arrays(columns, names=data.column_names).to_pylist()
    return out


def pick(table: Table, *columns: str) -> list[tuple[Any, ...]]:
    return [tuple(row[c] for c in columns) for row in rows(table)]


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Callable[[datetime], None]:
    def set_now(moment: datetime) -> None:
        monkeypatch.setattr(history_source, "_now", lambda: moment)

    set_now(NOW)
    return set_now


@pytest.fixture
def session(fake_s3: FakeS3, clock: Callable[[datetime], None]) -> Iterator[dm.MassiveSession]:
    m = dm.MassiveSession(config())
    yield m
    m.close()


@pytest.fixture
def put(
    *,
    fake_s3: FakeS3,
    load_fixture: Callable[[str, str], bytes],
    make_csv_gz: Callable[[str], bytes],
) -> Callable[[str, str], None]:
    def store(key: str, fixture: str) -> None:
        fake_s3.put(key, make_csv_gz(load_fixture("ingest/flatfiles", fixture).decode()))

    return store


@pytest.fixture
def many_blocks(
    *, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes], monkeypatch: pytest.MonkeyPatch
) -> Callable[[str], int]:
    # Stores a trades file of many small blocks under a key and returns its block count.
    block = 4096
    monkeypatch.setattr(flatfiles, "BLOCK_SIZE", block)

    def store(key: str) -> int:
        text = TRADE_HEADER + "".join(trade(n, 1741012200000000000 + n) for n in range(4000))
        fake_s3.put(key, make_csv_gz(text))
        return len(text) // block

    return store


class Applied:
    # The blocks each file's reader has transformed, and a hook run after each one.
    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self.hook: Callable[[str], object] | None = None


@pytest.fixture
def applied(monkeypatch: pytest.MonkeyPatch) -> Applied:
    record = Applied()
    batches = _massive.FlatFile.batches

    def counted(file: _massive.FlatFile, plan: _massive.ReadPlan) -> Iterator[pa.RecordBatch]:
        for batch in batches(file, plan):
            record.counts[file.key] = record.counts.get(file.key, 0) + 1
            if record.hook is not None:
                record.hook(file.key)
            yield batch

    monkeypatch.setattr(_massive.FlatFile, "batches", counted)
    return record


def flat_file_client(m: dm.MassiveSession) -> _massive.FlatFileClient:
    # The session's flat-file client, which its first flat-file read created.
    client = m._core._flat_files
    assert client is not None
    return client


def query(m: dm.MassiveSession, spec: DatasetSpec, **kwargs: object) -> HistoryQuery:
    q = build_query(m._core, spec, **cast("dict[str, Any]", kwargs))
    assert isinstance(q, HistoryQuery)
    return q


class ShutdownSignal(ThreadPoolExecutor):
    # The reader pool, recording its size and announcing its shutdown, which happens only after
    # the call's cancel event is set. The `pool` fixture installs a fresh subclass per test.
    sizes: ClassVar[list[int]]
    shut: ClassVar[threading.Event]

    def __init__(self, max_workers: int, thread_name_prefix: str = "") -> None:
        type(self).sizes.append(max_workers)
        super().__init__(max_workers, thread_name_prefix)

    @override
    def shutdown(self, *args: bool, **kwargs: bool) -> None:
        type(self).shut.set()
        super().shutdown(*args, **kwargs)


@pytest.fixture
def pool(monkeypatch: pytest.MonkeyPatch) -> type[ShutdownSignal]:
    class Recorded(ShutdownSignal):
        sizes: ClassVar[list[int]] = []
        shut: ClassVar[threading.Event] = threading.Event()

    monkeypatch.setattr(flatfiles, "ThreadPoolExecutor", Recorded)
    return Recorded


# --- The verb ----------------------------------------------------------------------------------


def test_history_is_guarded_like_every_verb(*, session: dm.MassiveSession, fake_s3: FakeS3) -> None:
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with (
        shared_lock(session._core.exec_ctx.update_graph),
        pytest.raises(ValueError, match="update-graph lock"),
    ):
        q.history("2025-03-03", "2025-03-03")
    assert fake_s3.requests == []


def test_history_after_close_is_a_closed_session_error(
    *, fake_s3: FakeS3, clock: Callable[[datetime], None]
) -> None:
    # The closed check comes before the source choice, which would fail with a ValueError.
    m = dm.MassiveSession(NO_S3)
    q = query(m, INDEX_VALUES_SPEC, symbols=["I:SPX"])
    m.close()
    with pytest.raises(dm.MassiveError, match="is closed"):
        q.history("2025-03-03", "2025-03-03")


def test_auto_without_any_source_raises_before_any_request(
    *, fake_s3: FakeS3, clock: Callable[[datetime], None]
) -> None:
    m = dm.MassiveSession(NO_S3)
    try:
        q = query(m, INDEX_VALUES_SPEC, symbols=["I:SPX"])
        with pytest.raises(
            ValueError,
            match=r"indices\.values history has no possible source\. Flat files: no S3 credentials",
        ):
            q.history("2025-03-03", "2025-03-03")
    finally:
        m.close()
    assert fake_s3.clients == []
    assert fake_s3.requests == []


# --- Reading files ---------------------------------------------------------------------------


def test_trades_are_renamed_typed_and_converted(
    *, session: dm.MassiveSession, put: Callable[[str, str], None], fake_s3: FakeS3
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    t = query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    assert {c.name: c.data_type for c in t.columns} == TRADES.schema.col_defs()
    first, msft, second, spy = rows(t)
    assert first == {
        "Symbol": "AAPL",
        "Timestamp": 1741012200000000123,
        "Price": 227.48,
        "Size": 100.0,
        "DecimalSize": None,
        "Exchange": 4,
        "Id": "1",
        "Conditions": [12, 37],
        "Tape": 3,
        "SequenceNumber": 100,
        "Correction": None,
        "ParticipantTimestamp": 1741012199999999000,
        "TrfId": None,
        "TrfTimestamp": None,
        "ReceivedTimestamp": None,
        "SessionEndDate": None,
        "Channel": None,
        "ReportSequence": None,
    }
    assert (msft["Symbol"], msft["Conditions"]) == ("MSFT", None)
    assert msft["TrfTimestamp"] is None  # the file says 0: no TRF
    assert (second["Size"], second["Correction"], second["Conditions"]) == (0.5, 1, [14])
    assert (second["TrfId"], second["TrfTimestamp"]) == (201, 1741013999999999000)
    assert spy["Symbol"] == "SPY"
    assert fake_s3.requests == [("flatfiles", TRADES_0303)]


def test_instant_columns_convert_by_their_declared_unit(
    *, session: dm.MassiveSession, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    units = {**STOCK_TRADES.time_units, (FF, "participant_timestamp"): TimeUnit.MILLISECONDS}
    spec = replace(STOCK_TRADES, time_units=units)
    row = trade(1, 1741012200000000123, participant=1741012199999)
    fake_s3.put(TRADES_0303, make_csv_gz(TRADE_HEADER + row))
    t = query(session, spec, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    assert pick(t, "Timestamp", "ParticipantTimestamp") == [
        (1741012200000000123, 1741012199999000000)
    ]


def test_an_unverified_unit_out_of_range_is_a_massive_error(
    *, session: dm.MassiveSession, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    spec = replace(STOCK_TRADES, unverified_time_units=frozenset({(FF, "participant_timestamp")}))
    row = trade(1, 1741012200000000000, participant=1741012200)  # seconds, not nanoseconds
    fake_s3.put(TRADES_0303, make_csv_gz(TRADE_HEADER + row))
    with pytest.raises(dm.MassiveError, match="timestamp unit mismatch"):
        query(session, spec, symbols=dm.ALL).history("2025-03-03", "2025-03-03")


def test_epoch_zero_is_null_except_in_timestamp(
    *, session: dm.MassiveSession, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    # Stocks: a 0 participant or TRF timestamp is absent; a 0 price and size are data.
    row = "12,,4,1,0,0,1,1741012200000000000,0,1,AAPL,,0\n"
    fake_s3.put(TRADES_0303, make_csv_gz(TRADE_HEADER + row))
    t = query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    assert pick(t, "Timestamp", "ParticipantTimestamp", "TrfTimestamp", "Price", "Size") == [
        (1741012200000000000, None, None, 0.0, 0.0)
    ]
    # Futures sessions are not filtered on Timestamp, so a 0 there shows that it is kept.
    fake_s3.put(
        FUTURES_0304.format("cme"),
        make_csv_gz(FUTURES_HEADER + ",0,0,1,1,1,2025-03-04,ESH5,0\n"),
    )
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venue=FuturesVenue.CME)
    assert pick(q.history("2025-03-04", "2025-03-04"), "Timestamp", "Price") == [(0, 0.0)]


def test_quote_lists_are_parsed(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    put(
        "us_stocks_sip/quotes_v1/2025/03/2025-03-03.csv.gz",
        "us_stocks_sip_quotes_v1_2025-03-03.csv",
    )
    t = query(session, STOCK_QUOTES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    assert pick(t, "Symbol", "Conditions", "Indicators", "BidSize", "AskExchange") == [
        ("AAPL", [1], [604, 607], 300.0, 12),
        ("MSFT", None, None, 100.0, 19),
    ]


def test_symbol_pushdown_keeps_only_the_requested_symbols(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=["aapl"])
    t = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert pick(t, "Symbol", "Id") == [("AAPL", "1"), ("AAPL", "3")]


@pytest.mark.parametrize("ticker", ["NA", "NULL"])
def test_tickers_that_look_like_null_markers_are_symbols(
    *, session: dm.MassiveSession, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes], ticker: str
) -> None:
    # Only an empty field is null: NA and NULL are valid tickers, which Arrow's default null
    # markers would read as null, while the empty correction and trf fields stay null.
    row = trade(1, 1741012200000000000, ticker)
    other = trade(2, 1741012200000000000, "SPY")
    fake_s3.put(TRADES_0303, make_csv_gz(TRADE_HEADER + row + other))
    q = query(session, STOCK_TRADES, symbols=[ticker])
    t = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert pick(t, "Symbol", "Correction", "TrfId") == [(ticker, None, None)]
    every = query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    assert pick(every, "Symbol") == [(ticker,), ("SPY",)]


def test_no_matching_rows_gives_an_empty_table_with_every_column(
    *, session: dm.MassiveSession, put: Callable[[str, str], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    converted: list[int] = []
    convert = dh_arrow.to_table

    def counted(data: pa.Table) -> Table:
        converted.append(data.num_rows)
        return convert(data)

    monkeypatch.setattr(flatfiles, "to_table", counted)
    q = query(session, STOCK_TRADES, symbols=["ZZZZ"])
    t = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert t.size == 0
    assert {c.name: c.data_type for c in t.columns} == TRADES.schema.col_defs()
    assert converted == [0]  # only the empty schema table: no table of an empty block


def test_instant_bounds_are_half_open(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    t = q.history(datetime(2025, 3, 3, 14, 30, tzinfo=UTC), datetime(2025, 3, 3, 15, tzinfo=UTC))
    # The trade at exactly 15:00 UTC is the excluded end.
    assert pick(t, "Id") == [("1",), ("2",)]
    t = q.history(datetime(2025, 3, 3, 15, tzinfo=UTC), datetime(2025, 3, 3, 21, tzinfo=UTC))
    # ... and the included start; the trade at exactly 21:00 UTC is excluded.
    assert pick(t, "Id") == [("3",)]


def test_dst_fall_back_hour_and_the_25_hour_day(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    make_csv_gz: Callable[[str], bytes],
) -> None:
    clock(datetime(2025, 11, 10, tzinfo=UTC))
    edt = epoch_ns(datetime(2025, 11, 2, 5, 30, tzinfo=UTC))  # 01:30 EDT
    est = epoch_ns(datetime(2025, 11, 2, 6, 30, tzinfo=UTC))  # 01:30 EST, the repeated hour
    late = epoch_ns(datetime(2025, 11, 3, 4, 30, tzinfo=UTC))  # 23:30 EST on November 2
    key = "us_stocks_sip/trades_v1/2025/11/2025-11-02.csv.gz"
    fake_s3.put(key, make_csv_gz(TRADE_HEADER + trade(1, edt) + trade(2, est) + trade(3, late)))
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    second = q.history(
        datetime(2025, 11, 2, 1, 30, fold=1, tzinfo=NEW_YORK),
        datetime(2025, 11, 2, 2, tzinfo=NEW_YORK),
    )
    assert pick(second, "Id") == [("2",)]
    assert pick(q.history("2025-11-02", "2025-11-02"), "Id") == [("1",), ("2",), ("3",)]
    assert fake_s3.keys() == [key, key]  # never the November 3 file


def test_spring_forward_23_hour_day(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    make_csv_gz: Callable[[str], bytes],
) -> None:
    clock(datetime(2025, 3, 20, tzinfo=UTC))
    before = epoch_ns(datetime(2025, 3, 9, 6, 59, tzinfo=UTC))  # 01:59 EST
    after = epoch_ns(datetime(2025, 3, 9, 7, tzinfo=UTC))  # 03:00 EDT
    late = epoch_ns(datetime(2025, 3, 10, 3, 59, tzinfo=UTC))  # 23:59 EDT on March 9
    key = "us_stocks_sip/trades_v1/2025/03/2025-03-09.csv.gz"
    data = TRADE_HEADER + trade(1, before) + trade(2, after) + trade(3, late)
    fake_s3.put(key, make_csv_gz(data))
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    assert pick(q.history("2025-03-09", "2025-03-09"), "Id") == [("1",), ("2",), ("3",)]
    assert fake_s3.keys() == [key]


def test_unadjusted_day_bars_of_every_symbol_read_flat_files(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    put(
        "us_stocks_sip/day_aggs_v1/2025/03/2025-03-03.csv.gz",
        "us_stocks_sip_day_aggs_v1_2025-03-03.csv",
    )
    q = query(
        session,
        STOCK_BARS,
        symbols=dm.ALL,
        timespan=Timespan.DAY,
        adjusted=False,
    )
    with caplog.at_level(logging.INFO, logger=LOGGER):
        t = q.history("2025-03-03", "2025-03-03")
    assert pick(t, "Symbol", "Timestamp", "Open", "Volume", "Transactions", "Vwap") == [
        ("AAPL", 1740978000000000000, 241.79, 47184000.0, 612403, None),
        ("MSFT", 1740978000000000000, 398.82, 26093000.0, 402122, None),
    ]
    assert caplog.messages == [
        "stocks.bars history: AUTO chose FLAT_FILES (every symbol)",
        "stocks.bars history: source=S3 files=1 skipped=none",
    ]


def test_each_file_becomes_one_table(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    put(TRADES_0305, "us_stocks_sip_trades_v1_2025-03-05.csv")
    converted: list[int] = []
    convert = dh_arrow.to_table

    def counted(data: pa.Table) -> Table:
        converted.append(data.num_rows)
        return convert(data)

    monkeypatch.setattr(flatfiles, "to_table", counted)
    monkeypatch.setattr(flatfiles, "BLOCK_SIZE", 256)  # several blocks per file
    t = query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-05")
    assert t.size == 6
    # The empty schema table, then one table per file, although each file has many blocks.
    assert sorted(converted) == [0, 2, 4]


def test_a_file_with_many_surviving_blocks_is_split_into_bounded_tables(
    *,
    session: dm.MassiveSession,
    many_blocks: Callable[[str], int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    blocks = many_blocks(TRADES_0303)
    converted: list[int] = []
    convert = dh_arrow.to_table

    def counted(data: pa.Table) -> Table:
        converted.append(data.num_rows)
        return convert(data)

    monkeypatch.setattr(flatfiles, "to_table", counted)
    monkeypatch.setattr(flatfiles, "_TABLE_BLOCKS", 2)
    t = query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    assert pick(t, "Id")[:3] == [("0",), ("1",), ("2",)]  # still in file order
    assert t.size == 4000
    assert len(converted) > 4  # the file was split, so no table holds the whole file
    assert len(converted) < blocks  # ... but into far fewer tables than blocks


def test_a_range_with_a_missing_weekday_warns_and_skips_it(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    fake_s3: FakeS3,
    caplog: pytest.LogCaptureFixture,
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    put(TRADES_0305, "us_stocks_sip_trades_v1_2025-03-05.csv")
    q = query(session, STOCK_TRADES, symbols=["AAPL", "MSFT"])
    with caplog.at_level(logging.INFO, logger=LOGGER):
        t = q.history("2025-03-03", "2025-03-05", source=HistorySource.FLAT_FILES)
    # Rows come in date order, although the three files are read concurrently.
    assert pick(t, "Id") == [("1",), ("2",), ("3",), ("9",), ("10",)]
    assert sorted(fake_s3.keys()) == [TRADES_0303, TRADES_0304, TRADES_0305]
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (
            logging.WARNING,
            f"stocks.trades: {TRADES_0304} does not exist; skipping 2025-03-04 (a market holiday?)",
        ),
        (logging.INFO, "stocks.trades history: source=S3 files=2 skipped=2025-03-04"),
    ]


def test_a_missing_friday_warns(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    thursday = epoch_ns(datetime(2025, 3, 6, 15, tzinfo=UTC))
    fake_s3.put(TRADES_KEY.format(6), make_csv_gz(TRADE_HEADER + trade(1, thursday)))
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with caplog.at_level(logging.INFO, logger=LOGGER):
        t = q.history("2025-03-06", "2025-03-08", source=HistorySource.FLAT_FILES)
    assert t.size == 1
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (
            logging.WARNING,
            (
                f"stocks.trades: {TRADES_KEY.format(7)} does not exist; skipping 2025-03-07 "
                "(a market holiday?)"
            ),
        ),
        (logging.INFO, "stocks.trades history: source=S3 files=1 skipped=2025-03-07,2025-03-08"),
    ]


def test_missing_weekend_files_are_skipped_silently(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with caplog.at_level(logging.INFO, logger=LOGGER):
        t = q.history("2025-03-01", "2025-03-03")
    assert t.size == 4
    assert caplog.messages == [
        (
            "stocks.trades history: AUTO chose FLAT_FILES (REST cannot serve it: stocks.trades "
            "history over REST needs explicit symbols)"
        ),
        "stocks.trades history: source=S3 files=1 skipped=2025-03-01,2025-03-02",
    ]


def test_a_missing_last_weekend_date_is_skipped_too(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    for day in (5, 6, 7):
        moment = epoch_ns(datetime(2025, 3, day, 15, tzinfo=UTC))
        fake_s3.put(TRADES_KEY.format(day), make_csv_gz(TRADE_HEADER + trade(day, moment)))
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with caplog.at_level(logging.INFO, logger=LOGGER):
        t = q.history("2025-03-05", "2025-03-08", source=HistorySource.FLAT_FILES)
    assert pick(t, "Id") == [("5",), ("6",), ("7",)]
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.INFO, "stocks.trades history: source=S3 files=3 skipped=2025-03-08")
    ]


def test_a_weekend_only_range_is_an_empty_table(
    *, session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with caplog.at_level(logging.INFO, logger=LOGGER):
        t = q.history("2025-03-01", "2025-03-02", source=HistorySource.FLAT_FILES)
    assert t.size == 0
    assert {c.name: c.data_type for c in t.columns} == TRADES.schema.col_defs()
    assert caplog.messages == [
        "stocks.trades history: source=S3 files=0 skipped=2025-03-01,2025-03-02"
    ]


def test_a_missing_last_date_within_a_day_of_publication_names_both_causes(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    clock: Callable[[datetime], None],
) -> None:
    # March 4's file counts as published from 11:30 New York time on March 5; for a day after
    # that, a missing file may still just be late.
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    clock(datetime(2025, 3, 6, 11, 29, tzinfo=NEW_YORK))
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(
        dm.MassiveError,
        match=re.escape(
            f"stocks.trades: {TRADES_0304} does not exist: 2025-03-04 is not yet published (flat "
            "files land ~11:00 ET next day), or was a market holiday"
        ),
    ):
        q.history("2025-03-03", "2025-03-04")


@pytest.mark.parametrize(
    "now",
    [
        pytest.param(datetime(2025, 3, 6, 11, 30, tzinfo=NEW_YORK), id="when-the-day-ends"),
        pytest.param(NOW, id="days-later"),
    ],
)
def test_a_missing_last_date_after_a_day_is_skipped_as_a_holiday(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    clock: Callable[[datetime], None],
    caplog: pytest.LogCaptureFixture,
    now: datetime,
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    clock(now)
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with caplog.at_level(logging.INFO, logger=LOGGER):
        t = q.history("2025-03-03", "2025-03-04", source=HistorySource.FLAT_FILES)
    assert pick(t, "Id") == [("1",), ("2",), ("3",), ("4",)]  # the rest of the range
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (
            logging.WARNING,
            f"stocks.trades: {TRADES_0304} does not exist; skipping 2025-03-04 (a market holiday?)",
        ),
        (logging.INFO, "stocks.trades history: source=S3 files=1 skipped=2025-03-04"),
    ]


def test_crypto_trades_every_day(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    clock: Callable[[datetime], None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    saturday = "global_crypto/trades_v1/2025/03/2025-03-01.csv.gz"
    put(saturday, "global_crypto_trades_v1_2025-03-01.csv")
    q = query(session, CRYPTO_TRADES, symbols=["BTC-USD"])
    t = q.history("2025-03-01", "2025-03-01", source=HistorySource.FLAT_FILES)
    assert pick(t, "Symbol", "Timestamp", "Size", "Id") == [
        ("X:BTCUSD", 1740830400000000000, 0.015, "a1")
    ]
    # A missing Sunday file is no weekend for crypto: within a day after its publication time
    # (11:30 New York on March 3) it may be late ...
    clock(datetime(2025, 3, 3, 12, tzinfo=NEW_YORK))
    with pytest.raises(dm.MassiveError, match=r"2025-03-02 is not yet published .*holiday"):
        q.history("2025-03-01", "2025-03-02", source=HistorySource.FLAT_FILES)
    clock(NOW)
    # ... or if that weekend day was a holiday, which warns like a missing weekday.
    put(
        "global_crypto/trades_v1/2025/03/2025-03-02.csv.gz",
        "global_crypto_trades_v1_2025-03-01.csv",
    )
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        q.history("2025-02-28", "2025-03-02", source=HistorySource.FLAT_FILES)
    assert caplog.messages == [
        (
            "crypto.trades: global_crypto/trades_v1/2025/02/2025-02-28.csv.gz does not exist; "
            "skipping 2025-02-28 (a market holiday?)"
        )
    ]


class RestRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple[DatasetSpec, Request, TimeRange | FuturesSessions]] = []

    def history(
        self, spec: DatasetSpec, request: Request, window: TimeRange | FuturesSessions
    ) -> Table:
        self.calls.append((spec, request, window))
        return new_table([string_col("Symbol", ["from REST"])])


def test_auto_delegates_few_symbols_to_rest(
    *,
    session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    fake_s3: FakeS3,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest = RestRecorder()
    monkeypatch.setattr(session._core, "rest_transport", lambda: rest)
    q = query(session, STOCK_TRADES, symbols=["msft", "aapl"])
    with caplog.at_level(logging.INFO, logger=LOGGER):
        t = q.history("2025-03-03", "2025-03-04")
    assert pick(t, "Symbol") == [("from REST",)]
    ((spec, req, window),) = rest.calls
    assert spec is STOCK_TRADES
    assert req.symbols == ("AAPL", "MSFT")
    assert (req.start, req.end) == (date(2025, 3, 3), date(2025, 3, 4))
    assert window == bounds(STOCK_TRADES, "2025-03-03", "2025-03-04")
    assert caplog.messages == [
        "stocks.trades history: AUTO chose REST (2 symbols < flat_file_symbol_threshold 50)"
    ]
    assert fake_s3.requests == []


# --- Futures ---------------------------------------------------------------------------------


def test_futures_read_every_venue_by_default_in_venue_order(
    *, session: dm.MassiveSession, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    for sequence, venue in enumerate(VENUES):
        row = f",1.0,1,1,1,{sequence},2025-03-04,ESH5,1741042800000000000\n"
        fake_s3.put(FUTURES_0304.format(venue), make_csv_gz(FUTURES_HEADER + row))
    q = query(session, FUTURES_TRADES, symbols=["ESH5"])
    t = q.history("2025-03-04", "2025-03-04", source=HistorySource.FLAT_FILES)
    assert sorted(fake_s3.keys()) == sorted(FUTURES_0304.format(v) for v in VENUES)
    # Venue order within a date: CME, CBOT, NYMEX, COMEX.
    assert pick(t, "ReportSequence", "SessionEndDate") == [
        (0, date(2025, 3, 4)),
        (1, date(2025, 3, 4)),
        (2, date(2025, 3, 4)),
        (3, date(2025, 3, 4)),
    ]


def test_futures_read_one_venue_when_asked(
    *, session: dm.MassiveSession, put: Callable[[str, str], None], fake_s3: FakeS3
) -> None:
    put(FUTURES_0304.format("cme"), "us_futures_trades_v1_2025-03-04.csv")
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venue=FuturesVenue.CME)
    t = q.history("2025-03-04", "2025-03-04")
    assert fake_s3.keys() == [FUTURES_0304.format("cme")]
    assert pick(t, "Symbol", "Timestamp", "SessionEndDate", "ReportSequence") == [
        ("ESH5", 1741042800000000000, date(2025, 3, 4), 31),
        ("ESH5", 1741100400000000000, date(2025, 3, 4), 32),
        ("NQH5", 1741100400000000000, date(2025, 3, 4), 33),
    ]


def test_futures_evening_instants_read_only_the_next_session(
    *, session: dm.MassiveSession, put: Callable[[str, str], None], fake_s3: FakeS3
) -> None:
    put(FUTURES_0304.format("cme"), "us_futures_trades_v1_2025-03-04.csv")
    q = query(session, FUTURES_TRADES, symbols=["ESH5"], venue=FuturesVenue.CME)
    # 17:00 Chicago on March 3 already trades in the session that ends on March 4.
    t = q.history(
        datetime(2025, 3, 3, 17, tzinfo=CHICAGO),
        datetime(2025, 3, 3, 18, tzinfo=CHICAGO),
        source=HistorySource.FLAT_FILES,
    )
    assert fake_s3.keys() == [FUTURES_0304.format("cme")]
    assert pick(t, "Symbol", "Timestamp", "SessionEndDate") == [
        ("ESH5", 1741042800000000000, date(2025, 3, 4))
    ]


def test_futures_instants_inside_one_session_need_only_its_file(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    make_csv_gz: Callable[[str], bytes],
) -> None:
    clock(datetime(2025, 3, 4, 17, tzinfo=UTC))  # March 3 is published; March 4 is not
    moment = epoch_ns(datetime(2025, 3, 3, 10, 30, tzinfo=CHICAGO))
    key = FUTURES_KEY.format("cme", date(2025, 3, 3))
    fake_s3.put(key, make_csv_gz(FUTURES_HEADER + f",1.0,1,1,1,1,2025-03-03,ESH5,{moment}\n"))
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venue=FuturesVenue.CME)
    t = q.history(
        datetime(2025, 3, 3, 10, tzinfo=CHICAGO),
        datetime(2025, 3, 3, 11, tzinfo=CHICAGO),
        source=HistorySource.FLAT_FILES,
    )
    assert pick(t, "Timestamp") == [(moment,)]
    assert fake_s3.keys() == [key]


def test_futures_instants_before_a_holiday_do_not_raise(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    make_csv_gz: Callable[[str], bytes],
) -> None:
    # There is no CME session ending on Christmas Day; the window lies inside December 24's.
    clock(datetime(2025, 1, 10, tzinfo=UTC))
    moment = epoch_ns(datetime(2024, 12, 24, 10, 30, tzinfo=CHICAGO))
    key = FUTURES_KEY.format("cme", date(2024, 12, 24))
    fake_s3.put(key, make_csv_gz(FUTURES_HEADER + f",1.0,1,1,1,1,2024-12-24,ESH5,{moment}\n"))
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venue=FuturesVenue.CME)
    t = q.history(
        datetime(2024, 12, 24, 10, tzinfo=CHICAGO), datetime(2024, 12, 24, 11, tzinfo=CHICAGO)
    )
    assert t.size == 1
    assert fake_s3.keys() == [key]


def test_futures_session_keys_are_checked(
    *, session: dm.MassiveSession, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    # The right session first: the message names the first wrong row.
    body = (
        FUTURES_HEADER
        + ",1.0,1,1,1,1,2025-03-04,ESH5,1741042800000000000\n"
        + ",1.0,1,1,1,2,2025-03-03,ESH5,1741042800000000000\n"
    )
    fake_s3.put(FUTURES_0304.format("cme"), make_csv_gz(body))
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venue=FuturesVenue.CME)
    with pytest.raises(
        dm.MassiveError,
        match=re.escape(
            "us_futures_cme/trades_v1/2025/03/2025-03-04.csv.gz holds session_end_date "
            "2025-03-03, not 2025-03-04: futures flat files are not keyed by trading session"
        ),
    ):
        q.history("2025-03-04", "2025-03-04")


def test_the_futures_session_check_covers_every_symbol(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    put(FUTURES_0304.format("cme"), "us_futures_trades_v1_wrong_session.csv")  # an ESH5 row
    q = query(session, FUTURES_TRADES, symbols=["NQH5"], venue=FuturesVenue.CME)
    with pytest.raises(dm.MassiveError, match="holds session_end_date 2025-03-03, not 2025-03-04"):
        q.history("2025-03-04", "2025-03-04", source=HistorySource.FLAT_FILES)


def test_a_missing_session_date_fails_the_session_check(
    *, session: dm.MassiveSession, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    row = ",1.0,1,1,1,1,,ESH5,1741042800000000000\n"
    fake_s3.put(FUTURES_0304.format("cme"), make_csv_gz(FUTURES_HEADER + row))
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venue=FuturesVenue.CME)
    with pytest.raises(dm.MassiveError, match="holds session_end_date empty, not 2025-03-04"):
        q.history("2025-03-04", "2025-03-04")


def test_a_refused_futures_venue_names_the_venue(
    *, fake_s3: FakeS3, clock: Callable[[datetime], None], put: Callable[[str, str], None]
) -> None:
    m = dm.MassiveSession(config(max_concurrency=1))
    try:
        put(FUTURES_0304.format("cme"), "us_futures_trades_v1_2025-03-04.csv")
        put(FUTURES_0304.format("cbot"), "us_futures_trades_v1_2025-03-04.csv")
        fake_s3.refuse(
            "us_futures_nymex/", status=403, code="AccessDenied", message="Access Denied"
        )
        q = query(m, FUTURES_TRADES, symbols=dm.ALL)
        with pytest.raises(
            dm.EntitlementError,
            match=(
                r"the plan does not include NYMEX futures flat files: HTTP 403 for "
                r"s3://flatfiles/us_futures_nymex/trades_v1/2025/03/2025-03-04\.csv\.gz at "
                r"https://files\.massive\.com \(AccessDenied: Access Denied\); pass "
                r"venue=dm\.FuturesVenue\.<VENUE> for a venue the plan includes"
            ),
        ):
            q.history("2025-03-04", "2025-03-04")
    finally:
        m.close()


# --- Errors ----------------------------------------------------------------------------------


def test_a_refused_file_is_an_entitlement_error(
    session: dm.MassiveSession, fake_s3: FakeS3
) -> None:
    fake_s3.refuse("us_stocks_sip/", status=403, code="AccessDenied", message="Access Denied")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(
        dm.EntitlementError,
        match=r"the plan does not include these flat files: .*\(AccessDenied: Access Denied\)",
    ) as info:
        q.history("2025-03-03", "2025-03-03")
    assert (info.value.status, info.value.request_id) == (403, fake_s3.request_id)


@pytest.mark.parametrize("code", ["InvalidAccessKeyId", "SignatureDoesNotMatch"])
@pytest.mark.parametrize(
    ("spec", "key", "venue"),
    [
        pytest.param(STOCK_TRADES, TRADES_0304, None, id="stocks"),
        pytest.param(FUTURES_TRADES, FUTURES_0304.format("cme"), FuturesVenue.CME, id="futures"),
    ],
)
def test_rejected_credentials_are_an_authentication_error(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    code: str,
    spec: DatasetSpec,
    key: str,
    venue: FuturesVenue | None,
) -> None:
    fake_s3.refuse("us_", status=403, code=code, message="Rejected")
    q = query(session, spec, symbols=dm.ALL, venue=venue)
    with pytest.raises(
        dm.AuthenticationError,
        match=re.escape(
            f"S3 rejected the flat-file credentials: HTTP 403 for s3://flatfiles/{key} at "
            f"https://files.massive.com ({code}: Rejected); check MassiveConfig "
            "s3_access_key_id and s3_secret_access_key"
        ),
    ) as info:
        q.history("2025-03-04", "2025-03-04")
    assert not isinstance(info.value, dm.EntitlementError)
    assert (info.value.status, info.value.request_id) == (403, fake_s3.request_id)


def test_a_server_error_is_a_massive_error(session: dm.MassiveSession, fake_s3: FakeS3) -> None:
    fake_s3.refuse("us_stocks_sip/", status=500, code="InternalError", message="Try again")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(dm.MassiveError, match=r"HTTP 500 \(InternalError: Try again\)") as info:
        q.history("2025-03-03", "2025-03-03")
    assert (info.value.status, info.value.request_id) == (500, fake_s3.request_id)


def test_an_unreachable_endpoint_is_a_massive_error(
    session: dm.MassiveSession, fake_s3: FakeS3
) -> None:
    fake_s3.fail(TRADES_0303, "unreachable")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(
        dm.MassiveError, match=re.escape("cannot read s3://flatfiles/us_stocks_sip")
    ):
        q.history("2025-03-03", "2025-03-03")


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b"not gzip", id="not-gzip"),
        pytest.param(gzip.compress(b"ticker,price\n" * 1000)[:-200], id="truncated-gzip"),
        pytest.param(
            gzip.compress(b"ticker,price\n")[:10] + b"\xff" * 40, id="corrupt-deflate-data"
        ),
        pytest.param("ticker,price\nAAPL,1.0\n", id="missing-documented-headers"),
        pytest.param("ticker,sip_timestamp\nAAPL,12:30\n", id="unparseable-value"),
        pytest.param(
            TRADE_HEADER
            + '"12,x",,4,1,1741012200000000000,1.5,1,1741012200000000000,10,1,AAPL,,\n',
            id="non-integer-condition",
        ),
    ],
)
def test_a_malformed_file_is_a_massive_error(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
    body: bytes | str,
) -> None:
    fake_s3.put(TRADES_0303, body if isinstance(body, bytes) else make_csv_gz(body))
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(
        dm.MassiveError, match=re.escape(f"cannot read s3://flatfiles/{TRADES_0303}")
    ):
        q.history("2025-03-03", "2025-03-03")


def test_a_dropped_connection_is_a_massive_error(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
    load_fixture: Callable[[str, str], bytes],
) -> None:
    data = make_csv_gz(
        load_fixture("ingest/flatfiles", "us_stocks_sip_trades_v1_2025-03-03.csv").decode()
    )
    fake_s3.put_dropped(TRADES_0303, data[: len(data) // 2])
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(
        dm.MassiveError, match=re.escape(f"cannot read s3://flatfiles/{TRADES_0303}")
    ):
        q.history("2025-03-03", "2025-03-03")


def test_every_response_body_is_closed(
    *, session: dm.MassiveSession, fake_s3: FakeS3, put: Callable[[str, str], None]
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    fake_s3.put(TRADES_0304, b"not gzip")
    with pytest.raises(dm.MassiveError):
        query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-04", "2025-03-04")
    assert fake_s3.closed_bodies == [TRADES_0303, TRADES_0304]


# --- Threads, cancellation, and close --------------------------------------------------------


def test_reads_run_in_parallel_up_to_max_concurrency(
    *,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    put: Callable[[str, str], None],
    pool: type[ShutdownSignal],
) -> None:
    pair = threading.Barrier(2, timeout=TIMEOUT)  # passes only if two reads are in flight at once
    fake_s3.before_answer = lambda _key: pair.wait()
    for day in (3, 4, 5, 6):
        put(TRADES_KEY.format(day), "us_stocks_sip_trades_v1_2025-03-05.csv")
    m = dm.MassiveSession(config(max_concurrency=2))
    try:
        query(m, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-06")
        query(m, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-04")
    finally:
        m.close()
    assert pool.sizes == [2, 2]
    assert len(fake_s3.keys()) == 6


def test_reader_threads_enter_the_session_context(
    *, session: dm.MassiveSession, put: Callable[[str, str], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    seen: list[tuple[str, bool]] = []
    convert = dh_arrow.to_table
    expected = session._core.exec_ctx.j_exec_ctx

    def recorded(data: pa.Table) -> Table:
        context = get_exec_ctx().j_exec_ctx.equals(expected)
        seen.append((threading.current_thread().name, context))
        return convert(data)

    monkeypatch.setattr(flatfiles, "to_table", recorded)
    query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    reader = f"{session._core.label} flat files"
    assert all(context for _, context in seen)
    assert any(name.startswith(reader) for name, _ in seen)


def test_history_runs_from_a_thread_without_an_execution_context(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=["aapl"])
    out: list[object] = []

    def run() -> None:  # a failure leaves `out` empty, which the assertion reports
        t = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
        out.append(pick(t, "Id"))

    worker = threading.Thread(target=run)
    worker.start()
    worker.join(TIMEOUT)
    assert out == [[("1",), ("3",)]]


def test_a_failed_read_cancels_the_reads_not_started(
    *, fake_s3: FakeS3, clock: Callable[[datetime], None], pool: type[ShutdownSignal]
) -> None:
    # The second read holds its worker until the pool shuts down, so the remaining reads are
    # still queued when the first failure is raised.
    fake_s3.before_answer = lambda key: pool.shut.wait(TIMEOUT) if key == TRADES_0304 else None
    fake_s3.refuse(TRADES_0303, status=500, code="InternalError", message="first")
    m = dm.MassiveSession(config(max_concurrency=1))
    try:
        with pytest.raises(dm.MassiveError, match="HTTP 500"):
            query(m, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-07")
    finally:
        m.close()
    assert fake_s3.keys()[0] == TRADES_0303
    assert set(fake_s3.keys()) <= {TRADES_0303, TRADES_0304}


def test_the_earliest_failure_is_raised(
    *, fake_s3: FakeS3, clock: Callable[[datetime], None]
) -> None:
    # Both reads fail for real: the March 3 answer waits until March 4 has been requested, so
    # March 4 fails first and March 3 still fails after it. The earlier file's failure wins.
    asked = threading.Event()

    def order(key: str) -> None:
        if key == TRADES_0304:
            asked.set()
        elif key == TRADES_0303:
            assert asked.wait(TIMEOUT)

    fake_s3.before_answer = order
    fake_s3.refuse(TRADES_0303, status=500, code="InternalError", message="first")
    fake_s3.refuse(TRADES_0304, status=403, code="AccessDenied", message="second")
    m = dm.MassiveSession(config(max_concurrency=2))
    try:
        with pytest.raises(dm.MassiveError, match="HTTP 500") as info:
            query(m, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-04")
        assert not isinstance(info.value, dm.EntitlementError)
    finally:
        m.close()
    assert sorted(fake_s3.keys()) == [TRADES_0303, TRADES_0304]


def test_a_cancelled_or_closed_read_makes_no_request(
    *, session: dm.MassiveSession, fake_s3: FakeS3, put: Callable[[str, str], None]
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    transport = session._core.flat_file_transport()
    assert isinstance(transport, flatfiles.FlatFileHistory)
    window = bounds(STOCK_TRADES, "2025-03-03", "2025-03-03")
    transform = flatfiles._Transform.build(STOCK_TRADES, dm.ALL, window)
    key = flatfiles._Key(TRADES_0303, date(2025, 3, 3), None)
    cancel = threading.Event()
    cancel.set()
    with pytest.raises(flatfiles._CancelledError):
        transport._read_key(transform, key, cancel=cancel)
    flat_file_client(session).close()
    with pytest.raises(dm.MassiveError, match="is closed"):
        transport._read_key(transform, key, cancel=threading.Event())
    assert fake_s3.requests == []


def test_a_failure_stops_the_reads_in_progress(
    *,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    many_blocks: Callable[[str], int],
    applied: Applied,
    pool: type[ShutdownSignal],
) -> None:
    blocks = many_blocks(TRADES_0303)
    fake_s3.refuse(TRADES_0304, status=500, code="InternalError", message="second")
    # The March 3 read pauses after its first block until the pool is shut down, which happens
    # only once the March 4 failure has set the cancel event.
    applied.hook = lambda _key: pool.shut.wait(TIMEOUT)
    m = dm.MassiveSession(config(max_concurrency=2))
    try:
        # The March 3 read stopped rather than failed, so the March 4 failure is raised.
        with pytest.raises(dm.MassiveError, match="HTTP 500"):
            query(m, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-04")
    finally:
        m.close()
    assert blocks > 10
    assert applied.counts[TRADES_0303] == 1


def test_an_interrupt_stops_every_read(
    *,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    many_blocks: Callable[[str], int],
    applied: Applied,
    pool: type[ShutdownSignal],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    many_blocks(TRADES_0303)

    def interrupted(futures: object, return_when: str) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(flatfiles, "wait", interrupted)  # Ctrl-C while the caller waits
    applied.hook = lambda _key: pool.shut.wait(TIMEOUT)
    m = dm.MassiveSession(config(max_concurrency=1))
    try:
        with pytest.raises(KeyboardInterrupt):
            query(m, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    finally:
        m.close()
    assert applied.counts.get(TRADES_0303, 0) <= 1


def test_closing_the_session_stops_a_read_in_progress(
    *,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    many_blocks: Callable[[str], int],
    applied: Applied,
) -> None:
    blocks = many_blocks(TRADES_0303)
    m = dm.MassiveSession(config())
    first_block = threading.Event()
    closed = threading.Event()

    def pause(key: str) -> None:
        if not first_block.is_set():
            first_block.set()
            assert closed.wait(TIMEOUT)

    applied.hook = pause

    def close_when_reading() -> None:
        assert first_block.wait(TIMEOUT)
        m.close()
        closed.set()

    closer = threading.Thread(target=close_when_reading)
    closer.start()
    try:
        with pytest.raises(dm.MassiveError, match=r"MassiveSession #\d+ is closed"):
            query(m, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    finally:
        closer.join(TIMEOUT)
        m.close()
    assert blocks > 10
    assert applied.counts[TRADES_0303] == 1
    assert fake_s3.keys() == [TRADES_0303]


@pytest.mark.parametrize(
    ("day", "stored"),
    [
        pytest.param(3, True, id="a-file-answered-200"),
        pytest.param(4, False, id="a-missing-tuesday"),
        pytest.param(8, False, id="a-missing-saturday"),
    ],
)
def test_a_close_during_a_request_reports_the_close(
    *,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    put: Callable[[str, str], None],
    monkeypatch: pytest.MonkeyPatch,
    day: int,
    stored: bool,
) -> None:
    # The session closes while the request is in flight, after the reader's last check: the
    # answer, whatever it is (a file, a missing weekday, a missing weekend day), is discarded
    # and the call reports the close.
    if stored:
        put(TRADES_KEY.format(day), "us_stocks_sip_trades_v1_2025-03-03.csv")
    m = dm.MassiveSession(config())
    fake_s3.before_answer = lambda _key: flat_file_client(m).close()
    when = f"2025-03-{day:02d}"
    try:
        with pytest.raises(dm.MassiveError, match=r"MassiveSession #\d+ is closed"):
            query(m, STOCK_TRADES, symbols=dm.ALL).history(when, when)
    finally:
        m.close()
    # Closed by the close, then once more as the in-flight request returned, which drops any
    # connection pool that request opened.
    assert fake_s3.closes == 2
    assert fake_s3.keys() == [TRADES_KEY.format(day)]


# --- The bucket and its client ---------------------------------------------------------------


def test_the_client_comes_from_the_config_and_is_shared(
    *, fake_s3: FakeS3, clock: Callable[[datetime], None], put: Callable[[str, str], None]
) -> None:
    m = dm.MassiveSession(
        config(s3_endpoint_url="https://s3.example.com", s3_bucket="other", max_concurrency=3)
    )
    try:
        fake_s3.put(TRADES_0303, b"")
        q = query(m, STOCK_TRADES, symbols=dm.ALL)
        with pytest.raises(dm.MassiveError):  # an empty object is not a CSV file
            q.history("2025-03-03", "2025-03-03")
        put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
        assert q.history("2025-03-03", "2025-03-03").size == 4
        # One client per session, created on first use from the configuration.
        assert fake_s3.clients == [
            {
                "endpoint_url": "https://s3.example.com",
                "access_key": "AKIDFLATFILES",
                "secret_key": "flat-file-secret",
                "signature_version": "s3v4",
                "max_pool_connections": 3,
            }
        ]
        assert {bucket for bucket, _ in fake_s3.requests} == {"other"}
    finally:
        m.close()


def test_a_client_that_cannot_be_created_is_a_massive_error(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    empty = tmp_path / "aws-config"
    empty.write_text("")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(empty))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(empty))
    monkeypatch.setenv("AWS_PROFILE", "no-such-profile")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(
        dm.MassiveError,
        match=r"cannot create the S3 client for https://files\.massive\.com: .*no-such-profile",
    ):
        q.history("2025-03-03", "2025-03-03")
    assert fake_s3.clients == []


def test_closing_the_session_closes_the_boto_client(
    *, session: dm.MassiveSession, fake_s3: FakeS3, put: Callable[[str, str], None]
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    client = flat_file_client(session)
    session.close()
    assert fake_s3.closes == 1
    assert client.closed
    client.close()  # idempotent
    assert fake_s3.closes == 1
    with pytest.raises(_massive.WireError, match="is closed"):
        client.open(TRADES_0303)  # a read that raced close() cannot reopen it
    assert fake_s3.keys() == [TRADES_0303]


def test_a_session_collected_unclosed_closes_its_bucket(
    *,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    put: Callable[[str, str], None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    m = dm.MassiveSession(config(api_key="collected-with-flat-files"))
    name = repr(m).removeprefix("<").removesuffix(" open>")
    table = query(m, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    client = flat_file_client(m)
    reaper = m._core._reaper.thread
    assert fake_s3.closes == 0
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._session"):
        del m
        gc.collect()
        reaper.join(TIMEOUT)
    assert not reaper.is_alive()
    assert fake_s3.closes == 1
    assert client.closed
    assert table.size == 4  # static results outlive their session
    assert caplog.messages == [
        f"{name} was garbage-collected without close(); closing its connections and pollers"
    ]


# --- Bounded memory --------------------------------------------------------------------------


def synthetic_trades(rows: int) -> str:
    # A trades file whose rows vary like real ones; one row in a thousand is the symbol RARE.
    moment = 1741012200000000000
    tickers = ("AAPL", "MSFT", "SPY", "NVDA")
    lines = [
        f"12,,4,{n},{moment + n * 1000},{100 + n % 997 / 7:.4f},{n},{moment + n * 1001},"
        f"{n % 5000 + 1},3,{'RARE' if n % 1000 == 0 else tickers[n % 4]},,\n"
        for n in range(rows)
    ]
    return TRADE_HEADER + "".join(lines)


class MemorySampler:
    # Samples Arrow's pool at each block and table, and the running Python peak (tracemalloc)
    # at each block, into buffers allocated up front: a list of Python ints would itself grow
    # inside the traced region by about 40 bytes per block.
    def __init__(self, capacity: int) -> None:
        self.arrow = array("q", bytes(8 * capacity))
        self.python = array("q", bytes(8 * capacity))
        self.taken = [0, 0]

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        batches = _massive.FlatFile.batches
        convert = dh_arrow.to_table

        def sampled_batches(
            file: _massive.FlatFile, plan: _massive.ReadPlan
        ) -> Iterator[pa.RecordBatch]:
            for batch in batches(file, plan):
                self.sample_arrow()
                self.python[self.taken[1]] = tracemalloc.get_traced_memory()[1]
                self.taken[1] += 1
                yield batch

        def sampled_convert(data: pa.Table) -> Table:
            self.sample_arrow()
            return convert(data)

        monkeypatch.setattr(_massive.FlatFile, "batches", sampled_batches)
        monkeypatch.setattr(flatfiles, "to_table", sampled_convert)

    def sample_arrow(self) -> None:
        self.arrow[self.taken[0]] = pa.total_allocated_bytes()
        self.taken[0] += 1

    def measure(self, read: Callable[[], Table]) -> tuple[int, int, int]:
        # Runs `read` and returns its rows, and the Python and Arrow peaks above the baselines.
        self.taken[:] = [0, 0]
        arrow_baseline = pa.total_allocated_bytes()
        tracemalloc.start()
        try:
            python_baseline = tracemalloc.get_traced_memory()[0]
            rows = read().size
            python_peak = tracemalloc.get_traced_memory()[1] - python_baseline
        finally:
            tracemalloc.stop()
        return rows, python_peak, max(self.arrow[: self.taken[0]]) - arrow_baseline

    def python_rise(self) -> int:
        # How far the running Python peak rose from a third of the blocks to the last one.
        blocks = self.taken[1]
        return self.python[blocks - 1] - self.python[blocks // 3]

    def arrow_rise(self) -> int:
        # How far the Arrow pool's running maximum rose from a third of the samples to the end.
        samples = self.taken[0]
        return max(self.arrow[:samples]) - max(self.arrow[: samples // 3])


def test_memory_per_reader_does_not_grow_with_the_file(
    *,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    make_csv_gz: Callable[[str], bytes],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    block = 16 * 1024
    monkeypatch.setattr(flatfiles, "BLOCK_SIZE", block)
    text = synthetic_trades(600_000)
    fake_s3.put(TRADES_0303, make_csv_gz(text))
    sampler = MemorySampler(2 * (len(text) // block) + 1000)
    sampler.install(monkeypatch)
    m = dm.MassiveSession(config(max_concurrency=1))

    def read(symbols: object) -> Callable[[], Table]:
        q = query(m, STOCK_TRADES, symbols=symbols)
        return lambda: q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)

    try:
        # A first read creates the client: botocore parses its service model once per client.
        fake_s3.put(TRADES_0304, make_csv_gz(TRADE_HEADER + trade(1, 1741012200000000000)))
        query(m, STOCK_TRADES, symbols=dm.ALL).history("2025-03-04", "2025-03-04")
        every, python_peak, arrow_peak = sampler.measure(read(dm.ALL))
        rare, _, _ = sampler.measure(read(["RARE"]))  # the samples kept are this read's
    finally:
        m.close()
    assert (every, rare) == (600_000, 600)
    # About 32 blocks of read-ahead and 16 of gathered rows. Small test blocks inflate the peak
    # through fixed per-block overhead (about 75 blocks with the GIL and 150 without it at
    # 16 KiB, against about 37 at the production 1 MiB), so 250 blocks is only a loose ceiling.
    bound = 250 * block
    assert len(text) > 10 * bound
    assert python_peak < bound
    assert arrow_peak < bound
    # The real check is the plateau: once the read-ahead has filled, the running peak must not
    # keep rising with the file. It is measured on the rare symbol's read, whose result is one
    # small table: with dm.ALL the result itself (a Deephaven table, and its Python wrapper,
    # per 16 blocks) legitimately grows with the file.
    assert sampler.python_rise() < 32 * block
    # Arrow's pool is invisible to tracemalloc: each small batch a selective read gathers costs
    # allocation overhead that its byte count does not show, so the batches must be merged.
    assert sampler.arrow_rise() < 32 * block


def test_the_production_block_size() -> None:
    assert flatfiles.BLOCK_SIZE == 2**20
    assert flatfiles._TABLE_BLOCKS == 16


def test_outcome_of_a_cancelled_read_is_skipped() -> None:
    cancelled: Future[list[Table] | None] = Future()
    assert cancelled.cancel()
    assert flatfiles._outcome(cancelled) is None


# --- The session's client and the library's own errors -------------------------------------


def test_a_session_without_a_flat_file_read_creates_no_client(
    *, fake_s3: FakeS3, clock: Callable[[datetime], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    m = dm.MassiveSession(config())
    rest = RestRecorder()
    monkeypatch.setattr(m._core, "rest_transport", lambda: rest)
    query(m, STOCK_TRADES, symbols=["AAPL"]).history("2025-03-03", "2025-03-03")
    assert len(rest.calls) == 1  # AUTO chose REST for one symbol
    m.close()
    assert m._core._flat_files is None
    assert (fake_s3.clients, fake_s3.closes) == ([], 0)


def test_every_read_of_a_session_shares_one_client(session: dm.MassiveSession) -> None:
    first = session._core.flat_file_transport()
    second = session._core.flat_file_transport()
    assert isinstance(first, flatfiles.FlatFileHistory)
    assert isinstance(second, flatfiles.FlatFileHistory)
    assert first._client is second._client is flat_file_client(session)


def test_a_client_created_after_close_is_closed_at_once(
    *, fake_s3: FakeS3, clock: Callable[[datetime], None]
) -> None:
    m = dm.MassiveSession(config())
    m.close()
    m._core.flat_file_transport()
    assert flat_file_client(m).closed


def test_the_flat_file_transport_needs_s3_credentials(
    *, fake_s3: FakeS3, clock: Callable[[datetime], None]
) -> None:
    m = dm.MassiveSession(NO_S3)
    try:
        with pytest.raises(ValueError, match="flat files need MassiveConfig s3_access_key_id"):
            m._core.flat_file_transport()
    finally:
        m.close()
    assert m._core._flat_files is None


def test_an_arrow_error_while_gathering_rows_is_a_massive_error(
    *, session: dm.MassiveSession, put: Callable[[str, str], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    # Converting a file's rows can fail in Arrow (out of memory, say): it is a failed read of
    # that file, named like any other.
    def exhausted(transform: flatfiles._Transform, batch: pa.RecordBatch) -> pa.RecordBatch:
        raise pa.ArrowMemoryError("malloc of size 1048576 failed")

    monkeypatch.setattr(flatfiles._Transform, "apply", exhausted)
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(
        dm.MassiveError,
        match=re.escape(
            f"cannot read s3://flatfiles/{TRADES_0303} at https://files.massive.com: malloc of "
            "size 1048576 failed"
        ),
    ) as info:
        q.history("2025-03-03", "2025-03-03")
    assert isinstance(info.value.__cause__, pa.ArrowMemoryError)
