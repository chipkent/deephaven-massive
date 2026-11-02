"""Tests for flat-file history: source choice, partition dates, and streaming S3 reads."""

import csv
import dataclasses
import functools
import gc
import gzip
import io
import itertools
import logging
import re
import threading
import time
import tracemalloc
import weakref
from array import array
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple, Protocol, cast
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven.execution_context import get_exec_ctx
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._datasets import history_source, row_times
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.dates import DaySpan, TimeRange
from deephaven_massive._datasets.futures_sessions import CHICAGO
from deephaven_massive._datasets.history_source import HistoryWindow, history_window
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.row_times import BarBounds, RowTimes, StartRule
from deephaven_massive._datasets.schema import Schema
from deephaven_massive._enums import AssetClass, FuturesVenue, HistorySource, NotBars, Timespan
from deephaven_massive._ingest import flatfiles
from deephaven_massive._log_limits import BoundedLog
from deephaven_massive._massive import DriftReport, Field, Source, Transport, WireType, WireUnit
from deephaven_massive._namespaces.query_building import build_query
from deephaven_massive._query import HistoryQuery
from deephaven_massive._session import CATALOG
from deephaven_massive._tables import columns
from deephaven_massive._tables.columns import col_defs
from deephaven_massive._types import override
from tests._fakes.clock import FakeClock

if TYPE_CHECKING:
    from deephaven_massive._ingest.clients import SessionClients

TIMEOUT = 30.0
RowsWhen = Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]]
NEW_YORK = ZoneInfo("America/New_York")
R, FF = Transport.REST, Transport.FLAT_FILES
NOW = datetime(2025, 3, 10, 16, tzinfo=UTC)
"""A Monday afternoon, when every file of the first week of March 2025 is published."""
LOGGER = "deephaven_massive"
# The once-per-session warnings a transform built outside a session logs to.
WARNINGS = BoundedLog(logging.getLogger("tests.flatfiles"), limit=10, level=logging.WARNING)
"""The package logger: the AUTO line comes from the query, the S3 line from the transport."""


class FakeS3(Protocol):
    # What the tests use of the `fake_s3` fixture (tests/conftest.py, tests/_fakes/s3.py).
    requests: list[tuple[str, str]]
    clients: list[dict[str, object]]
    closes: int
    closed_bodies: list[str]
    request_id: str
    before_answer: Callable[[str], object] | None
    before_drop: Callable[[str], object] | None

    def put(self, key: str, data: bytes) -> None: ...
    def put_csv(
        self, key: str, header: Sequence[str], rows: Iterable[Mapping[str, object]]
    ) -> None: ...
    def put_dropped(self, key: str, data: bytes) -> None: ...
    def drop(self, key: str, *, times: int, at: int) -> None: ...
    def data(self, key: str) -> bytes: ...
    def refuse(self, prefix: str, *, status: int, code: str, message: str) -> None: ...
    def fail(self, key: str, failure: str) -> None: ...
    def keys(self) -> list[str]: ...


def keys(symbol: str | KeySpec, time: str, *, session: bool = False) -> SourceKeys:
    # A source's keys: a plain symbol names its column; a futures source adds its session.
    return SourceKeys(
        KeySpec(symbol) if isinstance(symbol, str) else symbol,
        KeySpec(time),
        date=KeySpec("SessionEndDate") if session else None,
    )


def from_request(parameter: str) -> KeySpec:
    return KeySpec(parameter, from_request=True)


STOCK_TRADES = DatasetSpec(
    name="stocks.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    history_path="/v3/trades/{stockTicker}",
    flat_file_prefixes={NotBars.ANY: "us_stocks_sip/trades_v1"},
    keys={
        (R, "/v3/trades/{stockTicker}"): keys(from_request("stockTicker"), "SipTimestamp"),
        (FF, "us_stocks_sip/trades_v1"): keys("Ticker", "SipTimestamp"),
    },
)
STOCK_QUOTES = DatasetSpec(
    name="stocks.quotes",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    history_path="/v3/quotes/{stockTicker}",
    flat_file_prefixes={NotBars.ANY: "us_stocks_sip/quotes_v1"},
    keys={
        (R, "/v3/quotes/{stockTicker}"): keys(from_request("stockTicker"), "SipTimestamp"),
        (FF, "us_stocks_sip/quotes_v1"): keys("Ticker", "SipTimestamp"),
    },
)
STOCK_BARS = DatasetSpec(
    name="stocks.bars",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    is_bars=True,
    history_path="/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}",
    grouped_daily_path="/v2/aggs/grouped/locale/us/market/stocks/{date}",
    flat_file_prefixes={
        Timespan.MINUTE: "us_stocks_sip/minute_aggs_v1",
        Timespan.DAY: "us_stocks_sip/day_aggs_v1",
    },
    keys={
        (R, "/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}"): keys(
            from_request("stocksTicker"), "WindowStart"
        ),
        (R, "/v2/aggs/grouped/locale/us/market/stocks/{date}"): keys("Ticker", "WindowStart"),
        (FF, "us_stocks_sip/minute_aggs_v1"): keys("Ticker", "WindowStart"),
        (FF, "us_stocks_sip/day_aggs_v1"): keys("Ticker", "WindowStart"),
    },
)
INDEX_VALUES_SPEC = DatasetSpec(
    name="indices.values",
    summary="A dataset for the tests.",
    asset_class=AssetClass.INDICES,
    flat_file_prefixes={NotBars.ANY: "us_indices/values_v1"},
    keys={(FF, "us_indices/values_v1"): keys("Ticker", "Timestamp")},
)
FUTURES_TRADES = DatasetSpec(
    name="futures.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.FUTURES,
    row_times=RowTimes.TICK,
    history_path="/futures/v1/trades/{ticker}",
    flat_file_prefixes={NotBars.ANY: "us_futures_{venue}/trades_v1"},
    keys={
        (R, "/futures/v1/trades/{ticker}"): keys(from_request("ticker"), "Timestamp", session=True),
        (FF, "us_futures_{venue}/trades_v1"): keys("Ticker", "Timestamp", session=True),
    },
)
CRYPTO_TRADES = DatasetSpec(
    name="crypto.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.CRYPTO,
    history_path="/v3/trades/{cryptoTicker}",
    flat_file_prefixes={NotBars.ANY: "global_crypto/trades_v1"},
    keys={
        (R, "/v3/trades/{cryptoTicker}"): keys(
            from_request("cryptoTicker"), "ParticipantTimestamp"
        ),
        (FF, "global_crypto/trades_v1"): keys("Ticker", "ParticipantTimestamp"),
    },
)
YIELDS = DatasetSpec(
    name="economy.yields",
    summary="A dataset for the tests.",
    poll_keys=("Date",),
    history_path="/fed/v1/treasury-yields",
)


def schema_of(spec: DatasetSpec) -> Schema:
    return CATALOG.dataset_schema(spec)


TRADES_KEY = "us_stocks_sip/trades_v1/2025/03/2025-03-{:02d}.csv.gz"
TRADES_0303 = TRADES_KEY.format(3)
TRADES_0304 = TRADES_KEY.format(4)
TRADES_0305 = TRADES_KEY.format(5)
LOCATION = "s3://flatfiles/{} at https://files.massive.com"
FUTURES_KEY = "us_futures_{}/trades_v1/{:%Y/%m/%Y-%m-%d}.csv.gz"
FUTURES_0304 = "us_futures_{}/trades_v1/2025/03/2025-03-04.csv.gz"
STOCK_KEY = flatfiles._Key(TRADES_0303, date(2025, 3, 3), None)
"""The file a transform's block comes from, in the tests that apply one directly."""
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


def rows(table: Table, *, arrival: bool = False) -> list[dict[str, Any]]:
    # Instants become epoch nanoseconds, so assertions keep every digit. Every row carries the
    # instant its block was decoded, last; it is checked here and, unless asked for, left out,
    # so an assertion compares only what the files hold.
    data = dh_arrow.to_arrow(table)
    assert data.column_names[-1] == "TimestampInsert"
    assert data.column("TimestampInsert").null_count == 0
    columns = [c.cast(pa.int64()) if pa.types.is_timestamp(c.type) else c for c in data.columns]
    out: list[dict[str, Any]] = pa.Table.from_arrays(columns, names=data.column_names).to_pylist()
    if not arrival:
        for row in out:
            del row["TimestampInsert"]
    return out


def pick(table: Table, *columns: str) -> list[tuple[Any, ...]]:
    return [tuple(row[c] for c in columns) for row in rows(table)]


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Callable[[datetime], None]:
    def set_now(moment: datetime) -> None:
        monkeypatch.setattr(history_source, "_now", lambda: moment)

    set_now(NOW)
    return set_now


@pytest.fixture(autouse=True)
def retry_clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    """Give every session's flat-file client a fake clock, so its retry waits never sleep.

    The session reads `_massive.FlatFileClient` when it creates its client, so the patch reaches
    it; the tests that retry assert this clock's waits, so they fail rather than sleep for real
    if the session ever binds the class earlier. The patch goes into the module's namespace with
    `setitem`, so teardown removes the entry again. `setattr` would restore the lazily resolved
    class as a bound attribute, which the wire package's interface test forbids.

    Returns:
        The clock every flat-file client of the test waits on.
    """
    clock = FakeClock()
    monkeypatch.setitem(
        vars(_massive),
        "FlatFileClient",
        functools.partial(_massive.FlatFileClient, clock=clock),
    )
    return clock


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
    client = cast("SessionClients", m._core._ports)._flat_files
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
    assert {c.name: c.data_type for c in t.columns} == col_defs(schema_of(STOCK_TRADES))
    first, msft, second, spy = rows(t)
    assert first == {
        "Symbol": "AAPL",
        "Timestamp": 1741012200000000123,
        "RawSymbol": None,  # the default session keeps the column; null on a readable row
        "Conditions": [12, 37],
        "Correction": None,
        "Exchange": 4,
        "Id": "1",
        "ParticipantTimestamp": 1741012199999999000,
        "Price": 227.48,
        "SequenceNumber": 100,
        "Size": 100.0,
        "Tape": 3,
        "TrfId": None,
        "TrfTimestamp": None,
        # Only the REST path documents a decimal size: the files' rows hold it null.
        "DecimalSize": None,
    }
    assert (msft["Symbol"], msft["Conditions"]) == ("MSFT", None)
    assert msft["TrfTimestamp"] is None  # the file says 0: no TRF
    assert (second["Size"], second["Correction"], second["Conditions"]) == (0.5, 1, [14])
    assert (second["TrfId"], second["TrfTimestamp"]) == (201, 1741013999999999000)
    assert spy["Symbol"] == "SPY"
    assert fake_s3.requests == [("flatfiles", TRADES_0303)]


SYNTHETIC_FILES = "us_stocks_sip/trades_v1"
SYNTHETIC = DatasetSpec(
    name="stocks.synthetic",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    history_path="/v3/trades/{stockTicker}",
    flat_file_prefixes={NotBars.ANY: SYNTHETIC_FILES},
    keys={(FF, SYNTHETIC_FILES): keys("Ticker", "SipTimestamp")},
)


def synthetic_catalog() -> Catalog:
    # A file whose participant time is in milliseconds and whose size and conditions are
    # integers, merged with REST fields that widen them to a double and a list.
    files = Source(
        key=SYNTHETIC_FILES,
        transport=FF,
        fields=(
            Field(wire_key="ticker", column="Ticker", wire_type=WireType.STRING),
            Field(
                wire_key="sip_timestamp",
                column="SipTimestamp",
                wire_type=WireType.EPOCH,
                unit=WireUnit.NS,
            ),
            Field(
                wire_key="participant_timestamp",
                column="ParticipantTimestamp",
                wire_type=WireType.EPOCH,
                unit=WireUnit.MS,
            ),
            Field(wire_key="size", column="Size", wire_type=WireType.LONG),
            Field(wire_key="conditions", column="Conditions", wire_type=WireType.LONG),
        ),
    )
    rest = Source(
        key="/v3/trades/{stockTicker}",
        transport=R,
        fields=(
            Field(wire_key="size", column="Size", wire_type=WireType.DOUBLE),
            Field(wire_key="conditions", column="Conditions", wire_type=WireType.LONG_ARRAY),
        ),
    )
    tables = {FF: {SYNTHETIC_FILES: files}, R: {rest.key: rest}}
    return Catalog((SYNTHETIC,), source_tables=lambda t: tables.get(t, {}))


@pytest.mark.parametrize(("hours", "kept"), [(23, 0), (24, 1)], ids=["open", "ended"])
def test_a_bar_is_kept_only_when_it_ends_by_the_window_s_latest_end(hours: int, kept: int) -> None:
    midnight = datetime(2025, 3, 3, 5, tzinfo=UTC)  # 00:00 in New York
    times = TimeRange(midnight, midnight + timedelta(days=1))
    day = DaySpan(date(2025, 3, 3), date(2025, 3, 3))
    bars = BarBounds(midnight, midnight + timedelta(hours=hours), StartRule.RANGE)
    window = HistoryWindow(day, times, None, times, bars, now=midnight + timedelta(days=2))
    transform = flatfiles._Transform.build(
        CATALOG,
        STOCK_BARS,
        prefix="us_stocks_sip/day_aggs_v1",
        request=request(timespan=Timespan.DAY, adjusted=False),
        window=window,
        unreadable_symbols=dm.OnError.RAISE,
        warnings=WARNINGS,
    )
    batch = pa.RecordBatch.from_pydict(
        {
            "Symbol": ["AAPL"],
            "Volume": [1.0],
            "Open": [1.0],
            "Close": [1.0],
            "High": [1.0],
            "Low": [1.0],
            "TimestampStart": [epoch_ns(midnight)],
            "Transactions": [1],
        }
    )
    assert transform.apply(batch, received=7, key=STOCK_KEY)[0].num_rows == kept


def test_an_unreadable_spelling_under_raise_is_a_library_defect() -> None:
    transform = flatfiles._Transform.build(
        synthetic_catalog(),
        SYNTHETIC,
        prefix=SYNTHETIC_FILES,
        request=request(dm.ALL),
        window=whole_days(SYNTHETIC, "2025-03-03"),
        unreadable_symbols=dm.OnError.RAISE,
        warnings=WARNINGS,
    )
    batch = pa.RecordBatch.from_pydict(
        {
            "Symbol": ["!!"],
            "Timestamp": [1741012200000000123],
            "ParticipantTimestamp": [1741012199999],
            "Size": [100],
            "Conditions": [12],
        }
    )
    with pytest.raises(dm.MassiveError, match="'!!'") as info:
        transform.apply(batch, received=7, key=STOCK_KEY)
    assert info.value.cause is dm.ErrorCause.LIBRARY_DEFECT


def test_columns_convert_by_their_unit_and_widen_to_the_merged_type() -> None:
    window = whole_days(SYNTHETIC, "2025-03-03")
    transform = flatfiles._Transform.build(
        synthetic_catalog(),
        SYNTHETIC,
        prefix=SYNTHETIC_FILES,
        request=request(dm.ALL),
        window=window,
        unreadable_symbols=dm.OnError.RAISE,
        warnings=WARNINGS,
    )
    batch = pa.RecordBatch.from_pydict(
        {
            "Symbol": ["AAPL", "MSFT"],
            "Timestamp": [1741012200000000123, 1741012200000000124],
            "ParticipantTimestamp": [1741012199999, 0],
            "Size": [100, None],
            "Conditions": [12, None],
        }
    )
    converted, drift = transform.apply(batch, received=7, key=STOCK_KEY)
    assert drift == DriftReport()
    out = converted.to_pydict()
    # Every row of the block arrived together.
    assert [int(v.value) for v in out["TimestampInsert"]] == [7, 7]
    assert [
        None if v is None else int(v.timestamp() * 10**3) for v in out["ParticipantTimestamp"]
    ] == [
        1741012199999,
        None,  # a 0 outside the time column is absent
    ]
    assert out["Size"] == [100.0, None]
    assert out["Conditions"] == [[12], None]
    assert transform.plan.required == frozenset({"ticker", "sip_timestamp"})


def test_a_size_beyond_2_53_widened_to_a_double_is_served_rounded_and_reported() -> None:
    transform = flatfiles._Transform.build(
        synthetic_catalog(),
        SYNTHETIC,
        prefix=SYNTHETIC_FILES,
        request=request(dm.ALL),
        window=whole_days(SYNTHETIC, "2025-03-03"),
        unreadable_symbols=dm.OnError.RAISE,
        warnings=WARNINGS,
    )
    batch = pa.RecordBatch.from_pydict(
        {
            "Symbol": ["AAPL", "MSFT"],
            "Timestamp": [1741012200000000123, 1741012200000000124],
            "ParticipantTimestamp": [1741012199999, 1741012199999],
            "Size": [2**53 + 1, 100],
            "Conditions": [12, 12],
        }
    )
    converted, drift = transform.apply(batch, received=7, key=STOCK_KEY)
    assert converted.column("Size").to_pylist() == [float(2**53), 100.0]
    assert drift == DriftReport(rounded=frozenset({"size"}))
    assert drift.samples["size"].value == str(2**53 + 1)


def test_milliseconds_in_a_nanosecond_column_read_as_null_with_a_warning(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Milliseconds in a column documented in nanoseconds land in January 1970: never served.
    row = trade(1, 1741012200000000000, participant=1741012200000)
    fake_s3.put(TRADES_0303, make_csv_gz(TRADE_HEADER + row + trade(2, 1741012200000000001)))
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        t = q.history("2025-03-03", "2025-03-03")
    assert pick(t, "Id", "ParticipantTimestamp") == [("1", None), ("2", 1741012200000000001)]
    [warning] = [r.getMessage() for r in caplog.records if r.name == DRIFT_LOGGER]
    assert warning == (
        "stocks.trades: FLAT_FILES field 'participant_timestamp' held a value that does not fit "
        f"its type; it reads as null (schema drift) at {LOCATION.format(TRADES_0303)}; first "
        "value 1741012200000"
    )


def test_a_wrong_unit_in_the_time_column_drops_its_row_with_a_count(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    rows = trade(1, 1741012200000) + trade(2, 1741012200000000001) + trade(3, 5)
    fake_s3.put(TRADES_0303, make_csv_gz(TRADE_HEADER + rows))
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        t = query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    assert pick(t, "Id") == [("2",)]
    [warning] = [r.getMessage() for r in caplog.records if r.name == DRIFT_LOGGER]
    assert warning == (
        f"stocks.trades: dropped 2 FLAT_FILES row(s) whose key cannot be read, the first at "
        f"{LOCATION.format(TRADES_0303)}: timestamp unit mismatch for "
        "stocks.trades.Timestamp: 1741012200000 ns is outside 1990 to 2100; such rows are "
        "dropped and counted: the running count is logged at DEBUG and shown in the "
        "SchemaDrift of each live table that lost them"
    )


def test_malformed_lines_are_skipped_with_one_warning_per_file(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A line with too few fields and one with too many lose only themselves; the warning
    # names the first one's row and field count, never its text.
    rows = trade(1, 1741012200000000001) + "SECRET,7\n" + trade(2, 1741012200000000002)
    rows += "12,,4,3,1,1.5,3,1741012200000000003,10,1,AAPL,,,SECRET\n"
    fake_s3.put(TRADES_0303, make_csv_gz(TRADE_HEADER + rows))
    with caplog.at_level(logging.WARNING, logger=flatfiles.__name__):
        t = query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    assert pick(t, "Id") == [("1",), ("2",)]
    [warning] = [r.getMessage() for r in caplog.records if r.name == flatfiles.__name__]
    assert warning == (
        f"stocks.trades: skipped 2 malformed line(s) of {LOCATION.format(TRADES_0303)} that do "
        "not split into the header's fields, the first: row 2 after the header has 2 fields "
        "where the header has 13; the file's other rows are kept"
    )
    assert "SECRET" not in caplog.text


def test_a_file_whose_every_line_is_malformed_fails_the_call(
    *, session: dm.MassiveSession, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    fake_s3.put(TRADES_0303, make_csv_gz(TRADE_HEADER + "AAPL,1\nAAPL,2\n"))
    with pytest.raises(
        dm.MassiveError,
        match=rf"^cannot read {re.escape(LOCATION.format(TRADES_0303))}: every one of its 2 "
        r"lines is malformed, so the file's format may have changed; the first: row 1 after the "
        r"header has 2 fields where the header has 13$",
    ):
        query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")


def test_a_file_whose_every_time_is_in_a_wrong_unit_fails_the_call(
    *, session: dm.MassiveSession, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    # Every row dropped is a change in the file's format, not one odd row.
    fake_s3.put(TRADES_0303, make_csv_gz(TRADE_HEADER + trade(1, 1741012200000)))
    with pytest.raises(
        dm.MassiveError,
        match=rf"^cannot read {re.escape(LOCATION.format(TRADES_0303))}: every one of the 1 "
        r"rows of the requested symbols was dropped, so the file's format may have changed; the "
        r"first: timestamp unit mismatch for stocks\.trades\.Timestamp: 1741012200000 ns is "
        r"outside 1990 to 2100$",
    ) as info:
        query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    assert info.value.cause is dm.ErrorCause.LIBRARY_DEFECT


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
    # Futures sessions are not filtered on Timestamp, so a 0 there reaches the routine, which
    # keeps it in the time column, where it fails the bound and drops its row.
    fake_s3.put(
        FUTURES_0304.format("cme"),
        make_csv_gz(
            FUTURES_HEADER
            + ",0,0,1,1,1,2025-03-04,ESH5,0\n"
            + ",0,0,1,1,2,2025-03-04,ESH5,1741042800000000000\n"
        ),
    )
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    t = q.history("2025-03-04", "2025-03-04")
    assert pick(t, "ReportSequence") == [(2,)]


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
    q = query(session, STOCK_TRADES, symbols=["AAPL"])
    t = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert pick(t, "Symbol", "Id") == [("AAPL", "1"), ("AAPL", "3")]


def test_a_ticker_the_library_cannot_read_fails_the_read_under_raise(
    *, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes], clock: Callable[[datetime], None]
) -> None:
    rows = trade(1, 1741012200000000000, "BRK/B") + trade(2, 1741012200000000000, "AAPL")
    fake_s3.put(TRADES_0303, make_csv_gz(TRADE_HEADER + rows))
    with dm.MassiveSession(config(unreadable_symbols=dm.OnError.RAISE)) as m:
        every = query(m, STOCK_TRADES, symbols=dm.ALL)
        with pytest.raises(
            dm.MassiveError, match=r"^stocks\.trades: Massive sent the symbol 'BRK/B', a spelling"
        ):
            every.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)


def test_a_ticker_the_library_cannot_read_is_kept_by_default(
    *, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes], clock: Callable[[datetime], None]
) -> None:
    rows = trade(1, 1741012200000000000, "BRK/B") + trade(2, 1741012200000000000, "AAPL")
    fake_s3.put(TRADES_0303, make_csv_gz(TRADE_HEADER + rows))
    with dm.MassiveSession(config()) as m:
        t = query(m, STOCK_TRADES, symbols=dm.ALL).history(
            "2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES
        )
        assert pick(t, "Symbol", "RawSymbol", "Id") == [(None, "BRK/B", "1"), ("AAPL", None, "2")]


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
    convert = columns.dataset_table

    def counted(data: pa.Table, dates: Sequence[str]) -> Table:
        converted.append(data.num_rows)
        return convert(data, dates)

    monkeypatch.setattr(flatfiles, "dataset_table", counted)
    q = query(session, STOCK_TRADES, symbols=["ZZZZ"])
    t = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert t.size == 0
    assert {c.name: c.data_type for c in t.columns} == col_defs(schema_of(STOCK_TRADES))
    assert converted == [0]  # only the empty schema table: no table of an empty block


def test_each_file_logs_its_rows_after_each_step(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=["ZZZZ"])
    with caplog.at_level(logging.DEBUG, logger=flatfiles.__name__):
        q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    # The empty table is explained: the file held rows, none of the requested symbol.
    (line,) = [r.getMessage() for r in caplog.records if "rows read" in r.getMessage()]
    assert line.startswith(f"stocks.trades: s3://flatfiles/{TRADES_0303} at ")
    assert line.endswith(
        ": 4 rows read, 0 of the requested symbols, 0 in the window, 0 kept as whole bars"
    )


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
    # The day file stamps a bar at the 16:00 close; it runs from midnight to midnight.
    day = 1740978000000000000
    assert pick(
        t, "Symbol", "Date", "TimestampStart", "TimestampEnd", "Open", "Volume", "Transactions"
    ) == [
        ("AAPL", "2025-03-03", day, day + 86_400 * 10**9, 241.79, 47184000.0, 612403),
        ("MSFT", "2025-03-03", day, day + 86_400 * 10**9, 398.82, 26093000.0, 402122),
    ]
    assert pick(t, "Vwap") == [(None,), (None,)]
    assert caplog.messages == [
        "stocks.bars history: AUTO chose FLAT_FILES (every symbol)",
        # REST's grouped-daily bars could have served it, with the columns the file lacks.
        (
            "stocks.bars history: AUTO read flat files (every symbol); the read leaves Otc, "
            "Vwap null, which the other source, REST, fills: pass "
            "source=dm.HistorySource.REST to get them (logged once per session)"
        ),
        "stocks.bars history: source=S3 files=1 skipped=none unread=none",
    ]
    caplog.clear()
    with caplog.at_level(logging.INFO, logger=LOGGER):
        q.history("2025-03-03", "2025-03-03")
    # Once per session: the second read logs its choice and its files, and no WARNING.
    assert [r.levelno for r in caplog.records] == [logging.INFO, logging.INFO]


def test_each_file_becomes_one_table(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    put(TRADES_0305, "us_stocks_sip_trades_v1_2025-03-05.csv")
    converted: list[int] = []
    convert = columns.dataset_table

    def counted(data: pa.Table, dates: Sequence[str]) -> Table:
        converted.append(data.num_rows)
        return convert(data, dates)

    monkeypatch.setattr(flatfiles, "dataset_table", counted)
    monkeypatch.setattr(flatfiles, "BLOCK_SIZE", 256)  # several blocks per file
    t = query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-05")
    assert t.size == 6
    # The empty schema table, then one table per file, although each file has many blocks.
    assert sorted(converted) == [0, 2, 4]


def test_every_block_carries_the_instant_it_was_decoded(
    *,
    session: dm.MassiveSession,
    many_blocks: Callable[[str], int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Each block's rows share the instant the block was decoded; later blocks, later instants.
    blocks = many_blocks(TRADES_0303)
    ticks = itertools.count(1_000)
    monkeypatch.setattr(time, "time_ns", lambda: next(ticks))
    t = query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    arrived = [row["TimestampInsert"] for row in rows(t, arrival=True)]
    assert len(arrived) == 4000
    assert arrived == sorted(arrived)  # one reader reads the file's blocks in order
    # The reader parses the file in chunks of 16 blocks cut at a line end, each of which may
    # end in a short block of its own.
    chunks = blocks // 16 + 1
    assert 1 < len(set(arrived)) <= blocks + chunks + 1


def test_a_file_with_many_surviving_blocks_is_split_into_bounded_tables(
    *,
    session: dm.MassiveSession,
    many_blocks: Callable[[str], int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    blocks = many_blocks(TRADES_0303)
    converted: list[int] = []
    convert = columns.dataset_table

    def counted(data: pa.Table, dates: Sequence[str]) -> Table:
        converted.append(data.num_rows)
        return convert(data, dates)

    monkeypatch.setattr(flatfiles, "dataset_table", counted)
    monkeypatch.setattr(flatfiles, "_TABLE_BLOCKS", 2)
    t = query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    assert pick(t, "Id")[:3] == [("0",), ("1",), ("2",)]  # still in file order
    assert t.size == 4000
    assert len(converted) > 4  # the file was split, so no table holds the whole file
    assert len(converted) < blocks  # ... but into far fewer tables than blocks


def holiday_warning(spec: str, key: str, day: str) -> str:
    return (
        f"{spec}: {key} does not exist, though a later day's file does; skipping {day}, a day "
        "without trading (a holiday?)"
    )


def s3_line(spec: str, files: int, skipped: str) -> str:
    return f"{spec} history: source=S3 files={files} skipped={skipped} unread=none"


def not_published(spec: str, day: str, remedy: str) -> str:
    return (
        f"{spec}: the flat file for {day} is not published yet. Massive usually publishes a "
        f"day's flat files by about 11:00 ET the next day, but the time varies. {remedy}"
    )


READ_REST = "Retry later, or read REST with source=dm.HistorySource.REST"
NO_REST = (
    "Retry later; REST cannot serve this read: futures.trades history over REST needs explicit "
    "symbols"
)
MANY = [f"S{n:02d}" for n in range(49)] + ["AAPL"]
"""Fifty symbols: enough for `AUTO` to read flat files, and explicit, so REST can serve them."""


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
        (logging.WARNING, holiday_warning("stocks.trades", TRADES_0304, "2025-03-04")),
        (logging.INFO, s3_line("stocks.trades", 2, "2025-03-04")),
    ]


def test_a_missing_last_weekday_with_a_later_file_is_a_day_without_trading(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
    clock: Callable[[datetime], None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Friday 03-07 is missing; the look after the range skips the weekend and finds Monday's
    # file, so Friday was a day without trading. Monday's file is only opened, never read.
    clock(datetime(2025, 3, 12, 16, tzinfo=UTC))
    thursday = epoch_ns(datetime(2025, 3, 6, 15, tzinfo=UTC))
    fake_s3.put(TRADES_KEY.format(6), make_csv_gz(TRADE_HEADER + trade(1, thursday)))
    fake_s3.put(TRADES_KEY.format(10), make_csv_gz(TRADE_HEADER + trade(2, thursday)))
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with caplog.at_level(logging.INFO, logger=LOGGER):
        t = q.history("2025-03-06", "2025-03-08", source=HistorySource.FLAT_FILES)
    assert pick(t, "Id") == [("1",)]
    assert sorted(fake_s3.keys()) == [TRADES_KEY.format(d) for d in (6, 7, 8, 10)]
    assert TRADES_KEY.format(10) in fake_s3.closed_bodies
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (
            logging.INFO,
            (
                f"stocks.trades: {TRADES_KEY.format(7)} is missing and {TRADES_KEY.format(10)} "
                "exists, so it was a day without trading"
            ),
        ),
        (logging.WARNING, holiday_warning("stocks.trades", TRADES_KEY.format(7), "2025-03-07")),
        (logging.INFO, s3_line("stocks.trades", 1, "2025-03-07,2025-03-08")),
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
        s3_line("stocks.trades", 1, "2025-03-01,2025-03-02"),
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
        (logging.INFO, s3_line("stocks.trades", 3, "2025-03-08"))
    ]


def test_a_weekend_only_range_is_an_empty_table(
    *, session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with caplog.at_level(logging.INFO, logger=LOGGER):
        t = q.history("2025-03-01", "2025-03-02", source=HistorySource.FLAT_FILES)
    assert t.size == 0
    assert {c.name: c.data_type for c in t.columns} == col_defs(schema_of(STOCK_TRADES))
    assert caplog.messages == [s3_line("stocks.trades", 0, "2025-03-01,2025-03-02")]


def test_a_missing_last_day_with_no_later_file_raises_under_flat_files(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    fake_s3: FakeS3,
) -> None:
    # On Monday 03-10 no file after 03-04 exists, so 03-04 may simply not be out yet. The
    # look stops before today, whose file cannot exist.
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=["AAPL"])
    with pytest.raises(
        dm.MassiveError,
        match=re.escape(not_published("stocks.trades", "2025-03-04", READ_REST)) + "$",
    ):
        q.history("2025-03-03", "2025-03-04", source=HistorySource.FLAT_FILES)
    assert sorted(fake_s3.keys()) == [TRADES_KEY.format(d) for d in (3, 4, 5, 6, 7)]


def test_a_long_run_of_missing_files_well_before_today_has_no_file_at_all(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
) -> None:
    # Nine trading days are missing at the end of a range read three weeks later. The look
    # runs a week past the range's end, not past the run's first day, and with no file there
    # either the days have no file at all: retrying later would never help.
    clock(datetime(2025, 4, 7, 16, tzinfo=UTC))
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=["AAPL"])
    missing = [d for d in range(4, 15) if date(2025, 3, d).weekday() < 5]
    days = ", ".join(f"2025-03-{d:02d}" for d in missing)
    with pytest.raises(
        dm.MassiveError,
        match=re.escape(
            f"stocks.trades: no flat file exists for {days}, nor for any later day up to a "
            "week after the range; Massive publishes a day's file the next day, so these files "
            "will not appear. To read them, read REST with source=dm.HistorySource.REST, or end "
            "the range before them"
        )
        + "$",
    ):
        q.history("2025-03-03", "2025-03-14", source=HistorySource.FLAT_FILES)
    looked = [17, 18, 19, 20, 21]  # the weekdays of the week after the range
    assert sorted(fake_s3.keys()) == [TRADES_KEY.format(d) for d in [*range(3, 15), *looked]]


def test_a_refused_look_for_a_later_file_raises(
    *, session: dm.MassiveSession, put: Callable[[str, str], None], fake_s3: FakeS3
) -> None:
    # The look after a missing last day fails as any read does: here S3 refuses the key.
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    fake_s3.refuse(TRADES_0305, status=403, code="AccessDenied", message="Access Denied")
    q = query(session, STOCK_TRADES, symbols=["AAPL"])
    with pytest.raises(dm.EntitlementError, match=re.escape(TRADES_0305)):
        q.history("2025-03-03", "2025-03-04", source=HistorySource.FLAT_FILES)


def test_a_missing_day_rest_cannot_serve_raises_under_auto(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    remedy = (
        "Retry later; REST cannot serve this read: stocks.trades history over REST needs "
        "explicit symbols"
    )
    with pytest.raises(
        dm.MassiveError,
        match=re.escape(not_published("stocks.trades", "2025-03-04", remedy)) + "$",
    ):
        q.history("2025-03-03", "2025-03-04")


def test_auto_reads_the_days_not_published_yet_from_rest(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Flat files serve 03-03; 03-04 has no file and no later one, so REST reads it, and the
    # call returns the whole range: the file's AAPL rows, then REST's.
    rest = RestRecorder()
    monkeypatch.setattr(session._core, "rest_transport", lambda: rest)
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=MANY)
    with caplog.at_level(logging.INFO, logger=LOGGER):
        t = q.history("2025-03-03", "2025-03-04")
    assert pick(t, "Symbol") == [("AAPL",), ("AAPL",), ("from REST",)]
    ((spec, req, window),) = rest.calls
    assert spec is STOCK_TRADES
    assert (req.start, req.end) == (date(2025, 3, 4), date(2025, 3, 4))
    assert req.symbols == tuple(sorted(MANY))
    assert window.span == DaySpan(date(2025, 3, 4), date(2025, 3, 4))
    assert caplog.messages[-1].startswith(
        "stocks.trades history: AUTO read REST because the flat files are missing for these "
        "days: 1 [2025-03-04]"
    )
    assert t.attributes()["TableDescription"].endswith(
        "Read from flat files, and from REST for 2025-03-04, whose files are missing."
    )


def test_auto_reads_a_missing_weekday_mid_range_from_rest_in_date_order(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    fake_s3: FakeS3,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # REST can serve the read, so a missing 03-04 between two files is never taken for a
    # holiday: REST reads that one day, and its rows land between the two files' rows.
    rest = RestRecorder()
    monkeypatch.setattr(session._core, "rest_transport", lambda: rest)
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    put(TRADES_0305, "us_stocks_sip_trades_v1_2025-03-05.csv")
    q = query(session, STOCK_TRADES, symbols=MANY)
    t = q.history("2025-03-03", "2025-03-05")
    assert pick(t, "Symbol", "Id") == [
        ("AAPL", "1"),
        ("AAPL", "3"),
        ("from REST", None),
        ("AAPL", "9"),
    ]
    ((_, req, _),) = rest.calls
    assert (req.start, req.end) == (date(2025, 3, 4), date(2025, 3, 4))
    # Nothing was looked for after the range: no holiday is inferred.
    assert sorted(fake_s3.keys()) == [TRADES_0303, TRADES_0304, TRADES_0305]


def test_auto_reads_rest_from_the_unpublished_days_start_within_instants(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rest = RestRecorder()
    monkeypatch.setattr(session._core, "rest_transport", lambda: rest)
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=MANY)
    start = datetime(2025, 3, 3, 9, tzinfo=NEW_YORK)
    end = datetime(2025, 3, 4, 12, tzinfo=NEW_YORK)
    q.history(start, end)
    ((_, req, window),) = rest.calls
    # The day's first instant, not the call's start: the earlier hours came from the file.
    assert (req.start, req.end) == (datetime(2025, 3, 4, tzinfo=NEW_YORK), end)
    assert window.span == TimeRange(datetime(2025, 3, 4, tzinfo=NEW_YORK), end)


def test_a_missing_weekend_file_of_a_class_that_trades_at_weekends_warns(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Forex trades from Sunday evening UTC, so its Sunday is a trading day like crypto's: a
    # missing Sunday file before Monday's is reported as a day without trading, while its
    # Saturday, a day without trading, is skipped silently.
    forex = replace(CRYPTO_TRADES, name="forex.trades", asset_class=AssetClass.FOREX)
    key = "global_crypto/trades_v1/2025/03/2025-03-{:02d}.csv.gz"
    put(key.format(3), "global_crypto_trades_v1_2025-03-01.csv")
    q = query(session, forex, symbols=dm.ALL)
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        q.history("2025-03-01", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert caplog.messages == [holiday_warning("forex.trades", key.format(2), "2025-03-02")]


def test_a_missing_forex_saturday_file_is_skipped_silently_even_as_the_last_day(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    clock: Callable[[datetime], None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Forex does not trade on a UTC Saturday, so its file need not exist: no WARNING, and the
    # day is never taken for one not published yet, even before any later file can exist.
    forex = replace(CRYPTO_TRADES, name="forex.trades", asset_class=AssetClass.FOREX)
    put(
        "global_crypto/trades_v1/2025/02/2025-02-28.csv.gz",
        "global_crypto_trades_v1_2025-03-01.csv",
    )
    clock(datetime(2025, 3, 2, 12, tzinfo=NEW_YORK))
    q = query(session, forex, symbols=dm.ALL)
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        q.history("2025-02-28", "2025-03-01", source=HistorySource.FLAT_FILES)
    assert caplog.messages == []


def test_crypto_trades_every_day(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    clock: Callable[[datetime], None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    saturday = "global_crypto/trades_v1/2025/03/2025-03-01.csv.gz"
    put(saturday, "global_crypto_trades_v1_2025-03-01.csv")
    q = query(session, CRYPTO_TRADES, symbols=["X:BTCUSD"])
    t = q.history("2025-03-01", "2025-03-01", source=HistorySource.FLAT_FILES)
    assert pick(t, "Symbol", "Timestamp", "Size", "Id") == [
        ("X:BTCUSD", 1740830400000000000, 0.015, "a1")
    ]
    # A missing Sunday file is no weekend for crypto: with no later file yet, it may not be
    # out yet ...
    clock(datetime(2025, 3, 3, 12, tzinfo=NEW_YORK))
    with pytest.raises(dm.MassiveError, match=r"the flat file for 2025-03-02 is not published"):
        q.history("2025-03-01", "2025-03-02", source=HistorySource.FLAT_FILES)
    clock(NOW)
    caplog.clear()
    # ... and a missing day before an existing one was a day without trading, which warns.
    put(
        "global_crypto/trades_v1/2025/03/2025-03-02.csv.gz",
        "global_crypto_trades_v1_2025-03-01.csv",
    )
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        q.history("2025-02-28", "2025-03-02", source=HistorySource.FLAT_FILES)
    assert caplog.messages == [
        holiday_warning(
            "crypto.trades", "global_crypto/trades_v1/2025/02/2025-02-28.csv.gz", "2025-02-28"
        )
    ]


class RestRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple[DatasetSpec, Request, HistoryWindow]] = []

    def history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        call: int,
        stop: threading.Event | None = None,
    ) -> Table:
        assert call > 0
        assert stop is None  # a blocking read has no stop token
        self.calls.append((spec, request, window))
        # A table in the dataset's schema, as the REST transport returns: one row at the
        # window's first instant, so a merge sorted by time places it.
        row: dict[str, object] = {"Symbol": "from REST", "TimestampInsert": 1}
        if window.times is not None:
            row[row_times.time_column(spec.times)] = window.times.start_ns
        return dh_arrow.to_table(schema_of(spec).to_arrow([row]))


def test_auto_delegates_few_symbols_to_rest(
    *,
    session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    fake_s3: FakeS3,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest = RestRecorder()
    monkeypatch.setattr(session._core, "rest_transport", lambda: rest)
    q = query(session, STOCK_TRADES, symbols=["MSFT", "AAPL"])
    with caplog.at_level(logging.INFO, logger=LOGGER):
        t = q.history("2025-03-03", "2025-03-04")
    assert pick(t, "Symbol") == [("from REST",)]
    ((spec, req, window),) = rest.calls
    assert spec is STOCK_TRADES
    assert req.symbols == ("AAPL", "MSFT")
    assert (req.start, req.end) == (date(2025, 3, 3), date(2025, 3, 4))
    assert window.span == DaySpan(date(2025, 3, 3), date(2025, 3, 4))
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
    q = session.futures.trades(["ESH5"])  # every venue by default
    t = q.history("2025-03-04", "2025-03-04", source=HistorySource.FLAT_FILES)
    assert sorted(fake_s3.keys()) == sorted(FUTURES_0304.format(v) for v in VENUES)
    # Venue order within a date: CME, CBOT, NYMEX, COMEX.
    assert pick(t, "ReportSequence", "Date") == [
        (0, "2025-03-04"),
        (1, "2025-03-04"),
        (2, "2025-03-04"),
        (3, "2025-03-04"),
    ]


def test_futures_read_the_venues_asked_for_in_venue_order(
    *, session: dm.MassiveSession, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    for sequence, venue in enumerate(VENUES):
        row = f",1.0,1,1,1,{sequence},2025-03-04,ESH5,1741042800000000000\n"
        fake_s3.put(FUTURES_0304.format(venue), make_csv_gz(FUTURES_HEADER + row))
    q = session.futures.trades(["ESH5"], venues={FuturesVenue.COMEX, FuturesVenue.CME})
    t = q.history("2025-03-04", "2025-03-04", source=HistorySource.FLAT_FILES)
    assert sorted(fake_s3.keys()) == sorted(FUTURES_0304.format(v) for v in ("cme", "comex"))
    assert pick(t, "ReportSequence", "Date") == [(0, "2025-03-04"), (3, "2025-03-04")]


def test_futures_read_one_venue_when_asked(
    *, session: dm.MassiveSession, put: Callable[[str, str], None], fake_s3: FakeS3
) -> None:
    put(FUTURES_0304.format("cme"), "us_futures_trades_v1_2025-03-04.csv")
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    t = q.history("2025-03-04", "2025-03-04")
    assert fake_s3.keys() == [FUTURES_0304.format("cme")]
    assert pick(t, "Symbol", "Timestamp", "Date", "ReportSequence") == [
        ("ESH5", 1741042800000000000, "2025-03-04", 31),
        ("ESH5", 1741100400000000000, "2025-03-04", 32),
        ("NQH5", 1741100400000000000, "2025-03-04", 33),
    ]


def test_futures_evening_instants_read_only_the_next_session(
    *, session: dm.MassiveSession, put: Callable[[str, str], None], fake_s3: FakeS3
) -> None:
    put(FUTURES_0304.format("cme"), "us_futures_trades_v1_2025-03-04.csv")
    q = query(session, FUTURES_TRADES, symbols=["ESH5"], venues={FuturesVenue.CME})
    # 17:00 Chicago on March 3 already trades in the session that ends on March 4.
    t = q.history(
        datetime(2025, 3, 3, 17, tzinfo=CHICAGO),
        datetime(2025, 3, 3, 18, tzinfo=CHICAGO),
        source=HistorySource.FLAT_FILES,
    )
    assert fake_s3.keys() == [FUTURES_0304.format("cme")]
    assert pick(t, "Symbol", "Timestamp", "Date") == [("ESH5", 1741042800000000000, "2025-03-04")]


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
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
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
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    t = q.history(
        datetime(2024, 12, 24, 10, tzinfo=CHICAGO), datetime(2024, 12, 24, 11, tzinfo=CHICAGO)
    )
    assert t.size == 1
    assert fake_s3.keys() == [key]


def test_futures_instants_on_a_holiday_read_the_next_session(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    make_csv_gz: Callable[[str], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # CME traded Martin Luther King Day (Monday 2025-01-20) from Sunday 17:00 Chicago time
    # under the trade date 2025-01-21: no 2025-01-20 file exists, and the rows are in the next.
    clock(datetime(2025, 2, 10, tzinfo=UTC))
    sunday = epoch_ns(datetime(2025, 1, 19, 18, tzinfo=CHICAGO))
    tuesday = epoch_ns(datetime(2025, 1, 21, 9, tzinfo=CHICAGO))
    rows = "".join(f",1.0,1,1,1,1,2025-01-21,ESH5,{moment}\n" for moment in (sunday, tuesday))
    key = FUTURES_KEY.format("cme", date(2025, 1, 21))
    fake_s3.put(key, make_csv_gz(FUTURES_HEADER + rows))
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    with caplog.at_level(logging.INFO, "deephaven_massive._ingest.flatfiles"):
        t = q.history(
            datetime(2025, 1, 19, 17, tzinfo=CHICAGO),
            datetime(2025, 1, 20, 12, tzinfo=CHICAGO),
            source=HistorySource.FLAT_FILES,
        )
    assert pick(t, "Timestamp") == [(sunday,)]  # the range keeps only its own instants
    assert fake_s3.keys() == [FUTURES_KEY.format("cme", date(2025, 1, 20)), key]
    assert "read for the hours a holiday moved into it" in caplog.text


def test_a_stopped_read_stops_its_roll_forward(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    make_csv_gz: Callable[[str], bytes],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Martin Luther King Day again: the pool has read the missing 01-20 file when the
    # background read is released, so the roll forward reads nothing more and the call
    # reports the cancellation.
    clock(datetime(2025, 2, 10, tzinfo=UTC))
    tuesday = epoch_ns(datetime(2025, 1, 21, 9, tzinfo=CHICAGO))
    rolled = FUTURES_KEY.format("cme", date(2025, 1, 21))
    fake_s3.put(rolled, make_csv_gz(FUTURES_HEADER + f",1.0,1,1,1,1,2025-01-21,ESH5,{tuesday}\n"))
    stop = _massive.Cancel()
    warn = flatfiles._warn_if_none_exist

    def released_as_the_pool_ends(
        spec: DatasetSpec, keys: Sequence[flatfiles._Key], missing: Sequence[flatfiles._Key]
    ) -> None:
        stop.set()
        warn(spec, keys, missing)

    monkeypatch.setattr(flatfiles, "_warn_if_none_exist", released_as_the_pool_ends)
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    start = datetime(2025, 1, 20, 8, tzinfo=CHICAGO)
    end = datetime(2025, 1, 20, 12, tzinfo=CHICAGO)
    window = history_window(FUTURES_TRADES, q._request, start=start, end=end)
    dates = history_source.partition_dates(FUTURES_TRADES, window)
    with pytest.raises(dm.MassiveError, match=r"futures\.trades history: the read was cancelled"):
        session._core.flat_file_transport().history(
            FUTURES_TRADES, q._request, window, dates=dates, call=1, infer_holidays=True, stop=stop
        )
    assert fake_s3.keys() == [FUTURES_KEY.format("cme", date(2025, 1, 20))]


def test_a_venue_not_out_yet_sends_the_day_of_every_venue_to_rest(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # CME's file of 03-04 exists, CBOT's does not and no later one does: REST reads 03-04 for
    # every venue, so CME's rows of that day are dropped rather than returned twice.
    rest = RestRecorder()
    monkeypatch.setattr(session._core, "rest_transport", lambda: rest)
    put(FUTURES_KEY.format("cme", date(2025, 3, 4)), "us_futures_trades_v1_2025-03-04.csv")
    venues = {FuturesVenue.CME, FuturesVenue.CBOT}
    contracts = [f"E{n:02d}H5" for n in range(49)] + ["ESH5"]
    q = query(session, FUTURES_TRADES, symbols=contracts, venues=venues)
    t = q.history("2025-03-04", "2025-03-04")
    assert pick(t, "Symbol") == [("from REST",)]
    ((_, req, _),) = rest.calls
    assert req.start == date(2025, 3, 4)


@pytest.mark.parametrize("stopped_at", [2, 3])
def test_a_read_stopped_while_a_roll_forward_rereads_reports_the_cancellation(
    stopped_at: int,
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    make_csv_gz: Callable[[str], bytes],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The next session's file drops its connection on every read, and the background read is
    # released while the roll forward waits for its last reread (2), or during that last read,
    # once the rereads are spent (3): the call reports the stop, not the dropped connection.
    clock(datetime(2025, 2, 10, tzinfo=UTC))
    tuesday = epoch_ns(datetime(2025, 1, 21, 9, tzinfo=CHICAGO))
    rolled = FUTURES_KEY.format("cme", date(2025, 1, 21))
    data = make_csv_gz(FUTURES_HEADER + f",1.0,1,1,1,1,2025-01-21,ESH5,{tuesday}\n")
    fake_s3.put_dropped(rolled, data[: len(data) // 2])
    stop = _massive.Cancel()
    # The session's client class itself: the session fixture wraps `_massive.FlatFileClient`.
    transport = cast("flatfiles.FlatFileHistory", session._core.flat_file_transport())
    client_class = type(transport._client)
    wait_to_reread = client_class.wait_to_reread
    waits: list[int] = []

    def released_while_rereading(
        self: _massive.FlatFileClient, error: _massive.WireError, *args: object
    ) -> bool:
        retry, cancel = cast("tuple[int, threading.Event]", args)
        waits.append(retry)
        if retry == stopped_at:
            stop.set()
        return bool(wait_to_reread(self, error, retry, cancel))

    monkeypatch.setattr(client_class, "wait_to_reread", released_while_rereading)
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    start = datetime(2025, 1, 20, 8, tzinfo=CHICAGO)
    end = datetime(2025, 1, 20, 12, tzinfo=CHICAGO)
    window = history_window(FUTURES_TRADES, q._request, start=start, end=end)
    dates = history_source.partition_dates(FUTURES_TRADES, window)
    with pytest.raises(dm.MassiveError, match=r"futures\.trades history: the read was cancelled"):
        session._core.flat_file_transport().history(
            FUTURES_TRADES, q._request, window, dates=dates, call=1, infer_holidays=True, stop=stop
        )
    assert waits == list(range(stopped_at + 1))


def test_an_early_opened_session_and_rest_s_holiday_never_repeat_a_row(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
    clock: Callable[[datetime], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Martin Luther King Day, Monday 2025-01-20, has no file; REST reads its session,
    # [Sunday 17:00, Monday 17:00) Chicago. The 01-21 file also holds the hours from Sunday
    # 17:00 on, since that session opened early, so it is trimmed to its own session's hours:
    # every instant comes from one source, and the rows come out in time order.
    rest = RestRecorder()
    monkeypatch.setattr(session._core, "rest_transport", lambda: rest)
    clock(datetime(2025, 2, 10, tzinfo=UTC))
    friday = epoch_ns(datetime(2025, 1, 17, 9, tzinfo=CHICAGO))
    sunday = epoch_ns(datetime(2025, 1, 19, 18, tzinfo=CHICAGO))
    tuesday = epoch_ns(datetime(2025, 1, 21, 9, tzinfo=CHICAGO))
    wednesday = epoch_ns(datetime(2025, 1, 22, 9, tzinfo=CHICAGO))

    def put(day: date, *moments: int) -> None:
        rows = "".join(f",1.0,1,1,1,1,{day},ESH5,{moment}\n" for moment in moments)
        fake_s3.put(FUTURES_KEY.format("cme", day), make_csv_gz(FUTURES_HEADER + rows))

    put(date(2025, 1, 17), friday)
    put(date(2025, 1, 21), sunday, tuesday)
    put(date(2025, 1, 22), wednesday)
    contracts = [f"E{n:02d}H5" for n in range(49)] + ["ESH5"]
    q = query(session, FUTURES_TRADES, symbols=contracts, venues={FuturesVenue.CME})
    t = q.history(
        datetime(2025, 1, 17, 8, tzinfo=CHICAGO), datetime(2025, 1, 22, 12, tzinfo=CHICAGO)
    )
    opens = datetime(2025, 1, 19, 17, tzinfo=CHICAGO)
    assert pick(t, "Symbol", "Timestamp") == [
        ("ESH5", friday),
        ("from REST", epoch_ns(opens)),
        ("ESH5", tuesday),
        ("ESH5", wednesday),
    ]
    ((_, req, _),) = rest.calls
    assert (req.start, req.end) == (opens, datetime(2025, 1, 20, 17, tzinfo=CHICAGO))


class DayBars:
    # Stands in for REST's day bars: one bar each of 03-03, 03-04, and 03-05, whatever the
    # window, and records each call's request and window.
    def __init__(self) -> None:
        self.calls: list[tuple[Request, HistoryWindow]] = []
        self.days: list[Sequence[date] | None] = []

    def history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        call: int,
        stop: threading.Event | None = None,
        days: Sequence[date] | None = None,
    ) -> Table:
        del call, stop
        self.calls.append((request, window))
        self.days.append(days)
        rows = [
            {
                "Symbol": "from REST",
                "Date": f"2025-03-0{day}",
                "TimestampStart": epoch_ns(datetime(2025, 3, day, tzinfo=NEW_YORK)),
                "TimestampInsert": 1,
            }
            for day in (3, 4, 5)
        ]
        return dh_arrow.to_table(schema_of(spec).to_arrow(rows))


def test_rest_serves_day_bars_by_their_date_within_a_range_of_instants(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A day bar may end after its day's instants (an index bar at 01:00 the next day) or
    # start before them (a futures session bar), so REST reads the call's own range and only
    # the missing days' bars are kept, by their Date.
    day_bars = DayBars()
    monkeypatch.setattr(session._core, "rest_transport", lambda: day_bars)
    put(
        "us_stocks_sip/day_aggs_v1/2025/03/2025-03-03.csv.gz",
        "us_stocks_sip_day_aggs_v1_2025-03-03.csv",
    )
    q = query(session, STOCK_BARS, symbols=MANY, timespan=Timespan.DAY, adjusted=False)
    # The day bars of 03-03 and 03-04 fit the range; 03-04 has no file.
    start = datetime(2025, 3, 3, tzinfo=NEW_YORK)
    end = datetime(2025, 3, 5, 12, tzinfo=NEW_YORK)
    t = q.history(start, end)
    assert pick(t, "Symbol", "Date") == [("AAPL", "2025-03-03"), ("from REST", "2025-03-04")]
    ((request, window),) = day_bars.calls
    assert (request.start, request.end) == (start, end)
    assert window.span == TimeRange(start, end)
    assert day_bars.days == [(date(2025, 3, 4),)]  # an all-symbol read requests only that day


def test_a_futures_range_with_no_later_session_within_a_week_raises(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
) -> None:
    clock(datetime(2025, 2, 10, tzinfo=UTC))
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    with pytest.raises(
        dm.MassiveError,
        # The run's only missing trading day, Monday 01-20, lies three weeks before today and
        # no session file followed it within a week: it has no file at all.
        match=re.escape(
            "futures.trades: no flat file exists for 2025-01-20, nor for any later day up to a "
            "week after the range; Massive publishes a day's file the next day, so these files "
            "will not appear. To read them, end the range before them; REST cannot serve this "
            "read: futures.trades history over REST needs explicit symbols"
        ),
    ):
        q.history(
            datetime(2025, 1, 17, 18, tzinfo=CHICAGO),  # the weekend sessions are missing too
            datetime(2025, 1, 20, 12, tzinfo=CHICAGO),
            source=HistorySource.FLAT_FILES,
        )
    # The look tries the weekdays after the range up to a week past Monday, and no further.
    looked = [date(2025, 1, d) for d in (18, 19, 20, 21, 22, 23, 24, 27)]
    assert sorted(fake_s3.keys()) == [FUTURES_KEY.format("cme", day) for day in looked]
    assert timedelta(days=7) == flatfiles.ROLL_LIMIT


def test_a_futures_range_ending_on_a_holiday_before_the_next_session_is_out_raises(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
) -> None:
    # Good Friday 2024, asked for on Easter Monday morning: the next session, 04-01's, is the
    # current one, whose file cannot exist yet, so it is not looked for.
    clock(datetime(2024, 4, 1, 12, tzinfo=UTC))
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    with pytest.raises(
        dm.MassiveError, match=re.escape(not_published("futures.trades", "2024-03-29", NO_REST))
    ):
        q.history(
            datetime(2024, 3, 29, 8, tzinfo=CHICAGO),
            datetime(2024, 3, 30, tzinfo=CHICAGO),
            source=HistorySource.FLAT_FILES,
        )
    assert sorted(fake_s3.keys()) == [FUTURES_KEY.format("cme", date(2024, 3, d)) for d in (29, 30)]


def test_a_futures_holiday_whose_next_session_is_not_out_reads_rest_under_auto(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Martin Luther King Day: on 01-22 the 01-21 file is not out yet, so the holiday's hours,
    # which lie in it, are read from REST from the range's start inside the session.
    rest = RestRecorder()
    monkeypatch.setattr(session._core, "rest_transport", lambda: rest)
    clock(datetime(2025, 1, 22, 18, tzinfo=UTC))
    contracts = [f"E{n:02d}H5" for n in range(49)] + ["ESH5"]
    q = query(session, FUTURES_TRADES, symbols=contracts, venues={FuturesVenue.CME})
    start = datetime(2025, 1, 20, 8, tzinfo=CHICAGO)
    end = datetime(2025, 1, 20, 12, tzinfo=CHICAGO)
    t = q.history(start, end)
    assert pick(t, "Symbol") == [("from REST",)]
    ((_, req, _),) = rest.calls
    assert (req.start, req.end) == (start, end)
    assert fake_s3.keys() == [FUTURES_KEY.format("cme", date(2025, 1, 20))]


def test_auto_keeps_a_sessions_row_from_the_next_days_file_when_no_day_is_missing(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    make_csv_gz: Callable[[str], bytes],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The 01-15 file holds a late row of the 01-14 session. No day is read from REST, so no
    # file is trimmed to its own session: the row is inside the range and is kept.
    rest = RestRecorder()
    monkeypatch.setattr(session._core, "rest_transport", lambda: rest)
    clock(datetime(2025, 1, 22, 18, tzinfo=UTC))
    tuesday = epoch_ns(datetime(2025, 1, 14, 9, tzinfo=CHICAGO))
    late = epoch_ns(datetime(2025, 1, 14, 15, tzinfo=CHICAGO))
    wednesday = epoch_ns(datetime(2025, 1, 15, 9, tzinfo=CHICAGO))
    row = ",1.0,1,1,1,1,{},ESH5,{}\n"
    fake_s3.put(
        FUTURES_KEY.format("cme", date(2025, 1, 14)),
        make_csv_gz(FUTURES_HEADER + row.format("2025-01-14", tuesday)),
    )
    fake_s3.put(
        FUTURES_KEY.format("cme", date(2025, 1, 15)),
        make_csv_gz(
            FUTURES_HEADER + row.format("2025-01-14", late) + row.format("2025-01-15", wednesday)
        ),
    )
    contracts = [f"E{n:02d}H5" for n in range(49)] + ["ESH5"]
    q = query(session, FUTURES_TRADES, symbols=contracts, venues={FuturesVenue.CME})
    t = q.history(
        datetime(2025, 1, 14, 8, tzinfo=CHICAGO), datetime(2025, 1, 15, 12, tzinfo=CHICAGO)
    )
    assert pick(t, "Timestamp", "Date") == [
        (tuesday, "2025-01-14"),
        (late, "2025-01-14"),
        (wednesday, "2025-01-15"),
    ]
    assert rest.calls == []


def test_auto_drops_a_files_rows_inside_a_day_rest_reads(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    make_csv_gz: Callable[[str], bytes],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The 01-14 file is missing, so REST reads that session's hours; the 01-15 file's late
    # 01-14 row lies in them and is dropped, so no row comes twice.
    rest = RestRecorder()
    monkeypatch.setattr(session._core, "rest_transport", lambda: rest)
    clock(datetime(2025, 1, 22, 18, tzinfo=UTC))
    late = epoch_ns(datetime(2025, 1, 14, 15, tzinfo=CHICAGO))
    wednesday = epoch_ns(datetime(2025, 1, 15, 9, tzinfo=CHICAGO))
    row = ",1.0,1,1,1,1,{},ESH5,{}\n"
    fake_s3.put(
        FUTURES_KEY.format("cme", date(2025, 1, 15)),
        make_csv_gz(
            FUTURES_HEADER + row.format("2025-01-14", late) + row.format("2025-01-15", wednesday)
        ),
    )
    contracts = [f"E{n:02d}H5" for n in range(49)] + ["ESH5"]
    q = query(session, FUTURES_TRADES, symbols=contracts, venues={FuturesVenue.CME})
    t = q.history(
        datetime(2025, 1, 14, 8, tzinfo=CHICAGO), datetime(2025, 1, 15, 12, tzinfo=CHICAGO)
    )
    assert ("ESH5", late) not in pick(t, "Symbol", "Timestamp")
    assert ("ESH5", wednesday) in pick(t, "Symbol", "Timestamp")
    assert ("from REST",) in pick(t, "Symbol")
    assert len(rest.calls) == 1


def test_a_holiday_before_a_weekend_end_rolls_forward(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    make_csv_gz: Callable[[str], bytes],
) -> None:
    # Good Friday 2024: CME equity futures traded from Thursday 17:00 to Friday 10:15 Chicago
    # time under the trade date 2024-04-01. A range that ends on the Saturday after finds no
    # 03-29 or 03-30 file, and its Friday rows in the 04-01 file.
    clock(datetime(2024, 4, 20, tzinfo=UTC))
    friday = epoch_ns(datetime(2024, 3, 29, 9, tzinfo=CHICAGO))
    monday = epoch_ns(datetime(2024, 4, 1, 9, tzinfo=CHICAGO))
    rows = "".join(f",1.0,1,1,1,1,2024-04-01,ESM4,{m}\n" for m in (friday, monday))
    key = FUTURES_KEY.format("cme", date(2024, 4, 1))
    fake_s3.put(key, make_csv_gz(FUTURES_HEADER + rows))
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    t = q.history(
        datetime(2024, 3, 29, 8, tzinfo=CHICAGO),
        datetime(2024, 3, 30, tzinfo=CHICAGO),
        source=HistorySource.FLAT_FILES,
    )
    assert pick(t, "Timestamp") == [(friday,)]
    # The look after the range passes over Sunday 03-31, a day without a session.
    read = [FUTURES_KEY.format("cme", date(2024, 3, d)) for d in (29, 30)] + [key]
    assert sorted(fake_s3.keys()) == sorted(read)


def test_an_ordinary_weekend_end_does_not_roll_forward(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    make_csv_gz: Callable[[str], bytes],
) -> None:
    # The Friday file exists, so the missing Saturday after it is only a weekend.
    clock(datetime(2025, 2, 10, tzinfo=UTC))
    friday = epoch_ns(datetime(2025, 1, 24, 9, tzinfo=CHICAGO))
    key = FUTURES_KEY.format("cme", date(2025, 1, 24))
    fake_s3.put(key, make_csv_gz(FUTURES_HEADER + f",1.0,1,1,1,1,2025-01-24,ESH5,{friday}\n"))
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    t = q.history(
        datetime(2025, 1, 24, 8, tzinfo=CHICAGO),
        datetime(2025, 1, 25, tzinfo=CHICAGO),
        source=HistorySource.FLAT_FILES,
    )
    assert pick(t, "Timestamp") == [(friday,)]
    assert sorted(fake_s3.keys()) == [key, FUTURES_KEY.format("cme", date(2025, 1, 25))]


def test_futures_dates_do_not_roll_forward(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    make_csv_gz: Callable[[str], bytes],
) -> None:
    # A date names its own session exactly: a holiday's hours belong to another session, so
    # the next session's file is only opened, to tell the holiday from a file not out yet.
    clock(datetime(2025, 2, 10, tzinfo=UTC))
    tuesday = epoch_ns(datetime(2025, 1, 21, 9, tzinfo=CHICAGO))
    later = FUTURES_KEY.format("cme", date(2025, 1, 21))
    fake_s3.put(later, make_csv_gz(FUTURES_HEADER + f",1.0,1,1,1,1,2025-01-21,ESH5,{tuesday}\n"))
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    t = q.history("2025-01-20", "2025-01-20", source=HistorySource.FLAT_FILES)
    assert t.size == 0
    assert fake_s3.keys() == [FUTURES_KEY.format("cme", date(2025, 1, 20)), later]
    assert later in fake_s3.closed_bodies


def test_mixed_futures_bounds_are_refused_before_any_read(
    *, session: dm.MassiveSession, fake_s3: FakeS3
) -> None:
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    with pytest.raises(ValueError, match="start and end must both be dates or both be datetimes"):
        q.history(
            "2024-12-17",
            datetime(2024, 12, 17, 16, tzinfo=CHICAGO),
            source=HistorySource.FLAT_FILES,
        )
    assert fake_s3.requests == []


OTHER_SESSIONS = (
    "futures.trades: the flat file us_futures_cme/trades_v1/2025/03/2025-03-04.csv.gz holds rows "
    "of session 2025-03-03, not only of its own 2025-03-04; each row keeps its own session in "
    "Date and is kept when that session was requested, but rows of a requested session may "
    "sit in a neighbouring day's file, which a call by dates does not read, and be missing"
)


def test_a_futures_row_keeps_its_own_session_and_the_call_keeps_the_requested(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A file holding a row of another session: the row keeps its own session, never the
    # file's, and only rows of requested sessions are served; the file warns once.
    body = (
        FUTURES_HEADER
        + ",1.0,1,1,1,1,2025-03-04,ESH5,1741042800000000000\n"
        + ",1.0,1,1,1,2,2025-03-03,ESH5,1741042800000000000\n"
    )
    fake_s3.put(FUTURES_0304.format("cme"), make_csv_gz(body))
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        t = q.history("2025-03-04", "2025-03-04")
        q.history("2025-03-04", "2025-03-04")
    assert pick(t, "ReportSequence", "Date") == [(1, "2025-03-04")]
    assert [
        r.getMessage() for r in caplog.records if "holds rows of session" in r.getMessage()
    ] == [OTHER_SESSIONS]
    # The file held an earlier session's rows, so the last requested session's rows may sit
    # in the next day's file, which a read by dates does not open: the table says so.
    assert t.attributes()["TableDescription"].endswith(
        "Session(s) 2025-03-04 may be incomplete: Massive's flat files held rows of other "
        "sessions, so some of their rows may sit in a file outside the range."
    )
    doubts = [r.getMessage() for r in caplog.records if "may sit in a file" in r.getMessage()]
    assert (
        doubts
        == [
            (
                "futures.trades history: the flat files hold rows of sessions other than their "
                "own day's, so rows of session(s) 2025-03-04 may sit in a file outside the range "
                "and be missing; read a wider range of dates, or by datetimes, to include them"
            )
        ]
        * 2
    )


def test_a_futures_read_names_both_edge_sessions_when_files_hold_both_neighbours(
    *, session: dm.MassiveSession, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    later = ",1.0,1,1,1,1,2025-03-04,ESH5,1741042800000000000\n"
    earlier = ",1.0,1,1,1,2,2025-03-03,ESH5,1740956400000000000\n"
    fake_s3.put(FUTURES_0304.format("cme"), make_csv_gz(FUTURES_HEADER + earlier))
    fake_s3.put(
        "us_futures_cme/trades_v1/2025/03/2025-03-03.csv.gz",
        make_csv_gz(FUTURES_HEADER + later),
    )
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    t = q.history("2025-03-03", "2025-03-04")
    assert (
        "Session(s) 2025-03-03, 2025-03-04 may be incomplete"
        in (t.attributes()["TableDescription"])
    )


def test_a_read_whose_files_hold_only_their_own_sessions_names_no_doubt() -> None:
    seen = flatfiles._OtherSessions()
    span = DaySpan(date(2025, 3, 3), date(2025, 3, 5))
    assert seen.uncertain(span) == ()
    seen.saw(pa.array([date(2025, 3, 2)], pa.date32()), date(2025, 3, 3))
    assert seen.uncertain(span) == (date(2025, 3, 5),)
    assert seen.uncertain(None) == ()  # a read by instants covers its whole range


def test_a_futures_row_of_another_requested_session_is_kept_with_its_own_date(
    *, session: dm.MassiveSession, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    other = ",1.0,1,1,1,2,2025-03-03,ESH5,1741042800000000000\n"
    fake_s3.put(FUTURES_0304.format("cme"), make_csv_gz(FUTURES_HEADER + other))
    fake_s3.put(
        "us_futures_cme/trades_v1/2025/03/2025-03-03.csv.gz",
        make_csv_gz(FUTURES_HEADER + ",1.0,1,1,1,1,2025-03-03,ESH5,1740956400000000000\n"),
    )
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    t = q.history("2025-03-03", "2025-03-04")
    assert sorted(pick(t, "ReportSequence", "Date")) == [(1, "2025-03-03"), (2, "2025-03-03")]


def test_a_futures_call_named_by_instants_keeps_rows_by_time_with_their_own_session(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    put(FUTURES_0304.format("cme"), "us_futures_trades_v1_wrong_session.csv")  # an ESH5 row
    q = query(session, FUTURES_TRADES, symbols=["ESH5"], venues={FuturesVenue.CME})
    start = datetime(2025, 3, 3, 17, tzinfo=CHICAGO)
    t = q.history(start, start + timedelta(hours=20), source=HistorySource.FLAT_FILES)
    assert pick(t, "ReportSequence", "Date") == [(31, "2025-03-03")]


def test_a_futures_row_without_a_session_is_dropped(
    *, session: dm.MassiveSession, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    rows = (
        ",1.0,1,1,1,1,,ESH5,1741042800000000000\n,1.0,1,1,1,2,2025-03-04,ESH5,1741042800000000000\n"
    )
    fake_s3.put(FUTURES_0304.format("cme"), make_csv_gz(FUTURES_HEADER + rows))
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    assert pick(q.history("2025-03-04", "2025-03-04"), "ReportSequence") == [(2,)]


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
        q = m.futures.trades(dm.ALL)
        with pytest.raises(
            dm.EntitlementError,
            match=(
                r"the plan does not include NYMEX futures flat files: HTTP 403 for "
                r"s3://flatfiles/us_futures_nymex/trades_v1/2025/03/2025-03-04\.csv\.gz at "
                r"https://files\.massive\.com \(AccessDenied: Access Denied\); pass "
                r"venues={dm\.FuturesVenue\.<VENUE>, \.\.\.} with only the venues the plan "
                "includes"
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
    q = query(session, spec, symbols=dm.ALL, venues=None if venue is None else {venue})
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


def test_a_server_error_is_a_massive_error(
    session: dm.MassiveSession, fake_s3: FakeS3, retry_clock: FakeClock
) -> None:
    fake_s3.refuse("us_stocks_sip/", status=500, code="InternalError", message="Try again")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(dm.MassiveError, match=r"HTTP 500 \(InternalError: Try again\)") as info:
        q.history("2025-03-03", "2025-03-03")
    assert (info.value.status, info.value.request_id) == (500, fake_s3.request_id)
    assert len(retry_clock.waits) == 3  # retried on the fake clock, never on real time


def test_an_unreachable_endpoint_is_a_massive_error(
    session: dm.MassiveSession, fake_s3: FakeS3, retry_clock: FakeClock
) -> None:
    fake_s3.fail(TRADES_0303, "unreachable")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(
        dm.MassiveError, match=re.escape("cannot read s3://flatfiles/us_stocks_sip")
    ):
        q.history("2025-03-03", "2025-03-03")
    assert len(retry_clock.waits) == 3  # retried on the fake clock, never on real time


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b"not gzip", id="not-gzip"),
        pytest.param(gzip.compress(b"ticker,price\n" * 1000)[:-200], id="truncated-gzip"),
        pytest.param(
            gzip.compress(b"ticker,price\n")[:10] + b"\xff" * 40, id="corrupt-deflate-data"
        ),
        pytest.param("ticker,price\nAAPL,1.0\n", id="missing-documented-headers"),
        pytest.param("ticker,sip_timestamp\nAAPL,12:30\n", id="no-row-readable"),
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


_HOLD = 10.0
"""Seconds `hold_each_drop` holds one dropped read's failure at most: short enough that a test
whose reads build no table fails on its own assertions within the 60-second test timeout, even
after two held drops."""


def hold_each_drop(fake_s3: FakeS3, monkeypatch: pytest.MonkeyPatch) -> threading.Event:
    # Holds each dropped read's failure until the returned event is set, and clears the event
    # at every request, so a test can make each dropped read build a table first. A reader
    # reads a few chunks ahead of the tables it builds, so chunks of one block keep the first
    # table well before the drop; with longer chunks the reader would wait out the hold.
    monkeypatch.setattr(flatfiles, "CHUNK_BLOCKS", 1)
    built = threading.Event()
    fake_s3.before_answer = lambda _: built.clear()
    fake_s3.before_drop = lambda _: built.wait(_HOLD)
    return built


def test_a_dropped_connection_is_read_again_from_the_start(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    many_blocks: Callable[[str], int],
    monkeypatch: pytest.MonkeyPatch,
    retry_clock: FakeClock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    many_blocks(TRADES_0303)
    converted: list[int] = []
    convert = columns.dataset_table
    built = hold_each_drop(fake_s3, monkeypatch)

    def counted(data: pa.Table, dates: Sequence[str]) -> Table:
        converted.append(data.num_rows)
        built.set()  # the read under way has built a table, so it may now fail
        return convert(data, dates)

    monkeypatch.setattr(flatfiles, "dataset_table", counted)
    monkeypatch.setattr(flatfiles, "_TABLE_BLOCKS", 1)  # a table per block, so drops leave some
    size = len(fake_s3.data(TRADES_0303))
    fake_s3.drop(TRADES_0303, times=2, at=size // 2)
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with caplog.at_level(logging.INFO, logger=LOGGER):
        t = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    # Every row once, in file order: the tables of the two cut-short reads were discarded.
    assert pick(t, "Id") == [(str(n),) for n in range(4000)]
    assert sum(converted) > 4000  # ... although they had gathered some of them
    assert fake_s3.keys() == [TRADES_0303] * 3
    assert fake_s3.closed_bodies == [TRADES_0303] * 3
    waits = retry_clock.waits
    assert len(waits) == 2
    assert 0.5 <= waits[0] <= 0.6
    assert 1.0 <= waits[1] <= 1.2
    rereads = [r.getMessage() for r in caplog.records if "again from its start" in r.getMessage()]
    assert len(rereads) == 2
    assert rereads[0].startswith(f"cannot read {LOCATION.format(TRADES_0303)}: ")


def test_a_connection_that_keeps_dropping_fails_after_three_rereads(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    put: Callable[[str, str], None],
    retry_clock: FakeClock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    put(TRADES_0304, "us_stocks_sip_trades_v1_2025-03-03.csv")
    fake_s3.drop(TRADES_0304, times=4, at=len(fake_s3.data(TRADES_0304)) // 2)
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with (
        caplog.at_level(logging.DEBUG, logger=LOGGER),
        pytest.raises(
            dm.MassiveError, match=re.escape(f"cannot read {LOCATION.format(TRADES_0304)}: ")
        ),
    ):
        q.history("2025-03-03", "2025-03-04", source=HistorySource.FLAT_FILES)
    assert sorted(fake_s3.keys()) == [TRADES_0303] + [TRADES_0304] * 4
    assert len(retry_clock.waits) == 3
    # The call raises the failure, so the client's give-up is DEBUG detail.
    assert [r.levelno for r in caplog.records if "giving up after 4 reads" in r.getMessage()] == [
        logging.DEBUG
    ]


def test_a_reread_is_not_made_for_a_broken_file(
    *, session: dm.MassiveSession, fake_s3: FakeS3, retry_clock: FakeClock
) -> None:
    fake_s3.put(TRADES_0303, b"not gzip")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(dm.MassiveError, match="Not a gzipped file") as info:
        q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert (fake_s3.keys(), retry_clock.waits) == ([TRADES_0303], [])
    # A file that cannot be read is a defect of the library, never an expected failure.
    assert info.value.cause is dm.ErrorCause.LIBRARY_DEFECT


@pytest.mark.parametrize(
    ("code", "message"),
    [
        # How botocore reads the HTML "Massive 404 Not Found" page of Massive's front end,
        # which a wrong bucket meets.
        ("404", "Not Found"),
        ("NotFound", "Not Found"),
    ],
)
def test_a_404_that_is_not_a_missing_file_names_the_settings(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    caplog: pytest.LogCaptureFixture,
    code: str,
    message: str,
) -> None:
    # A wrong bucket answers 404 for every key; it must not read as a run of holidays.
    fake_s3.refuse("", status=404, code=code, message=message)
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with (
        caplog.at_level(logging.DEBUG, logger="deephaven_massive._query"),
        pytest.raises(dm.MassiveError) as info,
    ):
        q.history("2025-03-03", "2025-03-05", source=HistorySource.FLAT_FILES)
    assert type(info.value) is dm.MassiveError
    assert str(info.value) == (
        f"cannot read {LOCATION.format(TRADES_0303)}: HTTP 404 ({code}: {message}); check "
        "MassiveConfig s3_bucket and s3_endpoint_url"
    )
    assert (info.value.status, info.value.request_id) == (404, fake_s3.request_id)
    # Not skipped as holidays: the call fails, and its failure is logged with its id, at DEBUG
    # since the caller receives it, and nothing is logged above that.
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    [failed] = [m for m in caplog.messages if m.startswith("call 1 failed")]
    assert re.fullmatch(
        rf"call 1 failed after \d+\.\d{{3}} s: {re.escape(str(info.value))}", failed
    )


def test_a_range_without_any_file_raises(
    *, session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    # With no file at all, and none after the range, the days may not be out yet, or the
    # address is wrong: the call raises rather than return no rows, and the log says both.
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with (
        caplog.at_level(logging.WARNING, logger=LOGGER),
        pytest.raises(dm.MassiveError, match=r"the flat file for 2025-03-03 is not published"),
    ):
        q.history("2025-03-01", "2025-03-04", source=HistorySource.FLAT_FILES)
    assert caplog.messages == [
        (
            "stocks.trades: 0 of 2 trading days' files exist (2025-03-03..2025-03-04, first "
            f"looked for at {TRADES_0303}); the bucket, prefix, or venue may be wrong"
        )
    ]


def test_one_missing_trading_day_is_only_a_holiday(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Saturday, Sunday, and one Monday: a single missing trading day says nothing of the address.
    put(TRADES_0304, "us_stocks_sip_trades_v1_2025-03-05.csv")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        table = q.history("2025-03-01", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert caplog.messages == [holiday_warning("stocks.trades", TRADES_0303, "2025-03-03")]
    # The table says so too: the file may simply be missing, and its rows with it.
    assert table.attributes()["TableDescription"].endswith(
        "Read from flat files: requested with source=dm.HistorySource.FLAT_FILES. No flat file "
        "exists for 2025-03-03, though a later day's does, so that day was taken for a day "
        "without trading (a holiday?) and has no rows here."
    )


def test_a_crypto_weekend_without_files_warns_about_the_address(
    *, session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    # Crypto trades every day, so a Saturday and a Sunday are two trading days.
    saturday = "global_crypto/trades_v1/2025/03/2025-03-01.csv.gz"
    q = query(session, CRYPTO_TRADES, symbols=dm.ALL)
    with caplog.at_level(logging.WARNING, logger=LOGGER), pytest.raises(dm.MassiveError):
        q.history("2025-03-01", "2025-03-02", source=HistorySource.FLAT_FILES)
    assert caplog.messages == [
        (
            "crypto.trades: 0 of 2 trading days' files exist (2025-03-01..2025-03-02, first "
            f"looked for at {saturday}); the bucket, prefix, or venue may be wrong"
        )
    ]


class _Watching(FakeClock):
    """A fake clock that runs a check at the start of every wait."""

    def __init__(self, check: Callable[[], None]) -> None:
        super().__init__()
        self.check = check

    @override
    def wait(self, event: threading.Event, seconds: float) -> bool:
        self.check()
        return super().wait(event, seconds)


def test_a_dropped_reads_tables_are_freed_before_the_wait(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    many_blocks: Callable[[str], int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    many_blocks(TRADES_0303)
    monkeypatch.setattr(flatfiles, "_TABLE_BLOCKS", 1)  # a table per block, so drops leave some
    fake_s3.drop(TRADES_0303, times=1, at=len(fake_s3.data(TRADES_0303)) // 2)
    partial: list[weakref.ref[Table]] = []
    convert = flatfiles._convert
    built = hold_each_drop(fake_s3, monkeypatch)

    def recorded(batches: list[pa.RecordBatch], transform: flatfiles._Transform) -> Table:
        table = convert(batches, transform)
        if len(fake_s3.keys()) == 1:  # the read that drops
            partial.append(weakref.ref(table))
            built.set()  # it has built a table, so it may now fail
        return table

    alive: list[int] = []

    def count() -> None:
        gc.collect()
        alive.append(sum(ref() is not None for ref in partial))

    monkeypatch.setattr(flatfiles, "_convert", recorded)
    monkeypatch.setitem(
        vars(_massive),
        "FlatFileClient",
        functools.partial(vars(_massive)["FlatFileClient"].func, clock=_Watching(count)),
    )
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    t = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert t.size == 4000
    assert partial  # the dropped read had built tables ...
    assert alive == [0]  # ... and none of them outlived it into the wait


_RESCUE = 10.0
"""Seconds of real time after which `manual_retry_clock` moves time on, ending any wait."""


@pytest.fixture
def manual_retry_clock(
    monkeypatch: pytest.MonkeyPatch, retry_clock: FakeClock
) -> Iterator[FakeClock]:
    """Give the session's flat-file client a manual clock, so a reread wait lasts until ended.

    A wait that nothing ends would hang the test, so after `_RESCUE` seconds of real time the
    clock moves an hour on, which ends every wait: the test then fails on the time that passed.

    Yields:
        The clock.
    """
    clock = FakeClock(manual=True)
    monkeypatch.setitem(
        vars(_massive),
        "FlatFileClient",
        functools.partial(vars(_massive)["FlatFileClient"].func, clock=clock),
    )
    rescue = threading.Timer(_RESCUE, clock.advance, (3600.0,))
    rescue.start()
    yield clock
    rescue.cancel()


def test_a_failing_sibling_ends_a_reread_wait(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    put: Callable[[str, str], None],
    manual_retry_clock: FakeClock,
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    fake_s3.drop(TRADES_0303, times=1, at=len(fake_s3.data(TRADES_0303)) // 2)
    fake_s3.refuse(TRADES_0304, status=403, code="AccessDenied", message="Access Denied")

    def hold(key: str) -> None:
        # The refusal arrives only once the other file waits to be read again.
        if key == TRADES_0304:
            manual_retry_clock.wait_for_sleepers(1)

    fake_s3.before_answer = hold
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(dm.EntitlementError):
        q.history("2025-03-03", "2025-03-04", source=HistorySource.FLAT_FILES)
    assert manual_retry_clock.monotonic() == 0.0  # the wait ended without time passing
    assert fake_s3.keys().count(TRADES_0303) == 1  # and the file was not read again


def test_an_interrupt_ends_a_reread_wait(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    put: Callable[[str, str], None],
    manual_retry_clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    fake_s3.drop(TRADES_0303, times=1, at=len(fake_s3.data(TRADES_0303)) // 2)

    def interrupted(*_: object, **__: object) -> None:
        # Ctrl-C reaches the calling thread while the reader waits to read the file again.
        manual_retry_clock.wait_for_sleepers(1)
        raise KeyboardInterrupt

    monkeypatch.setattr(flatfiles, "wait", interrupted)
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(KeyboardInterrupt):
        q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert manual_retry_clock.monotonic() == 0.0
    assert fake_s3.keys() == [TRADES_0303]


def test_closing_the_session_ends_a_reread_wait(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    put: Callable[[str, str], None],
    manual_retry_clock: FakeClock,
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    fake_s3.drop(TRADES_0303, times=1, at=len(fake_s3.data(TRADES_0303)) // 2)

    def close_while_waiting() -> None:
        manual_retry_clock.wait_for_sleepers(1)
        session.close()

    closer = threading.Thread(target=close_while_waiting)
    closer.start()
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(dm.MassiveError, match=r"MassiveSession #\d+ is closed"):
        q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    closer.join(TIMEOUT)
    assert manual_retry_clock.monotonic() == 0.0
    assert fake_s3.keys() == [TRADES_0303]


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
    convert = columns.dataset_table
    expected = session._core.exec_ctx.j_exec_ctx

    def recorded(data: pa.Table, dates: Sequence[str]) -> Table:
        context = get_exec_ctx().j_exec_ctx.equals(expected)
        seen.append((threading.current_thread().name, context))
        return convert(data, dates)

    monkeypatch.setattr(flatfiles, "dataset_table", recorded)
    query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    # The readers are named after the session and the call: the call's id is its first.
    reader = f"{session._core.thread_prefix} call-1 files_"
    assert all(context for _, context in seen)
    assert any(name.startswith(reader) for name, _ in seen)


def test_history_runs_from_a_thread_without_an_execution_context(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=["AAPL"])
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
    fake_s3.refuse(TRADES_0303, status=501, code="NotImplemented", message="first")
    fake_s3.refuse(TRADES_0304, status=403, code="AccessDenied", message="second")
    m = dm.MassiveSession(config(max_concurrency=2))
    try:
        with pytest.raises(dm.MassiveError, match="HTTP 501") as info:
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
    window = whole_days(STOCK_TRADES, "2025-03-03")
    transform = flatfiles._Transform.build(
        CATALOG,
        STOCK_TRADES,
        prefix="us_stocks_sip/trades_v1",
        request=request(dm.ALL),
        window=window,
        unreadable_symbols=dm.OnError.RAISE,
        warnings=WARNINGS,
    )
    key = flatfiles._Key(TRADES_0303, date(2025, 3, 3), None)
    cancel = _massive.Cancel()
    cancel.set()
    with pytest.raises(flatfiles._CancelledError):
        transport._read_key(transform, key, cancel=cancel)
    flat_file_client(session).close()
    with pytest.raises(dm.MassiveError, match="is closed"):
        transport._read_key(transform, key, cancel=_massive.Cancel())
    assert fake_s3.requests == []


def test_a_read_stopped_while_it_waits_for_a_connection_is_cancelled_not_missing(
    *, fake_s3: FakeS3, put: Callable[[str, str], None]
) -> None:
    # The session reads one file at a time; while that file is open, another read waits for
    # the connection, and stopping its call ends the wait as a cancellation: it never reads
    # as a missing file, which would send a look for later days on.
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    m = dm.MassiveSession(config(max_concurrency=1))
    try:
        transport = cast("flatfiles.FlatFileHistory", m._core.flat_file_transport())
        transform = flatfiles._Transform.build(
            CATALOG,
            STOCK_TRADES,
            prefix="us_stocks_sip/trades_v1",
            request=request(dm.ALL),
            window=whole_days(STOCK_TRADES, "2025-03-03"),
            unreadable_symbols=dm.OnError.RAISE,
            warnings=WARNINGS,
        )
        key = flatfiles._Key(TRADES_0303, date(2025, 3, 3), None)
        client = flat_file_client(m)
        held = client.open(TRADES_0303)
        assert held is not None
        waiting = threading.Event()
        wait = client._slots._ready.wait

        def noted(timeout: float | None = None) -> bool:
            waiting.set()
            return wait(timeout)

        client._slots._ready.wait = noted  # type: ignore[method-assign]  # see the wait
        for read in (
            lambda cancel: transport._read_key(transform, key, cancel=cancel),
            lambda cancel: transport._opened(key, cancel=cancel),
        ):
            waiting.clear()
            cancel = _massive.Cancel()
            with ThreadPoolExecutor(1) as pool:
                outcome = pool.submit(read, cancel)
                assert waiting.wait(TIMEOUT)
                cancel.set()
                with pytest.raises(flatfiles._CancelledError):
                    outcome.result(TIMEOUT)
        held.close()
    finally:
        m.close()
    assert len(fake_s3.requests) == 1  # only the held file was requested


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
    # The March 4 refusal is answered only after the March 3 read has applied its first block,
    # so the failure always lands while that read is in progress. The March 3 read then pauses
    # until the pool is shut down, which happens only once the failure has set the cancel event.
    first_block = threading.Event()
    gate: list[bool] = []  # written once, by the thread answering March 4, before it fails

    def pause(key: str) -> None:
        if key == TRADES_0303:
            first_block.set()
            pool.shut.wait(TIMEOUT)

    def hold(key: str) -> None:
        if key == TRADES_0304 and not gate:
            gate.append(first_block.wait(TIMEOUT))

    fake_s3.before_answer = hold
    applied.hook = pause
    m = dm.MassiveSession(config(max_concurrency=2))
    try:
        # The March 3 read stopped rather than failed, so the March 4 failure is raised.
        with pytest.raises(dm.MassiveError, match="HTTP 500"):
            query(m, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-04")
    finally:
        m.close()
    assert gate == [True]  # the March 4 failure waited for the March 3 read's first block
    assert blocks > 10
    assert applied.counts.get(TRADES_0303) == 1


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
        fake_s3.before_answer = None  # the fake lives in a cycle: it must not keep the session
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
    with caplog.at_level(logging.INFO, logger="deephaven_massive._tables.session_core"):
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
    # A trades file whose rows vary like real ones; one row in 100 is the symbol RARE, so at
    # 16 KiB blocks (about 200 rows) every block holds about two RARE rows.
    moment = 1741012200000000000
    tickers = ("AAPL", "MSFT", "SPY", "NVDA")
    lines = [
        f"12,,4,{n},{moment + n * 1000},{100 + n % 997 / 7:.4f},{n},{moment + n * 1001},"
        f"{n % 5000 + 1},3,{'RARE' if n % 100 == 0 else tickers[n % 4]},,\n"
        for n in range(rows)
    ]
    return TRADE_HEADER + "".join(lines)


class ReadMemory(NamedTuple):
    # One read's rows and memory. The peaks are the highest Python memory (tracemalloc) and
    # Arrow pool level above where the read started. The rises are how far those running peaks
    # climbed from a third of the way through the file to its end, so they leave out whatever
    # the read set up before its first block.
    rows: int
    python_peak: int
    arrow_peak: int
    python_rise: int
    arrow_rise: int


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
        convert = columns.dataset_table

        def sampled_batches(
            file: _massive.FlatFile, plan: _massive.ReadPlan
        ) -> Iterator[pa.RecordBatch]:
            for batch in batches(file, plan):
                self.sample_arrow()
                self.python[self.taken[1]] = tracemalloc.get_traced_memory()[1]
                self.taken[1] += 1
                yield batch

        def sampled_convert(data: pa.Table, dates: Sequence[str]) -> Table:
            self.sample_arrow()
            return convert(data, dates)

        monkeypatch.setattr(_massive.FlatFile, "batches", sampled_batches)
        monkeypatch.setattr(flatfiles, "dataset_table", sampled_convert)

    def sample_arrow(self) -> None:
        self.arrow[self.taken[0]] = pa.total_allocated_bytes()
        self.taken[0] += 1

    def measure(self, read: Callable[[], Table]) -> ReadMemory:
        # Runs `read` and returns its rows and what its reader's memory did.
        self.taken[:] = [0, 0]
        arrow_baseline = pa.total_allocated_bytes()
        tracemalloc.start()
        try:
            rows = read().size
            python_peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
        blocks, samples = self.taken[1], self.taken[0]
        return ReadMemory(
            rows=rows,
            python_peak=python_peak,
            arrow_peak=max(self.arrow[:samples]) - arrow_baseline,
            python_rise=self.python[blocks - 1] - self.python[blocks // 3],
            arrow_rise=max(self.arrow[:samples]) - max(self.arrow[: samples // 3]),
        )


def test_memory_per_reader_does_not_grow_with_the_file(
    *,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A reader holds a window of its file, not the file: the chunks it reads ahead to parse
    # (`PARSE_THREADS + 1` chunks of `CHUNK_BLOCKS` blocks), plus at most `_TABLE_BLOCKS` blocks
    # of gathered rows. Small blocks, in chunks of four, give a file of about 310 blocks, cheap
    # to build and read, in which that window fills within the first third. Each block costs
    # the same to read whatever its size, so the block count sets the test's time; at this
    # count every regression named below still lands well past its slack. One parse thread:
    # with several, how many parsed chunks coexist depends on how the threads happen to run
    # (they truly run together without the GIL), which moves Arrow's peak by a few chunks from
    # one read to the next, more than the slack below.
    block = 16 * 1024
    monkeypatch.setattr(flatfiles, "BLOCK_SIZE", block)
    monkeypatch.setattr(flatfiles, "CHUNK_BLOCKS", 4)
    monkeypatch.setattr(flatfiles, "PARSE_THREADS", 1)
    window = (flatfiles.PARSE_THREADS + 1) * 4 + flatfiles._TABLE_BLOCKS
    rows = 64_000
    text = synthetic_trades(rows)
    blocks = len(text) // block
    assert blocks > 6 * window  # the last two thirds of the file dwarf the window

    def stored(csv_text: str) -> bytes:
        # Stored without compression, so the compressed body is as large as its text: a
        # reader that kept the body, or a copy of each compressed chunk, would hold a block
        # of memory per block of the file, as plainly as one that kept the text.
        return gzip.compress(csv_text.encode(), compresslevel=0)

    fake_s3.put(TRADES_0303, stored(text))
    fake_s3.put(TRADES_0304, stored(TRADE_HEADER + trade(1, 1741012200000000000)))
    sampler = MemorySampler(2 * blocks + 100)
    sampler.install(monkeypatch)
    m = dm.MassiveSession(config(max_concurrency=1))

    def read(symbols: object) -> Callable[[], Table]:
        q = query(m, STOCK_TRADES, symbols=symbols)
        return lambda: q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)

    try:
        # A first read creates the client: botocore parses its service model once per client.
        query(m, STOCK_TRADES, symbols=dm.ALL).history("2025-03-04", "2025-03-04")
        every = sampler.measure(read(dm.ALL))
        rare = sampler.measure(read(["RARE"]))
        fake_s3.put(TRADES_0303, stored(synthetic_trades(rows // 4)))
        short = sampler.measure(read(["RARE"]))
    finally:
        m.close()
    assert (every.rows, rare.rows, short.rows) == (rows, rows // 100, rows // 400)
    # Python's peak includes the read-ahead's decompressed text, taken while the parse threads
    # work, so it varies from run to run by up to about 40 blocks. A reader that kept a Python
    # copy of each compressed chunk it read rises by about 175 over the last two thirds of the
    # file.
    python_slack = 64 * block
    # Arrow's pool varies by a few blocks. The RARE rows a selective read gathers are its result
    # and legitimately grow it by about 6 blocks over the last two thirds of the file (about
    # 7 between the two files), but each small batch also costs allocation overhead that its
    # byte count does not show: kept as one batch per block instead of merged, they grow it by
    # about 21 (24 between the files).
    arrow_slack = 14 * block
    # From a third of the way through the file to its end, a reader that kept each block it
    # read would rise by about 280 blocks. When every row survives, the result itself (a
    # Deephaven table, and its Python wrapper, per `_TABLE_BLOCKS` blocks) legitimately grows
    # with the file, but by far less than a block per table.
    assert every.python_rise < python_slack
    assert every.arrow_rise < arrow_slack
    assert rare.python_rise < python_slack
    assert rare.arrow_rise < arrow_slack
    # The rises leave out whatever a read sets up before its first block, such as the whole
    # body read or decompressed up front: reading a file four times as long must peak about as
    # high as reading the short one. A reader that read the whole body first would peak about
    # 235 blocks higher.
    assert rare.python_peak - short.python_peak < python_slack
    assert rare.arrow_peak - short.arrow_peak < arrow_slack


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
    assert cast("SessionClients", m._core._ports)._flat_files is None
    assert (fake_s3.clients, fake_s3.closes) == ([], 0)


def test_an_arrow_error_while_gathering_rows_is_a_massive_error(
    *, session: dm.MassiveSession, put: Callable[[str, str], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    # Converting a file's rows can fail in Arrow (out of memory, say): it is a failed read of
    # that file, named like any other.
    def exhausted(
        transform: flatfiles._Transform, batch: pa.RecordBatch, *, received: int, key: object
    ) -> tuple[pa.RecordBatch, DriftReport]:
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


# Schema drift: headers are matched by name; extra and missing headers warn once.

DRIFT_LOGGER = "deephaven_massive._tables.diagnostics"


def csv_rows(
    load_fixture: Callable[[str, str], bytes], name: str
) -> tuple[list[str], list[dict[str, str]]]:
    # A fixture's header and its rows keyed by header, to store under another header.
    reader = csv.DictReader(io.StringIO(load_fixture("ingest/flatfiles", name).decode()))
    data = list(reader)
    assert reader.fieldnames is not None
    return list(reader.fieldnames), data


def extra_header(name: str) -> str:
    dropped = "it is dropped (schema drift)"
    return f"stocks.trades: FLAT_FILES field {name!r} is not in the schema; {dropped}"


def missing_header(name: str) -> str:
    return f"stocks.trades: FLAT_FILES field {name!r} is missing; it reads as null (schema drift)"


def drift_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    # Each WARNING ends with the file that showed the drift first, which for a call of several
    # files depends on which reader finished first; the file is checked where only one is read.
    return [
        r.getMessage().split(" at s3://", 1)[0]
        for r in caplog.records
        if r.name == DRIFT_LOGGER and r.levelno == logging.WARNING
    ]


def test_reordered_headers_give_identical_results(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    load_fixture: Callable[[str, str], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    names, data = csv_rows(load_fixture, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    fake_s3.put_csv(TRADES_0303, names, data)
    expected = rows(q.history("2025-03-03", "2025-03-03"))
    assert len(expected) == 4
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        for order in (list(reversed(names)), sorted(names, key=lambda n: n[::-1])):
            fake_s3.put_csv(TRADES_0303, order, data)
            assert rows(q.history("2025-03-03", "2025-03-03")) == expected
    assert drift_warnings(caplog) == []


def test_an_extra_header_is_dropped_and_warned_once_per_session(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    load_fixture: Callable[[str, str], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    names, first = csv_rows(load_fixture, "us_stocks_sip_trades_v1_2025-03-03.csv")
    _, second = csv_rows(load_fixture, "us_stocks_sip_trades_v1_2025-03-05.csv")
    wider = [*names[:4], "halted", *names[4:]]
    fake_s3.put_csv(TRADES_0303, wider, [{**row, "halted": "true"} for row in first])
    fake_s3.put_csv(TRADES_0305, wider, [{**row, "halted": "false"} for row in second])
    q = query(session, STOCK_TRADES, symbols=["AAPL"])
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        t = q.history("2025-03-03", "2025-03-05", source=HistorySource.FLAT_FILES)
        assert pick(t, "Id") == [("1",), ("3",), ("9",)]
        assert "Halted" not in {c.name for c in t.columns}
        # Two files, then a second call: still one warning for the session.
        q.history("2025-03-03", "2025-03-05", source=HistorySource.FLAT_FILES)
    assert drift_warnings(caplog) == [extra_header("halted")]


def test_a_missing_header_is_null_and_warned_once(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    load_fixture: Callable[[str, str], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    names, data = csv_rows(load_fixture, "us_stocks_sip_trades_v1_2025-03-03.csv")
    narrower = [name for name in names if name not in {"trf_id", "participant_timestamp"}]
    fake_s3.put_csv(TRADES_0303, narrower, data)
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        t = q.history("2025-03-03", "2025-03-03")
        q.history("2025-03-03", "2025-03-03")
    assert {c.name: c.data_type for c in t.columns} == col_defs(schema_of(STOCK_TRADES))
    assert pick(t, "Id", "TrfId", "ParticipantTimestamp", "TrfTimestamp") == [
        ("1", None, None, None),
        ("2", None, None, None),
        ("3", None, None, 1741013999999999000),
        ("4", None, None, None),
    ]
    assert drift_warnings(caplog) == [
        missing_header("participant_timestamp"),
        missing_header("trf_id"),
    ]


@pytest.mark.parametrize("key_header", ["ticker", "sip_timestamp"])
def test_a_missing_key_header_is_a_massive_error(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    load_fixture: Callable[[str, str], bytes],
    key_header: str,
) -> None:
    names, data = csv_rows(load_fixture, "us_stocks_sip_trades_v1_2025-03-03.csv")
    fake_s3.put_csv(TRADES_0303, [name for name in names if name != key_header], data)
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(dm.MassiveError) as info:
        q.history("2025-03-03", "2025-03-03")
    assert type(info.value) is dm.MassiveError
    assert str(info.value) == (
        f"cannot read s3://flatfiles/{TRADES_0303} at https://files.massive.com: the header "
        f"lacks the key column {key_header}"
    )


def test_a_futures_file_without_its_session_header_is_a_massive_error(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    names, data = csv_rows(load_fixture, "us_futures_trades_v1_2025-03-04.csv")
    lacking = [name for name in names if name != "session_end_date"]
    fake_s3.put_csv(FUTURES_0304.format("cme"), lacking, data)
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venues={FuturesVenue.CME})
    with pytest.raises(dm.MassiveError, match=r"the header lacks the key column session_end_date$"):
        q.history("2025-03-04", "2025-03-04")


def unconvertible_header(name: str, value: str) -> str:
    return (
        f"stocks.trades: FLAT_FILES field {name!r} held a value that does not fit its type; it "
        f"reads as null (schema drift) at {LOCATION.format(TRADES_0303)}; first value {value}"
    )


def test_an_unconvertible_value_is_null_and_warned_once(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    load_fixture: Callable[[str, str], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    names, data = csv_rows(load_fixture, "us_stocks_sip_trades_v1_2025-03-03.csv")
    fake_s3.put_csv(TRADES_0303, names, [*data[:2], {**data[2], "size": "lots"}, data[3]])
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        t = q.history("2025-03-03", "2025-03-03")
        q.history("2025-03-03", "2025-03-03")
    assert pick(t, "Id", "Size") == [("1", 100.0), ("2", 50.0), ("3", None), ("4", 10.0)]
    assert [r.getMessage() for r in caplog.records if r.name == DRIFT_LOGGER] == [
        unconvertible_header("size", "lots")
    ]


def test_an_unconvertible_list_is_null_and_warned(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    load_fixture: Callable[[str, str], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    names, data = csv_rows(load_fixture, "us_stocks_sip_trades_v1_2025-03-03.csv")
    fake_s3.put_csv(TRADES_0303, names, [{**data[0], "conditions": "12,abc"}, *data[1:]])
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        t = q.history("2025-03-03", "2025-03-03")
    assert pick(t, "Id", "Conditions")[0] == ("1", None)
    assert [r.getMessage() for r in caplog.records if r.name == DRIFT_LOGGER] == [
        unconvertible_header("conditions", "12,abc")
    ]


def test_a_file_with_a_header_and_no_rows_still_reports_its_drift(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    load_fixture: Callable[[str, str], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    names, _ = csv_rows(load_fixture, "us_stocks_sip_trades_v1_2025-03-03.csv")
    fake_s3.put_csv(TRADES_0303, [*names, "halted"], [])
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with caplog.at_level(logging.WARNING, logger=DRIFT_LOGGER):
        t = q.history("2025-03-03", "2025-03-03")
    assert rows(t) == []
    assert drift_warnings(caplog) == [extra_header("halted")]
    # The WARNING names the file, so the log says where to look.
    assert caplog.messages == [f"{extra_header('halted')} at {LOCATION.format(TRADES_0303)}"]


def test_an_empty_file_is_a_massive_error(*, session: dm.MassiveSession, fake_s3: FakeS3) -> None:
    fake_s3.put(TRADES_0303, b"")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(dm.MassiveError) as info:
        q.history("2025-03-03", "2025-03-03")
    assert str(info.value) == (
        f"cannot read {LOCATION.format(TRADES_0303)}: the file is empty: no header line"
    )


def whole_days(spec: DatasetSpec, day: str) -> HistoryWindow:
    return history_window(spec, Request(), start=day, end=day, now=NOW)


def test_minute_bar_files_keep_only_whole_bars_of_an_instant_range(
    *, session: dm.MassiveSession, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    # 14:30:30 to 14:32:30 UTC holds only the 14:31 bar whole.
    header = "ticker,volume,open,close,high,low,window_start,transactions\n"
    minutes = [epoch_ns(datetime(2025, 3, 3, 14, m, tzinfo=UTC)) for m in (30, 31, 32)]
    body = "".join(f"AAPL,1,1.0,1.0,1.0,1.0,{m},1\n" for m in minutes)
    key = "us_stocks_sip/minute_aggs_v1/2025/03/2025-03-03.csv.gz"
    fake_s3.put(key, make_csv_gz(header + body))
    q = query(session, STOCK_BARS, symbols=dm.ALL, timespan=Timespan.MINUTE, adjusted=False)
    t = q.history(
        datetime(2025, 3, 3, 14, 30, 30, tzinfo=UTC),
        datetime(2025, 3, 3, 14, 32, 30, tzinfo=UTC),
        source=HistorySource.FLAT_FILES,
    )
    assert pick(t, "Date", "TimestampStart", "TimestampEnd") == [
        ("2025-03-03", minutes[1], minutes[2])
    ]


def test_a_session_end_date_filter_with_dates_is_refused_before_any_read(
    *, session: dm.MassiveSession, fake_s3: FakeS3
) -> None:
    q = query(
        session,
        FUTURES_TRADES,
        symbols=dm.ALL,
        venues={FuturesVenue.CME},
        filters={"session_end_date": "2025-01-21"},
    )
    with pytest.raises(ValueError, match=r"history\(\) dates set session_end_date themselves"):
        q.history("2025-01-21", "2025-01-21", source=HistorySource.FLAT_FILES)
    assert fake_s3.requests == []


def test_every_row_of_a_day_shares_one_stored_date(
    *,
    session: dm.MassiveSession,
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
    string_objects: Callable[[Table, str], int],
) -> None:
    header = "ticker,volume,open,close,high,low,window_start,transactions\n"
    minutes = [epoch_ns(datetime(2025, 3, 3, 15, m, tzinfo=UTC)) for m in range(20)]
    body = "".join(f"AAPL,1,1.0,1.0,1.0,1.0,{m},1\n" for m in minutes)
    key = "us_stocks_sip/minute_aggs_v1/2025/03/2025-03-03.csv.gz"
    fake_s3.put(key, make_csv_gz(header + body))
    q = query(session, STOCK_BARS, symbols=dm.ALL, timespan=Timespan.MINUTE, adjusted=False)
    t = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert t.size == 20
    assert string_objects(t, "Date") == 1


# --- Background reads ------------------------------------------------------------------------


def test_a_background_history_lands_the_files_rows(
    *, session: dm.MassiveSession, put: Callable[[str, str], None], rows_when: RowsWhen
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    table = q.history("2025-03-03", "2025-03-03", wait=False)
    for state in list(session._core._workers.values()):
        cast("Any", state.fail).__self__.thread.join(TIMEOUT)
    # The read's row shows DONE only after the update cycle that added the rows, so the
    # table holds them from then on.
    (row,) = rows_when(session.streams(), lambda rows: bool(rows) and rows[0]["State"] != "LOADING")
    assert (row["State"], row["Error"], row["RowsPublished"]) == ("DONE", None, 4)
    assert [r["Symbol"] for r in rows(table)] == ["AAPL", "MSFT", "AAPL", "SPY"]
    session.release(table)


def test_a_background_read_rest_could_complete_says_so_in_its_description(
    *, session: dm.MassiveSession, put: Callable[[str, str], None], rows_when: RowsWhen
) -> None:
    # A background read's description is written before the read, so under AUTO with REST
    # able to serve it, it says what may happen; the files cover the range here.
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=MANY)
    table = q.history("2025-03-03", "2025-03-03", wait=False)
    assert (
        "A day the flat files cannot serve is read from REST; the log names it."
        in (table.attributes()["TableDescription"])
    )
    for state in list(session._core._workers.values()):
        cast("Any", state.fail).__self__.thread.join(TIMEOUT)
    (row,) = rows_when(session.streams(), lambda rows: bool(rows) and rows[0]["State"] != "LOADING")
    assert row["State"] == "DONE"
    session.release(table)


def test_a_set_stop_token_cancels_a_flat_file_read(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    day = date(2025, 3, 3)
    request = dataclasses.replace(q._request, start=day, end=day)
    window = history_window(STOCK_TRADES, request, start=day, end=day, classes=())
    stop = _massive.Cancel()
    stop.set()
    with pytest.raises(dm.MassiveError, match=r"stocks\.trades history: the read was cancelled"):
        session._core.flat_file_transport().history(
            STOCK_TRADES, request, window, dates=[day], call=1, infer_holidays=True, stop=stop
        )
