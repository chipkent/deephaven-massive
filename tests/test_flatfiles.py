"""Tests for flat-file history: source choice, partition dates, and streaming S3 reads."""

import gc
import gzip
import logging
import re
from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import UTC, date, datetime, time
from typing import Any, Protocol, cast
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest
from botocore.exceptions import EndpointConnectionError
from deephaven import arrow as dh_arrow
from deephaven import new_table
from deephaven.column import string_col
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive import _flatfiles
from deephaven_massive._catalog import DatasetSpec
from deephaven_massive._dates import CHICAGO, FuturesSessions, TimeRange, bounds
from deephaven_massive._domains.base import build_query
from deephaven_massive._enums import AssetClass, FuturesVenue, HistorySource, Timespan, Transport
from deephaven_massive._flatfiles import FlatFileBucket, choose_source, partition_dates
from deephaven_massive._query import HistoryQuery, Request
from deephaven_massive._schema import BARS, INDEX_VALUES, QUOTES, TRADES, Column, ColumnType, Schema
from deephaven_massive._timeunits import TimeUnit

TIMEOUT = 30.0
NEW_YORK = ZoneInfo("America/New_York")
FF = Transport.FLAT_FILES
NOW = datetime(2025, 3, 10, 16, tzinfo=UTC)
"""A Monday afternoon, when every file of the first week of March 2025 is published."""


class FakeS3(Protocol):
    # What the tests use of the `fake_s3` fixture (tests/conftest.py, tests/_fakes/s3.py).
    requests: list[tuple[str, str]]
    clients: list[dict[str, object]]

    def put(self, key: str, data: bytes) -> None: ...
    def put_dropped(self, key: str, data: bytes) -> None: ...
    def refuse(self, prefix: str, *, status: int, code: str, message: str) -> None: ...
    def fail(self, key: str, error: Exception) -> None: ...
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
INDEX_BARS = replace(
    STOCK_BARS,
    name="indices.bars",
    asset_class=AssetClass.INDICES,
    grouped_daily_method=None,
    flat_file_prefixes={
        Timespan.MINUTE: "us_indices/minute_aggs_v1",
        Timespan.DAY: "us_indices/day_aggs_v1",
    },
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
FUTURES_BARS = DatasetSpec(
    name="futures.bars",
    query_class=HistoryQuery,
    asset_class=AssetClass.FUTURES,
    schema=BARS.schema,
    is_bars=True,
    key_maps={FF: {"ticker": "Symbol", "window_start": "Timestamp"}},
    history_method="list_futures_aggregates",
    flat_file_prefixes={
        Timespan.MINUTE: "us_futures_{venue}/minute_aggs_v1",
        Timespan.SESSION: "us_futures_{venue}/session_aggs_v1",
    },
    time_units=_ns("window_start"),
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
FOREX_BARS = replace(
    STOCK_BARS,
    name="forex.bars",
    asset_class=AssetClass.FOREX,
    flat_file_prefixes={
        Timespan.MINUTE: "global_forex/minute_aggs_v1",
        Timespan.DAY: "global_forex/day_aggs_v1",
    },
)
YIELDS = DatasetSpec(
    name="economy.yields",
    query_class=HistoryQuery,
    schema=Schema("economy.yields", (Column("Date", ColumnType.LOCAL_DATE),)),
    poll_keys=("Date",),
    history_method="list_treasury_yields",
)

TRADES_0303 = "us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz"
TRADES_0304 = "us_stocks_sip/trades_v1/2025/03/2025-03-04.csv.gz"
TRADES_0305 = "us_stocks_sip/trades_v1/2025/03/2025-03-05.csv.gz"


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
        monkeypatch.setattr(_flatfiles, "_now", lambda: moment)

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
        fake_s3.put(key, make_csv_gz(load_fixture("_flatfiles", fixture).decode()))

    return store


def has_client(bucket: FlatFileBucket) -> bool:
    return bucket._client is not None


def query(m: dm.MassiveSession, spec: DatasetSpec, **kwargs: object) -> HistoryQuery:
    q = build_query(m._core, spec, **cast("dict[str, Any]", kwargs))
    assert isinstance(q, HistoryQuery)
    return q


# --- partition_dates -------------------------------------------------------------------------


def test_partition_dates_cover_the_range_in_the_class_day_zone() -> None:
    window = bounds(STOCK_TRADES, "2025-03-03", "2025-03-05")
    assert partition_dates(STOCK_TRADES, window) == (
        date(2025, 3, 3),
        date(2025, 3, 4),
        date(2025, 3, 5),
    )
    # The end is exclusive: midnight New York names no file of its own.
    window = bounds(
        STOCK_TRADES,
        datetime(2025, 3, 3, 10, tzinfo=NEW_YORK),
        datetime(2025, 3, 4, tzinfo=NEW_YORK),
    )
    assert partition_dates(STOCK_TRADES, window) == (date(2025, 3, 3),)


def test_partition_dates_of_forex_are_utc_dates() -> None:
    evening = datetime(2025, 3, 3, 22, tzinfo=NEW_YORK)  # 03:00 UTC on March 4
    window = TimeRange(evening.astimezone(UTC), datetime(2025, 3, 4, 4, tzinfo=UTC))
    assert partition_dates(FOREX_BARS, window) == (date(2025, 3, 4),)
    assert partition_dates(STOCK_BARS, window) == (date(2025, 3, 3),)


def test_partition_dates_of_futures_are_sessions() -> None:
    window = bounds(FUTURES_TRADES, "2025-03-03", "2025-03-05")
    assert isinstance(window, FuturesSessions)
    assert partition_dates(FUTURES_TRADES, window) == (
        date(2025, 3, 3),
        date(2025, 3, 4),
        date(2025, 3, 5),
    )


def test_partition_dates_of_futures_instants_add_the_next_session() -> None:
    # 18:00 Chicago on March 3 already trades in the session that ends on March 4.
    window = bounds(
        FUTURES_TRADES,
        datetime(2025, 3, 3, 16, tzinfo=CHICAGO),
        datetime(2025, 3, 3, 18, tzinfo=CHICAGO),
    )
    assert partition_dates(FUTURES_TRADES, window) == (date(2025, 3, 3), date(2025, 3, 4))


# --- choose_source: every branch of the AUTO rule --------------------------------------------

MARCH_3 = (date(2025, 3, 3),)


def choose(
    spec: DatasetSpec,
    req: Request,
    *,
    requested: HistorySource = HistorySource.AUTO,
    symbol_count: int | None = 1,
    cfg: dm.MassiveConfig | None = None,
    now: datetime = NOW,
    dates: tuple[date, ...] = MARCH_3,
) -> HistorySource:
    return choose_source(
        spec,
        req,
        dates,
        requested=requested,
        symbol_count=symbol_count,
        config=cfg or config(),
        now=now,
    )


def test_auto_with_neither_source_lists_every_violated_constraint() -> None:
    bars = request(dm.ALL, timespan=Timespan.HOUR, multiplier=5, params=(("adjusted", True),))
    with pytest.raises(ValueError, match=r"stocks\.bars history has no possible source") as info:
        choose(
            STOCK_BARS,
            bars,
            symbol_count=None,
            cfg=NO_S3,
            now=datetime(2025, 3, 4, 15, tzinfo=UTC),  # 10:00 New York: March 3 not published
        )
    message = str(info.value)
    assert "Flat files: no S3 credentials are configured" in message
    assert "stocks.bars flat files hold only 1-minute, 1-day bars" in message
    assert "flat-file bars are unadjusted; pass adjusted=False" in message
    assert (
        "the file for 2025-03-03 is not yet published; flat files land ~11:00 ET next day"
    ) in message
    assert message.endswith("REST: stocks.bars history over REST needs explicit symbols.")


def test_auto_with_only_flat_files_possible_reads_flat_files() -> None:
    # indices.values has no REST history, so even one symbol reads flat files.
    assert choose(INDEX_VALUES_SPEC, request(("I:SPX",))) is HistorySource.FLAT_FILES


def test_auto_with_only_rest_possible_uses_rest() -> None:
    assert choose(STOCK_TRADES, request(), symbol_count=500, cfg=NO_S3) is HistorySource.REST
    assert choose(YIELDS, request(None), symbol_count=None) is HistorySource.REST


@pytest.mark.parametrize(
    ("symbol_count", "expected"),
    [(49, HistorySource.REST), (50, HistorySource.FLAT_FILES), (None, HistorySource.FLAT_FILES)],
)
def test_auto_with_both_possible_switches_at_the_threshold(
    symbol_count: int | None, expected: HistorySource
) -> None:
    # None stands for dm.ALL: the daily grouped endpoint makes REST possible too.
    req = request(dm.ALL, timespan=Timespan.DAY, params=(("adjusted", False),))
    assert choose(STOCK_BARS, req, symbol_count=symbol_count) is expected


def test_auto_threshold_comes_from_the_config() -> None:
    cfg = config(flat_file_symbol_threshold=3)
    assert choose(STOCK_TRADES, request(), symbol_count=2, cfg=cfg) is HistorySource.REST
    assert choose(STOCK_TRADES, request(), symbol_count=3, cfg=cfg) is HistorySource.FLAT_FILES


def test_flat_files_forced_reads_flat_files_or_names_the_violation() -> None:
    forced = HistorySource.FLAT_FILES
    assert choose(STOCK_TRADES, request(), requested=forced) is HistorySource.FLAT_FILES
    with pytest.raises(
        ValueError, match=r"stocks\.trades cannot be read from flat files: no S3 credentials"
    ):
        choose(STOCK_TRADES, request(), requested=forced, cfg=NO_S3)


@pytest.mark.parametrize(
    ("spec", "timespan", "multiplier", "params", "problem"),
    [
        (STOCK_BARS, Timespan.MINUTE, 1, (("adjusted", False),), None),
        (STOCK_BARS, Timespan.DAY, 1, (("adjusted", False),), None),
        (STOCK_BARS, Timespan.HOUR, 1, (("adjusted", False),), "hold only 1-minute, 1-day bars"),
        (STOCK_BARS, Timespan.MINUTE, 5, (("adjusted", False),), "hold only 1-minute, 1-day"),
        (STOCK_BARS, Timespan.DAY, 1, (("adjusted", True),), "pass adjusted=False"),
        (INDEX_BARS, Timespan.DAY, 1, (), None),  # no adjusted parameter
        (FUTURES_BARS, Timespan.SESSION, 1, (), None),
        (FUTURES_BARS, Timespan.MINUTE, 1, (), None),
        (FUTURES_BARS, Timespan.HOUR, 1, (), "hold only 1-minute, 1-session bars"),
        (replace(STOCK_BARS, flat_file_prefixes={}), Timespan.DAY, 1, (), "has no flat files"),
        (YIELDS, None, 1, (), "economy.yields has no flat files"),
    ],
)
def test_flat_file_constraints_of_bars_and_datasets(
    *,
    spec: DatasetSpec,
    timespan: Timespan | None,
    multiplier: int,
    params: tuple[tuple[str, object], ...],
    problem: str | None,
) -> None:
    req = request(timespan=timespan, multiplier=multiplier, params=params)
    forced = HistorySource.FLAT_FILES
    if problem is None:
        assert choose(spec, req, requested=forced) is HistorySource.FLAT_FILES
    else:
        with pytest.raises(ValueError, match=problem):
            choose(spec, req, requested=forced)


@pytest.mark.parametrize(
    ("minute", "published"),
    [(29, False), (31, True)],
)
def test_the_publish_cutoff_applies_the_next_day(minute: int, published: bool) -> None:
    now = datetime(2025, 3, 4, 11, minute, tzinfo=NEW_YORK)
    forced = HistorySource.FLAT_FILES
    if published:
        assert choose(STOCK_TRADES, request(), requested=forced, now=now) is forced
    else:
        with pytest.raises(ValueError, match=r"2025-03-03 is not yet published.*11:30 New York"):
            choose(STOCK_TRADES, request(), requested=forced, now=now)
    # Under AUTO the same request falls back to REST until the file is published.
    expected = HistorySource.FLAT_FILES if published else HistorySource.REST
    assert choose(STOCK_TRADES, request(), symbol_count=50, now=now) is expected


def test_the_publish_cutoff_comes_from_the_config() -> None:
    now = datetime(2025, 3, 4, 9, 30, tzinfo=NEW_YORK)
    early = config(flat_file_publish_cutoff=time(9))
    forced = HistorySource.FLAT_FILES
    assert choose(STOCK_TRADES, request(), requested=forced, now=now, cfg=early) is forced
    with pytest.raises(ValueError, match="not yet published"):
        choose(STOCK_TRADES, request(), requested=forced, now=now)


def test_every_partition_date_must_be_published() -> None:
    now = datetime(2025, 3, 5, 12, tzinfo=NEW_YORK)  # March 4 is published, March 5 is not
    dates = (date(2025, 3, 4), date(2025, 3, 5))
    with pytest.raises(ValueError, match="2025-03-05 is not yet published"):
        choose(STOCK_TRADES, request(), requested=HistorySource.FLAT_FILES, now=now, dates=dates)


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
    assert (second["Size"], second["Correction"], second["Conditions"]) == (0.5, 1, [14])
    assert (second["TrfId"], second["TrfTimestamp"]) == (201, 1741013999999999000)
    assert spy["Symbol"] == "SPY"
    assert fake_s3.requests == [("flatfiles", TRADES_0303)]


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
    header = (
        "conditions,correction,exchange,id,participant_timestamp,price,sequence_number,"
        "sip_timestamp,size,tape,ticker,trf_id,trf_timestamp\n"
    )
    row = f"12,,4,1,1741012200000000000,1.5,1,1741012200000000000,10,1,{ticker},,\n"
    other = "12,,4,2,1741012200000000000,2.5,2,1741012200000000000,10,1,SPY,,\n"
    fake_s3.put(TRADES_0303, make_csv_gz(header + row + other))
    q = query(session, STOCK_TRADES, symbols=[ticker])
    t = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert pick(t, "Symbol", "Correction", "TrfId") == [(ticker, None, None)]
    every = query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
    assert pick(every, "Symbol") == [(ticker,), ("SPY",)]


def test_no_matching_rows_gives_an_empty_table_with_every_column(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=["ZZZZ"])
    t = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert t.size == 0
    assert {c.name: c.data_type for c in t.columns} == TRADES.schema.col_defs()


def test_instant_bounds_are_half_open(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    t = query(session, STOCK_TRADES, symbols=dm.ALL).history(
        datetime(2025, 3, 3, 14, 30, tzinfo=UTC), datetime(2025, 3, 3, 15, tzinfo=UTC)
    )
    # The trade at exactly 15:00 UTC is the excluded end.
    assert pick(t, "Id") == [("1",), ("2",)]


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
        params={"adjusted": False},
    )
    with caplog.at_level(logging.INFO, logger="deephaven_massive._flatfiles"):
        t = q.history("2025-03-03", "2025-03-03")
    assert pick(t, "Symbol", "Timestamp", "Open", "Volume", "Transactions", "Vwap") == [
        ("AAPL", 1740978000000000000, 241.79, 47184000.0, 612403, None),
        ("MSFT", 1740978000000000000, 398.82, 26093000.0, 402122, None),
    ]
    assert caplog.messages == ["stocks.bars history: source=S3 files=1 skipped=none"]


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
    with caplog.at_level(logging.INFO, logger="deephaven_massive._flatfiles"):
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


def test_missing_weekend_files_are_skipped_silently(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with caplog.at_level(logging.INFO, logger="deephaven_massive._flatfiles"):
        t = q.history("2025-03-01", "2025-03-03")
    assert t.size == 4
    assert caplog.messages == [
        "stocks.trades history: source=S3 files=1 skipped=2025-03-01,2025-03-02"
    ]


def test_a_missing_last_weekend_date_is_skipped_too(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    put(TRADES_0305, "us_stocks_sip_trades_v1_2025-03-05.csv")
    put(
        "us_stocks_sip/trades_v1/2025/03/2025-03-06.csv.gz",
        "us_stocks_sip_trades_v1_2025-03-05.csv",
    )
    put(
        "us_stocks_sip/trades_v1/2025/03/2025-03-07.csv.gz",
        "us_stocks_sip_trades_v1_2025-03-05.csv",
    )
    t = query(session, STOCK_TRADES, symbols=dm.ALL).history("2025-03-05", "2025-03-08")
    assert t.size == 6


def test_a_missing_last_trading_date_is_not_yet_published(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(
        dm.MassiveError,
        match=re.escape(
            f"stocks.trades: {TRADES_0304} is not yet published; flat files land ~11:00 ET next day"
        ),
    ):
        q.history("2025-03-03", "2025-03-04")


def test_crypto_trades_every_day(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    saturday = "global_crypto/trades_v1/2025/03/2025-03-01.csv.gz"
    put(saturday, "global_crypto_trades_v1_2025-03-01.csv")
    q = query(session, CRYPTO_TRADES, symbols=["BTC-USD"])
    t = q.history("2025-03-01", "2025-03-01", source=HistorySource.FLAT_FILES)
    assert pick(t, "Symbol", "Timestamp", "Size", "Id") == [
        ("X:BTCUSD", 1740830400000000000, 0.015, "a1")
    ]
    # A crypto file is missing on a Sunday only if it is not yet published.
    with pytest.raises(dm.MassiveError, match=r"2025-03-02\.csv\.gz is not yet published"):
        q.history("2025-03-01", "2025-03-02", source=HistorySource.FLAT_FILES)


def test_a_reactive_symbol_table_is_read_once(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    watch = new_table([string_col("Symbol", ["spy", "$$$"])])
    q = query(session, STOCK_TRADES, symbols=watch)
    t = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert pick(t, "Symbol") == [("SPY",)]
    empty = query(session, STOCK_TRADES, symbols=new_table([string_col("Symbol", ["$$$"])]))
    with pytest.raises(ValueError, match="the symbol table holds no valid symbols"):
        empty.history("2025-03-03", "2025-03-03")


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
    watch = new_table([string_col("Symbol", ["msft", "aapl"])])
    q = query(session, STOCK_TRADES, symbols=watch)
    with caplog.at_level(logging.INFO, logger="deephaven_massive._flatfiles"):
        t = q.history("2025-03-03", "2025-03-04")
    assert pick(t, "Symbol") == [("from REST",)]
    ((spec, req, window),) = rest.calls
    assert spec is STOCK_TRADES
    assert req.symbols == ("AAPL", "MSFT")  # the table was read for REST
    assert (req.start, req.end) == (date(2025, 3, 3), date(2025, 3, 4))
    assert window == bounds(STOCK_TRADES, "2025-03-03", "2025-03-04")
    assert caplog.messages == ["stocks.trades history: source=REST"]
    assert fake_s3.requests == []


# --- Futures ---------------------------------------------------------------------------------

FUTURES_0304 = "us_futures_{}/trades_v1/2025/03/2025-03-04.csv.gz"


def test_futures_read_every_venue_by_default(
    *, session: dm.MassiveSession, put: Callable[[str, str], None], fake_s3: FakeS3
) -> None:
    for venue in ("cme", "cbot", "nymex", "comex"):
        put(FUTURES_0304.format(venue), "us_futures_trades_v1_2025-03-04.csv")
    q = query(session, FUTURES_TRADES, symbols=["ESH5"])
    t = q.history("2025-03-04", "2025-03-04", source=HistorySource.FLAT_FILES)
    assert sorted(fake_s3.keys()) == sorted(
        FUTURES_0304.format(v) for v in ("cme", "cbot", "nymex", "comex")
    )
    # Venue order within a date: CME, CBOT, NYMEX, COMEX.
    assert len(pick(t, "Symbol")) == 8
    assert pick(t, "Symbol", "Timestamp", "SessionEndDate", "ReportSequence")[:2] == [
        ("ESH5", 1741042800000000000, date(2025, 3, 4), 31),
        ("ESH5", 1741100400000000000, date(2025, 3, 4), 32),
    ]


def test_futures_read_one_venue_when_asked(
    *, session: dm.MassiveSession, put: Callable[[str, str], None], fake_s3: FakeS3
) -> None:
    put(FUTURES_0304.format("cme"), "us_futures_trades_v1_2025-03-04.csv")
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venue=FuturesVenue.CME)
    t = q.history("2025-03-04", "2025-03-04")
    assert fake_s3.keys() == [FUTURES_0304.format("cme")]
    assert pick(t, "Symbol") == [("ESH5",), ("ESH5",), ("NQH5",)]


def test_futures_instants_read_the_next_session_and_filter_on_timestamp(
    *,
    session: dm.MassiveSession,
    put: Callable[[str, str], None],
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
) -> None:
    header = "correction,price,size,exchange,sequence_number,report_sequence,session_end_date,"
    fake_s3.put(
        "us_futures_cme/trades_v1/2025/03/2025-03-03.csv.gz",
        make_csv_gz(header + "ticker,timestamp\n"),
    )
    put(FUTURES_0304.format("cme"), "us_futures_trades_v1_2025-03-04.csv")
    q = query(session, FUTURES_TRADES, symbols=["ESH5"], venue=FuturesVenue.CME)
    # 17:00 Chicago on March 3 already trades in the session that ends on March 4.
    t = q.history(
        datetime(2025, 3, 3, 17, tzinfo=CHICAGO),
        datetime(2025, 3, 3, 18, tzinfo=CHICAGO),
        source=HistorySource.FLAT_FILES,
    )
    assert sorted(fake_s3.keys()) == [  # read concurrently
        "us_futures_cme/trades_v1/2025/03/2025-03-03.csv.gz",
        FUTURES_0304.format("cme"),
    ]
    assert pick(t, "Symbol", "Timestamp", "SessionEndDate") == [
        ("ESH5", 1741042800000000000, date(2025, 3, 4))
    ]


def test_futures_session_keys_are_checked(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    put(FUTURES_0304.format("cme"), "us_futures_trades_v1_wrong_session.csv")
    q = query(session, FUTURES_TRADES, symbols=dm.ALL, venue=FuturesVenue.CME)
    with pytest.raises(
        dm.MassiveError,
        match=re.escape(
            "us_futures_cme/trades_v1/2025/03/2025-03-04.csv.gz holds session_end_date "
            "2025-03-03, not 2025-03-04: futures flat files are not keyed by trading session"
        ),
    ):
        q.history("2025-03-04", "2025-03-04")


def test_futures_need_a_session_date_header(session: dm.MassiveSession) -> None:
    key_map = dict(FUTURES_TRADES.key_maps[FF])
    del key_map["session_end_date"]
    spec = replace(FUTURES_TRADES, key_maps={FF: key_map})
    q = query(session, spec, symbols=dm.ALL, venue=FuturesVenue.CME)
    with pytest.raises(ValueError, match="futures flat files need a session date header"):
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
    fake_s3.refuse("us_stocks_sip/", status=403, code="InvalidAccessKeyId", message="Bad key")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(
        dm.EntitlementError,
        match=r"the plan does not include these flat files: .*\(InvalidAccessKeyId: Bad key\)",
    ):
        q.history("2025-03-03", "2025-03-03")


def test_a_server_error_is_a_massive_error(session: dm.MassiveSession, fake_s3: FakeS3) -> None:
    fake_s3.refuse("us_stocks_sip/", status=500, code="InternalError", message="Try again")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(dm.MassiveError, match=r"HTTP 500 \(InternalError: Try again\)"):
        q.history("2025-03-03", "2025-03-03")


def test_an_unreachable_endpoint_is_a_massive_error(
    session: dm.MassiveSession, fake_s3: FakeS3
) -> None:
    fake_s3.fail(TRADES_0303, EndpointConnectionError(endpoint_url="https://files.massive.com"))
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
        load_fixture("_flatfiles", "us_stocks_sip_trades_v1_2025-03-03.csv").decode()
    )
    fake_s3.put_dropped(TRADES_0303, data[: len(data) // 2])
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    with pytest.raises(
        dm.MassiveError, match=re.escape(f"cannot read s3://flatfiles/{TRADES_0303}")
    ):
        q.history("2025-03-03", "2025-03-03")


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


def test_closing_the_session_closes_the_bucket(
    *, session: dm.MassiveSession, put: Callable[[str, str], None]
) -> None:
    put(TRADES_0303, "us_stocks_sip_trades_v1_2025-03-03.csv")
    q = query(session, STOCK_TRADES, symbols=dm.ALL)
    q.history("2025-03-03", "2025-03-03")
    bucket = session._core._flat_files
    assert has_client(bucket)
    session.close()
    assert not has_client(bucket)
    bucket.close()  # idempotent
    with pytest.raises(dm.MassiveError, match=r"MassiveSession #\d+ is closed"):
        bucket.client()  # a read that raced close() cannot reopen it


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
    bucket = m._core._flat_files
    reaper = m._core._reaper.thread
    assert has_client(bucket)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._session"):
        del m
        gc.collect()
        reaper.join(TIMEOUT)
    assert not reaper.is_alive()
    assert not has_client(bucket)
    assert table.size == 4  # static results outlive their session
    assert caplog.messages == [
        f"{name} was garbage-collected without close(); closing its connections and pollers"
    ]


# --- Bounded memory --------------------------------------------------------------------------


def test_memory_is_bounded_by_one_block(
    *,
    fake_s3: FakeS3,
    clock: Callable[[datetime], None],
    make_csv_gz: Callable[[str], bytes],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    block = 64 * 1024
    monkeypatch.setattr(_flatfiles, "BLOCK_SIZE", block)
    header = (
        "conditions,correction,exchange,id,participant_timestamp,price,sequence_number,"
        "sip_timestamp,size,tape,ticker,trf_id,trf_timestamp\n"
    )
    moment = 1741012200000000000
    tickers = ("AAPL", "MSFT", "SPY", "NVDA")
    lines = [
        f"12,,4,{n},{moment + n * 1000},{100 + n % 997 / 7:.4f},{n},{moment + n * 1001},"
        f"{n % 5000 + 1},3,{tickers[n % 4]},,\n"
        for n in range(100_000)
    ]
    text = header + "".join(lines)
    fake_s3.put(TRADES_0303, make_csv_gz(text))
    peaks: list[int] = []
    convert = dh_arrow.to_table

    def measured(data: pa.Table) -> Table:
        peaks.append(pa.total_allocated_bytes())
        return convert(data)

    monkeypatch.setattr(_flatfiles, "to_table", measured)
    m = dm.MassiveSession(config(max_concurrency=1))
    try:
        baseline = pa.total_allocated_bytes()
        t = query(m, STOCK_TRADES, symbols=dm.ALL).history("2025-03-03", "2025-03-03")
        assert t.size == 100_000
    finally:
        m.close()
    assert len(text) > 100 * block
    assert len(peaks) > 50  # one conversion per block, plus the empty schema table
    # A few blocks at most (the free-threaded build frees some buffers a little later), never
    # anything that grows with the file, which is more than 100 blocks long.
    assert max(peaks) - baseline < 8 * block
