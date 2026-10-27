"""Tests for the `history()` source choice: partition dates, REST capability, and AUTO."""

from dataclasses import replace
from datetime import UTC, date, datetime, time
from typing import Any, cast
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm
from deephaven_massive import _history_source
from deephaven_massive._catalog import DatasetSpec
from deephaven_massive._dates import CHICAGO, FuturesSessions, TimeRange, bounds
from deephaven_massive._enums import AssetClass, HistorySource, Timespan, Transport
from deephaven_massive._history_source import (
    SourceChoice,
    choose_source,
    flat_file_prefix,
    partition_dates,
    rest_history_problem,
    within_publication_grace,
)
from deephaven_massive._query import HistoryQuery, Request
from deephaven_massive._schema import BARS, INDEX_VALUES, TRADES, Column, ColumnType, Schema
from deephaven_massive._timeunits import TimeUnit

NEW_YORK = ZoneInfo("America/New_York")
FF = Transport.FLAT_FILES
NOW = datetime(2025, 3, 10, 16, tzinfo=UTC)
"""A Monday afternoon, when every file of the first week of March 2025 is published."""


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
FOREX_BARS = replace(
    STOCK_BARS,
    name="forex.bars",
    asset_class=AssetClass.FOREX,
    flat_file_prefixes={
        Timespan.MINUTE: "global_forex/minute_aggs_v1",
        Timespan.DAY: "global_forex/day_aggs_v1",
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
        FF: {"ticker": "Symbol", "timestamp": "Timestamp", "session_end_date": "SessionEndDate"}
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
    key_maps={
        FF: {"ticker": "Symbol", "window_start": "Timestamp", "session_end_date": "SessionEndDate"}
    },
    history_method="list_futures_aggregates",
    flat_file_prefixes={
        Timespan.MINUTE: "us_futures_{venue}/minute_aggs_v1",
        Timespan.SESSION: "us_futures_{venue}/session_aggs_v1",
    },
    time_units=_ns("window_start"),
)
YIELDS = DatasetSpec(
    name="economy.yields",
    query_class=HistoryQuery,
    schema=Schema("economy.yields", (Column("Date", ColumnType.LOCAL_DATE),)),
    poll_keys=("Date",),
    history_method="list_treasury_yields",
)
NOWHERE = replace(YIELDS, name="economy.nowhere", history_method=None)


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


def many(count: int) -> tuple[str, ...]:
    return tuple(f"S{n:03d}" for n in range(count))


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


def test_partition_dates_without_a_class_are_new_york_dates() -> None:
    evening = TimeRange(
        datetime(2025, 3, 3, 22, tzinfo=NEW_YORK), datetime(2025, 3, 3, 23, tzinfo=NEW_YORK)
    )
    assert partition_dates(YIELDS, evening) == (date(2025, 3, 3),)  # March 4 in UTC


def test_partition_dates_of_futures_are_sessions() -> None:
    window = bounds(FUTURES_TRADES, "2025-03-03", "2025-03-05")
    assert isinstance(window, FuturesSessions)
    assert partition_dates(FUTURES_TRADES, window) == (
        date(2025, 3, 3),
        date(2025, 3, 4),
        date(2025, 3, 5),
    )


@pytest.mark.parametrize(
    ("start", "end", "sessions"),
    [
        pytest.param((10, 0), (11, 0), (date(2025, 3, 3),), id="inside-one-session"),
        pytest.param((16, 0), (17, 0), (date(2025, 3, 3),), id="up-to-the-roll"),
        pytest.param((16, 0), (18, 0), (date(2025, 3, 3), date(2025, 3, 4)), id="across-the-roll"),
        pytest.param((17, 0), (18, 0), (date(2025, 3, 4),), id="evening-only"),
        # 23:30 Chicago is 00:30 on March 4 in New York: the session, not either date, decides.
        pytest.param((23, 30), (23, 45), (date(2025, 3, 4),), id="late-evening"),
    ],
)
def test_partition_dates_of_futures_instants_are_their_sessions(
    start: tuple[int, int], end: tuple[int, int], sessions: tuple[date, ...]
) -> None:
    window = bounds(
        FUTURES_TRADES,
        datetime(2025, 3, 3, *start, tzinfo=CHICAGO),
        datetime(2025, 3, 3, *end, tzinfo=CHICAGO),
    )
    assert partition_dates(FUTURES_TRADES, window) == sessions


# --- Prefixes and REST capability ------------------------------------------------------------


def test_flat_file_prefix_by_timespan_and_multiplier() -> None:
    assert flat_file_prefix(STOCK_TRADES, request()) == "us_stocks_sip/trades_v1"
    minute = request(timespan=Timespan.MINUTE)
    assert flat_file_prefix(STOCK_BARS, minute) == "us_stocks_sip/minute_aggs_v1"
    assert flat_file_prefix(STOCK_BARS, request(timespan=Timespan.MINUTE, multiplier=5)) is None
    assert flat_file_prefix(STOCK_BARS, request(timespan=Timespan.HOUR)) is None
    assert flat_file_prefix(YIELDS, request(None)) is None


def test_rest_history_problem() -> None:
    assert rest_history_problem(STOCK_TRADES, request()) is None
    assert rest_history_problem(NOWHERE, request(None)) == "economy.nowhere has no REST history"
    every = "stocks.trades history over REST needs explicit symbols"
    assert rest_history_problem(STOCK_TRADES, request(dm.ALL)) == every
    daily = request(dm.ALL, timespan=Timespan.DAY)
    assert rest_history_problem(STOCK_BARS, daily) is None  # the grouped-daily endpoint
    assert rest_history_problem(INDEX_BARS, daily) is not None  # no grouped-daily endpoint
    five = request(dm.ALL, timespan=Timespan.DAY, multiplier=5)
    assert rest_history_problem(STOCK_BARS, five) is not None
    minute = request(dm.ALL, timespan=Timespan.MINUTE)
    assert rest_history_problem(STOCK_BARS, minute) is not None


# --- choose_source: every branch of the AUTO rule --------------------------------------------

MARCH_3 = (date(2025, 3, 3),)


def decide(
    spec: DatasetSpec,
    req: Request,
    *,
    requested: HistorySource = HistorySource.AUTO,
    cfg: dm.MassiveConfig | None = None,
    now: datetime = NOW,
    dates: tuple[date, ...] = MARCH_3,
) -> SourceChoice:
    return choose_source(spec, req, dates, requested=requested, config=cfg or config(), now=now)


def choose(spec: DatasetSpec, req: Request, **kwargs: object) -> HistorySource:
    return decide(spec, req, **cast("dict[str, Any]", kwargs)).source


def test_auto_with_neither_source_lists_every_violated_constraint() -> None:
    bars = request(dm.ALL, timespan=Timespan.HOUR, multiplier=5, adjusted=True)
    with pytest.raises(ValueError, match=r"stocks\.bars history has no possible source") as info:
        choose(
            STOCK_BARS,
            bars,
            cfg=NO_S3,
            now=datetime(2025, 3, 4, 15, tzinfo=UTC),  # 10:00 New York: March 3 not published
        )
    assert str(info.value) == (
        "stocks.bars history has no possible source. Flat files: "
        "no S3 credentials are configured (MassiveConfig s3_access_key_id and "
        "s3_secret_access_key, or MASSIVE_S3_ACCESS_KEY_ID and MASSIVE_S3_SECRET_ACCESS_KEY); "
        "stocks.bars flat files hold only 1-minute, 1-day bars; "
        "flat-file bars are unadjusted; pass adjusted=False; "
        "the file for 2025-03-03 is not yet published; flat files land ~11:00 ET next day "
        "(a day's file counts as published from 11:30 New York time after the day ends). "
        "REST: stocks.bars history over REST needs explicit symbols."
    )


def test_auto_with_only_flat_files_possible_reads_flat_files() -> None:
    # indices.values has no REST history, so even one symbol reads flat files.
    assert decide(INDEX_VALUES_SPEC, request(("I:SPX",))) == SourceChoice(
        HistorySource.FLAT_FILES, "REST cannot serve it: indices.values has no REST history"
    )


def test_auto_with_only_rest_possible_uses_rest() -> None:
    choice = decide(STOCK_TRADES, request(many(500)), cfg=NO_S3)
    assert choice.source is HistorySource.REST
    assert choice.reason.startswith("flat files cannot serve it: no S3 credentials")
    assert decide(YIELDS, request(None)) == SourceChoice(
        HistorySource.REST, "flat files cannot serve it: economy.yields has no flat files"
    )


@pytest.mark.parametrize(
    ("symbols", "expected"),
    [
        (many(49), SourceChoice(HistorySource.REST, "49 symbols < flat_file_symbol_threshold 50")),
        (
            many(50),
            SourceChoice(HistorySource.FLAT_FILES, "50 symbols >= flat_file_symbol_threshold 50"),
        ),
        (dm.ALL, SourceChoice(HistorySource.FLAT_FILES, "every symbol")),
    ],
)
def test_auto_with_both_possible_switches_at_the_threshold(
    symbols: object, expected: SourceChoice
) -> None:
    # The daily grouped endpoint makes REST possible for dm.ALL too.
    req = request(symbols, timespan=Timespan.DAY, adjusted=False)
    assert decide(STOCK_BARS, req) == expected


def test_auto_threshold_comes_from_the_config() -> None:
    cfg = config(flat_file_symbol_threshold=3)
    assert choose(STOCK_TRADES, request(many(2)), cfg=cfg) is HistorySource.REST
    assert choose(STOCK_TRADES, request(many(3)), cfg=cfg) is HistorySource.FLAT_FILES


def test_flat_files_forced_reads_flat_files_or_names_every_violation() -> None:
    forced = HistorySource.FLAT_FILES
    assert decide(STOCK_TRADES, request(), requested=forced) == SourceChoice(forced, "requested")
    with pytest.raises(ValueError, match="cannot be read from flat files") as info:
        choose(
            STOCK_TRADES,
            request(),
            requested=forced,
            cfg=NO_S3,
            now=datetime(2025, 3, 4, 10, tzinfo=NEW_YORK),
        )
    assert str(info.value) == (
        "stocks.trades cannot be read from flat files: no S3 credentials are configured "
        "(MassiveConfig s3_access_key_id and s3_secret_access_key, or MASSIVE_S3_ACCESS_KEY_ID "
        "and MASSIVE_S3_SECRET_ACCESS_KEY); the file for 2025-03-03 is not yet published; flat "
        "files land ~11:00 ET next day (a day's file counts as published from 11:30 New York "
        "time after the day ends)"
    )


@pytest.mark.parametrize(
    ("spec", "timespan", "multiplier", "adjusted", "problem"),
    [
        (STOCK_BARS, Timespan.MINUTE, 1, False, None),
        (STOCK_BARS, Timespan.DAY, 1, False, None),
        (STOCK_BARS, Timespan.HOUR, 1, False, "hold only 1-minute, 1-day bars"),
        (STOCK_BARS, Timespan.MINUTE, 5, False, "hold only 1-minute, 1-day"),
        (STOCK_BARS, Timespan.DAY, 1, True, "pass adjusted=False"),
        (INDEX_BARS, Timespan.DAY, 1, None, None),  # no adjusted parameter
        (FUTURES_BARS, Timespan.SESSION, 1, None, None),
        (FUTURES_BARS, Timespan.MINUTE, 1, None, None),
        (FUTURES_BARS, Timespan.HOUR, 1, None, "hold only 1-minute, 1-session bars"),
        (replace(STOCK_BARS, flat_file_prefixes={}), Timespan.DAY, 1, None, "has no flat files"),
        (YIELDS, None, 1, None, "economy.yields has no flat files"),
    ],
)
def test_flat_file_constraints_of_bars_and_datasets(
    *,
    spec: DatasetSpec,
    timespan: Timespan | None,
    multiplier: int,
    adjusted: bool | None,
    problem: str | None,
) -> None:
    req = request(timespan=timespan, multiplier=multiplier, adjusted=adjusted)
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
    assert choose(STOCK_TRADES, request(many(50)), now=now) is expected


def test_the_publish_cutoff_comes_from_the_config() -> None:
    now = datetime(2025, 3, 4, 9, 30, tzinfo=NEW_YORK)
    early = config(flat_file_publish_cutoff=time(9))
    forced = HistorySource.FLAT_FILES
    assert choose(STOCK_TRADES, request(), requested=forced, now=now, cfg=early) is forced
    with pytest.raises(ValueError, match="not yet published"):
        choose(STOCK_TRADES, request(), requested=forced, now=now)


def test_futures_are_published_by_their_chicago_session_date() -> None:
    # With a 00:30 New York cutoff, a stocks day is published 30 minutes after New York's
    # midnight; a futures session only from the first 00:30 New York after Chicago's midnight,
    # which falls at 01:00 New York, so on the following night.
    cutoff = config(flat_file_publish_cutoff=time(0, 30))
    forced = HistorySource.FLAT_FILES
    req = request(("ESH5",))
    night = datetime(2025, 3, 4, 0, 45, tzinfo=NEW_YORK)
    next_night = datetime(2025, 3, 5, 0, 45, tzinfo=NEW_YORK)
    assert choose(STOCK_TRADES, req, requested=forced, now=night, cfg=cutoff) is forced
    with pytest.raises(ValueError, match="2025-03-03 is not yet published"):
        choose(FUTURES_TRADES, req, requested=forced, now=night, cfg=cutoff)
    assert choose(FUTURES_TRADES, req, requested=forced, now=next_night, cfg=cutoff) is forced


def test_every_partition_date_must_be_published() -> None:
    now = datetime(2025, 3, 5, 12, tzinfo=NEW_YORK)  # March 4 is published, March 5 is not
    dates = (date(2025, 3, 4), date(2025, 3, 5))
    with pytest.raises(ValueError, match="2025-03-05 is not yet published"):
        choose(STOCK_TRADES, request(), requested=HistorySource.FLAT_FILES, now=now, dates=dates)


def test_the_clock_is_read_when_no_instant_is_given(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _history_source._now().tzinfo is UTC

    def before_the_cutoff() -> datetime:
        return datetime(2025, 3, 4, 11, 29, tzinfo=NEW_YORK)

    monkeypatch.setattr(_history_source, "_now", before_the_cutoff)
    with pytest.raises(ValueError, match="2025-03-03 is not yet published"):
        choose_source(
            STOCK_TRADES, request(), MARCH_3, requested=HistorySource.FLAT_FILES, config=config()
        )


@pytest.mark.parametrize(
    ("spec", "day", "now", "late"),
    [
        # March 4's stocks file is published at 11:30 New York on March 5, so it may be late
        # until 11:30 on March 6.
        (STOCK_TRADES, date(2025, 3, 4), datetime(2025, 3, 5, 11, tzinfo=NEW_YORK), True),
        (STOCK_TRADES, date(2025, 3, 4), datetime(2025, 3, 6, 11, 29, tzinfo=NEW_YORK), True),
        (STOCK_TRADES, date(2025, 3, 4), datetime(2025, 3, 6, 11, 30, tzinfo=NEW_YORK), False),
        # A futures session's clock starts at Chicago's midnight, 01:00 New York, so its first
        # 11:30 New York is the same morning; a forex day's at UTC midnight.
        (FUTURES_TRADES, date(2025, 3, 4), datetime(2025, 3, 6, 11, 29, tzinfo=NEW_YORK), True),
        (FUTURES_TRADES, date(2025, 3, 4), datetime(2025, 3, 6, 11, 30, tzinfo=NEW_YORK), False),
        (FOREX_BARS, date(2025, 3, 4), datetime(2025, 3, 6, 11, 29, tzinfo=NEW_YORK), True),
    ],
)
def test_within_publication_grace(
    *, spec: DatasetSpec, day: date, now: datetime, late: bool
) -> None:
    assert within_publication_grace(spec, day, cutoff=time(11, 30), now=now) is late


def test_publication_grace_reads_the_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    def much_later() -> datetime:
        return datetime(2025, 4, 1, tzinfo=UTC)

    monkeypatch.setattr(_history_source, "_now", much_later)
    assert within_publication_grace(STOCK_TRADES, date(2025, 3, 4), cutoff=time(11, 30)) is False
