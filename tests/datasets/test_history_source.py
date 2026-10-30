"""Tests for the `history()` source choice: partition dates, publication, REST, and AUTO."""

from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, cast
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.bar_periods import futures_bar_request
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.dates import (
    DaySpan,
    TimeRange,
)
from deephaven_massive._datasets.futures_sessions import CHICAGO
from deephaven_massive._datasets.history_source import (
    WEEKEND_PUBLICATION_DELAY,
    Alternative,
    HistoryWindow,
    SourceChoice,
    choose_source,
    flat_file_prefix,
    flat_file_published,
    history_window,
    partition_dates,
    publish_time,
    published,
    rest_history_problem,
    within_publication_grace,
)
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.row_times import (
    BarBounds,
    RowTimes,
    StartRule,
)
from deephaven_massive._enums import AssetClass, HistorySource, NotBars, Timespan
from deephaven_massive._massive import Transport

NEW_YORK = ZoneInfo("America/New_York")
FF = Transport.FLAT_FILES
NOW = datetime(2025, 3, 10, 16, tzinfo=UTC)
"""A Monday afternoon, when every file of the first week of March 2025 is published."""


def files(
    prefixes: Mapping[Timespan | NotBars, str], time: str, *, session: bool = False
) -> dict[tuple[Transport, str], SourceKeys]:
    # The keys every flat-file prefix declares: its ticker, its time, and a futures session.
    keys = SourceKeys(
        KeySpec("Ticker"), KeySpec(time), date=KeySpec("SessionEndDate") if session else None
    )
    return {(FF, prefix): keys for prefix in prefixes.values()}


def bars(
    spec: DatasetSpec,
    name: str,
    *,
    asset_class: AssetClass,
    prefixes: dict[Timespan | NotBars, str],
) -> DatasetSpec:
    # Another class's bars: its own name, class, and flat files.
    return replace(
        spec,
        name=name,
        asset_class=asset_class,
        flat_file_prefixes=prefixes,
        keys=files(prefixes, "WindowStart"),
    )


STOCK_TRADES = DatasetSpec(
    name="stocks.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    history_path="/v3/trades/{stockTicker}",
    flat_file_prefixes={NotBars.ANY: "us_stocks_sip/trades_v1"},
    keys=files({NotBars.ANY: "us_stocks_sip/trades_v1"}, "SipTimestamp"),
)
STOCK_BAR_FILES: dict[Timespan | NotBars, str] = {
    Timespan.MINUTE: "us_stocks_sip/minute_aggs_v1",
    Timespan.DAY: "us_stocks_sip/day_aggs_v1",
}
STOCK_BARS = DatasetSpec(
    name="stocks.bars",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    is_bars=True,
    history_path="/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}",
    grouped_daily_path="/v2/aggs/grouped/locale/us/market/stocks/{date}",
    flat_file_prefixes=STOCK_BAR_FILES,
    keys=files(STOCK_BAR_FILES, "WindowStart"),
)
INDEX_BARS = replace(
    bars(
        STOCK_BARS,
        "indices.bars",
        asset_class=AssetClass.INDICES,
        prefixes={
            Timespan.MINUTE: "us_indices/minute_aggs_v1",
            Timespan.DAY: "us_indices/day_aggs_v1",
        },
    ),
    grouped_daily_path=None,
)
FOREX_BARS = bars(
    STOCK_BARS,
    "forex.bars",
    asset_class=AssetClass.FOREX,
    prefixes={
        Timespan.MINUTE: "global_forex/minute_aggs_v1",
        Timespan.DAY: "global_forex/day_aggs_v1",
    },
)
CRYPTO_BARS = bars(
    STOCK_BARS,
    "crypto.bars",
    asset_class=AssetClass.CRYPTO,
    prefixes={
        Timespan.MINUTE: "global_crypto/minute_aggs_v1",
        Timespan.DAY: "global_crypto/day_aggs_v1",
    },
)
INDEX_VALUES_SPEC = DatasetSpec(
    name="indices.values",
    summary="A dataset for the tests.",
    asset_class=AssetClass.INDICES,
    flat_file_prefixes={NotBars.ANY: "us_indices/values_v1"},
    keys=files({NotBars.ANY: "us_indices/values_v1"}, "Timestamp"),
)
FUTURES_TRADES = DatasetSpec(
    name="futures.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.FUTURES,
    row_times=RowTimes.TICK,
    history_path="/futures/v1/trades/{ticker}",
    flat_file_prefixes={NotBars.ANY: "us_futures_{venue}/trades_v1"},
    keys=files({NotBars.ANY: "us_futures_{venue}/trades_v1"}, "Timestamp", session=True),
)
FUTURES_BAR_FILES: dict[Timespan | NotBars, str] = {
    Timespan.MINUTE: "us_futures_{venue}/minute_aggs_v1",
    Timespan.SESSION: "us_futures_{venue}/session_aggs_v1",
}
FUTURES_BARS = DatasetSpec(
    name="futures.bars",
    summary="A dataset for the tests.",
    asset_class=AssetClass.FUTURES,
    is_bars=True,
    history_path="/futures/v1/aggs/{ticker}",
    flat_file_prefixes=FUTURES_BAR_FILES,
    keys=files(FUTURES_BAR_FILES, "WindowStart", session=True),
)
YIELDS = DatasetSpec(
    name="economy.yields",
    summary="A dataset for the tests.",
    poll_keys=("Date",),
    history_path="/fed/v1/treasury-yields",
)
# Served by fetch() alone, so it has no history source at all.
NOWHERE = replace(YIELDS, name="economy.nowhere", history_path=None, fetch_path="/fed/v1/inflation")


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


def window(
    spec: DatasetSpec,
    start: object,
    end: object,
    *,
    now: datetime = NOW,
    **fields: object,
) -> HistoryWindow:
    return history_window(
        spec,
        Request(**cast("dict[str, Any]", fields)),
        start=cast("Any", start),
        end=cast("Any", end),
        now=now,
    )


# --- history_window --------------------------------------------------------------------------


def test_ticks_keep_the_range_and_request_it() -> None:
    days = window(STOCK_TRADES, "2025-03-03", "2025-03-04")
    instants = DaySpan(date(2025, 3, 3), date(2025, 3, 4)).instants(NEW_YORK)
    assert days == HistoryWindow(
        DaySpan(date(2025, 3, 3), date(2025, 3, 4)), instants, None, instants
    )
    start = datetime(2025, 3, 3, 9, 30, tzinfo=NEW_YORK)
    exact = window(STOCK_TRADES, start, None)
    assert exact.times == TimeRange(start.astimezone(UTC), NOW)
    assert exact.requested == exact.times
    assert not exact.empty


def test_equal_datetimes_are_an_empty_window() -> None:
    moment = datetime(2025, 3, 3, 12, tzinfo=UTC)
    empty = window(STOCK_TRADES, moment, moment)
    assert empty.empty
    assert partition_dates(STOCK_TRADES, empty) == ()


def test_bars_keep_whole_windows_and_request_from_the_first() -> None:
    start = datetime(2025, 3, 3, 9, 30, tzinfo=NEW_YORK)
    end = datetime(2025, 3, 5, 9, 30, tzinfo=NEW_YORK)
    days = window(STOCK_BARS, start, end, timespan=Timespan.DAY)
    tuesday = datetime(2025, 3, 4, tzinfo=NEW_YORK).astimezone(UTC)
    assert days.times == TimeRange(tuesday, datetime(2025, 3, 5, tzinfo=NEW_YORK).astimezone(UTC))
    assert days.requested == TimeRange(tuesday, end.astimezone(UTC))
    assert partition_dates(STOCK_BARS, days) == (date(2025, 3, 4),)
    # A range shorter than one bar keeps nothing.
    assert window(STOCK_BARS, "2025-03-04", "2025-03-06", timespan=Timespan.WEEK).empty


def test_unfinished_bars_are_left_out() -> None:
    # NOW is Monday 2025-03-10 at 12:00 in New York: that day has not ended.
    days = window(STOCK_BARS, "2025-03-06", None, timespan=Timespan.DAY)
    assert days.span == DaySpan(date(2025, 3, 6), date(2025, 3, 10))
    assert partition_dates(STOCK_BARS, days) == tuple(date(2025, 3, d) for d in (6, 7, 8, 9))
    future = window(STOCK_BARS, "2025-03-01", "2025-03-31", timespan=Timespan.MONTH)
    assert future.empty  # March has not ended


def test_a_bar_must_lie_in_the_range_and_end_by_now() -> None:
    # Ticks have no bar window.
    assert window(STOCK_TRADES, "2025-03-03", "2025-03-04").bars is None
    # Dates start with their first day and end with their last: midnight after it, 01:00 New
    # York time for indices.
    stocks = window(STOCK_BARS, "2025-03-03", "2025-03-04", timespan=Timespan.DAY)
    assert stocks.bars == BarBounds(
        datetime(2025, 3, 3, tzinfo=NEW_YORK),
        datetime(2025, 3, 5, tzinfo=NEW_YORK),
        StartRule.RANGE,
    )
    indices = window(INDEX_BARS, "2025-03-03", "2025-03-04", timespan=Timespan.DAY)
    assert cast("BarBounds", indices.bars).latest == datetime(2025, 3, 5, 1, tzinfo=NEW_YORK)
    # Datetimes end at their end; an open end, or one after now, at now.
    end = datetime(2025, 3, 5, 9, 30, tzinfo=NEW_YORK)
    exact = window(STOCK_BARS, "2025-03-03T00:00:00-05:00", end, timespan=Timespan.DAY)
    assert cast("BarBounds", exact.bars).latest == end
    open_end = window(STOCK_BARS, "2025-03-03", None, timespan=Timespan.DAY)
    assert cast("BarBounds", open_end.bars).latest == NOW
    # Futures dates end at 17:00 Chicago on the last session's date, and are kept by their
    # label, not their start: a folded session opens before its first date.
    span = ("2025-03-03", "2025-03-05")
    for timespan in (Timespan.MINUTE, Timespan.SESSION):
        sessions = window(FUTURES_BARS, *span, timespan=timespan)
        assert sessions.bars == BarBounds(
            datetime(2025, 3, 2, 17, tzinfo=CHICAGO),
            datetime(2025, 3, 5, 17, tzinfo=CHICAGO),
            StartRule.LABEL,
        )


def test_an_index_month_covers_its_weekend_days_for_its_start_too() -> None:
    # June 2024 starts on a Saturday. A stock's month starts Monday the 3rd, so a range from
    # the 3rd holds it; an index can update at weekends, so its month starts on the 1st, and
    # only a range from the 1st holds it.
    stock = window(STOCK_BARS, "2024-06-03", "2024-06-30", timespan=Timespan.MONTH)
    index = window(INDEX_BARS, "2024-06-03", "2024-06-30", timespan=Timespan.MONTH)
    whole = window(INDEX_BARS, "2024-06-01", "2024-06-30", timespan=Timespan.MONTH)
    assert not stock.empty
    assert index.empty
    assert not whole.empty


def test_futures_trades_by_date_are_whole_sessions_requested_one_at_a_time() -> None:
    sessions = window(FUTURES_TRADES, "2025-03-03", "2025-03-05")
    span = DaySpan(date(2025, 3, 3), date(2025, 3, 5))
    assert sessions == HistoryWindow(span, None, span, None)
    assert partition_dates(FUTURES_TRADES, sessions) == span.days()


def test_the_session_date_parameter_is_read_from_the_history_path() -> None:
    assert history_source.session_date_param(FUTURES_TRADES) == "session_end_date"


def test_futures_bars_by_date() -> None:
    span = DaySpan(date(2025, 3, 3), date(2025, 3, 7))
    minute = window(FUTURES_BARS, "2025-03-03", "2025-03-07", timespan=Timespan.MINUTE)
    assert minute.sessions == span
    assert minute.requested == futures_bar_request(span)
    assert minute.times is not None
    assert minute.times.start == minute.requested.start
    # A session bar of a session still trading is left out: the session of 03-10 opened at
    # 17:00 Chicago on 03-09, and NOW is 11:00 Chicago on 03-10.
    sessions = window(FUTURES_BARS, "2025-03-06", None, timespan=Timespan.SESSION)
    # No session ends on a Saturday or Sunday, so the files read are Thursday's and Friday's.
    assert sessions.sessions == DaySpan(date(2025, 3, 6), date(2025, 3, 7))
    assert sessions.times is None
    assert partition_dates(FUTURES_BARS, sessions) == (date(2025, 3, 6), date(2025, 3, 7))
    # So is an intraday bar that has not ended.
    live = window(FUTURES_BARS, "2025-03-10", None, timespan=Timespan.MINUTE)
    assert live.times is not None
    assert live.times.end == NOW
    weeks = window(FUTURES_BARS, "2025-03-04", "2025-03-07", timespan=Timespan.WEEK)
    assert weeks.empty


def test_futures_bars_by_instants_request_them_exactly() -> None:
    start = datetime(2025, 3, 3, 17, tzinfo=CHICAGO)
    end = datetime(2025, 3, 4, 17, tzinfo=CHICAGO)
    bars_window = window(FUTURES_BARS, start, end, timespan=Timespan.SESSION)
    exact = TimeRange(start.astimezone(UTC), end.astimezone(UTC))
    assert bars_window.requested == exact
    assert bars_window.times == exact
    assert partition_dates(FUTURES_BARS, bars_window) == (date(2025, 3, 4),)


def test_published_reads_the_class_zone_and_the_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    cutoff = time(11, 30)
    assert published(STOCK_TRADES, date(2025, 3, 7), cutoff=cutoff, now=NOW)
    assert not published(STOCK_TRADES, date(2025, 3, 10), cutoff=cutoff, now=NOW)
    monkeypatch.setattr(history_source, "_now", lambda: NOW)
    assert published(STOCK_TRADES, date(2025, 3, 7), cutoff=cutoff)


# --- partition_dates -------------------------------------------------------------------------


def test_partition_dates_cover_the_range_in_the_class_day_zone() -> None:
    days = window(STOCK_TRADES, "2025-03-03", "2025-03-05")
    assert partition_dates(STOCK_TRADES, days) == (
        date(2025, 3, 3),
        date(2025, 3, 4),
        date(2025, 3, 5),
    )
    # The end is exclusive: midnight New York names no file of its own.
    exact = window(
        STOCK_TRADES,
        datetime(2025, 3, 3, 10, tzinfo=NEW_YORK),
        datetime(2025, 3, 4, tzinfo=NEW_YORK),
    )
    assert partition_dates(STOCK_TRADES, exact) == (date(2025, 3, 3),)


def instants(start: datetime, end: datetime) -> HistoryWindow:
    span = TimeRange(start.astimezone(UTC), end.astimezone(UTC))
    return HistoryWindow(span, span, None, span)


def test_partition_dates_of_forex_are_utc_dates() -> None:
    evening = datetime(2025, 3, 3, 22, tzinfo=NEW_YORK)  # 03:00 UTC on March 4
    exact = instants(evening, datetime(2025, 3, 4, 4, tzinfo=UTC))
    assert partition_dates(FOREX_BARS, exact) == (date(2025, 3, 4),)
    assert partition_dates(STOCK_BARS, exact) == (date(2025, 3, 3),)


def test_partition_dates_without_a_class_are_new_york_dates() -> None:
    evening = instants(
        datetime(2025, 3, 3, 22, tzinfo=NEW_YORK), datetime(2025, 3, 3, 23, tzinfo=NEW_YORK)
    )
    assert partition_dates(YIELDS, evening) == (date(2025, 3, 3),)  # March 4 in UTC


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
    exact = window(
        FUTURES_TRADES,
        datetime(2025, 3, 3, *start, tzinfo=CHICAGO),
        datetime(2025, 3, 3, *end, tzinfo=CHICAGO),
    )
    assert partition_dates(FUTURES_TRADES, exact) == sessions


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
        "adjusted=True needs REST, since flat-file bars are unadjusted (as traded) and one "
        "history() never mixes adjusted and unadjusted prices; pass adjusted=False for the "
        "flat files; "
        "the file for 2025-03-03 is not yet published; flat files land ~11:00 ET next day "
        "(it counts as published from 2025-03-04 11:30 New York time). "
        "REST: stocks.bars history over REST needs explicit symbols."
    )


def test_auto_with_only_flat_files_possible_reads_flat_files() -> None:
    # indices.values has no REST history, so even one symbol reads flat files.
    assert decide(INDEX_VALUES_SPEC, request(("I:SPX",))) == SourceChoice(
        HistorySource.FLAT_FILES,
        "REST cannot serve it: indices.values has no REST history",
        Alternative.UNAVAILABLE,
    )


def test_auto_with_only_rest_possible_uses_rest() -> None:
    choice = decide(STOCK_TRADES, request(many(500)), cfg=NO_S3)
    assert choice.source is HistorySource.REST
    assert choice.reason.startswith("flat files cannot serve it: no S3 credentials")
    assert decide(YIELDS, request(None)) == SourceChoice(
        HistorySource.REST,
        "flat files cannot serve it: economy.yields has no flat files",
        Alternative.UNAVAILABLE,
    )


@pytest.mark.parametrize(
    ("symbols", "cfg", "expected"),
    [
        # Only the unpublished file holds flat files back, and 500 symbols read them once it
        # lands: the same call changes source the next day.
        (many(500), None, Alternative.AFTER_PUBLICATION),
        (dm.ALL, None, Alternative.AFTER_PUBLICATION),
        # Two symbols stay on REST after publication too: nothing changes.
        (many(2), None, Alternative.UNAVAILABLE),
        # A reason that does not change with time comes with it.
        (many(500), "no s3", Alternative.UNAVAILABLE),
    ],
)
def test_an_unpublished_file_is_the_one_reason_that_changes(
    symbols: object, cfg: str | None, expected: Alternative
) -> None:
    unpublished = datetime(2025, 3, 4, 10, tzinfo=NEW_YORK)  # March 3's file lands at 11:30
    req = request(symbols, timespan=Timespan.DAY, adjusted=False)
    choice = decide(STOCK_BARS, req, now=unpublished, cfg=NO_S3 if cfg else None)
    assert (choice.source, choice.alternative) == (HistorySource.REST, expected)


@pytest.mark.parametrize(
    ("symbols", "expected"),
    [
        (
            many(49),
            SourceChoice(
                HistorySource.REST,
                "49 symbols < flat_file_symbol_threshold 50",
                Alternative.AVAILABLE,
            ),
        ),
        (
            many(50),
            SourceChoice(
                HistorySource.FLAT_FILES,
                "50 symbols >= flat_file_symbol_threshold 50",
                Alternative.AVAILABLE,
            ),
        ),
        (dm.ALL, SourceChoice(HistorySource.FLAT_FILES, "every symbol", Alternative.AVAILABLE)),
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
        "files land ~11:00 ET next day (it counts as published from 2025-03-04 11:30 New York "
        "time)"
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
        (
            replace(STOCK_BARS, flat_file_prefixes={}, keys={}),
            Timespan.DAY,
            1,
            None,
            "has no flat files",
        ),
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
    assert history_source._now().tzinfo is UTC

    def before_the_cutoff() -> datetime:
        return datetime(2025, 3, 4, 11, 29, tzinfo=NEW_YORK)

    monkeypatch.setattr(history_source, "_now", before_the_cutoff)
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

    monkeypatch.setattr(history_source, "_now", much_later)
    assert within_publication_grace(STOCK_TRADES, date(2025, 3, 4), cutoff=time(11, 30)) is False


def test_crypto_and_forex_day_bars_follow_the_usual_auto_rule() -> None:
    # Every source stamps them alike, and a crypto stamp off the cut fails loudly, so AUTO
    # picks the source by symbol count as for every other dataset.
    for spec, pair in ((CRYPTO_BARS, "X:BTCUSD"), (FOREX_BARS, "C:EURUSD")):
        assert choose(spec, request((pair,), timespan=Timespan.DAY)) is HistorySource.REST
        assert choose(spec, request(many(60), timespan=Timespan.DAY)) is (HistorySource.FLAT_FILES)
    # An unpublished day sends the whole range to REST, with S3 configured or not.
    unpublished = (date(2025, 3, 10),)
    crypto = request(many(60), timespan=Timespan.DAY)
    assert choose(CRYPTO_BARS, crypto, dates=unpublished) is HistorySource.REST


def test_futures_bars_of_several_sessions_by_date_fit_under_both_label_readings() -> None:
    # Whether Massive labels such a bar with its first or last session is not documented, so
    # the dates keep the labels whose bar fits either way.
    sessions = window(
        FUTURES_BARS, "2025-03-03", "2025-03-07", timespan=Timespan.SESSION, multiplier=2
    )
    assert sessions.sessions == DaySpan(date(2025, 3, 4), date(2025, 3, 6))
    assert sessions.times is None
    months = window(FUTURES_BARS, "2024-11-01", "2025-01-31", timespan=Timespan.MONTH, multiplier=2)
    assert months.sessions == DaySpan(date(2024, 12, 1), date(2024, 12, 31))
    weeks = window(FUTURES_BARS, "2025-03-03", "2025-03-07", timespan=Timespan.WEEK, multiplier=2)
    assert weeks.empty
    # Datetimes, multiplier 1, and intraday multiples are fine.
    start = datetime(2025, 3, 2, 17, tzinfo=CHICAGO)
    assert not window(
        FUTURES_BARS, start, start + timedelta(days=7), timespan=Timespan.SESSION, multiplier=2
    ).empty
    assert window(FUTURES_BARS, "2025-03-03", "2025-03-07", timespan=Timespan.SESSION).sessions
    assert window(
        FUTURES_BARS, "2025-03-03", "2025-03-07", timespan=Timespan.MINUTE, multiplier=5
    ).sessions


@pytest.mark.parametrize("key", ["session_end_date", "session_end_date_gte", "session_end_date_lt"])
def test_a_session_end_date_filter_needs_datetimes(key: str) -> None:
    filters = ((key, date(2025, 3, 4)),)
    with pytest.raises(ValueError, match=r"history\(\) dates set session_end_date themselves"):
        window(FUTURES_TRADES, "2025-03-04", "2025-03-04", filters=filters)
    start = datetime(2025, 3, 4, 8, tzinfo=CHICAGO)
    assert window(FUTURES_TRADES, start, start + timedelta(hours=1), filters=filters).times


# --- Publication ----------------------------------------------------------------------------


CUTOFF = time(11, 30)


def test_publish_time_in_new_york() -> None:
    assert publish_time(date(2025, 3, 3), zone=NEW_YORK, cutoff=CUTOFF) == datetime(
        2025, 3, 4, 11, 30, tzinfo=NEW_YORK
    )


def test_publish_time_in_utc() -> None:
    # March 4 00:00 UTC is March 3 19:00 in New York; the first 11:30 after it is March 4.
    assert publish_time(date(2025, 3, 3), zone=UTC, cutoff=CUTOFF) == datetime(
        2025, 3, 4, 11, 30, tzinfo=NEW_YORK
    )
    # A cutoff later than 19:00 New York time falls on the same New York evening.
    late = time(20, 0)
    assert publish_time(date(2025, 3, 3), zone=UTC, cutoff=late) == datetime(
        2025, 3, 3, 20, 0, tzinfo=NEW_YORK
    )
    early = time(18, 0)
    assert publish_time(date(2025, 3, 3), zone=UTC, cutoff=early) == datetime(
        2025, 3, 4, 18, 0, tzinfo=NEW_YORK
    )


def test_publish_time_is_strictly_after_the_day_starts() -> None:
    # A cutoff at exactly the New York midnight that starts the next day is not "after" it.
    assert publish_time(date(2025, 3, 3), zone=NEW_YORK, cutoff=time(0, 0)) == datetime(
        2025, 3, 5, 0, 0, tzinfo=NEW_YORK
    )


def _published(partition: date, now: datetime) -> bool:
    return flat_file_published(partition, now=now, zone=NEW_YORK, cutoff=CUTOFF)


def test_flat_file_publish_cutoff() -> None:
    today = date(2025, 3, 4)
    yesterday = date(2025, 3, 3)
    before = datetime(2025, 3, 4, 11, 29, tzinfo=NEW_YORK)
    after = datetime(2025, 3, 4, 11, 31, tzinfo=NEW_YORK)
    assert not _published(yesterday, before)
    assert _published(yesterday, after)
    assert _published(date(2025, 3, 2), before)
    assert not _published(today, after)
    assert not _published(date(2025, 3, 5), after)


def test_flat_file_day_zone_is_the_class_zone() -> None:
    # 01:00 UTC on March 4 is 20:00 New York time on March 3. For a UTC class, March 3's file
    # lands at 11:30 New York time on March 4, so it is not published yet.
    now = datetime(2025, 3, 4, 1, tzinfo=UTC)
    march_3 = date(2025, 3, 3)
    assert not flat_file_published(march_3, now=now, zone=NEW_YORK, cutoff=CUTOFF)
    assert not flat_file_published(march_3, now=now, zone=UTC, cutoff=CUTOFF)
    assert flat_file_published(date(2025, 3, 2), now=now, zone=UTC, cutoff=CUTOFF)
    later = datetime(2025, 3, 4, 11, 30, tzinfo=NEW_YORK)
    assert flat_file_published(march_3, now=later, zone=UTC, cutoff=CUTOFF)


def test_publish_time_across_the_spring_dst_change() -> None:
    # Saturday March 8's crypto file has its cutoff on Sunday, so it counts three hours later.
    published = publish_time(date(2025, 3, 8), zone=UTC, cutoff=time(11, 30))
    assert published == datetime(2025, 3, 9, 14, 30, tzinfo=NEW_YORK)
    assert published.astimezone(UTC) == datetime(2025, 3, 9, 18, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    ("partition", "expected"),
    [
        (date(2026, 10, 29), datetime(2026, 10, 30, 11, 30, tzinfo=NEW_YORK)),  # Thu -> Fri
        (date(2026, 10, 30), datetime(2026, 10, 31, 14, 30, tzinfo=NEW_YORK)),  # Fri -> Sat
        (date(2026, 10, 31), datetime(2026, 11, 1, 14, 30, tzinfo=NEW_YORK)),  # Sat -> Sun
        (date(2026, 11, 1), datetime(2026, 11, 2, 11, 30, tzinfo=NEW_YORK)),  # Sun -> Mon
    ],
)
def test_a_cutoff_on_a_weekend_counts_three_hours_later(
    partition: date, expected: datetime
) -> None:
    # Friday's files landed on Saturday at 10:27 and 11:17 New York time, near the 11:30 cutoff.
    assert publish_time(partition, zone=NEW_YORK, cutoff=CUTOFF) == expected
    assert timedelta(hours=3) == WEEKEND_PUBLICATION_DELAY


def test_a_weekend_file_is_not_published_until_the_delayed_cutoff() -> None:
    friday = date(2026, 10, 30)
    at_noon = datetime(2026, 10, 31, 12, 0, tzinfo=NEW_YORK)
    assert not flat_file_published(friday, now=at_noon, zone=NEW_YORK, cutoff=CUTOFF)
    later = datetime(2026, 10, 31, 14, 30, tzinfo=NEW_YORK)
    assert flat_file_published(friday, now=later, zone=NEW_YORK, cutoff=CUTOFF)


def test_publication_compares_instants_across_the_fall_back_hour() -> None:
    # Saturday November 1's crypto file: its 22:30 EDT cutoff that Saturday evening, three hours
    # later in UTC, is 01:30 EDT on Sunday, the first of the two 01:30s of the fall back.
    cutoff = time(22, 30)
    published = publish_time(date(2025, 11, 1), zone=UTC, cutoff=cutoff)
    assert published.astimezone(UTC) == datetime(2025, 11, 2, 5, 30, tzinfo=UTC)  # 01:30 EDT
    # 01:15 EST (fold=1) is 06:15Z, 45 minutes after publication, though its wall clock reads
    # earlier than 01:30.
    later = datetime(2025, 11, 2, 1, 15, tzinfo=NEW_YORK, fold=1)
    assert flat_file_published(date(2025, 11, 1), now=later, zone=UTC, cutoff=cutoff)
    earlier = datetime(2025, 11, 2, 1, 15, tzinfo=NEW_YORK, fold=0)
    assert not flat_file_published(date(2025, 11, 1), now=earlier, zone=UTC, cutoff=cutoff)
