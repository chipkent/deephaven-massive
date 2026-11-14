"""Tests for the `history()` source choice: partition dates, publication, REST, and AUTO."""

from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.bar_periods import futures_bar_request
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.dates import (
    DateLike,
    DaySpan,
    TimeRange,
)
from deephaven_massive._datasets.futures_sessions import CHICAGO
from deephaven_massive._datasets.history_source import (
    Alternative,
    HistoryWindow,
    SourceChoice,
    choose_source,
    current_partition,
    flat_file_prefix,
    history_window,
    partition_dates,
    rest_history_problem,
)
from deephaven_massive._datasets.request import ParamValue, Request
from deephaven_massive._datasets.row_times import (
    BarBounds,
    FuturesBarCompletion,
    RowTimes,
    StartRule,
)
from deephaven_massive._datasets.symbols import NormalizedSymbols, SymbolTable
from deephaven_massive._enums import AssetClass, HistorySource, NotBars, Timespan
from deephaven_massive._errors import BrokenGuaranteeError
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


CONFIG = dm.MassiveConfig(
    api_key="test-key",
    s3_access_key_id="AKIDFLATFILES",
    s3_secret_access_key="flat-file-secret",  # noqa: S106  # fake credential
)
NO_S3 = replace(CONFIG, s3_access_key_id=None, s3_secret_access_key=None)
AAPL = ("AAPL",)


def many(count: int) -> tuple[str, ...]:
    return tuple(f"S{n:03d}" for n in range(count))


def window(
    spec: DatasetSpec,
    start: DateLike,
    end: DateLike | None,
    *,
    now: datetime = NOW,
    timespan: Timespan | None = None,
    multiplier: int = 1,
    filters: tuple[tuple[str, ParamValue], ...] = (),
) -> HistoryWindow:
    return history_window(
        spec,
        Request(timespan=timespan, multiplier=multiplier, filters=filters),
        start=start,
        end=end,
        now=now,
    )


# --- history_window --------------------------------------------------------------------------


def test_ticks_keep_the_range_and_request_it() -> None:
    days = window(STOCK_TRADES, "2025-03-03", "2025-03-04")
    instants = DaySpan(date(2025, 3, 3), date(2025, 3, 4)).instants(NEW_YORK)
    assert days == HistoryWindow(
        DaySpan(date(2025, 3, 3), date(2025, 3, 4)), instants, None, instants, now=NOW
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
    assert indices.bars is not None
    assert indices.bars.latest == datetime(2025, 3, 5, 1, tzinfo=NEW_YORK)
    # Datetimes end at their end; an open end, or one after now, at now.
    end = datetime(2025, 3, 5, 9, 30, tzinfo=NEW_YORK)
    exact = window(STOCK_BARS, "2025-03-03T00:00:00-05:00", end, timespan=Timespan.DAY)
    assert exact.bars is not None
    assert exact.bars.latest == end
    open_end = window(STOCK_BARS, "2025-03-03", None, timespan=Timespan.DAY)
    assert open_end.bars is not None
    assert open_end.bars.latest == NOW
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
    assert sessions == HistoryWindow(span, None, span, None, now=NOW)
    assert partition_dates(FUTURES_TRADES, sessions) == span.days()


def test_the_session_date_parameter_is_read_from_the_history_path() -> None:
    assert history_source.session_date_param(FUTURES_TRADES) == "session_end_date"


def test_futures_bars_by_date() -> None:
    span = DaySpan(date(2025, 3, 3), date(2025, 3, 7))
    minute = window(FUTURES_BARS, "2025-03-03", "2025-03-07", timespan=Timespan.MINUTE)
    assert minute.sessions == span
    assert minute.requested == futures_bar_request(span, timespan=Timespan.MINUTE, multiplier=1)
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


def test_futures_bars_by_instants_request_them_exactly() -> None:
    start = datetime(2025, 3, 3, 17, tzinfo=CHICAGO)
    end = datetime(2025, 3, 4, 17, tzinfo=CHICAGO)
    bars_window = window(FUTURES_BARS, start, end, timespan=Timespan.SESSION)
    exact = TimeRange(start.astimezone(UTC), end.astimezone(UTC))
    assert bars_window.requested == exact
    assert bars_window.times == exact
    assert partition_dates(FUTURES_BARS, bars_window) == (date(2025, 3, 4),)


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
    return HistoryWindow(span, span, None, span, now=NOW)


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
    assert flat_file_prefix(STOCK_TRADES, Request(symbols=AAPL)) == "us_stocks_sip/trades_v1"
    minute = Request(symbols=AAPL, timespan=Timespan.MINUTE)
    assert flat_file_prefix(STOCK_BARS, minute) == "us_stocks_sip/minute_aggs_v1"
    assert (
        flat_file_prefix(STOCK_BARS, Request(symbols=AAPL, timespan=Timespan.MINUTE, multiplier=5))
        is None
    )
    assert flat_file_prefix(STOCK_BARS, Request(symbols=AAPL, timespan=Timespan.HOUR)) is None
    assert flat_file_prefix(YIELDS, Request(symbols=None)) is None


def test_rest_history_problem() -> None:
    assert rest_history_problem(STOCK_TRADES, Request(symbols=AAPL)) is None
    assert (
        rest_history_problem(NOWHERE, Request(symbols=None))
        == "economy.nowhere has no REST history"
    )
    every = "stocks.trades history over REST needs explicit symbols"
    assert rest_history_problem(STOCK_TRADES, Request(symbols=dm.ALL)) == every
    daily = Request(symbols=dm.ALL, timespan=Timespan.DAY)
    assert rest_history_problem(STOCK_BARS, daily) is None  # the grouped-daily endpoint
    # No grouped-daily endpoint, or bars it does not serve: dm.ALL needs explicit symbols.
    index_every = "indices.bars history over REST needs explicit symbols"
    assert rest_history_problem(INDEX_BARS, daily) == index_every
    stock_every = "stocks.bars history over REST needs explicit symbols"
    five = Request(symbols=dm.ALL, timespan=Timespan.DAY, multiplier=5)
    assert rest_history_problem(STOCK_BARS, five) == stock_every
    minute = Request(symbols=dm.ALL, timespan=Timespan.MINUTE)
    assert rest_history_problem(STOCK_BARS, minute) == stock_every


# --- choose_source: every branch of the AUTO rule --------------------------------------------


def decide(
    spec: DatasetSpec,
    req: Request,
    *,
    requested: HistorySource = HistorySource.AUTO,
    cfg: dm.MassiveConfig | None = None,
) -> SourceChoice:
    return choose_source(spec, req, requested=requested, config=cfg or CONFIG)


def choose(
    spec: DatasetSpec,
    req: Request,
    *,
    requested: HistorySource = HistorySource.AUTO,
    cfg: dm.MassiveConfig | None = None,
) -> HistorySource:
    return decide(spec, req, requested=requested, cfg=cfg).source


def test_auto_with_neither_source_lists_every_violated_constraint() -> None:
    bars = Request(symbols=dm.ALL, timespan=Timespan.HOUR, multiplier=5, adjusted=True)
    with pytest.raises(ValueError, match=r"stocks\.bars history has no possible source") as info:
        choose(STOCK_BARS, bars, cfg=NO_S3)
    assert str(info.value) == (
        "stocks.bars history has no possible source. Flat files: "
        "no S3 credentials are configured (MassiveConfig s3_access_key_id and "
        "s3_secret_access_key, or MASSIVE_S3_ACCESS_KEY_ID and MASSIVE_S3_SECRET_ACCESS_KEY); "
        "stocks.bars flat files hold only 1-minute, 1-day bars; "
        "adjusted=True needs REST, since flat-file bars are unadjusted (as traded) and one "
        "history() never mixes adjusted and unadjusted prices; pass adjusted=False for the "
        "flat files. "
        "REST: stocks.bars history over REST needs explicit symbols."
    )


def test_auto_with_only_flat_files_possible_reads_flat_files() -> None:
    # indices.values has no REST history, so even one symbol reads flat files.
    assert decide(INDEX_VALUES_SPEC, Request(symbols=("I:SPX",))) == SourceChoice(
        HistorySource.FLAT_FILES,
        "REST cannot serve it: indices.values has no REST history",
        Alternative.UNAVAILABLE,
    )


def test_auto_with_only_rest_possible_uses_rest() -> None:
    choice = decide(STOCK_TRADES, Request(symbols=many(500)), cfg=NO_S3)
    assert choice.source is HistorySource.REST
    assert choice.reason.startswith("flat files cannot serve it: no S3 credentials")
    assert decide(YIELDS, Request(symbols=None)) == SourceChoice(
        HistorySource.REST,
        "flat files cannot serve it: economy.yields has no flat files",
        Alternative.UNAVAILABLE,
    )


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
    symbols: NormalizedSymbols, expected: SourceChoice
) -> None:
    # The daily grouped endpoint makes REST possible for dm.ALL too.
    req = Request(symbols=symbols, timespan=Timespan.DAY, adjusted=False)
    assert decide(STOCK_BARS, req) == expected


def test_auto_threshold_comes_from_the_config() -> None:
    cfg = replace(CONFIG, flat_file_symbol_threshold=3)
    assert choose(STOCK_TRADES, Request(symbols=many(2)), cfg=cfg) is HistorySource.REST
    assert choose(STOCK_TRADES, Request(symbols=many(3)), cfg=cfg) is HistorySource.FLAT_FILES


def test_flat_files_forced_reads_flat_files_or_names_every_violation() -> None:
    forced = HistorySource.FLAT_FILES
    assert decide(STOCK_TRADES, Request(symbols=AAPL), requested=forced) == SourceChoice(
        forced, "requested"
    )
    with pytest.raises(ValueError, match="cannot be read from flat files") as info:
        choose(STOCK_TRADES, Request(symbols=AAPL), requested=forced, cfg=NO_S3)
    assert str(info.value) == (
        "stocks.trades cannot be read from flat files: no S3 credentials are configured "
        "(MassiveConfig s3_access_key_id and s3_secret_access_key, or MASSIVE_S3_ACCESS_KEY_ID "
        "and MASSIVE_S3_SECRET_ACCESS_KEY)"
    )


def test_adjusted_bars_of_every_symbol_say_why_no_source_serves_them() -> None:
    # Flat files hold as-traded prices and REST reads minute bars one symbol at a time.
    req = Request(symbols=dm.ALL, timespan=Timespan.MINUTE, adjusted=True)
    with pytest.raises(ValueError, match=r"with adjusted=True has no source") as info:
        choose(STOCK_BARS, req)
    assert str(info.value) == (
        "stocks.bars history of every symbol (dm.ALL) with adjusted=True has no source: only "
        "REST serves split-adjusted bars, and REST reads bars one symbol at a time (every "
        "symbol at once only for 1-day bars), while flat files hold as-traded prices. Pass "
        "adjusted=False to read the as-traded flat files, or name the symbols to read adjusted "
        "bars from REST"
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
    req = Request(symbols=AAPL, timespan=timespan, multiplier=multiplier, adjusted=adjusted)
    forced = HistorySource.FLAT_FILES
    if problem is None:
        assert choose(spec, req, requested=forced) is HistorySource.FLAT_FILES
    else:
        with pytest.raises(ValueError, match=problem):
            choose(spec, req, requested=forced)


def test_crypto_and_forex_day_bars_follow_the_usual_auto_rule() -> None:
    # Every source stamps them alike, and a crypto stamp off the cut fails loudly, so AUTO
    # picks the source by symbol count as for every other dataset.
    for spec, pair in ((CRYPTO_BARS, "X:BTCUSD"), (FOREX_BARS, "C:EURUSD")):
        assert choose(spec, Request(symbols=(pair,), timespan=Timespan.DAY)) is HistorySource.REST
        assert choose(spec, Request(symbols=many(60), timespan=Timespan.DAY)) is (
            HistorySource.FLAT_FILES
        )


def test_longer_futures_bars_by_date_need_every_session_in_the_range() -> None:
    # Dates name sessions. A month bar's sessions are those that open in the month, so
    # September 2026's last session is that of 10-01: the range must reach it.
    now = datetime(2026, 11, 2, 12, tzinfo=CHICAGO)
    september = window(FUTURES_BARS, "2026-09-01", "2026-10-01", now=now, timespan=Timespan.MONTH)
    assert september.requested == TimeRange(
        datetime(2026, 9, 1, tzinfo=UTC), datetime(2026, 10, 1, 18, tzinfo=CHICAGO)
    )
    assert september.sessions == DaySpan(date(2026, 9, 1), date(2026, 10, 1))
    assert september.times is None
    assert september.bars == BarBounds(
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 10, 1, 17, tzinfo=CHICAGO),
        StartRule.RANGE,
        FuturesBarCompletion(Timespan.MONTH, 1, now),
    )
    # Two sessions from Monday 2026-08-03: the bucket of Saturday and Sunday holds only
    # Monday's session, so it may start there.
    two = window(
        FUTURES_BARS,
        "2026-08-03",
        "2026-08-07",
        now=now,
        timespan=Timespan.SESSION,
        multiplier=2,
    )
    assert two.requested is not None
    assert two.requested.start == datetime(2026, 8, 1, tzinfo=UTC)
    assert two.bars is not None
    assert two.bars.earliest == datetime(2026, 8, 1, tzinfo=UTC)
    # From a Wednesday, a bucket that starts on Tuesday holds Wednesday's session, which opens
    # on Tuesday evening, and maybe Tuesday's, which opens on Monday: it is left out.
    wednesday = window(
        FUTURES_BARS,
        "2026-08-05",
        "2026-08-07",
        now=now,
        timespan=Timespan.SESSION,
        multiplier=2,
    )
    assert wednesday.bars is not None
    assert wednesday.bars.earliest == datetime(2026, 8, 5, tzinfo=UTC)
    # A week's request starts on the Sunday of the first date's week, but its bars must
    # still start no earlier than the first date's own first bucket day.
    weeks = window(FUTURES_BARS, "2026-08-05", "2026-08-21", now=now, timespan=Timespan.WEEK)
    assert weeks.requested is not None
    assert weeks.requested.start == datetime(2026, 8, 2, tzinfo=UTC)
    assert weeks.bars is not None
    assert weeks.bars.earliest == datetime(2026, 8, 5, tzinfo=UTC)
    # From a Monday, a bucket may start on the Saturday before, but the week's request starts
    # on Sunday: a bucket Massive cut short at that `{from}` is left out.
    monday = window(FUTURES_BARS, "2026-08-03", "2026-08-21", now=now, timespan=Timespan.WEEK)
    assert monday.bars is not None
    assert monday.bars.earliest == datetime(2026, 8, 2, tzinfo=UTC)


def test_longer_futures_bars_by_instants_are_kept_by_their_own_window() -> None:
    # A futures bucket starts at 00:00 UTC, before its first session's 17:00 Chicago open, so
    # a range from 00:00 UTC keeps the bar that starts there: the times bound the start
    # exactly, not at a session's open.
    start = datetime(2026, 8, 1, tzinfo=UTC)
    end = datetime(2026, 9, 1, tzinfo=UTC)
    now = datetime(2026, 11, 2, 12, tzinfo=CHICAGO)
    monthly = window(FUTURES_BARS, start, end, now=now, timespan=Timespan.SESSION, multiplier=2)
    assert monthly.requested == TimeRange(start, end)
    assert monthly.times == TimeRange(start, end)
    assert monthly.bars == BarBounds(
        start, end, StartRule.RANGE, FuturesBarCompletion(Timespan.SESSION, 2, now)
    )


@pytest.mark.parametrize("key", ["session_end_date", "session_end_date_gte", "session_end_date_lt"])
def test_a_session_end_date_filter_needs_datetimes(key: str) -> None:
    filters = ((key, date(2025, 3, 4)),)
    with pytest.raises(ValueError, match=r"history\(\) dates set session_end_date themselves"):
        window(FUTURES_TRADES, "2025-03-04", "2025-03-04", filters=filters)
    start = datetime(2025, 3, 4, 8, tzinfo=CHICAGO)
    assert window(FUTURES_TRADES, start, start + timedelta(hours=1), filters=filters).times


# --- The current partition --------------------------------------------------------------


def test_the_current_partition_is_today_in_the_day_zone_or_the_current_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 20:00 New York on March 3 is March 4 in UTC, and the futures session of March 4 (it
    # opened at 17:00 Chicago).
    evening = datetime(2025, 3, 3, 20, tzinfo=NEW_YORK)
    assert current_partition(STOCK_TRADES, now=evening) == date(2025, 3, 3)
    assert current_partition(FOREX_BARS, now=evening) == date(2025, 3, 4)
    assert current_partition(FUTURES_TRADES, now=evening) == date(2025, 3, 4)
    monkeypatch.setattr(history_source, "_now", lambda: evening)
    assert current_partition(STOCK_TRADES) == date(2025, 3, 3)


def test_listed_count_raises_a_library_defect_for_a_symbols_form_with_no_rule() -> None:
    with pytest.raises(BrokenGuaranteeError, match="choice has no rule for a value of type object"):
        history_source._listed_count(object())  # type: ignore[arg-type]  # the point: refused at run time


def test_listed_count_raises_a_library_defect_for_a_symbol_table() -> None:
    # history() reads a symbol table into a list before it chooses a source.
    with pytest.raises(BrokenGuaranteeError, match="got a symbol table"):
        history_source._listed_count(SymbolTable(new_table([string_col("Symbol", [])])))
