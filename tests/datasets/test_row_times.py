"""Tests for the library's own time columns (dates, bar windows, arrival times) and bar stamps."""

import logging
from collections.abc import Sequence
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from typing import Any, cast
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from deephaven_massive import _log_limits
from deephaven_massive._datasets import row_times
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.dates import DAY_ZONES, NEW_YORK
from deephaven_massive._datasets.futures_sessions import CHICAGO
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.row_times import (
    DATE,
    END,
    INSERT,
    ISSUES_URL,
    LIBRARY_COLUMNS,
    START,
    TIMESTAMP,
    BarBounds,
    RowTimes,
    StampRule,
    StartRule,
    TimeRule,
    day_starts,
    local_dates,
    stamp_rule,
    time_column,
    time_rule,
    within,
)
from deephaven_massive._enums import AssetClass, ClassFromPrefix, Timespan
from deephaven_massive._log_limits import BoundedLog
from deephaven_massive._namespaces import (
    crypto,
    forex,
    futures,
    indicators,
    indices,
    options,
    stocks,
)

INSTANT = pa.timestamp("ns", tz="UTC")
A, T = AssetClass, Timespan


def ns(text: str) -> int:
    """Return an ISO instant (UTC unless it says otherwise) as epoch nanoseconds."""
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return int(moment.timestamp()) * 10**9


def instants(values: Sequence[str | None]) -> pa.Array:
    return pa.array([None if v is None else ns(v) for v in values], pa.int64()).cast(INSTANT)


def bars(
    starts: Sequence[str | None],
    *,
    dates: Sequence[str | None] | None = None,
    ends: Sequence[str | None] | None = None,
) -> pa.RecordBatch:
    # A batch of bars in the shape the transports hand over: Massive's start, its session or
    # summary date where a source sends one, its WebSocket end where it sends one.
    count = len(starts)
    return pa.RecordBatch.from_pydict(
        {
            "Symbol": pa.array(["X"] * count),
            DATE: pa.array(dates or [None] * count, pa.string()),
            START: instants(starts),
            END: instants(ends or [None] * count),
            "Close": pa.array([1.0] * count),
            INSERT: pa.nulls(count, INSTANT),
        }
    )


def ticks(times: Sequence[str | None], *, dates: Sequence[str | None] | None = None) -> pa.Table:
    count = len(times)
    return pa.table(
        {
            "Symbol": pa.array(["ESZ5"] * count),
            DATE: pa.array(dates or [None] * count, pa.string()),
            TIMESTAMP: instants(times),
            INSERT: pa.nulls(count, INSTANT),
        }
    )


def text(values: pa.Array | pa.ChunkedArray) -> list[str | None]:
    return [None if v is None else str(v) for v in values.to_pylist()]


def iso(values: pa.Array | pa.ChunkedArray) -> list[str | None]:
    return [None if v is None else v.isoformat() for v in values.to_pylist()]


def test_only_bars_whose_window_lies_in_the_range_are_kept() -> None:
    rule = TimeRule("stocks.bars", RowTimes.BAR, A.STOCKS, T.DAY, 1)
    filled = rule.fill(bars(["2025-03-03T05:00:00", "2025-03-04T05:00:00"]), received=7)
    early, late = datetime(2025, 3, 3, 5, tzinfo=UTC), datetime(2025, 3, 4, 5, tzinfo=UTC)
    by_label = BarBounds(late, late, StartRule.LABEL)
    assert iso(within(filled, by_label).column(START)) == ["2025-03-03T05:00:00+00:00"]
    assert within(filled, BarBounds(early, late, StartRule.RANGE)).num_rows == 1
    # Under the range rule, a start bound drops a bar that starts before it; by label it
    # bounds nothing.
    assert within(filled, BarBounds(late, late, StartRule.RANGE)).num_rows == 0
    # A row whose end is unknown is kept; rows without an end column are all kept.
    unknown = filled.set_column(
        filled.schema.get_field_index(END), END, pa.nulls(2, filled.schema.field(END).type)
    )
    assert within(unknown, by_label).num_rows == 2
    ticks = pa.RecordBatch.from_pydict({"Price": [1.0]})
    assert within(ticks, by_label) is ticks


def fill(rule: TimeRule, rows: pa.RecordBatch) -> dict[str, list[object]]:
    filled = rule.fill(rows, received=7)
    assert filled.schema == rows.schema
    return {
        DATE: filled.column(DATE).to_pylist(),
        START: iso(filled.column(START)),
        END: iso(filled.column(END)),
    }


# --- Names and rules ------------------------------------------------------------------------


def test_the_library_owns_its_time_columns() -> None:
    assert {DATE, TIMESTAMP, START, END, INSERT} == LIBRARY_COLUMNS
    assert time_column(RowTimes.BAR) == time_column(RowTimes.DAY) == START
    assert time_column(RowTimes.TICK) == time_column(RowTimes.NONE) == TIMESTAMP


def test_a_rule_follows_the_dataset_and_the_request() -> None:
    log = session_warnings()
    request = Request(timespan=T.HOUR, multiplier=4)
    assert time_rule(stocks.BARS, request, asset_class=A.STOCKS, warnings=log) == TimeRule(
        "stocks.bars", RowTimes.BAR, A.STOCKS, T.HOUR, 4
    )
    # Only a daily summary's rule keeps the warnings, for a day not cut at UTC midnight.
    daily = time_rule(stocks.DAILY_OPEN_CLOSE, Request(), asset_class=A.STOCKS, warnings=log)
    assert daily == TimeRule("stocks.daily_open_close", RowTimes.DAY, A.STOCKS, T.DAY, warnings=log)
    assert time_rule(stocks.TRADES, Request(), asset_class=A.STOCKS, warnings=log) == TimeRule(
        "stocks.trades", RowTimes.TICK, A.STOCKS
    )
    # An indicator follows its symbol's class.
    sma = time_rule(indicators.SMA, Request(timespan=T.DAY), asset_class=A.CRYPTO, warnings=log)
    assert (sma.row_times, sma.asset_class, sma.timespan) == (RowTimes.BAR, A.CRYPTO, T.DAY)
    snapshots = time_rule(stocks.SNAPSHOTS, Request(), asset_class=A.STOCKS, warnings=log)
    assert snapshots.row_times is RowTimes.NONE


# --- Arrival time ---------------------------------------------------------------------------


def test_every_row_gets_its_arrival_time() -> None:
    rows = pa.table({"Symbol": ["A", "B"], INSERT: pa.nulls(2, INSTANT)})
    rule = TimeRule("stocks.snapshots", RowTimes.NONE, A.STOCKS)
    filled = rule.fill(rows, received=5)
    assert filled.column(INSERT).cast(pa.int64()).to_pylist() == [5, 5]
    # A batch gathered from several WebSocket frames carries one arrival time per row.
    per_row = rule.fill(rows, received=pa.array([1, 2], pa.int64()))
    assert per_row.column(INSERT).cast(pa.int64()).to_pylist() == [1, 2]


# --- Ticks ----------------------------------------------------------------------------------


def test_a_tick_s_date_is_its_zone_s_date() -> None:
    stock = TimeRule("stocks.trades", RowTimes.TICK, A.STOCKS)
    # 2025-03-09 07:00 UTC is 03:00 EDT, just after the clocks went forward; 2025-11-02 05:30
    # UTC is 01:30 EDT, before they went back; 04:59 UTC on 03-03 is still 03-02 in New York.
    times = [
        "2025-03-09T06:59:59",
        "2025-03-09T07:00:00",
        "2025-11-02T05:30:00",
        "2025-03-03T04:59:00",
        None,
    ]
    assert stock.fill(ticks(times), received=1).column(DATE).to_pylist() == [
        "2025-03-09",
        "2025-03-09",
        "2025-11-02",
        "2025-03-02",
        None,
    ]
    crypto = TimeRule("crypto.trades", RowTimes.TICK, A.CRYPTO)
    assert crypto.fill(ticks(["2025-03-03T04:59:00"]), received=1).column(DATE).to_pylist() == [
        "2025-03-03"
    ]


def test_local_dates_follow_the_clock_change_inside_a_day() -> None:
    # 2025-11-02 06:00 UTC is when New York goes back to EST: 05:59 is 01:59 EDT, 06:00 01:00 EST.
    values = pa.array([ns("2025-11-02T05:59:59"), ns("2025-11-02T06:00:00")], pa.int64())
    assert local_dates(values, NEW_YORK).to_pylist() == ["2025-11-02", "2025-11-02"]
    late = pa.array([ns("2025-11-03T04:59:59"), ns("2025-11-03T05:00:00")], pa.int64())
    assert local_dates(late, NEW_YORK).to_pylist() == ["2025-11-02", "2025-11-03"]
    assert local_dates(late, UTC).to_pylist() == ["2025-11-03", "2025-11-03"]


def test_the_wall_clock_takes_the_new_offset_at_the_change_itself() -> None:
    # At 06:00 UTC exactly New York reads 01:00 EST, not 02:00 EDT; a second before, 01:59:59.
    values = pa.array([ns("2025-11-02T05:59:59"), ns("2025-11-02T06:00:00")], pa.int64())
    wall = row_times._local_ns(values, NEW_YORK).to_pylist()
    assert wall == [ns("2025-11-02T01:59:59"), ns("2025-11-02T01:00:00")]


def test_a_futures_tick_keeps_massive_s_session() -> None:
    rule = TimeRule("futures.trades", RowTimes.TICK, A.FUTURES)
    sent = ticks(["2024-03-28T23:00:00"], dates=["2024-04-01"])  # Good Friday's fold
    assert rule.fill(sent, received=1).column(DATE).to_pylist() == ["2024-04-01"]


def test_a_streamed_futures_tick_is_dated_by_the_17_chicago_rule() -> None:
    rule = TimeRule("futures.trades", RowTimes.TICK, A.FUTURES)
    times = [
        "2025-03-07T22:59:00",  # Friday 16:59 Chicago: Friday's session
        "2025-03-07T23:00:00",  # Friday 17:00 Chicago: rolls past the weekend to Monday
        "2025-03-09T22:00:00",  # Sunday 17:00 Chicago (summer time): the Monday open
        "2025-03-08T12:00:00",  # Saturday: Monday too
    ]
    assert rule.fill(ticks(times), received=1).column(DATE).to_pylist() == [
        "2025-03-07",
        "2025-03-10",
        "2025-03-10",
        "2025-03-10",
    ]


def test_the_schedule_corrector_sees_the_rule_s_dates() -> None:
    seen: list[tuple[list[str], list[int], list[str]]] = []

    def corrector(symbols: pa.Array, instants: pa.Array, dates: pa.Array) -> pa.Array:
        seen.append((symbols.to_pylist(), instants.to_pylist(), dates.to_pylist()))
        return pa.array(["2024-04-01"] * len(dates), pa.string())

    rule = TimeRule("futures.trades", RowTimes.TICK, A.FUTURES)
    filled = rule.fill(ticks(["2024-03-28T23:00:00"]), received=1, session_dates=corrector)
    assert filled.column(DATE).to_pylist() == ["2024-04-01"]
    assert seen == [(["ESZ5"], [ns("2024-03-28T23:00:00")], ["2024-03-29"])]


# --- Intraday bars --------------------------------------------------------------------------


def test_an_intraday_bar_ends_its_length_after_its_start() -> None:
    minute = TimeRule("stocks.bars", RowTimes.BAR, A.STOCKS, T.MINUTE, 1)
    assert fill(minute, bars(["2025-03-03T14:30:00"])) == {
        DATE: ["2025-03-03"],
        START: ["2025-03-03T14:30:00+00:00"],
        END: ["2025-03-03T14:31:00+00:00"],
    }
    # Two hours from 23:00 New York cross midnight: no single date. From 21:00 they do not.
    hours = TimeRule("stocks.bars", RowTimes.BAR, A.STOCKS, T.HOUR, 2)
    assert fill(hours, bars(["2025-03-04T04:00:00", "2025-03-04T02:00:00"]))[DATE] == [
        None,
        "2025-03-03",
    ]
    # A bar rule cannot do without its timespan.
    with pytest.raises(ValueError, match=r"stocks\.bars: a bar rule needs its timespan"):
        TimeRule("stocks.bars", RowTimes.BAR, A.STOCKS)


def test_a_streamed_bar_s_own_end_is_recomputed() -> None:
    rule = TimeRule("stocks.bars", RowTimes.BAR, A.STOCKS, T.SECOND, 1)
    sent = bars(["2025-03-03T14:30:00"], ends=["2025-03-03T14:31:00"])
    assert fill(rule, sent)[END] == ["2025-03-03T14:30:01+00:00"]


def test_a_futures_second_bar_ends_at_the_later_of_its_length_and_massive_s_end(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_log_limits, "_logged", set())
    rule = TimeRule("futures.bars", RowTimes.BAR, A.FUTURES, T.SECOND, 1)
    sent = bars(
        ["2025-03-07T15:00:00", "2025-03-07T15:00:01", "2025-03-07T15:00:02"],
        ends=["2025-03-07T15:01:00", None, "2025-03-07T15:00:03"],
    )
    with caplog.at_level(logging.INFO, logger=row_times.__name__):
        assert fill(rule, sent)[END] == [
            "2025-03-07T15:01:00+00:00",
            "2025-03-07T15:00:02+00:00",
            "2025-03-07T15:00:03+00:00",
        ]
        fill(rule, sent)
    warned = [r for r in caplog.records if "is not its start plus one second" in r.message]
    assert [r.levelno for r in warned] == [logging.INFO]  # once per process; nothing to act on
    agreeing = bars(["2025-03-07T15:00:00"], ends=["2025-03-07T15:00:01"])
    caplog.clear()
    assert fill(rule, agreeing)[END] == ["2025-03-07T15:00:01+00:00"]


def test_a_futures_intraday_bar_is_dated_by_its_session() -> None:
    rule = TimeRule("futures.bars", RowTimes.BAR, A.FUTURES, T.HOUR, 2)
    # 20:00 to 22:00 UTC is 14:00 to 16:00 Chicago (winter time): inside Friday's session.
    # 23:00 UTC is the 17:00 roll, so that bar belongs to Monday's; 22:00 to 00:00 crosses it.
    sent = bars(["2025-03-07T20:00:00", "2025-03-07T23:00:00", "2025-03-07T22:00:00"])
    assert fill(rule, sent)[DATE] == ["2025-03-07", "2025-03-10", None]
    sent_dated = bars(["2025-03-07T20:00:00"], dates=["2025-03-06"])
    assert fill(rule, sent_dated)[DATE] == ["2025-03-06"]  # Massive's session wins


# --- Bars of a day or longer ----------------------------------------------------------------


def test_a_day_bar_ends_at_the_next_midnight_of_the_calendar() -> None:
    rule = TimeRule("stocks.bars", RowTimes.BAR, A.STOCKS, T.DAY, 1)
    # 2025-03-09 lasts 23 hours in New York and 2025-11-02 25.
    sent = bars(["2025-03-03T05:00:00", "2025-03-09T05:00:00", "2025-11-02T04:00:00", None])
    assert fill(rule, sent) == {
        DATE: ["2025-03-03", "2025-03-09", "2025-11-02", None],
        START: [
            "2025-03-03T05:00:00+00:00",
            "2025-03-09T05:00:00+00:00",
            "2025-11-02T04:00:00+00:00",
            None,
        ],
        END: [
            "2025-03-04T05:00:00+00:00",
            "2025-03-10T04:00:00+00:00",
            "2025-11-03T05:00:00+00:00",
            None,
        ],
    }


def test_an_index_day_ends_an_hour_late() -> None:
    rule = TimeRule("indices.bars", RowTimes.BAR, A.INDICES, T.DAY, 1)
    assert fill(rule, bars(["2025-03-03T05:00:00"])) == {
        DATE: ["2025-03-03"],
        START: ["2025-03-03T05:00:00+00:00"],
        END: ["2025-03-04T06:00:00+00:00"],
    }


def test_longer_bars_cover_their_whole_window_and_have_no_date() -> None:
    week = TimeRule("stocks.bars", RowTimes.BAR, A.STOCKS, T.WEEK, 1)
    # A stock week stamped on its Sunday runs Monday to Friday.
    assert fill(week, bars(["2025-03-02T05:00:00"])) == {
        DATE: [None],
        START: ["2025-03-03T05:00:00+00:00"],
        END: ["2025-03-08T05:00:00+00:00"],
    }
    # June 2024 starts on a Saturday: its stock month starts on Monday the 3rd.
    month = TimeRule("stocks.bars", RowTimes.BAR, A.STOCKS, T.MONTH, 1)
    index_month = TimeRule("indices.bars", RowTimes.BAR, A.INDICES, T.MONTH, 1)
    # An index month covers the whole calendar month, weekends included: June 2024 from
    # Saturday the 1st, to an hour after the month ends.
    assert fill(index_month, bars(["2024-06-01T04:00:00"])) == {
        DATE: [None],
        START: ["2024-06-01T04:00:00+00:00"],
        END: ["2024-07-01T05:00:00+00:00"],
    }
    assert fill(month, bars(["2024-06-01T04:00:00"]))[START] == ["2024-06-03T04:00:00+00:00"]
    assert fill(month, bars(["2024-06-01T04:00:00"]))[END] == ["2024-06-29T04:00:00+00:00"]
    # Crypto weeks are calendar weeks from Sunday 00:00 UTC, where Massive stamps them.
    crypto = TimeRule("crypto.bars", RowTimes.BAR, A.CRYPTO, T.WEEK, 1)
    assert fill(crypto, bars(["2025-03-02T00:00:00"])) == {
        DATE: [None],
        START: ["2025-03-02T00:00:00+00:00"],
        END: ["2025-03-09T00:00:00+00:00"],
    }
    # Several days.
    days = TimeRule("stocks.bars", RowTimes.BAR, A.STOCKS, T.DAY, 3)
    assert fill(days, bars(["2025-03-03T05:00:00"])) == {
        DATE: [None],
        START: ["2025-03-03T05:00:00+00:00"],
        END: ["2025-03-07T05:00:00+00:00"],
    }
    # An index can update around the clock, so its week holds its weekend: it runs from its
    # Sunday to an hour after the next Sunday begins.
    indices = TimeRule("indices.bars", RowTimes.BAR, A.INDICES, T.WEEK, 1)
    assert fill(indices, bars(["2025-03-02T05:00:00"])) == {
        DATE: [None],
        START: ["2025-03-02T05:00:00+00:00"],
        END: ["2025-03-09T06:00:00+00:00"],  # 01:00 New York time, still standard time
    }

    index_days = TimeRule("indices.bars", RowTimes.BAR, A.INDICES, T.DAY, 2)
    assert fill(index_days, bars(["2025-03-03T05:00:00"]))[END] == ["2025-03-06T06:00:00+00:00"]


def test_stock_bars_of_several_days_count_trading_days_with_a_holiday_allowance() -> None:
    # Whether Massive counts calendar or trading days is not documented, so a bar of n days
    # ends after the latest day n trading days can reach: a 2-day stock bar from a Friday may
    # hold the Monday, or the Tuesday when Monday is a holiday. Crypto trades every day.
    days = TimeRule("stocks.bars", RowTimes.BAR, A.STOCKS, T.DAY, 2)
    assert fill(days, bars(["2025-03-07T05:00:00"]))[END] == ["2025-03-12T04:00:00+00:00"]
    one = TimeRule("stocks.bars", RowTimes.BAR, A.STOCKS, T.DAY, 1)
    assert fill(one, bars(["2025-03-07T05:00:00"]))[END] == ["2025-03-08T05:00:00+00:00"]
    crypto = TimeRule("crypto.bars", RowTimes.BAR, A.CRYPTO, T.DAY, 2)
    assert fill(crypto, bars(["2025-03-07T00:00:00"]))[END] == ["2025-03-09T00:00:00+00:00"]


def test_an_indicator_value_ends_where_a_bar_of_its_period_ends() -> None:
    # Indicator stamps are checked like the bars', so a value covers its bar's window.
    week = TimeRule("indicators.sma", RowTimes.BAR, A.CRYPTO, T.WEEK, 1)
    assert fill(week, bars(["2025-03-02T00:00:00"]))[END] == ["2025-03-09T00:00:00+00:00"]
    day = TimeRule("indicators.sma", RowTimes.BAR, A.STOCKS, T.DAY, 1)
    assert fill(day, bars(["2025-03-03T05:00:00"]))[END] == ["2025-03-04T05:00:00+00:00"]


# --- Futures bars of a session or longer ----------------------------------------------------


def test_a_futures_session_bar_ends_at_17_chicago_on_its_date() -> None:
    rule = TimeRule("futures.bars", RowTimes.BAR, A.FUTURES, T.SESSION, 1)
    sent = bars(["2024-03-27T22:00:00", "2024-03-27T22:00:00"], dates=["2024-03-28", None])
    assert fill(rule, sent) == {
        DATE: ["2024-03-28", None],
        START: ["2024-03-27T22:00:00+00:00", "2024-03-27T22:00:00+00:00"],
        END: ["2024-03-28T22:00:00+00:00", None],
    }


def test_a_futures_bar_of_several_sessions_ends_at_its_latest_possible_close() -> None:
    rule = TimeRule("futures.bars", RowTimes.BAR, A.FUTURES, T.SESSION, 2)
    # Labelled Thursday 2024-03-28 before Good Friday: counted as weekdays it ends with
    # Friday's session, but the holiday moved that trading to Monday 04-01. The count allows
    # one session more for a fold and one weekday for a holiday, so it ends on Tuesday.
    sent = bars(["2024-03-27T22:00:00"], dates=["2024-03-28"])
    end = datetime(2024, 4, 2, 17, tzinfo=CHICAGO).astimezone(UTC)
    assert fill(rule, sent) == {
        DATE: [None],
        START: ["2024-03-27T22:00:00+00:00"],
        END: [end.isoformat()],
    }
    # A label later than the counted sessions and their allowance is the end.
    late = bars(["2024-03-27T22:00:00"], dates=["2024-04-08"])
    assert fill(rule, late)[END] == [
        datetime(2024, 4, 8, 17, tzinfo=CHICAGO).astimezone(UTC).isoformat()
    ]
    # Without a start, the sessions cannot be counted, so there is no end.
    no_start = bars([None], dates=["2024-03-28"])
    assert fill(rule, no_start)[END] == [None]


# Weekdays on which the CME equity and interest-rate futures hold no session of their own,
# 2024 to 2026: their trading folds into the next session, which opens the evening before.
CME_FOLDED = frozenset(
    {
        date(2024, 1, 1), date(2024, 1, 15), date(2024, 2, 19), date(2024, 3, 29),
        date(2024, 5, 27), date(2024, 6, 19), date(2024, 7, 4), date(2024, 9, 2),
        date(2024, 11, 28), date(2024, 12, 25),
        date(2025, 1, 1), date(2025, 1, 20), date(2025, 2, 17), date(2025, 4, 18),
        date(2025, 5, 26), date(2025, 6, 19), date(2025, 7, 4), date(2025, 9, 1),
        date(2025, 11, 27), date(2025, 12, 25),
        date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16), date(2026, 4, 3),
        date(2026, 5, 25), date(2026, 6, 19), date(2026, 7, 3), date(2026, 9, 7),
        date(2026, 11, 26), date(2026, 12, 25),
    }
)  # fmt: skip


def _cme_sessions() -> list[date]:
    day, sessions = date(2023, 12, 27), []
    while day.year < 2027:
        if day.weekday() < 5 and day not in CME_FOLDED:
            sessions.append(day)
        day += timedelta(days=1)
    return sessions


def _opens(sessions: list[date], index: int) -> datetime:
    # A session opens at 17:00 Chicago on the day the one before it closed (Sunday after a
    # Friday), so a session after a folded holiday opens on the evening before the holiday.
    before = sessions[index - 1]
    opens = datetime.combine(before, time(17), CHICAGO)
    return opens + timedelta(days=2) if before.weekday() == 4 else opens


def test_a_futures_bar_of_several_sessions_never_ends_before_its_last_session() -> None:
    # Every 2- to 40-session bar of the CME calendar of 2024 to 2026, counted from its real
    # open, Sunday and folded opens included, under both readings of its label.
    sessions = _cme_sessions()
    for count in range(2, 41):
        rule = TimeRule("futures.bars", RowTimes.BAR, A.FUTURES, T.SESSION, count)
        firsts = range(1, len(sessions) - count)
        starts = [
            _opens(sessions, i).astimezone(UTC).replace(tzinfo=None).isoformat() for i in firsts
        ]
        lasts = [sessions[i + count - 1] for i in firsts]
        for labels in ([sessions[i] for i in firsts], lasts):
            ends = fill(rule, bars(starts, dates=[d.isoformat() for d in labels]))[END]
            for end, last in zip(ends, lasts, strict=True):
                closes = datetime.combine(last, time(17), CHICAGO).astimezone(UTC)
                assert datetime.fromisoformat(cast("str", end)) >= closes, (count, last, end)


def test_a_five_session_bar_from_after_christmas_ends_with_its_fifth_session() -> None:
    # Its first session, 12-26, opened on Christmas Eve, whose session date is the holiday.
    rule = TimeRule("futures.bars", RowTimes.BAR, A.FUTURES, T.SESSION, 5)
    for opens, fifth in (
        (date(2024, 12, 24), date(2025, 1, 2)),
        (date(2025, 12, 24), date(2026, 1, 2)),
    ):
        start = datetime.combine(opens, time(17), CHICAGO).astimezone(UTC).replace(tzinfo=None)
        (end,) = fill(
            rule, bars([start.isoformat()], dates=[(opens + timedelta(days=2)).isoformat()])
        )[END]
        closes = datetime.combine(fifth, time(17), CHICAGO).astimezone(UTC)
        assert datetime.fromisoformat(cast("str", end)) >= closes


def test_a_futures_week_or_month_ends_with_the_latest_period_its_label_allows() -> None:
    week = TimeRule("futures.bars", RowTimes.BAR, A.FUTURES, T.WEEK, 1)
    sent = bars(["2024-03-24T22:00:00", "2024-03-24T22:00:00"], dates=["2024-03-25", "2024-03-25"])
    assert fill(week, sent) == {
        DATE: [None, None],
        START: ["2024-03-24T22:00:00+00:00", "2024-03-24T22:00:00+00:00"],
        END: [datetime(2024, 3, 29, 17, tzinfo=CHICAGO).astimezone(UTC).isoformat()] * 2,
    }
    months = TimeRule("futures.bars", RowTimes.BAR, A.FUTURES, T.MONTH, 2)
    end = fill(months, bars(["2024-02-29T23:00:00"], dates=["2024-03-01"]))[END]
    assert end == [datetime(2024, 4, 30, 17, tzinfo=CHICAGO).astimezone(UTC).isoformat()]


# --- Daily summaries ------------------------------------------------------------------------


def test_a_daily_summary_covers_its_date() -> None:
    log = session_warnings()
    stock = TimeRule("stocks.daily_open_close", RowTimes.DAY, A.STOCKS, T.DAY, warnings=log)
    sent = bars([None, None, None], dates=["2025-03-09", "2025-03-09", None])
    assert fill(stock, sent) == {
        DATE: ["2025-03-09", "2025-03-09", None],
        START: ["2025-03-09T05:00:00+00:00"] * 2 + [None],
        END: ["2025-03-10T04:00:00+00:00"] * 2 + [None],
    }
    index = TimeRule("indices.daily_open_close", RowTimes.DAY, A.INDICES, T.DAY, warnings=log)
    assert fill(index, bars([None], dates=["2025-03-03"]))[END] == ["2025-03-04T06:00:00+00:00"]
    crypto = TimeRule("crypto.daily_open_close", RowTimes.DAY, A.CRYPTO, T.DAY, warnings=log)
    assert fill(crypto, bars([None], dates=["2025-03-03"]))[START] == ["2025-03-03T00:00:00+00:00"]


def test_the_real_futures_datasets_get_their_session_rule() -> None:
    trades = time_rule(
        futures.TRADES, Request(), asset_class=A.FUTURES, warnings=session_warnings()
    )
    assert trades.asset_class is A.FUTURES
    assert (
        date(2025, 3, 3).isoformat()
        in fill(
            time_rule(
                futures.BARS,
                Request(timespan=T.SESSION, multiplier=1),
                asset_class=A.FUTURES,
                warnings=session_warnings(),
            ),
            bars(["2025-03-02T23:00:00"], dates=["2025-03-03"]),
        )[DATE]
    )


def test_a_daily_rule_needs_the_session_s_warnings() -> None:
    with pytest.raises(ValueError, match="a daily rule needs the session's warnings"):
        TimeRule("crypto.daily_open_close", RowTimes.DAY, A.CRYPTO, T.DAY)


def test_a_day_summary_not_in_utc_is_served_with_one_warning() -> None:
    # Massive cut the day elsewhere than UTC midnight: the row keeps the UTC day's window, and
    # the session warns once per dataset that its dates may not line up.
    log = Mock(spec=BoundedLog)
    rule = TimeRule("crypto.daily_open_close", RowTimes.DAY, A.CRYPTO, T.DAY, warnings=log)
    rows = bars([None, None], dates=["2025-03-03", "2025-03-04"]).append_column(
        "IsUtc", pa.array([True, False])
    )
    filled = rule.fill(rows, received=7)
    assert iso(filled.column(START)) == ["2025-03-03T00:00:00+00:00", "2025-03-04T00:00:00+00:00"]
    (call,) = log.log.call_args_list
    assert call.args[0] == "day summary not in UTC crypto.daily_open_close"
    assert "isUTC false" in call.args[1]
    assert "may not line up" in call.args[1]
    # A batch whose rows are all in UTC, or whose flags are unknown, warns of nothing.
    log.reset_mock()
    utc = bars([None], dates=["2025-03-03"]).append_column("IsUtc", pa.array([None], pa.bool_()))
    rule.fill(utc, received=7)
    log.log.assert_not_called()


# --- The bar-stamp rule ---------------------------------------------------------------------


def session_warnings() -> BoundedLog:
    """Return a session's once-per-session warnings, as `SessionCore.warnings` makes them."""
    return BoundedLog(logging.getLogger("tests.stamps"), limit=10, level=logging.WARNING)


def test_stamp_rule() -> None:
    # 1-day bars expect each class's known closes; longer bars only their period's start.
    log = session_warnings()
    day = Request(timespan=Timespan.DAY)
    close, early, chicago = time(16), time(13), time(1)
    last_ms = time(23, 59, 59, 999_000)
    stock_closes = frozenset({early, close})
    option_closes = frozenset({early, close, time(13, 15), time(16, 15)})

    def rule(spec: DatasetSpec, request: Request) -> StampRule | None:
        return stamp_rule(spec, request, warnings=log)

    def one_day(zone: tzinfo, known: frozenset[time]) -> StampRule:
        return StampRule(zone, known, "1-day bar", T.DAY, log)

    assert rule(stocks.BARS, day) == one_day(NEW_YORK, stock_closes)
    assert rule(options.BARS, day) == one_day(NEW_YORK, option_closes)
    assert rule(indices.BARS, day) == one_day(NEW_YORK, frozenset({chicago, early, close}))
    assert rule(forex.BARS, day) == one_day(UTC, frozenset({last_ms}))
    assert rule(crypto.BARS, day) == one_day(UTC, frozenset({last_ms}))
    # Several days: only 00:00 (01:00 for indices), on any day.
    two_days = Request(timespan=Timespan.DAY, multiplier=2)
    assert rule(stocks.BARS, two_days) == StampRule(NEW_YORK, frozenset(), "bar", T.DAY, log)
    assert rule(options.BARS, two_days) == StampRule(NEW_YORK, frozenset(), "bar", T.DAY, log)
    assert rule(indices.BARS, two_days) == StampRule(
        NEW_YORK, frozenset({chicago}), "bar", T.DAY, log
    )
    assert rule(forex.BARS, two_days) == StampRule(UTC, frozenset(), "bar", T.DAY, log)
    # A week on its Sunday, in every class, where Massive starts its weeks.
    sunday = frozenset({6})
    for multiplier in (1, 2):
        week = Request(timespan=Timespan.WEEK, multiplier=multiplier)
        assert rule(stocks.BARS, week) == StampRule(
            NEW_YORK, frozenset(), "bar", T.WEEK, log, weekdays=sunday
        )
        assert rule(indices.BARS, week) == StampRule(
            NEW_YORK, frozenset({chicago}), "bar", T.WEEK, log, weekdays=sunday
        )
        assert rule(forex.BARS, week) == StampRule(
            UTC, frozenset(), "bar", T.WEEK, log, weekdays=sunday
        )
        assert rule(crypto.BARS, week) == StampRule(
            UTC, frozenset(), "bar", T.WEEK, log, weekdays=sunday
        )
    # A month, quarter, or year on its 1st.
    for timespan, months in (
        (Timespan.MONTH, frozenset(range(1, 13))),
        (Timespan.QUARTER, frozenset({1, 4, 7, 10})),
        (Timespan.YEAR, frozenset({1})),
    ):
        longer = Request(timespan=timespan, multiplier=3)
        assert rule(options.BARS, longer) == StampRule(
            NEW_YORK, frozenset(), "bar", timespan, log, months=months
        )
        assert rule(crypto.BARS, longer) == StampRule(
            UTC, frozenset(), "bar", timespan, log, months=months
        )
    # Intraday bars, futures bars, and datasets that are not bars keep their stamps.
    assert rule(crypto.BARS, Request(timespan=Timespan.HOUR)) is None
    assert rule(stocks.BARS, Request(timespan=Timespan.MINUTE, multiplier=5)) is None
    assert rule(futures.BARS, Request(timespan=Timespan.SESSION)) is None
    assert rule(futures.BARS, Request(timespan=Timespan.WEEK)) is None
    # A dataset of every class has no day zone of its own: the indicators pass their symbols'.
    assert rule(indicators.SMA, day) is None
    assert stamp_rule(indicators.SMA, day, warnings=log, asset_class=ClassFromPrefix.ANY) is None
    assert rule(crypto.TRADES, day) is None


def test_every_class_with_a_day_zone_has_its_stamps() -> None:
    # The column descriptions call a class stamped when it has a day zone; so does the rule.
    tables = (row_times._ONE_DAY_STAMPS, row_times._LONGER_STAMPS)
    assert all(set(table) == set(DAY_ZONES) for table in tables)


def stamp_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


def test_known_stamps_move_to_the_days_start_and_log_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rule = stamp_rule(stocks.BARS, Request(timespan=Timespan.DAY), warnings=session_warnings())
    assert rule is not None
    # The 16:00 close, and the 13:00 close of a short day (July 3), move to the day's start.
    close = datetime(2025, 7, 1, 16, tzinfo=NEW_YORK)
    early = datetime(2025, 7, 3, 13, tzinfo=NEW_YORK)
    values = pa.array([close, early], INSTANT)
    with caplog.at_level(logging.DEBUG):
        moved = rule.stamp("stocks.bars", values, source="s").to_pylist()
    assert moved == [datetime(2025, 7, 1, tzinfo=NEW_YORK), datetime(2025, 7, 3, tzinfo=NEW_YORK)]
    assert not caplog.records


def test_an_unexpected_stamp_is_served_with_its_days_label_and_one_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    log = session_warnings()
    rule = stamp_rule(stocks.BARS, Request(timespan=Timespan.DAY), warnings=log)
    assert rule is not None
    # 14:30 on a full day is no stamp Massive is known to use; the bar is still served, at the
    # start of the New York day it falls on, beside a bar at a known stamp.
    odd = datetime(2024, 11, 29, 14, 30, tzinfo=NEW_YORK)
    close = datetime(2024, 11, 27, 16, tzinfo=NEW_YORK)
    values = pa.array([close, odd, None], INSTANT)
    with caplog.at_level(logging.WARNING):
        moved = rule.stamp("stocks.bars", values, source="the flat files x").to_pylist()
        # The same dataset and timespan warn once per session, whatever the stamp or source.
        rule.stamp("stocks.bars", pa.array([odd + timedelta(days=1)], INSTANT), source="y")
        week = stamp_rule(stocks.BARS, Request(timespan=Timespan.WEEK), warnings=log)
        assert week is not None
        # Another timespan warns again: a Saturday is not the Sunday a week starts on.
        saturday = datetime(2025, 3, 1, tzinfo=NEW_YORK)
        assert week.stamp("stocks.bars", pa.array([saturday], INSTANT), source="z").to_pylist() == [
            saturday
        ]
    assert moved == [
        datetime(2024, 11, 27, tzinfo=NEW_YORK),
        datetime(2024, 11, 29, tzinfo=NEW_YORK),
        None,
    ]
    assert stamp_warnings(caplog) == [
        (
            "stocks.bars: a 1-day bar from the flat files x is stamped 2024-11-29T19:30:00+00:00 "
            "(2024-11-29T14:30:00-05:00 in America/New_York), which is not a stamp Massive is "
            "known to use for such a bar (00:00:00.000, 13:00:00.000, 16:00:00.000 in "
            "America/New_York). The bar is served, labelled by the day its stamp falls on in "
            "America/New_York, so its Date, TimestampStart, and TimestampEnd may cover other "
            f"days than the bar does; please report the stamp at {ISSUES_URL} so the day cut "
            "can be settled (logged once per dataset and timespan per session)"
        ),
        (
            "stocks.bars: a bar from z is stamped 2025-03-01T05:00:00+00:00 "
            "(2025-03-01T00:00:00-05:00 in America/New_York), which is not a stamp Massive is "
            "known to use for such a bar (00:00:00.000 in America/New_York, on a Sunday). The "
            "bar is served, labelled by the day its stamp falls on in America/New_York, so its "
            "Date, TimestampStart, and TimestampEnd may cover other days than the bar does; "
            f"please report the stamp at {ISSUES_URL} so the day cut can be settled (logged "
            "once per dataset and timespan per session)"
        ),
    ]


def test_a_utc_warning_names_the_stamp_once_and_an_indicator_value() -> None:
    log = Mock(spec=BoundedLog)
    rule = StampRule(UTC, frozenset({LAST_MS}), "indicator value", T.DAY, log)
    rule.stamp("indicators.sma", stamps(MARCH_3 + 5 * HOUR_NS), source="s")
    key, message, *args = log.log.call_args.args
    text = message % tuple(args)
    assert key == "unexpected bar stamp indicators.sma day"
    assert text.startswith("indicators.sma: an indicator value from s is stamped ")
    assert "is stamped 2025-03-03T05:00:00+00:00, which is not a stamp" in text
    assert "(00:00:00.000, 23:59:59.999 in UTC)" in text


DAY_NS = 86_400 * 10**9

HOUR_NS = 3600 * 10**9

MS_NS = 10**6

MARCH_3 = 1_740_960_000 * 10**9  # 2025-03-03T00:00Z

CLOSE = time(16)

LAST_MS = time(23, 59, 59, 999_000)


def stamps(*values: int | None) -> pa.Array:
    return pa.array(values, pa.timestamp("ns", "UTC"))


def instant_ns(moment: datetime) -> int:
    return int(moment.timestamp()) * 10**9 + moment.microsecond * 1000


def starts(values: pa.Array, zone: tzinfo, *known: time) -> list[Any]:
    moved, odd = day_starts(values, zone=zone, known=known)
    assert moved.type == values.type
    assert odd is None
    return cast("list[Any]", moved.cast(pa.int64()).to_pylist())


def test_utc_day_stamps_move_each_known_stamp_to_its_days_start() -> None:
    # The aggregates' 00:00 UTC stays; day_aggs_v1's 23:59:59.999 UTC moves to its day's start.
    last_ms = MARCH_3 + DAY_NS - MS_NS
    assert starts(stamps(MARCH_3, last_ms, None), UTC, LAST_MS) == [MARCH_3, MARCH_3, None]
    # A stamp anywhere in the first or last millisecond counts as that boundary.
    edge = stamps(MARCH_3 + MS_NS - 1, MARCH_3 + DAY_NS - 1)
    assert starts(edge, UTC, LAST_MS) == [MARCH_3, MARCH_3]


def test_new_york_stamps_move_by_wall_clock_across_daylight_saving_time() -> None:
    # 16:00 New York is 21:00 UTC in winter and 20:00 UTC in summer; both are the close.
    winter, summer = date(2025, 3, 7), date(2025, 3, 10)  # the clocks change on 03-09
    days = [datetime.combine(d, time(), NEW_YORK) for d in (winter, summer)]
    closes = [datetime.combine(d, CLOSE, NEW_YORK) for d in (winter, summer)]
    values = stamps(*(instant_ns(moment) for moment in [*days, *closes]))
    assert starts(values, NEW_YORK, CLOSE) == [instant_ns(day) for day in days * 2]
    # Midnight in Chicago is 01:00 New York time all year.
    chicago = datetime(2025, 7, 1, tzinfo=ZoneInfo("America/Chicago"))
    expected = instant_ns(datetime(2025, 7, 1, tzinfo=NEW_YORK))
    assert starts(stamps(instant_ns(chicago)), NEW_YORK, time(1)) == [expected]


@pytest.mark.parametrize(
    ("expected", "offset", "day"),
    [
        ((UTC, (LAST_MS,)), MS_NS, MARCH_3),
        ((UTC, (LAST_MS,)), 5 * HOUR_NS, MARCH_3),  # midnight New York time: another day cut
        ((UTC, (LAST_MS,)), DAY_NS - MS_NS - 1, MARCH_3),
        ((UTC, ()), DAY_NS - MS_NS, MARCH_3),  # a longer bar is expected at 00:00
        ((NEW_YORK, (CLOSE,)), 19 * HOUR_NS, MARCH_3 + 5 * HOUR_NS),  # 14:00, no known stamp
        ((NEW_YORK, (CLOSE,)), 21 * HOUR_NS + MS_NS, MARCH_3 + 5 * HOUR_NS),  # after 16:00
        ((NEW_YORK, (CLOSE,)), 0, MARCH_3 - 19 * HOUR_NS),  # 19:00 New York, the day before
        ((NEW_YORK, ()), 21 * HOUR_NS, MARCH_3 + 5 * HOUR_NS),  # a longer bar at the close
    ],
)
def test_an_unexpected_stamp_moves_to_its_days_start_and_is_reported(
    expected: tuple[tzinfo, tuple[time, ...]], offset: int, day: int
) -> None:
    zone, known = expected
    values = stamps(MARCH_3 + offset)
    moved, odd = day_starts(values, zone=zone, known=known)
    # The stamp is reported, and still moves to the start of the day it falls on.
    assert odd == datetime(2025, 3, 3, tzinfo=UTC) + timedelta(microseconds=offset // 1000)
    assert moved.type == values.type
    assert moved.cast(pa.int64()).to_pylist() == [day]


def test_a_week_bar_is_expected_on_a_day_its_week_starts() -> None:
    # 2025-03-01 is a Saturday, 03-02 a Sunday, 03-03 a Monday. Massive starts its weeks on
    # Sunday: 00:00 UTC for crypto and forex, 00:00 New York time for stocks.
    saturday, sunday = MARCH_3 - 2 * DAY_NS, MARCH_3 - DAY_NS
    sundays = frozenset({6})
    moved, odd = day_starts(stamps(sunday, None), zone=UTC, known=(), weekdays=sundays)
    assert (moved.cast(pa.int64()).to_pylist(), odd) == ([sunday, None], None)
    new_york = instant_ns(datetime.combine(date(2025, 3, 2), time(), NEW_YORK))
    assert day_starts(stamps(new_york), zone=NEW_YORK, known=(), weekdays=sundays)[1] is None
    # A Monday is served at its own day's start, and reported.
    moved, odd = day_starts(stamps(sunday, MARCH_3), zone=UTC, known=(), weekdays=sundays)
    assert moved.cast(pa.int64()).to_pylist() == [sunday, MARCH_3]
    assert odd == datetime(2025, 3, 3, tzinfo=UTC)
    on_saturday = stamps(saturday + 5 * HOUR_NS)
    assert day_starts(on_saturday, zone=NEW_YORK, known=(), weekdays=sundays)[1] is not None
    assert day_starts(on_saturday, zone=NEW_YORK, known=(), weekdays=(6, 0))[1] is not None


@pytest.mark.parametrize(
    ("months", "named"),
    [
        (frozenset(range(1, 13)), "a month"),
        (frozenset({1, 4, 7, 10}), "January or April or July or October"),
        (frozenset({1}), "January"),
    ],
)
def test_a_longer_bar_is_expected_on_the_1st_of_its_period(
    months: frozenset[int], named: str
) -> None:
    first = instant_ns(datetime(2025, 1, 1, tzinfo=NEW_YORK))
    moved, odd = day_starts(stamps(first), zone=NEW_YORK, known=(), months=months)
    assert (moved.cast(pa.int64()).to_pylist(), odd) == ([first], None)
    # The 2nd is never a period's start; the 1st of February starts only a month.
    wrong = [first + DAY_NS] if 2 in months else [first + DAY_NS, first + 31 * DAY_NS]
    for stamp in wrong:
        assert day_starts(stamps(stamp), zone=NEW_YORK, known=(), months=months)[1] is not None
    # The WARNING names the period's 1st.
    log = Mock(spec=BoundedLog)
    rule = StampRule(NEW_YORK, frozenset(), "bar", T.MONTH, log, months=months)
    rule.stamp("x.bars", stamps(first + DAY_NS), source="s")
    _, message, *args = log.log.call_args.args
    assert f"on the 1st of {named})" in message % tuple(args)


def test_repeated_stamps_are_converted_once_and_kept_in_order() -> None:
    # A batch of grouped-daily bars holds one stamp per day, repeated on every row.
    close = MARCH_3 + 21 * HOUR_NS  # 16:00 New York time, in winter
    start = MARCH_3 + 5 * HOUR_NS
    values = stamps(close, None, close + DAY_NS, close, None)
    assert starts(values, NEW_YORK, CLOSE) == [start, None, start + DAY_NS, start, None]
