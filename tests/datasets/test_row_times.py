"""Tests for the library's own time columns (dates, bar windows, arrival times) and bar stamps."""

import logging
from collections.abc import Sequence
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from typing import Any, cast
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

import deephaven_massive.namespaces._crypto as crypto
import deephaven_massive.namespaces._forex as forex
import deephaven_massive.namespaces._futures as futures
import deephaven_massive.namespaces._indicators as indicators
import deephaven_massive.namespaces._indices as indices
import deephaven_massive.namespaces._options as options
import deephaven_massive.namespaces._stocks as stocks
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
    FuturesBarCompletion,
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

INSTANT = pa.timestamp("ns", tz="UTC")
A, T = AssetClass, Timespan


def ns(text: str) -> int:
    """Return an ISO instant (UTC unless it says otherwise) as epoch nanoseconds."""
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return instant_ns(moment)


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


def test_a_futures_second_bar_keeps_massive_s_end_and_warns_when_it_is_not_one_second(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_log_limits, "_logged", set())
    rule = TimeRule("futures.bars", RowTimes.BAR, A.FUTURES, T.SECOND, 1)
    sent = bars(
        [
            "2025-03-07T15:00:00",
            "2025-03-07T15:00:01",
            "2025-03-07T15:00:02",
            "2025-03-07T15:00:03",
        ],
        ends=["2025-03-07T15:01:00", None, "2025-03-07T15:00:03", "2025-03-07T15:00:03.500"],
    )
    with caplog.at_level(logging.INFO, logger=row_times.__name__):
        # Massive's `e` is kept, longer or shorter than one second; no `e` ends one second on.
        assert fill(rule, sent)[END] == [
            "2025-03-07T15:01:00+00:00",
            "2025-03-07T15:00:02+00:00",
            "2025-03-07T15:00:03+00:00",
            "2025-03-07T15:00:03.500000+00:00",
        ]
        fill(rule, sent)
    warned = [r for r in caplog.records if "is not its start plus one second" in r.message]
    # Once per process, at WARNING: the bar's window is then not one second long.
    assert [r.levelno for r in warned] == [logging.WARNING]
    assert warned[0].message == (
        "futures.bars: a one-second bar's end from Massive (1741359660000000000 ns) is not its "
        "start plus one second (1741359601000000000 ns); TimestampEnd keeps Massive's e, so the "
        "bar's window is not one second long"
    )
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
        END: ["2025-03-06T05:00:00+00:00"],
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
    assert fill(index_days, bars(["2025-03-03T05:00:00"]))[END] == ["2025-03-05T06:00:00+00:00"]


def test_bars_of_several_days_count_calendar_days() -> None:
    # Massive counts calendar days: a 2-day stock bar stamped on a Friday holds Friday and
    # Saturday and ends at 00:00 on Sunday, and the next is stamped on that Sunday. Options
    # and indices read the same aggregates; crypto trades every day.
    days = TimeRule("stocks.bars", RowTimes.BAR, A.STOCKS, T.DAY, 2)
    sent = bars(["2025-03-07T05:00:00", "2025-03-09T05:00:00"])
    assert fill(days, sent)[END] == ["2025-03-09T05:00:00+00:00", "2025-03-11T04:00:00+00:00"]
    options = TimeRule("options.bars", RowTimes.BAR, A.OPTIONS, T.DAY, 3)
    assert fill(options, bars(["2025-03-07T05:00:00"]))[END] == ["2025-03-10T04:00:00+00:00"]
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


def kept_as_sent(asset_class: AssetClass, timespan: Timespan, multiplier: int = 1) -> TimeRule:
    # The rule of real bars, with the stamps their class is known to use.
    spec = {A.STOCKS: stocks.BARS, A.INDICES: indices.BARS, A.CRYPTO: crypto.BARS}[asset_class]
    request = Request(timespan=timespan, multiplier=multiplier)
    rule = time_rule(spec, request, asset_class=asset_class, warnings=session_warnings())
    assert rule.stamps is not None
    return rule


def test_a_bar_kept_at_an_unexpected_stamp_ends_a_period_later_and_has_no_date() -> None:
    # A 1-day stock bar at 14:30 New York time might cover that day or the next: it keeps its
    # stamp, ends a day later, which is no earlier than either day's end, and has no Date. A
    # bar at a known stamp beside it is filled as usual.
    rule = kept_as_sent(A.STOCKS, T.DAY)
    sent = bars(["2025-03-03T19:30:00", "2025-03-04T05:00:00", None])
    assert fill(rule, sent) == {
        DATE: [None, "2025-03-04", None],
        START: ["2025-03-03T19:30:00+00:00", "2025-03-04T05:00:00+00:00", None],
        END: ["2025-03-04T19:30:00+00:00", "2025-03-05T05:00:00+00:00", None],
    }
    # A stock week stamped on a Wednesday keeps it, and ends seven days later; two weeks end
    # fourteen days later.
    wednesday = bars(["2025-03-05T05:00:00"])
    assert fill(kept_as_sent(A.STOCKS, T.WEEK), wednesday) == {
        DATE: [None],
        START: ["2025-03-05T05:00:00+00:00"],
        END: ["2025-03-12T04:00:00+00:00"],  # 00:00 New York time, daylight saving from 03-09
    }
    assert fill(kept_as_sent(A.STOCKS, T.WEEK, 2), wednesday)[END] == ["2025-03-19T04:00:00+00:00"]
    # A crypto day at midnight New York time, as Massive's REST samples show, ends a day later.
    crypto_day = fill(kept_as_sent(A.CRYPTO, T.DAY), bars(["2025-03-03T05:00:00"]))
    assert crypto_day[END] == ["2025-03-04T05:00:00+00:00"]
    assert crypto_day[DATE] == [None]
    # An index day stamped at 02:00 New York time ends a day later, not at the index day's
    # 01:00.
    index_day = fill(kept_as_sent(A.INDICES, T.DAY), bars(["2025-03-03T07:00:00"]))
    assert index_day[END] == ["2025-03-04T07:00:00+00:00"]


@pytest.mark.parametrize(
    ("timespan", "multiplier", "stamp", "end"),
    [
        # A day across the spring clock change keeps its wall-clock time, 23 hours later.
        (T.DAY, 1, "2025-03-08T19:30:00", "2025-03-09T18:30:00"),
        (T.DAY, 3, "2025-03-03T19:30:00", "2025-03-06T19:30:00"),
        (T.MONTH, 1, "2025-03-03T19:30:00", "2025-04-03T18:30:00"),
        # January 31 has no February twin: the end moves to March 1, later.
        (T.MONTH, 1, "2025-01-31T19:30:00", "2025-03-01T19:30:00"),
        # December 31 plus two months moves to March 1 of the next year.
        (T.MONTH, 2, "2024-12-31T19:30:00", "2025-03-01T19:30:00"),
        (T.QUARTER, 1, "2025-02-02T19:30:00", "2025-05-02T18:30:00"),
        (T.QUARTER, 2, "2025-11-30T19:30:00", "2026-05-30T18:30:00"),
        (T.YEAR, 1, "2024-02-29T19:30:00", "2025-03-01T19:30:00"),
    ],
)
def test_a_kept_stamp_ends_its_period_later_on_the_wall_clock(
    *, timespan: Timespan, multiplier: int, stamp: str, end: str
) -> None:
    rule = kept_as_sent(A.STOCKS, timespan, multiplier)
    assert fill(rule, bars([stamp]))[END] == [f"{end}+00:00"]


def test_a_kept_stamp_whose_end_falls_in_a_repeated_hour_takes_the_later_reading() -> None:
    # 01:30 New York time on 2025-11-02 happens twice; the later one, 06:30 UTC, is the end.
    rule = kept_as_sent(A.STOCKS, T.DAY)
    assert fill(rule, bars(["2025-11-01T05:30:00"]))[END] == ["2025-11-02T06:30:00+00:00"]


def test_a_rule_without_stamps_takes_every_stamp_as_known() -> None:
    # A rule made without the stamp rule, such as a stream's, labels by the day as before.
    rule = TimeRule("stocks.bars", RowTimes.BAR, A.STOCKS, T.DAY, 1)
    assert fill(rule, bars(["2025-03-03T19:30:00"]))[DATE] == ["2025-03-03"]


# --- Futures bars of a session or longer ----------------------------------------------------


def test_a_futures_session_bar_ends_at_17_chicago_on_its_date() -> None:
    rule = TimeRule("futures.bars", RowTimes.BAR, A.FUTURES, T.SESSION, 1)
    sent = bars(["2024-03-27T22:00:00", "2024-03-27T22:00:00"], dates=["2024-03-28", None])
    assert fill(rule, sent) == {
        DATE: ["2024-03-28", None],
        START: ["2024-03-27T22:00:00+00:00", "2024-03-27T22:00:00+00:00"],
        END: ["2024-03-28T22:00:00+00:00", None],
    }


def test_a_longer_futures_bar_ends_at_17_chicago_on_its_label() -> None:
    # The live ESZ6 bars of 2026: Massive's window start is kept, and the bar ends at 17:00
    # Chicago (22:00 UTC in summer) on its label, its bucket's last session.
    two = TimeRule("futures.bars", RowTimes.BAR, A.FUTURES, T.SESSION, 2)
    sent = bars(
        ["2026-08-01T00:00:00", "2026-08-13T00:00:00", "2026-08-15T00:00:00"],
        dates=["2026-08-03", "2026-08-14", None],
    )
    assert fill(two, sent) == {
        DATE: [None, None, None],
        START: [
            "2026-08-01T00:00:00+00:00",
            "2026-08-13T00:00:00+00:00",
            "2026-08-15T00:00:00+00:00",
        ],
        END: ["2026-08-03T22:00:00+00:00", "2026-08-14T22:00:00+00:00", None],
    }
    month = TimeRule("futures.bars", RowTimes.BAR, A.FUTURES, T.MONTH, 1)
    september = bars(["2026-09-01T00:00:00"], dates=["2026-10-01"])
    assert fill(month, september)[END] == ["2026-10-01T22:00:00+00:00"]
    # In winter, 17:00 Chicago is 23:00 UTC.
    year = TimeRule("futures.bars", RowTimes.BAR, A.FUTURES, T.YEAR, 1)
    assert fill(year, bars(["2025-01-01T00:00:00"], dates=["2025-12-31"]))[END] == [
        "2025-12-31T23:00:00+00:00"
    ]


def futures_bars(pairs: Sequence[tuple[str, str | None]], timespan: Timespan, n: int) -> pa.Table:
    # Filled futures bars from (window start, label) pairs.
    starts = [start for start, _ in pairs]
    labels = [label for _, label in pairs]
    rule = TimeRule("futures.bars", RowTimes.BAR, A.FUTURES, timespan, n)
    return pa.Table.from_batches([rule.fill(bars(starts, dates=labels), received=0)])


def completion(timespan: Timespan, n: int, now: datetime) -> BarBounds:
    # Bounds that keep every bar but the ones that have not certainly ended by `now`.
    early, late = datetime(2000, 1, 1, tzinfo=UTC), datetime(2100, 1, 1, tzinfo=UTC)
    return BarBounds(early, late, StartRule.RANGE, FuturesBarCompletion(timespan, n, now))


def test_a_longer_futures_bar_is_kept_once_it_has_certainly_ended(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # On Monday 2026-11-02 at noon Chicago, the live 2-month bars of ESZ6: July and August
    # ended with the session of 09-01; September and October with that of 10-30; the bucket of
    # November and December is open, labelled 11-02 so far, so it is left out.
    rows = futures_bars(
        [
            ("2026-07-01T00:00:00", "2026-09-01"),
            ("2026-09-01T00:00:00", "2026-10-30"),
            ("2026-11-01T00:00:00", "2026-11-02"),
        ],
        T.MONTH,
        2,
    )
    now = datetime(2026, 11, 2, 12, tzinfo=CHICAGO)
    with caplog.at_level(logging.DEBUG, logger=row_times.__name__):
        kept = within(rows, completion(T.MONTH, 2, now))
    assert iso(kept.column(START)) == [
        "2026-07-01T00:00:00+00:00",
        "2026-09-01T00:00:00+00:00",
    ]
    # The open bar would end with the session after 2027-01-01, the latest its bucket can
    # hold: Monday 2027-01-04 at 17:00 Chicago, 23:00 UTC.
    [record] = [r for r in caplog.records if "not certainly ended" in r.getMessage()]
    assert "1 futures 2-month bars left out" in record.getMessage()
    assert "2027-01-04T23:00:00+00:00" in record.getMessage()
    later = datetime(2027, 1, 4, 17, tzinfo=CHICAGO)
    assert within(rows, completion(T.MONTH, 2, later)).num_rows == 3


def test_a_futures_bar_whose_label_may_still_move_waits_a_session_more() -> None:
    # A 2-session bucket of Monday and Tuesday 2026-08-03 that so far holds only the session
    # of 08-04 (labelled 08-04): the session of 08-05 opens on Tuesday and may still join, so
    # the bar is kept only after the session of Thursday 08-06 has closed.
    # The bar comes twice (two venues, say) and is judged once.
    rows = futures_bars([("2026-08-03T00:00:00", "2026-08-04")] * 2, T.SESSION, 2)
    assert within(rows, completion(T.SESSION, 2, datetime(2026, 8, 6, 12, tzinfo=CHICAGO))) == (
        rows.slice(0, 0)
    )
    assert within(rows, completion(T.SESSION, 2, datetime(2026, 8, 6, 17, tzinfo=CHICAGO))) == rows
    # A complete bar of the same bucket ends with its label.
    whole = futures_bars([("2026-08-03T00:00:00", "2026-08-05")], T.SESSION, 2)
    assert within(whole, completion(T.SESSION, 2, datetime(2026, 8, 5, 17, tzinfo=CHICAGO))) == (
        whole
    )
    # A bar without its label or start has no window to judge and is kept.
    unknown = futures_bars([("2026-08-03T00:00:00", None)], T.SESSION, 2)
    assert within(unknown, completion(T.SESSION, 2, datetime(2026, 8, 3, tzinfo=CHICAGO))) == (
        unknown
    )


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


def test_a_day_summary_not_in_utc_gets_its_dates_widest_window_and_one_warning() -> None:
    # Massive cut the day elsewhere than UTC midnight and does not say where: the row keeps
    # its date, and its window is that date's day in any time zone, from 00:00 at UTC+14 to
    # 24:00 at UTC-12, so a join on its end never attaches it before its day ended.
    log = Mock(spec=BoundedLog)
    rule = TimeRule("crypto.daily_open_close", RowTimes.DAY, A.CRYPTO, T.DAY, warnings=log)
    rows = bars([None, None, None], dates=["2025-03-03", "2025-03-04", "2025-03-05"])
    rows = rows.append_column("IsUtc", pa.array([True, False, False]))
    filled = rule.fill(rows, received=7)
    assert filled.column(DATE).to_pylist() == ["2025-03-03", "2025-03-04", "2025-03-05"]
    assert iso(filled.column(START)) == [
        "2025-03-03T00:00:00+00:00",
        "2025-03-03T10:00:00+00:00",
        "2025-03-04T10:00:00+00:00",
    ]
    assert iso(filled.column(END)) == [
        "2025-03-04T00:00:00+00:00",
        "2025-03-05T12:00:00+00:00",
        "2025-03-06T12:00:00+00:00",
    ]
    (call,) = log.log.call_args_list
    key, message, *args = call.args
    assert key == "day summary not in UTC crypto.daily_open_close"
    assert message % tuple(args) == (
        "crypto.daily_open_close: Massive sent the day summary of X for 2025-03-04 with isUTC "
        "false, so its day is not the UTC day the other crypto tables use, and Massive does not "
        "say which zone's day it is. The row keeps Massive's Date and IsUtc, and gets the widest "
        "window that date's day has in any time zone, TimestampStart 2025-03-03T10:00:00+00:00 "
        "to TimestampEnd 2025-03-05T12:00:00+00:00 (00:00 at UTC+14 to 24:00 at UTC-12), so a "
        "join on TimestampEnd never attaches it before its day ended; please report it at "
        f"{ISSUES_URL} (logged once per dataset per session)"
    )
    # A batch whose rows are all in UTC, or whose flags are unknown, keeps the UTC day and
    # warns of nothing.
    log.reset_mock()
    utc = bars([None], dates=["2025-03-03"]).append_column("IsUtc", pa.array([None], pa.bool_()))
    assert iso(rule.fill(utc, received=7).column(END)) == ["2025-03-04T00:00:00+00:00"]
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
        return StampRule(zone, known, "1-day bar", T.DAY, 1, log)

    assert rule(stocks.BARS, day) == one_day(NEW_YORK, stock_closes)
    assert rule(options.BARS, day) == one_day(NEW_YORK, option_closes)
    assert rule(indices.BARS, day) == one_day(NEW_YORK, frozenset({chicago, early, close}))
    assert rule(forex.BARS, day) == one_day(UTC, frozenset({last_ms}))
    assert rule(crypto.BARS, day) == one_day(UTC, frozenset({last_ms}))
    # Several days: only 00:00 (01:00 for indices), on any day.
    two_days = Request(timespan=Timespan.DAY, multiplier=2)
    assert rule(stocks.BARS, two_days) == StampRule(NEW_YORK, frozenset(), "bar", T.DAY, 2, log)
    assert rule(options.BARS, two_days) == StampRule(NEW_YORK, frozenset(), "bar", T.DAY, 2, log)
    assert rule(indices.BARS, two_days) == StampRule(
        NEW_YORK, frozenset({chicago}), "bar", T.DAY, 2, log
    )
    assert rule(forex.BARS, two_days) == StampRule(UTC, frozenset(), "bar", T.DAY, 2, log)
    # A week on its Sunday, in every class, where Massive starts its weeks.
    sunday = frozenset({6})
    for multiplier in (1, 2):
        week = Request(timespan=Timespan.WEEK, multiplier=multiplier)
        assert rule(stocks.BARS, week) == StampRule(
            NEW_YORK, frozenset(), "bar", T.WEEK, multiplier, log, weekdays=sunday
        )
        assert rule(indices.BARS, week) == StampRule(
            NEW_YORK, frozenset({chicago}), "bar", T.WEEK, multiplier, log, weekdays=sunday
        )
        assert rule(forex.BARS, week) == StampRule(
            UTC, frozenset(), "bar", T.WEEK, multiplier, log, weekdays=sunday
        )
        assert rule(crypto.BARS, week) == StampRule(
            UTC, frozenset(), "bar", T.WEEK, multiplier, log, weekdays=sunday
        )
    # A month, quarter, or year on its 1st.
    for timespan, months in (
        (Timespan.MONTH, frozenset(range(1, 13))),
        (Timespan.QUARTER, frozenset({1, 4, 7, 10})),
        (Timespan.YEAR, frozenset({1})),
    ):
        longer = Request(timespan=timespan, multiplier=3)
        assert rule(options.BARS, longer) == StampRule(
            NEW_YORK, frozenset(), "bar", timespan, 3, log, months=months
        )
        assert rule(crypto.BARS, longer) == StampRule(
            UTC, frozenset(), "bar", timespan, 3, log, months=months
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


def stamped(
    rule: StampRule, values: Sequence[datetime | int | None], *, label: str = "stocks.bars"
) -> list[Any]:
    # Stamps a batch of one symbol's bars, as the transports hand them over.
    rows = pa.RecordBatch.from_pydict(
        {"Symbol": pa.array(["AAPL"] * len(values)), START: pa.array(values, INSTANT)}
    )
    return cast("list[Any]", rule.stamp(label, rows, source="s").column(START).to_pylist())


def test_known_stamps_move_to_the_days_start_and_log_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rule = stamp_rule(stocks.BARS, Request(timespan=Timespan.DAY), warnings=session_warnings())
    assert rule is not None
    # The 16:00 close, and the 13:00 close of a short day (July 3), move to the day's start.
    close = datetime(2025, 7, 1, 16, tzinfo=NEW_YORK)
    early = datetime(2025, 7, 3, 13, tzinfo=NEW_YORK)
    with caplog.at_level(logging.DEBUG):
        moved = stamped(rule, [close, early])
    assert moved == [datetime(2025, 7, 1, tzinfo=NEW_YORK), datetime(2025, 7, 3, tzinfo=NEW_YORK)]
    assert not caplog.records


def test_an_unexpected_stamp_is_kept_as_sent_with_one_warning_per_form(
    caplog: pytest.LogCaptureFixture,
) -> None:
    log = session_warnings()
    rule = stamp_rule(stocks.BARS, Request(timespan=Timespan.DAY), warnings=log)
    assert rule is not None
    # 14:30 on a full day is no stamp Massive is known to use: which day the bar covers would
    # be a guess, so its stamp is kept, beside a bar at a known stamp that moves.
    odd = datetime(2024, 11, 29, 14, 30, tzinfo=NEW_YORK)
    close = datetime(2024, 11, 27, 16, tzinfo=NEW_YORK)
    week = stamp_rule(stocks.BARS, Request(timespan=Timespan.WEEK, multiplier=2), warnings=log)
    assert week is not None
    saturday = datetime(2025, 3, 1, tzinfo=NEW_YORK)
    with caplog.at_level(logging.WARNING):
        moved = stamped(rule, [close, odd, None])
        # The same time of day on another day, from another source, is the same form: no
        # second WARNING.
        stamped(rule, [odd + timedelta(days=1)])
        # A week on a Saturday is another form of the same dataset, and warns.
        assert stamped(week, [saturday]) == [saturday]
        # So does the same stamp of another dataset.
        assert stamped(week, [saturday], label="options.bars") == [saturday]
    assert moved == [datetime(2024, 11, 27, tzinfo=NEW_YORK), odd, None]
    served = (
        "The library cannot tell which day it covers, so the row is served as sent: "
        "TimestampStart is Massive's stamp, TimestampEnd that stamp plus {period} (no earlier "
        "than the end of any such period that holds the stamp), and Date is left empty; "
        "history() returns such a row only when its range holds that whole window. Please "
        f"report the stamp at {ISSUES_URL} so the day cut can be settled (logged once per "
        "dataset, bar length, and time of day of the stamp per session)"
    )
    week_text = (
        "a bar of AAPL from s is stamped 2025-03-01T05:00:00+00:00 (2025-03-01T00:00:00-05:00 "
        "in America/New_York), which is not a stamp Massive is known to use for such a bar "
        "(00:00:00.000 in America/New_York, on a Sunday). " + served.format(period="2 weeks")
    )
    assert stamp_warnings(caplog) == [
        "stocks.bars: a 1-day bar of AAPL from s is stamped 2024-11-29T19:30:00+00:00 "
        "(2024-11-29T14:30:00-05:00 in America/New_York), which is not a stamp Massive is "
        "known to use for such a bar (00:00:00.000, 13:00:00.000, 16:00:00.000 in "
        "America/New_York). " + served.format(period="1 day"),
        f"stocks.bars: {week_text}",
        f"options.bars: {week_text}",
    ]


def test_each_distinct_odd_stamp_of_a_batch_warns() -> None:
    # A 22:00 day bar and a 14:30 one in the same batch are two forms: both warn, each naming
    # its own symbol.
    log = Mock(spec=BoundedLog)
    rule = StampRule(NEW_YORK, frozenset({CLOSE}), "1-day bar", T.DAY, 1, log)
    late = instant_ns(datetime(2025, 3, 3, 22, tzinfo=NEW_YORK))
    afternoon = instant_ns(datetime(2025, 3, 4, 14, 30, tzinfo=NEW_YORK))
    rows = pa.RecordBatch.from_pydict(
        {"Symbol": ["A", "B", "C"], START: pa.array([late, afternoon, late], INSTANT)}
    )
    rule.stamp("stocks.bars", rows, source="s")
    keys = [call.args[0] for call in log.log.call_args_list]
    assert keys == [
        "unexpected bar stamp stocks.bars America/New_York 1 day 22:00:00.000",
        "unexpected bar stamp stocks.bars America/New_York 1 day 14:30:00.000",
    ]
    symbols = [call.args[5] for call in log.log.call_args_list]
    assert symbols == ["A", "B"]
    # A week bar's form holds its weekday, a month bar's its day of the month.
    week = StampRule(UTC, frozenset(), "bar", T.WEEK, 1, log, weekdays=frozenset({6}))
    week.stamp(
        "crypto.bars", rows.set_column(1, START, stamps(MARCH_3, MARCH_3, MARCH_3)), source="s"
    )
    assert (
        log.log.call_args.args[0]
        == "unexpected bar stamp crypto.bars UTC 1 week 00:00:00.000 Monday"
    )
    month = StampRule(UTC, frozenset(), "bar", T.MONTH, 2, log, months=frozenset({1}))
    month.stamp(
        "crypto.bars", rows.set_column(1, START, stamps(MARCH_3, MARCH_3, MARCH_3)), source="s"
    )
    assert (
        log.log.call_args.args[0]
        == "unexpected bar stamp crypto.bars UTC 2 month 00:00:00.000 day 3"
    )


def test_every_batch_with_a_kept_stamp_logs_its_count_at_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sundays = frozenset({6})
    rule = StampRule(UTC, frozenset(), "bar", T.WEEK, 2, session_warnings(), weekdays=sundays)
    with caplog.at_level(logging.DEBUG, logger=row_times.__name__):
        stamped(rule, [MARCH_3, MARCH_3 - DAY_NS, None])
        stamped(rule, [MARCH_3])
    debug = [r.getMessage() for r in caplog.records if r.levelno == logging.DEBUG]
    first = (
        "rows from s kept at Massive's stamp, which it is not known to use for a 2-week bar; the "
        "first, of AAPL, at 2025-03-03T00:00:00+00:00"
    )
    assert debug == [f"stocks.bars: 1 of 3 {first}", f"stocks.bars: 1 of 1 {first}"]


def test_a_stamp_moved_to_its_day_is_still_expected() -> None:
    # `TimeRule` asks again after the transports stamped the rows: a moved stamp keeps its
    # day, and 00:00 is always expected; a stamp kept as sent is still unexpected.
    rule = StampRule(NEW_YORK, frozenset({CLOSE}), "1-day bar", T.DAY, 1, session_warnings())
    close = instant_ns(datetime(2025, 3, 3, 16, tzinfo=NEW_YORK))
    [moved] = stamped(rule, [close])
    assert rule.expects(close)
    assert rule.expects(instant_ns(moved))
    assert not rule.expects(instant_ns(datetime(2025, 3, 3, 14, 30, tzinfo=NEW_YORK)))
    week = StampRule(UTC, frozenset(), "bar", T.WEEK, 1, Mock(), weekdays=frozenset({6}))
    assert week.expects(MARCH_3 - DAY_NS)  # a Sunday
    assert not week.expects(MARCH_3)  # a Monday


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
    assert not any(odd.to_pylist())
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
    ("expected", "offset"),
    [
        ((UTC, (LAST_MS,)), MS_NS),
        ((UTC, (LAST_MS,)), 5 * HOUR_NS),  # midnight New York time: another day cut
        ((UTC, (LAST_MS,)), DAY_NS - MS_NS - 1),
        ((UTC, ()), DAY_NS - MS_NS),  # a longer bar is expected at 00:00
        ((NEW_YORK, (CLOSE,)), 19 * HOUR_NS),  # 14:00, no known stamp
        ((NEW_YORK, (CLOSE,)), 21 * HOUR_NS + MS_NS),  # after 16:00
        ((NEW_YORK, (CLOSE,)), 0),  # 19:00 New York, the day before
        ((NEW_YORK, ()), 21 * HOUR_NS),  # a longer bar at the close
    ],
)
def test_an_unexpected_stamp_is_kept_as_sent_and_marked(
    expected: tuple[tzinfo, tuple[time, ...]], offset: int
) -> None:
    zone, known = expected
    values = stamps(MARCH_3 + offset, MARCH_3 + offset, None)
    moved, odd = day_starts(values, zone=zone, known=known)
    # Which day the bar covers is not known, so its stamp stays as Massive sent it.
    assert odd.to_pylist() == [True, True, False]
    assert moved.type == values.type
    assert moved.cast(pa.int64()).to_pylist() == [MARCH_3 + offset, MARCH_3 + offset, None]


def test_a_week_bar_is_expected_on_a_day_its_week_starts() -> None:
    # 2025-03-01 is a Saturday, 03-02 a Sunday, 03-03 a Monday. Massive starts its weeks on
    # Sunday: 00:00 UTC for crypto and forex, 00:00 New York time for stocks.
    saturday, sunday = MARCH_3 - 2 * DAY_NS, MARCH_3 - DAY_NS
    sundays = frozenset({6})
    moved, odd = day_starts(stamps(sunday, None), zone=UTC, known=(), weekdays=sundays)
    assert (moved.cast(pa.int64()).to_pylist(), odd.to_pylist()) == ([sunday, None], [False] * 2)
    new_york = instant_ns(datetime.combine(date(2025, 3, 2), time(), NEW_YORK))
    assert day_starts(stamps(new_york), zone=NEW_YORK, known=(), weekdays=sundays)[
        1
    ].to_pylist() == [False]
    # A Monday is served as sent, and marked.
    moved, odd = day_starts(stamps(sunday, MARCH_3), zone=UTC, known=(), weekdays=sundays)
    assert moved.cast(pa.int64()).to_pylist() == [sunday, MARCH_3]
    assert odd.to_pylist() == [False, True]
    on_saturday = stamps(saturday + 5 * HOUR_NS)
    assert day_starts(on_saturday, zone=NEW_YORK, known=(), weekdays=sundays)[1].to_pylist() == [
        True
    ]
    assert day_starts(on_saturday, zone=NEW_YORK, known=(), weekdays=(6, 0))[1].to_pylist() == [
        True
    ]


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
    assert (moved.cast(pa.int64()).to_pylist(), odd.to_pylist()) == ([first], [False])
    # The 2nd is never a period's start; the 1st of February starts only a month.
    wrong = [first + DAY_NS] if 2 in months else [first + DAY_NS, first + 31 * DAY_NS]
    for stamp in wrong:
        assert day_starts(stamps(stamp), zone=NEW_YORK, known=(), months=months)[1].to_pylist() == [
            True
        ]
    # The WARNING names the period's 1st.
    log = Mock(spec=BoundedLog)
    rule = StampRule(NEW_YORK, frozenset(), "bar", T.MONTH, 1, log, months=months)
    stamped(rule, [first + DAY_NS], label="x.bars")
    _, message, *args = log.log.call_args.args
    assert f"on the 1st of {named})" in message % tuple(args)


def test_repeated_stamps_are_converted_once_and_kept_in_order() -> None:
    # A batch of grouped-daily bars holds one stamp per day, repeated on every row.
    close = MARCH_3 + 21 * HOUR_NS  # 16:00 New York time, in winter
    start = MARCH_3 + 5 * HOUR_NS
    values = stamps(close, None, close + DAY_NS, close, None)
    assert starts(values, NEW_YORK, CLOSE) == [start, None, start + DAY_NS, start, None]
