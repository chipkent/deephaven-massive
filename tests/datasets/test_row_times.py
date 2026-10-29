"""Tests for the library's own time columns: dates, bar windows, and arrival times."""

import logging
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pytest

from deephaven_massive import _errors
from deephaven_massive._datasets import row_times
from deephaven_massive._datasets.dates import (
    CHICAGO,
    NEW_YORK,
    futures_session,
    weekday_on_or_after,
)
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.row_times import (
    DATE,
    END,
    INSERT,
    LIBRARY_COLUMNS,
    START,
    TIMESTAMP,
    RowTimes,
    TimeRule,
    ended_by,
    local_dates,
    time_column,
    time_rule,
)
from deephaven_massive._enums import AssetClass, Timespan
from deephaven_massive._namespaces import futures, indicators, stocks

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


def test_only_rows_ended_by_an_instant_are_kept() -> None:
    rule = TimeRule("stocks.bars", RowTimes.BAR, A.STOCKS, T.DAY, 1)
    filled = rule.fill(bars(["2025-03-03T05:00:00", "2025-03-04T05:00:00"]), received=7)
    latest = datetime(2025, 3, 4, 5, tzinfo=UTC)
    assert iso(ended_by(filled, latest).column(START)) == ["2025-03-03T05:00:00+00:00"]
    # A row whose end is unknown is kept; rows without an end column are all kept.
    unknown = filled.set_column(
        filled.schema.get_field_index(END), END, pa.nulls(2, filled.schema.field(END).type)
    )
    assert ended_by(unknown, latest).num_rows == 2
    ticks = pa.RecordBatch.from_pydict({"Price": [1.0]})
    assert ended_by(ticks, latest) is ticks


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
    request = Request(timespan=T.HOUR, multiplier=4)
    assert time_rule(stocks.BARS, request) == TimeRule(
        "stocks.bars", RowTimes.BAR, A.STOCKS, T.HOUR, 4
    )
    assert time_rule(stocks.DAILY_OPEN_CLOSE, Request()) == TimeRule(
        "stocks.daily_open_close", RowTimes.DAY, A.STOCKS, T.DAY
    )
    assert time_rule(stocks.TRADES, Request()) == TimeRule("stocks.trades", RowTimes.TICK, A.STOCKS)
    # An indicator follows its symbol's class.
    sma = time_rule(indicators.SMA, Request(timespan=T.DAY), asset_class=A.CRYPTO)
    assert (sma.row_times, sma.asset_class, sma.timespan) == (RowTimes.BAR, A.CRYPTO, T.DAY)
    assert time_rule(stocks.SNAPSHOTS, Request()).row_times is RowTimes.NONE


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


def test_the_column_session_rule_agrees_with_the_scalar_one() -> None:
    # Every quarter hour of 2024 to 2026, both daylight-saving changes of each year included,
    # and the instants either side of each 17:00 Chicago roll.
    start = datetime(2024, 1, 1, tzinfo=UTC)
    moments = [start + timedelta(minutes=15 * k) for k in range(3 * 366 * 96)]
    moments += [m - timedelta(microseconds=1) for m in moments]
    column = pa.array([int(m.timestamp() * 10**6) * 1000 for m in moments], pa.int64())
    days = row_times._weekday_sessions(row_times._session_days(column)).to_pylist()
    epoch = date(1970, 1, 1)
    expected = [(weekday_on_or_after(futures_session(m)) - epoch).days for m in moments]
    assert days == expected


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
    monkeypatch.setattr(_errors, "_logged", set())
    rule = TimeRule("futures.bars", RowTimes.BAR, A.FUTURES, T.SECOND, 1)
    sent = bars(
        ["2025-03-07T15:00:00", "2025-03-07T15:00:01", "2025-03-07T15:00:02"],
        ends=["2025-03-07T15:01:00", None, "2025-03-07T15:00:03"],
    )
    with caplog.at_level(logging.WARNING, logger=row_times.__name__):
        assert fill(rule, sent)[END] == [
            "2025-03-07T15:01:00+00:00",
            "2025-03-07T15:00:02+00:00",
            "2025-03-07T15:00:03+00:00",
        ]
        fill(rule, sent)
    warned = [r for r in caplog.records if "is not its start plus one second" in r.message]
    assert len(warned) == 1  # once per process
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
    # An index month ends an hour after the calendar month does, weekend or not.
    assert fill(index_month, bars(["2024-06-03T04:00:00"]))[END] == ["2024-07-01T05:00:00+00:00"]
    assert fill(month, bars(["2024-06-01T04:00:00"]))[START] == ["2024-06-03T04:00:00+00:00"]
    assert fill(month, bars(["2024-06-01T04:00:00"]))[END] == ["2024-06-29T04:00:00+00:00"]
    # Crypto weeks are calendar weeks, in UTC.
    crypto = TimeRule("crypto.bars", RowTimes.BAR, A.CRYPTO, T.WEEK, 1)
    assert fill(crypto, bars(["2025-03-03T00:00:00"]))[END] == ["2025-03-10T00:00:00+00:00"]
    # Several days.
    days = TimeRule("stocks.bars", RowTimes.BAR, A.STOCKS, T.DAY, 3)
    assert fill(days, bars(["2025-03-03T05:00:00"])) == {
        DATE: [None],
        START: ["2025-03-03T05:00:00+00:00"],
        END: ["2025-03-07T05:00:00+00:00"],
    }
    # An index can update around the clock, so its week may hold its weekend: it ends an hour
    # after the next Monday begins.
    indices = TimeRule("indices.bars", RowTimes.BAR, A.INDICES, T.WEEK, 1)
    assert fill(indices, bars(["2025-03-03T05:00:00"]))[END] == ["2025-03-10T05:00:00+00:00"]
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
    assert fill(week, bars(["2025-03-03T00:00:00"]))[END] == ["2025-03-10T00:00:00+00:00"]
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
    # Friday's session, but the holiday moved that trading to Monday 04-01, which the holiday
    # allowance of one weekday covers.
    sent = bars(["2024-03-27T22:00:00"], dates=["2024-03-28"])
    end = datetime(2024, 4, 1, 17, tzinfo=CHICAGO).astimezone(UTC)
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
    stock = TimeRule("stocks.daily_open_close", RowTimes.DAY, A.STOCKS, T.DAY)
    sent = bars([None, None, None], dates=["2025-03-09", "2025-03-09", None])
    assert fill(stock, sent) == {
        DATE: ["2025-03-09", "2025-03-09", None],
        START: ["2025-03-09T05:00:00+00:00"] * 2 + [None],
        END: ["2025-03-10T04:00:00+00:00"] * 2 + [None],
    }
    index = TimeRule("indices.daily_open_close", RowTimes.DAY, A.INDICES, T.DAY)
    assert fill(index, bars([None], dates=["2025-03-03"]))[END] == ["2025-03-04T06:00:00+00:00"]
    crypto = TimeRule("crypto.daily_open_close", RowTimes.DAY, A.CRYPTO, T.DAY)
    assert fill(crypto, bars([None], dates=["2025-03-03"]))[START] == ["2025-03-03T00:00:00+00:00"]


def test_the_real_futures_datasets_get_their_session_rule() -> None:
    assert time_rule(futures.TRADES, Request()).asset_class is A.FUTURES
    assert (
        date(2025, 3, 3).isoformat()
        in fill(
            time_rule(futures.BARS, Request(timespan=T.SESSION, multiplier=1)),
            bars(["2025-03-02T23:00:00"], dates=["2025-03-03"]),
        )[DATE]
    )
