"""Tests for turning REST page rows into batches of a dataset's schema."""

import functools
from datetime import UTC, date, datetime

import pyarrow as pa
import pytest

from deephaven_massive._datasets.dates import DaySpan, TimeRange
from deephaven_massive._datasets.futures_sessions import CHICAGO
from deephaven_massive._datasets.history_source import HistoryWindow
from deephaven_massive._ingest import rest_rows
from deephaven_massive._ingest.rest_rows import is_error_row

NS = 10**9


def epoch_ns(moment: datetime) -> int:
    return int(moment.timestamp()) * NS


def window_of(*, times: TimeRange | None = None, sessions: DaySpan | None = None) -> HistoryWindow:
    span: TimeRange | DaySpan = times or sessions or DaySpan(date(2025, 1, 1), date(2025, 1, 1))
    return HistoryWindow(span, times, sessions, times)


def test_keep_bounds_times_and_sessions_where_the_rows_carry_them() -> None:
    late = epoch_ns(datetime(2025, 3, 4, 20, tzinfo=CHICAGO))
    times = pa.RecordBatch.from_pydict({"Timestamp": pa.array([late], pa.timestamp("ns", "UTC"))})
    days = pa.RecordBatch.from_pydict({"Date": pa.array(["2025-03-03"], pa.string())})
    other = pa.RecordBatch.from_pydict({"Other": pa.array([1])})
    sessions = window_of(sessions=DaySpan(date(2025, 3, 4), date(2025, 3, 4)))
    before_late = window_of(
        times=TimeRange(datetime(2025, 3, 4, tzinfo=UTC), datetime(2025, 3, 5, 1, tzinfo=UTC))
    )
    keep = functools.partial(rest_rows.keep_window, time="Timestamp")
    assert keep(times, sessions).num_rows == 1  # sessions bound only Date
    assert keep(times, before_late).num_rows == 0  # 02:00 UTC on 03-05 is past the end
    assert keep(days, sessions).num_rows == 0
    assert keep(days, before_late).num_rows == 1  # no Timestamp to bound
    assert keep(other, sessions).num_rows == 1
    assert keep(times, None).num_rows == 1
    upper = window_of(sessions=DaySpan(date(2025, 3, 1), date(2025, 3, 2)))
    assert keep(days, upper).num_rows == 0  # after the last session
    lower = window_of(
        times=TimeRange(datetime(2025, 3, 5, 3, tzinfo=UTC), datetime(2025, 3, 6, tzinfo=UTC))
    )
    assert keep(times, lower).num_rows == 0
    # A bar's time is its start: the same bounds apply to `TimestampStart`.
    starts = times.rename_columns(["TimestampStart"])
    assert rest_rows.keep_window(starts, before_late, time="TimestampStart").num_rows == 0
    assert (
        rest_rows.keep_window(starts, before_late, time="Timestamp").num_rows == 1
    )  # before the first instant


@pytest.mark.parametrize(
    ("row", "flagged"),
    [
        ({"ticker": "SPY", "error": "NOT_ENTITLED", "message": "no"}, True),
        ({"ticker": "SPY", "message": "a note"}, False),
        ({"ticker": "SPY", "error": None}, False),
        ({"ticker": "SPY"}, False),
    ],
)
def test_an_error_row_is_one_whose_error_is_set(row: dict[str, object], flagged: bool) -> None:
    assert is_error_row(row) is flagged
