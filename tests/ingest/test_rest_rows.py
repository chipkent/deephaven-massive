"""Tests for turning REST page rows into batches of a dataset's schema."""

import functools
from datetime import UTC, date, datetime

import pyarrow as pa
import pytest

from deephaven_massive._datasets.dates import DaySpan, TimeRange
from deephaven_massive._datasets.futures_sessions import CHICAGO
from deephaven_massive._datasets.history_source import HistoryWindow
from deephaven_massive._enums import OnError
from deephaven_massive._errors import LIBRARY_BUG, ErrorSource, RowDataError
from deephaven_massive._ingest import rest_rows
from deephaven_massive._ingest.rest_rows import is_error_row
from deephaven_massive._massive import DriftReport, DriftSample, Transport, source
from deephaven_massive._session import CATALOG

NS = 10**9


def epoch_ns(moment: datetime) -> int:
    return int(moment.timestamp()) * NS


def window_of(*, times: TimeRange | None = None, sessions: DaySpan | None = None) -> HistoryWindow:
    span: TimeRange | DaySpan = times or sessions or DaySpan(date(2025, 1, 1), date(2025, 1, 1))
    return HistoryWindow(span, times, sessions, times, now=datetime(2025, 1, 2, tzinfo=UTC))


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


# --- Odd values and rows ---------------------------------------------------------------------

TRADES_PATH = "/v3/trades/{stockTicker}"
WHEN = 1741012200000000000  # 2025-03-03T14:30Z, in nanoseconds


def trades_converter() -> rest_rows.RowConverter:
    return rest_rows.RowConverter.build(
        CATALOG,
        CATALOG.by_name["stocks.trades"],
        source(Transport.REST, TRADES_PATH),
        unreadable_symbols=OnError.LOG,
        raw_symbol=True,
    )


def test_a_value_that_does_not_fit_is_null_and_its_row_kept() -> None:
    rows: list[dict[str, object]] = [
        {"sip_timestamp": WHEN, "price": "1.5x", "id": "1"},
        {"sip_timestamp": WHEN + 1, "price": 2.0, "id": "2"},
    ]
    [batch], drift, _ = trades_converter().convert(rows, "AAPL")
    assert batch.column("Price").to_pylist() == [None, 2.0]
    assert batch.column("Symbol").to_pylist() == ["AAPL", "AAPL"]
    assert (drift.unconvertible, drift.dropped_rows) == (frozenset({"price"}), 0)
    assert drift.samples["price"] == DriftSample(None, '"1.5x"')


def test_a_row_whose_time_is_out_of_bounds_is_dropped_and_counted() -> None:
    rows: list[dict[str, object]] = [
        {"sip_timestamp": WHEN // 10**6, "id": "1"},  # milliseconds: January 1970
        {"sip_timestamp": WHEN, "id": "2"},
    ]
    [batch], drift, _ = trades_converter().convert(rows, "AAPL")
    assert batch.column("Id").to_pylist() == ["2"]
    assert (drift.unconvertible, drift.dropped_rows) == (frozenset(), 1)
    assert drift.dropped_reason == (
        "timestamp unit mismatch for stocks.trades.Timestamp: 1741012200000 ns is outside 1990 "
        "to 2100"
    )


def test_another_instant_out_of_bounds_is_null_and_drift() -> None:
    rows: list[dict[str, object]] = [{"sip_timestamp": WHEN, "participant_timestamp": 5}]
    [batch], drift, _ = trades_converter().convert(rows, "AAPL")
    assert batch.column("ParticipantTimestamp").to_pylist() == [None]
    assert drift.unconvertible == frozenset({"participant_timestamp"})
    assert drift.dropped_rows == 0
    assert drift.samples["participant_timestamp"] == DriftSample(None, "5")


def test_a_row_without_its_time_is_dropped_with_the_decoders_reason() -> None:
    rows: list[dict[str, object]] = [{"id": "1"}, {"sip_timestamp": WHEN, "id": "2"}]
    [batch], drift, _ = trades_converter().convert(rows, "AAPL")
    assert batch.column("Id").to_pylist() == ["2"]
    assert drift.dropped_rows == 1
    assert drift.dropped_reason == (
        f"{TRADES_PATH}: row 0: no SipTimestamp (wire key 'sip_timestamp'), which the dataset needs"
    )


def test_a_page_whose_every_row_is_dropped_raises() -> None:
    rows: list[dict[str, object]] = [{"id": "1"}, {"sip_timestamp": 5, "id": "2"}]
    with pytest.raises(RowDataError) as info:
        trades_converter().convert(rows, "AAPL")
    assert str(info.value) == (
        "stocks.trades: every row of a page of 2 was dropped, so Massive's format may have "
        f"changed; the first: {TRADES_PATH}: row 0: no SipTimestamp (wire key 'sip_timestamp'), "
        f"which the dataset needs. {LIBRARY_BUG}"
    )
    assert info.value.source is ErrorSource.LIBRARY


def test_an_empty_page_is_no_error() -> None:
    batches, drift, flagged = trades_converter().convert([], "AAPL")
    assert (batches, drift, flagged) == ([], DriftReport(), [])


def test_a_size_beyond_2_53_is_served_rounded_and_reported() -> None:
    # The quote's sizes are `long` on this path but `double` in the dataset: a size beyond
    # 2**53 cannot be held exactly, so it is served rounded and named in the drift.
    path = "/v2/last/nbbo/{stocksTicker}"
    converter = rest_rows.RowConverter.build(
        CATALOG,
        CATALOG.by_name["stocks.last_quote"],
        source(Transport.REST, path),
        unreadable_symbols=OnError.LOG,
        raw_symbol=True,
    )
    rows: list[dict[str, object]] = [
        {"T": "AAPL", "t": WHEN, "s": 2**53 + 1, "S": 100},
        {"T": "AAPL", "t": WHEN + 1, "s": 2**53 + 3, "S": 2**53},
    ]
    [batch], drift, _ = converter.convert(rows, "AAPL")
    assert batch.column("BidSize").to_pylist() == [float(2**53), float(2**53 + 4)]
    assert batch.column("AskSize").to_pylist() == [100.0, float(2**53)]
    assert drift.rounded == frozenset({"s"})
    assert drift.samples["s"] == DriftSample(None, str(2**53 + 1))
