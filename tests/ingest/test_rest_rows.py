"""Tests for turning REST page rows into batches of a dataset's schema."""

import functools
import logging
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pytest

from deephaven_massive._datasets.dates import DaySpan, TimeRange
from deephaven_massive._datasets.futures_sessions import CHICAGO
from deephaven_massive._datasets.history_source import HistoryWindow
from deephaven_massive._enums import OnError
from deephaven_massive._errors import ErrorCause, RowDataError
from deephaven_massive._ingest import rest_rows
from deephaven_massive._ingest.rest_rows import is_error_row
from deephaven_massive._massive import DriftReport, DriftSample, Transport, source
from deephaven_massive._session import CATALOG

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


# --- The bounds a fetch or poll sent --------------------------------------------------------


def converter_of(name: str, path: str) -> rest_rows.RowConverter:
    return rest_rows.RowConverter.build(
        CATALOG, CATALOG.by_name[name], source(Transport.REST, path), unreadable_symbols=OnError.LOG
    )


SMA = "/v1/indicators/sma/{stockTicker}"
DIVIDENDS = "/stocks/v1/dividends"
NEWS = "/v2/reference/news"
INDICATOR_AXIS = ("timestamp.gte", "timestamp.lt")
DIVIDEND_AXIS = ("ex_dividend_date.gte", "ex_dividend_date.lte")


def test_an_instant_axis_keeps_its_half_open_range_and_rows_without_a_time() -> None:
    first, end = datetime(2026, 10, 26, 4, tzinfo=UTC), datetime(2026, 10, 31, 4, tzinfo=UTC)
    bounds = converter_of("indicators.sma", SMA).sent_bounds(
        INDICATOR_AXIS, {"timestamp.gte": first, "timestamp.lt": end, "window": 5}
    )
    assert [(b.name, b.column) for b in bounds] == [
        ("timestamp.gte", "TimestampStart"),
        ("timestamp.lt", "TimestampStart"),
    ]
    stamps = [first - timedelta(days=1), first, end - timedelta(microseconds=1), end, None]
    batch = pa.RecordBatch.from_pydict(
        {"TimestampStart": pa.array(stamps, pa.timestamp("ns", "UTC"))}
    )
    kept = rest_rows.keep_sent_bounds(batch, bounds).column("TimestampStart").to_pylist()
    # The start is included, the end excluded, and a null is not outside the range.
    assert kept == [first, end - timedelta(microseconds=1), None]
    assert rest_rows.keep_sent_bounds(batch, ()) is batch


def test_a_calendar_date_axis_keeps_both_end_days() -> None:
    converter = converter_of("stocks.dividends", DIVIDENDS)
    days = ["2025-01-31", "2025-02-01", "2025-02-10", "2025-02-28", "2025-03-01"]
    batch = pa.RecordBatch.from_pydict({"ExDividendDate": pa.array(days, pa.string())})
    query = {"ex_dividend_date.gte": date(2025, 2, 1), "ex_dividend_date.lte": date(2025, 2, 28)}
    bounds = converter.sent_bounds(DIVIDEND_AXIS, query)
    kept = rest_rows.keep_sent_bounds(batch, bounds).column("ExDividendDate").to_pylist()
    assert kept == ["2025-02-01", "2025-02-10", "2025-02-28"]


def test_a_date_typed_parameter_over_an_instant_field_bounds_its_instants() -> None:
    # News's `published_utc.gte` is typed as a date, but its field is an instant.
    sent = datetime(2025, 2, 1, 5, tzinfo=UTC)
    bounds = converter_of("reference.news", NEWS).sent_bounds(
        ("published_utc.gte", "published_utc.lt"), {"published_utc.gte": sent}
    )
    assert [(b.name, b.column, b.value.as_py()) for b in bounds] == [
        ("published_utc.gte", "PublishedUtc", sent)
    ]


def test_a_bound_of_another_kind_than_its_column_drops_no_row(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A date never tests an instant, nor an instant a day's text, whichever way round.
    caplog.set_level(logging.DEBUG, logger="deephaven_massive._ingest.rest_rows")
    instant = datetime(2025, 2, 1, 5, tzinfo=UTC)
    dividends = converter_of("stocks.dividends", DIVIDENDS)
    sma = converter_of("indicators.sma", SMA)
    assert dividends.sent_bounds(DIVIDEND_AXIS, {"ex_dividend_date.gte": instant}) == ()
    assert dividends.sent_bounds(DIVIDEND_AXIS, {"ex_dividend_date.lte": "2025-02-01T05:00Z"}) == ()
    assert sma.sent_bounds(INDICATOR_AXIS, {"timestamp.gte": date(2025, 2, 1)}) == ()
    assert sma.sent_bounds(("timestamp.eq", "timestamp.lt"), {"timestamp.eq": instant}) == ()
    assert sma.sent_bounds(("missing.gte", "missing.lt"), {"missing.gte": instant}) == ()
    assert sma.sent_bounds(None, {"timestamp.gte": instant}) == ()
    messages = [r.getMessage() for r in caplog.records]
    assert messages[0] == (
        "stocks.dividends: ex_dividend_date.gte=datetime.datetime(2025, 2, 1, 5, 0, "
        "tzinfo=datetime.timezone.utc) sent to /stocks/v1/dividends tests none of its rows "
        "(ExDividendDate is String, another kind of value), so they are kept whatever their "
        "ex_dividend_date"
    )
    assert messages[-1] == (
        "indicators.sma: missing.gte=datetime.datetime(2025, 2, 1, 5, 0, "
        "tzinfo=datetime.timezone.utc) sent to /v1/indicators/sma/{stockTicker} tests none of "
        "its rows (its rows carry no such field), so they are kept whatever their missing"
    )
    assert "(its suffix is no comparison)" in messages[3]
    assert len(messages) == 5


def test_an_instant_bound_past_the_columns_range_holds_every_row() -> None:
    far = datetime(2999, 1, 2, tzinfo=UTC)
    bounds = converter_of("indicators.sma", SMA).sent_bounds(
        INDICATOR_AXIS, {"timestamp.gte": datetime(1, 1, 1, tzinfo=UTC), "timestamp.lt": far}
    )
    stamps = pa.array([datetime(2026, 10, 26, tzinfo=UTC)], pa.timestamp("ns", "UTC"))
    batch = pa.RecordBatch.from_pydict({"TimestampStart": stamps})
    assert rest_rows.keep_sent_bounds(batch, bounds).num_rows == 1


# --- Odd values and rows ---------------------------------------------------------------------

TRADES_PATH = "/v3/trades/{stockTicker}"
WHEN = 1741012200000000000  # 2025-03-03T14:30Z, in nanoseconds


def trades_converter() -> rest_rows.RowConverter:
    return rest_rows.RowConverter.build(
        CATALOG,
        CATALOG.by_name["stocks.trades"],
        source(Transport.REST, TRADES_PATH),
        unreadable_symbols=OnError.LOG,
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
        "which the dataset needs"
    )
    assert info.value.cause is ErrorCause.LIBRARY_DEFECT


def test_an_empty_page_is_no_error() -> None:
    batches, drift, flagged = trades_converter().convert([], "AAPL")
    assert (batches, drift, flagged) == ([], DriftReport(), [])
