"""Tests for turning REST page rows into batches of a dataset's schema."""

import functools
from datetime import UTC, date, datetime

import pyarrow as pa
import pytest

from deephaven_massive._datasets.dates import DaySpan, TimeRange
from deephaven_massive._datasets.futures_sessions import CHICAGO
from deephaven_massive._datasets.history_source import HistoryWindow
from deephaven_massive._datasets.wire_symbols import unreadable_symbol_reason
from deephaven_massive._ingest import rest_rows
from deephaven_massive._ingest.rest_rows import is_error_row
from deephaven_massive._massive import (
    DriftReport,
    DriftSample,
    RowFault,
    RowMark,
    Transport,
    source,
)
from deephaven_massive._session import CATALOG
from deephaven_massive._tables.problem_events import ProblemCause
from deephaven_massive._tables.withheld import FAULT, Withheld, with_marks, withhold_rows

NS = 10**9


def epoch_ns(moment: datetime) -> int:
    return int(moment.timestamp()) * NS


def marked(columns: dict[str, pa.Array]) -> pa.RecordBatch:
    # A batch as `keep_window` gets it: with the mark columns, no row marked.
    return with_marks(pa.RecordBatch.from_pydict(columns), {}, sent=None)


def window_of(*, times: TimeRange | None = None, sessions: DaySpan | None = None) -> HistoryWindow:
    span: TimeRange | DaySpan = times or sessions or DaySpan(date(2025, 1, 1), date(2025, 1, 1))
    return HistoryWindow(span, times, sessions, times, now=datetime(2025, 1, 2, tzinfo=UTC))


def test_keep_bounds_times_and_sessions_where_the_rows_carry_them() -> None:
    late = epoch_ns(datetime(2025, 3, 4, 20, tzinfo=CHICAGO))
    times = marked({"Timestamp": pa.array([late], pa.timestamp("ns", "UTC"))})
    days = marked({"Date": pa.array(["2025-03-03"], pa.string())})
    other = marked({"Other": pa.array([1])})
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
    starts = marked({"TimestampStart": pa.array([late], pa.timestamp("ns", "UTC"))})
    assert rest_rows.keep_window(starts, before_late, time="TimestampStart").num_rows == 0
    assert (
        rest_rows.keep_window(starts, before_late, time="Timestamp").num_rows == 1
    )  # before the first instant


def test_keep_keeps_a_marked_row_it_cannot_place() -> None:
    # A row whose time could not be read is kept to be withheld and reported; an unmarked row
    # with no time, and a marked row outside the window, are not.
    stamps = pa.array([None, None, 0, WHEN], pa.timestamp("ns", "UTC"))
    rows = pa.RecordBatch.from_pydict({"Timestamp": stamps, "Id": ["a", "b", "c", "d"]})
    found = RowMark(
        rows=(0, 2), fields=("Timestamp", "Price"), reasons=("x", "y"), symbols=(None, None)
    )
    batch = with_marks(rows, {RowFault.UNREADABLE_KEYS: found}, sent=None)
    window = window_of(
        times=TimeRange(datetime(2025, 3, 3, tzinfo=UTC), datetime(2025, 3, 4, tzinfo=UTC))
    )
    kept = rest_rows.keep_window(batch, window, time="Timestamp")
    assert kept.column("Id").to_pylist() == ["a", "d"]


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


@pytest.mark.parametrize(
    ("error", "cause", "documented"),
    [
        ("NOT_FOUND", ProblemCause.UNAVAILABLE_SYMBOLS, True),
        ("NOT_ENTITLED", ProblemCause.REFUSED_SYMBOLS, True),
        ("DELISTED", ProblemCause.REFUSED_SYMBOLS, False),
    ],
)
def test_an_error_row_is_the_problem_its_error_names(
    error: str, cause: ProblemCause, documented: bool
) -> None:
    row = {"ticker": "I:VIX", "error": error, "message": "Massive's text."}
    read = rest_rows.read_error_row(row, symbol_key="ticker", symbol=None)
    assert read == rest_rows.ErrorRow(
        "I:VIX", error, "Massive's text.", cause, documented=documented
    )
    assert read.answer == f'error {error}: "Massive\'s text."'
    # A path whose rows do not carry the ticker takes the request's one symbol.
    by_request = rest_rows.read_error_row(row, symbol_key=None, symbol="X:BTCUSD")
    assert by_request.ticker == "X:BTCUSD"


# --- Odd values and rows ---------------------------------------------------------------------

TRADES_PATH = "/v3/trades/{stockTicker}"
WHEN = 1741012200000000000  # 2025-03-03T14:30Z, in nanoseconds


def trades_converter() -> rest_rows.RowConverter:
    return rest_rows.RowConverter.build(
        CATALOG, CATALOG.by_name["stocks.trades"], source(Transport.REST, TRADES_PATH)
    )


def split(batch: pa.RecordBatch) -> Withheld:
    # The rows a reader publishes, and those it withholds, as the transports split them.
    return withhold_rows(batch, symbol="Symbol")


def test_a_value_that_does_not_fit_marks_its_row_and_is_drift() -> None:
    rows: list[dict[str, object]] = [
        {"sip_timestamp": WHEN, "price": "1.5x", "id": "1"},
        {"sip_timestamp": WHEN + 1, "price": 2.0, "id": "2"},
    ]
    [batch], drift, _ = trades_converter().convert(rows, "AAPL")
    assert batch.column(FAULT).to_pylist() == [RowFault.UNCONVERTIBLE_VALUES.value, None]
    assert (drift.unconvertible, drift.dropped_rows) == (frozenset({"price"}), 0)
    assert drift.samples["price"] == DriftSample(None, '"1.5x"')
    # The row is withheld, and the good row beside it is published.
    withheld = split(batch)
    assert withheld.rows.column("Id").to_pylist() == ["2"]
    assert withheld.rows.column("Price").to_pylist() == [2.0]
    assert withheld.counts() == {RowFault.UNCONVERTIBLE_VALUES: 1}
    assert [(g.symbol, g.field) for g in withheld.groups] == [("AAPL", "Price")]


def test_a_row_whose_time_is_out_of_bounds_is_marked_unreadable_keys() -> None:
    rows: list[dict[str, object]] = [
        {"sip_timestamp": WHEN // 10**6, "id": "1"},  # milliseconds: January 1970
        {"sip_timestamp": WHEN, "id": "2"},
    ]
    [batch], drift, _ = trades_converter().convert(rows, "AAPL")
    assert (drift.unconvertible, drift.dropped_rows) == (frozenset(), 0)
    withheld = split(batch)
    assert withheld.rows.column("Id").to_pylist() == ["2"]
    assert withheld.counts() == {RowFault.UNREADABLE_KEYS: 1}
    assert withheld.first_reason == (
        "timestamp unit mismatch for stocks.trades.Timestamp: 1741012200000 ns is outside 1990 "
        "to 2100"
    )


def test_another_instant_out_of_bounds_marks_its_row_and_is_drift() -> None:
    rows: list[dict[str, object]] = [
        {"sip_timestamp": WHEN, "participant_timestamp": 5, "id": "1"},
        {"sip_timestamp": WHEN, "id": "2"},
    ]
    [batch], drift, _ = trades_converter().convert(rows, "AAPL")
    assert drift.unconvertible == frozenset({"participant_timestamp"})
    assert drift.dropped_rows == 0
    assert drift.samples["participant_timestamp"] == DriftSample(None, "5")
    withheld = split(batch)
    assert withheld.rows.column("Id").to_pylist() == ["2"]
    assert [(g.fault, g.field) for g in withheld.groups] == [
        (RowFault.UNCONVERTIBLE_VALUES, "ParticipantTimestamp")
    ]


def test_a_row_without_its_time_is_marked_with_the_decoders_reason() -> None:
    rows: list[dict[str, object]] = [{"id": "1"}, {"sip_timestamp": WHEN, "id": "2"}]
    [batch], drift, _ = trades_converter().convert(rows, "AAPL")
    assert drift.dropped_rows == 0
    withheld = split(batch)
    assert withheld.rows.column("Id").to_pylist() == ["2"]
    assert withheld.counts() == {RowFault.UNREADABLE_KEYS: 1}
    assert withheld.first_reason == (
        f"{TRADES_PATH}: row 0: no SipTimestamp (wire key 'sip_timestamp'), which the dataset needs"
    )


def test_a_page_whose_every_row_is_unreadable_is_returned_marked() -> None:
    # The converter never raises: the reader decides what a page of no readable row means.
    rows: list[dict[str, object]] = [{"id": "1"}, {"sip_timestamp": 5, "id": "2"}]
    [batch], _, _ = trades_converter().convert(rows, "AAPL")
    assert batch.column(FAULT).null_count == 0
    assert split(batch).rows.num_rows == 0


def test_an_empty_page_is_no_error() -> None:
    batches, drift, flagged = trades_converter().convert([], "AAPL")
    assert (batches, drift, flagged) == ([], DriftReport(), [])


def test_a_symbol_the_library_cannot_read_marks_its_row() -> None:
    path = "/v2/last/nbbo/{stocksTicker}"
    converter = rest_rows.RowConverter.build(
        CATALOG, CATALOG.by_name["stocks.last_quote"], source(Transport.REST, path)
    )
    rows: list[dict[str, object]] = [
        {"T": "AAPL", "t": WHEN, "s": 1, "S": 1},
        {"T": "a b", "t": WHEN, "s": 1, "S": 1},
    ]
    [batch], _, _ = converter.convert(rows, None)
    withheld = split(batch)
    assert withheld.rows.column("Symbol").to_pylist() == ["AAPL"]
    [group] = withheld.groups
    assert (group.fault, group.symbol, group.sent, group.field) == (
        RowFault.UNREADABLE_SYMBOLS,
        None,
        "a b",
        "Symbol",
    )
    assert group.reason == unreadable_symbol_reason("a b")


def test_a_size_beyond_2_53_marks_its_row_and_is_reported() -> None:
    # The quote's sizes are `long` on this path but `double` in the dataset: a size beyond
    # 2**53 cannot be held exactly, so its row is withheld and the field named in the drift.
    path = "/v2/last/nbbo/{stocksTicker}"
    converter = rest_rows.RowConverter.build(
        CATALOG, CATALOG.by_name["stocks.last_quote"], source(Transport.REST, path)
    )
    rows: list[dict[str, object]] = [
        {"T": "AAPL", "t": WHEN, "s": 2**53 + 1, "S": 100},
        {"T": "AAPL", "t": WHEN + 1, "s": 2**53, "S": 2**53},
    ]
    [batch], drift, _ = converter.convert(rows, "AAPL")
    assert drift.rounded == frozenset({"s"})
    assert drift.samples["s"] == DriftSample(None, str(2**53 + 1))
    withheld = split(batch)
    assert withheld.rows.column("BidSize").to_pylist() == [float(2**53)]
    assert withheld.rows.column("AskSize").to_pylist() == [float(2**53)]
    assert [(g.fault, g.field) for g in withheld.groups] == [
        (RowFault.UNCONVERTIBLE_VALUES, "BidSize")
    ]
    assert withheld.first_reason == (
        f"BidSize holds {2**53 + 1}, a whole number its double column would round"
    )
