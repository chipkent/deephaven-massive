"""Tests for marking the rows the library could not read whole, and withholding them."""

from dataclasses import replace

import pyarrow as pa
import pytest

import deephaven_massive as dm
from deephaven_massive._error_policy import DEFAULT_POLICY, ResolvedPolicy, resolve_policy
from deephaven_massive._massive import DriftReport, RowFault, RowMark, Transport
from deephaven_massive._tables.problem_events import (
    EVENTS_PER_PAYLOAD,
    ProblemAction,
    ProblemCause,
    ProblemEvent,
)
from deephaven_massive._tables.withheld import (
    MARK_COLUMNS,
    REASON,
    SYMBOL_SENT,
    Withheld,
    WithheldGroup,
    failure_of,
    in_window,
    logs_withheld,
    mark,
    mark_failures,
    marked_count,
    merged,
    placed,
    setting_of,
    with_marks,
    withhold_rows,
)
from tests._fakes.rest import FakeRestServer, Reply, envelope

FAULT, FIELD, _, SENT = MARK_COLUMNS
KEYS, SYMBOLS, VALUES = (
    RowFault.UNREADABLE_KEYS,
    RowFault.UNREADABLE_SYMBOLS,
    RowFault.UNCONVERTIBLE_VALUES,
)


def rows_of(symbols: list[str | None]) -> pa.RecordBatch:
    return pa.record_batch({"Symbol": symbols, "Price": [float(i) for i in range(len(symbols))]})


def one_mark(
    *rows: int, field: str = "Price", reason: str = "bad", sent: str | None = None
) -> RowMark:
    return RowMark(
        rows=rows,
        fields=(field,) * len(rows),
        reasons=(reason,) * len(rows),
        symbols=(sent,) * len(rows),
    )


def policy(**settings: dm.OnError) -> ResolvedPolicy:
    return resolve_policy(dm.ErrorPolicy(**settings), dm.ErrorPolicy())


def never(_row: int) -> str:
    raise AssertionError("no row is newly marked")


# --- with_marks --------------------------------------------------------------------------------


def test_with_marks_adds_the_decoders_marks_as_columns_after_the_rows_own() -> None:
    rows = rows_of(["A", None, "C"])
    found = {KEYS: one_mark(1, field="Symbol", reason="no sym", sent="b")}
    marked = with_marks(rows, found, sent=None)
    assert marked.schema.names == ["Symbol", "Price", *MARK_COLUMNS]
    assert marked.column(FAULT).to_pylist() == [None, "unreadable_keys", None]
    assert marked.column(FIELD).to_pylist() == [None, "Symbol", None]
    assert marked.column(REASON).to_pylist() == [None, "no sym", None]
    # With no column of symbols as sent, a marked row takes the one its decoder quoted.
    assert marked.column(SENT).to_pylist() == [None, "b", None]
    # Rows gathered from several payloads take each payload's marks at its first row.
    parts = [(0, {VALUES: one_mark(0)}), (2, {KEYS: one_mark(0)})]
    gathered = with_marks(rows, parts, sent=rows.column("Symbol"))
    assert gathered.column(FAULT).to_pylist() == ["unconvertible_values", None, "unreadable_keys"]
    assert gathered.column(SENT).to_pylist() == ["A", None, "C"]


# --- mark and mark_failures --------------------------------------------------------------------


def test_mark_keeps_a_rows_first_fault_and_reads_a_null_as_no_failure() -> None:
    rows = with_marks(rows_of(["A", "B", "C"]), {KEYS: one_mark(0)}, sent=None)
    asked: list[int] = []

    def reason(row: int) -> str:
        asked.append(row)
        return f"row {row}"

    marked = mark(rows, VALUES, pa.array([True, None, True]), field="Price", reason=reason)
    assert marked.column(FAULT).to_pylist() == ["unreadable_keys", None, "unconvertible_values"]
    assert marked.column(REASON).to_pylist() == ["bad", None, "row 2"]
    assert asked == [2]  # only for a row marked now
    # A step that marks no new row leaves the rows as they were.
    assert mark(marked, VALUES, pa.array([True, False, True]), field="X", reason=never) is marked


def test_mark_failures_marks_each_steps_rows_in_order() -> None:
    rows = with_marks(rows_of(["A", "B"]), {}, sent=None)
    marked = mark_failures(
        rows,
        [
            (SYMBOLS, pa.array([True, False]), "Symbol", lambda _row: "symbol"),
            (VALUES, pa.array([True, True]), "Price", lambda _row: "value"),
        ],
    )
    assert marked.column(FAULT).to_pylist() == ["unreadable_symbols", "unconvertible_values"]
    assert marked.column(FIELD).to_pylist() == ["Symbol", "Price"]


def test_marked_count_counts_the_marked_rows() -> None:
    assert marked_count(with_marks(rows_of(["A", "B"]), {}, sent=None)) == 0
    assert marked_count(with_marks(rows_of(["A", "B"]), {KEYS: one_mark(0, 1)}, sent=None)) == 2


# --- in_window ---------------------------------------------------------------------------------


def test_in_window_keeps_a_marked_row_it_cannot_place() -> None:
    # Rows: inside; outside; unplaceable and marked; unplaceable but read whole; outside and
    # marked.
    times = pa.record_batch({"Timestamp": pa.array([5, 50, None, None, 60], pa.int64())})
    rows = with_marks(times, {KEYS: one_mark(2, 4)}, sent=None)
    inside = pa.array([True, False, None, None, False])
    kept = in_window(rows, (("Timestamp", inside),))
    assert kept.to_pylist() == [True, False, True, False, False]


def test_in_window_judges_each_bound_on_its_own() -> None:
    # Row 0's null session keeps it through the session bound, and its time is inside; row 1's
    # null session would too, but its time lies outside the time bound, so it goes; row 2 lies
    # outside the session bound.
    keys = pa.record_batch(
        {"Timestamp": [5, 50, 5], "Date": pa.array([None, None, "2025-03-04"], pa.string())}
    )
    rows = with_marks(keys, {VALUES: one_mark(0, 1, 2)}, sent=None)
    sessions = pa.array([None, None, False])
    times = pa.array([True, False, True])
    kept = in_window(rows, (("Date", sessions), ("Timestamp", times)))
    assert kept.to_pylist() == [True, False, False]


def test_placed_judges_rows_whose_keys_read_and_keeps_those_it_cannot_place() -> None:
    # Rows: a value marked, judged out; read whole, judged in; a key marked, kept unjudged;
    # read whole, judged out. The judgment never sees the mark columns.
    times = pa.record_batch({"Timestamp": pa.array([5, 6, None, 8], pa.int64())})
    rows = with_marks(times, {VALUES: one_mark(0), KEYS: one_mark(2)}, sent=None)
    seen: list[list[str]] = []

    def keep(judged: pa.RecordBatch | pa.Table) -> pa.Array:
        seen.append(judged.schema.names)
        assert judged.column("Timestamp").to_pylist() == [5, 6, 8]
        return pa.array([False, True, False])

    kept = placed(rows, keep)
    assert kept.column("Timestamp").to_pylist() == [6, None]
    assert kept.column(FAULT).to_pylist() == [None, KEYS.value]
    assert seen == [["Timestamp"]]
    # A table's chunks are judged as one, and no rows ask for no judgment.
    table = pa.Table.from_batches([rows.slice(0, 2), rows.slice(2)])
    assert placed(table, lambda _judged: pa.chunked_array([[True, False, True]])).num_rows == 3
    assert placed(rows.slice(0, 0), keep).num_rows == 0
    assert len(seen) == 1


# --- withhold_rows and Withheld ----------------------------------------------------------------


def test_a_payload_read_whole_is_returned_without_the_mark_columns() -> None:
    rows = rows_of(["A", "B"])
    withheld = withhold_rows(with_marks(rows, {}, sent=None), symbol="Symbol")
    assert withheld.rows.equals(rows)
    assert (withheld.groups, withheld.first_reason, withheld.total) == ((), "", 0)
    assert withheld.drift == DriftReport()
    assert withheld.counts() == {}


def test_a_column_beside_the_marks_stays_on_the_published_rows() -> None:
    # A caller keeps each published row's symbol as sent by copying `SENT` under `SYMBOL_SENT`
    # before the mark columns come off.
    rows = with_marks(
        rows_of(["A", "B", "C"]), {VALUES: one_mark(1)}, sent=pa.array(["a", "b", "c"])
    )
    withheld = withhold_rows(rows.append_column(SYMBOL_SENT, rows.column(SENT)), symbol="Symbol")
    assert withheld.rows.to_pydict() == {
        "Symbol": ["A", "C"],
        "Price": [0.0, 2.0],
        SYMBOL_SENT: ["a", "c"],
    }
    assert withheld.total == 1


def test_withheld_rows_are_taken_out_and_grouped() -> None:
    rows = with_marks(
        rows_of(["A", None, "A", "B", "A"]),
        {
            VALUES: one_mark(0, 2, reason="price"),
            SYMBOLS: one_mark(1, field="Symbol", reason="spelling", sent="a b"),
        },
        sent=None,
    )
    withheld = withhold_rows(rows, symbol="Symbol")
    assert withheld.rows.to_pydict() == {"Symbol": ["B", "A"], "Price": [3.0, 4.0]}
    assert withheld.groups == (
        WithheldGroup(VALUES, "A", None, "Price", 2, "price"),
        WithheldGroup(SYMBOLS, None, "a b", "Symbol", 1, "spelling"),
    )
    assert withheld.first_reason == "price"
    assert withheld.total == 3
    assert withheld.drift == DriftReport(dropped_rows=3, dropped_reason="price")
    assert withheld.by_symbol() == {"A": 2}  # an unread symbol belongs to no symbol's run
    # A dataset whose rows name no symbol groups them by fault, symbol as sent, and field.
    nameless = withhold_rows(rows, symbol=None)
    assert {g.symbol for g in nameless.groups} == {None}
    assert nameless.total == 3


def test_counts_give_a_symbol_stream_its_own_rows_and_every_unread_symbol() -> None:
    withheld = Withheld(
        pa.table({}),
        (
            WithheldGroup(VALUES, "A", "A", "Price", 2, "price"),
            WithheldGroup(VALUES, "B", "B", "Price", 1, "price"),
            WithheldGroup(SYMBOLS, None, "a b", "Symbol", 4, "spelling"),
        ),
        "price",
    )
    assert withheld.counts() == {VALUES: 3, SYMBOLS: 4}
    # A stream of B gets its row, and the unread symbol's, which could have been any stream's.
    assert withheld.counts({"B"}.__contains__) == {VALUES: 1, SYMBOLS: 4}
    assert withheld.counts({"C"}.__contains__) == {SYMBOLS: 4}


def test_events_give_one_per_group_of_the_streams_rows() -> None:
    groups = (
        WithheldGroup(VALUES, "A", "A", "Price", 2, "price"),
        WithheldGroup(VALUES, "B", "B b", "Price", 1, "price b"),
    )
    withheld = Withheld(pa.table({}), groups, "price")
    [event] = withheld.events(
        dataset="stocks.trades",
        transport=Transport.WS,
        where="wss://h/stocks channel T",
        stream_id=7,
        policy=resolve_policy(None, dm.ErrorPolicy(unconvertible_values=dm.OnError.IGNORE)),
        holds={"B"}.__contains__,
    )
    expected = ProblemEvent(
        cause=ProblemCause.UNCONVERTIBLE_VALUES,
        action=ProblemAction.WITHHELD,
        dataset="stocks.trades",
        stream_id=7,
        where="WS wss://h/stocks channel T",
        symbol="B b",  # as Massive sent it
        field="Price",
        rows=1,
        detail="price b",
        at=event.at,
        policy=dm.OnError.IGNORE,
    )
    assert event == expected
    assert event.at > 0


def test_each_event_records_the_setting_of_its_own_cause() -> None:
    groups = (
        WithheldGroup(VALUES, "A", "A", "Price", 2, "price"),
        WithheldGroup(KEYS, "B", "B", "Timestamp", 1, "time"),
    )
    withheld = Withheld(pa.table({}), groups, "price")
    found = withheld.events(
        dataset="stocks.trades",
        transport=Transport.REST,
        where="GET /v3/trades",
        stream_id=None,
        policy=policy(unconvertible_values=dm.OnError.IGNORE, unreadable_keys=dm.OnError.RAISE),
    )
    assert [(e.cause, e.policy) for e in found] == [
        (ProblemCause.UNCONVERTIBLE_VALUES, dm.OnError.IGNORE),
        (ProblemCause.UNREADABLE_KEYS, dm.OnError.RAISE),
    ]


def test_events_past_the_cap_are_one_overflow_event_per_fault() -> None:
    shown = [
        WithheldGroup(VALUES, f"S{i}", f"S{i}", "Price", 1, f"price {i}")
        for i in range(EVENTS_PER_PAYLOAD)
    ]
    rest = [
        WithheldGroup(VALUES, "X", "X", "Price", 2, "price x"),
        WithheldGroup(KEYS, "Y", "Y", "Timestamp", 3, "time y"),
        WithheldGroup(VALUES, "Z", "Z", "Size", 4, "size z"),
    ]
    withheld = Withheld(pa.table({}), (*shown, *rest), "price 0")
    found = withheld.events(
        dataset="stocks.trades",
        transport=Transport.REST,
        where="GET /v3/trades",
        stream_id=None,
        policy=DEFAULT_POLICY,
    )
    assert len(found) == EVENTS_PER_PAYLOAD + 2
    assert {e.action for e in found[:EVENTS_PER_PAYLOAD]} == {ProblemAction.WITHHELD}
    assert found[0] == replace(found[0], symbol="S0", field="Price", rows=1, detail="price 0")
    overflow = [(e.action, e.cause, e.symbol, e.field, e.rows, e.detail) for e in found[-2:]]
    assert overflow == [
        (ProblemAction.OVERFLOW, ProblemCause.UNCONVERTIBLE_VALUES, None, None, 6, "price x"),
        (ProblemAction.OVERFLOW, ProblemCause.UNREADABLE_KEYS, None, None, 3, "time y"),
    ]


# --- setting_of and failure_of -----------------------------------------------------------------


@pytest.mark.parametrize("setting", list(dm.OnError))
@pytest.mark.parametrize("fault", list(RowFault))
def test_setting_of_reads_the_field_the_fault_is_named_for(
    fault: RowFault, setting: dm.OnError
) -> None:
    assert setting_of(policy(**{fault.value: setting}), fault) is setting


@pytest.mark.parametrize("setting", list(dm.OnError))
@pytest.mark.parametrize("fault", list(RowFault))
def test_failure_of_fails_only_a_cause_set_to_raise(fault: RowFault, setting: dm.OnError) -> None:
    counts = {fault: 2, **{other: 1 for other in RowFault if other is not fault}}
    others = {other.value: dm.OnError.LOG for other in RowFault if other is not fault}
    failure = failure_of(
        counts,
        policy(**others, **{fault.value: setting}),
        dataset="stocks.trades",
        first_reason="the first",
    )
    if setting is not dm.OnError.RAISE:
        assert failure is None
        return
    assert failure == (
        "stocks.trades: Massive sent 2 rows the library cannot read whole, so it withheld them: "
        f"2 under {fault.value}. The first. To go on without such rows, pass "
        f"on_error=dm.ErrorPolicy({fault.value}=dm.OnError.LOG) to the call or to dm.MassiveConfig."
    )


def test_failure_of_names_every_failing_cause() -> None:
    failure = failure_of(
        {KEYS: 1, VALUES: 3, SYMBOLS: 2},
        policy(unreadable_keys=dm.OnError.RAISE, unconvertible_values=dm.OnError.RAISE),
        dataset="d",
        first_reason="r",
    )
    # The rows of a cause that logs are neither counted nor named: the message is about why the
    # read fails, and the policy it names sets every cause that fails it.
    assert failure == (
        "d: Massive sent 4 rows the library cannot read whole, so it withheld them: 1 under "
        "unreadable_keys and 3 under unconvertible_values. R. To go on without such rows, pass "
        "on_error=dm.ErrorPolicy(unreadable_keys=dm.OnError.LOG, "
        "unconvertible_values=dm.OnError.LOG) to the call or to dm.MassiveConfig."
    )
    every = failure_of(
        {KEYS: 1, SYMBOLS: 2, VALUES: 3},
        resolve_policy(dm.ErrorPolicy(dm.OnError.RAISE), dm.ErrorPolicy()),
        dataset="d",
        first_reason="r",
    )
    assert every is not None
    assert every.startswith(
        "d: Massive sent 6 rows the library cannot read whole, so it withheld them: 1 under "
        "unreadable_keys, 2 under unreadable_symbols, and 3 under unconvertible_values. R. "
    )


def test_failure_of_names_one_row_and_where_it_came_from() -> None:
    failure = failure_of(
        {KEYS: 1},
        policy(unreadable_keys=dm.OnError.RAISE),
        dataset="stocks.trades",
        first_reason="the required column sip_timestamp is absent or null",
        where="s3://flatfiles/us_stocks_sip/trades_v1/2025/06/2025-06-06.csv.gz",
    )
    assert failure == (
        "stocks.trades: Massive sent 1 row the library cannot read whole, so it withheld it: 1 "
        "under unreadable_keys, from s3://flatfiles/us_stocks_sip/trades_v1/2025/06/"
        "2025-06-06.csv.gz. The required column sip_timestamp is absent or null. To go on "
        "without such rows, pass on_error=dm.ErrorPolicy(unreadable_keys=dm.OnError.LOG) to the "
        "call or to dm.MassiveConfig."
    )


AAPL_DIVIDEND = {
    "ticker": "AAPL",
    "cash_amount": 0.25,
    "distribution_type": "recurring",
    "ex_dividend_date": "2025-02-10",
    "frequency": 4,
    "id": "E1",
}
"""One dividend row as Massive sends it."""


def test_the_policy_the_failure_names_reads_the_rows_beside_the_withheld_ones(
    fake_rest: FakeRestServer,
) -> None:
    # The advice works: a read that fails under two causes set to RAISE, read again with the
    # policy its message names, gives the rows the library could read whole.
    unreadable_key = {k: v for k, v in AAPL_DIVIDEND.items() if k != "ticker"}
    unconvertible_value = {**AAPL_DIVIDEND, "id": "E2", "frequency": 4.5}
    page = Reply(envelope([unreadable_key, unconvertible_value, AAPL_DIVIDEND]))
    fake_rest.reply("/stocks/v1/dividends", page, page)
    config = dm.MassiveConfig(
        api_key="test-key", rest_base_url=fake_rest.url, on_error=dm.ErrorPolicy(dm.OnError.RAISE)
    )
    with dm.MassiveSession(config) as m:
        with pytest.raises(dm.MassiveError) as info:
            m.stocks.dividends("AAPL").fetch(wait=True)
        assert str(info.value) == (
            "stocks.dividends: Massive sent 2 rows the library cannot read whole, so it withheld "
            "them: 1 under unreadable_keys and 1 under unconvertible_values. /stocks/v1/dividends: "
            "row 0: no Ticker (wire key 'ticker'), which the dataset needs. To go on "
            "without such rows, pass on_error=dm.ErrorPolicy(unreadable_keys=dm.OnError.LOG, "
            "unconvertible_values=dm.OnError.LOG) to the call or to dm.MassiveConfig."
        )
        named = dm.ErrorPolicy(unreadable_keys=dm.OnError.LOG, unconvertible_values=dm.OnError.LOG)
        table = m.stocks.dividends("AAPL", on_error=named).fetch(wait=True)
        assert table.size == 1


@pytest.mark.parametrize("setting", list(dm.OnError))
def test_withheld_rows_log_above_debug_only_under_a_cause_set_to_log(setting: dm.OnError) -> None:
    others = {fault.value: dm.OnError.IGNORE for fault in RowFault if fault is not VALUES}
    found = policy(**others, unconvertible_values=setting)
    assert logs_withheld({VALUES: 1}, found) is (setting is not dm.OnError.IGNORE)
    assert not logs_withheld({KEYS: 3}, found)
    assert not logs_withheld({}, found)


def test_merged_parts_add_up_each_fault_symbol_and_field_in_first_order() -> None:
    a = WithheldGroup(VALUES, "AAPL", "AAPL", "Price", 2, "first price")
    b = WithheldGroup(KEYS, None, None, "Symbol", 1, "no ticker")
    first = Withheld(pa.table({}), (a, b), "first price")
    second = Withheld(
        pa.table({}), (WithheldGroup(VALUES, "AAPL", "AAPL", "Price", 3, "later"),), "later"
    )
    both = merged((Withheld(pa.table({})), first, second))
    assert both.groups == (WithheldGroup(VALUES, "AAPL", "AAPL", "Price", 5, "first price"), b)
    assert (both.first_reason, both.total, both.rows.num_rows) == ("first price", 6, 0)
