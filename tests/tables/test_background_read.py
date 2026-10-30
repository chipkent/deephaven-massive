"""Tests for background reads: the one-update fill, the m.streams() row, release, close, failure."""

import gc
import logging
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

import pytest
from deephaven import arrow as dh_arrow
from deephaven import empty_table
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.schema import ColumnType, Schema
from deephaven_massive._enums import AssetClass, NotBars
from deephaven_massive._errors import LIBRARY_BUG
from deephaven_massive._massive import Transport
from deephaven_massive._tables import background_read
from deephaven_massive._tables.background_read import BACKGROUND_NOTE, BackgroundRead, open_read
from deephaven_massive._tables.diagnostics import StreamState
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import ResultRecord, ResultRegistry, new_scope

if TYPE_CHECKING:
    from deephaven.liveness_scope import LivenessScope

TIMEOUT = 30.0

SPEC = DatasetSpec(
    name="test.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    channels={NotBars.ANY: "T"},
)

Rows = Callable[[], list[dict[str, Any]]]
Run = Callable[[threading.Event], Table]


def until(condition: Callable[[], bool]) -> None:
    # Waits for the update graph to apply a published row; fails after TIMEOUT.
    event = threading.Event()
    for _ in range(int(TIMEOUT / 0.01)):
        if condition():
            return
        event.wait(0.01)
    assert condition()


def until_done(stream_rows: Rows) -> dict[str, Any]:
    # Waits for the read's row in m.streams() to show DONE, and returns it. The row is
    # recorded in the update cycle that adds the rows and shown on a later one, so once it
    # shows, every snapshot of the result holds the rows.
    until(lambda: stream_rows()[0]["State"] == "DONE")
    (row,) = stream_rows()
    return row


def schema(m: dm.MassiveSession) -> Schema:
    return m._core.dataset_schema(SPEC)


def static_rows(m: dm.MassiveSession, *tickers: str) -> Table:
    # A static table in the dataset's schema, as a transport returns.
    rows = [{"Ticker": ticker, "TimestampInsert": 1} for ticker in tickers]
    with entered(m._core.exec_ctx):
        return dh_arrow.to_table(schema(m).to_arrow(rows))


def returning(table: Table) -> Run:
    def run(stop: threading.Event) -> Table:
        return table

    return run


def open_test_read(m: dm.MassiveSession, run: Run, *, note: str | None = None) -> Table:
    call = m._core.next_stream_id()
    return open_read(
        m._core,
        spec=SPEC,
        call=call,
        query="<Query test.trades symbols=('AAPL',)>",
        symbols=("AAPL",),
        transport=Transport.FLAT_FILES,
        note=note,
        run=run,
    )


def the_read(m: dm.MassiveSession) -> BackgroundRead:
    # The one read the session registered: its fail hook is the read's bound method.
    (state,) = m._core._workers.values()
    return cast("BackgroundRead", cast("Any", state.fail).__self__)


def failed(table: Table) -> bool:
    return bool(table.j_table.isFailed())


def tickers(table: Table, to_pylist: Callable[[Table], list[dict[str, Any]]]) -> list[str]:
    return [row["Ticker"] for row in to_pylist(table)]


def test_a_read_fills_its_table_in_one_update(
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    gate = threading.Event()
    rows = static_rows(massive_session, "AAPL", "MSFT")

    def run(stop: threading.Event) -> Table:
        assert gate.wait(TIMEOUT)
        return rows

    result = open_test_read(massive_session, run, note="Read from flat files: a test.")
    assert result.is_refreshing
    assert result.size == 0
    assert [c.name for c in result.columns] == list(schema(massive_session).column_names)
    assert result.attributes()["TableDescription"] == (
        f"test.trades: A dataset for the tests. Read from flat files: a test. {BACKGROUND_NOTE}"
    )
    (row,) = stream_rows()
    assert {k: row[k] for k in ("Kind", "State", "Transport", "Host", "Path", "RowsPublished")} == {
        "Kind": "read",
        "State": "LOADING",
        "Transport": "flat files",
        "Host": None,
        "Path": None,
        "RowsPublished": 0,
    }
    assert (row["Query"], row["Symbols"], row["HeldResults"]) == (
        "<Query test.trades symbols=('AAPL',)>",
        "AAPL",
        1,
    )
    read = the_read(massive_session)
    gate.set()
    read.thread.join(TIMEOUT)
    row = until_done(stream_rows)
    assert tickers(result, dh_to_pylist) == ["AAPL", "MSFT"]
    assert (row["State"], row["RowsPublished"], row["Error"]) == ("DONE", 2, None)
    massive_session.release(result)
    (row,) = stream_rows()
    assert (row["State"], row["HeldResults"]) == ("DONE", 0)  # final: a release keeps it
    until(lambda: not massive_session._core._workers)


def test_a_read_without_a_note_says_only_that_it_reads_in_the_background(
    massive_session: dm.MassiveSession, stream_rows: Rows
) -> None:
    result = open_test_read(massive_session, returning(static_rows(massive_session)))
    assert result.attributes()["TableDescription"] == (
        f"test.trades: A dataset for the tests. {BACKGROUND_NOTE}"
    )
    the_read(massive_session).thread.join(TIMEOUT)
    # An empty result adds no rows, so the read itself records it DONE once published.
    (row,) = stream_rows()
    assert (row["State"], row["RowsPublished"]) == ("DONE", 0)
    assert result.size == 0
    massive_session.release(result)


@pytest.mark.jvm_held
def test_the_result_is_published_in_the_dataset_s_column_order(
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    # A transport's table with its columns in another order, and one more, still lands.
    rows = static_rows(massive_session, "AAPL")
    with entered(massive_session._core.exec_ctx):
        shuffled = rows.update_view("Extra = 1").move_columns_up(["TimestampInsert", "Price"])
    result = open_test_read(massive_session, returning(shuffled))
    until_done(stream_rows)
    assert tickers(result, dh_to_pylist) == ["AAPL"]
    massive_session.release(result)


def test_a_failed_download_fails_the_table_and_its_row(
    massive_session: dm.MassiveSession, stream_rows: Rows, caplog: pytest.LogCaptureFixture
) -> None:
    def run(stop: threading.Event) -> Table:
        raise dm.MassiveError("HTTP 500 for /v3/trades/AAPL (request_id=r1)")

    with caplog.at_level(logging.WARNING, logger=background_read.__name__):
        result = open_test_read(massive_session, run)
        the_read(massive_session).thread.join(TIMEOUT)
    until(lambda: failed(result))
    (row,) = stream_rows()
    assert (row["State"], row["Error"]) == (
        "FAILED",
        "HTTP 500 for /v3/trades/AAPL (request_id=r1)",
    )
    (record,) = caplog.records
    assert record.levelno == logging.WARNING  # an expected failure: stale data, not a defect
    assert record.getMessage().endswith(": HTTP 500 for /v3/trades/AAPL (request_id=r1)")
    assert record.getMessage().startswith(f"call {row['StreamId']} failed after ")
    assert record.exc_info is None  # the text says it all
    assert not the_read(massive_session).fail(dm.MassiveError("later"))  # once only
    massive_session.release(result)


def test_a_defect_fails_the_table_with_a_massive_error_naming_it(
    massive_session: dm.MassiveSession, stream_rows: Rows, caplog: pytest.LogCaptureFixture
) -> None:
    # A transport table that lacks one of the dataset's columns cannot be published.
    with entered(massive_session._core.exec_ctx):
        partial = static_rows(massive_session, "AAPL").drop_columns("Price")
    with caplog.at_level(logging.ERROR, logger=background_read.__name__):
        result = open_test_read(massive_session, returning(partial))
        the_read(massive_session).thread.join(TIMEOUT)
    until(lambda: failed(result))
    (row,) = stream_rows()
    assert row["State"] == "FAILED"
    assert row["Error"].startswith("test.trades: the background read failed: ")
    assert "Price" in row["Error"]
    assert row["Error"].endswith(LIBRARY_BUG)
    (record,) = caplog.records  # once, though the table's failure is logged too
    assert record.levelno == logging.ERROR
    assert record.getMessage().endswith(LIBRARY_BUG)
    assert record.exc_info is not None  # a defect is logged with its traceback
    massive_session.release(result)


def stopped_run(rows: Table, *, raises: type[Exception] | None) -> tuple[Run, threading.Event]:
    # A transport call that runs until its stop token is set, then ends the way a stopped call
    # may: with an error (the library's own, or any other), or with whatever it had read.
    began = threading.Event()

    def run(stop: threading.Event) -> Table:
        began.set()
        assert stop.wait(TIMEOUT)
        if raises is not None:
            raise raises("the read was stopped")  # a new error each time: none outlives the test
        return rows

    return run, began


@pytest.mark.parametrize(
    "raises",
    [dm.MassiveError, OSError, None],
    ids=["library-error", "other-error", "returned"],
)
def test_releasing_a_loading_read_stops_it_and_publishes_nothing(
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    caplog: pytest.LogCaptureFixture,
    *,
    raises: type[Exception] | None,
) -> None:
    run, began = stopped_run(static_rows(massive_session, "AAPL"), raises=raises)
    with caplog.at_level(logging.DEBUG, logger=background_read.__name__):
        result = open_test_read(massive_session, run)
        read = the_read(massive_session)
        assert began.wait(TIMEOUT)
        massive_session.release(result)
        del result
        gc.collect()
        until(lambda: read.stopped)
        read.thread.join(TIMEOUT)
    assert not read.thread.is_alive()
    (row,) = stream_rows()
    assert (row["State"], row["HeldResults"], row["RowsPublished"]) == ("RELEASED", 0, 0)
    assert [r.getMessage() for r in caplog.records if "stopped" in r.getMessage()] != []
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
    until(lambda: not massive_session._core._workers)


def test_a_derived_table_keeps_the_read_going_after_release(
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    gate = threading.Event()
    rows = static_rows(massive_session, "AAPL")

    def run(stop: threading.Event) -> Table:
        assert gate.wait(TIMEOUT)
        return rows

    result = open_test_read(massive_session, run)
    read = the_read(massive_session)
    scope = new_scope()  # the user's own scope, released below
    with scope.open():
        # Not a formula: it would capture this frame's locals in the query scope.
        derived = [result.tail(10)]
    massive_session.release(result)
    del result
    gc.collect()
    (row,) = stream_rows()
    assert row["State"] == "RELEASED"
    gate.set()
    read.thread.join(TIMEOUT)
    row = until_done(stream_rows)  # the released result still records its rows landing
    assert row["RowsPublished"] == 1
    assert tickers(derived[0], dh_to_pylist) == ["AAPL"]
    derived.clear()
    with entered(massive_session._core.exec_ctx):
        scope.release()
    until(lambda: not massive_session._core._workers)


def test_closing_the_session_fails_a_loading_read(
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    run, began = stopped_run(static_rows(massive_session), raises=dm.MassiveError)
    result = open_test_read(massive_session, run)
    read = the_read(massive_session)
    assert began.wait(TIMEOUT)
    rows = stream_rows()  # read before the close releases the diagnostics
    assert rows[0]["State"] == "LOADING"
    view = massive_session.streams()
    massive_session.close()
    read.thread.join(TIMEOUT)
    assert read.stopped
    until(lambda: failed(result))
    (row,) = [r for r in dh_to_pylist(view) if r["Kind"] == "read"]
    assert (row["State"], row["Error"]) == (
        "CLOSED",
        f"{massive_session._core.label} was closed; this table no longer updates",
    )
    del view


def test_a_close_after_the_rows_landed_changes_nothing(
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    result = open_test_read(massive_session, returning(static_rows(massive_session, "AAPL")))
    read = the_read(massive_session)
    read.thread.join(TIMEOUT)
    until_done(stream_rows)
    view = massive_session.streams()
    massive_session.close()
    assert not read.fail(dm.MassiveError("closed"))  # it has its rows
    assert not failed(result)
    assert tickers(result, dh_to_pylist) == ["AAPL"]
    (row,) = [r for r in dh_to_pylist(view) if r["Kind"] == "read"]
    assert row["State"] == "DONE"
    del view
    massive_session.release(result)


def test_a_read_racing_close_after_registering_never_starts(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    track = ResultRegistry.track
    reads: list[BackgroundRead] = []

    def track_then_close(self: ResultRegistry, *args: object) -> ResultRecord:
        reads.append(the_read(massive_session))
        record = track(self, *cast("tuple[Table, LivenessScope, Callable[[], None]]", args))
        massive_session.close()
        return record

    monkeypatch.setattr(ResultRegistry, "track", track_then_close)
    with pytest.raises(dm.MassiveError, match="is closed"):
        open_test_read(massive_session, returning(static_rows(massive_session)))
    monkeypatch.undo()
    (read,) = reads
    assert read.stopped
    assert read.thread.ident is None  # never started
    until(lambda: not massive_session._core._workers)


def test_a_read_on_a_closed_session_is_refused(massive_session: dm.MassiveSession) -> None:
    massive_session.close()
    with pytest.raises(dm.MassiveError, match="is closed"):
        open_test_read(massive_session, returning(static_rows(massive_session)))
    assert massive_session._core._workers == {}


def test_a_failed_registration_releases_everything(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch, stream_rows: Rows
) -> None:
    def refuse(*args: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(ResultRegistry, "track", refuse)
    with pytest.raises(KeyboardInterrupt):
        open_test_read(massive_session, returning(static_rows(massive_session)))
    monkeypatch.undo()
    assert massive_session._core._workers == {}
    (row,) = stream_rows()
    assert (row["State"], row["HeldResults"]) == ("RELEASED", 0)


def test_a_read_opened_in_a_console_command_lands_after_it(
    *,
    massive_session: dm.MassiveSession,
    console_command: Callable[[Callable[[], None]], None],
    stream_rows: Rows,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    # The command holds the update graph's lock: the read returns at once, and its rows land
    # in the first cycle after the command ends. Until then the read is not DONE, although
    # its thread has published: DONE means the rows are in the table.
    results: list[Table] = []
    rows = static_rows(massive_session, "AAPL")

    def command() -> None:
        results.append(open_test_read(massive_session, returning(rows)))
        read = the_read(massive_session)
        read.thread.join(TIMEOUT)  # it published without the lock
        assert results[0].size == 0
        (state,) = massive_session._core._workers.values()
        assert state.row.state == StreamState.LOADING

    console_command(command)
    row = until_done(stream_rows)
    assert row["RowsPublished"] == 1
    assert tickers(results[0], dh_to_pylist) == ["AAPL"]
    massive_session.release(results[0])


_NULLS = {
    ColumnType.STRING: "(String) null",
    ColumnType.LONG: "NULL_LONG",
    ColumnType.DOUBLE: "NULL_DOUBLE",
    ColumnType.INSTANT: "(java.time.Instant) null",
    ColumnType.LONG_ARRAY: "(long[]) null",
}


def lazy_rows(m: dm.MassiveSession, count: int) -> Table:
    # A static table of `count` rows whose every column is a formula computed on read, so
    # copying it would take seconds and gigabytes.
    formulas = [
        "Ticker = `T` + ii" if c.name == "Ticker" else f"{c.name} = {_NULLS[c.type]}"
        for c in schema(m).columns
    ]
    with entered(m._core.exec_ctx):
        return empty_table(count).view(formulas)


@pytest.mark.jvm_held
def test_a_billion_rows_land_in_one_cycle_without_a_copy(
    massive_session: dm.MassiveSession,
) -> None:
    rows = lazy_rows(massive_session, 1_000_000_000)
    gate = threading.Event()

    def run(stop: threading.Event) -> Table:
        assert gate.wait(TIMEOUT)
        return rows

    result = open_test_read(massive_session, run)
    read = the_read(massive_session)
    gate.set()
    read.thread.join(TIMEOUT)
    published = time.monotonic()
    until(lambda: result.size == 1_000_000_000)
    assert time.monotonic() - published < 1.0
    massive_session.release(result)
