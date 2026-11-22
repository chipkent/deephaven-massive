"""Tests for background reads: the one-update fill, the m.streams() row, release, close, failure."""

import dataclasses
import functools
import gc
import logging
import re
import threading
import time
import weakref
from collections.abc import Callable
from types import MethodType, SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest
from deephaven import agg, empty_table
from deephaven import arrow as dh_arrow
from deephaven.liveness_scope import LivenessScope
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.schema import Column, ColumnType, Schema
from deephaven_massive._enums import AssetClass, NotBars
from deephaven_massive._errors import LIBRARY_BUG, MassiveError
from deephaven_massive._massive import Transport
from deephaven_massive._tables import background_read, live_set
from deephaven_massive._tables.background_read import BACKGROUND_NOTE, BackgroundRead, open_read
from deephaven_massive._tables.diagnostics import StreamState
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import (
    ResultRecord,
    ResultRegistry,
    new_scope,
    release_scope,
)
from tests._fakes.rest import FakeRestServer, Reply, envelope

if TYPE_CHECKING:
    from deephaven.table_listener import TableUpdate


TIMEOUT = 30.0

ANSWER_500 = (
    "Massive answered GET /v3/trades/AAPL on api.massive.com with HTTP 500 (request_id r1): "
    '"Internal Server Error".'
)
"""Massive's answer to the read's request, as the REST client words it."""

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


def _read_of(fail: Callable[[MassiveError], bool]) -> BackgroundRead:
    # A registered read's fail hook is the read's bound method.
    assert isinstance(fail, MethodType)
    read = fail.__self__
    assert isinstance(read, BackgroundRead)
    return read


def the_read(m: dm.MassiveSession) -> BackgroundRead:
    # The one read the session registered.
    (state,) = m._core.workers._workers.values()
    return _read_of(state.fail)


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
    assert BACKGROUND_NOTE == (
        "This table is read in the background. It has no rows until the download completes, "
        "then gets the complete result in one update, which never changes. m.streams() shows "
        "LOADING, then DONE or FAILED. Pass wait=True to the call to wait for the rows instead."
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
    read.join(TIMEOUT)
    row = until_done(stream_rows)
    assert tickers(result, dh_to_pylist) == ["AAPL", "MSFT"]
    assert (row["State"], row["RowsPublished"], row["Error"]) == ("DONE", 2, None)
    massive_session.release(result)
    (row,) = stream_rows()
    assert (row["State"], row["HeldResults"]) == ("DONE", 0)  # final: a release keeps it
    until(lambda: not massive_session._core.workers._workers)


def test_a_read_without_a_note_says_only_that_it_reads_in_the_background(
    massive_session: dm.MassiveSession, stream_rows: Rows
) -> None:
    result = open_test_read(massive_session, returning(static_rows(massive_session)))
    assert result.attributes()["TableDescription"] == (
        f"test.trades: A dataset for the tests. {BACKGROUND_NOTE}"
    )
    the_read(massive_session).join(TIMEOUT)
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
        raise dm.MassiveError(ANSWER_500, status=500, request_id="r1")

    with caplog.at_level(logging.WARNING, logger=background_read.__name__):
        result = open_test_read(massive_session, run)
        the_read(massive_session).join(TIMEOUT)
    until(lambda: failed(result))
    (row,) = stream_rows()
    assert (row["State"], row["Error"]) == (
        "FAILED",
        ANSWER_500,
    )
    (record,) = caplog.records
    assert record.levelno == logging.WARNING  # an expected failure: stale data, not a defect
    assert re.fullmatch(
        r"test\.trades background read stream 1 failed after \d+\.\d{3} s: "
        + re.escape(ANSWER_500),
        record.getMessage(),
    )
    assert row["StreamId"] == 1
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
        the_read(massive_session).join(TIMEOUT)
    until(lambda: failed(result))
    (row,) = stream_rows()
    assert row["State"] == "FAILED"
    assert row["Error"].startswith(
        "test.trades: the background read failed on an unexpected DHError: table view operation "
        "failed. : RuntimeError: io.deephaven.engine.table.impl.NoSuchColumnException: Unknown "
        "column names [Price]"
    )
    assert row["Error"].endswith(f". {LIBRARY_BUG}")
    (record,) = caplog.records  # once, though the table's failure is logged too
    assert record.levelno == logging.ERROR
    assert re.fullmatch(
        r"test\.trades background read stream 1 failed after \d+\.\d{3} s\. "
        + re.escape(LIBRARY_BUG),
        record.getMessage(),
    )
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
        read.join(TIMEOUT)
    assert read.join(TIMEOUT)
    (row,) = stream_rows()
    assert (row["State"], row["HeldResults"], row["RowsPublished"]) == ("RELEASED", 0, 0)
    assert [r.getMessage() for r in caplog.records if "stopped" in r.getMessage()] != []
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
    until(lambda: not massive_session._core.workers._workers)


def _reads(m: dm.MassiveSession) -> list[BackgroundRead]:
    # Every read the session registered, in call order.
    states = sorted(m._core.workers._workers.items())
    return [_read_of(state.fail) for _, state in states]


def test_reads_beyond_max_concurrency_wait_their_turn_on_the_read_pool(
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(massive_session._core.reads, "_max_workers", 1)
    gate = threading.Event()
    threads: list[str] = []

    def held(stop: threading.Event) -> Table:
        threads.append(threading.current_thread().name)
        assert gate.wait(TIMEOUT)
        return static_rows(massive_session, "AAPL")

    def second(stop: threading.Event) -> Table:
        threads.append(threading.current_thread().name)
        return static_rows(massive_session, "MSFT")

    first_result = open_test_read(massive_session, held)
    until(lambda: len(threads) == 1)
    second_result = open_test_read(massive_session, second)
    assert [row["State"] for row in stream_rows()] == ["LOADING", "LOADING"]
    assert len(threads) == 1  # the one thread is busy: the second read waits
    gate.set()
    for read in _reads(massive_session):
        assert read.join(TIMEOUT)
    prefix = massive_session._core.thread_prefix
    assert threads == [f"{prefix} read_0", f"{prefix} read_0"]
    massive_session.release(first_result)
    massive_session.release(second_result)


def test_a_read_released_before_its_turn_makes_no_request(
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(massive_session._core.reads, "_max_workers", 1)
    gate, began = threading.Event(), threading.Event()
    calls: list[str] = []

    def held(stop: threading.Event) -> Table:
        began.set()
        assert gate.wait(TIMEOUT)
        return static_rows(massive_session, "AAPL")

    def waiting(stop: threading.Event) -> Table:
        calls.append("requested")
        return static_rows(massive_session, "MSFT")

    first_result = open_test_read(massive_session, held)
    assert began.wait(TIMEOUT)
    result = open_test_read(massive_session, waiting)
    first, second = _reads(massive_session)
    with caplog.at_level(logging.DEBUG, logger=background_read.__name__):
        massive_session.release(result)
        del result
        gc.collect()
        until(lambda: second.stopped)
        gate.set()
        assert second.join(TIMEOUT)
    assert calls == []
    assert any("stopped before its turn" in m for m in caplog.messages)
    assert first.join(TIMEOUT)
    massive_session.release(first_result)


def test_a_read_the_stopped_pool_refuses_never_runs(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    massive_session._core.reads.stop()  # as the close does, after it failed every read
    calls: list[str] = []

    def run(stop: threading.Event) -> Table:
        calls.append("requested")
        return static_rows(massive_session, "AAPL")

    with caplog.at_level(logging.DEBUG, logger=background_read.__name__):
        result = open_test_read(massive_session, run)
    read = the_read(massive_session)
    assert read.stopped
    assert not read.join(0)
    assert calls == []
    assert any("the read pool stopped" in m for m in caplog.messages)
    massive_session.release(result)


def test_the_landing_listener_marks_the_read_done_with_the_rows_added(
    massive_session: dm.MassiveSession, stream_rows: Rows, caplog: pytest.LogCaptureFixture
) -> None:
    # Called here, as Deephaven calls it on an update-graph thread, which a coverage tracer
    # before Python 3.14 does not follow.
    gate = threading.Event()
    rows = static_rows(massive_session, "AAPL")

    def run(stop: threading.Event) -> Table:
        assert gate.wait(TIMEOUT)
        return rows

    result = open_test_read(massive_session, run)
    read = the_read(massive_session)
    (call,) = massive_session._core.workers._workers
    landed = background_read._RowsLanded(weakref.ref(massive_session._core), call)
    added = SimpleNamespace(size=lambda: 3)
    update: TableUpdate = SimpleNamespace(  # type: ignore[assignment]  # a fake update: 3 added
        j_table_update=SimpleNamespace(added=lambda: added)
    )
    with caplog.at_level(logging.DEBUG, logger=background_read.__name__):
        landed.on_update(update, is_replay=False)
        landed.on_error(RuntimeError("the table broke"))
    (row,) = stream_rows()
    assert (row["State"], row["RowsPublished"]) == ("DONE", 3)
    assert [r.getMessage() for r in caplog.records] == [
        f"call {call} done: its 3 rows landed in the result",
        f"call {call}: its result failed: the table broke",
    ]
    gate.set()
    read.join(TIMEOUT)
    massive_session.release(result)
    until(lambda: not massive_session._core.workers._workers)


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
    read.join(TIMEOUT)
    row = until_done(stream_rows)  # the released result still records its rows landing
    assert row["RowsPublished"] == 1
    assert tickers(derived[0], dh_to_pylist) == ["AAPL"]
    derived.clear()
    release_scope(massive_session._core.exec_ctx, scope)
    until(lambda: not massive_session._core.workers._workers)


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
    read.join(TIMEOUT)
    assert read.stopped
    until(lambda: failed(result))
    (row,) = [r for r in dh_to_pylist(view) if r["Kind"] == "read"]
    assert (row["State"], row["Error"]) == (
        "CLOSED",
        (
            "This session was closed, so this table no longer updates. Open a new "
            "dm.MassiveSession() to read again."
        ),
    )
    del view


def test_a_close_after_the_rows_landed_changes_nothing(
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    result = open_test_read(massive_session, returning(static_rows(massive_session, "AAPL")))
    read = the_read(massive_session)
    read.join(TIMEOUT)
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


def test_a_console_close_between_the_publish_and_the_landing_shows_the_read_done(
    *,
    massive_session: dm.MassiveSession,
    console_command: Callable[[Callable[[], None]], None],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    # The command holds the update graph's lock, so the rows land only after the close has
    # released the diagnostics: the close records the published read DONE itself.
    results: list[Table] = []
    rows = static_rows(massive_session, "AAPL")
    views = [massive_session.streams()]  # cleared at the end, which drops the view

    def command() -> None:
        results.append(open_test_read(massive_session, returning(rows)))
        read = the_read(massive_session)
        read.join(TIMEOUT)  # it published; the rows wait for the next cycle
        massive_session.close()
        assert not read.fail(dm.MassiveError("closed"))  # it has its rows

    console_command(command)

    def read_rows() -> list[tuple[str, int, str | None]]:
        # The view gets its rows once the command has released the lock.
        return [
            (r["State"], r["RowsPublished"], r["Error"])
            for r in dh_to_pylist(views[0])
            if r["Kind"] == "read"
        ]

    until(lambda: read_rows() == [("DONE", 1, None)])
    assert not failed(results[0])
    assert tickers(results[0], dh_to_pylist) == ["AAPL"]
    views.clear()
    massive_session.release(results[0])


def test_a_read_racing_close_after_registering_never_starts(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    track = ResultRegistry.track
    reads: list[BackgroundRead] = []

    def track_then_close(
        self: ResultRegistry, *args: *tuple[Table, LivenessScope, Callable[[], None]]
    ) -> ResultRecord:
        reads.append(the_read(massive_session))
        record = track(self, *args)
        massive_session.close()
        return record

    monkeypatch.setattr(ResultRegistry, "track", track_then_close)
    with pytest.raises(dm.MassiveError, match="is closed"):
        open_test_read(massive_session, returning(static_rows(massive_session)))
    monkeypatch.undo()
    (read,) = reads
    assert read.stopped
    assert not read.join(0)  # never submitted
    until(lambda: not massive_session._core.workers._workers)


def test_a_read_on_a_closed_session_is_refused(massive_session: dm.MassiveSession) -> None:
    massive_session.close()
    with pytest.raises(dm.MassiveError, match="is closed"):
        open_test_read(massive_session, returning(static_rows(massive_session)))
    assert massive_session._core.workers._workers == {}


def test_a_failed_registration_releases_everything(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch, stream_rows: Rows
) -> None:
    def refuse(*args: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(ResultRegistry, "track", refuse)
    with pytest.raises(KeyboardInterrupt):
        open_test_read(massive_session, returning(static_rows(massive_session)))
    monkeypatch.undo()
    assert massive_session._core.workers._workers == {}
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
        read.join(TIMEOUT)  # it published without the lock
        assert results[0].size == 0
        (state,) = massive_session._core.workers._workers.values()
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
    read.join(TIMEOUT)
    published = time.monotonic()
    until(lambda: result.size == 1_000_000_000)
    assert time.monotonic() - published < 1.0
    massive_session.release(result)


@pytest.mark.jvm_held  # the rows' formulas hand this frame's locals to the JVM
def test_a_result_past_two_billion_rows_reads_every_value(
    massive_session: dm.MassiveSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    # Deephaven's ungroup reads a vector longer than 2^31 wrongly, so the read groups such a
    # result in slices: a value past 2^31 rows is the one the transport returned.
    count = 2**31 + 10
    result = open_test_read(massive_session, returning(lazy_rows(massive_session, count)))
    the_read(massive_session).join(TIMEOUT)
    until(lambda: result.size == count)
    with entered(massive_session._core.exec_ctx):
        late = result.slice(count - 3, count - 2)
    assert tickers(late, dh_to_pylist) == [f"T{count - 3}"]
    del late
    massive_session.release(result)


def test_a_result_grouped_in_slices_lands_whole_and_in_order(
    *,
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(background_read, "GROUP_ROWS", 2)
    names = ["A", "B", "C", "D", "E"]
    result = open_test_read(massive_session, returning(static_rows(massive_session, *names)))
    row = until_done(stream_rows)
    assert row["RowsPublished"] == 5  # the first update that landed held every row
    assert tickers(result, dh_to_pylist) == names
    massive_session.release(result)


# --- Failure delivery: a read loading or landed, every way it ends ------------------------------
# A table that hears of a second failure ends the process. A loading read fails once, from its
# own download or from the session's close, whichever comes first, while the user may be
# releasing it; a read whose rows landed is final and fails from nothing. Each case reads
# dividends from the fake REST server with `fetch(wait=False)` (as `history(wait=False)` does,
# through the same background read), derives the user's tables from the result, and checks
# which end failed, that each heard of it at most once, and that the process is still running.

DIVIDENDS_PATH = "/stocks/v1/dividends"
"""The REST path of `m.stocks.dividends`."""

DIVIDEND = {"ticker": "AAPL", "cash_amount": 0.25}
"""The one dividend the fake server answers with."""


@dataclasses.dataclass
class ReadCase:
    """One case's session, its held reply, the read's result, and the user's tables on it.

    Attributes:
        m: The session, on the fake REST server.
        reply: Set to let the fake server finish its reply to the read's request.
        result: The read's result.
        derived: The user's tables on the result (the `user_tables` fixture).
        heard: The result and each derived table, with the reader of its failure count.
        race: Runs calls at the same moment (the `race` fixture).
        cycles_ended: Waits until every queued failure is delivered (the `cycles_ended`
            fixture).
    """

    m: dm.MassiveSession
    reply: threading.Event
    result: Table
    race: Callable[..., None]
    cycles_ended: Callable[[], None]
    derived: list[Table] = dataclasses.field(default_factory=list)
    heard: list[tuple[Table, Callable[[], int]]] = dataclasses.field(default_factory=list)


def read_state(m: dm.MassiveSession, to_pylist: Callable[[Table], list[dict[str, Any]]]) -> str:
    # The read's state in m.streams(), once every row queued so far is applied.
    assert m._core.diagnostics.wait_until_shown(TIMEOUT)
    (row,) = to_pylist(m.streams())
    state = row["State"]
    assert isinstance(state, str)
    return state


def open_read_case(
    m: dm.MassiveSession,
    fake_rest: FakeRestServer,
    *,
    status: int,
    user_tables: Callable[[Table, str, str], list[Table]],
    failures_heard: Callable[[Table], Callable[[], int]] | None,
    race: Callable[..., None],
    cycles_ended: Callable[[], None],
) -> ReadCase:
    # Starts the read with its reply held after the status line: the read is loading, and its
    # download refused (403) or landing (200) once the case sets `reply`.
    reply = threading.Event()
    body = envelope([DIVIDEND]) if status == 200 else envelope(status="ERROR", message="no plan")
    fake_rest.reply(DIVIDENDS_PATH, Reply(body, status=status, hold=reply))
    case = ReadCase(m, reply, m.stocks.dividends("AAPL").fetch(wait=False), race, cycles_ended)
    until(lambda: len(fake_rest.requests) == 1)
    case.derived = user_tables(case.result, "Symbol", "CashAmount")
    if failures_heard is not None:
        case.heard = [(t, failures_heard(t)) for t in (case.result, *case.derived)]
    return case


def heard_once_each(case: ReadCase) -> list[bool]:
    # Once the cycles that delivered any failure are over, every table that failed heard of it
    # exactly once and every other heard nothing; returns whether each derived table failed.
    case.cycles_ended()
    heard = [(failed(table), count()) for table, count in case.heard]
    assert [count for _, count in heard] == [int(is_failed) for is_failed, _ in heard]
    return [is_failed for is_failed, _ in heard[1:]]


def refused_download(case: ReadCase) -> None:
    case.reply.set()


def closed(case: ReadCase) -> None:
    case.m.close()
    case.reply.set()


def refused_then_closed(case: ReadCase) -> None:
    case.reply.set()
    until(lambda: failed(case.result))
    case.cycles_ended()
    case.m.close()


def close_racing_a_refusal(case: ReadCase) -> None:
    case.race(case.m.close, case.reply.set)


def release_racing_a_refusal(case: ReadCase) -> None:
    # The user's tables keep the read going after the release, so its refusal still fails them.
    case.race(functools.partial(case.m.release, case.result), case.reply.set)


READ_PATHS: dict[str, Callable[[ReadCase], None]] = {
    "download refused": refused_download,
    "session closed": closed,
    "download refused, then session closed": refused_then_closed,
    "close racing a refusal": close_racing_a_refusal,
    "release racing a refusal": release_racing_a_refusal,
}
"""Each way a loading read fails, by test id."""


@pytest.mark.parametrize("path", list(READ_PATHS))
def test_a_loading_read_fails_once_by_every_path(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    user_tables: Callable[[Table, str, str], list[Table]],
    failures_heard: Callable[[Table], Callable[[], int]],
    race: Callable[..., None],
    cycles_ended: Callable[[], None],
    path: str,
) -> None:
    case = open_read_case(
        rest_session,
        fake_rest,
        status=403,
        user_tables=user_tables,
        failures_heard=failures_heard,
        race=race,
        cycles_ended=cycles_ended,
    )
    READ_PATHS[path](case)
    for table in case.derived:
        until(functools.partial(failed, table))
    assert heard_once_each(case) == [True] * len(case.derived)
    rest_session.release(case.result)


@pytest.mark.parametrize("release", [False, True], ids=["closed", "close racing a release"])
def test_a_landed_read_survives_a_close_unfailed(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    user_tables: Callable[[Table, str, str], list[Table]],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    race: Callable[..., None],
    cycles_ended: Callable[[], None],
    release: bool,
) -> None:
    # Its rows are final, so neither the close nor a release racing it fails anything. No
    # listener counts its failures: on a table that never fails, the JVM would hold the
    # listener, and through the table the session, until it collected them.
    case = open_read_case(
        rest_session,
        fake_rest,
        status=200,
        user_tables=user_tables,
        failures_heard=None,
        race=race,
        cycles_ended=cycles_ended,
    )
    case.reply.set()
    until(lambda: read_state(rest_session, dh_to_pylist) == "DONE")
    if release:
        race(rest_session.close, functools.partial(rest_session.release, case.result))
    else:
        rest_session.close()
    cycles_ended()
    assert not any(failed(table) for table in (case.result, *case.derived))
    assert [row["Symbol"] for row in dh_to_pylist(case.derived[0])] == ["AAPL"]
    rest_session.release(case.result)


def test_a_reads_blink_table_is_created_under_the_update_graphs_shared_lock(
    massive_session: dm.MassiveSession, blink_creations: list[bool]
) -> None:
    before = len(blink_creations)
    result = open_test_read(massive_session, returning(static_rows(massive_session, "AAPL")))
    assert blink_creations[before:] == [True]
    massive_session.release(result)


# --- The result is an ordinary table ------------------------------------------------------------


def priced_rows(m: dm.MassiveSession) -> Table:
    # Two trades in the dataset's schema, with prices.
    rows = [
        {"Ticker": "AAPL", "Price": 200.0, "TimestampInsert": 1},
        {"Ticker": "MSFT", "Price": 400.0, "TimestampInsert": 2},
    ]
    with entered(m._core.exec_ctx):
        return dh_arrow.to_table(schema(m).to_arrow(rows))


OPERATIONS: dict[str, Callable[[Table], Table]] = {
    "view": lambda t: t.view(["Ticker", "Price"]),
    "update_view": lambda t: t.update_view("Twice = Price * 2"),
    "where then view": lambda t: t.where("Price > 0").view(["Ticker"]),
    "select": lambda t: t.select(["Ticker"]),
    "update": lambda t: t.update("Twice = Price * 2"),
    "sort": lambda t: t.sort_descending("Price"),
    "agg_by": lambda t: t.agg_by([agg.first("Price")], by="Ticker"),
    "natural_join": lambda t: t.natural_join(t.last_by("Ticker"), on="Ticker", joins="Last=Price"),
}
"""Ordinary operations a user calls on a result, by test id; each keeps `Ticker`."""


@pytest.mark.jvm_held  # the operations' formulas hand this frame's locals to the JVM
@pytest.mark.parametrize("landed", [False, True], ids=["while loading", "once landed"])
def test_every_ordinary_operation_works_on_the_result(
    *,
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    landed: bool,
) -> None:
    # `view` and `update_view` refuse a stateful column, which every column of a merged
    # partitioned table is while it may gain partitions: the result must not be one.
    gate = threading.Event()
    rows = priced_rows(massive_session)

    def run(stop: threading.Event) -> Table:
        assert gate.wait(TIMEOUT)
        return rows

    result = open_test_read(massive_session, run)
    if landed:
        gate.set()
        until_done(stream_rows)
    scope = new_scope()  # the user's own scope, released below
    with scope.open():
        derived = {name: operation(result) for name, operation in OPERATIONS.items()}
    gate.set()
    until_done(stream_rows)
    for name, table in derived.items():
        assert sorted(tickers(table, dh_to_pylist)) == ["AAPL", "MSFT"], name
    derived.clear()
    release_scope(massive_session._core.exec_ctx, scope)
    massive_session.release(result)


def test_the_result_holds_every_column_type_and_null_as_read(
    *,
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    every = Schema("every type", tuple(Column(f"Col{t.name}", t) for t in ColumnType))
    monkeypatch.setattr(massive_session._core, "dataset_schema", lambda _: every)
    values = {
        ColumnType.INSTANT: 1_700_000_000_000_000_000,
        ColumnType.DOUBLE: 1.5,
        ColumnType.LONG: 7,
        ColumnType.INT: 3,
        ColumnType.STRING: "x",
        ColumnType.BOOLEAN: True,
        ColumnType.LONG_ARRAY: [1, 2],
        ColumnType.STRING_ARRAY: ["a", "b"],
    }
    assert set(values) == set(ColumnType)  # a new type is checked here too
    full = {f"Col{t.name}": value for t, value in values.items()}
    with entered(massive_session._core.exec_ctx):
        static = dh_arrow.to_table(every.to_arrow([full, {}]))  # a full row, then all nulls
    result = open_test_read(massive_session, returning(static))
    until_done(stream_rows)
    assert result.j_table.getDefinition().equals(static.j_table.getDefinition())
    assert dh_arrow.to_arrow(result).equals(dh_arrow.to_arrow(static))
    massive_session.release(result)


# --- A finished read lets its session go --------------------------------------------------------


@pytest.mark.parametrize(
    ("released_first", "count"),
    [(False, 1), (True, 1), (True, 0)],
    ids=["landed, then released", "released, then landed", "released, then empty"],
)
def test_a_done_read_with_a_released_result_no_longer_holds_its_session(
    *,
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    released_first: bool,
    count: int,
) -> None:
    # A table derived from the result keeps its rows, and nothing of the read needs the
    # session any more: the session leaves the live set without waiting for the JVM to
    # collect the derived table, which would shut the read's blink table down.
    gate = threading.Event()
    rows = static_rows(massive_session, *["AAPL"][:count])

    def run(stop: threading.Event) -> Table:
        assert gate.wait(TIMEOUT)
        return rows

    core = massive_session._core
    result = open_test_read(massive_session, run)
    scope = new_scope()  # the user's own scope, released below
    with scope.open():
        # Not a formula: it would capture this frame's locals in the query scope.
        derived = [result.tail(10), result.last_by("Ticker")]
    if released_first:
        massive_session.release(result)
        assert stream_rows()[0]["State"] == "RELEASED"
        gate.set()
    else:
        gate.set()
        until_done(stream_rows)
        massive_session.release(result)
    del result
    # The session leaves the live set once the read's record is gone and the reaper has let go
    # of this test's own `m.streams()` views. Other threads make both changes, and each
    # updates the live set last, so the test waits for the live set itself.
    until(lambda: id(core) not in live_set._live)
    (row,) = stream_rows()
    assert (row["State"], row["RowsPublished"]) == ("DONE", count)
    assert [tickers(table, dh_to_pylist) for table in derived] == [["AAPL"][:count]] * 2
    derived.clear()
    release_scope(core.exec_ctx, scope)


def test_a_derived_table_outliving_its_freed_session_shuts_the_read_down_quietly(
    caplog: pytest.LogCaptureFixture,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="freed-session"))
    core = m._core
    alive = weakref.ref(core)
    result = open_test_read(m, returning(static_rows(m, "AAPL")))
    read = the_read(m)
    stream_id = read._call
    workers = core.workers._workers  # read after the core is gone too
    until(lambda: workers[stream_id].row.state == StreamState.DONE)
    scope = new_scope()
    with scope.open():
        derived = [result.tail(10)]
    m.close()
    m.release(result)
    exec_ctx = core.exec_ctx
    del result, core, m
    until(lambda: gc.collect() >= 0 and alive() is None)  # freed while the derived table
    # still uses the read's table
    assert not read._stop.is_set()  # the read's blink table is still in use
    assert tickers(derived[0], dh_to_pylist) == ["AAPL"]
    derived.clear()
    with caplog.at_level(logging.DEBUG):
        # Shuts the read's blink table down, with no session to tell.
        release_scope(exec_ctx, scope)
    assert read._stop.is_set()
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []


def test_the_landing_listener_of_a_freed_session_records_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A close marks a published read DONE before its rows land; its session may then be freed
    # before the cycle that lands them. Called here, as on an update-graph thread.
    def gone() -> None:
        return None  # the core was freed

    landed = background_read._RowsLanded(gone, 7)
    added = SimpleNamespace(size=lambda: 3)
    update: TableUpdate = SimpleNamespace(  # type: ignore[assignment]  # a fake update: 3 added
        j_table_update=SimpleNamespace(added=lambda: added)
    )
    with caplog.at_level(logging.DEBUG, logger=background_read.__name__):
        landed.on_update(update, is_replay=False)
    (record,) = [r for r in caplog.records if r.name == background_read.__name__]
    assert record.getMessage() == "call 7 done: its 3 rows landed in the result"
