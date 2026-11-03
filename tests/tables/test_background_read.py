"""Tests for background reads: the one-update fill, the m.streams() row, release, close, failure."""

import dataclasses
import functools
import gc
import logging
import threading
import time
import weakref
from collections.abc import Callable
from types import SimpleNamespace
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
from tests._fakes.rest import FakeRestServer, Reply, envelope

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
    (call,) = massive_session._core._workers
    landed = background_read._RowsLanded(weakref.ref(massive_session._core), call)
    added = SimpleNamespace(size=lambda: 3)
    update = SimpleNamespace(j_table_update=SimpleNamespace(added=lambda: added))
    with caplog.at_level(logging.DEBUG, logger=background_read.__name__):
        landed.on_update(cast("Any", update), is_replay=False)
        landed.on_error(RuntimeError("the table broke"))
    (row,) = stream_rows()
    assert (row["State"], row["RowsPublished"]) == ("DONE", 3)
    assert [r.getMessage() for r in caplog.records] == [
        f"call {call} done: its 3 rows landed in the result",
        f"call {call}: its result failed: the table broke",
    ]
    gate.set()
    read.thread.join(TIMEOUT)
    massive_session.release(result)
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
        read.thread.join(TIMEOUT)  # it published; the rows wait for the next cycle
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
    return cast("str", row["State"])


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
