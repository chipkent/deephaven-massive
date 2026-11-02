"""Tests for followed reads: the result's tables, its watch, its lifetime, and its failures.

Each test follows a real Deephaven symbol table with a scripted read (`Script`) in place of a
transport, so the engine's behaviour is seen exactly: which symbols are read, when rows land,
and what a removal, a release, a close, or a failure does to the table.
"""

from __future__ import annotations

import dataclasses
import gc
import threading
import weakref
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, cast

import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, input_table, new_table
from deephaven.column import string_col
from deephaven.execution_context import get_exec_ctx
from deephaven.stream.table_publisher import table_publisher
from deephaven.table import Table
from deephaven.table_listener import listen
from deephaven.update_graph import exclusive_lock, shared_lock

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._enums import AssetClass, NotBars, Reread
from deephaven_massive._errors import AuthenticationError, MassiveError
from deephaven_massive._massive import Transport
from deephaven_massive._tables import follow_coordinator, followed_read
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.follow_coordinator import FirstRead
from deephaven_massive._tables.followed_read import FollowPlan, open_followed_read
from deephaven_massive._tables.liveness import ResultRecord, ResultRegistry, new_scope
from deephaven_massive._tables.symbol_read_rows import SymbolReadState
from deephaven_massive._tables.transports import (
    FailureKind,
    SymbolFailure,
    SymbolOutcome,
    SymbolRows,
)
from deephaven_massive._tables.watchlist import SymbolRowRule, SymbolWatch, skipped_rows_text

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
ToList = Callable[[Table], list[dict[str, Any]]]


def until(condition: Callable[[], bool]) -> None:
    # Waits for the coordinator or the update graph to catch up; fails after TIMEOUT.
    pause = threading.Event()
    for _ in range(int(TIMEOUT / 0.01)):
        if condition():
            return
        pause.wait(0.01)
    assert condition()


def rows_of(m: dm.MassiveSession, symbol: str, count: int) -> Table:
    # A symbol's static rows in the dataset's schema, as a read returns them.
    schema = m._core.dataset_schema(SPEC)
    rows = [{"Ticker": symbol, "Price": float(i), "TimestampInsert": 1} for i in range(count)]
    with entered(m._core.exec_ctx):
        return dh_arrow.to_table(schema.to_arrow(rows))


class Script:
    # Stands in for a transport's read by symbol: records each call, holds a symbol's read while
    # its gate is closed, and answers each symbol with its scripted outcome (by default, as many
    # rows as `sizes` names, else 2).
    def __init__(self, m: dm.MassiveSession) -> None:
        self.m = m
        self.calls: list[tuple[str, ...]] = []
        self.stops: list[threading.Event] = []
        self.gates: dict[str, threading.Event] = {}
        self.sizes: dict[str, int] = {}
        self.answers: dict[str, SymbolOutcome | BaseException] = {}
        self._lock = threading.Lock()

    def read(self, names: tuple[str, ...], stop: threading.Event) -> Mapping[str, SymbolOutcome]:
        with self._lock:
            self.calls.append(names)
            self.stops.append(stop)
        for name in names:
            gate = self.gates.get(name)
            if gate is not None:
                assert gate.wait(TIMEOUT)
        outcomes: dict[str, SymbolOutcome] = {}
        for name in names:
            answer = self.answers.get(name)
            if isinstance(answer, BaseException):
                raise answer
            outcomes[name] = answer or SymbolRows(rows_of(self.m, name, self.sizes.get(name, 2)))
        return outcomes

    def read_of(self, name: str) -> int:
        with self._lock:
            return sum(name in call for call in self.calls)


def watchlist(*symbols: str | None, keyed: bool = True) -> Table:
    # A user's watchlist, created under the update graph's lock: a table created while a cycle
    # runs and written at once can notify twice in one step, which ends the process.
    with shared_lock(get_exec_ctx().update_graph):
        table = input_table({"Symbol": dtypes.string}, key_cols="Symbol" if keyed else None)
    if symbols:
        table.add(new_table([string_col("Symbol", list(symbols))]))
    return table


def add(table: Table, *symbols: str) -> None:
    cast("Any", table).add(new_table([string_col("Symbol", list(symbols))]))


def delete(table: Table, *symbols: str) -> None:
    cast("Any", table).delete(new_table([string_col("Symbol", list(symbols))]))


def follow(
    m: dm.MassiveSession,
    table: Table,
    script: Script,
    *,
    reread: Reread = Reread.ALWAYS,
    first: FirstRead | None = None,
    one_scan: bool = False,
    note: str = "Follows a test.",
) -> Table:
    core = m._core
    plan = FollowPlan(
        Transport.FLAT_FILES if one_scan else Transport.REST, one_scan=one_scan, read=script.read
    )
    return open_followed_read(
        core,
        spec=SPEC,
        call=core.next_stream_id(),
        query="m.test.trades(<table>)",
        symbol_table=table,
        rule=SymbolRowRule.of(SPEC, core.config.noncanonical_symbols),
        reread=reread,
        plan=plan,
        note=note,
        first=first,
    )


def tickers(rows: list[dict[str, Any]]) -> list[str]:
    return sorted({row["Ticker"] for row in rows})


def counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    found: dict[str, int] = {}
    for row in rows:
        found[row["Ticker"]] = found.get(row["Ticker"], 0) + 1
    return found


def queued(m: dm.MassiveSession) -> set[str]:
    # The symbols whose m.symbol_reads() row the coordinator has written QUEUED.
    with m._core.registry_lock:
        rows = list(m._core._symbol_reads.values())
    return {row.symbol for row in rows if row.state is SymbolReadState.QUEUED}


def first_read(m: dm.MassiveSession, **sizes: int) -> FirstRead:
    outcomes = {name: SymbolRows(rows_of(m, name, size)) for name, size in sizes.items()}
    return FirstRead(outcomes, began_at=1, seconds=0.1)


def wait_rows(to_list: ToList, table: Table, expected: dict[str, int]) -> None:
    until(lambda: counts(to_list(table)) == expected)


# --- Rows landing -----------------------------------------------------------------------------


def test_each_symbol_lands_whole_in_one_update(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    script.sizes = {"AAPL": 3, "MSFT": 5}
    script.gates = {"MSFT": threading.Event()}
    wl = watchlist("AAPL", "MSFT")
    result = follow(massive_session, wl, script)
    assert result.is_refreshing
    added: list[int] = []
    # A wrapper of its own: the JVM holds a listener, and so its table's wrapper, until it
    # collects it, and the result's own wrapper would keep the result tracked.
    own = Table(j_table=result.j_table)
    handle = listen(own, lambda update, _replay: added.append(update.j_table_update.added().size()))
    try:
        wait_rows(dh_to_pylist, result, {"AAPL": 3})
        script.gates["MSFT"].set()
        wait_rows(dh_to_pylist, result, {"AAPL": 3, "MSFT": 5})
        assert set(added) <= {3, 5}  # each update adds one symbol's whole rows
    finally:
        handle.stop()
    assert sorted(script.calls) == [("AAPL",), ("MSFT",)]
    description = result.attributes()["TableDescription"]
    assert description == "test.trades: A dataset for the tests. Follows a test."
    massive_session.release(result)


def test_seeded_rows_are_in_the_result_when_it_is_returned(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL", "MSFT")
    result = follow(massive_session, wl, script, first=first_read(massive_session, AAPL=3, MSFT=1))
    assert counts(dh_to_pylist(result)) == {"AAPL": 3, "MSFT": 1}
    assert script.calls == []  # the seeded symbols are not read again
    massive_session.release(result)


def test_seeded_rows_are_there_inside_a_console_command(
    massive_session: dm.MassiveSession,
    dh_to_pylist: ToList,
    console_command: Callable[[Callable[[], None]], None],
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL")
    seen: list[dict[str, int]] = []

    def command() -> None:
        result = follow(massive_session, wl, script, first=first_read(massive_session, AAPL=2))
        seen.append(counts(dh_to_pylist(result)))
        massive_session.release(result)

    console_command(command)
    assert seen == [{"AAPL": 2}]


def test_an_empty_symbol_table_fills_as_symbols_are_added(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    wl = watchlist()
    result = follow(massive_session, wl, script)
    assert result.size == 0
    add(wl, "NVDA")
    wait_rows(dh_to_pylist, result, {"NVDA": 2})
    massive_session.release(result)


def test_a_removed_symbol_leaves_in_the_cycle_its_row_does(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL", "MSFT")
    result = follow(massive_session, wl, script)
    wait_rows(dh_to_pylist, result, {"AAPL": 2, "MSFT": 2})
    delete(wl, "MSFT")  # returns once the cycle that removed the row has run
    assert counts(dh_to_pylist(result)) == {"AAPL": 2}
    massive_session.release(result)


def test_a_seeded_symbol_removed_leaves_too(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL", "MSFT")
    result = follow(massive_session, wl, script, first=first_read(massive_session, AAPL=1, MSFT=1))
    delete(wl, "AAPL")
    assert counts(dh_to_pylist(result)) == {"MSFT": 1}
    add(wl, "AAPL")  # ALWAYS: read again, not the seed's rows
    wait_rows(dh_to_pylist, result, {"AAPL": 2, "MSFT": 1})
    assert script.calls == [("AAPL",)]
    massive_session.release(result)


def test_always_reads_a_returning_symbol_again(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    wait_rows(dh_to_pylist, result, {"AAPL": 2})
    delete(wl, "AAPL")
    script.sizes["AAPL"] = 4
    gate = script.gates["AAPL"] = threading.Event()
    add(wl, "AAPL")
    until(lambda: script.read_of("AAPL") == 2)
    assert counts(dh_to_pylist(result)) == {}  # no rows until the new download lands
    gate.set()
    wait_rows(dh_to_pylist, result, {"AAPL": 4})
    massive_session.release(result)


def test_never_gives_a_returning_symbol_its_earlier_download(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script, reread=Reread.NEVER)
    wait_rows(dh_to_pylist, result, {"AAPL": 2})
    delete(wl, "AAPL")
    assert counts(dh_to_pylist(result)) == {}
    add(wl, "AAPL")  # the kept cell shows in the cycle the row returns
    assert counts(dh_to_pylist(result)) == {"AAPL": 2}
    assert script.calls == [("AAPL",)]
    massive_session.release(result)


def test_null_and_duplicate_rows_count_once(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL", "AAPL", None, keyed=False)
    result = follow(massive_session, wl, script)
    wait_rows(dh_to_pylist, result, {"AAPL": 2})
    add(wl, "AAPL")
    assert counts(dh_to_pylist(result)) == {"AAPL": 2}
    assert script.calls == [("AAPL",)]
    massive_session.release(result)


def test_a_blink_symbol_table_keeps_every_symbol_it_delivered(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    with entered(massive_session._core.exec_ctx):
        blink, publisher = table_publisher("symbols", {"Symbol": dtypes.string})
    result = follow(massive_session, blink, script)
    publisher.add(new_table([string_col("Symbol", ["AAPL"])]))
    wait_rows(dh_to_pylist, result, {"AAPL": 2})
    publisher.add(new_table([string_col("Symbol", ["MSFT"])]))
    wait_rows(dh_to_pylist, result, {"AAPL": 2, "MSFT": 2})  # AAPL never flickers out
    assert sorted(script.calls) == [("AAPL",), ("MSFT",)]
    massive_session.release(result)
    with entered(massive_session._core.exec_ctx):
        publisher.publish_failure(RuntimeError("the test is over"))


def test_flat_file_reads_take_the_waiting_symbols_in_one_scan(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script, one_scan=True)
    until(lambda: script.calls == [("AAPL",)])
    add(wl, "MSFT")
    add(wl, "NVDA")
    until(lambda: queued(massive_session) == {"MSFT", "NVDA"})  # both wait behind the scan
    gate.set()
    wait_rows(dh_to_pylist, result, {"AAPL": 2, "MSFT": 2, "NVDA": 2})
    assert script.calls == [("AAPL",), ("MSFT", "NVDA")]
    massive_session.release(result)


# --- Failures of the whole table --------------------------------------------------------------


def test_a_rejected_key_fails_the_table(
    *,
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    failures_heard: Callable[[Table], Callable[[], int]],
    cycles_ended: Callable[[], None],
) -> None:
    script = Script(massive_session)
    script.answers["AAPL"] = AuthenticationError("HTTP 401: the key is wrong")
    gate = script.gates["AAPL"] = threading.Event()
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    heard = failures_heard(result)
    gate.set()
    until(lambda: bool(result.j_table.isFailed()))
    cycles_ended()
    assert heard() == 1
    (row,) = stream_rows()
    assert (row["State"], row["Error"]) == ("FAILED", "HTTP 401: the key is wrong")


def test_a_symbol_table_that_fails_fails_the_result_once(
    *,
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    failures_heard: Callable[[Table], Callable[[], int]],
    cycles_ended: Callable[[], None],
) -> None:
    script = Script(massive_session)
    with entered(massive_session._core.exec_ctx):
        symbols, publisher = table_publisher("symbols", {"Symbol": dtypes.string})
    result = follow(massive_session, symbols, script)
    heard = failures_heard(result)
    with entered(massive_session._core.exec_ctx):
        publisher.publish_failure(RuntimeError("upstream broke"))
    until(lambda: bool(result.j_table.isFailed()))
    until(lambda: stream_rows()[0]["State"] == "FAILED")
    cycles_ended()
    assert heard() == 1
    assert "failed upstream" in stream_rows()[0]["Error"]


def test_a_bad_row_added_under_raise_fails_the_table(
    massive_session: dm.MassiveSession, stream_rows: Rows, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = dataclasses.replace(
        massive_session._core.config, noncanonical_symbols=dm.OnError.RAISE
    )
    monkeypatch.setattr(massive_session._core, "config", config)
    script = Script(massive_session)
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    add(wl, "msft")
    until(lambda: bool(result.j_table.isFailed()))
    until(lambda: stream_rows()[0]["State"] == "FAILED")
    assert "a symbol table holds 'msft'" in stream_rows()[0]["Error"]


def test_a_row_that_is_not_a_ticker_is_skipped_and_listed(
    massive_session: dm.MassiveSession, stream_rows: Rows, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL", "msft")
    result = follow(massive_session, wl, script)
    wait_rows(dh_to_pylist, result, {"AAPL": 2})
    until(lambda: "'msft'" in (stream_rows()[0]["Error"] or ""))
    delete(wl, "msft")
    until(lambda: stream_rows()[0]["Error"] is None)
    assert script.calls == [("AAPL",)]
    massive_session.release(result)


def test_a_defect_of_a_read_fails_the_table_naming_it(
    massive_session: dm.MassiveSession, stream_rows: Rows
) -> None:
    script = Script(massive_session)
    script.answers["AAPL"] = ZeroDivisionError("oops")
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    until(lambda: bool(result.j_table.isFailed()))
    until(lambda: stream_rows()[0]["State"] == "FAILED")
    error = stream_rows()[0]["Error"]
    assert "ZeroDivisionError: oops" in error
    assert "This is a bug in deephaven-massive" in error


# --- Lifetime ---------------------------------------------------------------------------------


def test_release_marks_the_row_released_and_stops_the_reads(
    massive_session: dm.MassiveSession, stream_rows: Rows, drain: Callable[[], None]
) -> None:
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    until(lambda: script.calls == [("AAPL",)])
    massive_session.release(result)
    del result
    until(script.stops[0].is_set)  # nothing uses the result: its reads stop
    gate.set()
    drain()
    (row,) = stream_rows()
    assert row["State"] == "RELEASED"
    until(lambda: massive_session._core.follows.following == 0)


def test_a_derived_table_keeps_the_result_following(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList, stream_rows: Rows
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    scope = new_scope()  # the user's own, released below
    with entered(massive_session._core.exec_ctx), scope.open():
        # Through Java: a formula from Python would hand this frame's locals to the JVM.
        derived = Table(j_table=result.j_table.where("Price >= 0"))
    massive_session.release(result)
    del result
    gc.collect()
    add(wl, "MSFT")
    wait_rows(dh_to_pylist, derived, {"AAPL": 2, "MSFT": 2})
    assert stream_rows()[0]["State"] == "RELEASED"
    del derived
    with shared_lock(massive_session._core.exec_ctx.update_graph):
        scope.release()
    until(lambda: massive_session._core.follows.following == 0)


def test_close_inside_a_console_command_fails_the_table_and_joins_nothing(
    massive_session: dm.MassiveSession,
    console_command: Callable[[Callable[[], None]], None],
    cycles_ended: Callable[[], None],
) -> None:
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    until(lambda: script.calls == [("AAPL",)])
    console_command(massive_session.close)
    cycles_ended()
    assert result.j_table.isFailed()
    gate.set()
    until(lambda: massive_session._core.follows.thread is None)


def test_the_shutdown_callback_makes_no_deephaven_call(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL")
    calls: list[str] = []
    stop_watch = followed_read.FollowedRead.stop_watch

    def watched(read: followed_read.FollowedRead) -> None:
        calls.append(threading.current_thread().name)
        stop_watch(read)

    monkeypatch.setattr(followed_read.FollowedRead, "stop_watch", watched)
    result = follow(massive_session, wl, script)
    massive_session.release(result)
    del result
    until(lambda: bool(calls))
    assert all("reaper" in name for name in calls)  # the watch's stop runs on the reaper


def test_a_failed_registration_undoes_everything(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL")

    def interrupted(*_args: object, **_kwargs: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(ResultRegistry, "track", interrupted)
    with pytest.raises(KeyboardInterrupt):
        follow(massive_session, wl, script)
    assert massive_session._core._workers == {}
    assert script.calls == []
    assert massive_session._core.follows.thread is None


def test_an_interrupt_while_the_watch_starts_releases_the_result(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch, drain: Callable[[], None]
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL")

    def interrupted(_self: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(SymbolWatch, "start", interrupted)
    with pytest.raises(KeyboardInterrupt):
        follow(massive_session, wl, script)
    drain()
    until(lambda: massive_session._core._workers == {})
    until(lambda: massive_session._core.follows.following == 0)
    assert script.calls == []


def test_a_closed_session_refuses_to_follow(massive_session: dm.MassiveSession) -> None:
    massive_session.close()
    with pytest.raises(MassiveError, match=r"is closed$"):
        follow(massive_session, watchlist("AAPL"), Script(massive_session))


def test_a_close_racing_the_registration_releases_the_result(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = massive_session._core
    track = ResultRegistry.track

    def track_then_close(registry: ResultRegistry, *args: object) -> ResultRecord:
        typed = cast("tuple[Table, LivenessScope, Callable[[], None]]", args)
        record = track(registry, *typed)
        massive_session.close()
        return record

    monkeypatch.setattr(ResultRegistry, "track", track_then_close)
    with pytest.raises(MassiveError, match=r"is closed$"):
        follow(massive_session, watchlist("AAPL"), Script(massive_session))
    assert core.follows.thread is None


def test_a_cell_is_never_published_after_the_table_failed(
    massive_session: dm.MassiveSession,
) -> None:
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    until(lambda: script.calls == [("AAPL",)])
    read = cast("Any", massive_session._core._workers[next(iter(massive_session._core._workers))])
    handle = read.fail.__self__
    assert handle.fail_table(MassiveError("stop now")) is True
    assert handle.fail_table(MassiveError("again")) is False
    with entered(massive_session._core.exec_ctx):
        handle.publish("AAPL", 99, rows_of(massive_session, "AAPL", 1))  # dropped
    gate.set()
    until(lambda: bool(result.j_table.isFailed()))


def test_the_router_drops_a_report_for_a_collected_result(
    massive_session: dm.MassiveSession,
) -> None:
    class Gone:
        pass

    gone = Gone()
    router = followed_read._WatchRouter(massive_session._core.follows, cast("Any", gone))
    router.finish(cast("Any", gone))
    del gone
    router.on_change(frozenset({"AAPL"}), frozenset())  # nothing to report to; no error


def test_a_failure_during_the_first_read_is_kept_for_the_coordinator(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList, drain: Callable[[], None]
) -> None:
    failure = SymbolFailure(
        MassiveError("HTTP 403: not in the plan", status=403), FailureKind.PERMANENT
    )
    first = FirstRead(
        {"AAPL": SymbolRows(rows_of(massive_session, "AAPL", 1)), "MSFT": failure}, 1, 0.1
    )
    script = Script(massive_session)
    wl = watchlist("AAPL", "MSFT")
    result = follow(massive_session, wl, script, first=first)
    assert counts(dh_to_pylist(result)) == {"AAPL": 1}
    drain()
    massive_session.release(result)
    assert script.calls == []


_ = (weakref, exclusive_lock, follow_coordinator)


def test_always_hides_an_old_download_before_its_empty_cell_lands(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The symbol leaves and returns while the empty cell that frees its rows is held back:
    # the old download is still the symbol's latest cell, and must not show again.
    script = Script(massive_session)
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    wait_rows(dh_to_pylist, result, {"AAPL": 2})
    held = threading.Event()
    publish = followed_read.FollowedRead.publish

    def holding(read: followed_read.FollowedRead, *cell: object) -> None:
        symbol, seq, table = cast("tuple[str, int, Table | None]", cell)
        if table is None:  # the empty cell of a removal
            assert held.wait(TIMEOUT)
        publish(read, symbol, seq, table)

    monkeypatch.setattr(followed_read.FollowedRead, "publish", holding)
    gate = script.gates["AAPL"] = threading.Event()  # and the new download waits too
    delete(wl, "AAPL")
    add(wl, "AAPL")  # the next cycle: the old cell is still the latest
    assert counts(dh_to_pylist(result)) == {}
    held.set()
    gate.set()
    wait_rows(dh_to_pylist, result, {"AAPL": 2})  # the new download shows
    massive_session.release(result)


def test_never_shows_its_kept_download_in_the_cycle_the_symbol_returns(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script, reread=Reread.NEVER)
    wait_rows(dh_to_pylist, result, {"AAPL": 2})
    delete(wl, "AAPL")
    add(wl, "AAPL")
    assert counts(dh_to_pylist(result)) == {"AAPL": 2}
    massive_session.release(result)


def test_an_add_remove_and_re_add_inside_one_cycle_changes_nothing(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList, drain: Callable[[], None]
) -> None:
    script = Script(massive_session)
    wl = cast("Any", watchlist("AAPL"))
    result = follow(massive_session, wl, script)
    wait_rows(dh_to_pylist, result, {"AAPL": 2})
    with exclusive_lock(massive_session._core.exec_ctx.update_graph):
        # All three land in the next cycle together: the table's rows end where they began.
        wl.delete_async(new_table([string_col("Symbol", ["AAPL"])]))
        wl.add_async(new_table([string_col("Symbol", ["AAPL"])]))
        wl.add_async(new_table([string_col("Symbol", ["MSFT"])]))
        wl.delete_async(new_table([string_col("Symbol", ["MSFT"])]))
    add(wl, "NVDA")  # a later edit: the cycle above has run once it lands
    wait_rows(dh_to_pylist, result, {"AAPL": 2, "NVDA": 2})
    drain()
    assert sorted(script.calls) == [("AAPL",), ("NVDA",)]  # neither AAPL nor MSFT read again
    massive_session.release(result)


def test_an_empty_table_s_row_counts_no_symbol(
    massive_session: dm.MassiveSession, stream_rows: Rows
) -> None:
    result = follow(massive_session, watchlist(), Script(massive_session))
    (row,) = stream_rows()
    assert (row["SymbolCount"], row["Symbols"], row["Loading"]) == (0, "", 0)
    massive_session.release(result)


class _Heard:
    # Stands in for the coordinator: records each report a router hands on, in order.
    def __init__(self) -> None:
        self.reports: list[tuple[str, object]] = []

    def baseline(self, _read: object, symbols: frozenset[str]) -> None:
        self.reports.append(("baseline", symbols))

    def changed(self, _read: object, added: frozenset[str], removed: frozenset[str]) -> None:
        self.reports.append(("changed", (added, removed)))

    def skipped(self, _read: object, text: str | None) -> None:
        self.reports.append(("skipped", text))

    def invalid(self, _read: object, message: str) -> None:
        self.reports.append(("invalid", message))


def test_the_router_hands_on_the_replay_first_then_every_report_in_order() -> None:
    # The listener reports from an update-graph thread; here a plain thread stands in for it.
    class Read:
        pass

    read = Read()
    heard = _Heard()
    router = followed_read._WatchRouter(cast("Any", heard), cast("Any", read))
    router.on_change(frozenset({"AAPL", "MSFT"}), frozenset())  # the replay, on this thread

    def listener() -> None:  # a change that comes before the replay is handed on
        router.on_change(frozenset({"NVDA"}), frozenset({"MSFT"}))
        router.on_invalid("a bad row")

    other = threading.Thread(target=listener, name="update graph stand-in")
    other.start()
    other.join(TIMEOUT)
    router.on_skipped(("msft",))
    router.finish(cast("Any", read))
    router.on_skipped(())  # handed on at once now
    assert heard.reports == [
        ("baseline", frozenset({"AAPL", "MSFT"})),
        ("changed", (frozenset({"NVDA"}), frozenset({"MSFT"}))),
        ("invalid", "a bad row"),
        ("skipped", skipped_rows_text(("msft",))),
        ("skipped", None),
    ]


def test_a_stopped_coordinator_drops_every_report(massive_session: dm.MassiveSession) -> None:
    coordinator = massive_session._core.follows
    coordinator.stop()
    read = cast("Any", object())
    coordinator.follow(read, None)  # the session closed: nothing starts
    coordinator.changed(read, frozenset({"AAPL"}), frozenset())
    coordinator.invalid(read, "a bad row")
    coordinator.skipped(read, None)
    coordinator.wake()
    assert coordinator.thread is None
    assert coordinator.following == 0


def test_never_shows_a_seeded_symbol_until_it_leaves(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL")
    result = follow(
        massive_session, wl, script, reread=Reread.NEVER, first=first_read(massive_session, AAPL=3)
    )
    assert counts(dh_to_pylist(result)) == {"AAPL": 3}
    delete(wl, "AAPL")
    assert counts(dh_to_pylist(result)) == {}
    add(wl, "AAPL")  # its seed cell is still its download: shown again, with no request
    assert counts(dh_to_pylist(result)) == {"AAPL": 3}
    assert script.calls == []
    massive_session.release(result)


def test_the_shutdown_callback_stops_following_and_tells_the_session(
    massive_session: dm.MassiveSession, drain: Callable[[], None]
) -> None:
    # Deephaven runs it on whichever thread shut the table down; here, on the test thread.
    class Read:
        def __init__(self) -> None:
            self.shut_down_event = threading.Event()
            self.watch_stopped = threading.Event()

        def stop_watch(self) -> None:
            self.watch_stopped.set()

    read = Read()
    stop = threading.Event()
    core = massive_session._core
    followed_read._shut_down(
        weakref.ref(core), weakref.ref(cast("Any", read)), call=12_345, stop=stop
    )
    assert stop.is_set()
    assert read.shut_down_event.is_set()
    drain()  # the watch's stop ran on the reaper
    assert read.watch_stopped.is_set()
