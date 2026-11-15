"""Tests for followed reads: the result's tables, its watch, its lifetime, and its failures.

Each test follows a real Deephaven symbol table with a scripted read (`Script`) in place of a
transport, so the engine's behaviour is seen exactly: which symbols are read, when rows land,
and what a removal, a release, a close, or a failure does to the table.
"""

from __future__ import annotations

import dataclasses
import gc
import logging
import re
import threading
import weakref
from collections.abc import Callable, Mapping
from types import MethodType
from typing import TYPE_CHECKING, Any

import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.stream.table_publisher import table_publisher
from deephaven.table import Table
from deephaven.table_listener import listen
from deephaven.update_graph import exclusive_lock, shared_lock

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._enums import AssetClass, NotBars, Reread
from deephaven_massive._errors import NEW_SESSION, AuthenticationError, MassiveError
from deephaven_massive._massive import Transport
from deephaven_massive._tables import follow_coordinator, followed_read
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.follow_coordinator import FirstRead
from deephaven_massive._tables.followed_read import FollowPlan, open_followed_read
from deephaven_massive._tables.liveness import (
    ResultRecord,
    ResultRegistry,
    new_scope,
    release_scope,
)
from deephaven_massive._tables.symbol_read_rows import SymbolReadState
from deephaven_massive._tables.transports import (
    SymbolFailure,
    SymbolOutcome,
    SymbolRows,
)
from deephaven_massive._tables.watchlist import SymbolRowRule, SymbolWatch, skipped_rows_text
from tests._fakes.followed_read import QuietRead

if TYPE_CHECKING:
    from deephaven.liveness_scope import LivenessScope
    from deephaven.table_factory import InputTable

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


@pytest.fixture
def watchlist(dh_input_table: Callable[..., InputTable]) -> Callable[..., InputTable]:
    # Makes a user's watchlist holding the symbols, keyed on `Symbol` unless `keyed=False`.
    def make(*symbols: str | None, keyed: bool = True) -> InputTable:
        table = dh_input_table({"Symbol": dtypes.string}, key_cols="Symbol" if keyed else None)
        if symbols:
            table.add(new_table([string_col("Symbol", list(symbols))]))
        return table

    return make


def add(table: InputTable, *symbols: str) -> None:
    table.add(new_table([string_col("Symbol", list(symbols))]))


def delete(table: InputTable, *symbols: str) -> None:
    table.delete(new_table([string_col("Symbol", list(symbols))]))


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
        Transport.FLAT_FILES if one_scan else Transport.REST,
        one_scan=one_scan,
        symbols_per_read=1,
        read=script.read,
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
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, dh_to_pylist: ToList
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
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL", "MSFT")
    result = follow(massive_session, wl, script, first=first_read(massive_session, AAPL=3, MSFT=1))
    assert counts(dh_to_pylist(result)) == {"AAPL": 3, "MSFT": 1}
    assert script.calls == []  # the seeded symbols are not read again
    massive_session.release(result)


def test_seeded_rows_are_there_inside_a_console_command(
    *,
    watchlist: Callable[..., InputTable],
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
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    wl = watchlist()
    result = follow(massive_session, wl, script)
    assert result.size == 0
    add(wl, "NVDA")
    wait_rows(dh_to_pylist, result, {"NVDA": 2})
    massive_session.release(result)


def test_a_removed_symbol_leaves_in_the_cycle_its_row_does(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL", "MSFT")
    result = follow(massive_session, wl, script)
    wait_rows(dh_to_pylist, result, {"AAPL": 2, "MSFT": 2})
    delete(wl, "MSFT")  # returns once the cycle that removed the row has run
    assert counts(dh_to_pylist(result)) == {"AAPL": 2}
    massive_session.release(result)


def test_a_seeded_symbol_removed_leaves_too(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, dh_to_pylist: ToList
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
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, dh_to_pylist: ToList
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
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script, reread=Reread.NEVER)
    wait_rows(dh_to_pylist, result, {"AAPL": 2})
    delete(wl, "AAPL")
    assert counts(dh_to_pylist(result)) == {}
    add(wl, "AAPL")  # the kept download shows in the cycle the row returns
    assert counts(dh_to_pylist(result)) == {"AAPL": 2}
    assert script.calls == [("AAPL",)]
    massive_session.release(result)


def test_null_and_duplicate_rows_count_once(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, dh_to_pylist: ToList
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
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, dh_to_pylist: ToList
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
    watchlist: Callable[..., InputTable],
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
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    monkeypatch: pytest.MonkeyPatch,
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
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    dh_to_pylist: ToList,
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
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, stream_rows: Rows
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
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    stream_rows: Rows,
    drain: Callable[[], None],
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
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    dh_to_pylist: ToList,
    stream_rows: Rows,
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
    *,
    watchlist: Callable[..., InputTable],
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
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
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
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL")

    def interrupted(*_args: object, **_kwargs: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(ResultRegistry, "track", interrupted)
    with pytest.raises(KeyboardInterrupt):
        follow(massive_session, wl, script)
    assert massive_session._core.workers._workers == {}
    assert script.calls == []
    assert massive_session._core.follows.thread is None


def test_an_interrupt_while_the_watch_starts_releases_the_result(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    drain: Callable[[], None],
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL")

    def interrupted(_self: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(SymbolWatch, "start", interrupted)
    with pytest.raises(KeyboardInterrupt):
        follow(massive_session, wl, script)
    drain()
    until(lambda: massive_session._core.workers._workers == {})
    until(lambda: massive_session._core.follows.following == 0)
    assert script.calls == []


def test_an_interrupt_after_the_blink_table_is_made_stops_no_watch(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    drain: Callable[[], None],
) -> None:
    # The build is undone after its blink table exists but before the watch does, as a
    # test timeout's signal or a Ctrl-C can do: the table shuts down before `attach` runs.
    script = Script(massive_session)
    wl = watchlist("AAPL")

    def interrupted(*_args: object, **_kwargs: object) -> Table:
        raise KeyboardInterrupt

    monkeypatch.setattr(followed_read, "describe", interrupted)
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive"):
        with pytest.raises(KeyboardInterrupt):
            follow(massive_session, wl, script)
        drain()  # the shutdown's work on the reaper has run
    assert [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING] == []
    assert "shut down before its watch was made; no watch to stop" in caplog.text
    assert massive_session._core.workers._workers == {}
    assert massive_session._core.follows.following == 0
    assert script.calls == []


def test_a_closed_session_refuses_to_follow(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession
) -> None:
    massive_session.close()
    with pytest.raises(MassiveError, match=r"is closed; " + re.escape(NEW_SESSION) + "$"):
        follow(massive_session, watchlist("AAPL"), Script(massive_session))


def test_a_close_racing_the_registration_releases_the_result(
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = massive_session._core
    track = ResultRegistry.track

    def track_then_close(
        registry: ResultRegistry, *args: *tuple[Table, LivenessScope, Callable[[], None]]
    ) -> ResultRecord:
        record = track(registry, *args)
        massive_session.close()
        return record

    monkeypatch.setattr(ResultRegistry, "track", track_then_close)
    with pytest.raises(MassiveError, match=r"is closed; " + re.escape(NEW_SESSION) + "$"):
        follow(massive_session, watchlist("AAPL"), Script(massive_session))
    assert core.follows.thread is None


def test_a_download_is_never_published_after_the_table_failed(
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
) -> None:
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    until(lambda: script.calls == [("AAPL",)])
    fail = massive_session._core.workers._workers[
        next(iter(massive_session._core.workers._workers))
    ].fail
    assert isinstance(fail, MethodType)
    handle = fail.__self__
    assert isinstance(handle, followed_read.FollowedRead)
    assert handle.fail_table(MassiveError("stop now")) is True
    assert handle.fail_table(MassiveError("again")) is False
    with entered(massive_session._core.exec_ctx):
        rows = rows_of(massive_session, "AAPL", 1)
        assert handle.publish("AAPL", 99, rows, decided_at_step=0, parts_before=2) == 2  # dropped
    gate.set()
    until(lambda: bool(result.j_table.isFailed()))


def test_the_router_drops_a_report_for_a_collected_result(
    massive_session: dm.MassiveSession,
) -> None:
    gone = QuietRead()
    router = followed_read._WatchRouter(massive_session._core.follows, gone, lambda: 1)
    router.finish(gone)
    del gone
    router.on_change(frozenset({"AAPL"}), frozenset())  # nothing to report to; no error


def test_a_failure_during_the_first_read_is_kept_for_the_coordinator(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    dh_to_pylist: ToList,
    drain: Callable[[], None],
) -> None:
    failure = SymbolFailure(MassiveError("HTTP 403: not in the plan", status=403))
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


def test_always_hides_an_old_download_before_its_empty_download_lands(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    dh_to_pylist: ToList,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The symbol leaves and returns while the empty download that frees its rows is held
    # back: the old download is still the symbol's latest, and must not show again.
    script = Script(massive_session)
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    wait_rows(dh_to_pylist, result, {"AAPL": 2})
    held = threading.Event()
    publish = followed_read.FollowedRead.publish

    def holding(
        read: followed_read.FollowedRead,
        *download: *tuple[str, int, Table | None],
        decided_at_step: int,
        parts_before: int,
    ) -> int:
        symbol, seq, table = download
        if table is None:  # the empty download of a removal
            assert held.wait(TIMEOUT)
        return publish(
            read, symbol, seq, table, decided_at_step=decided_at_step, parts_before=parts_before
        )

    monkeypatch.setattr(followed_read.FollowedRead, "publish", holding)
    gate = script.gates["AAPL"] = threading.Event()  # and the new download waits too
    delete(wl, "AAPL")
    add(wl, "AAPL")  # the next cycle: the old download is still the latest
    assert counts(dh_to_pylist(result)) == {}
    held.set()
    gate.set()
    wait_rows(dh_to_pylist, result, {"AAPL": 2})  # the new download shows
    massive_session.release(result)


def test_always_never_shows_a_download_published_after_its_symbol_left_and_returned(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    dh_to_pylist: ToList,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The coordinator has taken the first read's outcome and is publishing it when the symbol
    # leaves and returns: the removal and the re-add queue behind the outcome, so the stale download
    # lands after the re-add, and must not show as the returning symbol's rows.
    script = Script(massive_session)
    wl = watchlist()
    result = follow(massive_session, wl, script)
    first_gate = script.gates["AAPL"] = threading.Event()
    publishing, go, published, freed = (threading.Event() for _ in range(4))
    publish = followed_read.FollowedRead.publish

    def slow(
        read: followed_read.FollowedRead,
        *download: *tuple[str, int, Table | None],
        decided_at_step: int,
        parts_before: int,
    ) -> int:
        symbol, seq, table = download
        if table is None:  # the removal's empty row waits, so the stale row is latest
            assert freed.wait(TIMEOUT)
        elif not publishing.is_set():  # the first download
            publishing.set()
            assert go.wait(TIMEOUT)
        parts = publish(
            read, symbol, seq, table, decided_at_step=decided_at_step, parts_before=parts_before
        )
        published.set()
        return parts

    monkeypatch.setattr(followed_read.FollowedRead, "publish", slow)
    add(wl, "AAPL")
    until(lambda: script.calls == [("AAPL",)])
    reread = script.gates["AAPL"] = threading.Event()  # the read after the re-add waits
    first_gate.set()
    assert publishing.wait(TIMEOUT)  # the coordinator checked the outcome and publishes it
    delete(wl, "AAPL")  # a cycle the coordinator has not heard of yet
    add(wl, "AAPL")  # and the re-add, a later one
    go.set()
    assert published.wait(TIMEOUT)
    add(wl, "MSFT")  # later cycles: the old download has landed
    add(wl, "NVDA")
    assert "AAPL" not in counts(dh_to_pylist(result))
    freed.set()
    reread.set()
    wait_rows(dh_to_pylist, result, {"AAPL": 2, "MSFT": 2, "NVDA": 2})  # the new one shows
    massive_session.release(result)


def test_always_reads_a_seeded_symbol_that_left_and_returned_while_the_result_was_made(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    dh_to_pylist: ToList,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Another thread deletes and re-adds the seeded symbol between the build and the watch's
    # start. Were a cycle to run in that gap, its seed row would be hidden as older than its
    # return, and the watch, starting after both, would see no change to read it again for.
    script = Script(massive_session)
    wl = watchlist("AAPL")
    exec_ctx = massive_session._core.exec_ctx
    lock = exec_ctx.update_graph.j_update_graph.sharedLock()
    hand_over = follow_coordinator.FollowCoordinator.follow
    editors: list[threading.Thread] = []

    def edit() -> None:
        with entered(exec_ctx):
            delete(wl, "AAPL")
            add(wl, "AAPL")

    def edits_meanwhile(
        coordinator: follow_coordinator.FollowCoordinator,
        *handed: *tuple[followed_read.FollowedRead, FirstRead | None],
    ) -> None:
        editor = threading.Thread(target=edit)
        editor.start()
        editors.append(editor)
        if not lock.isHeldByCurrentThread():  # cycles may run: the edits land in the gap
            editor.join(TIMEOUT)
        hand_over(coordinator, *handed)

    monkeypatch.setattr(follow_coordinator.FollowCoordinator, "follow", edits_meanwhile)
    result = follow(massive_session, wl, script, first=first_read(massive_session, AAPL=3))
    (editor,) = editors
    editor.join(TIMEOUT)
    wait_rows(dh_to_pylist, result, {"AAPL": 2})  # read again, as a returning symbol
    until(lambda: script.calls == [("AAPL",)])
    massive_session.release(result)


def test_never_shows_its_kept_download_in_the_cycle_the_symbol_returns(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, dh_to_pylist: ToList
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
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    dh_to_pylist: ToList,
    drain: Callable[[], None],
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL")
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
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, stream_rows: Rows
) -> None:
    result = follow(massive_session, watchlist(), Script(massive_session))
    (row,) = stream_rows()
    assert (row["SymbolCount"], row["Symbols"], row["Loading"]) == (0, "", 0)
    massive_session.release(result)


class _Heard:
    # Stands in for the coordinator: records each report a router hands on, in order.
    def __init__(self) -> None:
        self.reports: list[tuple[str, object]] = []

    def baseline(self, _read: object, symbols: frozenset[str], step: int) -> None:
        self.reports.append(("baseline", (symbols, step)))

    def changed(
        self, _read: object, added: frozenset[str], removed: frozenset[str], *, step: int
    ) -> None:
        self.reports.append(("changed", (added, removed, step)))

    def skipped(self, _read: object, text: str | None) -> None:
        self.reports.append(("skipped", text))

    def invalid(self, _read: object, message: str) -> None:
        self.reports.append(("invalid", message))


def test_the_router_hands_on_the_replay_first_then_every_report_in_order() -> None:
    # The listener reports from an update-graph thread; here a plain thread stands in for it.
    read = QuietRead()
    heard = _Heard()
    step = [7]  # the update cycle the update graph's clock shows
    router = followed_read._WatchRouter(heard, read, lambda: step[0])
    router.on_change(frozenset({"AAPL", "MSFT"}), frozenset())  # the replay, on this thread
    step[0] = 8

    def listener() -> None:  # a change that comes before the replay is handed on
        router.on_change(frozenset({"NVDA"}), frozenset({"MSFT"}))
        router.on_invalid("a bad row")

    other = threading.Thread(target=listener, name="update graph stand-in")
    other.start()
    other.join(TIMEOUT)
    router.on_skipped(("msft",))
    router.finish(read)
    router.on_skipped(())  # handed on at once now
    assert heard.reports == [
        ("baseline", (frozenset({"AAPL", "MSFT"}), 7)),
        ("changed", (frozenset({"NVDA"}), frozenset({"MSFT"}), 8)),
        ("invalid", "a bad row"),
        ("skipped", skipped_rows_text(("msft",))),
        ("skipped", None),
    ]


def test_a_stopped_coordinator_drops_every_report(massive_session: dm.MassiveSession) -> None:
    coordinator = massive_session._core.follows
    coordinator.stop()
    read = QuietRead()  # never followed
    coordinator.follow(read, None)  # the session closed: nothing starts
    coordinator.changed(read, frozenset({"AAPL"}), frozenset(), step=1)
    coordinator.invalid(read, "a bad row")
    coordinator.skipped(read, None)
    coordinator.wake()
    assert coordinator.thread is None
    assert coordinator.following == 0


def test_a_newer_download_replaces_a_seeded_symbol_s_rows(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    dh_to_pylist: ToList,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The symbol stays in the table, so only the seed's mask keeps its seed rows from showing
    # beside the newer download.
    handed: list[followed_read.FollowedRead] = []
    hand_over = follow_coordinator.FollowCoordinator.follow

    def keeps(
        coordinator: follow_coordinator.FollowCoordinator,
        read: followed_read.FollowedRead,
        first: FirstRead | None,
    ) -> None:
        handed.append(read)
        hand_over(coordinator, read, first)

    monkeypatch.setattr(follow_coordinator.FollowCoordinator, "follow", keeps)
    result = follow(
        massive_session,
        watchlist("AAPL"),
        Script(massive_session),
        first=first_read(massive_session, AAPL=3),
    )
    assert counts(dh_to_pylist(result)) == {"AAPL": 3}
    (read,) = handed
    exec_ctx = massive_session._core.exec_ctx
    step = int(exec_ctx.update_graph.j_update_graph.clock().currentStep())
    with entered(exec_ctx):
        read.publish(
            "AAPL", 1, rows_of(massive_session, "AAPL", 5), decided_at_step=step, parts_before=0
        )
    wait_rows(dh_to_pylist, result, {"AAPL": 5})
    massive_session.release(result)


def test_never_shows_a_seeded_symbol_until_it_leaves(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL")
    result = follow(
        massive_session, wl, script, reread=Reread.NEVER, first=first_read(massive_session, AAPL=3)
    )
    assert counts(dh_to_pylist(result)) == {"AAPL": 3}
    delete(wl, "AAPL")
    assert counts(dh_to_pylist(result)) == {}
    add(wl, "AAPL")  # its seed row groups are still its download: shown again, with no request
    assert counts(dh_to_pylist(result)) == {"AAPL": 3}
    assert script.calls == []
    massive_session.release(result)


def test_the_shutdown_callback_stops_following_and_tells_the_session(
    massive_session: dm.MassiveSession, drain: Callable[[], None]
) -> None:
    # Deephaven runs it on whichever thread shut the table down; here, on the test thread.
    class Read(QuietRead):
        def __init__(self) -> None:
            super().__init__()
            self.shut_down_event = threading.Event()
            self.watch_stopped = threading.Event()

        def stop_watch(self) -> None:
            self.watch_stopped.set()

    read = Read()
    core = massive_session._core
    followed_read._shut_down(
        weakref.ref(core),
        weakref.ref(read),
        worker_shut_down=core.workers.shutdown_callback(12_345, read.stop_token),
    )
    assert read.stop_token.is_set()
    assert read.shut_down_event.is_set()
    drain()  # the watch's stop ran on the reaper
    assert read.watch_stopped.is_set()


# --- The result is an ordinary table ------------------------------------------------------------


OPERATIONS: dict[str, Callable[[Table], Table]] = {
    "view": lambda t: t.view(["Ticker", "Price"]),
    "update_view": lambda t: t.update_view("Twice = Price * 2"),
    "where then view": lambda t: t.where("Price >= 0").view(["Ticker"]),
    "select": lambda t: t.select(["Ticker"]),
    "update": lambda t: t.update("Twice = Price * 2"),
    "sort": lambda t: t.sort_descending("Price"),
    "natural_join": lambda t: t.natural_join(t.last_by("Ticker"), on="Ticker", joins="Last=Price"),
}
"""Ordinary operations a user calls on a result, by test id; each keeps every row's `Ticker`."""


@pytest.mark.jvm_held  # the operations' formulas hand this frame's locals to the JVM
@pytest.mark.parametrize("seeded", [False, True], ids=["wait=False", "wait=True"])
def test_every_ordinary_operation_works_on_the_result_as_symbols_come_and_go(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    dh_to_pylist: ToList,
    seeded: bool,
) -> None:
    # `view` and `update_view` refuse a stateful column, which every column of a merged
    # partitioned table is while it may gain partitions: the result must not be one.
    script = Script(massive_session)
    wl = watchlist("AAPL")
    first = first_read(massive_session, AAPL=2) if seeded else None
    result = follow(massive_session, wl, script, first=first)
    scope = new_scope()  # the user's own scope, released below
    with scope.open():
        derived = {name: operation(result) for name, operation in OPERATIONS.items()}
    wait_rows(dh_to_pylist, result, {"AAPL": 2})
    add(wl, "MSFT")
    wait_rows(dh_to_pylist, result, {"AAPL": 2, "MSFT": 2})
    delete(wl, "AAPL")
    for name, table in derived.items():
        assert counts(dh_to_pylist(table)) == {"MSFT": 2}, name
    derived.clear()
    release_scope(massive_session._core.exec_ctx, scope)
    massive_session.release(result)


def test_a_symbol_of_many_slices_lands_whole_and_a_smaller_download_replaces_it(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    dh_to_pylist: ToList,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Each row group holds at most GROUP_ROWS rows: a symbol of several lands whole, and each
    # later download, of fewer parts or none, shows only its own rows.
    monkeypatch.setattr(followed_read, "GROUP_ROWS", 2)
    script = Script(massive_session)
    script.sizes = {"AAPL": 5}
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script, first=first_read(massive_session, AAPL=3))
    assert counts(dh_to_pylist(result)) == {"AAPL": 3}
    delete(wl, "AAPL")
    add(wl, "AAPL")
    wait_rows(dh_to_pylist, result, {"AAPL": 5})
    assert sorted(row["Price"] for row in dh_to_pylist(result)) == [0.0, 1.0, 2.0, 3.0, 4.0]
    script.sizes["AAPL"] = 1
    delete(wl, "AAPL")
    add(wl, "AAPL")
    wait_rows(dh_to_pylist, result, {"AAPL": 1})
    script.sizes["AAPL"] = 0
    delete(wl, "AAPL")
    add(wl, "AAPL")
    until(lambda: script.read_of("AAPL") == 3)
    add(wl, "MSFT")
    wait_rows(dh_to_pylist, result, {"MSFT": 2})  # AAPL's empty download shows no rows
    massive_session.release(result)


@pytest.mark.parametrize(
    ("size", "parts_before", "expected"),
    [
        (5, 0, [2, 2, 1]),
        (2, 3, [2, None, None]),  # the earlier download's parts 1 and 2 are freed
        (0, 2, [None, None]),
        (0, 0, [None]),  # a symbol with no rows has a part 0, which the seed's mask reads
    ],
    ids=["slices", "fewer parts than before", "empty after two parts", "empty"],
)
def test_a_download_s_row_groups_free_every_earlier_part(
    *,
    massive_session: dm.MassiveSession,
    dh_to_pylist: ToList,
    monkeypatch: pytest.MonkeyPatch,
    size: int,
    parts_before: int,
    expected: list[int | None],
) -> None:
    monkeypatch.setattr(followed_read, "GROUP_ROWS", 2)
    schema = massive_session._core.dataset_schema(SPEC)
    with entered(massive_session._core.exec_ctx):
        rows = followed_read._viewed(rows_of(massive_session, "AAPL", size), schema.column_names)
        groups, parts = followed_read._row_groups(
            "AAPL",
            7,
            rows,
            decided_at_step=3,
            parts_before=parts_before,
            empty=followed_read._empty_row_group(schema),
        )
    found = dh_to_pylist(groups)
    assert parts == sum(count is not None for count in expected)
    assert [row["__MassivePart"] for row in found] == list(range(len(expected)))
    assert {
        (row["__MassiveKey"], row["__MassiveSeq"], row["__MassiveDecidedAtStep"]) for row in found
    } == {("AAPL", 7, 3)}
    assert [None if row["Price"] is None else len(row["Price"]) for row in found] == expected


def test_a_removal_frees_every_part_of_the_symbol_s_download(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    dh_to_pylist: ToList,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The coordinator keeps each symbol's count of parts, so the removal's empty download sends
    # an empty row group for every part of the download it replaces.
    monkeypatch.setattr(followed_read, "GROUP_ROWS", 2)
    sent: list[tuple[bool, int, int]] = []
    publish = followed_read.FollowedRead.publish

    def counting(
        read: followed_read.FollowedRead,
        *download: *tuple[str, int, Table | None],
        decided_at_step: int,
        parts_before: int,
    ) -> int:
        symbol, seq, table = download
        parts = publish(
            read, symbol, seq, table, decided_at_step=decided_at_step, parts_before=parts_before
        )
        sent.append((table is None, parts_before, parts))
        return parts

    monkeypatch.setattr(followed_read.FollowedRead, "publish", counting)
    script = Script(massive_session)
    script.sizes = {"AAPL": 5}
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    wait_rows(dh_to_pylist, result, {"AAPL": 5})
    delete(wl, "AAPL")
    until(lambda: len(sent) == 2)
    assert sent == [(False, 0, 3), (True, 3, 0)]
    massive_session.release(result)
