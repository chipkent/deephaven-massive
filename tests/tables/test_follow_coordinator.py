"""Tests for the follow coordinator: each symbol's state, its retries, and the diagnostics rows.

Each test follows a real Deephaven symbol table with a scripted read in place of a transport,
and reads what the coordinator decided from `m.symbol_reads()` and `m.streams()`. Retries run
on a virtual clock (`tests/_fakes/clock.py`): a test moves it and wakes the coordinator, so
no test waits for real minutes.
"""

from __future__ import annotations

import dataclasses
import functools
import logging
import threading
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast

import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, input_table, new_table
from deephaven.column import string_col
from deephaven.table import Table
from deephaven.update_graph import exclusive_lock

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._enums import AssetClass, NotBars, Reread
from deephaven_massive._errors import MassiveError
from deephaven_massive._massive import Transport
from deephaven_massive._tables import follow_coordinator
from deephaven_massive._tables.diagnostics import (
    FLAT_FILES_AND_REST,
    SymbolReadRow,
    SymbolReadState,
)
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.follow_coordinator import RETRY_DELAYS, FirstRead
from deephaven_massive._tables.followed_read import FollowPlan, open_followed_read
from deephaven_massive._tables.transports import (
    FailureKind,
    SymbolFailure,
    SymbolOutcome,
    SymbolRows,
)
from deephaven_massive._tables.watchlist import SymbolRowRule
from tests._fakes.clock import FakeClock

TIMEOUT = 30.0
LOGGER = "deephaven_massive._tables.follow_coordinator"

SPEC = DatasetSpec(
    name="test.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    channels={NotBars.ANY: "T"},
)

ToList = Callable[[Table], list[dict[str, Any]]]
Answer = SymbolOutcome | BaseException


def until(condition: Callable[[], bool]) -> None:
    # Waits for the coordinator to catch up; fails after TIMEOUT.
    pause = threading.Event()
    for _ in range(int(TIMEOUT / 0.01)):
        if condition():
            return
        pause.wait(0.01)
    assert condition()


def rows_of(m: dm.MassiveSession, symbol: str, count: int) -> Table:
    schema = m._core.dataset_schema(SPEC)
    rows = [{"Ticker": symbol, "Price": float(i), "TimestampInsert": 1} for i in range(count)]
    with entered(m._core.exec_ctx):
        return dh_arrow.to_table(schema.to_arrow(rows))


def refused(status: int, text: str = "refused") -> SymbolFailure:
    kind = FailureKind.TRANSIENT if status in (429, 500, 503) else FailureKind.PERMANENT
    return SymbolFailure(MassiveError(f"HTTP {status}: {text}", status=status), kind)


class Script:
    # Stands in for a transport's read by symbol. Each symbol's answers are used in turn (the
    # last one again and again); a symbol without any gets 2 rows. A closed gate holds its read.
    def __init__(self, m: dm.MassiveSession) -> None:
        self.m = m
        self.calls: list[tuple[str, ...]] = []
        self.gates: dict[str, threading.Event] = {}
        # Gates of single reads: each read of a symbol takes the next one, if any.
        self.holds: dict[str, list[threading.Event]] = {}
        self.answers: dict[str, list[Answer]] = {}
        self._lock = threading.Lock()

    def read(self, names: tuple[str, ...], stop: threading.Event) -> Mapping[str, SymbolOutcome]:
        del stop
        with self._lock:
            self.calls.append(names)
        for name in names:
            gate = self.gates.get(name)
            with self._lock:
                held = self.holds.get(name, [])
                hold = held.pop(0) if held else None
            for event in (gate, hold):
                if event is not None:
                    assert event.wait(TIMEOUT)
        outcomes: dict[str, SymbolOutcome] = {}
        for name in names:
            with self._lock:
                queue = self.answers.get(name, [])
                answer = queue.pop(0) if len(queue) > 1 else (queue[0] if queue else None)
            if isinstance(answer, BaseException):
                raise answer
            outcomes[name] = answer or SymbolRows(rows_of(self.m, name, 2))
        return outcomes

    def read_count_is(self, name: str, count: int) -> bool:
        return self.reads_of(name) == count

    def reads_of(self, name: str) -> int:
        with self._lock:
            return sum(name in call for call in self.calls)


def watchlist(*symbols: str) -> Table:
    table = input_table({"Symbol": dtypes.string}, key_cols="Symbol")
    if symbols:
        add(table, *symbols)
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
        note="Follows a test.",
        first=first,
    )


def reads(m: dm.MassiveSession) -> dict[tuple[int, str], SymbolReadRow]:
    # The latest m.symbol_reads() row of each (result, symbol), as the core recorded it.
    with m._core.registry_lock:
        return dict(m._core._symbol_reads)


def row(m: dm.MassiveSession, symbol: str) -> SymbolReadRow:
    (found,) = [r for (_, name), r in reads(m).items() if name == symbol]
    return found


def state(m: dm.MassiveSession, symbol: str) -> SymbolReadState | None:
    found = [r.state for (_, name), r in reads(m).items() if name == symbol]
    return found[0] if found else None


def generation(m: dm.MassiveSession, symbol: str) -> int:
    # The symbol's generation in the coordinator's own state: one per add and removal seen.
    found = [
        result.symbols[symbol].generation
        for result in list(m._core.follows._results.values())
        if symbol in result.symbols
    ]
    return found[0] if found else 0


def wait_state(m: dm.MassiveSession, symbol: str, expected: SymbolReadState) -> SymbolReadRow:
    until(lambda: state(m, symbol) is expected)
    return row(m, symbol)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    fake = FakeClock(manual=True)
    monkeypatch.setattr(follow_coordinator, "CLOCK", fake)
    return fake


def stream(m: dm.MassiveSession, drain: Callable[[], None], to_list: ToList) -> dict[str, Any]:
    drain()
    (found,) = to_list(m.streams())
    return found


# --- A symbol's states ------------------------------------------------------------------------


def test_a_symbol_goes_queued_loading_done(
    *,
    massive_session: dm.MassiveSession,
    clock: FakeClock,
    drain: Callable[[], None],
    dh_to_pylist: ToList,
) -> None:
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    hold = script.gates["MSFT"] = threading.Event()
    wl = watchlist("MSFT")
    result = follow(massive_session, wl, script)
    until(lambda: script.calls == [("MSFT",)])
    add(wl, "AAPL")  # the pool has room: queued, then loading
    loading = wait_state(massive_session, "AAPL", SymbolReadState.LOADING)
    assert loading.attempts == 1
    assert loading.read_at is None
    gate.set()
    done = wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    assert (done.rows, done.error, done.transport) == (2, None, Transport.REST)
    assert done.read_at == done.added_at == int(clock.time() * 1e9)
    drain()
    shown = {r["Symbol"]: r for r in dh_to_pylist(massive_session.symbol_reads())}
    assert shown["AAPL"]["State"] == "DONE"
    assert shown["AAPL"]["Transport"] == "REST"
    assert shown["MSFT"]["State"] == "LOADING"
    hold.set()
    wait_state(massive_session, "MSFT", SymbolReadState.DONE)
    massive_session.release(result)


def test_a_symbol_waits_queued_while_the_pool_is_busy(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(massive_session._core.follows, "_max_workers", 1)
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    until(lambda: script.calls == [("AAPL",)])
    add(wl, "MSFT")
    assert wait_state(massive_session, "MSFT", SymbolReadState.QUEUED).attempts == 0
    gate.set()
    wait_state(massive_session, "MSFT", SymbolReadState.DONE)
    massive_session.release(result)


def test_no_data_is_done_with_no_rows(massive_session: dm.MassiveSession) -> None:
    script = Script(massive_session)
    script.answers["ZZZZ"] = [SymbolRows(rows_of(massive_session, "ZZZZ", 0))]
    result = follow(massive_session, watchlist("ZZZZ"), script)
    done = wait_state(massive_session, "ZZZZ", SymbolReadState.DONE)
    assert (done.rows, done.error) == (0, None)
    massive_session.release(result)


def test_a_permanent_failure_fails_only_its_symbol(
    massive_session: dm.MassiveSession,
    caplog: pytest.LogCaptureFixture,
    dh_to_pylist: ToList,
) -> None:
    script = Script(massive_session)
    script.answers["AAPL"] = [refused(403, "not in the plan (request_id abc)")]
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        result = follow(massive_session, watchlist("AAPL", "MSFT"), script)
        failed = wait_state(massive_session, "AAPL", SymbolReadState.FAILED)
        wait_state(massive_session, "MSFT", SymbolReadState.DONE)
    assert failed.error == "HTTP 403: not in the plan (request_id abc)"
    assert (failed.attempts, failed.next_attempt_at, failed.rows) == (1, None, 0)
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "AAPL failed after 1 attempts (permanent)" in warnings[0]
    until(lambda: {r["Ticker"] for r in dh_to_pylist(result)} == {"MSFT"})
    massive_session.release(result)


def test_a_transient_failure_retries_after_1_5_and_15_minutes_then_fails(
    massive_session: dm.MassiveSession, clock: FakeClock
) -> None:
    assert RETRY_DELAYS == (60.0, 300.0, 900.0)
    script = Script(massive_session)
    script.answers["AAPL"] = [refused(503, "unavailable")]
    result = follow(massive_session, watchlist("AAPL"), script)
    for attempt, delay in enumerate(RETRY_DELAYS, start=1):
        retrying = wait_state(massive_session, "AAPL", SymbolReadState.RETRYING)
        assert retrying.attempts == attempt
        assert retrying.next_attempt_at == int((clock.time() + delay) * 1e9)
        assert retrying.error == "HTTP 503: unavailable"
        clock.advance(delay - 1)
        massive_session._core.follows.wake()
        assert script.reads_of("AAPL") == attempt  # not due yet
        clock.advance(1)
        massive_session._core.follows.wake()
        until(functools.partial(script.read_count_is, "AAPL", attempt + 1))
    failed = wait_state(massive_session, "AAPL", SymbolReadState.FAILED)
    assert (failed.attempts, failed.next_attempt_at) == (4, None)
    massive_session.release(result)


def test_a_retry_that_succeeds_clears_the_error(
    massive_session: dm.MassiveSession, clock: FakeClock
) -> None:
    script = Script(massive_session)
    script.answers["AAPL"] = [
        refused(429, "too many requests"),
        SymbolRows(rows_of(massive_session, "AAPL", 3)),
    ]
    result = follow(massive_session, watchlist("AAPL"), script)
    wait_state(massive_session, "AAPL", SymbolReadState.RETRYING)
    clock.advance(RETRY_DELAYS[0])
    massive_session._core.follows.wake()
    done = wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    assert (done.error, done.rows, done.attempts) == (None, 3, 2)
    massive_session.release(result)


def test_a_retry_wakes_the_coordinator_by_itself(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # On the real clock a due retry needs no event: the coordinator's wait ends at it.
    script = Script(massive_session)
    script.answers["AAPL"] = [refused(500, "boom"), SymbolRows(rows_of(massive_session, "AAPL", 1))]
    monkeypatch.setattr(follow_coordinator, "RETRY_DELAYS", (0.05, 0.05, 0.05))
    result = follow(massive_session, watchlist("AAPL"), script)
    wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    massive_session.release(result)


def test_a_removal_cancels_a_pending_retry(
    massive_session: dm.MassiveSession, clock: FakeClock
) -> None:
    script = Script(massive_session)
    script.answers["AAPL"] = [refused(503)]
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    wait_state(massive_session, "AAPL", SymbolReadState.RETRYING)
    delete(wl, "AAPL")
    removed = wait_state(massive_session, "AAPL", SymbolReadState.REMOVED)
    assert removed.next_attempt_at is None
    clock.advance(3600)
    massive_session._core.follows.wake()
    add(wl, "MSFT")
    wait_state(massive_session, "MSFT", SymbolReadState.DONE)
    assert script.reads_of("AAPL") == 1
    massive_session.release(result)


def test_a_removal_cancels_a_queued_read(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(massive_session._core.follows, "_max_workers", 1)
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script, one_scan=True)
    until(lambda: script.calls == [("AAPL",)])
    add(wl, "MSFT")
    wait_state(massive_session, "MSFT", SymbolReadState.QUEUED)
    delete(wl, "MSFT")
    wait_state(massive_session, "MSFT", SymbolReadState.REMOVED)
    gate.set()
    wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    assert script.calls == [("AAPL",)]
    massive_session.release(result)


# --- Re-adds and stale outcomes ---------------------------------------------------------------


def test_a_removal_during_a_read_drops_its_outcome(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    wl = watchlist("AAPL", "MSFT")
    result = follow(massive_session, wl, script)
    until(lambda: script.reads_of("AAPL") == 1)
    delete(wl, "AAPL")
    wait_state(massive_session, "AAPL", SymbolReadState.REMOVED)
    gate.set()
    until(lambda: {r["Ticker"] for r in dh_to_pylist(result)} == {"MSFT"})
    assert state(massive_session, "AAPL") is SymbolReadState.REMOVED
    massive_session.release(result)


def test_a_re_add_during_a_read_waits_for_it_and_reads_again(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    first, second = threading.Event(), threading.Event()
    script.holds["AAPL"] = [first, second]
    script.answers["AAPL"] = [
        SymbolRows(rows_of(massive_session, "AAPL", 1)),
        SymbolRows(rows_of(massive_session, "AAPL", 5)),
    ]
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    until(lambda: script.reads_of("AAPL") == 1)
    delete(wl, "AAPL")
    add(wl, "AAPL")
    wait_state(massive_session, "AAPL", SymbolReadState.QUEUED)
    assert script.reads_of("AAPL") == 1  # never two reads of one symbol at once
    first.set()  # the overtaken read ends: its rows are dropped, and the second read starts
    until(lambda: script.reads_of("AAPL") == 2)
    loading = wait_state(massive_session, "AAPL", SymbolReadState.LOADING)
    assert (loading.rows, loading.read_at) == (0, None)
    assert dh_to_pylist(result) == []
    second.set()
    until(lambda: len(dh_to_pylist(result)) == 5)
    massive_session.release(result)


def test_five_add_remove_cycles_before_the_first_read_land_one_read(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    until(lambda: script.reads_of("AAPL") == 1)
    for _ in range(5):
        delete(wl, "AAPL")
        add(wl, "AAPL")
    # The table's changes reach the coordinator after the edits return: wait for all ten.
    until(lambda: generation(massive_session, "AAPL") == 11)
    gate.set()
    done = wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    assert script.reads_of("AAPL") == 2  # the first, dropped, and one final read
    assert done.attempts == 1
    assert len(dh_to_pylist(result)) == 2
    massive_session.release(result)


def test_a_failed_symbol_is_read_again_on_re_add_under_both_members(
    massive_session: dm.MassiveSession,
) -> None:
    for reread in Reread:
        script = Script(massive_session)
        script.answers["AAPL"] = [refused(403), SymbolRows(rows_of(massive_session, "AAPL", 1))]
        wl = watchlist("AAPL")
        result = follow(massive_session, wl, script, reread=reread)
        until(functools.partial(_latest_is, massive_session, SymbolReadState.FAILED))
        delete(wl, "AAPL")
        add(wl, "AAPL")
        until(functools.partial(_latest_is, massive_session, SymbolReadState.DONE))
        assert script.reads_of("AAPL") == 2
        massive_session.release(result)


def _latest_is(m: dm.MassiveSession, expected: SymbolReadState) -> bool:
    # Whether the AAPL row of the newest result (the largest StreamId) is in `expected`.
    rows = [r for (_, name), r in reads(m).items() if name == "AAPL"]
    return bool(rows) and max(rows, key=lambda r: r.stream_id).state is expected


def test_never_reuses_a_download_without_a_request(
    massive_session: dm.MassiveSession, clock: FakeClock
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script, reread=Reread.NEVER)
    done = wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    clock.advance(3600)
    delete(wl, "AAPL")
    removed = wait_state(massive_session, "AAPL", SymbolReadState.REMOVED)
    assert removed.rows == 0  # its rows left the result; the download is kept
    add(wl, "AAPL")
    back = wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    assert (back.read_at, back.rows, back.attempts) == (done.read_at, done.rows, done.attempts)
    assert back.read_at is not None
    assert back.read_at < back.added_at
    assert script.reads_of("AAPL") == 1
    massive_session.release(result)


def test_always_frees_a_removed_download_and_reads_it_again(
    massive_session: dm.MassiveSession, clock: FakeClock
) -> None:
    script = Script(massive_session)
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    done = wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    delete(wl, "AAPL")
    removed = wait_state(massive_session, "AAPL", SymbolReadState.REMOVED)
    assert (removed.rows, removed.read_at) == (0, None)
    clock.advance(60)
    add(wl, "AAPL")
    until(lambda: script.reads_of("AAPL") == 2)
    again = wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    assert again.read_at is not None
    assert done.read_at is not None
    assert again.read_at > done.read_at
    massive_session.release(result)


# --- The first read of history(wait=True) -----------------------------------------------------


def test_the_first_read_s_failures_retry_or_fail(
    massive_session: dm.MassiveSession, clock: FakeClock
) -> None:
    first = FirstRead(
        {
            "AAPL": SymbolRows(rows_of(massive_session, "AAPL", 1)),
            "MSFT": refused(503),
            "NVDA": refused(403),
        },
        began_at=int(clock.time() * 1e9),
        seconds=0.5,
    )
    script = Script(massive_session)
    result = follow(massive_session, watchlist("AAPL", "MSFT", "NVDA"), script, first=first)
    assert wait_state(massive_session, "MSFT", SymbolReadState.RETRYING).attempts == 1
    assert wait_state(massive_session, "NVDA", SymbolReadState.FAILED).attempts == 1
    assert wait_state(massive_session, "AAPL", SymbolReadState.DONE).rows == 1
    clock.advance(RETRY_DELAYS[0])
    massive_session._core.follows.wake()
    assert wait_state(massive_session, "MSFT", SymbolReadState.DONE).attempts == 2
    assert script.calls == [("MSFT",)]
    massive_session.release(result)


def test_a_call_time_symbol_gone_by_the_watch_s_start_is_removed(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    first = FirstRead(
        {name: SymbolRows(rows_of(massive_session, name, 1)) for name in ("AAPL", "MSFT")},
        began_at=1,
        seconds=0.1,
    )
    result = follow(massive_session, watchlist("AAPL"), Script(massive_session), first=first)
    wait_state(massive_session, "MSFT", SymbolReadState.REMOVED)
    assert {r["Ticker"] for r in dh_to_pylist(result)} == {"AAPL"}
    massive_session.release(result)


# --- Stops, failures, and the close -----------------------------------------------------------


def test_release_while_loading_stops_every_symbol(
    massive_session: dm.MassiveSession, drain: Callable[[], None]
) -> None:
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    result = follow(massive_session, watchlist("AAPL", "MSFT"), script)
    wait_state(massive_session, "MSFT", SymbolReadState.DONE)
    massive_session.release(result)
    del result
    wait_state(massive_session, "AAPL", SymbolReadState.STOPPED)
    wait_state(massive_session, "MSFT", SymbolReadState.STOPPED)
    gate.set()
    until(lambda: massive_session._core.follows.thread is None)
    drain()
    assert state(massive_session, "AAPL") is SymbolReadState.STOPPED  # the late outcome is dropped


def test_a_failed_table_stops_its_symbols(massive_session: dm.MassiveSession) -> None:
    script = Script(massive_session)
    gate = script.gates["MSFT"] = threading.Event()
    script.answers["AAPL"] = [dm.AuthenticationError("HTTP 401: wrong key")]
    result = follow(massive_session, watchlist("AAPL", "MSFT"), script)
    until(lambda: bool(result.j_table.isFailed()))
    wait_state(massive_session, "MSFT", SymbolReadState.STOPPED)
    gate.set()


def test_close_marks_every_unfinished_symbol_closed(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(massive_session._core.follows, "_max_workers", 1)
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    script.answers["NVDA"] = [refused(403)]
    wl = watchlist("AAPL", "NVDA")
    result = follow(massive_session, wl, script, one_scan=True)
    until(lambda: script.calls == [("AAPL", "NVDA")])
    add(wl, "MSFT")
    wait_state(massive_session, "MSFT", SymbolReadState.QUEUED)
    massive_session.close()
    assert {name: r.state for (_, name), r in reads(massive_session).items()} == {
        "AAPL": SymbolReadState.CLOSED,
        "NVDA": SymbolReadState.CLOSED,
        "MSFT": SymbolReadState.CLOSED,
    }
    gate.set()  # its rows land after the close: the row stays CLOSED
    until(lambda: massive_session._core.follows.thread is None)
    assert state(massive_session, "AAPL") is SymbolReadState.CLOSED
    assert result.j_table.isFailed()


def test_a_symbol_table_view_after_close_keeps_the_final_rows(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList, drain: Callable[[], None]
) -> None:
    result = follow(massive_session, watchlist("AAPL"), Script(massive_session))
    wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    drain()
    view = massive_session.symbol_reads()
    massive_session.close()
    assert [(r["Symbol"], r["State"]) for r in dh_to_pylist(view)] == [("AAPL", "CLOSED")]
    with pytest.raises(MassiveError, match=r"is closed$"):
        massive_session.symbol_reads()
    massive_session.release(view)
    del result


def test_two_results_of_one_table_keep_rows_of_their_own(
    massive_session: dm.MassiveSession, drain: Callable[[], None], dh_to_pylist: ToList
) -> None:
    wl = watchlist("AAPL")
    first = follow(massive_session, wl, Script(massive_session))
    second = follow(massive_session, wl, Script(massive_session))
    until(lambda: [r.state for r in reads(massive_session).values()] == [SymbolReadState.DONE] * 2)
    drain()
    shown = sorted(
        (r["StreamId"], r["Symbol"]) for r in dh_to_pylist(massive_session.symbol_reads())
    )
    assert [symbol for _, symbol in shown] == ["AAPL", "AAPL"]
    assert shown[0][0] != shown[1][0]
    massive_session.release(first)
    massive_session.release(second)


# --- The m.streams() row ----------------------------------------------------------------------


def test_the_streams_row_shows_loading_then_running(
    massive_session: dm.MassiveSession, drain: Callable[[], None], dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    gate = script.gates["MSFT"] = threading.Event()
    script.answers["AAPL"] = [SymbolRows(rows_of(massive_session, "AAPL", 3))]
    result = follow(massive_session, watchlist("AAPL", "MSFT"), script)
    wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    loading = stream(massive_session, drain, dh_to_pylist)
    assert {k: loading[k] for k in ("Kind", "State", "Loading", "SymbolCount", "Symbols")} == {
        "Kind": "read",
        "State": "LOADING",
        "Loading": 1,
        "SymbolCount": 2,
        "Symbols": "AAPL, MSFT",
    }
    assert loading["RowsPublished"] == 3
    assert loading["Transport"] == "REST"
    gate.set()
    wait_state(massive_session, "MSFT", SymbolReadState.DONE)
    until(lambda: stream(massive_session, drain, dh_to_pylist)["State"] == "RUNNING")
    running = stream(massive_session, drain, dh_to_pylist)
    assert (running["Loading"], running["RowsPublished"], running["Error"]) == (0, 5, None)
    massive_session.release(result)


def test_the_streams_error_groups_failed_and_retrying_symbols_by_cause(
    *,
    massive_session: dm.MassiveSession,
    clock: FakeClock,
    drain: Callable[[], None],
    dh_to_pylist: ToList,
) -> None:
    script = Script(massive_session)
    names = ["A", "B", "C", "D", "E", "F", "G"]
    for name in names:
        script.answers[name] = [refused(403, "not in the plan")]
    script.answers["H"] = [refused(404, "gone")]
    script.answers["R"] = [refused(503, "busy")]
    result = follow(massive_session, watchlist(*names, "H", "R"), script)
    wait_state(massive_session, "R", SymbolReadState.RETRYING)
    for name in [*names, "H"]:
        wait_state(massive_session, name, SymbolReadState.FAILED)
    until(
        lambda: "8 symbols failed" in (stream(massive_session, drain, dh_to_pylist)["Error"] or "")
    )
    error = stream(massive_session, drain, dh_to_pylist)["Error"]
    assert error == (
        "1 symbol retrying after a failure that may pass: HTTP 503: busy: R; "
        "8 symbols failed (delete and re-add a row to read one again): "
        "HTTP 403: not in the plan: A, B, C, D, E (+2 more); HTTP 404: gone: H"
    )
    massive_session.release(result)


def test_the_streams_error_names_at_most_three_causes(
    massive_session: dm.MassiveSession, drain: Callable[[], None], dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    statuses = {"A": 400, "B": 401, "C": 403, "D": 404, "E": 409}
    for name, status in statuses.items():
        script.answers[name] = [refused(status, "x" * 300)]
    script.answers["B"] = [SymbolFailure(MassiveError("no status"), FailureKind.PERMANENT)]
    result = follow(massive_session, watchlist(*statuses), script)
    for name in statuses:
        wait_state(massive_session, name, SymbolReadState.FAILED)
    until(
        lambda: "5 symbols failed" in (stream(massive_session, drain, dh_to_pylist)["Error"] or "")
    )
    error = stream(massive_session, drain, dh_to_pylist)["Error"]
    assert error.endswith("; 2 more causes")
    assert "…" in error  # a long cause is cut
    massive_session.release(result)


def test_the_flat_files_and_rest_label(
    massive_session: dm.MassiveSession, drain: Callable[[], None], dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    script.answers["AAPL"] = [
        SymbolRows(rows_of(massive_session, "AAPL", 1), source=FLAT_FILES_AND_REST)
    ]
    result = follow(massive_session, watchlist("AAPL"), script, one_scan=True)
    assert (
        wait_state(massive_session, "AAPL", SymbolReadState.DONE).transport == FLAT_FILES_AND_REST
    )
    until(
        lambda: stream(massive_session, drain, dh_to_pylist)["Transport"] == "flat files and REST"
    )
    (shown,) = dh_to_pylist(massive_session.symbol_reads())
    assert shown["Transport"] == "flat files and REST"
    massive_session.release(result)


def test_the_streams_summary_agrees_with_the_symbol_reads(
    massive_session: dm.MassiveSession, drain: Callable[[], None], dh_to_pylist: ToList
) -> None:
    script = Script(massive_session)
    script.answers["NVDA"] = [refused(403)]
    gate = script.gates["TSLA"] = threading.Event()
    result = follow(massive_session, watchlist("AAPL", "MSFT", "NVDA", "TSLA"), script)
    for name, expected in (("AAPL", "DONE"), ("MSFT", "DONE"), ("NVDA", "FAILED")):
        wait_state(massive_session, name, SymbolReadState(expected))
    wait_state(massive_session, "TSLA", SymbolReadState.LOADING)
    until(lambda: stream(massive_session, drain, dh_to_pylist)["RowsPublished"] == 4)
    summary = stream(massive_session, drain, dh_to_pylist)
    symbols = dh_to_pylist(massive_session.symbol_reads())
    assert summary["SymbolCount"] == len(symbols)
    assert summary["RowsPublished"] == sum(r["Rows"] for r in symbols)
    assert summary["Loading"] == sum(
        r["State"] in ("QUEUED", "LOADING", "RETRYING") for r in symbols
    )
    gate.set()
    massive_session.release(result)


# --- Threads ----------------------------------------------------------------------------------


def test_one_coordinator_and_pool_serve_every_result(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(massive_session._core.follows, "_max_workers", 2)
    prefix = massive_session._core.thread_prefix
    results = [
        follow(massive_session, watchlist("AAPL", "MSFT"), Script(massive_session))
        for _ in range(6)
    ]
    until(lambda: [r.state for r in reads(massive_session).values()] == [SymbolReadState.DONE] * 12)
    names = [t.name for t in threading.enumerate() if t.name.startswith(f"{prefix} follow")]
    assert names.count(f"{prefix} follow") == 1
    assert len([n for n in names if n.startswith(f"{prefix} follow-read")]) <= 2
    for result in results:
        massive_session.release(result)
    del results
    until(lambda: massive_session._core.follows.thread is None)
    until(lambda: not [t for t in threading.enumerate() if t.name.startswith(f"{prefix} follow")])


def test_a_dying_coordinator_fails_every_followed_table(
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    script = Script(massive_session)
    first = follow(massive_session, watchlist("AAPL"), script)
    second = follow(massive_session, watchlist("MSFT"), script)
    until(lambda: [r.state for r in reads(massive_session).values()] == [SymbolReadState.DONE] * 2)

    def dies(_self: follow_coordinator.FollowCoordinator, _touched: object = None) -> None:
        raise SystemExit("the coordinator is told to die")

    escaped: list[threading.ExceptHookArgs] = []
    # The thread's exception must still escape; collect it instead of pytest's warning.
    monkeypatch.setattr(threading, "excepthook", escaped.append)
    # Each batch checks the retries: an interrupt there ends the thread.
    monkeypatch.setattr(follow_coordinator.FollowCoordinator, "_retry_due", dies)
    with caplog.at_level(logging.ERROR):
        massive_session._core.follows.wake()
        until(lambda: bool(first.j_table.isFailed()) and bool(second.j_table.isFailed()))
    assert "massive #" in caplog.text
    assert "died; its queue is no longer handled" in caplog.text
    until(lambda: [args.exc_type for args in escaped] == [SystemExit])
    assert massive_session._core.follows.thread is None
    with massive_session._core.registry_lock:
        errors = [w.row.error for w in massive_session._core._workers.values()]
    assert all("the follow coordinator died" in (e or "") for e in errors)


def test_a_session_freed_while_following_needs_no_close(dh_to_pylist: ToList) -> None:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key"))
    result = follow(m, watchlist("AAPL"), Script(m))
    until(lambda: len(dh_to_pylist(result)) == 2)
    m.release(result)


def test_the_coordinator_s_stop_runs_with_the_update_graph_lock_held(
    massive_session: dm.MassiveSession,
) -> None:
    # A console command may release a result: the shutdown and the stop never wait on it.
    result = follow(massive_session, watchlist("AAPL"), Script(massive_session))
    wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    with exclusive_lock(massive_session._core.exec_ctx.update_graph):
        massive_session.release(result)
    del result
    until(lambda: massive_session._core.follows.thread is None)


def test_sixteen_threads_editing_one_watchlist_while_four_results_follow_it(
    massive_session: dm.MassiveSession, dh_to_pylist: ToList
) -> None:
    # The stress test the free-threaded build runs too: no lock but the library's own keeps
    # the coordinator's state consistent.
    pool = [f"S{n}" for n in range(12)]
    wl = watchlist()
    results = [
        follow(massive_session, wl, Script(massive_session), reread=reread)
        for reread in (Reread.ALWAYS, Reread.NEVER, Reread.ALWAYS, Reread.NEVER)
    ]
    start = threading.Barrier(16)

    def edit(seed: int, rounds: int) -> None:
        start.wait(TIMEOUT)
        with entered(massive_session._core.exec_ctx):
            for step in range(rounds):
                symbol = pool[(seed * 7 + step * 5) % len(pool)]
                if (seed + step) % 3:
                    add(wl, symbol)
                else:
                    delete(wl, symbol)

    def run_editors(rounds: int, during: Callable[[], None]) -> None:
        editors = ThreadPoolExecutor(16, thread_name_prefix="editor")
        try:
            futures = [editors.submit(edit, seed, rounds) for seed in range(16)]
            during()
            for future in futures:
                future.result(TIMEOUT)  # raises an editor's error
        finally:
            editors.shutdown(wait=False)

    run_editors(6, lambda: massive_session.release(results.pop(0)))
    held = sorted(row[0] for row in wl.iter_tuple(cols="Symbol"))

    def converged(result: Table) -> bool:
        # The result holds the table's symbols, two rows each.
        rows = dh_to_pylist(result)
        return sorted({r["Ticker"] for r in rows}) == held and len(rows) == 2 * len(held)

    for result in results:
        until(functools.partial(converged, result))
    run_editors(4, massive_session.close)
    until(lambda: all(bool(result.j_table.isFailed()) for result in results))
    until(lambda: massive_session._core.follows.thread is None)
    final = {SymbolReadState.CLOSED, SymbolReadState.REMOVED, SymbolReadState.STOPPED}
    assert {row.state for row in reads(massive_session).values()} <= final


def test_rows_from_rest_and_from_flat_files_show_both_sources(
    massive_session: dm.MassiveSession, drain: Callable[[], None], dh_to_pylist: ToList
) -> None:
    # A flat-file read that S3 refused is read from REST: its rows say so.
    script = Script(massive_session)
    script.answers["AAPL"] = [SymbolRows(rows_of(massive_session, "AAPL", 1), source="REST")]
    script.answers["MSFT"] = [SymbolRows(rows_of(massive_session, "MSFT", 1), source="flat files")]
    result = follow(massive_session, watchlist("AAPL", "MSFT"), script, one_scan=True)
    assert wait_state(massive_session, "AAPL", SymbolReadState.DONE).transport == "REST"
    assert wait_state(massive_session, "MSFT", SymbolReadState.DONE).transport == "flat files"
    until(lambda: stream(massive_session, drain, dh_to_pylist)["Transport"] == FLAT_FILES_AND_REST)
    massive_session.release(result)


def test_a_late_outcome_of_a_stopped_result_is_dropped(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    keeper = follow(massive_session, watchlist("MSFT"), Script(massive_session))
    result = follow(massive_session, watchlist("AAPL"), script)
    until(lambda: script.calls == [("AAPL",)])
    massive_session.release(result)
    del result
    wait_state(massive_session, "AAPL", SymbolReadState.STOPPED)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        gate.set()  # the coordinator still runs for `keeper`, and drops the outcome
        until(lambda: "after it stopped: _Outcome" in caplog.text)
    assert state(massive_session, "AAPL") is SymbolReadState.STOPPED
    massive_session.release(keeper)


def test_a_symbol_whose_rows_the_library_cannot_use_fails_alone_at_error(
    massive_session: dm.MassiveSession,
    caplog: pytest.LogCaptureFixture,
    dh_to_pylist: ToList,
) -> None:
    # As a read by symbol fails a table without the dataset's columns: a defect, ERROR.
    defect = MassiveError(
        "test.trades: AAPL's rows from REST lack Price", cause=dm.ErrorCause.LIBRARY_DEFECT
    )
    script = Script(massive_session)
    script.answers["AAPL"] = [SymbolFailure(defect, FailureKind.PERMANENT)]
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        result = follow(massive_session, watchlist("AAPL", "MSFT"), script)
        wait_state(massive_session, "AAPL", SymbolReadState.FAILED)
        wait_state(massive_session, "MSFT", SymbolReadState.DONE)
    assert [r.levelno for r in caplog.records] == [logging.ERROR]
    assert "This is a bug in deephaven-massive" in caplog.records[0].getMessage()
    assert not result.j_table.isFailed()
    until(lambda: {r["Ticker"] for r in dh_to_pylist(result)} == {"MSFT"})
    massive_session.release(result)


def test_two_bad_rows_fail_the_table_once(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = dataclasses.replace(
        massive_session._core.config, noncanonical_symbols=dm.OnError.RAISE
    )
    monkeypatch.setattr(massive_session._core, "config", config)
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, Script(massive_session))
    wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    add(wl, "msft", "nvda")  # two refusals in one update: the second finds the table failed
    until(lambda: bool(result.j_table.isFailed()))
    wait_state(massive_session, "AAPL", SymbolReadState.STOPPED)
