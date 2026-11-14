"""Tests for the follow coordinator: each symbol's state, its retries, and the diagnostics rows.

Each test follows a real Deephaven symbol table with a scripted read in place of a transport,
and reads what the coordinator decided from `m.symbol_reads()` and `m.streams()`. Retries run
on a virtual clock (`tests/_fakes/clock.py`): a test moves it and wakes the coordinator, so
no test waits for real minutes.
"""

from __future__ import annotations

import dataclasses
import functools
import gc
import logging
import threading
import weakref
from collections import UserDict
from collections.abc import Callable, Mapping, ValuesView
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from types import MethodType
from typing import TYPE_CHECKING, Any

import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.table import Table
from deephaven.update_graph import exclusive_lock
from typing_extensions import override

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._enums import AssetClass, NotBars, Reread
from deephaven_massive._errors import BrokenGuaranteeError, MassiveError, UnlistedPairError
from deephaven_massive._massive import Transport
from deephaven_massive._tables import follow_coordinator
from deephaven_massive._tables.diagnostics import (
    FLAT_FILES_AND_REST,
    ConnectionRow,
    Diagnostics,
    StreamRow,
    StreamUpdate,
)
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.follow_coordinator import RETRY_DELAYS, FirstRead
from deephaven_massive._tables.followed_read import FollowedRead, FollowPlan, open_followed_read
from deephaven_massive._tables.read_pool import ReadPool
from deephaven_massive._tables.symbol_read_rows import SymbolReadRow, SymbolReadState
from deephaven_massive._tables.transports import (
    FailureKind,
    SymbolFailure,
    SymbolOutcome,
    SymbolRows,
)
from deephaven_massive._tables.watchlist import SymbolRowRule
from tests._fakes.clock import FakeClock

if TYPE_CHECKING:
    from deephaven.execution_context import ExecutionContext
    from deephaven.table_factory import InputTable

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


@pytest.fixture
def watchlist(dh_input_table: Callable[..., InputTable]) -> Callable[..., InputTable]:
    # Makes a user's keyed watchlist holding the symbols.
    def make(*symbols: str) -> InputTable:
        table = dh_input_table({"Symbol": dtypes.string}, key_cols="Symbol")
        if symbols:
            add(table, *symbols)
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
    symbols_per_read: int = 1,
) -> Table:
    core = m._core
    plan = FollowPlan(
        Transport.FLAT_FILES if one_scan else Transport.REST,
        one_scan=one_scan,
        symbols_per_read=symbols_per_read,
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
        note="Follows a test.",
        first=first,
    )


_written: dict[int, dict[tuple[int, str], SymbolReadRow]] = {}
_written_lock = threading.Lock()


@pytest.fixture(autouse=True)
def _record_symbol_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    # Keeps every m.symbol_reads() row each session writes, by its diagnostics: the core keeps
    # only the rows of unfinished symbols, and the tests also read the finished ones.
    write = Diagnostics.write

    def recording(diagnostics: Diagnostics, row: ConnectionRow | StreamRow | SymbolReadRow) -> None:
        if isinstance(row, SymbolReadRow):
            with _written_lock:
                _written.setdefault(id(diagnostics), {})[row.stream_id, row.symbol] = row
        write(diagnostics, row)

    monkeypatch.setattr(Diagnostics, "write", recording)
    with _written_lock:
        _written.clear()


def reads(m: dm.MassiveSession) -> dict[tuple[int, str], SymbolReadRow]:
    # The latest m.symbol_reads() row of each (result, symbol) the session wrote.
    with _written_lock:
        return dict(_written.get(id(m._core.diagnostics), {}))


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
    watchlist: Callable[..., InputTable],
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
    view = massive_session.symbol_reads()

    def shown() -> dict[str, tuple[str, str]]:
        return {r["Symbol"]: (r["State"], r["Transport"]) for r in dh_to_pylist(view)}

    until(lambda: shown() == {"AAPL": ("DONE", "REST"), "MSFT": ("LOADING", None)})
    massive_session.release(view)
    hold.set()
    wait_state(massive_session, "MSFT", SymbolReadState.DONE)
    massive_session.release(result)


def test_a_symbol_waits_queued_while_the_pool_is_busy(
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(massive_session._core.reads, "_max_workers", 1)
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


def test_no_data_is_done_with_no_rows(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession
) -> None:
    script = Script(massive_session)
    script.answers["ZZZZ"] = [SymbolRows(rows_of(massive_session, "ZZZZ", 0))]
    result = follow(massive_session, watchlist("ZZZZ"), script)
    done = wait_state(massive_session, "ZZZZ", SymbolReadState.DONE)
    assert (done.rows, done.error) == (0, None)
    massive_session.release(result)


def test_a_permanent_failure_fails_only_its_symbol(
    *,
    watchlist: Callable[..., InputTable],
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
    assert "AAPL failed after 1 attempt (permanent)" in warnings[0]
    until(lambda: {r["Ticker"] for r in dh_to_pylist(result)} == {"MSFT"})
    massive_session.release(result)


def test_a_transient_failure_retries_after_1_5_and_15_minutes_then_fails(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, clock: FakeClock
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
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, clock: FakeClock
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
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    clock: FakeClock,
) -> None:
    # A due retry needs no event: the coordinator's wait ends at it. The retry falls due only
    # once the coordinator waits for it, so no batch of events can find it due first.
    script = Script(massive_session)
    script.answers["AAPL"] = [refused(500, "boom"), SymbolRows(rows_of(massive_session, "AAPL", 1))]
    monkeypatch.setattr(follow_coordinator, "RETRY_DELAYS", (0.05, 0.05, 0.05))
    waiting = threading.Event()
    next_due = follow_coordinator.FollowCoordinator._next_due

    def next_due_noted(self: follow_coordinator.FollowCoordinator) -> float | None:
        due = next_due(self)
        if due is not None:  # the batch that set the retry is done; the timed wait starts
            waiting.set()
        return due

    monkeypatch.setattr(follow_coordinator.FollowCoordinator, "_next_due", next_due_noted)
    result = follow(massive_session, watchlist("AAPL"), script)
    assert waiting.wait(TIMEOUT)
    clock.advance(1)  # the wait, 0.05 s at most on the real clock, ends with the retry due
    wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    massive_session.release(result)


def test_a_removal_cancels_a_pending_retry(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, clock: FakeClock
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


def test_a_re_added_symbol_retries_on_its_new_schedule_alone(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, clock: FakeClock
) -> None:
    # The first failure's retry stays in the heap after the removal; when it comes due, the
    # symbol waits for its second failure's retry, so the stale one reads nothing.
    script = Script(massive_session)
    script.answers["AAPL"] = [refused(503)]
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    wait_state(massive_session, "AAPL", SymbolReadState.RETRYING)
    clock.advance(RETRY_DELAYS[0] / 2)
    delete(wl, "AAPL")
    wait_state(massive_session, "AAPL", SymbolReadState.REMOVED)
    add(wl, "AAPL")
    until(lambda: script.reads_of("AAPL") == 2)
    wait_state(massive_session, "AAPL", SymbolReadState.RETRYING)
    follows = massive_session._core.follows
    clock.advance(RETRY_DELAYS[0] / 2)  # the first retry's time: no read
    follows.wake()
    until(lambda: len(follows._retries) == 1)
    assert script.reads_of("AAPL") == 2
    clock.advance(RETRY_DELAYS[0] / 2)  # the second retry's time
    follows.wake()
    until(lambda: script.reads_of("AAPL") == 3)
    massive_session.release(result)


def test_a_retry_of_a_released_result_never_reads(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, clock: FakeClock
) -> None:
    # The released result is forgotten with its retry still in the heap; the retry, when it
    # comes due, finds no result and is dropped.
    script = Script(massive_session)
    script.answers["AAPL"] = [refused(503)]
    result = follow(massive_session, watchlist("AAPL"), script)
    wait_state(massive_session, "AAPL", SymbolReadState.RETRYING)
    follows = massive_session._core.follows
    massive_session.release(result)
    del result
    until(lambda: not follows._results)
    assert len(follows._retries) == 1
    clock.advance(RETRY_DELAYS[0])
    follows.wake()
    until(lambda: not follows._retries)
    assert script.reads_of("AAPL") == 1


def test_a_removal_cancels_a_queued_read(
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(massive_session._core.reads, "_max_workers", 1)
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
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, dh_to_pylist: ToList
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
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, dh_to_pylist: ToList
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
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, dh_to_pylist: ToList
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
    until(lambda: len(dh_to_pylist(result)) == 2)
    massive_session.release(result)


def test_a_failed_symbol_is_read_again_on_re_add_under_both_members(
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
) -> None:
    for reread in Reread:
        script = Script(massive_session)
        script.answers["AAPL"] = [refused(403), SymbolRows(rows_of(massive_session, "AAPL", 1))]
        wl = watchlist()  # added after the call: a call-time symbol's failure would fail it
        result = follow(massive_session, wl, script, reread=reread)
        add(wl, "AAPL")
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
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, clock: FakeClock
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
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, clock: FakeClock
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
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, clock: FakeClock
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
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, dh_to_pylist: ToList
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
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    drain: Callable[[], None],
) -> None:
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    result = follow(massive_session, watchlist("AAPL", "MSFT"), script)
    wait_state(massive_session, "MSFT", SymbolReadState.DONE)
    core = massive_session._core
    with core.registry_lock:  # what a close would mark CLOSED: both, loading and done
        assert {name for _, name in core._symbol_reads} == {"AAPL", "MSFT"}
    massive_session.release(result)
    del result
    wait_state(massive_session, "AAPL", SymbolReadState.STOPPED)
    wait_state(massive_session, "MSFT", SymbolReadState.STOPPED)
    with core.registry_lock:  # a stopped symbol is finished: the core keeps no row of it
        assert core._symbol_reads == {}
    gate.set()
    until(lambda: massive_session._core.follows.following == 0)
    drain()
    assert state(massive_session, "AAPL") is SymbolReadState.STOPPED  # the late outcome is dropped


def test_a_failed_table_stops_its_symbols(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession
) -> None:
    script = Script(massive_session)
    gate = script.gates["MSFT"] = threading.Event()
    script.answers["AAPL"] = [dm.AuthenticationError("HTTP 401: wrong key")]
    result = follow(massive_session, watchlist("AAPL", "MSFT"), script)
    until(lambda: bool(result.j_table.isFailed()))
    wait_state(massive_session, "MSFT", SymbolReadState.STOPPED)
    gate.set()


def test_a_read_that_starts_as_its_table_stops_leaves_its_symbol_stopped(
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # AAPL's failure stops the result while MSFT's read is still starting. The coordinator
    # takes the stop and MSFT's start in one batch, the order a busy pool can produce: MSFT
    # must end STOPPED, not LOADING, although the batch forgets the result.
    coordinator = follow_coordinator.FollowCoordinator
    post, read, handle = coordinator.post, coordinator._read, coordinator._handle
    withheld: list[follow_coordinator._Message] = []
    stop_withheld, holding, held, resume, handled = (threading.Event() for _ in range(5))

    def is_msft_start(message: follow_coordinator._Message) -> bool:
        return isinstance(message, follow_coordinator._Began) and "MSFT" in message.generations

    def ordered_post(
        self: follow_coordinator.FollowCoordinator, message: follow_coordinator._Message
    ) -> None:
        if isinstance(message, follow_coordinator._Stop) and not stop_withheld.is_set():
            withheld.append(message)
            stop_withheld.set()
            return
        post(self, message)
        if is_msft_start(message):
            resume.set()

    def late_read(
        self: follow_coordinator.FollowCoordinator,
        followed: FollowedRead,
        generations: Mapping[str, int],
    ) -> None:
        if "MSFT" in generations:
            assert stop_withheld.wait(TIMEOUT)
            holding.set()
            self.wake()  # holds the coordinator thread until MSFT's start is queued too
            assert held.wait(TIMEOUT)
            post(self, withheld[0])
        read(self, followed, generations)

    def held_handle(
        self: follow_coordinator.FollowCoordinator, batch: list[follow_coordinator._Message]
    ) -> None:
        if holding.is_set() and not held.is_set():
            held.set()
            assert resume.wait(TIMEOUT)
        handle(self, batch)
        if any(is_msft_start(message) for message in batch):
            handled.set()

    monkeypatch.setattr(coordinator, "post", ordered_post)
    monkeypatch.setattr(coordinator, "_read", late_read)
    monkeypatch.setattr(coordinator, "_handle", held_handle)
    script = Script(massive_session)
    gate = script.gates["MSFT"] = threading.Event()
    script.answers["AAPL"] = [dm.AuthenticationError("HTTP 401: wrong key")]
    result = follow(massive_session, watchlist("AAPL", "MSFT"), script)
    until(lambda: bool(result.j_table.isFailed()))
    assert handled.wait(TIMEOUT)
    assert state(massive_session, "MSFT") is SymbolReadState.STOPPED
    gate.set()


def test_a_symbol_added_after_its_result_failed_is_stopped_not_queued(
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The table's failure is held back, so the result has failed but not yet stopped when the
    # symbol arrives: it is never read, and must not wait as QUEUED.
    failures: list[MassiveError] = []

    def held_back(_read: object, error: MassiveError) -> bool:
        failures.append(error)
        return True

    monkeypatch.setattr(FollowedRead, "fail_table", held_back)
    script = Script(massive_session)
    script.answers["AAPL"] = [dm.AuthenticationError("HTTP 401: wrong key")]
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    until(lambda: len(failures) == 1)
    add(wl, "NVDA")
    wait_state(massive_session, "NVDA", SymbolReadState.STOPPED)
    assert script.reads_of("NVDA") == 0
    monkeypatch.undo()
    massive_session.release(result)


def test_close_marks_every_unfinished_symbol_closed(
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(massive_session._core.reads, "_max_workers", 1)
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
    until(lambda: bool(result.j_table.isFailed()))  # delivered on the next update cycle


def test_a_symbol_table_view_after_close_keeps_the_final_rows(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    dh_to_pylist: ToList,
    drain: Callable[[], None],
) -> None:
    result = follow(massive_session, watchlist("AAPL"), Script(massive_session))
    wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    drain()
    view = massive_session.symbol_reads()
    massive_session.close()
    until(lambda: [(r["Symbol"], r["State"]) for r in dh_to_pylist(view)] == [("AAPL", "CLOSED")])
    with pytest.raises(MassiveError, match=r"is closed$"):
        massive_session.symbol_reads()
    massive_session.release(view)
    del result


def test_two_results_of_one_table_keep_rows_of_their_own(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    drain: Callable[[], None],
    dh_to_pylist: ToList,
) -> None:
    wl = watchlist("AAPL")
    first = follow(massive_session, wl, Script(massive_session))
    second = follow(massive_session, wl, Script(massive_session))
    until(lambda: [r.state for r in reads(massive_session).values()] == [SymbolReadState.DONE] * 2)
    view = massive_session.symbol_reads()
    until(lambda: len(dh_to_pylist(view)) == 2)
    shown = sorted((r["StreamId"], r["Symbol"]) for r in dh_to_pylist(view))
    massive_session.release(view)
    assert [symbol for _, symbol in shown] == ["AAPL", "AAPL"]
    assert shown[0][0] != shown[1][0]
    massive_session.release(first)
    massive_session.release(second)


# --- The m.streams() row ----------------------------------------------------------------------


def test_the_streams_row_shows_loading_then_running(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    drain: Callable[[], None],
    dh_to_pylist: ToList,
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
    watchlist: Callable[..., InputTable],
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
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    drain: Callable[[], None],
    dh_to_pylist: ToList,
) -> None:
    script = Script(massive_session)
    statuses = {"A": 400, "B": 401, "C": 403, "D": 404, "E": 409}
    for name, status in statuses.items():
        script.answers[name] = [refused(status, "x" * 300)]
    script.answers["B"] = [SymbolFailure(MassiveError("no status"), FailureKind.PERMANENT)]
    # One symbol with rows keeps the table: every call-time symbol failing would fail it.
    result = follow(massive_session, watchlist(*statuses, "OK"), script)
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
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    drain: Callable[[], None],
    dh_to_pylist: ToList,
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
    view = massive_session.symbol_reads()
    until(lambda: [r["Transport"] for r in dh_to_pylist(view)] == ["flat files and REST"])
    massive_session.release(view)
    massive_session.release(result)


def test_the_streams_summary_agrees_with_the_symbol_reads(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    drain: Callable[[], None],
    dh_to_pylist: ToList,
) -> None:
    script = Script(massive_session)
    script.answers["NVDA"] = [refused(403)]
    gate = script.gates["TSLA"] = threading.Event()
    wl = watchlist("AAPL", "MSFT", "NVDA", "TSLA")
    result = follow(massive_session, wl, script)
    for name, expected in (("AAPL", "DONE"), ("MSFT", "DONE"), ("NVDA", "FAILED")):
        wait_state(massive_session, name, SymbolReadState(expected))
    wait_state(massive_session, "TSLA", SymbolReadState.LOADING)
    until(lambda: stream(massive_session, drain, dh_to_pylist)["RowsPublished"] == 4)
    summary = stream(massive_session, drain, dh_to_pylist)
    view = massive_session.symbol_reads()
    final = {"AAPL": "DONE", "MSFT": "DONE", "NVDA": "FAILED", "TSLA": "LOADING"}
    until(lambda: {r["Symbol"]: r["State"] for r in dh_to_pylist(view)} == final)
    symbols = dh_to_pylist(view)
    massive_session.release(view)
    assert summary["SymbolCount"] == len(symbols)
    assert summary["RowsPublished"] == sum(r["Rows"] for r in symbols)
    assert summary["Loading"] == sum(
        r["State"] in ("QUEUED", "LOADING", "RETRYING") for r in symbols
    )
    assert "NVDA" in (summary["Error"] or "")
    delete(wl, "AAPL", "NVDA")  # a landed and a failed symbol leave: so do their counts
    until(lambda: stream(massive_session, drain, dh_to_pylist)["SymbolCount"] == 2)
    after = stream(massive_session, drain, dh_to_pylist)
    assert (after["Symbols"], after["RowsPublished"], after["Loading"]) == ("MSFT, TSLA", 2, 1)
    assert "NVDA" not in (after["Error"] or "")
    gate.set()
    massive_session.release(result)


# --- Threads ----------------------------------------------------------------------------------


def test_one_coordinator_and_pool_serve_every_result(
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(massive_session._core.reads, "_max_workers", 2)
    prefix = massive_session._core.thread_prefix
    results = [
        follow(massive_session, watchlist("AAPL", "MSFT"), Script(massive_session))
        for _ in range(6)
    ]
    until(lambda: [r.state for r in reads(massive_session).values()] == [SymbolReadState.DONE] * 12)
    names = [t.name for t in threading.enumerate() if t.name.startswith(f"{prefix} follow")]
    assert names.count(f"{prefix} follow") == 1
    assert len([n for n in names if n.startswith(f"{prefix} read_")]) <= 2
    for result in results:
        massive_session.release(result)
    del results
    until(lambda: massive_session._core.follows.following == 0)
    thread = massive_session._core.follows.thread
    assert thread is not None
    assert thread.is_alive()  # one coordinator for the session's life, idle now
    massive_session.close()
    until(lambda: not [t for t in threading.enumerate() if t.name.startswith(f"{prefix} follow")])


def test_a_dying_coordinator_fails_every_followed_table(
    *,
    watchlist: Callable[..., InputTable],
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
    assert massive_session._core.follows.following == 0
    with massive_session._core.registry_lock:
        errors = [w.row.error for w in massive_session._core.workers._workers.values()]
    assert all("the follow coordinator died" in (e or "") for e in errors)


def _holds_rows(to_list: ToList, table: Table, count: int) -> bool:
    return len(to_list(table)) == count


def test_a_session_freed_while_following_needs_no_close(
    watchlist: Callable[..., InputTable], dh_to_pylist: ToList
) -> None:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key"))
    result = follow(m, watchlist("AAPL"), Script(m))
    landed = functools.partial(_holds_rows, dh_to_pylist, result, 2)
    until(landed)
    del landed
    core = weakref.ref(m._core)
    thread = m._core.follows.thread
    assert thread is not None
    m.release(result)
    del result, m
    # Once nothing uses the result it stops, and nothing keeps the session: it is freed, and
    # its collection ends the coordinator thread.
    # A free-threaded build merges references dropped on other threads at a collection.
    until(lambda: gc.collect() >= 0 and core() is None)
    thread.join(TIMEOUT)
    assert not thread.is_alive()


def test_a_session_never_closed_outlives_its_last_stop(
    watchlist: Callable[..., InputTable],
    *,
    dh_to_pylist: ToList,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Nothing else holds the session while the coordinator handles its last result's stop:
    # the result holds the core, so the stop's rows are written to a live session, with no
    # error, and only then is the session freed.
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key"))
    result = follow(m, watchlist("AAPL"), Script(m))
    landed = functools.partial(_holds_rows, dh_to_pylist, result, 2)
    until(landed)
    del landed
    core = weakref.ref(m._core)
    thread = m._core.follows.thread
    assert thread is not None
    stopping, proceed = threading.Event(), threading.Event()
    stopped = follow_coordinator.FollowCoordinator._stopped

    def held(
        coordinator: follow_coordinator.FollowCoordinator, result: follow_coordinator._Result
    ) -> None:
        stopping.set()
        assert proceed.wait(TIMEOUT)
        stopped(coordinator, result)

    monkeypatch.setattr(follow_coordinator.FollowCoordinator, "_stopped", held)
    with caplog.at_level(logging.ERROR):
        m.release(result)
        del result, m
        assert stopping.wait(TIMEOUT)
        gc.collect()
        assert core() is not None  # the result, which the coordinator holds, holds it
        proceed.set()
        until(lambda: gc.collect() >= 0 and core() is None)
        thread.join(TIMEOUT)
    assert not thread.is_alive()
    assert [r.getMessage() for r in caplog.records] == []


def test_the_coordinator_s_stop_runs_with_the_update_graph_lock_held(
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
) -> None:
    # A console command may release a result: the shutdown and the stop never wait on it.
    result = follow(massive_session, watchlist("AAPL"), Script(massive_session))
    wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    with exclusive_lock(massive_session._core.exec_ctx.update_graph):
        massive_session.release(result)
    del result
    until(lambda: massive_session._core.follows.following == 0)


def test_sixteen_threads_editing_one_watchlist_while_four_results_follow_it(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, dh_to_pylist: ToList
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
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    drain: Callable[[], None],
    dh_to_pylist: ToList,
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
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    caplog: pytest.LogCaptureFixture,
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
    *,
    watchlist: Callable[..., InputTable],
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
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
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


def test_a_read_stopped_by_a_release_leaves_its_symbol_stopped(
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
) -> None:
    # The read sees the stop token and fails as reads do then; that failure is not the symbol's.
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    script.answers["AAPL"] = [refused(503, "stopped")]
    keeper = follow(massive_session, watchlist("MSFT"), Script(massive_session))
    result = follow(massive_session, watchlist("AAPL"), script)
    until(lambda: script.calls == [("AAPL",)])
    massive_session.release(result)
    del result
    wait_state(massive_session, "AAPL", SymbolReadState.STOPPED)
    gate.set()
    until(lambda: massive_session._core.follows.following == 1)
    assert state(massive_session, "AAPL") is SymbolReadState.STOPPED
    massive_session.release(keeper)


def test_one_result_s_defect_leaves_the_others_following(
    watchlist: Callable[..., InputTable],
    *,
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    dh_to_pylist: ToList,
) -> None:
    healthy_list, broken_list = watchlist("AAPL"), watchlist()
    healthy = follow(massive_session, healthy_list, Script(massive_session))
    wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    broken = follow(massive_session, broken_list, Script(massive_session))
    broken_id = max(_calls(massive_session))
    membership = follow_coordinator.FollowCoordinator._membership

    def fails(
        coordinator: follow_coordinator.FollowCoordinator,
        result: follow_coordinator._Result,
        message: follow_coordinator._Baseline | follow_coordinator._Changed,
    ) -> None:
        if result.read.call == broken_id:
            raise RuntimeError("a defect")
        membership(coordinator, result, message)

    monkeypatch.setattr(follow_coordinator.FollowCoordinator, "_membership", fails)
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        add(broken_list, "NVDA")
        add(healthy_list, "MSFT")
        until(lambda: bool(broken.j_table.isFailed()))
    assert "follow: handling _" in caplog.text  # its start's set, or the later change
    until(lambda: {r["Ticker"] for r in dh_to_pylist(healthy)} == {"AAPL", "MSFT"})
    with massive_session._core.registry_lock:
        error = massive_session._core.workers._workers[broken_id].row.error or ""
    assert "RuntimeError: a defect" in error
    massive_session.release(healthy)
    massive_session.release(broken)


def _calls(m: dm.MassiveSession) -> set[int]:
    # The StreamIds of the session's worker tables now.
    with m._core.registry_lock:
        return set(m._core.workers._workers)


@pytest.mark.parametrize(
    ("method", "step"),
    [
        ("_start", "starting"),
        ("_queue_retry", "queuing a due retry"),
        ("_schedule", "starting its reads"),
        ("_show", "writing its rows"),
        ("_forget", "forgetting it"),
    ],
)
def test_a_defect_in_any_step_fails_only_its_result(
    method: str,
    step: str,
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    dh_to_pylist: ToList,
    clock: FakeClock,
) -> None:
    # Every step the coordinator runs for one result is guarded: its error fails that result,
    # named in the log and the table's error, and the rest of the batch still runs.
    retry = method == "_queue_retry"  # that step runs only for a retry that came due
    healthy_list, broken_list = watchlist("AAPL"), watchlist("NVDA")
    healthy = follow(massive_session, healthy_list, Script(massive_session))
    wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    healthy_id = max(_calls(massive_session))
    original = getattr(follow_coordinator.FollowCoordinator, method)

    def breaks(
        coordinator: follow_coordinator.FollowCoordinator, *step_args: object, **named: object
    ) -> None:
        result = step_args[0]
        assert isinstance(result, follow_coordinator._Result)
        if result.read.call != healthy_id:
            raise RuntimeError(f"a defect in {method}")
        original(coordinator, *step_args, **named)

    monkeypatch.setattr(follow_coordinator.FollowCoordinator, method, breaks)
    broken_script = Script(massive_session)
    if retry:
        broken_script.answers["NVDA"] = [refused(503)]
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        broken = follow(massive_session, broken_list, broken_script)
        if retry:
            wait_state(massive_session, "NVDA", SymbolReadState.RETRYING)
            clock.advance(RETRY_DELAYS[0])
            massive_session._core.follows.wake()
        until(lambda: bool(broken.j_table.isFailed()))
    broken_id = max(_calls(massive_session))
    assert f"call {broken_id} follow: {step} failed (test.trades)" in caplog.text
    with massive_session._core.registry_lock:
        error = massive_session._core.workers._workers[broken_id].row.error or ""
    assert f"the followed read failed {step}: RuntimeError: a defect in {method}" in error
    add(healthy_list, "MSFT")
    until(lambda: {r["Ticker"] for r in dh_to_pylist(healthy)} == {"AAPL", "MSFT"})
    if method != "_show":  # its symbol is stopped, never left queued or loading
        until(lambda: state(massive_session, "NVDA") in (None, SymbolReadState.STOPPED))
    massive_session.release(healthy)
    massive_session.release(broken)


def test_a_defect_while_failing_a_broken_result_is_logged_too(
    watchlist: Callable[..., InputTable],
    *,
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    dh_to_pylist: ToList,
) -> None:
    healthy_list = watchlist("AAPL")
    healthy = follow(massive_session, healthy_list, Script(massive_session))
    wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    healthy_id = max(_calls(massive_session))
    show = follow_coordinator.FollowCoordinator._show

    def breaks(
        coordinator: follow_coordinator.FollowCoordinator, result: follow_coordinator._Result
    ) -> None:
        if result.read.call != healthy_id:
            raise RuntimeError("a defect in _show")
        show(coordinator, result)

    def cannot_fail(_read: object, _error: MassiveError) -> bool:
        raise RuntimeError("a defect in fail_table")

    monkeypatch.setattr(follow_coordinator.FollowCoordinator, "_show", breaks)
    monkeypatch.setattr(FollowedRead, "fail_table", cannot_fail)
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        broken = follow(massive_session, watchlist("NVDA"), Script(massive_session))
        until(lambda: "failing its table after writing its rows failed raised too" in caplog.text)
    monkeypatch.undo()
    add(healthy_list, "MSFT")  # the coordinator goes on
    until(lambda: {r["Ticker"] for r in dh_to_pylist(healthy)} == {"AAPL", "MSFT"})
    massive_session.release(healthy)
    massive_session.release(broken)


def test_a_result_handed_over_as_the_session_closes_never_starts(
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = massive_session._core
    start = follow_coordinator.FollowCoordinator._start
    started: list[bool] = []

    def closing(
        self: follow_coordinator.FollowCoordinator,
        result: follow_coordinator._Result,
        message: follow_coordinator._Follow,
    ) -> None:
        with core.registry_lock:
            core._closed = True  # as a close does, between the hand-over and the start
        try:
            start(self, result, message)
        finally:
            with core.registry_lock:
                core._closed = False
        started.append(result.ended)

    monkeypatch.setattr(follow_coordinator.FollowCoordinator, "_start", closing)
    script = Script(massive_session)
    result = follow(massive_session, watchlist("AAPL"), script)
    until(lambda: started == [True])
    massive_session.release(result)
    assert script.calls == []


def test_a_coordinator_that_dies_after_its_stop_has_nothing_to_fail(
    massive_session: dm.MassiveSession,
) -> None:
    coordinator = massive_session._core.follows
    coordinator.stop()
    coordinator._died(SystemExit("late"))  # nothing to fail
    assert coordinator.thread is None
    assert coordinator.following == 0


def test_a_flat_file_scan_fails_as_a_unit(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession
) -> None:
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    scan = SymbolFailure(
        MassiveError("HTTP 403: the files are refused", status=403), FailureKind.PERMANENT
    )
    script.answers["MSFT"] = [scan]
    script.answers["NVDA"] = [scan]
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script, one_scan=True)
    until(lambda: script.calls == [("AAPL",)])
    add(wl, "MSFT", "NVDA")
    until(lambda: state(massive_session, "NVDA") is SymbolReadState.QUEUED)
    gate.set()
    msft = wait_state(massive_session, "MSFT", SymbolReadState.FAILED)
    nvda = wait_state(massive_session, "NVDA", SymbolReadState.FAILED)
    assert script.calls == [("AAPL",), ("MSFT", "NVDA")]
    assert msft.error == nvda.error == "HTTP 403: the files are refused"
    massive_session.release(result)


@pytest.mark.parametrize(
    ("label", "wall"),
    [
        ("New York midnight", "2026-11-03T05:00:00Z"),
        ("UTC midnight", "2026-11-03T00:00:00Z"),
        ("17:00 Chicago", "2026-11-02T23:00:00Z"),
        ("the fall-back", "2026-11-01T06:30:00Z"),
    ],
)
def test_never_reuses_its_download_across_any_day_boundary(
    watchlist: Callable[..., InputTable],
    *,
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    label: str,
    wall: str,
) -> None:
    # NEVER reads no clock: crossing a day of any zone, in a JVM whose default zone is Tokyo,
    # changes nothing.
    jpy = pytest.importorskip("jpy")
    zones = jpy.get_type("java.util.TimeZone")
    before = zones.getDefault()
    zones.setDefault(zones.getTimeZone("Asia/Tokyo"))
    try:
        crossing = datetime.fromisoformat(wall).timestamp()
        clock = FakeClock(manual=True, wall=crossing - 60)
        monkeypatch.setattr(follow_coordinator, "CLOCK", clock)
        script = Script(massive_session)
        wl = watchlist("AAPL")
        result = follow(massive_session, wl, script, reread=Reread.NEVER)
        done = wait_state(massive_session, "AAPL", SymbolReadState.DONE)
        clock.advance(120)  # past the boundary
        delete(wl, "AAPL")
        wait_state(massive_session, "AAPL", SymbolReadState.REMOVED)
        add(wl, "AAPL")
        back = wait_state(massive_session, "AAPL", SymbolReadState.DONE)
        assert script.reads_of("AAPL") == 1, label
        assert back.read_at == done.read_at
        assert back.added_at > crossing * 1e9 > done.added_at
        massive_session.release(result)
    finally:
        zones.setDefault(before)


def test_results_opened_and_released_on_many_threads_share_one_coordinator(
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The coordinator thread never changes hands, so its state has one owner on any build.
    prefix = f"{massive_session._core.thread_prefix} follow"
    idents: set[int] = set()
    start = threading.Barrier(8)

    def churn() -> None:
        start.wait(TIMEOUT)
        with entered(massive_session._core.exec_ctx):
            for _ in range(4):
                result = follow(massive_session, watchlist("AAPL"), Script(massive_session))
                thread = massive_session._core.follows.thread
                if thread is not None and thread.ident is not None:
                    idents.add(thread.ident)
                massive_session.release(result)

    with caplog.at_level(logging.ERROR):
        workers = ThreadPoolExecutor(8, thread_name_prefix="churn")
        try:
            for future in [workers.submit(churn) for _ in range(8)]:
                future.result(TIMEOUT)
        finally:
            workers.shutdown(wait=False)
        until(lambda: massive_session._core.follows.following == 0)
    assert len(idents) == 1
    assert len([t for t in threading.enumerate() if t.name == prefix]) == 1
    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []


def test_an_outcome_after_the_stop_token_is_dropped(
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The token is set (a failure or the shutdown) before the result's stop is handled.
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    result = follow(massive_session, watchlist("AAPL"), script)
    until(lambda: script.calls == [("AAPL",)])
    (worker,) = massive_session._core.workers._workers.values()
    fail = worker.fail
    assert isinstance(fail, MethodType)
    handle = fail.__self__
    assert isinstance(handle, FollowedRead)
    handle.stop_token.set()
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        gate.set()
        until(lambda: "a read ended after the stop; dropped" in caplog.text)
    assert state(massive_session, "AAPL") is SymbolReadState.LOADING  # nothing recorded
    massive_session.release(result)


def test_a_read_scheduled_after_the_coordinator_stopped_is_dropped(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The session stops the coordinator while it still handles an add queued before the stop:
    # the add's read finds the coordinator stopped, and is dropped without killing the thread.
    coordinator = massive_session._core.follows
    script = Script(massive_session)
    wl = watchlist()
    result = follow(massive_session, wl, script)
    thread = coordinator.thread
    assert thread is not None
    membership = follow_coordinator.FollowCoordinator._membership
    handling, go = threading.Event(), threading.Event()

    def held(
        self: follow_coordinator.FollowCoordinator,
        followed: follow_coordinator._Result,
        message: follow_coordinator._Baseline | follow_coordinator._Changed,
    ) -> None:
        if isinstance(message, follow_coordinator._Changed):
            handling.set()
            assert go.wait(TIMEOUT)
        membership(self, followed, message)

    monkeypatch.setattr(follow_coordinator.FollowCoordinator, "_membership", held)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        add(wl, "AAPL")
        assert handling.wait(TIMEOUT)
        coordinator.stop()
        go.set()
        thread.join(TIMEOUT)
    assert not thread.is_alive()
    assert "the coordinator stopped; the read of 1 [AAPL] is dropped" in caplog.text
    assert "died" not in caplog.text
    assert script.calls == []
    massive_session.release(result)


def test_a_follow_handed_to_a_dying_coordinator_fails_its_table(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A result handed over after the thread died but before its death was handled queues its
    # start to the dead thread, which never handles it: the death fails it too.
    died = follow_coordinator.FollowCoordinator._died
    dying, go = threading.Event(), threading.Event()

    def slow_death(self: follow_coordinator.FollowCoordinator, death: BaseException) -> None:
        dying.set()
        assert go.wait(TIMEOUT)
        died(self, death)

    # The thread takes its death handler when it starts: replace it first.
    monkeypatch.setattr(follow_coordinator.FollowCoordinator, "_died", slow_death)
    script = Script(massive_session)
    first = follow(massive_session, watchlist("AAPL"), script)
    wait_state(massive_session, "AAPL", SymbolReadState.DONE)

    def dies(_self: follow_coordinator.FollowCoordinator, _touched: object = None) -> None:
        raise SystemExit("the coordinator is told to die")

    escaped: list[threading.ExceptHookArgs] = []
    monkeypatch.setattr(threading, "excepthook", escaped.append)
    monkeypatch.setattr(follow_coordinator.FollowCoordinator, "_retry_due", dies)
    with caplog.at_level(logging.ERROR):
        massive_session._core.follows.wake()
        assert dying.wait(TIMEOUT)
        second = follow(massive_session, watchlist("MSFT"), script)  # to the dead thread
        go.set()
        until(lambda: bool(first.j_table.isFailed()) and bool(second.j_table.isFailed()))
    until(lambda: [args.exc_type for args in escaped] == [SystemExit])
    assert massive_session._core.follows.following == 0
    assert script.calls == [("AAPL",)]


# --- Reads of several symbols, and crypto pairs Massive does not list ---------------------------


def test_a_rest_read_takes_up_to_symbols_per_read_symbols(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    dh_to_pylist: ToList,
) -> None:
    # A fetch() whose list parameter names many symbols a request reads them in groups.
    script = Script(massive_session)
    result = follow(massive_session, watchlist("AAPL", "MSFT", "NVDA"), script, symbols_per_read=2)
    until(lambda: len(dh_to_pylist(result)) == 6)
    assert sorted(len(names) for names in script.calls) == [1, 2]
    assert sorted(name for names in script.calls for name in names) == ["AAPL", "MSFT", "NVDA"]
    massive_session.release(result)


def unlisted() -> SymbolFailure:
    error = UnlistedPairError(
        "'X:DOTUSD' cannot be split: Massive's reference data does not list it"
    )
    return SymbolFailure(error, FailureKind.PERMANENT)


@pytest.mark.parametrize(("setting", "warns"), [(dm.OnError.LOG, True), (dm.OnError.IGNORE, False)])
def test_an_unlisted_pair_fails_alone_and_warns_only_under_log(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    setting: dm.OnError,
    warns: bool,
) -> None:
    core = massive_session._core
    monkeypatch.setattr(core, "config", dataclasses.replace(core.config, unlisted_symbols=setting))
    script = Script(massive_session)
    script.answers["DOT"] = [unlisted()]
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        result = follow(massive_session, watchlist("AAPL", "DOT"), script)
        failed = wait_state(massive_session, "DOT", SymbolReadState.FAILED)
        wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    assert "does not list it" in (failed.error or "")
    assert not result.j_table.isFailed()
    warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert [("DOT failed" in m) for m in warnings] == ([True] if warns else [])
    assert any("DOT failed" in r.getMessage() for r in caplog.records)  # DEBUG at least
    massive_session.release(result)


def test_an_unlisted_pair_fails_the_result_under_raise(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    drain: Callable[[], None],
    dh_to_pylist: ToList,
) -> None:
    core = massive_session._core
    config = dataclasses.replace(core.config, unlisted_symbols=dm.OnError.RAISE)
    monkeypatch.setattr(core, "config", config)
    script = Script(massive_session)
    script.answers["DOT"] = [unlisted()]
    result = follow(massive_session, watchlist("DOT"), script)
    until(lambda: bool(result.j_table.isFailed()))
    shown = stream(massive_session, drain, dh_to_pylist)
    assert shown["State"] == "FAILED"
    assert "does not list it" in shown["Error"]


# --- The call-time symbols of a call that did not wait ----------------------------------------


def test_a_table_whose_call_time_symbols_all_fail_for_good_fails(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    drain: Callable[[], None],
    dh_to_pylist: ToList,
) -> None:
    # A plan without the dataset refuses every symbol: the table fails with the first
    # symbol's error, in name order, as a call that waits raises it.
    script = Script(massive_session)
    script.answers["MSFT"] = [refused(403, "MSFT not in the plan")]
    script.answers["AAPL"] = [refused(403, "AAPL not in the plan")]
    result = follow(massive_session, watchlist("MSFT", "AAPL"), script)
    until(lambda: bool(result.j_table.isFailed()))
    shown = stream(massive_session, drain, dh_to_pylist)
    assert (shown["State"], shown["Error"]) == ("FAILED", "HTTP 403: AAPL not in the plan")
    assert row(massive_session, "AAPL").state is SymbolReadState.FAILED


def test_a_call_time_symbol_counts_as_failed_once_its_retries_run_out(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, clock: FakeClock
) -> None:
    # While AAPL is read again, the table waits; once its last retry fails, every call-time
    # symbol has failed, and the table fails, as a call that waits would have raised.
    script = Script(massive_session)
    script.answers["AAPL"] = [refused(503, "busy")]
    script.answers["MSFT"] = [refused(403, "not in the plan")]
    result = follow(massive_session, watchlist("AAPL", "MSFT"), script)
    wait_state(massive_session, "MSFT", SymbolReadState.FAILED)
    for attempt, delay in enumerate(RETRY_DELAYS, start=1):
        wait_state(massive_session, "AAPL", SymbolReadState.RETRYING)
        assert not result.j_table.isFailed()
        clock.advance(delay)
        massive_session._core.follows.wake()
        until(functools.partial(script.read_count_is, "AAPL", attempt + 1))
    until(lambda: bool(result.j_table.isFailed()))


def test_deleting_an_unsettled_call_time_symbol_keeps_the_table(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession
) -> None:
    # AAPL, still loading, is deleted: the user chose the symbols, so the check is settled.
    # MSFT, the one call-time symbol left, then fails alone, and so does one added later.
    script = Script(massive_session)
    script.gates["AAPL"] = threading.Event()
    script.gates["MSFT"] = threading.Event()
    script.answers["MSFT"] = [refused(403, "not in the plan")]
    script.answers["NVDA"] = [refused(403, "not in the plan")]
    wl = watchlist("AAPL", "MSFT")
    result = follow(massive_session, wl, script)
    until(lambda: state(massive_session, "MSFT") is SymbolReadState.LOADING)
    delete(wl, "AAPL")
    wait_state(massive_session, "AAPL", SymbolReadState.REMOVED)
    script.gates["MSFT"].set()
    wait_state(massive_session, "MSFT", SymbolReadState.FAILED)
    add(wl, "NVDA")
    wait_state(massive_session, "NVDA", SymbolReadState.FAILED)
    script.gates["AAPL"].set()
    assert not result.j_table.isFailed()
    massive_session.release(result)


def test_deleting_a_failed_call_time_symbol_leaves_the_check_to_the_others(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession
) -> None:
    script = Script(massive_session)
    script.gates["AAPL"] = threading.Event()
    script.answers["AAPL"] = [refused(403, "not in the plan")]
    script.answers["MSFT"] = [refused(403, "not in the plan")]
    wl = watchlist("AAPL", "MSFT")
    result = follow(massive_session, wl, script)
    wait_state(massive_session, "MSFT", SymbolReadState.FAILED)
    delete(wl, "MSFT")
    wait_state(massive_session, "MSFT", SymbolReadState.REMOVED)
    assert not result.j_table.isFailed()
    script.gates["AAPL"].set()  # the one call-time symbol left fails: the table fails
    until(lambda: bool(result.j_table.isFailed()))


# --- The coordinator's work grows with the symbols that change --------------------------------


class _CountedSymbol(follow_coordinator._Symbol):
    # A symbol's coordinator state that counts every read of one of its fields.
    looks = 0

    @override
    def __getattribute__(self, name: str) -> object:
        _CountedSymbol.looks += 1
        return super().__getattribute__(name)


class _QuietCore:
    # The core a followed read reports to, reduced to the calls the coordinator makes.
    closed = False
    config = dm.MassiveConfig(api_key="k")

    def __init__(self) -> None:
        self.progress: list[int | None] = []
        self.workers = self  # the worker-table registry's one call, `update_followed_read`

    def write_symbol_reads(self, rows: list[SymbolReadRow]) -> None:
        del rows

    def update_followed_read(self, call: int, update: StreamUpdate, *, loading: bool) -> None:
        del call, loading
        self.progress.append(update.loading)


class _QuietRead:
    # A followed read with no table: it only takes the coordinator's downloads.
    call = 1
    dataset = "test.trades"
    reread = Reread.ALWAYS
    built_step = 0
    shut_down = False

    def __init__(self) -> None:
        self.core = _QuietCore()
        self.plan = FollowPlan(Transport.REST, one_scan=False, symbols_per_read=1, read=self.read)
        self.stop_token = threading.Event()

    @staticmethod
    def read(names: tuple[str, ...], stop: threading.Event) -> Mapping[str, SymbolOutcome]:
        raise AssertionError(names, stop)  # the pool has stopped: nothing is read

    def fail_table(self, error: MassiveError) -> None:
        self.failed = error

    def publish(
        self, name: str, seq: int, table: object, *, decided_at_step: int, parts_before: int
    ) -> int:
        del name, seq, table, decided_at_step
        return parts_before


def _stopped_pool() -> ReadPool:
    # A pool that refuses every read, so a coordinator driven by hand reads nothing.
    pool = ReadPool("stopped", 1)
    pool.stop()
    return pool


@dataclasses.dataclass(frozen=True)
class _TwoRows:
    # A download's rows, as the coordinator reads them: only their count.
    size: int = 2


class _CountedScans(UserDict[str, MassiveError | None]):
    # The call-time symbols' failures, counting each pass over them.
    scans = 0

    @override
    def values(self) -> ValuesView[MassiveError | None]:
        _CountedScans.scans += 1
        return super().values()


def test_the_call_time_check_does_not_rescan_its_symbols(
    dh_exec_ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Regression: each final failure scanned every call-time symbol, so a plan without the
    # dataset failing 20,000 symbols in name order cost 20,000 passes over a growing prefix.
    count = 2_000
    coordinator = follow_coordinator.FollowCoordinator(dh_exec_ctx, "scan test", _stopped_pool())
    quiet = _QuietRead()
    read: FollowedRead = quiet  # type: ignore[assignment]  # a fake that only takes downloads
    handle = coordinator._handle
    names = [f"S{i:04}" for i in range(count)]
    handle([follow_coordinator._Follow(read, None)])
    result = coordinator._results[id(read)]
    handle([follow_coordinator._Baseline(read, frozenset(names), 1)])
    assert result.call_time is not None
    counted: dict[str, MassiveError | None] = _CountedScans(  # type: ignore[assignment]  # counts
        result.call_time
    )
    result.call_time = counted
    monkeypatch.setattr(_CountedScans, "scans", 0)
    for name in names:
        outcome = refused(403, f"{name} not in the plan")
        handle([follow_coordinator._Began(read, {name: 1})])
        handle([follow_coordinator._Outcome(read, {name: 1}, {name: outcome}, 0, 0.0)])
    assert str(quiet.failed) == "HTTP 403: S0000 not in the plan"
    assert _CountedScans.scans <= 1


def test_the_work_per_landed_symbol_does_not_grow_with_the_symbols(
    dh_exec_ctx: ExecutionContext, clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Regression: each batch looked at every symbol of every result (to find due retries, and
    # to count the progress), so 10,000 symbols landing one by one cost 10,000 passes over all
    # 10,000. Now a batch looks at the symbols it changed, plus a constant few.
    count = 2_000
    coordinator = follow_coordinator.FollowCoordinator(dh_exec_ctx, "work test", _stopped_pool())
    quiet = _QuietRead()
    read: FollowedRead = quiet  # type: ignore[assignment]  # a fake that only takes downloads
    handle = coordinator._handle
    names = [f"S{i:04}" for i in range(count)]
    monkeypatch.setattr(follow_coordinator, "_Symbol", _CountedSymbol)
    monkeypatch.setattr(_CountedSymbol, "looks", 0)
    handle([follow_coordinator._Follow(read, None)])
    result = coordinator._results[id(read)]
    handle([follow_coordinator._Baseline(read, frozenset(names), 1)])
    two_rows: Table = _TwoRows()  # type: ignore[assignment]  # a fake table: only its size
    rows = SymbolRows(two_rows)
    failure = refused(503)
    for i, name in enumerate(names):  # every tenth symbol retries once
        handle([follow_coordinator._Began(read, {name: 1})])
        outcome = failure if i % 10 == 0 else rows
        handle([follow_coordinator._Outcome(read, {name: 1}, {name: outcome}, 0, 0.0)])
    assert quiet.core.progress[-1] == count // 10  # the retrying symbols still load
    clock.advance(RETRY_DELAYS[0])
    coordinator._retry_due()  # the retries come due, and each is read and lands
    for name in names[::10]:
        handle([follow_coordinator._Began(read, {name: 1})])
        handle([follow_coordinator._Outcome(read, {name: 1}, {name: rows}, 0, 0.0)])
    assert quiet.core.progress[-1] == 0
    assert result.progress.rows == 2 * count
    # About 60 field reads per symbol; one pass over every symbol per batch made it 54 million.
    assert _CountedSymbol.looks < 100 * count


def test_a_read_that_starts_after_its_symbol_left_leaves_the_symbol_removed(
    dh_exec_ctx: ExecutionContext,
) -> None:
    # A pool thread queues a read's start, and the symbol leaves the table before the
    # coordinator takes it: the start is for an older generation, so the symbol stays REMOVED
    # and the attempt is not counted. The 16-thread stress test hits this only by chance.
    coordinator = follow_coordinator.FollowCoordinator(
        dh_exec_ctx, "late start test", _stopped_pool()
    )
    read: FollowedRead = _QuietRead()  # type: ignore[assignment]  # a fake that takes downloads
    handle = coordinator._handle
    handle([follow_coordinator._Follow(read, None)])
    symbols = coordinator._results[id(read)].symbols
    handle([follow_coordinator._Baseline(read, frozenset({"AAPL"}), 1)])
    started = symbols["AAPL"].generation
    handle([follow_coordinator._Changed(read, frozenset(), frozenset({"AAPL"}), 2)])
    handle([follow_coordinator._Began(read, {"AAPL": started})])
    assert symbols["AAPL"].state is SymbolReadState.REMOVED
    assert symbols["AAPL"].attempts == 0


def test_an_event_the_coordinator_has_no_rule_for_is_a_defect(
    dh_exec_ctx: ExecutionContext,
) -> None:
    # Every event kind has its rule; any other raises, never stops the result unseen.
    coordinator = follow_coordinator.FollowCoordinator(dh_exec_ctx, "rule test", _stopped_pool())
    quiet = _QuietRead()
    read: FollowedRead = quiet  # type: ignore[assignment]  # a fake that only takes downloads
    coordinator._handle([follow_coordinator._Follow(read, None)])
    result = coordinator._results[id(read)]
    with pytest.raises(BrokenGuaranteeError, match=r"^no rule applies 'text' to a result\."):
        coordinator._apply(result, "text")  # type: ignore[arg-type]  # the point: refused at run time
