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
import re
import threading
import weakref
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from types import MethodType
from typing import TYPE_CHECKING, Any

import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.stream.table_publisher import TablePublisher
from deephaven.table import Table
from deephaven.update_graph import exclusive_lock
from typing_extensions import override

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._enums import AssetClass, NotBars, Reread
from deephaven_massive._errors import (
    LIBRARY_BUG,
    BrokenGuaranteeError,
    FeedRefusalError,
    MassiveError,
    session_closed,
)
from deephaven_massive._massive import Transport
from deephaven_massive._tables import follow_coordinator, symbol_retry
from deephaven_massive._tables.diagnostics import (
    FLAT_FILES_AND_REST,
    ConnectionRow,
    Diagnostics,
    StreamRow,
)
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.follow_coordinator import FirstRead
from deephaven_massive._tables.followed_read import FollowedRead, FollowPlan, open_followed_read
from deephaven_massive._tables.read_pool import ReadPool
from deephaven_massive._tables.symbol_read_rows import SymbolReadRow, SymbolReadState
from deephaven_massive._tables.symbol_retry import RETRY_DELAYS, RETRY_EVERY
from deephaven_massive._tables.transports import (
    SymbolFailure,
    SymbolOutcome,
    SymbolRows,
)
from deephaven_massive._tables.watchlist import SymbolRowRule
from tests._fakes.clock import FakeClock
from tests._fakes.followed_read import QuietRead

if TYPE_CHECKING:
    from _collections_abc import dict_values

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


MASSIVE_TEXT = {
    403: "You are not entitled to this data",
    404: "Not Found",
    429: "You've exceeded the maximum requests per minute",
    500: "Internal Server Error",
    503: "Service Unavailable",
}
"""Massive's text for each status the tests answer with."""


def answer(status: int, symbol: str = "AAPL", *, request_id: str | None = None) -> str:
    """Return Massive's answer to the read of `symbol`, as the REST client words it."""
    asked = f" (request_id {request_id})" if request_id else ""
    return (
        f"Massive answered GET /v3/trades/{symbol} on api.massive.com with HTTP {status}{asked}: "
        f'"{MASSIVE_TEXT[status]}".'
    )


FLAT_FILES_403 = (
    "Massive answered the read of s3://flatfiles/us_stocks_sip/trades_v1/2026/11/2026-11-16.csv.gz "
    'at https://files.massive.com with HTTP 403 (request_id R1): "AccessDenied: Access Denied".'
)
"""S3's refusal of a flat-file scan, as the library words it."""


def refused(status: int, symbol: str = "AAPL", *, request_id: str | None = None) -> SymbolFailure:
    text = answer(status, symbol, request_id=request_id)
    return SymbolFailure(MassiveError(text, status=status, request_id=request_id))


def not_entitled(symbol: str = "AAPL") -> SymbolFailure:
    """Return Massive's refusal of `symbol` (HTTP 403), as the REST client raises it."""
    return SymbolFailure(dm.EntitlementError(answer(403, symbol), status=403))


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

    def read(
        self, names: tuple[str, ...], stop: threading.Event | None
    ) -> Mapping[str, SymbolOutcome]:
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
    refused_symbols: dm.OnError = dm.OnError.LOG,
) -> Table:
    core = m._core
    plan = FollowPlan(
        Transport.FLAT_FILES if one_scan else Transport.REST,
        one_scan=one_scan,
        symbols_per_read=symbols_per_read,
        read=script.read,
        policy=dataclasses.replace(core.policy, refused_symbols=refused_symbols),
    )
    return open_followed_read(
        core,
        spec=SPEC,
        call=core.next_stream_id(),
        query="m.test.trades(<table>)",
        symbol_table=table,
        rule=SymbolRowRule.of(SPEC, core.policy.noncanonical_symbols),
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
    script.answers["AAPL"] = [refused(403, request_id="abc")]
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        result = follow(massive_session, watchlist("AAPL", "MSFT"), script)
        failed = wait_state(massive_session, "AAPL", SymbolReadState.FAILED)
        wait_state(massive_session, "MSFT", SymbolReadState.DONE)
    assert failed.error == answer(403, request_id="abc")
    assert (failed.attempts, failed.next_attempt_at, failed.rows) == (1, None, 0)
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert re.fullmatch(
        r"test\.trades call \d+: AAPL failed after 1 attempt and is left out of the result\. "
        + re.escape(
            f"{answer(403, request_id='abc')} To read it again, delete its row from "
            "the symbol table and add it back."
        ),
        warnings[0],
    )
    until(lambda: {r["Ticker"] for r in dh_to_pylist(result)} == {"MSFT"})
    massive_session.release(result)


def settled(
    m: dm.MassiveSession, name: str, *, attempts: int, states: frozenset[SymbolReadState]
) -> bool:
    # Whether the symbol's row shows its `attempts`th read since its add ended in one of `states`.
    found = [r for (_, n), r in reads(m).items() if n == name]
    return bool(found) and found[0].attempts == attempts and found[0].state in states


def retry_after(
    m: dm.MassiveSession, script: Script, *, clock: FakeClock, name: str, earlier: int = 0
) -> float:
    # Waits until the symbol's latest read has failed and it retries, checks the retry does not
    # read before its time, then moves the clock to it; returns the delay it waited. `earlier`
    # is how many reads of it came before its latest add.
    count = script.reads_of(name)
    until(functools.partial(settled, m, name, attempts=count - earlier, states=_RETRYING))
    due = row(m, name).next_attempt_at
    assert due is not None
    delay = round(due / 1e9 - clock.time(), 6)
    clock.advance(delay - 1)
    m._core.follows.wake()
    assert script.reads_of(name) == count  # not due yet
    clock.advance(1)
    m._core.follows.wake()
    until(functools.partial(script.read_count_is, name, count + 1))
    return delay


def run_out_retries(
    m: dm.MassiveSession, script: Script, *, clock: FakeClock, name: str, earlier: int = 0
) -> list[float]:
    # Moves the clock through each retry of a symbol that keeps failing until it is FAILED;
    # returns the delays it waited.
    delays: list[float] = []
    while True:
        attempts = script.reads_of(name) - earlier
        until(functools.partial(settled, m, name, attempts=attempts, states=_SETTLED))
        if state(m, name) is SymbolReadState.FAILED:
            return delays
        delays.append(retry_after(m, script, clock=clock, name=name, earlier=earlier))


_RETRYING = frozenset({SymbolReadState.RETRYING})
_SETTLED = frozenset({SymbolReadState.RETRYING, SymbolReadState.FAILED})


def test_a_transient_failure_retries_soon_then_every_minute_until_max_outage(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, clock: FakeClock
) -> None:
    # Read again 2, 8, and 30 s after the first failures, then every minute; the failure that
    # lands 340 s after the first is the first at or past max_outage (300 s), so it is final.
    assert (RETRY_DELAYS, RETRY_EVERY) == ((2.0, 8.0, 30.0), 60.0)
    assert massive_session._core.config.max_outage == 300.0
    script = Script(massive_session)
    script.answers["AAPL"] = [refused(503)]
    result = follow(massive_session, watchlist("AAPL"), script)
    retrying = wait_state(massive_session, "AAPL", SymbolReadState.RETRYING)
    assert (retrying.attempts, retrying.error) == (1, answer(503))
    delays = run_out_retries(massive_session, script, clock=clock, name="AAPL")
    assert delays == [2.0, 8.0, 30.0, 60.0, 60.0, 60.0, 60.0, 60.0]
    failed = row(massive_session, "AAPL")
    assert (failed.attempts, failed.next_attempt_at) == (9, None)
    assert failed.error == (
        "AAPL has failed for 340 s, as long as max_outage=300 s allows. The latest error: "
        f"{answer(503)}"
    )
    massive_session.release(result)


def test_a_max_outage_of_zero_fails_a_symbol_at_its_first_failure(
    watchlist: Callable[..., InputTable], clock: FakeClock
) -> None:
    del clock  # the run of failures is timed on it, though no time passes
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", max_outage=0))
    try:
        script = Script(m)
        script.answers["AAPL"] = [refused(429)]
        result = follow(m, watchlist("AAPL", "MSFT"), script)
        failed = wait_state(m, "AAPL", SymbolReadState.FAILED)
        assert (failed.attempts, script.reads_of("AAPL")) == (1, 1)
        assert failed.error is not None
        assert failed.error == (
            "AAPL has failed for 0 s, as long as max_outage=0 s allows. The latest error: "
            f"{answer(429)}"
        )
        m.release(result)
    finally:
        m.close()


def test_rows_end_a_run_of_failures_so_the_next_one_starts_over(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    clock: FakeClock,
) -> None:
    # AAPL fails for 220 s, gets rows, then is removed and added back: its new failures start a
    # new run, with the full max_outage and the 2 s first retry.
    script = Script(massive_session)
    failing = refused(503)
    answers: list[Answer] = [failing] * 6
    script.answers["AAPL"] = [*answers, SymbolRows(rows_of(massive_session, "AAPL", 1))]
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, script)
    until(functools.partial(script.read_count_is, "AAPL", 1))
    delays = [retry_after(massive_session, script, clock=clock, name="AAPL") for _ in range(6)]
    assert delays == [2.0, 8.0, 30.0, 60.0, 60.0, 60.0]
    wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    script.answers["AAPL"] = [failing]
    delete(wl, "AAPL")
    wait_state(massive_session, "AAPL", SymbolReadState.REMOVED)
    add(wl, "AAPL")
    until(functools.partial(script.read_count_is, "AAPL", 8))
    delays = run_out_retries(massive_session, script, clock=clock, name="AAPL", earlier=7)
    assert delays == [2.0, 8.0, 30.0, 60.0, 60.0, 60.0, 60.0, 60.0]
    massive_session.release(result)


def test_a_retry_that_succeeds_clears_the_error(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession, clock: FakeClock
) -> None:
    script = Script(massive_session)
    script.answers["AAPL"] = [
        refused(429),
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
    script.answers["AAPL"] = [refused(500), SymbolRows(rows_of(massive_session, "AAPL", 1))]
    monkeypatch.setattr(symbol_retry, "RETRY_DELAYS", (0.05, 0.05, 0.05))
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


@pytest.mark.parametrize("status", [429, 503])
def test_a_run_of_failures_starts_when_its_first_failure_lands(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    clock: FakeClock,
    status: int,
) -> None:
    # A read that took 360 s, longer than max_outage, and then failed has failed once, not for
    # its whole length, even when the failure is a rate-limit refusal: it may have spent that
    # time receiving pages. The symbol retries, and its run counts from that failure.
    first = FirstRead(
        {"AAPL": refused(status)},
        began_at=int(clock.time() * 1e9),
        seconds=360.0,
    )
    script = Script(massive_session)
    script.answers["AAPL"] = [refused(status)]
    result = follow(massive_session, watchlist("AAPL"), script, first=first)
    assert wait_state(massive_session, "AAPL", SymbolReadState.RETRYING).attempts == 1
    delays = run_out_retries(massive_session, script, clock=clock, name="AAPL", earlier=-1)
    assert delays == [2.0, 8.0, 30.0, 60.0, 60.0, 60.0, 60.0, 60.0]
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
    script.answers["NVDA"] = [refused(403, "NVDA")]
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
    with pytest.raises(MassiveError, match="^" + re.escape(session_closed()) + "$"):
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


def troubled(problem_rows: Callable[[], list[dict[str, Any]]]) -> list[tuple[object, ...]]:
    # The read's symbol-read spans as (cause, symbol, detail, ended), in a stable order.
    return [
        (r["Cause"], r["Symbol"], r["Detail"], r["EndedAt"] is not None)
        for r in problem_rows()
        if r["Cause"] in {"symbol_read_failed", "symbol_read_retrying"}
    ]


def test_failed_and_retrying_symbols_are_spans_with_their_errors(
    watchlist: Callable[..., InputTable],
    *,
    massive_session: dm.MassiveSession,
    clock: FakeClock,
    drain: Callable[[], None],
    dh_to_pylist: ToList,
    problem_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    script = Script(massive_session)
    names = ["A", "B", "C", "D", "E", "F", "G"]
    for name in names:
        script.answers[name] = [refused(403, name)]
    script.answers["H"] = [refused(404, "H")]
    script.answers["R"] = [refused(503, "R")]
    result = follow(massive_session, watchlist(*names, "H", "R"), script)
    wait_state(massive_session, "R", SymbolReadState.RETRYING)
    for name in [*names, "H"]:
        wait_state(massive_session, name, SymbolReadState.FAILED)
    expected = [
        *[("symbol_read_failed", name, answer(403, name), False) for name in names],
        ("symbol_read_failed", "H", answer(404, "H"), False),
        ("symbol_read_retrying", "R", answer(503, "R"), False),
    ]
    until(lambda: troubled(problem_rows) == expected)
    # Neither fails the read: its Error names only why it stopped.
    assert stream(massive_session, drain, dh_to_pylist)["Error"] is None
    massive_session.release(result)


_REFUSAL_WARNINGS = {
    dm.OnError.LOG: [
        r"test\.trades table \d+: refused_symbols: AAPL: "
        + re.escape(answer(403).removesuffix("."))
        + r" \(at call \d+\)\. m\.problems\(\) lists it and every later refused_symbols "
        r"problem of this table, which the log shows at DEBUG only\."
    ],
    dm.OnError.IGNORE: [],
}
"""The WARNINGs a followed read's refused symbol logs under each `refused_symbols` setting that
reads the others; under RAISE the table fails instead."""


def test_each_setting_but_raise_has_its_refusal_warnings() -> None:
    assert set(_REFUSAL_WARNINGS) == set(dm.OnError) - {dm.OnError.RAISE}


@pytest.mark.parametrize("setting", list(_REFUSAL_WARNINGS))
def test_a_refused_symbol_is_a_refused_symbols_span_and_fails_only_itself(
    setting: dm.OnError,
    watchlist: Callable[..., InputTable],
    *,
    massive_session: dm.MassiveSession,
    drain: Callable[[], None],
    dh_to_pylist: ToList,
    problem_rows: Callable[[], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Massive refuses AAPL: under a setting that reads the others it alone fails, as a
    # refused_symbols span that carries the setting; the setting decides how loudly the log
    # says so.
    script = Script(massive_session)
    script.answers["AAPL"] = [not_entitled()]
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive"):
        result = follow(massive_session, watchlist("AAPL", "MSFT"), script, refused_symbols=setting)
        failed = wait_state(massive_session, "AAPL", SymbolReadState.FAILED)
        wait_state(massive_session, "MSFT", SymbolReadState.DONE)

        def spans() -> list[tuple[object, ...]]:
            return [
                (r["Cause"], r["Symbol"], r["Detail"], r["Policy"], r["EndedAt"] is not None)
                for r in problem_rows()
                if r["StreamId"] is not None
            ]

        until(lambda: spans() == [("refused_symbols", "AAPL", answer(403), setting.value, False)])
    assert (failed.error, failed.attempts) == (answer(403), 1)
    assert stream(massive_session, drain, dh_to_pylist)["Error"] is None
    until(lambda: {r["Ticker"] for r in dh_to_pylist(result)} == {"MSFT"})
    warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    patterns = _REFUSAL_WARNINGS[setting]
    assert len(warnings) == len(patterns), warnings
    for warning, pattern in zip(warnings, patterns, strict=True):
        assert re.fullmatch(pattern, warning), warning
    massive_session.release(result)


_RAISED = (
    f"{answer(403)} Remove AAPL, or, to read the others, pass "
    "on_error=dm.ErrorPolicy(refused_symbols=dm.OnError.LOG) to the call or to dm.MassiveConfig."
)
"""The Error of a followed read whose refused AAPL fails it under `refused_symbols=RAISE`."""


def test_a_refused_symbol_fails_the_table_under_raise(
    watchlist: Callable[..., InputTable],
    *,
    massive_session: dm.MassiveSession,
    drain: Callable[[], None],
    dh_to_pylist: ToList,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # MSFT's read is held, so the table may still read another symbol: AAPL's refusal fails it
    # as RAISE asks, with the setting that reads the others, and the table's failure is the
    # one WARNING.
    script = Script(massive_session)
    script.answers["AAPL"] = [not_entitled()]
    gate = script.gates["MSFT"] = threading.Event()
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive"):
        result = follow(
            massive_session,
            watchlist("AAPL", "MSFT"),
            script,
            refused_symbols=dm.OnError.RAISE,
        )
        until(lambda: bool(result.j_table.isFailed()))
        wait_state(massive_session, "AAPL", SymbolReadState.FAILED)
    gate.set()
    row = stream(massive_session, drain, dh_to_pylist)
    assert row["Error"] == _RAISED
    warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert warnings == [
        f"test.trades call {row['StreamId']}: the read failed, and so did its table: {_RAISED}"
    ]


def test_a_table_whose_every_call_time_symbol_is_refused_fails_with_the_refusal_under_raise(
    watchlist: Callable[..., InputTable],
    *,
    massive_session: dm.MassiveSession,
    drain: Callable[[], None],
    dh_to_pylist: ToList,
) -> None:
    # No other symbol is left to read, so the refusal fails the table as it is, as under any
    # setting.
    script = Script(massive_session)
    script.answers["AAPL"] = [not_entitled()]
    result = follow(massive_session, watchlist("AAPL"), script, refused_symbols=dm.OnError.RAISE)
    until(lambda: bool(result.j_table.isFailed()))
    assert stream(massive_session, drain, dh_to_pylist)["Error"] == answer(403)


def test_a_feed_refusal_of_a_symbol_is_a_failed_read_not_a_refused_symbol(
    watchlist: Callable[..., InputTable],
    *,
    massive_session: dm.MassiveSession,
    problem_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    # A refusal of the feed is no symbol's, so a symbol that met one is symbol_read_failed.
    script = Script(massive_session)
    script.answers["AAPL"] = [SymbolFailure(FeedRefusalError(answer(403), status=403))]
    result = follow(massive_session, watchlist("AAPL", "MSFT"), script)
    wait_state(massive_session, "AAPL", SymbolReadState.FAILED)
    until(lambda: troubled(problem_rows) == [("symbol_read_failed", "AAPL", answer(403), False)])
    massive_session.release(result)


def test_a_retrying_symbol_removed_from_the_watchlist_ends_its_span(
    watchlist: Callable[..., InputTable],
    *,
    massive_session: dm.MassiveSession,
    problem_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    # AAPL waits for its retry when its row is deleted: no read of it is missing any more, so
    # its span ends, and no failed span opens. MSFT lands, so the table lives on.
    script = Script(massive_session)
    script.answers["AAPL"] = [refused(503)]
    wl = watchlist("AAPL", "MSFT")
    result = follow(massive_session, wl, script)
    wait_state(massive_session, "MSFT", SymbolReadState.DONE)
    wait_state(massive_session, "AAPL", SymbolReadState.RETRYING)
    until(lambda: troubled(problem_rows) == [("symbol_read_retrying", "AAPL", answer(503), False)])
    delete(wl, "AAPL")
    wait_state(massive_session, "AAPL", SymbolReadState.REMOVED)
    until(lambda: troubled(problem_rows) == [("symbol_read_retrying", "AAPL", answer(503), True)])
    massive_session.release(result)


def test_a_retrying_symbol_s_span_ends_when_its_retry_lands(
    watchlist: Callable[..., InputTable],
    *,
    massive_session: dm.MassiveSession,
    clock: FakeClock,
    problem_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    script = Script(massive_session)
    script.answers["AAPL"] = [refused(503), SymbolRows(rows_of(massive_session, "AAPL", 1))]
    result = follow(massive_session, watchlist("AAPL"), script)
    wait_state(massive_session, "AAPL", SymbolReadState.RETRYING)
    retrying = ("symbol_read_retrying", "AAPL", answer(503))
    until(lambda: troubled(problem_rows) == [(*retrying, False)])
    clock.advance(RETRY_DELAYS[0])
    massive_session._core.follows.wake()
    wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    # The span ends, and no failed span opens: the symbol was read.
    until(lambda: troubled(problem_rows) == [(*retrying, True)])
    massive_session.release(result)


def test_a_retrying_symbol_that_gives_up_ends_its_span_and_opens_a_failed_one(
    watchlist: Callable[..., InputTable],
    *,
    massive_session: dm.MassiveSession,
    clock: FakeClock,
    problem_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    script = Script(massive_session)
    script.answers["AAPL"] = [refused(503)]
    # MSFT lands, so the table outlives AAPL's failure.
    result = follow(massive_session, watchlist("AAPL", "MSFT"), script)
    wait_state(massive_session, "MSFT", SymbolReadState.DONE)
    # The retries run on their schedule until AAPL has failed for max_outage.
    schedule: list[float] = []
    while sum(schedule) < massive_session._core.config.max_outage:
        schedule.append(
            RETRY_DELAYS[len(schedule)] if len(schedule) < len(RETRY_DELAYS) else RETRY_EVERY
        )
    for attempt, delay in enumerate(schedule, start=1):
        wait_state(massive_session, "AAPL", SymbolReadState.RETRYING)
        clock.advance(delay)
        massive_session._core.follows.wake()
        until(functools.partial(script.read_count_is, "AAPL", attempt + 1))
    wait_state(massive_session, "AAPL", SymbolReadState.FAILED)
    # Each wait for a retry is a span of its own, ended once the retry starts; the last one
    # gives up, and the failed span stays open.
    gave_up = (
        f"AAPL has failed for {sum(schedule):.0f} s, as long as "
        f"max_outage={massive_session._core.config.max_outage:.0f} s allows. The latest error: "
        f"{answer(503)}"
    )
    expected = [
        ("symbol_read_failed", "AAPL", gave_up, False),
        *[("symbol_read_retrying", "AAPL", answer(503), True)] * len(schedule),
    ]
    until(lambda: troubled(problem_rows) == expected)
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
    problem_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    script = Script(massive_session)
    script.answers["NVDA"] = [refused(403, "NVDA")]
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
    failed = ("symbol_read_failed", "NVDA", answer(403, "NVDA"))
    until(lambda: troubled(problem_rows) == [(*failed, False)])
    delete(wl, "AAPL", "NVDA")  # a landed and a failed symbol leave: so do their counts
    until(lambda: stream(massive_session, drain, dh_to_pylist)["SymbolCount"] == 2)
    after = stream(massive_session, drain, dh_to_pylist)
    assert (after["Symbols"], after["RowsPublished"], after["Loading"]) == ("MSFT, TSLA", 2, 1)
    until(lambda: troubled(problem_rows) == [(*failed, True)])  # its span ends with it
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
    assert "A library thread died, so its session's live tables fail. " in caplog.text
    until(lambda: [args.exc_type for args in escaped] == [SystemExit])
    assert massive_session._core.follows.following == 0
    with massive_session._core.registry_lock:
        errors = [w.row.error for w in massive_session._core.workers._workers.values()]
    assert all(
        (e or "").startswith("The library thread that follows symbol tables died: SystemExit(")
        for e in errors
    )


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
        "test.trades: AAPL's rows from REST lack Price", source=dm.ErrorSource.LIBRARY
    )
    script = Script(massive_session)
    script.answers["AAPL"] = [SymbolFailure(defect)]
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        result = follow(massive_session, watchlist("AAPL", "MSFT"), script)
        wait_state(massive_session, "AAPL", SymbolReadState.FAILED)
        wait_state(massive_session, "MSFT", SymbolReadState.DONE)
    assert [r.levelno for r in caplog.records] == [logging.ERROR]
    assert "This is a bug in deephaven-massive" in caplog.records[0].getMessage()
    assert caplog.records[0].exc_info is not None  # a bug, with its traceback
    assert not result.j_table.isFailed()
    until(lambda: {r["Ticker"] for r in dh_to_pylist(result)} == {"MSFT"})
    massive_session.release(result)


def test_two_bad_rows_fail_the_table_once(
    watchlist: Callable[..., InputTable],
    *,
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = dataclasses.replace(
        massive_session._core.config, on_error=dm.ErrorPolicy(noncanonical_symbols=dm.OnError.RAISE)
    )
    monkeypatch.setattr(massive_session._core, "config", config)
    wl = watchlist("AAPL")
    result = follow(massive_session, wl, Script(massive_session))
    wait_state(massive_session, "AAPL", SymbolReadState.DONE)
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        add(wl, "msft", "nvda")  # two refusals in one update: the second finds the table failed
        until(lambda: bool(result.j_table.isFailed()))
        wait_state(massive_session, "AAPL", SymbolReadState.STOPPED)
    # The record names the dataset once, though the failure's text starts with it too.
    [record] = caplog.records
    assert re.fullmatch(
        r"test\.trades call \d+: the read failed, and so did its table: "
        + re.escape(
            "A symbol table holds 'msft'. 'msft' is not a Massive stock ticker, because "
            "Massive's tickers are case-sensitive. If you mean 'MSFT', pass 'MSFT'. Fix the "
            "row, or convert the table with dm.canonicalize_symbols(table, "
            "dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO), naming the "
            "convention your tickers come from."
        ),
        record.getMessage(),
    )


def test_a_read_stopped_by_a_release_leaves_its_symbol_stopped(
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
) -> None:
    # The read sees the stop token and fails as reads do then; that failure is not the symbol's.
    script = Script(massive_session)
    gate = script.gates["AAPL"] = threading.Event()
    script.answers["AAPL"] = [refused(503)]
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
    assert ": handling _" in caplog.text  # its start's set, or the later change
    until(lambda: {r["Ticker"] for r in dh_to_pylist(healthy)} == {"AAPL", "MSFT"})
    with massive_session._core.registry_lock:
        error = massive_session._core.workers._workers[broken_id].row.error or ""
    assert "RuntimeError('a defect')" in error
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
    assert f"test.trades call {broken_id}: {step} failed. {LIBRARY_BUG}" in caplog.text
    with massive_session._core.registry_lock:
        error = massive_session._core.workers._workers[broken_id].row.error or ""
    assert error == (
        f"test.trades: the read that follows its symbol table failed while {step}: "
        f"RuntimeError('a defect in {method}'). {LIBRARY_BUG}"
    )
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
        until(
            lambda: (
                "stopping its table after writing its rows failed raised an error too"
                in caplog.text
            )
        )
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
    scan = SymbolFailure(MassiveError(FLAT_FILES_403, status=403, request_id="R1"))
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
    assert msft.error == nvda.error == FLAT_FILES_403
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
    script.answers["MSFT"] = [refused(403, "MSFT")]
    script.answers["AAPL"] = [refused(403)]
    result = follow(massive_session, watchlist("MSFT", "AAPL"), script)
    until(lambda: bool(result.j_table.isFailed()))
    shown = stream(massive_session, drain, dh_to_pylist)
    assert (shown["State"], shown["Error"]) == ("FAILED", answer(403))
    assert row(massive_session, "AAPL").state is SymbolReadState.FAILED


def test_a_call_time_symbol_counts_as_failed_once_it_has_failed_for_max_outage(
    *,
    watchlist: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # While AAPL is read again, the table waits; once it has failed for max_outage, every call-time
    # symbol has failed, and the table fails, as a call that waits would have raised.
    failures: list[Exception] = []
    publish = TablePublisher.publish_failure

    def recording(publisher: TablePublisher, error: Exception) -> None:
        failures.append(error)
        publish(publisher, error)

    monkeypatch.setattr(TablePublisher, "publish_failure", recording)
    script = Script(massive_session)
    script.answers["AAPL"] = [refused(503)]
    script.answers["MSFT"] = [refused(403, "MSFT")]
    result = follow(massive_session, watchlist("AAPL", "MSFT"), script)
    wait_state(massive_session, "MSFT", SymbolReadState.FAILED)
    wait_state(massive_session, "AAPL", SymbolReadState.RETRYING)
    assert not result.j_table.isFailed()
    run_out_retries(massive_session, script, clock=clock, name="AAPL")
    until(lambda: bool(result.j_table.isFailed()))
    # The table fails with AAPL's error, the first by name, which names max_outage; calling
    # again cannot help MSFT, so it is not retryable, though AAPL's HTTP 503 alone would be.
    (failure,) = failures
    assert isinstance(failure, dm.MassiveError)
    assert (failure.status, failure.is_retryable) == (503, False)
    assert "as long as max_outage=300 s allows" in str(failure)


def test_deleting_an_unsettled_call_time_symbol_keeps_the_table(
    watchlist: Callable[..., InputTable], massive_session: dm.MassiveSession
) -> None:
    # AAPL, still loading, is deleted: the user chose the symbols, so the check is settled.
    # MSFT, the one call-time symbol left, then fails alone, and so does one added later.
    script = Script(massive_session)
    script.gates["AAPL"] = threading.Event()
    script.gates["MSFT"] = threading.Event()
    script.answers["MSFT"] = [refused(403, "MSFT")]
    script.answers["NVDA"] = [refused(403, "NVDA")]
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
    script.answers["AAPL"] = [refused(403)]
    script.answers["MSFT"] = [refused(403, "MSFT")]
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


def _stopped_pool() -> ReadPool:
    # A pool that refuses every read, so a coordinator driven by hand reads nothing.
    pool = ReadPool("stopped", 1)
    pool.stop()
    return pool


@dataclasses.dataclass(frozen=True)
class _TwoRows:
    # A download's rows, as the coordinator reads them: only their count.
    size: int = 2


class _CountedScans(dict[str, MassiveError | None]):
    # The call-time symbols' failures, counting each pass over them.
    scans = 0

    @override
    def values(self) -> dict_values[str, MassiveError | None]:
        _CountedScans.scans += 1
        return super().values()


def test_the_call_time_check_does_not_rescan_its_symbols(
    dh_exec_ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Regression: each final failure scanned every call-time symbol, so a plan without the
    # dataset failing 20,000 symbols in name order cost 20,000 passes over a growing prefix.
    count = 2_000
    coordinator = follow_coordinator.FollowCoordinator(dh_exec_ctx, "scan test", _stopped_pool())
    read = QuietRead()
    handle = coordinator._handle
    names = [f"S{i:04}" for i in range(count)]
    handle([follow_coordinator._Follow(read, None)])
    result = coordinator._results[id(read)]
    handle([follow_coordinator._Baseline(read, frozenset(names), 1)])
    assert result.call_time is not None
    result.call_time = _CountedScans(result.call_time)
    monkeypatch.setattr(_CountedScans, "scans", 0)
    for name in names:
        outcome = refused(403, name)
        handle([follow_coordinator._Began(read, {name: 1})])
        handle([follow_coordinator._Outcome(read, {name: 1}, {name: outcome}, 0, 0.0)])
    assert str(read.failed) == answer(403, "S0000")
    assert _CountedScans.scans <= 1


def test_the_work_per_landed_symbol_does_not_grow_with_the_symbols(
    dh_exec_ctx: ExecutionContext, clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Regression: each batch looked at every symbol of every result (to find due retries, and
    # to count the progress), so 10,000 symbols landing one by one cost 10,000 passes over all
    # 10,000. Now a batch looks at the symbols it changed, plus a constant few.
    count = 2_000
    coordinator = follow_coordinator.FollowCoordinator(dh_exec_ctx, "work test", _stopped_pool())
    read = QuietRead()
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
    assert read.core.progress[-1] == count // 10  # the retrying symbols still load
    clock.advance(RETRY_DELAYS[0])
    coordinator._retry_due()  # the retries come due, and each is read and lands
    for name in names[::10]:
        handle([follow_coordinator._Began(read, {name: 1})])
        handle([follow_coordinator._Outcome(read, {name: 1}, {name: rows}, 0, 0.0)])
    assert read.core.progress[-1] == 0
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
    read = QuietRead()
    handle = coordinator._handle
    handle([follow_coordinator._Follow(read, None)])
    symbols = coordinator._results[id(read)].symbols
    handle([follow_coordinator._Baseline(read, frozenset({"AAPL"}), 1)])
    started = symbols["AAPL"].generation
    handle([follow_coordinator._Changed(read, frozenset(), frozenset({"AAPL"}), 2)])
    handle([follow_coordinator._Began(read, {"AAPL": started})])
    assert symbols["AAPL"].run.state is SymbolReadState.REMOVED
    assert symbols["AAPL"].run.attempts == 0


class _ShutsDownAtOnce(QuietRead):
    # A read whose table shuts down as its failure is published: the update-graph thread can
    # deliver the failure, and run the shutdown callback, before the coordinator's batch ends.
    @override
    def fail_table(self, error: MassiveError, /) -> bool:
        self.shut_down = True
        return super().fail_table(error)


def test_a_table_shut_down_before_its_batch_ends_still_stops_its_symbols(
    dh_exec_ctx: ExecutionContext,
) -> None:
    # Regression: a bad row under RAISE failed the table, and the failure's delivery shut it
    # down (setting `shut_down` and queuing the stop) while the coordinator was finishing the
    # same batch, whose last step forgot the result as ended and shut down. The stop then found
    # no result and was dropped, so the symbols never showed STOPPED. The result is forgotten
    # only once its stop is applied.
    coordinator = follow_coordinator.FollowCoordinator(
        dh_exec_ctx, "shutdown test", _stopped_pool()
    )
    read = _ShutsDownAtOnce()
    handle = coordinator._handle
    handle([follow_coordinator._Follow(read, None)])
    result = coordinator._results[id(read)]
    handle([follow_coordinator._Baseline(read, frozenset({"AAPL"}), 1)])
    handle([follow_coordinator._Invalid(read, "a bad row")])
    assert str(read.failed) == "a bad row"
    assert read.shut_down
    assert result.ended
    assert coordinator._results == {id(read): result}  # kept until its stop is applied
    handle([follow_coordinator._Stop(read)])
    assert result.symbols["AAPL"].run.state is SymbolReadState.STOPPED
    assert not coordinator._results


def test_a_result_stopped_before_its_table_shuts_down_is_kept_until_then(
    dh_exec_ctx: ExecutionContext,
) -> None:
    # The session's close stops a result before its table shuts down: the stop is applied, and
    # the result is kept until the shutdown's own stop, which changes nothing more.
    coordinator = follow_coordinator.FollowCoordinator(dh_exec_ctx, "kept test", _stopped_pool())
    read = QuietRead()
    handle = coordinator._handle
    handle([follow_coordinator._Follow(read, None)])
    result = coordinator._results[id(read)]
    handle([follow_coordinator._Baseline(read, frozenset({"AAPL"}), 1)])
    handle([follow_coordinator._Stop(read)])
    assert result.symbols["AAPL"].run.state is SymbolReadState.STOPPED
    assert coordinator._results == {id(read): result}
    read.shut_down = True
    handle([follow_coordinator._Stop(read)])
    assert result.symbols["AAPL"].run.state is SymbolReadState.STOPPED
    assert not coordinator._results


def test_a_bug_is_a_cause_of_its_own() -> None:
    # A bug never shares the one WARNING of a failure that is none, so it logs its ERROR.
    lost = dm.MassiveError("connection reset", source=dm.ErrorSource.NETWORK)
    broken = dm.MassiveError("a row broke", source=dm.ErrorSource.LIBRARY)
    misuse = dm.MassiveError("your table failed", source=dm.ErrorSource.USER, is_bug=True)
    refused = dm.EntitlementError("HTTP 403", status=403)
    keys = [follow_coordinator._cause(e) for e in (lost, broken, misuse, refused)]
    assert keys == [
        "MassiveError network",
        "MassiveError library bug",
        "MassiveError user bug",
        "EntitlementError massive 403",
    ]


def test_an_event_the_coordinator_has_no_rule_for_is_a_defect(
    dh_exec_ctx: ExecutionContext,
) -> None:
    # Every event kind has its rule; any other raises, never stops the result unseen.
    coordinator = follow_coordinator.FollowCoordinator(dh_exec_ctx, "rule test", _stopped_pool())
    read = QuietRead()
    coordinator._handle([follow_coordinator._Follow(read, None)])
    result = coordinator._results[id(read)]
    with pytest.raises(BrokenGuaranteeError, match=r"^no rule applies 'text' to a result\."):
        coordinator._apply(result, "text")  # type: ignore[arg-type]  # the point: refused at run time


def test_a_retry_step_no_case_handles_fails_the_result_as_a_defect(
    dh_exec_ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    coordinator = follow_coordinator.FollowCoordinator(dh_exec_ctx, "step test", _stopped_pool())
    read = QuietRead()
    handle = coordinator._handle
    handle([follow_coordinator._Follow(read, None)])
    handle([follow_coordinator._Baseline(read, frozenset({"AAPL"}), 1)])
    handle([follow_coordinator._Began(read, {"AAPL": 1})])
    monkeypatch.setattr(symbol_retry.SymbolRun, "failed", lambda *_, **__: "bogus")
    handle([follow_coordinator._Outcome(read, {"AAPL": 1}, {"AAPL": refused(503)}, 0, 0.0)])
    assert read.failed is not None
    assert read.failed.is_bug
    assert "bogus" in str(read.failed)
