"""Tests for `poll()` tables: the three modes, the cycle's failures, rows, and lifetime."""

import dataclasses
import functools
import gc
import logging
import queue
import threading
import weakref
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Any, cast

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, input_table, new_table
from deephaven.column import string_col
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive import _massive, _session
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.symbols import NormalizedSymbols, SymbolTable
from deephaven_massive._enums import AssetClass, PollMode
from deephaven_massive._errors import PermanentError
from deephaven_massive._ingest import poll
from deephaven_massive._ingest.poll import LIVE_COLUMN, Pollers, RowDataError, open_poll
from deephaven_massive._massive import DriftReport, Transport
from deephaven_massive._namespaces.base import build_query
from deephaven_massive._query import Query
from deephaven_massive._tables.liveness import ResultRegistry, new_scope
from tests._fakes.clock import FakeClock

# The reply and envelope builders are plain functions of the fake, which no fixture can hand
# over; the server itself comes from the `fake_rest` fixture.
from tests._fakes.rest import FakeRestServer, Reply, envelope

R = Transport.REST
TIMEOUT = 30.0
LOGGER = "deephaven_massive._ingest.poll"
PATH = "/stocks/v1/dividends"
DIVIDENDS = DatasetSpec(
    summary="A dataset for the tests.",
    name="stocks.dividends",
    asset_class=AssetClass.STOCKS,
    fetch_path=PATH,
    symbol_param="ticker.any_of",
    keys={(R, PATH): SourceKeys(symbol=KeySpec("Ticker"))},
)
ToPylist = Callable[[Table], list[dict[str, Any]]]


class Script:
    # A cycle reader that answers each cycle from a queue of outcomes: rows or an exception.
    def __init__(self, m: dm.MassiveSession) -> None:
        self.schema = m._core.catalog.dataset_schema(DIVIDENDS).arrow_schema()
        self.outcomes: queue.SimpleQueue[object] = queue.SimpleQueue()
        self.symbols: list[NormalizedSymbols | None] = []
        self.drift: DriftReport | None = None

    def rows(self, *pairs: tuple[str, float]) -> None:
        data = [{"Symbol": symbol, "CashAmount": amount} for symbol, amount in pairs]
        self.outcomes.put(pa.Table.from_pylist(data, schema=self.schema))

    def fail(self, error: BaseException) -> None:
        self.outcomes.put(error)

    def __call__(
        self,
        *,
        symbols: NormalizedSymbols | None,
        record: Callable[[DriftReport], None],
        executor: ThreadPoolExecutor,
    ) -> tuple[pa.Table, int]:
        self.symbols.append(symbols)
        if self.drift is not None:
            record(self.drift)
        outcome = self.outcomes.get(timeout=TIMEOUT)
        if isinstance(outcome, BaseException):
            raise outcome
        return cast("pa.Table", outcome), 3


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(manual=True)


@pytest.fixture
def script(massive_session: dm.MassiveSession) -> Script:
    return Script(massive_session)


@pytest.fixture
def pollers(massive_session: dm.MassiveSession) -> Pollers:
    return Pollers(massive_session._core.label)


def start(
    m: dm.MassiveSession,
    *,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    mode: PollMode = PollMode.REPLACE,
    symbols: NormalizedSymbols | None = ("AAPL", "MSFT"),
) -> Table:
    return open_poll(
        m._core,
        spec=DIVIDENDS,
        request=Request(symbols=symbols),
        interval=timedelta(seconds=60),
        mode=mode,
        read=script,
        pollers=pollers,
        host="api.massive.com",
        path=PATH,
        clock=clock,
    )


def until(condition: Callable[[], bool]) -> None:
    # Waits for the update graph to apply a published cycle; fails after TIMEOUT.
    event = threading.Event()
    for _ in range(int(TIMEOUT / 0.01)):
        if condition():
            return
        event.wait(0.01)
    assert condition()


def pairs(table: Table, to_pylist: ToPylist) -> list[tuple[str, float]]:
    return sorted((row["Symbol"], row["CashAmount"]) for row in to_pylist(table))


def next_cycle(clock: FakeClock) -> None:
    # Ends the wait after a cycle and returns once the next cycle has run and waits again.
    clock.wait_for_sleepers(1)
    waits = len(clock.waits)
    clock.advance(60)
    until(lambda: len(clock.waits) > waits)


def fail_one_poll_conversion(monkeypatch: pytest.MonkeyPatch) -> None:
    # Makes the next Arrow-to-Deephaven conversion on a poll thread fail once. The patch is
    # process-wide and the diagnostics thread converts its batches through the same function,
    # so a failure any thread could take would race the poller for it.
    convert = dh_arrow.to_table
    failures = [RuntimeError("conversion failed")]

    def flaky(data: pa.Table) -> Table:
        if failures and " poll #" in threading.current_thread().name:
            raise failures.pop()
        return convert(data)

    monkeypatch.setattr(dh_arrow, "to_table", flaky)


def poller_of(pollers: Pollers) -> poll.Poller:
    (one,) = tuple(pollers._live)
    return one


# --- Modes ---------------------------------------------------------------------------------------


def test_replace_evicts_keys_absent_from_the_latest_cycle(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
) -> None:
    script.rows(("AAPL", 1.0), ("MSFT", 2.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    assert LIVE_COLUMN not in table.column_names
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 1.0), ("MSFT", 2.0)])
    script.rows(("AAPL", 3.0))
    next_cycle(clock)
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 3.0)])
    script.rows()  # an empty cycle evicts every key
    next_cycle(clock)
    until(lambda: pairs(table, dh_to_pylist) == [])
    script.rows(("MSFT", 4.0))
    next_cycle(clock)
    until(lambda: pairs(table, dh_to_pylist) == [("MSFT", 4.0)])


def test_upsert_keeps_every_key(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
) -> None:
    script.rows(("AAPL", 1.0), ("MSFT", 2.0))
    table = start(
        massive_session, script=script, pollers=pollers, clock=clock, mode=PollMode.UPSERT
    )
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 1.0), ("MSFT", 2.0)])
    script.rows(("AAPL", 3.0))
    next_cycle(clock)
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 3.0), ("MSFT", 2.0)])


def test_upsert_keyed_on_every_column_keeps_each_distinct_row(
    dh_exec_ctx: object,
    rows_when: Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]],
) -> None:
    # Every vendor column of the related companies is a poll key; only the arrival time is
    # left to aggregate, so the table holds each distinct row once, across cycles, with the
    # time of the latest cycle that returned it.
    spec = _session.CATALOG.by_name["reference.related_companies"]
    schema = _session.CATALOG.dataset_schema(spec)
    assert {*spec.poll_keys, "TimestampInsert"} == set(schema.column_names)
    scope = new_scope()
    with scope.open():
        table, publisher = poll.build_table(
            spec, schema, PollMode.UPSERT, stream_id=1, stop=threading.Event()
        )

    def cycle(arrived: int, *pairs: tuple[str, str]) -> None:
        names = ("Underlying", "Symbol")
        data = {n: [p[i] for p in pairs] for i, n in enumerate(names)}
        stamps = pa.array([arrived] * len(pairs), pa.int64()).cast(pa.timestamp("ns", "UTC"))
        table = pa.table({**data, "TimestampInsert": stamps})
        publisher.add(dh_arrow.to_table(table.select(list(schema.column_names))))

    def held(rows: list[dict[str, Any]]) -> list[tuple[str, str, int]]:
        return sorted(
            (row["Underlying"], row["Symbol"], int(row["TimestampInsert"].value)) for row in rows
        )

    cycle(1, ("AAPL", "MSFT"), ("AAPL", "GOOGL"), ("AAPL", "MSFT"))
    rows_when(table, lambda rows: held(rows) == [("AAPL", "GOOGL", 1), ("AAPL", "MSFT", 1)])
    cycle(2, ("AAPL", "MSFT"), ("AAPL", "AMZN"))  # GOOGL absent: UPSERT never deletes
    expected = [("AAPL", "AMZN", 2), ("AAPL", "GOOGL", 1), ("AAPL", "MSFT", 2)]
    rows = rows_when(table, lambda rows: held(rows) == expected)
    assert len(rows) == 3
    scope.release()


def test_append_appends_only_keys_not_seen_before(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
) -> None:
    # The test dataset's poll key is the default `Symbol`: one row per symbol is appended.
    script.rows(("AAPL", 1.0))
    table = start(
        massive_session, script=script, pollers=pollers, clock=clock, mode=PollMode.APPEND
    )
    until(lambda: len(dh_to_pylist(table)) == 1)
    script.rows(("AAPL", 5.0), ("MSFT", 2.0))
    next_cycle(clock)
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 1.0), ("MSFT", 2.0)])
    # The wait between cycles counts from each cycle's start.
    assert all(wait <= 60 for wait in clock.waits)


@pytest.mark.parametrize("mode", list(PollMode))
def test_every_mode_is_described_and_ticks(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
    mode: PollMode,
) -> None:
    script.rows(("AAPL", 1.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock, mode=mode)
    schema = massive_session._core.catalog.dataset_schema(DIVIDENDS)
    found = table.attributes()["ColumnDescriptions"]
    described = {str(e.getKey()): str(e.getValue()) for e in found.entrySet().toArray()}
    assert described == {c.name: c.description for c in schema.columns if c.description}
    assert described  # the generated dividends path describes its fields
    assert table.attributes()["TableDescription"] == "stocks.dividends: A dataset for the tests."
    # The described copy ticks with the shape the poller publishes to.
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 1.0)])
    massive_session.release(table)


# --- Rows and failures ---------------------------------------------------------------------------


def test_the_streams_row_follows_the_cycles_and_the_release(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
    drain: Callable[[], None],
) -> None:
    script.drift = DriftReport(extra=frozenset({"brand_new"}))
    script.rows(("AAPL", 1.0), ("MSFT", 2.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    clock.wait_for_sleepers(1)
    (row,) = stream_rows()
    assert row["StreamId"] == 1
    assert (row["Dataset"], row["Transport"], row["Host"], row["Path"]) == (
        "stocks.dividends",
        "REST",
        "api.massive.com",
        PATH,
    )
    assert (row["SymbolCount"], row["Symbols"], row["HeldResults"]) == (2, "AAPL, MSFT", 1)
    assert (row["RowsPublished"], row["RequestsPerCycle"], row["State"]) == (2, 3, "RUNNING")
    assert row["SchemaDrift"] == "REST:brand_new+"
    poller = poller_of(pollers)
    massive_session.release(table)
    drain()
    (row,) = stream_rows()
    assert (row["State"], row["HeldResults"]) == ("RELEASED", 0)
    assert poller.stopped
    clock.advance(60)
    poller.thread.join(TIMEOUT)
    assert not poller.thread.is_alive()
    assert pollers._live == set()


def test_an_entitlement_error_fails_the_table_and_ends_the_poller(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    script.fail(dm.EntitlementError("HTTP 403 from GET /stocks/v1/dividends"))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    poller = poller_of(pollers)
    poller.thread.join(TIMEOUT)
    assert not poller.thread.is_alive()
    until(lambda: bool(table.j_table.isFailed()))
    (row,) = stream_rows()
    assert (row["State"], row["Error"]) == ("FAILED", "HTTP 403 from GET /stocks/v1/dividends")
    massive_session.release(table)
    assert stream_rows()[0]["State"] == "FAILED"  # a failed row keeps its state


@pytest.mark.parametrize(
    "error",
    [dm.AuthenticationError("HTTP 401"), RowDataError("stocks.dividends: row 0 lacks ticker")],
)
def test_authentication_and_data_failures_fail_the_table(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    error: dm.MassiveError,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    script.fail(error)
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    poller_of(pollers).thread.join(TIMEOUT)
    assert stream_rows()[0]["State"] == "FAILED"
    until(lambda: bool(table.j_table.isFailed()))


def test_other_errors_keep_the_rows_and_the_next_cycle_clears_them(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
    stream_rows: Callable[[], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    script.rows(("AAPL", 1.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 1.0)])
    script.fail(dm.MassiveError("HTTP 503 from GET /stocks/v1/dividends"))
    with caplog.at_level(logging.ERROR, LOGGER):
        next_cycle(clock)
    (row,) = stream_rows()
    assert (row["State"], row["Error"]) == ("RUNNING", "HTTP 503 from GET /stocks/v1/dividends")
    assert pairs(table, dh_to_pylist) == [("AAPL", 1.0)]
    assert "HTTP 503" in caplog.text
    script.fail(RuntimeError("a defect"))
    with caplog.at_level(logging.ERROR, LOGGER):
        next_cycle(clock)
    assert "the cycle failed" in caplog.text
    assert stream_rows()[0]["Error"] == "a defect"
    script.rows(("AAPL", 2.0))
    next_cycle(clock)
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 2.0)])
    assert stream_rows()[0]["Error"] is None


def test_a_reactive_symbol_table_is_read_each_cycle(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    watch = input_table({"Symbol": dtypes.string}, key_cols="Symbol")
    watch.add(new_table([string_col("Symbol", ["MSFT", "AAPL", None])]))
    script.drift = DriftReport(missing=frozenset({"ticker"}))  # every cycle: shown once
    script.rows()
    table = start(
        massive_session, script=script, pollers=pollers, clock=clock, symbols=SymbolTable(watch)
    )
    clock.wait_for_sleepers(1)
    assert script.symbols == [("AAPL", "MSFT")]
    assert stream_rows()[0]["SymbolCount"] == 2
    watch.add(new_table([string_col("Symbol", ["IBM"])]))
    script.rows()
    next_cycle(clock)
    assert script.symbols[-1] == ("AAPL", "IBM", "MSFT")
    assert stream_rows()[0]["SchemaDrift"] == "REST:ticker-"
    del table


def test_a_failing_upstream_watchlist_fails_the_poll(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    source = input_table({"Symbol": dtypes.string})
    source.add(new_table([string_col("Symbol", ["AAPL"])]))
    watch = dm.canonicalize_symbols(source, dm.AssetClass.STOCKS)
    script.rows()
    table = start(
        massive_session, script=script, pollers=pollers, clock=clock, symbols=SymbolTable(watch)
    )
    clock.wait_for_sleepers(1)
    source.add(new_table([string_col("Symbol", ["BRK-B"])]))
    until(lambda: bool(watch.j_table.isFailed()))
    clock.advance(60)
    poller_of(pollers).thread.join(TIMEOUT)
    until(lambda: bool(table.j_table.isFailed()))
    (row,) = stream_rows()
    assert row["State"] == "FAILED"
    assert "the symbol table failed, so the result can no longer follow it" in row["Error"]
    assert script.symbols == [("AAPL",)]


def test_a_late_symbol_table_row_that_is_not_canonical_fails_the_table(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    watch = input_table({"Symbol": dtypes.string}, key_cols="Symbol")
    watch.add(new_table([string_col("Symbol", ["AAPL"])]))
    script.rows()
    table = start(
        massive_session, script=script, pollers=pollers, clock=clock, symbols=SymbolTable(watch)
    )
    clock.wait_for_sleepers(1)
    watch.add(new_table([string_col("Symbol", ["msft"])]))
    until(lambda: watch.size == 2)
    clock.advance(60)
    poller_of(pollers).thread.join(TIMEOUT)
    until(lambda: bool(table.j_table.isFailed()))
    (row,) = stream_rows()
    assert row["State"] == "FAILED"
    assert row["Error"].startswith("stocks.dividends poll(): a symbol table holds 'msft'")
    assert (
        "dm.canonicalize_symbols(table, dm.AssetClass.STOCKS, on_error=dm.OnError.NULL)"
        in (row["Error"])
    )
    assert script.symbols == [("AAPL",)]


# --- Lifetime ------------------------------------------------------------------------------------


def test_close_stops_every_poller_and_marks_the_rows(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
) -> None:
    pollers = cast("Any", massive_session._core.rest_transport())._state.pollers
    script.rows(("AAPL", 1.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 1.0)])
    streams = massive_session.streams()
    poller = poller_of(pollers)
    massive_session.close()
    clock.advance(60)
    poller.thread.join(TIMEOUT)
    assert not poller.thread.is_alive()
    assert pairs(table, dh_to_pylist) == [("AAPL", 1.0)]  # static, with its rows
    del streams
    with pytest.raises(dm.MassiveError, match="is closed"):
        start(massive_session, script=script, pollers=pollers, clock=clock)


def test_a_poll_racing_close_never_starts(
    *, massive_session: dm.MassiveSession, script: Script, clock: FakeClock
) -> None:
    pollers = Pollers("label")
    pollers.close()
    with pytest.raises(dm.MassiveError, match="label is closed"):
        start(massive_session, script=script, pollers=pollers, clock=clock)
    assert massive_session._core._polls == {}
    # A session closed before the row is registered refuses it too.
    massive_session.close()
    with pytest.raises(dm.MassiveError, match="is closed"):
        start(massive_session, script=script, pollers=Pollers("open"), clock=clock)
    assert massive_session._core._polls == {}


def test_a_failed_registration_releases_everything(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    def refuse(*args: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(ResultRegistry, "track", refuse)
    with pytest.raises(KeyboardInterrupt):
        start(massive_session, script=script, pollers=pollers, clock=clock)
    assert pollers._live == set()
    assert massive_session._core._polls == {}
    (row,) = stream_rows()
    assert (row["State"], row["HeldResults"]) == ("RELEASED", 0)


def test_a_collected_table_stops_its_poller(
    *, massive_session: dm.MassiveSession, script: Script, pollers: Pollers, clock: FakeClock
) -> None:
    script.rows()
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    poller = poller_of(pollers)
    clock.wait_for_sleepers(1)
    del table
    gc.collect()
    until(lambda: poller.stopped)
    clock.advance(60)
    poller.thread.join(TIMEOUT)
    assert not poller.thread.is_alive()


def test_a_poller_never_keeps_its_session_alive(
    *, script: Script, clock: FakeClock, fake_rest: FakeRestServer
) -> None:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    pollers = cast("Any", m._core.rest_transport())._state.pollers
    script.rows()
    table = start(m, script=script, pollers=pollers, clock=clock)
    poller = poller_of(pollers)
    clock.wait_for_sleepers(1)
    core = weakref.ref(m._core)
    del m
    gc.collect()
    assert core() is None
    until(lambda: poller.stopped)  # the collected session's closer stopped it
    clock.advance(60)
    poller.thread.join(TIMEOUT)
    assert not poller.thread.is_alive()
    del table


def test_a_cycle_after_the_core_is_gone_ends_the_poller(
    *, massive_session: dm.MassiveSession, script: Script, pollers: Pollers
) -> None:
    poller = poll.Poller(
        massive_session._core,
        spec=DIVIDENDS,
        request=Request(),
        interval=timedelta(seconds=1),
        mode=PollMode.APPEND,
        stream_id=99,
        read=script,
        pollers=pollers,
        clock=poll.CLOCK,
        publisher=cast("Any", None),
        stop=threading.Event(),
    )
    poller._core = cast("Any", lambda: None)
    with ThreadPoolExecutor(1) as pool:
        assert poller._cycle(pool) is False


def test_a_stop_during_a_cycle_publishes_nothing(
    *, massive_session: dm.MassiveSession, script: Script, pollers: Pollers, clock: FakeClock
) -> None:
    gate = threading.Event()
    reading = threading.Event()

    def slow(**kwargs: object) -> tuple[pa.Table, int]:
        reading.set()
        gate.wait(TIMEOUT)
        return script(**cast("dict[str, Any]", kwargs))

    for _ in range(3):
        script.rows(("AAPL", 1.0))
    table = open_poll(
        massive_session._core,
        spec=DIVIDENDS,
        request=Request(symbols=("AAPL",)),
        interval=timedelta(seconds=60),
        mode=PollMode.APPEND,
        read=slow,
        pollers=pollers,
        host=None,
        path=PATH,
        clock=clock,
    )
    poller = poller_of(pollers)
    assert reading.wait(TIMEOUT)
    poller.stop()
    gate.set()
    poller.thread.join(TIMEOUT)
    assert not poller.thread.is_alive()
    assert poller._published == 0  # nothing reached the publisher
    assert table.size == 0


@pytest.mark.parametrize(
    "error", [dm.MassiveError("HTTP 503"), RuntimeError("defect"), dm.EntitlementError("403")]
)
def test_an_error_after_a_stop_is_not_reported(
    *,
    massive_session: dm.MassiveSession,
    pollers: Pollers,
    clock: FakeClock,
    error: Exception,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    reading = threading.Event()
    gate = threading.Event()

    def stopped(**kwargs: object) -> tuple[pa.Table, int]:
        reading.set()
        gate.wait(TIMEOUT)
        raise error

    table = open_poll(
        massive_session._core,
        spec=DIVIDENDS,
        request=Request(),
        interval=timedelta(seconds=60),
        mode=PollMode.UPSERT,
        read=stopped,
        pollers=pollers,
        host=None,
        path=PATH,
        clock=clock,
    )
    poller = poller_of(pollers)
    assert reading.wait(TIMEOUT)
    poller.stop()
    gate.set()
    poller.thread.join(TIMEOUT)
    (row,) = stream_rows()
    assert (row["State"], row["Error"], row["SymbolCount"], row["Symbols"]) == (
        "RUNNING",
        None,
        None,
        "",
    )
    del table


def test_pollers_forget_and_ignore_unknown(
    *, massive_session: dm.MassiveSession, pollers: Pollers
) -> None:
    pollers.discard(cast("Any", object()))
    assert not pollers.closed
    pollers.close()
    pollers.close()
    assert pollers.closed


def test_the_system_clock_waits_on_the_event() -> None:
    event = threading.Event()
    event.set()
    assert poll.CLOCK.wait(event, 10.0)
    assert poll.CLOCK.monotonic() > 0


# --- End to end ----------------------------------------------------------------------------------


@pytest.fixture
def rest_session(
    fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> Iterator[dm.MassiveSession]:
    monkeypatch.setattr(_session, "CATALOG", Catalog((DIVIDENDS,)))
    monkeypatch.setattr(poll, "CLOCK", clock)
    monkeypatch.setitem(
        vars(_massive), "RestClient", functools.partial(_massive.RestClient, clock=FakeClock())
    )
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    yield m
    m.close()


def test_query_poll_reads_the_fetch_path(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
) -> None:
    fake_rest.reply(PATH, Reply(envelope([{"ticker": "AAPL", "cash_amount": 0.25}])))
    q = build_query(rest_session._core, DIVIDENDS, symbols=["AAPL"])
    assert isinstance(q, Query)
    table = q.poll(60)
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 0.25)])
    clock.wait_for_sleepers(1)
    assert len(fake_rest.requests) == 1


def test_a_bad_parameter_fails_poll_at_once(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    q = build_query(rest_session._core, DIVIDENDS, symbols=["AAPL"], params={"bogus": 1})
    assert isinstance(q, Query)
    with pytest.raises(ValueError, match=r"does not take \['bogus'\]"):
        q.poll(60)
    assert fake_rest.requests == []


def test_a_reactive_symbol_table_reaches_the_reader_each_cycle(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
) -> None:
    fake_rest.reply(PATH, Reply(envelope([{"ticker": "AAPL", "cash_amount": 0.25}])))
    watch = new_table([string_col("Symbol", ["AAPL"])])
    q = build_query(rest_session._core, DIVIDENDS, symbols=watch)
    assert isinstance(q, Query)
    bad = build_query(
        rest_session._core, DIVIDENDS, symbols=new_table([string_col("Symbol", ["aapl"])])
    )
    assert isinstance(bad, Query)
    with pytest.raises(ValueError, match=r"^a symbol table holds 'aapl'"):
        bad.poll(60)
    table = q.poll(60, mode=PollMode.UPSERT)
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 0.25)])
    assert "ticker.any_of=AAPL" in fake_rest.requests[0].target


SMA_STOCK = "/v1/indicators/sma/{stockTicker}"
SMA_CRYPTO = "/v1/indicators/sma/{cryptoTicker}"
SMA = DatasetSpec(
    summary="A dataset for the tests.",
    name="indicators.sma",
    fetch_path=SMA_STOCK,
    fetch_paths_by_class={AssetClass.CRYPTO: SMA_CRYPTO},
    poll_keys=("Symbol", "Timestamp"),
    keys={
        (R, SMA_STOCK): SourceKeys(
            symbol=KeySpec("stockTicker", from_request=True), time=KeySpec("Timestamp")
        ),
        (R, SMA_CRYPTO): SourceKeys(
            symbol=KeySpec("cryptoTicker", from_request=True), time=KeySpec("Timestamp")
        ),
    },
)
INDEX_PATH = "/v3/snapshot/indices"
INDEX_SNAPSHOTS = DatasetSpec(
    summary="A dataset for the tests.",
    name="indices.snapshots",
    asset_class=AssetClass.INDICES,
    fetch_path=INDEX_PATH,
    symbol_param="ticker.any_of",
    keys={(R, INDEX_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
)


@pytest.fixture
def routed_session(
    fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> Iterator[dm.MassiveSession]:
    monkeypatch.setattr(_session, "CATALOG", Catalog((DIVIDENDS, SMA, INDEX_SNAPSHOTS)))
    monkeypatch.setattr(poll, "CLOCK", clock)
    monkeypatch.setitem(
        vars(_massive), "RestClient", functools.partial(_massive.RestClient, clock=FakeClock())
    )
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    yield m
    m.close()


def streams_of(m: dm.MassiveSession, to_pylist: ToPylist) -> list[dict[str, Any]]:
    # The session's m.streams() rows, once every row queued so far is applied.
    assert m._core.diagnostics.barrier().wait(TIMEOUT)
    return to_pylist(m.streams())


def symbols_of(table: Table, to_pylist: ToPylist) -> list[str]:
    return sorted(row["Symbol"] for row in to_pylist(table))


@pytest.mark.parametrize("spec", [INDEX_SNAPSHOTS, SMA], ids=["list parameter", "class paths"])
def test_an_emptied_symbol_table_evicts_every_key(
    *,
    routed_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
    spec: DatasetSpec,
) -> None:
    fake_rest.reply(INDEX_PATH, Reply(envelope([{"ticker": "I:SPX", "value": 1.0}])))
    fake_rest.reply(
        "/v1/indicators/sma/I%3ASPX",
        Reply(envelope({"values": [{"timestamp": 1704310200000, "value": 1.0}]})),
    )
    watch = input_table({"Symbol": dtypes.string}, key_cols="Symbol")
    watch.add(new_table([string_col("Symbol", ["I:SPX"])]))
    q = build_query(routed_session._core, spec, symbols=watch)
    assert isinstance(q, Query)
    table = q.poll(60, mode=PollMode.REPLACE)
    until(lambda: symbols_of(table, dh_to_pylist) == ["I:SPX"])
    watch.delete(new_table([string_col("Symbol", ["I:SPX"])]))
    until(lambda: dh_to_pylist(watch) == [])
    requests = len(fake_rest.requests)
    next_cycle(clock)
    until(lambda: symbols_of(table, dh_to_pylist) == [])
    assert len(fake_rest.requests) == requests  # an empty table asks for nothing
    clock.wait_for_sleepers(1)
    (row,) = streams_of(routed_session, dh_to_pylist)
    assert (row["State"], row["Error"], row["SymbolCount"]) == ("RUNNING", None, 0)


def test_a_symbol_routed_to_a_path_without_the_parameter_fails_the_table(
    *,
    routed_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: ToPylist,
) -> None:
    watch = new_table([string_col("Symbol", ["X:BTCUSD"])])
    q = build_query(routed_session._core, SMA, symbols=watch, adjusted=False)
    assert isinstance(q, Query)
    table = q.poll(60)  # the stock path takes `adjusted`, so nothing fails up front
    until(lambda: bool(table.j_table.isFailed()))
    until(lambda: streams_of(routed_session, dh_to_pylist)[0]["State"] == "FAILED")
    (row,) = streams_of(routed_session, dh_to_pylist)
    assert "does not take ['adjusted']" in row["Error"]
    assert row["Path"] == f"{SMA_STOCK}, {SMA_CRYPTO}"
    assert fake_rest.requests == []


def test_a_cycle_reports_its_pages_drift(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
) -> None:
    fake_rest.reply(PATH, Reply(envelope([{"ticker": "AAPL", "brand_new": 1}])))
    q = build_query(rest_session._core, DIVIDENDS, symbols=["AAPL"])
    assert isinstance(q, Query)
    table = q.poll(60)
    clock.wait_for_sleepers(1)
    drift = streams_of(rest_session, dh_to_pylist)[0]["SchemaDrift"]
    assert drift == "REST:brand_new+, REST:distribution_type-"
    del table


def test_a_failure_while_publishing_keeps_the_poller(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
    stream_rows: Callable[[], list[dict[str, Any]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publish = poll.Poller._publish
    failures = [RuntimeError("publish failed")]

    def flaky(self: poll.Poller, table: pa.Table) -> int:
        if failures:
            raise failures.pop()
        return publish(self, table)

    monkeypatch.setattr(poll.Poller, "_publish", flaky)
    script.rows(("AAPL", 1.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    clock.wait_for_sleepers(1)
    assert stream_rows()[0]["Error"] == "publish failed"
    script.rows(("AAPL", 2.0))
    next_cycle(clock)
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 2.0)])


def test_a_failed_publish_still_evicts_the_keys_it_dropped(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
    stream_rows: Callable[[], list[dict[str, Any]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    script.rows(("AAPL", 1.0), ("MSFT", 2.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 1.0), ("MSFT", 2.0)])
    fail_one_poll_conversion(monkeypatch)
    script.rows(("AAPL", 3.0))  # drops MSFT, but this cycle's publish fails
    next_cycle(clock)
    assert stream_rows()[0]["Error"] == "conversion failed"
    assert pairs(table, dh_to_pylist) == [("AAPL", 1.0), ("MSFT", 2.0)]
    script.rows(("AAPL", 4.0))  # the next cycle still tombstones MSFT
    next_cycle(clock)
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 4.0)])


def test_an_emptied_table_never_checks_the_paths_it_no_longer_reads(
    *,
    routed_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
) -> None:
    # The default path lacks `adjusted`; only the stock symbols' path takes it.
    crypto_first = DatasetSpec(
        summary="A dataset for the tests.",
        name="indicators.crypto_first",
        fetch_path=SMA_CRYPTO,
        fetch_paths_by_class={AssetClass.STOCKS: SMA_STOCK},
        poll_keys=("Symbol", "Timestamp"),
        keys=SMA.keys,
    )
    fake_rest.reply(
        "/v1/indicators/sma/AAPL",
        Reply(envelope({"values": [{"timestamp": 1704310200000, "value": 1.0}]})),
    )
    watch = input_table({"Symbol": dtypes.string}, key_cols="Symbol")
    watch.add(new_table([string_col("Symbol", ["AAPL"])]))
    q = build_query(routed_session._core, crypto_first, symbols=watch, adjusted=False)
    assert isinstance(q, Query)
    table = q.poll(60, mode=PollMode.REPLACE)
    until(lambda: symbols_of(table, dh_to_pylist) == ["AAPL"])
    watch.delete(new_table([string_col("Symbol", ["AAPL"])]))
    until(lambda: dh_to_pylist(watch) == [])
    next_cycle(clock)
    until(lambda: symbols_of(table, dh_to_pylist) == [])
    assert not table.j_table.isFailed()


def test_the_path_column_names_the_paths_static_symbols_read(
    *, routed_session: dm.MassiveSession, fake_rest: FakeRestServer, dh_to_pylist: ToPylist
) -> None:
    fake_rest.reply("/v1/indicators/sma/AAPL", Reply(envelope({"values": []})))
    q = build_query(routed_session._core, SMA, symbols=["AAPL"])
    assert isinstance(q, Query)
    table = q.poll(60)
    (row,) = streams_of(routed_session, dh_to_pylist)
    assert row["Path"] == SMA_STOCK
    del table


# --- APPEND end to end ---------------------------------------------------------------------------


FEED = DatasetSpec(
    summary="A dataset for the tests.",
    name="stocks.dividends",
    asset_class=AssetClass.STOCKS,
    fetch_path=PATH,
    symbol_param="ticker.any_of",
    keys={(R, PATH): SourceKeys(symbol=KeySpec("Ticker"))},
    poll_mode=PollMode.APPEND,
    poll_keys=("Id",),
)


@pytest.fixture
def feed_session(
    fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> Iterator[dm.MassiveSession]:
    monkeypatch.setattr(_session, "CATALOG", Catalog((FEED,)))
    monkeypatch.setattr(poll, "CLOCK", clock)
    monkeypatch.setitem(
        vars(_massive), "RestClient", functools.partial(_massive.RestClient, clock=FakeClock())
    )
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    yield m
    m.close()


def page(*rows: tuple[str, float]) -> Reply:
    # One cycle's reply: dividends identified by their id.
    return Reply(envelope([{"id": i, "ticker": "AAPL", "cash_amount": a} for i, a in rows]))


def feed(m: dm.MassiveSession) -> Table:
    # The default mode of the feed, APPEND.
    q = build_query(m._core, FEED, symbols=["AAPL"])
    assert isinstance(q, Query)
    return q.poll(60)


def ids(table: Table, to_pylist: ToPylist) -> list[tuple[str, float]]:
    return sorted((row["Id"], row["CashAmount"]) for row in to_pylist(table))


def test_append_skips_every_row_it_already_appended(
    *,
    feed_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
) -> None:
    fake_rest.reply(
        PATH,
        page(("a", 1.0), ("b", 2.0), ("b", 9.0)),  # a repeated key within one cycle
        page(("b", 2.0), ("a", 8.0), ("c", 3.0)),  # a and b again, with c new
        page(),
        page(("a", 1.0), ("c", 3.0), ("d", 4.0)),
    )
    table = feed(feed_session)
    until(lambda: ids(table, dh_to_pylist) == [("a", 1.0), ("b", 2.0)])
    next_cycle(clock)
    until(lambda: ids(table, dh_to_pylist) == [("a", 1.0), ("b", 2.0), ("c", 3.0)])
    next_cycle(clock)
    next_cycle(clock)
    expected = [("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)]
    until(lambda: ids(table, dh_to_pylist) == expected)
    (row,) = streams_of(feed_session, dh_to_pylist)
    assert row["RowsPublished"] == 4  # the skipped rows are not counted
    assert len(fake_rest.requests) == 4


def test_append_never_appends_a_row_twice_after_an_absence(
    *,
    feed_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
) -> None:
    # A symbol removed from a reactive table and added back hours later returns its rows.
    fake_rest.reply(
        PATH,
        page(("b", 2.0), ("a", 1.0)),
        *[page(("b", 2.0))] * 5,
        page(("a", 1.0), ("b", 2.0), ("c", 3.0)),
    )
    table = feed(feed_session)
    until(lambda: ids(table, dh_to_pylist) == [("a", 1.0), ("b", 2.0)])
    for _ in range(6):
        next_cycle(clock)
    until(lambda: ids(table, dh_to_pylist) == [("a", 1.0), ("b", 2.0), ("c", 3.0)])
    assert len(fake_rest.requests) == 7


def test_append_appends_each_revision_of_a_keyed_row(
    *,
    feed_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
) -> None:
    # Keyed by symbol with a revision column, as a snapshot is: each new state is a new row,
    # and a null revision is a value of its own.
    snapshot = dataclasses.replace(
        FEED, poll_mode=PollMode.REPLACE, poll_keys=("Symbol",), revision_keys=("PayDate",)
    )

    def state(pay_date: str | None, amount: float) -> dict[str, object]:
        return {"id": "x", "ticker": "AAPL", "cash_amount": amount, "pay_date": pay_date}

    fake_rest.reply(
        PATH,
        Reply(envelope([state(None, 1.0)])),
        Reply(envelope([state(None, 9.0)])),  # the same state: skipped
        Reply(envelope([state("2026-01-02", 2.0)])),
        Reply(envelope([state("2026-01-02", 9.0)])),
    )
    q = build_query(feed_session._core, snapshot, symbols=["AAPL"])
    assert isinstance(q, Query)
    table = q.poll(60, mode=PollMode.APPEND)
    until(lambda: len(dh_to_pylist(table)) == 1)
    for _ in range(3):
        next_cycle(clock)
    until(lambda: sorted(row["CashAmount"] for row in dh_to_pylist(table)) == [1.0, 2.0])
    # The keyed modes ignore the revision column.
    assert snapshot.poll_keys == ("Symbol",)


def test_a_failed_append_remembers_nothing(
    *,
    feed_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_rest.reply(PATH, page(("a", 1.0)), page(("b", 2.0)), page(("a", 1.0), ("b", 2.0)))
    table = feed(feed_session)
    until(lambda: ids(table, dh_to_pylist) == [("a", 1.0)])
    fail_one_poll_conversion(monkeypatch)
    next_cycle(clock)  # b's cycle fails to publish
    assert feed_session._core.diagnostics.barrier().wait(30.0)
    assert dh_to_pylist(feed_session.streams())[0]["Error"] == "conversion failed"
    assert ids(table, dh_to_pylist) == [("a", 1.0)]
    next_cycle(clock)  # so the next cycle appends b, and still skips a
    until(lambda: ids(table, dh_to_pylist) == [("a", 1.0), ("b", 2.0)])


NULL_ID = {"id": None, "ticker": "AAPL", "currency": "USD"}
NULL_CURRENCY = {"id": "b", "ticker": "AAPL", "currency": None}


@pytest.mark.parametrize("mode", list(PollMode))
@pytest.mark.parametrize(
    ("poll_keys", "bad", "message"),
    [
        (
            ("Id",),
            NULL_ID,
            "row 1 of the cycle has a null poll key 'Id': {'Symbol': 'AAPL', 'Currency': 'USD'}",
        ),
        # Every key column is checked, not only the first.
        (
            ("Id", "Currency"),
            NULL_CURRENCY,
            "row 1 of the cycle has a null poll key 'Currency': {'Symbol': 'AAPL', 'Id': 'b'}",
        ),
    ],
    ids=["first column", "second column"],
)
def test_a_null_poll_key_fails_the_table_in_every_mode(
    *,
    feed_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: ToPylist,
    mode: PollMode,
    poll_keys: tuple[str, ...],
    bad: dict[str, object],
    message: str,
) -> None:
    rows = [{"id": "a", "ticker": "AAPL", "currency": "USD"}, bad]
    fake_rest.reply(PATH, Reply(envelope(rows)))
    spec = dataclasses.replace(FEED, poll_keys=poll_keys)
    q = build_query(feed_session._core, spec, symbols=["AAPL"])
    assert isinstance(q, Query)
    table = q.poll(60, mode=mode)
    until(lambda: bool(table.j_table.isFailed()))
    (row,) = streams_of(feed_session, dh_to_pylist)
    assert (row["State"], row["Error"]) == ("FAILED", f"stocks.dividends poll(): {message}")
    assert row["RowsPublished"] == 0  # nothing of the cycle was published


@pytest.mark.parametrize("mode", list(PollMode))
def test_a_null_in_a_nullable_poll_key_is_a_value(
    *,
    feed_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: ToPylist,
    mode: PollMode,
) -> None:
    # `Currency` may be null; `Id` may not, so its null still fails the table.
    spec = dataclasses.replace(
        FEED, poll_keys=("Id", "Currency"), nullable_poll_keys={"Currency": "for the tests"}
    )
    rows = [{"id": "a", "ticker": "AAPL", "currency": None}, {"id": "b", "ticker": "AAPL"}]
    fake_rest.reply(PATH, Reply(envelope(rows)))
    q = build_query(feed_session._core, spec, symbols=["AAPL"])
    assert isinstance(q, Query)
    table = q.poll(60, mode=mode)
    until(lambda: sorted(row["Id"] for row in dh_to_pylist(table)) == ["a", "b"])
    assert not table.j_table.isFailed()
    (found,) = streams_of(feed_session, dh_to_pylist)
    assert (found["State"], found["RowsPublished"]) == ("RUNNING", 2)


def test_a_nan_poll_key_fails_and_a_long_row_is_truncated(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    # A NaN never equals itself, so as a key it would append its row again every cycle.
    script.rows(("x" * 400, float("nan")))
    table = open_poll(
        massive_session._core,
        spec=dataclasses.replace(DIVIDENDS, poll_keys=("CashAmount",)),
        request=Request(symbols=("AAPL",)),
        interval=timedelta(seconds=60),
        mode=PollMode.APPEND,
        read=script,
        pollers=pollers,
        host="api.massive.com",
        path=PATH,
        clock=clock,
    )
    until(lambda: bool(table.j_table.isFailed()))
    (row,) = stream_rows()
    prefix = "stocks.dividends poll(): row 0 of the cycle has a null poll key 'CashAmount': "
    assert row["Error"].startswith(prefix + "{'Symbol': 'xxx")
    rendered = row["Error"].removeprefix(prefix)
    assert len(rendered) == 300
    assert rendered.endswith("xxx...")


def test_polled_rows_share_one_stored_string_per_date(
    *, massive_session: dm.MassiveSession, script: Script, pollers: Pollers, clock: FakeClock
) -> None:
    data = [
        {"Symbol": symbol, "CashAmount": 1.0, "PayDate": "2025-03-14", "Id": symbol}
        for symbol in ("AAPL", "MSFT", "IBM")
    ]
    script.outcomes.put(pa.Table.from_pylist(data, schema=script.schema))
    table = start(
        massive_session, script=script, pollers=pollers, clock=clock, mode=PollMode.UPSERT
    )
    until(lambda: table.size == 3)
    # Counted inside Deephaven: a string crossing to Python is copied.
    refs = table.update("DateRef = java.lang.System.identityHashCode(PayDate)")
    assert refs.select_distinct("DateRef").size == 1
    massive_session.release(table)


@pytest.mark.parametrize("mode", list(PollMode))
def test_a_warn_session_keys_an_unreadable_symbol_by_its_text(
    *,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
    mode: PollMode,
) -> None:
    # Under WARN a row Massive spelled in a way the library cannot read keeps a null Symbol
    # and its text in RawSymbol; keyed on Symbol, it neither fails the null-key check nor
    # merges with another such row, and REPLACE removes it by its text.
    monkeypatch.setattr(_session, "CATALOG", Catalog((FEED,)))
    monkeypatch.setattr(poll, "CLOCK", clock)
    spec = dataclasses.replace(FEED, poll_keys=("Symbol",))
    rows = [
        {"id": "a", "ticker": "AAPL"},
        {"id": "b", "ticker": "@@1"},
        {"id": "c", "ticker": "@@2"},
    ]
    fake_rest.reply(PATH, Reply(envelope(rows)), Reply(envelope(rows[:2])))
    config = dm.MassiveConfig(
        api_key="test-key", rest_base_url=fake_rest.url, unknown_symbols=dm.UnknownSymbols.WARN
    )
    with dm.MassiveSession(config) as m:
        q = build_query(m._core, spec, symbols=["AAPL"])
        assert isinstance(q, Query)
        table = q.poll(60, mode=mode)
        expected = [("a", "AAPL", None), ("b", None, "@@1"), ("c", None, "@@2")]

        def held() -> list[tuple[str, str | None, str | None]]:
            return sorted((r["Id"], r["Symbol"], r["RawSymbol"]) for r in dh_to_pylist(table))

        until(lambda: held() == expected)
        next_cycle(clock)  # @@2 leaves: REPLACE removes its row alone; the others keep it
        until(lambda: len(fake_rest.requests) == 2)
        until(lambda: held() == (expected[:2] if mode is PollMode.REPLACE else expected))
        assert m._core.diagnostics.barrier().wait(30.0)
        assert held() == (expected[:2] if mode is PollMode.REPLACE else expected)
        assert not table.j_table.isFailed()


def test_the_errors_a_retry_cannot_fix_are_permanent() -> None:
    # A poll() table fails on a PermanentError, its fatal set names only the base.
    for error in (poll.RowDataError, poll.UnsupportedParameterError, poll.SymbolRowError):
        assert issubclass(error, PermanentError)
    assert (dm.AuthenticationError, dm.EntitlementError, PermanentError) == poll._FATAL
