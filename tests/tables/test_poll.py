"""Tests for `poll()` tables: the three modes, the cycle's failures, rows, and lifetime."""

import copy
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
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.execution_context import ExecutionContext
from deephaven.liveness_scope import LivenessScope
from deephaven.stream.table_publisher import TablePublisher
from deephaven.table import Table
from deephaven.table_factory import InputTable
from typing_extensions import override

import deephaven_massive as dm
from deephaven_massive import _session
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.request import Request, ValueTable
from deephaven_massive._datasets.symbols import AllSymbols, NormalizedSymbols, SymbolTable
from deephaven_massive._enums import AssetClass, PollMode, Recency
from deephaven_massive._errors import PermanentError, RowDataError, UnmatchedFormError
from deephaven_massive._ingest.rest import RestEndpoints
from deephaven_massive._massive import Cancel, DriftReport, Transport
from deephaven_massive._tables import poll
from deephaven_massive._tables.liveness import (
    ResultRecord,
    ResultRegistry,
    new_scope,
    release_scope,
)
from deephaven_massive._tables.poll import LIVE_COLUMN, Pollers, open_poll
from deephaven_massive._tables.session_core import SessionCore
from deephaven_massive.namespaces._query_building import build_query
from deephaven_massive.queries import FetchQuery
from tests._fakes.clock import FakeClock
from tests._fakes.rest import FakeRestServer, Reply, envelope

if TYPE_CHECKING:
    from deephaven_massive._tables.publisher import BlinkPublisher


# The reply and envelope builders are plain functions of the fake, which no fixture can hand
# over; the server itself comes from the `fake_rest` fixture.


R = Transport.REST
TIMEOUT = 30.0
LOGGER = "deephaven_massive._tables.poll"


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

GOOD = "last good cycle at 2027-01-15T08:00:00Z"
"""The `Error` suffix of a failing cycle after the first, good cycle at the fake clock's start."""

NO_GOOD = "no good cycle since the poll started at 2027-01-15T08:00:00Z"
"""The `Error` suffix of a failing cycle before any good one."""


class Script:
    # A cycle reader that answers each cycle from a queue of outcomes: rows or an exception.
    def __init__(self, m: dm.MassiveSession) -> None:
        self.schema = m._core.catalog.dataset_schema(DIVIDENDS).arrow_schema()
        self.outcomes: queue.SimpleQueue[object] = queue.SimpleQueue()
        self.symbols: list[NormalizedSymbols | None] = []
        self.values: list[tuple[str, ...] | None] = []
        self.drift: DriftReport | None = None
        # What each cycle's pages show about their recency, as the REST transport reports it.
        self.recency: tuple[Recency, ...] = ()
        self.took = 0.0  # how far each cycle's read moves the clock
        self.late_page = False  # whether a page arrives once the read has moved the clock
        # Whether each cycle keeps its page callback in `record`, which holds the session's
        # core: a test that sets it deletes `record` before it ends.
        self.keep_record = False
        self.clock: FakeClock | None = None
        # What each cycle says it left out (`CycleRows.left_out`).
        self.left_out: tuple[str, ...] = ()

    def rows(self, *pairs: tuple[str, float]) -> None:
        data = [{"Symbol": symbol, "CashAmount": amount} for symbol, amount in pairs]
        self.outcomes.put(pa.Table.from_pylist(data, schema=self.schema))

    def fail(self, error: BaseException) -> None:
        # A copy: a test's parameter lives as long as the run, and the raised exception's
        # traceback would keep the poller's frames, and its session, alive with it.
        self.outcomes.put(copy.copy(error))

    def __call__(
        self,
        *,
        symbols: NormalizedSymbols | None,
        values: tuple[str, ...] | None,
        record: Callable[[DriftReport], None],
        on_recency: Callable[[Recency], None],
        executor: ThreadPoolExecutor,
        cancel: threading.Event,
    ) -> poll.CycleRows:
        self.symbols.append(symbols)
        self.values.append(values)
        self.cancel = cancel
        if self.keep_record:  # a test calls it to deliver a page while the cycle runs
            self.record = record
        if self.drift is not None:
            record(self.drift)
        for shown in self.recency:
            on_recency(shown)
        outcome = self.outcomes.get(timeout=TIMEOUT)
        if self.clock is not None:
            self.clock.advance(self.took)
        if self.late_page:
            record(DriftReport())
        if isinstance(outcome, BaseException):
            raise outcome
        assert isinstance(outcome, pa.Table)
        return poll.CycleRows(outcome, 3, 600.0, self.left_out)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(manual=True)


@pytest.fixture
def script(massive_session: dm.MassiveSession) -> Script:
    return Script(massive_session)


class RecordingPollers(Pollers):
    # Remembers every poller it registered: a poller leaves the set when its thread ends.
    def __init__(self, label: str) -> None:
        super().__init__(label)
        self.added: list[poll.Poller] = []

    @override
    def add(self, poller: poll.Poller) -> None:
        super().add(poller)
        self.added.append(poller)


@pytest.fixture
def pollers(massive_session: dm.MassiveSession) -> Pollers:
    return RecordingPollers(massive_session._core.label)


def start(
    m: dm.MassiveSession,
    *,
    script: poll.CycleReader,
    pollers: Pollers,
    clock: FakeClock,
    mode: PollMode = PollMode.REPLACE,
    symbols: NormalizedSymbols | None = ("AAPL", "MSFT"),
    stop: threading.Event | None = None,
    interval: float = 60.0,
    values: ValueTable | None = None,
) -> Table:
    return open_poll(
        m._core,
        spec=DIVIDENDS,
        request=Request(symbols=symbols, value_table=values),
        interval=timedelta(seconds=interval),
        mode=mode,
        query="<Query test>",
        read=script,
        pollers=pollers,
        host="api.massive.com",
        path=PATH,
        recency=None,
        clock=clock,
        stop=threading.Event() if stop is None else stop,
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


def next_cycle(clock: FakeClock, interval: float = 60.0) -> None:
    # Ends the wait after a cycle and returns once the next cycle has run and waits again.
    clock.wait_for_sleepers(1)
    waits = len(clock.waits)
    clock.advance(interval)
    until(lambda: len(clock.waits) > waits)


def fail_one_poll_conversion(monkeypatch: pytest.MonkeyPatch) -> None:
    # Makes the next Arrow-to-Deephaven conversion on a poll thread fail once. The patch is
    # process-wide and the diagnostics thread converts its batches through the same function,
    # so a failure any thread could take would race the poller for it.
    convert = dh_arrow.to_table
    failures = [RuntimeError("conversion failed")]

    def flaky(data: pa.Table) -> Table:
        if failures and " poll-" in threading.current_thread().name:
            raise failures.pop()
        return convert(data)

    monkeypatch.setattr(dh_arrow, "to_table", flaky)


def _pollers_of(m: dm.MassiveSession) -> Pollers:
    # The session's own pollers, kept by its REST transport.
    transport = m._core.rest_transport()
    assert isinstance(transport, RestEndpoints)
    return transport._state.pollers


def poller_of(pollers: Pollers) -> poll.Poller:
    if isinstance(pollers, RecordingPollers):
        return pollers.added[-1]
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
    dh_exec_ctx: ExecutionContext,
    rows_when: Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]],
) -> None:
    # Every vendor column of the related companies is a poll key; only the arrival time is
    # left to aggregate, so the table holds each distinct row once, across cycles, with the
    # time of the latest cycle that returned it.
    spec = _session.CATALOG.by_name["reference.related_companies"]
    schema = _session.CATALOG.dataset_schema(spec, raw_symbol=False)
    assert {*spec.poll_keys, "TimestampInsert"} == set(schema.column_names)
    deferred: list[LivenessScope] = []  # nothing fails the table: it stays empty
    scope = new_scope()
    with scope.open():
        table, publisher = poll.build_table(
            spec,
            schema,
            PollMode.UPSERT,
            stream_id=1,
            on_shutdown=threading.Event().set,
            release_later=deferred.append,
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
    release_scope(dh_exec_ctx, scope)


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


def test_each_cycle_and_its_failure_are_logged_with_the_stream_id(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
    caplog: pytest.LogCaptureFixture,
) -> None:
    script.rows(("AAPL", 1.0), ("MSFT", 2.0))
    with caplog.at_level(logging.DEBUG, LOGGER):
        table = start(
            massive_session, script=script, pollers=pollers, clock=clock, mode=PollMode.APPEND
        )
        until(lambda: len(pairs(table, dh_to_pylist)) == 2)
        script.rows(("AAPL", 1.0), ("MSFT", 2.0))
        next_cycle(clock)  # the same rows again: read, but nothing new to publish
        script.fail(dm.MassiveError("HTTP 503 from GET /stocks/v1/dividends"))
        next_cycle(clock)
    patience = "waiting out at most 600 s of pauses; left out: none"
    assert caplog.messages == [
        f"{massive_session._core.label}: started the poll watch thread",
        (
            f"{massive_session._core.label}: poll() stream 1 started: <Query test> every "
            "60.000 s, mode APPEND, symbols 2 [AAPL, MSFT]"
        ),
        (
            f"stream 1: stocks.dividends poll() cycle: 3 requests, 2 rows read, 2 published in "
            f"0.000 s, {patience}"
        ),
        (
            f"stream 1: stocks.dividends poll() cycle: 3 requests, 2 rows read, 0 published in "
            f"0.000 s, {patience}"
        ),
        (
            "stocks.dividends poll() stream 1: a cycle failed, and the next one runs as scheduled: "
            "HTTP 503 from GET /stocks/v1/dividends"
        ),
    ]


def test_late_cycles_warn_once_and_on_time_cycles_say_so(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
    caplog: pytest.LogCaptureFixture,
) -> None:
    script.clock = clock
    script.rows(("AAPL", 1.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 1.0)])
    clock.wait_for_sleepers(1)
    # Two cycles whose reads take 90 s, longer than the 60 s interval: one WARNING for the run.
    script.took = 90.0
    with caplog.at_level(logging.INFO, LOGGER):
        script.rows(("AAPL", 2.0))
        script.rows(("AAPL", 3.0))
        clock.advance(60)
        until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 3.0)])
        script.took = 0.0
        script.rows(("AAPL", 4.0))  # in time again: INFO
        until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 4.0)])
        clock.wait_for_sleepers(1)
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (
            logging.WARNING,
            (
                "stocks.dividends poll() stream 1: a cycle took 90 s, longer than the 60 s "
                "interval, so the table updates late; m.rate_limits() shows whether its requests "
                "are paced or paused by Massive's rate limit"
            ),
        ),
        (
            logging.INFO,
            "stocks.dividends poll() stream 1: cycles end within the 60 s interval again",
        ),
    ]


def test_the_poller_and_its_pool_are_named_after_the_session_and_the_stream(
    *, massive_session: dm.MassiveSession, pollers: Pollers, clock: FakeClock
) -> None:
    names: queue.SimpleQueue[tuple[str, str]] = queue.SimpleQueue()
    schema = massive_session._core.catalog.dataset_schema(DIVIDENDS).arrow_schema()

    def read(*, executor: ThreadPoolExecutor, **_: object) -> poll.CycleRows:
        pooled = executor.submit(lambda: threading.current_thread().name).result(TIMEOUT)
        names.put((threading.current_thread().name, pooled))
        return poll.CycleRows(schema.empty_table(), 1, 600.0)

    # Held until the end: a table nothing refers to may be collected, which stops its poller
    # before its first cycle.
    table = start(massive_session, script=read, pollers=pollers, clock=clock)
    poller, pool = names.get(timeout=TIMEOUT)
    prefix = massive_session._core.thread_prefix
    assert poller == f"{prefix} poll-1"
    assert pool == f"{prefix} poll-1 rest_0"
    massive_session.release(table)


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


def test_polls_refused_for_one_entitlement_log_one_warning(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Each poll makes its own requests and gets its own refusal: the session's first poll of
    # the dataset refused this way logs a WARNING, the later ones only DEBUG. A refusal of
    # another kind is a cause of its own and warns again.
    refusals = [
        dm.EntitlementError("HTTP 403 from GET /stocks/v1/dividends", status=403),
        dm.EntitlementError("HTTP 403 from GET /stocks/v1/dividends", status=403),
        dm.AuthenticationError("HTTP 401 from GET /stocks/v1/dividends", status=401),
    ]
    tables = []
    with caplog.at_level(logging.DEBUG, LOGGER):
        for refusal in refusals:
            script.fail(refusal)
            tables.append(start(massive_session, script=script, pollers=pollers, clock=clock))
            poller = poller_of(pollers)
            poller.thread.join(TIMEOUT)
            assert not poller.thread.is_alive()
    failed = [(r.levelno, r.getMessage()) for r in caplog.records if "failed: HTTP" in r.message]
    assert [level for level, _ in failed if level >= logging.WARNING] == [
        logging.WARNING,
        logging.WARNING,
    ]
    warned = [message for level, message in failed if level == logging.WARNING]
    assert warned[0] == (
        "stocks.dividends poll() stream 1 failed: HTTP 403 from GET /stocks/v1/dividends (each "
        "later poll() table of stocks.dividends refused this way logs at DEBUG)"
    )
    assert warned[1].startswith("stocks.dividends poll() stream 3 failed: HTTP 401")
    # Every refusal still logs at DEBUG with its own stream.
    debug = [message for level, message in failed if level == logging.DEBUG]
    assert [m.split(" failed")[0] for m in debug] == [
        f"stocks.dividends poll() stream {n}" for n in (1, 2, 3)
    ]
    for table in tables:
        massive_session.release(table)


def test_a_failure_is_recorded_before_the_table_fails(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Whoever sees the table failed must find the failure already recorded, as for a stream.
    order: list[str] = []
    core = massive_session._core
    record, publish = core.fail_stream, TablePublisher.publish_failure

    def recording(stream_id: int, error: str) -> None:
        order.append("recorded")
        record(stream_id, error)

    def failing(publisher: TablePublisher, error: Exception) -> None:
        order.append("table failed")
        publish(publisher, error)

    monkeypatch.setattr(core, "fail_stream", recording)
    monkeypatch.setattr(TablePublisher, "publish_failure", failing)
    script.fail(dm.EntitlementError("HTTP 403 from GET /stocks/v1/dividends"))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    poller = poller_of(pollers)
    poller.thread.join(TIMEOUT)
    assert not poller.thread.is_alive()  # the poller is done with `order`
    assert order == ["recorded", "table failed"]
    massive_session.release(table)


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
    with caplog.at_level(logging.INFO, LOGGER):
        next_cycle(clock)
    (row,) = stream_rows()
    assert (row["State"], row["Error"]) == (
        "RUNNING",
        f"HTTP 503 from GET /stocks/v1/dividends; {GOOD}",
    )
    assert pairs(table, dh_to_pylist) == [("AAPL", 1.0)]
    # An expected failure: one WARNING for the run.
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (
            logging.WARNING,
            (
                "stocks.dividends poll() stream 1: a cycle failed, and the next one runs as "
                "scheduled: HTTP 503 from GET /stocks/v1/dividends"
            ),
        )
    ]
    caplog.clear()
    script.fail(dm.MassiveError("HTTP 503 from GET /stocks/v1/dividends"))
    with caplog.at_level(logging.INFO, LOGGER):
        next_cycle(clock)
    assert caplog.records == []  # the same run: DEBUG only
    script.fail(RuntimeError("a defect"))
    with caplog.at_level(logging.INFO, LOGGER):
        next_cycle(clock)
    # A defect is never hidden behind the outage: it starts its own run, at ERROR.
    [defect] = caplog.records
    assert defect.levelno == logging.ERROR
    assert defect.getMessage().startswith(
        "stocks.dividends poll() stream 1: the cycle failed. This is a bug in deephaven-massive."
    )
    assert stream_rows()[0]["Error"] == f"a defect; {GOOD}"
    assert stream_rows()[0]["Gaps"] is None  # a gap is recorded once it ends
    caplog.clear()
    script.rows(("AAPL", 2.0))
    with caplog.at_level(logging.INFO, LOGGER):
        next_cycle(clock)
        until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 2.0)])
    # The Error clears, but the time over which rows may be missing stays on the row.
    (row,) = stream_rows()
    assert (row["Error"], row["Gaps"]) == (
        None,
        "2027-01-15T08:01:00Z to 2027-01-15T08:04:00Z",
    )
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (
            logging.INFO,
            (
                "stocks.dividends poll() stream 1: a cycle succeeded again; rows from "
                "2027-01-15T08:01:00Z to 2027-01-15T08:04:00Z may be missing, recorded in its "
                "Gaps"
            ),
        )
    ]
    # A later run of failures adds a second gap; the first one stays.
    script.fail(dm.MassiveError("HTTP 503 from GET /stocks/v1/dividends"))
    next_cycle(clock)
    script.rows(("AAPL", 3.0))
    next_cycle(clock)
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 3.0)])
    assert stream_rows()[0]["Gaps"] == (
        "2027-01-15T08:01:00Z to 2027-01-15T08:04:00Z; 2027-01-15T08:05:00Z to 2027-01-15T08:06:00Z"
    )


def _last_cycle_fails_the_table(clock: FakeClock, pollers: Pollers) -> None:
    # Runs the next cycle, which ends the poller instead of waiting for another.
    clock.wait_for_sleepers(1)
    clock.advance(60)
    poller = poller_of(pollers)
    poller.thread.join(TIMEOUT)
    assert not poller.thread.is_alive()


@pytest.mark.parametrize("mode", list(PollMode))
def test_cycles_failing_for_max_outage_fail_the_table_in_every_mode(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
    mode: PollMode,
) -> None:
    script.rows(("AAPL", 1.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock, mode=mode)
    error = dm.MassiveError("HTTP 503 from GET /stocks/v1/dividends")
    for _ in range(5):  # failures at 60 to 300 s: the table runs and names its last good cycle
        script.fail(error)
        next_cycle(clock)
        (row,) = stream_rows()
        assert (row["State"], row["Error"]) == ("RUNNING", f"{error}; {GOOD}")
    script.rows()  # a good cycle with no new row: quiet data, which ends the run of failures
    next_cycle(clock)
    good_again = "last good cycle at 2027-01-15T08:06:00Z"
    for _ in range(5):  # failures at 420 to 660 s: under 300 s since the run's first
        script.fail(error)
        next_cycle(clock)
    assert stream_rows()[0]["Error"] == f"{error}; {good_again}"
    script.fail(error)  # 300 s after the run's first failure: max_outage
    _last_cycle_fails_the_table(clock, pollers)
    until(lambda: bool(table.j_table.isFailed()))
    (row,) = stream_rows()
    failure = (
        f"stocks.dividends poll(): {good_again}; every cycle since 2027-01-15T08:07:00Z has "
        "failed, for 300 s, longer than MassiveConfig.max_outage (300 s); the rows since then "
        "may be missing, so this table stopped; call the verb again for a new live table. The "
        f"latest error: {error}"
    )
    assert (row["State"], row["Error"]) == ("FAILED", failure)


@pytest.mark.parametrize(
    ("interval", "took"),
    [(3600.0, 0.0), (600.0, 400.0)],
    ids=["an hourly poll", "cycles slowed by rate-limit pauses"],
)
def test_one_failure_long_after_the_last_good_cycle_keeps_the_table(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
    interval: float,
    took: float,
) -> None:
    # The last good cycle began well over max_outage before the failure, but the failing cycle
    # received a page just before it failed: the table keeps running, and the next good cycle
    # clears it.
    script.clock, script.took, script.late_page = clock, took, True
    script.rows(("AAPL", 1.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock, interval=interval)
    error = dm.MassiveError("HTTP 429 from GET /stocks/v1/dividends")
    script.fail(error)
    next_cycle(clock, interval)
    (row,) = stream_rows()
    assert (row["State"], row["Error"]) == ("RUNNING", f"{error}; {GOOD}")
    script.rows(("AAPL", 2.0))
    next_cycle(clock, interval)
    (row,) = stream_rows()
    assert (row["State"], row["Error"]) == ("RUNNING", None)
    assert not table.j_table.isFailed()


def test_a_gap_starts_where_the_failed_cycle_started_not_at_its_last_page(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    # Each cycle reads for 40 s and receives a page at its end. The failing cycle starts at
    # 08:01:40 and fails after its page at 08:02:20: it published nothing, so its rows are
    # missing from its start, not from its last page.
    script.clock, script.took, script.late_page = clock, 40.0, True
    script.rows(("AAPL", 1.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    script.fail(dm.MassiveError("HTTP 503 from GET /stocks/v1/dividends"))
    next_cycle(clock)
    script.rows(("AAPL", 2.0))
    next_cycle(clock)
    (row,) = stream_rows()
    assert (row["Error"], row["Gaps"]) == (
        None,
        "2027-01-15T08:01:40Z to 2027-01-15T08:03:20Z",
    )
    del table


def test_a_failed_cycle_that_received_no_page_counts_its_whole_run(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    # A cycle paused 400 s by rate limits, with no page, then failing: the run of failing
    # cycles began at the cycle's start, so the table fails at once.
    script.rows(("AAPL", 1.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock, interval=600.0)
    clock.wait_for_sleepers(1)
    script.clock, script.took = clock, 400.0
    error = dm.MassiveError("HTTP 429 from GET /stocks/v1/dividends")
    script.fail(error)
    clock.advance(600.0)
    until(lambda: bool(table.j_table.isFailed()))
    (row,) = stream_rows()
    assert row["State"] == "FAILED"
    assert "every cycle since 2027-01-15T08:10:00Z has failed, for 400 s" in row["Error"]


def _second_cycle_running(script: Script, clock: FakeClock) -> None:
    # Ends the wait after the first cycle and returns once the second one is reading.
    clock.wait_for_sleepers(1)
    clock.advance(60.0)
    until(lambda: len(script.symbols) == 2)


def test_a_cycle_running_past_its_interval_is_marked_until_it_ends(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    script.rows(("AAPL", 1.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    _second_cycle_running(script, clock)
    clock.advance(60.0)  # exactly its interval: not late yet
    pollers.check_running()
    assert stream_rows()[0]["Error"] is None
    clock.advance(1.0)
    pollers.check_running()
    pollers.check_running()  # marked once
    late = f"cycle running since 2027-01-15T08:01:00Z; {GOOD}"
    assert stream_rows()[0]["Error"] == late
    script.rows(("AAPL", 2.0))  # the cycle ends late, so the next one starts at once
    until(lambda: len(script.symbols) == 3)
    pollers.check_running()  # the second cycle has ended: nothing to mark
    (row,) = stream_rows()
    assert (row["State"], row["Error"]) == ("RUNNING", None)
    assert not table.j_table.isFailed()
    script.rows()  # the third cycle ends, and the poller waits for the next
    clock.wait_for_sleepers(1)


def test_a_late_cycle_fails_only_once_it_receives_no_page_for_max_outage(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    script.rows(("AAPL", 1.0))
    script.keep_record = True
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    _second_cycle_running(script, clock)
    clock.advance(250.0)
    script.record(DriftReport())  # a page: the paced cycle is making progress
    script.keep_record = False
    del script.record  # it holds the session's core
    clock.advance(300.0)  # 550 s into the cycle, 300 s after its page
    pollers.check_running()
    assert stream_rows()[0]["State"] == "RUNNING"
    assert not script.cancel.is_set()
    clock.advance(1.0)
    with caplog.at_level(logging.DEBUG, LOGGER):
        pollers.check_running()
    assert script.cancel.is_set()  # the cycle's waits end
    script.rows(("AAPL", 2.0))  # the read returns, and nothing is published
    until(lambda: bool(table.j_table.isFailed()))
    poller_of(pollers).thread.join(TIMEOUT)
    (row,) = stream_rows()
    failure = (
        f"stocks.dividends poll(): {GOOD}; the cycle running since 2027-01-15T08:01:00Z has "
        "received nothing for 301 s, longer than MassiveConfig.max_outage (300 s); the rows "
        "since then may be missing, so this table stopped; call the verb again for a new live "
        "table"
    )
    assert (row["State"], row["Error"]) == ("FAILED", failure)
    assert (
        "stocks.dividends poll() stream 1: no page for 301 s, max_outage is 300 s; failing it"
        in caplog.messages
    )


@pytest.mark.parametrize(
    ("max_outage", "stall", "fails"),
    [(0.0, 500.0, False), (0.5, 0.9, False), (0.5, 1.1, True)],
    ids=["zero never fails a running cycle", "under one check", "past one check"],
)
def test_a_running_cycle_fails_only_past_both_max_outage_and_one_check(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
    monkeypatch: pytest.MonkeyPatch,
    max_outage: float,
    stall: float,
    fails: bool,
) -> None:
    core = massive_session._core
    monkeypatch.setattr(core, "config", dataclasses.replace(core.config, max_outage=max_outage))
    script.rows(("AAPL", 1.0))
    script.keep_record = True
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    _second_cycle_running(script, clock)
    clock.advance(61.0)
    script.record(DriftReport())  # a page, late in the cycle
    script.keep_record = False
    del script.record  # it holds the session's core
    clock.advance(stall)
    pollers.check_running()
    assert script.cancel.is_set() is fails
    script.rows(("AAPL", 2.0))  # the read returns
    if fails:
        until(lambda: bool(table.j_table.isFailed()))
    else:
        until(lambda: len(script.symbols) == 3)  # the cycle ended, and the next one runs
        script.rows()
        assert stream_rows()[0]["State"] == "RUNNING"
        assert not table.j_table.isFailed()


def test_the_watch_thread_checks_running_cycles_and_ends_with_its_last_poller(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(poll, "WATCH_INTERVAL", 0.01)
    script.rows(("AAPL", 1.0))
    with caplog.at_level(logging.DEBUG, LOGGER):
        table = start(massive_session, script=script, pollers=pollers, clock=clock)
        _second_cycle_running(script, clock)
        clock.advance(61.0)
        late = f"cycle running since 2027-01-15T08:01:00Z; {GOOD}"
        until(lambda: stream_rows()[0]["Error"] == late)  # marked by the thread itself
        script.rows(("AAPL", 2.0))
        until(lambda: len(script.symbols) == 3)
        poller = poller_of(pollers)
        poller.stop()
        script.rows()  # the third cycle's read returns, and the stopped poller ends
        poller.thread.join(TIMEOUT)
    label = massive_session._core.label
    assert f"{label}: ending the poll watch thread: no poller is live" in caplog.messages
    del table


def test_one_watch_thread_serves_every_poller_until_the_session_closes(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    script.rows(("AAPL", 1.0))
    script.rows(("AAPL", 1.0))
    with caplog.at_level(logging.DEBUG, LOGGER):
        first = start(massive_session, script=script, pollers=pollers, clock=clock)
        second = start(massive_session, script=script, pollers=pollers, clock=clock)
        clock.wait_for_sleepers(2)
        pollers.close()
        del first, second
    label = massive_session._core.label
    watch = [m for m in caplog.messages if "poll watch thread" in m]
    assert watch == [
        f"{label}: started the poll watch thread",
        f"{label}: ending the poll watch thread: the session closed",
    ]


def test_a_failing_check_is_logged_and_the_others_still_run(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    script.rows(("AAPL", 1.0))
    start(massive_session, script=script, pollers=pollers, clock=clock)
    checked: list[poll.Poller] = []

    def broken(self: poll.Poller) -> None:
        checked.append(self)
        raise RuntimeError("broken check")

    monkeypatch.setattr(poll.Poller, "check_running", broken)
    with caplog.at_level(logging.ERROR, LOGGER):
        pollers.check_running()
    assert checked == [poller_of(pollers)]
    assert any("checking a poll()'s running cycle failed" in m for m in caplog.messages)


def test_a_zero_max_outage_fails_a_poll_at_its_first_failed_cycle(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = massive_session._core
    monkeypatch.setattr(core, "config", dataclasses.replace(core.config, max_outage=0))
    script.fail(RuntimeError("a defect"))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    poller = poller_of(pollers)
    poller.thread.join(TIMEOUT)
    until(lambda: bool(table.j_table.isFailed()))
    (row,) = stream_rows()
    failure = (
        f"stocks.dividends poll(): {NO_GOOD}; every cycle since 2027-01-15T08:00:00Z has "
        "failed, for 0 s, longer than MassiveConfig.max_outage (0 s); the rows since then may "
        "be missing, so this table stopped; call the verb again for a new live table. The "
        "latest error: a defect"
    )
    assert (row["State"], row["Error"]) == ("FAILED", failure)


def test_a_reactive_symbol_table_is_read_each_cycle(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    watch = dh_input_table({"Symbol": dtypes.string}, key_cols="Symbol")
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


def product_codes(
    dh_input_table: Callable[..., InputTable], *codes: str
) -> tuple[InputTable, ValueTable]:
    # A refreshing table of futures product codes, and its box.
    table = dh_input_table({"ProductCode": dtypes.string}, key_cols="ProductCode")
    table.add(new_table([string_col("ProductCode", list(codes))]))
    values = ValueTable("product_code", "product_code", "ProductCode", table=table)
    return table, values


def test_a_table_of_values_is_read_each_cycle_and_a_bad_value_is_skipped(
    *,
    dh_input_table: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    codes, values = product_codes(dh_input_table, "NQ", "ES", "CL,NG")
    script.rows()
    table = start(
        massive_session, script=script, pollers=pollers, clock=clock, symbols=None, values=values
    )
    clock.wait_for_sleepers(1)
    assert script.values == [("ES", "NQ")]  # the code no request can carry is never requested
    (row,) = stream_rows()
    assert row["Error"].startswith("skipping 1 symbol-table value the library refuses: 'CL,NG'")
    codes.add(new_table([string_col("ProductCode", ["6E"])]))
    until(lambda: codes.size == 4)
    script.rows()
    next_cycle(clock)
    assert script.values[-1] == ("6E", "ES", "NQ")
    del table


def test_a_bad_value_fails_the_poll_under_raise(
    *,
    dh_input_table: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = massive_session._core
    strict = dataclasses.replace(core.config, unsendable_symbols=dm.OnError.RAISE)
    monkeypatch.setattr(core, "config", strict)
    codes, values = product_codes(dh_input_table, "ES")
    script.rows()
    table = start(
        massive_session, script=script, pollers=pollers, clock=clock, symbols=None, values=values
    )
    clock.wait_for_sleepers(1)
    codes.add(new_table([string_col("ProductCode", ["CL,NG"])]))
    until(lambda: codes.size == 2)
    clock.advance(60)
    poller_of(pollers).thread.join(TIMEOUT)
    until(lambda: bool(table.j_table.isFailed()))
    (row,) = stream_rows()
    assert row["State"] == "FAILED"
    assert row["Error"].startswith("stocks.dividends: a symbol table holds 'CL,NG'")
    assert script.values == [("ES",)]


def test_a_failing_upstream_watchlist_fails_the_poll(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    source = dh_input_table({"Symbol": dtypes.string})
    source.add(new_table([string_col("Symbol", ["AAPL"])]))
    watch = dm.canonicalize_symbols(
        source,
        dm.AssetClass.STOCKS,
        convention=dm.SymbolConvention.YAHOO,
        on_error=dm.OnError.RAISE,
    )
    script.rows()
    table = start(
        massive_session, script=script, pollers=pollers, clock=clock, symbols=SymbolTable(watch)
    )
    clock.wait_for_sleepers(1)
    source.add(new_table([string_col("Symbol", ["VOD.L"])]))
    until(lambda: bool(watch.j_table.isFailed()))
    clock.advance(60)
    poller_of(pollers).thread.join(TIMEOUT)
    until(lambda: bool(table.j_table.isFailed()))
    (row,) = stream_rows()
    assert row["State"] == "FAILED"
    assert "failed upstream, so the result can no longer follow it" in row["Error"]
    assert script.symbols == [("AAPL",)]


def test_a_late_symbol_table_row_no_request_can_carry_fails_the_table_under_raise(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
    monkeypatch: pytest.MonkeyPatch,
    dh_input_table: Callable[..., InputTable],
) -> None:
    core = massive_session._core
    strict = dataclasses.replace(core.config, unsendable_symbols=dm.OnError.RAISE)
    monkeypatch.setattr(core, "config", strict)
    watch = dh_input_table({"Symbol": dtypes.string}, key_cols="Symbol")
    watch.add(new_table([string_col("Symbol", ["AAPL"])]))
    script.rows()
    table = start(
        massive_session, script=script, pollers=pollers, clock=clock, symbols=SymbolTable(watch)
    )
    clock.wait_for_sleepers(1)
    watch.add(new_table([string_col("Symbol", ["MSFT,NVDA"])]))
    until(lambda: watch.size == 2)
    clock.advance(60)
    poller_of(pollers).thread.join(TIMEOUT)
    until(lambda: bool(table.j_table.isFailed()))
    (row,) = stream_rows()
    assert row["State"] == "FAILED"
    assert row["Error"].startswith("stocks.dividends: a symbol table holds 'MSFT,NVDA'")
    assert "no request to Massive can carry" in row["Error"]
    assert script.symbols == [("AAPL",)]


def test_a_symbol_table_row_no_request_can_carry_is_skipped_and_shown(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    watch = dh_input_table({"Symbol": dtypes.string}, key_cols="Symbol")
    watch.add(new_table([string_col("Symbol", ["AAPL", "MSFT,NVDA"])]))
    script.rows()
    table = start(
        massive_session, script=script, pollers=pollers, clock=clock, symbols=SymbolTable(watch)
    )
    clock.wait_for_sleepers(1)
    assert script.symbols == [("AAPL",)]  # the bad row is never requested
    (row,) = stream_rows()
    assert row["Error"] == (
        "skipping 1 symbol-table value the library refuses: 'MSFT,NVDA'; fix or delete that row"
    )
    watch.delete(new_table([string_col("Symbol", ["MSFT,NVDA"])]))  # deleting it clears the text
    watch.add(new_table([string_col("Symbol", ["MSFT"])]))  # and the fixed row is requested
    until(lambda: watch.size == 2 and "MSFT,NVDA" not in [r[0] for r in watch.iter_tuple()])
    script.rows()
    next_cycle(clock)
    assert script.symbols[-1] == ("AAPL", "MSFT")
    (row,) = stream_rows()
    assert row["State"] == "RUNNING"
    assert row["Error"] is None
    failed = bool(table.j_table.isFailed())  # pytest cannot describe a Java method
    assert not failed


# --- Lifetime ------------------------------------------------------------------------------------


def test_close_stops_every_poller_and_marks_the_rows(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
    caplog: pytest.LogCaptureFixture,
) -> None:
    pollers = _pollers_of(massive_session)
    script.rows(("AAPL", 1.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 1.0)])
    streams = massive_session.streams()
    poller = poller_of(pollers)
    with caplog.at_level(logging.DEBUG, LOGGER):
        massive_session.close()
        assert poller.stopped
        clock.advance(60)
        poller.thread.join(TIMEOUT)
    assert not poller.thread.is_alive()
    assert "stocks.dividends poll() stream 1 stopped after 1 cycles: its table failed" in (
        caplog.messages
    )
    # The table failed, naming the close, and keeps its rows for a snapshot.
    until(lambda: bool(table.j_table.isFailed()))
    assert pairs(table.snapshot(), dh_to_pylist) == [("AAPL", 1.0)]
    (row,) = dh_to_pylist(streams)  # a view taken before the close keeps the final rows
    assert (row["State"], row["Error"]) == (
        "CLOSED",
        f"{massive_session._core.label} was closed; this table no longer updates",
    )
    del streams
    with pytest.raises(dm.MassiveError, match="is closed"):
        start(massive_session, script=script, pollers=pollers, clock=clock)


def test_a_poll_racing_close_after_registering_never_starts(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
    drain: Callable[[], None],
) -> None:
    # The session closes after the poll is tracked but before its poller starts: the poll
    # fails, its result is released, and the poller never runs.
    track = ResultRegistry.track

    def track_then_close(
        self: ResultRegistry, *args: *tuple[Table, LivenessScope, Callable[[], None]]
    ) -> ResultRecord:
        record = track(self, *args)
        massive_session.close()
        return record

    monkeypatch.setattr(ResultRegistry, "track", track_then_close)
    with pytest.raises(dm.MassiveError, match="is closed"):
        start(massive_session, script=script, pollers=pollers, clock=clock)
    monkeypatch.undo()
    poller = poller_of(pollers)
    assert poller.stopped
    assert poller.thread.ident is None  # never started
    drain()
    until(lambda: not massive_session._core.workers._workers)


def test_a_poll_racing_close_never_starts(
    *, massive_session: dm.MassiveSession, script: Script, clock: FakeClock
) -> None:
    pollers = Pollers("label")
    pollers.close()
    with pytest.raises(dm.MassiveError, match="label is closed"):
        start(massive_session, script=script, pollers=pollers, clock=clock)
    assert massive_session._core.workers._workers == {}
    # A session closed before the row is registered refuses it too.
    massive_session.close()
    with pytest.raises(dm.MassiveError, match="is closed"):
        start(massive_session, script=script, pollers=Pollers("open"), clock=clock)
    assert massive_session._core.workers._workers == {}


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
    monkeypatch.undo()
    assert pollers._live == set()
    assert massive_session._core.workers._workers == {}
    (row,) = stream_rows()
    assert (row["State"], row["HeldResults"]) == ("RELEASED", 0)


@pytest.mark.parametrize("step", ["Poller", "describe_symbols"])
def test_an_interrupt_before_registration_releases_the_table(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
    step: str,
) -> None:
    # The table exists before the poller does: an interrupt in either step still shuts it down,
    # which sets the poller's stop token.
    stop = threading.Event()

    def interrupted(*_: object, **__: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(poll, step, interrupted)
    with pytest.raises(KeyboardInterrupt):
        start(massive_session, script=script, pollers=pollers, clock=clock, stop=stop)
    monkeypatch.undo()
    until(stop.is_set)
    assert pollers._live == set()
    assert massive_session._core.workers._workers == {}


def test_a_failure_that_cannot_fail_the_table_ends_the_poller_with_a_log(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Deephaven refusing the failure is logged by the library, and the poller's thread ends
    # without an exception escaping it.
    escaped: list[threading.ExceptHookArgs] = []
    monkeypatch.setattr(threading, "excepthook", escaped.append)

    def refused(publisher: TablePublisher, error: Exception) -> None:
        raise RuntimeError("refused")

    monkeypatch.setattr(TablePublisher, "publish_failure", refused)
    script.fail(dm.EntitlementError("HTTP 403 from GET /stocks/v1/dividends"))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    poller = poller_of(pollers)
    poller.thread.join(TIMEOUT)
    assert not poller.thread.is_alive()
    assert escaped == []
    assert not poller.fail(dm.MassiveError("closed"))  # counted as failed: not tried again
    assert (
        "stocks.dividends poll() stream 1: could not fail its table with HTTP 403 from GET "
        "/stocks/v1/dividends"
    ) in caplog.text
    monkeypatch.undo()
    massive_session.release(table)


def test_a_collected_table_stops_its_poller(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    script.rows()
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    poller = poller_of(pollers)
    clock.wait_for_sleepers(1)
    with caplog.at_level(logging.DEBUG, LOGGER):
        del table
        gc.collect()
        until(lambda: poller.stopped)
        clock.advance(60)
        poller.thread.join(TIMEOUT)
    assert not poller.thread.is_alive()
    assert (
        "stocks.dividends poll() stream 1 stopped after 1 cycles: it was stopped: its table "
        "shut down"
    ) in caplog.messages


def test_a_poll_keeps_its_session_alive_while_its_table_is_used(
    *, script: Script, clock: FakeClock, fake_rest: FakeRestServer
) -> None:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    pollers = _pollers_of(m)
    script.rows()
    tables = [start(m, script=script, pollers=pollers, clock=clock)]
    poller = poller_of(pollers)
    clock.wait_for_sleepers(1)
    core = weakref.ref(m._core)
    del m
    gc.collect()
    assert core() is not None  # the running poll holds its session
    script.rows(("AAPL", 1.0))
    next_cycle(clock)  # and keeps polling
    until(lambda: tables[0].size == 1)
    tables.clear()  # nothing uses the poll any more: it stops, and the session is freed
    gc.collect()
    until(lambda: poller.stopped)
    clock.advance(60)
    poller.thread.join(TIMEOUT)
    assert not poller.thread.is_alive()
    until(lambda: core() is None)


def test_a_poll_keeps_polling_while_a_derived_table_remains(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    script.rows(("AAPL", 1.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    poller = poller_of(pollers)
    scope = new_scope()  # the user's own scope, released below
    with scope.open():
        # Not a formula: `where` would capture this frame's locals, the session's fixtures
        # among them, in the query scope until the JVM collects the table.
        derived = [table.tail(100)]
    massive_session.release(table)
    del table
    gc.collect()
    (row,) = stream_rows()
    assert (row["State"], row["HeldResults"]) == ("RELEASED", 0)
    assert not poller.stopped
    script.rows(("AAPL", 1.0), ("MSFT", 2.0))
    next_cycle(clock)
    until(lambda: pairs(derived[0], dh_to_pylist) == [("AAPL", 1.0), ("MSFT", 2.0)])
    assert stream_rows()[0]["RowsPublished"] == 3
    derived.clear()
    release_scope(massive_session._core.exec_ctx, scope)  # the last use: its poller stops
    until(lambda: poller.stopped)
    clock.advance(60)
    poller.thread.join(TIMEOUT)
    assert not poller.thread.is_alive()
    until(lambda: not massive_session._core.workers._workers)


def test_a_poll_fails_once_and_publishes_nothing_after(
    *, massive_session: dm.MassiveSession, script: Script, pollers: Pollers, clock: FakeClock
) -> None:
    # A poll the session fails (a close, a dying thread) fails once, and publishes nothing
    # more, even from a cycle that read its rows before the failure.
    script.rows(("AAPL", 1.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    poller = poller_of(pollers)
    clock.wait_for_sleepers(1)
    error = dm.MassiveError("gone")
    assert poller.fail(error)
    assert not poller.fail(error)
    rows = pa.Table.from_pylist([{"Symbol": "IBM", "CashAmount": 3.0}], schema=script.schema)
    assert poller._publish(rows) == 0
    until(lambda: bool(table.j_table.isFailed()))
    clock.advance(60)
    poller.thread.join(TIMEOUT)


def test_a_failure_after_the_close_fails_the_poll_with_the_close(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A cycle's fatal error that loses the race to the close: the close marked the row first,
    # so the table names the close, as its row does, and the failure is not logged.
    script.rows()
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    poller = poller_of(pollers)
    clock.wait_for_sleepers(1)
    core = massive_session._core
    closed = f"{core.label} was closed; this table no longer updates"
    monkeypatch.setattr(core, "fail_stream", lambda _stream_id, _error: closed)
    failures: list[str] = []
    publisher = poller._publisher

    class _Recording:
        def publish_failure(self, error: Exception) -> None:
            failures.append(str(error))
            publisher.publish_failure(error)

    recording: BlinkPublisher = _Recording()  # type: ignore[assignment]  # records its failure
    poller._publisher = recording
    with caplog.at_level(logging.ERROR, LOGGER):
        assert poller.fail(dm.AuthenticationError("HTTP 401"))
    assert failures == [closed]
    assert not caplog.records
    until(lambda: bool(table.j_table.isFailed()))
    poller.thread.join(TIMEOUT)


def test_a_cycle_after_the_core_is_gone_ends_the_poller(
    *, massive_session: dm.MassiveSession, script: Script, pollers: Pollers
) -> None:
    unused: BlinkPublisher = None  # type: ignore[assignment]  # a fake: the cycle ends first
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
        publisher=unused,
        stop=Cancel(),
    )

    def gone() -> SessionCore | None:
        return None  # the core was collected

    poller._core = gone  # type: ignore[assignment]  # a weak reference whose core is gone
    with ThreadPoolExecutor(1) as pool:
        assert poller._cycle(pool) is False
    assert poller._end_reason() == "its session is gone"


def test_a_stop_during_a_cycle_publishes_nothing(
    *, massive_session: dm.MassiveSession, script: Script, pollers: Pollers, clock: FakeClock
) -> None:
    gate = threading.Event()
    reading = threading.Event()

    def slow(
        *,
        symbols: tuple[str, ...] | AllSymbols | None,
        values: tuple[str, ...] | None,
        record: Callable[[DriftReport], None],
        on_recency: Callable[[Recency], None],
        executor: ThreadPoolExecutor,
        cancel: threading.Event,
    ) -> poll.CycleRows:
        reading.set()
        gate.wait(TIMEOUT)
        return script(
            symbols=symbols,
            values=values,
            record=record,
            on_recency=on_recency,
            executor=executor,
            cancel=cancel,
        )

    for _ in range(3):
        script.rows(("AAPL", 1.0))
    table = open_poll(
        massive_session._core,
        spec=DIVIDENDS,
        request=Request(symbols=("AAPL",)),
        interval=timedelta(seconds=60),
        mode=PollMode.APPEND,
        query="<Query test>",
        read=slow,
        pollers=pollers,
        host=None,
        path=PATH,
        recency=None,
        clock=clock,
        stop=threading.Event(),
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

    def stopped(**kwargs: object) -> poll.CycleRows:
        reading.set()
        gate.wait(TIMEOUT)
        raise copy.copy(error)  # the parameter outlives the test; its traceback must not

    table = open_poll(
        massive_session._core,
        spec=DIVIDENDS,
        request=Request(),
        interval=timedelta(seconds=60),
        mode=PollMode.UPSERT,
        query="<Query test>",
        read=stopped,
        pollers=pollers,
        host=None,
        path=PATH,
        recency=None,
        clock=clock,
        stop=threading.Event(),
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
    stranger: poll.Poller = object()  # type: ignore[assignment]  # a poller it never had
    pollers.discard(stranger)
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
    q = build_query(rest_session._core, DIVIDENDS, query_type=FetchQuery, symbols=["AAPL"])
    table = q.poll(60)
    until(lambda: pairs(table, dh_to_pylist) == [("AAPL", 0.25)])
    clock.wait_for_sleepers(1)
    assert len(fake_rest.requests) == 1


def test_a_bad_parameter_fails_poll_at_once(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    q = build_query(
        rest_session._core, DIVIDENDS, query_type=FetchQuery, symbols=["AAPL"], params={"bogus": 1}
    )
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
    q = build_query(rest_session._core, DIVIDENDS, query_type=FetchQuery, symbols=watch)
    bad = build_query(
        rest_session._core,
        DIVIDENDS,
        query_type=FetchQuery,
        symbols=new_table([string_col("Symbol", ["AAPL,MSFT"])]),
    )
    with pytest.raises(
        ValueError, match=r"^m\.stocks\.dividends\(\): no row of the symbol table can be sent"
    ):
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
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    yield m
    m.close()


def streams_of(m: dm.MassiveSession, to_pylist: ToPylist) -> list[dict[str, Any]]:
    # The session's m.streams() rows, once every row queued so far is applied.
    assert m._core.diagnostics.wait_until_shown(TIMEOUT)
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
    dh_input_table: Callable[..., InputTable],
) -> None:
    fake_rest.reply(INDEX_PATH, Reply(envelope([{"ticker": "I:SPX", "value": 1.0}])))
    fake_rest.reply(
        "/v1/indicators/sma/I%3ASPX",
        Reply(envelope({"values": [{"timestamp": 1704310200000, "value": 1.0}]})),
    )
    watch = dh_input_table({"Symbol": dtypes.string}, key_cols="Symbol")
    watch.add(new_table([string_col("Symbol", ["I:SPX"])]))
    q = build_query(routed_session._core, spec, query_type=FetchQuery, symbols=watch)
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


SMA_VALUE = Reply(envelope({"values": [{"timestamp": 1704310200000, "value": 1.0}]}))
"""One indicator value, as Massive answers a symbol's indicator path."""

CRYPTO_REFUSED = "X:BTCUSD (/v1/indicators/sma/{cryptoTicker} does not take ['adjusted'])"
"""The `Error` text of a crypto symbol whose path does not take `adjusted`."""


def test_a_symbol_routed_to_a_path_without_the_parameter_is_left_out_and_listed(
    *,
    routed_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
    caplog: pytest.LogCaptureFixture,
    dh_input_table: Callable[..., InputTable],
) -> None:
    # One symbol never fails the table: the crypto pair's path does not take `adjusted`, so
    # it is left out of each cycle and named in the Error, and the stock is polled.
    fake_rest.reply("/v1/indicators/sma/AAPL", SMA_VALUE)
    watch = dh_input_table({"Symbol": dtypes.string}, key_cols="Symbol")
    watch.add(new_table([string_col("Symbol", ["AAPL", "X:BTCUSD"])]))
    q = build_query(routed_session._core, SMA, query_type=FetchQuery, symbols=watch, adjusted=False)
    with caplog.at_level(logging.DEBUG, "deephaven_massive._ingest.rest"):
        table = q.poll(60)  # the stock path takes `adjusted`, so nothing fails up front
        until(lambda: symbols_of(table, dh_to_pylist) == ["AAPL"])
        clock.wait_for_sleepers(1)
        (row,) = streams_of(routed_session, dh_to_pylist)
        assert (row["State"], row["Error"]) == (
            "RUNNING",
            f"no rows this cycle for 1 symbol: {CRYPTO_REFUSED}",
        )
        assert row["Path"] == f"{SMA_STOCK}, {SMA_CRYPTO}"
        next_cycle(clock)  # the next cycle leaves it out again, logging only at DEBUG
    assert not table.j_table.isFailed()
    left = [r for r in caplog.records if "X:BTCUSD] left out of the cycle" in r.getMessage()]
    assert [r.levelno for r in left] == [logging.WARNING, logging.DEBUG]
    assert left[0].getMessage() == (
        "indicators.sma poll(): 1 [X:BTCUSD] left out of the cycle: "
        "/v1/indicators/sma/{cryptoTicker} does not take ['adjusted']; the table lists it in "
        "its Error and polls the other symbols"
    )
    # Once the symbol leaves the table, the Error clears.
    watch.delete(new_table([string_col("Symbol", ["X:BTCUSD"])]))
    until(lambda: dh_to_pylist(watch) == [{"Symbol": "AAPL"}])
    next_cycle(clock)
    clock.wait_for_sleepers(1)
    (row,) = streams_of(routed_session, dh_to_pylist)
    assert (row["State"], row["Error"]) == ("RUNNING", None)
    assert all("X%3ABTCUSD" not in r.target for r in fake_rest.requests)


def test_a_replace_poll_lists_a_symbol_massive_has_no_data_for(
    *,
    routed_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
) -> None:
    # A 404 for one symbol removes its rows from a REPLACE table; the Error says why, until
    # Massive has data for it again.
    fake_rest.reply("/v1/indicators/sma/AAPL", SMA_VALUE)
    fake_rest.reply("/v1/indicators/sma/MSFT", SMA_VALUE, Reply(b"{}", status=404), SMA_VALUE)
    q = build_query(routed_session._core, SMA, query_type=FetchQuery, symbols=["AAPL", "MSFT"])
    table = q.poll(60, mode=PollMode.REPLACE)
    until(lambda: symbols_of(table, dh_to_pylist) == ["AAPL", "MSFT"])
    next_cycle(clock)
    until(lambda: symbols_of(table, dh_to_pylist) == ["AAPL"])
    clock.wait_for_sleepers(1)
    (row,) = streams_of(routed_session, dh_to_pylist)
    assert (row["State"], row["Error"]) == (
        "RUNNING",
        "no rows this cycle for 1 symbol: MSFT (HTTP 404: no data)",
    )
    next_cycle(clock)
    until(lambda: symbols_of(table, dh_to_pylist) == ["AAPL", "MSFT"])
    clock.wait_for_sleepers(1)
    (row,) = streams_of(routed_session, dh_to_pylist)
    assert row["Error"] is None


@pytest.fixture
def ignoring_session(
    fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> Iterator[dm.MassiveSession]:
    # A routed session whose polls leave out an unavailable symbol quietly.
    monkeypatch.setattr(_session, "CATALOG", Catalog((DIVIDENDS, SMA, INDEX_SNAPSHOTS)))
    monkeypatch.setattr(poll, "CLOCK", clock)
    config = dm.MassiveConfig(
        api_key="test-key", rest_base_url=fake_rest.url, unavailable_symbols=dm.OnError.IGNORE
    )
    m = dm.MassiveSession(config)
    yield m
    m.close()


@pytest.fixture
def raising_session(
    fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> Iterator[dm.MassiveSession]:
    # A routed session whose polls fail on an unavailable symbol.
    monkeypatch.setattr(_session, "CATALOG", Catalog((DIVIDENDS, SMA, INDEX_SNAPSHOTS)))
    monkeypatch.setattr(poll, "CLOCK", clock)
    config = dm.MassiveConfig(
        api_key="test-key", rest_base_url=fake_rest.url, unavailable_symbols=dm.OnError.RAISE
    )
    m = dm.MassiveSession(config)
    yield m
    m.close()


def test_ignore_leaves_unavailable_symbols_out_with_no_warning(
    *,
    ignoring_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The crypto pair's path refuses `adjusted`, and Massive has no data for MSFT: both are
    # left out and listed, and nothing is logged above DEBUG.
    fake_rest.reply("/v1/indicators/sma/AAPL", SMA_VALUE)
    fake_rest.reply("/v1/indicators/sma/MSFT", Reply(b"{}", status=404))
    watch = new_table([string_col("Symbol", ["AAPL", "MSFT", "X:BTCUSD"])])
    q = build_query(
        ignoring_session._core, SMA, query_type=FetchQuery, symbols=watch, adjusted=False
    )
    with caplog.at_level(logging.DEBUG, "deephaven_massive._ingest.rest"):
        table = q.poll(60, mode=PollMode.REPLACE)
        until(lambda: symbols_of(table, dh_to_pylist) == ["AAPL"])
        clock.wait_for_sleepers(1)
    (row,) = streams_of(ignoring_session, dh_to_pylist)
    assert (row["State"], row["Error"]) == (
        "RUNNING",
        f"no rows this cycle for 2 symbols: {CRYPTO_REFUSED}; MSFT (HTTP 404: no data)",
    )
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
    assert any("X:BTCUSD] left out of the cycle" in r.getMessage() for r in caplog.records)
    assert any("no data for 1 [MSFT]" in r.getMessage() for r in caplog.records)
    # A fetch() of the same session follows the same setting: the symbol is left out quietly.
    caplog.clear()
    fetched = build_query(ignoring_session._core, SMA, query_type=FetchQuery, symbols=["MSFT"])
    with caplog.at_level(logging.WARNING, "deephaven_massive._ingest.rest"):
        fetched.fetch(wait=True)
    assert caplog.records == []


def test_raise_fails_a_poll_on_a_symbol_whose_path_refuses_a_parameter(
    *,
    raising_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: ToPylist,
) -> None:
    watch = new_table([string_col("Symbol", ["AAPL", "X:BTCUSD"])])
    q = build_query(
        raising_session._core, SMA, query_type=FetchQuery, symbols=watch, adjusted=False
    )
    table = q.poll(60)
    until(lambda: bool(table.j_table.isFailed()))
    until(lambda: streams_of(raising_session, dh_to_pylist)[0]["State"] == "FAILED")
    (row,) = streams_of(raising_session, dh_to_pylist)
    assert row["Error"] == (
        "indicators.sma poll(): X:BTCUSD cannot be polled: /v1/indicators/sma/{cryptoTicker} "
        "does not take ['adjusted']. Remove the symbol, or set MassiveConfig("
        "unavailable_symbols=dm.OnError.LOG) to leave it out and keep polling the others"
    )
    assert fake_rest.requests == []


def test_raise_fails_a_poll_on_a_symbol_massive_has_no_data_for(
    *,
    raising_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: ToPylist,
) -> None:
    fake_rest.reply("/v1/indicators/sma/AAPL", SMA_VALUE)
    fake_rest.reply("/v1/indicators/sma/MSFT", Reply(b"{}", status=404))
    q = build_query(raising_session._core, SMA, query_type=FetchQuery, symbols=["AAPL", "MSFT"])
    table = q.poll(60, mode=PollMode.REPLACE)
    until(lambda: bool(table.j_table.isFailed()))
    until(lambda: streams_of(raising_session, dh_to_pylist)[0]["State"] == "FAILED")
    (row,) = streams_of(raising_session, dh_to_pylist)
    assert row["Error"] == (
        "indicators.sma poll(): MSFT cannot be polled: HTTP 404 from "
        "/v1/indicators/sma/{stockTicker}: no data. Remove the symbol, or set MassiveConfig("
        "unavailable_symbols=dm.OnError.LOG) to leave it out and keep polling the others"
    )


def test_a_long_list_of_left_out_symbols_is_counted(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    script.left_out = tuple(f"S{n} (HTTP 404: no data)" for n in range(poll.LEFT_OUT_SHOWN + 2))
    script.rows(("AAPL", 1.0))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    clock.wait_for_sleepers(1)
    shown = "; ".join(script.left_out[: poll.LEFT_OUT_SHOWN])
    assert stream_rows()[0]["Error"] == (
        f"no rows this cycle for {poll.LEFT_OUT_SHOWN + 2} symbols: {shown} and 2 more"
    )
    del table


def test_a_cycle_reports_its_pages_drift(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
) -> None:
    fake_rest.reply(PATH, Reply(envelope([{"ticker": "AAPL", "brand_new": 1}])))
    q = build_query(rest_session._core, DIVIDENDS, query_type=FetchQuery, symbols=["AAPL"])
    table = q.poll(60)
    clock.wait_for_sleepers(1)
    drift = streams_of(rest_session, dh_to_pylist)[0]["SchemaDrift"]
    assert drift == "REST:brand_new+, REST:distribution_type-"
    del table


def test_a_cycle_with_odd_rows_keeps_the_table_and_shows_the_drift(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
) -> None:
    # A row without its symbol is dropped, a value that does not fit is null, and the table
    # keeps polling: one odd row never fails it.
    rows = [{"id": "x"}, {"ticker": "AAPL", "id": "a", "frequency": 4.5, "cash_amount": 1.0}]
    fake_rest.reply(PATH, Reply(envelope(rows)))
    q = build_query(rest_session._core, DIVIDENDS, query_type=FetchQuery, symbols=["AAPL"])
    table = q.poll(60)
    clock.wait_for_sleepers(1)
    (row,) = streams_of(rest_session, dh_to_pylist)
    assert (row["State"], row["Error"], row["RowsPublished"]) == ("RUNNING", None, 1)
    assert "REST:frequency?" in row["SchemaDrift"]
    assert [(r["Id"], r["Frequency"]) for r in dh_to_pylist(table)] == [("a", None)]
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
    assert stream_rows()[0]["Error"] == f"publish failed; {NO_GOOD}"
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
    assert stream_rows()[0]["Error"] == f"conversion failed; {GOOD}"
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
    dh_input_table: Callable[..., InputTable],
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
    watch = dh_input_table({"Symbol": dtypes.string}, key_cols="Symbol")
    watch.add(new_table([string_col("Symbol", ["AAPL"])]))
    q = build_query(
        routed_session._core, crypto_first, query_type=FetchQuery, symbols=watch, adjusted=False
    )
    table = q.poll(60, mode=PollMode.REPLACE)
    until(lambda: symbols_of(table, dh_to_pylist) == ["AAPL"])
    watch.delete(new_table([string_col("Symbol", ["AAPL"])]))
    until(lambda: dh_to_pylist(watch) == [])
    next_cycle(clock)
    until(lambda: symbols_of(table, dh_to_pylist) == [])
    failed = bool(table.j_table.isFailed())  # pytest cannot describe a Java method
    assert not failed


def test_the_path_column_names_the_paths_static_symbols_read(
    *, routed_session: dm.MassiveSession, fake_rest: FakeRestServer, dh_to_pylist: ToPylist
) -> None:
    fake_rest.reply("/v1/indicators/sma/AAPL", Reply(envelope({"values": []})))
    q = build_query(routed_session._core, SMA, query_type=FetchQuery, symbols=["AAPL"])
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
    time_axis=("ex_dividend_date.gte", "ex_dividend_date.lte"),
    poll_mode=PollMode.APPEND,
    poll_keys=("Id",),
)


@pytest.fixture
def feed_session(
    fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> Iterator[dm.MassiveSession]:
    monkeypatch.setattr(_session, "CATALOG", Catalog((FEED,)))
    monkeypatch.setattr(poll, "CLOCK", clock)
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    yield m
    m.close()


def page(*rows: tuple[str, float]) -> Reply:
    # One cycle's reply: dividends identified by their id.
    return Reply(envelope([{"id": i, "ticker": "AAPL", "cash_amount": a} for i, a in rows]))


def feed(m: dm.MassiveSession) -> Table:
    # The default mode of the feed, APPEND.
    q = build_query(m._core, FEED, query_type=FetchQuery, symbols=["AAPL"], start="2025-01-02")
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
    # The row names the query, which tells two polls of one dataset apart, and its transport.
    assert (row["Query"], row["Transport"]) == (
        "m.stocks.dividends()",
        "REST",
    )
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
    q = build_query(feed_session._core, snapshot, query_type=FetchQuery, symbols=["AAPL"])
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
    assert feed_session._core.diagnostics.wait_until_shown(30.0)
    assert dh_to_pylist(feed_session.streams())[0]["Error"] == f"conversion failed; {GOOD}"
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
    q = build_query(
        feed_session._core, spec, query_type=FetchQuery, symbols=["AAPL"], start="2025-01-02"
    )
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
    q = build_query(
        feed_session._core, spec, query_type=FetchQuery, symbols=["AAPL"], start="2025-01-02"
    )
    table = q.poll(60, mode=mode)
    until(lambda: sorted(row["Id"] for row in dh_to_pylist(table)) == ["a", "b"])
    failed = bool(table.j_table.isFailed())  # pytest cannot describe a Java method
    assert not failed
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
        query="<Query test>",
        read=script,
        pollers=pollers,
        host="api.massive.com",
        path=PATH,
        recency=None,
        clock=clock,
        stop=threading.Event(),
    )
    until(lambda: bool(table.j_table.isFailed()))
    (row,) = stream_rows()
    prefix = "stocks.dividends poll(): row 0 of the cycle has a null poll key 'CashAmount': "
    assert row["Error"].startswith(prefix + "{'Symbol': 'xxx")
    rendered = row["Error"].removeprefix(prefix)
    assert len(rendered) == 300
    assert rendered.endswith("xxx...")


def test_polled_rows_share_one_stored_string_per_date(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    string_objects: Callable[[Table, str], int],
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
    assert string_objects(table, "PayDate") == 1
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
    # Under WARN a forex row whose pair the library cannot read keeps a null Symbol and its
    # text in RawSymbol; keyed on Symbol, it neither fails the null-key check nor
    # merges with another such row, and REPLACE removes it by its text.
    monkeypatch.setattr(_session, "CATALOG", Catalog((FEED,)))
    monkeypatch.setattr(poll, "CLOCK", clock)
    spec = dataclasses.replace(FEED, asset_class=AssetClass.FOREX, poll_keys=("Symbol",))
    rows = [
        {"id": "a", "ticker": "C:EURUSD"},
        {"id": "b", "ticker": "@@1"},
        {"id": "c", "ticker": "@@2"},
    ]
    fake_rest.reply(PATH, Reply(envelope(rows)), Reply(envelope(rows[:2])))
    config = dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url)
    with dm.MassiveSession(config) as m:
        q = build_query(
            m._core, spec, query_type=FetchQuery, symbols=["C:EURUSD"], start="2025-01-02"
        )
        table = q.poll(60, mode=mode)
        expected = [("a", "C:EURUSD", None), ("b", None, "@@1"), ("c", None, "@@2")]

        def held() -> list[tuple[str, str | None, str | None]]:
            return sorted((r["Id"], r["Symbol"], r["RawSymbol"]) for r in dh_to_pylist(table))

        until(lambda: held() == expected)
        next_cycle(clock)  # @@2 leaves: REPLACE removes its row alone; the others keep it
        until(lambda: len(fake_rest.requests) == 2)
        until(lambda: held() == (expected[:2] if mode is PollMode.REPLACE else expected))
        assert m._core.diagnostics.wait_until_shown(30.0)
        assert held() == (expected[:2] if mode is PollMode.REPLACE else expected)
        failed = bool(table.j_table.isFailed())  # pytest cannot describe a Java method
        assert not failed


def test_the_errors_a_retry_cannot_fix_are_permanent() -> None:
    # A poll() table fails on a PermanentError, its fatal set names only the base.
    assert issubclass(poll.SymbolRowError, PermanentError)
    assert (dm.AuthenticationError, dm.EntitlementError, PermanentError) == poll._FATAL


# --- A list column among the poll keys -------------------------------------------------------

# The 13-F holdings keyed by their CUSIP and `OtherManagers`, a `String[]`, which Deephaven
# cannot key an aggregation on.
HOLDINGS = dataclasses.replace(
    _session.CATALOG.by_name["stocks.filings.institutional_holdings"],
    poll_keys=("Cusip", "OtherManagers"),
    nullable_poll_keys={"OtherManagers": "for the tests"},
)
HOLDINGS_PATH = "/stocks/filings/vX/13-F"


class ListScript(Script):
    # A cycle reader for `HOLDINGS`: each row is a CUSIP, its other managers, and a value.
    def __init__(self, m: dm.MassiveSession) -> None:
        super().__init__(m)
        self.schema = m._core.catalog.dataset_schema(HOLDINGS).arrow_schema()

    def holdings(self, *rows: tuple[str, list[str] | None, float]) -> None:
        data = [{"Cusip": c, "OtherManagers": o, "MarketValue": v} for c, o, v in rows]
        self.outcomes.put(pa.Table.from_pylist(data, schema=self.schema))


def start_holdings(
    m: dm.MassiveSession, *, script: ListScript, pollers: Pollers, clock: FakeClock, mode: PollMode
) -> Table:
    return open_poll(
        m._core,
        spec=HOLDINGS,
        request=Request(),
        interval=timedelta(seconds=60),
        mode=mode,
        query="<Query test>",
        read=script,
        pollers=pollers,
        host="api.massive.com",
        path=HOLDINGS_PATH,
        recency=None,
        clock=clock,
        stop=threading.Event(),
    )


def holdings(table: Table, to_pylist: ToPylist) -> list[tuple[str, tuple[str, ...] | None, float]]:
    rows = to_pylist(table)
    found = []
    for r in rows:
        managers = None if r["OtherManagers"] is None else tuple(r["OtherManagers"])
        found.append((r["Cusip"], managers, r["MarketValue"]))
    return sorted(found, key=repr)


@pytest.mark.parametrize("mode", [PollMode.UPSERT, PollMode.REPLACE])
def test_a_list_poll_key_keys_by_its_items(
    *,
    massive_session: dm.MassiveSession,
    pollers: Pollers,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
    mode: PollMode,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    script = ListScript(massive_session)
    # Equal lists are one key; a null list, an empty list, and other lists are each their own.
    script.holdings(("C1", ["A"], 1.0), ("C1", ["A", "B"], 2.0), ("C1", None, 3.0), ("C1", [], 4.0))
    table = start_holdings(massive_session, script=script, pollers=pollers, clock=clock, mode=mode)
    schema = massive_session._core.catalog.dataset_schema(HOLDINGS)
    assert table.column_names == list(schema.column_names)  # no hidden column, schema order
    # The substitution of the hidden key is logged once, with the keys the table uses.
    (record,) = [r for r in caplog.records if "list poll keys" in r.getMessage()]
    assert record.levelno == logging.DEBUG
    assert record.getMessage().endswith(
        f"poll(mode={mode.value}) keys on ['Cusip', '__MassiveListKey']: the list poll keys "
        "['OtherManagers'] are keyed by their JSON text in __MassiveListKey"
    )
    first = [("C1", (), 4.0), ("C1", ("A",), 1.0), ("C1", ("A", "B"), 2.0), ("C1", None, 3.0)]
    until(lambda: holdings(table, dh_to_pylist) == sorted(first, key=repr))
    script.holdings(("C1", ["A"], 5.0))
    next_cycle(clock)
    kept: list[tuple[str, tuple[str, ...] | None, float]] = [("C1", ("A",), 5.0)]
    if mode is PollMode.UPSERT:  # the other keys stay; REPLACE tombstones them
        kept += [("C1", (), 4.0), ("C1", ("A", "B"), 2.0), ("C1", None, 3.0)]
    until(lambda: holdings(table, dh_to_pylist) == sorted(kept, key=repr))


def test_append_identities_hold_a_list_poll_key(
    *,
    massive_session: dm.MassiveSession,
    pollers: Pollers,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
) -> None:
    script = ListScript(massive_session)
    script.holdings(("C1", ["A"], 1.0), ("C1", ["A"], 9.0))  # the second is the same identity
    table = start_holdings(
        massive_session, script=script, pollers=pollers, clock=clock, mode=PollMode.APPEND
    )
    until(lambda: holdings(table, dh_to_pylist) == [("C1", ("A",), 1.0)])
    script.holdings(("C1", ["A"], 2.0), ("C1", ["B"], 3.0))
    next_cycle(clock)
    until(lambda: holdings(table, dh_to_pylist) == [("C1", ("A",), 1.0), ("C1", ("B",), 3.0)])


@pytest.mark.parametrize("mode", list(PollMode))
def test_a_list_poll_key_is_order_sensitive(
    *,
    massive_session: dm.MassiveSession,
    pollers: Pollers,
    clock: FakeClock,
    dh_to_pylist: ToPylist,
    mode: PollMode,
) -> None:
    # The same managers in another order are another key in every mode, as Deephaven compares
    # arrays: the library never reorders a list Massive sends.
    script = ListScript(massive_session)
    script.holdings(("C1", ["A", "B"], 1.0), ("C1", ["B", "A"], 2.0))
    table = start_holdings(massive_session, script=script, pollers=pollers, clock=clock, mode=mode)
    expected = [("C1", ("A", "B"), 1.0), ("C1", ("B", "A"), 2.0)]
    until(lambda: holdings(table, dh_to_pylist) == expected)


# --- Failure delivery: every mode, every way a poll fails --------------------------------------
# A table that hears of a second failure ends the process, and a poll can fail from its own
# cycle (a refused or rejected request), from the session's close, or both, while the user
# releases it or edits its watchlist. Each case polls through the fake REST server, derives the
# user's tables from the result, and checks that they end failed, that every table that failed
# heard of it exactly once, and that the process is still running.

ROW = Reply(envelope([{"ticker": "AAPL", "cash_amount": 0.25}]))
"""The one dividend each cycle reads until a case fails it."""

REFUSED = Reply(envelope(status="ERROR", message="not entitled to dividends"), status=403)
"""A cycle's reply that Massive refuses for the plan: an `EntitlementError`."""

REJECTED = Reply(envelope(status="ERROR", message="Unknown API Key"), status=401)
"""A cycle's reply that rejects the key: an `AuthenticationError`."""


@dataclasses.dataclass
class PollCase:
    """One case's session, clock, result, and the user's tables on it.

    Attributes:
        m: The session, on the fake REST server.
        fake_rest: The fake REST server.
        clock: The pollers' clock, which only the case moves.
        table: The poll's result.
        watchlist: The symbol table the poll reads each cycle, or None for static symbols.
        derived: The user's tables on the result (the `user_tables` fixture).
        heard: The result and each derived table, with the reader of its failure count.
        race: Runs calls at the same moment (the `race` fixture).
        cycles_ended: Waits until every queued failure is delivered (the `cycles_ended`
            fixture).
    """

    m: dm.MassiveSession
    fake_rest: FakeRestServer
    clock: FakeClock
    table: Table
    race: Callable[..., None]
    cycles_ended: Callable[[], None]
    watchlist: InputTable | None = None
    derived: list[Table] = dataclasses.field(default_factory=list)
    heard: list[tuple[Table, Callable[[], int]]] = dataclasses.field(default_factory=list)

    def fail_next_cycle(self, reply: Reply) -> None:
        self.fake_rest.reply_first(PATH, reply)

    def next_cycle(self) -> None:
        self.clock.advance(60)


def open_poll_case(
    m: dm.MassiveSession,
    *,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    mode: PollMode,
    watched: bool,
    user_tables: Callable[[Table, str, str], list[Table]],
    failures_heard: Callable[[Table], Callable[[], int]],
    race: Callable[..., None],
    cycles_ended: Callable[[], None],
    dh_input_table: Callable[..., InputTable],
) -> PollCase:
    # Polls AAPL's dividends, of a watchlist under `watched`, until the first cycle landed and
    # the poller waits for the next; then derives and listens.
    fake_rest.reply(PATH, ROW)
    watchlist: InputTable | None = None
    symbols: dm.SymbolsLike = ["AAPL"]
    if watched:
        symbols = watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols="Symbol")
        watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    q = build_query(m._core, DIVIDENDS, query_type=FetchQuery, symbols=symbols)
    case = PollCase(m, fake_rest, clock, q.poll(60, mode=mode), race, cycles_ended, watchlist)
    until(lambda: case.table.size == 1)
    clock.wait_for_sleepers(1)
    case.derived = user_tables(case.table, "Symbol", "CashAmount")
    case.heard = [(t, failures_heard(t)) for t in (case.table, *case.derived)]
    return case


def failed_by_cycle(reply: Reply) -> Callable[[PollCase], None]:
    def path(case: PollCase) -> None:
        case.fail_next_cycle(reply)
        case.next_cycle()

    return path


def closed(case: PollCase) -> None:
    case.m.close()


def refused_then_closed(case: PollCase) -> None:
    failed_by_cycle(REFUSED)(case)
    until(lambda: bool(case.table.j_table.isFailed()))
    case.cycles_ended()
    case.m.close()


def close_racing_a_refusal(case: PollCase) -> None:
    case.fail_next_cycle(REFUSED)
    case.race(case.m.close, case.next_cycle)


def release_racing_a_refusal(case: PollCase) -> None:
    # The user's tables keep the poll running after the release, so its refused cycle still
    # fails them.
    case.fail_next_cycle(REFUSED)
    case.race(functools.partial(case.m.release, case.table), case.next_cycle)


POLL_PATHS: dict[str, Callable[[PollCase], None]] = {
    "HTTP 403": failed_by_cycle(REFUSED),
    "HTTP 401": failed_by_cycle(REJECTED),
    "session closed": closed,
    "HTTP 403, then session closed": refused_then_closed,
    "close racing an HTTP 403": close_racing_a_refusal,
    "release racing an HTTP 403": release_racing_a_refusal,
}
"""Each way a case's poll fails, by test id."""


def is_failed(table: Table) -> bool:
    return bool(table.j_table.isFailed())


def poll_failed_once(case: PollCase) -> None:
    # The user's tables end failed; once the cycles that delivered the failure are over, every
    # table that failed heard of it exactly once.
    for table in case.derived:
        until(functools.partial(is_failed, table))
    case.cycles_ended()
    heard = [(bool(table.j_table.isFailed()), count()) for table, count in case.heard]
    assert [count for _, count in heard] == [int(failed) for failed, _ in heard]


@pytest.mark.parametrize("path", list(POLL_PATHS))
@pytest.mark.parametrize("mode", [PollMode.REPLACE, PollMode.UPSERT, PollMode.APPEND])
def test_every_poll_mode_fails_once_by_every_path(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    user_tables: Callable[[Table, str, str], list[Table]],
    failures_heard: Callable[[Table], Callable[[], int]],
    race: Callable[..., None],
    cycles_ended: Callable[[], None],
    dh_input_table: Callable[..., InputTable],
    mode: PollMode,
    path: str,
) -> None:
    case = open_poll_case(
        rest_session,
        fake_rest=fake_rest,
        clock=clock,
        mode=mode,
        watched=False,
        user_tables=user_tables,
        failures_heard=failures_heard,
        race=race,
        cycles_ended=cycles_ended,
        dh_input_table=dh_input_table,
    )
    POLL_PATHS[path](case)
    poll_failed_once(case)
    rest_session.release(case.table)


@pytest.mark.parametrize("path", ["HTTP 403", "session closed", "close racing an HTTP 403"])
def test_a_poll_failing_while_its_watchlist_changes_fails_once(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    clock: FakeClock,
    user_tables: Callable[[Table, str, str], list[Table]],
    failures_heard: Callable[[Table], Callable[[], int]],
    race: Callable[..., None],
    cycles_ended: Callable[[], None],
    dh_input_table: Callable[..., InputTable],
    path: str,
) -> None:
    # The edit lands while the cycle that reads it fails, or while the session closes.
    case = open_poll_case(
        rest_session,
        fake_rest=fake_rest,
        clock=clock,
        mode=PollMode.REPLACE,
        watched=True,
        user_tables=user_tables,
        failures_heard=failures_heard,
        race=race,
        cycles_ended=cycles_ended,
        dh_input_table=dh_input_table,
    )
    assert case.watchlist is not None
    case.watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
    POLL_PATHS[path](case)
    poll_failed_once(case)
    rest_session.release(case.table)


@pytest.mark.parametrize(
    ("pages", "shown"),
    [
        ((), "UNKNOWN"),
        ((Recency.REALTIME,), "REALTIME"),
        ((Recency.REALTIME, Recency.DELAYED), "DELAYED"),
    ],
)
def test_a_cycles_labels_set_the_rows_recency(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    clock: FakeClock,
    rows_when: Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]],
    pages: tuple[Recency, ...],
    shown: str,
) -> None:
    # The row starts UNKNOWN and shows real-time only once a page's label says so; a delayed
    # page wins over a real-time one in the same cycle.
    script.recency = pages
    script.rows(("AAPL", 0.25))
    table = open_poll(
        massive_session._core,
        spec=DIVIDENDS,
        request=Request(symbols=("AAPL",)),
        interval=timedelta(seconds=60),
        mode=PollMode.REPLACE,
        query="<Query test>",
        read=script,
        pollers=Pollers(massive_session._core.label),
        host="api.test",
        path=PATH,
        recency=Recency.UNKNOWN,
        clock=clock,
        stop=threading.Event(),
    )
    (row,) = rows_when(
        massive_session.streams(), lambda rows: bool(rows and rows[0]["RowsPublished"])
    )
    assert row["Recency"] == shown
    massive_session.release(table)


def test_a_polls_blink_table_is_created_under_the_update_graphs_shared_lock(
    *,
    massive_session: dm.MassiveSession,
    script: Script,
    pollers: Pollers,
    clock: FakeClock,
    blink_creations: list[bool],
) -> None:
    before = len(blink_creations)
    # The first cycle's rows, so its read returns at once: a read with none waits for TIMEOUT,
    # and the poller, and through it the session's reaper, outlives the test until then.
    script.rows(("AAPL", 0.25))
    table = start(massive_session, script=script, pollers=pollers, clock=clock)
    assert blink_creations[before:] == [True]
    massive_session.release(table)


def test_symbol_count_raises_a_library_defect_for_a_symbols_form_with_no_rule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # With no rule for `AllSymbols`, `dm.ALL` stands for a form added without one.
    monkeypatch.setattr(poll, "AllSymbols", type("NoRule", (), {}))
    with pytest.raises(UnmatchedFormError, match="symbol count has no rule for a value"):
        poll._symbol_count(dm.ALL)


def test_logged_symbols_name_each_form_and_raise_for_a_form_with_no_rule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert poll._logged_symbols(("AAPL", "MSFT")) == "2 [AAPL, MSFT]"
    assert poll._logged_symbols(dm.ALL) == "ALL"
    assert poll._logged_symbols(None) == "none"
    # With no rule for `AllSymbols`, `dm.ALL` stands for a form added without one.
    monkeypatch.setattr(poll, "AllSymbols", type("NoRule", (), {}))
    with pytest.raises(UnmatchedFormError, match="logged symbols has no rule for a value"):
        poll._logged_symbols(dm.ALL)
