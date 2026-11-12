"""Tests for `queries/_query.py`: what every query shares, from its guards to its columns."""

import logging
import re
import threading
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from deephaven import dtypes, new_table, time_table
from deephaven.column import string_col
from deephaven.table import Table
from deephaven.table_factory import InputTable
from deephaven.table_listener import listen
from deephaven.update_graph import shared_lock
from typing_extensions import override

import deephaven_massive as dm
from deephaven_massive import _session
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._datasets.request import Request
from deephaven_massive._enums import (
    AssetClass,
    ClassFromPrefix,
    HistorySource,
    PollMode,
    Timespan,
)
from deephaven_massive._errors import LIBRARY_BUG
from deephaven_massive._tables import poll
from deephaven_massive._tables.background_read import BACKGROUND_NOTE
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive.queries import (
    FetchQuery,
    HistoryQuery,
    Query,
    StreamHistoryQuery,
    StreamQuery,
    SymbolStreamQuery,
)
from tests._fakes.clock import FakeClock
from tests._fakes.queries import (
    HISTORY_SPEC,
    LAST_TRADE_SPEC,
    NEWS_SPEC,
    TIMEOUT,
    TRADES_SPEC,
    VALUES_SPEC,
    FakeStreams,
    FlatFiles,
    Recorder,
    join_reads,
    last_trade,
    make,
    news,
    released,
    trades,
)
from tests._fakes.stream import FakeStreamTransport


def test_queries_are_immutable_hashable_and_typed(massive_session: dm.MassiveSession) -> None:
    a = trades(massive_session, ["MSFT", "AAPL"])
    b = trades(massive_session, ["AAPL", "MSFT"])
    assert a == b
    assert hash(a) == hash(b)
    assert a != make(massive_session, HISTORY_SPEC)
    assert isinstance(a, StreamQuery)
    assert isinstance(a, HistoryQuery)
    assert not hasattr(a, "dataset")
    assert [name for name in dir(a) if not name.startswith("_")] == [
        "append_only",
        "blink",
        "columns",
        "history",
        "last_by",
        "ring",
    ]
    with pytest.raises(AttributeError):
        a.__setattr__("_spec", None)


def test_class_hierarchy() -> None:
    assert issubclass(StreamHistoryQuery, SymbolStreamQuery)
    assert issubclass(SymbolStreamQuery, StreamQuery)
    assert issubclass(StreamHistoryQuery, HistoryQuery)
    assert not hasattr(StreamQuery, "fetch")
    assert not hasattr(StreamQuery, "poll")
    assert not hasattr(StreamQuery, "last_by")
    assert not hasattr(HistoryQuery, "fetch")
    assert not hasattr(StreamHistoryQuery, "fetch")
    assert not hasattr(FetchQuery, "history")


# `call 2 done: 0 rows in 0.001 s`, with the elapsed time made predictable.
_ELAPSED = re.compile(r"\d+\.\d{3} s")


def _calls(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [_ELAPSED.sub("<s> s", m) for m in caplog.messages if m.startswith("call ")]


def test_each_static_verb_is_one_logged_call(
    massive_session: dm.MassiveSession, rest: Recorder, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive.queries"):
        news(massive_session).fetch(wait=True)
        last_trade(massive_session).fetch(wait=True)
        trades(massive_session).history(
            "2025-03-03", "2025-03-04", source=HistorySource.REST, wait=True
        )
    query = repr(trades(massive_session))
    assert _calls(caplog) == [
        f"call 1 start: {news(massive_session)!r}.fetch(wait=True)",
        "call 1 done: 0 rows in <s> s",
        f"call 2 start: {last_trade(massive_session)!r}.fetch(wait=True)",
        "call 2 done: 0 rows in <s> s",
        (
            f"call 3 start: {query}.history('2025-03-03', '2025-03-04', "
            "source=<HistorySource.REST: 'rest'>, wait=True)"
        ),
        "call 3 done: 0 rows in <s> s",
    ]
    # The transport gets the id, and names its worker threads after it.
    assert rest.call_ids == [1, 2, 3]


def test_a_failed_call_logs_its_error(
    massive_session: dm.MassiveSession, rest: Recorder, caplog: pytest.LogCaptureFixture
) -> None:
    every = trades(massive_session, dm.ALL)
    with (
        caplog.at_level(logging.DEBUG, logger="deephaven_massive.queries"),
        pytest.raises(ValueError, match="needs explicit symbols") as info,
    ):
        every.history("2025-03-03", "2025-03-03", source=HistorySource.REST, wait=True)
    assert _calls(caplog) == [
        (
            f"call 1 start: {every!r}.history('2025-03-03', '2025-03-03', "
            "source=<HistorySource.REST: 'rest'>, wait=True)"
        ),
        f"call 1 failed after <s> s: ValueError: {info.value}",
    ]
    # A caller's mistake is DEBUG, with its traceback for the log.
    [failed] = [r for r in caplog.records if "failed after" in r.getMessage()]
    assert (failed.levelno, failed.exc_info is not None) == (logging.DEBUG, True)


def test_an_expected_failure_is_logged_once_at_debug(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def refused(*_: object, **__: object) -> Table:
        raise dm.EntitlementError("HTTP 403 from GET /v3/x (request_id r1): not entitled")

    monkeypatch.setattr(rest, "fetch", refused)
    with (
        caplog.at_level(logging.DEBUG, logger="deephaven_massive.queries"),
        pytest.raises(dm.EntitlementError),
    ):
        news(massive_session).fetch(wait=True)
    # The caller receives the refusal, so the call logs it only at DEBUG.
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    failed = [r for r in caplog.records if "failed after" in r.getMessage()]
    assert [(r.levelno, _ELAPSED.sub("<s> s", r.getMessage())) for r in failed] == [
        (
            logging.DEBUG,
            "call 1 failed after <s> s: HTTP 403 from GET /v3/x (request_id r1): not entitled",
        )
    ]
    assert failed[0].exc_info is None  # the traceback travels with the exception instead


@pytest.mark.parametrize(
    ("cause", "suffix"),
    [(dm.ErrorCause.LIBRARY_DEFECT, f". {LIBRARY_BUG}"), (dm.ErrorCause.USER_DEFECT, "")],
)
def test_a_defect_is_logged_at_error_even_though_it_is_raised(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    cause: dm.ErrorCause,
    suffix: str,
) -> None:
    def broken(*_: object, **__: object) -> Table:
        raise dm.MassiveError("stocks.trades: a row broke", cause=cause)

    monkeypatch.setattr(rest, "fetch", broken)
    with pytest.raises(dm.MassiveError):
        news(massive_session).fetch(wait=True)
    # A broad except in the caller must never hide a defect, so it is an ERROR that says whose.
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert [_ELAPSED.sub("<s> s", r.getMessage()) for r in errors] == [
        f"call 1 failed after <s> s: stocks.trades: a row broke{suffix}"
    ]


def test_verbs_raise_on_the_update_graph_thread(massive_session: dm.MassiveSession) -> None:
    # The JVM holds the listener, and so its closure, until it collects it: the closure reaches
    # the session through lists emptied below.
    queries = [trades(massive_session)]
    errors: list[BaseException] = []
    seen = threading.Event()

    def on_update(_update: object, _replay: bool) -> None:
        if seen.is_set():
            return
        for call in (queries[0].append_only, queries[0].columns):
            try:
                call()
            except ValueError as e:
                errors.append(e)
        seen.set()

    scope = new_scope()
    with scope.open():
        ticking = time_table("PT0.01S")
        handle = listen(ticking, on_update)
    assert seen.wait(TIMEOUT)
    handle.stop()
    scope.release()
    assert len(errors) == 2
    assert all("update-graph thread" in str(error) for error in errors)
    queries.clear()
    errors.clear()  # its traceback holds the session too


def test_verbs_work_while_holding_the_shared_lock(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    # A thread holding the update graph's shared lock is not an update-graph thread: as in a
    # console command, which holds the exclusive lock, every call works.
    q = trades(massive_session)
    with shared_lock(massive_session._core.exec_ctx.update_graph):
        result = q.append_only()
        columns = q.columns()
    assert result.is_refreshing
    assert columns.size == len(expected_columns(massive_session, TRADES_SPEC))
    massive_session.release(result)


def test_a_refused_static_verb_logs_nothing(
    massive_session: dm.MassiveSession, rest: Recorder, caplog: pytest.LogCaptureFixture
) -> None:
    # The verb's guard comes first: a refused verb (here, on a closed session) takes no call id
    # and logs no call line.
    q = trades(massive_session)
    one_row = last_trade(massive_session)
    massive_session.close()
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive.queries"):
        verbs: list[Callable[[], Table]] = [
            one_row.fetch,
            lambda: q.history("2025-03-03", source=HistorySource.REST, wait=True),
        ]
        for verb in verbs:
            with pytest.raises(dm.MassiveError, match="is closed"):
                verb()
    assert [r.getMessage() for r in caplog.records if r.levelno < logging.WARNING] == []
    assert rest.call_ids == []


def test_verbs_raise_after_close(massive_session: dm.MassiveSession) -> None:
    q = trades(massive_session)
    massive_session.close()
    with pytest.raises(dm.MassiveError, match="is closed"):
        q.blink()


def test_the_guard_runs_before_argument_checks(massive_session: dm.MassiveSession) -> None:
    q = trades(massive_session)
    massive_session.close()
    with pytest.raises(dm.MassiveError, match="is closed"):
        q.ring(0)
    with pytest.raises(dm.MassiveError, match="is closed"):
        q.last_by(by="Nope")


def assert_described(session: dm.MassiveSession, spec: DatasetSpec, table: Table) -> None:
    schema = session._core.catalog.dataset_schema(spec)
    found = table.attributes()["ColumnDescriptions"]
    described = {str(e.getKey()): str(e.getValue()) for e in found.entrySet().toArray()}
    assert described == {c.name: c.description for c in schema.columns if c.description}
    assert described
    assert table.attributes()["TableDescription"].startswith(f"{spec.name}: {spec.summary}")


def test_every_stream_shape_is_described(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    dh_input_table: Callable[..., InputTable],
) -> None:
    watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    q, reactive = trades(massive_session), trades(massive_session, watchlist)
    results = [
        q.blink(),
        q.append_only(),
        q.ring(5),
        q.last_by(),
        reactive.last_by(),  # where_in after the aggregation
        reactive.last_by(by="Exchange"),  # where_in before it
    ]
    for result in results:
        assert_described(massive_session, TRADES_SPEC, result)
    assert results[0].is_blink
    # The stream's own blink table is never described: every result is a copy.
    streams = massive_session._core.streams._streams.values()
    blinks = [state.handle.blink for state in streams if state.handle is not None]
    assert blinks
    assert all("TableDescription" not in blink.attributes() for blink in blinks)
    for result in results:
        massive_session.release(result)


def test_every_static_verb_is_described(
    *, s3_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    rest, flat = Recorder(s3_session._core.catalog), Recorder(s3_session._core.catalog)
    monkeypatch.setattr(s3_session._core, "rest_transport", lambda: rest)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
    values = make(s3_session, VALUES_SPEC, symbols=["I:SPX"])
    assert isinstance(values, HistoryQuery)
    cases = [
        (NEWS_SPEC, news(s3_session).fetch(wait=True)),
        (LAST_TRADE_SPEC, last_trade(s3_session).fetch(wait=True)),
        (TRADES_SPEC, trades(s3_session).history("2025-03-03", "2025-03-04", wait=True)),  # REST
        (VALUES_SPEC, values.history("2025-03-03", "2025-03-04", wait=True)),  # flat files
    ]
    for spec, table in cases:
        assert_described(s3_session, spec, table)
        assert not table.is_refreshing
    assert [c[0] for c in rest.calls] == ["fetch", "fetch", "history"]
    assert [c[0] for c in flat.calls] == ["history"]


def expected_columns(session: dm.MassiveSession, spec: DatasetSpec) -> list[dict[str, str]]:
    return [
        {
            "Name": c.name,
            "Type": c.type.value,
            "Description": c.description,
            "SuppliedBy": ", ".join(TRANSPORT_LABELS[t] for t in c.supplied_by),
        }
        for c in session._core.catalog.dataset_schema(spec).columns
    ]


def test_columns_lists_the_schema_without_any_request(
    massive_session: dm.MassiveSession,
    rest: Recorder,
    flat: Recorder,
    *,
    fake_streams: FakeStreams,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    table = trades(massive_session).columns()
    assert not table.is_refreshing
    assert [(c.name, c.data_type) for c in table.columns] == [
        ("Name", dtypes.string),
        ("Type", dtypes.string),
        ("Description", dtypes.string),
        ("SuppliedBy", dtypes.string),
    ]
    listed = dh_to_pylist(table)
    assert listed == expected_columns(massive_session, TRADES_SPEC)
    assert listed[0]["Name"] == "Symbol"
    assert listed[0]["Description"].startswith("The row's instrument")
    assert listed[0]["SuppliedBy"] == "REST, WebSocket"
    # The arguments choose rows, never columns.
    assert dh_to_pylist(trades(massive_session, ["MSFT", "IBM"]).columns()) == listed
    assert dh_to_pylist(trades(massive_session, dm.ALL).columns()) == listed
    assert dh_to_pylist(news(massive_session).columns()) == expected_columns(
        massive_session, NEWS_SPEC
    )
    assert (rest.calls, flat.calls, fake_streams.opened) == ([], [], [])
    # Not a result: the session tracks nothing and rejects it.
    assert massive_session._core.results.held() == 0
    with pytest.raises(ValueError, match="not"):
        massive_session.release(table)


def test_columns_works_on_a_closed_session(
    massive_session: dm.MassiveSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    q = trades(massive_session)
    massive_session.close()
    assert dh_to_pylist(q.columns()) == expected_columns(massive_session, TRADES_SPEC)


def test_columns_from_many_threads_at_once(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    # 16 threads list the columns and open blink() results of one session at the same time.
    q = trades(massive_session)
    start = threading.Barrier(16)
    sizes: list[int] = []
    blinks: list[Table] = []
    lock = threading.Lock()

    def work() -> None:
        start.wait(TIMEOUT)
        with entered(massive_session._core.exec_ctx):
            table = q.columns()
            blink = q.blink()
        with lock:
            sizes.append(table.size)
            blinks.append(blink)

    threads = [threading.Thread(target=work) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(TIMEOUT)
    columns = len(massive_session._core.catalog.dataset_schema(TRADES_SPEC).columns)
    assert sizes == [columns] * 16
    assert len(blinks) == 16
    assert fake_streams.opened == [("stocks.trades", "T")]
    for blink in blinks:
        massive_session.release(blink)


class EmptyTransport(Recorder):
    # Stands in for the REST and flat-file transports of every dataset at once. `fetch()` and
    # `history()` get an empty table in the dataset's schema, as `Recorder` gives; `poll()`
    # opens a real poll table (its publisher and `last_by` shaping) whose cycles return no rows,
    # so the result's columns come from the same code as a real poll's.
    def __init__(self, session: dm.MassiveSession) -> None:
        super().__init__(session._core.catalog)
        self._core = session._core
        self.pollers = poll.Pollers(session._core.label)
        self._clock = FakeClock(manual=True)  # one cycle per table, then a wait never ended

    @override
    def poll(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        interval: timedelta,
        mode: PollMode,
        query: str,
    ) -> Table:
        empty = self._catalog.dataset_schema(spec).arrow_schema().empty_table()
        return poll.open_poll(
            self._core,
            spec=spec,
            request=request,
            interval=interval,
            mode=mode,
            query=query,
            read=lambda **_: poll.CycleRows(empty, 0, 600.0),
            pollers=self.pollers,
            host="api.massive.com",
            path=spec.fetch_path or "",
            recency=None,
            clock=self._clock,
            stop=threading.Event(),
        )


@pytest.fixture
def faked_session(monkeypatch: pytest.MonkeyPatch) -> Iterator[dm.MassiveSession]:
    # A session whose three transports are fakes: `EmptyTransport` for REST and flat files,
    # which the S3 keys make a possible `history()` source, and the fake stream factory.
    unused = "never-sent"  # no request is made: the flat-file transport is the fake
    session = dm.MassiveSession(
        dm.MassiveConfig(api_key="test-key", s3_access_key_id="id", s3_secret_access_key=unused)
    )
    transport = EmptyTransport(session)
    streams = FakeStreamTransport(session._core)
    monkeypatch.setattr(session._core, "rest_transport", lambda: transport)
    monkeypatch.setattr(
        session._core, "flat_file_transport", lambda: FlatFiles(transport, session._core)
    )
    monkeypatch.setattr(session._core, "stream_transport", lambda: streams)
    yield session
    transport.pollers.close()
    session.close()


# A valid symbol of each asset class, for the symbols every query below names; a dataset
# without a class reads it as a stock ticker, and one without instruments ignores it.
SAMPLE_SYMBOLS = {
    AssetClass.STOCKS: "AAPL",
    AssetClass.OPTIONS: "O:AAPL250117C00150000",
    AssetClass.FUTURES: "ESZ5",
    AssetClass.INDICES: "I:SPX",
    AssetClass.FOREX: "C:EURUSD",
    AssetClass.CRYPTO: "X:BTCUSD",
    ClassFromPrefix.ANY: "AAPL",
}


def every_query(session: dm.MassiveSession, spec: DatasetSpec) -> Iterator[Query]:
    # One query per kind of call the dataset takes, each with a symbol list and with a reactive
    # symbol table (which changes the shape of `last_by()`): a bars dataset gets them for every
    # timespan it accepts (the timespan decides whether its stream methods work), any other
    # dataset once.
    symbol = SAMPLE_SYMBOLS[spec.asset_class]
    refused = Timespan.DAY if spec.asset_class is AssetClass.FUTURES else Timespan.SESSION
    spans = [span for span in Timespan if span is not refused] if spec.is_bars else [None]
    # A feed polls only with a start.
    bounds: dict[str, object] = {"start": "2025-01-02"} if spec.is_feed else {}
    for symbols in ((symbol,), new_table([string_col("Symbol", [symbol])])):
        for span in spans:
            yield make(session, spec, symbols=symbols, timespan=span, **bounds)


def verb_results(query: Query, columns: Sequence[str]) -> Iterator[tuple[str, Table]]:
    # Every result shape the query's verbs make, labelled by the call. The last column is a
    # `last_by` key that the aggregation itself would move to the front.
    spec, request = query._spec, query._request
    # The stream methods of bars Massive doesn't stream refuse the call (`_check_streams`).
    streams = not spec.is_bars or (request.timespan in spec.channels and request.multiplier == 1)
    if isinstance(query, StreamQuery) and streams:
        yield "blink()", query.blink()
        yield "append_only()", query.append_only()
        yield "ring(5)", query.ring(5)
    if isinstance(query, SymbolStreamQuery) and streams:
        yield "last_by()", query.last_by()
        yield f"last_by({columns[-1]!r})", query.last_by(columns[-1])
    if isinstance(query, FetchQuery):
        yield "fetch()", query.fetch(wait=True)
        for mode in PollMode:
            yield f"poll(mode={mode.name})", query.poll(60, mode=mode)
    if isinstance(query, HistoryQuery):
        yield "history()", query.history("2025-03-03", "2025-03-04", wait=True)


@pytest.mark.parametrize("name", list(_session.CATALOG.by_name))
def test_every_verb_returns_the_columns_in_the_order_columns_lists(
    faked_session: dm.MassiveSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    name: str,
) -> None:
    calls = []
    for query in every_query(faked_session, _session.CATALOG.by_name[name]):
        listed = [row["Name"] for row in dh_to_pylist(query.columns())]
        for call, result in verb_results(query, listed):
            calls.append(call)
            assert result.column_names == listed, f"{type(query).__name__}.{call}"
            if result.is_refreshing:
                faked_session.release(result)
    assert calls  # every dataset has a verb


def test_wait_false_reads_in_the_background(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    stream_rows: Callable[[], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    q = news(massive_session)
    with caplog.at_level(logging.DEBUG):
        fetched = q.fetch()  # the default reads in the background
        history = trades(massive_session).history(
            "2025-03-03", "2025-03-04", source=HistorySource.REST
        )
        join_reads(massive_session)
    assert fetched.is_refreshing
    assert history.is_refreshing
    # The parameters were checked on the calling thread, in call order, then each read ran on
    # the session's read pool with a token, so the reads may finish in either order.
    assert [check[0] for check in rest.checks] == ["fetch", "history"]
    assert sorted(call[0] for call in rest.calls) == ["fetch", "history"]
    assert all(isinstance(call[3]["stop"], threading.Event) for call in rest.calls)
    rows = stream_rows()
    assert [(r["StreamId"], r["Kind"], r["State"], r["Transport"]) for r in rows] == [
        (1, "read", "DONE", "REST"),
        (2, "read", "DONE", "REST"),
    ]
    # Each read logs its own outcome, from its thread; the call logs no `done` line.
    assert sorted(_calls(caplog)) == [
        "call 1 published: 0 rows in <s> s",
        f"call 1 start: {q!r}.fetch()",
        "call 2 published: 0 rows in <s> s",
        (
            f"call 2 start: {trades(massive_session)!r}.history('2025-03-03', '2025-03-04', "
            "source=<HistorySource.REST: 'rest'>)"
        ),
    ]
    released(massive_session, fetched, history)


@pytest.mark.jvm_held  # the formula hands this frame's locals to the JVM
def test_a_wait_false_result_takes_view_and_update_view(
    massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    fetched = news(massive_session).fetch(wait=False)
    history = trades(massive_session).history(
        "2025-03-03", "2025-03-04", source=HistorySource.REST, wait=False
    )
    join_reads(massive_session)
    for result in (fetched, history):
        first = result.columns[0].name
        assert result.view([first]).column_names == [first]
        assert result.update_view(f"Again = {first}").column_names[-1] == "Again"
    released(massive_session, fetched, history)


def test_wait_must_be_a_bool(massive_session: dm.MassiveSession, rest: Recorder) -> None:
    with pytest.raises(TypeError, match="wait must be a bool, not int"):
        news(massive_session).fetch(wait=cast("bool", 0))
    with pytest.raises(TypeError, match="wait must be a bool, not str"):
        trades(massive_session).history("2025-03-03", wait=cast("bool", "no"))
    assert rest.calls == []
    assert massive_session._core.workers._workers == {}


def test_wait_false_raises_argument_errors_at_the_call(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    flat: Recorder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    every = trades(massive_session, dm.ALL)
    with pytest.raises(ValueError, match="needs explicit symbols"):
        every.history("2025-03-03", "2025-03-03", source=HistorySource.REST, wait=False)
    with pytest.raises(ValueError, match=r"history has no possible source"):
        every.history("2025-03-03", "2025-03-03", wait=False)

    def refuse(*_: object, **__: object) -> None:
        raise ValueError("/fed/v1/inflation does not take adjusted")

    monkeypatch.setattr(rest, "check_fetch", refuse)  # a path that refuses a parameter
    with pytest.raises(ValueError, match="does not take adjusted"):
        news(massive_session).fetch(wait=False)
    assert rest.calls == []
    assert flat.calls == []
    assert massive_session._core.workers._workers == {}


def test_wait_false_on_an_empty_range_lands_no_rows_without_a_request(
    *,
    s3_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    rest, flat = Recorder(s3_session._core.catalog), Recorder(s3_session._core.catalog)
    monkeypatch.setattr(s3_session._core, "rest_transport", lambda: rest)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
    values = make(s3_session, VALUES_SPEC, symbols=["I:SPX"])
    assert isinstance(values, HistoryQuery)
    instant = datetime(2025, 3, 3, 15, tzinfo=UTC)  # equal instants: no row lies between
    files = values.history(instant, instant, wait=False)
    forced = trades(s3_session).history(instant, instant, source=HistorySource.REST, wait=False)
    landed = values.history("2025-03-03", "2025-03-03", wait=False)
    join_reads(s3_session)
    assert (rest.calls, rest.checks) == ([], [])
    assert [call[0] for call in flat.calls] == ["history"]  # only the range with rows
    assert isinstance(flat.calls[0][3]["stop"], threading.Event)
    assert s3_session._core.diagnostics.wait_until_shown(TIMEOUT)
    rows = sorted(dh_to_pylist(s3_session.streams()), key=lambda r: r["StreamId"])
    assert [(r["State"], r["Transport"]) for r in rows] == [
        ("DONE", "flat files"),
        ("DONE", "REST"),
        ("DONE", "flat files"),
    ]
    assert files.attributes()["TableDescription"].endswith(
        "AUTO chose it (REST cannot serve it: indices.values has no REST history). "
        + BACKGROUND_NOTE
    )
    released(s3_session, files, forced, landed)
