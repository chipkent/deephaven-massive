"""Tests for the query classes and the verbs' synchronous validation."""

import logging
import threading
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol, cast

import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, empty_table, input_table, new_table, time_table
from deephaven.column import string_col
from deephaven.execution_context import get_exec_ctx
from deephaven.stream.table_publisher import TablePublisher
from deephaven.table import Table
from deephaven.table_listener import listen
from deephaven.update_graph import shared_lock

import deephaven_massive as dm
from deephaven_massive import _errors
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.dates import FuturesSessions, TimeRange
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.schema import (
    BARS,
    INDEX_VALUES,
    TRADES,
    Column,
    ColumnType,
    Schema,
)
from deephaven_massive._datasets.timestamps import TimeUnit
from deephaven_massive._enums import AssetClass, HistorySource, PollMode, Timespan, Transport
from deephaven_massive._namespaces.base import build_query
from deephaven_massive._query import (
    HistoryLatestQuery,
    HistoryQuery,
    Query,
    QueryBase,
    StreamHistoryLatestQuery,
    StreamHistoryQuery,
    StreamQuery,
)
from deephaven_massive._tables.liveness import new_scope

TIMEOUT = 30.0

TRADES_SPEC = DatasetSpec(
    name="stocks.trades",
    query_class=StreamHistoryLatestQuery,
    asset_class=AssetClass.STOCKS,
    schema=TRADES.schema,
    channels={None: "T"},
    fetch_method="get_last_trade",
    history_method="list_trades",
)
BARS_SPEC = DatasetSpec(
    name="forex.bars",
    query_class=HistoryQuery,
    asset_class=AssetClass.FOREX,
    schema=BARS.schema,
    is_bars=True,
    channels={Timespan.SECOND: "CAS", Timespan.MINUTE: "CA"},
    fetch_method="get_previous_close_agg",
    history_method="list_aggs",
    grouped_daily_method="get_grouped_daily_aggs",
)
VALUES_SPEC = DatasetSpec(
    name="indices.values",
    query_class=StreamHistoryLatestQuery,
    asset_class=AssetClass.INDICES,
    schema=INDEX_VALUES.schema,
    channels={None: "V"},
    flat_file_prefixes={None: "us_indices/values_v1"},
    key_maps={Transport.FLAT_FILES: INDEX_VALUES.key_maps[Transport.FLAT_FILES]},
    time_units={(Transport.FLAT_FILES, "timestamp"): TimeUnit.NANOSECONDS},
)
NEWS_SPEC = DatasetSpec(
    name="economy.inflation",
    query_class=Query,
    schema=Schema("economy.inflation", (Column("Date", ColumnType.LOCAL_DATE),)),
    fetch_method="list_inflation",
    poll_mode=PollMode.APPEND,
    poll_keys=("Date",),
)
HISTORY_SPEC = DatasetSpec(
    name="economy.yields",
    query_class=HistoryQuery,
    schema=Schema("economy.yields", (Column("Date", ColumnType.LOCAL_DATE),)),
    poll_keys=("Date",),
    history_method="x",
)
FUTURES_SPEC = DatasetSpec(
    name="futures.trades",
    query_class=StreamHistoryQuery,
    asset_class=AssetClass.FUTURES,
    schema=TRADES.schema,
    channels={None: "T"},
    history_method="list_futures_trades",
)


class FakeStreams(Protocol):
    # What the tests read from the `fake_streams` fixture (tests/conftest.py).
    opened: list[tuple[str, str | None]]
    publishers: dict[int, TablePublisher]


class Recorder:
    # Stands in for the REST and flat-file transports and records every call.
    def __init__(self) -> None:
        self.calls: list[tuple[str, DatasetSpec, Request, dict[str, Any]]] = []

    def _record(self, verb: str, spec: DatasetSpec, request: Request, **extra: object) -> Table:
        self.calls.append((verb, spec, request, dict(extra)))
        return empty_table(0)

    def fetch(self, spec: DatasetSpec, request: Request) -> Table:
        return self._record("fetch", spec, request)

    def poll(
        self, spec: DatasetSpec, request: Request, *, interval: timedelta, mode: PollMode
    ) -> Table:
        return self._record("poll", spec, request, interval=interval, mode=mode)

    def history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: TimeRange | FuturesSessions,
        *,
        dates: Sequence[date] | None = None,
    ) -> Table:
        return self._record("history", spec, request, window=window, dates=dates)


@pytest.fixture
def rest(massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch) -> Recorder:
    recorder = Recorder()
    monkeypatch.setattr(massive_session._core, "rest_transport", lambda: recorder)
    return recorder


@pytest.fixture
def flat(massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch) -> Recorder:
    recorder = Recorder()
    monkeypatch.setattr(massive_session._core, "flat_file_transport", lambda: recorder)
    return recorder


def make(session: dm.MassiveSession, spec: DatasetSpec, **kwargs: object) -> QueryBase:
    return build_query(session._core, spec, **cast("dict[str, Any]", kwargs))


def trades(session: dm.MassiveSession, symbols: object = ("AAPL",)) -> StreamHistoryLatestQuery:
    q = make(session, TRADES_SPEC, symbols=symbols)
    assert isinstance(q, StreamHistoryLatestQuery)
    return q


def news(session: dm.MassiveSession) -> Query:
    q = make(session, NEWS_SPEC)
    assert isinstance(q, Query)
    return q


# --- Query objects ---------------------------------------------------------------------------


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
        "fetch",
        "history",
        "last_by",
        "ring",
    ]
    with pytest.raises(AttributeError):
        a.__setattr__("_spec", None)


def test_query_repr(massive_session: dm.MassiveSession) -> None:
    assert repr(trades(massive_session, ["MSFT", "AAPL"])) == (
        "<StreamHistoryLatestQuery stocks.trades symbols=('AAPL', 'MSFT')>"
    )
    assert (
        repr(trades(massive_session, dm.ALL))
        == "<StreamHistoryLatestQuery stocks.trades symbols=ALL>"
    )
    watchlist = new_table([string_col("Symbol", ["A"])])
    assert repr(trades(massive_session, watchlist)) == (
        "<StreamHistoryLatestQuery stocks.trades symbols=<reactive table>>"
    )
    assert repr(make(massive_session, NEWS_SPEC)) == "<Query economy.inflation>"
    bars = make(
        massive_session,
        BARS_SPEC,
        symbols=["EUR/USD"],
        timespan=Timespan.MINUTE,
        multiplier=5,
        adjusted=False,
        start="2025-01-02",
        params={"sort": "timestamp"},
        filters={"limit": 3},
    )
    assert repr(bars) == (
        "<HistoryQuery forex.bars symbols=('C:EURUSD',) timespan=Timespan.MINUTE multiplier=5 "
        "adjusted=False start=2025-01-02 params={'sort': 'timestamp'} filters={'limit': 3}>"
    )


def test_class_hierarchy() -> None:
    assert issubclass(StreamHistoryQuery, StreamQuery)
    assert issubclass(StreamHistoryQuery, HistoryQuery)
    assert issubclass(StreamHistoryLatestQuery, StreamHistoryQuery)
    assert issubclass(HistoryLatestQuery, HistoryQuery)
    assert not hasattr(StreamQuery, "fetch")
    assert not hasattr(StreamQuery, "poll")
    assert not hasattr(HistoryQuery, "fetch")
    assert not hasattr(StreamHistoryQuery, "fetch")
    assert not hasattr(Query, "history")


# --- Transport hooks that are not implemented yet --------------------------------------------


def test_transport_hooks_raise_not_implemented(massive_session: dm.MassiveSession) -> None:
    q = trades(massive_session)
    with pytest.raises(NotImplementedError, match="REST transport"):
        q.fetch()
    with pytest.raises(NotImplementedError, match="WebSocket transport"):
        q.append_only()
    with pytest.raises(NotImplementedError, match="REST transport"):
        q.history("2025-03-03", "2025-03-03", source=HistorySource.REST)
    with pytest.raises(NotImplementedError, match="REST transport"):
        q.history("2025-03-03", "2025-03-03")  # AUTO: the dataset has no flat files
    with pytest.raises(NotImplementedError, match="REST transport"):
        news(massive_session).poll(30)


# --- fetch and poll --------------------------------------------------------------------------


def test_fetch_and_poll_delegate(massive_session: dm.MassiveSession, rest: Recorder) -> None:
    q = news(massive_session)
    q.fetch()
    q.poll(30)
    q.poll(timedelta(minutes=1), mode=PollMode.REPLACE)
    assert [(c[0], c[3]) for c in rest.calls] == [
        ("fetch", {}),
        ("poll", {"interval": timedelta(seconds=30), "mode": PollMode.APPEND}),
        ("poll", {"interval": timedelta(minutes=1), "mode": PollMode.REPLACE}),
    ]


@pytest.mark.parametrize(
    "interval", [0, -1, float("inf"), float("nan"), timedelta(0), 1e-9, 1e300, 10**400]
)
def test_poll_interval_values(
    massive_session: dm.MassiveSession, rest: Recorder, interval: object
) -> None:
    with pytest.raises(ValueError, match=r"interval must be (positive|a positive, finite)"):
        news(massive_session).poll(cast("float", interval))


@pytest.mark.parametrize("interval", ["30", True, None])
def test_poll_interval_types(
    massive_session: dm.MassiveSession, rest: Recorder, interval: object
) -> None:
    with pytest.raises(TypeError, match="interval must be a number"):
        news(massive_session).poll(cast("float", interval))


def test_poll_mode_is_an_enum(massive_session: dm.MassiveSession, rest: Recorder) -> None:
    with pytest.raises(
        TypeError, match=r"^mode must be dm\.PollMode, e\.g\. dm\.PollMode\.UPSERT$"
    ):
        news(massive_session).poll(30, mode=cast("PollMode", "upsert"))


def test_latest_fetch_rejects_all(massive_session: dm.MassiveSession, rest: Recorder) -> None:
    with pytest.raises(ValueError, match=r"m\.stocks\.snapshots\(dm\.ALL\)"):
        trades(massive_session, dm.ALL).fetch()
    trades(massive_session).fetch()
    assert rest.calls[0][2].symbols == ("AAPL",)


def test_previous_session_fetch(massive_session: dm.MassiveSession, rest: Recorder) -> None:
    q = make(massive_session, BARS_SPEC, symbols=["EUR/USD"], timespan=Timespan.DAY)
    assert isinstance(q, HistoryLatestQuery)
    q.fetch()
    assert rest.calls[0][2].symbols == ("C:EURUSD",)


# --- history ---------------------------------------------------------------------------------


def test_history_rest_passes_the_window(massive_session: dm.MassiveSession, rest: Recorder) -> None:
    trades(massive_session).history("2025-03-03", "2025-03-04", source=HistorySource.REST)
    verb, _, request, extra = rest.calls[0]
    window = extra["window"]
    assert verb == "history"
    assert window.start == datetime(2025, 3, 3, 5, tzinfo=UTC)
    assert window.end == datetime(2025, 3, 5, 5, tzinfo=UTC)
    # Transports receive the raw bounds as well as the window.
    assert (request.start, request.end) == (date(2025, 3, 3), date(2025, 3, 4))


def test_history_of_futures_dates_passes_sessions(
    massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    q = make(massive_session, FUTURES_SPEC, symbols=["ESZ5"])
    assert isinstance(q, HistoryQuery)
    q.history("2025-03-03", "2025-03-05", source=HistorySource.REST)
    window = rest.calls[0][3]["window"]
    assert isinstance(window, FuturesSessions)
    assert (window.first, window.last) == (date(2025, 3, 3), date(2025, 3, 5))


def test_history_day_zone_per_class(massive_session: dm.MassiveSession, rest: Recorder) -> None:
    q = make(massive_session, BARS_SPEC, symbols=["EUR/USD"], timespan=Timespan.HOUR)
    assert isinstance(q, HistoryQuery)
    q.history("2025-03-03", "2025-03-03", source=HistorySource.REST)
    assert rest.calls[0][3]["window"].start == datetime(2025, 3, 3, tzinfo=UTC)
    yields = make(massive_session, HISTORY_SPEC)
    assert isinstance(yields, HistoryQuery)
    yields.history("2025-03-03", "2025-03-03", source=HistorySource.REST)
    assert rest.calls[1][3]["window"].start == datetime(2025, 3, 3, 5, tzinfo=UTC)


def test_history_rest_rejects_all_except_grouped_daily(
    massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    with pytest.raises(ValueError, match="needs explicit symbols"):
        trades(massive_session, dm.ALL).history(
            "2025-03-03", "2025-03-03", source=HistorySource.REST
        )
    minute = make(massive_session, BARS_SPEC, symbols=dm.ALL, timespan=Timespan.MINUTE)
    assert isinstance(minute, HistoryQuery)
    with pytest.raises(ValueError, match="FLAT_FILES"):
        minute.history("2025-03-03", "2025-03-03", source=HistorySource.REST)
    five = make(massive_session, BARS_SPEC, symbols=dm.ALL, timespan=Timespan.DAY, multiplier=5)
    assert isinstance(five, HistoryQuery)
    with pytest.raises(ValueError, match="needs explicit symbols"):
        five.history("2025-03-03", "2025-03-03", source=HistorySource.REST)
    daily = make(massive_session, BARS_SPEC, symbols=dm.ALL, timespan=Timespan.DAY)
    assert isinstance(daily, HistoryLatestQuery)
    daily.history("2025-03-03", "2025-03-03", source=HistorySource.REST)
    assert len(rest.calls) == 1


def test_history_rest_needs_a_rest_endpoint(
    massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    values = make(massive_session, VALUES_SPEC, symbols=["I:SPX"])
    assert isinstance(values, HistoryQuery)
    with pytest.raises(ValueError, match=r"indices\.values has no REST history"):
        values.history("2025-03-03", "2025-03-03", source=HistorySource.REST)


@pytest.fixture
def s3_session(monkeypatch: pytest.MonkeyPatch) -> Iterator[dm.MassiveSession]:
    # S3 credentials set explicitly, and a clock at which early March 2025 is published.
    monkeypatch.setattr(history_source, "_now", lambda: datetime(2025, 3, 10, 16, tzinfo=UTC))
    fields = {"api_key": "test-key", "s3_access_key_id": "AKID", "s3_secret_access_key": "sk"}
    config = dm.MassiveConfig(**cast("dict[str, Any]", fields))
    session = dm.MassiveSession(config)
    yield session
    session.close()


def test_history_dispatches_by_the_source_choice(
    *,
    s3_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest, flat = Recorder(), Recorder()
    monkeypatch.setattr(s3_session._core, "rest_transport", lambda: rest)
    monkeypatch.setattr(s3_session._core, "flat_file_transport", lambda: flat)
    values = make(s3_session, VALUES_SPEC, symbols=["I:SPX"])
    assert isinstance(values, HistoryQuery)
    with caplog.at_level(logging.INFO, logger="deephaven_massive._query"):
        values.history("2025-03-03", "2025-03-04")  # AUTO: flat files are the only source
        values.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
        trades(s3_session).history("2025-03-03", "2025-03-03")  # AUTO: no flat files
    assert [c[3]["dates"] for c in flat.calls] == [
        (date(2025, 3, 3), date(2025, 3, 4)),
        (date(2025, 3, 3),),
    ]
    assert len(rest.calls) == 1
    # Only AUTO calls log their choice; a forced source does not.
    assert caplog.messages == [
        (
            "indices.values history: AUTO chose FLAT_FILES (REST cannot serve it: indices.values "
            "has no REST history)"
        ),
        (
            "stocks.trades history: AUTO chose REST (flat files cannot serve it: stocks.trades has "
            "no flat files)"
        ),
    ]


def test_history_with_no_possible_source_raises_before_any_transport(
    *, massive_session: dm.MassiveSession, rest: Recorder, flat: Recorder
) -> None:
    every = trades(massive_session, dm.ALL)  # no flat files, and REST needs explicit symbols
    with pytest.raises(ValueError, match=r"stocks\.trades history has no possible source"):
        every.history("2025-03-03", "2025-03-03")
    with pytest.raises(ValueError, match=r"stocks\.trades cannot be read from flat files"):
        trades(massive_session).history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert rest.calls == []
    assert flat.calls == []


def test_history_argument_errors(massive_session: dm.MassiveSession, flat: Recorder) -> None:
    q = trades(massive_session)
    with pytest.raises(TypeError, match=r"source must be dm\.HistorySource"):
        q.history("2025-03-03", "2025-03-03", source=cast("HistorySource", "rest"))
    with pytest.raises(ValueError, match="needs a timezone: pass an aware datetime or a date"):
        q.history("2025-03-03T09:30", "2025-03-04")
    with pytest.raises(ValueError, match="must be before end"):
        q.history("2025-03-04", "2025-03-03")
    assert flat.calls == []


def test_history_reads_a_reactive_symbol_table_once_for_every_source(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    flat: Recorder,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(_errors, "_logged", set())  # the invalid-row WARNING is logged once
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["msft", "aapl", "$$$"])]))
    reads: list[bool] = []
    iterate = Table.iter_tuple
    expected = massive_session._core.exec_ctx.j_exec_ctx

    def recorded(table: Table, *args: object, **kwargs: object) -> object:
        reads.append(get_exec_ctx().j_exec_ctx.equals(expected))
        return cast("Any", iterate)(table, *args, **kwargs)

    monkeypatch.setattr(Table, "iter_tuple", recorded)
    q = trades(massive_session, watchlist)

    def forced_rest() -> None:  # from a thread with no execution context of its own
        q.history("2025-03-03", "2025-03-03", source=HistorySource.REST)

    worker = threading.Thread(target=forced_rest)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._datasets.symbols"):
        worker.start()
        worker.join(TIMEOUT)
    q.history("2025-03-03", "2025-03-03")
    # Each verb read the table once, inside the session's context, and every source received
    # the same sorted canonical tuple, never the table itself.
    assert reads == [True, True]
    assert rest.calls[0][2].symbols == ("AAPL", "MSFT")
    assert rest.calls[1][2].symbols == ("AAPL", "MSFT")  # AUTO: the dataset has no flat files
    assert caplog.messages == [
        "a symbol table holds '$$$', which is not a valid stocks symbol; such rows are ignored"
    ]


def test_history_rejects_a_symbol_table_without_a_valid_symbol(
    *, massive_session: dm.MassiveSession, rest: Recorder, flat: Recorder
) -> None:
    for content in ([], ["$$$"]):
        q = trades(massive_session, new_table([string_col("Symbol", content)]))
        for source in HistorySource:
            with pytest.raises(ValueError, match="the symbol table holds no valid symbols"):
                q.history("2025-03-03", "2025-03-03", source=source)
    assert rest.calls == []
    assert flat.calls == []


# --- Stream verbs ----------------------------------------------------------------------------


@pytest.mark.parametrize("capacity", [0, -1, 2**31])
def test_ring_bounds(massive_session: dm.MassiveSession, capacity: int) -> None:
    with pytest.raises(ValueError, match="capacity must be between 1 and 2147483647"):
        trades(massive_session).ring(capacity)


@pytest.mark.parametrize("capacity", [1.5, True, "10"])
def test_ring_types(massive_session: dm.MassiveSession, capacity: object) -> None:
    with pytest.raises(TypeError, match="capacity must be an int"):
        trades(massive_session).ring(cast("int", capacity))


def test_last_by_takes_its_keys_positionally(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    # As Deephaven's Table.last_by(by): q.last_by("Exchange") and q.last_by(by="Exchange").
    q = trades(massive_session)
    positional = q.last_by("Exchange")
    keyword = q.last_by(by=["Exchange"])
    assert positional.is_refreshing
    assert keyword.is_refreshing
    assert len(fake_streams.opened) == 1
    massive_session.release(positional)
    massive_session.release(keyword)


def test_duplicate_last_by_keys_open_no_stream(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    with pytest.raises(ValueError, match="more than once"):
        trades(massive_session).last_by(["Exchange", "Exchange"])
    assert fake_streams.opened == []  # rejected synchronously, before any stream opens


@pytest.mark.parametrize(
    ("by", "expected"),
    [
        ((), (ValueError, "at least one column")),
        ("Nope", (ValueError, r"has no columns \['Nope'\]")),
        (["Symbol", "Nope"], (ValueError, "Nope")),
        ([1], (TypeError, "only column names")),
        ({"Symbol"}, (TypeError, "by must be a str or a sequence of str")),
        (["Symbol", "Exchange", "Symbol"], (ValueError, r"by names \['Symbol'\] more than once")),
    ],
)
def test_last_by_keys(
    massive_session: dm.MassiveSession, by: object, expected: tuple[type[Exception], str]
) -> None:
    error, message = expected
    with pytest.raises(error, match=message):
        trades(massive_session).last_by(by=cast("str", by))


def test_stream_verbs_build_results(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    q = trades(massive_session)
    results = [
        q.blink(),
        q.append_only(),
        q.ring(5),
        q.last_by(),
        q.last_by(by="Symbol"),
        q.last_by(by=("Symbol", "Exchange")),
    ]
    assert all(r.is_refreshing for r in results)
    assert [c.name for c in results[0].columns] == list(TRADES.schema.column_names)
    assert len(fake_streams.opened) == 1
    for result in results:
        massive_session.release(result)


def test_last_by_follows_a_reactive_table(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    watchlist = new_table([string_col("Symbol", ["AAPL"])])
    current = trades(massive_session, watchlist).last_by()
    assert current.is_refreshing
    massive_session.release(current)


def test_bar_channels_follow_the_timespan(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    second = make(massive_session, BARS_SPEC, symbols=["EUR/USD"], timespan=Timespan.SECOND)
    assert isinstance(second, StreamHistoryQuery)
    massive_session.release(second.blink())
    assert fake_streams.opened == [("forex.bars", "CAS")]


# --- Thread and lifecycle guards -------------------------------------------------------------


def test_verbs_raise_on_the_update_graph_thread(massive_session: dm.MassiveSession) -> None:
    q = trades(massive_session)
    errors: list[BaseException] = []
    seen = threading.Event()

    def on_update(_update: object, _replay: bool) -> None:
        if seen.is_set():
            return
        try:
            q.append_only()
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
    assert len(errors) == 1
    assert "update-graph thread" in str(errors[0])


def test_verbs_raise_while_holding_the_update_graph_lock(
    massive_session: dm.MassiveSession,
) -> None:
    q = trades(massive_session)
    update_graph = massive_session._core.exec_ctx.update_graph
    with shared_lock(update_graph), pytest.raises(ValueError, match="update-graph lock"):
        q.fetch()


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


FOREX_SPEC = DatasetSpec(
    name="forex.quotes",
    query_class=StreamQuery,
    asset_class=AssetClass.FOREX,
    schema=TRADES.schema,
    channels={None: "C"},
)


def _wait_for(condition: Callable[[], bool]) -> bool:
    # Bounded wait for an update-graph cycle to apply a change.
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(200):
        if condition():
            return True
        pause.wait(0.05)
    return condition()


def test_last_by_matches_non_canonical_spellings_in_a_reactive_table(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["EUR/USD", "not a pair"])]))
    q = make(massive_session, FOREX_SPEC, symbols=watchlist)
    assert isinstance(q, StreamQuery)
    current = q.last_by()
    row = TRADES.schema.to_arrow([{"Symbol": "C:EURUSD", "Price": 1.08}])
    fake_streams.publishers[1].add(dh_arrow.to_table(row))
    assert _wait_for(lambda: current.size == 1)
    assert dh_arrow.to_arrow(current).column("Symbol").to_pylist() == ["C:EURUSD"]
    # Removing the spelling from the watchlist evicts the canonical row.
    watchlist.delete(new_table([string_col("Symbol", ["EUR/USD"])]))
    assert _wait_for(lambda: current.size == 0)
    massive_session.release(current)


def test_last_by_over_groups_of_symbols_filters_before_aggregating(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL", "MSFT"])]))
    current = trades(massive_session, watchlist).last_by(by="Exchange")
    watchlist.delete(new_table([string_col("Symbol", ["MSFT"])]))
    assert _wait_for(lambda: watchlist.size == 1)
    # A late MSFT trade on exchange 4 must not hide AAPL's latest trade there.
    rows = TRADES.schema.to_arrow(
        [
            {"Symbol": "AAPL", "Exchange": 4, "Price": 200.0},
            {"Symbol": "MSFT", "Exchange": 4, "Price": 400.0},
        ]
    )
    fake_streams.publishers[1].add(dh_arrow.to_table(rows))
    assert _wait_for(lambda: current.size == 1)
    shown = dh_arrow.to_arrow(current).select(["Symbol", "Exchange"]).to_pylist()
    assert shown == [{"Symbol": "AAPL", "Exchange": 4}]
    massive_session.release(current)


def test_previous_session_fetch_rejects_all(
    massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    q = make(massive_session, BARS_SPEC, symbols=dm.ALL, timespan=Timespan.DAY)
    assert isinstance(q, HistoryLatestQuery)
    with pytest.raises(ValueError, match=r"forex\.bars\.fetch\(\) needs explicit symbols"):
        q.fetch()
    assert rest.calls == []


def test_ring_accepts_its_lower_bound(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    result = trades(massive_session).ring(1)
    assert result.is_refreshing
    massive_session.release(result)
