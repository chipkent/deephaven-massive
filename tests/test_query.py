"""Tests for the query classes, the verbs' synchronous validation, and their columns' order."""

import logging
import re
import threading
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol, TypeAlias, cast

import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, input_table, new_table, time_table
from deephaven.column import string_col
from deephaven.execution_context import get_exec_ctx
from deephaven.table import Table
from deephaven.table_listener import listen
from deephaven.update_graph import shared_lock

import deephaven_massive as dm
from deephaven_massive import _session
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.dates import DaySpan, TimeRange
from deephaven_massive._datasets.history_source import HistoryWindow
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.schema import TRANSPORT_LABELS
from deephaven_massive._enums import (
    AssetClass,
    ClassFromPrefix,
    HistorySource,
    NotBars,
    PollMode,
    Timespan,
)
from deephaven_massive._ingest import poll
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.base import build_query
from deephaven_massive._query import (
    HistoryQuery,
    Query,
    QueryBase,
    StreamHistoryQuery,
    StreamQuery,
)
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive._tables.publisher import BlinkPublisher
from deephaven_massive._types import override
from tests._fakes.clock import FakeClock
from tests._fakes.stream import FakeStreamTransport

TIMEOUT = 30.0

R, W, FF = Transport.REST, Transport.WS, Transport.FLAT_FILES
TICKER_AND_SIP = SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))

TRADES_SPEC = DatasetSpec(
    name="stocks.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    channels={NotBars.ANY: "T"},
    history_path="/v3/trades/{stockTicker}",
    keys={
        (R, "/v3/trades/{stockTicker}"): SourceKeys(
            KeySpec("stockTicker", from_request=True), KeySpec("SipTimestamp")
        ),
        (W, "stocks/T"): TICKER_AND_SIP,
    },
)
LAST_TRADE_SPEC = DatasetSpec(
    name="stocks.last_trade",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    fetch_path="/v2/last/trade/{stocksTicker}",
    keys={(R, "/v2/last/trade/{stocksTicker}"): TICKER_AND_SIP},
)
BARS_SPEC = DatasetSpec(
    name="forex.bars",
    summary="A dataset for the tests.",
    asset_class=AssetClass.FOREX,
    is_bars=True,
    channels={Timespan.SECOND: "CAS", Timespan.MINUTE: "CA"},
    history_path="/v2/aggs/ticker/{forexTicker}/range/{multiplier}/{timespan}/{from}/{to}",
    grouped_daily_path="/v2/aggs/grouped/locale/global/market/fx/{date}",
)
VALUES_SPEC = DatasetSpec(
    name="indices.values",
    summary="A dataset for the tests.",
    asset_class=AssetClass.INDICES,
    channels={NotBars.ANY: "V"},
    flat_file_prefixes={NotBars.ANY: "us_indices/values_v1"},
    keys={(FF, "us_indices/values_v1"): SourceKeys(KeySpec("Ticker"), KeySpec("Timestamp"))},
)
NEWS_SPEC = DatasetSpec(
    name="economy.inflation",
    summary="A dataset for the tests.",
    fetch_path="/fed/v1/inflation",
    poll_mode=PollMode.APPEND,
    poll_keys=("Date",),
)
HISTORY_SPEC = DatasetSpec(
    name="economy.yields",
    summary="A dataset for the tests.",
    poll_keys=("Date",),
    history_path="/fed/v1/treasury-yields",
)
FUTURES_SPEC = DatasetSpec(
    name="futures.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.FUTURES,
    channels={NotBars.ANY: "T"},
    history_path="/futures/v1/trades/{ticker}",
)


RowsWhen: TypeAlias = Callable[
    [Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]
]
"""The `rows_when` fixture: waits until a live table's rows satisfy a predicate."""


class FakeStreams(Protocol):
    # What the tests read from the `fake_streams` fixture (tests/conftest.py).
    opened: list[tuple[str, str | None]]
    publishers: dict[int, BlinkPublisher]


class Recorder:
    # Stands in for the REST and flat-file transports and records every call. Each call returns
    # an empty table in the dataset's schema, as a transport does when no row matches.
    def __init__(self, catalog: Catalog) -> None:
        self.calls: list[tuple[str, DatasetSpec, Request, dict[str, Any]]] = []
        # The call id each static verb passed, in call order.
        self.call_ids: list[int] = []
        self._catalog = catalog

    def _record(self, verb: str, spec: DatasetSpec, request: Request, **extra: object) -> Table:
        self.calls.append((verb, spec, request, dict(extra)))
        return dh_arrow.to_table(self._catalog.dataset_schema(spec).to_arrow([]))

    def fetch(self, spec: DatasetSpec, request: Request, *, call: int) -> Table:
        self.call_ids.append(call)
        return self._record("fetch", spec, request)

    def poll(
        self, spec: DatasetSpec, request: Request, *, interval: timedelta, mode: PollMode
    ) -> Table:
        return self._record("poll", spec, request, interval=interval, mode=mode)

    def history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        dates: Sequence[date] | None = None,
        call: int,
    ) -> Table:
        self.call_ids.append(call)
        return self._record("history", spec, request, window=window, dates=dates)


@pytest.fixture
def rest(massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch) -> Recorder:
    recorder = Recorder(massive_session._core.catalog)
    monkeypatch.setattr(massive_session._core, "rest_transport", lambda: recorder)
    return recorder


@pytest.fixture
def flat(massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch) -> Recorder:
    recorder = Recorder(massive_session._core.catalog)
    monkeypatch.setattr(massive_session._core, "flat_file_transport", lambda: recorder)
    return recorder


def make(session: dm.MassiveSession, spec: DatasetSpec, **kwargs: object) -> QueryBase:
    return build_query(session._core, spec, **cast("dict[str, Any]", kwargs))


def trades(session: dm.MassiveSession, symbols: object = ("AAPL",)) -> StreamHistoryQuery:
    q = make(session, TRADES_SPEC, symbols=symbols)
    assert isinstance(q, StreamHistoryQuery)
    return q


def last_trade(session: dm.MassiveSession) -> Query:
    q = make(session, LAST_TRADE_SPEC, symbols=("AAPL",))
    assert isinstance(q, Query)
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
        "columns",
        "history",
        "last_by",
        "ring",
    ]
    with pytest.raises(AttributeError):
        a.__setattr__("_spec", None)


def test_query_repr(massive_session: dm.MassiveSession) -> None:
    assert repr(trades(massive_session, ["MSFT", "AAPL"])) == (
        "<StreamHistoryQuery stocks.trades symbols=('AAPL', 'MSFT')>"
    )
    assert repr(trades(massive_session, dm.ALL)) == "<StreamHistoryQuery stocks.trades symbols=ALL>"
    watchlist = new_table([string_col("Symbol", ["A"])])
    assert repr(trades(massive_session, watchlist)) == (
        "<StreamHistoryQuery stocks.trades symbols=<symbol table>>"
    )
    assert repr(make(massive_session, NEWS_SPEC)) == "<Query economy.inflation>"
    bars = make(
        massive_session,
        BARS_SPEC,
        symbols=["C:EURUSD"],
        timespan=Timespan.MINUTE,
        multiplier=5,
        adjusted=False,
        start="2025-01-02",
        params={"sort": "timestamp"},
    )
    assert repr(bars) == (
        "<HistoryQuery forex.bars symbols=('C:EURUSD',) timespan=Timespan.MINUTE multiplier=5 "
        "adjusted=False start=2025-01-02 params={'sort': 'timestamp'}>"
    )
    filtered = make(massive_session, NEWS_SPEC, filters={"date_gte": "2025-01-02"})
    # The filter is held as the date its text names, rendered like `start`.
    assert repr(filtered) == "<Query economy.inflation filters={'date_gte': 2025-01-02}>"


def test_class_hierarchy() -> None:
    assert issubclass(StreamHistoryQuery, StreamQuery)
    assert issubclass(StreamHistoryQuery, HistoryQuery)
    assert not hasattr(StreamQuery, "fetch")
    assert not hasattr(StreamQuery, "poll")
    assert not hasattr(HistoryQuery, "fetch")
    assert not hasattr(StreamHistoryQuery, "fetch")
    assert not hasattr(Query, "history")


def _missing(query: QueryBase, verb: str) -> str:
    # The message of the AttributeError a missing verb raises.
    with pytest.raises(AttributeError) as info:
        getattr(query, verb)
    assert (info.value.name, info.value.obj) == (verb, query)
    return str(info.value)


def test_a_missing_verb_names_the_dataset_of_the_same_kind_that_has_it(
    massive_session: dm.MassiveSession,
) -> None:
    m = massive_session
    assert not hasattr(m.stocks.trades("AAPL"), "fetch")
    assert _missing(m.stocks.trades("AAPL"), "fetch") == (
        "stocks.trades has no fetch(); its verbs are blink(), append_only(), ring(), last_by(), "
        "history(); use m.stocks.last_trade(...).fetch()"
    )
    assert _missing(m.stocks.bars("AAPL", dm.Timespan.DAY), "poll") == (
        "stocks.bars has no poll(); its verbs are history(); use "
        "m.stocks.previous_day_bar(...).poll()"
    )
    # And back: the stream and the history of a one-row dataset are its kind's other datasets.
    assert _missing(m.stocks.last_quote("AAPL"), "last_by") == (
        "stocks.last_quote has no last_by(); its verbs are fetch(), poll(); use "
        "m.stocks.quotes(...).last_by()"
    )
    assert _missing(m.indices.previous_day_bar("I:SPX"), "history") == (
        "indices.previous_day_bar has no history(); its verbs are fetch(), poll(); use "
        "m.indices.bars(...).history()"
    )


def test_a_missing_verb_without_a_sibling_names_what_else_serves(
    massive_session: dm.MassiveSession,
) -> None:
    m = massive_session
    # No last-quote endpoint for options: the snapshot is each contract's current state.
    assert _missing(m.options.quotes("O:AAPL250117C00150000"), "fetch") == (
        "options.quotes has no fetch(); its verbs are blink(), append_only(), ring(), "
        "last_by(), history(); for each symbol's current state use "
        "m.options.snapshots(...).fetch()"
    )
    assert _missing(m.stocks.dividends("AAPL"), "history") == (
        "stocks.dividends has no history(); its verbs are fetch(), poll(); pass start= and end= "
        "to m.stocks.dividends(...) instead"
    )
    assert _missing(m.stocks.bars("AAPL", dm.Timespan.HOUR), "blink") == (
        "stocks.bars has no blink(); its verbs are history(); only bars of dm.Timespan.SECOND "
        "and dm.Timespan.MINUTE with multiplier=1 stream"
    )
    # Nothing else serves it: the message names the verbs the query has.
    assert _missing(m.stocks.luld("AAPL"), "history") == (
        "stocks.luld has no history(); its verbs are blink(), append_only(), ring(), last_by()"
    )
    assert _missing(make(m, HISTORY_SPEC), "fetch") == (
        "economy.yields has no fetch(); its verbs are history()"
    )


def test_any_other_missing_attribute_raises_python_s_own_error(
    massive_session: dm.MassiveSession,
) -> None:
    q = trades(massive_session)
    with pytest.raises(AttributeError, match=r"^'StreamHistoryQuery' object has no attribute "):
        _ = q.fetchh
    assert not hasattr(q, "__setstate_ex__")
    assert callable(q.blink)  # a verb the query has never reaches the fallback


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


# `call 2 done: 0 rows in 0.001 s`, with the elapsed time made predictable.
_ELAPSED = re.compile(r"\d+\.\d{3} s")


def _calls(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [_ELAPSED.sub("<s> s", m) for m in caplog.messages if m.startswith("call ")]


def test_each_static_verb_is_one_logged_call(
    massive_session: dm.MassiveSession, rest: Recorder, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive._query"):
        news(massive_session).fetch()
        last_trade(massive_session).fetch()
        trades(massive_session).history("2025-03-03", "2025-03-04", source=HistorySource.REST)
    query = repr(trades(massive_session))
    assert _calls(caplog) == [
        f"call 1 start: {news(massive_session)!r}.fetch()",
        "call 1 done: 0 rows in <s> s",
        f"call 2 start: {last_trade(massive_session)!r}.fetch()",
        "call 2 done: 0 rows in <s> s",
        (
            f"call 3 start: {query}.history('2025-03-03', '2025-03-04', "
            "source=<HistorySource.REST: 'rest'>)"
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
        caplog.at_level(logging.DEBUG, logger="deephaven_massive._query"),
        pytest.raises(ValueError, match="needs explicit symbols") as info,
    ):
        every.history("2025-03-03", "2025-03-03", source=HistorySource.REST)
    assert _calls(caplog) == [
        (
            f"call 1 start: {every!r}.history('2025-03-03', '2025-03-03', "
            "source=<HistorySource.REST: 'rest'>)"
        ),
        f"call 1 failed after <s> s: ValueError: {info.value}",
    ]
    # A caller's mistake is DEBUG, with its traceback for the log.
    [failed] = [r for r in caplog.records if "failed after" in r.getMessage()]
    assert (failed.levelno, failed.exc_info is not None) == (logging.DEBUG, True)


def test_a_failed_request_is_logged_once_at_error(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def refused(*_: object, **__: object) -> Table:
        raise dm.EntitlementError("HTTP 403 from GET /v3/x (request_id r1): not entitled")

    monkeypatch.setattr(rest, "fetch", refused)
    with pytest.raises(dm.EntitlementError):
        news(massive_session).fetch()
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert [_ELAPSED.sub("<s> s", r.getMessage()) for r in errors] == [
        "call 1 failed after <s> s: HTTP 403 from GET /v3/x (request_id r1): not entitled"
    ]
    assert errors[0].exc_info is None  # the traceback travels with the exception instead


def test_a_history_call_through_flat_files_passes_its_id(
    *, s3_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    flat = Recorder(s3_session._core.catalog)
    monkeypatch.setattr(s3_session._core, "flat_file_transport", lambda: flat)
    values = make(s3_session, VALUES_SPEC, symbols=["I:SPX"])
    assert isinstance(values, HistoryQuery)
    values.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert flat.call_ids == [1]


def test_a_one_row_fetch_passes_its_symbols(
    massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    last_trade(massive_session).fetch()
    assert [(c[0], c[1], c[2].symbols) for c in rest.calls] == [
        ("fetch", LAST_TRADE_SPEC, ("AAPL",))
    ]


# --- history ---------------------------------------------------------------------------------


def test_history_rest_passes_the_window(massive_session: dm.MassiveSession, rest: Recorder) -> None:
    trades(massive_session).history("2025-03-03", "2025-03-04", source=HistorySource.REST)
    verb, _, request, extra = rest.calls[0]
    window = extra["window"]
    assert verb == "history"
    assert window.span == DaySpan(date(2025, 3, 3), date(2025, 3, 4))
    assert window.times == TimeRange(
        datetime(2025, 3, 3, 5, tzinfo=UTC), datetime(2025, 3, 5, 5, tzinfo=UTC)
    )
    # Transports receive the parsed bounds as well as the window.
    assert (request.start, request.end) == (date(2025, 3, 3), date(2025, 3, 4))


def test_history_reads_through_now_by_default(
    massive_session: dm.MassiveSession, rest: Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime(2025, 3, 5, 15, tzinfo=UTC)
    monkeypatch.setattr(history_source, "_now", lambda: now)
    trades(massive_session).history("2025-03-03", source=HistorySource.REST)
    _, _, request, extra = rest.calls[0]
    assert request.end is None
    assert extra["window"].span == DaySpan(date(2025, 3, 3), date(2025, 3, 5))
    start = datetime(2025, 3, 5, 14, tzinfo=UTC)
    trades(massive_session).history(start, source=HistorySource.REST)
    assert rest.calls[1][3]["window"].times == TimeRange(start, now)


def test_an_empty_history_range_calls_no_transport(
    massive_session: dm.MassiveSession, rest: Recorder, flat: Recorder
) -> None:
    moment = datetime(2025, 3, 3, 15, tzinfo=UTC)
    for source in (HistorySource.AUTO, HistorySource.REST):  # the session has no S3 keys
        table = trades(massive_session).history(moment, moment, source=source)
        assert table.size == 0
        assert "Timestamp" in table.column_names
    assert rest.calls == []
    assert flat.calls == []


def test_history_of_futures_dates_passes_sessions(
    massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    q = make(massive_session, FUTURES_SPEC, symbols=["ESZ5"])
    assert isinstance(q, HistoryQuery)
    q.history("2025-03-03", "2025-03-05", source=HistorySource.REST)
    window = rest.calls[0][3]["window"]
    assert window.sessions == DaySpan(date(2025, 3, 3), date(2025, 3, 5))
    assert window.requested is None  # requested one session at a time


def test_history_day_zone_per_class(massive_session: dm.MassiveSession, rest: Recorder) -> None:
    q = make(massive_session, BARS_SPEC, symbols=["C:EURUSD"], timespan=Timespan.HOUR)
    assert isinstance(q, HistoryQuery)
    q.history("2025-03-03", "2025-03-03", source=HistorySource.REST)
    assert rest.calls[0][3]["window"].times.start == datetime(2025, 3, 3, tzinfo=UTC)
    yields = make(massive_session, HISTORY_SPEC)
    assert isinstance(yields, HistoryQuery)
    yields.history("2025-03-03", "2025-03-03", source=HistorySource.REST)
    assert rest.calls[1][3]["window"].times.start == datetime(2025, 3, 3, 5, tzinfo=UTC)


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
    assert type(daily) is HistoryQuery
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
    rest, flat = Recorder(s3_session._core.catalog), Recorder(s3_session._core.catalog)
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
    with pytest.raises(ValueError, match="must not be after end"):
        q.history("2025-03-04", "2025-03-03")
    with pytest.raises(ValueError, match="start and end must both be dates or both be datetimes"):
        q.history("2025-03-03", datetime(2025, 3, 4, tzinfo=UTC))
    assert flat.calls == []


def test_history_reads_a_reactive_symbol_table_once_for_every_source(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    flat: Recorder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["MSFT", "AAPL"])]))
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
    worker.start()
    worker.join(TIMEOUT)
    q.history("2025-03-03", "2025-03-03")
    # Each verb read the table once, inside the session's context, and every source received
    # the same sorted tuple, never the table itself.
    assert reads == [True, True]
    assert rest.calls[0][2].symbols == ("AAPL", "MSFT")
    assert rest.calls[1][2].symbols == ("AAPL", "MSFT")  # AUTO: the dataset has no flat files


def test_history_refuses_a_symbol_table_row_that_is_not_canonical(
    *, massive_session: dm.MassiveSession, rest: Recorder, flat: Recorder
) -> None:
    q = trades(massive_session, new_table([string_col("Symbol", ["AAPL", "msft", None])]))
    for source in HistorySource:
        with pytest.raises(
            ValueError, match=r"^a symbol table holds 'msft' .*dm\.canonicalize_symbols"
        ):
            q.history("2025-03-03", "2025-03-03", source=source)
    assert rest.calls == []
    assert flat.calls == []


def test_history_rejects_a_symbol_table_without_symbols(
    *, massive_session: dm.MassiveSession, rest: Recorder, flat: Recorder
) -> None:
    for content in ([], [None]):
        q = trades(massive_session, new_table([string_col("Symbol", content)]))
        for source in (HistorySource.AUTO, HistorySource.REST):  # the session has no S3 keys
            with pytest.raises(
                ValueError, match=r"^stocks\.trades: the symbol table holds no symbols$"
            ):
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
    schema = massive_session._core.catalog.dataset_schema(TRADES_SPEC)
    assert [c.name for c in results[0].columns] == list(schema.column_names)
    assert len(fake_streams.opened) == 1
    for result in results:
        massive_session.release(result)


def test_last_by_follows_a_reactive_table(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    current = trades(massive_session, watchlist).last_by()
    assert current.is_refreshing
    massive_session.release(current)


def test_a_static_symbol_table_is_a_fixed_symbol_list(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    # Read once, like fetch() reads it: every verb opens the stream an equal symbol list opens.
    fixed = trades(massive_session, new_table([string_col("Symbol", ["MSFT", "AAPL", "AAPL"])]))
    results = [
        fixed.blink(),
        fixed.append_only(),
        fixed.ring(3),
        fixed.last_by(),
        fixed.last_by("Exchange"),
        trades(massive_session, ["AAPL", "MSFT"]).blink(),
    ]
    assert all(r.is_refreshing for r in results)
    assert len(fake_streams.opened) == 1
    assert massive_session._core.diagnostics.barrier().wait(10.0)
    [row] = dh_to_pylist(massive_session.streams())
    assert (row["SymbolCount"], row["Symbols"]) == (2, "AAPL, MSFT")
    for result in results:
        massive_session.release(result)


@pytest.mark.parametrize("verb", ["blink", "append_only", "ring", "last_by"])
def test_a_static_symbol_table_without_symbols_opens_no_stream(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams, verb: str
) -> None:
    for content in ([], [None]):
        q = trades(massive_session, new_table([string_col("Symbol", content)]))
        call = getattr(q, verb)
        with pytest.raises(
            ValueError, match=r"^stocks\.trades: the symbol table holds no symbols$"
        ):
            call(3) if verb == "ring" else call()
    assert fake_streams.opened == []


@pytest.mark.parametrize("verb", ["blink", "append_only", "ring", "last_by"])
def test_a_static_symbol_table_row_that_is_not_canonical_opens_no_stream(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams, verb: str
) -> None:
    # Refused, as the same text in a list is: never canonicalized and never dropped.
    q = trades(massive_session, new_table([string_col("Symbol", ["AAPL", "msft"])]))
    call = getattr(q, verb)
    with pytest.raises(
        ValueError, match=r"^a symbol table holds 'msft' .*dm\.canonicalize_symbols"
    ):
        call(3) if verb == "ring" else call()
    assert fake_streams.opened == []


def test_bar_channels_follow_the_timespan(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    second = make(massive_session, BARS_SPEC, symbols=["C:EURUSD"], timespan=Timespan.SECOND)
    assert isinstance(second, StreamHistoryQuery)
    massive_session.release(second.blink())
    assert fake_streams.opened == [("forex.bars", "CAS")]


# --- Thread and lifecycle guards -------------------------------------------------------------


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
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive._query"):
        for verb in (one_row.fetch, lambda: q.history("2025-03-03", source=HistorySource.REST)):
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


# --- Column descriptions on every result -----------------------------------------------------


def assert_described(session: dm.MassiveSession, spec: DatasetSpec, table: Table) -> None:
    schema = session._core.catalog.dataset_schema(spec)
    found = table.attributes()["ColumnDescriptions"]
    described = {str(e.getKey()): str(e.getValue()) for e in found.entrySet().toArray()}
    assert described == {c.name: c.description for c in schema.columns if c.description}
    assert described
    assert table.attributes()["TableDescription"] == f"{spec.name}: {spec.summary}"


def test_every_stream_shape_is_described(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
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
    streams = massive_session._core._streams.values()
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
    monkeypatch.setattr(s3_session._core, "flat_file_transport", lambda: flat)
    values = make(s3_session, VALUES_SPEC, symbols=["I:SPX"])
    assert isinstance(values, HistoryQuery)
    cases = [
        (NEWS_SPEC, news(s3_session).fetch()),
        (LAST_TRADE_SPEC, last_trade(s3_session).fetch()),
        (TRADES_SPEC, trades(s3_session).history("2025-03-03", "2025-03-04")),  # REST
        (VALUES_SPEC, values.history("2025-03-03", "2025-03-04")),  # flat files
    ]
    for spec, table in cases:
        assert_described(s3_session, spec, table)
        assert not table.is_refreshing
    assert [c[0] for c in rest.calls] == ["fetch", "fetch", "history"]
    assert [c[0] for c in flat.calls] == ["history"]


# --- columns() -------------------------------------------------------------------------------


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
    assert listed[0]["Description"].startswith("The instrument")
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


FOREX_SPEC = DatasetSpec(
    name="forex.quotes",
    summary="A dataset for the tests.",
    asset_class=AssetClass.FOREX,
    channels={NotBars.ANY: "C"},
    keys={(W, "forex/C"): SourceKeys(KeySpec("Ticker"), KeySpec("ParticipantTimestamp"))},
)


def test_last_by_matches_the_rows_of_a_reactive_table(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams, rows_when: RowsWhen
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["C:EURUSD", "C:GBPUSD"])]))
    q = make(massive_session, FOREX_SPEC, symbols=watchlist)
    assert isinstance(q, StreamQuery)
    current = q.last_by()
    schema = massive_session._core.catalog.dataset_schema(FOREX_SPEC)
    row = schema.to_arrow([{"Symbol": "C:EURUSD", "AskPrice": 1.08}])
    fake_streams.publishers[1].add(dh_arrow.to_table(row))
    shown = rows_when(current, lambda rows: len(rows) == 1)
    assert [r["Symbol"] for r in shown] == ["C:EURUSD"]
    # Removing the symbol from the watchlist evicts its row.
    watchlist.delete(new_table([string_col("Symbol", ["C:EURUSD"])]))
    rows_when(current, lambda rows: not rows)
    massive_session.release(current)


def test_a_live_verb_refuses_a_symbol_table_row_that_is_not_canonical(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    watchlist = new_table([string_col("Symbol", ["C:EURUSD", "EUR/USD"])])
    q = make(massive_session, FOREX_SPEC, symbols=watchlist)
    assert isinstance(q, StreamQuery)
    for verb in (q.blink, q.append_only, q.last_by):
        with pytest.raises(ValueError, match=r"^a symbol table holds 'EUR/USD' .*use 'C:EURUSD'"):
            verb()
    assert fake_streams.publishers == {}


def test_last_by_over_groups_of_symbols_filters_before_aggregating(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams, rows_when: RowsWhen
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL", "MSFT"])]))
    current = trades(massive_session, watchlist).last_by(by="Exchange")
    watchlist.delete(new_table([string_col("Symbol", ["MSFT"])]))
    rows_when(watchlist, lambda rows: len(rows) == 1)
    # A late MSFT trade on exchange 4 must not hide AAPL's latest trade there.
    rows = massive_session._core.catalog.dataset_schema(TRADES_SPEC).to_arrow(
        [
            {"Symbol": "AAPL", "Exchange": 4, "Price": 200.0},
            {"Symbol": "MSFT", "Exchange": 4, "Price": 400.0},
        ]
    )
    fake_streams.publishers[1].add(dh_arrow.to_table(rows))
    shown = rows_when(current, lambda rows: len(rows) == 1)
    assert [(r["Symbol"], r["Exchange"]) for r in shown] == [("AAPL", 4)]
    massive_session.release(current)


def test_ring_accepts_its_lower_bound(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    result = trades(massive_session).ring(1)
    assert result.is_refreshing
    massive_session.release(result)


def test_an_empty_window_in_a_warn_session_has_the_raw_symbol_column() -> None:
    # The empty result of a window no row fits in has the session's columns, RawSymbol
    # included, as every other result of a WARN session does.
    config = dm.MassiveConfig(api_key="test-key", unknown_symbols=dm.UnknownSymbols.WARN)
    with dm.MassiveSession(config) as session:
        q = trades(session)
        empty = q._empty()
        names = [c.name for c in empty.columns]
        assert "RawSymbol" in names
        assert names == [name for (name,) in q.columns().iter_tuple(cols="Name")]


# --- column order: every dataset, every verb -------------------------------------------------


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
        self, spec: DatasetSpec, request: Request, *, interval: timedelta, mode: PollMode
    ) -> Table:
        empty = self._catalog.dataset_schema(spec).arrow_schema().empty_table()
        return poll.open_poll(
            self._core,
            spec=spec,
            request=request,
            interval=interval,
            mode=mode,
            read=lambda **_: (empty, 0),
            pollers=self.pollers,
            host="api.massive.com",
            path=spec.fetch_path or "",
            clock=self._clock,
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
    monkeypatch.setattr(session._core, "flat_file_transport", lambda: transport)
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


def every_query(session: dm.MassiveSession, spec: DatasetSpec) -> Iterator[QueryBase]:
    # One query per query class the dataset's calls can make, each with a symbol list and with
    # a reactive symbol table (which changes the shape of `last_by()`): a bars dataset gets
    # them for every timespan it accepts (the timespan picks the class), any other dataset once.
    symbol = SAMPLE_SYMBOLS[spec.asset_class]
    refused = Timespan.DAY if spec.asset_class is AssetClass.FUTURES else Timespan.SESSION
    spans = [span for span in Timespan if span is not refused] if spec.is_bars else [None]
    for symbols in ((symbol,), new_table([string_col("Symbol", [symbol])])):
        for span in spans:
            yield make(session, spec, symbols=symbols, timespan=span)


def verb_results(query: QueryBase, columns: Sequence[str]) -> Iterator[tuple[str, Table]]:
    # Every result shape the query's verbs make, labelled by the call. The last column is a
    # `last_by` key that the aggregation itself would move to the front.
    if isinstance(query, StreamQuery):
        yield "blink()", query.blink()
        yield "append_only()", query.append_only()
        yield "ring(5)", query.ring(5)
        yield "last_by()", query.last_by()
        yield f"last_by({columns[-1]!r})", query.last_by(columns[-1])
    if isinstance(query, Query):
        yield "fetch()", query.fetch()
        for mode in PollMode:
            yield f"poll(mode={mode.name})", query.poll(60, mode=mode)
    if isinstance(query, HistoryQuery):
        yield "history()", query.history("2025-03-03", "2025-03-04")


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
