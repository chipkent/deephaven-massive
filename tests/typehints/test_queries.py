"""Tests for the query classes, the verbs' synchronous validation, and their columns' order."""

import dataclasses
import logging
import re
import threading
from collections import deque
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, cast

import numpy as np
import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, new_table, time_table
from deephaven.column import string_col
from deephaven.execution_context import get_exec_ctx
from deephaven.table import Table
from deephaven.table_factory import InputTable
from deephaven.table_listener import listen
from deephaven.time import to_j_instant, to_j_local_date, to_j_time_zone
from deephaven.update_graph import shared_lock

import deephaven_massive as dm
from deephaven_massive import _session
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._datasets.dates import DaySpan, TimeRange
from deephaven_massive._datasets.history_source import HistoryWindow
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._enums import (
    AssetClass,
    ClassFromPrefix,
    HistorySource,
    NotBars,
    OnError,
    PollMode,
    Timespan,
)
from deephaven_massive._errors import LIBRARY_BUG, UnlistedPairError
from deephaven_massive._ingest import history_reads
from deephaven_massive._ingest import rest as rest_module
from deephaven_massive._massive import Patience, Transport
from deephaven_massive._override import override
from deephaven_massive._tables import poll
from deephaven_massive._tables.background_read import BACKGROUND_NOTE
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive._tables.publisher import BlinkPublisher
from deephaven_massive._tables.session_core import SessionCore
from deephaven_massive._tables.transports import (
    FailureKind,
    FlatFileRead,
    SymbolFailure,
    SymbolOutcome,
    SymbolRows,
)
from deephaven_massive.namespaces import _futures as futures
from deephaven_massive.namespaces._options import CHAIN
from deephaven_massive.namespaces._query_building import build_query
from deephaven_massive.typehints._queries import (
    MINIMUM_POLL_INTERVAL,
    FetchQuery,
    HistoryQuery,
    Query,
    QueryCall,
    StreamHistoryQuery,
    StreamQuery,
    SymbolStreamQuery,
    _shown,
    _symbols_per_read,
    bar_query_class,
    query_class,
)
from tests._fakes.clock import FakeClock
from tests._fakes.rest import FakeRestServer, Received, Reply, envelope
from tests._fakes.stream import FakeStreamTransport

if TYPE_CHECKING:
    from deephaven_massive._ingest.flatfiles import FlatFileHistory

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
    time_axis=("date.gte", "date.lte"),
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

# Both history sources, with the keys of the real datasets, so the columns each fills are real.
TRADES_FILES_SPEC = DatasetSpec(
    name="stocks.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    history_path="/v3/trades/{stockTicker}",
    flat_file_prefixes={NotBars.ANY: "us_stocks_sip/trades_v1"},
    keys={
        (R, "/v3/trades/{stockTicker}"): SourceKeys(
            KeySpec("stockTicker", from_request=True), KeySpec("SipTimestamp")
        ),
        (FF, "us_stocks_sip/trades_v1"): TICKER_AND_SIP,
    },
)
FUTURES_FILES_SPEC = DatasetSpec(
    name="futures.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.FUTURES,
    row_times=RowTimes.TICK,
    history_path="/futures/v1/trades/{ticker}",
    flat_file_prefixes={NotBars.ANY: "us_futures_{venue}/trades_v1"},
    keys={
        (R, "/futures/v1/trades/{ticker}"): SourceKeys(
            KeySpec("ticker", from_request=True),
            KeySpec("Timestamp"),
            date=KeySpec("SessionEndDate"),
        ),
        (FF, "us_futures_{venue}/trades_v1"): SourceKeys(
            KeySpec("Ticker"), KeySpec("Timestamp"), date=KeySpec("SessionEndDate")
        ),
    },
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
        # The request checks a background read made on the calling thread.
        self.checks: list[tuple[str, DatasetSpec, Request]] = []
        # Each read by symbol (a followed history()): its symbols, window, and thread.
        self.by_symbol: list[tuple[tuple[str, ...], HistoryWindow, str]] = []
        # The outcome of a symbol's read by symbol, by symbol; rows of none otherwise.
        self.outcomes: dict[str, SymbolOutcome] = {}
        # Each followed fetch() read: its symbols, its request, and its thread.
        self.fetched: list[tuple[tuple[str, ...], Request, str]] = []
        # The gates that hold a followed fetch() read of a symbol, one per read, in turn.
        self.gates: dict[str, list[threading.Event]] = {}
        # A followed fetch() read's outcome of a symbol, one per read, in turn; then `outcomes`.
        self.turns: dict[str, list[SymbolOutcome]] = {}
        # The unlisted_symbols setting each static fetch() passed.
        self.unlisted: list[OnError] = []
        # Each read of a value table's values: the values, the request, and the thread.
        self.valued: list[tuple[tuple[str, ...], Request, str]] = []
        # Raised by every read by symbol, when set: a failure of every symbol alike.
        self.raises: BaseException | None = None
        self._catalog = catalog

    def _record(self, verb: str, spec: DatasetSpec, request: Request, **extra: object) -> Table:
        self.calls.append((verb, spec, request, dict(extra)))
        return dh_arrow.to_table(self._catalog.dataset_schema(spec).to_arrow([]))

    def fetch(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        call: int,
        stop: threading.Event | None = None,
        unlisted: OnError,
    ) -> Table:
        self.call_ids.append(call)
        self.unlisted.append(unlisted)
        return self._record("fetch", spec, request, **({} if stop is None else {"stop": stop}))

    def fetch_by_symbol(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        symbols: Sequence[str],
        call: int,
        stop: threading.Event | None = None,
    ) -> dict[str, SymbolOutcome]:
        del stop
        self.call_ids.append(call)
        self.fetched.append((tuple(symbols), request, threading.current_thread().name))
        if self.raises is not None:
            raise self.raises
        for name in symbols:  # a held read waits for its gate
            held = self.gates.get(name, [])
            if held:
                assert held.pop(0).wait(TIMEOUT)
        empty = dh_arrow.to_table(self._catalog.dataset_schema(spec).to_arrow([]))
        found: dict[str, SymbolOutcome] = {}
        for name in symbols:
            turns = self.turns.get(name, [])
            found[name] = turns.pop(0) if turns else self.outcomes.get(name, SymbolRows(empty))
        return found

    def fetch_by_value(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        values: Sequence[str],
        call: int,
        stop: threading.Event | None = None,
    ) -> dict[str, SymbolOutcome]:
        del stop
        self.call_ids.append(call)
        self.valued.append((tuple(values), request, threading.current_thread().name))
        empty = dh_arrow.to_table(self._catalog.dataset_schema(spec).to_arrow([]))
        return {name: self.outcomes.get(name, SymbolRows(empty)) for name in values}

    def check_fetch(self, spec: DatasetSpec, request: Request) -> None:
        self.checks.append(("fetch", spec, request))

    def check_history(self, spec: DatasetSpec, request: Request, window: HistoryWindow) -> None:
        self.checks.append(("history", spec, request))

    def poll(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        interval: timedelta,
        mode: PollMode,
        query: str,
    ) -> Table:
        return self._record("poll", spec, request, interval=interval, mode=mode, query=query)

    def history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        dates: Sequence[date] | None = None,
        call: int,
        stop: threading.Event | None = None,
    ) -> Table:
        self.call_ids.append(call)
        extra = {} if stop is None else {"stop": stop}
        return self._record("history", spec, request, window=window, dates=dates, **extra)

    def history_by_symbol(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        symbols: Sequence[str],
        call: int,
        stop: threading.Event | None = None,
    ) -> dict[str, SymbolOutcome]:
        del request, stop
        self.call_ids.append(call)
        self.by_symbol.append((tuple(symbols), window, threading.current_thread().name))
        if self.raises is not None:
            raise self.raises
        empty = dh_arrow.to_table(self._catalog.dataset_schema(spec).to_arrow([]))
        return {name: self.outcomes.get(name, SymbolRows(empty)) for name in symbols}


class FlatFiles:
    # Stands in for the flat-file transport: a recorder's table as one part, and the days the
    # files could not serve (none: every file was read). The combined read is the library's
    # own, over this fake's files and the session's (possibly faked) REST transport.
    def __init__(
        self, recorder: Recorder, core: SessionCore, unread: tuple[date, ...] = ()
    ) -> None:
        self.recorder = recorder
        self.core = core
        self.unread = unread
        self.inferred: list[bool] = []

    def history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        dates: Sequence[date],
        call: int,
        infer_holidays: bool,
        stop: threading.Event | None = None,
    ) -> FlatFileRead:
        self.inferred.append(infer_holidays)
        table = self.recorder.history(spec, request, window, dates=dates, call=call, stop=stop)
        return FlatFileRead(((date.min, table),), table, self.unread)

    def history_by_symbol(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        symbols: Sequence[str],
        dates: Sequence[date],
        call: int,
        forced: bool,
        stop: threading.Event | None = None,
    ) -> dict[str, SymbolOutcome]:
        del dates, forced
        return self.recorder.history_by_symbol(
            spec, request, window, symbols=symbols, call=call, stop=stop
        )

    def read_history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        dates: Sequence[date],
        call: int,
        forced: bool,
        stop: threading.Event | None = None,
    ) -> tuple[Table, str | None, str | None]:
        return history_reads.read_history(
            spec,
            request,
            window,
            core=self.core,
            files=cast("FlatFileHistory", self),
            dates=dates,
            call=call,
            forced=forced,
            stop=stop,
        )


@pytest.fixture
def rest(massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch) -> Recorder:
    recorder = Recorder(massive_session._core.catalog)
    monkeypatch.setattr(massive_session._core, "rest_transport", lambda: recorder)
    return recorder


@pytest.fixture
def flat(massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch) -> Recorder:
    recorder = Recorder(massive_session._core.catalog)
    monkeypatch.setattr(
        massive_session._core,
        "flat_file_transport",
        lambda: FlatFiles(recorder, massive_session._core),
    )
    return recorder


def make(session: dm.MassiveSession, spec: DatasetSpec, **kwargs: object) -> Query:
    return build_query(session._core, spec, **cast("dict[str, Any]", kwargs))


def trades(session: dm.MassiveSession, symbols: object = ("AAPL",)) -> StreamHistoryQuery:
    q = make(session, TRADES_SPEC, symbols=symbols)
    assert isinstance(q, StreamHistoryQuery)
    return q


def last_trade(session: dm.MassiveSession) -> FetchQuery:
    q = make(session, LAST_TRADE_SPEC, symbols=("AAPL",))
    assert isinstance(q, FetchQuery)
    return q


def news(session: dm.MassiveSession, **kwargs: object) -> FetchQuery:
    q = make(session, NEWS_SPEC, **kwargs)
    assert isinstance(q, FetchQuery)
    return q


def feed(session: dm.MassiveSession) -> FetchQuery:
    # A feed query that may poll: it has a start.
    return news(session, start="2025-01-02")


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


def test_query_repr_is_the_call_that_built_it(massive_session: dm.MassiveSession) -> None:
    m = massive_session
    # Symbols come first and positionally, sorted, whatever the caller's order.
    assert repr(m.stocks.trades(["MSFT", "AAPL"])) == "m.stocks.trades(['AAPL', 'MSFT'])"
    assert repr(m.stocks.trades("AAPL")) == "m.stocks.trades('AAPL')"
    assert repr(m.stocks.trades(dm.ALL)) == "m.stocks.trades(dm.ALL)"
    watchlist = new_table([string_col("Symbol", ["A"])])
    assert repr(m.stocks.trades(watchlist)) == "m.stocks.trades(<symbol table>)"
    many = ["A", "B", "C", "D", "E", "F", "G"]
    assert repr(m.stocks.trades(many)) == ("m.stocks.trades(['A', 'B', 'C', 'D', 'E', … (2 more)])")
    # A required argument goes by position; only arguments that differ from their defaults
    # are shown, by their public names.
    assert repr(m.forex.bars("C:EURUSD", Timespan.MINUTE, multiplier=1)) == (
        "m.forex.bars('C:EURUSD', dm.Timespan.MINUTE)"
    )
    assert repr(m.forex.bars("C:EURUSD", timespan=Timespan.HOUR, multiplier=5)) == (
        "m.forex.bars('C:EURUSD', dm.Timespan.HOUR, multiplier=5)"
    )
    # Time bounds are written as the request holds them, parsed.
    assert repr(m.economy.inflation(start=date(2025, 1, 2), limit=10)) == (
        "m.economy.inflation(start='2025-01-02', limit=10)"
    )
    # Filters as the request parsed them; an empty mapping selects nothing and is left out.
    filtered = m.stocks.dividends(filters={"distribution_type": "special"})
    assert repr(filtered) == "m.stocks.dividends(filters={'distribution_type': 'special'})"
    assert repr(m.stocks.dividends(filters={})) == "m.stocks.dividends()"
    # A query built without its dataset method names only the dataset.
    assert repr(make(m, NEWS_SPEC)) == "m.economy.inflation()"


def test_query_repr_writes_values_as_a_user_types_them() -> None:
    assert _shown(Timespan.DAY) == "dm.Timespan.DAY"
    assert _shown(datetime(2025, 1, 2, 3, 4, tzinfo=UTC)) == "'2025-01-02T03:04:00+00:00'"
    # A set prints in one order every run: enum members in their enum's order, others by text.
    assert _shown(frozenset({Timespan.DAY, Timespan.MINUTE})) == (
        "{dm.Timespan.MINUTE, dm.Timespan.DAY}"
    )
    assert _shown(frozenset({"b", "a"})) == "{'a', 'b'}"
    assert _shown(("x", "y")) == "['x', 'y']"
    assert _shown(1.5) == "1.5"


def test_query_repr_writes_java_and_numpy_times_as_text(
    massive_session: dm.MassiveSession,
) -> None:
    # A day argument the request does not hold parsed is written as the caller's value.
    day = to_j_local_date("2025-01-02")
    assert day is not None
    assert repr(massive_session.stocks.daily_open_close("AAPL", day)) == (
        "m.stocks.daily_open_close('AAPL', '2025-01-02')"
    )
    assert _shown(np.datetime64("2025-01-02")) == "'2025-01-02'"
    assert _shown(to_j_instant("2025-01-02T03:04:00Z")) == "'2025-01-02T03:04:00+00:00'"
    # A Java object of another class, and a time no parser reads, keep their repr.
    other = to_j_time_zone("UTC")
    assert _shown(other) == repr(other)
    assert _shown(np.datetime64("NaT", "D")) == repr(np.datetime64("NaT", "D"))


def test_query_repr_falls_back_to_the_symbols_passed() -> None:
    # A request that does not store its symbols shows them as the caller passed them.
    assert QueryCall("m.x.y", (("symbols", "AAPL"),)).text(Request()) == "m.x.y('AAPL')"
    assert QueryCall("m.x.y", (("symbols", ("B", "A")),)).text(Request()) == "m.x.y(['B', 'A'])"


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


def _missing(query: Query, verb: str) -> str:
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
        "stocks.trades has no fetch(); its methods are blink(), append_only(), ring(), last_by(), "
        "history(); use m.stocks.last_trade(...).fetch()"
    )
    assert _missing(m.stocks.bars("AAPL", dm.Timespan.DAY), "poll") == (
        "stocks.bars has no poll(); its methods are history(); use "
        "m.stocks.previous_day_bar(...).poll()"
    )
    # And back: the stream and the history of a one-row dataset are its kind's other datasets.
    assert _missing(m.stocks.last_quote("AAPL"), "last_by") == (
        "stocks.last_quote has no last_by(); its methods are fetch(), poll(); use "
        "m.stocks.quotes(...).last_by()"
    )
    # Intraday bars are read over a range: history() comes first, the previous day after it.
    assert _missing(m.stocks.bars("AAPL", dm.Timespan.MINUTE), "fetch") == (
        "stocks.bars has no fetch(); its methods are blink(), append_only(), ring(), last_by(), "
        "history(); use history(start, end) for these bars, or "
        "m.stocks.previous_day_bar(...).fetch() for each symbol's previous day"
    )
    assert _missing(m.futures.bars("ESZ5", dm.Timespan.HOUR), "poll") == (
        "futures.bars has no poll(); its methods are history(); use history(start, end) for these "
        "bars"
    )
    assert _missing(m.indices.previous_day_bar("I:SPX"), "history") == (
        "indices.previous_day_bar has no history(); its methods are fetch(), poll(); use "
        "m.indices.bars(...).history()"
    )


def test_a_missing_verb_without_a_sibling_names_what_else_serves(
    massive_session: dm.MassiveSession,
) -> None:
    m = massive_session
    # No last-quote endpoint for options: the snapshot is each contract's current state.
    assert _missing(m.options.quotes("O:AAPL250117C00150000"), "fetch") == (
        "options.quotes has no fetch(); its methods are blink(), append_only(), ring(), "
        "last_by(), history(); for each symbol's current state use "
        "m.options.snapshots(...).fetch()"
    )
    # Massive's REST has no fair market value endpoint: the snapshot's Fmv column carries it.
    for asset_class, symbol in (
        ("stocks", "AAPL"),
        ("options", "O:AAPL250117C00150000"),
        ("forex", "C:EURUSD"),
        ("crypto", "X:BTCUSD"),
    ):
        assert _missing(getattr(m, asset_class).fmv(symbol), "fetch") == (
            f"{asset_class}.fmv has no fetch(); its methods are blink(), append_only(), ring(), "
            f"last_by(); for each symbol's current fair market value use "
            f"m.{asset_class}.snapshots(...).fetch(), its Fmv column"
        )
    assert _missing(m.stocks.dividends("AAPL"), "history") == (
        "stocks.dividends has no history(); its methods are fetch(), poll(); pass start= and end= "
        "to m.stocks.dividends(...) instead"
    )
    assert _missing(m.stocks.bars("AAPL", dm.Timespan.HOUR), "blink") == (
        "stocks.bars has no blink(); its methods are history(); only bars of dm.Timespan.SECOND "
        "and dm.Timespan.MINUTE with multiplier=1 stream"
    )
    # Nothing else serves it: the message names the verbs the query has.
    assert _missing(m.stocks.luld("AAPL"), "history") == (
        "stocks.luld has no history(); its methods are blink(), append_only(), ring(), last_by()"
    )
    assert _missing(make(m, HISTORY_SPEC), "fetch") == (
        "economy.yields has no fetch(); its methods are history()"
    )


def test_any_other_missing_attribute_raises_python_s_own_error(
    massive_session: dm.MassiveSession,
) -> None:
    q = trades(massive_session)
    with pytest.raises(AttributeError, match=r"^'StreamHistoryQuery' object has no attribute "):
        _ = q.fetchh  # type: ignore[attr-defined]  # the missing verb is the point
    assert not hasattr(q, "__setstate_ex__")
    assert callable(q.blink)  # a verb the query has never reaches the fallback


# --- fetch and poll --------------------------------------------------------------------------


def test_fetch_and_poll_delegate(
    massive_session: dm.MassiveSession, rest: Recorder, caplog: pytest.LogCaptureFixture
) -> None:
    q = feed(massive_session)
    q.fetch()
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive.typehints._queries"):
        q.poll(30)
        q.poll(30, mode=PollMode.AUTO)
        q.poll(timedelta(minutes=1), mode=PollMode.REPLACE)
    shown = repr(q)
    # Each poll() logs the mode it asked for and the mode it got.
    assert [m for m in caplog.messages if " poll(): " in m] == [
        f"{shown} poll(): every 30.000 s, mode AUTO -> APPEND",
        f"{shown} poll(): every 30.000 s, mode AUTO -> APPEND",
        f"{shown} poll(): every 60.000 s, mode REPLACE -> REPLACE",
    ]
    # AUTO, the default, is the dataset's own mode: APPEND for this feed.
    assert [(c[0], c[3]) for c in rest.calls] == [
        ("fetch", {}),
        ("poll", {"interval": timedelta(seconds=30), "mode": PollMode.APPEND, "query": shown}),
        ("poll", {"interval": timedelta(seconds=30), "mode": PollMode.APPEND, "query": shown}),
        ("poll", {"interval": timedelta(minutes=1), "mode": PollMode.REPLACE, "query": shown}),
    ]


@pytest.mark.parametrize("mode", [PollMode.AUTO, PollMode.REPLACE])
def test_a_feed_without_start_refuses_to_poll(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    mode: PollMode,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Every cycle would re-read Massive's whole archive; the message names the fix.
    message = (
        "m.economy.inflation().poll() needs a start or a limit: without either, every cycle "
        "re-reads the whole archive Massive keeps. Build the query with a start, such as "
        "m.economy.inflation(..., start=dh_today(to_j_time_zone('America/New_York'))), or "
        "with a limit and a newest-first order, then call poll(). A fixed start keeps widening "
        "the window on a poll that runs for days"
    )
    with (
        pytest.raises(ValueError, match=f"^{re.escape(message)}$"),
        caplog.at_level(logging.DEBUG, logger="deephaven_massive"),
    ):
        news(massive_session).poll(30, mode=mode)
    assert rest.calls == []
    # The error is the user's report; the log has the decision and the refusal, at DEBUG.
    assert [r.levelno for r in caplog.records] == [logging.DEBUG, logging.DEBUG]
    assert caplog.messages[1] == f"m.economy.inflation().poll() raised ValueError: {message}"
    news(massive_session).fetch()  # fetch() reads the whole range, as asked
    assert [c[0] for c in rest.calls] == ["fetch"]


def test_a_feed_with_a_limit_polls_its_latest_rows(
    massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    # The "latest headlines" poll: each cycle reads at most `limit` rows, never the archive.
    news(massive_session, limit=50).poll(60)
    assert [c[0] for c in rest.calls] == ["poll"]


@pytest.mark.parametrize("interval", [0, -1, float("inf"), float("nan"), 1e300, 10**400])
def test_poll_interval_values(
    massive_session: dm.MassiveSession, rest: Recorder, interval: object
) -> None:
    with pytest.raises(ValueError, match=r"interval must be (positive|a positive, finite)"):
        feed(massive_session).poll(cast("float", interval))
    assert rest.calls == []


@pytest.mark.parametrize(
    "interval",
    [0.999, timedelta(milliseconds=999), 1e-9, timedelta(0), timedelta(seconds=-5)],
)
def test_poll_refuses_an_interval_under_one_second(
    massive_session: dm.MassiveSession, rest: Recorder, interval: float | timedelta
) -> None:
    # A typo such as poll(0.01) for poll(10) would send requests back to back on a plan
    # Massive does not pace; the error names the minimum and the value given.
    message = (
        f"interval must be at least 1 second, got {interval!r}: each poll() cycle sends a "
        "request; use a stream for faster updates"
    )
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        feed(massive_session).poll(interval)
    assert rest.calls == []


@pytest.mark.parametrize("interval", [1, 1.0, timedelta(seconds=1)])
def test_poll_accepts_an_interval_of_one_second(
    massive_session: dm.MassiveSession, rest: Recorder, interval: float | timedelta
) -> None:
    feed(massive_session).poll(interval)
    assert [(c[0], c[3]["interval"]) for c in rest.calls] == [("poll", MINIMUM_POLL_INTERVAL)]


@pytest.mark.parametrize("interval", ["30", True, None])
def test_poll_interval_types(
    massive_session: dm.MassiveSession, rest: Recorder, interval: object
) -> None:
    with pytest.raises(TypeError, match="interval must be a number"):
        feed(massive_session).poll(cast("float", interval))


def test_poll_mode_is_an_enum(massive_session: dm.MassiveSession, rest: Recorder) -> None:
    with pytest.raises(
        TypeError, match=r"^mode must be dm\.PollMode, e\.g\. dm\.PollMode\.UPSERT$"
    ):
        feed(massive_session).poll(30, mode=cast("PollMode", "upsert"))


# `call 2 done: 0 rows in 0.001 s`, with the elapsed time made predictable.
_ELAPSED = re.compile(r"\d+\.\d{3} s")


def _calls(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [_ELAPSED.sub("<s> s", m) for m in caplog.messages if m.startswith("call ")]


def test_each_static_verb_is_one_logged_call(
    massive_session: dm.MassiveSession, rest: Recorder, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive.typehints._queries"):
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
        caplog.at_level(logging.DEBUG, logger="deephaven_massive.typehints._queries"),
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
        caplog.at_level(logging.DEBUG, logger="deephaven_massive.typehints._queries"),
        pytest.raises(dm.EntitlementError),
    ):
        news(massive_session).fetch()
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
        news(massive_session).fetch()
    # A broad except in the caller must never hide a defect, so it is an ERROR that says whose.
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert [_ELAPSED.sub("<s> s", r.getMessage()) for r in errors] == [
        f"call 1 failed after <s> s: stocks.trades: a row broke{suffix}"
    ]


def test_a_history_call_through_flat_files_passes_its_id(
    *, s3_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    flat = Recorder(s3_session._core.catalog)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
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
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
    values = make(s3_session, VALUES_SPEC, symbols=["I:SPX"])
    assert isinstance(values, HistoryQuery)
    with caplog.at_level(logging.INFO, logger="deephaven_massive.typehints._queries"):
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
    with pytest.raises(TypeError, match=r"reread must be dm\.Reread"):
        q.history("2025-03-03", "2025-03-03", reread=cast("dm.Reread", "never"))
    with pytest.raises(ValueError, match="needs a timezone: pass an aware datetime or a date"):
        q.history("2025-03-03T09:30", "2025-03-04")
    with pytest.raises(ValueError, match="must not be after end"):
        q.history("2025-03-04", "2025-03-03")
    with pytest.raises(ValueError, match="start and end must both be dates or both be datetimes"):
        q.history("2025-03-03", datetime(2025, 3, 4, tzinfo=UTC))
    assert flat.calls == []


def test_history_reads_a_static_symbol_table_once_for_every_source(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    flat: Recorder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A refreshing table is followed instead (tests/tables/test_followed_read.py).
    watchlist = new_table([string_col("Symbol", ["MSFT", "AAPL"])])
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


def strict(session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch) -> None:
    # The session's rule for symbol-table rows becomes RAISE.
    config = dataclasses.replace(session._core.config, noncanonical_symbols=dm.OnError.RAISE)
    monkeypatch.setattr(session._core, "config", config)


def test_history_refuses_a_symbol_table_row_that_is_not_canonical_under_raise(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    flat: Recorder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strict(massive_session, monkeypatch)
    q = trades(massive_session, new_table([string_col("Symbol", ["AAPL", "msft", None])]))
    for source in HistorySource:
        with pytest.raises(
            ValueError,
            match=r"^stocks\.trades: a symbol table holds 'msft' .*dm\.canonicalize_symbols",
        ):
            q.history("2025-03-03", "2025-03-03", source=source)
    assert rest.calls == []
    assert flat.calls == []


def test_history_skips_a_symbol_table_row_that_is_not_canonical(
    *, massive_session: dm.MassiveSession, rest: Recorder, flat: Recorder
) -> None:
    q = trades(massive_session, new_table([string_col("Symbol", ["AAPL", "msft", None])]))
    q.history("2025-03-03", "2025-03-03", source=HistorySource.REST)
    assert rest.calls[0][2].symbols == ("AAPL",)
    assert flat.calls == []
    only_bad = trades(massive_session, new_table([string_col("Symbol", ["msft"])]))
    with pytest.raises(
        ValueError,
        match=r"^m\.stocks\.trades\(\): no row of the symbol table is a ticker this dataset",
    ):
        only_bad.history("2025-03-03", "2025-03-03", source=HistorySource.REST)


def test_history_rejects_a_symbol_table_without_symbols(
    *, massive_session: dm.MassiveSession, rest: Recorder, flat: Recorder
) -> None:
    for content in ([], [None]):
        q = trades(massive_session, new_table([string_col("Symbol", content)]))
        for source in (HistorySource.AUTO, HistorySource.REST):  # the session has no S3 keys
            with pytest.raises(
                ValueError, match=r"^m\.stocks\.trades\(\): the symbol table holds no symbols$"
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
        # None is no mode: the default is "Symbol", written out.
        (None, (TypeError, "by must be a str or a sequence of str, not NoneType")),
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
        q.last_by(by=deque(["Symbol", "Exchange"])),  # any sequence, as the message says
    ]
    assert all(r.is_refreshing for r in results)
    schema = massive_session._core.catalog.dataset_schema(TRADES_SPEC)
    assert [c.name for c in results[0].columns] == list(schema.column_names)
    assert len(fake_streams.opened) == 1
    for result in results:
        massive_session.release(result)


def test_last_by_follows_a_reactive_table(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    dh_input_table: Callable[..., InputTable],
) -> None:
    watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
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
    assert massive_session._core.diagnostics.wait_until_shown(10.0)
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
            ValueError, match=r"^m\.stocks\.trades\(\): the symbol table holds no symbols$"
        ):
            call(3) if verb == "ring" else call()
    assert fake_streams.opened == []


@pytest.mark.parametrize("verb", ["blink", "append_only", "ring", "last_by"])
def test_a_static_symbol_table_row_that_is_not_canonical_opens_no_stream_under_raise(
    massive_session: dm.MassiveSession,
    *,
    fake_streams: FakeStreams,
    verb: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Refused, as the same text in a list always is: never canonicalized and never dropped.
    strict(massive_session, monkeypatch)
    q = trades(massive_session, new_table([string_col("Symbol", ["AAPL", "msft"])]))
    call = getattr(q, verb)
    with pytest.raises(
        ValueError,
        match=r"^stocks\.trades: a symbol table holds 'msft' .*dm\.canonicalize_symbols",
    ):
        call(3) if verb == "ring" else call()
    assert fake_streams.opened == []


@pytest.mark.parametrize("verb", ["blink", "append_only", "ring", "last_by"])
def test_a_static_symbol_table_row_that_is_not_canonical_is_skipped(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams, verb: str
) -> None:
    q = trades(massive_session, new_table([string_col("Symbol", ["AAPL", "msft"])]))
    call = getattr(q, verb)
    massive_session.release(call(3) if verb == "ring" else call())
    assert len(fake_streams.opened) == 1
    only_bad = trades(massive_session, new_table([string_col("Symbol", ["msft"])]))
    call = getattr(only_bad, verb)
    with pytest.raises(ValueError, match=r"^m\.stocks\.trades\(\): no row of the symbol table is"):
        call(3) if verb == "ring" else call()
    assert len(fake_streams.opened) == 1


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
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive.typehints._queries"):
        verbs: list[Callable[[], Table]] = [
            one_row.fetch,
            lambda: q.history("2025-03-03", source=HistorySource.REST),
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


# --- Column descriptions on every result -----------------------------------------------------


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


FOREX_SPEC = DatasetSpec(
    name="forex.quotes",
    summary="A dataset for the tests.",
    asset_class=AssetClass.FOREX,
    channels={NotBars.ANY: "C"},
    keys={(W, "forex/C"): SourceKeys(KeySpec("Ticker"), KeySpec("ParticipantTimestamp"))},
)


def test_last_by_matches_the_rows_of_a_reactive_table(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    rows_when: RowsWhen,
    *,
    dh_input_table: Callable[..., InputTable],
) -> None:
    watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["C:EURUSD", "C:GBPUSD"])]))
    q = make(massive_session, FOREX_SPEC, symbols=watchlist)
    assert isinstance(q, SymbolStreamQuery)
    current = q.last_by()
    schema = massive_session._core.catalog.dataset_schema(FOREX_SPEC)
    # Received now: a row received before its symbol joined the watchlist is hidden.
    received = datetime.now(UTC)
    row = schema.to_arrow([{"Symbol": "C:EURUSD", "AskPrice": 1.08, "TimestampInsert": received}])
    fake_streams.publishers[1].add(dh_arrow.to_table(row))
    shown = rows_when(current, lambda rows: len(rows) == 1)
    assert [r["Symbol"] for r in shown] == ["C:EURUSD"]
    # Removing the symbol from the watchlist evicts its row.
    watchlist.delete(new_table([string_col("Symbol", ["C:EURUSD"])]))
    rows_when(current, lambda rows: not rows)
    massive_session.release(current)


def test_a_live_verb_refuses_a_symbol_table_row_that_is_not_canonical_under_raise(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams, monkeypatch: pytest.MonkeyPatch
) -> None:
    strict(massive_session, monkeypatch)
    watchlist = new_table([string_col("Symbol", ["C:EURUSD", "EUR/USD"])])
    q = make(massive_session, FOREX_SPEC, symbols=watchlist)
    assert isinstance(q, SymbolStreamQuery)
    for verb in (q.blink, q.append_only, q.last_by):
        with pytest.raises(
            ValueError, match=r"^forex\.\w+: a symbol table holds 'EUR/USD' .*use 'C:EURUSD'"
        ):
            verb()
    assert fake_streams.publishers == {}


def test_last_by_over_groups_of_symbols_filters_before_aggregating(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    rows_when: RowsWhen,
    *,
    dh_input_table: Callable[..., InputTable],
) -> None:
    watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
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


@pytest.mark.parametrize("unreadable", list(dm.OnError))
def test_an_empty_window_has_the_session_columns(unreadable: dm.OnError) -> None:
    # The empty result of a window no row fits in has the session's columns, with RawSymbol
    # exactly when the session keeps unreadable rows, as every other result of it does.
    config = dm.MassiveConfig(api_key="test-key", unreadable_symbols=unreadable)
    with dm.MassiveSession(config) as session:
        q = trades(session)
        empty = q._empty_rows()
        names = [c.name for c in empty.columns]
        assert ("RawSymbol" in names) is (unreadable is not dm.OnError.RAISE)
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
    # One query per query class the dataset's calls can make, each with a symbol list and with
    # a reactive symbol table (which changes the shape of `last_by()`): a bars dataset gets
    # them for every timespan it accepts (the timespan picks the class), any other dataset once.
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
    if isinstance(query, StreamQuery):
        yield "blink()", query.blink()
        yield "append_only()", query.append_only()
        yield "ring(5)", query.ring(5)
    if isinstance(query, SymbolStreamQuery):
        yield "last_by()", query.last_by()
        yield f"last_by({columns[-1]!r})", query.last_by(columns[-1])
    if isinstance(query, FetchQuery):
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


def test_a_history_table_names_its_source(
    *, s3_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    rest, flat = Recorder(s3_session._core.catalog), Recorder(s3_session._core.catalog)
    monkeypatch.setattr(s3_session._core, "rest_transport", lambda: rest)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
    days = ("2025-03-03", "2025-03-04")
    ticks = trades(s3_session).history(*days)
    forced = trades(s3_session).history(*days, source=HistorySource.REST)
    values = make(s3_session, VALUES_SPEC, symbols=["I:SPX"])
    assert isinstance(values, HistoryQuery)
    files = values.history(*days, source=HistorySource.FLAT_FILES)
    summary = "A dataset for the tests."
    assert [t.attributes()["TableDescription"] for t in (ticks, forced, files)] == [
        (
            f"stocks.trades: {summary} Read from REST: AUTO chose it (flat files cannot serve "
            "it: stocks.trades has no flat files)."
        ),
        f"stocks.trades: {summary} Read from REST: requested with source=dm.HistorySource.REST.",
        (
            f"indices.values: {summary} Read from flat files: requested with "
            "source=dm.HistorySource.FLAT_FILES."
        ),
    ]


def _symbols(count: int) -> list[str]:
    return [f"S{n:03d}" for n in range(count)]


def test_auto_warns_once_about_columns_the_other_source_fills(
    *,
    s3_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest, flat = Recorder(s3_session._core.catalog), Recorder(s3_session._core.catalog)
    monkeypatch.setattr(s3_session._core, "rest_transport", lambda: rest)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
    q = make(s3_session, TRADES_FILES_SPEC, symbols=_symbols(60))
    assert isinstance(q, HistoryQuery)
    instant = datetime(2025, 3, 3, 15, tzinfo=UTC)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive"):
        q.history(instant, instant)  # no row can lie in the range: nothing is read, no warning
        assert caplog.messages == []
        q.history("2025-03-03", "2025-03-03")
        q.history("2025-03-04", "2025-03-04")  # once per session
    warning = (
        "stocks.trades history: AUTO read flat files (60 symbols >= flat_file_symbol_threshold "
        "50); the read leaves DecimalSize null, which the other source, REST, fills: pass "
        "source=dm.HistorySource.REST to get them (logged once per session)"
    )
    assert caplog.messages == [warning]


def test_auto_reads_the_days_not_published_yet_from_rest(
    *,
    s3_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The flat files hold March 3 but not yet March 4: AUTO reads March 4 from REST and
    # returns the whole range; FLAT_FILES raises; so does AUTO where REST cannot serve.
    rest, flat = Recorder(s3_session._core.catalog), Recorder(s3_session._core.catalog)
    monkeypatch.setattr(s3_session._core, "rest_transport", lambda: rest)
    files = FlatFiles(flat, s3_session._core, (date(2025, 3, 4),))
    monkeypatch.setattr(s3_session._core, "flat_file_transport", lambda: files)
    many = make(s3_session, TRADES_FILES_SPEC, symbols=_symbols(60))
    assert isinstance(many, HistoryQuery)
    with caplog.at_level(logging.INFO, logger="deephaven_massive.typehints._queries"):
        many.history("2025-03-03", "2025-03-04")
    [(verb, _, request, extra)] = rest.calls
    assert (verb, request.start, request.end) == ("history", date(2025, 3, 4), date(2025, 3, 4))
    assert extra["window"].span == DaySpan(date(2025, 3, 4), date(2025, 3, 4))
    assert caplog.messages[-1] == (
        "stocks.trades history: AUTO read REST because the flat files are missing for these "
        "days: 1 [2025-03-04]"
    )
    # Holidays are inferred only where no other source can serve the read.
    assert files.inferred == [False]
    note = (
        "stocks.trades: the flat file for 2025-03-04 is not published yet. Massive usually "
        "publishes a day's flat files by about 11:00 ET the next day, but the time varies. "
    )
    with pytest.raises(dm.MassiveError) as forced:
        many.history("2025-03-03", "2025-03-04", source=HistorySource.FLAT_FILES)
    assert str(forced.value) == note + (
        "Retry later, or read REST with source=dm.HistorySource.REST"
    )
    values = make(s3_session, VALUES_SPEC, symbols=["I:SPX"])
    assert isinstance(values, HistoryQuery)
    with pytest.raises(dm.MassiveError) as unserved:
        values.history("2025-03-03", "2025-03-04")
    assert files.inferred == [False, True, True]
    assert str(unserved.value) == note.replace("stocks.trades", "indices.values") + (
        "Retry later; REST cannot serve this read: indices.values has no REST history"
    )
    assert len(rest.calls) == 1


def _futures(count: int) -> list[str]:
    months = "FGHJKMNQUVXZ"
    return [f"E{chr(65 + n // 12)}{months[n % 12]}5" for n in range(count)]


# --- wait=False ------------------------------------------------------------------------------


def _join_reads(session: dm.MassiveSession) -> None:
    # Waits for every background read of the session to end, then for its row to be written.
    for state in list(session._core.workers._workers.values()):
        cast("Any", state.fail).__self__.thread.join(TIMEOUT)


def _released(session: dm.MassiveSession, *tables: Table) -> None:
    for table in tables:
        session.release(table)


def test_wait_false_reads_in_the_background(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    stream_rows: Callable[[], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    q = news(massive_session)
    with caplog.at_level(logging.DEBUG):
        fetched = q.fetch(wait=False)
        history = trades(massive_session).history(
            "2025-03-03", "2025-03-04", source=HistorySource.REST, wait=False
        )
        _join_reads(massive_session)
    assert fetched.is_refreshing
    assert history.is_refreshing
    # The parameters were checked on the calling thread, in call order, then each read ran on
    # its own background thread with a token, so the reads may finish in either order.
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
        f"call 1 start: {q!r}.fetch(wait=False)",
        "call 2 published: 0 rows in <s> s",
        (
            f"call 2 start: {trades(massive_session)!r}.history('2025-03-03', '2025-03-04', "
            "source=<HistorySource.REST: 'rest'>, wait=False)"
        ),
    ]
    _released(massive_session, fetched, history)


@pytest.mark.jvm_held  # the formula hands this frame's locals to the JVM
def test_a_wait_false_result_takes_view_and_update_view(
    massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    fetched = news(massive_session).fetch(wait=False)
    history = trades(massive_session).history(
        "2025-03-03", "2025-03-04", source=HistorySource.REST, wait=False
    )
    _join_reads(massive_session)
    for result in (fetched, history):
        first = result.columns[0].name
        assert result.view([first]).column_names == [first]
        assert result.update_view(f"Again = {first}").column_names[-1] == "Again"
    _released(massive_session, fetched, history)


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

    def refuse(*_: object) -> None:
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
    _join_reads(s3_session)
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
    _released(s3_session, files, forced, landed)


def test_a_refused_flat_file_read_is_read_from_rest_under_auto(
    *,
    s3_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest, flat = Recorder(s3_session._core.catalog), Recorder(s3_session._core.catalog)
    monkeypatch.setattr(s3_session._core, "rest_transport", lambda: rest)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )

    def refused(*_: object, **__: object) -> Table:
        raise dm.EntitlementError("the plan does not include these flat files", status=403)

    monkeypatch.setattr(flat, "history", refused)
    q = make(s3_session, TRADES_FILES_SPEC, symbols=_symbols(60))
    assert isinstance(q, HistoryQuery)
    # FLAT_FILES raises, naming REST.
    with pytest.raises(dm.EntitlementError) as info:
        q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert str(info.value) == (
        "the plan does not include these flat files; REST serves this read too: pass "
        "source=dm.HistorySource.REST to history() (it can fill other columns; columns() "
        "shows which)"
    )
    assert info.value.status == 403
    assert rest.calls == []
    # AUTO chose the flat files (60 symbols), and serves the read from REST instead, warning.
    with caplog.at_level(logging.WARNING, logger="deephaven_massive"):
        q.history("2025-03-03", "2025-03-03")
    assert [c[0] for c in rest.calls] == ["history"]
    assert caplog.messages[-1] == (
        "stocks.trades history: AUTO read REST because S3 refused the flat files (the plan does "
        "not include these flat files)"
    )

    def rejected(*_: object, **__: object) -> Table:
        raise dm.AuthenticationError("S3 rejected the flat-file credentials", status=403)

    # A rejection of the S3 credentials is served from REST the same way, and raised as it
    # came under FLAT_FILES.
    monkeypatch.setattr(flat, "history", rejected)
    with pytest.raises(dm.AuthenticationError, match=r"^S3 rejected the flat-file credentials$"):
        q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    described = q.history("2025-03-03", "2025-03-03")
    assert [c[0] for c in rest.calls] == ["history", "history"]
    assert described.attributes()["TableDescription"].endswith(
        "Read from REST: AUTO chose flat files, which S3 refused."
    )
    # Where REST cannot serve the read, the refusal is raised as it came.
    monkeypatch.setattr(flat, "history", refused)
    values = make(s3_session, VALUES_SPEC, symbols=["I:SPX"])
    assert isinstance(values, HistoryQuery)
    with pytest.raises(dm.EntitlementError, match=r"^the plan does not include these flat files$"):
        values.history("2025-03-03", "2025-03-03")


FF = Transport.FLAT_FILES

SESSION_FILES = "us_futures_{venue}/session_aggs_v1"

INDEX_FILES = "us_indices/minute_aggs_v1"

STOCK_BARS = DatasetSpec(
    name="stocks.bars",
    summary="A dataset for the tests.",
    is_bars=True,
    asset_class=AssetClass.STOCKS,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    history_path="/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}",
)

FUTURES_BARS = DatasetSpec(
    name="futures.bars",
    summary="A dataset for the tests.",
    is_bars=True,
    asset_class=AssetClass.FUTURES,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    history_path="/futures/v1/aggs/{ticker}",
    flat_file_prefixes={Timespan.SESSION: SESSION_FILES},
    keys={
        (FF, SESSION_FILES): SourceKeys(
            KeySpec("Ticker"), KeySpec("WindowStart"), date=KeySpec("SessionEndDate")
        )
    },
)

INDEX_BARS = DatasetSpec(
    name="indices.bars",
    summary="A dataset for the tests.",
    is_bars=True,
    asset_class=AssetClass.INDICES,
    flat_file_prefixes={Timespan.MINUTE: INDEX_FILES},
    keys={(FF, INDEX_FILES): SourceKeys(KeySpec("Ticker"), KeySpec("WindowStart"))},
)


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ((STOCK_BARS, Timespan.SECOND, 1), StreamHistoryQuery),
        ((STOCK_BARS, Timespan.MINUTE, 1), StreamHistoryQuery),
        ((STOCK_BARS, Timespan.DAY, 1), HistoryQuery),
        ((STOCK_BARS, Timespan.MINUTE, 5), HistoryQuery),
        ((STOCK_BARS, Timespan.DAY, 2), HistoryQuery),
        ((STOCK_BARS, Timespan.HOUR, 1), HistoryQuery),
        ((FUTURES_BARS, Timespan.MINUTE, 1), StreamHistoryQuery),
        ((FUTURES_BARS, Timespan.SESSION, 1), HistoryQuery),
        ((INDEX_BARS, Timespan.DAY, 1), HistoryQuery),
    ],
)
def test_bar_query_class_selection(case: tuple[DatasetSpec, Timespan, int], expected: type) -> None:
    assert bar_query_class(*case) is expected


def test_bar_timespan_rules() -> None:
    with pytest.raises(ValueError, match="futures bars have no DAY timespan"):
        bar_query_class(FUTURES_BARS, Timespan.DAY, 1)
    with pytest.raises(ValueError, match="SESSION exists only for futures"):
        bar_query_class(STOCK_BARS, Timespan.SESSION, 1)
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan, e\.g\. dm\.Timespan\.DAY"):
        bar_query_class(STOCK_BARS, "day", 1)
    with pytest.raises(ValueError, match="multiplier must be positive"):
        bar_query_class(STOCK_BARS, Timespan.DAY, 0)


@pytest.mark.parametrize("multiplier", [True, 1.0])
def test_bar_multiplier_must_be_an_int(multiplier: object) -> None:
    with pytest.raises(TypeError, match="multiplier must be an int"):
        bar_query_class(STOCK_BARS, Timespan.DAY, multiplier)


_FETCH = {"fetch_path": "/v2/last/trade/{stocksTicker}"}

_HISTORY = {"history_path": "/v3/trades/{stockTicker}"}

_CHANNELS = {"channels": {NotBars.ANY: "T"}}


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({**_FETCH}, FetchQuery),
        ({**_CHANNELS}, SymbolStreamQuery),
        ({**_HISTORY}, HistoryQuery),
        ({"grouped_daily_path": "/v2/aggs/grouped/locale/us/market/stocks/{date}"}, HistoryQuery),
        ({**_CHANNELS, **_HISTORY}, StreamHistoryQuery),
    ],
)
def test_the_query_class_follows_the_transports(
    changes: dict[str, Any], expected: type[Query]
) -> None:
    spec = DatasetSpec(
        name="stocks.trades",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        **changes,
    )
    assert query_class(spec) is expected


def test_flat_files_are_a_history_source() -> None:
    assert query_class(INDEX_BARS) is HistoryQuery
    values = DatasetSpec(
        name="indices.values",
        summary="A dataset for the tests.",
        asset_class=AssetClass.INDICES,
        channels={NotBars.ANY: "V"},
        flat_file_prefixes={NotBars.ANY: "us_indices/values_v1"},
        keys={(FF, "us_indices/values_v1"): SourceKeys(KeySpec("Ticker"), KeySpec("Timestamp"))},
    )
    assert query_class(values) is StreamHistoryQuery


def test_a_bars_dataset_has_the_class_of_its_general_overload() -> None:
    # Every bars spec, whatever its transports; each call picks its own class.
    assert {query_class(STOCK_BARS), query_class(FUTURES_BARS)} == {HistoryQuery}


# --- history() of a refreshing symbol table ---------------------------------------------------


@pytest.fixture
def live(dh_input_table: Callable[..., InputTable]) -> Callable[..., Table]:
    # Makes a refreshing, keyed watchlist holding the symbols.
    def make(*symbols: str) -> Table:
        table = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
        if symbols:
            table.add(new_table([string_col("Symbol", list(symbols))]))
        return table

    return make


def follows(m: dm.MassiveSession) -> None:
    # Waits until no followed read of the session is left: its coordinator has ended.
    pause = threading.Event()
    for _ in range(int(TIMEOUT / 0.01)):
        if m._core.follows.following == 0:
            return
        pause.wait(0.01)
    raise AssertionError("a followed read kept running")


def _until(condition: Callable[[], bool]) -> None:
    pause = threading.Event()
    for _ in range(int(TIMEOUT / 0.01)):
        if condition():
            return
        pause.wait(0.01)
    assert condition()


def test_history_follows_a_refreshing_table_from_rest_under_auto(
    live: Callable[..., Table], *, s3_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    rest, flat = Recorder(s3_session._core.catalog), Recorder(s3_session._core.catalog)
    monkeypatch.setattr(s3_session._core, "rest_transport", lambda: rest)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
    many = [f"S{i:02d}" for i in range(60)]  # past the flat-file threshold of a static call
    q = make(s3_session, TRADES_FILES_SPEC, symbols=live(*many))
    assert isinstance(q, HistoryQuery)
    result = q.history("2025-03-03", "2025-03-03")
    assert result.is_refreshing
    assert flat.by_symbol == []
    assert [names for names, _, _ in rest.by_symbol] == [tuple(many)]  # the call's own read
    assert result.attributes()["TableDescription"].endswith(
        "Follows its symbol table over 2025-03-03 through 2025-03-03. Read from REST: AUTO "
        "reads a followed symbol table from REST, which can serve it."
    )
    s3_session.release(result)
    few = make(s3_session, TRADES_FILES_SPEC, symbols=live("AAPL", "MSFT", "NVDA"))
    assert isinstance(few, HistoryQuery)
    s3_session.release(few.history("2025-03-03", "2025-03-03"))
    assert [names for names, _, _ in rest.by_symbol][1] == ("AAPL", "MSFT", "NVDA")
    assert flat.by_symbol == []
    follows(s3_session)


def test_history_follows_from_flat_files_when_rest_cannot_serve(
    live: Callable[..., Table], *, s3_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    flat = Recorder(s3_session._core.catalog)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
    q = make(s3_session, VALUES_SPEC, symbols=live("I:SPX"))
    assert isinstance(q, HistoryQuery)
    result = q.history("2025-03-03", "2025-03-03", wait=False)
    _until(lambda: [names for names, _, _ in flat.by_symbol] == [("I:SPX",)])
    assert result.attributes()["TableDescription"].endswith(
        "Read from flat files: AUTO reads a followed symbol table from flat files, since "
        "indices.values has no REST history."
    )
    s3_session.release(result)
    follows(s3_session)


def test_history_forced_to_flat_files_reads_the_waiting_symbols_in_one_scan(
    live: Callable[..., Table], *, s3_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    flat = Recorder(s3_session._core.catalog)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
    q = make(s3_session, TRADES_FILES_SPEC, symbols=live("AAPL", "MSFT"))
    assert isinstance(q, HistoryQuery)
    result = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    assert [names for names, _, _ in flat.by_symbol] == [("AAPL", "MSFT")]
    assert (
        "Read from flat files: requested with source=dm.HistorySource.FLAT_FILES."
        in (result.attributes()["TableDescription"])
    )
    s3_session.release(result)
    follows(s3_session)


def test_history_forced_to_rest_follows_from_rest(
    live: Callable[..., Table], *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    result = trades(massive_session, live("AAPL")).history(
        "2025-03-03", "2025-03-03", source=HistorySource.REST
    )
    assert (
        "Read from REST: requested with source=dm.HistorySource.REST."
        in (result.attributes()["TableDescription"])
    )
    assert rest.checks == [("history", TRADES_SPEC, rest.checks[0][2])]  # checked at the call
    massive_session.release(result)
    follows(massive_session)


def test_an_added_symbol_is_read_over_the_call_s_window(
    live: Callable[..., Table], *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    watchlist = live("AAPL")
    result = trades(massive_session, watchlist).history("2025-03-03")  # through the call instant
    cast("Any", watchlist).add(new_table([string_col("Symbol", ["MSFT"])]))
    _until(lambda: len(rest.by_symbol) == 2)
    (first, window, caller), (added, later, reader) = rest.by_symbol
    assert (first, added) == (("AAPL",), ("MSFT",))
    assert later is window  # the same window, fixed at the call: the same end and cutoff
    assert caller == threading.current_thread().name
    assert "follow-read" in reader
    end = window.now.strftime("%Y-%m-%dT%H:%M:%SZ")
    assert f"over 2025-03-03 through {end}." in result.attributes()["TableDescription"]
    massive_session.release(result)
    follows(massive_session)


def test_an_instant_range_prints_its_instants(
    live: Callable[..., Table], *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    start = datetime(2025, 3, 3, 14, 30, tzinfo=UTC)
    end = datetime(2025, 3, 3, 15, tzinfo=UTC)
    result = trades(massive_session, live("AAPL")).history(start, end, wait=False)
    assert (
        "over 2025-03-03T14:30:00Z through 2025-03-03T15:00:00Z."
        in (result.attributes()["TableDescription"])
    )
    massive_session.release(result)
    follows(massive_session)


def test_wait_false_returns_at_once_and_reads_in_the_background(
    live: Callable[..., Table], *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    result = trades(massive_session, live("AAPL")).history("2025-03-03", "2025-03-03", wait=False)
    assert result.is_refreshing
    _until(lambda: len(rest.by_symbol) == 1)
    assert "follow-read" in rest.by_symbol[0][2]
    massive_session.release(result)
    follows(massive_session)


def test_an_empty_refreshing_table_returns_an_empty_following_result(
    live: Callable[..., Table], *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    result = trades(massive_session, live()).history("2025-03-03", "2025-03-03")
    assert result.is_refreshing
    assert result.size == 0
    assert rest.by_symbol == []
    assert [verb for verb, _, _ in rest.checks] == ["history"]  # checked with no symbol too
    massive_session.release(result)
    follows(massive_session)


def test_wait_true_raises_when_every_call_time_symbol_fails(
    live: Callable[..., Table], *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    refusal = dm.EntitlementError("HTTP 403: not in the plan")
    rest.outcomes = {
        "AAPL": SymbolFailure(refusal, FailureKind.PERMANENT),
        "MSFT": SymbolFailure(dm.EntitlementError("HTTP 403: other"), FailureKind.PERMANENT),
    }
    with pytest.raises(dm.EntitlementError, match="not in the plan"):
        trades(massive_session, live("AAPL", "MSFT")).history("2025-03-03", "2025-03-03")
    assert massive_session._core.workers._workers == {}
    rest.outcomes = {}
    rest.raises = dm.AuthenticationError("HTTP 401: wrong key")  # the read fails as a unit
    with pytest.raises(dm.AuthenticationError, match="wrong key"):
        trades(massive_session, live("AAPL")).history("2025-03-03", "2025-03-03")
    follows(massive_session)


def test_wait_true_warns_once_when_some_call_time_symbols_fail(
    live: Callable[..., Table],
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest.outcomes = {
        "MSFT": SymbolFailure(
            dm.EntitlementError("HTTP 403: not in the plan"), FailureKind.PERMANENT
        )
    }
    with caplog.at_level(logging.WARNING, logger="deephaven_massive.typehints._queries"):
        result = trades(massive_session, live("AAPL", "MSFT")).history("2025-03-03", "2025-03-03")
    assert caplog.messages == [
        (
            "stocks.trades history: 1 of the 2 symbols of the call failed and are left out of the "
            "result for now; m.symbol_reads() shows each one (retrying, or failed): "
            "MSFT: HTTP 403: not in the plan"
        )
    ]
    massive_session.release(result)
    follows(massive_session)


def test_reread_is_ignored_for_a_static_symbol_table(
    *, massive_session: dm.MassiveSession, rest: Recorder, caplog: pytest.LogCaptureFixture
) -> None:
    static = new_table([string_col("Symbol", ["AAPL"])])
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive.typehints._queries"):
        result = trades(massive_session, static).history(
            "2025-03-03", "2025-03-03", reread=dm.Reread.NEVER
        )
    assert not result.is_refreshing
    assert rest.by_symbol == []
    assert any(
        "history('2025-03-03', '2025-03-03', source=<HistorySource.AUTO: 'auto'>, "
        "reread=<Reread.NEVER: 'never'>)" in message
        for message in caplog.messages
    )


def test_a_static_call_logs_its_start_as_before(
    *, massive_session: dm.MassiveSession, rest: Recorder, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive.typehints._queries"):
        trades(massive_session).history("2025-03-03", "2025-03-03")
    starts = [m for m in caplog.messages if " start: " in m]
    assert len(starts) == 1
    assert starts[0].endswith(
        ".history('2025-03-03', '2025-03-03', source=<HistorySource.AUTO: 'auto'>)"
    )


def test_a_followed_table_refuses_a_bad_row_or_a_failed_table_at_the_call(
    live: Callable[..., Table],
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    only_bad = live("msft")
    with pytest.raises(ValueError, match="no row of the symbol table is a ticker"):
        trades(massive_session, only_bad).history("2025-03-03", "2025-03-03")
    strict(massive_session, monkeypatch)
    with pytest.raises(ValueError, match="a symbol table holds 'msft'"):
        trades(massive_session, live("AAPL", "msft")).history("2025-03-03", "2025-03-03")
    assert rest.by_symbol == []


def test_a_followed_history_reads_each_symbol_from_rest_with_its_own_outcome(
    live: Callable[..., Table],
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    drain: Callable[[], None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Through the real REST transport: Massive has no data for AAPL (404), the plan lacks MSFT
    # (403), and NVDA has one trade.
    nanos = int(datetime(2025, 3, 3, 15, tzinfo=UTC).timestamp() * 1e9)
    fake_rest.reply("/v3/trades/AAPL", Reply(envelope(status="NOT_FOUND"), status=404))
    fake_rest.reply(
        "/v3/trades/MSFT",
        Reply(envelope(status="NOT_AUTHORIZED", message="not in your plan"), status=403),
    )
    fake_rest.reply(
        "/v3/trades/NVDA",
        Reply(envelope([{"sip_timestamp": nanos, "price": 1.0, "size": 10}])),
    )
    with caplog.at_level(logging.WARNING):
        result = rest_session.stocks.trades(live("AAPL", "MSFT", "NVDA")).history(
            "2025-03-03", "2025-03-03", wait=False
        )
        _until(lambda: [r["Symbol"] for r in dh_to_pylist(result)] == ["NVDA"])

        def states() -> dict[str, tuple[str, int, str | None]]:
            drain()
            return {
                r["Symbol"]: (r["State"], r["Rows"], r["Error"])
                for r in dh_to_pylist(rest_session.symbol_reads())
            }

        _until(
            lambda: (
                {name: shown[0] for name, shown in states().items()}
                == {"AAPL": "DONE", "MSFT": "FAILED", "NVDA": "DONE"}
            )
        )
    found = states()
    assert found["AAPL"] == ("DONE", 0, None)
    assert found["NVDA"] == ("DONE", 1, None)
    assert found["MSFT"][:2] == ("FAILED", 0)
    assert "403" in (found["MSFT"][2] or "")
    assert any("AAPL" in m for m in caplog.messages)  # the 404's WARNING is in the log only
    rest_session.release(result)
    follows(rest_session)


# --- fetch() of a refreshing symbol table -----------------------------------------------------


def rows_of(m: dm.MassiveSession, spec: DatasetSpec, count: int) -> SymbolRows:
    # A symbol's static rows, as a read returns them: `count` rows of nulls.
    schema = m._core.dataset_schema(spec)
    with entered(m._core.exec_ctx):
        return SymbolRows(dh_arrow.to_table(schema.to_arrow([{}] * count)))


def symbol_reads(
    m: dm.MassiveSession, drain: Callable[[], None], to_list: Callable[[Table], list[Any]]
) -> dict[str, dict[str, Any]]:
    drain()
    return {row["Symbol"]: row for row in to_list(m.symbol_reads())}


def test_a_static_fetch_is_unchanged_and_passes_the_unlisted_setting(
    *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    static = new_table([string_col("Symbol", ["AAPL"])])
    q = make(massive_session, LAST_TRADE_SPEC, symbols=static)
    assert isinstance(q, FetchQuery)
    assert not q.fetch().is_refreshing
    assert not last_trade(massive_session).fetch().is_refreshing
    # A symbol table's crypto pairs follow the session's setting; symbols in code always raise.
    assert rest.unlisted == [OnError.LOG, OnError.RAISE]
    assert rest.fetched == []


def test_fetch_follows_a_refreshing_table_and_reads_a_returning_symbol_again(
    *,
    live: Callable[..., Table],
    massive_session: dm.MassiveSession,
    rest: Recorder,
    drain: Callable[[], None],
    dh_to_pylist: Callable[[Table], list[Any]],
) -> None:
    rest.outcomes = {"AAPL": rows_of(massive_session, LAST_TRADE_SPEC, 1)}
    wl = live("AAPL")
    q = make(massive_session, LAST_TRADE_SPEC, symbols=wl)
    assert isinstance(q, FetchQuery)
    result = q.fetch()
    assert result.is_refreshing
    assert result.size == 1  # the call-time read is in the result when the call returns
    assert [(names, thread) for names, _, thread in rest.fetched] == [
        (("AAPL",), threading.current_thread().name)
    ]
    assert result.attributes()["TableDescription"].endswith(
        "Follows its symbol table: each symbol's rows are Massive's answer as of its read."
    )
    rest.outcomes["MSFT"] = rows_of(massive_session, LAST_TRADE_SPEC, 2)
    cast("Any", wl).add(new_table([string_col("Symbol", ["MSFT"])]))
    _until(lambda: len(dh_to_pylist(result)) == 3)
    assert "follow-read" in rest.fetched[1][2]
    _until(lambda: symbol_reads(massive_session, drain, dh_to_pylist)["MSFT"]["State"] == "DONE")
    reads = symbol_reads(massive_session, drain, dh_to_pylist)
    # Each symbol's rows are Massive's answer as of its own read.
    assert reads["MSFT"]["ReadAt"] > reads["AAPL"]["ReadAt"]
    cast("Any", wl).delete(new_table([string_col("Symbol", ["MSFT"])]))
    _until(lambda: len(dh_to_pylist(result)) == 1)
    cast("Any", wl).add(new_table([string_col("Symbol", ["MSFT"])]))
    _until(lambda: len(rest.fetched) == 3)  # read again, with no reread option
    _until(lambda: len(dh_to_pylist(result)) == 3)
    massive_session.release(result)
    follows(massive_session)


def test_a_late_outcome_of_a_re_added_symbol_is_dropped(
    *,
    live: Callable[..., Table],
    massive_session: dm.MassiveSession,
    rest: Recorder,
    dh_to_pylist: Callable[[Table], list[Any]],
) -> None:
    # MSFT's first read is held while its row is deleted and added again; that read's outcome
    # (5 rows) lands after the re-add was queued, so only the second read's 1 row is shown.
    gate = threading.Event()
    rest.gates["MSFT"] = [gate]
    rest.turns["MSFT"] = [
        rows_of(massive_session, LAST_TRADE_SPEC, 5),
        rows_of(massive_session, LAST_TRADE_SPEC, 1),
    ]
    wl = live()
    q = make(massive_session, LAST_TRADE_SPEC, symbols=wl)
    assert isinstance(q, FetchQuery)
    result = q.fetch()
    cast("Any", wl).add(new_table([string_col("Symbol", ["MSFT"])]))
    _until(lambda: len(rest.fetched) == 1)
    cast("Any", wl).delete(new_table([string_col("Symbol", ["MSFT"])]))
    cast("Any", wl).add(new_table([string_col("Symbol", ["MSFT"])]))
    gate.set()
    _until(lambda: len(rest.fetched) == 2)
    _until(lambda: len(dh_to_pylist(result)) == 1)
    massive_session.release(result)
    follows(massive_session)


def test_fetch_wait_false_returns_at_once_and_reads_in_the_background(
    *, live: Callable[..., Table], massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    q = make(massive_session, LAST_TRADE_SPEC, symbols=live("AAPL"))
    assert isinstance(q, FetchQuery)
    result = q.fetch(wait=False)
    assert result.is_refreshing
    _until(lambda: len(rest.fetched) == 1)
    assert "follow-read" in rest.fetched[0][2]
    assert rest.checks == [("fetch", LAST_TRADE_SPEC, rest.checks[0][2])]  # checked at the call
    massive_session.release(result)
    follows(massive_session)


def test_a_followed_fetch_raises_only_when_every_call_time_symbol_fails(
    *,
    live: Callable[..., Table],
    massive_session: dm.MassiveSession,
    rest: Recorder,
    caplog: pytest.LogCaptureFixture,
) -> None:
    refusal = dm.EntitlementError("HTTP 403: not in the plan")
    rest.outcomes = {"AAPL": SymbolFailure(refusal, FailureKind.PERMANENT)}
    q = make(massive_session, LAST_TRADE_SPEC, symbols=live("AAPL"))
    assert isinstance(q, FetchQuery)
    with pytest.raises(dm.EntitlementError, match="not in the plan"):
        q.fetch()
    both = make(massive_session, LAST_TRADE_SPEC, symbols=live("AAPL", "MSFT"))
    assert isinstance(both, FetchQuery)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive.typehints._queries"):
        result = both.fetch()
    assert caplog.messages == [
        (
            "stocks.last_trade fetch: 1 of the 2 symbols of the call failed and are left out "
            "of the result for now; m.symbol_reads() shows each one (retrying, or failed): "
            "AAPL: HTTP 403: not in the plan"
        )
    ]
    rest.outcomes = {}  # the raised error's traceback holds the session
    massive_session.release(result)
    follows(massive_session)


@pytest.mark.parametrize("setting", [OnError.LOG, OnError.IGNORE, OnError.RAISE])
def test_a_followed_fetch_s_unlisted_pair_follows_unlisted_symbols(
    *,
    live: Callable[..., Table],
    massive_session: dm.MassiveSession,
    rest: Recorder,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    setting: OnError,
) -> None:
    core = massive_session._core
    monkeypatch.setattr(core, "config", dataclasses.replace(core.config, unlisted_symbols=setting))
    error = UnlistedPairError("'DOT' cannot be split: Massive's reference data does not list it")
    rest.outcomes = {"DOT": SymbolFailure(error, FailureKind.PERMANENT)}
    q = make(massive_session, LAST_TRADE_SPEC, symbols=live("AAPL", "DOT"))
    assert isinstance(q, FetchQuery)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive.typehints._queries"):
        if setting is OnError.RAISE:
            with pytest.raises(UnlistedPairError, match="does not list it"):
                q.fetch()
            rest.outcomes = {}  # the raised error's traceback holds the session
            return
        result = q.fetch()
    assert len(caplog.messages) == (1 if setting is OnError.LOG else 0)
    massive_session.release(result)
    follows(massive_session)


def test_a_followed_fetch_groups_symbols_only_where_a_request_names_many() -> None:
    batched = dataclasses.replace(LAST_TRADE_SPEC, symbols_per_request=250)
    assert _symbols_per_read(batched) == 250
    assert _symbols_per_read(LAST_TRADE_SPEC) == 1


def test_a_followed_snapshot_reads_its_symbols_in_one_request_and_splits_the_rows(
    *,
    live: Callable[..., Table],
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: Callable[[Table], list[Any]],
) -> None:
    # Through the real REST transport: one ticker.any_of request names both symbols, and each
    # row goes to its symbol by its ticker.
    rows = [{"ticker": "AAPL", "type": "stocks"}, {"ticker": "MSFT", "type": "stocks"}]
    fake_rest.reply("/v3/snapshot", Reply(envelope(rows)))
    result = rest_session.snapshots(live("AAPL", "MSFT")).fetch()
    assert sorted(r["Symbol"] for r in dh_to_pylist(result)) == ["AAPL", "MSFT"]
    ((path, query),) = fake_rest.get_queries()
    assert path == "/v3/snapshot"
    assert query["ticker.any_of"] == ["AAPL,MSFT"]
    rest_session.release(result)
    follows(rest_session)


def test_a_followed_fetch_sends_its_limit_on_its_request(
    *, live: Callable[..., Table], rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # `limit` is Massive's own cap on a request's rows, sent as the caller gave it.
    fake_rest.reply("/v3/snapshot", Reply(envelope([{"ticker": "AAPL", "type": "stocks"}])))
    result = rest_session.snapshots(live("AAPL"), limit=1).fetch()
    ((_, query),) = fake_rest.get_queries()
    assert (query["ticker.any_of"], query["limit"]) == (["AAPL"], ["1"])
    rest_session.release(result)
    follows(rest_session)


def test_a_fetch_of_an_empty_refreshing_table_fills_as_symbols_are_added(
    *,
    live: Callable[..., Table],
    massive_session: dm.MassiveSession,
    rest: Recorder,
    dh_to_pylist: Callable[[Table], list[Any]],
) -> None:
    rest.outcomes = {"AAPL": rows_of(massive_session, LAST_TRADE_SPEC, 1)}
    wl = live()
    q = make(massive_session, LAST_TRADE_SPEC, symbols=wl)
    assert isinstance(q, FetchQuery)
    result = q.fetch()
    assert result.is_refreshing
    assert result.size == 0
    assert (rest.fetched, rest.checks) == ([], [])  # the first read checks the parameters
    cast("Any", wl).add(new_table([string_col("Symbol", ["AAPL"])]))
    _until(lambda: len(dh_to_pylist(result)) == 1)
    massive_session.release(result)
    follows(massive_session)


def test_a_followed_fetch_lists_an_article_under_each_symbol_it_answers(
    *,
    live: Callable[..., Table],
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: Callable[[Table], list[Any]],
) -> None:
    # Through the real REST transport: each ticker is read in a request of its own, and an
    # article Massive returns for both is the data listed by symbol, so it is listed twice;
    # deleting one ticker leaves the other's copy.
    shared = {
        "id": "both",
        "published_utc": "2024-06-24T18:00:00Z",
        "tickers": ["AAPL", "MSFT"],
        "title": "Article both",
        "article_url": "https://example.com/both",
        "publisher": {"name": "Example"},
    }
    for ticker in ("AAPL", "MSFT"):
        fake_rest.reply(f"/v2/reference/news?limit=1000&ticker={ticker}", Reply(envelope([shared])))
    wl = live("AAPL", "MSFT")
    result = rest_session.reference.news(symbols=wl).fetch()
    assert [r["Id"] for r in dh_to_pylist(result)] == ["both", "both"]
    assert sorted(query["ticker"][0] for _, query in fake_rest.get_queries()) == ["AAPL", "MSFT"]
    cast("Any", wl).delete(new_table([string_col("Symbol", ["AAPL"])]))
    _until(lambda: [r["Id"] for r in dh_to_pylist(result)] == ["both"])
    rest_session.release(result)
    follows(rest_session)


def test_a_followed_read_s_patience_starts_when_its_read_does(
    live: Callable[..., Table], *, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # One read thread: MSFT waits in the queue while AAPL is read. Its patience for rate
    # limits begins only when its own read begins, so the time it queued never counts.
    events: list[str] = []
    lock = threading.Lock()
    patience = rest_module.RestReader.patience

    def recorded(reader: rest_module.RestReader) -> Patience:
        with lock:
            events.append("patience")
        return patience(reader)

    def heard(received: Received) -> None:
        with lock:
            events.append(received.target.split("?")[0].rsplit("/", 1)[-1])

    monkeypatch.setattr(rest_module.RestReader, "patience", recorded)
    fake_rest.on_request(heard)
    fake_rest.reply_default(Reply(envelope([])))
    config = dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url, max_concurrency=1)
    with dm.MassiveSession(config) as m:
        result = m.stocks.trades(live("AAPL", "MSFT")).history(
            "2025-03-03", "2025-03-03", wait=False
        )
        _until(lambda: events.count("patience") == 2 and len(events) == 4)
        m.release(result)
    assert events == ["patience", "AAPL", "patience", "MSFT"]


def test_a_followed_range_no_row_can_lie_in_checks_nothing(
    live: Callable[..., Table], *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    moment = datetime(2025, 3, 3, 15, tzinfo=UTC)
    result = trades(massive_session, live("AAPL")).history(moment, moment)  # an empty range
    assert rest.checks == []  # nothing can be asked: no request, no parameter to check
    massive_session.release(result)
    follows(massive_session)


# --- fetch() and poll() of a table passed where a dataset takes one value ---------------------


@pytest.fixture
def live_codes(dh_input_table: Callable[..., InputTable]) -> Callable[..., Table]:
    # Makes a refreshing, keyed table of futures product codes, as `m.futures.products()` rows
    # carry them.
    def make(*codes: str) -> Table:
        table = dh_input_table({"ProductCode": dtypes.string}, key_cols=["ProductCode"])
        if codes:
            table.add(new_table([string_col("ProductCode", list(codes))]))
        return table

    return make


def test_a_static_table_of_underlyings_reads_each_chain_once(
    *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    rest.outcomes = {
        "AAPL": rows_of(massive_session, CHAIN, 2),
        "MSFT": rows_of(massive_session, CHAIN, 1),
    }
    underlyings = new_table([string_col("Symbol", ["MSFT", "AAPL", "MSFT"])])
    q = massive_session.options.chain(underlyings)
    assert repr(q) == "m.options.chain(<table>)"
    result = q.fetch()
    assert not result.is_refreshing
    assert result.size == 3
    [(values, request, _)] = rest.valued
    assert values == ("AAPL", "MSFT")
    assert request.value_table is not None
    assert request.for_value("AAPL").params == (("underlyingAsset", "AAPL"),)


def test_a_static_table_of_values_fails_when_one_value_fails(
    *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    refusal = dm.EntitlementError("HTTP 403: not in the plan")
    rest.outcomes = {"NQ": SymbolFailure(refusal, FailureKind.PERMANENT)}
    codes = new_table([string_col("ProductCode", ["ES", "NQ"])])
    with pytest.raises(dm.EntitlementError, match="not in the plan"):
        massive_session.futures.products(product_code=codes).fetch()
    rest.outcomes = {}  # the raised error's traceback holds the session


def test_a_static_table_of_values_reads_in_the_background_with_wait_false(
    *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    codes = new_table([string_col("ProductCode", ["ES"])])
    result = massive_session.futures.products(product_code=codes).fetch(wait=False)
    assert result.is_refreshing
    [(_, _, checked)] = rest.checks  # checked at the call, with the table's first value
    assert ("product_code", "ES") in checked.params
    _until(lambda: len(rest.valued) == 1)
    massive_session.release(result)


def test_a_table_of_values_with_no_value_it_takes_is_refused(
    *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    empty = new_table([string_col("ProductCode", [None])])
    with pytest.raises(ValueError, match=r"the product_code table holds no values$"):
        massive_session.futures.products(product_code=empty).fetch()
    options = new_table([string_col("Symbol", ["O:AAPL250117C00150000"])])
    q = massive_session.options.chain(options)
    with pytest.raises(ValueError, match="no row of the underlying table is a value this dataset"):
        q.fetch()
    with pytest.raises(ValueError, match="no row of the underlying table is a value this dataset"):
        q.poll(5)
    assert rest.valued == []


def test_fetch_follows_a_refreshing_table_of_product_codes(
    *,
    live_codes: Callable[..., Table],
    massive_session: dm.MassiveSession,
    rest: Recorder,
    drain: Callable[[], None],
    dh_to_pylist: Callable[[Table], list[Any]],
) -> None:
    rest.outcomes = {"ES": rows_of(massive_session, futures.PRODUCTS, 1)}
    codes = live_codes("ES")
    result = massive_session.futures.products(product_code=codes).fetch()
    assert result.is_refreshing
    assert result.size == 1  # the call-time code is in the result when the call returns
    assert result.attributes()["TableDescription"].endswith(
        "Follows its table of product_code values: each value's rows are Massive's answer as of "
        "its read."
    )
    rest.outcomes["NQ"] = rows_of(massive_session, futures.PRODUCTS, 2)
    cast("Any", codes).add(new_table([string_col("ProductCode", ["NQ"])]))
    _until(lambda: len(dh_to_pylist(result)) == 3)
    assert [values for values, _, _ in rest.valued] == [("ES",), ("NQ",)]
    assert "follow-read" in rest.valued[1][2]
    assert set(symbol_reads(massive_session, drain, dh_to_pylist)) == {"ES", "NQ"}
    cast("Any", codes).delete(new_table([string_col("ProductCode", ["ES"])]))
    _until(lambda: len(dh_to_pylist(result)) == 2)
    massive_session.release(result)
    follows(massive_session)


def test_fetch_follows_a_refreshing_table_of_underlyings_with_wait_false(
    *, live: Callable[..., Table], massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    result = massive_session.options.chain(live()).fetch(wait=False)
    assert result.is_refreshing
    assert (rest.valued, rest.checks) == ([], [])  # nothing to read or check yet
    assert "Follows its table of underlying values" in result.attributes()["TableDescription"]
    massive_session.release(result)
    follows(massive_session)
    result = massive_session.options.chain(live("AAPL")).fetch(wait=False)
    _until(lambda: len(rest.valued) == 1)
    [(_, _, checked)] = rest.checks
    assert ("underlyingAsset", "AAPL") in checked.params
    massive_session.release(result)
    follows(massive_session)


def test_fetch_follows_one_refreshing_table_at_a_time(
    *,
    live: Callable[..., Table],
    live_codes: Callable[..., Table],
    massive_session: dm.MassiveSession,
    rest: Recorder,
) -> None:
    q = massive_session.futures.snapshots(live("ESZ6"), product_code=live_codes("ES"))
    with pytest.raises(
        ValueError,
        match=r"both symbols and product_code are refreshing tables\. Use poll\(interval\), "
        r"which reads both at every cycle, or pass one of them as a static snapshot, such as "
        r"product_code=table\.snapshot\(\)$",
    ):
        q.fetch()
    # A static symbol table beside it is read once, and the codes are followed.
    contracts = new_table([string_col("Symbol", ["ESZ6"])])
    result = massive_session.futures.snapshots(contracts, product_code=live_codes("ES")).fetch()
    [(_, request, _)] = rest.valued
    assert request.symbols == ("ESZ6",)
    massive_session.release(result)
    follows(massive_session)


def test_poll_reads_a_table_of_values_at_its_call(
    *, live_codes: Callable[..., Table], massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    codes = live_codes("ES")
    massive_session.futures.market_status(product_code=codes).poll(5)
    [(verb, _, request, _)] = rest.calls
    assert verb == "poll"
    assert request.value_table is not None
    assert request.value_table.table is codes
