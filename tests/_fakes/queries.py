"""Fake transports, test datasets, and query builders the query tests share."""

import dataclasses
import threading
from collections.abc import Callable, Sequence
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any, Protocol, cast

import pytest
from deephaven import arrow as dh_arrow
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.history_source import HistoryWindow
from deephaven_massive._datasets.request import Request
from deephaven_massive._enums import (
    AssetClass,
    NotBars,
    OnError,
    PollMode,
    Timespan,
)
from deephaven_massive._ingest import history_reads
from deephaven_massive._massive import Transport
from deephaven_massive._tables.publisher import BlinkPublisher
from deephaven_massive._tables.session_core import SessionCore
from deephaven_massive._tables.transports import (
    FlatFileRead,
    SymbolOutcome,
    SymbolRows,
)
from deephaven_massive.namespaces._query_building import build_query
from deephaven_massive.queries import (
    FetchQuery,
    Query,
    StreamHistoryQuery,
)

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
        self.split_pairs: list[bool] = []
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

    def check_fetch(self, spec: DatasetSpec, request: Request, *, split_pairs: bool) -> None:
        self.checks.append(("fetch", spec, request))
        self.split_pairs.append(split_pairs)

    def check_history(
        self, spec: DatasetSpec, request: Request, window: HistoryWindow, *, split_pairs: bool
    ) -> None:
        self.checks.append(("history", spec, request))
        self.split_pairs.append(split_pairs)

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


def make(session: dm.MassiveSession, spec: DatasetSpec, **kwargs: object) -> Query:
    return build_query(session._core, spec, query_type=Query, **cast("dict[str, Any]", kwargs))


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


def strict(session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch) -> None:
    # The session's rule for symbol-table rows becomes RAISE.
    config = dataclasses.replace(session._core.config, unsendable_symbols=dm.OnError.RAISE)
    monkeypatch.setattr(session._core, "config", config)


def join_reads(session: dm.MassiveSession) -> None:
    # Waits for every background read of the session to end, then for its row to be written.
    for state in list(session._core.workers._workers.values()):
        cast("Any", state.fail).__self__.join(TIMEOUT)


def released(session: dm.MassiveSession, *tables: Table) -> None:
    for table in tables:
        session.release(table)


def follows(m: dm.MassiveSession) -> None:
    # Waits until no followed read of the session is left: its coordinator has ended.
    pause = threading.Event()
    for _ in range(int(TIMEOUT / 0.01)):
        if m._core.follows.following == 0:
            return
        pause.wait(0.01)
    raise AssertionError("a followed read kept running")


def until(condition: Callable[[], bool]) -> None:
    pause = threading.Event()
    for _ in range(int(TIMEOUT / 0.01)):
        if condition():
            return
        pause.wait(0.01)
    assert condition()


def symbol_reads(
    m: dm.MassiveSession, drain: Callable[[], None], to_list: Callable[[Table], list[Any]]
) -> dict[str, dict[str, Any]]:
    drain()
    return {row["Symbol"]: row for row in to_list(m.symbol_reads())}
