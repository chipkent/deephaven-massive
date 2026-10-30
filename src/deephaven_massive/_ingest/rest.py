"""The REST transport: every REST dataset's requests, pages, and tables, on the session's client.

Massive's REST API serves the `fetch()` and `poll()` verbs of list, snapshot, and "last"
datasets and the REST side of `history()`. This module turns a dataset's declaration
(`DatasetSpec`) and a call's arguments (`Request`) into requests on the session's one
`_massive.RestClient`, and the pages that come back into tables in the dataset's schema:

- **Requests.** Each REST path is a generated `RestPath`, whose parameters say what can be
  sent and how. Path parameters come from the symbol (a key the dataset fills from the request,
  such as `{stocksTicker}`; a currency pair's `{from}/{to}`), from the typed parameters, from
  the bar `multiplier` and `timespan`, and for `history()` from the window (the aggregates'
  `{from}/{to}` instants, the grouped-daily `{date}`). Query parameters are the typed parameters,
  the validated `filters` (each key mapped back to its dotted wire name), the time axis
  (`DatasetSpec.time_axis`: whole days on a calendar-date axis, and on an instant axis,
  whether its parameter is a timestamp or a date typed over an instant field such as
  `published_utc`, the instants that start the bounds' days in the request's day zone), the
  symbol parameter (`DatasetSpec.symbol_param`, at most `DatasetSpec.symbols_per_request`
  symbols a request), `adjusted` (`Request.adjusted`), the futures `resolution`, the page
  size (`DatasetSpec.page_size`, or `Request.limit` when that is smaller, so a small cap
  fetches no more than it keeps), and for `history()` the ascending direction (`_ascending`).
  `Request.limit` caps the total rows, and the client stops paging there.
- **History.** `history()` requests the instants its window covers
  (`HistoryWindow.requested`) and keeps the rows the window keeps (`keep_window`). Futures trades
  and quotes named by session dates are requested one session at a time
  (`session_end_date=<date>`, weekdays only); `dm.ALL` daily bars one grouped day at a time
  (weekdays only for the classes that do not trade at weekends). Bars of a day or longer
  are stamped at 00:00 of their day in the class day zone, and a stamp Massive is not known to
  use raises (`row_times.stamp_rule`, `stamped`).
- **Time columns.** Every page's rows get the library's time columns (`row_times.TimeRule`):
  `Date`, a bar's `TimestampStart` and `TimestampEnd`, and `TimestampInsert`, the instant the
  page arrived. The day zone of each call follows its own symbols' class (`dates.day_class`),
  so a crypto indicator gets UTC days and a stock indicator New York days.
- **Adjustment.** Bars requested with an explicit `adjusted` must come back on that basis: each
  page's `adjusted` echo (`Page.adjusted`) that differs from the request raises
  `MassiveError`, and a page without one logs a WARNING once per path and session, so one
  call never silently mixes adjusted and unadjusted prices.
- **Fan-out.** A path that takes one symbol a request is called once per symbol, at most
  `MassiveConfig.max_concurrency` at a time. A symbol with no data returns no rows: HTTP 404
  on its first request drops that symbol with one WARNING. HTTP 404 on a later page of a
  symbol's pages means its data was cut short and raises `MassiveError`. Any other error
  fails the call, and every request of the read still waiting for the rate governor stops at
  once (each read has a `Cancel` token of its own, set when a fan-out ends, and a `poll()`
  cycle's token follows its poller's). The token is also the read's lane in the governor:
  the reads, poll cycles, and host decisions waiting on a bucket take turns, whatever their
  number of threads.
- **Rate limits.** Each request is charged to its Massive subscription (`DatasetSpec`'s class,
  or for a dataset with a path per symbol class the class of the request's symbols; a request
  of no class goes to the class its `market` filter names, else to the unpaced `other`
  bucket), and the session's client asks the shared
  `RequestGovernor` before every attempt (`_ingest/rate_limits.py`). A static read whose
  paced requests cannot all start within a minute logs its least duration at INFO, and a
  `poll()` whose cycles need more paced requests than its interval allows logs one WARNING.
- **Rows.** Each page's rows go through `_massive.to_batch` (strict decoding against the path's
  generated fields), then `source_columns.rename`'s column mapping for the dataset, the timestamp
  routine of `_datasets/timestamps.py`, and the lattice widening of `source_columns.widen`, into one
  Arrow batch of the dataset's merged schema. Only one page of decoded rows is held at a time.
  A row that carries an `error` (a ticker the plan does not include) keeps its `Error` and
  `Message` columns and is logged once.
- **Drift.** Each page's `DriftReport` goes to the session's `DriftRegistry` (one WARNING per
  dataset, transport, and field; undocumented envelope statuses once per path); a `poll()`
  table's drift also reaches its `SchemaDrift` through `SessionCore.record_stream_drift`.
- **Errors.** Every `WireError` becomes its public exception through `_errors.from_wire`
  (401 `AuthenticationError`, 403 `EntitlementError`), except a pause longer than the read's
  patience still covers (`rate_limits.read_patience`: 10 minutes of pauses, or 90 s while the
  thread holds the update graph's lock), which becomes `MassiveError("rate limited: ...")`
  with status 429 (the first 429 of each subscription has already logged its WARNING). A row that
  breaks the schema (a missing required key, a value that does not convert, a timestamp out of
  bounds) raises `RowDataError`. Static verbs log each error at ERROR and raise it.
- **Host decisions.** `resolve_host` decides each WebSocket host once per session
  (`_ingest/host_decision.py`), single-flight: the first caller of a (class, family) makes the one
  REST request and every concurrent caller waits for its answer.

`RestState` is the part a session shares (the client, the host decisions, the live pollers,
the once-per-session warnings); `SessionClients` creates it on first use and registers its
close with the session. `RestEndpoints`, the session core's `RestTransport`, is a lightweight
view of the core and that state, made for each call. `poll()` itself is `_ingest/poll.py`.
The requests of a call are planned by `rest_requests.py`, and its pages become batches in
`rest_rows.py`; this module runs the plan, page by page, and turns its failures into the
public errors.

The module is one subject although it runs past 1,000 lines: it is one transport, and the
reads, the fan-out, the crypto ticker list, the host decisions, and the failures all act on
the same session state (`RestState`) and the same read (`RestReader`).
"""

from __future__ import annotations

import functools
import logging
import re
import threading
import time
from collections import Counter
from concurrent.futures import FIRST_EXCEPTION, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Final, TypeAlias, cast
from urllib.parse import urlsplit

import pyarrow as pa

from deephaven_massive._datasets.dates import day_class, is_trading_day
from deephaven_massive._datasets.history_source import partition_dates, session_date_param
from deephaven_massive._datasets.row_times import RowTimes, stamp_rule, time_rule
from deephaven_massive._datasets.symbols import SymbolTable, symbol_class
from deephaven_massive._enums import AssetClass, Timespan, UnknownSymbols, subscription_of
from deephaven_massive._errors import MassiveError, PermanentError, from_wire
from deephaven_massive._ingest import host_decision, poll
from deephaven_massive._ingest.host_decision import HostDecisionCache
from deephaven_massive._ingest.poll import Pollers, UnsupportedParameterError
from deephaven_massive._ingest.rate_limits import (
    OTHER,
    PaceCheck,
    ReadShape,
    announce,
    bucket_of,
    read_patience,
)
from deephaven_massive._ingest.rest_requests import (
    Call,
    axis_query,
    fetch_paths,
    history_calls,
    own_symbols,
    plan_calls,
    shared_params,
)
from deephaven_massive._ingest.rest_rows import (
    RowConverter,
    keep_window,
    stamped,
    whole_bars,
    with_remedy,
)
from deephaven_massive._massive import Cancel, DriftReport, Transport, WireError
from deephaven_massive._tables.columns import dataset_table
from deephaven_massive._tables.diagnostics import ConnectionRow, ConnectionState
from deephaven_massive._tables.execution_context import entered, holds_update_graph_lock
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from datetime import timedelta

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._enums import ChannelFamily, PollMode
    from deephaven_massive._massive import Patience, RestClient, RestPath
    from deephaven_massive._tables.diagnostics import DriftRegistry
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._tables.transports import HostDecision

__all__ = [
    "RestEndpoints",
    "RestReader",
    "RestState",
    "call_bucket",
]

logger = logging.getLogger(__name__)

_TICKERS_PATH: Final = "/v3/reference/tickers"
"""The reference path that lists crypto tickers with their base and quote currencies."""
_TICKERS_PAGE: Final = 1000
"""The rows per page of the crypto ticker list: the endpoint's largest page."""

CryptoTickers: TypeAlias = "Mapping[str, tuple[str, str] | str]"
"""Massive's crypto tickers: each one's `(base, quote)`, or why its currencies cannot be used."""


def _crypto_entry(ticker: str, row: Mapping[str, object]) -> tuple[str, str] | str:
    """Read one row of Massive's crypto ticker list.

    Args:
        ticker: The row's ticker, such as `X:DOTUSD`.
        row: The row, with `base_currency_symbol` and `currency_symbol`.

    Returns:
        `(base, quote)` when the two currencies join to the ticker; otherwise why not.
    """
    base, quote = row.get("base_currency_symbol"), row.get("currency_symbol")
    if isinstance(base, str) and isinstance(quote, str) and f"X:{base}{quote}" == ticker:
        return base, quote
    return (
        f"Massive's reference data gives the currencies {base!r} and {quote!r}, which do not "
        "join to it"
    )


class UnlistedPairError(PermanentError):
    """A crypto ticker whose two currencies Massive's reference data does not give.

    A path that takes the pair as two currencies (`/v1/last/crypto/{from}/{to}`,
    `/v1/open-close/crypto/{from}/{to}/{date}`) needs the split, which only Massive's reference
    data gives; the session's crypto ticker list, read once, does not list the ticker, or gives
    currencies that do not join to it (`RestReader.crypto_pair`). A `poll()` table fails with
    it, since every later cycle would read the same list.
    """


@dataclass(frozen=True, slots=True)
class _Outcome:
    """What one call returned.

    Attributes:
        batches: Its rows, in the dataset's schema.
        requests: How many requests it made (pages, and a dropped symbol's 404).
    """

    batches: tuple[pa.RecordBatch, ...]
    requests: int


class RestState:
    """The REST resources one session shares: the client, the host decisions, and the pollers.

    `SessionClients.rest()` creates one per session on first use and registers `close` with the
    session, so the client closes, and every poller stops, when the session closes or is
    collected unclosed. It holds no reference to the session core, so that closer never keeps
    the core alive. Thread-safe: the once-only flags sit behind one leaf lock, held only to read
    or change them; the host decisions (`hosts`, a `HostDecisionCache`), the client, and the
    pollers are thread-safe. It
    also holds, for the session, Massive's list of crypto tickers with their two currencies
    (`RestReader.crypto_pair`): read once, on first need, by the first caller, while every
    other caller waits on its future outside the lock.
    """

    __slots__ = ("_client", "_crypto", "_lock", "_once", "hosts", "label", "pollers")

    def __init__(self, client: RestClient, label: str) -> None:
        """Hold the session's client; no request is made.

        Args:
            client: The session's REST client.
            label: The session's label, such as `"MassiveSession #1"`.
        """
        self._client = client
        self.label: Final = label
        self.pollers: Final = Pollers(label)
        self._lock = threading.Lock()
        self.hosts: Final = HostDecisionCache()
        self._once: set[tuple[str, ...]] = set()
        self._crypto: Future[CryptoTickers] | None = None

    @property
    def client(self) -> RestClient:
        """The session's REST client."""
        return self._client

    @property
    def closed(self) -> bool:
        """True once `close` has run."""
        return self.pollers.closed

    def close(self) -> None:
        """Stop every poller and close the client; idempotent, and safe from any thread.

        A request on the wire finishes or fails on its own; every later request, and every
        retry wait, raises at once.
        """
        self.pollers.close()
        self._client.close()

    def crypto_future(self) -> tuple[Future[CryptoTickers], bool]:
        """Return the session's crypto ticker list, or claim it for the caller to read.

        Returns:
            The list's future and whether the caller created it: the creator reads the list
            and completes the future; everyone else waits on it, outside every lock.
        """
        with self._lock:
            if self._crypto is not None:
                return self._crypto, False
            self._crypto = Future()
            return self._crypto, True

    def forget_crypto(self) -> None:
        """Remove a failed read of the crypto ticker list, so that the next caller tries again.

        Only the read's creator calls it, and no one can claim a new read before, so the read
        removed is always the failed one.
        """
        with self._lock:
            self._crypto = None

    def first(self, *key: str) -> bool:
        """Tell whether a fact is seen for the first time in this session, and remember it.

        Args:
            *key: The fact, such as `("missing", "stocks.prev", "AAPL")`.

        Returns:
            True the first time, False afterwards.
        """
        with self._lock:
            if key in self._once:
                return False
            self._once.add(key)
            return True


@dataclass(frozen=True, eq=False, slots=True)
class RestReader:
    """Reads REST paths into Arrow tables of a dataset's schema; holds no session core.

    `RestEndpoints` builds one for each call, and a `poll()` table keeps its own for its life,
    so a poller never keeps the session core alive. Thread-safe: it holds only thread-safe
    objects.

    Attributes:
        state: The session's REST resources.
        catalog: The dataset catalog.
        drift: The session's drift registry.
        max_concurrency: The most requests of one fan-out at a time.
        unknown_symbols: The session's rule for a symbol spelling the library cannot read
            (`MassiveConfig.unknown_symbols`).
        threads: The name prefix of the threads of a fan-out pool this reader makes, such as
            `"massive #1 call-7 rest"`: the session, the call, and the transport.
        exec_ctx: The session's execution context, which tells whether a read's thread holds
            the update graph's lock, and so how long the read waits out a pause.
    """

    state: RestState
    catalog: Catalog
    drift: DriftRegistry
    max_concurrency: int
    unknown_symbols: UnknownSymbols
    threads: str
    exec_ctx: ExecutionContext

    def patience(self) -> Patience:
        """Return the patience of a read starting now (`rate_limits.read_patience`).

        Call it on the read's own thread, before any fan-out and outside every library lock:
        it asks Deephaven whether this thread holds the update graph's lock.

        Returns:
            A new `Patience`, for every request of the read.
        """
        return read_patience(self.exec_ctx)

    def rest_path(self, template: str) -> RestPath:
        """Return a generated REST path.

        Args:
            template: The path template.

        Returns:
            The path.
        """
        return cast("RestPath", self.catalog.source(Transport.REST, template))

    def crypto_pair(self, symbol: str) -> tuple[str, str]:
        """Return a crypto ticker's base and quote currencies, with no guess.

        A joined ticker does not say where its quote starts (`X:DOTUSD` could be `DOT-USD` or
        `DO-TUSD`; `X:ETHWBTC` ends in `BTC` but quotes in `WBTC`), so every split comes from
        Massive's reference data: the session reads Massive's whole list of crypto tickers
        once, on first need (`/v3/reference/tickers?market=crypto`, every page, first with
        `active=true` and then with `active=false`, so a delisted pair splits too), keeping
        each ticker's `base_currency_symbol` and `currency_symbol`, whose join must be the
        ticker. Its requests count against Massive's limit on reference data, so on a
        Basic subscription they wait their turn. Blocks the calling thread for that read, or
        while another thread makes it; holds no lock meanwhile. A failed read is not kept, so
        the next call tries again. A read that succeeds is kept for the session as it is, even
        when it lists no pair: every pair is then refused.

        Args:
            symbol: A canonical crypto ticker, such as `X:DOTUSD`.

        Returns:
            `(base, quote)`, such as `("DOT", "USD")`.

        Raises:
            UnlistedPairError: If the list does not hold the ticker, or its currencies do not
                join to the ticker; a `poll()` table fails with it.

        A failed read raises its public error (`AuthenticationError`, `EntitlementError`, or
        `MassiveError`). Every message names the ticker; nothing falls back to a guessed split.
        """
        where = f"{symbol!r} cannot be split into its two currencies"
        try:
            tickers = self._crypto_tickers()
        except MassiveError as public:
            raise type(public)(
                f"{where}: reading Massive's crypto tickers failed: {public}",
                status=public.status,
                request_id=public.request_id,
            ) from public
        entry = tickers.get(symbol)
        if entry is None:
            raise UnlistedPairError(f"{where}: Massive's reference data does not list it")
        if isinstance(entry, str):
            raise UnlistedPairError(f"{where}: {entry}")
        return entry

    def _crypto_tickers(self) -> CryptoTickers:
        """Return the session's crypto ticker list, reading it on first need.

        Returns:
            Each crypto ticker Massive lists: its `(base, quote)`, or why its currencies
            cannot be used.

        A failed read raises its public `MassiveError` (`AuthenticationError`,
        `EntitlementError`), also to every caller that waited on it; an unexpected error of
        the reader propagates to it, and its waiters get a `MassiveError` naming it.
        """
        future, created = self.state.crypto_future()
        if not created:
            return future.result()
        tickers: dict[str, tuple[str, str] | str] = {}
        try:
            # The list is reference data, which Massive counts in a bucket of its own. Its
            # pages share one lane of their own, so they wait their turn with other reads
            # instead of queueing behind them; closing the client still ends their waits.
            patience = self.patience()
            with Cancel() as lane:
                # Listed pairs first, then delisted ones, so that a past date of a delisted
                # pair still splits; a ticker in both keeps its listed entry.
                for active in (True, False):
                    query = {"market": "crypto", "active": active, "limit": _TICKERS_PAGE}
                    for page in self.state.client.pages(
                        self.rest_path(_TICKERS_PATH),
                        {},
                        query,
                        bucket=OTHER,
                        cancel=lane,
                        patience=patience,
                    ):
                        for row in page.rows:
                            ticker = row.get("ticker")
                            if isinstance(ticker, str):
                                tickers.setdefault(ticker, _crypto_entry(ticker, row))
        except WireError as error:
            public = self._public(error)
            self.state.forget_crypto()
            future.set_exception(public)
            raise public from error
        except BaseException as error:
            # Every waiter gets a MassiveError; the reader's own unexpected error propagates.
            self.state.forget_crypto()
            future.set_exception(MassiveError(f"the read failed: {error!r}"))
            raise
        future.set_result(tickers)
        return tickers

    def read(
        self,
        spec: DatasetSpec,
        rest_path: RestPath,
        calls: Sequence[Call],
        *,
        request: Request,
        limit: int | None,
        window: HistoryWindow | None = None,
        remedy: str = "",
        record: Callable[[DriftReport], None] | None = None,
        executor: ThreadPoolExecutor | None = None,
        cancel: Cancel | None = None,
        patience: Patience,
    ) -> tuple[pa.Table, int]:
        """Make the calls and return their rows as one Arrow table of the dataset's schema.

        One call runs on this thread; several run on `executor`, or on a pool of
        `min(max_concurrency, calls)` threads made for this read. Every request of the read
        listens to a `Cancel` token of the read's own (following `cancel`), which is also the
        read's lane in the rate governor, so a read takes its turn with the others whatever
        its number of threads. A fan-out sets the token when it ends. So if a call fails, or
        this thread is interrupted, the calls not yet started are cancelled, the running ones
        stop waiting for the rate governor at once, the read waits for the requests already on
        the wire, and the failure of the earliest call that failed on its own is raised. Every
        page's rows get the library's time columns (`_call`).

        Args:
            spec: The dataset.
            rest_path: The path.
            calls: The planned calls, in result order.
            request: The normalized arguments: the timespan and multiplier of bars, and the
                `adjusted` basis every page must echo.
            limit: The cap on each call's rows, or None; `read_fetch` caps the total.
            window: The `history()` window whose rows to keep, or None.
            remedy: What the caller can do instead when a bar's stamp is refused, which the
                verb supplies; bars of a day or longer move to 00:00 of their day in the class
                day zone, and a stamp Massive is not known to use raises `MassiveError`
                (`row_times.stamp_rule`).
            record: Also given each page's drift, for a `poll()` table's `SchemaDrift`.
            executor: A pool to run several calls on, which the caller keeps; None for a pool
                of this read's own.
            cancel: Ends the read's waits when set, such as a poller's stop token; None when
                only the session's close should.
            patience: The patience of the whole read (`patience()`, taken once when the read
                began, on its own thread): the most time its requests wait out pauses, shared
                by every path of a read that spans several.

        Returns:
            The rows, in call order and then page order, and the number of requests made.

        Errors: the public error of each `WireError` (`_public`), `RowDataError` for a row
        that breaks the schema, and the `MassiveError` of `_call` for a page on another
        adjustment basis, a refused stamp, or a symbol whose data was cut short. A closed
        session raises `MassiveError("... is closed")`.
        """
        converter = RowConverter.build(
            self.catalog, spec, rest_path, unknown_symbols=self.unknown_symbols
        )
        with Cancel(*(() if cancel is None else (cancel,))) as stop:
            run = functools.partial(
                self._call,
                converter,
                request=request,
                limit=limit,
                window=window,
                remedy=remedy,
                record=record,
                spec=spec,
                stop=stop,
                patience=patience,
            )
            if len(calls) <= 1:
                outcomes = [run(call) for call in calls]
            else:
                outcomes = self._fan_out(run, calls, executor=executor, stop=stop)
        batches = [batch for outcome in outcomes for batch in outcome.batches]
        table = pa.Table.from_batches(batches, schema=converter.arrow_schema)
        return table, sum(outcome.requests for outcome in outcomes)

    def _fan_out(
        self,
        run: Callable[[Call], _Outcome],
        calls: Sequence[Call],
        *,
        executor: ThreadPoolExecutor | None,
        stop: Cancel,
    ) -> list[_Outcome]:
        """Run several calls concurrently and return their outcomes in call order.

        When a call fails, or this thread is interrupted, the read's token is set before the
        read waits for its calls, so a call waiting for the rate governor stops at once
        instead of being sent. The calls that failed only because of that are not reported.

        Args:
            run: Makes one call.
            calls: The calls.
            executor: The caller's pool, or None for one of this read's own.
            stop: The read's token.

        Returns:
            The outcomes, in call order.

        The earliest call, in call order, among those that had failed when the first failure
        was seen raises its error.
        """
        own = executor is None
        pool = executor or ThreadPoolExecutor(min(self.max_concurrency, len(calls)), self.threads)
        futures = [pool.submit(run, call) for call in calls]
        try:
            done, _ = wait(futures, return_when=FIRST_EXCEPTION)
        finally:
            stop.set()
            for future in futures:
                future.cancel()
            wait(futures)
            if own:
                pool.shutdown()
        failed = [future for future in futures if future in done and future.exception()]
        if failed:
            failed[0].result()  # raises the failure
        return [future.result() for future in futures]

    def _call(
        self,
        converter: RowConverter,
        call: Call,
        *,
        spec: DatasetSpec,
        request: Request,
        limit: int | None,
        window: HistoryWindow | None,
        remedy: str,
        record: Callable[[DriftReport], None] | None,
        stop: Cancel,
        patience: Patience,
    ) -> _Outcome:
        """Request one call's pages and convert their rows.

        Each page is checked against the requested adjustment basis (`_check_adjusted`), its
        bars of a day or longer are stamped (`stamped`), its rows outside the `history()`
        window dropped (`keep_window`), and its rows given the library's time columns
        (`row_times.TimeRule.fill`), with `TimestampInsert` the instant the page arrived. The
        call's day zone follows its own symbols' class (`dates.day_class`).

        Args:
            converter: The path's conversion.
            call: The call.
            spec: The dataset.
            request: The normalized arguments.
            limit: The cap on the call's rows, or None.
            window: The `history()` window whose rows to keep, or None.
            remedy: What the caller can do instead when a stamp is refused.
            record: Also given each page's drift, or None.
            stop: The read's token, which ends the call's waits.
            patience: The patience of the call's read, which its pause waits are charged to.

        Returns:
            The call's rows and request count; no rows for a per-symbol call whose first
            request Massive answered with HTTP 404 (a symbol with no data), which logs one
            WARNING per symbol and session.

        Raises:
            MassiveError: If a per-symbol call's later page answers HTTP 404, so the symbol's
                data was cut short: the message names the dataset, the symbol, the path, and
                how many pages had arrived.

        A failed request otherwise raises the public error of its `WireError` (`_public`); a
        row that breaks the schema the `RowDataError` of `_Converter.convert`; a page on
        another adjustment basis the `MassiveError` of `_check_adjusted`; and a bar stamped
        at a time Massive is not known to use the `MassiveError` of `stamped`.
        """
        kept: list[pa.RecordBatch] = []
        requests = 0
        template = converter.rest_path.template
        own = own_symbols(spec, call.symbol, call.query)
        zone_class = day_class(spec, map(symbol_class, own))
        stamps = with_remedy(stamp_rule(spec, request, asset_class=zone_class), remedy)
        times = time_rule(spec, request, asset_class=zone_class)
        bucket = _request_bucket(spec, template, call)
        try:
            for page in self.state.client.pages(
                converter.rest_path,
                call.path_params,
                call.query,
                bucket=bucket,
                cancel=stop,
                patience=patience,
                limit=limit,
            ):
                received = time.time_ns()
                requests += 1
                self.drift.report_statuses(template, page.drift.statuses)
                self._check_adjusted(spec, request, template=template, echoed=page.adjusted)
                batches, drift, flagged = converter.convert(page.rows, call.symbol)
                self.drift.report(
                    spec.name, Transport.REST, drift, where=f"GET {template}", symbol=call.symbol
                )
                if record is not None:
                    record(drift)
                self._warn_error_rows(converter, flagged, call.symbol)
                # Stamp before keeping: an unknown stamp outside the window must still raise,
                # not be dropped unseen by `keep_window`.
                if stamps is not None:
                    batches = [stamped(spec, template, batch, rule=stamps) for batch in batches]
                filled = (
                    times.fill(
                        keep_window(batch, window, time=converter.time_name), received=received
                    )
                    for batch in batches
                )
                kept.extend(whole_bars(rows, window) for rows in filled)
        except WireError as error:
            if error.status == HTTPStatus.NOT_FOUND and call.symbol is not None:
                if requests:
                    raise MassiveError(
                        f"{spec.name}: {call.symbol}'s data was cut short: Massive answered "
                        f"HTTP 404 for page {requests + 1} of {template} after {requests} "
                        f"page{'s' if requests > 1 else ''} had arrived; retry the call",
                        status=error.status,
                        request_id=error.request_id,
                    ) from error
                # A futures session is requested on its own: a 404 there is that session's.
                session = (
                    call.query.get(session_date_param(spec))
                    if window is not None and window.requested is None
                    else None
                )
                scope = "" if session is None else f" for the session {session}"
                if self.state.first("missing", spec.name, call.symbol, scope):
                    logger.warning(
                        "%s: %s has no data%s (HTTP 404 from %s); it is left out",
                        spec.name,
                        call.symbol,
                        scope,
                        template,
                    )
                return _Outcome(tuple(kept), requests + 1)
            raise self._public(error) from error
        return _Outcome(tuple(kept), requests)

    def _check_adjusted(
        self, spec: DatasetSpec, request: Request, *, template: str, echoed: bool | None
    ) -> None:
        """Refuse a page of bars on another adjustment basis than the request asked for.

        Bars sent with an explicit `adjusted` (stocks and options) are checked page by page,
        `next_url` pages included: Massive echoes the basis of each aggregates response in its
        envelope's `adjusted`. A page whose echo differs from the request would mix split
        bases in one table, so it fails the call; a page without one cannot be checked, which
        logs a WARNING once per dataset, path, and session. Every other dataset is left
        alone: its paths echo nothing (the indicators, the daily open-close), or it has no
        `adjusted` at all.

        Args:
            spec: The dataset.
            request: The normalized arguments, whose `adjusted` was sent.
            template: The path template, for messages.
            echoed: The page's echo (`Page.adjusted`), or None.

        Raises:
            MassiveError: If the echo differs from the request, naming the dataset, the path,
                and both values.
        """
        sent = request.adjusted
        if not spec.is_bars or sent is None:
            return
        if echoed is None:
            if self.state.first("adjusted-echo", spec.name, template):
                logger.warning(
                    "%s: %s answered without its adjusted echo, so this page's split basis "
                    "(adjusted=%s requested) cannot be checked",
                    spec.name,
                    template,
                    sent,
                )
            return
        if echoed is not sent:
            raise MassiveError(
                f"{spec.name}: Massive answered {template} with adjusted={echoed} although "
                f"adjusted={sent} was requested, so the call would mix split-adjusted and "
                "as-traded prices; retry it"
            )

    def _warn_error_rows(
        self, converter: RowConverter, flagged: Sequence[Mapping[str, object]], symbol: str | None
    ) -> None:
        """Log each ticker of an error row once per session, naming Massive's message.

        A projected path dropped the row; any other path kept it, with its `Error` and
        `Message` columns.

        Args:
            converter: The path's conversion.
            flagged: The page's error rows.
            symbol: The request's one symbol, for a path whose rows do not carry it.
        """
        dataset = converter.dataset
        fate = "dropped" if converter.projected else "kept with its Error and Message columns"
        for row in flagged:
            key = converter.symbol_key
            ticker = str(symbol if key is None else row.get(key))
            if self.state.first("error-row", dataset, ticker):
                logger.warning(
                    "%s: Massive answered %s for %s: %s; the row is %s",
                    dataset,
                    row.get("error"),
                    ticker,
                    row.get("message"),
                    fate,
                )

    def _public(self, error: WireError) -> MassiveError:
        """Return the public error of a wire error.

        A closed session's error is `MassiveError("MassiveSession #1 is closed")`, whatever
        the request returned. A pause that outlasts the read's patience is
        `MassiveError("rate limited: ...")` with status 429: the governor's message (which
        requests, the latest refusal, when its next request (a probe) goes, the read's
        patience, the usual cause, and the remedies), then where to watch the subscriptions'
        state. Every other error is `_errors.from_wire`'s.

        Args:
            error: The wire error.

        Returns:
            The public error, with the wire error's status and request id.
        """
        if self.state.closed:
            return MassiveError(f"{self.state.label} is closed")
        if error.status == HTTPStatus.TOO_MANY_REQUESTS:
            return MassiveError(
                f"rate limited: {error.message}. m.rate_limits() shows each subscription's state",
                status=429,
                request_id=error.request_id,
            )
        return from_wire(error)

    def rows(
        self, template: str, query: Mapping[str, object], *, bucket: str, cancel: Cancel
    ) -> list[dict[str, object]]:
        """Return every raw JSON row of a REST path for a query, following its pages.

        For the library's own lookups, such as the futures schedule calendar of the WebSocket
        streams (`_ingest/futures_calendar.py`), which reads a few fields and keeps no table.

        Args:
            template: The path template, such as `"/futures/v1/schedules"`; it takes no path
                parameter.
            query: The query parameters, by wire name.
            bucket: The rate governor bucket every page is charged to, such as `"futures"`.
            cancel: The lookup's token: its lane in the governor, and, once set, the end of
                every page's wait for a slot.

        Returns:
            The rows of every page, every key kept.

        A failed request raises the public error of its `WireError` (`_public`):
        `AuthenticationError`, `EntitlementError`, or `MassiveError`.
        """
        rest_path = self.rest_path(template)
        patience = self.patience()
        try:
            return [
                row
                for page in self.state.client.pages(
                    rest_path, {}, query, bucket=bucket, cancel=cancel, patience=patience
                )
                for row in page.rows
            ]
        except WireError as error:
            raise self._public(error) from error

    def probe(
        self, probe: host_decision.SnapshotProbe, *, asset_class: AssetClass
    ) -> list[dict[str, object]]:
        """Make a host-decision request and return its raw JSON rows.

        Args:
            probe: The request.
            asset_class: The class whose host is decided; the request is charged to its
                subscription, whatever path it reads.

        Returns:
            The rows, every key kept.

        A failed request raises the public error of its `WireError` (`_public`):
        `AuthenticationError`, `EntitlementError`, or `MassiveError`.
        """
        patience = self.patience()
        try:
            page = self.state.client.get(
                self.rest_path(probe.path),
                probe.path_params,
                probe.query,
                bucket=bucket_of(subscription_of(asset_class)),
                patience=patience,
            )
        except WireError as error:
            raise self._public(error) from error
        return page.rows

    def read_cycle(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        symbols: NormalizedSymbols | None,
        record: Callable[[DriftReport], None],
        executor: ThreadPoolExecutor,
        cancel: Cancel,
        pace: PaceCheck,
    ) -> tuple[pa.Table, int]:
        """Run one `poll()` cycle: the planned requests of `read_fetch` for the cycle's symbols.

        A reactive symbol table can route a new symbol to a path that lacks a parameter the
        caller set (an explicit `adjusted` on the crypto indicators). Retrying cannot fix that,
        so its `ValueError` becomes `UnsupportedParameterError`, which fails the table. Before
        reading, the cycle's requests per bucket go to `pace`, which warns once when the paced
        ones outlast the interval.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            symbols: The cycle's static symbols (a reactive table already read).
            record: Records each page's drift in the table's `SchemaDrift`.
            executor: The poller's fan-out pool.
            cancel: The poller's stop token, which ends the cycle's waits.
            pace: The table's pace check.

        Returns:
            The cycle's rows and request count.

        Raises:
            UnsupportedParameterError: If a path of the cycle does not take a parameter the
                caller set.

        Any other error is that of `read_fetch`.
        """
        try:
            planned = self.plan_fetch(spec, request, symbols)
        except ValueError as error:
            raise UnsupportedParameterError(str(error)) from error
        pace.check(self.state.client.governor, Counter(_buckets(spec, planned)))
        return self._read_planned(
            spec, request, planned, record=record, executor=executor, cancel=cancel
        )

    def check_parameters(self, spec: DatasetSpec, request: Request) -> None:
        """Check, before any symbol is known, that some fetch path can take the parameters.

        A `poll()` over a reactive symbol table cannot plan its paths until it reads the
        table, but a parameter that none of the dataset's fetch paths takes (`fetch_path`,
        `detail_path`, and the per-class paths) can never succeed, so it fails at once.

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments.

        When every fetch path refuses a parameter, the first path's `ValueError` (from
        `shared_params` or `axis_query`) is raised.
        """
        candidates = (spec.fetch_path, spec.detail_path, *spec.fetch_paths_by_class.values())
        errors: list[ValueError] = []
        templates = [template for template in candidates if template is not None]
        for template in templates:
            rest_path = self.rest_path(template)
            try:
                shared_params(spec, rest_path, request, fixed={})
                axis_query(
                    spec,
                    rest_path,
                    dates=(request.start, request.end),
                    asset_class=spec.asset_class,
                )
            except ValueError as error:
                errors.append(error)
        if len(errors) == len(templates):
            raise errors[0]

    def plan_fetch(
        self, spec: DatasetSpec, request: Request, symbols: NormalizedSymbols | None
    ) -> list[tuple[RestPath, list[Call]]]:
        """Plan the requests of a `fetch()` or `poll()` cycle, path by path.

        Makes no request, except that the first crypto pair a path takes as two currencies
        reads the session's crypto ticker list (`crypto_pair`).

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments.
            symbols: The static symbols.

        Returns:
            Each path the cycle reads (`fetch_paths`) with its calls; none for an empty symbol
            tuple (a reactive table with no valid row), which reads nothing.

        A parameter a path does not take raises the `ValueError` of `shared_params`; a crypto pair
        that cannot be split raises the `MassiveError` of `crypto_pair`.
        """
        if symbols == ():
            return []  # an emptied symbol table asks for nothing, whatever the paths take
        planned = []
        for template, own in fetch_paths(spec, symbols):
            rest_path = self.rest_path(template)
            calls = plan_calls(
                spec,
                rest_path,
                request,
                symbols=own,
                dates=(request.start, request.end),
                split=self.crypto_pair,
            )
            planned.append((rest_path, calls))
        return planned

    def read_fetch(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        symbols: NormalizedSymbols | None,
        record: Callable[[DriftReport], None] | None = None,
        executor: ThreadPoolExecutor | None = None,
        pauses_live_tables: bool = False,
    ) -> tuple[pa.Table, int]:
        """Read a dataset's fetch paths for the symbols: `fetch()`.

        Once planned, the read logs at INFO the least time its paced requests take, when they
        cannot all start within a minute (`rate_limits.announce`); at WARNING when the read
        holds the update graph's lock.

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments.
            symbols: The static symbols.
            record: Receives each page's drift, or None.
            executor: A pool for fan-out requests, or None for one of each read's own.
            pauses_live_tables: True when the calling thread holds the update graph's lock (a
                console command), so the read pauses every live table of the worker while it
                runs.

        Returns:
            The rows of every path, in path order, capped at `Request.limit`, and the request
            count.

        Errors are those of `read`, and a parameter a path does not take raises the
        `ValueError` of `shared_params`.
        """
        planned = self.plan_fetch(spec, request, symbols)
        announce(
            self.state.client.governor,
            f"{spec.name} fetch",
            _buckets(spec, planned),
            pauses_live_tables=pauses_live_tables,
        )
        return self._read_planned(
            spec, request, planned, record=record, executor=executor, cancel=None
        )

    def _read_planned(
        self,
        spec: DatasetSpec,
        request: Request,
        planned: Sequence[tuple[RestPath, list[Call]]],
        *,
        record: Callable[[DriftReport], None] | None,
        executor: ThreadPoolExecutor | None,
        cancel: Cancel | None,
    ) -> tuple[pa.Table, int]:
        """Make planned calls, path by path, and cap the total rows at `Request.limit`.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            planned: Each path with its calls (`plan_fetch`); empty for no symbols.
            record: Receives each page's drift, or None.
            executor: A pool for fan-out requests, or None for one of each read's own.
            cancel: Ends the reads' waits when set, or None.

        Returns:
            The rows of every path, in path order, and the request count.

        Errors are those of `read`.
        """
        tables = []
        requests = 0
        patience = self.patience()  # once for every path, on this thread, before any fan-out
        for rest_path, calls in planned:
            table, made = self.read(
                spec,
                rest_path,
                calls,
                request=request,
                limit=request.limit,
                remedy=_fetch_remedy(spec),
                record=record,
                executor=executor,
                cancel=cancel,
                patience=patience,
            )
            tables.append(table)
            requests += made
        if not tables:
            schema = self.catalog.dataset_schema(
                spec, raw_symbol=self.unknown_symbols is UnknownSymbols.WARN
            )
            return schema.arrow_schema().empty_table(), 0
        rows = pa.concat_tables(tables)
        if request.limit is not None:
            rows = rows.slice(0, request.limit)
        return rows, requests


class RestEndpoints:
    """The session core's REST transport: `fetch`, `poll`, REST `history`, and host decisions.

    `SessionClients.rest()` makes one per call, bound to the session's core and its shared
    `RestState`, so it may hold the core strongly. Every method runs on the calling thread
    (inside the session's execution context for the Deephaven calls) and holds no library lock
    while it waits on the network.
    """

    __slots__ = ("_core", "_state")

    def __init__(self, core: SessionCore, state: RestState) -> None:
        """Bind the transport to the session's core and REST state.

        Args:
            core: The session's core: configuration, catalog, drift registry, diagnostics.
            state: The session's REST resources.
        """
        self._core = core
        self._state = state

    def _reader(self, role: str) -> RestReader:
        """Return a reader of the session's paths.

        Args:
            role: What the reader serves, such as `"call-7"` or `"hosts"`: its fan-out
                threads are named `"<session prefix> <role> rest_<n>"`.

        Returns:
            The reader.
        """
        core = self._core
        return RestReader(
            self._state,
            core.catalog,
            core.drift,
            core.config.max_concurrency,
            core.config.unknown_symbols,
            f"{core.thread_prefix} {role} rest",
            core.exec_ctx,
        )

    def _static(self, spec: DatasetSpec, table: pa.Table) -> Table:
        """Convert rows to a static Deephaven table, inside the session's execution context.

        Every calendar-date column stores one shared string per distinct date
        (`columns.dataset_table`).

        Args:
            spec: The dataset, whose schema names its date columns.
            table: The rows.

        Returns:
            The static table, with every column of the schema even without rows.
        """
        dates = self._core.catalog.dataset_schema(spec).date_columns
        with entered(self._core.exec_ctx):
            return dataset_table(table, dates)

    def resolve_host(self, asset_class: AssetClass, family: ChannelFamily) -> HostDecision:
        """Decide the WebSocket host of a (class, family), once per session, single-flight.

        The first caller for a pair decides (`host_decision.decide`, at most one REST request) and
        records the decision in the pair's `Transport=REST` row of `m.connections()`; callers
        that arrive meanwhile wait for that decision, outside every lock, and later callers
        get it at once. A failed decision is not cached: its row shows `FAILED` and the error,
        every waiting caller raises the same error, and the next call tries again.

        Args:
            asset_class: The asset class of the stream.
            family: The channel family.

        Returns:
            The decision.

        Errors: `EntitlementError` when the plan has no WebSocket access for the class,
        `AuthenticationError` for a rejected key, and `MassiveError` for an undocumented
        timeframe or a failed request; each is logged at ERROR by the deciding caller.
        """
        key = (asset_class, family)
        future, owner = self._state.hosts.host_future(key)
        if not owner:
            return _waited(future)
        try:
            decision = host_decision.decide(
                asset_class,
                family,
                ws_hosts=self._core.config.ws_hosts,
                fetch=functools.partial(self._reader("hosts").probe, asset_class=asset_class),
            )
        except BaseException as error:
            self._state.hosts.forget_host(key, future)
            failure = (
                error
                if isinstance(error, MassiveError)
                else MassiveError(
                    f"deciding the {asset_class.value} WebSocket host failed: {error!r}"
                )
            )
            future.set_exception(failure)
            _report_host_failure(failure)
            self._core.diagnostics.write(
                ConnectionRow(
                    transport=Transport.REST,
                    asset_class=asset_class,
                    family=family,
                    path=f"/{asset_class.value}",
                    state=ConnectionState.FAILED,
                    updated_at=time.time_ns(),
                    last_error=str(failure),
                )
            )
            raise
        # Completed first, so no later step can leave the waiters of this pair blocked.
        future.set_result(decision)
        self._write_decision(asset_class, family, decision)
        return decision

    def fetch_rows(
        self, template: str, query: Mapping[str, object], *, bucket: str, cancel: Cancel
    ) -> list[dict[str, object]]:
        """Return every raw JSON row of a REST path for a query (`RestReader.rows`).

        Runs on the calling thread, here the WebSocket transport's futures schedule worker,
        and holds no library lock while it waits on the network or the rate governor.

        Args:
            template: The path template; it takes no path parameter.
            query: The query parameters, by wire name.
            bucket: The rate governor bucket the requests are charged to.
            cancel: The lookup's token: its lane, and the end of its waits once set.

        Returns:
            The rows of every page.

        A failed request raises the public error of `RestReader.rows`.
        """
        return self._reader("schedules").rows(template, query, bucket=bucket, cancel=cancel)

    def crypto_pair(self, symbol: str) -> tuple[str, str]:
        """Return a crypto ticker's base and quote, from Massive's reference data if needed.

        The WebSocket transport calls it, before a stream starts or on its lookup worker, for a
        crypto ticker (`RestReader.crypto_pair`), from the session's crypto ticker list, read
        once.

        Args:
            symbol: A canonical crypto ticker.

        Returns:
            `(base, quote)`.

        A failed or empty lookup raises the `MassiveError` of `RestReader.crypto_pair`.
        """
        return self._reader("crypto pairs").crypto_pair(symbol)

    def record_verdict(
        self, asset_class: AssetClass, family: ChannelFamily, decision: HostDecision
    ) -> None:
        """Replace the cached host decision of a (class, family) with a server's verdict.

        The WebSocket transport calls it when the explicit-verdict rule re-routes a
        connection. Every later `resolve_host` for the pair returns `decision` without a
        request, and the pair's `Transport=REST` row of `m.connections()` is rewritten with
        its host, recency, and evidence. Thread-safe: the cache changes under its leaf lock,
        and the row is only queued for the diagnostics writer; nothing blocks.

        Args:
            asset_class: The asset class of the connection.
            family: The channel family.
            decision: The new decision, such as the delayed host with `Recency.DELAYED` and
                evidence `"server verdict: <message>"`.
        """
        self._state.hosts.replace_host((asset_class, family), decision)
        logger.warning(
            "%s %s WebSocket host is now %s (%s)",
            asset_class.value,
            family.value,
            decision.host,
            decision.evidence,
        )
        self._write_decision(asset_class, family, decision)

    def _write_decision(
        self, asset_class: AssetClass, family: ChannelFamily, decision: HostDecision
    ) -> None:
        """Queue the pair's `Transport=REST` row of `m.connections()` for a decision.

        Args:
            asset_class: The asset class.
            family: The channel family.
            decision: The decision the row shows.
        """
        self._core.diagnostics.write(
            ConnectionRow(
                transport=Transport.REST,
                asset_class=asset_class,
                family=family,
                host=decision.host,
                path=f"/{asset_class.value}",
                recency=decision.recency,
                evidence=decision.evidence,
                state=ConnectionState.CONNECTED,
                updated_at=time.time_ns(),
            )
        )

    def fetch(self, spec: DatasetSpec, request: Request, *, call: int) -> Table:
        """Return every row the dataset's fetch path returns for the request, as a static table.

        Requests run on this thread, or for a per-symbol path on a pool of up to
        `max_concurrency` threads; `Request.limit` caps the total rows. Drift is logged, once
        per field.

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments, with static symbols (the query reads a reactive
                table before this call).
            call: The call's id (`SessionCore.next_stream_id`), which names its fan-out
                threads (`"massive #1 call-7 rest_0"`).

        Returns:
            A static table in the dataset's schema.

        Errors, which the verb's call logs at ERROR: `AuthenticationError` (HTTP 401),
        `EntitlementError` (HTTP 403 or `NOT_AUTHORIZED`), `RowDataError` for a row that breaks
        the schema, and `MassiveError` itself for any other failure (a rate limit as `"rate
        limited: ..."`, a closed session as `"... is closed"`).

        A parameter a path does not take raises the `ValueError` of `shared_params` before any
        request: every path is planned before the first one is read.
        """
        reader = self._reader(f"call-{call}")
        table, _ = reader.read_fetch(
            spec,
            request,
            symbols=request.symbols,
            pauses_live_tables=holds_update_graph_lock(self._core.exec_ctx),
        )
        return self._static(spec, table)

    def history(
        self, spec: DatasetSpec, request: Request, window: HistoryWindow, *, call: int
    ) -> Table:
        """Return the dataset's REST history over `window`, paged internally, as a static table.

        `dm.ALL` reads the grouped-daily path, one request per trading day of the window
        (`dates.is_trading_day`: weekdays, plus Sundays for forex and every day for crypto). Futures
        trades and quotes named by session dates read one request series per weekday session
        and symbol (`session_end_date=<date>`), sessions in date order and the symbols of one
        session in symbol order; Saturdays and Sundays hold no session and are skipped. Every
        other request covers the window's instants (`HistoryWindow.requested`): on the
        dataset's time axis, or as the aggregates' `{from}/{to}` millisecond epochs, one
        request series per symbol where the path takes one. Rows outside the window are
        dropped (`keep_window`), bars of a day or longer are stamped at 00:00 of their day in the
        class day zone (`stamped`, `row_times.day_starts`), and every row gets the library's
        time columns. A symbol with no data adds no rows. Before the first request, one INFO
        line announces a long download (`rate_limits.announce`): a grouped-daily or
        per-session download of more than five requests, and any read whose paced requests
        cannot all start within a minute, with the least time it takes. Logs
        `source=REST requests=<n>` at INFO.

        Args:
            spec: The dataset; `history()` has checked that REST can serve the request.
            request: The normalized arguments, with static symbols.
            window: The call's window (`history_window` in `_datasets/history_source.py`).
            call: The call's id, which names its fan-out threads, as in `fetch`.

        Returns:
            A static table in the dataset's schema.

        Errors: as `fetch` raises them, and `MassiveError` for a bar of a day or longer stamped
        at a time or on a day Massive is not known to use for its class, for a page whose
        `adjusted` echo differs from the request, and for a symbol whose later page answers
        HTTP 404, its data cut short.
        """
        reader = self._reader(f"call-{call}")
        if isinstance(request.symbols, _All):
            rest_path = reader.rest_path(cast("str", spec.grouped_daily_path))
            path, query = shared_params(spec, rest_path, request, fixed={})
            # A class that does not trade at weekends has no grouped day on a Saturday or Sunday.
            days = [
                day
                for day in partition_dates(spec, window)
                if is_trading_day(spec.asset_class, day)
            ]
            calls = [Call({**path, "date": day}, query) for day in days]
            shape = ReadShape.PER_DAY
        else:
            rest_path = reader.rest_path(cast("str", spec.history_path))
            calls = history_calls(spec, rest_path, request, window=window, split=reader.crypto_pair)
            shape = ReadShape.PER_SESSION if window.requested is None else ReadShape.PAGED
        announce(
            self._state.client.governor,
            f"{spec.name} history",
            [call_bucket(spec, rest_path.template, call) for call in calls],
            shape=shape,
            pauses_live_tables=holds_update_graph_lock(self._core.exec_ctx),
        )
        table, requests = reader.read(
            spec,
            rest_path,
            calls,
            request=request,
            limit=None,
            window=window,
            remedy=_history_remedy(request),
            patience=reader.patience(),
        )
        logger.info("%s history: source=REST requests=%d", spec.name, requests)
        return self._static(spec, table)

    def poll(
        self, spec: DatasetSpec, request: Request, *, interval: timedelta, mode: PollMode
    ) -> Table:
        """Return a live table that re-reads the dataset's fetch path every `interval`.

        See `_ingest/poll.py`: the table is created here, on the caller thread, and one poller
        thread of its own runs the cycles until the table is released or the session closes.

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments; a reactive symbol table is read each cycle.
            interval: The time from one cycle's start to the next.
            mode: How cycles combine.

        Returns:
            The live table, tracked by the session.

        A parameter the caller set that a path of the call does not take raises `ValueError`
        before the table exists. With static symbols every path is planned now; with a reactive
        table, whose symbols choose the paths at each cycle, only a parameter that no fetch path
        takes fails here (`RestReader.check_parameters`), and one the cycle's path lacks fails
        the table then (`UnsupportedParameterError`).
        """
        reader = self._reader("poll")
        if isinstance(request.symbols, SymbolTable):
            reader.check_parameters(spec, request)
            # The table's symbols choose among every fetch path at each cycle.
            candidates = (spec.fetch_path, spec.detail_path, *spec.fetch_paths_by_class.values())
            used = [path for path in candidates if path is not None]
        else:
            used = [path.template for path, _ in reader.plan_fetch(spec, request, request.symbols)]
        host = urlsplit(self._core.config.rest_base_url).hostname
        paths = ", ".join(dict.fromkeys(used))
        return poll.open_poll(
            self._core,
            spec=spec,
            request=request,
            interval=interval,
            mode=mode,
            read=functools.partial(
                reader.read_cycle,
                spec,
                request,
                pace=PaceCheck(f"{spec.name} poll()", interval.total_seconds()),
            ),
            pollers=self._state.pollers,
            host=host,
            path=paths,
            clock=poll.CLOCK,
        )


_REFERENCE_PATH: Final = re.compile(r"/v\w+/reference/")
"""The start of every reference endpoint's path (`/v3/reference/...`, `/vX/reference/...`)."""


def call_bucket(spec: DatasetSpec, template: str, call: Call) -> str:
    """Return the governor bucket a planned request is charged to: its Massive subscription.

    Massive counts reference endpoints in a bucket of their own, so a request of any reference
    path (`/v3/reference/...`, `/vX/reference/...`) goes to `rate_limits.OTHER`, whatever its
    dataset's class and filters: options contracts and IPOs as well as the ticker list, and
    `m.reference.tickers(market=dm.Market.CRYPTO)` as well as the session's own read of the
    crypto ticker list. Any other request reads the data of the class `dates.day_class` gives
    it (the dataset's class, or for a dataset with a path per symbol class the class its
    symbols share), and that class's subscription covers it; one of no class goes to `OTHER`.

    Args:
        spec: The dataset.
        template: The request's path template, such as `"/v3/reference/options/contracts"`.
        call: The planned request.

    Returns:
        The bucket's name, such as `"stocks"`, `"currencies"`, or `"other"`.
    """
    return _charge(spec, template, call)[0]


def _request_bucket(spec: DatasetSpec, template: str, call: Call) -> str:
    """Return the bucket a request about to be made is charged to, logging it and why.

    Called once per request, so the log has one line for each; the planning that announces a
    read's buckets beforehand uses `call_bucket`, which logs nothing.

    Args:
        spec: The dataset.
        template: The request's path template.
        call: The planned request.

    Returns:
        The bucket's name (`call_bucket`).
    """
    bucket, reason = _charge(spec, template, call)
    logger.debug(
        "%s %s (symbols: %s) is charged to the %s bucket: %s",
        spec.name,
        template,
        ", ".join(own_symbols(spec, call.symbol, call.query)) or "none",
        bucket,
        reason,
    )
    return bucket


def _charge(spec: DatasetSpec, template: str, call: Call) -> tuple[str, str]:
    """Return the bucket a planned request is charged to (`call_bucket`) and why.

    Args:
        spec: The dataset.
        template: The request's path template.
        call: The planned request.

    Returns:
        The bucket's name and the reason, `"a reference endpoint"` or `"its data's class"`,
        which `_request_bucket` logs once per request.
    """
    if _REFERENCE_PATH.match(template):
        return OTHER, "a reference endpoint"
    classes = map(symbol_class, own_symbols(spec, call.symbol, call.query))
    return bucket_of(subscription_of(day_class(spec, classes))), "its data's class"


def _buckets(spec: DatasetSpec, planned: Sequence[tuple[RestPath, list[Call]]]) -> list[str]:
    """Return the bucket of every planned request, path by path.

    Args:
        spec: The dataset.
        planned: Each path with its calls.

    Returns:
        One bucket per call.
    """
    return [call_bucket(spec, path.template, call) for path, calls in planned for call in calls]


def _report_host_failure(error: MassiveError) -> None:
    """Log a failed host decision at ERROR with exactly the text its exception carries.

    No verb call owns a host decision (every caller waiting for it shares the failure), so the
    record holds the message alone, which names the asset class, the request, and its status.

    Args:
        error: The failure.
    """
    logger.error("%s", error)


def _waited(future: Future[HostDecision]) -> HostDecision:
    """Wait for another caller's host decision.

    Args:
        future: The decision's future.

    Returns:
        The decision.

    A failed decision raises a copy of the deciding caller's `MassiveError` (its type, text,
    status, and request id), so that no exception object is raised on two threads at once.
    """
    try:
        return future.result()
    except MassiveError as error:
        raise type(error)(str(error), status=error.status, request_id=error.request_id) from None


_FLAT_FILES_REMEDY: Final = (
    "read the 1-day flat files with source=dm.HistorySource.FLAT_FILES if S3 credentials are "
    "configured"
)
"""What a `history()` caller of unadjusted 1-day bars can do when a REST stamp is refused."""

_UNADJUSTED_REMEDY: Final = (
    "read the 1-day flat files with source=dm.HistorySource.FLAT_FILES and adjusted=False "
    "(as-traded prices; the files hold no adjusted bars) if S3 credentials are configured"
)
"""What a `history()` caller of adjusted 1-day bars can do when a REST stamp is refused: the
flat files serve only unadjusted bars, so the way out names the price basis it changes."""

_LONGER_REMEDY: Final = "request 1-day bars; no other source serves these"
"""What a `history()` caller of longer or multiplied bars can do: only REST serves them."""

_FETCH_REMEDY: Final = "use history() for that day"
"""What a `fetch()` or `poll()` caller can do when `/prev`'s stamp is refused."""

_INDICATOR_REMEDY: Final = "compute the indicator from bars().history() of the same timespan"
"""What an indicator caller can do when a value's stamp is refused: no other source serves the
values, but the bars they are computed over are checked and labelled the same way."""


def _fetch_remedy(spec: DatasetSpec) -> str:
    """Return what a `fetch()` or `poll()` caller can do when a stamp is refused.

    Args:
        spec: The dataset.

    Returns:
        Computing the indicator from bars for an indicator; reading `history()` for the day
        for `/prev`'s bars.
    """
    if not spec.is_bars and spec.times is RowTimes.BAR:
        return _INDICATOR_REMEDY
    return _FETCH_REMEDY


def _history_remedy(request: Request) -> str:
    """Return what a REST `history()` caller can do when a bar's stamp is refused.

    Args:
        request: The normalized arguments.

    Returns:
        For 1-day bars, reading the 1-day flat files, which for adjusted bars (`adjusted`
        True) means passing adjusted=False as well; for longer or multiplied bars, asking for
        1-day bars, since no flat file holds them.
    """
    if request.timespan is Timespan.DAY and request.multiplier == 1:
        return _UNADJUSTED_REMEDY if request.adjusted is True else _FLAT_FILES_REMEDY
    return _LONGER_REMEDY
