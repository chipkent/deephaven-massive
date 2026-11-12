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
  size (`DatasetSpec.page_size`, else the path's documented maximum `limit`, or `Request.limit`
  when that is smaller, so a small cap fetches no more than it keeps), and for `history()` the
  ascending direction (`_ascending`).
  `Request.limit` caps the total rows, and the client stops paging there; on a dataset that
  sends one symbol per request (a per-symbol path such as Benzinga's consensus ratings, or an
  indicator, or the news of one ticker) it caps each symbol's rows instead.
- **Listed tickers.** A dataset whose rows list their tickers (`DatasetSpec.ticker_list`: news,
  financial statements, insider and 8-K filings) gets its `Symbol` from the request: each
  ticker the request named that a row lists, the row repeated once per such ticker
  (`_listed_rows`), so the split comes after `limit`, which caps what Massive returns.
- **History.** `history()` requests the instants its window covers
  (`HistoryWindow.requested`) and keeps the rows the window keeps (`keep_window`). Futures trades
  and quotes named by session dates are requested one session at a time
  (`session_end_date=<date>`, weekdays only); `dm.ALL` daily bars one grouped day at a time
  (weekdays only for the classes that do not trade at weekends). Bars of a day or longer
  are stamped at 00:00 of their day in the class day zone, and every bar is served: a stamp
  Massive is not known to use is kept as sent and logs one WARNING per form of stamp per
  session (`row_times.stamp_rule`, `StampRule.stamp`).
- **Time columns.** Every page's rows get the library's time columns (`row_times.TimeRule`):
  `Date`, a bar's `TimestampStart` and `TimestampEnd`, and `TimestampInsert`, the instant the
  page arrived. The day zone of each call follows its own symbols' class (`dates.day_class`),
  so a crypto indicator gets UTC days and a stock indicator New York days.
- **Adjustment.** Bars requested with an explicit `adjusted` must come back on that basis: each
  page's `adjusted` echo (`Page.adjusted`) that differs from the request raises
  `MassiveError`, and a page without one logs a WARNING once per path and session, so one
  call never silently mixes adjusted and unadjusted prices.
- **Recency.** A `fetch()` or `poll()` of a dataset of current data
  (`DatasetSpec.serves_current_data`: snapshots, the last trade and quote, movers, the option
  chain) fails on each page charged to a subscription not in
  `MassiveConfig.delayed_subscriptions` that Massive labels `DELAYED` (`recency_labels`): the
  envelope status or a row's `timeframe`. An indicator's latest value is computed from
  current bars (`DatasetSpec.latest_is_current`), so its `poll()`, and a `fetch()` whose
  window reaches the present (`dates.reaches_now`), are checked the same way. Datasets of past
  data (the previous day's bar, dividends, an indicator over a past range), which Massive
  labels `DELAYED` on such plans though their rows are final, reference data, a listed
  subscription's pages, and `history()` are not checked.
- **Fan-out.** A path that takes one symbol a request is called once per symbol, at most
  `MassiveConfig.max_concurrency` at a time. A symbol with no data returns no rows: HTTP 404
  on its first request drops that symbol with one WARNING. HTTP 404 on a later page of a
  symbol's pages means its data was cut short and raises `CutShortError`. Any other error
  fails the call, and every request of the read still waiting for the rate governor stops at
  once (each read has a `Cancel` token of its own, set when a fan-out ends, and a `poll()`
  cycle's token follows its poller's). The token is also the read's lane in the governor:
  the reads, poll cycles, and host decisions waiting on a bucket take turns, whatever their
  number of threads. A read by symbol (`RestReader.read_each`, for a table that follows a
  live symbol table) instead returns each request's failure, so one symbol's failure never
  ends another's read; only a rejected API key ends it (`_ingest/symbol_reads.py`).
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
  with status 429 (the first 429 of each subscription has already logged its INFO notice). One
  odd row never fails a read: a value that does not fit is null and a row whose key cannot be
  read is dropped, both reported as drift; a page whose every row was dropped raises
  `RowDataError`, a defect of the library. Static verbs raise each error, and log it at DEBUG,
  or at ERROR for a defect.
- **Host decisions.** `resolve_host` decides each WebSocket host once per session
  (`_ingest/host_decision.py`), single-flight: the first caller of a (class, family) makes the one
  REST request and every concurrent caller waits for its answer.
- **Shared lookups.** The host decisions and the session's crypto ticker list
  (`_ingest/crypto_tickers.py`) are each made by one caller while the others wait
  (`SharedLookup.claim_or_wait`). A waiting caller charges the lookup's pauses to its own
  patience and gives up when they outlast it; if the caller making the lookup gives up or is
  interrupted, each waiting caller tries again with what it has left.

`RestState` is the part a session shares (the client, the host decisions, the live pollers,
the once-per-session warnings); `SessionClients` creates it on first use and registers its
close with the session. `RestEndpoints`, the session core's `RestTransport`, is a lightweight
view of the core and that state, made for each call. `poll()` itself is `_tables/poll.py`.
The requests of a call are planned by `rest_requests.py`, and its pages become batches in
`rest_rows.py`; this module runs the plan, page by page, and turns its failures into the
public errors.

The module is one subject although it runs past 1,000 lines: it is one transport, and the
reads, the fan-out, the host decisions, and the failures all act on the same session state
(`RestState`) and the same read (`RestReader`).
"""

from __future__ import annotations

import functools
import logging
import threading
import time
from collections import Counter
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from typing import TYPE_CHECKING, Final, TypeVar, cast
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.catalog import REFERENCE_PATH
from deephaven_massive._datasets.dates import day_class, is_trading_day, reaches_now
from deephaven_massive._datasets.history_source import partition_dates, session_date_param
from deephaven_massive._datasets.row_times import time_rule
from deephaven_massive._datasets.symbols import AllSymbols, SymbolTable, symbol_class
from deephaven_massive._datasets.wire_symbols import contracts_underlying
from deephaven_massive._enums import AssetClass, OnError, Recency, Subscription, subscription_of
from deephaven_massive._errors import (
    AuthenticationError,
    CutShortError,
    EntitlementError,
    ErrorCause,
    MassiveError,
    PermanentError,
    UnlistedPairError,
    failure_text,
    from_wire,
    library_bug,
)
from deephaven_massive._ingest import host_decision
from deephaven_massive._ingest.crypto_tickers import CryptoTickerList, recording_split
from deephaven_massive._ingest.rate_limits import (
    OTHER,
    PaceCheck,
    ReadShape,
    announce,
    bucket_of,
    read_patience,
)
from deephaven_massive._ingest.recency_labels import DELAYED as DELAYED_LABEL
from deephaven_massive._ingest.recency_labels import delayed_refusal, read_labels
from deephaven_massive._ingest.rest_requests import (
    BARE_INDEX_PARAM,
    Call,
    CryptoSplit,
    axis_query,
    fetch_paths,
    history_calls,
    own_symbols,
    plan_calls,
    shared_params,
)
from deephaven_massive._ingest.rest_rows import (
    CFI,
    UNDERLYING,
    RowConverter,
    keep_window,
    whole_bars,
)
from deephaven_massive._ingest.shared_lookup import Claim, SharedLookup
from deephaven_massive._ingest.symbol_reads import (
    RestUnit,
    fetch_units,
    history_units,
    read_rest_by_symbol,
    read_rest_by_value,
)
from deephaven_massive._log_limits import bounded_list, log_once
from deephaven_massive._massive import Cancel, DriftReport, Transport, WireError
from deephaven_massive._tables import poll
from deephaven_massive._tables.columns import dataset_table, rest_recency_note
from deephaven_massive._tables.diagnostics import ConnectionRow, ConnectionState
from deephaven_massive._tables.execution_context import entered, holds_update_graph_lock
from deephaven_massive._tables.poll import Pollers

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from collections.abc import Set as AbstractSet
    from datetime import date, timedelta

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._enums import ChannelFamily, PollMode
    from deephaven_massive._log_limits import BoundedLog
    from deephaven_massive._massive import Page, Patience, RestClient, RestPath
    from deephaven_massive._tables.diagnostics import DriftRegistry
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._tables.transports import HostDecision, SymbolOutcome

__all__ = [
    "RestEndpoints",
    "RestReader",
    "RestState",
    "call_bucket",
]

logger = logging.getLogger(__name__)

_Item = TypeVar("_Item")
"""What one task of a fan-out is given: a planned call, or a path and a call."""
_Done = TypeVar("_Done")
"""What one task of a fan-out returns."""

_SYMBOL: Final = "Symbol"
"""The column of a row's instrument, which `_listed_rows` fills on a dataset with a ticker list."""

_FACTS_LIMIT: Final = 10_000
"""The most facts a session remembers having warned about (`RestState.first`); past it, it
forgets them all and starts over, so a long `poll()` over many symbols keeps its memory bounded,
at the cost of warning about an old fact once more."""

_ANSWER_SHOWN: Final = 200
"""The most characters of one error row's answer a WARNING shows (`_cut`); the row's DEBUG
line shows it whole."""


class UnavailableSymbolError(PermanentError):
    """A symbol Massive cannot serve, under `MassiveConfig.unavailable_symbols=RAISE`.

    Massive answered HTTP 404 for it (`ErrorCause.EXPECTED`), or, in a `poll()` cycle, its path
    does not take a parameter the poll sends (`ErrorCause.USER_DEFECT`). Under `LOG` and
    `IGNORE` the read leaves the symbol out instead, and a `poll()` table's `Error` lists it.
    A `fetch()` or `history()` raises it, a followed `fetch()` marks the symbol `FAILED` with
    it, and a `poll()` table fails with it.
    """


def _missing_error(
    spec: DatasetSpec, template: str, missing: Sequence[str]
) -> UnavailableSymbolError:
    """Return the failure of symbols with no data, under `unavailable_symbols=RAISE`.

    Args:
        spec: The dataset.
        template: The path that answered HTTP 404.
        missing: The symbols, in call order.

    Returns:
        The error, naming the symbols, the path, and the setting that would leave them out.
    """
    names = bounded_list(missing)
    logger.debug("%s: no data for %s (HTTP 404 from %s); raising", spec.name, names, template)
    return UnavailableSymbolError(
        f"{spec.name}: no data for {names}: Massive answered HTTP 404 from {template}. "
        "Remove the symbol, or set MassiveConfig(unavailable_symbols=dm.OnError.LOG) to "
        "leave it out and read the others",
        cause=ErrorCause.EXPECTED,
    )


def _bare_index(request: Request, call: Call) -> str | None:
    """Return the index a call asked for by its bare ticker (`SPX` sent for `I:SPX`).

    Args:
        request: The normalized arguments, which keep the canonical ticker.
        call: The planned call, whose query holds the ticker as sent.

    Returns:
        The canonical index when the call sent it without its `I:`; None for any other call.
    """
    asked = dict(request.params).get(BARE_INDEX_PARAM)
    if isinstance(asked, str) and asked != call.query.get(BARE_INDEX_PARAM, asked):
        return asked
    return None


def _listed_rows(
    batch: pa.RecordBatch, named: Sequence[str], *, column: str
) -> tuple[pa.RecordBatch, int]:
    """Fill a page's `Symbol` from the tickers its request named and each row lists.

    A request that named one ticker gives it to every row. A request that named several gives
    each row every one of them its list holds, the row repeated once per such ticker, in the
    order of its list; a row that lists none of them keeps a null `Symbol`, never dropped. A
    request that named none leaves `Symbol` null.

    Args:
        batch: One page's rows in the dataset's schema, with a null `Symbol`.
        named: The tickers the request named (`own_symbols`).
        column: The column of each row's tickers (`DatasetSpec.ticker_list`).

    Returns:
        The rows with their `Symbol`, and how many rows listed none of several named tickers.
    """
    if not named:
        return batch, 0
    if len(named) == 1:
        taken = batch
        symbols: list[str | None] = [named[0]] * batch.num_rows
        unmatched = 0
    else:
        wanted = set(named)
        rows: list[int] = []
        symbols = []
        unmatched = 0
        for index, listed in enumerate(batch.column(column).to_pylist()):
            hits: list[str | None] = [t for t in dict.fromkeys(listed or ()) if t in wanted]
            if not hits:
                unmatched += 1
                hits = [None]
            rows.extend([index] * len(hits))
            symbols.extend(hits)
        taken = batch.take(pa.array(rows, pa.int64()))
    position = taken.schema.get_field_index(_SYMBOL)
    arrays = list(taken.columns)
    arrays[position] = pa.array(symbols, taken.schema.field(position).type)
    return pa.RecordBatch.from_arrays(arrays, schema=taken.schema), unmatched


def _cut(text: str) -> str:
    """Return a text from Massive cut to `_ANSWER_SHOWN` characters, for a WARNING's list.

    Args:
        text: The text.

    Returns:
        The text, or its first characters ending in `…` when it is longer.
    """
    return text if len(text) <= _ANSWER_SHOWN else text[: _ANSWER_SHOWN - 1] + "…"


def _page_rows(
    batches: Sequence[pa.RecordBatch],
    *,
    window: HistoryWindow | None,
    time_name: str,
    fill: Callable[[pa.RecordBatch], pa.RecordBatch],
    where: str,
    read: int,
) -> list[pa.RecordBatch]:
    """Keep a page's rows of the `history()` window, fill their time columns, keep whole bars.

    Logs one DEBUG line per page with the rows left after each step, so an empty table can be
    traced to the step that emptied it.

    Args:
        batches: The page's converted rows, stamped.
        window: The `history()` window, or None for `fetch()` and `poll()`.
        time_name: The dataset's time column, for `keep_window`.
        fill: Fills the library's time columns (`row_times.TimeRule.fill`).
        where: The dataset, page, path, and symbol, for the line.
        read: How many rows the page held, before conversion.

    Returns:
        The rows kept.
    """
    windowed = [keep_window(batch, window, time=time_name) for batch in batches]
    whole = [whole_bars(fill(batch), window) for batch in windowed]
    logger.debug(
        "%s: %d rows read, %d converted, %d in the window, %d kept as whole bars",
        where,
        read,
        sum(batch.num_rows for batch in batches),
        sum(batch.num_rows for batch in windowed),
        sum(batch.num_rows for batch in whole),
    )
    return whole


@dataclass(frozen=True, slots=True)
class _Outcome:
    """What one call returned.

    Attributes:
        batches: Its rows, in the dataset's schema.
        requests: How many requests it made (pages, and a dropped symbol's 404).
        missing: The symbol Massive answered HTTP 404 for, so left out, with the futures
            session it was asked for (`"ESZ6 for the session 2026-10-30"`); None when the call
            was answered.
    """

    batches: tuple[pa.RecordBatch, ...]
    requests: int
    missing: str | None = None


@dataclass(frozen=True, slots=True)
class _PageRecency:
    """What the `timeframe` labels of one page's rows say (`_page_recency`).

    Attributes:
        delayed: Whether a row of a known subscription is labelled delayed, refused or not.
        real_time: Whether a row is labelled real-time.
        refused: The tickers and first evidence of the rows labelled delayed, by their
            subscription, for each subscription neither listed nor `CURRENCIES`: the tickers
            in the order seen, each once, such as `{STOCKS: (("AAPL", "MSFT"),
            "timeframe=DELAYED")}`.
        unknown: Each row's label that is neither `DELAYED` nor `REAL-TIME`, as its row's
            subscription (None when unknown) and the field and value, in row order.
    """

    delayed: bool
    real_time: bool
    refused: Mapping[Subscription, tuple[tuple[str, ...], str]]
    unknown: tuple[tuple[Subscription | None, tuple[str, object]], ...]


def _page_recency(
    rows: Sequence[Mapping[str, object]],
    *,
    fixed: Subscription | None,
    listed: AbstractSet[Subscription],
    symbol_key: str | None,
    symbol: str | None,
) -> _PageRecency:
    """Read the recency labels of a page's rows, and which rows they refuse.

    A row's subscription is `fixed` when the page is charged to one, else its ticker's class's
    (`symbol_class`); a row whose subscription is unknown is never refused. The page is read
    whole, so a refusal can name every refused row's ticker.

    Args:
        rows: The page's decoded rows.
        fixed: The subscription the page is charged to, or None for the `other` bucket.
        listed: The subscriptions whose delayed data is accepted
            (`MassiveConfig.delayed_subscriptions`).
        symbol_key: The wire key of the rows' ticker, or None.
        symbol: The call's one symbol, which stands for its rows when they carry none, or None.

    Returns:
        The page's labels.
    """
    delayed = False
    real_time = False
    refused: dict[Subscription, tuple[dict[str, None], str]] = {}
    unknown: list[tuple[Subscription | None, tuple[str, object]]] = []
    for row in rows:
        labels = read_labels("", [row])
        real_time = real_time or labels.real_time is not None
        if labels.delayed is None and labels.unknown is None:
            continue
        ticker = row.get(symbol_key) if symbol_key is not None else symbol
        subscription = fixed or (
            subscription_of(symbol_class(ticker)) if isinstance(ticker, str) else None
        )
        if labels.unknown is not None:
            unknown.append((subscription, labels.unknown))
        if labels.delayed is None or subscription is None:
            continue
        if subscription is not Subscription.CURRENCIES and subscription not in listed:
            tickers, _ = refused.setdefault(subscription, ({}, labels.delayed))
            if isinstance(ticker, str):
                tickers[ticker] = None
        delayed = True
    return _PageRecency(
        delayed=delayed,
        real_time=real_time,
        refused={key: (tuple(tickers), evidence) for key, (tickers, evidence) in refused.items()},
        unknown=tuple(unknown),
    )


class RestState:
    """The REST resources one session shares: the client, the shared lookups, and the pollers.

    `SessionClients.rest()` creates one per session on first use and registers `close` with the
    session, so the client closes, and every poller stops, when the session closes or is
    collected unclosed. It holds no reference to the session core, so that closer never keeps
    the core alive. Thread-safe: the once-only flags sit behind one leaf lock, held only to read
    or change them; the shared lookups, the client, and the pollers are thread-safe.

    Attributes:
        label: The session's label.
        pollers: The session's live `poll()` tables.
        hosts: The WebSocket host decisions, by (asset class, channel family)
            (`RestEndpoints.resolve_host`).
        crypto: Massive's list of crypto tickers with their two currencies
            (`RestReader.crypto_pair`): read on first need, and read again when a ticker it
            does not hold is asked for and it is an hour old or more.
    """

    __slots__ = ("_client", "_lock", "_once", "crypto", "hosts", "label", "pollers")

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
        self.hosts: Final[SharedLookup[tuple[AssetClass, ChannelFamily], HostDecision]] = (
            SharedLookup(label, client.governor)
        )
        self.crypto: Final = CryptoTickerList(client, label)
        self._once: set[tuple[str, ...]] = set()

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

    def first(self, *key: str) -> bool:
        """Tell whether a fact is seen for the first time in this session, and remember it.

        The memory is bounded: once it holds `_FACTS_LIMIT` facts, a new one empties it first,
        logged at DEBUG, so a fact forgotten then counts as new again.

        Args:
            *key: The fact, such as `("missing", "stocks.prev", "AAPL")`.

        Returns:
            True the first time, and again after the memory was emptied; False otherwise.
        """
        with self._lock:
            if key in self._once:
                return False
            forgot = len(self._once) if len(self._once) >= _FACTS_LIMIT else 0
            if forgot:
                self._once.clear()
            self._once.add(key)
        if forgot:
            logger.debug(
                "%s: forgot the %d facts already warned about, past the limit of %d",
                self.label,
                forgot,
                _FACTS_LIMIT,
            )
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
        unreadable_symbols: The session's setting for a symbol spelling the library cannot
            read (`MassiveConfig.unreadable_symbols`).
        unavailable_symbols: The session's setting for a symbol the library cannot read from
            Massive (`MassiveConfig.unavailable_symbols`), which every read follows.
        unlisted_symbols: The session's setting for a crypto pair that Massive's crypto ticker
            list does not hold (`MassiveConfig.unlisted_symbols`), which a `poll()` cycle
            (`_plan_cycle`) and a `history()` read (`listed_split`) follow.
        delayed_subscriptions: The subscriptions whose delayed data the session accepts
            (`MassiveConfig.delayed_subscriptions`); a current-data read of any other fails on
            a `DELAYED` label (`_check_recency`).
        threads: The name prefix of the threads of a fan-out pool this reader makes, such as
            `"massive #1 call-7 rest"`: the session, the call, and the transport.
        exec_ctx: The session's execution context, which tells whether a read's thread holds
            the update graph's lock, and so how long the read waits out a pause.
        warnings: The session's once-per-session warnings (`SessionCore.warnings`), such as
            that of a bar stamped where Massive is not known to stamp one.
    """

    state: RestState
    catalog: Catalog
    drift: DriftRegistry
    max_concurrency: int
    unreadable_symbols: OnError
    unavailable_symbols: OnError
    unlisted_symbols: OnError
    delayed_subscriptions: AbstractSet[Subscription]
    threads: str
    exec_ctx: ExecutionContext
    warnings: BoundedLog

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
        return self.catalog.rest_path(template)

    def crypto_pair(self, symbol: str, *, cancel: Cancel | None = None) -> tuple[str, str]:
        """Return a crypto ticker's base and quote currencies, from the session's ticker list.

        The session's `CryptoTickerList.pair` (`_ingest/crypto_tickers.py`), read through this
        reader: it may read Massive's whole crypto ticker list, once per session, blocking the
        calling thread meanwhile.

        Args:
            symbol: A canonical crypto ticker, such as `X:DOTUSD`.
            cancel: A background read's stop token, which the list's read listens to; None
                when only the session's close should stop it.

        Returns:
            `(base, quote)`, such as `("DOT", "USD")`.

        Raises:
            UnlistedPairError: If the list does not hold the ticker, or its currencies do not
                join to the ticker.

        A failed read of the list raises its public `MassiveError`, naming the ticker.
        """
        return self.state.crypto.pair(symbol, reader=self, cancel=cancel)

    def read(
        self,
        spec: DatasetSpec,
        rest_path: RestPath,
        calls: Sequence[Call],
        *,
        request: Request,
        limit: int | None,
        window: HistoryWindow | None = None,
        current: bool = False,
        on_recency: Callable[[Recency], None] | None = None,
        record: Callable[[DriftReport], None] | None = None,
        executor: ThreadPoolExecutor | None = None,
        cancel: Cancel | None = None,
        patience: Patience,
        on_missing: Callable[[str, Sequence[str]], None] | None = None,
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
            window: The `history()` window whose rows to keep, or None. Bars of a day or longer
                move to 00:00 of their day in the class day zone, and a stamp Massive is not
                known to use is kept as sent and logs one WARNING per form of stamp per session
                (`row_times.stamp_rule`).
            current: True for a `fetch()` or `poll()` of current data (`_read_planned`), whose
                pages are checked for a `DELAYED` label (`_check_recency`); False, the default,
                for past data, such as `history()` or a previous day's bar.
            on_recency: Given what each page of current data shows about its recency
                (`_check_recency`), for a `poll()` table's `Recency`; None to ignore it.
            record: Also given each page's drift, for a `poll()` table's `SchemaDrift`.
            executor: A pool to run several calls on, which the caller keeps; None for a pool
                of this read's own.
            cancel: Ends the read's waits when set, such as a poller's stop token; None when
                only the session's close should.
            patience: The patience of the whole read (`patience()`, taken once when the read
                began, on its own thread): the most time its requests wait out pauses, shared
                by every path of a read that spans several.
            on_missing: Given the path's template and the symbols of the read that Massive
                answered with HTTP 404 (no data), in call order, when there are any; the
                caller then logs them (a `poll()` cycle, under
                `MassiveConfig.unavailable_symbols`). None to apply that setting here
                (`_report_missing`).

        Returns:
            The rows, in call order and then page order, and the number of requests made.

        Errors: the public error of each `WireError` (`public_error`), `RowDataError` for a page
        whose every row was dropped, and the `MassiveError` of `_call` for a page on another
        adjustment basis, its `CutShortError` for a symbol whose data was cut short, the
        `EntitlementError` of `_check_recency` for delayed data, and, with no `on_missing`, the
        `UnavailableSymbolError` of `_report_missing` for a symbol Massive answered HTTP 404. A
        closed session raises `MassiveError("... is closed")`.
        """
        converter = RowConverter.build(
            self.catalog, spec, rest_path, unreadable_symbols=self.unreadable_symbols
        )
        with Cancel(*(() if cancel is None else (cancel,))) as stop:
            run = functools.partial(
                self._call,
                converter,
                request=request,
                limit=limit,
                window=window,
                current=current,
                on_recency=on_recency,
                record=record,
                spec=spec,
                stop=stop,
                patience=patience,
            )
            if len(calls) <= 1:
                outcomes = [run(call) for call in calls]
            else:
                outcomes = self._fan_out(run, calls, executor=executor, stop=stop)
        missing = [o.missing for o in outcomes if o.missing is not None]
        if missing and on_missing is not None:
            on_missing(rest_path.template, missing)
        elif missing:
            self._report_missing(spec, rest_path.template, missing)
        batches = [batch for outcome in outcomes for batch in outcome.batches]
        table = pa.Table.from_batches(batches, schema=converter.arrow_schema)
        return table, sum(outcome.requests for outcome in outcomes)

    def read_each(
        self,
        spec: DatasetSpec,
        units: Sequence[RestUnit],
        *,
        request: Request,
        limit: int | None,
        window: HistoryWindow | None,
        current: bool,
        cancel: Cancel | None,
        patience: Patience,
    ) -> tuple[list[pa.Table | MassiveError], int]:
        """Make each planned request and return each one's rows or failure, in plan order.

        The read by symbol (`_ingest/symbol_reads.py`): unlike `read`, a request that fails
        leaves the others running, so one symbol's failure never ends another's read. The
        requests may span several paths. They run on this thread when there is one, else on a
        pool of `min(max_concurrency, requests)` threads made for this read, and listen to a
        `Cancel` token of the read's own (following `cancel`), which is also the read's lane in
        the rate governor. Each is made as `read` makes it (`_call`), capped at `limit`. A
        symbol Massive answered HTTP 404 on its first request has no data and follows
        `MassiveConfig.unavailable_symbols`: under `LOG` and `IGNORE` it has no rows and one
        line per path names it (`_report_missing`); under `RAISE` its result is an
        `UnavailableSymbolError`.

        Args:
            spec: The dataset.
            units: The planned requests, each with its path.
            request: The normalized arguments, as `read` takes them.
            limit: The cap on each request's rows: `Request.limit` for `fetch()`, None for
                `history()`, as their static reads cap them.
            window: The `history()` window whose rows to keep, or None for `fetch()`.
            current: True when the pages are checked for a `DELAYED` label (`read`).
            cancel: Ends the read's waits when set, or None.
            patience: The patience of the whole read, shared by every request.

        Returns:
            Each request's rows (an Arrow table of the dataset's schema) or its
            `MassiveError`, in plan order, and the number of requests that were answered.

        Raises:
            AuthenticationError: If Massive rejects the API key on any request: every request
                would be refused, so the requests not yet made are cancelled.

        Any exception that is not a `MassiveError`, a defect, is raised as `read` raises it.
        """
        converters: dict[str, RowConverter] = {}
        for rest_path, _ in units:
            if rest_path.template not in converters:
                converters[rest_path.template] = RowConverter.build(
                    self.catalog, spec, rest_path, unreadable_symbols=self.unreadable_symbols
                )
        with Cancel(*(() if cancel is None else (cancel,))) as stop:
            run = functools.partial(
                self._call_alone,
                converters,
                spec=spec,
                request=request,
                limit=limit,
                window=window,
                current=current,
                stop=stop,
                patience=patience,
            )
            if len(units) <= 1:
                done = [run(unit) for unit in units]
            else:
                done = self._fan_out(run, units, executor=None, stop=stop)
        answered = [
            (rest_path.template, outcome)
            for (rest_path, _), outcome in zip(units, done, strict=True)
            if isinstance(outcome, _Outcome)
        ]
        refused = self.unavailable_symbols is OnError.RAISE
        for template in converters:
            missing = [o.missing for t, o in answered if t == template and o.missing is not None]
            if missing and not refused:
                self._report_missing(spec, template, missing)
        results: list[pa.Table | MassiveError] = []
        for (rest_path, _), outcome in zip(units, done, strict=True):
            if not isinstance(outcome, _Outcome):
                results.append(outcome)
            elif refused and outcome.missing is not None:
                # Under RAISE a symbol with no data fails alone; the others keep their rows.
                results.append(_missing_error(spec, rest_path.template, [outcome.missing]))
            else:
                schema = converters[rest_path.template].arrow_schema
                results.append(pa.Table.from_batches(outcome.batches, schema=schema))
        return results, sum(outcome.requests for _, outcome in answered)

    def _call_alone(
        self,
        converters: Mapping[str, RowConverter],
        unit: RestUnit,
        *,
        spec: DatasetSpec,
        request: Request,
        limit: int | None,
        window: HistoryWindow | None,
        current: bool,
        stop: Cancel,
        patience: Patience,
    ) -> _Outcome | MassiveError:
        """Make one request of a read by symbol, returning its failure instead of raising it.

        Args:
            converters: Each path's conversion, by template.
            unit: The request and its path.
            spec: The dataset.
            request: The normalized arguments.
            limit: The cap on the request's rows, or None.
            window: The `history()` window whose rows to keep, or None.
            current: Whether the pages are checked for a `DELAYED` label.
            stop: The read's token.
            patience: The read's patience.

        Returns:
            The request's outcome (`_call`), or the `MassiveError` it failed with.

        Raises:
            AuthenticationError: If Massive rejects the API key, which fails the whole read.
        """
        rest_path, call = unit
        try:
            return self._call(
                converters[rest_path.template],
                call,
                spec=spec,
                request=request,
                limit=limit,
                window=window,
                current=current,
                on_recency=None,
                record=None,
                stop=stop,
                patience=patience,
            )
        except AuthenticationError:
            raise  # every request of the read would be refused: the whole read fails
        except MassiveError as error:
            logger.debug(
                "%s: the request of %s for %s failed, and fails only its own symbols: %s",
                spec.name,
                rest_path.template,
                ", ".join(own_symbols(spec, call.symbol, call.query)) or "no symbol",
                error,
            )
            return error

    def _report_missing(self, spec: DatasetSpec, template: str, missing: Sequence[str]) -> None:
        """Apply `MassiveConfig.unavailable_symbols` to the symbols a read found no data for.

        A `fetch()` or `history()` symbol Massive answered HTTP 404 has no data. Under `LOG` one
        WARNING per read names each such symbol once per session, under `IGNORE` only DEBUG,
        and the read leaves them out; under `RAISE` the read fails. A `poll()` cycle applies
        the setting itself (`_unavailable`).

        Args:
            spec: The dataset.
            template: The path's template.
            missing: The symbols (a futures session's with its session), in call order.

        Raises:
            UnavailableSymbolError: Under `RAISE` (`_missing_error`).
        """
        setting = self.unavailable_symbols
        if setting is OnError.RAISE:
            error = _missing_error(spec, template, missing)
            raise error
        level = logging.WARNING if setting is OnError.LOG else logging.DEBUG
        self._warn_missing(spec, template, missing, level=level)

    def _warn_missing(
        self, spec: DatasetSpec, template: str, missing: Sequence[str], *, level: int
    ) -> None:
        """Log the symbols Massive answered with HTTP 404: at `level` for those new this session.

        One line per read names each symbol once per session at `level`, however many there
        are; the others, and every symbol under a `level` of DEBUG, log at DEBUG. A DEBUG line
        marks no symbol as named.

        Args:
            spec: The dataset.
            template: The path's template.
            missing: The symbols (a futures session's with its session), in call order.
            level: The level of the symbols not yet named this session.
        """
        new = (
            [symbol for symbol in missing if self.state.first("missing", spec.name, symbol)]
            if level > logging.DEBUG
            else []
        )
        logger.log(
            level if new else logging.DEBUG,
            "%s: no data for %s: Massive answered HTTP 404 from %s, so they are left out",
            spec.name,
            bounded_list(new or missing),
            template,
        )

    def _fan_out(
        self,
        run: Callable[[_Item], _Done],
        calls: Sequence[_Item],
        *,
        executor: ThreadPoolExecutor | None,
        stop: Cancel,
    ) -> list[_Done]:
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
        current: bool,
        on_recency: Callable[[Recency], None] | None,
        record: Callable[[DriftReport], None] | None,
        stop: Cancel,
        patience: Patience,
    ) -> _Outcome:
        """Request one call's pages and convert their rows.

        Each page of current data is checked for a `DELAYED` label (`_check_recency`), each
        page against the requested adjustment basis (`_check_adjusted`), its bars of a day or
        longer are stamped (`StampRule.stamp`; a stamp Massive is not known to use is kept as
        sent and logs one WARNING per form of stamp per session), its rows outside the
        `history()` window dropped (`keep_window`), and its rows given the library's time columns
        (`row_times.TimeRule.fill`), with `TimestampInsert` the instant the page arrived. The
        call's day zone follows its own symbols' class (`dates.day_class`).

        Args:
            converter: The path's conversion.
            call: The call.
            spec: The dataset.
            request: The normalized arguments.
            limit: The cap on the call's rows, or None.
            window: The `history()` window whose rows to keep, or None.
            current: Whether the read is of current data (`read`).
            on_recency: Given each page's recency evidence (`read`), or None.
            record: Also given each page's drift, or None.
            stop: The read's token, which ends the call's waits.
            patience: The patience of the call's read, which its pause waits are charged to.

        Returns:
            The call's rows and request count; no rows for a per-symbol call whose first
            request Massive answered with HTTP 404 (a symbol with no data), logged at DEBUG
            and named in `missing`; `read` warns once per read for the symbols new to the
            session. Each page logs one DEBUG line with its rows after each step
            (`_page_rows`).

        Raises:
            CutShortError: If a per-symbol call's later page answers HTTP 404, so the symbol's
                data was cut short: the message names the dataset, the symbol, the path, and
                how many pages had arrived.

        A failed request otherwise raises the public error of its `WireError` (`public_error`), and
        once the session is closed every failed request does, a 404 included: the call raises
        `MassiveError("... is closed")` rather than dropping its symbol. A row that breaks the
        schema raises the `RowDataError` of `rest_rows.RowConverter.convert`, a page on
        another adjustment basis the `MassiveError` of `_check_adjusted`, and delayed data the
        `EntitlementError` of `_check_recency`.
        """
        kept: list[pa.RecordBatch] = []
        requests = 0
        template = converter.rest_path.template
        own = own_symbols(spec, call.symbol, call.query)
        zone_class = day_class(spec, map(symbol_class, own))
        times = time_rule(spec, request, asset_class=zone_class, warnings=self.warnings)
        stamps = times.stamps
        bucket = _request_bucket(spec, template, call)
        bare_index = _bare_index(request, call)
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
                if current:
                    self._check_recency(
                        spec,
                        page,
                        template=template,
                        bucket=bucket,
                        own=own,
                        call=call,
                        symbol_key=converter.symbol_key,
                        on_recency=on_recency,
                    )
                self._check_adjusted(spec, request, template=template, echoed=page.adjusted)
                batches, drift, flagged = converter.convert(page.rows, call.symbol)
                self.drift.report(
                    spec.name, Transport.REST, drift, where=f"GET {template}", symbol=call.symbol
                )
                if record is not None:
                    record(drift)
                self._warn_error_rows(converter, flagged, call.symbol, template=template)
                if spec.ticker_list is not None:
                    batches = self._listed(spec, batches, own, template=template)
                if bare_index is not None:
                    self._check_index_rows(spec, batches, bare_index, template=template)
                # Stamp before keeping: `keep_window` judges a bar by its day's start, and an
                # unexpected stamp outside the window still logs its WARNING.
                if stamps is not None:
                    batches = [stamps.stamp(spec.name, batch, source=template) for batch in batches]
                kept.extend(
                    _page_rows(
                        batches,
                        window=window,
                        time_name=converter.time_name,
                        fill=functools.partial(times.fill, received=received),
                        where=f"{spec.name}: page {requests} of {template} for "
                        f"{call.symbol or 'the request'}",
                        read=len(page.rows),
                    )
                )
        except WireError as error:
            return self._not_found(
                error,
                spec=spec,
                call=call,
                template=template,
                bucket=bucket,
                window=window,
                kept=kept,
                requests=requests,
            )
        return _Outcome(tuple(kept), requests)

    def _not_found(
        self,
        error: WireError,
        *,
        spec: DatasetSpec,
        call: Call,
        template: str,
        bucket: str,
        window: HistoryWindow | None,
        kept: Sequence[pa.RecordBatch],
        requests: int,
    ) -> _Outcome:
        """Settle a call's failed request: a symbol with no data, or the call's failure.

        Only HTTP 404 on a per-symbol call while the session is open is about the symbol. On the
        call's first request it means the symbol has no data: the call returns no rows with the
        symbol in `missing`, scoped to the futures session a `history()` call asked for, and
        logs it at DEBUG.

        Args:
            error: The request's failure.
            spec: The dataset.
            call: The call.
            template: The call's path template, for messages.
            bucket: The call's rate-limit bucket, for the public error.
            window: The `history()` window whose rows the call keeps, or None.
            kept: The rows the call kept before the failure.
            requests: How many of the call's requests were answered before the failure.

        Returns:
            The call's outcome for a symbol with no data, the 404 counted as a request.

        Raises:
            CutShortError: If a per-symbol call's later page answers HTTP 404, so the symbol's
                data was cut short: the message names the dataset, the symbol, the path, and
                how many pages had arrived.

        A 404 that arrives after the session closed raises the `MassiveError("... is closed")`
        of `public_error`, and any other failure the error of `_dataset_error`.
        """
        if error.status != HTTPStatus.NOT_FOUND or call.symbol is None:
            raise self._dataset_error(spec, bucket, error) from error
        # A 404 that arrives after the session closed is no answer about the symbol,
        # so the call reports the close rather than dropping a symbol with no data.
        if self.state.closed:
            logger.debug(
                "%s: HTTP 404 for %s from %s arrived after %s closed; the call reports the close",
                spec.name,
                call.symbol,
                template,
                self.state.label,
            )
            raise self.public_error(error) from error
        if requests:
            raise CutShortError(
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
        logger.debug(
            "%s: %s has no data%s (HTTP 404 from %s, request_id %s: %s); it is left out",
            spec.name,
            call.symbol,
            scope,
            template,
            error.request_id,
            error.message,
        )
        return _Outcome(tuple(kept), requests + 1, missing=f"{call.symbol}{scope}")

    def _listed(
        self,
        spec: DatasetSpec,
        batches: Sequence[pa.RecordBatch],
        named: Sequence[str],
        *,
        template: str,
    ) -> list[pa.RecordBatch]:
        """Fill the `Symbol` of a dataset whose rows list their tickers (`_listed_rows`).

        Logs one WARNING per dataset a session, then DEBUG, when a row lists none of the
        request's several named tickers: it keeps a null `Symbol`.

        Args:
            spec: The dataset, with a `ticker_list`.
            batches: One page's rows in the dataset's schema.
            named: The tickers the request named.
            template: The path, for the message.

        Returns:
            The rows with their `Symbol`.
        """
        listed: list[pa.RecordBatch] = []
        unmatched = 0
        for batch in batches:
            rows, missed = _listed_rows(batch, named, column=cast("str", spec.ticker_list))
            listed.append(rows)
            unmatched += missed
        if unmatched:
            first = self.state.first("unlisted rows", spec.name)
            logger.log(
                logging.WARNING if first else logging.DEBUG,
                "%s: %d rows Massive returned from %s list none of the request's tickers "
                "(%s) in %s, so their Symbol is null",
                spec.name,
                unmatched,
                template,
                bounded_list(named),
                spec.ticker_list,
            )
        return listed

    def _check_index_rows(
        self, spec: DatasetSpec, batches: Sequence[pa.RecordBatch], index: str, *, template: str
    ) -> None:
        """Warn about rows of an index's contracts that do not read as that index's.

        The contracts list takes an index bare (`SPX` for `I:SPX`), and a row gets its `I:`
        back only when its CFI code marks an option on an index
        (`rest_rows.RowConverter`). A bare ticker can also name a stock (`DJT` is a stock;
        `I:DJT` is not one of Massive's indices), so a row whose code marks another kind of
        option is Massive's answer about another instrument: it keeps the underlying Massive
        wrote, and one WARNING per dataset and index per session names the request and the
        rows. A row without a CFI code keeps the bare ticker, since nothing says it is an
        index: one WARNING per dataset per session says so. Neither drops a row.

        Args:
            spec: The dataset.
            batches: One page's rows in the dataset's schema.
            index: The canonical index the call asked for, such as `I:SPX`.
            template: The path, for the messages.
        """
        if not batches:
            return
        underlyings = pa.chunked_array([batch.column(UNDERLYING) for batch in batches])
        other = pc.fill_null(pc.not_equal(underlyings, index), fill_value=True)
        if not pc.any(other).as_py():
            return
        cfi = pa.chunked_array([batch.column(CFI) for batch in batches])
        symbols = pa.chunked_array([batch.column("Symbol") for batch in batches])
        no_code = pc.and_(other, pc.is_null(cfi))
        other_kind = pc.and_(other, pc.is_valid(cfi))
        if pc.any(other_kind).as_py() and self.state.first("index-other-kind", spec.name, index):
            shown = list(
                zip(
                    pc.filter(symbols, other_kind).to_pylist()[:5],
                    pc.filter(underlyings, other_kind).to_pylist()[:5],
                    pc.filter(cfi, other_kind).to_pylist()[:5],
                    strict=True,
                )
            )
            logger.warning(
                "%s: asked %s for the contracts of %s by its bare ticker, %s=%s, and %d rows "
                "are not options on an index by their CFI code, so Massive answered about "
                "another instrument; they keep the underlying Massive wrote. The first "
                "(Symbol, Underlying, Cfi): %s",
                spec.name,
                template,
                index,
                BARE_INDEX_PARAM,
                contracts_underlying(index),
                pc.sum(other_kind).as_py(),
                shown,
            )
        if pc.any(no_code).as_py() and self.state.first("index-no-cfi", spec.name):
            logger.warning(
                "%s: %d rows of the contracts of %s from %s have no CFI code, so nothing says "
                "their underlying is an index and they keep Massive's bare %r; the first: %s",
                spec.name,
                pc.sum(no_code).as_py(),
                index,
                template,
                contracts_underlying(index),
                pc.filter(symbols, no_code)[0].as_py(),
            )

    def _check_recency(
        self,
        spec: DatasetSpec,
        page: Page,
        *,
        template: str,
        bucket: str,
        own: Sequence[str],
        call: Call,
        symbol_key: str | None,
        on_recency: Callable[[Recency], None] | None,
    ) -> None:
        """Refuse a page of current data Massive labels delayed, unless delayed is asked for.

        Only current data is checked (`_read_planned`: a dataset of current data, or the latest
        values of one computed from it), and none reads a reference endpoint. Each label is held
        against the subscription of the data it labels, and refused when that subscription is not in
        `MassiveConfig.delayed_subscriptions`. A page charged to a subscription (its rate-limit
        bucket names one) is that subscription's data. A page of the `other` bucket, such as the
        unified snapshot of several classes, is each row's own: a row's subscription is its ticker's
        class's (`symbol_class`). An envelope `status` of `DELAYED` labels the whole page, so it is
        refused unless every class the request names is listed. A row's `timeframe` fields
        (`recency_labels.read_labels`, which never reads `underlying_asset.timeframe`) label that
        row; the page is read whole first, so the refusal names every refused row's ticker (the
        first five, then a count) and the field that said `DELAYED`. Forex and crypto data is never
        refused: Massive has no delayed currencies feed, and `delayed_subscriptions` cannot list
        `CURRENCIES`, so a label can name no remedy; the label still shows as `DELAYED`. A page of
        no subscription and no tickers has nothing to refuse. REST cannot ask for delayed data, so a
        listed subscription's labels pass. A `timeframe` that is neither label is served and logs a
        WARNING once per subscription, field, and value in the session (`_warn_unknown_label`).

        Args:
            spec: The dataset, for the message.
            page: The page.
            template: The page's path template, for the WARNING of an unknown label.
            bucket: The page's rate-limit bucket.
            own: The symbols the request names (`own_symbols`).
            call: The request, whose one symbol stands for its rows when they carry none.
            symbol_key: The wire key of the rows' ticker, or None.
            on_recency: Given what a checked page shows, when it carries a label: `DELAYED`
                when a label it passed says so (a listed subscription's), else `REALTIME` when
                a label says real-time; None to ignore it.

        Raises:
            EntitlementError: If a label says delayed for a subscription not listed
                (`recency_labels.delayed_refusal`).
        """
        listed = self.delayed_subscriptions
        fixed = None if bucket == OTHER else Subscription(bucket)
        named = {fixed} if fixed is not None else {subscription_of(symbol_class(s)) for s in own}
        # Massive has no delayed currencies feed, and the configuration cannot list one, so a
        # delay label is never held against forex or crypto data; it still shows as delayed.
        held = named - {Subscription.CURRENCIES}
        if page.status == DELAYED_LABEL:
            unlisted = sorted(held - listed)
            if unlisted:
                refusal = delayed_refusal(
                    unlisted[0],
                    _delayed_what(spec, own),
                    f"status={DELAYED_LABEL}",
                    streams=False,
                )
                raise refusal
        recency = _page_recency(
            page.rows, fixed=fixed, listed=listed, symbol_key=symbol_key, symbol=call.symbol
        )
        for subscription, unknown in recency.unknown:
            self._warn_unknown_label(
                spec, template=template, subscription=subscription, unknown=unknown
            )
        if recency.refused:
            subscription = min(recency.refused)
            tickers, evidence = recency.refused[subscription]
            refusal = delayed_refusal(
                subscription, _delayed_what(spec, list(tickers)), evidence, streams=False
            )
            raise refusal
        delayed = recency.delayed or (page.status == DELAYED_LABEL and bool(named))
        if on_recency is not None and (delayed or recency.real_time):
            on_recency(Recency.DELAYED if delayed else Recency.REALTIME)

    def _warn_unknown_label(
        self,
        spec: DatasetSpec,
        *,
        template: str,
        subscription: Subscription | None,
        unknown: tuple[str, object],
    ) -> None:
        """Warn of a recency label that is neither `DELAYED` nor `REAL-TIME`, once per value.

        Such a label tells nothing about the row's recency, so the row is neither refused
        nor counted as real-time; the WARNING says so once per subscription, field, and value
        in the session, since a new label from Massive may mean delayed data passes unseen.

        Args:
            spec: The dataset.
            template: The page's path template.
            subscription: The subscription of the row's data, or None when it has none.
            unknown: The field and its value (`RecencyLabels.unknown`).
        """
        field, value = unknown
        owner = "no subscription" if subscription is None else str(subscription)
        if self.state.first("recency-label", owner, field, repr(value)):
            logger.warning(
                "%s: %s answered %s=%r, which is neither DELAYED nor REAL-TIME, for %s data; "
                "the library cannot tell whether the row is delayed and serves it",
                spec.name,
                template,
                field,
                value,
                owner,
            )

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
        if not spec.holds_bars or sent is None:
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
        self,
        converter: RowConverter,
        flagged: Sequence[Mapping[str, object]],
        symbol: str | None,
        *,
        template: str,
    ) -> None:
        """Log a page's error rows: each at DEBUG, and one WARNING for the tickers new to it.

        The WARNING lists each ticker not warned about before in the session, with Massive's
        error and message, through `bounded_list`, so a page of thousands of error rows logs
        one short line. The rows are kept, with their `Error` and `Message` columns.

        Args:
            converter: The path's conversion.
            flagged: The page's error rows.
            symbol: The request's one symbol, for a path whose rows do not carry it.
            template: The path template, for the record.
        """
        dataset = converter.dataset
        key = converter.symbol_key
        new = []
        for row in flagged:
            ticker = str(symbol if key is None else row.get(key))
            answer = f"{ticker} ({row.get('error')}: {row.get('message')})"
            logger.debug(
                "%s: Massive answered an error row from %s for %s", dataset, template, answer
            )
            if self.state.first("error-row", dataset, ticker):
                # The WARNING shows the start of each answer; the DEBUG line above has it all.
                new.append(_cut(answer))
        if new:
            logger.warning(
                "%s: Massive answered error rows from %s for %s; the rows are kept with their "
                "Error and Message columns",
                dataset,
                template,
                bounded_list(new),
            )

    def public_error(self, error: WireError) -> MassiveError:
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

    def _dataset_error(self, spec: DatasetSpec, bucket: str, error: WireError) -> MassiveError:
        """Return the public error of a dataset's failed request, naming what was refused.

        A refusal (HTTP 403, or a 200 response whose envelope says `NOT_AUTHORIZED`) becomes an
        `EntitlementError` that names the dataset and the subscription the request is charged
        to, then Massive's answer, and, where the dataset has one
        (`DatasetSpec.refused_alternative`), the dataset to try instead. Every other error is
        `public_error`'s.

        Args:
            spec: The dataset.
            bucket: The request's rate-limit bucket: its subscription, or `other` for data of
                no subscription (reference, economy, partner data).
            error: The wire error.

        Returns:
            The public error, with the wire error's status and request id, such as
            `EntitlementError("stocks.last_trade: the Stocks subscription of your plan does not
            include this data; Massive answered HTTP 403 from GET ...; try
            m.stocks.snapshots(...), which carries the same values and is in more plans")`.
        """
        public = self.public_error(error)
        if type(public) is not EntitlementError:
            return public
        holder = (
            "your account" if bucket == OTHER else f"the {bucket.title()} subscription of your plan"
        )
        message = f"{spec.name}: {holder} does not include this data; Massive answered {public}"
        if spec.refused_alternative is not None:
            message += (
                f"; try m.{spec.refused_alternative}(...), which carries the same values and is "
                "in more plans"
            )
        return EntitlementError(
            message, status=public.status, request_id=public.request_id, cause=public.cause
        )

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

        A failed request raises the public error of its `WireError` (`public_error`):
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
            raise self.public_error(error) from error

    def probe(
        self, probe: host_decision.SnapshotProbe, *, asset_class: AssetClass, patience: Patience
    ) -> host_decision.SnapshotAnswer:
        """Make a host-decision request and return its envelope status and raw JSON rows.

        Args:
            probe: The request.
            asset_class: The class whose host is decided; the request is charged to its
                subscription, whatever path it reads.
            patience: The deciding caller's patience (`patience()`, taken on its thread
                before it waited on another caller's decision, if it did).

        Returns:
            The envelope status and the rows, every key kept.

        A failed request raises the public error of its `WireError` (`public_error`):
        `AuthenticationError`, `EntitlementError`, or `MassiveError`.
        """
        try:
            page = self.state.client.get(
                self.rest_path(probe.path),
                probe.path_params,
                probe.query,
                bucket=bucket_of(subscription_of(asset_class)),
                patience=patience,
            )
        except WireError as error:
            raise self.public_error(error) from error
        return host_decision.SnapshotAnswer(page.status, page.rows)

    def read_cycle(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        symbols: NormalizedSymbols | None,
        values: tuple[str, ...] | None,
        record: Callable[[DriftReport], None],
        on_recency: Callable[[Recency], None],
        executor: ThreadPoolExecutor,
        cancel: Cancel,
        pace: PaceCheck,
    ) -> poll.CycleRows:
        """Run one `poll()` cycle, once for each value of a value table when there is one.

        Without a value table the cycle is one read (`_read_one_cycle`). With one, each of the
        cycle's values is read as if passed alone (`Request.for_value`), one after another,
        and the reads are joined: their rows in value order, their requests and pauses summed,
        and what each left out. A value Massive answers with HTTP 404 follows
        `MassiveConfig.unavailable_symbols`, as a symbol does: under `LOG` and `IGNORE` it is
        left out of the cycle and named in its `left_out`.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            symbols: The cycle's static symbols (a reactive table already read).
            values: The values the value table holds now (already checked), or None when the
                request has no value table.
            record: Records each page's drift in the table's `SchemaDrift`.
            on_recency: Records each page's recency evidence, for the table's `Recency`.
            executor: The poller's fan-out pool.
            cancel: The poller's stop token, which ends the cycle's waits.
            pace: The table's pace check.

        Returns:
            The cycle's read (`poll.CycleRows`).

        Raises:
            UnavailableSymbolError: Under `MassiveConfig.unavailable_symbols=RAISE`, for a
                value Massive answered with HTTP 404.
            MassiveError: Any other failure of a read, as `_read_one_cycle` raises it.
        """
        read = functools.partial(
            self._read_one_cycle,
            spec,
            symbols=symbols,
            record=record,
            on_recency=on_recency,
            executor=executor,
            cancel=cancel,
            pace=pace,
        )
        if values is None:
            return read(request)
        reads: list[poll.CycleRows] = []
        absent: list[str] = []
        for value in values:
            try:
                reads.append(read(request.for_value(value)))
            except MassiveError as error:
                if error.status != HTTPStatus.NOT_FOUND or self.state.closed:
                    raise
                self._unavailable(
                    spec,
                    cast("str", spec.fetch_path),
                    (value,),
                    reason="HTTP 404: no data",
                    missing=True,
                )
                absent.append(f"{value} (HTTP 404: no data)")
        schema = self.catalog.dataset_schema(
            spec, raw_symbol=self.unreadable_symbols is not OnError.RAISE
        )
        return poll.CycleRows(
            pa.concat_tables([one.rows for one in reads])
            if reads
            else schema.arrow_schema().empty_table(),
            sum(one.requests for one in reads),
            sum(one.patience for one in reads),
            (*(name for one in reads for name in one.left_out), *absent),
        )

    def _read_one_cycle(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        symbols: NormalizedSymbols | None,
        record: Callable[[DriftReport], None],
        on_recency: Callable[[Recency], None],
        executor: ThreadPoolExecutor,
        cancel: Cancel,
        pace: PaceCheck,
    ) -> poll.CycleRows:
        """Run one read of a `poll()` cycle: the planned requests of `read_fetch` for its symbols.

        By default one symbol never fails the table. A reactive symbol table can route a new
        symbol to a path that lacks a parameter the caller set (`adjusted=False` on the crypto
        indicators), and Massive can answer HTTP 404 for a symbol (no data).
        `MassiveConfig.unavailable_symbols` decides (`_unavailable`): under `LOG` and `IGNORE`
        such symbols are left out of the cycle and named in its `left_out`, which the table's
        `Error` lists; under `RAISE` the table fails.
        Before reading, the cycle's requests per bucket go to `pace`, which warns once when
        the paced ones outlast the interval.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            symbols: The cycle's static symbols (a reactive table already read).
            record: Records each page's drift in the table's `SchemaDrift`.
            on_recency: Records each page's recency evidence, for the table's `Recency`.
            executor: The poller's fan-out pool.
            cancel: The poller's stop token, which ends the cycle's waits.
            pace: The table's pace check.

        Returns:
            The cycle's read (`poll.CycleRows`): its rows, its request count, the seconds of
            rate-limit pauses its reads wait out (`patience()`, taken on the poller's thread
            before any fan-out), and the symbols it left out, each with why.

        Raises:
            UnavailableSymbolError: Under `MassiveConfig.unavailable_symbols=RAISE`, for a
                symbol Massive answered with HTTP 404 or whose path does not take a parameter
                the poll sends (`_unavailable`).

        Any other error is that of `read_fetch`, but for a parameter a path does not take.
        """
        planned, left_out = self._plan_cycle(spec, request, symbols, cancel=cancel)
        pace.check(self.state.client.governor, Counter(_buckets(spec, planned)))
        patience = self.patience()  # once for every path, on this thread, before any fan-out
        missing: list[tuple[str, Sequence[str]]] = []
        table, requests = self._read_planned(
            spec,
            request,
            planned,
            current=spec.serves_current_data or spec.latest_is_current,
            record=record,
            on_recency=on_recency,
            executor=executor,
            cancel=cancel,
            patience=patience,
            on_missing=lambda template, found: missing.append((template, found)),
        )
        for template, absent in missing:
            reason = f"HTTP 404 from {template}: no data"
            self._unavailable(spec, template, absent, reason=reason, missing=True)
            left_out.extend(f"{symbol} (HTTP 404: no data)" for symbol in absent)
        return poll.CycleRows(table, requests, patience.seconds, tuple(left_out))

    def _unavailable(
        self,
        spec: DatasetSpec,
        template: str,
        symbols: Sequence[str],
        *,
        reason: str,
        missing: bool,
    ) -> None:
        """Apply `MassiveConfig.unavailable_symbols` to symbols a `poll()` cycle cannot read.

        Under `LOG` each symbol logs one WARNING per session (then DEBUG), under `IGNORE` only
        DEBUG, and the cycle leaves them out; under `RAISE` the table fails.

        Args:
            spec: The dataset.
            template: The path that cannot serve them.
            symbols: The symbols.
            reason: Why, for the log and the error, such as `"/v1/indicators/sma/{cryptoTicker}
                does not take ['adjusted']"`.
            missing: True when Massive answered HTTP 404, False when the path does not take a
                parameter the poll sends.

        Raises:
            UnavailableSymbolError: Under `RAISE`, naming the symbols, the path, the reason,
                and the setting.
        """
        setting = self.unavailable_symbols
        if setting is OnError.RAISE:
            raise UnavailableSymbolError(
                f"{spec.name} poll(): {', '.join(symbols)} cannot be polled: {reason}. Remove "
                "the symbol, or set MassiveConfig(unavailable_symbols=dm.OnError.LOG) to leave "
                "it out and keep polling the others",
                cause=ErrorCause.EXPECTED if missing else ErrorCause.USER_DEFECT,
            )
        if missing:
            level = logging.WARNING if setting is OnError.LOG else logging.DEBUG
            self._warn_missing(spec, template, symbols, level=level)
            return
        new = [name for name in symbols if self.state.first("refused", spec.name, template, name)]
        logger.log(
            logging.WARNING if new and setting is OnError.LOG else logging.DEBUG,
            "%s poll(): %s left out of the cycle: %s; the table lists %s in its Error and "
            "polls the other symbols",
            spec.name,
            bounded_list(symbols),
            reason,
            "it" if len(symbols) == 1 else "them",
        )

    def _plan_cycle(
        self,
        spec: DatasetSpec,
        request: Request,
        symbols: NormalizedSymbols | None,
        *,
        cancel: Cancel,
    ) -> tuple[list[tuple[RestPath, list[Call]]], list[str]]:
        """Plan a `poll()` cycle path by path, leaving out each path that refuses a parameter.

        A path that does not take a parameter the caller set (`shared_params` raises
        `ValueError`) is left out with its symbols, and the cycle reads every other path, as
        `MassiveConfig.unavailable_symbols` says (`_unavailable`): under `LOG` each symbol logs
        one WARNING per session, naming the path and the parameter, and DEBUG on later cycles.
        A crypto pair that Massive's crypto ticker list does not hold follows
        `MassiveConfig.unlisted_symbols`: under `LOG` and `IGNORE` it is left out of the cycle
        (`_skip_unlisted`); under `RAISE` its `UnlistedPairError` fails the table.

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments.
            symbols: The cycle's static symbols.
            cancel: The poller's stop token, for a read of the crypto ticker list.

        Returns:
            Each path the cycle reads with its calls (`plan_fetch`), and each symbol left out
            with why, such as `"X:BTCUSD (/v1/indicators/sma/{cryptoTicker} does not take
            ['adjusted'])"`.

        A crypto pair that cannot be split raises the `MassiveError` of `crypto_pair` (a pair
        the list does not hold only under `unlisted_symbols=RAISE`), and
        a refusing path under `RAISE` the `UnavailableSymbolError` of `_unavailable`.
        """
        if symbols == ():
            return [], []
        planned = []
        left_out: list[str] = []
        unlisted: dict[str, UnlistedPairError] = {}
        skips = self.unlisted_symbols is not OnError.RAISE
        split = self._unlisted_recorder(unlisted, cancel) if skips else None
        for template, own in fetch_paths(spec, symbols):
            try:
                path = self._plan_path(
                    spec, request, template=template, own=own, split=split, cancel=cancel
                )
            except ValueError as error:
                reason = str(error).removeprefix(f"{spec.name}: ")
            else:
                planned.append(_without(path, unlisted))
                continue
            # Only a reactive table's symbols can meet a refusing path here: static symbols
            # were planned when the table was made, so `own` is a tuple.
            names = cast("tuple[str, ...]", own)
            self._unavailable(spec, template, names, reason=reason, missing=False)
            left_out.extend(f"{name} ({reason})" for name in names)
        if unlisted:
            self._skip_unlisted(spec, unlisted, verb="poll()", setting=self.unlisted_symbols)
            left_out.extend(
                f"{name} (not in Massive's crypto ticker list)" for name in sorted(unlisted)
            )
        return planned, left_out

    def listed_split(
        self, unlisted: dict[str, UnlistedPairError], cancel: Cancel | None
    ) -> CryptoSplit:
        """Return the crypto split a read uses under the session's `unlisted_symbols`.

        Under `RAISE` a pair Massive's crypto ticker list does not hold raises its
        `UnlistedPairError`; under `LOG` and `IGNORE` it is recorded, so the read leaves its
        requests out (`_without`) and logs it (`skip_unlisted`).

        Args:
            unlisted: Receives each pair recorded, with its error.
            cancel: The read's stop token, which the list's read listens to.

        Returns:
            The split.
        """
        if self.unlisted_symbols is OnError.RAISE:
            return functools.partial(self.crypto_pair, cancel=cancel)
        return self._unlisted_recorder(unlisted, cancel)

    def skip_unlisted(
        self, spec: DatasetSpec, unlisted: Mapping[str, UnlistedPairError], *, verb: str
    ) -> None:
        """Log the crypto pairs a read leaves out under the session's `unlisted_symbols`.

        Args:
            spec: The dataset.
            unlisted: The pairs, with their errors.
            verb: The verb that leaves them out, such as `"history()"`.
        """
        self._skip_unlisted(spec, unlisted, verb=verb, setting=self.unlisted_symbols)

    def _unlisted_recorder(
        self, unlisted: dict[str, UnlistedPairError], cancel: Cancel | None
    ) -> CryptoSplit:
        """Return a crypto split that records a pair Massive does not list instead of raising.

        Any other failure to split, such as a failed read of the list, still raises.

        Args:
            unlisted: Receives each pair the list does not hold, with its error; the plan's
                calls of that pair are then dropped (`_without`).
            cancel: The read's stop token, which the list's read listens to.

        Returns:
            A split that returns `("", "")` for a pair it recorded (`recording_split`).
        """
        pair = functools.partial(self.crypto_pair, cancel=cancel)
        return recording_split(pair, unlisted, catching=UnlistedPairError)

    def _skip_unlisted(
        self,
        spec: DatasetSpec,
        unlisted: Mapping[str, UnlistedPairError],
        *,
        verb: str,
        setting: OnError,
    ) -> None:
        """Log the crypto pairs a read leaves out, each once per session.

        At WARNING under `unlisted_symbols=LOG` the first time a pair is left out in the
        session, and at DEBUG after that and under `IGNORE`. Never called under `RAISE`.

        Args:
            spec: The dataset.
            unlisted: The pairs, with their errors.
            verb: The verb that leaves them out, `"fetch()"` or `"poll()"`.
            setting: The `unlisted_symbols` setting the read follows.
        """
        for symbol, error in sorted(unlisted.items()):
            first = self.state.first("unlisted", spec.name, symbol)
            logger.log(
                logging.WARNING if first and setting is OnError.LOG else logging.DEBUG,
                "%s %s: %s; it is left out, as a pair Massive has no data for, and the other "
                "symbols are read. Set dm.MassiveConfig(unlisted_symbols=dm.OnError.RAISE) to "
                "fail instead",
                spec.name,
                verb,
                error,
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
        try:
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
        finally:
            # Each error's traceback holds this frame, and the frame's list holds the error: a
            # cycle that keeps this reader, and so its session, alive after the session is
            # dropped, until the cyclic garbage collector next runs. Emptying the list breaks it.
            errors.clear()

    def plan_fetch(
        self,
        spec: DatasetSpec,
        request: Request,
        symbols: NormalizedSymbols | None,
        *,
        split: CryptoSplit | None = None,
        cancel: Cancel | None = None,
    ) -> list[tuple[RestPath, list[Call]]]:
        """Plan the requests of a `fetch()` or `poll()` cycle, path by path.

        Makes no request, except that the first crypto pair a path takes as two currencies
        reads the session's crypto ticker list (`crypto_pair`), unless `split` is given.

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments.
            symbols: The static symbols.
            split: Splits a crypto pair into its currencies; None for `crypto_pair`. A check
                that must make no request passes `_unsplit`.
            cancel: The read's stop token, which a read of the crypto ticker list listens to;
                None for none.

        Returns:
            Each path the cycle reads (`fetch_paths`) with its calls; none for an empty symbol
            tuple (a reactive table with no valid row), which reads nothing.

        A parameter a path does not take raises the `ValueError` of `shared_params`; a crypto pair
        that cannot be split raises the `MassiveError` of `crypto_pair`.
        """
        if symbols == ():
            return []  # an emptied symbol table asks for nothing, whatever the paths take
        return [
            self._plan_path(spec, request, template=template, own=own, split=split, cancel=cancel)
            for template, own in fetch_paths(spec, symbols)
        ]

    def _plan_path(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        template: str,
        own: NormalizedSymbols | None,
        split: CryptoSplit | None,
        cancel: Cancel | None,
    ) -> tuple[RestPath, list[Call]]:
        """Plan one fetch path's calls for its symbols (`plan_calls`).

        Args:
            spec: The dataset.
            request: The normalized arguments.
            template: The path's template (`fetch_paths`).
            own: The symbols the path reads.
            split: Splits a crypto pair, or None for `crypto_pair` (`plan_fetch`).
            cancel: The read's stop token, for a read of the crypto ticker list, or None.

        Returns:
            The path and its calls.

        A parameter the path does not take raises the `ValueError` of `shared_params`.
        """
        rest_path = self.rest_path(template)
        calls = plan_calls(
            spec,
            rest_path,
            request,
            symbols=own,
            dates=(request.start, request.end),
            split=functools.partial(self.crypto_pair, cancel=cancel) if split is None else split,
        )
        return rest_path, calls

    def read_fetch(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        symbols: NormalizedSymbols | None,
        record: Callable[[DriftReport], None] | None = None,
        executor: ThreadPoolExecutor | None = None,
        cancel: Cancel | None = None,
        pauses_live_tables: bool = False,
        unlisted: OnError,
    ) -> tuple[pa.Table, int]:
        """Read a dataset's fetch paths for the symbols: `fetch()`.

        Once planned, the read logs at DEBUG its request count and how many seconds of rate
        limit pauses it waits out (`patience`), and at INFO the least time its paced requests
        take, when they cannot all start within a minute (`rate_limits.announce`); at WARNING
        when the read holds the update graph's lock.

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments.
            symbols: The static symbols.
            record: Receives each page's drift, or None.
            executor: A pool for fan-out requests, or None for one of each read's own.
            cancel: A background read's stop token, which ends the read's waits when set; None
                when only the session's close should.
            pauses_live_tables: True when the calling thread holds the update graph's lock (a
                console command), so the read pauses every live table of the server while it
                runs.
            unlisted: What a crypto pair Massive's crypto ticker list does not hold does:
                `RAISE` raises its `UnlistedPairError`; `LOG` and `IGNORE` leave it out
                (`_skip_unlisted`), so a read whose every pair is left out reads nothing.

        Returns:
            The rows of every path, in path order, capped at `Request.limit` (each symbol's
            rows where a request names one symbol, each request's rows for a dataset whose
            symbols overlap; `_read_planned`), and the request count.

        Errors are those of `read`, and a parameter a path does not take raises the
        `ValueError` of `shared_params`.
        """
        found: dict[str, UnlistedPairError] = {}
        skips = unlisted is not OnError.RAISE
        split = self._unlisted_recorder(found, cancel) if skips else None
        planned = self.plan_fetch(spec, request, symbols, split=split, cancel=cancel)
        if found:
            self._skip_unlisted(spec, found, verb="fetch()", setting=unlisted)
            planned = [_without(path, found) for path in planned]
        patience = self.patience()  # once for every path, on this thread, before any fan-out
        logger.debug(
            "%s fetch: %d requests on %d paths; the read waits out %.0f s of pauses",
            spec.name,
            sum(len(calls) for _, calls in planned),
            len(planned),
            patience.seconds,
        )
        announce(
            self.state.client.governor,
            f"{spec.name} fetch",
            _buckets(spec, planned),
            pauses_live_tables=pauses_live_tables,
        )
        return self._read_planned(
            spec,
            request,
            planned,
            current=_fetch_is_current(spec, request),
            record=record,
            executor=executor,
            cancel=cancel,
            patience=patience,
        )

    def _read_planned(
        self,
        spec: DatasetSpec,
        request: Request,
        planned: Sequence[tuple[RestPath, list[Call]]],
        *,
        current: bool,
        record: Callable[[DriftReport], None] | None,
        executor: ThreadPoolExecutor | None,
        cancel: Cancel | None,
        patience: Patience,
        on_recency: Callable[[Recency], None] | None = None,
        on_missing: Callable[[str, Sequence[str]], None] | None = None,
    ) -> tuple[pa.Table, int]:
        """Make planned calls, path by path, and cap the rows at `Request.limit`.

        The cap is on the total rows, except where each request names one symbol
        (`Call.symbol`, a per-symbol path): there it caps each symbol's rows, so a few symbols
        with many rows never crowd out the rest. A dataset whose rows list their tickers may
        hold more rows than the cap: each row is repeated once per named ticker it lists
        (`_listed_rows`) after Massive capped it.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            planned: Each path with its calls (`plan_fetch`); empty for no symbols.
            current: True when the pages are checked for a `DELAYED` label (`read`): a dataset
                of current data, or the latest values of one computed from it
                (`DatasetSpec.latest_is_current`) in a `poll()` or a `fetch()` that reaches
                the present.
            record: Receives each page's drift, or None.
            executor: A pool for fan-out requests, or None for one of each read's own.
            cancel: Ends the reads' waits when set, or None.
            patience: The patience of the whole read, shared by every path's requests.
            on_recency: Receives each page's recency evidence (`read`), or None.
            on_missing: Receives each path's template and its symbols Massive answered with
                HTTP 404 (`read`), or None.

        Returns:
            The rows of every path, in path order, and the request count.

        Errors are those of `read`.
        """
        tables = []
        requests = 0
        for rest_path, calls in planned:
            table, made = self.read(
                spec,
                rest_path,
                calls,
                request=request,
                limit=request.limit,
                current=current,
                on_recency=on_recency,
                record=record,
                executor=executor,
                cancel=cancel,
                patience=patience,
                on_missing=on_missing,
            )
            tables.append(table)
            requests += made
        if not tables:
            schema = self.catalog.dataset_schema(
                spec, raw_symbol=self.unreadable_symbols is not OnError.RAISE
            )
            return schema.arrow_schema().empty_table(), 0
        rows = pa.concat_tables(tables)
        per_symbol = any(call.symbol is not None for _, calls in planned for call in calls)
        if request.limit is not None and per_symbol:
            # Each request named one symbol and was capped on its own.
            logger.debug(
                "%s: kept %d rows from %d one-symbol requests; limit=%d per symbol",
                spec.name,
                rows.num_rows,
                requests,
                request.limit,
            )
        elif request.limit is not None and spec.ticker_list is not None:
            # Massive capped the rows; listing each under every named ticker it lists can
            # give more, all of which are kept.
            logger.debug(
                "%s: kept %d rows, each listed under its named tickers, from rows Massive "
                "capped at limit=%d",
                spec.name,
                rows.num_rows,
                request.limit,
            )
        elif request.limit is not None:
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
            core.config.unreadable_symbols,
            core.config.unavailable_symbols,
            core.config.unlisted_symbols,
            core.config.delayed_subscriptions,
            f"{core.thread_prefix} {role} rest",
            core.exec_ctx,
            core.warnings,
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
        that arrive meanwhile wait for that decision, outside every lock, each within its own
        patience (`SharedLookup.claim_or_wait`), and later callers get it at once. A failed
        decision is not cached: its row shows `FAILED` and the error, every waiting caller
        raises a copy of the error, and the next call tries again. If the deciding caller
        stopped for a reason of its own (it gave up on a pause, or was interrupted), each
        waiting caller tries again instead, with the patience it has left.

        Args:
            asset_class: The asset class of the stream.
            family: The channel family.

        Returns:
            The decision.

        Errors: `AuthenticationError` for a rejected key and `MassiveError` for a failed
        request (a refused one is not an error; see `host_decision.decide`); each is logged by
        the deciding caller, at DEBUG, or at ERROR for a defect of the library. A waiting
        caller that gives up raises the error `SharedLookup.claim_or_wait` describes.
        """
        key = (asset_class, family)
        known = self._state.hosts.known(key)
        if known is not None:
            return known  # a decision already made needs no patience (a Java call)
        reader = self._reader("hosts")
        # Taken before claiming the decision: a claim is ended only by the block that decides.
        patience = reader.patience()
        claim = self._state.hosts.claim_or_wait(
            key,
            bucket=bucket_of(subscription_of(asset_class)),
            patience=patience,
            what=f"the {asset_class.value} WebSocket host decision",
            public=reader.public_error,
        )
        if not isinstance(claim, Claim):
            return claim
        context = f"deciding the {asset_class.value} WebSocket host failed"
        try:
            decision = host_decision.decide(
                asset_class,
                family,
                websocket_hosts=self._core.config.websocket_hosts,
                delayed_subscriptions=self._core.config.delayed_subscriptions,
                fetch=functools.partial(reader.probe, asset_class=asset_class, patience=patience),
            )
        except BaseException as error:
            self._state.hosts.fail(claim, error, context)
            failure = (
                error
                if isinstance(error, MassiveError)
                else MassiveError(
                    library_bug(f"{context}: {error!r}"), cause=ErrorCause.LIBRARY_DEFECT
                )
            )
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
        self._state.hosts.complete(claim, decision)
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

        The WebSocket transport calls it, before a stream starts or on its `symbol changes`
        worker, for a crypto ticker (`RestReader.crypto_pair`), from the session's crypto
        ticker list, read once.

        Args:
            symbol: A canonical crypto ticker.

        Returns:
            `(base, quote)`.

        A failed or empty lookup raises the `MassiveError` of `RestReader.crypto_pair`.
        """
        return self._reader("crypto pairs").crypto_pair(symbol)

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

    def fetch(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        call: int,
        stop: threading.Event | None = None,
        unlisted: OnError,
    ) -> Table:
        """Return every row the dataset's fetch path returns for the request, as a static table.

        Requests run on this thread, or for a per-symbol path on a pool of up to
        `max_concurrency` threads; `Request.limit` caps the total rows, each symbol's rows on a
        per-symbol path, or each request's rows for a dataset whose symbols overlap, which keeps
        a row several requests return once (`RestReader._read_planned`). Drift is logged, once
        per field.

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments, with static symbols (the query reads a reactive
                table before this call).
            call: The call's id (`SessionCore.next_stream_id`), which names its fan-out
                threads (`"massive #1 call-7 rest_0"`).
            stop: A background read's stop token (`SessionClients.read_stop`, a `Cancel`),
                which ends the read's waits when set; None when only the session's close
                should.
            unlisted: What a crypto pair Massive's crypto ticker list does not hold does
                (`RestReader.read_fetch`): the session's `MassiveConfig.unlisted_symbols`.

        Returns:
            A static table in the dataset's schema.

        Errors, which the verb's call logs at the level their cause gives (DEBUG for an
        expected one, ERROR for a defect such as `RowDataError`): `AuthenticationError` (HTTP 401),
        `EntitlementError` (HTTP 403 or `NOT_AUTHORIZED`, or a `DELAYED` label on a subscription
        not in `MassiveConfig.delayed_subscriptions`, `RestReader._check_recency`),
        `RowDataError` for a row that breaks the schema, and `MassiveError` itself for any other
        failure (a rate limit as `"rate limited: ..."`, a closed session as `"... is closed"`).

        A parameter a path does not take raises the `ValueError` of `shared_params` before any
        request: every path is planned before the first one is read.
        """
        reader = self._reader(f"call-{call}")
        table, _ = reader.read_fetch(
            spec,
            request,
            symbols=request.symbols,
            cancel=_token(stop),
            pauses_live_tables=holds_update_graph_lock(self._core.exec_ctx),
            unlisted=unlisted,
        )
        return self._static(spec, table)

    def check_fetch(self, spec: DatasetSpec, request: Request, *, split_pairs: bool) -> None:
        """Plan `fetch`'s requests with no data request, so a parameter error raises now.

        A background `fetch()` calls it on the caller thread before its table exists. With
        `split_pairs`, for crypto pairs passed in code, each pair is split by the session's
        crypto ticker list (`RestReader.crypto_pair`), as `poll()` splits it at the call, so a
        pair Massive does not list raises its `UnlistedPairError` now; otherwise pairs are not
        split (`_unsplit`), and a table's pair follows `MassiveConfig.unlisted_symbols` in the
        read.

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments, with static symbols.
            split_pairs: True for symbols passed in code.

        A parameter a path does not take raises the `ValueError` of `shared_params`.
        """
        split = None if split_pairs else _unsplit
        self._reader("check").plan_fetch(spec, request, request.symbols, split=split)

    def fetch_by_symbol(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        symbols: Sequence[str],
        call: int,
        stop: threading.Event | None = None,
    ) -> dict[str, SymbolOutcome]:
        """Read `fetch` for each symbol: each one's rows, or the failure that ended its read.

        See `RestTransport.fetch_by_symbol` for what is read and returned, and
        `_ingest/symbol_reads.py` for how a failure is classed. The requests are planned with
        the crypto pairs split by Massive's ticker list, so a pair the list does not hold
        fails alone; a parameter a symbol's path does not take fails that path's symbols.
        Before the first request, one INFO line announces a read whose paced requests cannot
        all start within a minute (`rate_limits.announce`).

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments; its symbols are ignored.
            symbols: The canonical symbols to read.
            call: The call's id, which names the read's fan-out threads.
            stop: A read's stop token, or None.

        Returns:
            Each symbol, in the order given, with its outcome.

        Errors are those of `symbol_reads.read_rest_by_symbol`.
        """
        reader = self._reader(f"call-{call}")
        cancel = _token(stop)
        units, failed = fetch_units(
            reader,
            spec,
            request,
            symbols=symbols,
            columns=self._core.dataset_schema(spec).column_names,
            cancel=cancel,
        )
        announce(
            self._state.client.governor,
            f"{spec.name} fetch",
            [call_bucket(spec, rest_path.template, unit) for rest_path, unit in units],
            pauses_live_tables=holds_update_graph_lock(self._core.exec_ctx),
        )
        return read_rest_by_symbol(
            reader,
            self._core,
            spec,
            request=request,
            symbols=symbols,
            units=units,
            failed=failed,
            limit=request.limit,
            window=None,
            current=_fetch_is_current(spec, request),
            call=call,
            cancel=cancel,
            to_table=functools.partial(self._static, spec),
        )

    def fetch_by_value(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        values: Sequence[str],
        call: int,
        stop: threading.Event | None = None,
    ) -> dict[str, SymbolOutcome]:
        """Read `fetch` for each value of a value table: each one's rows, or its failure.

        See `RestTransport.fetch_by_value` for what is read and returned, and
        `symbol_reads.read_rest_by_value` for how.

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments, with a value table and static symbols.
            values: The values to read, already checked.
            call: The call's id, which names the read's fan-out threads.
            stop: A read's stop token, or None.

        Returns:
            Each value, in the order given, with its outcome.

        Errors are those of `symbol_reads.read_rest_by_value`.
        """
        return read_rest_by_value(
            self._reader(f"call-{call}"),
            self._core,
            spec,
            request=request,
            values=values,
            call=call,
            cancel=_token(stop),
            to_table=functools.partial(self._static, spec),
        )

    def history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        call: int,
        stop: threading.Event | None = None,
        days: Sequence[date] | None = None,
    ) -> Table:
        """Return the dataset's REST history over `window`, paged internally, as a static table.

        `dm.ALL` reads the grouped-daily path, one request per trading day of the window (of
        `days` only, when given)
        (`dates.is_trading_day`: weekdays, plus Sundays for forex and every day for crypto). Futures
        trades and quotes named by session dates read one request series per weekday session
        and symbol (`session_end_date=<date>`), sessions in date order and the symbols of one
        session in symbol order; Saturdays and Sundays hold no session and are skipped. Every
        other request covers the window's instants (`HistoryWindow.requested`): on the
        dataset's time axis, or as the aggregates' `{from}/{to}` millisecond epochs, one
        request series per symbol where the path takes one. Rows outside the window are
        dropped (`keep_window`), bars of a day or longer are stamped at 00:00 of their day in the
        class day zone (`StampRule.stamp`, `row_times.day_starts`; a stamp Massive is not known
        to use is kept as sent and logs one WARNING per form of stamp per session), and every
        row gets the library's time columns. A symbol with no data adds no rows. Before the
        first request, one INFO line announces a long download (`rate_limits.announce`): a
        grouped-daily or per-session download of more than five requests, and any read whose
        paced requests cannot all start within a minute, with the least time it takes. Logs
        `source=REST requests=<n>` at INFO.

        Args:
            spec: The dataset; `history()` has checked that REST can serve the request.
            request: The normalized arguments, with static symbols.
            window: The call's window (`history_window` in `_datasets/history_source.py`).
            call: The call's id, which names its fan-out threads, as in `fetch`.
            stop: A background read's stop token, as `fetch` takes it.
            days: For `dm.ALL`, the only days of the window to request, such as the days the
                flat files lack; None for every day. Explicit symbols ignore it.

        Returns:
            A static table in the dataset's schema.

        Errors: as `fetch` raises them, and `MassiveError` for a page whose `adjusted` echo
        differs from the request and for a symbol whose later page answers HTTP 404, its data
        cut short.
        """
        reader = self._reader(f"call-{call}")
        unlisted: dict[str, UnlistedPairError] = {}
        rest_path, calls, shape = _history_plan(
            reader,
            spec,
            request,
            window=window,
            split=reader.listed_split(unlisted, _token(stop)),
            days=days,
        )
        if unlisted:
            reader.skip_unlisted(spec, unlisted, verb="history()")
            rest_path, calls = _without((rest_path, calls), unlisted)
        announce(
            self._state.client.governor,
            f"{spec.name} history",
            [call_bucket(spec, rest_path.template, call) for call in calls],
            shape=shape,
            pauses_live_tables=holds_update_graph_lock(self._core.exec_ctx),
        )
        patience = reader.patience()
        table, requests = reader.read(
            spec,
            rest_path,
            calls,
            request=request,
            limit=None,
            window=window,
            cancel=_token(stop),
            patience=patience,
        )
        logger.info(
            "%s history: source=REST requests=%d, waiting out at most %.0f s of pauses",
            spec.name,
            requests,
            patience.seconds,
        )
        return self._static(spec, table)

    def check_history(
        self, spec: DatasetSpec, request: Request, window: HistoryWindow, *, split_pairs: bool
    ) -> None:
        """Plan `history`'s REST requests with no data request, so a parameter error raises now.

        The `check_fetch` of a background `history()` that REST serves or may serve.

        Args:
            spec: The dataset; `history()` has checked that REST can serve the request.
            request: The normalized arguments, with static symbols and the parsed bounds.
            window: The call's window.
            split_pairs: True for symbols passed in code, whose crypto pairs are split by the
                session's crypto ticker list now, as `check_fetch` splits them.

        A parameter the path does not take raises the `ValueError` of `shared_params`.
        """
        reader = self._reader("check")
        split = reader.crypto_pair if split_pairs else _unsplit
        _history_plan(reader, spec, request, window=window, split=split)

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
        """Read REST `history` for each symbol over the call's window, each one's own outcome.

        See `RestTransport.history_by_symbol` for what is read and returned. The requests are
        those `history` makes for the symbols on the history path (`history_calls`), with the
        crypto pairs split by Massive's ticker list; an empty window makes none, and every
        symbol has no rows. Before the first request, one INFO line announces a long download
        (`rate_limits.announce`).

        Args:
            spec: The dataset; REST can serve the request with explicit symbols.
            request: The normalized arguments, with the parsed bounds; its symbols are ignored.
            window: The call's window.
            symbols: The canonical symbols to read.
            call: The call's id, which names the read's fan-out threads.
            stop: A read's stop token, or None.

        Returns:
            Each symbol, in the order given, with its outcome.

        Errors are those of `symbol_reads.read_rest_by_symbol`.
        """
        reader = self._reader(f"call-{call}")
        cancel = _token(stop)
        units, failed = history_units(
            reader, spec, request, window=window, symbols=symbols, cancel=cancel
        )
        announce(
            self._state.client.governor,
            f"{spec.name} history",
            [call_bucket(spec, rest_path.template, unit) for rest_path, unit in units],
            shape=ReadShape.PER_SESSION if window.requested is None else ReadShape.PAGED,
            pauses_live_tables=holds_update_graph_lock(self._core.exec_ctx),
        )
        return read_rest_by_symbol(
            reader,
            self._core,
            spec,
            request=request,
            symbols=symbols,
            units=units,
            failed=failed,
            limit=None,
            window=window,
            current=False,
            call=call,
            cancel=cancel,
            to_table=functools.partial(self._static, spec),
        )

    def poll(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        interval: timedelta,
        mode: PollMode,
        query: str,
    ) -> Table:
        """Return a live table that re-reads the dataset's fetch path every `interval`.

        See `_tables/poll.py`: the table is created here, on the caller thread, and one poller
        thread of its own runs the cycles until the table is released or the session closes.

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments; a reactive symbol table is read each cycle.
            interval: The time from one cycle's start to the next.
            mode: How cycles combine.
            query: The `repr` of the query, for the `Query` column of `m.streams()`.

        Returns:
            The live table, tracked by the session.

        A parameter the caller set that a path of the call does not take raises `ValueError`
        before the table exists. With static symbols every path is planned now; with a reactive
        table, whose symbols choose the paths at each cycle, only a parameter that no fetch path
        takes fails here (`RestReader.check_parameters`), and the symbols of a path that lacks
        one are left out of each cycle and listed in the table's `Error` (`read_cycle`).
        """
        reader = self._reader("poll")
        if isinstance(request.symbols, SymbolTable) or request.value_table is not None:
            reader.check_parameters(spec, request)
            # The table's symbols choose among every fetch path at each cycle.
            candidates = (spec.fetch_path, spec.detail_path, *spec.fetch_paths_by_class.values())
            used = [path for path in candidates if path is not None]
        else:
            # A pair Massive's crypto ticker list does not hold raises now only under RAISE;
            # otherwise each cycle leaves it out (`_plan_cycle`).
            split = None if reader.unlisted_symbols is OnError.RAISE else _unsplit
            planned = reader.plan_fetch(spec, request, request.symbols, split=split)
            used = [path.template for path, _ in planned]
        host = urlsplit(self._core.config.rest_base_url).hostname
        paths = ", ".join(dict.fromkeys(used))
        return poll.open_poll(
            self._core,
            spec=spec,
            request=request,
            interval=interval,
            mode=mode,
            query=query,
            # The poller hands each cycle the `Cancel` passed as `stop` below.
            read=cast(
                "poll.CycleReader",
                functools.partial(
                    reader.read_cycle,
                    spec,
                    request,
                    pace=PaceCheck(f"{spec.name} poll()", interval.total_seconds()),
                ),
            ),
            pollers=self._state.pollers,
            host=host,
            path=paths,
            recency=_poll_recency(spec),
            note=rest_recency_note(spec, self._core.config.delayed_subscriptions),
            clock=poll.CLOCK,
            stop=Cancel(),
        )


_DELAYED_TICKERS_SHOWN: Final = 5
"""How many tickers a refusal of delayed data names before it counts the rest."""


def _delayed_what(spec: DatasetSpec, tickers: Sequence[str]) -> str:
    """Return what a refusal of delayed data says would be delayed: the dataset and tickers.

    Args:
        spec: The dataset.
        tickers: The tickers of the refused rows or request, in the order seen; empty for a
            request that names none.

    Returns:
        Such as `"stocks.snapshots data"`, `"stocks.snapshots data for AAPL"`, or
        `"stocks.snapshots data for AAPL, MSFT, NVDA, TSLA, AMZN and 3 more"`.
    """
    if not tickers:
        return f"{spec.name} data"
    shown = ", ".join(tickers[:_DELAYED_TICKERS_SHOWN])
    rest = len(tickers) - _DELAYED_TICKERS_SHOWN
    return f"{spec.name} data for {shown}" + (f" and {rest} more" if rest > 0 else "")


def _poll_recency(spec: DatasetSpec) -> Recency | None:
    """Return a `poll()` table's first `Recency`, for its `m.streams()` row.

    Args:
        spec: The dataset.

    Returns:
        `UNKNOWN` for the current data of one subscription (`DatasetSpec.current_subscription`):
        nothing is real-time until a label says so. None otherwise, which a cycle's labels
        still turn into `REALTIME` or `DELAYED` when Massive sends any (the cross-class
        snapshot does); reference, economy, and partner data never carry one.
    """
    return None if spec.current_subscription is None else Recency.UNKNOWN


def _fetch_is_current(spec: DatasetSpec, request: Request) -> bool:
    """Tell whether a `fetch()`'s pages are checked for a `DELAYED` label (`RestReader.read`).

    Args:
        spec: The dataset.
        request: The normalized arguments, whose `end` says whether the read reaches now.

    Returns:
        True for a dataset of current data, and for one whose latest values are computed
        from current data (`DatasetSpec.latest_is_current`) when the request's range reaches
        the present (`dates.reaches_now`, by the clock).
    """
    return spec.serves_current_data or (
        spec.latest_is_current and reaches_now(request.end, spec.asset_class, now=datetime.now(UTC))
    )


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

    Called once per request. The choice is logged at DEBUG once per process for each dataset,
    path template, and bucket (`log_once`), with the first request's symbols and the reason: a
    `poll()` every few seconds would otherwise log the same choice for every request. The
    planning that announces a read's buckets beforehand uses `call_bucket`, which logs nothing.

    Args:
        spec: The dataset.
        template: The request's path template.
        call: The planned request.

    Returns:
        The bucket's name (`call_bucket`).
    """
    bucket, reason = _charge(spec, template, call)
    log_once(
        logger,
        f"bucket\0{spec.name}\0{template}\0{bucket}",
        "%s %s (symbols: %s) is charged to the %s bucket: %s; later requests of this path "
        "charged to it are not logged",
        spec.name,
        template,
        ", ".join(own_symbols(spec, call.symbol, call.query)) or "none",
        bucket,
        reason,
        level=logging.DEBUG,
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
    if REFERENCE_PATH.match(template):
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
    """Log a failed host decision with exactly the text its exception carries.

    No verb call owns a host decision (every caller waiting for it shares the failure), so the
    record holds the message alone, which names the asset class, the request, and its status.
    The deciding caller raises the error, so an expected failure (a refusal, a network
    failure) logs at DEBUG; a defect logs at ERROR, a defect of the library with how to report
    it.

    Args:
        error: The failure.
    """
    level = logging.DEBUG if error.cause is ErrorCause.EXPECTED else logging.ERROR
    logger.log(level, "%s", failure_text(error))


def _history_plan(
    reader: RestReader,
    spec: DatasetSpec,
    request: Request,
    *,
    window: HistoryWindow,
    split: CryptoSplit,
    days: Sequence[date] | None = None,
) -> tuple[RestPath, list[Call], ReadShape]:
    """Plan a REST `history()`'s requests: the path, its calls, and how they are counted.

    `dm.ALL` reads the grouped-daily path, one request per trading day of the window (of
    `days` only, when given); any other request reads the history path (`history_calls`).
    Makes no request unless `split` does.

    Args:
        reader: The reader whose paths the plan names.
        spec: The dataset.
        request: The normalized arguments, with static symbols.
        window: The call's window.
        split: Splits a crypto pair into its currencies (`RestReader.crypto_pair`, or
            `_unsplit` for a check).
        days: For `dm.ALL`, the only days of the window to request; None for every day.

    Returns:
        The path, the planned calls in result order, and the read's `ReadShape`.

    A parameter the path does not take raises the `ValueError` of `shared_params`.
    """
    if isinstance(request.symbols, AllSymbols):
        rest_path = reader.rest_path(cast("str", spec.grouped_daily_path))
        path, query = shared_params(spec, rest_path, request, fixed={})
        # A class that does not trade at weekends has no grouped day on a Saturday or Sunday.
        wanted = None if days is None else set(days)
        planned = [
            day
            for day in partition_dates(spec, window)
            if is_trading_day(spec.asset_class, day) and (wanted is None or day in wanted)
        ]
        return rest_path, [Call({**path, "date": day}, query) for day in planned], ReadShape.PER_DAY
    rest_path = reader.rest_path(cast("str", spec.history_path))
    calls = history_calls(spec, rest_path, request, window=window, split=split)
    shape = ReadShape.PER_SESSION if window.requested is None else ReadShape.PAGED
    return rest_path, calls, shape


def _unsplit(symbol: str) -> tuple[str, str]:
    """Stand in for a crypto pair's split in a plan that must make no request.

    `check_fetch` and `check_history` plan requests only to raise their parameter errors at
    the call; the read itself splits each pair by Massive's ticker list.

    Args:
        symbol: A canonical crypto ticker, unused.

    Returns:
        Two empty currencies.
    """
    del symbol
    return "", ""


def _without(
    path: tuple[RestPath, list[Call]], unlisted: Mapping[str, UnlistedPairError]
) -> tuple[RestPath, list[Call]]:
    """Return a planned path without the requests of the crypto pairs left out.

    Args:
        path: A path and its planned calls.
        unlisted: The pairs left out (`RestReader._unlisted_recorder`).

    Returns:
        The path with only the calls of the other symbols.
    """
    rest_path, calls = path
    return rest_path, [call for call in calls if call.symbol not in unlisted]


def _token(stop: threading.Event | None) -> Cancel | None:
    """Return a background read's stop token as the wire layer's `Cancel` it is.

    The ports type the token as the `threading.Event` the tables layer may name;
    `SessionClients.read_stop` makes every one a `Cancel`.

    Args:
        stop: The token, or None.

    Returns:
        The same object, typed as a `Cancel`.
    """
    return cast("Cancel | None", stop)
