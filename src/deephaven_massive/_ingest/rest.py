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
  indicator) it caps each symbol's rows instead, and for a dataset whose symbols overlap
  (`DatasetSpec.symbols_overlap`, the news of a watchlist) each request's rows, keeping a row
  several requests return once (`_first_copies`).
- **History.** `history()` requests the instants its window covers
  (`HistoryWindow.requested`) and keeps the rows the window keeps (`keep_window`). Futures trades
  and quotes named by session dates are requested one session at a time
  (`session_end_date=<date>`, weekdays only); `dm.ALL` daily bars one grouped day at a time
  (weekdays only for the classes that do not trade at weekends). Bars of a day or longer
  are stamped at 00:00 of their day in the class day zone, and every bar is served: a stamp
  Massive is not known to use logs one WARNING per dataset and timespan per session
  (`row_times.stamp_rule`, `stamped`).
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
  with status 429 (the first 429 of each subscription has already logged its INFO notice). One
  odd row never fails a read: a value that does not fit is null and a row whose key cannot be
  read is dropped, both reported as drift; a page whose every row was dropped raises
  `RowDataError`, a defect of the library. Static verbs raise each error, and log it at DEBUG,
  or at ERROR for a defect.
- **Host decisions.** `resolve_host` decides each WebSocket host once per session
  (`_ingest/host_decision.py`), single-flight: the first caller of a (class, family) makes the one
  REST request and every concurrent caller waits for its answer.
- **Shared lookups.** The host decisions and the session's crypto ticker list are each made by
  one caller while the others wait (`SharedLookup.claim_or_wait`). A waiting caller charges the
  lookup's pauses to its own patience and gives up when they outlast it; if the caller making
  the lookup gives up or is interrupted, each waiting caller tries again with what it has left.

`RestState` is the part a session shares (the client, the host decisions, the live pollers,
the once-per-session warnings); `SessionClients` creates it on first use and registers its
close with the session. `RestEndpoints`, the session core's `RestTransport`, is a lightweight
view of the core and that state, made for each call. `poll()` itself is `_tables/poll.py`.
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
import threading
import time
from collections import Counter
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from typing import TYPE_CHECKING, Final, TypeAlias, cast
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.catalog import REFERENCE_PATH
from deephaven_massive._datasets.dates import day_class, is_trading_day, reaches_now
from deephaven_massive._datasets.history_source import partition_dates, session_date_param
from deephaven_massive._datasets.row_times import stamp_rule, time_rule
from deephaven_massive._datasets.symbols import SymbolTable, symbol_class
from deephaven_massive._enums import AssetClass, OnError, Recency, Subscription, subscription_of
from deephaven_massive._errors import (
    LIBRARY_BUG,
    EntitlementError,
    ErrorCause,
    MassiveError,
    PermanentError,
    UnsupportedParameterError,
    cause_of,
    failure_text,
    from_wire,
    library_bug,
)
from deephaven_massive._ingest import host_decision
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
    RowConverter,
    keep_window,
    stamped,
    whole_bars,
)
from deephaven_massive._ingest.shared_lookup import Claim, SharedLookup
from deephaven_massive._log_limits import bounded_list, log_once
from deephaven_massive._massive import Cancel, DriftReport, Transport, WireError
from deephaven_massive._tables import poll
from deephaven_massive._tables.columns import dataset_table, rest_recency_note
from deephaven_massive._tables.diagnostics import ConnectionRow, ConnectionState
from deephaven_massive._tables.execution_context import entered, holds_update_graph_lock
from deephaven_massive._tables.poll import Pollers
from deephaven_massive._types import _All

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
    from deephaven_massive._tables.transports import HostDecision

__all__ = [
    "RestEndpoints",
    "RestReader",
    "RestState",
    "call_bucket",
]

logger = logging.getLogger(__name__)

_ROW_NUMBER: Final = "__row_number"
"""The scratch column `_first_copies` numbers rows in; no dataset column starts with `_`."""


_TICKERS_PATH: Final = "/v3/reference/tickers"
"""The reference path that lists crypto tickers with their base and quote currencies."""
_TICKERS_PAGE: Final = 1000
"""The rows per page of the crypto ticker list: the endpoint's largest page."""
_CRYPTO_KEY: Final = "crypto tickers"
"""The key of the session's one crypto ticker list in `RestState.crypto`."""
_TICKERS_REREAD: Final = 3600.0
"""The seconds after which a ticker the crypto list does not hold makes the session read the list
again, so a pair Massive lists later is found without a restart, at most once an hour."""


CryptoTickers: TypeAlias = "Mapping[str, tuple[str, str] | str]"
"""Massive's crypto tickers: each one's `(base, quote)`, or why its currencies cannot be used."""

_FACTS_LIMIT: Final = 10_000
"""The most facts a session remembers having warned about (`RestState.first`); past it, it
forgets them all and starts over, so a long `poll()` over many symbols keeps its memory bounded,
at the cost of warning about an old fact once more."""

_ANSWER_SHOWN: Final = 200
"""The most characters of one error row's answer a WARNING shows (`_cut`); the row's DEBUG
line shows it whole."""


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
    data gives; the session's crypto ticker list, read again on a miss at most once an hour,
    does not list the ticker, or gives currencies that do not join to it
    (`RestReader.crypto_pair`). A `poll()` table fails with it.
    """


def _first_copies(rows: pa.Table, keys: Sequence[str]) -> pa.Table:
    """Drop the repeats of a row that several symbols' requests returned, keeping its first.

    A row is identified by `keys` alone. A row with a null key is never merged with another:
    a null cannot tell two rows apart, so every such row is kept, in place.

    Args:
        rows: The rows of every request, in request order.
        keys: The columns that identify a row (`DatasetSpec.poll_keys`).

    Returns:
        The rows, each identity once, in the order of their first copies.
    """
    numbered = rows.append_column(_ROW_NUMBER, pa.array(range(rows.num_rows), pa.int64()))
    keyed = functools.reduce(pc.and_, (pc.is_valid(rows[key]) for key in keys))
    identified = numbered.filter(keyed)
    firsts = identified.group_by(list(keys), use_threads=False).aggregate([(_ROW_NUMBER, "min")])
    unkeyed = numbered.filter(pc.invert(keyed))[_ROW_NUMBER]
    kept = pa.chunked_array([*firsts[f"{_ROW_NUMBER}_min"].chunks, *unkeyed.chunks], pa.int64())
    return rows.take(kept.sort())


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
        crypto: Massive's list of crypto tickers with their two currencies, under
            `_CRYPTO_KEY` (`RestReader.crypto_pair`): read on first need, and read again when
            a ticker it does not hold is asked for and it is an hour old or more.
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
        self.crypto: Final[SharedLookup[str, CryptoTickers]] = SharedLookup(label, client.governor)
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
        """Return a crypto ticker's base and quote currencies, with no guess.

        A joined ticker does not say where its quote starts (`X:DOTUSD` could be `DOT-USD` or
        `DO-TUSD`; `X:ETHWBTC` ends in `BTC` but quotes in `WBTC`), so every split comes from
        Massive's reference data: the session reads Massive's whole list of crypto tickers
        once, on first need (`/v3/reference/tickers?market=crypto`, every page, first with
        `active=true` and then with `active=false`, so a delisted pair splits too), keeping
        each ticker's `base_currency_symbol` and `currency_symbol`, whose join must be the
        ticker. Its requests count against Massive's limit on reference data, so on a
        Basic subscription they wait their turn. Blocks the calling thread for that read, or
        while another thread makes it, sharing that read rather than starting a second one; a
        caller that shares it still waits out no more of its pauses than its own patience
        covers (`SharedLookup.claim_or_wait`). Holds no lock meanwhile. A failed read is not
        kept, so the next call tries again. A read that succeeds is kept, even when it lists
        no pair; a ticker it does not hold makes the session read the list again before
        refusing it, when the list is an hour old or more (`SharedLookup.retire`), so a pair
        Massive lists later is found without a restart, and the list is read at most once an
        hour.

        Args:
            symbol: A canonical crypto ticker, such as `X:DOTUSD`.
            cancel: A background read's stop token: once it is set, the list's pages and the
                wait for another caller's read of it stop, and the call raises. None when only
                the session's close should stop them.

        Returns:
            `(base, quote)`, such as `("DOT", "USD")`.

        Raises:
            UnlistedPairError: If the list does not hold the ticker, or its currencies do not
                join to the ticker; a `poll()` table fails with it.

        A failed read raises its public error (`AuthenticationError`, `EntitlementError`, or
        `MassiveError`), and so does a stopped one (`"... cancelled"`). Every message names the
        ticker; nothing falls back to a guessed split.
        """
        where = f"{symbol!r} cannot be split into its two currencies"
        tickers = self._listed_crypto(where, cancel)
        entry = tickers.get(symbol)
        if entry is None and self.state.crypto.retire(_CRYPTO_KEY, tickers, _TICKERS_REREAD):
            logger.info(
                "%s: Massive's crypto ticker list does not hold %s; reading the list again, "
                "since it is an hour old or more",
                self.state.label,
                symbol,
            )
            tickers = self._listed_crypto(where, cancel)
            entry = tickers.get(symbol)
        if entry is None:
            raise UnlistedPairError(f"{where}: Massive's reference data does not list it")
        if isinstance(entry, str):
            raise UnlistedPairError(f"{where}: {entry}")
        return entry

    def _listed_crypto(self, where: str, cancel: Cancel | None) -> CryptoTickers:
        """Return the session's crypto ticker list for `crypto_pair`, naming the ticker on failure.

        Args:
            where: The start of every message: which ticker cannot be split.
            cancel: The caller's stop token, or None.

        Returns:
            The list (`_crypto_tickers`).

        Raises:
            MassiveError: The failed read's public error, of the same type, status, and
                request id, its message prefixed with `where`.
        """
        try:
            return self._crypto_tickers(cancel)
        except MassiveError as public:
            raise type(public)(
                f"{where}: reading Massive's crypto tickers failed: {public}",
                status=public.status,
                request_id=public.request_id,
                cause=public.cause,
            ) from public

    def _crypto_tickers(self, cancel: Cancel | None) -> CryptoTickers:
        """Return the session's crypto ticker list, reading it on first need.

        The first caller reads the list; a caller that arrives meanwhile waits for that read
        within its own patience (`SharedLookup.claim_or_wait`). If the caller reading it stops
        for a reason of its own (it gave up on a pause, was interrupted, or its read was
        stopped), each waiting caller tries again with the patience it has left: one of them
        reads the list, and the others wait on it.

        Args:
            cancel: The caller's stop token, which its pages and its wait listen to; None for
                none.

        Returns:
            Each crypto ticker Massive lists: its `(base, quote)`, or why its currencies
            cannot be used.

        A failed read raises its public `MassiveError` (`AuthenticationError`,
        `EntitlementError`), and every caller that waited on it a copy of it; an unexpected
        error of the reader, a defect, is logged at ERROR with how to report it and propagates
        to it, and its waiters get a `MassiveError` naming it.
        A waiting caller that gives up raises the error `SharedLookup.claim_or_wait` describes.
        """
        known = self.state.crypto.known(_CRYPTO_KEY)
        if known is not None:
            return known  # a list already read needs no patience (a Java call)
        # Taken before claiming the read: a claim is ended only by the block that reads.
        patience = self.patience()
        claim = self.state.crypto.claim_or_wait(
            _CRYPTO_KEY,
            bucket=OTHER,
            patience=patience,
            what="the crypto ticker list",
            public=self.public_error,
            cancel=cancel,
        )
        if not isinstance(claim, Claim):
            return claim
        # The claim is ended whatever stops the read, an interrupt (as by Ctrl-C) as it
        # starts included: a claim left open would hang every later caller.
        try:
            tickers = self._read_crypto_tickers(patience, cancel)
        except WireError as error:
            public = self.public_error(error)
            # A stop ends a request with an error of no status; one with a status is Massive's
            # answer, a refusal the waiters copy even when the stop came at the same moment.
            stopped = cancel is not None and cancel.is_set() and error.status is None
            logger.debug(
                "%s: reading the crypto ticker list failed (status %s, stop set %s); its "
                "waiters %s: %s",
                self.state.label,
                error.status,
                cancel is not None and cancel.is_set(),
                "try again" if stopped else "get a copy of the error",
                public,
            )
            self.state.crypto.fail(claim, public, "the read failed", stopped=stopped)
            raise public from error
        except BaseException as error:
            # The reader's own unexpected error propagates; its waiters get a MassiveError.
            self.state.crypto.fail(claim, error, "the read failed")
            if cause_of(error) is ErrorCause.LIBRARY_DEFECT:
                logger.exception(
                    "%s: reading Massive's crypto tickers failed. %s", self.state.label, LIBRARY_BUG
                )
            raise
        self.state.crypto.complete(claim, tickers)
        return tickers

    def _read_crypto_tickers(self, patience: Patience, cancel: Cancel | None) -> CryptoTickers:
        """Read Massive's whole crypto ticker list, every page of both lists.

        Args:
            patience: The reading caller's patience, for every page.
            cancel: The reading caller's stop token, which every page listens to; None for
                none.

        Returns:
            The list.

        A failed request raises its `WireError`; `_crypto_tickers` completes the shared
        claim either way.
        """
        tickers: dict[str, tuple[str, str] | str] = {}
        # The list is reference data, which Massive counts in a bucket of its own. Its pages
        # share one lane of their own, so they wait their turn with other reads instead of
        # queueing behind them; closing the client, or setting the caller's stop token, still
        # ends their waits.
        with Cancel(*(() if cancel is None else (cancel,))) as lane:
            # Listed pairs first, then delisted ones, so that a past date of a delisted pair
            # still splits; a ticker in both keeps its listed entry.
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
        current: bool = False,
        on_recency: Callable[[Recency], None] | None = None,
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
            window: The `history()` window whose rows to keep, or None. Bars of a day or longer
                move to 00:00 of their day in the class day zone, and a stamp Massive is not
                known to use logs one WARNING per dataset and timespan per session
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

        Returns:
            The rows, in call order and then page order, and the number of requests made.

        Errors: the public error of each `WireError` (`public_error`), `RowDataError` for a page
        whose every row was dropped, and the `MassiveError` of `_call` for a page on another
        adjustment basis or a symbol whose data was cut short, and the `EntitlementError` of
        `_check_recency` for delayed data. A closed session raises
        `MassiveError("... is closed")`.
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
        # One WARNING per read, naming each symbol once per session, however many there are.
        new = [symbol for symbol in missing if self.state.first("missing", spec.name, symbol)]
        if new:
            logger.warning(
                "%s: no data for %s: Massive answered HTTP 404 from %s, so they are left out",
                spec.name,
                bounded_list(new),
                rest_path.template,
            )
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
        current: bool,
        on_recency: Callable[[Recency], None] | None,
        record: Callable[[DriftReport], None] | None,
        stop: Cancel,
        patience: Patience,
    ) -> _Outcome:
        """Request one call's pages and convert their rows.

        Each page of current data is checked for a `DELAYED` label (`_check_recency`), each
        page against the requested adjustment basis (`_check_adjusted`), its
        bars of a day or longer are stamped (`stamped`; a stamp Massive is not known to use is
        served and logs one WARNING per dataset and timespan per session), its rows outside the
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
            MassiveError: If a per-symbol call's later page answers HTTP 404, so the symbol's
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
        stamps = stamp_rule(spec, request, warnings=self.warnings, asset_class=zone_class)
        times = time_rule(spec, request, asset_class=zone_class, warnings=self.warnings)
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
                # Stamp before keeping: `keep_window` judges a bar by its day's start, and an
                # unexpected stamp outside the window still logs its WARNING.
                if stamps is not None:
                    batches = [stamped(spec, template, batch, rule=stamps) for batch in batches]
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
            if error.status == HTTPStatus.NOT_FOUND and call.symbol is not None:
                # A 404 that arrives after the session closed is no answer about the symbol,
                # so the call reports the close rather than dropping a symbol with no data.
                if self.state.closed:
                    logger.debug(
                        "%s: HTTP 404 for %s from %s arrived after %s closed; the call "
                        "reports the close",
                        spec.name,
                        call.symbol,
                        template,
                        self.state.label,
                    )
                    raise self.public_error(error) from error
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
            raise self._dataset_error(spec, bucket, error) from error
        return _Outcome(tuple(kept), requests)

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
        delayed = page.status == DELAYED_LABEL and bool(named)
        real_time = False
        # Every refused row of the page, so the message names its tickers, not only the first:
        # each subscription's tickers in the order seen, each once, and its first evidence.
        refused: dict[Subscription, tuple[dict[str, None], str]] = {}
        for row in page.rows:
            labels = read_labels("", [row])
            real_time = real_time or labels.real_time is not None
            if labels.delayed is None and labels.unknown is None:
                continue
            ticker = row.get(symbol_key) if symbol_key is not None else call.symbol
            subscription = fixed or (
                subscription_of(symbol_class(ticker)) if isinstance(ticker, str) else None
            )
            if labels.unknown is not None:
                self._warn_unknown_label(
                    spec, template=template, subscription=subscription, unknown=labels.unknown
                )
            if labels.delayed is None or subscription is None:
                continue
            if subscription is not Subscription.CURRENCIES and subscription not in listed:
                tickers, _ = refused.setdefault(subscription, ({}, labels.delayed))
                if isinstance(ticker, str):
                    tickers[ticker] = None
            delayed = True
        if refused:
            subscription = min(refused)
            tickers, evidence = refused[subscription]
            refusal = delayed_refusal(
                subscription, _delayed_what(spec, list(tickers)), evidence, streams=False
            )
            raise refusal
        if on_recency is not None and (delayed or real_time):
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
        record: Callable[[DriftReport], None],
        on_recency: Callable[[Recency], None],
        executor: ThreadPoolExecutor,
        cancel: Cancel,
        pace: PaceCheck,
    ) -> tuple[pa.Table, int, float]:
        """Run one `poll()` cycle: the planned requests of `read_fetch` for the cycle's symbols.

        A reactive symbol table can route a new symbol to a path that lacks a parameter the
        caller set (`adjusted=False` on the crypto indicators). Retrying cannot fix that,
        so its `ValueError` becomes `UnsupportedParameterError`, which fails the table. Before
        reading, the cycle's requests per bucket go to `pace`, which warns once when the paced
        ones outlast the interval.

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
            The cycle's rows, its request count, and the seconds of rate-limit pauses its
            reads wait out (`patience()`, taken on the poller's thread before any fan-out).

        Raises:
            UnsupportedParameterError: If a path of the cycle does not take a parameter the
                caller set.

        Any other error is that of `read_fetch`.
        """
        try:
            planned = self.plan_fetch(spec, request, symbols, cancel=cancel)
        except ValueError as error:
            raise UnsupportedParameterError(str(error)) from error
        pace.check(self.state.client.governor, Counter(_buckets(spec, planned)))
        patience = self.patience()  # once for every path, on this thread, before any fan-out
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
        )
        return table, requests, patience.seconds

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
        planned = []
        for template, own in fetch_paths(spec, symbols):
            rest_path = self.rest_path(template)
            calls = plan_calls(
                spec,
                rest_path,
                request,
                symbols=own,
                dates=(request.start, request.end),
                split=functools.partial(self.crypto_pair, cancel=cancel)
                if split is None
                else split,
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
        cancel: Cancel | None = None,
        pauses_live_tables: bool = False,
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
                console command), so the read pauses every live table of the worker while it
                runs.

        Returns:
            The rows of every path, in path order, capped at `Request.limit` (each symbol's
            rows where a request names one symbol, each request's rows for a dataset whose
            symbols overlap; `_read_planned`), and the request count.

        Errors are those of `read`, and a parameter a path does not take raises the
        `ValueError` of `shared_params`.
        """
        planned = self.plan_fetch(spec, request, symbols, cancel=cancel)
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
            current=spec.serves_current_data
            or (
                spec.latest_is_current
                and reaches_now(request.end, spec.asset_class, now=datetime.now(UTC))
            ),
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
    ) -> tuple[pa.Table, int]:
        """Make planned calls, path by path, and cap the rows at `Request.limit`.

        The cap is on the total rows, except where each request names one symbol
        (`Call.symbol`, a per-symbol path): there it caps each symbol's rows, so a few symbols
        with many rows never crowd out the rest. For a dataset whose symbols overlap
        (`DatasetSpec.symbols_overlap`), such as the news of a watchlist, each request is
        capped too, and a row several requests returned is kept once, at its first copy
        (`_first_copies`).

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
            )
            tables.append(table)
            requests += made
        if not tables:
            schema = self.catalog.dataset_schema(
                spec, raw_symbol=self.unreadable_symbols is not OnError.RAISE
            )
            return schema.arrow_schema().empty_table(), 0
        rows = pa.concat_tables(tables)
        if spec.symbols_overlap:
            # Each request was capped on its own, so one symbol's rows never crowd out another's.
            kept = _first_copies(rows, spec.poll_keys)
            logger.debug(
                "%s: kept %d of %d rows from %d requests, each row once by %s; limit=%s per "
                "request",
                spec.name,
                kept.num_rows,
                rows.num_rows,
                requests,
                ", ".join(spec.poll_keys),
                request.limit,
            )
            return kept, requests
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
                ws_hosts=self._core.config.ws_hosts,
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
        self, spec: DatasetSpec, request: Request, *, call: int, stop: threading.Event | None = None
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
        )
        return self._static(spec, table)

    def check_fetch(self, spec: DatasetSpec, request: Request) -> None:
        """Plan `fetch`'s requests with none made, so a parameter error raises now.

        A background `fetch(wait=False)` calls it on the caller thread before its table
        exists. Crypto pairs are not split (`_unsplit`): the read splits them, and a pair
        Massive does not list fails the read's table instead.

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments, with static symbols.

        A parameter a path does not take raises the `ValueError` of `shared_params`.
        """
        self._reader("check").plan_fetch(spec, request, request.symbols, split=_unsplit)

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
        class day zone (`stamped`, `row_times.day_starts`; a stamp Massive is not known to use
        is served and logs one WARNING per dataset and timespan per session), and every row gets
        the library's
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
        rest_path, calls, shape = _history_plan(
            reader,
            spec,
            request,
            window=window,
            split=functools.partial(reader.crypto_pair, cancel=_token(stop)),
            days=days,
        )
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

    def check_history(self, spec: DatasetSpec, request: Request, window: HistoryWindow) -> None:
        """Plan `history`'s requests with none made, so a parameter error raises now.

        The `check_fetch` of a background `history(..., wait=False)` served by REST.

        Args:
            spec: The dataset; `history()` has checked that REST can serve the request.
            request: The normalized arguments, with static symbols and the parsed bounds.
            window: The call's window.

        A parameter the path does not take raises the `ValueError` of `shared_params`.
        """
        _history_plan(self._reader("check"), spec, request, window=window, split=_unsplit)

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
    if isinstance(request.symbols, _All):
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
