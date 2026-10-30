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
  (`HistoryWindow.requested`) and keeps the rows the window keeps (`_keep`). Futures trades
  and quotes named by session dates are requested one session at a time
  (`session_end_date=<date>`, weekdays only); `dm.ALL` daily bars one grouped day at a time
  (weekdays only for the classes that do not trade at weekends). Bars of a day or longer
  are stamped at 00:00 of their day in the class day zone, and a stamp Massive is not known to
  use raises (`row_times.stamp_rule`, `_stamped`).
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
  generated fields), then `schema.rename`'s column mapping for the dataset, the timestamp
  routine of `_datasets/timestamps.py`, and the lattice widening of `schema.widen`, into one
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
  (`_ingest/hosts.py`), single-flight: the first caller of a (class, family) makes the one
  REST request and every concurrent caller waits for its answer.
- **Shared lookups.** The host decisions and the session's crypto ticker list are each made by
  one caller while the others wait (`RestReader.wait_for_shared`). A waiting caller charges the
  lookup's pauses to its own patience and gives up when they outlast it; if the caller making
  the lookup gives up or is interrupted, each waiting caller tries again with what it has left.

`RestState` is the part a session shares (the client, the host decisions, the live pollers,
the once-per-session warnings); `SessionClients` creates it on first use and registers its
close with the session. `RestEndpoints`, the session core's `RestTransport`, is a lightweight
view of the core and that state, made for each call. `poll()` itself is `_ingest/poll.py`.
"""

from __future__ import annotations

import functools
import logging
import math
import re
import threading
import time
from collections import Counter
from concurrent.futures import FIRST_EXCEPTION, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from http import HTTPStatus
from typing import TYPE_CHECKING, Final, TypeAlias, TypeVar, cast
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.catalog import filter_key, takes_instants
from deephaven_massive._datasets.dates import day_class, instant_bound, is_trading_day
from deephaven_massive._datasets.history_source import partition_dates, session_date_param
from deephaven_massive._datasets.row_times import (
    DATE,
    RowTimes,
    StampRule,
    stamp_rule,
    time_column,
    time_rule,
    within,
)
from deephaven_massive._datasets.schema import (
    OPTION_ROOT,
    RAW_SYMBOL,
    rename,
    required_fields,
    widen,
)
from deephaven_massive._datasets.symbols import (
    SymbolTable,
    WireSymbols,
    forex_codes,
    option_roots,
    symbol_class,
    unknown_symbol_message,
)
from deephaven_massive._datasets.timestamps import (
    UnitMismatchError,
    instants,
)
from deephaven_massive._enums import (
    AssetClass,
    ClassFromPrefix,
    Timespan,
    UnknownSymbols,
    subscription_of,
)
from deephaven_massive._errors import MassiveError, PermanentError, from_wire
from deephaven_massive._ingest import hosts, poll
from deephaven_massive._ingest.poll import Pollers, RowDataError, UnsupportedParameterError
from deephaven_massive._ingest.rate_limits import (
    OTHER,
    PaceCheck,
    ReadShape,
    announce,
    bucket_of,
    read_patience,
)
from deephaven_massive._massive import (
    Cancel,
    DriftReport,
    ParamKind,
    Transport,
    WireError,
    to_batch,
)
from deephaven_massive._tables.columns import dataset_table
from deephaven_massive._tables.diagnostics import ConnectionRow, ConnectionState
from deephaven_massive._tables.execution_context import entered, holds_update_graph_lock
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.dates import DaySpan, TimeRange
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._datasets.schema import ColumnType, SourceColumn
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._enums import ChannelFamily, PollMode
    from deephaven_massive._massive import DriftSample, Patience, RestClient, RestPath
    from deephaven_massive._tables.diagnostics import DriftRegistry
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._tables.transports import HostDecision

__all__ = [
    "Call",
    "RestEndpoints",
    "RestReader",
    "RestState",
    "call_bucket",
    "fetch_paths",
    "plan_calls",
]

logger = logging.getLogger(__name__)

_FUTURES_UNITS: Final[Mapping[Timespan, str]] = {
    Timespan.SECOND: "sec",
    Timespan.MINUTE: "min",
    Timespan.HOUR: "hour",
    Timespan.SESSION: "session",
    Timespan.WEEK: "week",
    Timespan.MONTH: "month",
    Timespan.QUARTER: "quarter",
    Timespan.YEAR: "year",
}
"""The unit of the futures aggregates' `resolution` parameter (`"5min"`) for each timespan; the
endpoint has no day unit, so `Timespan.DAY` is rejected when the query is built."""

_ONE_DAY: Final = timedelta(days=1)
_ONE_MICROSECOND: Final = timedelta(microseconds=1)
_ROW_NUMBER: Final = "__row_number"
"""The scratch column `_first_copies` numbers rows in; no dataset column starts with `_`."""


_TICKERS_PATH: Final = "/v3/reference/tickers"
"""The reference path that lists crypto tickers with their base and quote currencies."""
_TICKERS_PAGE: Final = 1000
"""The rows per page of the crypto ticker list: the endpoint's largest page."""

CryptoSplit: TypeAlias = "Callable[[str], tuple[str, str]]"
"""Splits a crypto ticker into its base and quote currencies (`RestReader.crypto_pair`)."""

CryptoTickers: TypeAlias = "Mapping[str, tuple[str, str] | str]"
"""Massive's crypto tickers: each one's `(base, quote)`, or why its currencies cannot be used."""

T = TypeVar("T")
"""The outcome of a shared lookup (`RestReader.wait_for_shared`)."""


class _AbandonedError(Exception):
    """What a shared lookup's future holds when the caller making it stopped for its own reason.

    That caller gave up on a pause its own patience could not cover, or was interrupted (as
    by Ctrl-C). Neither says anything about the lookup, so no caller waiting on it may fail
    with it: each tries again with the patience it has left (`RestReader.wait_for_shared`).
    Only ever set on a future, never raised; its text is the maker's error, for the log.
    """


def _shared_failure(error: BaseException, context: str) -> BaseException:
    """Return what a failed shared lookup's future holds for the callers waiting on it.

    Args:
        error: What stopped the caller making the lookup: its public error, or the exception
            that interrupted it.
        context: What failed, for an unexpected error's message, such as `"the read failed"`.

    Returns:
        `_AbandonedError` when the maker stopped for its own reason: a `MassiveError` with
        status 429 (its patience ran out; the client never gives up on a 429 otherwise) or an
        interruption that is not an `Exception` (`KeyboardInterrupt`). Otherwise the
        `MassiveError` itself, or for any other error a `MassiveError` naming it, which every
        waiter raises a copy of (`_outcome`).
    """
    if isinstance(error, MassiveError):
        if error.status == HTTPStatus.TOO_MANY_REQUESTS:
            return _AbandonedError(str(error))
        return error
    if not isinstance(error, Exception):
        return _AbandonedError(repr(error))
    return MassiveError(f"{context}: {error!r}")


def _outcome(future: Future[T]) -> T:
    """Return a completed shared lookup's result, or raise a copy of its error.

    Args:
        future: A completed future that holds a result or a `MassiveError`.

    Returns:
        The result.

    A failed lookup raises a copy of the maker's `MassiveError` (its type, text, status, and
    request id), so that no exception object is raised on two threads at once.
    """
    try:
        return future.result()
    except MassiveError as error:
        raise type(error)(str(error), status=error.status, request_id=error.request_id) from None


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
class Call:
    """One planned request of a REST path, which may span several pages.

    Attributes:
        path_params: A value for each placeholder of the path template.
        query: The query parameters of the first page, by wire name.
        symbol: The one symbol of a per-symbol request: it fills the keys the dataset takes
            from the request (`KeySpec.from_request`), and HTTP 404 for it drops the symbol.
            None for a request that is not per symbol.
    """

    path_params: Mapping[str, object]
    query: Mapping[str, object]
    symbol: str | None = None


def _date_bound(name: str, value: date, *, upper: bool) -> date:
    """Return the day a date parameter of a calendar-date time axis is sent.

    A bound is a whole day: the day itself, the day after for an exclusive upper `.lt`, the
    day before for an exclusive lower `.gt`. A datetime never reaches here: a calendar-date
    axis refuses one when the query is built.

    Args:
        name: The parameter's wire name, such as `"ex_dividend_date.gte"`.
        value: The bound.
        upper: True for the upper bound.

    Returns:
        The day to send.
    """
    suffix = name.rpartition(".")[2]
    if upper and suffix == "lt":
        return value + _ONE_DAY
    if not upper and suffix == "gt":
        return value - _ONE_DAY
    return value


def _axis_query(
    spec: DatasetSpec,
    rest_path: RestPath,
    *,
    dates: tuple[date | None, date | None],
    asset_class: AssetClass | ClassFromPrefix,
) -> dict[str, object]:
    """Return the time-axis parameters of a request, by wire name.

    An instant parameter (`catalog.takes_instants`: a timestamp, or a date parameter whose
    row field is an instant, such as `published_utc`) gets an instant
    (`dates.instant_bound`): a date bound becomes the start of its day in the request's day
    zone, an upper one the start of the next day, so a date never leaves the day boundary to
    Massive. A date parameter of a calendar-date field gets a whole day (`_date_bound`), and
    a parameter of any other kind the bound's ISO-8601 text.

    Args:
        spec: The dataset, whose `time_axis` names the lower and upper parameters.
        rest_path: The path; a name it does not take is left out.
        dates: The bounds: as the caller gave them (a `date` or an aware `datetime`, or None
            for an open end), or the exact instants a `history()` window requests. A
            calendar-date axis only ever receives dates (`build_query` refuses datetimes
            there).
        asset_class: The class whose day rules apply to this request (`dates.day_class`), or
            `ClassFromPrefix.ANY` for New York days.

    Returns:
        The parameters to send; empty when the dataset has no time axis.

    Raises:
        ValueError: If a `.gt` or `.lte` bound of an instant axis counts nanoseconds or takes
            ISO-8601 text, which no `datetime` step ends exactly (`dates.instant_bound`); the
            message names the dataset.
    """
    if spec.time_axis is None:
        return {}
    params = {p.name: p for p in rest_path.params}
    query: dict[str, object] = {}
    for index, name in enumerate(spec.time_axis):
        upper = index == 1
        param = params.get(name)
        value = dates[index]
        if param is None or value is None:
            continue
        if takes_instants(rest_path, name, param.kind):
            try:
                query[name] = instant_bound(asset_class, name, value, unit=param.unit, upper=upper)
            except ValueError as error:
                raise ValueError(f"{spec.name}: {error}") from None
        elif param.kind is ParamKind.DATE:
            query[name] = _date_bound(name, value, upper=upper)
        else:
            query[name] = value.isoformat()
    return query


def _filter_days(
    rest_path: RestPath, query: Mapping[str, object], asset_class: AssetClass | ClassFromPrefix
) -> dict[str, object]:
    """Return the query with each date on an instant range filter as its day's bound.

    `Catalog.check_filters` keeps a date given to a range filter of a parameter that compares
    instants (`catalog.takes_instants`, such as `last_updated.gte`), because its day zone
    depends on the request's symbols. Here it becomes the instant that keeps the filter's
    calendar meaning (`dates.instant_bound`): `.gte` a day is from its start, `.lt` a day
    before its start, `.gt` a day after its end, and `.lte` a day up to its end. The check
    has refused the `.gt` and `.lte` forms whose unit cannot end a day exactly. Every other
    value is kept.

    Args:
        rest_path: The path, whose parameters and fields are read.
        query: The query parameters, by wire name.
        asset_class: The class whose day rules apply (`dates.day_class`), or
            `ClassFromPrefix.ANY` for New York days.

    Returns:
        A new mapping.
    """
    params = {p.name: p for p in rest_path.params}
    sent = dict(query)
    for name, value in query.items():
        param = params.get(name)
        suffix = name.rpartition(".")[2]
        if (
            param is None
            or suffix not in ("gt", "gte", "lt", "lte")
            or not isinstance(value, date)
            or isinstance(value, datetime)
            or not takes_instants(rest_path, name, param.kind)
        ):
            continue
        sent[name] = instant_bound(
            asset_class, name, value, unit=param.unit, upper=suffix in ("gt", "lte")
        )
    return sent


def _pair(spec: DatasetSpec, symbol: str, split: CryptoSplit) -> tuple[str, str]:
    """Split a forex or crypto symbol into the two currencies a `{from}/{to}` path takes.

    Args:
        spec: The dataset, whose asset class is forex or crypto.
        symbol: The canonical symbol, such as `C:EURUSD` or `X:BTCUSD`.
        split: Splits a crypto symbol by Massive's crypto ticker list
            (`RestReader.crypto_pair`); a forex pair never needs it, since its two codes have
            three letters each.

    Returns:
        The base and quote currencies, such as `("EUR", "USD")`.

    Raises:
        ValueError: If the dataset's class has no currency pairs.

    A crypto symbol `split` cannot split raises its `MassiveError`; no split is ever guessed.
    """
    asset_class = spec.asset_class
    if asset_class is not AssetClass.FOREX and asset_class is not AssetClass.CRYPTO:
        raise ValueError(f"{spec.name}: only forex and crypto symbols are currency pairs")
    if asset_class is AssetClass.CRYPTO:
        return split(symbol)
    return forex_codes(symbol)


def _own_symbols(
    spec: DatasetSpec, symbol: str | None, own_query: Mapping[str, object]
) -> tuple[str, ...]:
    """Return the symbols one planned request carries.

    Args:
        spec: The dataset, whose `symbol_param` holds a chunk of symbols.
        symbol: The one symbol of a per-symbol request, or None.
        own_query: The request's own query parameters (`_symbol_parts`).

    Returns:
        The symbol of a per-symbol request, the chunk in the symbol parameter of a listed
        request, or nothing for a request without symbols.
    """
    if symbol is not None:
        return (symbol,)
    chunk = own_query.get(spec.symbol_param) if spec.symbol_param is not None else None
    return chunk if isinstance(chunk, tuple) else ()


def _symbol_targets(keys: list[str], *, slot: str | None, pair: bool) -> list[str]:
    """Return where each per-symbol request puts its symbol.

    Args:
        keys: The parameters of the keys the dataset fills from the request.
        slot: The path's one placeholder nothing else fills, or None.
        pair: True when an otherwise unfilled `{from}/{to}` pair takes the symbol.

    Returns:
        The parameter names, `"from"` standing for the whole pair.
    """
    targets = list(keys)
    if slot is not None:
        targets.append(slot)
    if pair:
        targets.append("from")
    return targets


def _symbol_parts(
    spec: DatasetSpec,
    rest_path: RestPath,
    symbols: NormalizedSymbols | None,
    *,
    slot: str | None,
    pair: bool = False,
    split: CryptoSplit,
) -> list[tuple[str | None, dict[str, object], dict[str, object]]]:
    """Return how a request's symbols spread over requests of one path.

    A path that takes the symbol itself (a key the dataset fills from the request, such as
    `{stocksTicker}`, or a currency pair's `{from}/{to}`), a path whose one placeholder nothing
    else fills (`slot`: a per-symbol detail path whose rows also carry the ticker), a path
    whose `{from}/{to}` pair nothing else fills (`pair`: crypto's daily open-close, whose rows
    carry their own symbol), a `per_symbol` dataset, and a symbol parameter that takes one
    value (`ticker`) get one request per symbol. A list parameter
    (`ticker.any_of`, `tickers`) gets up to `DatasetSpec.symbols_per_request` symbols a
    request (all of them when the dataset sets no limit). `dm.ALL`, no
    symbols, and a path that takes neither get one request without symbols.

    Args:
        spec: The dataset.
        rest_path: The path.
        symbols: The static symbols: a sorted tuple, `ALL`, or None.
        slot: The path's one placeholder that no parameter, key, or window fills, or None.
        pair: True when the path's `{from}` and `{to}` placeholders are filled by nothing
            else, so each symbol's currency pair (`_pair`) fills them.
        split: Splits a crypto symbol into its two currencies (`_pair`).

    Returns:
        `(symbol, path parameters, query parameters)` per request: `symbol` is set for a
        per-symbol request. Empty for an empty symbol tuple (a reactive table with no valid
        row).

    Raises:
        ValueError: If the path needs a symbol per request and the symbols are `dm.ALL` or
            missing; the dataset's factory normally rejects that first.
    """
    params = {p.name: p for p in rest_path.params}
    keys = spec.keys_for(Transport.REST, rest_path.template)
    from_request = [key for _, key in keys.declared() if key.from_request]
    param = spec.symbol_param if spec.symbol_param in params else None
    listed = param is not None and params[param].kind is ParamKind.LIST
    takes_one = bool(from_request) or slot is not None or pair or spec.per_symbol
    per_symbol = takes_one or (param is not None and not listed)
    if symbols is None or isinstance(symbols, _All):
        if takes_one:
            raise ValueError(f"{spec.name}: {rest_path.template} needs explicit symbols")
        return [(None, {}, {})]
    explicit = cast("tuple[str, ...]", symbols)
    if not explicit:
        return []
    if not per_symbol:
        if param is None:
            return [(None, {}, {})]
        size = spec.symbols_per_request or len(explicit)
        return [(None, {}, {param: explicit[i : i + size]}) for i in range(0, len(explicit), size)]
    targets = _symbol_targets([key.column for key in from_request], slot=slot, pair=pair)
    parts: list[tuple[str | None, dict[str, object], dict[str, object]]] = []
    for symbol in explicit:
        path: dict[str, object] = {}
        query: dict[str, object] = {}
        for name in targets:
            if name == "from" and "to" in params:
                path["from"], path["to"] = _pair(spec, symbol, split)
            elif params[name].in_path:
                path[name] = symbol
            else:
                query[name] = symbol
        if param is not None:
            query[param] = (symbol,) if listed else symbol
        parts.append((symbol, path, query))
    return parts


def _caller_params(
    spec: DatasetSpec, rest_path: RestPath, request: Request
) -> tuple[dict[str, object], dict[str, object]]:
    """Return the path and query parameters the caller set: typed parameters and filters.

    Typed parameters go wherever the path takes them: a path segment (`{date}`,
    `{direction}`) or a query parameter. `order` on a path without an `order` parameter folds
    into `sort` as its `.asc` or `.desc` suffix (`sort=date.desc`); on a path with one it is
    sent as is. A filter goes out under the wire name whose `filters` key it is.

    Args:
        spec: The dataset, for messages.
        rest_path: The path.
        request: The normalized arguments.

    Returns:
        `(path parameters, query parameters)`.

    Raises:
        ValueError: If a typed parameter or a filter the caller set is not a parameter of the
            path (for example a filter of a dataset's list path on its per-symbol path), or
            `order` is set where the path has neither `order` nor `sort`.
    """
    params = {p.name: p for p in rest_path.params}
    wire_names = {filter_key(name): name for name in params}
    path: dict[str, object] = {}
    query: dict[str, object] = {}
    typed = dict(request.params)
    folded = "order" in typed and "order" not in params
    unknown = [name for name in typed if name not in params and not (folded and name == "order")]
    for name, value in typed.items():
        if name in params:
            (path if params[name].in_path else query)[name] = value
    if folded and "sort" in query:
        query["sort"] = f"{typed['sort']}.{typed['order']}"
    elif folded:
        unknown.append("order")
    for key, value in request.filters:
        wire = wire_names.get(key)
        if wire is None:
            unknown.append(key)
        else:
            query[wire] = value
    if unknown:
        raise ValueError(f"{spec.name}: {rest_path.template} does not take {sorted(set(unknown))}")
    return path, query


def _base(
    spec: DatasetSpec, rest_path: RestPath, request: Request, *, fixed: Mapping[str, object]
) -> tuple[dict[str, object], dict[str, object]]:
    """Return the path and query parameters every request of a path shares.

    Only parameters the path declares are sent: the caller's (`_caller_params`); `multiplier`
    in a path placeholder of that name; `timespan` wherever the path takes it (the aggregates'
    segment, the indicators' query parameter); `Request.adjusted` when it is set; and the
    futures `resolution` and the page size when the path takes them. The page size is
    `DatasetSpec.page_size`, or `Request.limit` when that is smaller.

    Args:
        spec: The dataset.
        rest_path: The path.
        request: The normalized arguments.
        fixed: Path parameters the caller sets, such as a `history()` window's `{from}` and
            `{to}` dates; they take precedence.

    Returns:
        `(path parameters, query parameters)`.

    Raises:
        ValueError: If `Request.adjusted` is set and the path has no `adjusted` parameter
            (the crypto indicators); an explicit value is never dropped.

    Any other parameter the caller set that the path does not take raises the `ValueError` of
    `_caller_params`.
    """
    params = {p.name: p for p in rest_path.params}
    path, query = _caller_params(spec, rest_path, request)
    if "multiplier" in params and params["multiplier"].in_path:
        path["multiplier"] = request.multiplier
    if request.timespan is not None and "timespan" in params:
        (path if params["timespan"].in_path else query)["timespan"] = request.timespan
    if request.adjusted is not None:
        if "adjusted" not in params:
            raise ValueError(f"{spec.name}: {rest_path.template} does not take ['adjusted']")
        query["adjusted"] = request.adjusted
    if request.timespan is not None and "resolution" in params:
        query["resolution"] = f"{request.multiplier}{_FUTURES_UNITS[request.timespan]}"
    if spec.page_size is not None and "limit" in params:
        # A page size, as the list paths read `limit`. The aggregates' range path reads its
        # `limit` as the number of base aggregates queried instead (at most 50,000); its
        # datasets set that maximum, and bars take no `limit` argument, so no cap shrinks it.
        cap = request.limit
        query["limit"] = spec.page_size if cap is None else min(cap, spec.page_size)
    path.update(fixed)
    return path, query


def fetch_paths(
    spec: DatasetSpec, symbols: NormalizedSymbols | None
) -> list[tuple[str, NormalizedSymbols | None]]:
    """Return the REST paths a `fetch()` or `poll()` cycle reads, each with its symbols.

    A dataset with a per-symbol detail path (`DatasetSpec.detail_path`, such as one option
    contract's reference data) reads it once per symbol when the call names symbols, and its
    list path (`fetch_path`) otherwise. A dataset that serves some asset classes on paths of
    their own (`DatasetSpec.fetch_paths_by_class`) sends each symbol to its class's path
    (`symbol_class`), and every other symbol to `fetch_path`.

    Args:
        spec: The dataset, with a fetch path.
        symbols: The call's static symbols.

    Returns:
        `(path template, symbols)` pairs, in the order of each path's first symbol.
    """
    default = cast("str", spec.fetch_path)
    if not isinstance(symbols, tuple) or not symbols:
        return [(default, symbols)]
    if spec.detail_path is not None:
        return [(spec.detail_path, symbols)]
    if not spec.fetch_paths_by_class:
        return [(default, symbols)]
    groups: dict[str, list[str]] = {}
    for symbol in symbols:
        path = spec.fetch_paths_by_class.get(symbol_class(symbol), default)
        groups.setdefault(path, []).append(symbol)
    return [(path, tuple(group)) for path, group in groups.items()]


def plan_calls(
    spec: DatasetSpec,
    rest_path: RestPath,
    request: Request,
    *,
    symbols: NormalizedSymbols | None,
    fixed: Mapping[str, object] | None = None,
    dates: tuple[date | None, date | None] = (None, None),
    extra: Mapping[str, object] | None = None,
    split: CryptoSplit,
) -> list[Call]:
    """Plan the requests that serve a dataset call on one REST path.

    Makes no request itself; only `split` may, the first time a crypto pair is split. The
    REST client checks the parameters against the path again before it sends anything.

    Args:
        spec: The dataset.
        rest_path: The path to request.
        request: The normalized arguments.
        symbols: The static symbols to request, in place of `request.symbols` (a reactive
            table has been read into a tuple by now).
        fixed: Path parameters the caller sets.
        dates: The time-axis bounds (`_axis_query`); open by default.
        extra: For `history()`, query parameters added to every call after the time axis:
            the ascending direction (`_ascending`) and, for one futures session, its
            `session_end_date`; None adds none.
        split: Splits a crypto symbol a `{from}/{to}` path takes as two currencies, by
            Massive's crypto ticker list (`RestReader.crypto_pair`); called only for such a
            path.

    Returns:
        The calls, in result order: per symbol, or per chunk of symbols, or one. Each call's
        dates (its time axis and its date filters on instant parameters) follow the day rules
        `dates.day_class` picks: the dataset's class, or for a dataset with a path per symbol
        class (the indicators) the class of the call's own symbols, so a crypto indicator gets
        UTC days and a stock indicator New York days. Every other dataset without a class
        gets New York days.

    A path that needs a symbol per request, called without explicit symbols, raises the
    `ValueError` of `_symbol_parts`; an inexact axis bound the `ValueError` of `_axis_query`.
    """
    path, query = _base(spec, rest_path, request, fixed=fixed or {})
    keys = spec.keys_for(Transport.REST, rest_path.template).declared()
    filled = set(path) | {key.column for _, key in keys if key.from_request}
    if "from" in filled:
        filled.add("to")  # a currency pair fills both
    placeholders = {p.name for p in rest_path.params if p.in_path}
    # A `{from}/{to}` pair nothing else fills takes each symbol's currency pair, wherever the
    # rows' symbol key comes from.
    pair = {"from", "to"} <= placeholders - filled
    if pair:
        filled |= {"from", "to"}
    empty = [p.name for p in rest_path.params if p.in_path and p.name not in filled]
    slot = empty[0] if len(empty) == 1 else None
    calls = []
    parts = _symbol_parts(spec, rest_path, symbols, slot=slot, pair=pair, split=split)
    for symbol, own_path, own_query in parts:
        zone_class = day_class(spec, map(symbol_class, _own_symbols(spec, symbol, own_query)))
        axis = _axis_query(spec, rest_path, dates=dates, asset_class=zone_class)
        sent = _filter_days(rest_path, {**query, **axis}, zone_class)
        calls.append(Call({**path, **own_path}, {**sent, **(extra or {}), **own_query}, symbol))
    return calls


@dataclass(frozen=True, eq=False, slots=True)
class _Converter:
    """How one dataset turns the rows of one REST path into batches of its schema.

    Built per read and shared, read-only, by the read's worker threads.

    Attributes:
        dataset: The dataset name, for messages.
        rest_path: The path.
        columns: Per schema column, in order: its type, its name, and the source column that
            fills it, or None for a column the path does not carry (null).
        arrow_schema: The dataset's Arrow schema.
        required: The long-form columns every row must carry: the dataset's keys for the path
            that are not filled from the request.
        required_on_error: The required columns of an error row (`hosts.is_error_row`): only
            its symbol key, so a ticker the plan does not include keeps its row.
        symbols: How a symbol read from the rows becomes canonical (`WireSymbols`), and
            what a spelling the library cannot read does.
        symbol_key: The wire key of the rows' symbol field, or None when the symbol is filled
            from the request.
        time_name: The dataset's time column (`row_times.time_column`), whose epoch `0` is
            kept, and so fails the timestamp bound, rather than read as absent.
    """

    dataset: str
    rest_path: RestPath
    columns: tuple[tuple[ColumnType, str, SourceColumn | None], ...]
    arrow_schema: pa.Schema
    required: frozenset[str]
    required_on_error: frozenset[str]
    symbols: WireSymbols
    symbol_key: str | None
    time_name: str

    @classmethod
    def build(
        cls,
        catalog: Catalog,
        spec: DatasetSpec,
        rest_path: RestPath,
        *,
        unknown_symbols: UnknownSymbols,
    ) -> _Converter:
        """Prepare the conversion from the path's generated fields and the dataset's schema.

        Args:
            catalog: The catalog, for the dataset's merged schema.
            spec: The dataset.
            rest_path: The path.
            unknown_symbols: The session's rule for a symbol spelling the library cannot
                read; `WARN` also adds the `RawSymbol` column to the schema.

        Returns:
            The converter.
        """
        schema = catalog.dataset_schema(spec, raw_symbol=unknown_symbols is UnknownSymbols.WARN)
        renamed = {c.column: c for c in rename(spec, rest_path)}
        symbol = renamed.get("Symbol")
        # The rows' own symbol field, unless the key is filled from the request.
        symbol_field = None if symbol is None else symbol.field
        return cls(
            dataset=spec.name,
            rest_path=rest_path,
            columns=tuple((c.type, c.name, renamed.get(c.name)) for c in schema.columns),
            arrow_schema=schema.arrow_schema(),
            required=frozenset(f.column for f in required_fields(spec, rest_path)),
            required_on_error=frozenset(() if symbol_field is None else (symbol_field.column,)),
            symbols=WireSymbols(
                spec.name, spec.asset_class, unknown_symbols, spec.symbols_as_written
            ),
            symbol_key=None if symbol_field is None else symbol_field.wire_key,
            time_name=time_column(spec.times),
        )

    def convert(
        self, rows: Sequence[Mapping[str, object]], symbol: str | None
    ) -> tuple[list[pa.RecordBatch], DriftReport, list[Mapping[str, object]]]:
        """Convert one page's rows into batches of the dataset's schema.

        Error rows (`hosts.is_error_row`: a ticker the plan does not include) are decoded
        apart, with only their symbol required, and come after the page's other rows.

        Args:
            rows: The page's rows.
            symbol: The request's one symbol, which fills the keys taken from the request.

        Returns:
            The batches (none for an empty page), the page's drift, and the error rows.

        Raises:
            RowDataError: If a row lacks a required key or holds a value that does not convert
                (`to_batch`), or a timestamp overflows or lies outside 1990 to 2100.
        """
        flagged = [row for row in rows if hosts.is_error_row(row)]
        good = [row for row in rows if not hosts.is_error_row(row)] if flagged else rows
        batches: list[pa.RecordBatch] = []
        extra: set[str] = set()
        missing: set[str] = set()
        samples: dict[str, DriftSample] = {}
        for part, required in ((good, self.required), (flagged, self.required_on_error)):
            if not part:
                continue
            try:
                batch, drift = to_batch(part, self.rest_path, required=required)
                batches.append(self._apply(batch, symbol))
            except WireError as error:
                raise RowDataError(f"{self.dataset}: {error.message}") from error
            except UnitMismatchError as error:
                raise RowDataError(str(error)) from error
            extra |= drift.extra
            missing |= drift.missing
            samples = {**drift.samples, **samples}  # the first part's sample of a key wins
        drift = DriftReport(extra=frozenset(extra), missing=frozenset(missing), samples=samples)
        return batches, drift, flagged

    def _apply(self, batch: pa.RecordBatch, symbol: str | None) -> pa.RecordBatch:
        """Turn one decoded batch into a batch of the dataset's schema.

        Args:
            batch: The path's fields, by long-form column (`to_batch`).
            symbol: The value of the keys taken from the request.

        Returns:
            The batch in the dataset's schema, with the derived columns (`OptionRoot`,
            `RawSymbol`) computed from the symbols.

        Raises:
            RowDataError: If a row's symbol is a spelling the library cannot read and the
                session's `unknown_symbols` is `FAIL`; under `WARN` such a row keeps a null
                `Symbol` and the text in `RawSymbol`.

        A timestamp out of range raises the `UnitMismatchError` of `timestamps.instants`.
        """
        rows = batch.num_rows
        arrays: dict[str, pa.Array] = {}
        raw = pa.nulls(rows, pa.string())
        for column_type, name, source in self.columns:
            target = column_type.arrow_type
            if name == OPTION_ROOT:
                values = option_roots(arrays["Symbol"])
            elif name == RAW_SYMBOL:
                values = raw
            elif source is None:
                values = pa.nulls(rows, target)
            elif source.field is None:
                values = pa.array([symbol] * rows, target)
            else:
                values = batch.column(source.field.column)
                if name == "Symbol":
                    read = self.symbols.read(values, {})
                    if read.unknown and self.symbols.unknown is UnknownSymbols.FAIL:
                        raise RowDataError(unknown_symbol_message(self.dataset, read.unknown))
                    values, raw = read.symbols, read.raw
                if source.unit is not None:
                    label = f"{self.dataset}.{name}"
                    values = instants(
                        label, source.unit, values, time_column=name == self.time_name
                    )
            arrays[name] = widen(values, target)
        return pa.RecordBatch.from_arrays(list(arrays.values()), schema=self.arrow_schema)


def _first_copies(rows: pa.Table, keys: Sequence[str]) -> pa.Table:
    """Drop the repeats of a row that several symbols' requests returned, keeping its first.

    Args:
        rows: The rows of every request, in request order.
        keys: The columns that identify a row (`DatasetSpec.poll_keys`).

    Returns:
        The rows, each identity once, in the order of their first copies.
    """
    numbered = rows.append_column(_ROW_NUMBER, pa.array(range(rows.num_rows), pa.int64()))
    firsts = numbered.group_by(list(keys), use_threads=False).aggregate([(_ROW_NUMBER, "min")])
    return rows.take(firsts[f"{_ROW_NUMBER}_min"].sort())


def _whole(rows: pa.RecordBatch, window: HistoryWindow | None) -> pa.RecordBatch:
    """Keep the bars of a `history()` window whose filled window lies in it (`row_times.within`).

    Args:
        rows: Rows with their time columns filled.
        window: The `history()` window, or None for `fetch()` and `poll()`.

    Returns:
        The rows kept; every row where the window bounds no bar (ticks, `fetch()`, `poll()`).
    """
    if window is None or window.bars is None:
        return rows
    return within(rows, window.bars)


def _keep(batch: pa.RecordBatch, window: HistoryWindow | None, *, time: str) -> pa.RecordBatch:
    """Keep the rows of a `history()` window: on their time, and for sessions on `Date`.

    A request can return rows outside the window: the aggregates' `{from}/{to}` reach whole
    periods, a bar may not lie wholly inside the range, and futures bars named by date are
    requested over a window widened for holidays. A row is kept when its time column lies in
    `HistoryWindow.times` and its session date, `Date` (Massive's `session_end_date`, as ISO
    text, which orders as dates do), in `HistoryWindow.sessions`, where the window sets them.
    Without the column a bound is about, that bound keeps every row. It runs before the time
    columns are filled, so a bar of several sessions still carries its label here.

    Args:
        batch: Rows in the dataset's schema.
        window: The `history()` window, or None for `fetch()` and `poll()`.
        time: The dataset's time column: `Timestamp`, or a bar's `TimestampStart`.

    Returns:
        The rows inside the window.
    """
    if window is None:
        return batch
    names = batch.schema.names
    masks = []
    if window.sessions is not None and DATE in names:
        sessions = batch.column(DATE)
        masks.append(pc.greater_equal(sessions, pa.scalar(window.sessions.first.isoformat())))
        masks.append(pc.less_equal(sessions, pa.scalar(window.sessions.last.isoformat())))
    if window.times is not None and time in names:
        times = batch.column(time)
        masks.append(pc.greater_equal(times, pa.scalar(window.times.start_ns, times.type)))
        masks.append(pc.less(times, pa.scalar(window.times.end_ns, times.type)))
    if not masks:
        return batch
    return batch.filter(functools.reduce(pc.and_, masks))


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
    the core alive. Thread-safe: the host decisions and the once-only flags sit behind one leaf
    lock, held only to read or change them; the client and the pollers are thread-safe. It
    also holds, for the session, Massive's list of crypto tickers with their two currencies
    (`RestReader.crypto_pair`): read once, on first need, by the first caller, while every
    other caller waits on its future outside the lock.
    """

    __slots__ = ("_client", "_crypto", "_hosts", "_lock", "_once", "label", "pollers")

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
        self._hosts: dict[tuple[AssetClass, ChannelFamily], Future[HostDecision]] = {}
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

    def host_future(
        self, key: tuple[AssetClass, ChannelFamily]
    ) -> tuple[Future[HostDecision], bool]:
        """Return the decision of a (class, family), or claim it for the caller to make.

        Args:
            key: The asset class and channel family.

        Returns:
            The decision's future and whether the caller created it: the creator makes the
            decision and completes the future; everyone else waits on it, outside every lock.
        """
        with self._lock:
            future = self._hosts.get(key)
            if future is not None:
                return future, False
            future = self._hosts[key] = Future()
            return future, True

    def known_host(self, key: tuple[AssetClass, ChannelFamily]) -> HostDecision | None:
        """Return the decision of a (class, family) if one is made, without claiming it.

        A failed decision is removed before its future completes, so a completed future under
        the key always holds a decision.

        Args:
            key: The asset class and channel family.

        Returns:
            The decision, or None while none is made or one is being made.
        """
        with self._lock:
            future = self._hosts.get(key)
        return future.result() if future is not None and future.done() else None

    def replace_host(self, key: tuple[AssetClass, ChannelFamily], decision: HostDecision) -> None:
        """Cache a decision for a (class, family) in place of any earlier one.

        A caller already waiting on an earlier decision still gets that one; every later
        caller gets this one.

        Args:
            key: The asset class and channel family.
            decision: The new decision.
        """
        future: Future[HostDecision] = Future()
        future.set_result(decision)
        with self._lock:
            self._hosts[key] = future

    def forget_host(
        self, key: tuple[AssetClass, ChannelFamily], future: Future[HostDecision]
    ) -> None:
        """Remove a failed decision, so that the next caller tries again.

        Args:
            key: The asset class and channel family.
            future: The failed decision's future; another one under the key is kept.
        """
        with self._lock:
            if self._hosts.get(key) is future:
                del self._hosts[key]

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

    def known_crypto(self) -> CryptoTickers | None:
        """Return the session's crypto ticker list if it is read, without claiming it.

        A failed read is removed before its future completes, so a completed future always
        holds the list.

        Returns:
            The list, or None while it is not read or being read.
        """
        with self._lock:
            future = self._crypto
        return future.result() if future is not None and future.done() else None

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
        while another thread makes it, sharing that read rather than starting a second one; a
        caller that shares it still waits out no more of its pauses than its own patience
        covers (`wait_for_shared`). Holds no lock meanwhile. A failed read is not kept, so
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

        The first caller reads the list; a caller that arrives meanwhile waits for that read
        within its own patience (`wait_for_shared`). If the caller reading it stops for a reason
        of its own (it gave up on a pause, or was interrupted), each waiting caller tries again
        with the patience it has left: one of them reads the list, and the others wait on it.

        Returns:
            Each crypto ticker Massive lists: its `(base, quote)`, or why its currencies
            cannot be used.

        A failed read raises its public `MassiveError` (`AuthenticationError`,
        `EntitlementError`), and every caller that waited on it a copy of it; an unexpected
        error of the reader propagates to it, and its waiters get a `MassiveError` naming it.
        A waiting caller that gives up raises the error `wait_for_shared` describes.
        """
        known = self.state.known_crypto()
        if known is not None:
            return known  # a list already read needs no patience (a Java call)
        # Taken before claiming the read: a claim is completed only by the block that reads.
        patience = self.patience()
        future, created = self.state.crypto_future()
        while not created:
            if self.wait_for_shared(
                future, bucket=OTHER, patience=patience, what="the crypto ticker list"
            ):
                return _outcome(future)
            future, created = self.state.crypto_future()
        # The claim is completed whatever stops the read, an interrupt (as by Ctrl-C) as it
        # starts included: a claim left open would hang every later caller. The claim is
        # forgotten before its future fails, so a waiter that tries again never finds it.
        try:
            tickers = self._read_crypto_tickers(patience)
        except WireError as error:
            public = self._public(error)
            self.state.forget_crypto()
            future.set_exception(_shared_failure(public, "the read failed"))
            raise public from error
        except BaseException as error:
            # The reader's own unexpected error propagates; its waiters get a MassiveError.
            self.state.forget_crypto()
            future.set_exception(_shared_failure(error, "the read failed"))
            raise
        future.set_result(tickers)
        return tickers

    def _read_crypto_tickers(self, patience: Patience) -> CryptoTickers:
        """Read Massive's whole crypto ticker list, every page of both lists.

        Args:
            patience: The reading caller's patience, for every page.

        Returns:
            The list.

        A failed request raises its `WireError`; `_crypto_tickers` completes the shared
        claim either way.
        """
        tickers: dict[str, tuple[str, str] | str] = {}
        # The list is reference data, which Massive counts in a bucket of its own. Its pages
        # share one lane of their own, so they wait their turn with other reads instead of
        # queueing behind them; closing the client still ends their waits.
        with Cancel() as lane:
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

    def wait_for_shared(
        self, future: Future[T], *, bucket: str, patience: Patience, what: str
    ) -> bool:
        """Wait for a lookup another caller is making, within this caller's own patience.

        The session shares some lookups between the callers that need them at once: the crypto
        ticker list (`_crypto_tickers`) and each WebSocket host decision
        (`RestEndpoints.resolve_host`). The lookup is shared, but its pauses are not: this
        caller charges each pause of the lookup's bucket to its own patience
        (`RequestGovernor.charge_pause`), when it starts waiting and after every change of that
        bucket, and stops waiting once a pause outlasts what its patience still covers. So a
        console command, which waits out 90 s of pauses, never waits out the ten minutes of the
        caller it joined; the lookup goes on for that caller and every other one still waiting.

        Blocks the calling thread until the lookup ends or this caller gives up; holds no lock
        meanwhile, and leaves no listener behind. Logs the wait at DEBUG, a lookup abandoned by
        its maker at INFO, and giving up at ERROR.

        Args:
            future: The lookup's future, which the caller making it completes.
            bucket: The governor bucket the lookup's requests are charged to.
            patience: This caller's patience; the pauses it waits out are charged to it.
            what: The lookup, for the log, such as `"the crypto ticker list"`.

        Returns:
            True when the future holds the lookup's outcome (`_outcome` returns or raises it);
            False when the caller making it stopped for a reason of its own (it gave up on a
            pause, or was interrupted; see `_shared_failure`), so this caller tries again.

        Once a pause of the lookup outlasts this caller's patience, it raises the public error
        of a request that gives up (`_public`): `MassiveError("rate limited: ...")` with status
        429, or `"... is closed"` if the session closed meanwhile.
        """
        logger.debug(
            "%s: waits for %s another caller is making, through at most %.0f s of its pauses",
            self.state.label,
            what,
            patience.seconds,
        )
        governor = self.state.client.governor
        changed = threading.Event()
        future.add_done_callback(lambda _: changed.set())
        governor.subscribe_bucket(bucket, changed.set)
        try:
            while True:
                changed.clear()  # before looking, so no change is missed
                if future.done():
                    abandoned = future.exception()
                    if isinstance(abandoned, _AbandonedError):
                        logger.info(
                            "%s: the caller making %s stopped (%s); this caller tries again",
                            self.state.label,
                            what,
                            abandoned,
                        )
                        return False
                    return True
                failure = governor.charge_pause(bucket, patience)
                if failure is not None:
                    public = self._public(failure)
                    logger.error(
                        "%s: stops waiting for %s, which goes on for the caller making it: %s",
                        self.state.label,
                        what,
                        public,
                    )
                    raise public from failure
                governor.clock.wait(changed, math.inf)
        finally:
            governor.unsubscribe(changed.set)

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
        converter = _Converter.build(
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
        converter: _Converter,
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
        bars of a day or longer are stamped (`_stamped`), its rows outside the `history()`
        window dropped (`_keep`), and its rows given the library's time columns
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

        A failed request otherwise raises the public error of its `WireError` (`_public`), and
        once the session is closed every failed request does, a 404 included: the call raises
        `MassiveError("... is closed")` rather than dropping its symbol. A row that breaks the
        schema raises the `RowDataError` of `_Converter.convert`; a page on another adjustment
        basis the `MassiveError` of `_check_adjusted`; and a bar stamped at a time Massive is
        not known to use the `MassiveError` of `_stamped`.
        """
        kept: list[pa.RecordBatch] = []
        requests = 0
        template = converter.rest_path.template
        own = _own_symbols(spec, call.symbol, call.query)
        zone_class = day_class(spec, map(symbol_class, own))
        stamps = _with_remedy(stamp_rule(spec, request, asset_class=zone_class), remedy)
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
                # not be dropped unseen by `_keep`.
                if stamps is not None:
                    batches = [_stamped(spec, template, batch, rule=stamps) for batch in batches]
                filled = (
                    times.fill(_keep(batch, window, time=converter.time_name), received=received)
                    for batch in batches
                )
                kept.extend(_whole(rows, window) for rows in filled)
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
                    raise self._public(error) from error
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
        self, converter: _Converter, flagged: Sequence[Mapping[str, object]], symbol: str | None
    ) -> None:
        """Log each ticker of an error row once per session, naming Massive's message.

        The row is kept, with its `Error` and `Message` columns.

        Args:
            converter: The path's conversion.
            flagged: The page's error rows.
            symbol: The request's one symbol, for a path whose rows do not carry it.
        """
        dataset = converter.dataset
        for row in flagged:
            key = converter.symbol_key
            ticker = str(symbol if key is None else row.get(key))
            if self.state.first("error-row", dataset, ticker):
                logger.warning(
                    "%s: Massive answered %s for %s: %s; the row is kept with its Error and "
                    "Message columns",
                    dataset,
                    row.get("error"),
                    ticker,
                    row.get("message"),
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
        self, probe: hosts.SnapshotProbe, *, asset_class: AssetClass, patience: Patience
    ) -> list[dict[str, object]]:
        """Make a host-decision request and return its raw JSON rows.

        Args:
            probe: The request.
            asset_class: The class whose host is decided; the request is charged to its
                subscription, whatever path it reads.
            patience: The deciding caller's patience (`patience()`, taken on its thread
                before it waited on another caller's decision, if it did).

        Returns:
            The rows, every key kept.

        A failed request raises the public error of its `WireError` (`_public`):
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

        When every fetch path refuses a parameter, the first path's `ValueError` (from `_base`
        or `_axis_query`) is raised.
        """
        candidates = (spec.fetch_path, spec.detail_path, *spec.fetch_paths_by_class.values())
        errors: list[ValueError] = []
        templates = [template for template in candidates if template is not None]
        for template in templates:
            rest_path = self.rest_path(template)
            try:
                _base(spec, rest_path, request, fixed={})
                _axis_query(
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

        A parameter a path does not take raises the `ValueError` of `_base`; a crypto pair
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
            The rows of every path, in path order, capped at `Request.limit` (each request's
            rows, for a dataset whose symbols overlap; `_read_planned`), and the request
            count.

        Errors are those of `read`, and a parameter a path does not take raises the
        `ValueError` of `_base`.
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

        For a dataset whose symbols overlap (`DatasetSpec.symbols_overlap`), such as the news
        of a watchlist, each request is capped instead, and a row several requests returned
        is kept once, at its first copy (`_first_copies`).

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
        if spec.symbols_overlap:
            # Each request was capped on its own, so one symbol's rows never crowd out another's.
            return _first_copies(rows, spec.poll_keys), requests
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

        The first caller for a pair decides (`hosts.decide`, at most one REST request) and
        records the decision in the pair's `Transport=REST` row of `m.connections()`; callers
        that arrive meanwhile wait for that decision, outside every lock, each within its own
        patience (`RestReader.wait_for_shared`), and later callers get it at once. A failed
        decision is not cached: its row shows `FAILED` and the error, every waiting caller
        raises a copy of the error, and the next call tries again. If the deciding caller
        stopped for a reason of its own (it gave up on a pause, or was interrupted), each
        waiting caller tries again instead, with the patience it has left.

        Args:
            asset_class: The asset class of the stream.
            family: The channel family.

        Returns:
            The decision.

        Errors: `EntitlementError` when the plan has no WebSocket access for the class,
        `AuthenticationError` for a rejected key, and `MassiveError` for an undocumented
        timeframe or a failed request; each is logged at ERROR by the deciding caller. A
        waiting caller that gives up raises the error `RestReader.wait_for_shared` describes.
        """
        key = (asset_class, family)
        known = self._state.known_host(key)
        if known is not None:
            return known  # a decision already made needs no patience (a Java call)
        reader = self._reader("hosts")
        # Taken before claiming the decision: a claim is completed only by the block that
        # decides.
        patience = reader.patience()
        bucket = bucket_of(subscription_of(asset_class))
        future, owner = self._state.host_future(key)
        while not owner:
            what = f"the {asset_class.value} WebSocket host decision"
            if reader.wait_for_shared(future, bucket=bucket, patience=patience, what=what):
                return _outcome(future)
            future, owner = self._state.host_future(key)
        try:
            decision = hosts.decide(
                asset_class,
                family,
                ws_hosts=self._core.config.ws_hosts,
                fetch=functools.partial(reader.probe, asset_class=asset_class, patience=patience),
            )
        except BaseException as error:
            # Forgotten before its future fails, so a waiter that tries again never finds it.
            self._state.forget_host(key, future)
            failure = (
                error
                if isinstance(error, MassiveError)
                else MassiveError(
                    f"deciding the {asset_class.value} WebSocket host failed: {error!r}"
                )
            )
            future.set_exception(
                _shared_failure(error, f"deciding the {asset_class.value} WebSocket host failed")
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
        self._state.replace_host((asset_class, family), decision)
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

        A parameter a path does not take raises the `ValueError` of `_base` before any request:
        every path is planned before the first one is read.
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
        dropped (`_keep`), bars of a day or longer are stamped at 00:00 of their day in the
        class day zone (`_stamped`, `row_times.day_starts`), and every row gets the library's
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
            path, query = _base(spec, rest_path, request, fixed={})
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
            calls = _history_calls(
                spec, rest_path, request, window=window, split=reader.crypto_pair
            )
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
        ", ".join(_own_symbols(spec, call.symbol, call.query)) or "none",
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
    classes = map(symbol_class, _own_symbols(spec, call.symbol, call.query))
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


def _ascending(spec: DatasetSpec, rest_path: RestPath, request: Request) -> dict[str, object]:
    """Return the parameters that make a history path return each request's rows oldest first.

    Several REST history paths default to the newest first (the futures trades and bars, the
    stocks `/v3/trades`), so `history()` asks every path that takes a direction for ascending
    order, unless the caller set one. The order holds within each request, so within each
    symbol: the rows of several requests follow one another in request order. The field sorted
    on is the time axis's field wherever the path lists it. A path with an `order` parameter
    gets `order=asc`, with `sort` set to the axis field when `sort` lists it, or else to the
    one value `sort` takes. A path with only `sort` gets the axis field's `.asc` value when it
    is listed, otherwise the first value that sorts ascending (`asc` on the aggregates,
    `window_start.asc`), or, where `sort` lists no values, the axis field with `.asc`
    (`timestamp.asc`).

    Args:
        spec: The dataset, whose time axis names the field to sort on.
        rest_path: The history path.
        request: The normalized arguments; a `sort` or `order` already set is kept.

    Returns:
        The parameters to add, by wire name; empty for a path that takes no direction.
    """
    params = {p.name: p for p in rest_path.params if not p.in_path}
    if {"sort", "order"} & dict(request.params).keys():
        return {}
    sort = params.get("sort")
    axis = None if spec.time_axis is None else spec.time_axis[0].partition(".")[0]
    if "order" in params:
        fields = sort.values if sort is not None else ()
        field = axis if axis in fields else fields[0] if len(fields) == 1 else None
        return {"order": "asc", **({"sort": field} if field is not None else {})}
    if sort is None:
        return {}
    values = [v for v in sort.values if v == "asc" or v.endswith(".asc")]
    if f"{axis}.asc" in values:
        chosen = f"{axis}.asc"
    elif values:
        chosen = values[0]
    elif axis is not None and not sort.values:
        chosen = f"{axis}.asc"
    else:
        return {}
    return {"sort": chosen}


def _range_segments(requested: TimeRange) -> dict[str, object]:
    """Return an aggregates path's `{from}` and `{to}` segments for a `history()` window.

    The segments are millisecond epochs, both inclusive: the first instant requested (the
    start of the first bar that fits the window, `HistoryWindow.requested`, so a request never
    starts part way through a period) and the last whole millisecond before the end. Rows
    outside the window are dropped afterwards (`_keep`).

    Args:
        requested: The instants to request.

    Returns:
        `{"from": ..., "to": ...}`, as instants the client counts in milliseconds.
    """
    first = requested.start.replace(microsecond=requested.start.microsecond // 1000 * 1000)
    last = requested.end - _ONE_MICROSECOND
    return {"from": first, "to": last.replace(microsecond=last.microsecond // 1000 * 1000)}


def _history_calls(
    spec: DatasetSpec,
    rest_path: RestPath,
    request: Request,
    *,
    window: HistoryWindow,
    split: CryptoSplit,
) -> list[Call]:
    """Plan the requests of a `history()` call with explicit symbols on the history path.

    Every call asks for ascending rows (`_ascending`). A window that requests instants
    (`HistoryWindow.requested`) sends them on the time axis, or as the aggregates' `{from}` and
    `{to}` (`_range_segments`). A window without them (futures trades and quotes named by
    session dates) sends one call per weekday session and symbol with
    `session_end_date=<date>`: a session is exact by its date even when a holiday opened it
    days early, and no session ends on a Saturday or Sunday.

    Args:
        spec: The dataset.
        rest_path: The history path.
        request: The normalized arguments, with static symbols.
        window: The call's window.
        split: Splits a crypto symbol into its two currencies (`plan_calls`).

    Returns:
        The calls, in result order: for sessions, by date and then by symbol.
    """
    order = _ascending(spec, rest_path, request)
    requested = window.requested
    if requested is None:
        sessions = cast("DaySpan", window.sessions)
        session = session_date_param(spec)
        return [
            call
            for day in sessions.days()
            if is_trading_day(spec.asset_class, day)
            for call in plan_calls(
                spec,
                rest_path,
                request,
                symbols=request.symbols,
                extra={**order, session: day},
                split=split,
            )
        ]
    # The aggregates take their range as `{from}/{to}` timestamp segments.
    kinds = {p.name: p.kind for p in rest_path.params if p.in_path}
    ranged = kinds.get("from") is ParamKind.TIMESTAMP
    return plan_calls(
        spec,
        rest_path,
        request,
        symbols=request.symbols,
        fixed=_range_segments(requested) if ranged else {},
        dates=(requested.start, requested.end),
        extra=order,
        split=split,
    )


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

_FETCH_REMEDY: Final = "use bars(symbols, dm.Timespan.DAY).history() for that day"
"""What a `previous_day_bar` caller can do when `/prev`'s stamp is refused: the 1-day bars of
`history()` hold the same day, from a source whose stamp the day rules accept."""

_INDICATOR_REMEDY: Final = "compute the indicator from bars().history() of the same timespan"
"""What an indicator caller can do when a value's stamp is refused: no other source serves the
values, but the bars they are computed over are checked and labelled the same way."""


def _fetch_remedy(spec: DatasetSpec) -> str:
    """Return what a `fetch()` or `poll()` caller can do when a stamp is refused.

    Args:
        spec: The dataset.

    Returns:
        Computing the indicator from bars for an indicator; reading the 1-day bars'
        `history()` for the day for `previous_day_bar`.
    """
    if not spec.holds_bars and spec.times is RowTimes.BAR:
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


def _with_remedy(rule: StampRule | None, remedy: str) -> StampRule | None:
    """Give a stamp rule the remedy of the verb that applies it.

    Args:
        rule: The request's rule (`row_times.stamp_rule`), or None.
        remedy: What the caller can do instead when a stamp is refused.

    Returns:
        The rule with `remedy`, or None for None.
    """
    return None if rule is None else replace(rule, remedy=remedy)


def _stamped(
    spec: DatasetSpec, source: str, batch: pa.RecordBatch, *, rule: StampRule
) -> pa.RecordBatch:
    """Apply a stamp rule to a batch of bars (`row_times.stamp_rule`).

    Args:
        spec: The dataset, for the message.
        source: The REST path template the bars came from, for the message.
        batch: Rows in the dataset's schema.
        rule: The day zone, the stamps Massive is known to use besides 00:00, and the
            remedy of the verb that reads the bars.

    Returns:
        The batch, with its `TimestampStart` at 00:00 of each bar's day in the class day zone.

    A stamp at another time or on another day raises the `MassiveError` of
    `row_times.day_starts`, which ends with the rule's remedy.
    """
    name = time_column(spec.times)
    index = batch.schema.get_field_index(name)
    moved = rule.stamp(spec.name, batch.column(index), source=source)
    return batch.set_column(index, name, moved)
