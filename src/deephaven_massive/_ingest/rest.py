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
  (weekdays only for the classes that do not trade at weekends). Forex and crypto 1-day bars
  are stamped at 00:00 UTC of their day, and a crypto bar of a day or longer off the UTC day
  cut raises (`history_source.stamp_rule`, `_stamped`).
- **Fan-out.** A path that takes one symbol a request is called once per symbol, at most
  `MassiveConfig.max_concurrency` at a time. HTTP 404 for one symbol drops that symbol with
  one WARNING; any other error fails the call.
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
  (401 `AuthenticationError`, 403 `EntitlementError`), except a rate limit that outlived the
  retries, which becomes `MassiveError("rate limited: ...")` and logs, once per session, that
  Basic keys allow 5 requests per minute. A row that breaks the schema (a missing required key,
  a value that does not convert, a timestamp out of bounds) raises `RowDataError`. Static verbs
  log each error at ERROR and raise it.
- **Host decisions.** `resolve_host` decides each WebSocket host once per session
  (`_ingest/hosts.py`), single-flight: the first caller of a (class, family) makes the one
  REST request and every concurrent caller waits for its answer.

`RestState` is the part a session shares (the client, the host decisions, the live pollers,
the once-per-session warnings); `SessionClients` creates it on first use and registers its
close with the session. `RestEndpoints`, the session core's `RestTransport`, is a lightweight
view of the core and that state, made for each call. `poll()` itself is `_ingest/poll.py`.
"""

from __future__ import annotations

import functools
import logging
import threading
import time
from concurrent.futures import FIRST_EXCEPTION, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from http import HTTPStatus
from typing import TYPE_CHECKING, Final, cast
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.compute as pc
from deephaven import arrow as dh_arrow

from deephaven_massive._datasets.catalog import filter_key, takes_instants
from deephaven_massive._datasets.dates import day_class, instant_bound, trades_at_weekends
from deephaven_massive._datasets.history_source import StampRule, partition_dates, stamp_rule
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
    listed_pair,
    option_roots,
    symbol_class,
    unknown_symbol_message,
    ws_symbol,
)
from deephaven_massive._datasets.timestamps import (
    UnitMismatchError,
    check_utc_midnights,
    instants,
    utc_day_stamps,
)
from deephaven_massive._enums import AssetClass, Timespan, UnknownSymbols
from deephaven_massive._errors import MassiveError, from_wire
from deephaven_massive._ingest import hosts, poll
from deephaven_massive._ingest.poll import Pollers, RowDataError, UnsupportedParameterError
from deephaven_massive._massive import (
    DriftReport,
    ParamKind,
    Transport,
    WireError,
    to_batch,
)
from deephaven_massive._tables.diagnostics import ConnectionRow, ConnectionState
from deephaven_massive._tables.liveness import entered
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.dates import DaySpan, TimeRange
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import HostDecision, Request
    from deephaven_massive._datasets.schema import ColumnType, SourceColumn
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._enums import ChannelFamily, PollMode
    from deephaven_massive._massive import RestClient, RestPath
    from deephaven_massive._tables.core import SessionCore
    from deephaven_massive._tables.diagnostics import DriftRegistry

__all__ = [
    "Call",
    "RestEndpoints",
    "RestReader",
    "RestState",
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
_SATURDAY: Final = 5
_SESSION_DATE: Final = "session_end_date"
"""The futures date parameter that names a trading session, not a calendar day."""

_BASIC_REQUESTS_PER_MINUTE: Final = 5
"""How many REST requests a minute a Basic key allows."""


_RATE_LIMIT_ADVICE: Final = (
    "%s: Massive rate-limited this session's REST requests; Basic keys allow 5 requests per "
    "minute, and a fan-out poll() makes one request per symbol per cycle"
)


_TICKERS_PATH: Final = "/v3/reference/tickers"
"""The reference path that gives a crypto ticker's base and quote currencies."""


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
    asset_class: AssetClass | None = None,
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
        asset_class: The class whose day rules apply to this request (`dates.day_class`);
            None, the default, takes the dataset's own class (New York days without one).

    Returns:
        The parameters to send; empty when the dataset has no time axis.

    Raises:
        ValueError: If a `.gt` or `.lte` bound of an instant axis counts nanoseconds or takes
            ISO-8601 text, which no `datetime` step ends exactly (`dates.instant_bound`); the
            message names the dataset.
    """
    if spec.time_axis is None:
        return {}
    zone_class = spec.asset_class if asset_class is None else asset_class
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
                query[name] = instant_bound(zone_class, name, value, unit=param.unit, upper=upper)
            except ValueError as error:
                raise ValueError(f"{spec.name}: {error}") from None
        elif param.kind is ParamKind.DATE:
            query[name] = _date_bound(name, value, upper=upper)
        else:
            query[name] = value.isoformat()
    return query


def _filter_days(
    rest_path: RestPath, query: Mapping[str, object], asset_class: AssetClass | None
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
        asset_class: The class whose day rules apply (`dates.day_class`), or None.

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


def _pair(spec: DatasetSpec, symbol: str, pairs: Mapping[str, tuple[str, str]]) -> tuple[str, str]:
    """Split a forex or crypto symbol into the two currencies a `{from}/{to}` path takes.

    Args:
        spec: The dataset, whose asset class is forex or crypto.
        symbol: The canonical symbol, such as `C:EURUSD` or `X:BTCUSD`.
        pairs: The currencies of crypto symbols the quote list cannot split, from Massive's
            reference data (`RestReader.crypto_pairs`).

    Returns:
        The base and quote currencies, such as `("EUR", "USD")`.

    Raises:
        ValueError: If the dataset's class has no currency pairs.

    A crypto symbol neither in `pairs` nor split by the quote list raises the `ValueError` of
    `ws_symbol`.
    """
    if spec.asset_class not in (AssetClass.FOREX, AssetClass.CRYPTO):
        raise ValueError(f"{spec.name}: only forex and crypto symbols are currency pairs")
    base, _, quote = ws_symbol(spec.asset_class, symbol, pairs).partition("-")
    return base, quote


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
    pairs: Mapping[str, tuple[str, str]] | None = None,
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
        pairs: The currencies of crypto symbols the quote list cannot split (`_pair`); None
            for none.

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
                path["from"], path["to"] = _pair(spec, symbol, pairs or {})
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
    pairs: Mapping[str, tuple[str, str]] | None = None,
) -> list[Call]:
    """Plan the requests that serve a dataset call on one REST path.

    Pure: no I/O. The REST client checks the parameters against the path again before it
    sends anything.

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
        pairs: The currencies of crypto symbols whose `{from}/{to}` the quote list cannot
            split, from Massive's reference data (`RestReader.crypto_pairs`); None for none.

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
    parts = _symbol_parts(spec, rest_path, symbols, slot=slot, pair=pair, pairs=pairs)
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
        projected: True when the path joins the dataset through a projection of its kind
            membership (the indices snapshot as index values): a row that carries an `error`
            has no value to project, so it is dropped instead of kept.
        symbols: How a symbol read from the rows becomes canonical (`WireSymbols`), and
            what a spelling the library cannot read does.
        symbol_key: The wire key of the rows' symbol field, or None when the symbol is filled
            from the request.
    """

    dataset: str
    rest_path: RestPath
    columns: tuple[tuple[ColumnType, str, SourceColumn | None], ...]
    arrow_schema: pa.Schema
    required: frozenset[str]
    required_on_error: frozenset[str]
    projected: bool
    symbols: WireSymbols
    symbol_key: str | None

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
            projected=any(
                m.kind == spec.kind and m.group == spec.group and m.projection is not None
                for m in rest_path.memberships
            ),
            symbols=WireSymbols(
                spec.name, spec.asset_class, unknown_symbols, spec.symbols_as_written
            ),
            symbol_key=None if symbol_field is None else symbol_field.wire_key,
        )

    def convert(
        self, rows: Sequence[Mapping[str, object]], symbol: str | None
    ) -> tuple[list[pa.RecordBatch], DriftReport, list[Mapping[str, object]]]:
        """Convert one page's rows into batches of the dataset's schema.

        Error rows (`hosts.is_error_row`: a ticker the plan does not include) are decoded
        apart, with only their symbol required, and come after the page's other rows; a
        projected path drops them instead.

        Args:
            rows: The page's rows.
            symbol: The request's one symbol, which fills the keys taken from the request.

        Returns:
            The batches (none for an empty page), the page's drift, and the error rows, kept or
            dropped.

        Raises:
            RowDataError: If a row lacks a required key or holds a value that does not convert
                (`to_batch`), or a timestamp overflows or lies outside 1990 to 2100.
        """
        flagged = [row for row in rows if hosts.is_error_row(row)]
        good = [row for row in rows if not hosts.is_error_row(row)] if flagged else rows
        batches: list[pa.RecordBatch] = []
        extra: set[str] = set()
        missing: set[str] = set()
        kept = [] if self.projected else flagged
        for part, required in ((good, self.required), (kept, self.required_on_error)):
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
        return batches, DriftReport(extra=frozenset(extra), missing=frozenset(missing)), flagged

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
                    read = self.symbols.read(values)
                    if read.unknown and self.symbols.unknown is UnknownSymbols.FAIL:
                        raise RowDataError(unknown_symbol_message(self.dataset, read.unknown))
                    values, raw = read.symbols, read.raw
                if source.unit is not None:
                    label = f"{self.dataset}.{name}"
                    values = instants(label, source.unit, values, time_column=name == "Timestamp")
            arrays[name] = widen(values, target)
        return pa.RecordBatch.from_arrays(list(arrays.values()), schema=self.arrow_schema)


def _keep(batch: pa.RecordBatch, window: HistoryWindow | None) -> pa.RecordBatch:
    """Keep the rows of a `history()` window: on `Timestamp`, and for sessions `SessionEndDate`.

    A request can return rows outside the window: the aggregates' `{from}/{to}` reach whole
    periods, a bar may not lie wholly inside the range, and futures bars named by date are
    requested over a window widened for holidays. A row is kept when its `Timestamp` lies in
    `HistoryWindow.times` and its `SessionEndDate` in `HistoryWindow.sessions`, where the
    window sets them. Without the column a bound is about, that bound keeps every row.

    Args:
        batch: Rows in the dataset's schema.
        window: The `history()` window, or None for `fetch()` and `poll()`.

    Returns:
        The rows inside the window.
    """
    if window is None:
        return batch
    names = batch.schema.names
    masks = []
    if window.sessions is not None and "SessionEndDate" in names:
        sessions = batch.column("SessionEndDate")
        masks.append(pc.greater_equal(sessions, pa.scalar(window.sessions.first)))
        masks.append(pc.less_equal(sessions, pa.scalar(window.sessions.last)))
    if window.times is not None and "Timestamp" in names:
        times = batch.column("Timestamp")
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
    also caches, for the session, the two currencies of each crypto ticker whose split
    Massive's reference data gave (`RestReader.crypto_pair`), under the same lock.
    """

    __slots__ = ("_client", "_hosts", "_lock", "_once", "_pairs", "label", "pollers")

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
        self._pairs: dict[str, tuple[str, str]] = {}

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

    def cached_pair(self, symbol: str) -> tuple[str, str] | None:
        """Return the cached base and quote of a crypto ticker, if a lookup found them.

        Args:
            symbol: The canonical crypto ticker.

        Returns:
            `(base, quote)`, or None when no lookup of the session found them yet.
        """
        with self._lock:
            return self._pairs.get(symbol)

    def store_pair(self, symbol: str, pair: tuple[str, str]) -> None:
        """Cache the base and quote Massive's reference data gave for a crypto ticker.

        Args:
            symbol: The canonical crypto ticker.
            pair: Its `(base, quote)`.
        """
        with self._lock:
            self._pairs[symbol] = pair

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

    def warn_rate_limit(self) -> None:
        """Log, once per session, why Massive rate-limited the session and what multiplies it."""
        if self.first("rate-limit"):
            logger.warning(_RATE_LIMIT_ADVICE, self.label)


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
    """

    state: RestState
    catalog: Catalog
    drift: DriftRegistry
    max_concurrency: int
    unknown_symbols: UnknownSymbols

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

        The quote list splits a joined ticker where exactly one quote it knows ends it
        (`symbols.listed_pair`). Where none does, or several do (`X:DOTUSD` is `DOT-USD` or
        `DO-TUSD`), Massive's reference data says: one request to `/v3/reference/tickers` for
        the ticker, reading `base_currency_symbol` and `currency_symbol`, whose join must be
        the ticker. A successful answer is cached for the session (`RestState`), so a ticker
        is not looked up again once found (two first lookups at once may both ask). Blocks
        the calling thread for the request; holds no lock meanwhile.

        Args:
            symbol: A canonical crypto ticker, such as `X:DOTUSD`.

        Returns:
            `(base, quote)`, such as `("DOT", "USD")`.

        Raises:
            MassiveError: If the request fails (its public error: `AuthenticationError`,
                `EntitlementError`, or `MassiveError`), Massive does not know the ticker, or its
                currencies do not join to the ticker. The message names the ticker; nothing
                falls back to a guessed split.
        """
        listed = listed_pair(symbol)
        if listed is not None:
            return listed
        cached = self.state.cached_pair(symbol)
        if cached is not None:
            return cached
        where = f"{symbol!r} cannot be split into its two currencies"
        try:
            page = self.state.client.get(
                self.rest_path(_TICKERS_PATH), {}, {"ticker": symbol, "market": "crypto"}
            )
        except WireError as error:
            public = self._public(error)
            raise type(public)(
                f"{where}: looking it up in Massive's reference data failed: {public}",
                status=public.status,
                request_id=public.request_id,
            ) from error
        row = next((r for r in page.rows if r.get("ticker") == symbol), None)
        if row is None:
            raise MassiveError(f"{where}: Massive's reference data does not list it")
        base, quote = row.get("base_currency_symbol"), row.get("currency_symbol")
        if not (isinstance(base, str) and isinstance(quote, str)) or f"X:{base}{quote}" != symbol:
            raise MassiveError(
                f"{where}: Massive's reference data gives the currencies {base!r} and "
                f"{quote!r}, which do not join to it"
            )
        self.state.store_pair(symbol, (base, quote))
        return base, quote

    def crypto_pairs(
        self, spec: DatasetSpec, rest_path: RestPath, symbols: NormalizedSymbols | None
    ) -> dict[str, tuple[str, str]]:
        """Return the currencies of the crypto symbols a path's `{from}/{to}` needs split.

        Only a crypto dataset's path with `{from}` and `{to}` placeholders takes a symbol as two
        currencies (`/v1/open-close/crypto/{from}/{to}/{date}`); every other path needs none,
        and a symbol the quote list splits needs no lookup.

        Args:
            spec: The dataset.
            rest_path: The path the calls go to.
            symbols: The call's static symbols.

        Returns:
            The base and quote of each symbol that needed a lookup (`crypto_pair`).

        A failed lookup raises the `MassiveError` of `crypto_pair`.
        """
        placeholders = {p.name for p in rest_path.params if p.in_path}
        if spec.asset_class is not AssetClass.CRYPTO or not {"from", "to"} <= placeholders:
            return {}
        listed = symbols if isinstance(symbols, tuple) else ()
        return {s: self.crypto_pair(s) for s in listed if listed_pair(s) is None}

    def read(
        self,
        spec: DatasetSpec,
        rest_path: RestPath,
        calls: Sequence[Call],
        *,
        limit: int | None,
        window: HistoryWindow | None = None,
        stamps: StampRule = StampRule.KEEP,
        record: Callable[[DriftReport], None] | None = None,
        executor: ThreadPoolExecutor | None = None,
    ) -> tuple[pa.Table, int]:
        """Make the calls and return their rows as one Arrow table of the dataset's schema.

        One call runs on this thread; several run on `executor`, or on a pool of
        `min(max_concurrency, calls)` threads made for this read. If a call fails, or this
        thread is interrupted, the calls not yet started are cancelled, the read waits for the
        running ones, and the failure of the earliest call is raised.

        Args:
            spec: The dataset.
            rest_path: The path.
            calls: The planned calls, in result order.
            limit: The cap on each call's rows, or None; `read_fetch` caps the total.
            window: The `history()` window whose rows to keep, or None.
            stamps: How the bars' stamps are treated (`history_source.stamp_rule`): forex and
                crypto 1-day bars move to 00:00 UTC of their day, and a crypto stamp off the
                UTC day cut raises `MassiveError`; `KEEP`, the default, for everything else.
            record: Also given each page's drift, for a `poll()` table's `SchemaDrift`.
            executor: A pool to run several calls on, which the caller keeps; None for a pool
                of this read's own.

        Returns:
            The rows, in call order and then page order, and the number of requests made.

        Errors: the public error of each `WireError` (`_public`), and `RowDataError` for a row
        that breaks the schema. A closed session raises `MassiveError("... is closed")`.
        """
        converter = _Converter.build(
            self.catalog, spec, rest_path, unknown_symbols=self.unknown_symbols
        )
        run = functools.partial(
            self._call,
            converter,
            limit=limit,
            window=window,
            stamps=stamps,
            record=record,
            spec=spec,
        )
        if len(calls) <= 1:
            outcomes = [run(call) for call in calls]
        else:
            outcomes = self._fan_out(run, calls, executor)
        batches = [batch for outcome in outcomes for batch in outcome.batches]
        table = pa.Table.from_batches(batches, schema=converter.arrow_schema)
        return table, sum(outcome.requests for outcome in outcomes)

    def _fan_out(
        self,
        run: Callable[[Call], _Outcome],
        calls: Sequence[Call],
        executor: ThreadPoolExecutor | None,
    ) -> list[_Outcome]:
        """Run several calls concurrently and return their outcomes in call order.

        Args:
            run: Makes one call.
            calls: The calls.
            executor: The caller's pool, or None for one of this read's own.

        Returns:
            The outcomes, in call order.
        """
        own = executor is None
        pool = executor or ThreadPoolExecutor(
            min(self.max_concurrency, len(calls)), f"{self.state.label} REST"
        )
        futures = [pool.submit(run, call) for call in calls]
        try:
            wait(futures, return_when=FIRST_EXCEPTION)
        finally:
            for future in futures:
                future.cancel()
            wait(futures)
            if own:
                pool.shutdown()
        return [future.result() for future in futures if not future.cancelled()]

    def _call(
        self,
        converter: _Converter,
        call: Call,
        *,
        spec: DatasetSpec,
        limit: int | None,
        window: HistoryWindow | None,
        stamps: StampRule,
        record: Callable[[DriftReport], None] | None,
    ) -> _Outcome:
        """Request one call's pages and convert their rows.

        Args:
            converter: The path's conversion.
            call: The call.
            spec: The dataset.
            limit: The cap on the call's rows, or None.
            window: The `history()` window whose rows to keep, or None.
            stamps: How the bars' stamps are treated (`_stamped`).
            record: Also given each page's drift, or None.

        Returns:
            The call's rows and request count; no rows for a per-symbol call answered with
            HTTP 404, which logs one WARNING per symbol and session.

        A failed request raises the public error of its `WireError` (`_public`), and a row
        that breaks the schema the `RowDataError` of `_Converter.convert`; a crypto bar
        stamped off the UTC day cut the `MassiveError` of `_stamped`.
        """
        kept: list[pa.RecordBatch] = []
        requests = 0
        template = converter.rest_path.template
        try:
            for page in self.state.client.pages(
                converter.rest_path, call.path_params, call.query, limit=limit
            ):
                requests += 1
                self.drift.report_statuses(template, page.drift.statuses)
                batches, drift, flagged = converter.convert(page.rows, call.symbol)
                self.drift.report(spec.name, Transport.REST, drift)
                if record is not None:
                    record(drift)
                self._warn_error_rows(converter, flagged, call.symbol)
                if stamps is not StampRule.KEEP:
                    batches = [_stamped(spec, template, batch, rule=stamps) for batch in batches]
                kept.extend(_keep(batch, window) for batch in batches)
        except WireError as error:
            if error.status == HTTPStatus.NOT_FOUND and call.symbol is not None:
                if self.state.first("missing", spec.name, call.symbol):
                    logger.warning(
                        "%s: %s has no data (HTTP 404 from %s); it is left out",
                        spec.name,
                        call.symbol,
                        template,
                    )
                return _Outcome(tuple(kept), requests + 1)
            raise self._public(error) from error
        return _Outcome(tuple(kept), requests)

    def _warn_error_rows(
        self, converter: _Converter, flagged: Sequence[Mapping[str, object]], symbol: str | None
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
        the request returned. A rate limit that outlived the retries is `MassiveError("rate
        limited: ...")` with status 429, and logs the rate-limit advice once per session.
        Every other error is `_errors.from_wire`'s.

        Args:
            error: The wire error.

        Returns:
            The public error, with the wire error's status and request id.
        """
        if self.state.closed:
            return MassiveError(f"{self.state.label} is closed")
        if error.status == HTTPStatus.TOO_MANY_REQUESTS:
            self.state.warn_rate_limit()
            return MassiveError(
                f"rate limited: {error.message}", status=429, request_id=error.request_id
            )
        return from_wire(error)

    def probe(self, probe: hosts.SnapshotProbe) -> list[dict[str, object]]:
        """Make a host-decision request and return its raw JSON rows.

        Args:
            probe: The request.

        Returns:
            The rows, every key kept.

        A failed request raises the public error of its `WireError` (`_public`):
        `AuthenticationError`, `EntitlementError`, or `MassiveError`.
        """
        try:
            page = self.state.client.get(self.rest_path(probe.path), probe.path_params, probe.query)
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
    ) -> tuple[pa.Table, int]:
        """Run one `poll()` cycle: `read_fetch` for the cycle's symbols.

        A reactive symbol table can route a new symbol to a path that lacks a parameter the
        caller set (an explicit `adjusted` on the crypto indicators). Retrying cannot fix that,
        so its `ValueError` becomes `UnsupportedParameterError`, which fails the table.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            symbols: The cycle's static symbols (a reactive table already read).
            record: Records each page's drift in the table's `SchemaDrift`.
            executor: The poller's fan-out pool.

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
        return self._read_planned(spec, request, planned, record=record, executor=executor)

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
                _axis_query(spec, rest_path, dates=(request.start, request.end))
            except ValueError as error:
                errors.append(error)
        if len(errors) == len(templates):
            raise errors[0]

    def plan_fetch(
        self, spec: DatasetSpec, request: Request, symbols: NormalizedSymbols | None
    ) -> list[tuple[RestPath, list[Call]]]:
        """Plan the requests of a `fetch()` or `poll()` cycle, path by path.

        Pure: no I/O.

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments.
            symbols: The static symbols.

        Returns:
            Each path the cycle reads (`fetch_paths`) with its calls; none for an empty symbol
            tuple (a reactive table with no valid row), which reads nothing.

        A parameter a path does not take raises the `ValueError` of `_base`.
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
                pairs=self.crypto_pairs(spec, rest_path, own),
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
    ) -> tuple[pa.Table, int]:
        """Read a dataset's fetch paths for the symbols: `fetch()`, or one `poll()` cycle.

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments.
            symbols: The static symbols.
            record: Receives each page's drift, or None.
            executor: A pool for fan-out requests, or None for one of each read's own.

        Returns:
            The rows of every path, in path order, capped at `Request.limit`, and the request
            count.

        Errors are those of `read`, and a parameter a path does not take raises the
        `ValueError` of `_base`.
        """
        planned = self.plan_fetch(spec, request, symbols)
        return self._read_planned(spec, request, planned, record=record, executor=executor)

    def _read_planned(
        self,
        spec: DatasetSpec,
        request: Request,
        planned: Sequence[tuple[RestPath, list[Call]]],
        *,
        record: Callable[[DriftReport], None] | None,
        executor: ThreadPoolExecutor | None,
    ) -> tuple[pa.Table, int]:
        """Make planned calls, path by path, and cap the total rows at `Request.limit`.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            planned: Each path with its calls (`plan_fetch`); empty for no symbols.
            record: Receives each page's drift, or None.
            executor: A pool for fan-out requests, or None for one of each read's own.

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
                limit=request.limit,
                stamps=stamp_rule(spec, request),
                record=record,
                executor=executor,
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

    def _reader(self) -> RestReader:
        """Return a reader of the session's paths."""
        core = self._core
        return RestReader(
            self._state,
            core.catalog,
            core.drift,
            core.config.max_concurrency,
            core.config.unknown_symbols,
        )

    def _static(self, table: pa.Table) -> Table:
        """Convert rows to a static Deephaven table, inside the session's execution context.

        Args:
            table: The rows.

        Returns:
            The static table, with every column of the schema even without rows.
        """
        with entered(self._core.exec_ctx):
            return dh_arrow.to_table(table)

    def resolve_host(self, asset_class: AssetClass, family: ChannelFamily) -> HostDecision:
        """Decide the WebSocket host of a (class, family), once per session, single-flight.

        The first caller for a pair decides (`hosts.decide`, at most one REST request) and
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
        future, owner = self._state.host_future(key)
        if not owner:
            return _waited(future)
        try:
            decision = hosts.decide(
                asset_class,
                family,
                ws_hosts=self._core.config.ws_hosts,
                fetch=self._reader().probe,
            )
        except BaseException as error:
            self._state.forget_host(key, future)
            failure = (
                error
                if isinstance(error, MassiveError)
                else MassiveError(
                    f"deciding the {asset_class.value} WebSocket host failed: {error!r}"
                )
            )
            future.set_exception(failure)
            _report(failure)
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

    def crypto_pair(self, symbol: str) -> tuple[str, str]:
        """Return a crypto ticker's base and quote, from Massive's reference data if needed.

        The WebSocket transport calls it, before a stream starts or on its lookup worker, for a
        ticker the quote list cannot split (`RestReader.crypto_pair`); the answer is cached for
        the session.

        Args:
            symbol: A canonical crypto ticker.

        Returns:
            `(base, quote)`.

        A failed or empty lookup raises the `MassiveError` of `RestReader.crypto_pair`.
        """
        return self._reader().crypto_pair(symbol)

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

    def fetch(self, spec: DatasetSpec, request: Request) -> Table:
        """Return every row the dataset's fetch path returns for the request, as a static table.

        Requests run on this thread, or for a per-symbol path on a pool of up to
        `max_concurrency` threads; `Request.limit` caps the total rows. Drift is logged, once
        per field.

        Args:
            spec: The dataset, with a fetch path.
            request: The normalized arguments, with static symbols (the query reads a reactive
                table before this call).

        Returns:
            A static table in the dataset's schema.

        Raises:
            MassiveError: Logged at ERROR first: `AuthenticationError` (HTTP 401),
                `EntitlementError` (HTTP 403 or `NOT_AUTHORIZED`), `RowDataError` for a row
                that breaks the schema, and `MassiveError` itself for any other failure (a
                rate limit as `"rate limited: ..."`, a closed session as `"... is closed"`).

        A parameter a path does not take raises the `ValueError` of `_base` before any request:
        every path is planned before the first one is read.
        """
        reader = self._reader()
        try:
            table, _ = reader.read_fetch(spec, request, symbols=request.symbols)
        except MassiveError as error:
            _report(error)
            raise
        return self._static(table)

    def history(self, spec: DatasetSpec, request: Request, window: HistoryWindow) -> Table:
        """Return the dataset's REST history over `window`, paged internally, as a static table.

        `dm.ALL` reads the grouped-daily path, one request per day of the window (weekdays only
        for the classes that do not trade at weekends, `dates.trades_at_weekends`). Futures
        trades and quotes named by session dates read one request series per weekday session
        and symbol (`session_end_date=<date>`), sessions in date order and the symbols of one
        session in symbol order; Saturdays and Sundays hold no session and are skipped. Every
        other request covers the window's instants (`HistoryWindow.requested`): on the
        dataset's time axis, or as the aggregates' `{from}/{to}` millisecond epochs, one
        request series per symbol where the path takes one. Rows outside the window are
        dropped (`_keep`), and forex and crypto 1-day bars are stamped at 00:00 UTC of their
        day. A grouped-daily or per-session download of more than five requests first logs, at
        INFO, how many requests it makes and about how long a Basic key's 5 requests a minute
        make it last. Logs `source=REST requests=<n>` at INFO.

        Args:
            spec: The dataset; `history()` has checked that REST can serve the request.
            request: The normalized arguments, with static symbols.
            window: The call's window (`history_window` in `_datasets/history_source.py`).

        Returns:
            A static table in the dataset's schema.

        Raises:
            MassiveError: As `fetch` raises it, logged at ERROR first, and for a crypto 1-day
                bar stamped elsewhere in its UTC day than its first or last millisecond.
        """
        reader = self._reader()
        if isinstance(request.symbols, _All):
            rest_path = reader.rest_path(cast("str", spec.grouped_daily_path))
            path, query = _base(spec, rest_path, request, fixed={})
            # A class that does not trade at weekends has no grouped day on a Saturday or Sunday.
            days = [
                day
                for day in partition_dates(spec, window)
                if day.weekday() < _SATURDAY or trades_at_weekends(spec.asset_class)
            ]
            calls = [Call({**path, "date": day}, query) for day in days]
            _announce(spec, len(calls), sessions=False)
        else:
            rest_path = reader.rest_path(cast("str", spec.history_path))
            pairs = reader.crypto_pairs(spec, rest_path, request.symbols)
            calls = _history_calls(spec, rest_path, request, window=window, pairs=pairs)
            if window.requested is None:
                _announce(spec, len(calls), sessions=True)
        try:
            table, requests = reader.read(
                spec,
                rest_path,
                calls,
                limit=None,
                window=window,
                stamps=stamp_rule(spec, request),
            )
        except MassiveError as error:
            _report(error)
            raise
        logger.info("%s history: source=REST requests=%d", spec.name, requests)
        return self._static(table)

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
        reader = self._reader()
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
            read=functools.partial(reader.read_cycle, spec, request),
            pollers=self._state.pollers,
            host=host,
            path=paths,
            clock=poll.CLOCK,
        )


def _report(error: MassiveError) -> None:
    """Log a failure at ERROR with exactly the text its exception carries.

    The exception itself propagates to the caller, whose traceback shows where it came from,
    so the record holds the message alone: the HTTP status, the path, and the request id.

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
    pairs: Mapping[str, tuple[str, str]],
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
        pairs: The currencies of crypto symbols the quote list cannot split (`plan_calls`).

    Returns:
        The calls, in result order: for sessions, by date and then by symbol.
    """
    order = _ascending(spec, rest_path, request)
    requested = window.requested
    if requested is None:
        sessions = cast("DaySpan", window.sessions)
        return [
            call
            for day in sessions.days()
            if day.weekday() < _SATURDAY
            for call in plan_calls(
                spec,
                rest_path,
                request,
                symbols=request.symbols,
                extra={**order, _SESSION_DATE: day},
                pairs=pairs,
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
        pairs=pairs,
    )


def _announce(spec: DatasetSpec, requests: int, *, sessions: bool) -> None:
    """Log, before a long one-request-per-day download, how many requests it makes.

    A grouped-daily download makes one request per day, and a per-session download one request
    series per session and symbol, more where a session spans several pages. At a Basic key's 5
    requests a minute, the first five go at once and every further five take a minute. Only a
    download of more than five requests is announced.

    Args:
        spec: The dataset.
        requests: The number of planned requests (for sessions, of first pages).
        sessions: True for a per-session download, whose pages may add requests.
    """
    if requests <= _BASIC_REQUESTS_PER_MINUTE:
        return
    minutes = max(1, -(-(requests - _BASIC_REQUESTS_PER_MINUTE) // _BASIC_REQUESTS_PER_MINUTE))
    unit = "minute" if minutes == 1 else "minutes"
    if sessions:
        logger.info(
            "%s history: at least %d requests, one per session and symbol, more where a "
            "session spans several pages; at a Basic key's %d requests a minute that takes "
            "at least %d %s",
            spec.name,
            requests,
            _BASIC_REQUESTS_PER_MINUTE,
            minutes,
            unit,
        )
    else:
        logger.info(
            "%s history: %d requests, one per day; at a Basic key's %d requests a minute that "
            "takes about %d %s",
            spec.name,
            requests,
            _BASIC_REQUESTS_PER_MINUTE,
            minutes,
            unit,
        )


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


_REST_REMEDY: Final = (
    "read the 1-day flat files with source=dm.HistorySource.FLAT_FILES if S3 credentials are "
    "configured"
)
"""What a caller can do when Massive's REST bars are not cut at UTC midnight."""


def _stamped(
    spec: DatasetSpec, source: str, batch: pa.RecordBatch, *, rule: StampRule
) -> pa.RecordBatch:
    """Apply a stamp rule to a batch of bars (`history_source.stamp_rule`).

    Args:
        spec: The dataset, for the message.
        source: The REST path template the bars came from, for the message.
        batch: Rows in the dataset's schema.
        rule: `UTC_DAY` or `UTC_DAY_CHECKED` moves each stamp to 00:00 UTC of its day (the
            latter refusing a stamp off the day's first or last millisecond);
            `UTC_MIDNIGHT` refuses a stamp that is not 00:00 UTC and keeps the rest.

    Returns:
        The batch, with its `Timestamp` at the start of each bar's UTC day.

    A refused stamp raises the `MassiveError` of `timestamps.utc_day_stamps` or
    `timestamps.check_utc_midnights`, which names `source=dm.HistorySource.FLAT_FILES` as the
    way around it.
    """
    index = batch.schema.get_field_index("Timestamp")
    values = batch.column(index)
    if rule is StampRule.UTC_MIDNIGHT:
        check_utc_midnights(spec.name, values, source=source, remedy=_REST_REMEDY)
        return batch
    checked = rule is StampRule.UTC_DAY_CHECKED
    moved = utc_day_stamps(spec.name, values, source=source, checked=checked, remedy=_REST_REMEDY)
    return batch.set_column(index, "Timestamp", moved)
