"""The requests of a REST call: which paths it reads, and each request's parameters.

A REST read of a dataset (`fetch()`, a `poll()` cycle, the REST side of `history()`) is planned
before any request is made. This module turns a dataset's declaration (`DatasetSpec`), a call's
arguments (`Request`), and the generated REST paths into `Call`s, one per request series:

- `fetch_paths` picks the paths a `fetch()` or `poll()` reads, by the class of each symbol;
- `plan_calls` fills each path's parameters: the path placeholders from the symbol (a key the
  dataset fills from the request, a currency pair's `{from}/{to}`), the typed parameters, the
  bar `multiplier` and `timespan` (`shared_params`); the validated `filters`; the time axis,
  whole days on a calendar-date axis and the instants that start the bounds' days on an
  instant axis, in the day zone of the request's own symbols (`axis_query`); the symbol
  parameter, at most `DatasetSpec.symbols_per_request` symbols a request; `adjusted`, the
  futures `resolution`, the page size (`page_size`), and for `history()` the ascending
  direction;
- `history_calls` plans a `history()` call on the history path: futures sessions one session
  at a time by `session_end_date`, and every other window as its aggregate range segments.

Everything here is pure: it reads the catalog's generated paths and the call's arguments, makes
no request, starts no thread, and keeps no state, so it is tested without a fake server. A
crypto pair is split by the caller's `CryptoSplit` (the REST transport reads Massive's crypto
ticker list), never by a guess. The REST transport (`rest.py`) runs the calls this module plans.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum, auto
from typing import TYPE_CHECKING, Final, TypeAlias

from deephaven_massive._datasets.dates import day_class, instant_bound, is_trading_day
from deephaven_massive._datasets.filters import filter_key, param_base, takes_instants
from deephaven_massive._datasets.history_source import session_date_param
from deephaven_massive._datasets.symbols import AllSymbols, forex_codes, symbol_class
from deephaven_massive._datasets.wire_symbols import contracts_underlying
from deephaven_massive._enums import AssetClass, ClassFromPrefix, Timespan
from deephaven_massive._errors import UnmatchedFormError, required
from deephaven_massive._massive import ParamKind, Transport

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.dates import TimeRange
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._massive import RestPath

__all__ = [
    "BARE_INDEX_PARAM",
    "Call",
    "CryptoSplit",
    "axis_query",
    "fetch_paths",
    "history_calls",
    "largest_page",
    "own_symbols",
    "page_size",
    "plan_calls",
    "shared_params",
]


_FUTURES_UNITS: Final[Mapping[Timespan, str]] = {
    Timespan.SECOND: "sec",
    Timespan.MINUTE: "min",
    Timespan.HOUR: "hour",
    Timespan.DAY: "day",
    Timespan.SESSION: "session",
    Timespan.WEEK: "week",
    Timespan.MONTH: "month",
    Timespan.QUARTER: "quarter",
    Timespan.YEAR: "year",
}
"""The unit of the futures aggregates' `resolution` parameter (`"5min"`) for each timespan.
Massive's documentation lists no `day` unit, but it serves `1day` bars in the shape of
`1session` bars."""

_ONE_DAY: Final = timedelta(days=1)

_ONE_MICROSECOND: Final = timedelta(microseconds=1)

BARE_INDEX_PARAM: Final = "underlying_ticker"
"""The query parameter that takes an index without its `I:`: the options contracts list's
`underlying_ticker` finds no contract for `I:SPX` and every one of `SPX`, so the request keeps
the canonical ticker and only the wire gets the bare one (`wire_symbols.contracts_underlying`)."""

CryptoSplit: TypeAlias = "Callable[[str], tuple[str, str]]"
"""Splits a crypto ticker into its base and quote currencies (`RestReader.crypto_pair`)."""


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


def axis_query(
    spec: DatasetSpec,
    rest_path: RestPath,
    *,
    dates: tuple[date | None, date | None],
    asset_class: AssetClass | ClassFromPrefix,
) -> dict[str, object]:
    """Return the time-axis parameters of a request, by wire name.

    An instant parameter (`filters.takes_instants`: a timestamp, or a date parameter whose
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

    `filters.check_filters` keeps a date given to a range filter of a parameter that compares
    instants (`filters.takes_instants`, such as `last_updated.gte`), because its day zone
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
        The base and quote currencies, such as `("EUR", "USD")`; for a forex symbol without
        the `C:` prefix, the symbol and an empty quote.

    Raises:
        ValueError: If the dataset's class has no currency pairs.

    A crypto symbol `split` cannot split raises its `MassiveError`; no split is ever guessed.
    """
    asset_class = spec.asset_class
    if asset_class is not AssetClass.FOREX and asset_class is not AssetClass.CRYPTO:
        raise ValueError(f"{spec.name}: only forex and crypto symbols are currency pairs")
    if asset_class is AssetClass.CRYPTO:
        return split(symbol)
    codes = forex_codes(symbol)
    # A symbol without the `C:` prefix has no codes to derive: it goes on the wire as written,
    # whole, and Massive answers it.
    return (symbol, "") if codes is None else codes


def own_symbols(
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


class _Spread(Enum):
    """How a request's symbols spread over requests of one path (`_symbol_parts`).

    Attributes:
        PATH: The path or the dataset takes each symbol itself: one request per symbol, and
            the request must name its symbols.
        ONE: The symbol parameter takes one value: one request per symbol.
        LIST: The symbol parameter takes a list: up to `DatasetSpec.symbols_per_request`
            symbols a request.
        NONE: The path takes no symbol: one request without symbols.
    """

    PATH = auto()
    ONE = auto()
    LIST = auto()
    NONE = auto()


def _symbol_parts(
    spec: DatasetSpec,
    rest_path: RestPath,
    symbols: tuple[str, ...] | AllSymbols | None,
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
    if bool(from_request) or slot is not None or pair or spec.per_symbol:
        spread = _Spread.PATH
    elif param is None:
        spread = _Spread.NONE
    elif params[param].kind is ParamKind.LIST:
        spread = _Spread.LIST
    else:
        spread = _Spread.ONE
    if symbols is None or isinstance(symbols, AllSymbols):
        if spread is _Spread.PATH:
            raise ValueError(f"{spec.name}: {rest_path.template} needs explicit symbols")
        return [(None, {}, {})]
    explicit = symbols
    if not explicit:
        return []
    if spread is _Spread.NONE:
        return [(None, {}, {})]
    if spread is _Spread.LIST:
        size = spec.symbols_per_request or len(explicit)
        listed = required(param, f"{rest_path.template}'s list parameter")
        return [(None, {}, {listed: explicit[i : i + size]}) for i in range(0, len(explicit), size)]
    targets = _symbol_targets([key.column for key in from_request], slot=slot, pair=pair)
    return [
        _symbol_request(spec, symbol, rest_path, targets=targets, param=param, split=split)
        for symbol in explicit
    ]


def _symbol_request(
    spec: DatasetSpec,
    symbol: str,
    rest_path: RestPath,
    *,
    targets: Sequence[str],
    param: str | None,
    split: CryptoSplit,
) -> tuple[str, dict[str, object], dict[str, object]]:
    """Return the parameters of one symbol's request.

    The symbol fills each target: a `{from}/{to}` pair takes its two currencies (`_pair`), a
    path placeholder or a query parameter the symbol itself. The symbol parameter, when the
    path has one, takes the symbol too: as a one-symbol tuple when it takes a list.

    Args:
        spec: The dataset, for messages.
        symbol: The symbol.
        rest_path: The path.
        targets: The parameters the symbol fills (`_symbol_targets`), `"from"` standing for
            the `{from}/{to}` pair.
        param: The dataset's symbol parameter when the path has it, or None.
        split: Splits a crypto symbol into its two currencies (`_pair`).

    Returns:
        `(symbol, path parameters, query parameters)`.
    """
    params = {p.name: p for p in rest_path.params}
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
        query[param] = (symbol,) if params[param].kind is ParamKind.LIST else symbol
    return symbol, path, query


def _caller_params(
    spec: DatasetSpec, rest_path: RestPath, request: Request
) -> tuple[dict[str, object], dict[str, object]]:
    """Return the path and query parameters the caller set: typed parameters and filters.

    Typed parameters go wherever the path takes them: a path segment (`{date}`,
    `{direction}`) or a query parameter. An index given to `BARE_INDEX_PARAM` goes without its
    `I:` (`I:SPX` as `SPX`). `order` on a path without an `order` parameter folds
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
            (path if params[name].in_path else query)[name] = _sent_value(name, value)
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


def page_size(spec: DatasetSpec, rest_path: RestPath) -> int | None:
    """Return the page size a request of a dataset's path asks for, before any row cap.

    It is the dataset's own `DatasetSpec.page_size` where it sets one (below the documented
    maximum for long rows, or a maximum the specification states only in prose), and the
    path's documented maximum `limit` (`Param.maximum`) otherwise. The catalog's `page size`
    gate ensures every catalog dataset's paged path has one.

    Args:
        spec: The dataset.
        rest_path: One of its REST paths.

    Returns:
        The page size; None when the path takes no `limit` query parameter, or documents no
        maximum for a dataset that sets no `page_size`.
    """
    for param in rest_path.params:
        if param.name == "limit" and not param.in_path:
            return param.maximum if spec.page_size is None else spec.page_size
    return None


def largest_page(rest_path: RestPath) -> int:
    """Return the largest page a path serves: its documented maximum `limit`.

    The library's own lookups read whole lists (the crypto ticker list, the futures contracts
    and schedules), so each asks for this page, the fewest requests Massive allows, and
    follows the specification when Massive changes it.

    Args:
        rest_path: The path.

    Returns:
        The maximum of its `limit` query parameter (`Param.maximum`), such as `1000`.

    Raises:
        ValueError: If the path documents no maximum `limit`, a defect of the library: a
            lookup was pointed at a path that does not serve one.
    """
    for param in rest_path.params:
        if param.name == "limit" and not param.in_path and param.maximum is not None:
            return param.maximum
    raise ValueError(f"{rest_path.template} documents no maximum page size (`limit`)")


def _sent_value(name: str, value: object) -> object:
    """Return a typed parameter's value as sent: an index underlying bare, every other as is.

    Args:
        name: The wire parameter's name.
        value: Its value.

    Returns:
        `contracts_underlying(value)` for the options contracts' underlying; `value` otherwise.
    """
    if name == BARE_INDEX_PARAM and isinstance(value, str):
        return contracts_underlying(value)
    return value


def shared_params(
    spec: DatasetSpec, rest_path: RestPath, request: Request, *, fixed: Mapping[str, object]
) -> tuple[dict[str, object], dict[str, object]]:
    """Return the path and query parameters every request of a path shares.

    Only parameters the path declares are sent: the caller's (`_caller_params`); `multiplier`
    in a path placeholder of that name; `timespan` wherever the path takes it (the aggregates'
    segment, the indicators' query parameter); `Request.adjusted` when it is set and the path
    takes it (a path without it, the crypto indicators', serves prices no split changes, so
    `True` is left out there); and the
    futures `resolution` and the page size when the path takes them. The page size is
    `DatasetSpec.page_size`, or else the path's documented maximum `limit`, or `Request.limit`
    when that is smaller; a path that documents no maximum, for a dataset without a
    `page_size`, gets only `Request.limit`.

    Args:
        spec: The dataset.
        rest_path: The path.
        request: The normalized arguments.
        fixed: Path parameters the caller sets, such as a `history()` window's `{from}` and
            `{to}` dates; they take precedence.

    Returns:
        `(path parameters, query parameters)`.

    Raises:
        ValueError: If `Request.adjusted` is False and the path has no `adjusted` parameter
            (the crypto indicators): unadjusted prices are never dropped silently.

    Any other parameter the caller set that the path does not take raises the `ValueError` of
    `_caller_params`.
    """
    params = {p.name: p for p in rest_path.params}
    path, query = _caller_params(spec, rest_path, request)
    if "multiplier" in params and params["multiplier"].in_path:
        path["multiplier"] = request.multiplier
    if request.timespan is not None and "timespan" in params:
        (path if params["timespan"].in_path else query)["timespan"] = request.timespan
    if "adjusted" in params and request.adjusted is not None:
        query["adjusted"] = request.adjusted
    elif request.adjusted is False:
        raise ValueError(f"{spec.name}: {rest_path.template} does not take ['adjusted']")
    if request.timespan is not None and "resolution" in params:
        query["resolution"] = f"{request.multiplier}{_FUTURES_UNITS[request.timespan]}"
    if "limit" in params:
        # A page size, as the list paths read `limit`. The aggregates' range path reads its
        # `limit` as the number of base aggregates queried instead (at most 50,000); its
        # datasets set that maximum, and bars take no `limit` argument, so no cap shrinks it.
        sizes = [v for v in (page_size(spec, rest_path), request.limit) if v is not None]
        if sizes:
            query["limit"] = min(sizes)
    path.update(fixed)
    return path, query


def fetch_paths(
    spec: DatasetSpec, symbols: tuple[str, ...] | AllSymbols | None
) -> list[tuple[str, tuple[str, ...] | AllSymbols | None]]:
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

    Raises:
        UnmatchedFormError: For a form of symbols with no rule here.
    """
    default = required(spec.fetch_path, f"{spec.name}'s fetch path")
    match symbols:
        case tuple() if symbols:
            pass
        case tuple() | AllSymbols() | None:
            return [(default, symbols)]
        case _:
            raise UnmatchedFormError(symbols, "a REST call's paths")
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
    symbols: tuple[str, ...] | AllSymbols | None,
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
        dates: The time-axis bounds (`axis_query`); open by default.
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
    `ValueError` of `_symbol_parts`; an inexact axis bound the `ValueError` of `axis_query`.
    """
    path, query = shared_params(spec, rest_path, request, fixed=fixed or {})
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
        zone_class = day_class(spec, map(symbol_class, own_symbols(spec, symbol, own_query)))
        axis = axis_query(spec, rest_path, dates=dates, asset_class=zone_class)
        sent = _filter_days(rest_path, {**query, **axis}, zone_class)
        calls.append(Call({**path, **own_path}, {**sent, **(extra or {}), **own_query}, symbol))
    return calls


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
    axis = None if spec.time_axis is None else param_base(spec.time_axis[0])
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
    outside the window are dropped afterwards (`keep_window`).

    Args:
        requested: The instants to request.

    Returns:
        `{"from": ..., "to": ...}`, as instants the client counts in milliseconds.
    """
    first = requested.start.replace(microsecond=requested.start.microsecond // 1000 * 1000)
    last = requested.end - _ONE_MICROSECOND
    return {"from": first, "to": last.replace(microsecond=last.microsecond // 1000 * 1000)}


def history_calls(
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
        sessions = required(window.sessions, f"{spec.name}'s history sessions")
        session = session_date_param(spec)
        return [
            call
            for day in sessions.days()
            if is_trading_day(spec.asset_class, day)
            for call in plan_calls(
                spec,
                rest_path,
                request,
                symbols=request.static_symbols,
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
        symbols=request.static_symbols,
        fixed=_range_segments(requested) if ranged else {},
        dates=(requested.start, requested.end),
        extra=order,
        split=split,
    )
