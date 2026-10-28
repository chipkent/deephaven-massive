"""The REST transport: every REST dataset's requests, pages, and tables, on the session's client.

Massive's REST API serves the `fetch()` and `poll()` verbs of list, snapshot, and "last"
datasets and the REST side of `history()`. This module turns a dataset's declaration
(`DatasetSpec`) and a call's arguments (`Request`) into requests on the session's one
`_massive.RestClient`, and the pages that come back into tables in the dataset's schema:

- **Requests.** Each REST path is a generated `RestPath`, whose parameters say what can be
  sent and how. Path parameters come from the symbol (a key the dataset fills from the request,
  such as `{stocksTicker}`; a currency pair's `{from}/{to}`), from the typed parameters, from
  the bar `multiplier` and `timespan`, and for `history()` from the window (the aggregates'
  `{from}/{to}` dates, the grouped-daily `{date}`). Query parameters are the typed parameters,
  the validated `filters` (each key mapped back to its dotted wire name), the time axis
  (`DatasetSpec.time_axis`, encoded by its parameter's kind), the symbol parameter
  (`DatasetSpec.symbol_param`, at most `DatasetSpec.symbols_per_request` symbols a request),
  `adjusted`
  (`Request.adjusted`), the futures `resolution`, and the page size (`DatasetSpec.page_size`).
  `Request.limit` is never sent: it caps the total rows, and the client stops paging there.
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

from deephaven_massive._datasets.catalog import filter_key
from deephaven_massive._datasets.dates import CHICAGO, DAY_ZONES, NEW_YORK, FuturesSessions
from deephaven_massive._datasets.history_source import partition_dates
from deephaven_massive._datasets.schema import rename, required_fields, widen
from deephaven_massive._datasets.symbols import (
    SymbolTable,
    canonical,
    symbol_class,
    ws_symbol,
)
from deephaven_massive._datasets.timestamps import UnitMismatchError, instants
from deephaven_massive._enums import AssetClass, Timespan
from deephaven_massive._errors import MassiveError, from_wire, log_once
from deephaven_massive._ingest import hosts, poll
from deephaven_massive._ingest.poll import Pollers, RowDataError, UnsupportedParameterError
from deephaven_massive._massive import (
    DriftReport,
    ParamKind,
    Transport,
    WireError,
    WireUnit,
    to_batch,
)
from deephaven_massive._tables.diagnostics import ConnectionRow, ConnectionState
from deephaven_massive._tables.liveness import entered
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.dates import TimeRange
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

_SESSION_OPEN_HOUR: Final = 17
"""The Chicago hour a futures trading session opens, on the day before its session date."""

_UNIT_STEPS: Final[Mapping[WireUnit, timedelta]] = {
    WireUnit.S: timedelta(seconds=1),
    WireUnit.MS: timedelta(milliseconds=1),
    WireUnit.US: timedelta(microseconds=1),
}
"""One step of each timestamp unit a `datetime` can express exactly; nanoseconds cannot be."""

_RATE_LIMIT_ADVICE: Final = (
    "%s: Massive rate-limited this session's REST requests; Basic keys allow 5 requests per "
    "minute, and a fan-out poll() makes one request per symbol per cycle"
)


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


def _day_start(spec: DatasetSpec, day: date) -> datetime:
    """Return the instant a date of the dataset's time axis starts.

    A calendar day starts at midnight in the asset class's day zone (New York for a dataset
    without a class). A futures date names a trading session, which opens at 17:00 Chicago time
    on the day before its `session_end_date` (`dates.futures_session`).

    Args:
        spec: The dataset.
        day: The date.

    Returns:
        The aware instant.
    """
    if spec.asset_class is AssetClass.FUTURES:
        eve = day - _ONE_DAY
        return datetime(eve.year, eve.month, eve.day, _SESSION_OPEN_HOUR, tzinfo=CHICAGO)
    zone = NEW_YORK if spec.asset_class is None else DAY_ZONES[spec.asset_class]
    return datetime(day.year, day.month, day.day, tzinfo=zone)


def _instant_bound(
    spec: DatasetSpec, name: str, value: date, *, unit: WireUnit | None, upper: bool
) -> datetime:
    """Return the instant a timestamp parameter of the time axis is sent.

    A bound is a `date` (a whole day, inclusive at either end) or an aware `datetime` (the
    first instant included at the lower end, the first excluded at the upper end). A date
    becomes the instant its day starts (`_day_start`: a midnight in the day zone, or a futures
    session's 17:00 Chicago open), the next day's start for an upper bound. An exclusive lower
    `.gt` or an inclusive upper `.lte` moves the instant back one unit of the parameter.

    Args:
        spec: The dataset, for its day rules and messages.
        name: The parameter's wire name, such as `"timestamp.gte"`.
        unit: The unit the parameter counts in.
        value: The bound.
        upper: True for the upper bound.

    Returns:
        The instant to send.

    Raises:
        ValueError: If a parameter in nanoseconds is a `.gt` or `.lte` bound, which a
            `datetime` cannot move by one nanosecond exactly; such a dataset declares the
            `.gte` and `.lt` pair instead.
    """
    if isinstance(value, datetime):
        instant = value
    else:
        instant = _day_start(spec, value + _ONE_DAY if upper else value)
    if name.rpartition(".")[2] not in ("gt", "lte"):
        return instant
    step = _UNIT_STEPS.get(cast("WireUnit", unit))
    if step is None:
        raise ValueError(
            f"{spec.name}: {name} counts nanoseconds, so it cannot bound a half-open range "
            "exactly; declare the .gte and .lt pair"
        )
    return instant - step


def _date_bound(name: str, value: date, *, upper: bool) -> date:
    """Return the value a date parameter of the time axis is sent.

    A `date` bound is a whole day: the day itself, the day after for an exclusive upper
    `.lt`, the day before for an exclusive lower `.gt`. An aware `datetime` is sent as is, as
    ISO-8601 text, which Massive's date filters accept.

    Args:
        name: The parameter's wire name, such as `"ex_dividend_date.gte"`.
        value: The bound.
        upper: True for the upper bound.

    Returns:
        The value to send.
    """
    suffix = name.rpartition(".")[2]
    if isinstance(value, datetime):
        return value
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
    exact: tuple[datetime, datetime] | None = None,
) -> dict[str, object]:
    """Return the time-axis parameters of a request, by wire name.

    A timestamp parameter gets an instant (`_instant_bound`), a date parameter a date or an
    instant as ISO text (`_date_bound`), and a parameter of any other kind the bound's
    ISO-8601 text.

    Args:
        spec: The dataset, whose `time_axis` names the lower and upper parameters.
        rest_path: The path; a name it does not take is left out.
        dates: The bounds as the caller gave them (a `date` or an aware `datetime`, or None
            for an open end), or for date-bounded futures the first and last sessions.
        exact: For `history()`, the window's exact instants, which a timestamp parameter uses
            instead of `dates`; None to derive instants from `dates`.

    Returns:
        The parameters to send; empty when the dataset has no time axis.

    A nanosecond parameter that is a `.gt` or `.lte` bound raises the `ValueError` of
    `_instant_bound`.
    """
    if spec.time_axis is None:
        return {}
    params = {p.name: (p.kind, p.unit) for p in rest_path.params}
    query: dict[str, object] = {}
    for index, name in enumerate(spec.time_axis):
        upper = index == 1
        found = params.get(name)
        value = dates[index]
        if found is None or value is None:
            continue
        kind, unit = found
        if kind is ParamKind.TIMESTAMP:
            bound = value if exact is None else exact[index]
            query[name] = _instant_bound(spec, name, bound, unit=unit, upper=upper)
        elif kind is ParamKind.DATE:
            query[name] = _date_bound(name, value, upper=upper)
        else:
            query[name] = value.isoformat()
    return query


def _pair(spec: DatasetSpec, symbol: str) -> tuple[str, str]:
    """Split a forex or crypto symbol into the two currencies a `{from}/{to}` path takes.

    Args:
        spec: The dataset, whose asset class is forex or crypto.
        symbol: The canonical symbol, such as `C:EURUSD` or `X:BTCUSD`.

    Returns:
        The base and quote currencies, such as `("EUR", "USD")`.

    Raises:
        ValueError: If the dataset's class has no currency pairs.
    """
    if spec.asset_class not in (AssetClass.FOREX, AssetClass.CRYPTO):
        raise ValueError(f"{spec.name}: only forex and crypto symbols are currency pairs")
    spelled = ws_symbol(spec.asset_class, symbol)
    base, _, quote = spelled.replace("/", "-").partition("-")
    return base, quote


def _symbol_parts(
    spec: DatasetSpec,
    rest_path: RestPath,
    symbols: NormalizedSymbols | None,
    *,
    slot: str | None,
) -> list[tuple[str | None, dict[str, object], dict[str, object]]]:
    """Return how a request's symbols spread over requests of one path.

    A path that takes the symbol itself (a key the dataset fills from the request, such as
    `{stocksTicker}`, or a currency pair's `{from}/{to}`), a path whose one placeholder nothing
    else fills (`slot`: a per-symbol detail path whose rows also carry the ticker), a
    `per_symbol` dataset, and a symbol parameter that takes one value (`ticker`) get one
    request per symbol. A list parameter
    (`ticker.any_of`, `tickers`) gets up to `DatasetSpec.symbols_per_request` symbols a
    request (all of them when the dataset sets no limit). `dm.ALL`, no
    symbols, and a path that takes neither get one request without symbols.

    Args:
        spec: The dataset.
        rest_path: The path.
        symbols: The static symbols: a sorted tuple, `ALL`, or None.
        slot: The path's one placeholder that no parameter, key, or window fills, or None.

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
    takes_one = bool(from_request) or slot is not None or spec.per_symbol
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
    parts: list[tuple[str | None, dict[str, object], dict[str, object]]] = []
    for symbol in explicit:
        path: dict[str, object] = {}
        query: dict[str, object] = {}
        for key in from_request:
            if key.column == "from" and "to" in params:
                path["from"], path["to"] = _pair(spec, symbol)
            elif params[key.column].in_path:
                path[key.column] = symbol
            else:
                query[key.column] = symbol
        if slot is not None:
            path[slot] = symbol
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
    futures `resolution` and the page size when the path takes them.

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
        query["limit"] = spec.page_size
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
    axis: Mapping[str, object] | None = None,
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
        axis: Time-axis query parameters.

    Returns:
        The calls, in result order: per symbol, or per chunk of symbols, or one.

    A path that needs a symbol per request, called without explicit symbols, raises the
    `ValueError` of `_symbol_parts`.
    """
    path, query = _base(spec, rest_path, request, fixed=fixed or {})
    query.update(axis or {})
    keys = spec.keys_for(Transport.REST, rest_path.template).declared()
    filled = set(path) | {key.column for _, key in keys if key.from_request}
    if "from" in filled:
        filled.add("to")  # a currency pair fills both
    empty = [p.name for p in rest_path.params if p.in_path and p.name not in filled]
    slot = empty[0] if len(empty) == 1 else None
    return [
        Call({**path, **own_path}, {**query, **own_query}, symbol)
        for symbol, own_path, own_query in _symbol_parts(spec, rest_path, symbols, slot=slot)
    ]


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
        asset_class: The dataset's asset class, by whose rules a symbol read from the rows is
            canonicalized.
    """

    dataset: str
    rest_path: RestPath
    columns: tuple[tuple[ColumnType, str, SourceColumn | None], ...]
    arrow_schema: pa.Schema
    required: frozenset[str]
    required_on_error: frozenset[str]
    projected: bool
    asset_class: AssetClass | None

    @classmethod
    def build(cls, catalog: Catalog, spec: DatasetSpec, rest_path: RestPath) -> _Converter:
        """Prepare the conversion from the path's generated fields and the dataset's schema.

        Args:
            catalog: The catalog, for the dataset's merged schema.
            spec: The dataset.
            rest_path: The path.

        Returns:
            The converter.
        """
        schema = catalog.dataset_schema(spec)
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
            asset_class=spec.asset_class,
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
            The batch in the dataset's schema.

        A timestamp out of range raises the `UnitMismatchError` of `timestamps.instants`.
        """
        rows = batch.num_rows
        arrays = []
        for column_type, name, source in self.columns:
            target = column_type.arrow_type
            if source is None:
                arrays.append(pa.nulls(rows, target))
            elif source.field is None:
                arrays.append(pa.array([symbol] * rows, target))
            else:
                values = batch.column(source.field.column)
                if name == "Symbol":
                    values = pa.array(
                        [self._canonical(value) for value in values.to_pylist()], pa.string()
                    )
                if source.unit is not None:
                    label = f"{self.dataset}.{name}"
                    values = instants(label, source.unit, values, time_column=name == "Timestamp")
                arrays.append(widen(values, target))
        return pa.RecordBatch.from_arrays(arrays, schema=self.arrow_schema)

    def _canonical(self, value: str) -> str:
        """Return a symbol read from the rows in its canonical form (`AUD/USD` as `C:AUDUSD`).

        Args:
            value: The row's symbol; the rows always carry it (a required key).

        Returns:
            The canonical symbol; a value that is no valid symbol of the class is kept as sent,
            and logged once per process at WARNING.
        """
        try:
            return canonical(self.asset_class, value)
        except (TypeError, ValueError):
            log_once(
                logger,
                f"row-symbol:{self.dataset}",
                "%s: Massive sent the symbol %r, which is not a valid symbol of its class; it is "
                "kept as sent",
                self.dataset,
                value,
                level=logging.WARNING,
            )
            return value


def _keep(batch: pa.RecordBatch, window: TimeRange | FuturesSessions | None) -> pa.RecordBatch:
    """Keep the rows of a `history()` window: `[start, end)` on `Timestamp`, or the sessions.

    A request can return rows outside the window: the aggregates' `{from}/{to}` take whole
    days, and futures bars are requested over a widened window. Without the column the window
    is about, every row is kept.

    Args:
        batch: Rows in the dataset's schema.
        window: The `history()` window, or None for `fetch()` and `poll()`.

    Returns:
        The rows inside the window.
    """
    names = batch.schema.names
    if isinstance(window, FuturesSessions):
        if "SessionEndDate" not in names:
            return batch
        sessions = batch.column("SessionEndDate")
        mask = pc.and_(
            pc.greater_equal(sessions, pa.scalar(window.first)),
            pc.less_equal(sessions, pa.scalar(window.last)),
        )
        return batch.filter(mask)
    if window is None or "Timestamp" not in names:
        return batch
    times = batch.column("Timestamp")
    start = pa.scalar(window.start_ns, times.type)
    end = pa.scalar(window.end_ns, times.type)
    return batch.filter(pc.and_(pc.greater_equal(times, start), pc.less(times, end)))


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
    lock, held only to read or change them; the client and the pollers are thread-safe.
    """

    __slots__ = ("_client", "_hosts", "_lock", "_once", "label", "pollers")

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
    """

    state: RestState
    catalog: Catalog
    drift: DriftRegistry
    max_concurrency: int

    def rest_path(self, template: str) -> RestPath:
        """Return a generated REST path.

        Args:
            template: The path template.

        Returns:
            The path.
        """
        return cast("RestPath", self.catalog.source(Transport.REST, template))

    def read(
        self,
        spec: DatasetSpec,
        rest_path: RestPath,
        calls: Sequence[Call],
        *,
        limit: int | None,
        window: TimeRange | FuturesSessions | None = None,
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
            record: Also given each page's drift, for a `poll()` table's `SchemaDrift`.
            executor: A pool to run several calls on, which the caller keeps; None for a pool
                of this read's own.

        Returns:
            The rows, in call order and then page order, and the number of requests made.

        Errors: the public error of each `WireError` (`_public`), and `RowDataError` for a row
        that breaks the schema. A closed session raises `MassiveError("... is closed")`.
        """
        converter = _Converter.build(self.catalog, spec, rest_path)
        run = functools.partial(
            self._call, converter, limit=limit, window=window, record=record, spec=spec
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
        window: TimeRange | FuturesSessions | None,
        record: Callable[[DriftReport], None] | None,
    ) -> _Outcome:
        """Request one call's pages and convert their rows.

        Args:
            converter: The path's conversion.
            call: The call.
            spec: The dataset.
            limit: The cap on the call's rows, or None.
            window: The `history()` window whose rows to keep, or None.
            record: Also given each page's drift, or None.

        Returns:
            The call's rows and request count; no rows for a per-symbol call answered with
            HTTP 404, which logs one WARNING per symbol and session.

        A failed request raises the public error of its `WireError` (`_public`), and a row
        that breaks the schema the `RowDataError` of `_Converter.convert`.
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
                self._warn_error_rows(converter, flagged)
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
        self, converter: _Converter, flagged: Sequence[Mapping[str, object]]
    ) -> None:
        """Log each ticker of an error row once per session, naming Massive's message.

        A projected path dropped the row; any other path kept it, with its `Error` and
        `Message` columns.

        Args:
            converter: The path's conversion.
            flagged: The page's error rows.
        """
        dataset = converter.dataset
        fate = "dropped" if converter.projected else "kept with its Error and Message columns"
        for row in flagged:
            ticker = str(row.get("ticker"))
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
            Each path the cycle reads (`fetch_paths`) with its calls.

        A parameter a path does not take raises the `ValueError` of `_base`.
        """
        planned = []
        for template, own in fetch_paths(spec, symbols):
            rest_path = self.rest_path(template)
            axis = _axis_query(spec, rest_path, dates=(request.start, request.end))
            planned.append(
                (rest_path, plan_calls(spec, rest_path, request, symbols=own, axis=axis))
            )
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
            planned: Each path with its calls (`plan_fetch`); never empty.
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
                spec, rest_path, calls, limit=request.limit, record=record, executor=executor
            )
            tables.append(table)
            requests += made
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
        return RestReader(self._state, core.catalog, core.drift, core.config.max_concurrency)

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
        return decision

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

    def history(
        self, spec: DatasetSpec, request: Request, window: TimeRange | FuturesSessions
    ) -> Table:
        """Return the dataset's REST history over `window`, paged internally, as a static table.

        `dm.ALL` reads the grouped-daily path, one request per day of the window. Explicit
        symbols read the history path: the window goes out as the dataset's time axis (exact
        instants for a timestamp parameter, the sessions for date-bounded futures), or as the
        aggregates' `{from}/{to}` millisecond epochs; one request per symbol where the path
        takes one.
        Rows outside the window (`[start, end)` on `Timestamp`, or the sessions on
        `SessionEndDate`) are dropped. Logs `source=REST requests=<n>` at INFO.

        Args:
            spec: The dataset; `history()` has checked that REST can serve the request.
            request: The normalized arguments, with the raw `start` and `end` bounds and static
                symbols.
            window: The bounds under the class's day rules.

        Returns:
            A static table in the dataset's schema.

        Raises:
            MassiveError: As `fetch` raises it, logged at ERROR first.
        """
        reader = self._reader()
        dates = partition_dates(spec, window)
        if isinstance(request.symbols, _All):
            rest_path = reader.rest_path(cast("str", spec.grouped_daily_path))
            path, query = _base(spec, rest_path, request, fixed={})
            calls = [Call({**path, "date": day}, query) for day in dates]
        else:
            rest_path = reader.rest_path(cast("str", spec.history_path))
            # The aggregates take their range as `{from}/{to}` timestamp segments.
            kinds = {p.name: p.kind for p in rest_path.params if p.in_path}
            ranged = kinds.get("from") is ParamKind.TIMESTAMP
            fixed = _range_segments(window) if ranged else {}
            bounds: tuple[date | None, date | None]
            if isinstance(window, FuturesSessions):
                bounds = (window.first, window.last)
                window_instants = (window.window.start, window.window.end)
            else:
                bounds = (request.start, request.end)
                window_instants = (window.start, window.end)
            axis = _axis_query(spec, rest_path, dates=bounds, exact=window_instants)
            calls = plan_calls(
                spec, rest_path, request, symbols=request.symbols, fixed=fixed, axis=axis
            )
        try:
            table, requests = reader.read(spec, rest_path, calls, limit=None, window=window)
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
        else:
            reader.plan_fetch(spec, request, request.symbols)
        host = urlsplit(self._core.config.rest_base_url).hostname
        candidates = (spec.fetch_path, spec.detail_path, *spec.fetch_paths_by_class.values())
        paths = ", ".join(dict.fromkeys(path for path in candidates if path is not None))
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


def _range_segments(window: TimeRange | FuturesSessions) -> dict[str, object]:
    """Return an aggregates path's `{from}` and `{to}` segments for a `history()` window.

    The segments are millisecond epochs, both inclusive: the window's first instant and its
    last whole millisecond before the end. Rows outside the window are dropped afterwards
    (`_keep`).

    Args:
        window: The window; date-bounded futures use their widened bar window.

    Returns:
        `{"from": ..., "to": ...}`, as instants the client counts in milliseconds.
    """
    span = window.window if isinstance(window, FuturesSessions) else window
    first = span.start.replace(microsecond=span.start.microsecond // 1000 * 1000)
    last = span.end - timedelta(microseconds=1)
    return {"from": first, "to": last.replace(microsecond=last.microsecond // 1000 * 1000)}


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
