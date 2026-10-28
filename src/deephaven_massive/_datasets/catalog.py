"""The dataset catalog: one frozen `DatasetSpec` per dataset, and the `Catalog` that holds them.

Every dataset (`m.stocks.trades`, `m.reference.news`, ...) is described once, declaratively, by
a `DatasetSpec` in its namespace module under `_namespaces/`. Each of those modules exports
`DATASETS: tuple[DatasetSpec, ...]`, and `_session.py` composes them, once, at import, into the
process's `Catalog` (`CATALOG`), which every session core holds. This module names no namespace
module and imports nothing above the dataset layer, so the catalog is built by the code that
owns the namespaces and handed down.

A spec names its sources, never its columns. Each source is a generated `Source` of the wire
package: a REST path by role (`fetch_path`, `detail_path`, `history_path`,
`grouped_daily_path`, `extra_paths`), a WebSocket channel, or a flat-file prefix. Its fields,
their wire types, and their timestamp units come from the committed generated files; the spec
adds only what the vendor's files cannot say: the dataset's kind, and, per source, which column
holds each key (`KeySpec`, grouped per source in a `SourceKeys`). The catalog merges the rest
(`Catalog.dataset_schema`).

A spec also says which transports serve the dataset, and its query class follows from them:
`query_class` in `_namespaces/base.py` maps the channels, the history sources, and the fetch
path to one of the query classes, and `DatasetSpec` rejects, when it is created, the
combinations no query class serves.

`Catalog.problems` runs the catalog gates over every spec, against the generated sources:
generated sources, declared keys, unique columns, member types, kind membership, and wire
parameters (see the method). The tests run it on the process's catalog and on synthetic specs.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import date, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, cast

from deephaven_massive._datasets.dates import parse_date_like, require_aware
from deephaven_massive._datasets.schema import dataset_schema, rename
from deephaven_massive._enums import AssetClass, ChannelFamily, PollMode, Timespan
from deephaven_massive._massive import ParamKind, Transport, WireType, sources

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from deephaven_massive._datasets.schema import Schema
    from deephaven_massive._massive import RestPath, Source

__all__ = [
    "Catalog",
    "DatasetSpec",
    "KeySpec",
    "SourceKeys",
]

logger = logging.getLogger(__name__)

_FROM_REQUEST_ROLES: Final = frozenset({"symbol", "fanout"})
"""The key roles a source may leave out of its rows, filled from the request instead."""

_LIBRARY_PARAMS: Final = frozenset({"limit", "sort", "order", "adjusted"})
"""Query parameters the library sets from typed arguments, never through `filters`."""

_TIME_KINDS: Final = frozenset({ParamKind.DATE, ParamKind.TIMESTAMP})
"""The parameter kinds whose text value must be an ISO-8601 date or an instant with an offset."""

_FILTER_SUFFIXES: Final = ("gt", "gte", "lt", "lte", "any_of", "all_of")
"""The suffixes the specification adds to a parameter's base name (`timestamp.gte`)."""

_EQUALITY_SUFFIXES: Final = ("any_of", "all_of")
"""The suffixes that, like the base name, select rows by equal values (`tickers.any_of`)."""

_LIST_TYPES: Final = frozenset({WireType.JSON_TEXT, WireType.INT_ARRAY})
"""Wire types of a field that holds several values, such as a filing's list of tickers."""

_SOURCE_GONE: Final = "deprecated or removed in the snapshot"


@dataclass(frozen=True, slots=True)
class KeySpec:
    """Where one source carries one of a dataset's keys.

    Example:
        ::

            from deephaven_massive._datasets.catalog import KeySpec

            KeySpec("SipTimestamp")  # the source's own SipTimestamp column
            KeySpec("stocksTicker", from_request=True)  # the symbol is a path parameter
            KeySpec("FilerCik", instrument=False)  # a fan-out key that is no instrument
            KeySpec("Ticker", required=False)  # a ticker Massive sends only "if available"

    Attributes:
        column: The source's generated column that holds the key, after the projection of the
            dataset's membership of its kind (`"Ticker"`, `"SipTimestamp"`). For a key filled
            from the request, the request parameter the key's value is sent in
            (`"stocksTicker"`). A symbol a path takes as two parameters (`{from}/{to}`) names
            the first, `"from"`: the library sends the pair's two currencies in `from` and
            `to` and fills the key with the whole symbol, never with `from` alone.
        from_request: True when the source's rows do not carry the key: a symbol taken as a
            path parameter (`/v3/trades/{stockTicker}`) or held outside the result key
            (`/v1/last/crypto/{from}/{to}`). The library then fills the key as a constant
            column, and it is not a required key of the source.
        instrument: For a `fanout` key, whether it names an instrument. True, the default,
            renames it to `Underlying` (the options underlying, the related company asked
            about); False keeps its generated column (a filer CIK stays `FilerCik`). Only a
            `fanout` key may set it False, and such a key comes from the source's rows, never
            from the request (`DatasetSpec` rejects both, when it is created).
        required: Whether every row must carry the key. True, the default, makes a row
            without it a loud failure (`RowDataError`), never a null key. False lets such a
            row through with a null key column; it is for a symbol Massive documents as
            present only "if available", such as a filing's `ticker`. Only a `symbol` key read
            from a REST path's rows may set it False: a flat file and a stream filter and
            route rows by their symbol, and the `time`, `fanout`, and `session` keys are
            always required (`DatasetSpec` rejects the rest, when it is created).
    """

    column: str
    from_request: bool = False
    instrument: bool = True
    required: bool = True


@dataclass(frozen=True, slots=True)
class SourceKeys:
    """The key columns one source declares for one dataset, at most one per role.

    Every source of a dataset in a kind declares `symbol` and `time`, every source of an
    instrument dataset declares `symbol`, and `fanout` and (futures) `session` are declared only
    on the sources that carry them. A dataset whose rows describe no instrument may declare no
    key at all.

    Attributes:
        symbol: The instrument the row describes; renamed to `Symbol`.
        time: The row's primary time; renamed to `Timestamp`.
        fanout: The key a request fans out over when it differs from the row's instrument:
            an instrument (the options underlying, the related company asked about), renamed
            to `Underlying`, or, with `KeySpec.instrument` False, another identifier (a filer
            CIK), which keeps its column.
        session: The futures trading session the row belongs to; keeps its column
            (`SessionEndDate`). Only `symbol` and `fanout` may come from the request.
    """

    symbol: KeySpec | None = None
    time: KeySpec | None = None
    fanout: KeySpec | None = None
    session: KeySpec | None = None

    def declared(self) -> tuple[tuple[str, KeySpec], ...]:
        """Return the declared keys and their roles.

        Returns:
            `(role, key)` pairs in the order `symbol`, `time`, `fanout`, `session`, for the
            roles that are declared.
        """
        roles = (
            ("symbol", self.symbol),
            ("time", self.time),
            ("fanout", self.fanout),
            ("session", self.session),
        )
        return tuple((role, key) for role, key in roles if key is not None)


_NO_KEYS: Final = SourceKeys()


@dataclass(frozen=True, kw_only=True, eq=False, slots=True)
class DatasetSpec:
    """The declarative description of one dataset, shared by every transport.

    A spec says what a dataset *is* (its name, asset class, and kind) and which generated
    sources serve it (REST paths by role, WebSocket channels, flat-file prefixes), plus the
    facts the generated files cannot give: the key columns of each source, the wire parameters
    behind `start` and `end` and behind the symbols, the default `poll()` mode and its key
    columns, and the REST page size. Columns, types, and timestamp units are never declared:
    `Catalog.dataset_schema` merges them from the sources. Specs are immutable singletons
    compared by identity, and safe to share across threads.

    The transports decide the dataset's query class (`query_class` in `_namespaces/base.py`).
    With S meaning the spec has channels, H that it has a history source (a history path, a
    grouped-daily path, or flat-file prefixes), and F that it has a fetch path: F alone gives
    `Query`, S alone `StreamQuery`, H alone `HistoryQuery`, S and H `StreamHistoryQuery`, and
    S, H, and F `StreamHistoryLatestQuery`. A `bars` dataset needs H; its class is picked per
    call from the timespan and multiplier (`bar_query_class`). Every other combination is
    rejected when the spec is created.

    Example:
        ::

            from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
            from deephaven_massive._enums import AssetClass
            from deephaven_massive._massive import Transport

            # Channels, history, and a fetch path: a StreamHistoryLatestQuery.
            STOCK_TRADES = DatasetSpec(
                name="stocks.trades",
                summary="Trades of US-listed stocks, tick by tick.",
                asset_class=AssetClass.STOCKS,
                kind="trades",
                fetch_path="/v2/last/trade/{stocksTicker}",
                history_path="/v3/trades/{stockTicker}",
                channels={None: "T"},
                flat_file_prefixes={None: "us_stocks_sip/trades_v1"},
                keys={
                    (Transport.REST, "/v2/last/trade/{stocksTicker}"): SourceKeys(
                        symbol=KeySpec("Ticker"), time=KeySpec("SipTimestamp")
                    ),
                    (Transport.REST, "/v3/trades/{stockTicker}"): SourceKeys(
                        symbol=KeySpec("stockTicker", from_request=True),
                        time=KeySpec("SipTimestamp"),
                    ),
                    (Transport.WS, "stocks/T"): SourceKeys(
                        symbol=KeySpec("Ticker"), time=KeySpec("SipTimestamp")
                    ),
                    (Transport.FLAT_FILES, "us_stocks_sip/trades_v1"): SourceKeys(
                        symbol=KeySpec("Ticker"), time=KeySpec("SipTimestamp")
                    ),
                },
                time_axis=("timestamp.gte", "timestamp.lt"),
            )

    Attributes:
        name: The dataset's dotted name, `"<namespace>.<dataset>"`, such as `"stocks.trades"`.
        summary: What the dataset holds, in one sentence ending in a period, such as
            `"Trades of US-listed stocks, tick by tick."`. The dataset method's docstring
            starts with the same sentence; the library keeps it here so it never reads
            `__doc__` at run time, where `python -OO` would have removed it.
        asset_class: The instruments' asset class, or None for datasets that span classes or
            have no instruments (reference data, economy, most partner datasets). Its value is
            the WebSocket cluster of the channels and the group of the dataset's kind.
        is_bars: True for a `bars()` dataset: its `channels` and `flat_file_prefixes` are keyed
            by `Timespan`, and the factory picks the query class from the timespan and
            multiplier. False datasets key them by None.
        kind: The dataset kind its rows share a schema with, across asset classes and
            transports (`"trades"`, `"quotes"`, `"bars"`, `"values"`, `"luld"`,
            `"imbalances"`, `"fmv"`), or None for a dataset in no kind. The sources of a kind
            dataset are exactly those holding a membership of `(kind, asset class)` in the
            generated files (the kind-membership gate).
        fetch_path: The REST path behind `fetch()` and `poll()`, such as
            `"/v2/last/trade/{stocksTicker}"`.
        history_path: The REST path behind `history(source=REST)`.
        grouped_daily_path: The REST path that serves `history()` for `dm.ALL` daily bars, one
            request per day.
        detail_path: The REST path that serves `fetch()` and `poll()`, once per symbol, when the
            call names symbols, such as one option contract's
            `"/v3/reference/options/contracts/{options_ticker}"`; `fetch_path` serves calls
            without symbols.
        extra_paths: Further REST paths the dataset reads.
        channels: The WebSocket channel per bar timespan, or under the key None for datasets
            that are not bars, such as `{None: "T"}` or
            `{Timespan.SECOND: "A", Timespan.MINUTE: "AM"}`. Its source is
            `"<asset class>/<channel>"`.
        family: The channel family, which decides the WebSocket host.
        flat_file_prefixes: The S3 key prefix per bar timespan, or under None for non-bar
            datasets. A futures prefix contains the placeholder `{venue}`.
        keys: The key columns of each source, keyed by `(transport, source key)` as
            `sources` gives them; a source without an entry declares no key.
        time_axis: The wire parameter pair that `start` and `end` map to, such as
            `("timestamp.gte", "timestamp.lt")`, for datasets with a `…T` time axis. A
            parameter of the history path, or of the fetch path when there is none.
        symbol_param: The query parameter of the same path as `time_axis` that carries the
            requested symbols, such as `"ticker.any_of"` or `"tickers"`; None when the path
            takes the symbol as a path parameter or the dataset has no symbols.
        poll_mode: The default `poll()` mode.
        poll_keys: The key columns of a keyed (`REPLACE`, `UPSERT`) `poll()` table.
        per_symbol: True when the endpoint takes one symbol per request, so a symbol list fans
            out to one request per symbol.
        fetch_paths_by_class: The REST path behind `fetch()` and `poll()` for the symbols of
            one asset class, where it differs from `fetch_path`, such as
            `{AssetClass.CRYPTO: "/v1/indicators/sma/{cryptoTicker}"}`; the symbols of every
            other class read `fetch_path`.
        symbols_per_request: The most symbols one request carries in a list-valued symbol
            parameter (`ticker.any_of`), such as 250 for the unified snapshot; a longer list is
            split over several requests. None sends every symbol in one request.
        page_size: The REST page size requested; `limit` is a separate total-row cap.
    """

    name: str
    summary: str
    asset_class: AssetClass | None = None
    is_bars: bool = False
    kind: str | None = None
    fetch_path: str | None = None
    history_path: str | None = None
    grouped_daily_path: str | None = None
    detail_path: str | None = None
    extra_paths: tuple[str, ...] = ()
    channels: Mapping[Timespan | None, str] = field(default_factory=dict)
    family: ChannelFamily = ChannelFamily.TICK
    flat_file_prefixes: Mapping[Timespan | None, str] = field(default_factory=dict)
    keys: Mapping[tuple[Transport, str], SourceKeys] = field(default_factory=dict)
    time_axis: tuple[str, str] | None = None
    symbol_param: str | None = None
    poll_mode: PollMode = PollMode.REPLACE
    poll_keys: tuple[str, ...] = ("Symbol",)
    per_symbol: bool = False
    fetch_paths_by_class: Mapping[AssetClass, str] = field(default_factory=dict)
    symbols_per_request: int | None = None
    page_size: int | None = None

    def __post_init__(self) -> None:
        """Check the spec's internal consistency; nothing here reads the generated files.

        Raises:
            ValueError: If the name is not `"<namespace>.<dataset>"`; the summary is not one
                line of text ending in a period (`_check_summary`); the keys of `channels`
                or `flat_file_prefixes` do not match `is_bars`; a dataset with channels or a
                kind has no `asset_class`; `keys` names a source the dataset does not use,
                fills a `time` or `session` key from the request, marks a key other than
                `fanout` as no instrument, or fills a non-instrument fan-out key from the
                request; a flat-file prefix does not
                declare its `symbol` and `time` keys from its own columns (and, for futures,
                its `session` key, against which every futures file is checked); or the
                transports match no query class (see the class docstring and `_check_verbs`).
        """
        namespace, _, dataset = self.name.partition(".")
        if not namespace or not dataset:
            raise ValueError(f"dataset name {self.name!r} must be '<namespace>.<dataset>'")
        self._check_summary()
        keys = {*self.channels, *self.flat_file_prefixes}
        if any((key is None) == self.is_bars for key in keys):
            kind = "Timespan" if self.is_bars else "None"
            raise ValueError(
                f"{self.name}: channels and flat_file_prefixes must be keyed by {kind}"
            )
        if self.channels and self.asset_class is None:
            raise ValueError(f"{self.name}: a WebSocket dataset (with channels) needs a class")
        if self.kind is not None and self.asset_class is None:
            raise ValueError(f"{self.name}: a dataset in a kind needs a class (its group)")
        self._check_keys()
        self._check_flat_files()
        self._check_verbs()

    def _check_summary(self) -> None:
        """Check that the summary is one sentence: one line of text that ends in a period.

        Raises:
            ValueError: If the summary is empty, has leading or trailing whitespace, spans
                more than one line, or does not end in a period; the message names the
                dataset.
        """
        text = self.summary
        if not text or text != text.strip() or "\n" in text or not text.endswith("."):
            raise ValueError(
                f"{self.name}: the summary must be one sentence ending in a period, got {text!r}"
            )

    @property
    def group(self) -> str | None:
        """The dataset's group within its kind: its asset class's value, or None."""
        return None if self.asset_class is None else self.asset_class.value

    @property
    def rest_paths(self) -> tuple[str, ...]:
        """The REST paths the dataset reads: the role paths, then `extra_paths`.

        The role paths are `fetch_path`, the per-class fetch paths (`fetch_paths_by_class`),
        `detail_path`, `history_path`, and `grouped_daily_path`, without repeats.
        """
        roles = (
            self.fetch_path,
            *self.fetch_paths_by_class.values(),
            self.detail_path,
            self.history_path,
            self.grouped_daily_path,
        )
        found = (*(path for path in roles if path is not None), *self.extra_paths)
        return tuple(dict.fromkeys(found))

    @property
    def sources(self) -> tuple[tuple[Transport, str], ...]:
        """Every source the dataset reads, as `(transport, source key)`, without repeats.

        In declared order: the REST paths (`rest_paths`), then the channels as
        `"<asset class>/<channel>"`, then the flat-file prefixes.
        """
        cluster = self.group
        found = [
            *((Transport.REST, path) for path in self.rest_paths),
            *((Transport.WS, f"{cluster}/{channel}") for channel in self.channels.values()),
            *((Transport.FLAT_FILES, prefix) for prefix in self.flat_file_prefixes.values()),
        ]
        return tuple(dict.fromkeys(found))

    @property
    def axis_params(self) -> frozenset[str]:
        """The wire parameters of the time axis, which only `start` and `end` may set.

        Each name of `time_axis`, its base name, and every `.gt`, `.gte`, `.lt`, `.lte`, and
        `.any_of` form of the base: `("timestamp.gte", "timestamp.lt")` reserves `timestamp`
        and all of its forms, so no filter can send a second bound beside `start` and `end`.
        """
        return frozenset(name for axis in self.time_axis or () for name in param_family(axis))

    def keys_for(self, transport: Transport, key: str) -> SourceKeys:
        """Return the key columns one source declares.

        Args:
            transport: The source's transport.
            key: The source's key, as `sources` gives it.

        Returns:
            The source's keys; an empty `SourceKeys` when it declares none.
        """
        return self.keys.get((transport, key), _NO_KEYS)

    @property
    def has_channels(self) -> bool:
        """True when the dataset streams over the WebSocket (the stream verbs work)."""
        return bool(self.channels)

    @property
    def has_history(self) -> bool:
        """True when `history()` has a source: REST history, grouped-daily bars, or flat files."""
        return self.has_rest_history or self.has_grouped_daily or bool(self.flat_file_prefixes)

    @property
    def has_fetch(self) -> bool:
        """True when the dataset has a REST fetch path (`fetch()` works)."""
        return self.fetch_path is not None

    @property
    def has_rest_history(self) -> bool:
        """True when the dataset has a REST history path (`history(source=REST)` works)."""
        return self.history_path is not None

    @property
    def has_grouped_daily(self) -> bool:
        """True when a grouped-daily REST path serves `dm.ALL` 1-day bars, one request a day."""
        return self.grouped_daily_path is not None

    def _check_keys(self) -> None:
        """Check that keys are declared only for the dataset's sources, and fit their roles.

        Raises:
            ValueError: If `keys` names a source the dataset does not read; a `time` or
                `session` key is filled from the request; or a key other than `fanout` is
                marked as no instrument, a fan-out key that is no instrument is filled from
                the request (it would have no column to keep), or a key other than a `symbol`
                key read from a REST path's rows is optional (`KeySpec.required` False). The
                message names the dataset.
        """
        unused = sorted(f"{t.value} {k}" for t, k in set(self.keys) - set(self.sources))
        if unused:
            raise ValueError(
                f"{self.name}: keys are declared for sources it does not read {unused}"
            )
        for (transport, key), declared in self.keys.items():
            where = f"{self.name}: {transport.value} {key}"
            for role, spec in declared.declared():
                if spec.from_request and role not in _FROM_REQUEST_ROLES:
                    raise ValueError(f"{where}: the {role} key cannot come from the request")
                if not spec.instrument and role != "fanout":
                    raise ValueError(f"{where}: only a fanout key can be no instrument")
                if not spec.instrument and spec.from_request:
                    raise ValueError(
                        f"{where}: a fanout key that is no instrument keeps a column of the "
                        "rows, so it cannot come from the request"
                    )
                optional_ok = (
                    role == "symbol" and transport is Transport.REST and not spec.from_request
                )
                if not spec.required and not optional_ok:
                    raise ValueError(
                        f"{where}: only a symbol key read from a REST path's rows can be optional"
                    )

    def _check_flat_files(self) -> None:
        """Check that every flat-file prefix can be filtered and checked.

        Every flat-file read filters on the symbol and time columns, and every futures file is
        checked against its key's session, so each prefix must declare those keys from its
        own columns.

        Raises:
            ValueError: If a prefix lacks its `symbol` or `time` key, or, for futures, its
                `session` key, or fills one from the request.
        """
        futures = self.asset_class is AssetClass.FUTURES
        needed = ("symbol", "time", *(("session",) if futures else ()))
        for prefix in self.flat_file_prefixes.values():
            declared = dict(self.keys_for(Transport.FLAT_FILES, prefix).declared())
            lacking = [r for r in needed if r not in declared or declared[r].from_request]
            if lacking:
                raise ValueError(
                    f"{self.name}: flat files {prefix} must declare the {lacking} keys from "
                    "their own columns"
                )

    def _check_verbs(self) -> None:
        """Check that the dataset's transports match one query class.

        Raises:
            ValueError: If the dataset has no verb; if it has channels and a fetch path but no
                history source (no query class streams and fetches without history); if,
                outside `bars`, it has a history source and a fetch path but no channels (that
                class is kept for daily bars); or if a `bars` dataset has no history source.
                The message names the dataset.
        """
        stream, history, fetch = self.has_channels, self.has_history, self.has_fetch
        if self.is_bars:
            if not history:
                raise ValueError(f"{self.name}: a bars dataset needs a history source")
            return
        if not (stream or history or fetch):
            raise ValueError(
                f"{self.name}: a dataset needs channels, a history source, or a fetch path"
            )
        if stream and fetch and not history:
            raise ValueError(
                f"{self.name}: a dataset with channels and a fetch path needs a history source"
            )
        if history and fetch and not stream:
            raise ValueError(
                f"{self.name}: only bars combine a history source and a fetch path without channels"
            )


def param_family(name: str) -> frozenset[str]:
    """Return a wire parameter, its base name, and every suffixed form of the base.

    The time axis reserves this whole family: no filter may bound or select the axis beside
    `start` and `end`, although `start` and `end` cannot express an `.any_of` list of dates.

    Args:
        name: A wire parameter name, such as `"timestamp.gte"` or `"ticker"`.

    Returns:
        The base (`"timestamp"`) and its `.gt`, `.gte`, `.lt`, `.lte`, `.any_of`, and
        `.all_of` forms.
    """
    base = name.split(".", 1)[0]
    return frozenset({base, *(f"{base}.{suffix}" for suffix in _FILTER_SUFFIXES)})


def symbol_family(name: str) -> frozenset[str]:
    """Return the equality forms of a symbol parameter, which only `symbols` may set.

    The range forms stay filters: `ticker_gte` and `ticker_lt` select an alphabetical range
    of tickers, which `symbols` cannot express.

    Args:
        name: The symbol parameter, such as `"ticker.any_of"` or `"tickers"`.

    Returns:
        The base (`"ticker"`) and its `.any_of` and `.all_of` forms.
    """
    base = name.split(".", 1)[0]
    return frozenset({base, *(f"{base}.{suffix}" for suffix in _EQUALITY_SUFFIXES)})


def filter_key(name: str) -> str:
    """Return the `filters` key of a wire parameter: its dots written as underscores.

    Args:
        name: The wire name, such as `"strike_price.gte"`.

    Returns:
        The key, such as `"strike_price_gte"`.
    """
    return name.replace(".", "_")


def _fits(kind: ParamKind, values: tuple[str, ...], value: object) -> bool:
    """Tell whether a normalized `filters` value fits a parameter's kind.

    Args:
        kind: The parameter's kind.
        values: The parameter's accepted values, for an `ENUM`.
        value: The value; sequences are already tuples.

    Returns:
        True when the value is of a form the parameter takes: a `str` for `STRING`, an `int`
        for `INTEGER`, an `int` or `float` for `NUMBER` (never a `bool` for either), a `bool`
        for `BOOLEAN`, one of `values` for `ENUM`, a `date`, `datetime`, or text for `DATE`
        and `TIMESTAMP` (`Catalog.check_filters` then parses the text), and a `str` or a tuple
        of `str` for `LIST`.
    """
    number = isinstance(value, int | float) and not isinstance(value, bool)
    checks = {
        ParamKind.STRING: isinstance(value, str),
        ParamKind.INTEGER: number and isinstance(value, int),
        ParamKind.NUMBER: number,
        ParamKind.BOOLEAN: isinstance(value, bool),
        ParamKind.ENUM: isinstance(value, str) and str(value) in values,
        ParamKind.DATE: isinstance(value, date | str),
        ParamKind.TIMESTAMP: isinstance(value, date | str),
        ParamKind.LIST: isinstance(value, str)
        or (isinstance(value, tuple) and all(isinstance(item, str) for item in value)),
    }
    return checks[kind]


class Catalog:
    """Every declared dataset of the process, by name, with the merge of their schemas.

    `_session.py` builds the one catalog, `CATALOG`, at import from the `DATASETS` of every
    namespace module, and hands it to each session core (`SessionCore.catalog`), where the
    transports read it. The specs are immutable once collected; the only mutable state is the
    `dataset_schema` cache, guarded by a leaf lock under which nothing else runs, so the
    catalog is safe to share across threads and sessions.

    Example:
        ::

            from deephaven_massive._datasets.catalog import (
                Catalog,
                DatasetSpec,
                KeySpec,
                SourceKeys,
            )
            from deephaven_massive._enums import AssetClass
            from deephaven_massive._massive import Transport

            trades = DatasetSpec(
                name="stocks.trades",
                summary="Trades of US-listed stocks, tick by tick.",
                asset_class=AssetClass.STOCKS,
                channels={None: "T"},
                keys={
                    (Transport.WS, "stocks/T"): SourceKeys(
                        KeySpec("Ticker"), KeySpec("SipTimestamp")
                    )
                },
            )
            catalog = Catalog((trades,))
            catalog.by_name["stocks.trades"] is trades  # True
            catalog.problems()  # (): every gate passes
            catalog.dataset_schema(trades).column_names[:2]  # ('Symbol', 'Timestamp')
    """

    __slots__ = ("_by_name", "_lock", "_schemas", "_sources")

    def __init__(
        self,
        specs: Iterable[DatasetSpec],
        *,
        source_tables: Callable[[Transport], Mapping[str, Source]] = sources,
    ) -> None:
        """Collect the specs; nothing is loaded or checked against the generated files yet.

        Args:
            specs: Every dataset, in declaration order.
            source_tables: Returns the generated sources of one transport by key. The wire
                package's `sources`, which loads the committed files once, by default; tests
                pass synthetic sources.

        Raises:
            ValueError: If two specs share a dataset name.
        """
        by_name: dict[str, DatasetSpec] = {}
        for spec in specs:
            if spec.name in by_name:
                raise ValueError(f"dataset {spec.name} is declared twice")
            by_name[spec.name] = spec
        self._by_name: Final = MappingProxyType(by_name)
        self._sources: Final = source_tables
        self._lock: Final = threading.Lock()
        self._schemas: dict[DatasetSpec, Schema] = {}

    @property
    def by_name(self) -> Mapping[str, DatasetSpec]:
        """The specs, keyed by `DatasetSpec.name`, in declaration order; read-only."""
        return self._by_name

    def source(self, transport: Transport, key: str) -> Source:
        """Return the generated source of a transport and key.

        Args:
            transport: The source's transport.
            key: The REST path template, the `"<cluster>/<channel>"`, or the flat-file prefix.

        Returns:
            The source.

        Raises:
            KeyError: If the generated files hold no such source; the message names it and
                says it was deprecated or removed in the snapshot.
        """
        try:
            return self._sources(transport)[key]
        except KeyError:
            raise KeyError(f"{transport.value} {key} is not generated: {_SOURCE_GONE}") from None

    def members(self, kind: str) -> tuple[DatasetSpec, ...]:
        """Return the datasets of one kind, in the order of their memberships.

        Ordered by group (the asset classes in `AssetClass` order), then by the smallest
        position any of the dataset's sources holds in its kind's group, then by declaration.

        Args:
            kind: The dataset kind, such as `"trades"`.

        Returns:
            The specs whose `kind` is `kind`; empty when none is declared.

        A source missing from the generated files raises the `KeyError` of `source`.
        """
        ranks = {cls.value: rank for rank, cls in enumerate(AssetClass)}
        ranked = []
        for index, spec in enumerate(self._by_name.values()):
            if spec.kind != kind:
                continue
            positions = [
                m.position
                for transport, key in spec.sources
                for m in self.source(transport, key).memberships
                if m.kind == kind and m.group == spec.group
            ]
            ranked.append((ranks[str(spec.group)], min(positions, default=0), index, spec))
        return tuple(spec for *_, spec in sorted(ranked, key=lambda r: r[:3]))

    def dataset_schema(self, spec: DatasetSpec) -> Schema:
        """Return a dataset's merged schema, computed once and cached on the catalog.

        The merge of `schema.dataset_schema` over the members of the dataset's kind, or over
        the dataset alone when it is in no kind. The first call for a dataset computes it
        outside the lock and publishes it under the lock; when two threads race, the first
        to publish wins and both return that one schema. Pure: no I/O beyond the wire
        package's one-time load of the generated files, no Deephaven call.

        Args:
            spec: The dataset; a dataset in a kind must be in this catalog.

        Returns:
            The schema: `Symbol` and `Timestamp` first where the dataset
            declares them, then every other column of the kind.

        Raises:
            LookupError: If `spec` has a kind but is not one of this catalog's datasets.

        The merge raises the `ValueError` of `schema.dataset_schema` for a conflict or a bad
        key, and an unknown source the `KeyError` of `source`.
        """
        with self._lock:
            cached = self._schemas.get(spec)
        if cached is not None:
            return cached
        if spec.kind is None:
            members: tuple[DatasetSpec, ...] = (spec,)
        else:
            members = self.members(spec.kind)
            if spec not in members:
                raise LookupError(f"{spec.name} is not in the catalog of its kind {spec.kind}")
        schema = dataset_schema(spec, members, self.source)
        with self._lock:
            return self._schemas.setdefault(spec, schema)

    def check_filters(
        self, spec: DatasetSpec, filters: Mapping[str, object], *, fixed: Iterable[str] = ()
    ) -> None:
        """Validate a `filters` mapping against the generated parameters of the dataset's paths.

        Runs synchronously when the query is built. A key is a wire parameter's name with its
        dots written as underscores (`strike_price_gte` for `strike_price.gte`), for any query
        parameter of any of the dataset's REST paths, except the ones the library sets itself:
        the equality forms of the symbol parameter (`symbol_family`: its base, `.any_of`, and
        `.all_of`), the whole family of the time axis (`param_family`: its base and every
        suffixed form), the whole family of each parameter the dataset's factory fixes,
        `limit`, `sort`, `order`, and `adjusted`.

        Args:
            spec: The dataset.
            filters: The normalized filters by key; sequences are already tuples.
            fixed: The wire names of the parameters the library sets itself for this call
                (`build_query(fixed=...)`); neither accepted nor named among the valid keys.

        Raises:
            ValueError: If a key is not a filter of the dataset (the message lists the valid
                keys), or a value is not of the parameter's kind or not one of an enum
                parameter's values. A naive `datetime`, or text of a date or timestamp
                parameter that is not ISO-8601 or has a time without an offset, raises the
                `ValueError` of `dates.require_aware` or `dates.parse_date_like`.

        An unknown REST path raises the `KeyError` of `source`.
        """
        if not filters:
            return
        symbols = () if spec.symbol_param is None else symbol_family(spec.symbol_param)
        library = (form for name in fixed for form in param_family(name))
        reserved = {*_LIBRARY_PARAMS, *spec.axis_params, *symbols, *library}
        params = {
            filter_key(param.name): param
            for path in spec.rest_paths
            for param in self._rest(path).params
            if not param.in_path and param.name not in reserved
        }
        for key, value in filters.items():
            if isinstance(value, datetime):
                require_aware(key, value, value)
            param = params.get(key)
            if param is None:
                raise ValueError(
                    f"{spec.name} has no filter {key!r}; valid filters: {sorted(params)}"
                )
            if not _fits(param.kind, param.values, value):
                expected = list(param.values) if param.values else f"a {param.kind.value} value"
                raise ValueError(f"{spec.name} filter {key!r} takes {expected}, not {value!r}")
            if param.kind in _TIME_KINDS and isinstance(value, str):
                parse_date_like(value, key)  # ISO-8601 text, with an offset if it has a time

    def _rest(self, path: str) -> RestPath:
        """Return the generated REST path of a template.

        Args:
            path: The path template.

        Returns:
            The path.

        An unknown path raises the `KeyError` of `source`.
        """
        # The REST table of the generated files holds only `RestPath`s.
        return cast("RestPath", self.source(Transport.REST, path))

    def problems(self) -> tuple[str, ...]:
        """Run the catalog gates over every dataset; return every failure found.

        Each failure is one line, `"<gate>: <dataset>: <detail>"`, so a test can assert the
        whole list is empty and a reviewer sees every problem at once. The gates:

        - **generated sources:** every path, channel, and prefix a dataset uses is generated; a
          miss names the dataset and says "deprecated or removed in the snapshot". The other
          gates read the sources, so they run only once this one passes for every dataset.
        - **declared keys:** every key a dataset declares for a source is a column of that
          source after the projection of the dataset's membership, or is filled from the
          request; every source of a dataset in a kind declares `symbol` and `time`, and every
          source of an instrument dataset (`takes_symbols`) declares `symbol`; a
          `time` key is an `epoch` or `iso-instant` field, so `Timestamp` is an `Instant`; and
          the `fanout` keys agree on `KeySpec.instrument` across the dataset's sources and
          across the members of its kind.
        - **unique columns:** after the rename, no source has a repeated column.
        - **member types:** across the members of a kind, one column gets one wire type, up to
          the lattice joins.
        - **kind membership:** a kind dataset's sources equal the sources holding a membership
          of its `(kind, group)`.
        - **wire parameters:** every wire name the dataset declares is a parameter of its
          REST path: the `start`/`end` pair and the symbol parameter of the history path (or
          of the fetch path when there is none), and each key filled from the request, which
          names a parameter of its own path.
        - **poll keys:** every `poll_keys` column is a column of the dataset's schema.

        Returns:
            The failures, in dataset order and then gate order, each once; empty when every
            gate passes.
        """
        missing = [
            f"generated sources: {spec.name}: {t.value} {k} is not generated: {_SOURCE_GONE}"
            for spec in self._by_name.values()
            for t, k in spec.sources
            if k not in self._sources(t)
        ]
        if missing:
            return tuple(missing)
        found: list[str] = []
        for spec in self._by_name.values():
            keys = self._key_problems(spec)
            found.extend(keys)
            found.extend(self._kind_problems(spec))
            found.extend(self._parameter_problems(spec))
            if keys:
                continue  # the merge would only repeat them
            try:
                schema = self.dataset_schema(spec)
            except ValueError as error:
                # A conflict, or another member's bad key, which that member also reports.
                found.append(str(error))
                continue
            lacking = sorted(set(spec.poll_keys) - set(schema.column_names))
            if lacking:
                found.append(f"poll keys: {spec.name}: {lacking} are not columns")
        return tuple(dict.fromkeys(found))

    def _key_problems(self, spec: DatasetSpec) -> list[str]:
        """Check the declared keys of every source of one dataset.

        Args:
            spec: The dataset; every source is generated.

        Returns:
            The `declared keys` and `unique columns` failures.
        """
        needed: tuple[str, ...] = ()
        if spec.kind is not None:
            needed = ("symbol", "time")
        elif self.takes_symbols(spec):
            needed = ("symbol",)
        found = []
        for transport, key in spec.sources:
            roles = {role for role, _ in spec.keys_for(transport, key).declared()}
            lacking = [role for role in needed if role not in roles]
            if lacking:
                found.append(
                    f"declared keys: {spec.name}: {transport.value} {key} declares no "
                    f"{' or '.join(lacking)} key"
                )
            try:
                rename(spec, self.source(transport, key))
            except ValueError as error:
                found.append(str(error))
        return found + self._fanout_problems(spec)

    def takes_symbols(self, spec: DatasetSpec) -> bool:
        """Tell whether a dataset is an instrument dataset: one whose rows each name one.

        A dataset takes symbols when it has channels or flat files (both are read per
        instrument), `per_symbol` requests, a symbol filled from the request, or a
        `symbol_param` that selects rows by one ticker each. A symbol parameter that filters a
        field holding several tickers (a filing's or an article's `tickers`, a JSON list) does
        not make one: such a row describes no single instrument, so the dataset has no
        `Symbol` and keeps its `Tickers` column. The filtered field is the row field of the
        parameter's base name, singular or plural (`tickers.any_of` filters `tickers`, and
        `ticker` filters `tickers` on the news path); with no such field, the parameter counts.
        Its asset class alone does not make a dataset an instrument dataset:
        `futures.exchanges` keeps `FUTURES` for the session-day rule and takes no symbols.

        Args:
            spec: The dataset; its REST paths are generated.

        Returns:
            True for an instrument dataset.

        An unknown REST path raises the `KeyError` of `source`.
        """
        from_request = any(
            keys.symbol is not None and keys.symbol.from_request for keys in spec.keys.values()
        )
        if spec.channels or spec.flat_file_prefixes or spec.per_symbol or from_request:
            return True
        primary = spec.history_path or spec.fetch_path
        if spec.symbol_param is None or primary is None:
            return False
        stem = spec.symbol_param.split(".", 1)[0].removesuffix("s")
        filtered = [f for f in self._rest(primary).fields if f.wire_key.removesuffix("s") == stem]
        return not any(f.wire_type in _LIST_TYPES for f in filtered)

    def _fanout_problems(self, spec: DatasetSpec) -> list[str]:
        """Check that the dataset's fan-out keys agree on whether they name an instrument.

        Otherwise one table would get both an `Underlying` column and the key's own column.
        The keys agree across the dataset's sources and across the members of its kind.

        Args:
            spec: The dataset.

        Returns:
            The `declared keys` failures about `KeySpec.instrument`.
        """

        def flags(dataset: DatasetSpec) -> set[bool]:
            return {k.fanout.instrument for k in dataset.keys.values() if k.fanout is not None}

        if len(flags(spec)) > 1:
            return [f"declared keys: {spec.name}: its fanout keys disagree on instrument"]
        members = [
            m for m in self._by_name.values() if spec.kind is not None and m.kind == spec.kind
        ]
        if len(set().union(*map(flags, members))) > 1:
            return [
                (
                    f"declared keys: {spec.name}: the fanout keys of the {spec.kind} kind disagree "
                    "on instrument"
                )
            ]
        return []

    def _kind_problems(self, spec: DatasetSpec) -> list[str]:
        """Check that a kind dataset reads exactly the sources of its kind's group.

        Args:
            spec: The dataset; every source is generated.

        Returns:
            The `kind membership` failures; none for a dataset in no kind.
        """
        if spec.kind is None:
            return []
        expected = {
            (transport, key)
            for transport in Transport
            for key, source in self._sources(transport).items()
            if any(m.kind == spec.kind and m.group == spec.group for m in source.memberships)
        }
        actual = set(spec.sources)
        found = []
        for what, names in (("lacks", expected - actual), ("reads outside it", actual - expected)):
            if names:
                listed = sorted(f"{t.value} {k}" for t, k in names)
                found.append(
                    f"kind membership: {spec.name}: {what} the {spec.kind}/{spec.group} "
                    f"sources {listed}"
                )
        return found

    def _parameter_problems(self, spec: DatasetSpec) -> list[str]:
        """Check that every wire name the dataset declares is a parameter of its path.

        The `start`/`end` pair and the symbol parameter belong to the dataset's primary path,
        its history path or, when it has none, its fetch path; a key filled from the request
        belongs to its own path.

        Args:
            spec: The dataset; every source is generated.

        Returns:
            The `wire parameters` failures.
        """
        primary = spec.history_path or spec.fetch_path
        declared = [("time axis", name) for name in spec.time_axis or ()] + [
            ("symbol parameter", name) for name in (spec.symbol_param,) if name is not None
        ]
        if declared and primary is None:
            return [f"wire parameters: {spec.name}: a {declared[0][0]} needs a REST path"]
        expected = [(what, cast("str", primary), name) for what, name in declared]
        for path in spec.rest_paths:
            for role, key in spec.keys_for(Transport.REST, path).declared():
                if key.from_request:
                    expected.append((f"{role} key", path, key.column))
        return [
            f"wire parameters: {spec.name}: {what} {name!r} is not a parameter of {path}"
            for what, path, name in expected
            if name not in {param.name for param in self._rest(path).params}
        ]
