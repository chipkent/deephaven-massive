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

from deephaven_massive._datasets.dates import instant_bound, parse_date_like, require_aware
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._datasets.schema import dataset_schema, filled_columns, rename
from deephaven_massive._enums import (
    AssetClass,
    ChannelFamily,
    ClassFromPrefix,
    NotBars,
    PollMode,
    Timespan,
)
from deephaven_massive._massive import ParamKind, Transport, WireType, encode_value, sources

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from deephaven_massive._datasets.request import ParamValue
    from deephaven_massive._datasets.schema import Schema
    from deephaven_massive._massive import RestPath, Source, WireUnit

__all__ = [
    "Catalog",
    "DatasetSpec",
    "KeySpec",
    "SourceKeys",
    "timespan_key",
]

logger = logging.getLogger(__name__)

_FROM_REQUEST_ROLES: Final = frozenset({"symbol", "fanout"})
"""The key roles a source may leave out of its rows, filled from the request instead."""

_OPTIONAL_ROLES: Final = frozenset({"symbol", "fanout"})
"""The key roles a REST path's rows may lack (`KeySpec.required` False): a ticker Massive sends
only "if available", and the underlying that only the options rows of a mixed snapshot carry."""

_LIBRARY_PARAMS: Final = frozenset({"limit", "sort", "order", "adjusted"})
"""Query parameters the library sets from typed arguments, never through `filters`."""

_TIME_KINDS: Final = frozenset({ParamKind.DATE, ParamKind.TIMESTAMP})
"""The parameter kinds whose text value must be an ISO-8601 date or an instant with an offset."""
_NUMBER_KINDS: Final = frozenset({ParamKind.INTEGER, ParamKind.NUMBER})
"""The parameter kinds whose `.any_of` list takes `int` or `float` items."""
_SORT_SUFFIXES: Final = frozenset({"asc", "desc"})
"""The direction suffixes a part of a comma-separated `sort` may carry."""

_FILTER_SUFFIXES: Final = ("gt", "gte", "lt", "lte", "any_of", "all_of")
"""The suffixes the specification adds to a parameter's base name (`timestamp.gte`)."""

_EQUALITY_SUFFIXES: Final = ("any_of", "all_of")
"""The suffixes that, like the base name, select rows by equal values (`tickers.any_of`)."""

_INSTANT_TYPES: Final = frozenset({WireType.ISO_INSTANT, WireType.EPOCH})
"""Wire types of a row field that holds an instant rather than a calendar date."""

_LIST_TYPES: Final = frozenset({WireType.JSON_TEXT, WireType.LONG_ARRAY, WireType.STRING_ARRAY})
"""Wire types of a field that holds several values, such as a filing's list of tickers."""

_SOURCE_GONE: Final = "deprecated or removed in the snapshot"

_DEFAULT_POLL_KEYS: Final = ("Symbol",)
"""The `poll()` key of a dataset that declares none: one row per symbol."""


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
            present only "if available", such as a filing's `ticker`, and for an underlying
            only some rows have, such as the options rows of the unified snapshot. Only a
            `symbol` or `fanout` key read from a REST path's rows may set it False: a flat
            file and a stream filter and route rows by their symbol, and the `time`, `end`, and
            `date` keys are always required (`DatasetSpec` rejects the rest, when it is
            created).
    """

    column: str
    from_request: bool = False
    instrument: bool = True
    required: bool = True


@dataclass(frozen=True, slots=True)
class SourceKeys:
    """The key columns one source declares for one dataset, at most one per role.

    Every source of a dataset in a kind declares `symbol` and `time`, every source of an
    instrument dataset declares `symbol`, every source of a daily summary its `date`, and
    `fanout`, `date`, and `end` are otherwise declared only on the sources that carry them. A
    dataset whose rows describe no instrument may declare no key at all.

    Attributes:
        symbol: The instrument the row describes; renamed to `Symbol`.
        time: The row's primary time; renamed to `Timestamp`, or for a bar to `TimestampStart`,
            the start of its window (`row_times.time_column`).
        end: The end of a bar's window as the source sends it (the WebSocket bars' `e`);
            renamed to `TimestampEnd`, which the library recomputes (`row_times.TimeRule`), so
            the vendor's column is not kept beside it.
        fanout: The key a request fans out over when it differs from the row's instrument:
            an instrument (the options underlying, the related company asked about), renamed
            to `Underlying`, or, with `KeySpec.instrument` False, another identifier (a filer
            CIK), which keeps its column.
        date: The row's trading day as the source sends it: the futures `session_end_date`, a
            daily summary's `from` or `day`; renamed to `Date`. Only `symbol` and `fanout`
            may come from the request.
    """

    symbol: KeySpec | None = None
    time: KeySpec | None = None
    end: KeySpec | None = None
    fanout: KeySpec | None = None
    date: KeySpec | None = None

    def declared(self) -> tuple[tuple[str, KeySpec], ...]:
        """Return the declared keys and their roles.

        Returns:
            `(role, key)` pairs in the order `symbol`, `time`, `end`, `fanout`, `date`, for the
            roles that are declared.
        """
        roles = (
            ("symbol", self.symbol),
            ("time", self.time),
            ("end", self.end),
            ("fanout", self.fanout),
            ("date", self.date),
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
    `Query`, S alone `StreamQuery`, H alone `HistoryQuery`, and S and H `StreamHistoryQuery`.
    A fetch path is never combined with channels or a history source, so `fetch()` always
    returns every row its endpoint returns: a REST endpoint that answers one row per symbol
    (the last trade, the previous day's bar) is a one-row dataset of its own, in the same kind
    as the stream and history dataset of its class. A `bars` dataset needs H and no F; its
    class is picked per call from the timespan and multiplier (`bar_query_class`). Every other
    combination is rejected when the spec is created.

    Example:
        ::

            from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
            from deephaven_massive._enums import AssetClass, NotBars
            from deephaven_massive._massive import Transport

            # Channels and history: a StreamHistoryQuery.
            STOCK_TRADES = DatasetSpec(
                name="stocks.trades",
                summary="Trades of US-listed stocks, tick by tick.",
                asset_class=AssetClass.STOCKS,
                kind="trades",
                history_path="/v3/trades/{stockTicker}",
                channels={NotBars.ANY: "T"},
                flat_file_prefixes={NotBars.ANY: "us_stocks_sip/trades_v1"},
                keys={
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

            # The same kind's one-row endpoint: a Query of its own.
            STOCK_LAST_TRADE = DatasetSpec(
                name="stocks.last_trade",
                summary="The latest trade of each US-listed stock.",
                asset_class=AssetClass.STOCKS,
                kind="trades",
                fetch_path="/v2/last/trade/{stocksTicker}",
                keys={
                    (Transport.REST, "/v2/last/trade/{stocksTicker}"): SourceKeys(
                        symbol=KeySpec("Ticker"), time=KeySpec("SipTimestamp")
                    ),
                },
                revision_keys=("Timestamp",),
            )

    Attributes:
        name: The dataset's dotted name, `"<namespace>.<dataset>"`, such as `"stocks.trades"`;
            a dataset the session serves itself, which belongs to no namespace, has one plain
            name, the session method's (`"snapshots"`).
        summary: What the dataset holds, in one sentence ending in a period, such as
            `"Trades of US-listed stocks, tick by tick."`. The dataset method's docstring
            starts with the same sentence; the library keeps it here so it never reads
            `__doc__` at run time, where `python -OO` would have removed it.
        asset_class: The instruments' asset class, or `ClassFromPrefix.ANY`, the default, for
            datasets that span classes or have no instruments (reference data, economy, most
            partner datasets): each symbol's prefix then names its class. Its value is
            the WebSocket cluster of the channels and the group of the dataset's kind.
        is_bars: True for a `bars()` dataset: its `channels` and `flat_file_prefixes` are keyed
            by `Timespan`, and the factory picks the query class from the timespan and
            multiplier. False datasets key them by `NotBars.ANY`; a one-row dataset of the
            `bars` kind (`previous_day_bar`) is False too, though its rows are bars
            (`holds_bars`).
        kind: The dataset kind its rows share a schema with, across asset classes and
            transports (`"trades"`, `"quotes"`, `"bars"`, `"values"`, `"luld"`,
            `"imbalances"`, `"fmv"`), or None for a dataset in no kind. The sources of a kind
            dataset are exactly those holding a membership of `(kind, asset class)` in the
            generated files (the kind-membership gate).
        fetch_path: The REST path behind `fetch()` and `poll()`, such as
            `"/v2/last/trade/{stocksTicker}"`; never set together with channels or a history
            source.
        history_path: The REST path behind `history(source=REST)`.
        grouped_daily_path: The REST path that serves `history()` for `dm.ALL` daily bars, one
            request per day.
        detail_path: The REST path that serves `fetch()` and `poll()`, once per symbol, when the
            call names symbols, such as one option contract's
            `"/v3/reference/options/contracts/{options_ticker}"`; `fetch_path` serves calls
            without symbols.
        extra_paths: Further REST paths the dataset reads.
        channels: The WebSocket channel per bar timespan, or under the key `NotBars.ANY` for
            datasets that are not bars, such as `{NotBars.ANY: "T"}` or
            `{Timespan.SECOND: "A", Timespan.MINUTE: "AM"}`. Its source is
            `"<asset class>/<channel>"`.
        family: The channel family, which decides the WebSocket host.
        flat_file_prefixes: The S3 key prefix per bar timespan, or under the key `NotBars.ANY`
            for datasets that are not bars, such as `{NotBars.ANY: "us_stocks_sip/trades_v1"}`.
            A futures prefix contains the placeholder `{venue}`. `timespan_key` gives a
            request's key.
        keys: The key columns of each source, keyed by `(transport, source key)` as
            `sources` gives them; a source without an entry declares no key.
        time_axis: The wire parameter pair that `start` and `end` map to, such as
            `("timestamp.gte", "timestamp.lt")`, for datasets with a `…T` time axis. A
            parameter of the history path, or of the fetch path when there is none.
        symbol_param: The query parameter of the same path as `time_axis` that carries the
            requested symbols, such as `"ticker.any_of"` or `"tickers"`; None when the path
            takes the symbol as a path parameter or the dataset has no symbols.
        poll_mode: The default `poll()` mode.
        poll_keys: The key columns of a keyed (`REPLACE`, `UPSERT`) `poll()` table, and the
            first columns of a row's identity in an `APPEND` table; a null value in one fails
            the `poll()` table. Empty only for a one-row dataset (the latest market status),
            which then declares `revision_keys`, and never on an `APPEND` dataset. A dataset
            whose `poll_mode` is `APPEND` declares its own (a catalog gate refuses the
            default), which its tests check is unique per row.
        nullable_poll_keys: The `poll_keys` columns whose null is a value rather than a data
            failure, each mapped to the reason the vendor leaves it empty, such as
            `{"PutCall": "empty for common stock"}`. A null in any other poll-key column fails
            the `poll()` table; a catalog gate requires at least one poll-key column outside
            this map, so a row is never identified by nulls alone. Empty by default.
        revision_keys: Further columns of a row's identity in an `APPEND` table only, after
            `poll_keys`: a current-state dataset's update time, such as `("Updated",)` on a
            snapshot, so that an explicit `mode=APPEND` appends each new state of a symbol
            while `REPLACE` and `UPSERT` stay keyed by `poll_keys`. A null in one is a value
            (a ticker not yet updated). Empty by default.
        per_symbol: True when the endpoint takes one symbol per request, so a symbol list fans
            out to one request per symbol.
        symbols_overlap: True when one row can answer several symbols' requests, such as a
            news article that mentions two tickers of a watchlist, so a fan-out over several
            symbols can return it more than once. It has two effects: a `fetch()` or `poll()`
            cycle keeps the first copy of each row whose `poll_keys` (and only those columns)
            are all set, and `limit` caps each request's rows rather than the total, so one
            symbol's rows never crowd out another's. Such a dataset needs `poll_keys` of scalar
            columns, since rows are grouped by their values, and no `nullable_poll_keys`.
            `fetch()` keeps every row whose key is null; a `poll()` table fails on one. False
            by default.
        fetch_paths_by_class: The REST path behind `fetch()` and `poll()` for the symbols of
            one asset class, where it differs from `fetch_path`, such as
            `{AssetClass.CRYPTO: "/v1/indicators/sma/{cryptoTicker}"}`; the symbols of every
            other class read `fetch_path`.
        symbols_per_request: The most symbols one request carries in a list-valued symbol
            parameter (`ticker.any_of`), such as 250 for the unified snapshot; a longer list is
            split over several requests. None sends every symbol in one request.
        page_size: The REST page size requested: the endpoint's documented maximum `limit`,
            lowered where the rows are long (article bodies, quoted filing text, financial
            statements), so one page decompresses to some tens of megabytes at most, far below
            the REST client's cap on a response body. `limit` is a separate total-row cap.
        fixed_params: Query parameters the library sets itself on every REST request, by wire
            name, such as the indicators' `{"expand_underlying": False}`; they have no
            argument of their own. Every REST path of the dataset must take each one (the catalog's
            `wire parameters` gate), so a request never carries a parameter its path lacks.
            Each is reserved, with its whole family, from `filters`.
        row_times: Which of the library's time columns the rows carry (`RowTimes`);
            `RowTimes.BY_KIND`, the default, derives it (`times`): `BAR` for a `bars` dataset,
            `TICK` for any other dataset in a kind, `NONE` otherwise. The indicators declare
            `BAR` and the daily open-close summaries `DAY`.
        column_notes: A sentence the library appends to a column's description for this
            dataset, by column, where the column's meaning depends on the query in a way no
            vendor text says, such as the split adjustment of a bar's prices. Empty by
            default.
        symbols_as_written: True for a dataset whose symbols are a third party's global
            tickers rather than Massive's US-market ones (the TMX corporate events, a global
            calendar): `symbols=`, the rows of a symbol table, and the rows' symbols are then
            taken exactly as Massive writes them (`symbols.is_canonical`), not through the
            asset class's grammar. Only a REST dataset may set it: a stream's topics and a
            flat file's filter follow the class's spellings.
        option_rows: True for a dataset of every class (`ClassFromPrefix.ANY`) whose rows
            include options contracts beside other classes' tickers (the unified snapshot): it
            gets the derived `OptionRoot` column every options dataset has, null on a row
            whose `Symbol` is not an options contract. Only such a dataset may set it.
    """

    name: str
    summary: str
    asset_class: AssetClass | ClassFromPrefix = ClassFromPrefix.ANY
    is_bars: bool = False
    kind: str | None = None
    fetch_path: str | None = None
    history_path: str | None = None
    grouped_daily_path: str | None = None
    detail_path: str | None = None
    extra_paths: tuple[str, ...] = ()
    channels: Mapping[Timespan | NotBars, str] = field(default_factory=dict)
    family: ChannelFamily = ChannelFamily.TICK
    flat_file_prefixes: Mapping[Timespan | NotBars, str] = field(default_factory=dict)
    keys: Mapping[tuple[Transport, str], SourceKeys] = field(default_factory=dict)
    time_axis: tuple[str, str] | None = None
    symbol_param: str | None = None
    poll_mode: PollMode = PollMode.REPLACE
    poll_keys: tuple[str, ...] = _DEFAULT_POLL_KEYS
    nullable_poll_keys: Mapping[str, str] = field(default_factory=dict)
    revision_keys: tuple[str, ...] = ()
    per_symbol: bool = False
    symbols_overlap: bool = False
    fetch_paths_by_class: Mapping[AssetClass, str] = field(default_factory=dict)
    symbols_per_request: int | None = None
    page_size: int | None = None
    fixed_params: Mapping[str, str | int | float | bool] = field(default_factory=dict)
    row_times: RowTimes = RowTimes.BY_KIND
    column_notes: Mapping[str, str] = field(default_factory=dict)
    symbols_as_written: bool = False
    option_rows: bool = False

    def __post_init__(self) -> None:
        """Check the spec's internal consistency; nothing here reads the generated files.

        Raises:
            ValueError: If the name has an empty part (`"stocks."`, `".trades"`); the summary is not
                one line of text ending in a period (`_check_summary`); the keys of `channels` or
                `flat_file_prefixes` do not match `is_bars`; a dataset with channels, flat files, or
                a kind has no `asset_class` (`ClassFromPrefix.ANY`); `keys` names a source the
                dataset does not use, fills a `time`, `end`, or `date` key from the request, marks a
                key other than `fanout` as no instrument, or fills a non-instrument fan-out key from
                the request, or makes a key optional other than a `symbol` or `fanout` key read from
                a REST path's rows, declares an `end` key outside a bars dataset or a `date` key on
                a dataset without row times, or, for a daily summary, leaves a source without its
                `date` key; `poll_keys` holds the `Symbol` of an optional symbol key, is empty on an
                `APPEND` dataset or one whose symbols overlap, is empty together with
                `revision_keys`, or shares a column with `revision_keys`; a dataset whose symbols
                overlap has `nullable_poll_keys`;
                `nullable_poll_keys` names a column that is no poll key, or gives an empty
                reason; a flat-file prefix does not declare its `symbol` and `time` keys from
                its own columns (and, for futures, its `date` key, the session against which
                every futures file is checked); a dataset that keeps its symbols as written has
                channels or flat-file prefixes; a dataset of one class sets `option_rows`; or
                the transports match no query class (see the
                class docstring and `_check_verbs`).
        """
        if not all(self.name.split(".")):
            raise ValueError(
                f"dataset name {self.name!r} must be '<namespace>.<dataset>', or one name for a "
                "dataset the session serves itself"
            )
        self._check_summary()
        keys = {*self.channels, *self.flat_file_prefixes}
        if any((key is NotBars.ANY) == self.is_bars for key in keys):
            kind = "Timespan" if self.is_bars else "NotBars.ANY"
            raise ValueError(
                f"{self.name}: channels and flat_file_prefixes must be keyed by {kind}"
            )
        if self.channels and self.asset_class is ClassFromPrefix.ANY:
            raise ValueError(f"{self.name}: a WebSocket dataset (with channels) needs a class")
        if self.kind is not None and self.asset_class is ClassFromPrefix.ANY:
            raise ValueError(f"{self.name}: a dataset in a kind needs a class (its group)")
        if self.flat_file_prefixes and self.asset_class is ClassFromPrefix.ANY:
            # A file filter looks for the spellings of one class (`symbols.wire_spellings`).
            raise ValueError(f"{self.name}: a flat-file dataset needs a class")
        if self.symbols_as_written and (self.channels or self.flat_file_prefixes):
            # A stream subscribes and a flat file filters by the class's spellings.
            raise ValueError(f"{self.name}: symbols kept as written are for REST datasets only")
        if self.option_rows and self.asset_class is not ClassFromPrefix.ANY:
            # A dataset of one class either is the options class or has no options rows.
            raise ValueError(f"{self.name}: option_rows is for a dataset of every class")
        self._check_keys()
        self._check_poll_keys()
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
    def times(self) -> RowTimes:
        """Which of the library's time columns the rows carry: `row_times`, or derived from it.

        `BAR` for a dataset of bars (`holds_bars`), `TICK` for any other dataset in a
        kind (trades, quotes, values, LULD, imbalances, FMV), and `NONE` for the rest, unless
        `row_times` says otherwise.
        """
        if self.row_times is not RowTimes.BY_KIND:
            return self.row_times
        if self.holds_bars:
            return RowTimes.BAR
        return RowTimes.NONE if self.kind is None else RowTimes.TICK

    @property
    def holds_bars(self) -> bool:
        """True when the rows are price bars of the request's timespan.

        Every `bars()` dataset (`is_bars`) holds them, and so does every other dataset of the
        `bars` kind, such as `stocks.previous_day_bar`; their bars are stamped by the day rules
        (`history_source.stamp_rule`) and, where the request sends `adjusted`, checked against
        Massive's echo of it. The indicators carry bar times too (`RowTimes.BAR`) but hold
        indicator values, not bars.
        """
        return self.is_bars or self.kind == "bars"

    @property
    def group(self) -> str | None:
        """The dataset's group within its kind: its asset class's value, or None."""
        return None if self.asset_class is ClassFromPrefix.ANY else self.asset_class.value

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
    def is_feed(self) -> bool:
        """True for a feed: a dataset whose default `poll()` mode is `APPEND` (news, filings).

        Massive keeps a feed's whole archive, and its `start` and `end` are the only bound on
        what a cycle reads, so `poll()` on a feed refuses to start without `start`. Every feed
        declares a `time_axis`, which the catalog's feed-axis gate checks.
        """
        return self.poll_mode is PollMode.APPEND

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
            ValueError: If `keys` names a source the dataset does not read; a `time`, `end`, or
                `date` key is filled from the request; an `end` key is declared outside a bars
                dataset, or a `date` key on a dataset whose rows have no `Date` (`times` is `NONE`);
                a daily summary (`DAY`) leaves a source without its `date` key; or a key other than
                `fanout` is marked as no instrument, a fan-out key that is no instrument is filled
                from the request (it would have no column to keep), or a key other than a `symbol`
                or `fanout` key read from a REST path's rows is optional (`KeySpec.required` False),
                or a keyed (`REPLACE` or `UPSERT`) `poll()` keys on the `Symbol` of an optional
                symbol key, where every row without a symbol would share the null key. The message
                names the dataset.
        """
        unused = sorted(f"{t.value} {k}" for t, k in set(self.keys) - set(self.sources))
        if unused:
            raise ValueError(
                f"{self.name}: keys are declared for sources it does not read {unused}"
            )
        times = self.times
        if times is RowTimes.DAY:
            lacking = sorted(
                f"{t.value} {k}" for t, k in self.sources if self.keys_for(t, k).date is None
            )
            if lacking:
                raise ValueError(f"{self.name}: a daily summary needs a date key on {lacking}")
        for (transport, key), declared in self.keys.items():
            where = f"{self.name}: {transport.value} {key}"
            if declared.end is not None and times is not RowTimes.BAR:
                raise ValueError(f"{where}: only a bars dataset declares an end key")
            if declared.date is not None and times is RowTimes.NONE:
                raise ValueError(f"{where}: a date key needs a dataset whose rows have a Date")
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
                    role in _OPTIONAL_ROLES
                    and transport is Transport.REST
                    and not spec.from_request
                )
                if not spec.required and not optional_ok:
                    raise ValueError(
                        f"{where}: only a symbol or fanout key read from a REST path's rows can "
                        "be optional"
                    )

    def _check_poll_keys(self) -> None:
        """Check the `poll()` keys: the identity every mode keys or appends by.

        Raises:
            ValueError: If `poll_keys` holds the `Symbol` of an optional symbol key (a row
                without one would have a null poll key); is empty on an `APPEND` dataset, on
                one whose symbols overlap (its repeated rows are found by them), or together
                with `revision_keys`; shares a column with `revision_keys`; or if
                `nullable_poll_keys` names a column that is no poll key, gives no reason, or
                is set on a dataset whose symbols overlap.
        """
        optional = any(
            declared.symbol is not None and not declared.symbol.required
            for declared in self.keys.values()
        )
        if optional and "Symbol" in self.poll_keys:
            raise ValueError(
                f"{self.name}: poll_keys cannot hold Symbol when the symbol is optional: a row "
                "without one would have a null poll key, which fails the poll() table; declare "
                "other poll_keys"
            )
        if not self.poll_keys and self.poll_mode is PollMode.APPEND:
            raise ValueError(f"{self.name}: an APPEND dataset needs poll_keys")
        if self.symbols_overlap and not self.poll_keys:
            raise ValueError(
                f"{self.name}: a dataset whose symbols overlap needs poll_keys, which find the "
                "rows several symbols return"
            )
        if self.symbols_overlap and self.nullable_poll_keys:
            raise ValueError(
                f"{self.name}: a dataset whose symbols overlap cannot have nullable_poll_keys: "
                "a null key cannot tell two rows apart"
            )
        if not (*self.poll_keys, *self.revision_keys):
            raise ValueError(
                f"{self.name}: an explicit APPEND poll() needs a row identity: declare "
                "poll_keys or revision_keys"
            )
        if repeated := sorted(set(self.poll_keys) & set(self.revision_keys)):
            raise ValueError(f"{self.name}: revision_keys repeat poll_keys {repeated}")
        if stray := sorted(set(self.nullable_poll_keys) - set(self.poll_keys)):
            raise ValueError(f"{self.name}: nullable_poll_keys {stray} are not poll_keys")
        if unexplained := sorted(k for k, why in self.nullable_poll_keys.items() if not why):
            raise ValueError(f"{self.name}: nullable_poll_keys {unexplained} need a reason")

    def _check_flat_files(self) -> None:
        """Check that every flat-file prefix can be filtered and checked.

        Every flat-file read filters on the symbol and time columns, and every futures file is
        checked against its key's session, so each prefix must declare those keys (for futures,
        the session as its `date` key) from its own columns.

        Raises:
            ValueError: If a prefix lacks its `symbol` or `time` key, or, for futures, its
                `date` key (the session), or fills one from the request.
        """
        futures = self.asset_class is AssetClass.FUTURES
        needed = ("symbol", "time", *(("date",) if futures else ()))
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

        A fetch path is a dataset of its own: `fetch()` always returns every row its endpoint
        returns, so an endpoint that answers one row per symbol (the last trade, the previous
        day's bar) is a one-row dataset beside the stream and history one, never a verb of it.

        Raises:
            ValueError: If the dataset has no verb; if a `bars` dataset has no history source;
                or if a fetch path is combined with channels or a history source. The message
                names the dataset.
        """
        stream, history, fetch = self.has_channels, self.has_history, self.has_fetch
        if self.is_bars and not history:
            raise ValueError(f"{self.name}: a bars dataset needs a history source")
        if not (stream or history or fetch):
            raise ValueError(
                f"{self.name}: a dataset needs channels, a history source, or a fetch path"
            )
        if fetch and (stream or history):
            raise ValueError(
                f"{self.name}: a fetch path is a dataset of its own, never combined with "
                "channels or a history source"
            )


def timespan_key(timespan: Timespan | None) -> Timespan | NotBars:
    """Return the key of a request's channel and flat-file prefix in a dataset's mappings.

    `DatasetSpec.channels` and `DatasetSpec.flat_file_prefixes` key a bars dataset's sources by
    the bar timespan and every other dataset's one source by `NotBars.ANY`; a request carries
    a timespan only for bars. Call this to look a request's source up in either mapping.

    Args:
        timespan: The request's bar timespan (`Request.timespan`), or None for a request
            without one, which is any request of a dataset that is not bars.

    Returns:
        The timespan itself, or `NotBars.ANY` when there is none.
    """
    return NotBars.ANY if timespan is None else timespan


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


def takes_instants(rest_path: RestPath, name: str, kind: ParamKind) -> bool:
    """Tell whether a REST parameter compares instants rather than calendar days.

    A timestamp parameter does. Massive also types some instant filters as dates
    (`published_utc.gte` on the news paths, `last_updated.gte` on the benzinga paths); the
    generated row field the parameter is named after says which: its wire key is the
    parameter's base name, and an ISO-8601 or epoch instant field makes the parameter compare
    instants. A date parameter over a calendar-date field (`ex_dividend_date`), or with no
    top-level field of its name, compares days.

    Args:
        rest_path: The path, whose generated fields are read.
        name: The wire name of one of the path's parameters, such as `"published_utc.gte"`.
        kind: That parameter's kind.

    Returns:
        True when a date given to the parameter must become an instant before it is sent.
    """
    if kind is ParamKind.TIMESTAMP:
        return True
    if kind is not ParamKind.DATE:
        return False
    base = name.partition(".")[0]
    return any(f.wire_key == base and f.wire_type in _INSTANT_TYPES for f in rest_path.fields)


def _list_items(
    spec: DatasetSpec, key: str, value: ParamValue, *, rest_path: RestPath, name: str
) -> ParamValue:
    """Return a list filter's values one text item each, so every spelling gives one key.

    The wire joins a list with commas, so a comma can never be part of one value: `"a,b"`,
    `("a,b",)`, and `("a", "b")` all send `a,b`. Splitting every item on commas, and
    stripping the spaces around each part, makes them one value, so equal filters give equal
    queries. On a list of dates (`date.any_of`), a `date` item is its ISO-8601 day; on a list
    of numbers (`strike_price.any_of`), an `int` or `float` item is written as the base
    parameter writes one number (`encode_value`), so `(150, 152.5)` sends `150,152.5`. A
    `bool` is never a number.

    Args:
        spec: The dataset, for messages.
        key: The filter's key, for messages.
        value: A `str`, or a tuple of `str` and the items the base parameter takes; anything
            else is returned unchanged for the encoder to refuse.
        rest_path: The path the list parameter belongs to.
        name: The list parameter's wire name, such as `strike_price.any_of`. Its base
            parameter on the same path (`strike_price`) decides by its kind which items
            besides text the list takes; a list without a base parameter takes text only.

    Returns:
        The tuple of text items.

    Raises:
        ValueError: If an item is empty (`"a,,b"`, `""`, or `" "`), which Massive would read
            as a value of its own, or an item is of a kind the base parameter does not take:
            a `date` for a list that is not of dates, a number for a list that is not of
            numbers, or a `float` for a list of integers.
    """
    if isinstance(value, str):
        value = (value,)
    if not isinstance(value, tuple):
        return value
    stem = name.rpartition(".")[0]
    base = next((p for p in rest_path.params if p.name == stem), None)
    kind = None if base is None else base.kind
    texts: list[str] = []
    for item in value:
        if isinstance(item, date) and not isinstance(item, datetime):
            if kind is not ParamKind.DATE:
                raise ValueError(f"{spec.name} filter {key!r} takes no dates, not {item!r}")
            texts.append(item.isoformat())
        elif isinstance(item, (int, float)) and not isinstance(item, bool):
            if base is None or kind not in _NUMBER_KINDS:
                raise ValueError(f"{spec.name} filter {key!r} takes no numbers, not {item!r}")
            try:
                texts.append(encode_value(base, item, label=f"{spec.name} filter {key!r}"))
            except TypeError as error:
                raise ValueError(str(error)) from None
        elif isinstance(item, str):
            texts.extend(part.strip() for part in item.split(","))
        else:
            return value  # the encoder names the item it cannot send
    if "" in texts:
        raise ValueError(f"{spec.name} filter {key!r} has an empty value in {value!r}")
    return tuple(texts)


def _check_day_filter(
    spec: DatasetSpec, value: date, *, key: str, name: str, unit: WireUnit | None
) -> None:
    """Check that a date given to a range filter of an instant parameter can be sent exactly.

    The REST transport sends such a date as the instant that keeps the filter's calendar
    meaning, in the day zone of each request's symbols (`dates.instant_bound`): `_gte` a day
    is from its start, `_lt` a day before its start, `_gt` a day after its end, and `_lte` a
    day up to its end. The date itself is stored, since the zone depends on the symbols. The
    end of a day is the next day's start less one unit of the parameter, which needs a unit
    a `datetime` can step by exactly; the same function decides that here, at construction.

    Args:
        spec: The dataset, for messages.
        value: The date.
        key: The filter's key, for messages.
        name: The wire name of the parameter, which compares instants (`takes_instants`).
        unit: The parameter's timestamp unit, or None for a date parameter, which takes the
            instant as ISO-8601 text.

    Raises:
        ValueError: If the filter is a `_gt` or `_lte` bound and the parameter counts
            nanoseconds or takes ISO-8601 instants of no documented precision, so the end of
            the day cannot be written exactly; the message names the forms that work.
    """
    base, _, suffix = name.partition(".")
    if suffix not in ("gt", "gte", "lt", "lte"):
        return
    try:
        instant_bound(spec.asset_class, name, value, unit=unit, upper=suffix in ("gt", "lte"))
    except ValueError:
        raise ValueError(
            f"{spec.name} filter {key!r}: {name} has no exact unit to end a day with; "
            f"pass an aware datetime, or a date to {base}_gte or {base}_lt"
        ) from None


def _sent_value(
    spec: DatasetSpec,
    rest_path: RestPath,
    value: ParamValue,
    *,
    key: str,
    name: str,
    kind: ParamKind,
    unit: WireUnit | None,
) -> ParamValue:
    """Return a normalized `filters` value in the form that is sent.

    The request must carry the value, never the caller's spelling, so it is turned into its
    wire form here, once, and that form is both checked and stored: ISO-8601 text of a date
    or timestamp parameter is parsed, and a list is split into its items (`_list_items`). A
    date on a range filter of a parameter that compares instants stays a date, because its
    day zone follows each request's symbols; the REST transport turns it into its day's
    bound, and `_check_day_filter` refuses here the forms it could not send exactly.

    Args:
        spec: The dataset, for messages.
        rest_path: The path the parameter belongs to, whose fields say if it compares instants.
        value: The value; sequences are already tuples.
        key: The filter's key, for messages.
        name: The parameter's wire name.
        kind: The parameter's kind.
        unit: The parameter's timestamp unit, or None.

    Returns:
        The value to store; any other value unchanged.

    Text that is not ISO-8601, or has a time without an offset, raises the `ValueError` of
    `dates.parse_date_like`; a bad list or day bound the `ValueError` of `_list_items` or
    `_check_day_filter`.
    """
    if kind is ParamKind.LIST:
        return _list_items(spec, key, value, rest_path=rest_path, name=name)
    if isinstance(value, str) and kind in _TIME_KINDS:
        # An upper bound (`.lt`, `.lte`) rounds text finer than a microsecond up, so no row
        # is lost; every other form rounds down.
        value = parse_date_like(value, key, upper=name.rpartition(".")[2] in ("lt", "lte"))
    if (
        isinstance(value, date)
        and not isinstance(value, datetime)
        and takes_instants(rest_path, name, kind)
    ):
        _check_day_filter(spec, value, key=key, name=name, unit=unit)
    return value


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
            from deephaven_massive._enums import AssetClass, NotBars
            from deephaven_massive._massive import Transport

            trades = DatasetSpec(
                name="stocks.trades",
                summary="Trades of US-listed stocks, tick by tick.",
                asset_class=AssetClass.STOCKS,
                channels={NotBars.ANY: "T"},
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
        self._schemas: dict[tuple[DatasetSpec, bool], Schema] = {}

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

    def dataset_schema(self, spec: DatasetSpec, *, raw_symbol: bool = True) -> Schema:
        """Return a dataset's merged schema, computed once per shape and cached on the catalog.

        The merge of `schema.dataset_schema` over the members of the dataset's kind, or over
        the dataset alone when it is in no kind. The first call for a dataset computes it
        outside the lock and publishes it under the lock; when two threads race, the first
        to publish wins and both return that one schema. Pure: no I/O beyond the wire
        package's one-time load of the generated files, no Deephaven call.

        Args:
            spec: The dataset; a dataset in a kind must be in this catalog.
            raw_symbol: True, the default, for a session whose `unreadable_symbols` keeps an
                unreadable row (`LOG`, its default, or `IGNORE`): a dataset that reads its
                symbol from rows then has a `RawSymbol` column (`schema.dataset_schema`).
                False for a session whose `unreadable_symbols` is `RAISE`, which has none.

        Returns:
            The schema: `Symbol` and `Timestamp` first where the dataset
            declares them, then the derived columns, then every other column of the kind.

        Raises:
            LookupError: If `spec` has a kind but is not one of this catalog's datasets.

        The merge raises the `ValueError` of `schema.dataset_schema` for a conflict or a bad
        key, and an unknown source the `KeyError` of `source`.
        """
        with self._lock:
            cached = self._schemas.get((spec, raw_symbol))
        if cached is not None:
            return cached
        if spec.kind is None:
            members: tuple[DatasetSpec, ...] = (spec,)
        else:
            members = self.members(spec.kind)
            if spec not in members:
                raise LookupError(f"{spec.name} is not in the catalog of its kind {spec.kind}")
        schema = dataset_schema(spec, members, self.source, raw_symbol=raw_symbol)
        with self._lock:
            return self._schemas.setdefault((spec, raw_symbol), schema)

    def history_columns(self, spec: DatasetSpec, transport: Transport) -> frozenset[str]:
        """Return the columns a `history()` read of one transport fills, by the generated schemas.

        A REST read uses the dataset's history path and its grouped-daily path; a flat-file
        read uses its prefixes. A column outside the set is null in every row the transport
        returns. `history()` compares the two sets to warn when its `AUTO` choice leaves
        columns null that the other source fills. Pure, like `dataset_schema`.

        Args:
            spec: The dataset.
            transport: `Transport.REST` or `Transport.FLAT_FILES`.

        Returns:
            The column names (`schema.filled_columns`); only the library's time columns for a
            transport the dataset has no history source on.

        An unknown source raises the `KeyError` of `source`.
        """
        keys: list[tuple[Transport, str]]
        if transport is Transport.REST:
            paths = (spec.history_path, spec.grouped_daily_path)
            keys = [(transport, path) for path in paths if path is not None]
        else:
            keys = [(transport, prefix) for prefix in spec.flat_file_prefixes.values()]
        return filled_columns(spec, keys, self.source)

    def check_filters(
        self,
        spec: DatasetSpec,
        filters: Mapping[str, ParamValue],
        *,
        typed: Iterable[str] = (),
    ) -> tuple[tuple[str, ParamValue], ...]:
        """Validate a `filters` mapping and return the values the request carries and sends.

        Runs synchronously when the query is built. A key is a wire parameter's name with its
        dots written as underscores (`strike_price_gte` for `strike_price.gte`), for any query
        parameter of any of the dataset's REST paths, except the ones the library sets itself:
        the equality forms of the symbol parameter (`symbol_family`: its base, `.any_of`, and
        `.all_of`), the whole family of the time axis (`param_family`: its base and every
        suffixed form), the whole family of each of the dataset's `fixed_params`, `limit`,
        `sort`, `order`, and `adjusted`. None of these is accepted or named among the valid
        keys.

        Every path that declares the key checks it, and all of them must read it alike, since
        any of them may send it. Each value is first put in the form that is sent
        (`_sent_value`): ISO-8601 text of a
        date or timestamp parameter becomes a `date` or an aware `datetime`; a date on a range
        filter of a parameter that compares instants (`takes_instants`, such as
        `last_updated_gte`) stays a date, which the REST transport sends as its day's bound
        in the day zone of each request's symbols, after this check has refused a `_gt` or
        `_lte` form whose unit cannot end a day exactly; and a list becomes its
        comma-separated items, so `"a,b"` and `("a", "b")` are one value. The parsed value is
        then checked by the wire package's own encoder (`encode_value`), the same function
        that writes it into the request, so a filter that passes here cannot fail to encode
        later.

        Args:
            spec: The dataset.
            filters: The normalized filters by key; sequences are already tuples.
            typed: The wire names of the factory's typed parameters, set or not; each one's
                whole family (`param_family`) is reserved like the time axis, so neither
                accepted nor named among the valid keys.

        Returns:
            The `(key, value)` pairs in the order given, each value in its parsed form; empty
            when `filters` is.

        Raises:
            ValueError: If a key is not a filter of the dataset (the message lists the valid
                keys), or a value cannot be sent as the parameter's kind: the wrong type, a
                value outside an enum parameter's values, a non-finite number, an empty list,
                or an instant finer than a timestamp parameter's unit (the message quotes the
                encoder's). A naive `datetime`, or text of a date or timestamp parameter that
                is not ISO-8601 or has a time without an offset, raises the `ValueError` of
                `dates.require_aware` or `dates.parse_date_like`. An empty list item, or a
                date on a `_gt` or `_lte` filter whose parameter has no exact unit to end a
                day with, raises the `ValueError` of `_list_items` or `_check_day_filter`. A
                value two of the dataset's paths read differently (a date parameter on one, a
                text parameter on another) raises `ValueError`, and a path that cannot encode
                it names itself in the message.

        An unknown REST path raises the `KeyError` of `source`.
        """
        if not filters:
            return ()
        symbols = () if spec.symbol_param is None else symbol_family(spec.symbol_param)
        fixed = (form for name in spec.fixed_params for form in param_family(name))
        families = (form for name in typed for form in param_family(name))
        reserved = {*_LIBRARY_PARAMS, *spec.axis_params, *symbols, *families, *fixed}
        # Every path that declares a filter, with the parameter's position in it.
        params: dict[str, list[tuple[RestPath, int]]] = {}
        for path in spec.rest_paths:
            rest_path = self._rest(path)
            for index, param in enumerate(rest_path.params):
                if not param.in_path and param.name not in reserved:
                    params.setdefault(filter_key(param.name), []).append((rest_path, index))
        checked: list[tuple[str, ParamValue]] = []
        for key, value in filters.items():
            if isinstance(value, datetime):
                require_aware(key, value, value)
            found = params.get(key)
            if found is None:
                raise ValueError(
                    f"{spec.name} has no filter {key!r}; valid filters: {sorted(params)}"
                )
            label = f"{spec.name} filter {key!r}"
            forms = []
            for rest_path, index in found:
                param = rest_path.params[index]
                parsed = _sent_value(
                    spec,
                    rest_path,
                    value,
                    key=key,
                    name=param.name,
                    kind=param.kind,
                    unit=param.unit,
                )
                try:
                    encode_value(param, parsed, label=label)
                except (TypeError, ValueError) as error:
                    where = f" (on {rest_path.template})" if len(found) > 1 else ""
                    raise ValueError(f"{error}{where}") from None
                forms.append(parsed)
            first = forms[0]
            if any(type(form) is not type(first) or form != first for form in forms):
                raise ValueError(
                    f"{label}: the dataset's paths read {value!r} as different values "
                    f"({[f'{type(form).__name__} {form!r}' for form in forms]}), so it cannot "
                    "be sent alike"
                )
            checked.append((key, first))
        return tuple(checked)

    def check_sort(self, spec: DatasetSpec, sort: str, *, ordered: bool) -> None:
        """Check a `sort` argument against every REST path of the dataset that takes `sort`.

        Runs synchronously when the query is built, as `check_filters` does, so a misspelled
        field fails at the call rather than as an HTTP 400 at the verb. Massive documents
        `sort` in two forms. Where a path lists its sort fields (an enum `sort`, such as
        `ex_dividend_date` and `pay_date` on the dividends), `sort` must be one of them.
        Where it documents `sort` as a comma-separated list of columns, each with an optional
        `.asc` or `.desc` suffix (a text `sort`, such as `period_end.desc` on the financial
        statements), each part must name a field of the path's rows (`Field.wire_key`), as
        every default Massive documents for such a `sort` does. A path without `sort` is left
        alone: whether its dataset may send one is `build_query`'s concern.

        Args:
            spec: The dataset.
            sort: The `sort` argument, a `str`.
            ordered: True when `order` is set too, whose direction a suffixed part would
                contradict.

        Raises:
            ValueError: If `sort` is not one of an enum `sort`'s fields; or, on a text `sort`,
                if a part is empty, names no field of the path's rows, or carries its own
                `.asc` or `.desc` suffix while `order` is set. The message lists the fields
                the path takes.

        An unknown REST path raises the `KeyError` of `source`.
        """
        for path in spec.rest_paths:
            rest_path = self._rest(path)
            param = next((p for p in rest_path.params if p.name == "sort"), None)
            if param is None:
                continue
            if param.kind is ParamKind.ENUM:
                if sort not in param.values:
                    raise ValueError(
                        f"{spec.name}: sort {sort!r} is not a sort field of {path}; it takes "
                        f"{list(param.values)}"
                    )
                continue
            fields = sorted({field.wire_key for field in rest_path.fields})
            for part in (part.strip() for part in sort.split(",")):
                name, dot, suffix = part.rpartition(".")
                if not (dot and suffix in _SORT_SUFFIXES):
                    name, suffix = part, ""
                if name not in fields:
                    raise ValueError(
                        f"{spec.name}: sort part {part!r} names no field of {path}; sort takes "
                        f"comma-separated fields, each with an optional .asc or .desc, of "
                        f"{fields}"
                    )
                if suffix and ordered:
                    raise ValueError(
                        f"{spec.name}: sort part {part!r} has its own direction, so it cannot "
                        "be combined with order; drop the suffix or the order"
                    )

    def takes_param(self, spec: DatasetSpec, name: str) -> bool:
        """Tell whether any of the dataset's REST paths takes a parameter.

        Args:
            spec: The dataset.
            name: The wire name, such as `"order"`.

        Returns:
            True when at least one of the dataset's REST paths (`DatasetSpec.rest_paths`)
            declares `name` in its generated parameters.

        An unknown REST path raises the `KeyError` of `source`.
        """
        return any(
            param.name == name for path in spec.rest_paths for param in self._rest(path).params
        )

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
        - **kind membership:** a kind dataset reads only sources holding a membership of its
          `(kind, group)`, and the datasets of one `(kind, group)` together read each of those
          sources exactly once: a group may be split over a stream and history dataset and the
          one-row dataset of the same class (`stocks.trades` and `stocks.last_trade`).
        - **wire parameters:** every wire name the dataset declares is a parameter of its
          REST path: the `start`/`end` pair and the symbol parameter of the history path (or
          of the fetch path when there is none), each key filled from the request, which
          names a parameter of its own path, and each `fixed_params` name, a query parameter
          of every REST path of the dataset.
        - **append keys:** a dataset whose `poll_mode` is `APPEND` declares its own
          `poll_keys`, not the default `("Symbol",)`. The gate checks only that the key is
          explicit; the dataset's tests check that it is unique per row.
        - **feed axis:** a feed (`DatasetSpec.is_feed`) declares a `time_axis`, so the
          `start` its `poll()` requires can be given.
        - **nullable keys:** a dataset with `poll_keys` has at least one that is not in
          `nullable_poll_keys`, so no row is identified by nulls alone.
        - **poll keys:** every `poll_keys` and `revision_keys` column is a column of the
          dataset's schema.

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
            if spec.poll_mode is PollMode.APPEND and spec.poll_keys == _DEFAULT_POLL_KEYS:
                found.append(
                    f"append keys: {spec.name}: an APPEND dataset declares its own poll_keys, "
                    "unique per row, not the default ['Symbol']"
                )
            if spec.is_feed and spec.time_axis is None:
                found.append(
                    f"feed axis: {spec.name}: an APPEND dataset declares a time_axis, since its "
                    "poll() needs start"
                )
            if spec.poll_keys and set(spec.poll_keys) <= set(spec.nullable_poll_keys):
                found.append(
                    f"nullable keys: {spec.name}: every poll key may be null; at least one "
                    "must not be"
                )
            if keys:
                continue  # the merge would only repeat them
            try:
                schema = self.dataset_schema(spec)
            except ValueError as error:
                # A conflict, or another member's bad key, which that member also reports.
                found.append(str(error))
                continue
            lacking = sorted({*spec.poll_keys, *spec.revision_keys} - set(schema.column_names))
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
        field holding several tickers (a filing's or an article's `tickers`, a list) does
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
        """Check that a kind's group is read, source by source, by exactly one of its datasets.

        A kind's group (`(kind, group)`, such as trades of stocks) may be split over several
        datasets of its class: the stream and history dataset and the one-row dataset of the
        same rows (`stocks.trades` and `stocks.last_trade`). Each dataset may read only the
        group's sources; the group's first dataset in declaration order also reports the
        sources no dataset of the group reads and those several read, so a group's failure is
        listed once.

        Args:
            spec: The dataset; every source is generated.

        Returns:
            The `kind membership` failures; none for a dataset in no kind.
        """
        if spec.kind is None:
            return []
        group = f"{spec.kind}/{spec.group}"
        expected = {
            (transport, key)
            for transport in Transport
            for key, source in self._sources(transport).items()
            if any(m.kind == spec.kind and m.group == spec.group for m in source.memberships)
        }
        found = []
        if outside := set(spec.sources) - expected:
            listed = sorted(f"{t.value} {k}" for t, k in outside)
            found.append(
                f"kind membership: {spec.name}: reads outside it the {group} sources {listed}"
            )
        members = [
            m for m in self._by_name.values() if m.kind == spec.kind and m.group == spec.group
        ]
        if members[0] is not spec:
            return found
        read = [source for m in members for source in m.sources if source in expected]
        unread = expected - set(read)
        repeated = {source for source in read if read.count(source) > 1}
        for what, names in (
            (f"no {group} dataset reads", unread),
            (f"several {group} datasets read", repeated),
        ):
            if names:
                listed = sorted(f"{t.value} {k}" for t, k in names)
                found.append(f"kind membership: {spec.name}: {what} the sources {listed}")
        return found

    def _parameter_problems(self, spec: DatasetSpec) -> list[str]:
        """Check that every wire name the dataset declares is a parameter of its path.

        The `start`/`end` pair and the symbol parameter belong to the dataset's primary path,
        its history path or, when it has none, its fetch path; a key filled from the request
        belongs to its own path; and each of `fixed_params` belongs to every REST path, as a
        query parameter.

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
        ] + [
            f"wire parameters: {spec.name}: fixed parameter {name!r} is not a query parameter "
            f"of {path}"
            for path in spec.rest_paths
            for name in spec.fixed_params
            if name not in {p.name for p in self._rest(path).params if not p.in_path}
        ]
