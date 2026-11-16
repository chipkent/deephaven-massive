"""The dataset catalog: one frozen `DatasetSpec` per dataset, and the `Catalog` that holds them.

Every dataset (`m.stocks.trades`, `m.reference.news`, ...) is described once, declaratively, by a
`DatasetSpec` in its namespace module under `namespaces/`, or in `_snapshots.py`. Each of those
modules exports `DATASETS: tuple[DatasetSpec, ...]`, and `_session.py` composes them, once, at
import, into the process's `Catalog` (`CATALOG`), which every session core holds. This module names
no namespace module and imports nothing above the dataset layer, so the catalog is built by the code
that owns the namespaces and handed down.

A spec names its sources, never its columns. Each source is a generated `Source` of the wire
package: a REST path by role (`fetch_path`, `detail_path`, `history_path`,
`grouped_daily_path`, `extra_paths`), a WebSocket channel, or a flat-file prefix. Its fields,
their wire types, and their timestamp units come from the committed generated files; the spec
adds only what the vendor's files cannot say: the dataset's kind, and, per source, which column
holds each key (`KeySpec`, grouped per source in a `SourceKeys`). The catalog merges the rest
(`Catalog.dataset_schema`).

A spec also says which transports serve the dataset, and its query class follows from them:
`query_class` in `queries/_choice.py` maps the channels, the history sources, and the fetch
path to one of the query classes, and `DatasetSpec` rejects, when it is created, the
combinations no query class serves.

The `filters=` rules a query checks against the catalog's REST paths are `filters.py`'s.

`Catalog.problems` runs the catalog gates over every spec, against the generated sources:
generated sources, declared keys, unique columns, member types, kind membership, and wire
parameters (see the method). The tests run it on the process's catalog and on synthetic specs.

The module is one subject although it runs past 1,000 lines: `DatasetSpec` checks every
declaration when it is created, and `Catalog` checks the specs against the generated sources,
so the declarations and the gates that keep them true read whole only side by side.
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from deephaven_massive._datasets.dates import DAY_ZONES
from deephaven_massive._datasets.filters import param_base, param_family
from deephaven_massive._datasets.row_times import DATE, INSERT, RowTimes
from deephaven_massive._datasets.schema import dataset_schema, filled_columns
from deephaven_massive._datasets.source_columns import rename
from deephaven_massive._enums import (
    AssetClass,
    ChannelFamily,
    ClassFromPrefix,
    NotBars,
    PollMode,
    Subscription,
    Timespan,
    subscription_of,
)
from deephaven_massive._errors import BrokenGuaranteeError, library_bug, required_instance
from deephaven_massive._massive import ParamKind, RestPath, Transport, WireType, sources

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from deephaven_massive._datasets.schema import Schema
    from deephaven_massive._massive import Source

__all__ = [
    "NO_NAMED_TICKER",
    "REFERENCE_PATH",
    "Catalog",
    "DatasetSpec",
    "KeySpec",
    "SourceKeys",
    "timespan_key",
]


REFERENCE_PATH: Final = re.compile(r"/v\w+/reference/")
"""The start of every reference endpoint's path (`/v3/reference/...`, `/vX/reference/...`).

Massive counts reference requests in a rate-limit bucket of their own, whatever their class."""

_FROM_REQUEST_ROLES: Final = frozenset({"symbol", "fanout"})
"""The key roles a source may leave out of its rows, filled from the request instead."""

_OPTIONAL_ROLES: Final = frozenset({"symbol", "fanout"})
"""The key roles a REST path's rows may lack (`KeySpec.required` False): a ticker Massive sends
only "if available", and the underlying that only the options rows of a mixed snapshot carry."""

_SORT_SUFFIXES: Final = frozenset({"asc", "desc"})
"""The direction suffixes a part of a comma-separated `sort` may carry."""

_LIST_TYPES: Final = frozenset({WireType.JSON_TEXT, WireType.LONG_ARRAY, WireType.STRING_ARRAY})
"""Wire types of a field that holds several values, such as a filing's list of tickers."""

_SOURCE_GONE: Final = "deprecated or removed in the snapshot"

_LIBRARY_TIME_NAME: Final = re.compile(r"Timestamp(?:[A-Z][A-Za-z0-9]*)?")
"""`Timestamp` alone or with a qualifier (`TimestampEnd`): the names of the library's time
columns, which no vendor field keeps outside a declared key."""

NO_NAMED_TICKER: Final = (
    "the call named no ticker, or the row lists none of its request's named tickers"
)
"""Why the `Symbol` of a dataset with a `ticker_list` can be null: its `nullable_poll_keys`
reason."""

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
        required: Whether every row must carry the key. True, the default, drops a row
            without it, counted in its payload's drift, never a null key. False lets such a
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

    def check(self, role: str, transport: Transport, *, where: str) -> None:
        """Check that the key fits the role it is declared for, as the attributes above say.

        Args:
            role: The key's role in its `SourceKeys`: `symbol`, `time`, `end`, `fanout`, or
                `date`.
            transport: The transport of the source that declares it.
            where: The dataset, transport, and source, which start the message.

        Raises:
            ValueError: If a key other than `symbol` or `fanout` is filled from the request; a
                key other than `fanout` is marked as no instrument; a fan-out key that is no
                instrument is filled from the request (it would have no column to keep); or a
                key other than a `symbol` or `fanout` key read from a REST path's rows is
                optional.
        """
        if self.from_request and role not in _FROM_REQUEST_ROLES:
            raise ValueError(f"{where}: the {role} key cannot come from the request")
        if not self.instrument and role != "fanout":
            raise ValueError(f"{where}: only a fanout key can be no instrument")
        if not self.instrument and self.from_request:
            raise ValueError(
                f"{where}: a fanout key that is no instrument keeps a column of the "
                "rows, so it cannot come from the request"
            )
        optional_ok = (
            role in _OPTIONAL_ROLES and transport is Transport.REST and not self.from_request
        )
        if not self.required and not optional_ok:
            raise ValueError(
                f"{where}: only a symbol or fanout key read from a REST path's rows can be optional"
            )


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

    The transports decide the dataset's query class (`query_class` in `queries/_choice.py`).
    With S meaning the spec has channels, H that it has a history source (a history path, a
    grouped-daily path, or flat-file prefixes), and F that it has a fetch path: F alone gives
    `FetchQuery`, S alone `SymbolStreamQuery` (`StreamQuery`, without `last_by()`, for a stream
    whose rows name no symbol), H alone `HistoryQuery`, and S and H `StreamHistoryQuery`.
    A fetch path is never combined with channels or a history source, so `fetch()` always
    returns every row its endpoint returns: a REST endpoint that answers one row per symbol
    (the last trade, the previous day's bar) is a one-row dataset of its own, in the same kind
    as the stream and history dataset of its class. A `bars` dataset needs H and no F; with
    S, a call's query is a `StreamHistoryQuery` only for the bars that have a channel, and a
    `HistoryQuery` for the rest (`bar_query_class`). Every other combination is rejected when
    the spec is created.

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

            # The same kind's one-row endpoint: a `FetchQuery` of its own.
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
            `"imbalances"`, `"fmv"`, `"admin_messages"`), or None for a dataset in no kind. The
            sources of a kind dataset are exactly those holding a membership of `(kind, asset
            class)` in the generated files (the kind-membership gate).
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
        channel_family: The channel family, which decides the WebSocket host.
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
        ticker_list: The column of a dataset whose rows each list several tickers, such as a
            news article's `Tickers`, or None for any other dataset. The library then fills a
            `Symbol` column of its own: each ticker the request named that the row lists, one
            row per such ticker, so an article about AAPL and MSFT read for both is listed
            once under each. A request that named one ticker gives it to every row; a row that
            lists none of a request's several named tickers keeps a null `Symbol`, with one
            WARNING per dataset; a call that names no ticker leaves `Symbol` null. Such a
            dataset has `Symbol` among its `poll_keys` and its `nullable_poll_keys`.
        fetch_paths_by_class: The REST path behind `fetch()` and `poll()` for the symbols of
            one asset class, where it differs from `fetch_path`, such as
            `{AssetClass.CRYPTO: "/v1/indicators/sma/{cryptoTicker}"}`; the symbols of every
            other class read `fetch_path`.
        symbols_per_request: The most symbols one request carries in a list-valued symbol
            parameter (`ticker.any_of`), such as 250 for the unified snapshot; a longer list is
            split over several requests. None sends every symbol in one request.
        page_size: The REST page size requested where it is not the path's documented
            maximum `limit` (`Param.maximum`), which every other request asks for: lower
            where the rows are long (article bodies, quoted filing text, financial
            statements), so one page decompresses to some tens of megabytes at most, far below
            the REST client's cap on a response body; or the maximum a path states only in
            prose (the aggregates' 50,000). `limit` is a separate total-row cap. None, the
            default, for the documented maximum; the `page size` gate requires it for a path
            that documents none, so no request falls back to Massive's default page.
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
        serves_current_data: True for a dataset whose `fetch()` and `poll()` read the market's
            current state, such as a snapshot, the last trade or quote, the day's movers, or
            an option chain. Only such a dataset's REST answers are checked for Massive's
            `DELAYED` label against `MassiveConfig.delayed_subscriptions`. A dataset of past
            data (the previous day's bar, a day's open and close, dividends) keeps
            False, the default: Massive labels it `DELAYED` on plans whose current data is
            delayed, yet its rows are final. Only a dataset with a `fetch_path` that is no
            reference endpoint (`REFERENCE_PATH`) may set it.
        latest_is_current: True for a dataset of past values whose latest one Massive computes
            from the market's current data, such as an indicator over the latest bars. Its
            `poll()`, and a `fetch()` whose window reaches the present
            (`dates.reaches_now`), are checked like current data; a `fetch()` of a past range
            is not. False, the default, otherwise. Only a dataset with a `fetch_path` that is
            no reference endpoint may set it, and never with `serves_current_data`.
        refused_alternative: The dataset to suggest when Massive refuses this dataset's REST
            requests (HTTP 403), such as `"stocks.snapshots"` for `stocks.last_trade`: one
            that serves the same data and that more plans include. None, the default,
            suggests nothing.
        names_symbols: True, the default, for a dataset whose call takes symbols or whose rows
            each name an instrument. False for a stream whose rows name none, such as the
            options admin messages, which Massive sends per exchange: its call takes no
            symbols, it subscribes its channel's every topic (`<channel>.*`), its rows have no
            `Symbol`, and its query has no `last_by()`. Only a dataset with channels and no
            other transport, not bars, may set it False, and then no source declares a `symbol`
            key; every source of such a dataset in a kind declares its `time` key (the
            `declared keys` gate).
        real_time_only: True for a stream Massive serves only in real time, with no delayed
            feed: fair market value, and the options admin messages, whose page documents only
            the real-time feed. Its stream verbs refuse delayed data at the call rather than open
            a stream Massive would refuse (`host_decision.require_real_time`): `ValueError` when
            `MassiveConfig.delayed_subscriptions` lists the subscription, `EntitlementError`
            when Massive labels the plan's data delayed. False, the default,
            otherwise. Only a dataset with channels may set it.
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
    channel_family: ChannelFamily = ChannelFamily.TICK
    flat_file_prefixes: Mapping[Timespan | NotBars, str] = field(default_factory=dict)
    keys: Mapping[tuple[Transport, str], SourceKeys] = field(default_factory=dict)
    time_axis: tuple[str, str] | None = None
    symbol_param: str | None = None
    poll_mode: PollMode = PollMode.REPLACE
    poll_keys: tuple[str, ...] = _DEFAULT_POLL_KEYS
    nullable_poll_keys: Mapping[str, str] = field(default_factory=dict)
    revision_keys: tuple[str, ...] = ()
    per_symbol: bool = False
    ticker_list: str | None = None
    fetch_paths_by_class: Mapping[AssetClass, str] = field(default_factory=dict)
    symbols_per_request: int | None = None
    page_size: int | None = None
    fixed_params: Mapping[str, str | int | float | bool] = field(default_factory=dict)
    row_times: RowTimes = RowTimes.BY_KIND
    column_notes: Mapping[str, str] = field(default_factory=dict)
    symbols_as_written: bool = False
    option_rows: bool = False
    serves_current_data: bool = False
    latest_is_current: bool = False
    refused_alternative: str | None = None
    names_symbols: bool = True
    real_time_only: bool = False

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
                `date` key; a daily summary has an asset class without a calendar day zone
                (futures, or none); `poll_keys` holds the `Symbol` of an optional symbol key or
                `TimestampInsert`, is empty on an `APPEND` dataset, is empty together with
                `revision_keys`, or shares a column with `revision_keys`; a dataset with a
                `ticker_list` lacks `Symbol` among its `poll_keys` or `nullable_poll_keys`;
                `nullable_poll_keys` names a column that is no poll key, or gives an empty
                reason; a flat-file prefix does not declare its `symbol` and `time` keys from
                its own columns (and, for futures, its `date` key, the session against which
                every futures file is checked); a dataset that keeps its symbols as written has
                channels or flat-file prefixes; a dataset of one class sets `option_rows`; a
                dataset without a fetch path, or whose fetch path is a reference endpoint, sets
                `serves_current_data` or `latest_is_current`, or a dataset sets both; a
                dataset without channels sets `real_time_only`; a dataset whose rows name no
                symbol is not a stream alone (`_check_symbol_free`); or
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
        self._check_class_requirements()
        self._check_current_data_flags()
        self._check_keys()
        self._check_symbol_free()
        if self.times is RowTimes.DAY and self.asset_class not in DAY_ZONES:
            raise ValueError(
                f"{self.name}: a daily summary needs an asset class whose days are calendar "
                f"days of one zone ({', '.join(c.value for c in DAY_ZONES)}), so its window "
                "and its descriptions can name that zone"
            )
        self._check_poll_keys()
        self._check_flat_files()
        self._check_verbs()

    def _check_class_requirements(self) -> None:
        """Check the fields that need the dataset to be of one asset class, or of every class.

        A stream, a dataset in a kind, and a flat-file dataset are of one class; symbols kept
        as written are for REST datasets only; `option_rows` is for a dataset of every class.

        Raises:
            ValueError: If a dataset with channels, flat files, or a kind has no `asset_class`
                (`ClassFromPrefix.ANY`); a dataset that keeps its symbols as written has
                channels or flat-file prefixes; or a dataset of one class sets `option_rows`.
        """
        if self.channels and self.asset_class is ClassFromPrefix.ANY:
            raise ValueError(f"{self.name}: a WebSocket dataset (with channels) needs a class")
        if self.kind is not None and self.asset_class is ClassFromPrefix.ANY:
            raise ValueError(f"{self.name}: a dataset in a kind needs a class (its group)")
        if self.flat_file_prefixes and self.asset_class is ClassFromPrefix.ANY:
            # A file filter looks for the spellings of one class (`wire_symbols.wire_spellings`).
            raise ValueError(f"{self.name}: a flat-file dataset needs a class")
        if self.symbols_as_written and (self.channels or self.flat_file_prefixes):
            # A stream subscribes and a flat file filters by the class's spellings.
            raise ValueError(f"{self.name}: symbols kept as written are for REST datasets only")
        if self.option_rows and self.asset_class is not ClassFromPrefix.ANY:
            # A dataset of one class either is the options class or has no options rows.
            raise ValueError(f"{self.name}: option_rows is for a dataset of every class")

    def _check_current_data_flags(self) -> None:
        """Check the flags that say a dataset serves current or real-time data.

        Raises:
            ValueError: If a dataset without a fetch path, or whose fetch path is a reference
                endpoint, sets `serves_current_data` or `latest_is_current`; a dataset sets
                both; or a dataset without channels sets `real_time_only`.
        """
        if (self.serves_current_data or self.latest_is_current) and (
            self.fetch_path is None or REFERENCE_PATH.match(self.fetch_path)
        ):
            # Reference data is no market state, and Massive counts it in no subscription.
            raise ValueError(
                f"{self.name}: only a dataset with a fetch path that is no reference endpoint "
                "serves current data"
            )
        if self.serves_current_data and self.latest_is_current:
            raise ValueError(
                f"{self.name}: a dataset serves current data or computes its latest value from "
                "it, not both"
            )
        if self.real_time_only and not self.channels:
            raise ValueError(f"{self.name}: only a stream (with channels) can be real-time only")

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
        kind (trades, quotes, values, LULD, imbalances, FMV, admin messages), and `NONE` for the
        rest, unless `row_times` says otherwise.
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
        (`row_times.stamp_rule`) and, where the request sends `adjusted`, checked against
        Massive's echo of it. The indicators carry bar times too (`RowTimes.BAR`) but hold
        indicator values, not bars.
        """
        return self.is_bars or self.kind == "bars"

    @property
    def group(self) -> str | None:
        """The dataset's group within its kind: its asset class's value, or None."""
        return None if self.asset_class is ClassFromPrefix.ANY else self.asset_class.value

    @property
    def single_class(self) -> AssetClass:
        """The dataset's one asset class, which a dataset read by channel, file, or kind has.

        `__post_init__` refuses a dataset with channels, flat files, or a kind whose symbols
        span classes (`ClassFromPrefix.ANY`).

        A dataset whose symbols span classes raises the `BrokenGuaranteeError` of
        `required_instance`.
        """
        return required_instance(self.asset_class, AssetClass, f"{self.name}'s asset class")

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

        Each name of `time_axis`, its base name, and every form of the base `param_family`
        lists (`.gt`, `.gte`, `.lt`, `.lte`, `.any_of`, `.all_of`): `("timestamp.gte",
        "timestamp.lt")` reserves `timestamp` and all of its forms, so no filter can send a
        second bound beside `start` and `end`.
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
    def current_subscription(self) -> Subscription | None:
        """The subscription whose current data `fetch()` and `poll()` of the dataset read.

        That is the dataset's class's subscription, unless the dataset reads no current data
        (`serves_current_data` is False) or has no single class (the cross-class snapshot). It
        names the `MassiveConfig.delayed_subscriptions` member that governs the table's
        recency.
        """
        if not self.serves_current_data or not isinstance(self.asset_class, AssetClass):
            return None
        return subscription_of(self.asset_class)

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
                or `fanout` key read from a REST path's rows is optional (`KeySpec.required`
                False). The message names the dataset. A `poll()` keyed on the `Symbol` of an
                optional symbol key is refused by `_check_poll_keys`.
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
                spec.check(role, transport, where=where)

    def _check_symbol_free(self) -> None:
        """Check that a dataset whose rows name no instrument is a plain stream.

        Such a stream subscribes its channel's every topic and routes no row by symbol, so it
        reads nothing a symbol selects (a flat file, a REST path) and declares no symbol.

        Raises:
            ValueError: If a dataset with `names_symbols` False has no channels, or has flat
                files, a REST path, bars, or a `symbol` key on any source.
        """
        if self.names_symbols:
            return
        others = self.flat_file_prefixes or self.rest_paths or self.is_bars
        if not self.channels or others:
            raise ValueError(
                f"{self.name}: a dataset whose rows name no symbol is a stream alone: channels, "
                "and no flat files, REST path, or bars"
            )
        if any(keys.symbol is not None for keys in self.keys.values()):
            raise ValueError(f"{self.name}: a dataset whose rows name no symbol has no symbol key")

    def _check_poll_keys(self) -> None:
        """Check the `poll()` keys: the identity every mode keys or appends by.

        Raises:
            ValueError: If `poll_keys` holds the `Symbol` of an optional symbol key (a row
                without one would have a null poll key) or `TimestampInsert`; is empty on an
                `APPEND` dataset or together with `revision_keys`; shares a column with
                `revision_keys`; lacks `Symbol`, as a key and as a nullable one, on a dataset
                with a `ticker_list`; or if `nullable_poll_keys` names a column that is no poll
                key or gives no reason.
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
        if INSERT in self.poll_keys:
            # A keyed poll() table's `last_by` aggregates every column outside the keys, and
            # the arrival time, every schema's last column, is the one it can always rely on.
            raise ValueError(
                f"{self.name}: poll_keys cannot hold {INSERT}: it is when a row arrived, not "
                "what the row is, and a keyed poll() table needs it outside its keys"
            )
        if not self.poll_keys and self.poll_mode is PollMode.APPEND:
            raise ValueError(f"{self.name}: an APPEND dataset needs poll_keys")
        if self.ticker_list is not None and "Symbol" not in self.nullable_poll_keys:
            # A row is listed once per named ticker it lists, and a call that names none
            # leaves Symbol null.
            raise ValueError(
                f"{self.name}: a dataset whose rows list their tickers ({self.ticker_list}) "
                "needs Symbol among its poll_keys and its nullable_poll_keys"
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
            memberships = (self.source(t, k).membership(kind, spec.group) for t, k in spec.sources)
            positions = [m.position for m in memberships if m is not None]
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
            The schema: `Symbol`, `Date`, `Timestamp`, `TimestampStart`, and `TimestampEnd`
            first where the dataset has them, then the derived columns, then every other column
            some transport of the dataset fills, and `TimestampInsert` last.

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

        Raises:
            BrokenGuaranteeError: If `transport` is one `history()` never reads from.
        """
        keys: list[tuple[Transport, str]]
        if transport is Transport.REST:
            paths = (spec.history_path, spec.grouped_daily_path)
            keys = [(transport, path) for path in paths if path is not None]
        elif transport is Transport.FLAT_FILES:
            keys = [(transport, prefix) for prefix in spec.flat_file_prefixes.values()]
        else:
            raise BrokenGuaranteeError(library_bug(f"history() never reads by {transport}"))
        return filled_columns(spec, keys, self.source)

    def check_sort(self, spec: DatasetSpec, sort: str, *, ordered: bool) -> None:
        """Check that a `sort` argument and an `order` can be sent together.

        Runs synchronously when the query is built, as `check_filters` does. Massive documents
        `sort` in two forms: a path that lists its sort fields (an enum `sort`, such as
        `ex_dividend_date` on the dividends), and a path that takes a comma-separated list of
        fields, each with an optional `.asc` or `.desc` suffix (a text `sort`, such as
        `period_end.desc` on the financial statements). Massive judges the fields itself: it
        answers HTTP 400 to a value an enum `sort` does not list and to a field its rows do
        not have, and it reads a part with surrounding whitespace (`"ticker, date"`) as the
        field it names, so every such `sort` is sent as written. Only one combination cannot
        be written into a request: a text `sort` takes its direction as the suffix of its last
        part, so a part with its own suffix and an `order` would send two directions.

        Args:
            spec: The dataset.
            sort: The `sort` argument, a `str`.
            ordered: True when `order` is set too.

        Raises:
            ValueError: If `ordered` and a part of `sort` carries its own `.asc` or `.desc`
                suffix on a path whose `sort` is text.

        An unknown REST path raises the `KeyError` of `source`.
        """
        if not ordered:
            return
        for path in spec.rest_paths:
            param = next((p for p in self.rest_path(path).params if p.name == "sort"), None)
            if param is None or param.kind is ParamKind.ENUM:
                continue
            for part in sort.split(","):
                _, dot, suffix = part.rpartition(".")
                if dot and suffix in _SORT_SUFFIXES:
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
            param.name == name for path in spec.rest_paths for param in self.rest_path(path).params
        )

    def rest_path(self, path: str) -> RestPath:
        """Return the generated REST path of a template.

        Args:
            path: The path template.

        Returns:
            The path.

        An unknown path raises the `KeyError` of `source`.

        A generated REST source that is not a `RestPath` raises the `BrokenGuaranteeError` of
        `required_instance`.
        """
        return required_instance(
            self.source(Transport.REST, path), RestPath, f"the generated REST source {path}"
        )

    def problems(self) -> tuple[str, ...]:
        """Run the catalog gates over every dataset; return every failure found.

        Each failure is one line, `"<gate>: <dataset>: <detail>"`, so a test can assert the
        whole list is empty and a reviewer sees every problem at once. The gates:

        - **generated sources:** every path, channel, and prefix a dataset uses is generated; a
          miss names the dataset and says "deprecated or removed in the snapshot". The other
          gates read the sources, so they run only once this one passes for every dataset.
        - **declared keys:** every key a dataset declares for a source is a column of that
          source after the projection of the dataset's membership, or is filled from the
          request; every source of a dataset in a kind declares `symbol` and `time` (only
          `time` where its rows name no symbol, `names_symbols` False), and every source of an
          instrument dataset (`takes_symbols`) declares `symbol`; a
          `time` key is an `epoch` or `iso-instant` field, so `Timestamp` is an `Instant`; and
          the `fanout` keys agree on `KeySpec.instrument` across the dataset's sources and
          across the members of its kind.
        - **unique columns:** after the rename, no source has a repeated column.
        - **library names:** no vendor field keeps, under its own name and outside a declared
          key, the name of a library time column: `Timestamp` or `Timestamp<Qualifier>`, nor
          `Date` on a dataset whose rows carry the library's `Date`. Each such column comes
          from a declared key or is one the library fills.
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
        - **page size:** every request of a path that takes a `limit` query parameter has a
          page size, so none falls back to Massive's default page: a dataset with such a path
          that documents no maximum sets `page_size`. A `page_size` never exceeds the
          documented maximum of a path of its dataset, and is set only where it is read and
          changes something: some path takes `limit`, and not every one of them documents
          exactly that maximum.
        - **append keys:** a dataset whose `poll_mode` is `APPEND` declares its own
          `poll_keys`, not the default `("Symbol",)`. The gate checks only that the key is
          explicit; the dataset's tests check that it is unique per row.
        - **feed axis:** a feed (`DatasetSpec.is_feed`) declares a `time_axis`, so the
          `start` its `poll()` requires can be given.
        - **nullable keys:** a dataset with `poll_keys` has at least one that is not in
          `nullable_poll_keys`, so no row is identified by nulls alone.
        - **poll keys:** every `poll_keys` and `revision_keys` column of a dataset with
          `poll()` (a fetch path) is a column of the dataset's schema.

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
            found.extend(self._page_size_problems(spec))
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
            if lacking and spec.has_fetch:  # only `poll()` reads them
                found.append(f"poll keys: {spec.name}: {lacking} are not columns")
        return tuple(dict.fromkeys(found))

    def _key_problems(self, spec: DatasetSpec) -> list[str]:
        """Check the declared keys of every source of one dataset.

        Args:
            spec: The dataset; every source is generated.

        Returns:
            The `declared keys`, `unique columns`, and `library names` failures.
        """
        needed: tuple[str, ...] = ()
        if spec.kind is not None:
            needed = ("symbol", "time") if spec.names_symbols else ("time",)
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
                columns = rename(spec, self.source(transport, key))
            except ValueError as error:
                found.append(str(error))
                continue
            claimed = {k.column for _, k in spec.keys_for(transport, key).declared()}
            dated = spec.times is not RowTimes.NONE
            for column in columns:
                kept = column.field is not None and column.field.column == column.column
                owned = _LIBRARY_TIME_NAME.fullmatch(column.column) or (
                    dated and column.column == DATE
                )
                if kept and owned and column.column not in claimed:
                    found.append(
                        f"library names: {spec.name}: {transport.value} {key}: the vendor field "
                        f"{column.column!r} has the name of a library time column but is no "
                        "declared key"
                    )
        return found + self._fanout_problems(spec)

    def takes_symbols(self, spec: DatasetSpec) -> bool:
        """Tell whether a dataset is an instrument dataset: one whose rows each name one.

        A stream whose rows name no instrument (`DatasetSpec.names_symbols` False) takes none.
        Any other dataset takes symbols when it has channels or flat files (both are read per
        instrument), `per_symbol` requests, a symbol filled from the request, or a
        `symbol_param` that selects rows by one ticker each. A symbol parameter that filters a
        field holding several tickers (a filing's or an article's `tickers`, a list) does
        not make one: such a row describes no single instrument, so the dataset keeps its
        `Tickers` column, and its `Symbol` is the library's, the requested ticker a row was
        read for (`DatasetSpec.ticker_list`). The filtered field is the row field of the
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
        if not spec.names_symbols:
            return False
        from_request = any(
            keys.symbol is not None and keys.symbol.from_request for keys in spec.keys.values()
        )
        if spec.channels or spec.flat_file_prefixes or spec.per_symbol or from_request:
            return True
        primary = spec.history_path or spec.fetch_path
        if spec.symbol_param is None or primary is None:
            return False
        stem = param_base(spec.symbol_param).removesuffix("s")
        filtered = [
            f for f in self.rest_path(primary).fields if f.wire_key.removesuffix("s") == stem
        ]
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
            if source.membership(spec.kind, spec.group) is not None
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

    def _page_size_problems(self, spec: DatasetSpec) -> list[str]:
        """Check that every paged request has a page size and that `page_size` is needed.

        Args:
            spec: The dataset; every source is generated.

        Returns:
            The `page size` failures.
        """
        maxima: dict[str, int | None] = {}
        for path in spec.rest_paths:
            for param in self.rest_path(path).params:
                if param.name == "limit" and not param.in_path:
                    maxima[path] = param.maximum
        size = spec.page_size
        if size is None:
            return [
                f"page size: {spec.name}: {path} documents no maximum limit, so page_size "
                "must be set"
                for path, most in maxima.items()
                if most is None
            ]
        if not maxima:
            return [f"page size: {spec.name}: page_size is set, but no REST path takes limit"]
        found = [
            f"page size: {spec.name}: page_size {size} exceeds the maximum limit {most} of {path}"
            for path, most in maxima.items()
            if most is not None and size > most
        ]
        if all(most == size for most in maxima.values()):
            found.append(
                f"page size: {spec.name}: page_size {size} is the documented maximum limit of "
                "every path, so it is read from the specification and must be left unset"
            )
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
        expected: list[tuple[str, str, str]] = []
        if primary is not None:
            expected = [(what, primary, name) for what, name in declared]
        elif declared:
            return [f"wire parameters: {spec.name}: a {declared[0][0]} needs a REST path"]
        for path in spec.rest_paths:
            for role, key in spec.keys_for(Transport.REST, path).declared():
                if key.from_request:
                    expected.append((f"{role} key", path, key.column))
        return [
            f"wire parameters: {spec.name}: {what} {name!r} is not a parameter of {path}"
            for what, path, name in expected
            if name not in {param.name for param in self.rest_path(path).params}
        ] + [
            f"wire parameters: {spec.name}: fixed parameter {name!r} is not a query parameter "
            f"of {path}"
            for path in spec.rest_paths
            for name in spec.fixed_params
            if name not in {p.name for p in self.rest_path(path).params if not p.in_path}
        ]
