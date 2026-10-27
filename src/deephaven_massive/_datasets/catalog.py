"""The dataset catalog: one frozen `DatasetSpec` per dataset, the `Catalog`, and the SDK parameters.

Every dataset (`m.stocks.trades`, `m.reference.news`, ...) is described once, declaratively, by
a `DatasetSpec` in its namespace module under `_namespaces/`. Each of those modules exports
`DATASETS: tuple[DatasetSpec, ...]`, and `_session.py` composes them, once, at import, into the
process's `Catalog` (`CATALOG`), which every session core holds. Adding a dataset never touches
a shared file: the time-unit rules, the schema registry, and the coverage tests are all derived
from the specs. This module names no namespace module and imports nothing above the dataset
layer, so the catalog is built by the code that owns the namespaces and handed down.

A spec says which transports serve the dataset, and its query class follows from them:
`query_class` in `_namespaces/base.py` maps the channels, the history sources, and the fetch
method to one of the query classes, and `DatasetSpec` rejects, when it is created, the
combinations no query class serves.

`SDK_PARAMS` is the table of every filter parameter each SDK `RESTClient` method accepts, keyed
by method name. It is generated from the installed SDK by `scripts/refresh_massive_catalog.py`
into the delimited block below and checked against the SDK by a parity test. `filter_params`
combines it with a dataset's own `extra_params` (for the two documented endpoints the released
SDK lacks) to give the keys the `filters` argument of that dataset accepts.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from deephaven_massive._datasets.dates import require_aware
from deephaven_massive._datasets.schema import ColumnType, Schema, model_schema
from deephaven_massive._datasets.timestamps import TimeField, TimeUnit, UnitRule
from deephaven_massive._enums import AssetClass, ChannelFamily, PollMode, Timespan, Transport

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from deephaven_massive._datasets.schema import ColumnOverride, KeyMap

__all__ = [
    "SCHEMA_FROM_MODEL",
    "SDK_PARAMS",
    "Catalog",
    "DatasetSpec",
    "ParamKind",
    "SymbolParam",
    "check_filters",
    "filter_params",
    "sdk_params",
    "unit_rules",
]

logger = logging.getLogger(__name__)


class ParamKind(StrEnum):
    """The kind of value an SDK filter parameter accepts, as `filters` validates it."""

    STRING = "str"
    INTEGER = "int"
    NUMBER = "float"
    BOOLEAN = "bool"
    DATE = "date"
    """A calendar date: a `date` or a `YYYY-MM-DD` string."""
    DATETIME = "datetime"
    """An instant or date: a `datetime`, a `date`, or an ISO-8601 string."""
    STRING_SEQUENCE = "Sequence[str]"


SCHEMA_FROM_MODEL: Final = Schema("<derived from rest_model>", ())
"""The `DatasetSpec.schema` default: the schema is derived from `rest_model` at construction."""


class SymbolParam(StrEnum):
    """How a REST dataset's symbols reach its SDK request (internal).

    Declared on each `DatasetSpec` so that the REST transport can build every request from the
    spec alone, without a second table keyed by SDK method name.
    """

    NONE = "none"
    """The dataset takes no symbols (economy data, most partner datasets)."""
    TICKER = "ticker"
    """One symbol per request in the `ticker` parameter (`list_trades`); see `per_symbol`."""
    TICKERS = "tickers"
    """A list of symbols in one `tickers` parameter (`get_snapshot_all`)."""
    TICKER_ANY_OF = "ticker.any_of"
    """A list of symbols in the `ticker.any_of` filter (`list_universal_snapshots`)."""
    PATH = "path"
    """One symbol per request as a path segment (`get_ticker_details`)."""
    PAIR = "pair"
    """A forex or crypto pair split into `from_` and `to` (`get_last_forex_quote`)."""


@dataclass(frozen=True, kw_only=True, eq=False, slots=True)
class DatasetSpec:
    """The declarative description of one dataset, shared by every transport.

    A spec says what a dataset *is* (its name and schema) and how each transport reaches it (SDK
    methods, WebSocket channels, flat-file prefixes), plus the facts the transports must not
    guess: the unit of every timestamp field, the SDK parameters behind
    `start` and `end`, the default `poll()` mode and its key columns, and the REST page size.
    Specs are immutable singletons compared by identity, and safe to share across threads.

    The transports decide the dataset's query class (`query_class` in `_namespaces/base.py`).
    With S meaning the spec has channels, H that it has a history source (a history method, a
    grouped-daily method, or flat-file prefixes), and F that it has a fetch method: F alone
    gives `Query`, S alone `StreamQuery`, H alone `HistoryQuery`, S and H `StreamHistoryQuery`,
    and S, H, and F `StreamHistoryLatestQuery`. A `bars` dataset needs H; its class is picked per
    call from the timespan and multiplier (`bar_query_class`). Every other combination is
    rejected when the spec is created.

    Example:
        ::

            from deephaven_massive._datasets.catalog import DatasetSpec
            from deephaven_massive._datasets.schema import TRADES
            from deephaven_massive._datasets.timestamps import TimeUnit
            from deephaven_massive._enums import AssetClass, Transport

            ms, ns = TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS
            # Channels, history, and a fetch method: a StreamHistoryLatestQuery.
            STOCK_TRADES = DatasetSpec(
                name="stocks.trades",
                asset_class=AssetClass.STOCKS,
                schema=TRADES.schema,
                key_maps={Transport.WS: TRADES.key_maps[Transport.WS]},
                fetch_method="get_last_trade",
                history_method="list_trades",
                channels={None: "T"},
                flat_file_prefixes={None: "us_stocks_sip/trades_v1"},
                # Every key-map field that fills an Instant column needs a unit.
                time_units={(Transport.WS, "t"): ms, (Transport.WS, "trft"): ms},
            )

    Attributes:
        name: The dataset's dotted name, `"<namespace>.<dataset>"`, such as `"stocks.trades"`.
        asset_class: The instruments' asset class, or None for datasets without instruments
            (economy, most partner datasets).
        is_bars: True for a `bars()` dataset: its `channels` and `flat_file_prefixes` are keyed
            by `Timespan`, and the factory picks the query class from the timespan and
            multiplier. False datasets key them by None.
        schema: The dataset's union schema. Leave it out for a REST dataset with a
            `rest_model`: the schema is then derived from the model at construction
            (`schema.model_schema`, with the dataset's REST time units and `overrides`), so
            every check below runs at import for REST datasets too. Exactly one of `schema` and
            `rest_model` is given.
        rest_model: The SDK model class whose fields define the REST columns.
        rest_columns: Set at construction from `rest_model`: the column each flattened SDK
            field path fills (`ModelSchema.columns_by_path`), for the REST transport's rows;
            empty for a dataset with an explicit `schema`.
        key_maps: Per transport, which wire key, SDK field path, or CSV header fills each
            column (tick datasets).
        overrides: Corrections to the columns derived from `rest_model`, keyed by field path.
        fetch_method: The `RESTClient` method behind `fetch()` and `poll()`.
        history_method: The `RESTClient` method behind `history(source=REST)`.
        grouped_daily_method: The `RESTClient` method that serves `history()` for `dm.ALL`
            daily bars, one request per day.
        rest_paths: The documented REST path templates the dataset uses.
        covers: Further documented paths this dataset serves, listed for the coverage test
            (for example `/v3/snapshot/options/{u}/{o}` is covered by `options.snapshots`).
        channels: The WebSocket channel per bar timespan, or under the key None for datasets
            that are not bars, such as `{None: "T"}` or
            `{Timespan.SECOND: "A", Timespan.MINUTE: "AM"}`.
        family: The channel family, which decides the WebSocket host.
        flat_file_prefixes: The S3 key prefix per bar timespan, or under None for non-bar
            datasets. A futures prefix contains the placeholder `{venue}`.
        time_units: The unit of every timestamp field, keyed by `(transport, field)`.
        unverified_time_units: The keys of `time_units` whose unit could not be verified;
            they get the 1990 to 2100 range check.
        time_axis: The SDK parameter pair that `start` and `end` map to, such as
            `("timestamp_gte", "timestamp_lt")`, for datasets with a `…T` time axis.
        poll_mode: The default `poll()` mode.
        poll_keys: The key columns of a keyed (`REPLACE`, `UPSERT`) `poll()` table.
        per_symbol: True when the endpoint takes one symbol per request, so a symbol list fans
            out to one request per symbol.
        symbol_param: How the symbols reach the SDK request (`SymbolParam`).
        page_size: The REST page size requested; `limit` is a separate total-row cap.
        extra_params: Filter parameters the SDK method lacks (the two documented SDK gaps).
    """

    name: str
    asset_class: AssetClass | None = None
    is_bars: bool = False
    schema: Schema = SCHEMA_FROM_MODEL
    rest_model: type | None = None
    rest_columns: Mapping[str, str] = field(init=False, default_factory=dict)
    key_maps: Mapping[Transport, KeyMap] = field(default_factory=dict)
    overrides: Mapping[str, ColumnOverride] = field(default_factory=dict)
    fetch_method: str | None = None
    history_method: str | None = None
    grouped_daily_method: str | None = None
    rest_paths: tuple[str, ...] = ()
    covers: tuple[str, ...] = ()
    channels: Mapping[Timespan | None, str] = field(default_factory=dict)
    family: ChannelFamily = ChannelFamily.TICK
    flat_file_prefixes: Mapping[Timespan | None, str] = field(default_factory=dict)
    time_units: Mapping[tuple[Transport, str], TimeUnit] = field(default_factory=dict)
    unverified_time_units: frozenset[tuple[Transport, str]] = frozenset()
    time_axis: tuple[str, str] | None = None
    poll_mode: PollMode = PollMode.REPLACE
    poll_keys: tuple[str, ...] = ("Symbol",)
    per_symbol: bool = False
    symbol_param: SymbolParam = SymbolParam.NONE
    page_size: int | None = None
    extra_params: Mapping[str, ParamKind] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Check the spec's internal consistency.

        Raises:
            ValueError: If the name is not `"<namespace>.<dataset>"`, the keys of `channels`
                or `flat_file_prefixes` do not match `is_bars`, not exactly one of `schema` and
                `rest_model` is given, a WebSocket dataset (one with `channels`) has a
                `rest_model` instead of a hand-written schema or no `asset_class`, an
                unverified unit is not among `time_units`, a key map fills an `Instant` column
                from a field with no `time_units` entry, a key column is not in `schema`, or a
                dataset with flat files has no `FLAT_FILES` key map filling `Symbol` and
                `Timestamp` (and, for futures, `SessionEndDate`, against which every futures
                file is checked), or the transports match no query class (see the class
                docstring and `_check_verbs`).

        Deriving the schema from `rest_model` raises the `TypeError` or `ValueError` of
        `schema.model_schema` (an unmapped SDK hint, an unknown override path).
        """
        namespace, _, dataset = self.name.partition(".")
        if not namespace or not dataset:
            raise ValueError(f"dataset name {self.name!r} must be '<namespace>.<dataset>'")
        keys = {*self.channels, *self.flat_file_prefixes}
        if any((key is None) == self.is_bars for key in keys):
            kind = "Timespan" if self.is_bars else "None"
            raise ValueError(
                f"{self.name}: channels and flat_file_prefixes must be keyed by {kind}"
            )
        given = self.schema is not SCHEMA_FROM_MODEL
        if not given and self.rest_model is None:
            raise ValueError(f"{self.name}: give exactly one of schema and rest_model")
        if self.channels and self.rest_model is not None:
            # The stream verbs build every shape from the hand-written tick schema and its key
            # maps (`last_by` validates its keys against it).
            raise ValueError(f"{self.name}: a WebSocket dataset (with channels) needs a schema")
        if self.channels and self.asset_class is None:
            raise ValueError(f"{self.name}: a WebSocket dataset (with channels) needs a class")
        if self.rest_model is not None:
            rest_units = {f for (t, f) in self.time_units if t is Transport.REST}
            model = model_schema(
                self.name, self.rest_model, instant_fields=rest_units, overrides=self.overrides
            )
            # A copy made with `dataclasses.replace` passes the derived schema back in; any
            # other schema next to a `rest_model` is a second, conflicting definition.
            if given and self.schema != model.schema:
                raise ValueError(f"{self.name}: give exactly one of schema and rest_model")
            object.__setattr__(self, "schema", model.schema)
            object.__setattr__(self, "rest_columns", model.columns_by_path)
        stray = sorted(
            f"{t.value}:{f}" for t, f in self.unverified_time_units - set(self.time_units)
        )
        if stray:
            raise ValueError(f"{self.name}: unverified units {stray} are not in time_units")
        missing = sorted(set(self.poll_keys) - set(self.schema.column_names))
        if missing:
            raise ValueError(f"{self.name}: poll_keys {missing} are not columns")
        self._check_flat_files()
        instants = {c.name for c in self.schema.columns if c.type is ColumnType.INSTANT}
        unitless = sorted(
            f"{transport.value}:{key}"
            for transport, key_map in self.key_maps.items()
            for key, column in key_map.items()
            if column in instants and (transport, key) not in self.time_units
        )
        if unitless:
            # Caught here, at import, rather than as a LookupError on a transport thread at
            # the first frame or page.
            raise ValueError(f"{self.name}: Instant fields {unitless} have no time_units")
        self._check_verbs()

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
        """True when the dataset has a REST fetch method (`fetch()` works)."""
        return self.fetch_method is not None

    @property
    def has_rest_history(self) -> bool:
        """True when the dataset has a REST history endpoint (`history(source=REST)` works)."""
        return self.history_method is not None

    @property
    def has_grouped_daily(self) -> bool:
        """True when a grouped-daily REST endpoint serves `dm.ALL` 1-day bars, one request a day."""
        return self.grouped_daily_method is not None

    def _check_verbs(self) -> None:
        """Check that the dataset's transports match one query class.

        Raises:
            ValueError: If the dataset has no verb; if it has channels and a fetch method but
                no history source (no query class streams and fetches without history); if,
                outside `bars`, it has a history source and a fetch method but no channels
                (that class is kept for daily bars); or if a `bars` dataset has no history
                source. The message names the dataset.
        """
        stream, history, fetch = self.has_channels, self.has_history, self.has_fetch
        if self.is_bars:
            if not history:
                raise ValueError(f"{self.name}: a bars dataset needs a history source")
            return
        if not (stream or history or fetch):
            raise ValueError(
                f"{self.name}: a dataset needs channels, a history source, or a fetch method"
            )
        if stream and fetch and not history:
            raise ValueError(
                f"{self.name}: a dataset with channels and a fetch method needs a history source"
            )
        if history and fetch and not stream:
            raise ValueError(
                f"{self.name}: only bars combine a history source and a fetch method "
                "without channels"
            )

    def _check_flat_files(self) -> None:
        """Check that a dataset with flat files can be read from them.

        Every flat-file read filters on `Symbol` and `Timestamp`, and every futures file is
        checked against its key's session through `SessionEndDate`, so those columns must be
        filled from the files' headers.

        Raises:
            ValueError: If the dataset has flat-file prefixes but its `FLAT_FILES` key map does
                not fill `Symbol` and `Timestamp`, and `SessionEndDate` for futures.
        """
        if not self.flat_file_prefixes:
            return
        needed = {"Symbol", "Timestamp"}
        if self.asset_class is AssetClass.FUTURES:
            needed.add("SessionEndDate")
        lacking = sorted(needed - set(self.key_maps.get(Transport.FLAT_FILES, {}).values()))
        if lacking:
            raise ValueError(f"{self.name}: the FLAT_FILES key map must fill {lacking}")


# --- BEGIN GENERATED SDK_PARAMS ---------------------------------------------------------------
# Generated by scripts/refresh_massive_catalog.py from inspect.signature(RESTClient.<method>).
# Do not edit by hand; a parity test compares this block with the installed SDK.
SDK_PARAMS: Final[Mapping[str, Mapping[str, ParamKind]]] = {}
# --- END GENERATED SDK_PARAMS -----------------------------------------------------------------


def sdk_params(method: str) -> Mapping[str, ParamKind]:
    """Return the parameters one SDK `RESTClient` method accepts, from the generated table.

    Args:
        method: The method name, such as `"list_trades"`.

    Returns:
        The method's parameters and their kinds.

    Raises:
        LookupError: If the method is not in `SDK_PARAMS`; run
            `scripts/refresh_massive_catalog.py` to regenerate the table.
    """
    try:
        return SDK_PARAMS[method]
    except KeyError:
        raise LookupError(
            f"RESTClient.{method} is not in SDK_PARAMS; run scripts/refresh_massive_catalog.py"
        ) from None


def filter_params(spec: DatasetSpec) -> Mapping[str, ParamKind]:
    """Return every filter key a dataset's `filters` argument accepts, with its kind.

    The keys are the union of the parameters of the dataset's REST methods (from
    `SDK_PARAMS`) and its `extra_params`.

    Args:
        spec: The dataset.

    Returns:
        The accepted keys and their kinds.
    """
    params: dict[str, ParamKind] = {}
    for method in (spec.fetch_method, spec.history_method):
        if method is not None:
            params.update(sdk_params(method))
    params.update(spec.extra_params)
    return params


_KINDS: Final[Mapping[ParamKind, tuple[type, ...]]] = {
    ParamKind.STRING: (str,),
    ParamKind.INTEGER: (int,),
    ParamKind.NUMBER: (int, float),
    ParamKind.BOOLEAN: (bool,),
    ParamKind.DATE: (date, str),
    ParamKind.DATETIME: (date, str),
    ParamKind.STRING_SEQUENCE: (tuple, str),
}


def check_filters(spec: DatasetSpec, filters: Mapping[str, object]) -> None:
    """Validate a `filters` mapping against the dataset's SDK parameters.

    Runs synchronously when the query is built. The parameter table is generated from the SDK
    (`SDK_PARAMS`); until it has been generated it is empty and the check accepts every key.
    Values are already normalized (sequences are tuples).

    Args:
        spec: The dataset.
        filters: The normalized filters by SDK parameter name.

    Raises:
        ValueError: If a key is not a parameter of the dataset's SDK methods (the message lists
            the valid keys), or a value is of the wrong kind. A naive `datetime` raises the
            `ValueError` of `dates.require_aware`.
    """
    if not SDK_PARAMS:
        return
    params = filter_params(spec)
    for key, value in filters.items():
        if isinstance(value, datetime):
            require_aware(key, value, value)
        kind = params.get(key)
        if kind is None:
            raise ValueError(f"{spec.name} has no filter {key!r}; valid filters: {sorted(params)}")
        wrong_bool = isinstance(value, bool) and kind is not ParamKind.BOOLEAN
        if wrong_bool or not isinstance(value, _KINDS[kind]) or not _is_date(kind, value):
            raise ValueError(
                f"{spec.name} filter {key!r} takes a {kind.value} value, not {value!r}"
            )


def _is_date(kind: ParamKind, value: object) -> bool:
    """Tell whether a value fits a `DATE` parameter; other kinds pass.

    A `DATE` takes a `date` (not a `datetime`) or a `YYYY-MM-DD` string.

    Args:
        kind: The parameter's kind.
        value: The filter value.

    Returns:
        False only for a `DATE` value that is a `datetime` or text that is not an ISO date.
    """
    if kind is not ParamKind.DATE:
        return True
    if isinstance(value, str):
        try:
            date.fromisoformat(value)
        except ValueError:
            return False
        return True
    return not isinstance(value, datetime)


def unit_rules(specs: Mapping[str, DatasetSpec]) -> dict[TimeField, UnitRule]:
    """Collect the declared timestamp units of a set of datasets.

    Args:
        specs: Datasets keyed by name, as `Catalog.by_name` holds them.

    Returns:
        One rule per declared `(dataset, transport, field)`.
    """
    return {
        TimeField(spec.name, transport, name): UnitRule(
            unit, unverified=(transport, name) in spec.unverified_time_units
        )
        for spec in specs.values()
        for (transport, name), unit in spec.time_units.items()
    }


class Catalog:
    """Every declared dataset of the process, by name.

    `_session.py` builds the one catalog, `CATALOG`, at import from the `DATASETS` of every
    namespace module, and hands it to each session core (`SessionCore.catalog`), where the
    transports read it. The catalog is immutable once built, so it is safe to share across
    threads and sessions.

    Example:
        ::

            from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
            from deephaven_massive._datasets.schema import TRADES
            from deephaven_massive._enums import AssetClass

            trades = DatasetSpec(
                name="stocks.trades",
                asset_class=AssetClass.STOCKS,
                schema=TRADES.schema,
                channels={None: "T"},
            )
            catalog = Catalog((trades,))
            catalog.by_name["stocks.trades"] is trades  # True
    """

    __slots__ = ("_by_name",)

    def __init__(self, specs: Iterable[DatasetSpec]) -> None:
        """Collect the specs.

        Args:
            specs: Every dataset, in declaration order.

        Raises:
            ValueError: If two specs share a dataset name.
        """
        by_name: dict[str, DatasetSpec] = {}
        for spec in specs:
            if spec.name in by_name:
                raise ValueError(f"dataset {spec.name} is declared twice")
            by_name[spec.name] = spec
        self._by_name: Final = MappingProxyType(by_name)

    @property
    def by_name(self) -> Mapping[str, DatasetSpec]:
        """The specs, keyed by `DatasetSpec.name`, in declaration order; read-only."""
        return self._by_name
