"""The dataset catalog: one frozen `DatasetSpec` per dataset, and the generated SDK parameters.

Every dataset (`m.stocks.trades`, `m.reference.news`, ...) is described once, declaratively, by
a `DatasetSpec` in its namespace module under `_domains/`. Each of those modules exports
`DATASETS: tuple[DatasetSpec, ...]`, and this module composes them from a *fixed* module list,
`DOMAIN_MODULES`, so that adding a dataset never touches a shared file: the time-unit table,
the schema registry, and the coverage tests are all derived from the specs.

The composition is lazy (`datasets()`, `time_units()`): the domain modules import this module
for `DatasetSpec`, so importing them here at module level would be a cycle.

`SDK_PARAMS` is the table of every filter parameter each SDK `RESTClient` method accepts, keyed
by method name. It is generated from the installed SDK by `scripts/refresh_massive_catalog.py`
into the delimited block below and checked against the SDK by a parity test. `filter_params`
combines it with a dataset's own `extra_params` (for the two documented endpoints the released
SDK lacks) to give the keys the `filters` argument of that dataset accepts.
"""

from __future__ import annotations

import functools
import importlib
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Final

from deephaven_massive._dates import require_aware
from deephaven_massive._enums import AssetClass, ChannelFamily, PollMode, Timespan, Transport
from deephaven_massive._schema import ColumnType, Schema, model_schema
from deephaven_massive._timeunits import TimeField, TimeUnit, TimeUnitTable, UnitRule

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._query import QueryBase
    from deephaven_massive._schema import ColumnOverride, KeyMap

__all__ = [
    "DOMAIN_MODULES",
    "SCHEMA_FROM_MODEL",
    "SDK_PARAMS",
    "DatasetSpec",
    "ParamKind",
    "SymbolParam",
    "check_filters",
    "datasets",
    "filter_params",
    "sdk_params",
    "time_units",
    "unit_rules",
]

logger = logging.getLogger(__name__)

DOMAIN_MODULES: Final = (
    "stocks",
    "options",
    "futures",
    "indices",
    "forex",
    "crypto",
    "reference",
    "indicators",
    "economy",
    "partners",
)
"""The namespace modules under `_domains/`, each exporting `DATASETS`. A test asserts this list
equals the set of modules in the package."""


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

    A spec says what a dataset *is* (its name, schema, and query class) and how each transport
    reaches it (SDK methods, WebSocket channels, flat-file prefixes), plus the facts the
    transports must not guess: the unit of every timestamp field, the SDK parameters behind
    `start` and `end`, the default `poll()` mode and its key columns, and the REST page size.
    Specs are immutable singletons compared by identity, and safe to share across threads.

    Example:
        ::

            from deephaven_massive import _schema
            from deephaven_massive._catalog import DatasetSpec
            from deephaven_massive._enums import AssetClass, Transport
            from deephaven_massive._query import StreamHistoryLatestQuery
            from deephaven_massive._timeunits import TimeUnit

            ms, ns = TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS
            TRADES = DatasetSpec(
                name="stocks.trades",
                query_class=StreamHistoryLatestQuery,
                asset_class=AssetClass.STOCKS,
                schema=_schema.TRADES.schema,
                key_maps={Transport.WS: _schema.TRADES.key_maps[Transport.WS]},
                fetch_method="get_last_trade",
                history_method="list_trades",
                channels={None: "T"},
                flat_file_prefixes={None: "us_stocks_sip/trades_v1"},
                # Every key-map field that fills an Instant column needs a unit.
                time_units={(Transport.WS, "t"): ms, (Transport.WS, "trft"): ms},
            )

    Attributes:
        name: The dataset's dotted name, `"<namespace>.<dataset>"`, such as `"stocks.trades"`.
        query_class: The query class the factory returns. For `bars` this is the most general
            class; the overload selection in `_domains/base.py` narrows it by timespan.
        asset_class: The instruments' asset class, or None for datasets without instruments
            (economy, most partner datasets).
        is_bars: True for a `bars()` dataset: its `channels` and `flat_file_prefixes` are keyed
            by `Timespan`, and the factory picks the query class from the timespan and
            multiplier (DESIGN § 7.1). False datasets key them by None.
        schema: The dataset's union schema. Leave it out for a REST dataset with a
            `rest_model`: the schema is then derived from the model at construction
            (`_schema.model_schema`, with the dataset's REST time units and `overrides`), so
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
    query_class: type[QueryBase]
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
                from a field with no `time_units` entry, or a key column is not in `schema`.

        Deriving the schema from `rest_model` raises the `TypeError` or `ValueError` of
        `_schema.model_schema` (an unmapped SDK hint, an unknown override path).
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
    """Validate a `filters` mapping against the dataset's SDK parameters (DESIGN § 9).

    Runs synchronously when the query is built. The parameter table is generated from the SDK
    (`SDK_PARAMS`); until it has been generated it is empty and the check accepts every key.
    Values are already normalized (sequences are tuples).

    Args:
        spec: The dataset.
        filters: The normalized filters by SDK parameter name.

    Raises:
        ValueError: If a key is not a parameter of the dataset's SDK methods (the message lists
            the valid keys), or a value is of the wrong kind. A naive `datetime` raises the
            `ValueError` of `_dates.require_aware`.
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


@functools.cache
def datasets() -> Mapping[str, DatasetSpec]:
    """Return every dataset of every namespace module, keyed by dataset name.

    Imports each module of `DOMAIN_MODULES` on first call and caches the result for the life of
    the process.

    Returns:
        The specs, keyed by `DatasetSpec.name`.

    Raises:
        ValueError: If two modules declare the same dataset name.
    """
    found: dict[str, DatasetSpec] = {}
    for module_name in DOMAIN_MODULES:
        module = importlib.import_module(f"deephaven_massive._domains.{module_name}")
        specs: tuple[DatasetSpec, ...] = module.DATASETS
        for spec in specs:
            if spec.name in found:
                raise ValueError(f"dataset {spec.name} is declared twice")
            found[spec.name] = spec
    return found


def unit_rules(specs: Mapping[str, DatasetSpec]) -> dict[TimeField, UnitRule]:
    """Collect the declared timestamp units of a set of datasets.

    Args:
        specs: Datasets keyed by name, as `datasets()` returns them.

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


@functools.cache
def time_units() -> TimeUnitTable:
    """Return the process-wide `(dataset, transport, field)` unit table, built from the specs.

    Returns:
        The table of every declared timestamp unit.
    """
    return TimeUnitTable(unit_rules(datasets()))
