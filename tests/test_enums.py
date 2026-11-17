"""Tests for the enums, the runtime enum check, and the enums-only rule."""

import collections.abc
import importlib
import inspect
import typing
from enum import Enum, StrEnum
from pathlib import Path

import pytest
from deephaven.table import Table
from deephaven.table_factory import InputTable

import deephaven_massive as dm
from deephaven_massive import _enums
from deephaven_massive._enums import ChannelFamily, Recency, _require_enum
from deephaven_massive._massive import ParamKind, Transport, source, sources

EXPECTED_VALUES: dict[type[StrEnum], list[str]] = {
    dm.AssetClass: ["stocks", "options", "futures", "indices", "forex", "crypto"],
    dm.Timespan: [
        "second",
        "minute",
        "hour",
        "day",
        "week",
        "month",
        "quarter",
        "year",
        "session",
    ],
    dm.PollMode: ["auto", "replace", "upsert", "append"],
    dm.HistorySource: ["auto", "rest", "flat_files"],
    dm.Reread: ["always", "never"],
    dm.FuturesVenue: ["cme", "cbot", "nymex", "comex"],
    dm.Direction: ["gainers", "losers"],
    dm.ContractType: ["call", "put"],
    dm.SeriesType: ["open", "high", "low", "close"],
    dm.Timeframe: ["annual", "quarterly", "trailing_twelve_months"],
    dm.Order: ["asc", "desc"],
    dm.Market: ["stocks", "otc", "indices", "fx", "crypto"],
    dm.Locale: ["us", "global"],
    dm.ConditionDataType: ["trade", "bbo", "nbbo"],
    dm.Sip: ["CTA", "UTP", "OPRA"],
    dm.Subscription: ["stocks", "options", "indices", "currencies", "futures"],
    dm.TickerType: [
        "CS",
        "ADRC",
        "ADRP",
        "ADRR",
        "UNIT",
        "RIGHT",
        "PFD",
        "FUND",
        "SP",
        "WARRANT",
        "INDEX",
        "ETF",
        "ETN",
        "OS",
        "GDR",
        "OTHER",
        "NYRS",
        "AGEN",
        "EQLK",
        "BOND",
        "ADRW",
        "BASKET",
        "LT",
    ],
    dm.SymbolConvention: ["massive", "yahoo", "nasdaq", "cms"],
    dm.OnError: ["raise", "log", "ignore"],
    dm.ConnectionTaken: ["fail", "reconnect"],
}

# Enums that mirror a parameter enum of the generated REST paths, by parameter name. Every path
# that has the parameter as an enum must accept exactly the enum's values.
REST_MIRRORS: dict[type[StrEnum], str] = {
    dm.Order: "order",
    dm.Locale: "locale",
    dm.Market: "market",
    dm.Direction: "direction",
    dm.SeriesType: "series_type",
    dm.Sip: "sip",
    dm.ConditionDataType: "data_type",
    dm.ContractType: "contract_type",
}
# dm.TickerType mirrors the `type` parameter of one path only: `/v3/snapshot` also has a
# `type`, the asset class of a row; see test_ticker_type_mirrors_the_tickers_type_parameter.
# Not mirrors: dm.AssetClass (its values are the WebSocket cluster paths, with futures and
# forex, while the REST asset_class parameters say "fx" and differ per path); dm.Timespan
# (it adds SESSION, the futures resolution, and the indicators' timespan has no SECOND);
# dm.Timeframe (the financials take it as free text).


@pytest.mark.parametrize(("enum", "values"), EXPECTED_VALUES.items())
def test_each_exported_enum_is_a_strenum_of_wire_values(
    enum: type[StrEnum], values: list[str]
) -> None:
    assert issubclass(enum, StrEnum)
    assert [member.value for member in enum] == values
    assert all(member == member.value for member in enum)


def test_every_exported_enum_is_tested() -> None:
    exported = {
        getattr(dm, name)
        for name in dm.__all__
        if isinstance(getattr(dm, name), type)
        and issubclass(getattr(dm, name), Enum)
        # The wire enums; `LogOutput` is the debug log's, tested with it.
        and getattr(dm, name).__module__ == "deephaven_massive._enums"
    }
    assert exported == set(EXPECTED_VALUES)


@pytest.mark.parametrize(("ours", "parameter"), REST_MIRRORS.items())
def test_mirrored_enums_have_the_value_set_of_the_rest_parameter(
    ours: type[StrEnum], parameter: str
) -> None:
    found = {
        param.values
        for path in sources(Transport.REST).values()
        for param in path.params
        if param.name == parameter and param.kind is ParamKind.ENUM
    }
    assert found  # the parameter is still an enum somewhere
    assert all(set(values) == {m.value for m in ours} for values in found)


def test_ticker_type_mirrors_the_tickers_type_parameter() -> None:
    tickers = source(Transport.REST, "/v3/reference/tickers")
    (param,) = [p for p in tickers.params if p.name == "type"]
    assert param.kind is ParamKind.ENUM
    assert [m.value for m in dm.TickerType] == list(param.values)


def test_internal_enums() -> None:
    assert [m.value for m in Recency] == ["REALTIME", "DELAYED", "UNKNOWN"]
    assert [m.value for m in ChannelFamily] == ["TICK", "FMV"]
    assert not hasattr(dm, "Recency")
    assert not hasattr(dm, "ChannelFamily")


def test_require_enum_returns_members() -> None:
    assert _require_enum(dm.Timespan.MINUTE, dm.Timespan, "timespan") is dm.Timespan.MINUTE


def test_require_enum_names_the_matching_member() -> None:
    with pytest.raises(TypeError) as info:
        _require_enum("minute", dm.Timespan, "timespan")
    assert str(info.value) == "timespan must be dm.Timespan, e.g. dm.Timespan.MINUTE"


def test_require_enum_matches_names_and_ignores_case() -> None:
    with pytest.raises(TypeError, match=r"e\.g\. dm\.Sip\.OPRA$"):
        _require_enum("opra", dm.Sip, "sip")
    with pytest.raises(TypeError, match=r"e\.g\. dm\.HistorySource\.FLAT_FILES$"):
        _require_enum("FLAT_FILES", dm.HistorySource, "source")


def test_require_enum_falls_back_to_the_first_member() -> None:
    with pytest.raises(TypeError, match=r"^mode must be dm\.PollMode, e\.g\. dm\.PollMode\.AUTO$"):
        _require_enum("sometimes", dm.PollMode, "mode")
    with pytest.raises(TypeError, match=r"dm\.Order\.ASC$"):
        _require_enum(3, dm.Order, "order")


def test_require_enum_rejects_other_enums() -> None:
    with pytest.raises(TypeError, match=r"dm\.Market"):
        _require_enum(dm.AssetClass.STOCKS, dm.Market, "market")


def test_text_converts_with_the_constructor() -> None:
    assert dm.Timespan("minute") is dm.Timespan.MINUTE


def test_module_exports() -> None:
    assert set(_enums.__all__) == {e.__name__ for e in EXPECTED_VALUES} | {
        "Recency",
        "ChannelFamily",
        "subscription_of",
        "ClassFromPrefix",
        "NotBars",
    }
    # The class of a dataset that spans classes is internal: no public signature takes it.
    assert not hasattr(dm, "ClassFromPrefix")
    assert [member.name for member in _enums.ClassFromPrefix] == ["ANY"]
    # The source key of a dataset that is not bars is internal too.
    assert not hasattr(dm, "NotBars")
    assert [member.name for member in _enums.NotBars] == ["ANY"]
    # The transport is the wire package's, named once for both sides of the boundary.
    assert not hasattr(_enums, "Transport")


# --- The enums-only rule: no Literal of strings in any public signature. ---------------------


def _resolution_namespace() -> dict[str, object]:
    # Names that annotations in the package refer to, including TYPE_CHECKING-only ones.
    namespace: dict[str, object] = {}
    namespace.update(vars(typing))
    namespace.update(vars(collections.abc))
    for module_name in (
        "_enums",
        "_errors",
        "_config",
        "_debug_log",
        "queries",
        "queries._call",
        "queries._query",
        "queries._fetch",
        "queries._stream",
        "queries._history",
        "_datasets.filters",
        "_session",
        "_datasets.catalog",
        "_tables.liveness",
        "_datasets.symbols",
        "_datasets.dates",
        "_datasets.schema",
        "_datasets.timestamps",
        "_tables.diagnostics",
        "_tables.watchlist",
        "namespaces._query_building",
        "namespaces._partners",
        "namespaces._stocks",
        "namespaces._stock_financials",
        "namespaces._stock_filings",
    ):
        namespace.update(vars(importlib.import_module(f"deephaven_massive.{module_name}")))
    namespace.update({"Table": Table, "InputTable": InputTable})
    return namespace


def _literal_strings(annotation: object) -> list[str]:
    # Every `str` (not enum member) inside a `Literal` anywhere in an annotation.
    found: list[str] = []
    if typing.get_origin(annotation) is typing.Literal:
        found.extend(a for a in typing.get_args(annotation) if type(a) is str)
    for arg in typing.get_args(annotation):
        found.extend(_literal_strings(arg))
    return found


def _public_callables() -> list[tuple[str, object]]:
    # Every public function and method reachable from the package and its namespaces.
    query = importlib.import_module("deephaven_massive.queries")
    classes: list[type] = [dm.MassiveSession, dm.MassiveConfig, dm.ErrorPolicy]
    classes += [
        getattr(query, name)
        for name in (
            "FetchQuery",
            "StreamQuery",
            "NoSymbolStreamQuery",
            "HistoryQuery",
            "StreamHistoryQuery",
        )
    ]
    seen: set[type] = set()
    found: list[tuple[str, object]] = [
        (name, getattr(dm, name)) for name in dm.__all__ if inspect.isfunction(getattr(dm, name))
    ]
    while classes:
        cls = classes.pop()
        if cls in seen:
            continue
        seen.add(cls)
        for name, member in inspect.getmembers(cls):
            if name.startswith("_") and name != "__init__":
                continue
            if isinstance(member, property):
                returned = typing.get_type_hints(member.fget, _resolution_namespace()).get("return")
                if isinstance(returned, type) and returned.__module__.startswith(
                    "deephaven_massive"
                ):
                    classes.append(returned)
                continue
            if callable(member):
                found.append((f"{cls.__qualname__}.{name}", member))
    return found


def test_public_signatures_have_no_string_literals() -> None:
    namespace = _resolution_namespace()
    callables = _public_callables()
    assert len(callables) > 10
    offenders = {}
    for name, function in callables:
        hints = typing.get_type_hints(function, namespace)
        literals = [s for hint in hints.values() for s in _literal_strings(hint)]
        if literals:
            offenders[name] = literals
    assert offenders == {}


def test_namespaces_are_reached_by_the_walk() -> None:
    names = {name.split(".")[0] for name, _ in _public_callables()}
    assert {"MassiveSession", "FetchQuery", "StreamQuery", "HistoryQuery"} <= names


def test_literal_detection_ignores_enum_members() -> None:
    assert _literal_strings(typing.Literal[dm.Timespan.DAY]) == []
    assert _literal_strings(typing.Literal["a", 1] | None) == ["a"]


# Every public parameter typed with an enum, by `Class.method.parameter`. Each one must reject a
# plain string through `_require_enum`, and each has a test that says so (named alongside). A new
# enum parameter fails `test_enum_parameters_are_the_allowlist` until it is added here together
# with its own rejection test.
_ECONOMY_ENUMS = "namespaces/test_economy.py::test_enums_reject_text"
_INDICATOR_ENUMS = "namespaces/test_indicators.py::test_enums_reject_text"
_REFERENCE_ENUMS = "namespaces/test_reference.py::test_enums_reject_text"
_FUTURES_ENUMS = "namespaces/test_futures.py::test_enum_parameters_reject_plain_strings"

_CONVERTER_ENUMS = "datasets/test_symbols.py::test_converter_enums_reject_text"
_CONVERTER_ON_ERROR = (
    "datasets/test_symbols.py::test_a_converter_refuses_an_on_error_that_is_no_member"
)
_POLICY_CAUSES = "test_error_policy.py::test_a_cause_must_be_the_enum"
_CANONICALIZE_ENUMS = "tables/test_symbols.py::test_canonicalize_symbols_rejects_bad_arguments"

ENUM_PARAMETERS = {
    "MassiveConfig.__init__.websocket_hosts": "test_config.py::test_websocket_hosts_types",
    "MassiveConfig.__init__.rest_basic_subscriptions": (
        "test_config.py::test_rest_basic_subscriptions_hold_subscriptions_only"
    ),
    "MassiveConfig.__init__.delayed_subscriptions": (
        "test_config.py::test_delayed_subscriptions_hold_subscriptions_only"
    ),
    "ErrorPolicy.__init__.every": (
        "test_error_policy.py::test_the_setting_for_every_cause_must_be_the_enum"
    ),
    "ErrorPolicy.__init__.noncanonical_symbols": _POLICY_CAUSES,
    "ErrorPolicy.__init__.unreadable_symbols": _POLICY_CAUSES,
    "ErrorPolicy.__init__.unavailable_symbols": _POLICY_CAUSES,
    "ErrorPolicy.__init__.unreadable_keys": _POLICY_CAUSES,
    "ErrorPolicy.__init__.unconvertible_values": _POLICY_CAUSES,
    "ErrorPolicy.__init__.malformed_rows": _POLICY_CAUSES,
    "ErrorPolicy.__init__.unlisted_contracts": _POLICY_CAUSES,
    "ErrorPolicy.__init__.unscheduled_products": _POLICY_CAUSES,
    "ErrorPolicy.__init__.refused_venues": _POLICY_CAUSES,
    "MassiveConfig.__init__.connection_taken": "test_config.py::test_connection_taken_is_the_enum",
    "canonical_symbol.asset_class": _CONVERTER_ENUMS,
    "canonical_symbol.convention": _CONVERTER_ENUMS,
    "canonical_symbol.on_error": _CONVERTER_ON_ERROR,
    "canonical_symbol_crypto.on_error": _CONVERTER_ON_ERROR,
    "canonical_symbol_forex.on_error": _CONVERTER_ON_ERROR,
    "canonical_symbol_future.on_error": _CONVERTER_ON_ERROR,
    "canonical_symbol_future_spread.on_error": _CONVERTER_ON_ERROR,
    "canonical_symbol_index.on_error": _CONVERTER_ON_ERROR,
    "canonical_symbol_option.on_error": _CONVERTER_ON_ERROR,
    "canonical_symbol_stock.on_error": _CONVERTER_ON_ERROR,
    "canonical_symbol_stock_cms.on_error": _CONVERTER_ON_ERROR,
    "canonical_symbol_stock_nasdaq.on_error": _CONVERTER_ON_ERROR,
    "canonical_symbol_stock_yahoo.on_error": _CONVERTER_ON_ERROR,
    "canonicalize_symbols.asset_class": _CANONICALIZE_ENUMS,
    "canonicalize_symbols.convention": _CANONICALIZE_ENUMS,
    "canonicalize_symbols.on_error": _CANONICALIZE_ENUMS,
    "MassiveSession.snapshots.order": "test_session.py::test_unified_snapshots",
    "EconomyNamespace.funding_conditions.order": _ECONOMY_ENUMS,
    "EconomyNamespace.inflation.order": _ECONOMY_ENUMS,
    "EconomyNamespace.inflation_expectations.order": _ECONOMY_ENUMS,
    "EconomyNamespace.labor_market.order": _ECONOMY_ENUMS,
    "EconomyNamespace.treasury_yields.order": _ECONOMY_ENUMS,
    "IndicatorsNamespace.ema.order": _INDICATOR_ENUMS,
    "IndicatorsNamespace.ema.series_type": _INDICATOR_ENUMS,
    "IndicatorsNamespace.ema.timespan": _INDICATOR_ENUMS,
    "IndicatorsNamespace.macd.order": _INDICATOR_ENUMS,
    "IndicatorsNamespace.macd.series_type": _INDICATOR_ENUMS,
    "IndicatorsNamespace.macd.timespan": _INDICATOR_ENUMS,
    "IndicatorsNamespace.rsi.order": _INDICATOR_ENUMS,
    "IndicatorsNamespace.rsi.series_type": _INDICATOR_ENUMS,
    "IndicatorsNamespace.rsi.timespan": _INDICATOR_ENUMS,
    "IndicatorsNamespace.sma.order": _INDICATOR_ENUMS,
    "IndicatorsNamespace.sma.series_type": _INDICATOR_ENUMS,
    "IndicatorsNamespace.sma.timespan": _INDICATOR_ENUMS,
    "ReferenceNamespace.conditions.asset_class": _REFERENCE_ENUMS,
    "ReferenceNamespace.conditions.data_type": _REFERENCE_ENUMS,
    "ReferenceNamespace.conditions.order": _REFERENCE_ENUMS,
    "ReferenceNamespace.conditions.sip": _REFERENCE_ENUMS,
    "ReferenceNamespace.exchanges.asset_class": _REFERENCE_ENUMS,
    "ReferenceNamespace.exchanges.locale": _REFERENCE_ENUMS,
    "ReferenceNamespace.news.order": _REFERENCE_ENUMS,
    "ReferenceNamespace.ticker_types.asset_class": _REFERENCE_ENUMS,
    "ReferenceNamespace.ticker_types.locale": _REFERENCE_ENUMS,
    "ReferenceNamespace.tickers.market": _REFERENCE_ENUMS,
    "ReferenceNamespace.tickers.ticker_type": _REFERENCE_ENUMS,
    "ReferenceNamespace.tickers.order": _REFERENCE_ENUMS,
    "FetchQuery.poll.mode": "queries/test_fetch.py::test_poll_mode_is_an_enum",
    "HistoryQuery.history.source": "queries/test_history.py::test_history_argument_errors",
    "StreamHistoryQuery.history.source": "queries/test_history.py::test_history_argument_errors",
    "HistoryQuery.history.reread": "queries/test_history.py::test_history_argument_errors",
    "StreamHistoryQuery.history.reread": "queries/test_history.py::test_history_argument_errors",
    "FuturesNamespace.trades.venues": _FUTURES_ENUMS,
    "FuturesNamespace.quotes.venues": _FUTURES_ENUMS,
    "FuturesNamespace.bars.venues": _FUTURES_ENUMS,
    "FuturesNamespace.bars.timespan": (
        "namespaces/test_futures.py::test_bars_take_the_day_timespan"
    ),
    "FuturesNamespace.snapshots.order": _FUTURES_ENUMS,
    "FuturesNamespace.contracts.order": _FUTURES_ENUMS,
    "FuturesNamespace.products.order": _FUTURES_ENUMS,
    "FuturesNamespace.schedules.order": _FUTURES_ENUMS,
    "ForexNamespace.bars.timespan": "namespaces/test_forex.py::test_bars_reject_bad_arguments",
    "ForexNamespace.movers.direction": "namespaces/test_forex.py::test_movers",
    "IndicesNamespace.bars.timespan": (
        "namespaces/test_indices.py::test_bars_check_their_argument_types"
    ),
    "IndicesNamespace.snapshots.order": (
        "namespaces/test_indices.py::test_snapshots_check_their_list_parameters"
    ),
    "CryptoNamespace.bars.timespan": "namespaces/test_crypto.py::test_bar_arguments_are_checked",
    "CryptoNamespace.movers.direction": "namespaces/test_crypto.py::test_movers",
    **dict.fromkeys(
        (
            "PartnersBenzingaNamespace.analysts.order",
            "PartnersBenzingaNamespace.analyst_insights.order",
            "PartnersBenzingaNamespace.ratings.order",
            "PartnersBenzingaNamespace.bulls_bears_say.order",
            "PartnersBenzingaNamespace.guidance.order",
            "PartnersBenzingaNamespace.earnings.order",
            "PartnersBenzingaNamespace.firms.order",
            "PartnersBenzingaNamespace.news.order",
            "PartnersEtfGlobalNamespace.analytics.order",
            "PartnersEtfGlobalNamespace.constituents.order",
            "PartnersEtfGlobalNamespace.fund_flows.order",
            "PartnersEtfGlobalNamespace.profiles.order",
            "PartnersEtfGlobalNamespace.taxonomies.order",
            "PartnersTmxNamespace.corporate_events.order",
            "PartnersFableNamespace.merchant_aggregates.order",
            "PartnersFableNamespace.merchant_hierarchy.order",
        ),
        "namespaces/test_partners.py::test_order_is_an_enum",
    ),
    **dict.fromkeys(
        (
            "OptionsNamespace.bars.timespan",
            "OptionsNamespace.chain.contract_type",
            "OptionsNamespace.chain.order",
            "OptionsNamespace.snapshots.order",
            "OptionsNamespace.contracts.contract_type",
            "OptionsNamespace.contracts.order",
        ),
        "namespaces/test_options.py::test_enum_arguments_reject_plain_strings",
    ),
}

# The `m.stocks` factories' enum parameters, each rejected in `tests/namespaces/test_stocks.py`.
_STOCKS = "namespaces/test_stocks.py::"
_STOCK_FINANCIALS = "namespaces/test_stock_financials.py::"
_STOCK_LIST_DATASETS = (
    *(f"StocksNamespace.{name}" for name in ("dividends", "splits", "ipos", "short_interest")),
    *(f"StocksNamespace.{name}" for name in ("short_volume", "share_float")),
    *(f"StocksFinancialsNamespace.{name}" for name in ("balance_sheets", "income_statements")),
    *(f"StocksFinancialsNamespace.{name}" for name in ("cash_flow_statements", "ratios")),
    *(f"StocksFilingsNamespace.{name}" for name in ("index", "sections_10k", "text_8k")),
    *(f"StocksFilingsNamespace.{name}" for name in ("disclosures_8k", "risk_factors")),
    *(
        f"StocksFilingsNamespace.{name}"
        for name in ("institutional_holdings", "initial_insider_ownership")
    ),
    *(
        f"StocksFilingsNamespace.{name}"
        for name in ("insider_transactions", "risk_factor_taxonomy")
    ),
    *(f"StocksFilingsNamespace.{name}" for name in ("insider_activity", "insider_holdings")),
    "StocksFilingsNamespace.disclosure_taxonomy",
)
ENUM_PARAMETERS |= {
    "StocksNamespace.bars.timespan": f"{_STOCKS}test_bars_reject_bad_arguments",
    "StocksNamespace.movers.direction": f"{_STOCKS}test_movers_take_a_direction",
    **{
        f"{name}.order": f"{_STOCKS}test_list_datasets_take_the_list_block"
        for name in _STOCK_LIST_DATASETS
    },
    **{
        f"StocksFinancialsNamespace.{name}.timeframe": (
            f"{_STOCK_FINANCIALS}test_statements_take_a_timeframe"
        )
        for name in ("balance_sheets", "income_statements", "cash_flow_statements")
    },
}


def _enum_types(annotation: object) -> set[type]:
    # The StrEnum classes in an annotation, looking through unions and mapping keys.
    if isinstance(annotation, type) and issubclass(annotation, StrEnum):
        return {annotation}
    found: set[type] = set()
    for arg in typing.get_args(annotation):
        found |= _enum_types(arg)
    return found


def test_enum_parameters_are_the_allowlist() -> None:
    namespace = _resolution_namespace()
    found = set()
    for name, function in _public_callables():
        hints = typing.get_type_hints(function, namespace)
        for parameter, hint in hints.items():
            if parameter != "return" and _enum_types(hint):
                found.add(f"{name}.{parameter}")
    assert found == set(ENUM_PARAMETERS)


def test_every_enum_parameter_names_an_existing_rejection_test() -> None:
    tests = Path(__file__).parent
    for location in ENUM_PARAMETERS.values():
        file_name, test_name = location.split("::")
        assert f"def {test_name}(" in (tests / file_name).read_text(), location


def test_each_asset_class_has_one_subscription() -> None:
    assert {asset_class: _enums.subscription_of(asset_class) for asset_class in dm.AssetClass} == {
        dm.AssetClass.STOCKS: dm.Subscription.STOCKS,
        dm.AssetClass.OPTIONS: dm.Subscription.OPTIONS,
        dm.AssetClass.FUTURES: dm.Subscription.FUTURES,
        dm.AssetClass.INDICES: dm.Subscription.INDICES,
        dm.AssetClass.FOREX: dm.Subscription.CURRENCIES,
        dm.AssetClass.CRYPTO: dm.Subscription.CURRENCIES,
    }
    assert _enums.subscription_of(_enums.ClassFromPrefix.ANY) is None


def test_forex_has_one_spelling_in_the_enums() -> None:
    # Massive's wire value is "fx"; the member is FOREX in both enums that name forex.
    assert dm.Market.FOREX.value == "fx"
    assert dm.AssetClass.FOREX.name == dm.Market.FOREX.name
    assert "FX" not in dm.Market.__members__
