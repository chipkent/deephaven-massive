"""Tests for the dataset catalog and the SDK parameter table (DESIGN § 4.1, § 9).

This file is also where the catalog's Massive coverage assertions will live (DESIGN § 4.2).
"""

import dataclasses
import importlib
import pkgutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest

from deephaven_massive import _catalog, _domains
from deephaven_massive._catalog import (
    DOMAIN_MODULES,
    SDK_PARAMS,
    DatasetSpec,
    ParamKind,
    datasets,
    filter_params,
    sdk_params,
    time_units,
    unit_rules,
)
from deephaven_massive._enums import AssetClass, Timespan
from deephaven_massive._query import Query, StreamHistoryLatestQuery
from deephaven_massive._schema import TRADES
from deephaven_massive._timeunits import TimeField, TimeUnit, Transport, UnitRule


@pytest.fixture
def fresh_catalog() -> Iterator[None]:
    datasets.cache_clear()
    time_units.cache_clear()
    yield
    datasets.cache_clear()
    time_units.cache_clear()


BASE = DatasetSpec(
    name="test.trades",
    query_class=StreamHistoryLatestQuery,
    asset_class=AssetClass.STOCKS,
    schema=TRADES.schema,
    channels={None: "T"},
    time_units={
        (Transport.WS, "t"): TimeUnit.MILLISECONDS,
        (Transport.REST, "timestamp"): TimeUnit.MILLISECONDS,
    },
    unverified_time_units=frozenset({(Transport.REST, "timestamp")}),
)


def _trades(**changes: object) -> DatasetSpec:
    return dataclasses.replace(BASE, **cast("dict[str, Any]", changes))


def test_fixed_module_list_equals_the_domain_package() -> None:
    package = Path(_domains.__file__).parent
    found = {info.name for info in pkgutil.iter_modules([str(package)])} - {"base"}
    assert set(DOMAIN_MODULES) == found
    assert len(DOMAIN_MODULES) == len(set(DOMAIN_MODULES))


def test_every_domain_module_exports_datasets() -> None:
    for name in DOMAIN_MODULES:
        module = importlib.import_module(f"deephaven_massive._domains.{name}")
        assert isinstance(module.DATASETS, tuple)
        assert all(isinstance(spec, DatasetSpec) for spec in module.DATASETS)


def test_datasets_aggregate_every_module() -> None:
    specs = datasets()
    expected = sum(
        len(importlib.import_module(f"deephaven_massive._domains.{n}").DATASETS)
        for n in DOMAIN_MODULES
    )
    assert len(specs) == expected
    assert all(name == spec.name for name, spec in specs.items())


def test_datasets_reject_duplicates(fresh_catalog: None, monkeypatch: pytest.MonkeyPatch) -> None:
    spec = _trades()
    stocks = importlib.import_module("deephaven_massive._domains.stocks")
    options = importlib.import_module("deephaven_massive._domains.options")
    monkeypatch.setattr(stocks, "DATASETS", (spec,))
    monkeypatch.setattr(options, "DATASETS", (spec,))
    with pytest.raises(ValueError, match=r"test\.trades is declared twice"):
        datasets()


def test_time_units_compose_the_specs(fresh_catalog: None, monkeypatch: pytest.MonkeyPatch) -> None:
    stocks = importlib.import_module("deephaven_massive._domains.stocks")
    monkeypatch.setattr(stocks, "DATASETS", (_trades(),))
    table = time_units()
    assert len(table) == 2
    assert table.rule(TimeField("test.trades", Transport.WS, "t")) == UnitRule(
        TimeUnit.MILLISECONDS
    )
    assert table.rule(TimeField("test.trades", Transport.REST, "timestamp")).unverified


def test_unit_rules() -> None:
    rules = unit_rules({"test.trades": _trades()})
    assert rules[TimeField("test.trades", Transport.REST, "timestamp")] == UnitRule(
        TimeUnit.MILLISECONDS, unverified=True
    )


def test_spec_validation() -> None:
    with pytest.raises(ValueError, match=r"must be '<namespace>\.<dataset>'"):
        _trades(name="trades")
    with pytest.raises(ValueError, match=r"unverified units \['WS:q'\]"):
        _trades(unverified_time_units=frozenset({(Transport.WS, "q")}))
    with pytest.raises(ValueError, match=r"poll_keys \['Ticker'\]"):
        _trades(poll_keys=("Ticker",))
    assert _trades(schema=None, poll_keys=("Ticker",)).schema is None


def test_specs_are_identity_hashed() -> None:
    a, b = _trades(), _trades()
    assert a != b
    assert len({a, b}) == 2


def test_sdk_params_block_is_generated_and_empty_until_p3() -> None:
    source = Path(_catalog.__file__).read_text()
    begin = source.index("# --- BEGIN GENERATED SDK_PARAMS")
    end = source.index("# --- END GENERATED SDK_PARAMS")
    assert "SDK_PARAMS: Final[" in source[begin:end]
    assert dict(SDK_PARAMS) == {}


def test_sdk_params_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(LookupError, match="refresh_massive_catalog"):
        sdk_params("list_trades")
    monkeypatch.setattr(
        _catalog, "SDK_PARAMS", {"list_trades": {"timestamp_gte": ParamKind.DATETIME}}
    )
    assert sdk_params("list_trades") == {"timestamp_gte": ParamKind.DATETIME}


def test_filter_params_merge_methods_and_extras(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        _catalog,
        "SDK_PARAMS",
        {
            "get_last_trade": {"ticker": ParamKind.STRING},
            "list_trades": {"timestamp_gte": ParamKind.DATETIME},
        },
    )
    spec = _trades(
        fetch_method="get_last_trade",
        history_method="list_trades",
        extra_params={"precision": ParamKind.INTEGER},
    )
    assert filter_params(spec) == {
        "ticker": ParamKind.STRING,
        "timestamp_gte": ParamKind.DATETIME,
        "precision": ParamKind.INTEGER,
    }
    assert filter_params(_trades(query_class=Query)) == {}


def test_bar_channels_and_prefixes_are_keyed_by_timespan() -> None:
    spec = _trades(
        channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
        flat_file_prefixes={Timespan.MINUTE: "us_stocks_sip/minute_aggs_v1"},
    )
    assert spec.channels[Timespan.MINUTE] == "AM"
