"""Tests for the dataset catalog and the SDK parameter table (DESIGN § 4.1, § 9).

This file is also where the catalog's Massive coverage assertions will live (DESIGN § 4.2).
"""

import dataclasses
import importlib
import pkgutil
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from deephaven_massive import _catalog, _domains
from deephaven_massive._catalog import (
    DOMAIN_MODULES,
    SDK_PARAMS,
    DatasetSpec,
    ParamKind,
    check_filters,
    datasets,
    filter_params,
    sdk_params,
    time_units,
    unit_rules,
)
from deephaven_massive._enums import AssetClass, Timespan, Transport
from deephaven_massive._query import Query, StreamHistoryLatestQuery
from deephaven_massive._schema import TRADES
from deephaven_massive._timeunits import TimeField, TimeUnit, UnitRule


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
    assert TimeField("test.trades", Transport.WS, "t") in table
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
    # A REST dataset may derive its schema later; a WebSocket dataset may not.
    assert _trades(schema=None, channels={}, poll_keys=("Ticker",)).schema is None
    with pytest.raises(ValueError, match=r"test\.trades: a WebSocket dataset .* needs a schema"):
        _trades(schema=None)


def test_every_instant_field_of_a_key_map_needs_a_time_unit() -> None:
    key_maps = {
        Transport.WS: {"sym": "Symbol", "t": "Timestamp", "p": "Price"},
        Transport.FLAT_FILES: {"ticker": "Symbol", "sip_timestamp": "Timestamp"},
    }
    with pytest.raises(ValueError, match=r"Instant fields \['FLAT_FILES:sip_timestamp'\]"):
        _trades(key_maps=key_maps)
    units = {**BASE.time_units, (Transport.FLAT_FILES, "sip_timestamp"): TimeUnit.NANOSECONDS}
    assert _trades(key_maps=key_maps, time_units=units).key_maps == key_maps


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
        is_bars=True,
        channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
        flat_file_prefixes={Timespan.MINUTE: "us_stocks_sip/minute_aggs_v1"},
    )
    assert spec.channels[Timespan.MINUTE] == "AM"
    with pytest.raises(ValueError, match="must be keyed by Timespan"):
        _trades(is_bars=True)
    with pytest.raises(ValueError, match="must be keyed by None"):
        _trades(flat_file_prefixes={Timespan.DAY: "us_stocks_sip/day_aggs_v1"})


def test_filters_are_accepted_until_the_table_is_generated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _trades(history_method="list_trades")
    assert dict(SDK_PARAMS) == {}
    check_filters(spec, {"anything": 1})  # accepted: there is no table to check against yet
    monkeypatch.setattr(_catalog, "SDK_PARAMS", {"list_trades": {"limit": ParamKind.INTEGER}})
    with pytest.raises(ValueError, match="has no filter 'anything'"):
        check_filters(spec, {"anything": 1})  # the same filter, once the table exists


def test_filters_are_checked_against_the_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        _catalog,
        "SDK_PARAMS",
        {
            "list_trades": {
                "timestamp_gte": ParamKind.DATETIME,
                "limit": ParamKind.INTEGER,
                "price_gt": ParamKind.NUMBER,
                "ticker_any_of": ParamKind.STRING_SEQUENCE,
                "otc": ParamKind.BOOLEAN,
                "date": ParamKind.DATE,
                "sort": ParamKind.STRING,
            }
        },
    )
    spec = _trades(history_method="list_trades")
    check_filters(
        spec,
        {
            "timestamp_gte": "2025-01-01",
            "limit": 5,
            "price_gt": 1,
            "ticker_any_of": ("A", "B"),
            "otc": False,
            "date": date(2025, 1, 1),
            "sort": "price",
        },
    )
    with pytest.raises(ValueError, match=r"has no filter 'nope'; valid filters: \['date'"):
        check_filters(spec, {"nope": 1})
    with pytest.raises(ValueError, match="takes a int value, not True"):
        check_filters(spec, {"limit": True})
    with pytest.raises(ValueError, match=r"takes a float value, not 'x'"):
        check_filters(spec, {"price_gt": "x"})
    with pytest.raises(ValueError, match="takes a str value"):
        check_filters(spec, {"sort": 1})


def test_date_filters_must_be_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_catalog, "SDK_PARAMS", {"list_trades": {"date": ParamKind.DATE}})
    spec = _trades(history_method="list_trades")
    check_filters(spec, {"date": "2025-01-02"})
    check_filters(spec, {"date": date(2025, 1, 2)})
    for bad in ("2025-01-02T00:00:00Z", "yesterday", datetime(2025, 1, 2, tzinfo=UTC)):
        with pytest.raises(ValueError, match="takes a date value"):
            check_filters(spec, {"date": bad})
