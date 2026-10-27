"""Tests for the dataset catalog and the SDK parameter table.

This file is also where the catalog's Massive coverage assertions will live.
"""

import dataclasses
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from deephaven_massive._datasets import catalog
from deephaven_massive._datasets.catalog import (
    SCHEMA_FROM_MODEL,
    SDK_PARAMS,
    Catalog,
    DatasetSpec,
    ParamKind,
    SymbolParam,
    check_filters,
    filter_params,
    sdk_params,
    unit_rules,
)
from deephaven_massive._datasets.schema import TRADES, ColumnOverride, ColumnType
from deephaven_massive._datasets.timestamps import TimeField, TimeUnit, UnitRule
from deephaven_massive._enums import AssetClass, Timespan, Transport

BASE = DatasetSpec(
    name="test.trades",
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


def test_the_catalog_holds_the_specs_by_name() -> None:
    first, second = _trades(), _trades(name="test.quotes")
    catalog = Catalog((first, second))
    assert list(catalog.by_name) == ["test.trades", "test.quotes"]
    assert catalog.by_name["test.quotes"] is second
    with pytest.raises(TypeError):
        cast("dict[str, DatasetSpec]", catalog.by_name)["test.other"] = first


def test_the_catalog_rejects_a_name_declared_twice() -> None:
    with pytest.raises(ValueError, match=r"dataset test\.trades is declared twice"):
        Catalog((_trades(), _trades()))


def test_unit_rules() -> None:
    rules = unit_rules({"test.trades": _trades()})
    assert rules[TimeField("test.trades", Transport.REST, "timestamp")] == UnitRule(
        TimeUnit.MILLISECONDS, unverified=True
    )


def test_a_spec_must_match_a_query_class() -> None:
    with pytest.raises(
        ValueError,
        match=r"^test\.trades: a dataset needs channels, a history source, or a fetch method$",
    ):
        _trades(channels={})
    with pytest.raises(
        ValueError, match=r"^test\.trades: a dataset with channels and a fetch method needs"
    ):
        _trades(fetch_method="get_last_trade")
    with pytest.raises(ValueError, match=r"^test\.trades: only bars combine a history source"):
        _trades(channels={}, fetch_method="get_last_trade", history_method="list_trades")
    with pytest.raises(ValueError, match=r"^test\.trades: a bars dataset needs a history source$"):
        _trades(is_bars=True, channels={Timespan.MINUTE: "AM"}, fetch_method="get_prev")
    # Flat files are a history source too.
    flat = _trades(
        is_bars=True,
        channels={},
        flat_file_prefixes={Timespan.MINUTE: "us_stocks_sip/minute_aggs_v1"},
        key_maps={Transport.FLAT_FILES: {"ticker": "Symbol", "sip_timestamp": "Timestamp"}},
        time_units={(Transport.FLAT_FILES, "sip_timestamp"): TimeUnit.NANOSECONDS},
        unverified_time_units=frozenset(),
    )
    assert flat.has_history
    assert not flat.has_channels
    assert not flat.has_fetch


def test_spec_validation() -> None:
    with pytest.raises(ValueError, match=r"must be '<namespace>\.<dataset>'"):
        _trades(name="trades")
    with pytest.raises(ValueError, match=r"unverified units \['WS:q'\]"):
        _trades(unverified_time_units=frozenset({(Transport.WS, "q")}))
    with pytest.raises(ValueError, match=r"poll_keys \['Ticker'\]"):
        _trades(poll_keys=("Ticker",))
    with pytest.raises(ValueError, match="give exactly one of schema and rest_model"):
        _trades(schema=SCHEMA_FROM_MODEL, channels={})
    with pytest.raises(ValueError, match="give exactly one of schema and rest_model"):
        _dividends(schema=TRADES.schema)  # a second, conflicting schema
    with pytest.raises(ValueError, match=r"test\.trades: a WebSocket dataset .* needs a schema"):
        _trades(schema=SCHEMA_FROM_MODEL, rest_model=_Dividend)
    with pytest.raises(ValueError, match=r"test\.trades: a WebSocket dataset .* needs a class"):
        _trades(asset_class=None)
    for name in ("trades.", ".trades"):  # an empty half of the name, on either side
        with pytest.raises(ValueError, match=r"must be '<namespace>\.<dataset>'"):
            _trades(name=name)


@dataclasses.dataclass
class _Dividend:
    # A stand-in SDK model: a REST dataset's schema is derived from such a class.
    ticker: str | None = None
    ex_dividend_date: str | None = None
    cash_amount: float | None = None
    last_updated: int | None = None


def _dividends(**changes: object) -> DatasetSpec:
    spec = DatasetSpec(
        name="stocks.dividends",
        asset_class=AssetClass.STOCKS,
        rest_model=_Dividend,
        fetch_method="list_stocks_dividends",
        overrides={"ticker": ColumnOverride(name="Symbol")},
        time_units={(Transport.REST, "last_updated"): TimeUnit.NANOSECONDS},
        symbol_param=SymbolParam.TICKER,
        per_symbol=True,
    )
    return dataclasses.replace(spec, **cast("dict[str, Any]", changes))


def test_a_rest_dataset_derives_its_schema_from_its_model() -> None:
    spec = _dividends()
    columns = {c.name: c.type for c in spec.schema.columns}
    assert columns == {
        "Symbol": ColumnType.STRING,
        "ExDividendDate": ColumnType.LOCAL_DATE,
        "CashAmount": ColumnType.DOUBLE,
        "LastUpdated": ColumnType.INSTANT,  # it has a REST time unit
    }
    assert spec.rest_columns == {
        "ticker": "Symbol",
        "ex_dividend_date": "ExDividendDate",
        "cash_amount": "CashAmount",
        "last_updated": "LastUpdated",
    }
    assert spec.symbol_param is SymbolParam.TICKER
    assert _trades().rest_columns == {}  # a hand-written schema has no model columns
    # The import-time checks now run for REST datasets too.
    with pytest.raises(ValueError, match=r"poll_keys \['Ticker'\]"):
        _dividends(poll_keys=("Ticker",))
    with pytest.raises(ValueError, match="has no fields"):
        _dividends(overrides={"nope": ColumnOverride(name="Nope")})


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


def test_sdk_params_block_is_generated_and_still_empty() -> None:
    source = Path(catalog.__file__).read_text()
    begin = source.index("# --- BEGIN GENERATED SDK_PARAMS")
    end = source.index("# --- END GENERATED SDK_PARAMS")
    assert "SDK_PARAMS: Final[" in source[begin:end]
    assert dict(SDK_PARAMS) == {}


def test_sdk_params_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(LookupError, match="refresh_massive_catalog"):
        sdk_params("list_trades")
    monkeypatch.setattr(
        catalog, "SDK_PARAMS", {"list_trades": {"timestamp_gte": ParamKind.DATETIME}}
    )
    assert sdk_params("list_trades") == {"timestamp_gte": ParamKind.DATETIME}


def test_filter_params_merge_methods_and_extras(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        catalog,
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
    assert filter_params(_trades()) == {}


def test_bar_channels_and_prefixes_are_keyed_by_timespan() -> None:
    spec = _trades(
        is_bars=True,
        channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
        flat_file_prefixes={Timespan.MINUTE: "us_stocks_sip/minute_aggs_v1"},
        key_maps=_FLAT_TRADES,
        time_units=_FLAT_UNITS,
    )
    assert spec.channels[Timespan.MINUTE] == "AM"
    with pytest.raises(ValueError, match="must be keyed by Timespan"):
        _trades(is_bars=True)
    with pytest.raises(ValueError, match="must be keyed by None"):
        _trades(flat_file_prefixes={Timespan.DAY: "us_stocks_sip/day_aggs_v1"})


_FLAT_TRADES = {Transport.FLAT_FILES: {"ticker": "Symbol", "sip_timestamp": "Timestamp"}}
_FLAT_UNITS = {**BASE.time_units, (Transport.FLAT_FILES, "sip_timestamp"): TimeUnit.NANOSECONDS}


def test_flat_files_need_a_key_map_for_symbol_and_timestamp() -> None:
    prefixes = {None: "us_stocks_sip/trades_v1"}
    spec = _trades(flat_file_prefixes=prefixes, key_maps=_FLAT_TRADES, time_units=_FLAT_UNITS)
    assert spec.flat_file_prefixes == prefixes
    with pytest.raises(ValueError, match=r"key map must fill \['Symbol', 'Timestamp'\]"):
        _trades(flat_file_prefixes=prefixes)
    with pytest.raises(ValueError, match=r"key map must fill \['Timestamp'\]"):
        _trades(flat_file_prefixes=prefixes, key_maps={Transport.FLAT_FILES: {"ticker": "Symbol"}})


def test_futures_flat_files_need_the_session_date() -> None:
    futures: dict[str, object] = {
        "asset_class": AssetClass.FUTURES,
        "flat_file_prefixes": {None: "us_futures_{venue}/trades_v1"},
    }
    with pytest.raises(ValueError, match=r"key map must fill \['SessionEndDate'\]"):
        _trades(key_maps=_FLAT_TRADES, time_units=_FLAT_UNITS, **futures)
    sessions = {**_FLAT_TRADES[Transport.FLAT_FILES], "session_end_date": "SessionEndDate"}
    spec = _trades(key_maps={Transport.FLAT_FILES: sessions}, time_units=_FLAT_UNITS, **futures)
    assert spec.key_maps[Transport.FLAT_FILES]["session_end_date"] == "SessionEndDate"


def test_filters_are_accepted_until_the_table_is_generated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _trades(history_method="list_trades")
    assert dict(SDK_PARAMS) == {}
    check_filters(spec, {"anything": 1})  # accepted: there is no table to check against yet
    monkeypatch.setattr(catalog, "SDK_PARAMS", {"list_trades": {"limit": ParamKind.INTEGER}})
    with pytest.raises(ValueError, match="has no filter 'anything'"):
        check_filters(spec, {"anything": 1})  # the same filter, once the table exists


def test_filters_are_checked_against_the_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        catalog,
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
    naive = datetime(2025, 1, 2, 9, 30, tzinfo=UTC).replace(tzinfo=None)  # naive on purpose
    with pytest.raises(
        ValueError, match=r"^timestamp_gte=datetime\.datetime\(2025, 1, 2, 9, 30\) needs a timezone"
    ):
        check_filters(spec, {"timestamp_gte": naive})


def test_date_filters_must_be_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(catalog, "SDK_PARAMS", {"list_trades": {"date": ParamKind.DATE}})
    spec = _trades(history_method="list_trades")
    check_filters(spec, {"date": "2025-01-02"})
    check_filters(spec, {"date": date(2025, 1, 2)})
    for bad in ("2025-01-02T00:00:00Z", "yesterday", datetime(2025, 1, 2, tzinfo=UTC)):
        with pytest.raises(ValueError, match="takes a date value"):
            check_filters(spec, {"date": bad})


def test_rest_capabilities() -> None:
    assert not BASE.has_rest_history
    assert not BASE.has_grouped_daily
    spec = _trades(history_method="list_trades", grouped_daily_method="get_grouped_daily_aggs")
    assert spec.has_rest_history
    assert spec.has_grouped_daily
