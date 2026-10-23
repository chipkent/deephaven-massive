"""Tests for the schema registry, SDK-model flattening, and the tick schemas (DESIGN § 2, § 11)."""

import dataclasses
import inspect
import re
from collections.abc import Callable
from datetime import date
from typing import Protocol, cast

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes
from massive.rest.models import Agg, LastTrade, Quote, TickerSnapshot, Trade
from massive.rest.models.contracts import OptionsContract
from massive.rest.models.snapshot import OptionContractSnapshot
from massive.websocket.models import EquityAgg, EquityQuote, EquityTrade, IndexValue

from deephaven_massive._errors import MassiveError
from deephaven_massive._schema import (
    BARS,
    INDEX_VALUES,
    QUOTES,
    TRADES,
    Column,
    ColumnOverride,
    ColumnType,
    Schema,
    TickSchema,
    flatten,
    model_schema,
    pascal_case,
)
from deephaven_massive._timeunits import Transport

ALL_TYPES = Schema("all", tuple(Column(t.name.title().replace("_", ""), t) for t in ColumnType))


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("sip_timestamp", "SipTimestamp"),
        ("last_trade.sip_timestamp", "LastTradeSipTimestamp"),
        ("id", "Id"),
        ("vwap", "Vwap"),
        ("filer_cik", "FilerCik"),
        ("underlying_asset.ticker", "UnderlyingAssetTicker"),
        ("alreadyCamel", "AlreadyCamel"),
    ],
)
def test_pascal_case(path: str, expected: str) -> None:
    assert pascal_case(path) == expected


def test_column_types_map_to_deephaven_and_arrow() -> None:
    assert ColumnType.INSTANT.dh_type == dtypes.Instant
    assert ColumnType.LOCAL_DATE.dh_type == dtypes.LocalDate
    assert ColumnType.INT_ARRAY.dh_type == dtypes.int32_array
    assert ColumnType.INSTANT.arrow_type == pa.timestamp("ns", tz="UTC")
    assert ColumnType.INT.arrow_type == pa.int32()
    assert {t.value for t in ColumnType} == {
        "Instant",
        "LocalDate",
        "double",
        "long",
        "int",
        "String",
        "boolean",
        "int[]",
    }


def test_schema_rejects_duplicate_columns() -> None:
    with pytest.raises(ValueError, match=r"repeats columns \['A'\]"):
        Schema("dup", (Column("A", ColumnType.INT), Column("A", ColumnType.LONG)))


def test_to_arrow_round_trips_through_deephaven(dh_exec_ctx: object) -> None:
    rows = [
        {
            "Instant": 1_700_000_000_000_000_000,
            "LocalDate": "2025-03-03",
            "Double": 1,
            "Long": 2,
            "Int": 3,
            "String": {"b": 1, "a": [2]},
            "Boolean": True,
            "IntArray": 7,
            "Ignored": "extra keys are ignored",
        },
        {"LocalDate": date(2025, 3, 4), "IntArray": [1, 2]},
    ]
    batch = ALL_TYPES.to_arrow(rows)
    assert batch.schema == ALL_TYPES.arrow_schema()
    table = dh_arrow.to_table(batch)
    assert {c.name: c.data_type for c in table.columns} == ALL_TYPES.col_defs()
    values = dh_arrow.to_arrow(table).to_pylist()
    assert values[0]["String"] == '{"a": [2], "b": 1}'
    assert values[0]["IntArray"] == [7]
    assert values[0]["LocalDate"] == date(2025, 3, 3)
    assert values[1]["Double"] is None
    assert values[1]["IntArray"] == [1, 2]
    assert ALL_TYPES.column_names[0] == "Instant"


def test_to_arrow_reports_schema_mismatches() -> None:
    with pytest.raises(MassiveError, match=r"schema mismatch in all\.Double \(expected double\)"):
        ALL_TYPES.to_arrow([{"Double": "x"}])
    with pytest.raises(MassiveError, match="LocalDate"):
        ALL_TYPES.to_arrow([{"LocalDate": "03/03/2025"}])


def test_flatten_nested_models() -> None:
    snapshot = TickerSnapshot.from_dict(
        {
            "ticker": "AAPL",
            "lastTrade": {"p": 189.5, "s": 100, "t": 1_700_000_000_000_000_000},
        }
    )
    flat = flatten(snapshot)
    assert flat["ticker"] == "AAPL"
    assert flat["last_trade.price"] == 189.5
    assert flat["last_trade.sip_timestamp"] == 1_700_000_000_000_000_000
    assert "day.open" not in flat


def test_flatten_rejects_non_models() -> None:
    with pytest.raises(ValueError, match="not an SDK model instance"):
        flatten({"ticker": "AAPL"})
    with pytest.raises(ValueError, match="not an SDK model instance"):
        flatten(Trade)


def test_model_schema_from_a_flat_model() -> None:
    derived = model_schema(
        "stocks.trades",
        Trade,
        instant_fields=("sip_timestamp", "participant_timestamp", "trf_timestamp"),
        overrides={"exchange": ColumnOverride(type=ColumnType.INT)},
    )
    columns = {c.name: c.type for c in derived.schema.columns}
    assert columns["Conditions"] is ColumnType.INT_ARRAY
    assert columns["SipTimestamp"] is ColumnType.INSTANT
    assert columns["Exchange"] is ColumnType.INT
    assert columns["Size"] is ColumnType.DOUBLE
    assert columns["SequenceNumber"] is ColumnType.LONG
    assert columns["DecimalSize"] is ColumnType.STRING
    assert derived.schema.column_names[0] == "Conditions"
    row = derived.row(Trade(price=1.5, size=10.0, exchange=4))
    assert row["Price"] == 1.5
    assert row["Exchange"] == 4
    assert row["Id"] is None


def test_model_schema_flattens_nested_models_and_maps_dates() -> None:
    derived = model_schema("options.snapshots", OptionContractSnapshot)
    names = derived.schema.column_names
    assert "DetailsStrikePrice" in names
    assert "GreeksDelta" in names
    assert "UnderlyingAssetTicker" in names
    assert derived.columns_by_path["last_quote.midpoint"] == "LastQuoteMidpoint"
    columns = {c.name: c.type for c in derived.schema.columns}
    assert columns["DetailsExpirationDate"] is ColumnType.LOCAL_DATE
    assert columns["LastTradeConditions"] is ColumnType.INT_ARRAY


def test_overrides_rename_and_drop() -> None:
    derived = model_schema(
        "stocks.last_trade",
        LastTrade,
        overrides={
            "ticker": ColumnOverride(name="Symbol"),
            "fractional_size": ColumnOverride(drop=True),
        },
    )
    assert "Symbol" in derived.schema.column_names
    assert "FractionalSize" not in derived.schema.column_names
    assert "fractional_size" not in derived.columns_by_path


def test_unmapped_hints_need_an_override() -> None:
    with pytest.raises(TypeError, match=r"'additional_underlyings' .* declare a ColumnOverride"):
        model_schema("options.contracts", OptionsContract)
    derived = model_schema(
        "options.contracts",
        OptionsContract,
        overrides={"additional_underlyings": ColumnOverride(type=ColumnType.STRING)},
    )
    assert "AdditionalUnderlyings" in derived.schema.column_names


def test_model_schema_validation() -> None:
    with pytest.raises(ValueError, match="not an SDK model class"):
        model_schema("x.y", dict)
    with pytest.raises(ValueError, match=r"Agg has no fields \['nope'\]"):
        model_schema("x.y", Agg, instant_fields=("nope",))
    with pytest.raises(ValueError, match="no fields"):
        model_schema("x.y", Agg, overrides={"missing": ColumnOverride(drop=True)})


def test_union_hints_need_an_override() -> None:
    @dataclasses.dataclass
    class Weird:
        value: int | str | None = None

    with pytest.raises(TypeError, match="no column mapping"):
        model_schema("x.y", Weird)


def test_non_optional_hints_map_directly() -> None:
    @dataclasses.dataclass
    class Plain:
        count: int = 0
        name: str = ""

    columns = {c.name: c.type for c in model_schema("x.y", Plain).schema.columns}
    assert columns == {"Count": ColumnType.LONG, "Name": ColumnType.STRING}


@pytest.mark.parametrize("tick", [TRADES, QUOTES, BARS, INDEX_VALUES])
def test_tick_schemas_lead_with_symbol_and_timestamp(tick: TickSchema) -> None:
    assert tick.schema.column_names[:2] == ("Symbol", "Timestamp")


class _WsModel(Protocol):
    from_dict: Callable[..., object]


def _ws_keys(model: _WsModel) -> set[str]:
    # The single-letter keys an SDK WebSocket model's from_dict reads, minus the event type.
    source = inspect.getsource(model.from_dict)
    keys = set(re.findall(r'd\.get\("([^"]+)"', source))
    return keys - {"ev"}


@pytest.mark.parametrize(
    ("tick", "model", "unmapped"),
    [
        (TRADES, EquityTrade, set()),
        (QUOTES, EquityQuote, set()),
        (BARS, EquityAgg, {"dv", "dav"}),
        (INDEX_VALUES, IndexValue, set()),
    ],
)
def test_ws_key_maps_cover_the_sdk_models(
    tick: TickSchema, model: _WsModel, unmapped: set[str]
) -> None:
    assert set(tick.key_maps[Transport.WS]) == _ws_keys(model) - unmapped


@pytest.mark.parametrize(
    ("tick", "model"),
    [
        (TRADES, Trade),
        (QUOTES, Quote),
        (BARS, Agg),
    ],
)
def test_rest_key_maps_cover_the_sdk_models(tick: TickSchema, model: type) -> None:
    assert set(tick.key_maps[Transport.REST]) == {f.name for f in dataclasses.fields(model)}


@pytest.mark.parametrize(
    ("tick", "headers"),
    [
        (
            TRADES,
            (
                "conditions,correction,exchange,id,participant_timestamp,price,sequence_number,"
                "sip_timestamp,size,tape,ticker,trf_id,trf_timestamp"
            ),
        ),
        (
            QUOTES,
            (
                "ask_exchange,ask_price,ask_size,bid_exchange,bid_price,bid_size,conditions,"
                "indicators,participant_timestamp,sequence_number,sip_timestamp,tape,ticker,"
                "trf_timestamp"
            ),
        ),
        (BARS, "ticker,volume,open,close,high,low,window_start,transactions"),
        (INDEX_VALUES, "ticker,timestamp,value"),
    ],
)
def test_flat_file_key_maps_cover_the_documented_headers(tick: TickSchema, headers: str) -> None:
    assert set(tick.key_maps[Transport.FLAT_FILES]) == set(headers.split(","))


def test_tick_schema_key_maps_must_target_columns() -> None:
    with pytest.raises(ValueError, match=r"WS key map targets unknown \['Nope'\]"):
        TickSchema(TRADES.schema, {Transport.WS: {"x": "Nope"}})


def test_tick_rows_fill_the_schema(dh_exec_ctx: object) -> None:
    frame = {"ev": "T", "sym": "AAPL", "p": 1.5, "s": 100, "c": [12], "t": 1_700_000_000_000}
    key_map = TRADES.key_maps[Transport.WS]
    row = {key_map[k]: v for k, v in frame.items() if k in key_map}
    row["Timestamp"] = cast("int", row["Timestamp"]) * 1_000_000
    table = dh_arrow.to_table(TRADES.schema.to_arrow([row]))
    assert {c.name: c.data_type for c in table.columns} == TRADES.schema.col_defs()
