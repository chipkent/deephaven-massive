"""Tests for the column types, `Schema`, the key rename, and the merge of a kind's sources."""

import dataclasses
from datetime import UTC, datetime
from typing import cast

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import merge

from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._datasets.schema import (
    COLUMN_TYPES,
    JSON_TEXT_NOTE,
    KEY_COLUMNS,
    TRANSPORT_LABELS,
    Column,
    ColumnType,
    Schema,
    SourceColumn,
    dataset_schema,
    key_column,
    pascal_case,
    rename,
    required_fields,
    widen,
)
from deephaven_massive._datasets.timestamps import TimeUnit
from deephaven_massive._enums import AssetClass, ClassFromPrefix, NotBars, Timespan
from deephaven_massive._errors import MassiveError
from deephaven_massive._massive import Field, Source, Transport, WireType, WireUnit, source
from deephaven_massive._namespaces import crypto, forex, futures, indicators, indices, stocks
from deephaven_massive._session import CATALOG
from deephaven_massive._tables.columns import col_defs

R, W, F = Transport.REST, Transport.WS, Transport.FLAT_FILES
ALL_TYPES = Schema("all", tuple(Column(t.name.title().replace("_", ""), t) for t in ColumnType))
# The wire interface names `Source.memberships` but exports no constructor for an element.
MEMBERSHIP = type(source(W, "stocks/T").memberships[0])


# --- Column types and Schema -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("sip_timestamp", "SipTimestamp"),
        ("last_trade.sip_timestamp", "LastTradeSipTimestamp"),
        ("id", "Id"),
        ("filer_cik", "FilerCik"),
        ("alreadyCamel", "AlreadyCamel"),
    ],
)
def test_pascal_case(path: str, expected: str) -> None:
    assert pascal_case(path) == expected


def test_column_types_map_to_arrow() -> None:
    assert ColumnType.INSTANT.arrow_type == pa.timestamp("ns", tz="UTC")
    assert ColumnType.INT.arrow_type == pa.int32()
    assert ColumnType.LONG_ARRAY.arrow_type == pa.list_(pa.int64())
    assert ColumnType.STRING_ARRAY.arrow_type == pa.list_(pa.string())
    assert {t.value for t in ColumnType} == {
        "Instant",
        "double",
        "long",
        "int",
        "String",
        "boolean",
        "long[]",
        "String[]",
    }


def test_every_wire_type_has_a_column_type() -> None:
    assert set(COLUMN_TYPES) == set(WireType)
    assert COLUMN_TYPES[WireType.EPOCH] is COLUMN_TYPES[WireType.ISO_INSTANT] is ColumnType.INSTANT
    assert COLUMN_TYPES[WireType.JSON_TEXT] is ColumnType.STRING
    assert COLUMN_TYPES[WireType.BOOL] is ColumnType.BOOLEAN
    # Every calendar date is ISO yyyy-MM-dd text.
    assert COLUMN_TYPES[WireType.DATE] is ColumnType.STRING
    assert COLUMN_TYPES[WireType.LONG_ARRAY] is ColumnType.LONG_ARRAY
    assert COLUMN_TYPES[WireType.STRING_ARRAY] is ColumnType.STRING_ARRAY
    # A dataset column is never a 32-bit int: exchange and tape ids are long.
    assert ColumnType.INT not in COLUMN_TYPES.values()


def test_schema_rejects_duplicate_columns() -> None:
    with pytest.raises(ValueError, match=r"repeats columns \['A'\]"):
        Schema("dup", (Column("A", ColumnType.INT), Column("A", ColumnType.LONG)))


def test_to_arrow_round_trips_through_deephaven(dh_exec_ctx: object) -> None:
    rows: list[dict[str, object]] = [
        {
            "Instant": 1_700_000_000_000_000_000,
            "Double": 1,
            "Long": 2,
            "Int": 3,
            "String": {"b": 1, "a": [2]},
            "Boolean": True,
            "LongArray": 7,
            "StringArray": ["AAPL", "BRK.B"],
            "Ignored": "extra keys are ignored",
        },
        {"LongArray": [1, 2**40], "StringArray": []},
    ]
    batch = ALL_TYPES.to_arrow(rows)
    assert batch.schema == ALL_TYPES.arrow_schema()
    table = dh_arrow.to_table(batch)
    assert {c.name: c.data_type for c in table.columns} == col_defs(ALL_TYPES)
    values = dh_arrow.to_arrow(table).to_pylist()
    assert values[0]["String"] == '{"a": [2], "b": 1}'
    assert values[0]["LongArray"] == [7]
    assert values[0]["StringArray"] == ["AAPL", "BRK.B"]
    assert values[1]["Double"] is None
    assert values[1]["LongArray"] == [1, 2**40]
    assert values[1]["StringArray"] == []
    assert ALL_TYPES.column_names[0] == "Instant"


def test_to_arrow_reports_schema_mismatches() -> None:
    with pytest.raises(MassiveError, match=r"schema mismatch in all\.Double \(expected double\)"):
        ALL_TYPES.to_arrow([{"Double": "x"}])
    with pytest.raises(MassiveError, match=r"schema mismatch in all\.String"):
        ALL_TYPES.to_arrow([{"String": {"a": {1, 2}}}])  # JSON cannot encode a set


def test_to_arrow_never_truncates_floats_in_integer_columns() -> None:
    for column in ("Long", "Int"):
        with pytest.raises(MassiveError, match=rf"schema mismatch in all\.{column}.*7140\.08"):
            ALL_TYPES.to_arrow([{column: 7140.08}])
        with pytest.raises(MassiveError, match=rf"schema mismatch in all\.{column}"):
            ALL_TYPES.to_arrow([{column: float("nan")}])
    values = ALL_TYPES.to_arrow([{"Long": 7140.0, "Int": 3.0}]).to_pylist()[0]
    assert (values["Long"], values["Int"]) == (7140, 3)


def test_to_arrow_reports_integers_beyond_the_column_range() -> None:
    for value in (2**63, -(2**63) - 1, 1e20):
        with pytest.raises(MassiveError, match=r"schema mismatch in all\.Long"):
            ALL_TYPES.to_arrow([{"Long": value}])


def test_to_arrow_never_truncates_floats_in_integer_lists() -> None:
    with pytest.raises(MassiveError, match=r"schema mismatch in all\.LongArray.*7140\.08"):
        ALL_TYPES.to_arrow([{"LongArray": [7140.08]}])
    values = ALL_TYPES.to_arrow([{"LongArray": [1.0, None, 3]}]).column("LongArray")
    assert values.to_pylist() == [[1, None, 3]]


def test_to_arrow_reads_a_naive_datetime_as_utc() -> None:
    aware = datetime(2025, 3, 3, 14, 30, tzinfo=UTC)
    naive = aware.replace(tzinfo=None)  # the same wall-clock time, without a timezone
    values = ALL_TYPES.to_arrow([{"Instant": naive}, {"Instant": aware}]).column("Instant")
    assert values[0] == values[1]


def test_iso_text_becomes_an_instant_to_the_nanosecond() -> None:
    schema = Schema("x", (Column("At", ColumnType.INSTANT),))
    batch = schema.to_arrow(
        [
            {"At": "2024-06-24T18:33:53.123456789Z"},
            {"At": "2024-06-24T14:33:53.000000001-04:00"},
            {"At": "2024-06-24T18:33:53.5"},
            {"At": "2024-06-24T18:33:53.123456789999+00:00"},  # beyond ns: truncated
            {"At": "2024-06-24T18:33:53Z"},
        ]
    )
    assert batch.column("At").cast(pa.int64()).to_pylist() == [
        1719254033123456789,
        1719254033000000001,
        1719254033500000000,
        1719254033123456789,
        1719254033000000000,
    ]
    with pytest.raises(MassiveError, match=r"schema mismatch in x\.At"):
        schema.to_arrow([{"At": "not a time"}])


# --- The lattice -----------------------------------------------------------------------------


def test_widening_a_large_integer_to_a_double_rounds_it() -> None:
    big = pa.array([2**53 + 1, None], pa.int64())
    assert widen(big, pa.float64()).to_pylist() == [float(2**53), None]


def test_widening_gives_each_column_its_merged_arrow_type() -> None:
    # Checked on the arrays, before a batch is assembled, which on some pyarrow versions casts
    # by itself.
    longs = pa.array([100, None], pa.int64())
    assert widen(longs, pa.float64()).type == pa.float64()
    assert widen(longs, pa.list_(pa.int32())).to_pylist() == [[100], None]
    lists = pa.array([[1], None], pa.list_(pa.int32()))
    assert widen(lists, pa.list_(pa.int32())).to_pylist() == [[1], None]


# --- The rename ------------------------------------------------------------------------------


def spec(
    name: str,
    one: dict[tuple[Transport, str], SourceKeys],
    *,
    asset_class: AssetClass,
    kind: str | None = None,
) -> DatasetSpec:
    # A dataset of one REST path or one channel, with that source's keys.
    ((transport, key),) = one
    return DatasetSpec(
        name=name,
        summary="A dataset for the tests.",
        asset_class=asset_class,
        kind=kind,
        fetch_path=key if transport is R else None,
        channels={NotBars.ANY: key.split("/")[1]} if transport is W else {},
        keys=one,
    )


def test_rename_renames_only_the_declared_keys() -> None:
    nbbo = (R, "/v2/last/nbbo/{stocksTicker}")
    quotes = spec(
        "stocks.quotes",
        {nbbo: SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
        asset_class=AssetClass.STOCKS,
    )
    columns = rename(quotes, source(*nbbo))
    names = [c.column for c in columns]
    assert names[0] == "Symbol"
    assert "Timestamp" in names
    assert {"Ticker", "SipTimestamp"}.isdisjoint(names)
    assert "ParticipantTimestamp" in names  # every other field keeps its generated column
    time = next(c for c in columns if c.column == "Timestamp")
    assert time.unit is TimeUnit.NANOSECONDS
    assert time.field is not None
    assert time.field.wire_key == "t"
    assert next(c for c in columns if c.column == "AskPrice").unit is None


def test_a_key_from_the_request_comes_first_as_text() -> None:
    trades = (R, "/v3/trades/{stockTicker}")
    history = spec(
        "stocks.trades",
        {trades: SourceKeys(KeySpec("stockTicker", from_request=True), KeySpec("SipTimestamp"))},
        asset_class=AssetClass.STOCKS,
    )
    first = rename(history, source(*trades))[0]
    assert first == SourceColumn("Symbol", WireType.STRING, parameter="stockTicker")


def test_the_date_key_becomes_the_date_column() -> None:
    files = (F, "us_futures_{venue}/trades_v1")
    futures = DatasetSpec(
        name="futures.trades",
        summary="A dataset for the tests.",
        asset_class=AssetClass.FUTURES,
        row_times=RowTimes.TICK,
        flat_file_prefixes={NotBars.ANY: files[1]},
        keys={
            files: SourceKeys(
                KeySpec("Ticker"), KeySpec("Timestamp"), date=KeySpec("SessionEndDate")
            )
        },
    )
    columns = {c.column: c for c in rename(futures, source(*files))}
    assert {"Symbol", "Timestamp", "Date"} <= set(columns)
    assert "SessionEndDate" not in columns  # the vendor's column is not kept beside it
    assert cast("Field", columns["Date"].field).wire_key == "session_end_date"
    assert KEY_COLUMNS == {
        "symbol": "Symbol",
        "time": "Timestamp",
        "end": "TimestampEnd",
        "fanout": "Underlying",
        "date": "Date",
    }


def test_a_bar_takes_its_start_and_its_end_from_its_keys() -> None:
    channel = (W, "stocks/AM")
    keys = SourceKeys(KeySpec("Ticker"), KeySpec("WindowStart"), end=KeySpec("EndTimestamp"))
    bars = DatasetSpec(
        name="stocks.bars",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        is_bars=True,
        history_path="/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}",
        channels={Timespan.MINUTE: "AM"},
        keys={channel: keys},
    )
    names = [c.column for c in rename(bars, source(*channel))]
    assert {"TimestampStart", "TimestampEnd"} <= set(names)
    assert {"Timestamp", "WindowStart", "EndTimestamp"}.isdisjoint(names)
    assert [f.wire_key for f in required_fields(bars, source(*channel))] == ["sym", "s", "e"]


def test_rename_refuses_an_end_or_date_key_of_the_wrong_type() -> None:
    channel = (W, "stocks/AM")
    bars = DatasetSpec(
        name="stocks.bars",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        is_bars=True,
        history_path="/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}",
        channels={Timespan.MINUTE: "AM"},
        keys={channel: SourceKeys(KeySpec("Ticker"), KeySpec("WindowStart"), end=KeySpec("Open"))},
    )
    with pytest.raises(ValueError, match=r"the end key 'Open' is double, not an instant"):
        rename(bars, source(*channel))
    dated = dataclasses.replace(
        bars,
        keys={
            channel: SourceKeys(KeySpec("Ticker"), KeySpec("WindowStart"), date=KeySpec("Close"))
        },
    )
    with pytest.raises(ValueError, match=r"the date key 'Close' is double, not a date"):
        rename(dated, source(*channel))


def test_a_projection_keeps_and_names_the_member_fields() -> None:
    last_quote = (R, "/v1/last_quote/currencies/{from}/{to}")
    forex = spec(
        "forex.quotes",
        {last_quote: SourceKeys(KeySpec("from", from_request=True), KeySpec("Timestamp"))},
        asset_class=AssetClass.FOREX,
        kind="quotes",
    )
    names = [c.column for c in rename(forex, source(*last_quote))]
    # The forex last quote's `ask` and `bid` join the quotes kind as AskPrice and BidPrice.
    assert names == ["Symbol", "AskPrice", "BidPrice", "Exchange", "Timestamp"]
    # Outside the kind, the same source keeps its own names.
    alone = spec("forex.last", {last_quote: SourceKeys()}, asset_class=AssetClass.FOREX)
    assert [c.column for c in rename(alone, source(*last_quote))] == [
        "Ask",
        "Bid",
        "Exchange",
        "Timestamp",
    ]


def test_a_projection_belongs_to_the_dataset_group() -> None:
    # One source in two groups of one kind, projected differently in each.
    shared = Source(
        key="/shared",
        transport=R,
        fields=(
            Field(wire_key="ticker", column="Ticker", wire_type=WireType.STRING),
            Field(wire_key="ask", column="Ask", wire_type=WireType.DOUBLE),
        ),
        memberships=(
            MEMBERSHIP(kind="quotes", group="stocks", position=0, projection={"ask": "AskPrice"}),
            MEMBERSHIP(
                kind="quotes",
                group="forex",
                position=0,
                projection={"ticker": "Ticker", "ask": "Offer"},
            ),
        ),
    )
    forex = spec(
        "forex.quotes",
        {(R, "/shared"): SourceKeys(KeySpec("Ticker"))},
        asset_class=AssetClass.FOREX,
        kind="quotes",
    )
    assert [c.column for c in rename(forex, shared)] == ["Symbol", "Offer"]


def test_required_keys_resolve_to_the_fields_behind_them() -> None:
    # A source that joins the values kind with a few of its fields, `last_updated` as Timestamp.
    snapshot = Source(
        key="/snapshot",
        transport=R,
        fields=(
            Field(wire_key="ticker", column="Ticker", wire_type=WireType.STRING),
            Field(wire_key="value", column="Value", wire_type=WireType.DOUBLE),
            Field(
                wire_key="last_updated",
                column="LastUpdated",
                wire_type=WireType.EPOCH,
                unit=WireUnit.NS,
            ),
        ),
        memberships=(
            MEMBERSHIP(
                kind="values",
                group="indices",
                position=0,
                projection={"ticker": "Ticker", "last_updated": "Timestamp"},
            ),
        ),
    )
    values = spec(
        "indices.values",
        {(R, "/snapshot"): SourceKeys(KeySpec("Ticker"), KeySpec("Timestamp"))},
        asset_class=AssetClass.INDICES,
        kind="values",
    )
    # The kind's Timestamp is the source's own LastUpdated, which the wire client decodes.
    fields = required_fields(values, snapshot)
    assert [(f.wire_key, f.column) for f in fields] == [
        ("ticker", "Ticker"),
        ("last_updated", "LastUpdated"),
    ]
    # A key filled from the request is no column of the rows, so it is never required.
    files = (F, "us_futures_{venue}/trades_v1")
    trades = (R, "/futures/v1/trades/{ticker}")
    futures = DatasetSpec(
        name="futures.trades",
        summary="A dataset for the tests.",
        asset_class=AssetClass.FUTURES,
        row_times=RowTimes.TICK,
        history_path=trades[1],
        flat_file_prefixes={NotBars.ANY: files[1]},
        keys={
            trades: SourceKeys(
                KeySpec("ticker", from_request=True),
                KeySpec("Timestamp"),
                date=KeySpec("SessionEndDate"),
            ),
            files: SourceKeys(
                KeySpec("Ticker"), KeySpec("Timestamp"), date=KeySpec("SessionEndDate")
            ),
        },
    )
    assert [f.wire_key for f in required_fields(futures, source(*trades))] == [
        "timestamp",
        "session_end_date",
    ]
    assert [f.wire_key for f in required_fields(futures, source(*files))] == [
        "ticker",
        "timestamp",
        "session_end_date",
    ]


def test_the_fmv_channel_takes_the_magnitude_rule() -> None:
    fmv = (W, "stocks/FMV")
    stream = spec(
        "stocks.fmv",
        {fmv: SourceKeys(KeySpec("Ticker"), KeySpec("FmvLastUpdated"))},
        asset_class=AssetClass.STOCKS,
    )
    time = next(c for c in rename(stream, source(*fmv)) if c.column == "Timestamp")
    assert time.unit is TimeUnit.FMV_MAGNITUDE


def test_rename_refuses_a_key_the_source_lacks() -> None:
    trades = (W, "stocks/T")
    lacking = spec(
        "stocks.trades",
        {trades: SourceKeys(KeySpec("Symbol"))},
        asset_class=AssetClass.STOCKS,
    )
    with pytest.raises(
        ValueError,
        match=r"^declared keys: stocks\.trades: WS stocks/T: the symbol key 'Symbol' is not",
    ):
        rename(lacking, source(*trades))


def test_rename_refuses_two_keys_on_one_column() -> None:
    contracts = (R, "/v3/reference/options/contracts")
    overlapping = spec(
        "options.contracts",
        {contracts: SourceKeys(KeySpec("Ticker"), fanout=KeySpec("Ticker"))},
        asset_class=AssetClass.OPTIONS,
    )
    message = (
        r"^declared keys: options\.contracts: REST /v3/reference/options/contracts: the fanout "
        r"key 'Ticker' is already the symbol key$"
    )
    with pytest.raises(ValueError, match=message):
        rename(overlapping, source(*contracts))
    # The gate reports it, and nothing raises past it.
    assert Catalog((overlapping,)).problems() == (message[1:-1].replace("\\", ""),)


def test_rename_refuses_a_repeated_column() -> None:
    open_close = (R, "/v1/open-close/{stocksTicker}/{date}")
    dup = spec(
        "stocks.daily_open_close",
        {open_close: SourceKeys(KeySpec("stocksTicker", from_request=True))},
        asset_class=AssetClass.STOCKS,
    )
    # The open-close rows already carry a Symbol column of their own.
    with pytest.raises(
        ValueError,
        match=r"^unique columns: stocks\.daily_open_close: REST /v1/open-close/\{stocksTicker\}"
        r"/\{date\}: columns \['Symbol'\] repeat after the rename$",
    ):
        rename(dup, source(*open_close))


def test_the_column_of_each_key() -> None:
    ticks, bars = stocks.TRADES, stocks.BARS
    assert key_column(ticks, "symbol", KeySpec("Ticker")) == "Symbol"
    assert key_column(ticks, "time", KeySpec("SipTimestamp")) == "Timestamp"
    assert key_column(bars, "time", KeySpec("WindowStart")) == "TimestampStart"
    assert key_column(bars, "end", KeySpec("EndTimestamp")) == "TimestampEnd"
    assert key_column(ticks, "fanout", KeySpec("UnderlyingTicker")) == "Underlying"
    assert key_column(ticks, "fanout", KeySpec("FilerCik", instrument=False)) == "FilerCik"
    assert key_column(ticks, "date", KeySpec("SessionEndDate")) == "Date"


def test_a_fanout_key_that_is_no_instrument_keeps_its_column_and_is_required() -> None:
    holdings = (R, "/stocks/filings/vX/13-F")
    filings = DatasetSpec(
        name="stocks.institutional_holdings",
        summary="A dataset for the tests.",
        fetch_path=holdings[1],
        keys={holdings: SourceKeys(fanout=KeySpec("FilerCik", instrument=False))},
    )
    names = [c.column for c in rename(filings, source(*holdings))]
    assert "FilerCik" in names
    assert "Underlying" not in names
    assert [f.wire_key for f in required_fields(filings, source(*holdings))] == ["filer_cik"]


def test_an_optional_symbol_key_is_not_required() -> None:
    index = (R, "/stocks/filings/vX/index")
    filings = DatasetSpec(
        name="stocks.filings",
        summary="A dataset for the tests.",
        fetch_path=index[1],
        keys={index: SourceKeys(symbol=KeySpec("Ticker", required=False))},
        poll_keys=("AccessionNumber",),
    )
    assert "Symbol" in [c.column for c in rename(filings, source(*index))]
    assert required_fields(filings, source(*index)) == ()
    required = dataclasses.replace(filings, keys={index: SourceKeys(symbol=KeySpec("Ticker"))})
    assert [f.wire_key for f in required_fields(required, source(*index))] == ["ticker"]


def test_a_fanout_key_from_the_request_precedes_the_fields() -> None:
    trades = (W, "stocks/T")
    fanout = spec(
        "stocks.trades",
        {trades: SourceKeys(KeySpec("Ticker"), fanout=KeySpec("x", from_request=True))},
        asset_class=AssetClass.STOCKS,
    )
    names = [c.column for c in rename(fanout, source(*trades))]
    assert names[:2] == ["Underlying", "Symbol"]


# --- The merge -------------------------------------------------------------------------------


def lookup(transport: Transport, key: str) -> Source:
    return source(transport, key)


def test_the_merge_puts_the_declared_keys_first() -> None:
    contracts = (R, "/v3/reference/options/contracts")
    dataset = spec(
        "options.contracts",
        {contracts: SourceKeys(KeySpec("Ticker"), fanout=KeySpec("UnderlyingTicker"))},
        asset_class=AssetClass.OPTIONS,
    )
    schema = dataset_schema(dataset, (dataset,), lookup)
    names = schema.column_names
    assert names[:4] == ("Symbol", "OptionRoot", "AdditionalUnderlyings", "Cfi")
    assert schema.name == "options.contracts"
    # Only Symbol leads, then the derived columns; Underlying keeps its field's place in the
    # source, and the arrival time comes last.
    fields = [f.column for f in source(*contracts).fields if f.column != "Ticker"]
    assert names[2:] == (
        *("Underlying" if c == "UnderlyingTicker" else c for c in fields),
        "TimestampInsert",
    )


def test_derived_columns_follow_the_leading_keys() -> None:
    trades = (R, "/v3/trades/{optionsTicker}")
    rows = (R, "/v3/reference/options/contracts")
    by_request = spec(
        "options.trades",
        {trades: SourceKeys(KeySpec("optionsTicker", from_request=True), KeySpec("SipTimestamp"))},
        asset_class=AssetClass.OPTIONS,
    )
    from_rows = spec(
        "options.contracts",
        {rows: SourceKeys(KeySpec("Ticker"))},
        asset_class=AssetClass.OPTIONS,
    )
    plain = dataset_schema(by_request, (by_request,), lookup)
    assert plain.column_names[:3] == ("Symbol", "Timestamp", "OptionRoot")
    root = plain.columns[2]
    assert root.type is ColumnType.STRING
    assert root.description.startswith("The OCC option root, read from `Symbol`")
    assert root.supplied_by == plain.columns[0].supplied_by
    assert root.wire_keys == plain.columns[0].wire_keys
    # RawSymbol exists only where a symbol is read from rows, and only when asked for.
    assert dataset_schema(by_request, (by_request,), lookup, raw_symbol=True) == plain
    kept = dataset_schema(from_rows, (from_rows,), lookup, raw_symbol=True)
    assert kept.column_names[:3] == ("Symbol", "OptionRoot", "RawSymbol")
    assert kept.columns[2].description.endswith(
        "Present unless the session's `unreadable_symbols` is `dm.OnError.RAISE`, which fails on "
        "such a row instead."
    )
    assert "RawSymbol" not in dataset_schema(from_rows, (from_rows,), lookup).column_names


def test_only_options_datasets_have_an_option_root() -> None:
    quotes = (R, "/v3/quotes/{stockTicker}")
    stocks = spec(
        "stocks.quotes",
        {quotes: SourceKeys(KeySpec("stockTicker", from_request=True), KeySpec("SipTimestamp"))},
        asset_class=AssetClass.STOCKS,
    )
    assert "OptionRoot" not in dataset_schema(stocks, (stocks,), lookup).column_names
    unkeyed = DatasetSpec(
        name="options.contracts",
        summary="A dataset for the tests.",
        asset_class=AssetClass.OPTIONS,
        fetch_path="/v3/reference/options/contracts",
    )
    names = dataset_schema(unkeyed, (unkeyed,), lookup, raw_symbol=True).column_names
    assert "OptionRoot" not in names
    assert "RawSymbol" not in names


def test_the_merge_follows_the_membership_positions() -> None:
    # The stocks trades group puts the history path (position 0) before the last-trade path
    # (position 1) and the stream (position 2), whichever dataset of the group reads each.
    sip = SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))
    history = (R, "/v3/trades/{stockTicker}")
    last = (R, "/v2/last/trade/{stocksTicker}")
    trades = DatasetSpec(
        name="stocks.trades",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        kind="trades",
        history_path=history[1],
        channels={NotBars.ANY: "T"},
        keys={
            history: SourceKeys(KeySpec("stockTicker", from_request=True), sip.time),
            (W, "stocks/T"): sip,
        },
    )
    last_trade = DatasetSpec(
        name="stocks.last_trade",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        kind="trades",
        fetch_path=last[1],
        keys={last: sip},
    )
    made = Source(
        key=last[1],
        transport=R,
        fields=(
            Field(wire_key="T", column="Ticker", wire_type=WireType.STRING),
            Field(wire_key="z", column="Zeta", wire_type=WireType.LONG),
            Field(wire_key="t", column="SipTimestamp", wire_type=WireType.EPOCH, unit=WireUnit.NS),
        ),
        memberships=(MEMBERSHIP(kind="trades", group="stocks", position=1),),
    )

    def tables(transport: Transport, key: str) -> Source:
        return made if key == last[1] else source(transport, key)

    names = dataset_schema(trades, (trades, last_trade), tables).column_names
    # Only the last-trade path fills Zeta, so the trades dataset has none of it.
    assert names[:3] == ("Symbol", "Date", "Timestamp")
    assert "Zeta" not in names
    last_names = dataset_schema(last_trade, (trades, last_trade), tables).column_names
    assert last_names == ("Symbol", "Date", "Timestamp", "Zeta", "TimestampInsert")
    # The group's sources keep their positions whichever member comes first, so each dataset
    # of the group has the same columns in the same order.
    assert dataset_schema(trades, (last_trade, trades), tables).column_names == names
    assert dataset_schema(last_trade, (last_trade, trades), tables).column_names == last_names


def test_the_merge_joins_types_across_sources() -> None:
    # The quote stream sends one condition code as an integer; REST sends a list.
    nbbo, stream = (R, "/v2/last/nbbo/{stocksTicker}"), (W, "stocks/Q")
    both = SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))
    quotes = DatasetSpec(
        name="stocks.quotes",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        history_path="/v3/quotes/{stockTicker}",
        channels={NotBars.ANY: "Q"},
        keys={stream: both},
    )
    last = spec("stocks.last_quote", {nbbo: both}, asset_class=AssetClass.STOCKS)
    schema = dataset_schema(quotes, (quotes, last), lookup)
    types = {c.name: c.type for c in schema.columns}
    assert types["Conditions"] is ColumnType.LONG_ARRAY
    assert types["AskSize"] is ColumnType.DOUBLE  # long on the stream, double on /v3/quotes
    assert schema.column_names[:2] == ("Symbol", "Timestamp")


def test_the_merge_reports_a_type_conflict() -> None:
    made = Source(
        key="stocks/T",
        transport=W,
        fields=(
            Field(wire_key="sym", column="Ticker", wire_type=WireType.STRING),
            Field(wire_key="p", column="Price", wire_type=WireType.STRING),
        ),
    )
    stream = DatasetSpec(
        name="stocks.trades",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        history_path="/v3/trades/{stockTicker}",
        channels={NotBars.ANY: "T"},
    )

    def made_up(transport: Transport, key: str) -> Source:
        return made if transport is W else source(transport, key)

    with pytest.raises(
        ValueError,
        match=r"^member types: stocks\.trades: column 'Price' is double elsewhere but string "
        r"in stocks\.trades WS stocks/T$",
    ):
        dataset_schema(stream, (stream,), made_up)


def test_epochs_of_different_units_share_a_column() -> None:
    made = Source(
        key="stocks/T",
        transport=W,
        fields=(Field(wire_key="t", column="At", wire_type=WireType.EPOCH, unit=WireUnit.MS),),
    )
    other = Source(
        key="stocks/Q",
        transport=W,
        fields=(Field(wire_key="t", column="At", wire_type=WireType.EPOCH, unit=WireUnit.NS),),
    )
    stream = DatasetSpec(
        name="stocks.x",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        channels={NotBars.ANY: "T"},
    )
    second = DatasetSpec(
        name="stocks.y",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        channels={NotBars.ANY: "Q"},
    )
    tables = {"stocks/T": made, "stocks/Q": other}
    schema = dataset_schema(stream, (stream, second), lambda _, key: tables[key])
    assert [(c.name, c.type) for c in schema.columns] == [
        ("At", ColumnType.INSTANT),
        ("TimestampInsert", ColumnType.INSTANT),
    ]


# --- Column descriptions ---------------------------------------------------------------------


def described(text: str, wire_key: str, column: str) -> Field:
    return Field(wire_key=wire_key, column=column, wire_type=WireType.DOUBLE, description=text)


def test_a_plain_column_has_no_description_or_origin() -> None:
    plain = Column("Rows", ColumnType.LONG)
    assert (plain.description, plain.supplied_by, dict(plain.wire_keys)) == ("", (), {})
    assert hash(plain) == hash(Column("Rows", ColumnType.LONG))  # wire_keys is not hashed


def test_rest_text_wins_and_source_order_breaks_ties() -> None:
    # Two REST paths and one channel; the descriptions differ per source.
    dataset = DatasetSpec(
        name="stocks.x",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        history_path="/first",
        extra_paths=("/second",),
        channels={NotBars.ANY: "T"},
    )
    made = {
        "/first": Source(
            key="/first",
            transport=R,
            fields=(described("", "price", "Price"), described("First size.", "size", "Size")),
        ),
        "/second": Source(
            key="/second",
            transport=R,
            fields=(
                described("Second price.", "price", "Price"),
                described("Second size.", "size_2", "Size"),
            ),
        ),
        "stocks/T": Source(
            key="stocks/T",
            transport=W,
            fields=(
                described("WS price.", "p", "Price"),
                described("WS size.", "s", "Size"),
                described("", "f", "Flag"),
            ),
        ),
    }
    schema = dataset_schema(dataset, (dataset,), lambda _, key: made[key])
    columns = {c.name: c for c in schema.columns}
    # The first REST path has no text for Price, so the second REST path's wins over WS.
    assert columns["Price"].description == "Second price."
    # Both REST paths describe Size: the first declared source wins.
    assert columns["Size"].description == "First size."
    # Nothing describes Flag: an empty description, and nothing fails.
    assert columns["Flag"].description == ""
    assert columns["Price"].supplied_by == (R, W)
    assert columns["Flag"].supplied_by == (W,)
    # The distinct wire keys per transport, in source order.
    assert dict(columns["Price"].wire_keys) == {R: ("price",), W: ("p",)}
    assert dict(columns["Size"].wire_keys) == {R: ("size", "size_2"), W: ("s",)}


def test_a_dataset_takes_its_asset_class_s_description_of_a_shared_source() -> None:
    # One source serves several asset classes; its price text is corrected for options only.
    price = Field(
        wire_key="price",
        column="Price",
        wire_type=WireType.DOUBLE,
        description="Per whole share.",
        class_descriptions=(("options", "Per share of the underlying."),),
    )
    made = Source(key="/shared", transport=R, fields=(price,))
    found = {}
    for asset_class in (AssetClass.OPTIONS, AssetClass.STOCKS, ClassFromPrefix.ANY):
        dataset = DatasetSpec(
            name="reference.x",
            summary="A dataset for the tests.",
            asset_class=asset_class,
            fetch_path="/shared",
        )
        column, _ = dataset_schema(dataset, (dataset,), lambda *_: made).columns
        found[asset_class] = column.description
    assert found == {
        AssetClass.OPTIONS: "Per share of the underlying.",
        AssetClass.STOCKS: "Per whole share.",
        ClassFromPrefix.ANY: "Per whole share.",
    }


def test_a_key_column_quotes_its_asset_class_s_description() -> None:
    ticker = Field(
        wire_key="ticker",
        column="Ticker",
        wire_type=WireType.STRING,
        description="The stock symbol.",
        class_descriptions=(("options", "The option contract symbol."),),
    )
    made = Source(key="/shared", transport=R, fields=(ticker,))
    quoted = {}
    for asset_class in (AssetClass.OPTIONS, AssetClass.STOCKS):
        dataset = DatasetSpec(
            name="reference.x",
            summary="A dataset for the tests.",
            asset_class=asset_class,
            fetch_path="/shared",
            keys={(R, "/shared"): SourceKeys(KeySpec("Ticker"))},
        )
        column = dataset_schema(dataset, (dataset,), lambda *_: made).columns[0]  # then OptionRoot
        quoted[asset_class] = column.description.split("REST `ticker`: ")[1]
    assert quoted == {
        AssetClass.OPTIONS: '"The option contract symbol."',
        AssetClass.STOCKS: '"The stock symbol."',
    }


TRADES_KEYS = SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))
STOCK_TRADES = DatasetSpec(
    name="stocks.trades",
    summary="Trades of US-listed stocks, tick by tick.",
    asset_class=AssetClass.STOCKS,
    kind="trades",
    history_path="/v3/trades/{stockTicker}",
    channels={NotBars.ANY: "T"},
    flat_file_prefixes={NotBars.ANY: "us_stocks_sip/trades_v1"},
    keys={
        (R, "/v3/trades/{stockTicker}"): SourceKeys(
            KeySpec("stockTicker", from_request=True), KeySpec("SipTimestamp")
        ),
        (W, "stocks/T"): TRADES_KEYS,
        (F, "us_stocks_sip/trades_v1"): TRADES_KEYS,
    },
)
STOCK_LAST_TRADE = DatasetSpec(
    name="stocks.last_trade",
    summary="The latest trade of each US-listed stock.",
    asset_class=AssetClass.STOCKS,
    kind="trades",
    fetch_path="/v2/last/trade/{stocksTicker}",
    keys={(R, "/v2/last/trade/{stocksTicker}"): TRADES_KEYS},
)
CRYPTO_TRADES = DatasetSpec(
    name="crypto.trades",
    summary="Trades of crypto pairs, tick by tick.",
    asset_class=AssetClass.CRYPTO,
    kind="trades",
    history_path="/v3/trades/{cryptoTicker}",
    channels={NotBars.ANY: "XT"},
    keys={
        (R, "/v3/trades/{cryptoTicker}"): SourceKeys(
            KeySpec("cryptoTicker", from_request=True), KeySpec("ParticipantTimestamp")
        ),
        (W, "crypto/XT"): SourceKeys(KeySpec("Pair"), KeySpec("ParticipantTimestamp")),
    },
)
TRADES = Catalog((STOCK_TRADES, STOCK_LAST_TRADE, CRYPTO_TRADES))


def wire(transport: Transport, key: str, column: str) -> Field:
    return source(transport, key).field(column)


def test_a_dataset_keeps_only_the_columns_its_own_sources_fill() -> None:
    crypto_columns = {c.name: c for c in TRADES.dataset_schema(CRYPTO_TRADES).columns}
    stock_columns = {c.name: c for c in TRADES.dataset_schema(STOCK_TRADES).columns}
    # Only stocks fill `Tape`; crypto renames its own participant time to `Timestamp`.
    assert "Tape" not in crypto_columns
    assert "ParticipantTimestamp" not in crypto_columns
    assert {"Tape", "ParticipantTimestamp"} <= set(stock_columns)
    # The shared columns keep the kind's order and type, so the tables merge on them.
    shared = [name for name in stock_columns if name in crypto_columns]
    assert shared == [name for name in crypto_columns if name in stock_columns]
    assert all(stock_columns[n].type is crypto_columns[n].type for n in shared)
    # Each dataset gets its own text, never the kind's: the stocks Tape is the vendor's.
    assert stock_columns["Tape"].description == (
        wire(R, "/v3/trades/{stockTicker}", "Tape").description
    )


@pytest.mark.parametrize("name", sorted(CATALOG.by_name))
def test_every_dataset_column_is_supplied_by_some_transport(name: str) -> None:
    # A dataset has only the columns some transport of its own fills, library columns included.
    spec = CATALOG.by_name[name]
    for raw_symbol in (False, True):
        empty = [
            c.name
            for c in CATALOG.dataset_schema(spec, raw_symbol=raw_symbol).columns
            if not c.supplied_by
        ]
        assert empty == [], name


def test_tables_of_two_classes_merge_on_their_shared_columns(dh_exec_ctx: object) -> None:
    # The recipe the column reference gives for a cross-class merge.
    stock = CATALOG.dataset_schema(CATALOG.by_name["stocks.quotes"])
    fx = CATALOG.dataset_schema(CATALOG.by_name["forex.quotes"])
    assert stock.column_names != fx.column_names
    a = dh_arrow.to_table(stock.to_arrow([{"Symbol": "SPY", "BidPrice": 1.0}]))
    b = dh_arrow.to_table(fx.to_arrow([{"Symbol": "C:EURUSD", "BidPrice": 2.0}]))
    common = [c for c in a.column_names if c in b.column_names]
    both = merge([t.view(common) for t in (a, b)])
    assert both.column_names == common
    assert both.size == 2


def test_a_json_text_column_says_so_and_a_list_column_is_an_array() -> None:
    contracts_spec = CATALOG.by_name["options.contracts"]
    contracts = {c.name: c for c in CATALOG.dataset_schema(contracts_spec).columns}
    assert contracts["AdditionalUnderlyings"].type is ColumnType.STRING
    assert contracts["AdditionalUnderlyings"].description.endswith(JSON_TEXT_NOTE)
    news = {c.name: c for c in CATALOG.dataset_schema(CATALOG.by_name["reference.news"]).columns}
    assert news["Tickers"].type is ColumnType.STRING_ARRAY
    assert JSON_TEXT_NOTE not in news["Tickers"].description
    assert news["Insights"].description.endswith(JSON_TEXT_NOTE)


def test_every_transport_has_one_user_facing_spelling() -> None:
    assert dict(TRANSPORT_LABELS) == {
        Transport.REST: "REST",
        Transport.FLAT_FILES: "flat files",
        Transport.WS: "WebSocket",
    }
    with pytest.raises(TypeError):  # read-only: no caller can respell a transport
        cast("dict[Transport, str]", TRANSPORT_LABELS)[Transport.WS] = "WS"


def test_key_columns_quote_the_wire_key_or_the_request_parameter() -> None:
    columns = {c.name: c for c in TRADES.dataset_schema(STOCK_TRADES).columns}
    history = "/v3/trades/{stockTicker}"
    files = "us_stocks_sip/trades_v1"
    symbol = [
        "REST: the request's `stockTicker` parameter",
        f'flat files `ticker`: "{wire(F, files, "Ticker").description}"',
        f'WebSocket `sym`: "{wire(W, "stocks/T", "Ticker").description}"',
    ]
    assert columns["Symbol"].description == (
        f"The instrument, in the library's canonical symbol form. From {'; '.join(symbol)}"
    )
    assert dict(columns["Symbol"].wire_keys) == {R: (None,), F: ("ticker",), W: ("sym",)}
    assert columns["Symbol"].supplied_by == (R, F, W)
    timestamp = [
        f'REST `sip_timestamp`: "{wire(R, history, "SipTimestamp").description}"',
        f'flat files `sip_timestamp`: "{wire(F, files, "SipTimestamp").description}"',
        f'WebSocket `t`: "{wire(W, "stocks/T", "SipTimestamp").description}"',
    ]
    assert columns["Timestamp"].description == (
        f"The row's primary time, as an Instant. From {'; '.join(timestamp)}"
    )


def test_an_underlying_column_takes_its_own_sentence() -> None:
    contracts = (R, "/v3/reference/options/contracts")
    dataset = spec(
        "options.contracts",
        {contracts: SourceKeys(KeySpec("Ticker"), fanout=KeySpec("UnderlyingTicker"))},
        asset_class=AssetClass.OPTIONS,
    )
    underlying = next(
        c for c in dataset_schema(dataset, (dataset,), lookup).columns if c.name == "Underlying"
    )
    assert underlying.description == (
        "The instrument Massive links the row to, exactly as Massive sends it; for an option, "
        "the linked instrument, not what the contract delivers. From "
        f'REST `underlying_ticker`: "{wire(*contracts, "UnderlyingTicker").description}"'
    )


def test_a_vendor_field_named_like_a_key_keeps_its_vendor_text() -> None:
    # A `timestamp` field that is no declared time key is not the row's primary time.
    stamped = Field(
        wire_key="timestamp",
        column="Timestamp",
        wire_type=WireType.EPOCH,
        unit=WireUnit.MS,
        description="When the status was last updated.",
    )
    status = DatasetSpec(
        name="reference.x", summary="A dataset for the tests.", fetch_path="/status"
    )
    made = Source(key="/status", transport=R, fields=(stamped,))
    column, _ = dataset_schema(status, (status,), lambda *_: made).columns
    assert (column.name, column.description) == ("Timestamp", "When the status was last updated.")


def test_a_key_part_without_text_names_the_key_and_repeats_once() -> None:
    # Two REST paths rename the same undescribed wire key: it is named once, without a quote.
    plain = Field(wire_key="sym", column="Ticker", wire_type=WireType.STRING)
    dataset = DatasetSpec(
        name="reference.x",
        summary="A dataset for the tests.",
        fetch_path="/first",
        extra_paths=("/second",),
        keys={
            (R, "/first"): SourceKeys(KeySpec("Ticker")),
            (R, "/second"): SourceKeys(KeySpec("Ticker")),
        },
    )
    made = Source(key="/first", transport=R, fields=(plain,))
    schema = dataset_schema(dataset, (dataset,), lambda *_: made)
    assert schema.columns[0].description == (
        "The instrument, in the library's canonical symbol form. From REST `sym`."
    )


def description(dataset: DatasetSpec, name: str) -> str:
    columns = CATALOG.dataset_schema(dataset).columns
    return next(c.description for c in columns if c.name == name)


@pytest.mark.parametrize(
    ("dataset", "zone", "first"),
    [
        (
            stocks.BARS,
            "New York time",
            "a week's Monday, a month's, quarter's, or year's first weekday",
        ),
        (indices.BARS, "New York time", "a week's Sunday, a month's, quarter's, or year's 1st"),
        (forex.BARS, "UTC", "a week's Sunday, a month's, quarter's, or year's 1st)"),
        (crypto.BARS, "UTC", "a week's Sunday, a month's, quarter's, or year's 1st)"),
    ],
)
def test_a_bars_start_is_described_as_the_window_start(
    dataset: DatasetSpec, zone: str, first: str
) -> None:
    # Some sources' own text calls their stamp the window's end; the library's never is.
    described = description(dataset, "TimestampStart")
    assert described.startswith(
        "The start of the bar's window, as an Instant. A bar of a day or longer starts at "
        f"00:00 {zone} of its window's first day ("
    )
    assert first in described
    assert "each source's own stamp, quoted next, is moved there. From REST `" in described
    assert description(stocks.TRADES, "Timestamp").startswith(
        "The row's primary time, as an Instant. From REST `"
    )


def test_a_futures_bar_keeps_massive_s_window_start_and_its_session() -> None:
    assert description(futures.BARS, "TimestampStart").startswith(
        "The start of the bar's window, as an Instant: Massive's own window start. From REST "
        "`window_start`"
    )
    date_text = description(futures.BARS, "Date")
    assert date_text.startswith("The futures session of the bar (its trading date)")
    assert "From REST `session_end_date`" in date_text
    end = description(futures.BARS, "TimestampEnd")
    assert "17:00 Chicago time on its session date" in end
    assert "the later of that and Massive's `e`" in end
    trades = description(futures.TRADES, "Date")
    assert "the next 17:00 Chicago time" in trades
    assert "m.streams()" in trades


def test_the_library_describes_its_time_columns() -> None:
    stock_end = description(stocks.BARS, "TimestampEnd")
    assert stock_end.startswith("The exclusive end of the bar's window, as an Instant")
    assert 'on=["Symbol", "Timestamp>=TimestampEnd"]' in stock_end
    assert stock_end.endswith("at 00:00 New York time after its window's last day.")
    assert description(indices.BARS, "TimestampEnd").endswith("Chicago day is not verified.")
    assert description(stocks.BARS, "Date").startswith(
        "The trading day of the bar, as ISO yyyy-MM-dd text: the New York date of `TimestampStart`"
    )
    assert "join longer bars on `TimestampEnd`" in description(stocks.BARS, "Date")
    assert description(crypto.TRADES, "Date") == (
        "The trading day of the row, as ISO yyyy-MM-dd text: the UTC date of `Timestamp`."
    )
    insert = description(stocks.SNAPSHOTS, "TimestampInsert")
    assert insert.startswith("When the library received the row, as an Instant")


def test_the_daily_summaries_and_indicators_describe_their_windows() -> None:
    assert description(stocks.DAILY_OPEN_CLOSE, "TimestampStart") == (
        "The start of the day the summary covers, as an Instant: 00:00 New York time of `Date`."
    )
    assert description(stocks.DAILY_OPEN_CLOSE, "TimestampEnd") == (
        "The exclusive end of the day the summary covers, as an Instant: 00:00 New York time of "
        "the next day."
    )
    assert description(indices.DAILY_OPEN_CLOSE, "TimestampEnd").endswith(
        "Chicago day is not verified."
    )
    assert description(crypto.DAILY_OPEN_CLOSE, "Date").startswith(
        "The day the summary covers, as ISO yyyy-MM-dd text: a UTC calendar day. From REST `day`"
    )
    assert description(indicators.SMA, "TimestampStart").startswith(
        "The start of the bar the value is computed at, as an Instant."
    )
    assert "day zone of the symbol's class" in description(indicators.SMA, "TimestampEnd")
    assert "day zone of the symbol's class" in description(indicators.SMA, "Date")


def test_library_time_columns_are_supplied_by_every_transport() -> None:
    columns = {c.name: c for c in CATALOG.dataset_schema(futures.TRADES).columns}
    # Date comes from the rows' session over REST and the files, and is computed on the stream.
    assert columns["Date"].supplied_by == (R, F, W)
    assert dict(columns["Date"].wire_keys) == {
        R: ("session_end_date",),
        F: ("session_end_date",),
    }
    assert columns["TimestampInsert"].supplied_by == (R, F, W)
    assert dict(columns["TimestampInsert"].wire_keys) == {}
    names = CATALOG.dataset_schema(stocks.BARS).column_names
    assert names[:4] == ("Symbol", "Date", "TimestampStart", "TimestampEnd")
    assert names[-1] == "TimestampInsert"
    assert "EndTimestamp" not in names


def test_a_column_note_follows_the_description() -> None:
    made = Source(key="/notes", transport=R, fields=(described("A price.", "p", "Price"),))
    dataset = DatasetSpec(
        name="reference.x",
        summary="A dataset for the tests.",
        fetch_path="/notes",
        column_notes={"Price": "A note.", "TimestampInsert": "Another."},
    )
    columns = {c.name: c for c in dataset_schema(dataset, (dataset,), lambda *_: made).columns}
    assert columns["Price"].description == "A price. A note."
    assert columns["TimestampInsert"].description.endswith("the latest poll's. Another.")
    bare = Source(key="/notes", transport=R, fields=(described("", "p", "Price"),))
    alone = dataset_schema(dataset, (dataset,), lambda *_: bare).columns[0]
    assert alone.description == "A note."
    # A vendor text without its closing period still ends its sentence before the note.
    open_ended = Source(key="/notes", transport=R, fields=(described("A price", "p", "Price"),))
    closed = dataset_schema(dataset, (dataset,), lambda *_: open_ended).columns[0]
    assert closed.description == "A price. A note."
    assert "Split-adjusted when" in description(stocks.BARS, "Close")
    assert "multiplied by its ratio" in description(stocks.BARS, "Volume")


def test_the_merge_refuses_a_vendor_column_of_another_type_than_the_library_s() -> None:
    made = Source(
        key="/x",
        transport=R,
        fields=(Field(wire_key="i", column="TimestampInsert", wire_type=WireType.STRING),),
    )
    dataset = DatasetSpec(name="reference.x", summary="A dataset for the tests.", fetch_path="/x")
    with pytest.raises(
        ValueError,
        match=r"^member types: reference\.x: column 'TimestampInsert' is string in a source but "
        r"epoch as the library fills it$",
    ):
        dataset_schema(dataset, (dataset,), lambda *_: made)
