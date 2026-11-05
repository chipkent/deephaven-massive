"""Tests for the column types, `Schema`, and the merge of a kind's sources."""

from datetime import UTC, datetime

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import merge

from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.schema import (
    COLUMN_TYPES,
    Column,
    ColumnType,
    Schema,
    dataset_schema,
)
from deephaven_massive._enums import AssetClass, NotBars
from deephaven_massive._errors import MassiveError
from deephaven_massive._massive import Field, Source, Transport, WireType, WireUnit, source
from deephaven_massive._namespaces import futures, stocks
from deephaven_massive._session import CATALOG
from deephaven_massive._tables.columns import col_defs

R, W, F = Transport.REST, Transport.WS, Transport.FLAT_FILES
ALL_TYPES = Schema("all", tuple(Column(t.name.title().replace("_", ""), t) for t in ColumnType))
# The wire interface names `Source.memberships` but exports no constructor for an element.
MEMBERSHIP = type(source(W, "stocks/T").memberships[0])


# --- Column types and Schema -----------------------------------------------------------------


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
        "When `MassiveConfig.unreadable_symbols` is `dm.OnError.RAISE`, such a row fails the "
        "table instead, and the column is absent."
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


def test_a_plain_column_has_no_description_or_origin() -> None:
    plain = Column("Rows", ColumnType.LONG)
    assert (plain.description, plain.supplied_by, dict(plain.wire_keys)) == ("", (), {})
    assert hash(plain) == hash(Column("Rows", ColumnType.LONG))  # wire_keys is not hashed


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
